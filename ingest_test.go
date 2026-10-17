package rollupdb

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func ingestionDB(t *testing.T) (*DB, *time.Time, Definition, string) {
	t.Helper()
	now := time.Date(2026, 10, 17, 12, 2, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "events.rollupdb")
	db, err := Create(context.Background(), path, WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	d := definition("api")
	d.Statistics = []string{"count", "sum", "min", "max", "avg", "variance_population", "variance_sample"}
	if err = db.CreateStream(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	return db, &now, d, path
}

func observation(at time.Time, value float64, service string) Observation {
	return Observation{Time: at, Value: value, Dimensions: map[string]string{"service": service}}
}

func batchOf(d Definition, observations ...Observation) Batch {
	return Batch{Stream: d.Name, Version: 1, Observations: observations}
}

func queryRange(t *testing.T, db *DB, d Definition, from, to time.Time) QueryResult {
	t.Helper()
	r, err := db.Query(context.Background(), Query{Stream: d.Name, Version: 1, From: from, To: to})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestIngestionBoundariesOutOfOrderAndCoverage(t *testing.T) {
	db, now, d, _ := ingestionDB(t)
	ctx := context.Background()
	at := now.Add(-time.Second)
	batch := batchOf(d,
		observation(at, 10, "web"), observation(at.Add(-time.Second), 20, "web"), observation(at, 30, "worker"),
		observation(now.Add(-121*time.Second), 99, "late"),
		observation(now.Add(-120*time.Second), 40, "web"),
		observation(now.Add(time.Nanosecond), 2, "future"),
		observation(at, math.NaN(), "web"),
		Observation{Time: at, Value: 3, Dimensions: map[string]string{"unknown": "x"}},
		observation(d.EffectiveAt.Add(-time.Nanosecond), 3, "web"))
	r, err := db.Ingest(ctx, batch)
	if err != nil {
		t.Fatal(err)
	}
	if r.Counters != (Counters{Accepted: 4, RejectedInvalid: 4, RejectedLate: 1}) {
		t.Fatalf("outcomes: %+v", r)
	}
	// At 12:02:00, the 12:00:00 second closes at 12:02:01 and is
	// still open. The 11:59:59 second closes exactly now and is late.
	// The pre-effective event is invalid, so only one of the time cases is late.
	got := queryRange(t, db, d, now.Add(-121*time.Second), *now)
	if got.Complete || len(got.Missing) != 1 || got.Missing[0].Reason != "not_collected" || got.Statistics["count"] != int64(4) || got.Statistics["sum"] != float64(100) {
		t.Fatalf("coverage: %+v", got)
	}
}

func TestIngestQueryRestartAndSnapshot(t *testing.T) {
	db, now, d, path := ingestionDB(t)
	ctx := context.Background()
	at := now.Add(-time.Second)
	values := []float64{10, 20, 20, 20}
	for _, batch := range []Batch{batchOf(d, observation(at, values[0], "web")), batchOf(d, observation(at.Add(-time.Second), values[1], "worker"), observation(at, values[2], "worker"), observation(at, values[3], "worker"))} {
		r, err := db.Ingest(ctx, batch)
		if err != nil || r.Accepted != int64(len(batch.Observations)) {
			t.Fatalf("ingest: %+v %v", r, err)
		}
	}
	from, to := now.Add(-2*time.Second), *now
	before := queryRange(t, db, d, from, to)
	if !before.Complete || before.Statistics["count"] != int64(4) || before.Statistics["avg"] != 17.5 || before.BucketsMerged != 3 {
		t.Fatalf("query: %+v", before)
	}
	_, _, pop, sample := rationalReference(values)
	near(t, "population", before.Statistics["variance_population"].(float64), pop, 1e-12, 0)
	near(t, "sample", before.Statistics["variance_sample"].(float64), sample, 1e-12, 0)
	filtered, err := db.Query(ctx, Query{Stream: d.Name, Version: 1, From: from, To: to, Filter: map[string]string{"service": "web"}})
	if err != nil || filtered.Statistics["count"] != int64(1) || filtered.Statistics["variance_sample"] != nil {
		t.Fatalf("filter: %+v %v", filtered, err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, path, WithClock(func() time.Time { return *now }))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	after := queryRange(t, reopened, d, from, to)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("restart changed query")
	}
	dest := filepath.Join(t.TempDir(), "snapshot.rollupdb")
	if err = reopened.Snapshot(ctx, dest); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	copyPath := filepath.Join(t.TempDir(), "copy.rollupdb")
	if err = os.WriteFile(copyPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	copyDB, err := Open(ctx, copyPath)
	if err != nil {
		t.Fatal(err)
	}
	defer copyDB.Close()
	if got := queryRange(t, copyDB, d, from, to); !reflect.DeepEqual(before, got) {
		t.Fatal("snapshot changed state")
	}
	// A replay is explicitly a new observation, not a deduplicated update.
	if _, err = reopened.Ingest(ctx, batchOf(d, observation(at, 10, "web"))); err != nil {
		t.Fatal(err)
	}
	if got := queryRange(t, reopened, d, from, to); got.Statistics["count"] != int64(5) {
		t.Fatal("retry unexpectedly deduplicated")
	}
}

func TestGraceCutoffSurvivesClockRollbackAndRestart(t *testing.T) {
	db, now, d, path := ingestionDB(t)
	ctx := context.Background()
	event := now.Add(-120 * time.Second)
	r, err := db.Ingest(ctx, batchOf(d, observation(event, 1, "web")))
	if err != nil || r.Accepted != 1 {
		t.Fatalf("before cutoff: %+v %v", r, err)
	}
	*now = now.Add(time.Second) // exactly event bucket end + 120 seconds
	r, err = db.Ingest(ctx, batchOf(d, observation(event, 2, "web")))
	if err != nil || r.RejectedLate != 1 {
		t.Fatalf("at cutoff: %+v %v", r, err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(-2 * time.Second)
	reopened, err := Open(ctx, path, WithClock(func() time.Time { return *now }))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	r, err = reopened.Ingest(ctx, batchOf(d, observation(event, 3, "web")))
	if err != nil || r.RejectedLate != 1 {
		t.Fatalf("rollback reopened bucket: %+v %v", r, err)
	}
	s, err := reopened.Status(ctx, d.Name)
	if err != nil || s.Accepted != 1 || s.RejectedLate != 2 {
		t.Fatalf("counters: %+v %v", s, err)
	}
}

func TestGroupLimitsCanonicalTuplesAndNumericRejections(t *testing.T) {
	db, now, d, _ := ingestionDB(t)
	d2 := d
	d2.Name = "bounded"
	d2.MaxGroups = 1
	if err := db.CreateStream(context.Background(), d2); err != nil {
		t.Fatal(err)
	}
	at := now.Add(-time.Second)
	r, err := db.Ingest(context.Background(), batchOf(d2, observation(at, 1, "first"), observation(at, 2, "second"), observation(at, 3, "first")))
	if err != nil || r.Counters != (Counters{Accepted: 2, RejectedGroupLimit: 1}) {
		t.Fatalf("group admission: %+v %v", r, err)
	}
	r, err = db.Ingest(context.Background(), batchOf(d, observation(at, 1e308, "huge"), observation(at, -1e308, "huge")))
	if err != nil || r.Accepted != 1 || r.RejectedInvalid != 1 || r.Outcomes[1].Reason != "numeric_overflow" {
		t.Fatalf("numeric overflow: %+v %v", r, err)
	}
	keyA, err := tupleKey(Definition{Dimensions: []string{"a", "b"}}, map[string]string{"a": "a,b", "b": "c"})
	if err != nil {
		t.Fatal(err)
	}
	keyB, err := tupleKey(Definition{Dimensions: []string{"a", "b"}}, map[string]string{"b": "b,c", "a": "a"})
	if err != nil || keyA == keyB {
		t.Fatal("tuple collision")
	}
	if _, err = tupleKey(d, map[string]string{"service": strings.Repeat("x", 1025)}); err == nil {
		t.Fatal("unbounded value")
	}
	if _, err = tupleKey(d, map[string]string{"service": string([]byte{0xff})}); err == nil {
		t.Fatal("invalid UTF-8 admitted")
	}
}

func TestBatchFailureRollsBackStateGroupsCountersAndWatermark(t *testing.T) {
	db, now, d, _ := ingestionDB(t)
	ctx := context.Background()
	at := now.Add(-time.Second)
	if _, err := db.Ingest(ctx, batchOf(d, observation(at, 1, "original"))); err != nil {
		t.Fatal(err)
	}
	before, err := db.Status(ctx, d.Name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.sql.Exec(`CREATE TRIGGER fail_write BEFORE INSERT ON buckets BEGIN SELECT RAISE(ABORT,'injected storage failure'); END`); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(time.Second)
	r, err := db.Ingest(ctx, batchOf(d, observation(at, 2, "new_group"), observation(at, math.Inf(1), "invalid")))
	if err == nil || r.Accepted != 0 || len(r.Outcomes) != 0 {
		t.Fatalf("acknowledged failed batch: %+v %v", r, err)
	}
	after, err := db.Status(ctx, d.Name)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("partial writes survived: before=%+v after=%+v %v", before, after, err)
	}
	got := queryRange(t, db, d, at, at.Add(time.Second))
	if got.Statistics["sum"] != float64(1) {
		t.Fatal("state changed after rollback")
	}
}

func TestInvalidEnvelopeCancellationAndLongGrace(t *testing.T) {
	db, now, d, _ := ingestionDB(t)
	ctx := context.Background()
	at := now.Add(-time.Second)
	for _, b := range []Batch{{Stream: d.Name, Version: 1}, {Stream: d.Name, Version: 2, Observations: []Observation{observation(at, 1, "x")}}} {
		if _, err := db.Ingest(ctx, b); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("bad envelope: %v", err)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := db.Ingest(canceled, batchOf(d, observation(at, 1, "x"))); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	s, err := db.Status(ctx, d.Name)
	if err != nil || s.Accepted != 0 || s.CoverageStart != nil {
		t.Fatalf("bad envelope changed state: %+v %v", s, err)
	}
	d.Name = "long_grace"
	d.Tiers = []Tier{{WidthSeconds: 1, RetainSeconds: 0, GraceSeconds: 100 * 366 * 86400}}
	if err = db.CreateStream(ctx, d); err != nil {
		t.Fatal(err)
	}
	*now = time.Date(2199, 1, 1, 0, 0, 0, 0, time.UTC)
	r, err := db.Ingest(ctx, batchOf(d, observation(now.Add(-time.Second), 1, "x")))
	if err != nil || r.Accepted != 1 {
		t.Fatalf("grace arithmetic overflow: %+v %v", r, err)
	}
}

func TestConcurrentIngestDoesNotLoseUpdates(t *testing.T) {
	db, now, d, _ := ingestionDB(t)
	at := now.Add(-time.Second)
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := db.Ingest(context.Background(), batchOf(d, observation(at, 1, "web"))); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	r := queryRange(t, db, d, at, at.Add(time.Second))
	if r.Statistics["count"] != int64(20) {
		t.Fatalf("lost concurrent observations: %+v", r)
	}
}

func TestMinuteBucketsAreHalfOpen(t *testing.T) {
	db, now, d, _ := ingestionDB(t)
	d.Name = "minutes"
	d.Tiers = []Tier{{WidthSeconds: 60, RetainSeconds: 0, GraceSeconds: 120}}
	ctx := context.Background()
	if err := db.CreateStream(ctx, d); err != nil {
		t.Fatal(err)
	}
	start := now.Add(-2 * time.Minute)
	r, err := db.Ingest(ctx, batchOf(d, observation(start, 10, "web"), observation(start.Add(time.Minute-time.Nanosecond), 20, "web"), observation(start.Add(time.Minute), 30, "web")))
	if err != nil || r.Accepted != 3 {
		t.Fatalf("minute ingestion: %+v %v", r, err)
	}
	q := queryRange(t, db, d, start, *now)
	if q.BucketsMerged != 2 || q.Statistics["count"] != int64(3) || q.Statistics["avg"] != float64(20) {
		t.Fatalf("half-open buckets: %+v", q)
	}
	*now = now.Add(time.Minute)
	r, err = db.Ingest(ctx, batchOf(d, observation(start.Add(30*time.Second), 1, "web")))
	if err != nil || r.RejectedLate != 1 {
		t.Fatalf("minute cutoff: %+v %v", r, err)
	}
}

func TestLifetimeCounterOverflowRollsBackAcceptedState(t *testing.T) {
	db, now, d, _ := ingestionDB(t)
	ctx := context.Background()
	if _, err := db.sql.Exec("UPDATE stream_runtime SET accepted=? WHERE stream=?", int64(math.MaxInt64), d.Name); err != nil {
		t.Fatal(err)
	}
	r, err := db.Ingest(ctx, batchOf(d, observation(now.Add(-time.Second), 1, "web")))
	if !errors.Is(err, ErrNumericOverflow) || r.Accepted != 0 {
		t.Fatalf("counter overflow: %+v %v", r, err)
	}
	s, err := db.Status(ctx, d.Name)
	if err != nil || s.Accepted != math.MaxInt64 || s.Groups != 0 || s.Buckets != 0 || s.ObservedThrough != nil {
		t.Fatalf("counter overflow partially applied: %+v %v", s, err)
	}
}
