package rollupdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/rollupdb/rollupdb/internal/accuracy"
)

func retentionDB(t *testing.T, tiers []Tier, sketch bool) (*DB, *time.Time, Definition, string) {
	t.Helper()
	db, now, d, path := ingestionDB(t)
	d.Name = "retained"
	d.MaxGroups = 2
	d.Tiers = tiers
	if sketch {
		c := defaultSketchConfig()
		d.Sketch = &c
	}
	if err := db.CreateStream(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	return db, now, d, path
}

func advanceOK(t *testing.T, db *DB, d Definition) AdvanceResult {
	t.Helper()
	r, err := db.Advance(context.Background(), d.Name)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func fillTwoMinutes(t *testing.T, db *DB, now *time.Time, d Definition) time.Time {
	t.Helper()
	from := now.Add(-2 * time.Minute)
	var obs []Observation
	for i := 0; i < 120; i++ {
		group := "web"
		if i%3 == 0 {
			group = "worker"
		}
		obs = append(obs, observation(from.Add(time.Duration(i)*time.Second), float64(i), group))
	}
	r, err := db.Ingest(context.Background(), batchOf(d, obs...))
	if err != nil || r.Accepted != 120 {
		t.Fatalf("fill: %+v %v", r, err)
	}
	return from
}

func TestPromotionBoundaryMixedQueryAndRestart(t *testing.T) {
	ctx := context.Background()
	db, now, d, path := retentionDB(t, []Tier{{1, 120, 120}, {60, 3600, 120}, {3600, 0, 120}}, true)
	from := fillTwoMinutes(t, db, now, d)
	before := queryRange(t, db, d, from, from.Add(2*time.Minute))
	*now = from.Add(3*time.Minute - time.Nanosecond)
	r := advanceOK(t, db, d)
	if r.SourceBuckets != 0 || !r.Frontiers[0].Through.Equal(from) {
		t.Fatalf("promoted before parent deadline: %+v", r)
	}
	*now = now.Add(time.Nanosecond)
	r = advanceOK(t, db, d)
	if r.SourceBuckets != 60 || r.ParentBuckets != 2 || !r.Frontiers[0].Through.Equal(from.Add(time.Minute)) {
		t.Fatalf("boundary: %+v", r)
	}
	after := queryRange(t, db, d, from, from.Add(2*time.Minute))
	if !after.Complete || after.BucketsMerged != 62 || len(after.Resolutions) != 2 || after.Resolutions[0].WidthSeconds != 60 || after.Resolutions[1].WidthSeconds != 1 || after.ResolutionSeconds != 60 {
		t.Fatalf("mixed plan: %+v", after)
	}
	for k, v := range before.Statistics {
		if k == "variance_population" || k == "variance_sample" {
			if math.Abs(v.(float64)-after.Statistics[k].(float64)) > 1e-10 {
				t.Fatal("variance changed")
			}
		} else if v != after.Statistics[k] {
			t.Fatalf("%s changed: %v %v", k, v, after.Statistics[k])
		}
	}
	q := Query{Stream: d.Name, Version: 1, From: from.Add(time.Second), To: from.Add(time.Minute), Filter: map[string]string{"service": "missing"}}
	if _, err := db.Query(ctx, q); !errors.Is(err, ErrPartialBucket) {
		t.Fatalf("split coarse even with empty filter: %v", err)
	}
	q.From = from
	q.To = from.Add(time.Minute + time.Second)
	if got, err := db.Query(ctx, q); err != nil || got.Statistics["count"] != int64(0) {
		t.Fatalf("filter: %+v %v", got, err)
	}
	// Lower-tier progress and the hour deadline permit a second promotion.
	*now = from.Add(2 * time.Hour)
	r = advanceOK(t, db, d)
	if r.ParentBuckets != 4 || r.SourceBuckets != 64 {
		t.Fatalf("two-stage promotion: %+v", r)
	}
	q = Query{Stream: d.Name, Version: 1, From: from, To: from.Add(time.Hour), Filter: map[string]string{"service": "web"}, Quantiles: []float64{0, .5, .99, 1}}
	stored, err := db.Query(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Statistics["count"] != int64(80) || stored.ResolutionSeconds != 3600 || stored.BucketsMerged != 1 {
		t.Fatalf("hour state: %+v", stored)
	}
	var values []float64
	for i := 0; i < 120; i++ {
		if i%3 != 0 {
			values = append(values, float64(i))
		}
	}
	estimates := make([]float64, len(stored.Quantiles))
	for i, e := range stored.Quantiles {
		estimates[i] = *e.Value
	}
	measured, err := accuracy.Evaluate(values, q.Quantiles, estimates)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range measured {
		if m.RankError > 1.0/80 {
			t.Fatalf("rank changed: %+v", m)
		}
	}
	snapshot := filepath.Join(t.TempDir(), "promoted.rollupdb")
	if err = db.Snapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	bytes, err := os.ReadFile(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	copyPath := filepath.Join(t.TempDir(), "copied.rollupdb")
	if err = os.WriteFile(copyPath, bytes, 0600); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{path, copyPath} {
		reopened, err := Open(ctx, p, WithClock(func() time.Time { return *now }))
		if err != nil {
			t.Fatal(err)
		}
		got, err := reopened.Query(ctx, q)
		if err != nil || !reflect.DeepEqual(got, stored) {
			t.Fatalf("restart changed persisted sketch/state: %+v %v", got, err)
		}
		again := advanceOK(t, reopened, d)
		if again.SourceBuckets != 0 || again.ParentBuckets != 0 || again.ExpiredBuckets != 0 {
			t.Fatalf("rerun duplicated: %+v", again)
		}
		got, err = reopened.Query(ctx, q)
		if err != nil || !reflect.DeepEqual(got, stored) {
			t.Fatal("idempotent run altered sketch")
		}
		reopened.Close()
	}
}

func TestExpiryAvailabilityAndGroupReclamation(t *testing.T) {
	db, now, d, _ := retentionDB(t, []Tier{{1, 120, 120}, {60, 180, 120}}, true)
	from := fillTwoMinutes(t, db, now, d)
	*now = from.Add(4*time.Minute - time.Nanosecond)
	if r := advanceOK(t, db, d); r.ExpiredBuckets != 0 {
		t.Fatal("expired early")
	}
	*now = now.Add(time.Nanosecond)
	r := advanceOK(t, db, d)
	if r.ExpiredBuckets != 2 || r.ReclaimedGroups != 0 {
		t.Fatalf("first minute expiry: %+v", r)
	}
	got := queryRange(t, db, d, from, from.Add(2*time.Minute))
	if got.Complete || got.Statistics["count"] != int64(60) || len(got.Missing) != 1 || got.Missing[0].Reason != "expired" || !got.Covered.From.Equal(from.Add(time.Minute)) {
		t.Fatalf("expiry gap: %+v", got)
	}
	q := Query{Stream: d.Name, Version: 1, From: from, To: from.Add(time.Minute), Quantiles: []float64{.5}}
	unavailable, err := db.Query(context.Background(), q)
	if err != nil || unavailable.Statistics["count"] != nil || unavailable.QuantileAccuracy.Availability != "expired" || unavailable.Quantiles[0].Value != nil {
		t.Fatalf("expired is not empty: %+v %v", unavailable, err)
	}
	*now = from.Add(5 * time.Minute)
	r = advanceOK(t, db, d)
	if r.ExpiredBuckets != 2 || r.ReclaimedGroups != 2 {
		t.Fatalf("last references: %+v", r)
	}
	status, err := db.Status(context.Background(), d.Name)
	if err != nil || status.Groups != 0 || status.Buckets != 0 || status.Accepted != 120 || !status.CoverageStart.Equal(from) {
		t.Fatalf("lost recorder history/counters: %+v %v", status, err)
	}
	// Empty retained time remains an ordinary zero, while expired time is null.
	empty := queryRange(t, db, d, from.Add(3*time.Minute), from.Add(4*time.Minute))
	if !empty.Complete || empty.Statistics["count"] != int64(0) {
		t.Fatalf("empty: %+v", empty)
	}
	// Space for a new key is available after final expiry.
	result, err := db.Ingest(context.Background(), batchOf(d, observation(*now, 7, "new"), observation(*now, 8, "other"), observation(*now, 9, "excess")))
	if err != nil || result.Accepted != 2 || result.RejectedGroupLimit != 1 {
		t.Fatalf("reclaimed key slots unavailable: %+v %v", result, err)
	}
}

func TestSparseAdvanceAndNoInventedCollection(t *testing.T) {
	db, now, d, _ := retentionDB(t, []Tier{{1, 120, 120}, {60, 3600, 120}, {3600, 86400, 120}, {86400, 0, 120}}, false)
	*now = time.Date(2199, 12, 30, 0, 0, 0, 0, time.UTC)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := db.Advance(ctx, d.Name); err != nil {
		t.Fatalf("empty centuries should be range work: %v", err)
	}
	s, err := db.Status(ctx, d.Name)
	if err != nil || s.CoverageStart != nil || s.Buckets != 0 {
		t.Fatalf("invented collection: %+v %v", s, err)
	}
	got := queryRange(t, db, d, d.EffectiveAt, d.EffectiveAt.Add(24*time.Hour))
	if got.Statistics["count"] != nil || got.Missing[0].Reason != "not_collected" {
		t.Fatal("advanced empty stream became historical zeros")
	}
	result, err := db.Ingest(ctx, batchOf(d, observation(*now, 3, "web")))
	if err != nil || result.Accepted != 1 {
		t.Fatalf("first input after sparse advance: %+v %v", result, err)
	}
	*now = now.Add(24 * time.Hour)
	if _, err = db.Advance(ctx, d.Name); err != nil {
		t.Fatal(err)
	}
}

func TestZeroGraceClockRollbackAndForeverRetention(t *testing.T) {
	db, now, d, _ := retentionDB(t, []Tier{{1, 1, 0}, {60, 0, 0}}, false)
	at := *now
	if r, err := db.Ingest(context.Background(), batchOf(d, observation(at, 5, "web"))); err != nil || r.Accepted != 1 {
		t.Fatalf("active: %+v %v", r, err)
	}
	*now = at.Add(time.Second)
	if r, err := db.Ingest(context.Background(), batchOf(d, observation(at, 9, "web"))); err != nil || r.RejectedLate != 1 {
		t.Fatalf("zero grace exact closure: %+v %v", r, err)
	}
	*now = at.Add(time.Minute)
	if r := advanceOK(t, db, d); r.SourceBuckets != 0 {
		t.Fatalf("minimum retention lost: %+v", r)
	}
	*now = at.Add(time.Minute + time.Second)
	if r := advanceOK(t, db, d); r.SourceBuckets != 1 {
		t.Fatalf("deadline: %+v", r)
	}
	*now = at.Add(30 * time.Second)
	if r, err := db.Ingest(context.Background(), batchOf(d, observation(at, 9, "web"))); err != nil || r.RejectedLate != 1 {
		t.Fatalf("reopened after rollback: %+v %v", r, err)
	}
	r := advanceOK(t, db, d)
	if !r.ObservedThrough.Equal(at.Add(time.Minute + time.Second)) {
		t.Fatal("advance rolled watermark back")
	}
	*now = latestTime.Add(-time.Nanosecond)
	if r = advanceOK(t, db, d); r.ExpiredBuckets != 0 || r.ReclaimedGroups != 0 {
		t.Fatalf("forever expired: %+v", r)
	}
	got := queryRange(t, db, d, at, at.Add(time.Minute))
	if got.Statistics["count"] != int64(1) || got.Statistics["sum"] != float64(5) {
		t.Fatalf("forever lost data: %+v", got)
	}
}

// Persistent logical state includes exact sketch bytes, so rollback cannot hide
// a mutated percentile sample behind an unchanged count.
func logicalRetentionState(t *testing.T, db *DB, d Definition) string {
	t.Helper()
	var state []any
	for _, query := range []string{`SELECT * FROM buckets WHERE stream=? ORDER BY tuple_key,tier,start_ns`, `SELECT * FROM retention_frontiers WHERE stream=? ORDER BY tier`, `SELECT * FROM stream_runtime WHERE stream=?`, `SELECT * FROM dimension_groups WHERE stream=? ORDER BY tuple_key`} {
		rows, err := db.sql.Query(query, d.Name)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			values := make([]any, len(columns))
			ptrs := make([]any, len(columns))
			for i := range values {
				ptrs[i] = &values[i]
			}
			if err = rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			state = append(state, values)
		}
		if err = rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
	}
	body, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func TestAdvanceRollbackAtEveryPublicationStage(t *testing.T) {
	for _, trigger := range []string{
		`CREATE TRIGGER fail BEFORE INSERT ON buckets WHEN NEW.tier=1 BEGIN SELECT RAISE(ABORT,'parent'); END`,
		`CREATE TRIGGER fail BEFORE DELETE ON buckets WHEN OLD.tier=0 BEGIN SELECT RAISE(ABORT,'source'); END`,
		`CREATE TRIGGER fail BEFORE INSERT ON retention_frontiers BEGIN SELECT RAISE(ABORT,'frontier'); END`,
		`CREATE TRIGGER fail BEFORE UPDATE ON stream_runtime BEGIN SELECT RAISE(ABORT,'watermark'); END`,
		`CREATE TRIGGER fail BEFORE DELETE ON dimension_groups BEGIN SELECT RAISE(ABORT,'reclaim'); END`,
	} {
		t.Run(trigger, func(t *testing.T) {
			db, now, d, _ := retentionDB(t, []Tier{{1, 120, 120}, {60, 180, 120}}, true)
			from := fillTwoMinutes(t, db, now, d)
			before := logicalRetentionState(t, db, d)
			*now = from.Add(6 * time.Minute)
			if _, err := db.sql.Exec(trigger); err != nil {
				t.Fatal(err)
			}
			result, err := db.Advance(context.Background(), d.Name)
			if err == nil || !reflect.DeepEqual(result, AdvanceResult{}) {
				t.Fatalf("failure acknowledged: %+v %v", result, err)
			}
			if after := logicalRetentionState(t, db, d); after != before {
				t.Fatal("partial publication survived rollback")
			}
			if _, err = db.sql.Exec(`DROP TRIGGER fail`); err != nil {
				t.Fatal(err)
			}
			r := advanceOK(t, db, d)
			if r.SourceBuckets != 120 || r.ExpiredBuckets != 4 || r.ReclaimedGroups != 2 {
				t.Fatalf("retry: %+v", r)
			}
		})
	}
}

func TestCorruptSketchAbortsPromotionWithoutProgress(t *testing.T) {
	db, now, d, _ := retentionDB(t, []Tier{{1, 120, 120}, {60, 0, 120}}, true)
	from := fillTwoMinutes(t, db, now, d)
	// The last group fails after another target has already been written in tx.
	if _, err := db.sql.Exec(`UPDATE buckets SET sketch=x'00' WHERE stream=? AND tuple_key='["worker"]'`, d.Name); err != nil {
		t.Fatal(err)
	}
	before := logicalRetentionState(t, db, d)
	*now = from.Add(4 * time.Minute)
	if _, err := db.Advance(context.Background(), d.Name); !errors.Is(err, ErrSketchEncoding) {
		t.Fatalf("corrupt sketch accepted: %v", err)
	}
	if logicalRetentionState(t, db, d) != before {
		t.Fatal("corrupt source partially published")
	}
}

func TestRevision3MigrationAndFailure(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(map[bool]string{false: "migrate", true: "rollback"}[conflict], func(t *testing.T) {
			path := revision2File(t)
			s, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.Exec(schemaV3); err != nil {
				t.Fatal(err)
			}
			if conflict {
				if _, err = s.Exec(`CREATE TABLE retention_frontiers(conflict INTEGER)`); err != nil {
					t.Fatal(err)
				}
			}
			s.Close()
			if _, err = Open(context.Background(), path); !errors.Is(err, ErrFormatVersion) {
				t.Fatal("implicit upgrade")
			}
			err = Migrate(context.Background(), path)
			if conflict {
				if err == nil {
					t.Fatal("accepted conflict")
				}
				s, err = sql.Open("sqlite", path)
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				var v, n int
				if err = s.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil {
					t.Fatal(err)
				}
				if err = s.QueryRow(`SELECT n FROM buckets`).Scan(&n); err != nil {
					t.Fatal(err)
				}
				if v != 3 || n != 4 {
					t.Fatalf("migration changed source: %d %d", v, n)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			now := time.Date(2026, 10, 17, 14, 0, 0, 0, time.UTC)
			db, err := Open(context.Background(), path, WithClock(func() time.Time { return now }))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			d, err := db.Stream(context.Background(), "legacy")
			if err != nil {
				t.Fatal(err)
			}
			r := advanceOK(t, db, d)
			if r.SourceBuckets != 1 || r.ParentBuckets != 1 {
				t.Fatalf("migrated state not promotable: %+v", r)
			}
			got := queryRange(t, db, d, time.Date(2026, 10, 17, 12, 1, 0, 0, time.UTC), time.Date(2026, 10, 17, 12, 2, 0, 0, time.UTC))
			if got.Statistics["count"] != int64(4) || got.Statistics["avg"] != 17.5 {
				t.Fatalf("migration lost state: %+v", got)
			}
		})
	}
}

func TestFourTierSparseHistoryKeepsWeightedStateAndEstimatedQuantiles(t *testing.T) {
	db, now, d, _ := retentionDB(t, []Tier{{1, 120, 120}, {60, 3600, 120}, {3600, 86400, 120}, {86400, 0, 120}}, true)
	ctx := context.Background()
	from := now.Add(-2 * time.Minute)
	values, err := accuracy.Dataset("skewed", 20000)
	if err != nil {
		t.Fatal(err)
	}
	// Unequal per-second weights plus a second burst two years later; no iteration
	// over the intervening empty seconds is needed for promotion.
	for offset := 0; offset < len(values); offset += 1000 {
		var obs []Observation
		for i := offset; i < offset+1000; i++ {
			obs = append(obs, observation(from.Add(time.Duration(i%91)*time.Second), values[i], "web"))
		}
		if r, e := db.Ingest(ctx, batchOf(d, obs...)); e != nil || r.Accepted != 1000 {
			t.Fatalf("fill: %+v %v", r, e)
		}
	}
	*now = from.AddDate(2, 0, 0)
	if r, e := db.Ingest(ctx, batchOf(d, observation(*now, 2000, "web"))); e != nil || r.Accepted != 1 {
		t.Fatalf("second burst: %+v %v", r, e)
	}
	deadline, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if _, err = db.Advance(deadline, d.Name); err != nil {
		t.Fatalf("sparse promotion: %v", err)
	}
	day := from.Truncate(24 * time.Hour)
	q := Query{Stream: d.Name, Version: 1, From: day, To: day.Add(24 * time.Hour), Quantiles: []float64{0, .5, .95, .99, 1}}
	got, err := db.Query(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if got.Statistics["count"] != int64(len(values)) || got.ResolutionSeconds != 86400 || got.BucketsMerged != 1 {
		t.Fatalf("four-tier state: %+v", got)
	}
	var sum, mean, m2 float64
	for i, v := range values {
		sum += v
		delta := v - mean
		mean += delta / float64(i+1)
		m2 += delta * (v - mean)
	}
	if math.Abs(got.Statistics["sum"].(float64)-sum) > 1e-8 || math.Abs(got.Statistics["variance_population"].(float64)-m2/float64(len(values))) > 1e-8 {
		t.Fatal("weighted state changed through four tiers")
	}
	estimates := make([]float64, len(q.Quantiles))
	for i, e := range got.Quantiles {
		estimates[i] = *e.Value
	}
	measured, err := accuracy.Evaluate(values, q.Quantiles, estimates)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range measured {
		if m.RankError > 4*(*got.QuantileAccuracy.NormalizedRankError)+1/float64(len(values)) {
			t.Fatalf("gross rank regression: %+v", m)
		}
	}
	// A day with no rows is still coarse authoritative history; lack of rows does
	// not grant finer detail or make the collected interval unavailable.
	emptyDay := day.Add(24 * time.Hour)
	empty := queryRange(t, db, d, emptyDay, emptyDay.Add(24*time.Hour))
	if !empty.Complete || empty.Statistics["count"] != int64(0) || empty.ResolutionSeconds != 86400 {
		t.Fatalf("empty sparse day: %+v", empty)
	}
	q.From = emptyDay.Add(time.Hour)
	q.To = emptyDay.Add(2 * time.Hour)
	if _, err = db.Query(ctx, q); !errors.Is(err, ErrPartialBucket) {
		t.Fatalf("empty coarse interval split: %v", err)
	}
	// Both the old daily bucket and new fine bucket reference the same key.
	status, err := db.Status(ctx, d.Name)
	if err != nil || status.Groups != 1 || status.Buckets != 2 {
		t.Fatalf("lost sparse group's retained references: %+v %v", status, err)
	}
}

func TestSingleTierExpiryAndMaximumPolicyArithmetic(t *testing.T) {
	db, now, d, _ := retentionDB(t, []Tier{{1, 120, 120}}, false)
	from := fillTwoMinutes(t, db, now, d)
	// Exactly one second expires at the exact age, with no target tier.
	*now = now.Add(time.Second)
	r := advanceOK(t, db, d)
	if r.ExpiredBuckets != 1 || r.ParentBuckets != 0 {
		t.Fatalf("single tier: %+v", r)
	}
	got := queryRange(t, db, d, from, from.Add(time.Second))
	if got.Statistics["count"] != nil || got.ResolutionSeconds != 0 || got.Missing[0].Reason != "expired" {
		t.Fatalf("expired single bucket: %+v", got)
	}
	d.Name = "long_policy"
	d.Tiers = []Tier{{1, 100 * 366 * 86400, 100 * 366 * 86400}, {86400, 0, 100 * 366 * 86400}}
	if err := db.CreateStream(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	*now = latestTime.Add(-time.Nanosecond)
	if r, err := db.Ingest(context.Background(), batchOf(d, observation(now.Add(-time.Second), 1, "web"))); err != nil || r.Accepted != 1 {
		t.Fatalf("maximum grace near supported end: %+v %v", r, err)
	}
	advanceOK(t, db, d)
}

func TestAdvanceInvalidClosedCanceledAndInactive(t *testing.T) {
	db, now, d, _ := retentionDB(t, []Tier{{1, 120, 120}}, false)
	before := logicalRetentionState(t, db, d)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := db.Advance(ctx, d.Name); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	if logicalRetentionState(t, db, d) != before {
		t.Fatal("canceled advance wrote state")
	}
	*now = d.EffectiveAt.Add(-time.Second)
	if _, err := db.Advance(context.Background(), d.Name); !errors.Is(err, ErrStreamNotActive) {
		t.Fatalf("inactive: %v", err)
	}
	*now = latestTime
	if _, err := db.Advance(context.Background(), d.Name); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("invalid clock: %v", err)
	}
	if _, err := db.Advance(context.Background(), "bad name"); !errors.Is(err, ErrInvalidRequest) {
		t.Fatal(err)
	}
	db.Close()
	if _, err := db.Advance(context.Background(), d.Name); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestIngestionAfterPromotionUsesFinestGraceOnly(t *testing.T) {
	db, now, d, _ := retentionDB(t, []Tier{{1, 120, 120}, {60, 3600, 3600}, {3600, 0, 3600}}, false)
	from := fillTwoMinutes(t, db, now, d)
	*now = from.Add(3 * time.Minute)
	advanceOK(t, db, d)
	result, err := db.Ingest(context.Background(), batchOf(d,
		observation(from.Add(time.Minute), 7, "web"),             // closes one second later
		observation(from.Add(time.Minute-time.Second), 9, "web"), // closes exactly now
		observation(from.Add(30*time.Second), 11, "web")))        // coarse grace does not admit it
	if err != nil || result.Accepted != 1 || result.RejectedLate != 2 {
		t.Fatalf("finest grace after promotion: %+v %v", result, err)
	}
	got := queryRange(t, db, d, from, from.Add(2*time.Minute))
	if got.Statistics["count"] != int64(121) || got.Statistics["sum"] != float64(7147) {
		t.Fatalf("late data duplicated/coarse corrected: %+v", got)
	}
	*now = from.Add(3*time.Minute + time.Second)
	result, err = db.Ingest(context.Background(), batchOf(d, observation(from.Add(time.Minute), 17, "web")))
	if err != nil || result.RejectedLate != 1 {
		t.Fatalf("exact closure after advancement: %+v %v", result, err)
	}
}

func TestRevision3MigrationPreservesPersistedSketchBytes(t *testing.T) {
	ctx := context.Background()
	db, now, d, path := retentionDB(t, []Tier{{1, 120, 120}, {60, 0, 120}}, true)
	if r, err := db.Ingest(ctx, batchOf(d, observation(now.Add(-time.Second), 1, "web"), observation(now.Add(-time.Second), 9, "web"))); err != nil || r.Accepted != 2 {
		t.Fatalf("fill: %+v %v", r, err)
	}
	q := Query{Stream: d.Name, Version: 1, From: now.Add(-time.Second), To: *now, Quantiles: []float64{0, .5, 1}}
	before, err := db.Query(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	var encoded []byte
	if err = db.sql.QueryRow(`SELECT sketch FROM buckets WHERE stream=?`, d.Name).Scan(&encoded); err != nil {
		t.Fatal(err)
	}
	// Revision 4 adds only the frontier table. Remove it to construct the exact
	// previous schema with real persisted sketch state rather than invented bytes.
	if _, err = db.sql.Exec(`DROP TABLE retention_frontiers; PRAGMA user_version=3;`); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	bytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Open(ctx, path); !errors.Is(err, ErrFormatVersion) {
		t.Fatalf("implicit migration: %v", err)
	}
	unchanged, err := os.ReadFile(path)
	if err != nil || !reflect.DeepEqual(bytes, unchanged) {
		t.Fatal("Open modified old file")
	}
	if err = Migrate(ctx, path); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	after, err := reopened.Query(ctx, q)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("migration changed query: %+v %v", after, err)
	}
	var retained []byte
	if err = reopened.sql.QueryRow(`SELECT sketch FROM buckets WHERE stream=?`, d.Name).Scan(&retained); err != nil || !reflect.DeepEqual(encoded, retained) {
		t.Fatal("migration rewrote sketch bytes")
	}
}
