package rollupdb

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"
)

func countRows(t *testing.T, db *DB, query string, args ...any) int {
	t.Helper()
	var n int
	if err := db.sql.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// A batch sent twice under one ID is counted once; the retry gets the first
// result back, marked replayed.
func TestBatchIDRetryIsCountedOnce(t *testing.T) {
	ctx := context.Background()
	db, now, d, _ := ingestionDB(t)
	b := batchOf(d, observation(now.Add(-time.Second), 5, "web"), observation(now.Add(-200*time.Second), 1, "web"))
	b.BatchID = "job-7:1"
	first, err := db.Ingest(ctx, b)
	if err != nil || first.Accepted != 1 || first.RejectedLate != 1 || first.Replayed {
		t.Fatalf("first: %+v %v", first, err)
	}
	*now = now.Add(10 * time.Second)
	again, err := db.Ingest(ctx, b)
	if err != nil || !again.Replayed || again.Accepted != 1 || again.RejectedLate != 1 || !again.ObservedThrough.Equal(first.ObservedThrough) || len(again.Outcomes) != 2 || again.Outcomes[1].Status != first.Outcomes[1].Status {
		t.Fatalf("retry: %+v %v", again, err)
	}
	s, err := db.Status(ctx, d.Name)
	if err != nil || s.Accepted != 1 || s.RejectedLate != 1 {
		t.Fatalf("status counted the retry: %+v %v", s, err)
	}
	q := queryRange(t, db, d, b.Observations[0].Time, *now)
	if q.Statistics["count"] != int64(1) || q.Statistics["sum"] != 5.0 {
		t.Fatalf("data counted twice: %+v", q.Statistics)
	}
	// Same ID, same observations but times written in another zone: same content.
	zoned := b
	zoned.Observations = []Observation{
		observation(b.Observations[0].Time.In(time.FixedZone("x", 3600)), 5, "web"),
		b.Observations[1],
	}
	if r, err := db.Ingest(ctx, zoned); err != nil || !r.Replayed {
		t.Fatalf("same instant in another zone: %+v %v", r, err)
	}
}

func TestBatchIDWithOtherContentIsRefused(t *testing.T) {
	ctx := context.Background()
	db, now, d, _ := ingestionDB(t)
	b := batchOf(d, observation(now.Add(-time.Second), 5, "web"))
	b.BatchID = "x"
	if _, err := db.Ingest(ctx, b); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*Batch){
		"value":     func(c *Batch) { c.Observations = []Observation{observation(now.Add(-time.Second), 6, "web")} },
		"dimension": func(c *Batch) { c.Observations = []Observation{observation(now.Add(-time.Second), 5, "api")} },
		"time":      func(c *Batch) { c.Observations = []Observation{observation(now.Add(-2*time.Second), 5, "web")} },
		"extra": func(c *Batch) {
			c.Observations = []Observation{observation(now.Add(-time.Second), 5, "web"), observation(now.Add(-time.Second), 5, "web")}
		},
	} {
		c := b
		change(&c)
		if _, err := db.Ingest(ctx, c); !errors.Is(err, ErrBatchConflict) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if s, _ := db.Status(ctx, d.Name); s.Accepted != 1 {
		t.Fatalf("a refused batch wrote: %+v", s)
	}
	// IDs are per stream: another stream may use the same one.
	other := d
	other.Name = "other"
	if err := db.CreateStream(ctx, other); err != nil {
		t.Fatal(err)
	}
	ob := batchOf(other, observation(now.Add(-time.Second), 1, "web"))
	ob.BatchID = "x"
	if r, err := db.Ingest(ctx, ob); err != nil || r.Replayed || r.Accepted != 1 {
		t.Fatalf("same ID on another stream: %+v %v", r, err)
	}
}

func TestBatchIDValidation(t *testing.T) {
	ctx := context.Background()
	db, now, d, _ := ingestionDB(t)
	for _, id := range []string{strings.Repeat("a", 129), "has space", "tab\t", "é", "\x00"} {
		b := batchOf(d, observation(now.Add(-time.Second), 1, "web"))
		b.BatchID = id
		if _, err := db.Ingest(ctx, b); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("%q: %v", id, err)
		}
	}
	b := batchOf(d, observation(now.Add(-time.Second), 1, "web"))
	b.BatchID = strings.Repeat("~", 128)
	if _, err := db.Ingest(ctx, b); err != nil {
		t.Fatal(err)
	}
	// No ID: the same batch twice is two batches, as before.
	plain := batchOf(d, observation(now.Add(-time.Second), 1, "web"))
	for i := 0; i < 2; i++ {
		if r, err := db.Ingest(ctx, plain); err != nil || r.Replayed {
			t.Fatalf("plain: %+v %v", r, err)
		}
	}
	if s, _ := db.Status(ctx, d.Name); s.Accepted != 3 {
		t.Fatalf("accepted %d", s.Accepted)
	}
	if n := countRows(t, db, "SELECT count(*) FROM ingest_batches"); n != 1 {
		t.Fatalf("stored %d IDs", n)
	}
}

// A batch that fails rolls back its ID too, so a retry with the same ID runs.
func TestBatchIDNotStoredWhenIngestFails(t *testing.T) {
	ctx := context.Background()
	db, now, d, _ := ingestionDB(t)
	b := batchOf(d, observation(now.Add(-time.Second), 1, "web"))
	b.BatchID = "retry-me"
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := db.Ingest(cancelled, b); err == nil {
		t.Fatal("cancelled ingest succeeded")
	}
	if n := countRows(t, db, "SELECT count(*) FROM ingest_batches"); n != 0 {
		t.Fatalf("failed ingest stored its ID")
	}
	if r, err := db.Ingest(ctx, b); err != nil || r.Replayed || r.Accepted != 1 {
		t.Fatalf("retry: %+v %v", r, err)
	}
}

func TestAdvanceForgetsOldBatchIDs(t *testing.T) {
	ctx := context.Background()
	db, now, d, _ := retentionDB(t, []Tier{{1, 120, 120}, {60, 0, 120}}, false)
	db.batchIDRetention = time.Hour
	b := batchOf(d, observation(now.Add(-time.Second), 1, "web"))
	b.BatchID = "old"
	if _, err := db.Ingest(ctx, b); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(30 * time.Minute)
	nb := batchOf(d, observation(now.Add(-time.Second), 1, "web"))
	nb.BatchID = "new"
	if _, err := db.Ingest(ctx, nb); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(31 * time.Minute)
	advanceOK(t, db, d)
	if n := countRows(t, db, "SELECT count(*) FROM ingest_batches WHERE batch_id='old'"); n != 0 {
		t.Fatal("old ID kept past retention")
	}
	if n := countRows(t, db, "SELECT count(*) FROM ingest_batches WHERE batch_id='new'"); n != 1 {
		t.Fatal("new ID forgotten early")
	}
}

func TestWithBatchIDRetention(t *testing.T) {
	db, _, _, _ := ingestionDB(t)
	if db.batchIDRetention != DefaultBatchIDRetention {
		t.Fatalf("default %v", db.batchIDRetention)
	}
	d2 := &DB{}
	if err := WithBatchIDRetention(time.Minute)(d2); err != nil || d2.batchIDRetention != time.Minute {
		t.Fatalf("%v %v", d2.batchIDRetention, err)
	}
	d3 := &DB{batchIDRetention: DefaultBatchIDRetention}
	if err := WithBatchIDRetention(0)(d3); err != nil || d3.batchIDRetention != DefaultBatchIDRetention {
		t.Fatalf("zero changed it: %v", d3.batchIDRetention)
	}
}

// A revision-4 file (0.4 and earlier) is refused until migrate, which adds
// the batch table and keeps everything else.
func TestRevision4MigratesTo5(t *testing.T) {
	ctx := context.Background()
	db, now, d, path := ingestionDB(t)
	if _, err := db.Ingest(ctx, batchOf(d, observation(now.Add(-time.Second), 3, "web"))); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Exec("DROP TABLE ingest_batches; PRAGMA user_version=4"); err != nil {
		t.Fatal(err)
	}
	s.Close()
	if _, err = Open(ctx, path); !errors.Is(err, ErrFormatVersion) {
		t.Fatalf("revision 4 opened without migrate: %v", err)
	}
	if err = Migrate(ctx, path); err != nil {
		t.Fatal(err)
	}
	clock := *now
	db2, err := Open(ctx, path, WithClock(func() time.Time { return clock }))
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if st, err := db2.Status(ctx, d.Name); err != nil || st.Accepted != 1 {
		t.Fatalf("data after migrate: %+v %v", st, err)
	}
	b := batchOf(d, observation(clock.Add(-time.Second), 1, "web"))
	b.BatchID = "after"
	for i, want := range []bool{false, true} {
		if r, err := db2.Ingest(ctx, b); err != nil || r.Replayed != want {
			t.Fatalf("ingest %d: %+v %v", i, r, err)
		}
	}
	if i, err := db2.Info(ctx); err != nil || i.FormatVersion != 5 {
		t.Fatalf("inspect: %+v %v", i, err)
	}
}
