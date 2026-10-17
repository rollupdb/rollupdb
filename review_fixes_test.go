package rollupdb

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"
)

// A SQL statement cut short by its context must not leave the shared
// connection in query_only mode: writes afterwards still work.
func TestSQLCancelledQueryKeepsWritesWorking(t *testing.T) {
	db, now, d, _ := retentionDB(t, []Tier{{1, 120, 120}, {60, 0, 120}}, false)
	fillTwoMinutes(t, db, now, d)
	for _, wait := range []time.Duration{time.Millisecond, 20 * time.Millisecond, 100 * time.Millisecond} {
		ctx, cancel := context.WithTimeout(context.Background(), wait)
		_, err := db.SQL(ctx, "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 50000000) SELECT count(*) FROM n")
		cancel()
		if err == nil {
			t.Fatalf("a long statement finished inside %v", wait)
		}
		if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled statement: %v", err)
		}
		if r, err := db.Ingest(context.Background(), batchOf(d, observation(now.Add(-time.Second), 1, "web"))); err != nil || r.Accepted != 1 {
			t.Fatalf("ingest after a statement cancelled at %v: %+v %v", wait, r, err)
		}
	}
	if _, err := db.SQL(context.Background(), "SELECT count(*) FROM retained"); err != nil {
		t.Fatalf("SQL after cancellations: %v", err)
	}
}

// A later version with a longer tier-0 grace mustn't let Ingest write into
// time that's already been rolled up.
func TestLongerGraceDoesNotReopenRolledUpTime(t *testing.T) {
	ctx := context.Background()
	db, now, d, _ := retentionDB(t, []Tier{{1, 120, 120}, {60, 0, 120}}, false)
	from := fillTwoMinutes(t, db, now, d)
	*now = from.Add(5 * time.Minute)
	if r, err := db.Advance(ctx, d.Name); err != nil || r.ParentBuckets != 4 {
		t.Fatalf("advance: %+v %v", r, err)
	}
	next := d
	next.Version = 2
	next.Tiers = []Tier{{1, 600, 600}, {60, 0, 600}}
	if err := db.UpdateStream(ctx, next); err != nil {
		t.Fatal(err)
	}
	r, err := db.Ingest(ctx, batchOf(d, observation(from.Add(30*time.Second), 1000, "web"), observation(now.Add(-90*time.Second), 7, "web")))
	if err != nil || r.Accepted != 1 || r.RejectedLate != 1 || r.Outcomes[0].Status != "rejected_late" {
		t.Fatalf("ingest: %+v %v", r, err)
	}
	q, err := db.Query(ctx, Query{Stream: d.Name, Version: 2, From: from, To: from.Add(2 * time.Minute)})
	if err != nil || q.Statistics["count"] != int64(120) {
		t.Fatalf("rolled-up range changed: %+v %v", q.Statistics, err)
	}
	got, err := db.SQL(ctx, "SELECT count(*) FROM retained WHERE tier = 0 AND start < '"+from.Add(3*time.Minute).Format(time.RFC3339)+"'")
	if err != nil || got.Rows[0][0] != int64(0) {
		t.Fatalf("tier-0 rows below the frontier: %v %v", got.Rows, err)
	}
}

// A sketch added while the clock is behind the stored watermark starts after
// every bucket that exists, so none of them is expected to have one.
func TestSketchAddedAfterClockStepBack(t *testing.T) {
	ctx := context.Background()
	db, now, d, _ := retentionDB(t, []Tier{{1, 120, 120}, {60, 0, 120}}, false)
	from := fillTwoMinutes(t, db, now, d)
	watermark := *now
	*now = watermark.Add(-10 * time.Minute)
	next := d
	next.Version = 2
	sketch := defaultSketchConfig()
	next.Sketch = &sketch
	if err := db.UpdateStream(ctx, next); err != nil {
		t.Fatal(err)
	}
	current, err := db.Stream(ctx, d.Name)
	if err != nil || current.SketchSince == nil || !current.SketchSince.Equal(watermark.Add(time.Second)) {
		t.Fatalf("sketch_since: %+v %v", current.SketchSince, err)
	}
	*now = watermark
	if r, err := db.Ingest(ctx, batchOf(next, observation(from.Add(119*time.Second), 5, "web"))); err != nil || r.Accepted != 1 {
		t.Fatalf("ingest into an older bucket: %+v %v", r, err)
	}
	*now = from.Add(5 * time.Minute)
	if r, err := db.Advance(ctx, d.Name); err != nil || r.ParentBuckets != 4 {
		t.Fatalf("advance: %+v %v", r, err)
	}
	q, err := db.Query(ctx, Query{Stream: d.Name, Version: 2, From: from, To: from.Add(2 * time.Minute), Quantiles: []float64{0.5}})
	if err != nil || q.QuantileAccuracy.Availability != "not_retained" {
		t.Fatalf("query: %+v %v", q.QuantileAccuracy, err)
	}
}

// One parent can hold children from before and after a sketch was added.
// It becomes exact-only, and Advance doesn't fail on it.
func TestPromotionAcrossTheSketchStart(t *testing.T) {
	ctx := context.Background()
	db, now, d, _ := retentionDB(t, []Tier{{1, 120, 120}, {60, 0, 120}}, false)
	minute := now.Add(-time.Minute) // 12:01:00
	*now = minute.Add(40 * time.Second)
	if r, err := db.Ingest(ctx, batchOf(d, observation(minute.Add(30*time.Second), 1, "web"))); err != nil || r.Accepted != 1 {
		t.Fatalf("ingest before the sketch: %+v %v", r, err)
	}
	next := d
	next.Version = 2
	sketch := defaultSketchConfig()
	next.Sketch = &sketch
	if err := db.UpdateStream(ctx, next); err != nil {
		t.Fatal(err)
	}
	*now = minute.Add(55 * time.Second)
	if r, err := db.Ingest(ctx, batchOf(next, observation(minute.Add(50*time.Second), 3, "web"))); err != nil || r.Accepted != 1 {
		t.Fatalf("ingest after the sketch: %+v %v", r, err)
	}
	*now = minute.Add(5 * time.Minute)
	if r, err := db.Advance(ctx, d.Name); err != nil || r.ParentBuckets != 1 || r.SourceBuckets != 2 {
		t.Fatalf("advance: %+v %v", r, err)
	}
	q, err := db.Query(ctx, Query{Stream: d.Name, Version: 2, From: minute, To: minute.Add(time.Minute), Quantiles: []float64{0.5}})
	if err != nil || q.Statistics["count"] != int64(2) || q.QuantileAccuracy.Availability != "not_retained" {
		t.Fatalf("query: %+v %+v %v", q.Statistics, q.QuantileAccuracy, err)
	}
}

// Stream names that clash with the file's tables, or SQLite's reserved
// prefix, get a stream_ prefixed view; dimension columns stay unique.
func TestSQLViewNamesNeverShadowTables(t *testing.T) {
	ctx := context.Background()
	db, _ := newDB(t)
	for _, name := range []string{"buckets", "stream_definitions", "sqlite_things", "latency"} {
		d := definition(name)
		if name == "latency" {
			d.Dimensions = []string{"start", "dim_start"}
		}
		if err := db.CreateStream(ctx, d); err != nil {
			t.Fatal(err)
		}
	}
	got, err := db.SQL(ctx, "SELECT name FROM sqlite_temp_schema WHERE type = 'view' ORDER BY name")
	if err != nil {
		t.Fatal(err)
	}
	var views []string
	for _, row := range got.Rows {
		views = append(views, row[0].(string))
	}
	if strings.Join(views, ",") != "latency,stream_buckets,stream_sqlite_things,stream_stream_definitions" {
		t.Fatalf("views: %v", views)
	}
	if got, err = db.SQL(ctx, "SELECT name FROM pragma_table_info('latency') ORDER BY cid"); err != nil {
		t.Fatal(err)
	}
	var columns []string
	for _, row := range got.Rows {
		columns = append(columns, row[0].(string))
	}
	if strings.Join(columns, ",") != "start,end,width_seconds,tier,dim_start,dim_dim_start,count,sum,min,max,mean,m2,sketch" {
		t.Fatalf("columns: %v", columns)
	}
	if _, err = db.SQL(ctx, "SELECT count(*) FROM stream_buckets"); err != nil {
		t.Fatal(err)
	}
}

// rollup_quantile checks a sketch passed as a literal the same way Ingest
// checks a stored one, even when its checksum is right.
func TestRollupQuantileValidatesLiteralSketches(t *testing.T) {
	ctx := context.Background()
	db, _ := newDB(t)
	values := make([]float64, 1000)
	for i := range values {
		values[i] = float64(i)
	}
	s, _ := makeSketch(t, defaultSketchConfig(), values)
	encoded, err := s.encode()
	if err != nil {
		t.Fatal(err)
	}
	query := func(blob []byte) (SQLResult, error) {
		return db.SQL(ctx, "SELECT rollup_quantile(x'"+hex.EncodeToString(blob)+"', 0.5)")
	}
	got, err := query(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if median, ok := got.Rows[0][0].(float64); !ok || median < 400 || median > 600 {
		t.Fatalf("median: %v", got.Rows)
	}
	tampered := append([]byte(nil), encoded...)
	binary.LittleEndian.PutUint64(tampered[sketchHeaderBytes+8:], 1<<40) // claim far more items
	sum := sha256.Sum256(tampered[sketchHeaderBytes:])
	copy(tampered[16:48], sum[:])
	for name, blob := range map[string][]byte{"tampered count": tampered, "short": encoded[:sketchHeaderBytes+4], "flag byte": append(append([]byte(nil), encoded[:9]...), append([]byte{1}, encoded[10:]...)...)} {
		if _, err = query(blob); !errors.Is(err, ErrSQL) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// AdvanceAll keeps the results of streams that rolled up when another fails,
// and RunRetention reports them even when the run is cancelled.
func TestAdvanceAllKeepsResultsAlongsideFailures(t *testing.T) {
	ctx := context.Background()
	db, now, d, _ := retentionDB(t, []Tier{{1, 120, 120}, {60, 0, 120}}, false)
	db.maxClockStep, db.stepSet = time.Hour, true
	fillTwoMinutes(t, db, now, d)
	other := definition("other")
	if err := db.CreateStream(ctx, other); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(2 * time.Hour)
	results, err := db.AdvanceAll(ctx)
	if !errors.Is(err, ErrClockJump) || !strings.Contains(err.Error(), `"retained"`) {
		t.Fatalf("error: %v", err)
	}
	if _, ok := results["other"]; !ok || results == nil {
		t.Fatalf("results: %+v", results)
	}
}
