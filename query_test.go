package rollupdb

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestQueryCoverageDistinguishesMissingEmptyAndCurrent(t *testing.T) {
	db, now, d, _ := ingestionDB(t)
	ctx := context.Background()
	from, to := now.Add(-10*time.Second), now.Add(-9*time.Second)
	uncollected := queryRange(t, db, d, from, to)
	if uncollected.Covered != nil || uncollected.Statistics["count"] != nil || len(uncollected.Missing) != 1 {
		t.Fatalf("never collected: %+v", uncollected)
	}
	if _, err := db.Ingest(ctx, batchOf(d, observation(*now, 7, "web"))); err != nil {
		t.Fatal(err)
	}
	empty := queryRange(t, db, d, from, to)
	if !empty.Complete || empty.Statistics["count"] != int64(0) || empty.Statistics["sum"] != float64(0) || empty.Statistics["avg"] != nil || empty.Statistics["variance_population"] != nil {
		t.Fatalf("empty covered: %+v", empty)
	}
	missing := queryRange(t, db, d, d.EffectiveAt, d.EffectiveAt.Add(time.Second))
	if missing.Complete || missing.Statistics["count"] != nil {
		t.Fatalf("fabricated historical zero: %+v", missing)
	}
	current := queryRange(t, db, d, *now, now.Add(time.Second))
	if current.Complete || current.Statistics["count"] != int64(1) || current.Covered == nil || !current.Covered.To.Equal(now.Add(time.Nanosecond)) || len(current.Missing) != 1 || current.Missing[0].Reason != "not_observed_yet" {
		t.Fatalf("active bucket coverage: %+v", current)
	}
	if current.ResolutionSeconds != 1 || current.DefinitionVersion != 1 {
		t.Fatal("missing resolution/definition metadata")
	}
	future := queryRange(t, db, d, now.Add(time.Hour), now.Add(time.Hour+time.Second))
	if future.Statistics["count"] != nil || future.Missing[0].Reason != "not_observed_yet" {
		t.Fatalf("future coverage: %+v", future)
	}
}

func TestFinalSupportedBucketHasQueryableExclusiveEnd(t *testing.T) {
	db, now, d, _ := ingestionDB(t)
	*now = latestTime.Add(-time.Nanosecond)
	start := latestTime.Add(-time.Second)
	if r, err := db.Ingest(context.Background(), batchOf(d, observation(start, 1, "web"))); err != nil || r.Accepted != 1 {
		t.Fatalf("last bucket: %+v %v", r, err)
	}
	r := queryRange(t, db, d, start, latestTime)
	if !r.Complete || r.Statistics["count"] != int64(1) {
		t.Fatalf("exclusive upper bound: %+v", r)
	}
}

func TestQueryRejectsUnsupportedDetailAndUndeclaredFields(t *testing.T) {
	db, now, d, _ := ingestionDB(t)
	ctx := context.Background()
	base := Query{Stream: d.Name, Version: 1, From: now.Add(-2 * time.Second), To: *now}
	q := base
	q.From = q.From.Add(time.Nanosecond)
	if _, err := db.Query(ctx, q); !errors.Is(err, ErrPartialBucket) {
		t.Fatalf("partial boundary: %v", err)
	}
	q = base
	q.Filter = map[string]string{"customer": "missing"}
	if _, err := db.Query(ctx, q); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("invented dimension: %v", err)
	}
	q = base
	q.Version = 2
	if _, err := db.Query(ctx, q); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("unknown definition: %v", err)
	}
	d.Name = "count_only"
	d.Statistics = []string{"count"}
	if err := db.CreateStream(ctx, d); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Ingest(ctx, batchOf(d, observation(now.Add(-time.Second), 12, "web"))); err != nil {
		t.Fatal(err)
	}
	r := queryRange(t, db, d, base.From, base.To)
	if len(r.Statistics) != 1 || r.Statistics["count"] != int64(1) {
		t.Fatalf("exposed unselected state: %+v", r)
	}
	q = base
	q.Stream = d.Name
	q.Filter = map[string]string{"service": "no_match"}
	r, err := db.Query(ctx, q)
	if err != nil || r.Statistics["count"] != int64(0) {
		t.Fatalf("empty filter: %+v %v", r, err)
	}
}

func TestDimensionTupleOrderingAndNoDimensions(t *testing.T) {
	db, now, d, _ := ingestionDB(t)
	ctx := context.Background()
	d.Name = "pairs"
	d.Dimensions = []string{"left", "right"}
	if err := db.CreateStream(ctx, d); err != nil {
		t.Fatal(err)
	}
	at := now.Add(-time.Second)
	observations := []Observation{
		{Time: at, Value: 1, Dimensions: map[string]string{"left": "a,b", "right": "c"}},
		{Time: at, Value: 2, Dimensions: map[string]string{"right": "c", "left": "a,b"}},
		{Time: at, Value: 3, Dimensions: map[string]string{"left": "a", "right": "b,c"}},
	}
	if r, err := db.Ingest(ctx, batchOf(d, observations...)); err != nil || r.Accepted != 3 {
		t.Fatalf("tuples: %+v %v", r, err)
	}
	s, err := db.Status(ctx, d.Name)
	if err != nil || s.Groups != 2 {
		t.Fatalf("tuple order/collision: %+v %v", s, err)
	}
	d.Name = "scalar"
	d.Dimensions = nil
	if err = db.CreateStream(ctx, d); err != nil {
		t.Fatal(err)
	}
	if r, err := db.Ingest(ctx, batchOf(d, Observation{Time: at, Value: 1}, Observation{Time: at, Value: 2, Dimensions: map[string]string{}})); err != nil || r.Accepted != 2 {
		t.Fatalf("no dimensions: %+v %v", r, err)
	}
	s, err = db.Status(ctx, d.Name)
	if err != nil || s.Groups != 1 {
		t.Fatalf("scalar groups: %+v %v", s, err)
	}
}
