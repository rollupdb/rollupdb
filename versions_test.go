package rollupdb

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestUpdateStreamChangesSettingsAndKeepsHistory(t *testing.T) {
	ctx := context.Background()
	db, now, d, _ := retentionDB(t, []Tier{{1, 120, 120}, {60, 0, 120}}, false)
	d.Statistics = []string{"count", "avg"}
	from := fillTwoMinutes(t, db, now, d)

	next := d
	next.Version = 2
	next.Statistics = []string{"count", "sum", "min", "max", "avg"}
	next.MaxGroups = 5
	next.Tiers = []Tier{{1, 180, 120}, {60, 0, 60}}
	if err := db.UpdateStream(ctx, next); err != nil {
		t.Fatal(err)
	}
	current, err := db.Stream(ctx, d.Name)
	if err != nil || current.Version != 2 || current.MaxGroups != 5 || current.Tiers[0].RetainSeconds != 180 {
		t.Fatalf("current definition: %+v %v", current, err)
	}
	versions, err := db.StreamVersions(ctx, d.Name)
	if err != nil || len(versions) != 2 || versions[0].Version != 1 || versions[1].Version != 2 {
		t.Fatalf("versions: %+v %v", versions, err)
	}

	// Statistics added in version 2 cover the buckets written under version 1,
	// because every bucket keeps the full exact state.
	for _, version := range []int{1, 2} {
		r, err := db.Query(ctx, Query{Stream: d.Name, Version: version, From: from, To: from.Add(2 * time.Minute)})
		if err != nil {
			t.Fatal(err)
		}
		if r.DefinitionVersion != 2 || r.Statistics["count"] != int64(120) || r.Statistics["min"] != 0.0 || r.Statistics["max"] != 119.0 || r.Statistics["sum"] != 7140.0 {
			t.Fatalf("query as version %d: %+v", version, r)
		}
	}

	// Writers may use any existing version; a version that doesn't exist yet is refused.
	if _, err = db.Ingest(ctx, batchOf(d, observation(now.Add(-time.Second), 1, "web"))); err != nil {
		t.Fatalf("ingest as version 1: %v", err)
	}
	future := batchOf(d, observation(now.Add(-time.Second), 1, "web"))
	future.Version = 3
	if _, err = db.Ingest(ctx, future); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("ingest as version 3: %v", err)
	}
	if _, err = db.Query(ctx, Query{Stream: d.Name, Version: 3, From: from, To: from.Add(time.Minute)}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("query as version 3: %v", err)
	}
}

func TestUpdateStreamRefusesStructuralChanges(t *testing.T) {
	ctx := context.Background()
	db, _, d, _ := retentionDB(t, []Tier{{1, 120, 120}, {60, 0, 120}}, true)
	next := func(change func(*Definition)) Definition {
		n := d
		n.Version = 2
		n.Dimensions = append([]string(nil), d.Dimensions...)
		n.Tiers = append([]Tier(nil), d.Tiers...)
		sketch := *d.Sketch
		n.Sketch = &sketch
		change(&n)
		return n
	}
	cases := map[string]Definition{
		"skipped version":  next(func(n *Definition) { n.Version = 3 }),
		"same version":     next(func(n *Definition) { n.Version = 1 }),
		"dimensions":       next(func(n *Definition) { n.Dimensions = []string{"region"} }),
		"tier width":       next(func(n *Definition) { n.Tiers[1].WidthSeconds = 120 }),
		"tier count":       next(func(n *Definition) { n.Tiers = n.Tiers[:1] }),
		"effective time":   next(func(n *Definition) { n.EffectiveAt = n.EffectiveAt.Add(-time.Hour) }),
		"sketch removed":   next(func(n *Definition) { n.Sketch = nil }),
		"sketch k changed": next(func(n *Definition) { n.Sketch.K = 400 }),
	}
	for name, n := range cases {
		if err := db.UpdateStream(ctx, n); !errors.Is(err, ErrDefinitionChange) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if err := db.UpdateStream(ctx, next(func(n *Definition) { n.Name = "missing" })); !errors.Is(err, ErrStreamNotFound) {
		t.Errorf("missing stream: %v", err)
	}
	if err := db.UpdateStream(ctx, next(func(*Definition) {})); err != nil {
		t.Fatalf("an unchanged next version should be accepted: %v", err)
	}
	if err := db.CreateStream(ctx, next(func(n *Definition) { n.Name = "fresh" })); err == nil {
		t.Fatal("CreateStream accepted version 2")
	}
}

func TestSketchAddedLaterLeavesOlderBucketsExactOnly(t *testing.T) {
	ctx := context.Background()
	db, now, d, _ := retentionDB(t, []Tier{{1, 120, 120}, {60, 0, 120}}, false)
	from := fillTwoMinutes(t, db, now, d)
	next := d
	next.Version = 2
	sketch := defaultSketchConfig()
	next.Sketch = &sketch
	if err := db.UpdateStream(ctx, next); err != nil {
		t.Fatal(err)
	}
	// A reading into a bucket that predates the sketch keeps that bucket exact-only.
	last := from.Add(119 * time.Second)
	if r, err := db.Ingest(ctx, batchOf(next, observation(last, 500, "web"))); err != nil || r.Accepted != 1 {
		t.Fatalf("ingest into an older bucket: %+v %v", r, err)
	}
	ranks := []float64{0.5}
	old, err := db.Query(ctx, Query{Stream: d.Name, Version: 2, From: from, To: from.Add(2 * time.Minute), Quantiles: ranks})
	if err != nil {
		t.Fatal(err)
	}
	if old.Statistics["count"] != int64(121) || old.QuantileAccuracy == nil || old.QuantileAccuracy.Availability != "not_retained" || old.Quantiles[0].Value != nil {
		t.Fatalf("older range: %+v %+v", old.Statistics, old.QuantileAccuracy)
	}
	// Promotion of buckets without sketches works and gives exact-only parents.
	*now = from.Add(5 * time.Minute)
	if r, err := db.Advance(ctx, d.Name); err != nil || r.ParentBuckets != 4 {
		t.Fatalf("advance: %+v %v", r, err)
	}
	rolled, err := db.Query(ctx, Query{Stream: d.Name, Version: 2, From: from, To: from.Add(2 * time.Minute), Quantiles: ranks})
	if err != nil || rolled.Statistics["count"] != int64(121) || rolled.QuantileAccuracy.Availability != "not_retained" {
		t.Fatalf("after rollup: %+v %v", rolled, err)
	}
	// New buckets carry sketches from the start.
	at := now.Add(-time.Second)
	if r, err := db.Ingest(ctx, batchOf(next, observation(at, 1, "web"), observation(at, 3, "web"))); err != nil || r.Accepted != 2 {
		t.Fatalf("ingest into a new bucket: %+v %v", r, err)
	}
	fresh, err := db.Query(ctx, Query{Stream: d.Name, Version: 2, From: at, To: at.Add(time.Second), Quantiles: ranks})
	if err != nil || fresh.QuantileAccuracy.Availability != "available" || fresh.Quantiles[0].Value == nil {
		t.Fatalf("new range: %+v %v", fresh.QuantileAccuracy, err)
	}
}
