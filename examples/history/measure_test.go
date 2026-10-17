package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rollupdb/rollupdb"
)

func TestIndependentIntegerReferenceKnownDistribution(t *testing.T) {
	r := &reference{}
	for _, v := range []float64{2, 4, 4, 4, 5, 5, 7, 9} {
		r.add(v)
	}
	got := r.statistics()
	for key, value := range map[string]any{"count": int64(8), "sum": float64(40), "min": float64(2), "max": float64(9), "avg": float64(5), "variance_population": float64(4), "variance_sample": float64(32.0 / 7)} {
		if got[key] != value {
			t.Fatalf("%s: %v want %v", key, got[key], value)
		}
	}
}

func TestReferenceReadsExternalSourceAndFiltersTimeAndGroup(t *testing.T) {
	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "source.ndjson")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	encoder := json.NewEncoder(f)
	for _, o := range []rollupdb.Observation{
		{Time: start.Add(-time.Second), Value: 999, Dimensions: map[string]string{"service": "g0"}},
		{Time: start, Value: 2, Dimensions: map[string]string{"service": "g0"}},
		{Time: start.Add(time.Hour), Value: 8, Dimensions: map[string]string{"service": "g1"}},
		{Time: start.Add(24 * time.Hour), Value: 100, Dimensions: map[string]string{"service": "g0"}},
	} {
		if err = encoder.Encode(o); err != nil {
			t.Fatal(err)
		}
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	all, day, err := sourceReferences(path, start, start.Add(24*time.Hour), start, start.Add(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if all.statistics()["sum"] != float64(10) || all.statistics()["count"] != int64(2) || day.statistics()["sum"] != float64(2) || day.statistics()["count"] != int64(1) {
		t.Fatal("external reference included excluded records")
	}
	// Compare the independent result to a real query and ensure a gross mismatch
	// is rejected; this reference path never reads engine statistical state.
	db, err := rollupdb.Create(context.Background(), filepath.Join(t.TempDir(), "check.rollupdb"), rollupdb.WithClock(func() time.Time { return start.Add(24 * time.Hour) }))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	d := rollupdb.Definition{Name: "test", Version: 1, EffectiveAt: start, Statistics: []string{"count", "sum", "min", "max", "avg", "variance_population", "variance_sample"}, MaxGroups: 1, Tiers: []rollupdb.Tier{{WidthSeconds: 1, RetainSeconds: 0, GraceSeconds: 86400}}}
	if err = db.CreateStream(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Ingest(context.Background(), rollupdb.Batch{Stream: d.Name, Version: 1, Observations: []rollupdb.Observation{{Time: start, Value: 2}, {Time: start.Add(time.Hour), Value: 8}}}); err != nil {
		t.Fatal(err)
	}
	q, err := db.Query(context.Background(), rollupdb.Query{Stream: d.Name, Version: 1, From: start, To: start.Add(24 * time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = evaluate(all, q); err != nil {
		t.Fatal(err)
	}
	q.Statistics["count"] = int64(1)
	if _, err = evaluate(all, q); err == nil {
		t.Fatal("independent evaluator accepted lost observations")
	}
}
