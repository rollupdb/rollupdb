package rollupdb

import (
	"bytes"
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/rollupdb/rollupdb/internal/accuracy"
)

func sketchStream(t *testing.T, db *DB, d Definition) Definition {
	t.Helper()
	d.Name = "quantiles"
	c := defaultSketchConfig()
	d.Sketch = &c
	if err := db.CreateStream(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	return d
}

func quantileQuery(d Definition, now time.Time) Query {
	return Query{Stream: d.Name, Version: 1, From: now.Add(-time.Second), To: now, Quantiles: []float64{0, 0.5, 0.9, 0.95, 0.99, 1}}
}

func TestQuantilePersistenceUpdateSnapshotAndUnavailableStates(t *testing.T) {
	db, now, d, path := ingestionDB(t)
	ctx := context.Background()
	d = sketchStream(t, db, d)
	q := quantileQuery(d, *now)
	r, err := db.Query(ctx, q)
	if err != nil || r.QuantileAccuracy.Availability != "not_collected" || r.Quantiles[0].Value != nil {
		t.Fatalf("uncollected: %+v %v", r, err)
	}
	values, _ := accuracy.Dataset("heavy_tail", 12000)
	for offset := 0; offset < len(values); offset += 4000 {
		batch := batchOf(d)
		for _, v := range values[offset : offset+4000] {
			batch.Observations = append(batch.Observations, observation(q.From, v, "web"))
		}
		if r, err := db.Ingest(ctx, batch); err != nil || r.Accepted != 4000 {
			t.Fatalf("sketch ingest: %+v %v", r, err)
		}
	}
	before, err := db.Query(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if before.Statistics["count"] != int64(len(values)) || before.QuantileAccuracy.Availability != "available" || !before.QuantileAccuracy.EstimationMode {
		t.Fatalf("quantile metadata: %+v", before)
	}
	emptyQuery := q
	emptyQuery.From = q.From.Add(-time.Second)
	emptyQuery.To = q.From
	empty, err := db.Query(ctx, emptyQuery)
	if err != nil || empty.QuantileAccuracy.Availability != "no_observations" || empty.Quantiles[1].Value != nil {
		t.Fatalf("empty: %+v %v", empty, err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, path, WithClock(func() time.Time { return *now }))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	after, err := reopened.Query(ctx, q)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("persisted single-bucket estimate changed: %v", err)
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
	copied, err := copyDB.Query(ctx, q)
	if err != nil || !reflect.DeepEqual(before, copied) {
		t.Fatalf("snapshot estimate changed: %v", err)
	}
	if r, err := reopened.Ingest(ctx, batchOf(d, observation(q.From, 123, "web"))); err != nil || r.Accepted != 1 {
		t.Fatalf("decoded sketch not updatable: %+v %v", r, err)
	}
}

func TestQuantileDatabaseMergesPartitionsAndFilters(t *testing.T) {
	db, now, d, _ := ingestionDB(t)
	ctx := context.Background()
	d = sketchStream(t, db, d)
	values, _ := accuracy.Dataset("skewed", 10000)
	batch := batchOf(d)
	for i, v := range values {
		service := "worker"
		if i%3 == 0 {
			service = "web"
		}
		batch.Observations = append(batch.Observations, observation(now.Add(-time.Duration(i%7+1)*time.Second), v, service))
	}
	if r, err := db.Ingest(ctx, batch); err != nil || r.Accepted != 10000 {
		t.Fatalf("ingestion: %+v %v", r, err)
	}
	q := quantileQuery(d, *now)
	q.From = now.Add(-7 * time.Second)
	r, err := db.Query(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if r.BucketsMerged != 14 || r.Statistics["count"] != int64(10000) {
		t.Fatalf("partition count: %+v", r)
	}
	check := func(q Query, reference []float64) {
		t.Helper()
		r, err := db.Query(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		estimates := make([]float64, len(r.Quantiles))
		for i, a := range r.Quantiles {
			estimates[i] = *a.Value
		}
		measurements, err := accuracy.Evaluate(reference, q.Quantiles, estimates)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range measurements {
			if m.RankError > 4*(*r.QuantileAccuracy.NormalizedRankError)+1/float64(len(reference)) {
				t.Fatalf("partition rank error %.6f", m.RankError)
			}
		}
	}
	check(q, values)
	q.Filter = map[string]string{"service": "web"}
	filtered := []float64{}
	for i, v := range values {
		if i%3 == 0 {
			filtered = append(filtered, v)
		}
	}
	check(q, filtered)
}

func TestCorruptSketchPreventsMutation(t *testing.T) {
	db, now, d, _ := ingestionDB(t)
	ctx := context.Background()
	d = sketchStream(t, db, d)
	q := quantileQuery(d, *now)
	if _, err := db.Ingest(ctx, batchOf(d, observation(q.From, 1, "web"), observation(q.From, 2, "web"))); err != nil {
		t.Fatal(err)
	}
	var original []byte
	if err := db.sql.QueryRow("SELECT sketch FROM buckets WHERE stream=?", d.Name).Scan(&original); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func([]byte) []byte{func(b []byte) []byte { b[4] = 99; return b }, func(b []byte) []byte { b[len(b)-1] ^= 1; return b }, func(b []byte) []byte { return nil }} {
		corrupt := mutate(append([]byte(nil), original...))
		if _, err := db.sql.Exec("UPDATE buckets SET sketch=? WHERE stream=?", corrupt, d.Name); err != nil {
			t.Fatal(err)
		}
		before, err := db.Status(ctx, d.Name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = db.Query(ctx, q); !errors.Is(err, ErrSketchEncoding) {
			t.Fatalf("corrupt query: %v", err)
		}
		r, err := db.Ingest(ctx, batchOf(d, observation(q.From, 3, "web")))
		if !errors.Is(err, ErrSketchEncoding) || r.Accepted != 0 {
			t.Fatalf("corrupt update: %+v %v", r, err)
		}
		after, err := db.Status(ctx, d.Name)
		if err != nil || !reflect.DeepEqual(before, after) {
			t.Fatal("corruption error mutated runtime")
		}
		var stored []byte
		if err = db.sql.QueryRow("SELECT sketch FROM buckets WHERE stream=?", d.Name).Scan(&stored); err != nil || !bytes.Equal(corrupt, stored) {
			t.Fatal("corrupt state overwritten")
		}
	}
}

func TestQuantileValidationAndExactOnlyHistory(t *testing.T) {
	db, now, d, _ := ingestionDB(t)
	ctx := context.Background()
	q := quantileQuery(d, *now)
	if _, err := db.Ingest(ctx, batchOf(d, observation(q.From, 5, "web"))); err != nil {
		t.Fatal(err)
	}
	r, err := db.Query(ctx, q)
	if err != nil || r.QuantileAccuracy.Availability != "not_retained" || r.Quantiles[0].Value != nil {
		t.Fatalf("invented exact-only distribution: %+v %v", r, err)
	}
	for _, ranks := range [][]float64{{-0.01}, {1.01}, {math.NaN()}, {math.Inf(1)}, {0.5, 0.5}, make([]float64, 33)} {
		q.Quantiles = ranks
		if _, err = db.Query(ctx, q); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("invalid quantiles: %v", err)
		}
	}
	d = sketchStream(t, db, d)
	q = quantileQuery(d, *now)
	if _, err = db.Ingest(ctx, batchOf(d, observation(q.From, 0, "web"), observation(q.From, 10, "web"), observation(q.From, 10, "web"))); err != nil {
		t.Fatal(err)
	}
	r, err = db.Query(ctx, q)
	if err != nil || r.QuantileAccuracy.EstimationMode || *r.QuantileAccuracy.NormalizedRankError != 0 || *r.Quantiles[1].Value != 10 {
		t.Fatalf("exact-mode quantile: %+v %v", r, err)
	}
}
