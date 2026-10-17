// Retention replays deterministic input, coarsens through four UTC tiers, then
// snapshots, copies one file and verifies a stable stored distribution.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/rollupdb/rollupdb"
	"github.com/rollupdb/rollupdb/internal/accuracy"
)

func main() {
	n := flag.Int("n", 20000, "observations (1..1000000)")
	flag.Parse()
	if err := run(*n); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(n int) error {
	if n < 1 || n > 1000000 {
		return fmt.Errorf("n must be in [1,1000000]")
	}
	ctx := context.Background()
	from := time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)
	now := from.Add(2 * time.Minute)
	dir, err := os.MkdirTemp("", "rollupdb-retention-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	db, err := rollupdb.Create(ctx, filepath.Join(dir, "live.rollupdb"), rollupdb.WithClock(func() time.Time { return now }))
	if err != nil {
		return err
	}
	defer db.Close()
	config := rollupdb.SketchConfig{Algorithm: rollupdb.SketchAlgorithm, EncodingVersion: 1, K: 200, M: 8}
	d := rollupdb.Definition{Name: "latency", Version: 1, EffectiveAt: from, Dimensions: []string{"service"}, Statistics: []string{"count", "sum", "min", "max", "avg", "variance_population", "variance_sample"}, MaxGroups: 1, Tiers: []rollupdb.Tier{{WidthSeconds: 1, RetainSeconds: 120, GraceSeconds: 120}, {WidthSeconds: 60, RetainSeconds: 3600, GraceSeconds: 120}, {WidthSeconds: 3600, RetainSeconds: 86400, GraceSeconds: 120}, {WidthSeconds: 86400, RetainSeconds: 0, GraceSeconds: 120}}, Sketch: &config}
	if err = db.CreateStream(ctx, d); err != nil {
		return err
	}
	values, err := accuracy.Dataset("skewed", n)
	if err != nil {
		return err
	}
	var rawBytes int64
	for offset := 0; offset < n; offset += rollupdb.MaxBatchSize {
		batch := rollupdb.Batch{Stream: d.Name, Version: 1}
		for i := offset; i < min(n, offset+rollupdb.MaxBatchSize); i++ {
			o := rollupdb.Observation{Time: from.Add(time.Duration(i%120) * time.Second), Value: values[i], Dimensions: map[string]string{"service": "web"}}
			encoded, e := json.Marshal(o)
			if e != nil {
				return e
			}
			rawBytes += int64(len(encoded) + 1) // NDJSON reference representation, outside engine
			batch.Observations = append(batch.Observations, o)
		}
		r, e := db.Ingest(ctx, batch)
		if e != nil {
			return e
		}
		if r.Accepted != int64(len(batch.Observations)) {
			return fmt.Errorf("unexpected rejection: %+v", r)
		}
	}
	beforeStatus, err := db.Status(ctx, d.Name)
	if err != nil {
		return err
	}
	q := rollupdb.Query{Stream: d.Name, Version: 1, From: from, To: from.Add(24 * time.Hour), Quantiles: []float64{0, .5, .9, .95, .99, 1}}
	before, err := db.Query(ctx, q)
	if err != nil {
		return err
	}
	finePath := filepath.Join(dir, "fine.rollupdb")
	if err = db.Snapshot(ctx, finePath); err != nil {
		return err
	}
	fine, err := os.Stat(finePath)
	if err != nil {
		return err
	}
	now = from.AddDate(5, 0, 0)
	advanced, err := db.Advance(ctx, d.Name)
	if err != nil {
		return err
	}
	after, err := db.Query(ctx, q)
	if err != nil {
		return err
	}
	if !after.Complete || after.Statistics["count"] != int64(n) || after.BucketsMerged != 1 || after.ResolutionSeconds != 86400 {
		return fmt.Errorf("wrong retained state: %+v", after)
	}
	for _, name := range []string{"count", "min", "max"} {
		if !reflect.DeepEqual(before.Statistics[name], after.Statistics[name]) {
			return fmt.Errorf("%s changed through promotion", name)
		}
	}
	for _, name := range []string{"sum", "avg", "variance_population", "variance_sample"} {
		a, b := before.Statistics[name], after.Statistics[name]
		if a == nil || b == nil {
			if a != b {
				return fmt.Errorf("%s availability changed", name)
			}
			continue
		}
		av, bv := a.(float64), b.(float64)
		if math.Abs(av-bv) > 1e-10+1e-12*math.Abs(av) {
			return fmt.Errorf("%s changed beyond tolerance", name)
		}
	}

	estimates := make([]float64, len(q.Quantiles))
	for i, e := range after.Quantiles {
		estimates[i] = *e.Value
	}
	measured, err := accuracy.Evaluate(values, q.Quantiles, estimates)
	if err != nil {
		return err
	}
	snapshot := filepath.Join(dir, "snapshot.rollupdb")
	if err = db.Snapshot(ctx, snapshot); err != nil {
		return err
	}
	bytes, err := os.ReadFile(snapshot)
	if err != nil {
		return err
	}
	copied := filepath.Join(dir, "copied.rollupdb")
	if err = os.WriteFile(copied, bytes, 0600); err != nil {
		return err
	}
	reopened, err := rollupdb.Open(ctx, copied, rollupdb.WithClock(func() time.Time { return now }))
	if err != nil {
		return err
	}
	defer reopened.Close()
	check, err := reopened.Query(ctx, q)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(check, after) {
		return fmt.Errorf("snapshot/copy/reopen changed results")
	}
	again, err := reopened.Advance(ctx, d.Name)
	if err != nil {
		return err
	}
	if again.SourceBuckets != 0 || again.ParentBuckets != 0 || again.ExpiredBuckets != 0 {
		return fmt.Errorf("repeat advancement did work: %+v", again)
	}
	repeated, err := reopened.Query(ctx, q)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(repeated, after) {
		return fmt.Errorf("repeat advancement changed state")
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(struct {
		Result                string                 `json:"result"`
		Dataset               string                 `json:"dataset"`
		Observations          int                    `json:"observations"`
		SimulatedYears        int                    `json:"simulated_years"`
		SerializedInputBytes  int64                  `json:"serialized_input_bytes"`
		FineSnapshotBytes     int64                  `json:"fine_snapshot_bytes"`
		RetainedSnapshotBytes int                    `json:"retained_snapshot_bytes"`
		RawToRetainedRatio    float64                `json:"raw_to_retained_ratio"`
		FineToRetainedRatio   float64                `json:"fine_to_retained_ratio"`
		BeforeBuckets         int                    `json:"before_buckets"`
		Advance               rollupdb.AdvanceResult `json:"advance"`
		Query                 rollupdb.QueryResult   `json:"retained_query"`
		Measurements          []accuracy.Measurement `json:"quantile_measurements"`
		Scope                 string                 `json:"measurement_scope"`
	}{"PASS", "skewed", n, 5, rawBytes, fine.Size(), len(bytes), float64(rawBytes) / float64(len(bytes)), float64(fine.Size()) / float64(len(bytes)), beforeStatus.Buckets, advanced, after, measured, "One group; two minutes of observations followed by a five-year clock jump. Input bytes are serialized observations plus newline. Sizes are completed snapshots including metadata/indexes, excluding WAL, peak working disk and external reference memory. No continuous five-year workload or throughput claim."})
}
