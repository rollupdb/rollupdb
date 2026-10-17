// This experiment measures lossiness and exported size on declared, small-group
// workloads. It is not a general compression guarantee or a retention benchmark.
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
	"runtime"
	"time"

	"github.com/rollupdb/rollupdb"
	"github.com/rollupdb/rollupdb/internal/accuracy"
)

type caseReport struct {
	Dataset                      string                 `json:"dataset"`
	Layout                       string                 `json:"layout"`
	Observations                 int                    `json:"observations"`
	RawNDJSONBytes               int64                  `json:"raw_ndjson_bytes"`
	SnapshotBytes                int                    `json:"snapshot_bytes"`
	RawToSnapshotRatio           float64                `json:"raw_to_snapshot_ratio"`
	ReductionFraction            float64                `json:"reduction_fraction"`
	NormalizedRankErrorModel     float64                `json:"normalized_rank_error_model"`
	MaxObservedRankError         float64                `json:"max_observed_rank_error"`
	ObservedWithinEmpiricalModel bool                   `json:"observed_within_empirical_model"`
	MaxAbsoluteValueError        float64                `json:"max_absolute_value_error"`
	ExactStatisticsPreserved     bool                   `json:"exact_statistics_preserved"`
	QuantileValuesIdentical      bool                   `json:"quantile_values_identical_after_snapshot"`
	Measurements                 []accuracy.Measurement `json:"measurements,omitempty"`
}

func main() {
	n := flag.Int("n", 20000, "observations per dataset (1..1000000)")
	summary := flag.Bool("summary", false, "omit individual quantile measurements")
	flag.Parse()
	if err := run(*n, *summary); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(n int, summary bool) error {
	if n < 1 || n > 1000000 {
		return fmt.Errorf("n must be in 1..1000000")
	}
	report := struct {
		GeneratedAt time.Time             `json:"generated_at_utc"`
		GoVersion   string                `json:"go_version"`
		Platform    string                `json:"platform"`
		Sketch      rollupdb.SketchConfig `json:"sketch"`
		Note        string                `json:"note"`
		Cases       []caseReport          `json:"cases"`
	}{GeneratedAt: time.Now().UTC(), GoVersion: runtime.Version(), Platform: runtime.GOOS + "/" + runtime.GOARCH, Sketch: rollupdb.SketchConfig{Algorithm: rollupdb.SketchAlgorithm, EncodingVersion: 1, K: 200, M: 8}, Note: "Fixed deterministic inputs, randomized KLL compaction. One group per stream; direct layout has one second bucket, partitioned has seven. No promotion/expiry. Size is completed compact snapshot bytes versus serialized input NDJSON, excluding peak working/WAL/reference storage. Empirical 99% rank error is not a hard bound or relative value-error guarantee. Reference data is held outside the database for this experiment."}
	for _, name := range accuracy.DatasetNames {
		values, err := accuracy.Dataset(name, n)
		if err != nil {
			return err
		}
		for _, partitions := range []int{1, 7} {
			row, err := measure(name, values, partitions, report.Sketch)
			if err != nil {
				return err
			}
			if summary {
				row.Measurements = nil
			}
			report.Cases = append(report.Cases, row)
		}
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

func measure(name string, values []float64, partitions int, config rollupdb.SketchConfig) (caseReport, error) {
	ctx := context.Background()
	now := time.Date(2026, 10, 17, 12, 2, 0, 0, time.UTC)
	dir, err := os.MkdirTemp("", "rollupdb-accuracy-")
	if err != nil {
		return caseReport{}, err
	}
	defer os.RemoveAll(dir)
	db, err := rollupdb.Create(ctx, filepath.Join(dir, "live.rollupdb"), rollupdb.WithClock(func() time.Time { return now }))
	if err != nil {
		return caseReport{}, err
	}
	defer db.Close()
	d := rollupdb.Definition{Name: "telemetry", Version: 1, EffectiveAt: time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC), Dimensions: []string{"service"}, Statistics: []string{"count", "sum", "min", "max", "avg", "variance_population", "variance_sample"}, MaxGroups: 1, Tiers: []rollupdb.Tier{{WidthSeconds: 1, RetainSeconds: 0, GraceSeconds: 120}}, Sketch: &config}
	if err = db.CreateStream(ctx, d); err != nil {
		return caseReport{}, err
	}
	row := caseReport{Dataset: name, Layout: "direct", Observations: len(values)}
	if partitions > 1 {
		row.Layout = "partitioned_7"
	}
	batch := rollupdb.Batch{Stream: d.Name, Version: 1}
	flush := func() error {
		if len(batch.Observations) == 0 {
			return nil
		}
		r, err := db.Ingest(ctx, batch)
		if err != nil {
			return err
		}
		if r.Accepted != int64(len(batch.Observations)) {
			return fmt.Errorf("rejected experiment input: %+v", r)
		}
		batch.Observations = batch.Observations[:0]
		return nil
	}
	for i, value := range values {
		o := rollupdb.Observation{Time: now.Add(-time.Duration(i%partitions+1) * time.Second), Value: value, Dimensions: map[string]string{"service": "all"}}
		encoded, err := json.Marshal(o)
		if err != nil {
			return row, err
		}
		row.RawNDJSONBytes += int64(len(encoded) + 1)
		batch.Observations = append(batch.Observations, o)
		if len(batch.Observations) == rollupdb.MaxBatchSize {
			if err = flush(); err != nil {
				return row, err
			}
		}
	}
	if err = flush(); err != nil {
		return row, err
	}
	q := rollupdb.Query{Stream: d.Name, Version: 1, From: now.Add(-time.Duration(partitions) * time.Second), To: now, Quantiles: []float64{0, 0.5, 0.9, 0.95, 0.99, 1}}
	before, err := db.Query(ctx, q)
	if err != nil {
		return row, err
	}
	snapshot := filepath.Join(dir, "snapshot.rollupdb")
	if err = db.Snapshot(ctx, snapshot); err != nil {
		return row, err
	}
	bytes, err := os.ReadFile(snapshot)
	if err != nil {
		return row, err
	}
	copyPath := filepath.Join(dir, "copy.rollupdb")
	if err = os.WriteFile(copyPath, bytes, 0600); err != nil {
		return row, err
	}
	copyDB, err := rollupdb.Open(ctx, copyPath)
	if err != nil {
		return row, err
	}
	defer copyDB.Close()
	after, err := copyDB.Query(ctx, q)
	if err != nil {
		return row, err
	}
	row.ExactStatisticsPreserved = reflect.DeepEqual(before.Statistics, after.Statistics)
	if !row.ExactStatisticsPreserved {
		return row, fmt.Errorf("snapshot changed exact statistics")
	}
	row.QuantileValuesIdentical = reflect.DeepEqual(before.Quantiles, after.Quantiles)
	if partitions == 1 && !row.QuantileValuesIdentical {
		return row, fmt.Errorf("single stored sketch changed on snapshot/reopen")
	}
	row.SnapshotBytes = len(bytes)
	row.RawToSnapshotRatio = float64(row.RawNDJSONBytes) / float64(len(bytes))
	row.ReductionFraction = 1 - float64(len(bytes))/float64(row.RawNDJSONBytes)
	row.NormalizedRankErrorModel = *after.QuantileAccuracy.NormalizedRankError
	estimates := make([]float64, len(after.Quantiles))
	for i, estimate := range after.Quantiles {
		if estimate.Value == nil {
			return row, fmt.Errorf("missing experiment quantile")
		}
		estimates[i] = *estimate.Value
	}
	row.Measurements, err = accuracy.Evaluate(values, q.Quantiles, estimates)
	if err != nil {
		return row, err
	}
	for _, m := range row.Measurements {
		row.MaxObservedRankError = math.Max(row.MaxObservedRankError, m.RankError)
		row.MaxAbsoluteValueError = math.Max(row.MaxAbsoluteValueError, m.AbsoluteValueError)
	}
	row.ObservedWithinEmpiricalModel = row.MaxObservedRankError <= row.NormalizedRankErrorModel
	return row, nil
}
