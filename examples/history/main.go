// History measures a bounded, regularly sampled multi-year replay and compares
// independently decoded source observations with retained state. It is a scoped
// experiment, not a general compression or production throughput benchmark.
package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"time"

	"github.com/rollupdb/rollupdb"
)

func fmtError(format string, args ...any) error { return fmt.Errorf(format, args...) }

type settings struct {
	RetainDays int `json:"final_retain_days_zero_means_forever"`
	Days       int `json:"days"`
	Slots      int `json:"slots_per_day"`
	Groups     int `json:"groups"`
	Repeats    int `json:"observations_per_slot_and_group"`
}
type timing struct {
	TotalSeconds         float64 `json:"total_case_seconds"`
	IngestSeconds        float64 `json:"engine_ingest_seconds"`
	AdvanceSeconds       float64 `json:"engine_advance_seconds"`
	QueryMilliseconds    float64 `json:"retained_query_milliseconds"`
	SnapshotMilliseconds float64 `json:"snapshot_milliseconds"`
}
type caseReport struct {
	Name                          string                `json:"name"`
	Settings                      settings              `json:"settings"`
	Observations                  int64                 `json:"observations"`
	SerializedInputBytes          int64                 `json:"serialized_input_bytes"`
	InputSHA256                   string                `json:"input_sha256"`
	SnapshotBytes                 int64                 `json:"snapshot_bytes"`
	InputToSnapshotRatio          float64               `json:"input_to_snapshot_ratio"`
	LiveBeforeSnapshot            fileSizes             `json:"live_before_snapshot"`
	ClosedMainBytes               int64                 `json:"closed_main_bytes"`
	PeakSampledLiveBytes          int64                 `json:"peak_sampled_live_bytes"`
	PeakSampledWorkingExportBytes int64                 `json:"peak_sampled_working_export_bytes"`
	ReferenceValues               int                   `json:"reference_values_in_memory"`
	Timings                       timing                `json:"timings"`
	Status                        rollupdb.StreamStatus `json:"status"`
	Query                         rollupdb.QueryResult  `json:"retained_query"`
	Comparison                    comparison            `json:"retained_comparison"`
	CopiedComparison              comparison            `json:"copied_comparison"`
	SingleBucketComparison        comparison            `json:"single_bucket_comparison"`
	StableSingleBucket            bool                  `json:"single_bucket_identical_after_restart_copy"`
	ExpiredOnly                   string                `json:"expired_only_availability"`
	PartialBucketRefused          bool                  `json:"partial_bucket_refused"`
}
type countWriter struct {
	writer io.Writer
	n      int64
}

func (w *countWriter) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	w.n += int64(n)
	return n, err
}

func main() {
	retainDays := flag.Int("retain-days", 0, "final fixed UTC retention days; 0 means forever, otherwise at least 8")
	days := flag.Int("days", 1825, "simulated fixed UTC days (10..3660)")
	slots := flag.Int("slots", 24, "regular observation times per day; must divide 86400")
	repeats := flag.Int("repeats", 16, "observations per time slot and group")
	groups := flag.Int("groups", 2, "baseline joint dimension groups")
	volumeFactor := flag.Int("volume-factor", 2, "repeat multiplier for fixed-group volume case")
	cardinalityFactor := flag.Int("cardinality-factor", 4, "group multiplier; divides baseline repeats to keep total events fixed")
	flag.Parse()
	if err := run(settings{Days: *days, Slots: *slots, Groups: *groups, Repeats: *repeats, RetainDays: *retainDays}, *volumeFactor, *cardinalityFactor); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(base settings, volumeFactor, cardinalityFactor int) error {
	if base.RetainDays < 0 || base.RetainDays > 3660 || (base.RetainDays > 0 && base.RetainDays < 8) || base.Days < 10 || base.Days > 3660 || base.Slots < 1 || base.Slots > 1440 || 86400%base.Slots != 0 || base.Groups < 1 || base.Groups > 32 || base.Repeats < 1 || base.Repeats > 1000 || volumeFactor < 2 || volumeFactor > 8 || cardinalityFactor < 2 || cardinalityFactor > 8 || base.Repeats%cardinalityFactor != 0 {
		return errors.New("invalid bounded settings; cardinality factor must divide repeats and slots must divide 86400")
	}
	cases := []struct {
		name string
		s    settings
	}{{"baseline", base}, {"more_volume_same_groups_and_times", settings{Days: base.Days, Slots: base.Slots, Groups: base.Groups, Repeats: base.Repeats * volumeFactor, RetainDays: base.RetainDays}}, {"more_groups_same_event_count_and_times", settings{Days: base.Days, Slots: base.Slots, Groups: base.Groups * cardinalityFactor, Repeats: base.Repeats / cardinalityFactor, RetainDays: base.RetainDays}}}
	for _, c := range cases {
		if int64(c.s.Days)*int64(c.s.Slots)*int64(c.s.Groups)*int64(c.s.Repeats) > 10000000 || c.s.Groups > 128 {
			return errors.New("case exceeds ten million observations or 128 groups")
		}
	}
	reports := make([]caseReport, 0, len(cases))
	for _, c := range cases {
		fmt.Fprintf(os.Stderr, "history: %s, %d days, %d groups, %d observations per slot/group\n", c.name, c.s.Days, c.s.Groups, c.s.Repeats)
		r, err := runCase(c.name, c.s)
		if err != nil {
			return fmt.Errorf("%s: %w", c.name, err)
		}
		reports = append(reports, r)
	}
	if reports[0].Status.Buckets != reports[1].Status.Buckets || reports[0].Status.Groups != reports[1].Status.Groups {
		return errors.New("volume experiment changed bucket/group count")
	}
	if reports[0].Observations != reports[2].Observations || reports[2].Status.Groups != reports[0].Status.Groups*cardinalityFactor {
		return errors.New("cardinality experiment changed total observations or wrong group count")
	}
	out := struct {
		Result           string       `json:"result"`
		Format           int          `json:"format_version"`
		OS               string       `json:"os"`
		Arch             string       `json:"arch"`
		GoVersion        string       `json:"go_version"`
		LogicalCPUs      int          `json:"logical_cpus"`
		CPU              string       `json:"cpu_model"`
		RecordedAt       time.Time    `json:"recorded_at"`
		Cases            []caseReport `json:"cases"`
		MeasurementScope string       `json:"measurement_scope"`
	}{"PASS", rollupdb.FormatVersion, runtime.GOOS, runtime.GOARCH, runtime.Version(), runtime.NumCPU(), cpuModel(), time.Now().UTC(), reports, "Regular per-day observation grid across the entire simulated interval, not continuous per-second telemetry. Daily batch ingestion and explicit daily retention; final advance adds 12h34m of covered idle time. Final retention is explicitly reported per case; zero means forever. Input-to-snapshot ratios with finite expiry include deliberately expired input and must not be described as aggregation-only savings. Input bytes/hash are actual NDJSON written outside the engine directory. Snapshots include metadata/indexes. Apparent file sizes are sampled every 10ms and after original ingestion/advancement/export/copy/close operations: directory scans are non-atomic and may miss transients or combine file lengths from different instants, so maxima are approximate and not physical allocation or guaranteed disk requirements. Working/export includes live database, sidecars, snapshot and copied file; source/reference disk and memory and SQLite temp files outside that directory are excluded. Engine phase timings exclude generation/reference evaluation; total case includes them. Shared execution environment; these measurements do not predict production throughput. KLL points vary across merges/runs; one stored sketch is compared identically after restart/copy. Rank regression checks use four times the empirical model plus 1/N, not a deterministic 99% guarantee."}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(out)
}

func runCase(name string, s settings) (report caseReport, err error) {
	started := time.Now()
	report.Name = name
	report.Settings = s
	root, err := os.MkdirTemp("", "rollupdb-history-")
	if err != nil {
		return report, err
	}
	defer os.RemoveAll(root)
	dbDir := filepath.Join(root, "database")
	if err = os.Mkdir(dbDir, 0700); err != nil {
		return report, err
	}
	sampler := startSampler(dbDir)
	samplerClosed := false
	defer func() {
		if !samplerClosed {
			_, _, _ = sampler.close()
		}
	}()
	path := filepath.Join(dbDir, "live.rollupdb")
	ctx := context.Background()
	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	now := start
	db, err := rollupdb.Create(ctx, path, rollupdb.WithClock(func() time.Time { return now }))
	if err != nil {
		return report, err
	}
	defer func() { _ = db.Close() }()
	config := rollupdb.SketchConfig{Algorithm: rollupdb.SketchAlgorithm, EncodingVersion: 1, K: 200, M: 8}
	d := rollupdb.Definition{Name: "telemetry", Version: 1, EffectiveAt: start, Dimensions: []string{"service"}, Statistics: []string{"count", "sum", "min", "max", "avg", "variance_population", "variance_sample"}, MaxGroups: s.Groups, Tiers: []rollupdb.Tier{{WidthSeconds: 1, RetainSeconds: 86400, GraceSeconds: 86400}, {WidthSeconds: 60, RetainSeconds: 86400, GraceSeconds: 86400}, {WidthSeconds: 3600, RetainSeconds: 7 * 86400, GraceSeconds: 86400}, {WidthSeconds: 86400, RetainSeconds: int64(s.RetainDays) * 86400, GraceSeconds: 86400}}, Sketch: &config}
	if err = db.CreateStream(ctx, d); err != nil {
		return report, err
	}
	sourcePath := filepath.Join(root, "reference.ndjson")
	source, err := os.OpenFile(sourcePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return report, err
	}
	defer source.Close()
	hash := sha256.New()
	counter := &countWriter{writer: io.MultiWriter(source, hash)}
	buffer := bufio.NewWriterSize(counter, 64<<10)
	encoder := json.NewEncoder(buffer)
	batch := rollupdb.Batch{Stream: d.Name, Version: 1}
	flush := func() error {
		if len(batch.Observations) == 0 {
			return nil
		}
		phase := time.Now()
		r, e := db.Ingest(ctx, batch)
		report.Timings.IngestSeconds += time.Since(phase).Seconds()
		if e != nil {
			return e
		}
		if r.Accepted != int64(len(batch.Observations)) {
			return fmt.Errorf("unexpected rejection: %+v", r)
		}
		report.Observations += r.Accepted
		batch.Observations = nil
		sampler.sample()
		return nil
	}
	for day := 0; day < s.Days; day++ {
		dayStart := start.Add(time.Duration(day) * 24 * time.Hour)
		now = dayStart.Add(24 * time.Hour)
		for slot := 0; slot < s.Slots; slot++ {
			at := dayStart.Add(time.Duration(slot) * 24 * time.Hour / time.Duration(s.Slots))
			for group := 0; group < s.Groups; group++ {
				for repeat := 0; repeat < s.Repeats; repeat++ {
					// Fixed bounded integer skew: ordinary values 0..999, periodic outliers
					// 1000..99999. Neither the engine nor KLL supplies the reference generator.
					x := (day*7919 + slot*3571 + group*101 + repeat*53) % 10000
					value := float64(x % 1000)
					if x%20 == 0 {
						value = float64(1000 + (x*97)%99000)
					}
					o := rollupdb.Observation{Time: at, Value: value, Dimensions: map[string]string{"service": fmt.Sprintf("g%d", group)}}
					if err = encoder.Encode(o); err != nil {
						return report, err
					}
					batch.Observations = append(batch.Observations, o)
					if len(batch.Observations) == rollupdb.MaxBatchSize {
						if err = flush(); err != nil {
							return report, err
						}
					}
				}
			}
		}
		if err = flush(); err != nil {
			return report, err
		}
		phase := time.Now()
		if _, err = db.Advance(ctx, d.Name); err != nil {
			return report, err
		}
		report.Timings.AdvanceSeconds += time.Since(phase).Seconds()
		sampler.sample()
	}
	if err = buffer.Flush(); err != nil {
		return report, err
	}
	if err = source.Close(); err != nil {
		return report, err
	}
	report.SerializedInputBytes = counter.n
	report.InputSHA256 = hex.EncodeToString(hash.Sum(nil))
	now = now.Add(12*time.Hour + 34*time.Minute)
	phase := time.Now()
	if _, err = db.Advance(ctx, d.Name); err != nil {
		return report, err
	}
	report.Timings.AdvanceSeconds += time.Since(phase).Seconds()
	sampler.sample()
	report.Status, err = db.Status(ctx, d.Name)
	if err != nil {
		return report, err
	}
	if report.Status.Accepted != report.Observations || report.Status.Groups != s.Groups {
		return report, errors.New("lifetime/group counts disagree")
	}
	// The independent availability boundary is calculated from the declared final
	// age, not from the query result or returned frontiers.
	expiredThrough := start
	if s.RetainDays > 0 {
		expiredThrough = now.Add(-time.Duration(s.RetainDays) * 24 * time.Hour).Truncate(24 * time.Hour)
	}
	availableFrom := maxTime(start, expiredThrough)
	q := rollupdb.Query{Stream: d.Name, Version: 1, From: start, To: now, Quantiles: []float64{0, .5, .9, .95, .99, 1}}
	phase = time.Now()
	report.Query, err = db.Query(ctx, q)
	report.Timings.QueryMilliseconds = float64(time.Since(phase)) / float64(time.Millisecond)
	if err != nil {
		return report, err
	}
	if report.Query.Covered == nil || !report.Query.Covered.From.Equal(availableFrom) || !report.Query.Covered.To.Equal(now) || len(report.Query.Resolutions) != 4 {
		return report, fmt.Errorf("wrong independent coverage/resolutions: %+v", report.Query)
	}
	dayFrom := availableFrom
	dayTo := dayFrom.Add(24 * time.Hour)
	singleQuery := rollupdb.Query{Stream: d.Name, Version: 1, From: dayFrom, To: dayTo, Filter: map[string]string{"service": "g0"}, Quantiles: q.Quantiles}
	single, err := db.Query(ctx, singleQuery)
	if err != nil {
		return report, err
	}
	if single.BucketsMerged != 1 || single.ResolutionSeconds != 86400 {
		return report, errors.New("single stored daily bucket not selected")
	}
	referenceAll, referenceDay, err := sourceReferences(sourcePath, availableFrom, now, dayFrom, dayTo)
	if err != nil {
		return report, err
	}
	report.ReferenceValues = len(referenceAll.values) + len(referenceDay.values)
	report.Comparison, err = evaluate(referenceAll, report.Query)
	if err != nil {
		return report, err
	}
	report.SingleBucketComparison, err = evaluate(referenceDay, single)
	if err != nil {
		return report, err
	}
	partial := singleQuery
	partial.From = partial.From.Add(time.Hour)
	_, err = db.Query(ctx, partial)
	report.PartialBucketRefused = errors.Is(err, rollupdb.ErrPartialBucket)
	if !report.PartialBucketRefused {
		return report, fmt.Errorf("partial daily cut was accepted: %v", err)
	}
	report.ExpiredOnly = "not_applicable_no_expired_input"
	if expiredThrough.After(start) {
		expiredQuery := q
		expiredQuery.To = start.Add(24 * time.Hour)
		expired, queryErr := db.Query(ctx, expiredQuery)
		if queryErr != nil {
			return report, queryErr
		}
		if expired.Statistics["count"] != nil || expired.QuantileAccuracy.Availability != "expired" {
			return report, errors.New("expired history fabricated data")
		}
		report.ExpiredOnly = expired.QuantileAccuracy.Availability
	}
	report.LiveBeforeSnapshot, err = directorySizes(dbDir)
	if err != nil {
		return report, err
	}
	snapshot := filepath.Join(dbDir, "snapshot.rollupdb")
	phase = time.Now()
	if err = db.Snapshot(ctx, snapshot); err != nil {
		return report, err
	}
	report.Timings.SnapshotMilliseconds = float64(time.Since(phase)) / float64(time.Millisecond)
	sampler.sample()
	info, err := os.Stat(snapshot)
	if err != nil {
		return report, err
	}
	report.SnapshotBytes = info.Size()
	report.InputToSnapshotRatio = float64(report.SerializedInputBytes) / float64(report.SnapshotBytes)
	copied := filepath.Join(dbDir, "copied.rollupdb")
	if err = copyFile(snapshot, copied); err != nil {
		return report, err
	}
	sampler.sample()
	if err = db.Close(); err != nil {
		return report, err
	}
	sampler.sample()
	info, err = os.Stat(path)
	if err != nil {
		return report, err
	}
	report.ClosedMainBytes = info.Size()
	for _, p := range []string{path, copied} {
		reopened, e := rollupdb.Open(ctx, p, rollupdb.WithClock(func() time.Time { return now }))
		if e != nil {
			return report, e
		}
		check, e := reopened.Query(ctx, singleQuery)
		if e != nil {
			reopened.Close()
			return report, e
		}
		if !reflect.DeepEqual(single, check) {
			reopened.Close()
			return report, errors.New("persisted single sketch changed on restart/copy")
		}
		// All-history query merges may randomize; evaluate against independent values
		// again rather than requiring identical approximate answers.
		merged, e := reopened.Query(ctx, q)
		if e != nil {
			reopened.Close()
			return report, e
		}
		report.CopiedComparison, e = evaluate(referenceAll, merged)
		if e != nil {
			reopened.Close()
			return report, e
		}
		again, e := reopened.Advance(ctx, d.Name)
		if e != nil {
			reopened.Close()
			return report, e
		}
		if again.SourceBuckets != 0 || again.ParentBuckets != 0 || again.ExpiredBuckets != 0 || again.ReclaimedGroups != 0 {
			reopened.Close()
			return report, errors.New("restart rerun did work")
		}
		check, e = reopened.Query(ctx, singleQuery)
		if e != nil || !reflect.DeepEqual(single, check) {
			reopened.Close()
			return report, errors.New("rerun altered persisted sketch")
		}
		if e = reopened.Close(); e != nil {
			return report, e
		}
	}
	report.StableSingleBucket = true
	report.PeakSampledLiveBytes, report.PeakSampledWorkingExportBytes, err = sampler.close()
	samplerClosed = true
	if err != nil {
		return report, err
	}
	report.Timings.TotalSeconds = time.Since(started).Seconds()
	return report, nil
}
func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
