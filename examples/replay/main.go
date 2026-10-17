// The replay example uses an explicit clock so it is reproducible on any day.
// It ingests exact statistics, snapshots, copies one file and verifies its result.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/rollupdb/rollupdb"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()
	now := time.Date(2026, 10, 17, 12, 2, 0, 0, time.UTC)
	dir, err := os.MkdirTemp("", "rollupdb-replay-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	db, err := rollupdb.Create(ctx, filepath.Join(dir, "live.rollupdb"), rollupdb.WithClock(func() time.Time { return now }))
	if err != nil {
		return err
	}
	defer db.Close()
	d := rollupdb.Definition{Name: "latency", Version: 1, EffectiveAt: time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC), Dimensions: []string{"service"}, Statistics: []string{"count", "sum", "min", "max", "avg", "variance_population", "variance_sample"}, MaxGroups: 100, Tiers: []rollupdb.Tier{{WidthSeconds: 1, RetainSeconds: 3600, GraceSeconds: 120}, {WidthSeconds: 60, RetainSeconds: 0, GraceSeconds: 120}}}
	if err = db.CreateStream(ctx, d); err != nil {
		return err
	}
	batch := rollupdb.Batch{Stream: d.Name, Version: 1}
	for i, value := range []float64{10, 20, 20, 20} {
		service := "worker"
		if i == 0 {
			service = "web"
		}
		batch.Observations = append(batch.Observations, rollupdb.Observation{Time: now.Add(-time.Duration(i%2+1) * time.Second), Value: value, Dimensions: map[string]string{"service": service}})
	}
	accepted, err := db.Ingest(ctx, batch)
	if err != nil {
		return err
	}
	q := rollupdb.Query{Stream: d.Name, Version: 1, From: now.Add(-2 * time.Second), To: now}
	before, err := db.Query(ctx, q)
	if err != nil {
		return err
	}
	if before.Statistics["count"] != int64(4) || before.Statistics["sum"] != float64(70) || before.Statistics["avg"] != 17.5 {
		return fmt.Errorf("unexpected statistics: %+v", before)
	}
	snapshot := filepath.Join(dir, "snapshot.rollupdb")
	if err = db.Snapshot(ctx, snapshot); err != nil {
		return err
	}
	bytes, err := os.ReadFile(snapshot)
	if err != nil {
		return err
	}
	copyPath := filepath.Join(dir, "copy.rollupdb")
	if err = os.WriteFile(copyPath, bytes, 0600); err != nil {
		return err
	}
	copyDB, err := rollupdb.Open(ctx, copyPath)
	if err != nil {
		return err
	}
	defer copyDB.Close()
	after, err := copyDB.Query(ctx, q)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(before, after) {
		return fmt.Errorf("snapshot/copy/reopen changed results")
	}
	return json.NewEncoder(os.Stdout).Encode(struct {
		Result        string               `json:"result"`
		Accepted      int64                `json:"accepted"`
		SnapshotBytes int                  `json:"snapshot_bytes"`
		Query         rollupdb.QueryResult `json:"query"`
	}{"PASS", accepted.Accepted, len(bytes), after})
}
