package rollupdb

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestKilledOwnerDuringAdvanceRollsBackPromotion kills the process after
// Advance has staged its bucket/frontier/watermark writes but before Commit.
// The context-scoped hook never runs in production without an explicit value.
func TestKilledOwnerDuringAdvanceRollsBackPromotion(t *testing.T) {
	const helper = "ROLLUPDB_ADVANCE_INFLIGHT_HELPER"
	const pathEnv = "ROLLUPDB_ADVANCE_INFLIGHT_PATH"
	from := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	if os.Getenv(helper) == "1" {
		ctx := context.Background()
		clock := from.Add(90 * time.Second)
		db, err := Create(ctx, os.Getenv(pathEnv), WithClock(func() time.Time { return clock }))
		if err != nil {
			t.Fatal(err)
		}
		d := definition("inflight")
		d.Tiers = []Tier{{WidthSeconds: 1, RetainSeconds: 120, GraceSeconds: 120}, {WidthSeconds: 60, RetainSeconds: 0, GraceSeconds: 120}}
		sketch := defaultSketchConfig()
		d.Sketch = &sketch
		if err := db.CreateStream(ctx, d); err != nil {
			t.Fatal(err)
		}
		r, err := db.Ingest(ctx, batchOf(d,
			observation(from, 10, "web"),
			observation(from.Add(time.Second), 20, "web"),
			observation(from.Add(2*time.Second), 30, "web"),
		))
		if err != nil || r.Accepted != 3 {
			t.Fatalf("setup: %+v %v", r, err)
		}
		clock = from.Add(5 * time.Minute)
		advanceCtx := context.WithValue(ctx, advanceBeforeCommitKey{}, func() {
			fmt.Fprintln(os.Stdout, "ADVANCE_UNCOMMITTED")
			// Parent terminates the owner while the SQL transaction is open.
			time.Sleep(24 * time.Hour)
		})
		if _, err := db.Advance(advanceCtx, d.Name); err != nil {
			t.Fatal(err)
		}
		t.Fatal("advance unexpectedly committed")
	}
	path := filepath.Join(t.TempDir(), "inflight.rollupdb")
	cmd := exec.Command(os.Args[0], "-test.run=^TestKilledOwnerDuringAdvanceRollsBackPromotion$")
	cmd.Env = append(os.Environ(), helper+"=1", pathEnv+"="+path)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	reaped := false
	defer func() {
		if !reaped {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	ack := make(chan string, 1)
	go func() {
		line, _ := bufio.NewReader(stdout).ReadString('\n')
		ack <- strings.TrimSpace(line)
	}()
	select {
	case line := <-ack:
		if line != "ADVANCE_UNCOMMITTED" {
			t.Fatalf("helper failed before transaction boundary: %q", line)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("inflight advance helper timed out")
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	reaped = true
	now := from.Add(5 * time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	db, err := Open(ctx, path, WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	d, err := db.Stream(ctx, "inflight")
	if err != nil {
		t.Fatal(err)
	}
	status, err := db.Status(ctx, d.Name)
	if err != nil || status.Accepted != 3 || status.Buckets != 3 || status.Groups != 1 {
		t.Fatalf("uncommitted promotion leaked into status: %+v %v", status, err)
	}
	var parents, frontiers int
	if err := db.sql.QueryRowContext(ctx, "SELECT count(*) FROM buckets WHERE stream=? AND tier=1", d.Name).Scan(&parents); err != nil {
		t.Fatal(err)
	}
	if err := db.sql.QueryRowContext(ctx, "SELECT count(*) FROM retention_frontiers WHERE stream=?", d.Name).Scan(&frontiers); err != nil {
		t.Fatal(err)
	}
	if parents != 0 || frontiers != 0 {
		t.Fatalf("partial promotion survived crash: parents=%d frontiers=%d", parents, frontiers)
	}
	advanced, err := db.Advance(ctx, d.Name)
	if err != nil || advanced.SourceBuckets != 3 || advanced.ParentBuckets != 1 {
		t.Fatalf("recovery retry did not promote once: %+v %v", advanced, err)
	}
	got, err := db.Query(ctx, Query{Stream: d.Name, Version: 1, From: from, To: from.Add(time.Minute), Quantiles: []float64{0, .5, 1}})
	if err != nil || got.Statistics["count"] != int64(3) || got.Statistics["avg"] != float64(20) || got.BucketsMerged != 1 {
		t.Fatalf("recovered promotion: %+v %v", got, err)
	}
	if len(got.Quantiles) != 3 {
		t.Fatalf("lost promoted sketch: %+v", got.Quantiles)
	}
	for i, want := range []float64{10, 20, 30} {
		if got.Quantiles[i].Value == nil || *got.Quantiles[i].Value != want {
			t.Fatalf("quantile %d: %+v", i, got.Quantiles[i])
		}
	}
}
