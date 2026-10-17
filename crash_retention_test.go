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

// TestKilledOwnerAfterAdvanceKeepsFrontiersAndSketches verifies that an
// acknowledged retention transaction survives owner-process termination.
// This is a process-kill test, not a power-loss or mid-transaction kill test.
func TestKilledOwnerAfterAdvanceKeepsFrontiersAndSketches(t *testing.T) {
	const helperKey = "ROLLUPDB_ADVANCE_KILL_HELPER"
	const pathKey = "ROLLUPDB_ADVANCE_KILL_PATH"
	from := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	now := from.Add(5 * time.Minute)
	if os.Getenv(helperKey) == "1" {
		ctx := context.Background()
		clock := from.Add(2 * time.Minute)
		db, err := Create(ctx, os.Getenv(pathKey), WithClock(func() time.Time { return clock }))
		if err != nil {
			t.Fatal(err)
		}
		d := definition("advanced")
		d.Statistics = []string{"count", "sum", "avg"}
		d.Tiers = []Tier{{WidthSeconds: 1, RetainSeconds: 120, GraceSeconds: 120}, {WidthSeconds: 60, RetainSeconds: 0, GraceSeconds: 120}}
		sketch := defaultSketchConfig()
		d.Sketch = &sketch
		if err := db.CreateStream(ctx, d); err != nil {
			t.Fatal(err)
		}
		result, err := db.Ingest(ctx, batchOf(d,
			observation(from, 10, "web"),
			observation(from.Add(time.Second), 20, "web"),
			observation(from.Add(2*time.Second), 30, "web"),
		))
		if err != nil || result.Accepted != 3 {
			t.Fatalf("ingest: %+v %v", result, err)
		}
		clock = now
		advanced, err := db.Advance(ctx, d.Name)
		if err != nil || advanced.SourceBuckets != 3 || advanced.ParentBuckets != 1 {
			t.Fatalf("advance: %+v %v", advanced, err)
		}
		fmt.Fprintln(os.Stdout, "ACK_ADVANCE")
		// The parent kills the owner without closing SQLite.
		time.Sleep(24 * time.Hour)
		return
	}
	path := filepath.Join(t.TempDir(), "advanced-owner.rollupdb")
	cmd := exec.Command(os.Args[0], "-test.run=^TestKilledOwnerAfterAdvanceKeepsFrontiersAndSketches$")
	cmd.Env = append(os.Environ(), helperKey+"=1", pathKey+"="+path)
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
		if line != "ACK_ADVANCE" {
			t.Fatalf("unexpected helper output: %q", line)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("advance helper timed out")
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	reaped = true
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	db, err := Open(ctx, path, WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	d, err := db.Stream(ctx, "advanced")
	if err != nil {
		t.Fatal(err)
	}
	got, err := db.Query(ctx, Query{Stream: d.Name, Version: 1, From: from, To: from.Add(time.Minute), Quantiles: []float64{0, .5, 1}})
	if err != nil {
		t.Fatal(err)
	}
	if got.Statistics["count"] != int64(3) || got.Statistics["sum"] != float64(60) || got.ResolutionSeconds != 60 || got.BucketsMerged != 1 {
		t.Fatalf("promoted data lost or double counted: %+v", got)
	}
	if len(got.Quantiles) != 3 {
		t.Fatalf("promoted sketch missing: %+v", got.Quantiles)
	}
	for i, want := range []float64{10, 20, 30} {
		if got.Quantiles[i].Value == nil || *got.Quantiles[i].Value != want {
			t.Fatalf("quantile %d: %+v", i, got.Quantiles[i])
		}
	}
	status, err := db.Status(ctx, d.Name)
	if err != nil || status.Accepted != 3 || status.Buckets != 1 || status.Groups != 1 {
		t.Fatalf("status after crash: %+v %v", status, err)
	}
	again, err := db.Advance(ctx, d.Name)
	if err != nil || again.SourceBuckets != 0 || again.ParentBuckets != 0 || again.ExpiredBuckets != 0 {
		t.Fatalf("replayed committed promotion: %+v %v", again, err)
	}
	if len(again.Frontiers) != 2 || !again.Frontiers[0].Through.Equal(from.Add(3*time.Minute)) {
		t.Fatalf("lost committed frontier: %+v", again.Frontiers)
	}
}
