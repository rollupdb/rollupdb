package rollupdb

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// TestKilledOwnerPreservesAcknowledgedIngestAndSketch verifies an acknowledged
// ingestion transaction after forced process termination (not power loss).
func TestKilledOwnerPreservesAcknowledgedIngestAndSketch(t *testing.T) {
	const helperKey = "ROLLUPDB_INGEST_KILL_HELPER"
	const pathKey = "ROLLUPDB_INGEST_KILL_PATH"
	at := time.Date(2026, 10, 17, 12, 1, 59, 0, time.UTC)
	now := at.Add(time.Second)
	if os.Getenv(helperKey) == "1" {
		ctx := context.Background()
		db, err := Create(ctx, os.Getenv(pathKey), WithClock(func() time.Time { return now }))
		if err != nil { t.Fatal(err) }
		d := definition("durable_ingest")
		d.Statistics = []string{"count", "sum", "avg", "variance_population"}
		sketch := defaultSketchConfig()
		d.Sketch = &sketch
		if err := db.CreateStream(ctx, d); err != nil { t.Fatal(err) }
		r, err := db.Ingest(ctx, batchOf(d,
			observation(at, 10, "web"),
			observation(at, 20, "web"),
			observation(at, 30, "web"),
		))
		if err != nil || r.Accepted != 3 { t.Fatalf("ingest: %+v %v", r, err) }
		fmt.Fprintln(os.Stdout, "ACK_INGEST")
		// Parent kills this process. No Close or deferred cleanup.
		time.Sleep(24 * time.Hour)
		return
	}

	path := filepath.Join(t.TempDir(), "acknowledged-ingest.rollupdb")
	cmd := exec.Command(os.Args[0], "-test.run=^TestKilledOwnerPreservesAcknowledgedIngestAndSketch$")
	cmd.Env = append(os.Environ(), helperKey+"=1", pathKey+"="+path)
	stdout, err := cmd.StdoutPipe()
	if err != nil { t.Fatal(err) }
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil { t.Fatal(err) }
	reaped := false
	defer func() {
		if !reaped {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	lineCh := make(chan string, 1)
	go func() {
		line, _ := bufio.NewReader(stdout).ReadString('\n')
		lineCh <- strings.TrimSpace(line)
	}()
	select {
	case line := <-lineCh:
		if line != "ACK_INGEST" { t.Fatalf("unexpected helper output: %q", line) }
	case <-time.After(15 * time.Second):
		t.Fatal("ingest helper did not acknowledge before deadline")
	}
	if err := cmd.Process.Kill(); err != nil { t.Fatal(err) }
	_ = cmd.Wait()
	reaped = true

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	db, err := Open(ctx, path, WithClock(func() time.Time { return now }))
	if err != nil { t.Fatalf("reopen killed owner: %v", err) }
	defer db.Close()
	d, err := db.Stream(ctx, "durable_ingest")
	if err != nil { t.Fatal(err) }
	q := Query{Stream: d.Name, Version: 1, From: at, To: at.Add(time.Second), Quantiles: []float64{0, .5, 1}}
	result, err := db.Query(ctx, q)
	if err != nil { t.Fatal(err) }
	if result.Statistics["count"] != int64(3) || result.Statistics["sum"] != float64(60) || result.Statistics["avg"] != float64(20) || math.Abs(result.Statistics["variance_population"].(float64)-200.0/3.0) > 1e-12 {
		t.Fatalf("acknowledged aggregates lost or altered: %+v", result.Statistics)
	}
	if len(result.Quantiles) != 3 {
		t.Fatalf("missing recovered KLL quantiles: %+v", result.Quantiles)
	}
	for i, want := range []float64{10, 20, 30} {
		if result.Quantiles[i].Value == nil || *result.Quantiles[i].Value != want {
			t.Fatalf("recovered quantile %d: %+v (want %g)", i, result.Quantiles[i], want)
		}
	}
	status, err := db.Status(ctx, d.Name)
	if err != nil || status.Accepted != 3 || status.Buckets != 1 || status.Groups != 1 {
		t.Fatalf("recovered counters/groups: %+v %v", status, err)
	}
	again, err := db.Query(ctx, q)
	if err != nil || !reflect.DeepEqual(result, again) {
		t.Fatalf("repeated query altered recovered sketch: %+v %v", again, err)
	}
}
