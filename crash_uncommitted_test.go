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

// TestKilledOwnerRollsBackUncommittedState deliberately kills the owner with
// an open SQLite write transaction. This exercises WAL recovery across
// buckets, runtime counters and retention frontiers. It is NOT a power-loss
// test or a deterministic kill inside DB.Advance's internal transaction.
func TestKilledOwnerRollsBackUncommittedState(t *testing.T) {
	const helper = "ROLLUPDB_UNCOMMITTED_KILL_HELPER"
	const pathEnv = "ROLLUPDB_UNCOMMITTED_KILL_PATH"
	at := time.Date(2026, 10, 17, 12, 1, 59, 0, time.UTC)
	now := at.Add(time.Second)
	if os.Getenv(helper) == "1" {
		ctx := context.Background()
		db, err := Create(ctx, os.Getenv(pathEnv), WithClock(func() time.Time { return now }))
		if err != nil {
			t.Fatal(err)
		}
		d := definition("uncommitted")
		d.Statistics = []string{"count", "sum"}
		if err := db.CreateStream(ctx, d); err != nil {
			t.Fatal(err)
		}
		result, err := db.Ingest(ctx, batchOf(d, observation(at, 7, "web")))
		if err != nil || result.Accepted != 1 {
			t.Fatalf("setup ingest: %+v %v", result, err)
		}
		tx, err := db.sql.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		// These writes must never become visible after a kill without Commit.
		if _, err := tx.ExecContext(ctx, "UPDATE buckets SET n=n+100 WHERE stream=?", d.Name); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, "UPDATE stream_runtime SET accepted=accepted+100 WHERE stream=?", d.Name); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO retention_frontiers(stream,version,tier,through_ns) VALUES(?,1,0,?)", d.Name, d.EffectiveAt.UnixNano()); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintln(os.Stdout, "UNCOMMITTED_WRITES_READY")
		// Keep the transaction open until the parent kills the process.
		time.Sleep(24 * time.Hour)
		return
	}

	path := filepath.Join(t.TempDir(), "uncommitted-kill.rollupdb")
	cmd := exec.Command(os.Args[0], "-test.run=^TestKilledOwnerRollsBackUncommittedState$")
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
		if line != "UNCOMMITTED_WRITES_READY" {
			t.Fatalf("helper failed to stage writes: %q", line)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("uncommitted helper timed out")
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
	d, err := db.Stream(ctx, "uncommitted")
	if err != nil {
		t.Fatal(err)
	}
	got, err := db.Query(ctx, Query{Stream: d.Name, Version: 1, From: at, To: at.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if got.Statistics["count"] != int64(1) || got.Statistics["sum"] != float64(7) {
		t.Fatalf("uncommitted bucket modification survived kill: %+v", got.Statistics)
	}
	status, err := db.Status(ctx, d.Name)
	if err != nil || status.Accepted != 1 || status.Buckets != 1 {
		t.Fatalf("uncommitted counters survived kill: %+v %v", status, err)
	}
	var frontiers int
	if err := db.sql.QueryRowContext(ctx, "SELECT count(*) FROM retention_frontiers WHERE stream=?", d.Name).Scan(&frontiers); err != nil {
		t.Fatal(err)
	}
	if frontiers != 0 {
		t.Fatalf("uncommitted retention frontier survived kill: %d", frontiers)
	}
}
