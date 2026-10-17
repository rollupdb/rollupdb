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

// TestKilledOwnerKeepsAcknowledgedDefinition exercises OS process termination,
// not power loss. The helper deliberately does not close its DB.
func TestKilledOwnerKeepsAcknowledgedDefinition(t *testing.T) {
	if os.Getenv("ROLLUPDB_KILL_HELPER") == "1" {
		path := os.Getenv("ROLLUPDB_KILL_PATH")
		db, err := Create(context.Background(), path)
		if err != nil {
			t.Fatal(err)
		}
		if err := db.CreateStream(context.Background(), definition("acknowledged")); err != nil {
			t.Fatal(err)
		}
		// The line is emitted only after CreateStream returns successfully.
		fmt.Fprintln(os.Stdout, "ACK")
		// A deliberate unclean process exit is performed by the parent.
		_ = db
		select {}
	}
	path := filepath.Join(t.TempDir(), "killed-owner.rollupdb")
	cmd := exec.Command(os.Args[0], "-test.run=^TestKilledOwnerKeepsAcknowledgedDefinition$")
	cmd.Env = append(os.Environ(), "ROLLUPDB_KILL_HELPER=1", "ROLLUPDB_KILL_PATH="+path)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	terminated := false
	defer func() {
		if !terminated {
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
		if line != "ACK" {
			t.Fatalf("helper did not acknowledge: %q", line)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("helper did not acknowledge before deadline")
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait() // Killed process is expected to exit unsuccessfully.
	terminated = true
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	db, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("reopen after kill: %v", err)
	}
	defer db.Close()
	got, err := db.Stream(ctx, "acknowledged")
	if err != nil {
		t.Fatalf("acknowledged definition lost after kill: %v", err)
	}
	if got.Name != "acknowledged" {
		t.Fatalf("wrong stream after recovery: %q", got.Name)
	}
	info, err := db.Info(ctx)
	if err != nil || info.Streams != 1 {
		t.Fatalf("recovered stream count: %+v, %v", info, err)
	}
}
