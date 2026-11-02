package rollupdb

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func dirNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// A finished snapshot leaves exactly the destination in its folder: the
// temporary file is gone.
func TestSnapshotLeavesNoTemporaryFile(t *testing.T) {
	db, _ := newDB(t)
	dir := t.TempDir()
	dest := filepath.Join(dir, "copy.rollupdb")
	if err := db.Snapshot(context.Background(), dest); err != nil {
		t.Fatal(err)
	}
	if names := dirNames(t, dir); len(names) != 1 || names[0] != "copy.rollupdb" {
		t.Fatalf("folder holds %v", names)
	}
	copyDB, err := Open(context.Background(), dest)
	if err != nil {
		t.Fatal(err)
	}
	copyDB.Close()
}

// If the destination name appears while the copy is being written, the
// snapshot refuses to publish and leaves the other file alone.
func TestSnapshotNeverReplacesANameThatAppearsMeanwhile(t *testing.T) {
	db, _ := newDB(t)
	dir := t.TempDir()
	dest := filepath.Join(dir, "raced.rollupdb")
	ctx := context.WithValue(context.Background(), snapshotBeforePublishKey{}, func() {
		if err := os.WriteFile(dest, []byte("someone else"), 0600); err != nil {
			t.Error(err)
		}
	})
	if err := db.Snapshot(ctx, dest); !errors.Is(err, os.ErrExist) {
		t.Fatalf("raced snapshot: %v", err)
	}
	if got, _ := os.ReadFile(dest); string(got) != "someone else" {
		t.Fatalf("other file replaced: %q", got)
	}
	if names := dirNames(t, dir); len(names) != 1 {
		t.Fatalf("temporary file left: %v", names)
	}
}

func TestPublishFallbackRefusesExistingName(t *testing.T) {
	dir := t.TempDir()
	tmp := filepath.Join(dir, ".x.tmp")
	dst := filepath.Join(dir, "x")
	if err := os.WriteFile(tmp, []byte("new"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := publish(tmp, dst); !errors.Is(err, os.ErrExist) {
		t.Fatalf("publish over a file: %v", err)
	}
	if got, _ := os.ReadFile(dst); string(got) != "old" {
		t.Fatalf("replaced: %q", got)
	}
	if err := os.Remove(dst); err != nil {
		t.Fatal(err)
	}
	if err := publish(tmp, dst); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(dst); string(got) != "new" {
		t.Fatalf("published %q", got)
	}
}

// A snapshot killed after writing its copy but before publishing it leaves no
// file under the destination name, and a later snapshot to that name works.
// A second run kills it as soon as the copy starts growing, in the middle of
// VACUUM INTO. Either way the destination is absent or a complete database.
func TestKilledSnapshotLeavesNoPartialDestination(t *testing.T) {
	if os.Getenv("ROLLUPDB_SNAPSHOT_HELPER") != "" {
		ctx := context.Background()
		db, err := Open(ctx, os.Getenv("ROLLUPDB_SNAPSHOT_SOURCE"))
		if err != nil {
			t.Fatal(err)
		}
		if os.Getenv("ROLLUPDB_SNAPSHOT_HELPER") == "hold" {
			ctx = context.WithValue(ctx, snapshotBeforePublishKey{}, func() {
				fmt.Fprintln(os.Stdout, "WRITTEN")
				time.Sleep(24 * time.Hour)
			})
		}
		fmt.Fprintln(os.Stdout, "START")
		err = db.Snapshot(ctx, os.Getenv("ROLLUPDB_SNAPSHOT_DEST"))
		fmt.Fprintln(os.Stdout, "DONE", err)
		time.Sleep(24 * time.Hour)
	}
	ctx := context.Background()
	source := filepath.Join(t.TempDir(), "source.rollupdb")
	now := time.Date(2026, 10, 17, 12, 2, 0, 0, time.UTC)
	db, err := Create(ctx, source, WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	d := definition("big")
	d.MaxGroups = 100000
	if err = db.CreateStream(ctx, d); err != nil {
		t.Fatal(err)
	}
	// Enough distinct groups that the copy takes a moment to write.
	for b := 0; b < 20; b++ {
		var obs []Observation
		for i := 0; i < 2000; i++ {
			obs = append(obs, observation(now.Add(-time.Duration(i%100)*time.Second), float64(i), fmt.Sprintf("svc-%d-%d", b, i)))
		}
		if _, err = db.Ingest(ctx, batchOf(d, obs...)); err != nil {
			t.Fatal(err)
		}
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}

	for _, mode := range []string{"hold", "early"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			dest := filepath.Join(dir, "snap.rollupdb")
			cmd := exec.Command(os.Args[0], "-test.run=^TestKilledSnapshotLeavesNoPartialDestination$")
			cmd.Env = append(os.Environ(), "ROLLUPDB_SNAPSHOT_HELPER="+mode, "ROLLUPDB_SNAPSHOT_SOURCE="+source, "ROLLUPDB_SNAPSHOT_DEST="+dest)
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			cmd.Stderr = os.Stderr
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
			lines := make(chan string, 4)
			go func() {
				r := bufio.NewReader(stdout)
				for {
					line, err := r.ReadString('\n')
					if err != nil {
						close(lines)
						return
					}
					lines <- strings.TrimSpace(line)
				}
			}()
			want := "WRITTEN"
			if mode == "early" {
				want = "START"
			}
			deadline := time.After(60 * time.Second)
		wait:
			for {
				select {
				case line, ok := <-lines:
					if !ok {
						t.Fatal("helper exited early")
					}
					if line == want {
						break wait
					}
				case <-deadline:
					t.Fatalf("helper never printed %s", want)
				}
			}
			if mode == "early" {
				// Kill once the temporary copy exists and has some bytes in it.
				for i := 0; i < 2000; i++ {
					grown := false
					for _, name := range dirNames(t, dir) {
						if strings.HasSuffix(name, ".tmp") {
							if fi, err := os.Stat(filepath.Join(dir, name)); err == nil && fi.Size() > 0 {
								grown = true
							}
						}
					}
					if grown {
						break
					}
					time.Sleep(time.Millisecond)
				}
			}
			if err = cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = cmd.Wait()
			if fi, err := os.Stat(dest); err == nil {
				if mode == "hold" {
					t.Fatalf("destination published before the hold: %d bytes", fi.Size())
				}
				// The copy finished before the kill: then it must be whole.
				copyDB, err := Open(ctx, dest)
				if err != nil {
					t.Fatalf("destination present but not a database: %v", err)
				}
				copyDB.Close()
				t.Logf("snapshot finished before the kill")
				return
			} else if !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			for _, name := range dirNames(t, dir) {
				// The hidden temp file, and SQLite's journal for it if VACUUM was cut short.
				if !strings.HasPrefix(name, ".snap.rollupdb.") || !(strings.HasSuffix(name, ".tmp") || strings.HasSuffix(name, ".tmp-journal")) {
					t.Fatalf("unexpected file after kill: %s", name)
				}
			}
			// A new snapshot to the same name succeeds and is complete.
			db, err := Open(ctx, source)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err = db.Snapshot(ctx, dest); err != nil {
				t.Fatalf("snapshot after a killed one: %v", err)
			}
			copyDB, err := Open(ctx, dest)
			if err != nil {
				t.Fatal(err)
			}
			defer copyDB.Close()
			if st, err := copyDB.Status(ctx, d.Name); err != nil || st.Accepted != 40000 {
				t.Fatalf("copy: %+v %v", st, err)
			}
		})
	}
}
