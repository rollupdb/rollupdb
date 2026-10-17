package rollupdb

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

func definition(name string) Definition {
	return Definition{Name: name, Version: 1, EffectiveAt: time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC), Dimensions: []string{"service"}, Statistics: []string{"count", "avg", "variance_population"}, MaxGroups: 100, Tiers: []Tier{{WidthSeconds: 1, RetainSeconds: 3600, GraceSeconds: 120}, {WidthSeconds: 60, RetainSeconds: 0, GraceSeconds: 120}}}
}

func newDB(t *testing.T) (*DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "history.rollupdb")
	db, err := Create(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db, path
}

func TestCreateReopenAndDefinitions(t *testing.T) {
	ctx := context.Background()
	db, path := newDB(t)
	d := definition("latency")
	if err := db.CreateStream(ctx, d); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateStream(ctx, d); !errors.Is(err, ErrStreamExists) {
		t.Fatalf("duplicate: %v", err)
	}
	before, err := db.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if before.Streams != 1 || before.FormatVersion != FormatVersion || len(before.FileID) != 32 || before.JournalMode != "wal" {
		t.Fatalf("bad info: %+v", before)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Info(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed info: %v", err)
	}
	reopened, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	after, err := reopened.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("identity changed: %+v vs %+v", before, after)
	}
	got, err := reopened.Stream(ctx, d.Name)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(d, got) {
		t.Fatalf("definition changed: %+v", got)
	}
	if _, err = reopened.Stream(ctx, "missing"); !errors.Is(err, ErrStreamNotFound) {
		t.Fatalf("missing stream: %v", err)
	}
}

func TestSnapshotCommittedStateCopyAndReopen(t *testing.T) {
	ctx := context.Background()
	db, source := newDB(t)
	d := definition("committed")
	if err := db.CreateStream(ctx, d); err != nil {
		t.Fatal(err)
	}
	// Hold an uncommitted write on a separate connection. A live WAL snapshot
	// must include committed definitions only, without copying live sidecars.
	writer, err := sql.Open("sqlite", source)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	tx, err := writer.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO stream_definitions VALUES('uncommitted',1,'2026-10-17T00:00:00Z','{}')`); err != nil {
		t.Fatal(err)
	}
	snapshot := filepath.Join(t.TempDir(), "snapshot ' quoted ?#.rollupdb")
	if err = db.Snapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err = os.Stat(snapshot + suffix); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("snapshot requires sidecar %s: %v", suffix, err)
		}
	}
	// Copy bytes to a different directory while the source is still open.
	bytes, err := os.ReadFile(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	copyPath := filepath.Join(t.TempDir(), "copy.rollupdb")
	if err = os.WriteFile(copyPath, bytes, 0600); err != nil {
		t.Fatal(err)
	}
	copyDB, err := Open(ctx, copyPath)
	if err != nil {
		t.Fatal(err)
	}
	defer copyDB.Close()
	i, err := copyDB.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if i.Streams != 1 {
		t.Fatalf("snapshot includes uncommitted data: %+v", i)
	}
	got, err := copyDB.Stream(ctx, "committed")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(d, got) {
		t.Fatal("copied definition changed")
	}
	var check string
	if err = copyDB.sql.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&check); err != nil || check != "ok" {
		t.Fatalf("integrity: %q %v", check, err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err = db.CreateStream(ctx, definition("after_snapshot")); err != nil {
		t.Fatal(err)
	}
	i, err = copyDB.Info(ctx)
	if err != nil || i.Streams != 1 {
		t.Fatalf("snapshot changed after source write: %+v %v", i, err)
	}
}

func TestSnapshotNoOverwriteAndCancellation(t *testing.T) {
	db, source := newDB(t)
	ctx := context.Background()
	dest := filepath.Join(t.TempDir(), "existing.rollupdb")
	if err := os.WriteFile(dest, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := db.Snapshot(ctx, dest); !errors.Is(err, os.ErrExist) {
		t.Fatalf("overwrite: %v", err)
	}
	got, err := os.ReadFile(dest)
	if err != nil || string(got) != "keep" {
		t.Fatalf("destination changed: %q %v", got, err)
	}
	if err := db.Snapshot(ctx, source); !errors.Is(err, os.ErrExist) {
		t.Fatalf("source overwrite: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	dest = filepath.Join(t.TempDir(), "canceled.rollupdb")
	if err := db.Snapshot(canceled, dest); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled snapshot: %v", err)
	}
	if _, err = os.Stat(dest); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed snapshot remains: %v", err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	if err = db.Snapshot(ctx, dest); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed snapshot: %v", err)
	}
}

func TestOpenNeverCreatesOrAdopts(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	missing := filepath.Join(dir, "missing.rollupdb")
	if _, err := Open(ctx, missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing: %v", err)
	}
	if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("Open created a file")
	}
	if _, err := Open(ctx, dir); err == nil {
		t.Fatal("accepted directory")
	}
	for _, contents := range []string{"", "not sqlite"} {
		path := filepath.Join(t.TempDir(), "foreign")
		if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
		if db, err := Open(ctx, path); err == nil {
			db.Close()
			t.Fatal("adopted invalid file")
		}
		if db, err := Create(ctx, path); !errors.Is(err, os.ErrExist) {
			if db != nil {
				db.Close()
			}
			t.Fatalf("Create overwrote: %v", err)
		}
		got, err := os.ReadFile(path)
		if err != nil || string(got) != contents {
			t.Fatal("foreign file changed")
		}
	}
	path := filepath.Join(dir, "foreign.sqlite")
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = raw.Exec("CREATE TABLE unrelated(x INTEGER); INSERT INTO unrelated VALUES(42)"); err != nil {
		t.Fatal(err)
	}
	if err = raw.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Open(ctx, path); !errors.Is(err, ErrNotRollupDB) {
		t.Fatalf("foreign sqlite: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("foreign SQLite database changed")
	}
}

func TestFutureFormatRejected(t *testing.T) {
	db, path := newDB(t)
	if _, err := db.sql.Exec("PRAGMA user_version=999"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), path); !errors.Is(err, ErrFormatVersion) {
		t.Fatalf("future format: %v", err)
	}
}

func TestSourceFilenameEscaping(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history ' ?#%.rollupdb")
	db, err := Create(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.CreateStream(context.Background(), definition("api")); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Stream(context.Background(), "api"); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentDefinitionWrites(t *testing.T) {
	db, _ := newDB(t)
	var wg sync.WaitGroup
	for _, name := range []string{"alpha", "beta", "gamma", "delta"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := db.CreateStream(context.Background(), definition(name)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	i, err := db.Info(context.Background())
	if err != nil || i.Streams != 4 {
		t.Fatalf("concurrent streams: %+v %v", i, err)
	}
}
