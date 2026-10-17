package rollupdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func legacyFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "revision1.rollupdb")
	s, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// Independent revision-1 fixture, matching the previously committed foundation.
	const legacy = `CREATE TABLE store_meta(singleton INTEGER PRIMARY KEY CHECK(singleton=1),file_id TEXT NOT NULL CHECK(length(file_id)=32),created_at TEXT NOT NULL) STRICT;
CREATE TABLE stream_definitions(name TEXT NOT NULL,version INTEGER NOT NULL CHECK(version>0),effective_at TEXT NOT NULL,definition_json TEXT NOT NULL CHECK(json_valid(definition_json)),PRIMARY KEY(name,version)) STRICT;
INSERT INTO store_meta VALUES(1,'0123456789abcdef0123456789abcdef','2026-10-17T00:00:00Z');
PRAGMA application_id=1380729922; PRAGMA user_version=1;`
	if _, err = s.Exec(legacy); err != nil {
		t.Fatal(err)
	}
	d := definition("legacy")
	encoded, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Exec("INSERT INTO stream_definitions VALUES(?,1,?,?)", d.Name, d.EffectiveAt.Format("2006-01-02T15:04:05Z07:00"), string(encoded)); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestExplicitMigrationPreservesDefinitionAndIdentity(t *testing.T) {
	ctx := context.Background()
	path := legacyFile(t)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Open(ctx, path); !errors.Is(err, ErrFormatVersion) {
		t.Fatalf("implicit migration: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("Open changed legacy file")
	}
	if err = Migrate(ctx, path); err != nil {
		t.Fatal(err)
	}
	if err = Migrate(ctx, path); err != nil {
		t.Fatalf("migration not idempotent: %v", err)
	}
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	db, err := Open(ctx, path, WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	i, err := db.Info(ctx)
	if err != nil || i.FormatVersion != 2 || i.FileID != "0123456789abcdef0123456789abcdef" || i.Streams != 1 {
		t.Fatalf("migration info: %+v %v", i, err)
	}
	d, err := db.Stream(ctx, "legacy")
	if err != nil || !reflect.DeepEqual(d, definition("legacy")) {
		t.Fatalf("migration definition: %+v %v", d, err)
	}
	s, err := db.Status(ctx, "legacy")
	if err != nil || s.CoverageStart != nil || s.Accepted != 0 {
		t.Fatalf("migration invented history: %+v %v", s, err)
	}
	// A migrated stream is usable, not just readable metadata.
	at := now.Add(-time.Second)
	if r, err := db.Ingest(ctx, batchOf(d, observation(at, 3, "web"))); err != nil || r.Accepted != 1 {
		t.Fatalf("migrated ingestion: %+v %v", r, err)
	}
}

func TestFailedMigrationRollsBackDDLAndVersion(t *testing.T) {
	path := legacyFile(t)
	s, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Exec("CREATE TABLE dimension_groups(conflict INTEGER)"); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if err = Migrate(context.Background(), path); err == nil {
		t.Fatal("accepted conflicting schema")
	}
	s, err = sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var version, count int
	if err = s.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err = s.QueryRow("SELECT count(*) FROM sqlite_master WHERE name='stream_runtime'").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if version != 1 || count != 0 {
		t.Fatalf("partial migration survived: version=%d runtime=%d", version, count)
	}
}
