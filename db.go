package rollupdb

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const (
	// FormatVersion identifies the on-disk schema, independently of releases.
	FormatVersion = 2
	applicationID = 0x524c4442 // RLDB
)

var (
	ErrClosed         = errors.New("rollupdb: database is closed")
	ErrNotRollupDB    = errors.New("rollupdb: not a RollupDB database")
	ErrFormatVersion  = errors.New("rollupdb: unsupported format version")
	ErrStreamExists   = errors.New("rollupdb: stream already exists")
	ErrStreamNotFound = errors.New("rollupdb: stream not found")
)

// DB owns one serialized SQLite connection. Use one owning process per file.
// The handle is safe for concurrent Go callers; Close waits for active methods.
type DB struct {
	mu     sync.Mutex
	sql    *sql.DB
	path   string
	closed bool
	now    func() time.Time
}

// Option configures a handle. Production defaults to time.Now.
type Option func(*DB) error

// WithClock makes event-time acceptance deterministic for tests and replay tools.
// A persisted watermark prevents a clock rollback from reopening closed buckets.
func WithClock(now func() time.Time) Option {
	return func(db *DB) error {
		if now == nil {
			return errors.New("rollupdb: nil clock")
		}
		db.now = now
		return nil
	}
}

// Info describes a database without exposing its storage connection.
type Info struct {
	FormatVersion int       `json:"format_version"`
	FileID        string    `json:"file_id"`
	CreatedAt     time.Time `json:"created_at"`
	Streams       int       `json:"streams"`
	SQLiteVersion string    `json:"sqlite_version"`
	JournalMode   string    `json:"journal_mode"`
}

// Create creates a new database with owner-only permissions. It never overwrites
// an existing path, including an empty file or a symlink.
func Create(ctx context.Context, path string, options ...Option) (*DB, error) {
	if path == "" {
		return nil, errors.New("rollupdb: empty path")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(abs, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("create database: %w", err)
	}
	if err = f.Close(); err != nil {
		_ = os.Remove(abs)
		return nil, err
	}
	db, err := open(ctx, abs, true, false, options...)
	if err != nil {
		_ = os.Remove(abs)
		return nil, err
	}
	return db, nil
}

// Open opens an existing database. It never creates a missing file or adopts an
// arbitrary SQLite database. Unsupported formats are rejected before migration.
func Open(ctx context.Context, path string, options ...Option) (*DB, error) {
	if path == "" {
		return nil, errors.New("rollupdb: empty path")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	return open(ctx, abs, false, false, options...)
}

// Migrate explicitly upgrades a revision-1 file to revision 2 in one transaction.
// Take a backup first. A current file is a no-op; unknown formats are refused.
func Migrate(ctx context.Context, path string) error {
	if path == "" {
		return errors.New("rollupdb: empty path")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	db, err := open(ctx, abs, false, true)
	if err != nil {
		return err
	}
	return db.Close()
}

func open(ctx context.Context, path string, fresh, upgrade bool, options ...Option) (_ *DB, err error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		return nil, errors.New("rollupdb: database path must be a regular file")
	}
	if !fresh && fi.Size() == 0 {
		return nil, ErrNotRollupDB
	}
	uriPath := filepath.ToSlash(path)
	// Windows drive paths need a leading slash in a file URI; otherwise C:
	// would be parsed as the URI authority instead of part of the filename.
	if len(uriPath) >= 2 && uriPath[1] == ':' {
		uriPath = "/" + uriPath
	}
	u := url.URL{Scheme: "file", Path: uriPath}
	q := url.Values{"mode": {"rw"}, "_pragma": {"busy_timeout(5000)", "foreign_keys(ON)", "synchronous(FULL)"}}
	u.RawQuery = q.Encode()
	s, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, fmt.Errorf("open storage: %w", err)
	}
	s.SetMaxOpenConns(1)
	s.SetMaxIdleConns(1)
	defer func() {
		if err != nil {
			_ = s.Close()
		}
	}()
	if err = s.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("open storage: %w", err)
	}
	if fresh {
		if err = initialize(ctx, s); err != nil {
			return nil, err
		}
	}
	var app, version int
	if err = s.QueryRowContext(ctx, "PRAGMA application_id").Scan(&app); err != nil {
		return nil, err
	}
	if app != applicationID {
		return nil, ErrNotRollupDB
	}
	if err = s.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return nil, err
	}
	if version == 1 && upgrade {
		if err = migrateV2(ctx, s); err != nil {
			return nil, err
		}
		version = FormatVersion
	}
	if version != FormatVersion {
		return nil, fmt.Errorf("%w: got %d, support %d", ErrFormatVersion, version, FormatVersion)
	}
	// Check the format's required metadata before changing persistent settings.
	var id string
	if err = s.QueryRowContext(ctx, "SELECT file_id FROM store_meta WHERE singleton = 1").Scan(&id); err != nil {
		return nil, fmt.Errorf("read format metadata: %w", err)
	}
	var mode string
	if err = s.QueryRowContext(ctx, "PRAGMA journal_mode=WAL").Scan(&mode); err != nil {
		return nil, err
	}
	if mode != "wal" {
		return nil, fmt.Errorf("rollupdb: expected WAL journal mode, got %q", mode)
	}
	db := &DB{sql: s, path: path, now: time.Now}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("rollupdb: nil option")
		}
		if err = option(db); err != nil {
			return nil, err
		}
	}
	return db, nil
}

func initialize(ctx context.Context, s *sql.DB) error {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return err
	}
	tx, err := s.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	const schema = `
CREATE TABLE store_meta (
 singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
 file_id TEXT NOT NULL CHECK(length(file_id) = 32),
 created_at TEXT NOT NULL
) STRICT;
CREATE TABLE stream_definitions (
 name TEXT NOT NULL,
 version INTEGER NOT NULL CHECK(version > 0),
 effective_at TEXT NOT NULL,
 definition_json TEXT NOT NULL CHECK(json_valid(definition_json)),
 PRIMARY KEY(name, version)
) STRICT;
PRAGMA application_id = 1380729922;
PRAGMA user_version = 1;
`
	if _, err = tx.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("initialize schema: %w", err)
	}
	if _, err = tx.ExecContext(ctx, schemaV2); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO store_meta VALUES (1, ?, ?)", hex.EncodeToString(random[:]), time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return err
	}
	return tx.Commit()
}

// Info returns format, identity and storage diagnostics.
func (db *DB) Info(ctx context.Context) (Info, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return Info{}, ErrClosed
	}
	var i Info
	var created string
	i.FormatVersion = FormatVersion
	if err := db.sql.QueryRowContext(ctx, "SELECT file_id, created_at FROM store_meta WHERE singleton=1").Scan(&i.FileID, &created); err != nil {
		return i, err
	}
	var err error
	if i.CreatedAt, err = time.Parse(time.RFC3339Nano, created); err != nil {
		return i, fmt.Errorf("invalid creation time: %w", err)
	}
	if err = db.sql.QueryRowContext(ctx, "SELECT count(DISTINCT name) FROM stream_definitions").Scan(&i.Streams); err != nil {
		return i, err
	}
	if err = db.sql.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&i.SQLiteVersion); err != nil {
		return i, err
	}
	err = db.sql.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&i.JournalMode)
	return i, err
}

// Snapshot exports a consistent, compact database into a new destination file.
// Existing destinations are never overwritten. The destination must not be used
// until this call succeeds. A process crash can leave an incomplete destination.
func (db *DB) Snapshot(ctx context.Context, destination string) (err error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if destination == "" {
		return errors.New("rollupdb: empty snapshot destination")
	}
	abs, err := filepath.Abs(destination)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(abs, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("reserve snapshot: %w", err)
	}
	defer func() {
		if err != nil {
			_ = os.Remove(abs)
		}
	}()
	if err = f.Close(); err != nil {
		return err
	}
	// Bind the filename; quotes, question marks and URI-like strings are data.
	if _, err = db.sql.ExecContext(ctx, "VACUUM main INTO ?", abs); err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}
	f, err = os.OpenFile(abs, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	err = f.Sync()
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	return err
}

// Close closes the handle. Repeated calls succeed. A clean last-connection close
// normally checkpoints WAL; use Snapshot rather than copying a live database.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	db.closed = true
	return db.sql.Close()
}
