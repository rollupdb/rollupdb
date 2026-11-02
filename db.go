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
	FormatVersion = 5
	applicationID = 0x524c4442 // RLDB
)

var (
	ErrClosed         = errors.New("rollupdb: database is closed")
	ErrNotRollupDB    = errors.New("rollupdb: not a RollupDB database")
	ErrFormatVersion  = errors.New("rollupdb: unsupported format version")
	ErrStreamExists   = errors.New("rollupdb: stream already exists")
	ErrStreamNotFound = errors.New("rollupdb: stream not found")
	// ErrClockJump reports a clock reading too far ahead of a stream's stored
	// watermark. Nothing was written. See WithMaxClockStep.
	ErrBatchConflict = errors.New("rollupdb: batch ID already used for other observations")
	ErrClockJump     = errors.New("rollupdb: clock jumped too far ahead of the stored watermark")
)

// DefaultMaxClockStep is how far the host clock may move past a stream's stored
// watermark in one step before Ingest and Advance refuse it. It catches a clock
// set years ahead by mistake, which would otherwise become a permanent
// watermark. It applies to the host clock only; an injected clock (WithClock)
// has no limit unless WithMaxClockStep sets one.
const DefaultMaxClockStep = 366 * 24 * time.Hour

// DB owns one serialized SQLite connection. Use one owning process per file.
// The handle is safe for concurrent Go callers; Close waits for active methods.
type DB struct {
	mu     sync.Mutex
	sql    *sql.DB
	path   string
	closed bool
	now    func() time.Time

	// maxClockStep limits the forward step from a stored watermark; 0 is no
	// limit. customClock and stepSet decide the default.
	maxClockStep time.Duration
	customClock  bool
	stepSet      bool

	// batchIDRetention is how long a batch ID is remembered (see Batch.BatchID).
	batchIDRetention time.Duration
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
		db.customClock = true
		return nil
	}
}

// WithMaxClockStep sets how far the clock may move past a stream's stored
// watermark in one step. A reading further ahead makes Ingest and Advance fail
// with ErrClockJump and write nothing, so a wrong clock can't become the
// permanent watermark. Zero or a negative value removes the limit. If a gap is
// real (a stream left idle longer than the limit), open the database once with
// a larger step or no limit.
func WithMaxClockStep(step time.Duration) Option {
	return func(db *DB) error {
		db.maxClockStep = max(step, 0)
		db.stepSet = true
		return nil
	}
}

// DefaultBatchIDRetention is how long a batch ID is remembered by default.
const DefaultBatchIDRetention = 7 * 24 * time.Hour

// WithBatchIDRetention sets how long Ingest remembers a batch ID after the
// batch commits, so a retry within that time is recognized and not counted
// twice. Advance forgets older IDs. Zero or less keeps the default.
func WithBatchIDRetention(d time.Duration) Option {
	return func(db *DB) error {
		if d > 0 {
			db.batchIDRetention = d
		}
		return nil
	}
}

// checkClockStep refuses a clock reading more than maxClockStep past the stored
// watermark. Both values are inside the supported time range, so the
// difference can't overflow.
func (db *DB) checkClockStep(stream string, now int64, watermark sql.NullInt64) error {
	if db.maxClockStep <= 0 || !watermark.Valid || now <= watermark.Int64 {
		return nil
	}
	if step := time.Duration(now - watermark.Int64); step > db.maxClockStep {
		return fmt.Errorf("%w: stream %q, clock %s is %s past the watermark %s (limit %s)",
			ErrClockJump, stream, time.Unix(0, now).UTC().Format(time.RFC3339),
			step.Round(time.Second), time.Unix(0, watermark.Int64).UTC().Format(time.RFC3339), db.maxClockStep)
	}
	return nil
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

// Migrate explicitly upgrades a revision-1/2/3/4 file to revision 5 in one transaction.
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
	if version >= 1 && version < FormatVersion && upgrade {
		if err = migrateCurrent(ctx, s, version); err != nil {
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
	db := &DB{sql: s, path: path, now: time.Now, batchIDRetention: DefaultBatchIDRetention}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("rollupdb: nil option")
		}
		if err = option(db); err != nil {
			return nil, err
		}
	}
	if !db.stepSet && !db.customClock {
		db.maxClockStep = DefaultMaxClockStep
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
	if _, err = tx.ExecContext(ctx, schemaV3); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, schemaV4); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, schemaV5); err != nil {
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
// Existing destinations are never overwritten. The copy is written to a hidden
// temporary file in the destination's folder, synced, and only then published
// under the destination name, so a crash or kill at any point leaves either no
// destination or a complete one (plus, at worst, a leftover hidden temp file).
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
	if _, err = os.Lstat(abs); err == nil {
		return fmt.Errorf("reserve snapshot: %w", &os.PathError{Op: "snapshot", Path: abs, Err: os.ErrExist})
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("reserve snapshot: %w", err)
	}
	dir, base := filepath.Split(abs)
	tmp, err := os.CreateTemp(dir, "."+base+".*.tmp")
	if err != nil {
		return fmt.Errorf("reserve snapshot: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	if err = tmp.Close(); err != nil {
		return err
	}
	// Bind the filename; quotes, question marks and URI-like strings are data.
	// VACUUM INTO accepts an existing empty file.
	if _, err = db.sql.ExecContext(ctx, "VACUUM main INTO ?", tmpPath); err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}
	if err = syncFile(tmpPath); err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}
	if hook, ok := ctx.Value(snapshotBeforePublishKey{}).(func()); ok {
		hook()
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = publish(tmpPath, abs); err != nil {
		return fmt.Errorf("publish snapshot: %w", err)
	}
	// The name is published; a failed directory sync can't undo that, but the
	// caller hears about it.
	if err = syncDir(dir); err != nil {
		return fmt.Errorf("snapshot published but its folder did not sync: %w", err)
	}
	return nil
}

// snapshotBeforePublishKey lets tests stop a snapshot between writing the
// temporary file and publishing it.
type snapshotBeforePublishKey struct{}

func syncFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	err = f.Sync()
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	return err
}

// publish gives tmp the name dst without ever replacing an existing dst. A hard
// link fails atomically if dst exists; where links aren't supported, a rename
// after checking that dst is still free is the fallback (a writer racing for
// the same name in that window could be replaced).
func publish(tmp, dst string) error {
	err := os.Link(tmp, dst)
	if err == nil {
		return nil
	}
	if errors.Is(err, os.ErrExist) {
		return err
	}
	if _, statErr := os.Lstat(dst); statErr == nil {
		return &os.PathError{Op: "publish", Path: dst, Err: os.ErrExist}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}
	return os.Rename(tmp, dst)
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
