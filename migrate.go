package rollupdb

import (
	"context"
	"database/sql"
	"fmt"
)

const schemaV2 = `
CREATE TABLE stream_runtime (
 stream TEXT NOT NULL, version INTEGER NOT NULL,
 watermark_ns INTEGER, coverage_start_ns INTEGER,
 accepted INTEGER NOT NULL DEFAULT 0 CHECK(accepted >= 0),
 rejected_invalid INTEGER NOT NULL DEFAULT 0 CHECK(rejected_invalid >= 0),
 rejected_late INTEGER NOT NULL DEFAULT 0 CHECK(rejected_late >= 0),
 rejected_group_limit INTEGER NOT NULL DEFAULT 0 CHECK(rejected_group_limit >= 0),
 PRIMARY KEY(stream, version),
 FOREIGN KEY(stream, version) REFERENCES stream_definitions(name, version)
) STRICT;
INSERT INTO stream_runtime(stream,version) SELECT name,version FROM stream_definitions;
CREATE TABLE dimension_groups (
 stream TEXT NOT NULL, version INTEGER NOT NULL, tuple_key TEXT NOT NULL,
 last_seen_ns INTEGER NOT NULL,
 PRIMARY KEY(stream,version,tuple_key),
 FOREIGN KEY(stream,version) REFERENCES stream_definitions(name,version)
) STRICT;
CREATE TABLE buckets (
 stream TEXT NOT NULL, version INTEGER NOT NULL, tuple_key TEXT NOT NULL,
 tier INTEGER NOT NULL CHECK(tier >= 0), start_ns INTEGER NOT NULL,
 n INTEGER NOT NULL CHECK(n > 0), sum REAL NOT NULL, sum_comp REAL NOT NULL,
 min REAL NOT NULL, max REAL NOT NULL, mean REAL NOT NULL, m2 REAL NOT NULL CHECK(m2 >= 0),
 PRIMARY KEY(stream,version,tuple_key,tier,start_ns),
 FOREIGN KEY(stream,version,tuple_key) REFERENCES dimension_groups(stream,version,tuple_key)
) STRICT;
CREATE INDEX buckets_time ON buckets(stream,version,tier,start_ns);
PRAGMA user_version = 2;
`

const schemaV3 = `ALTER TABLE buckets ADD COLUMN sketch BLOB;
PRAGMA user_version=3;`

const schemaV4 = `CREATE TABLE retention_frontiers (
 stream TEXT NOT NULL, version INTEGER NOT NULL, tier INTEGER NOT NULL CHECK(tier >= 0),
 through_ns INTEGER NOT NULL CHECK(through_ns >= 0),
 PRIMARY KEY(stream,version,tier),
 FOREIGN KEY(stream,version) REFERENCES stream_definitions(name,version)
) STRICT;
PRAGMA user_version=4;`

// Revision 5 remembers batch IDs, so a retried batch is recognized.
const schemaV5 = `CREATE TABLE ingest_batches (
 stream TEXT NOT NULL, batch_id TEXT NOT NULL,
 content_sha256 TEXT NOT NULL CHECK(length(content_sha256) = 64),
 committed_ns INTEGER NOT NULL,
 result_json TEXT NOT NULL CHECK(json_valid(result_json)),
 PRIMARY KEY(stream, batch_id)
) STRICT;
CREATE INDEX ingest_batches_time ON ingest_batches(stream, committed_ns);
PRAGMA user_version=5;`

func migrateCurrent(ctx context.Context, s *sql.DB, version int) error {
	tx, err := s.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Check the source schema inside the same transaction before making changes.
	var id string
	if err = tx.QueryRowContext(ctx, "SELECT file_id FROM store_meta WHERE singleton=1").Scan(&id); err != nil {
		return fmt.Errorf("migration source: %w", err)
	}
	if version == 1 {
		if _, err = tx.ExecContext(ctx, schemaV2); err != nil {
			return fmt.Errorf("migrate revision 1 to 2: %w", err)
		}
	}
	if version < 3 {
		if _, err = tx.ExecContext(ctx, schemaV3); err != nil {
			return fmt.Errorf("migrate revision %d to 3: %w", version, err)
		}
	}
	if version < 4 {
		if _, err = tx.ExecContext(ctx, schemaV4); err != nil {
			return fmt.Errorf("migrate revision %d to 4: %w", version, err)
		}
	}
	if _, err = tx.ExecContext(ctx, schemaV5); err != nil {
		return fmt.Errorf("migrate revision %d to 5: %w", version, err)
	}
	return tx.Commit()
}
