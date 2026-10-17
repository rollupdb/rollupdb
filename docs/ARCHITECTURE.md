# Architecture and storage decision

Status: Session 2 exact-statistics prototype. October 17, 2026.

RollupDB retains declared information about observations, rather than retaining the observations themselves. A stream defines one scalar numeric value, event time, a joint set of dimensions, supported statistics and a retention policy. Statistics must carry sufficient mergeable state to survive multiple levels of aggregation. Storage reduction depends on retained groups, time buckets and sketch parameters; it is not a guaranteed ratio of raw bytes.

## Storage decision: SQLite, pure Go

The first implementation uses `database/sql` and `modernc.org/sqlite v1.60.1`, with `modernc.org/libc` resolved at the exact version required by that driver. WAL plus `synchronous=FULL` provides the intended transactional base. One connection and a handle-level mutex serialize driver access and lifecycle operations. A five-second SQLite busy timeout bounds ordinary external lock waits; callers should use context deadlines. Waiting for the Go handle mutex is not currently context-aware.

The choice avoids building a page allocator, transaction log, locking scheme and crash-recovery protocol merely to demonstrate aggregate retention. It comes with a sizeable transitive dependency graph and initial compilation cost; those are accepted prototype tradeoffs. A custom file engine is deferred until a measured requirement justifies it. No claim of lower overhead than RRDtool is made.

The supported portable object is a successful `VACUUM INTO` snapshot. The code reserves a new destination with owner-only permissions, binds its path as SQL data, performs export, and explicitly syncs the resulting file. It refuses overwrite. This produces a compact consistent snapshot without copying the source WAL. It is not atomic publication: incomplete output may survive process termination, and parent-directory syncing is not implemented. Those limits are explicit rather than hidden behind a portability claim.

SQLite identity is checked before persistent journal settings are changed. A database with a different application ID or unsupported schema version is rejected. `Open` never silently creates a database. `Create` never adopts an existing file. External SQL edits are unsupported even though the file is SQLite-readable.

## Boundaries

The root package owns lifecycle, metadata, stream definitions, exact statistical state, ingestion, single-tier queries and snapshot export. Statistical arithmetic is isolated in state.go. A batch caches touched buckets in memory and flushes their final state in one transaction, rather than updating a row for every observation. Dimension group admission follows input order, and queries merge in stable bucket/key order. The CLI is an adapter over that package.

Format revision 2 adds bucket state, canonical dimension groups and per-definition runtime metadata. Migration from revision 1 is explicit and transactional. A persisted nondecreasing watermark prevents a backward clock jump from reopening a closed ingestion window. No raw-event table is created. Recorded coverage begins at the first batch's earliest admissible bucket and extends through its committed watermark; it describes the recorder, not completeness of an upstream telemetry feed.

Sketch state follows in Session 3. Transactional promotion, retention and mixed-resolution query planning follow in Session 4. The scheduler will use the same explicit clock concept, but is not implemented or run implicitly by reads or ingestion at this checkpoint.

Transport must not implement its own aggregation logic. CLI and eventual HTTP requests must call the same embedded ingestion/query operations. No cloud service is necessary. The optional HTTP service will bind locally by default and is deferred until the embedded path is useful.

The first prototype does not offer concurrent independent owners, hosted multi-tenancy, SQL, event deduplication, raw-event retention/search, clustering, arbitrary historical regrouping or calendar-month buckets. All of those increase semantics and maintenance surface without proving the core product.

## Validation strategy

Session 1 tests storage identity, invalid-file refusal, immutable stream creation, reopen behavior, copying a live snapshot while a different connection holds an uncommitted write, destination non-overwrite, canceled-export cleanup and concurrent callers. Session 2 compares statistics with an independent rational-arithmetic reference and tests unequal bucket weights, compensation, numeric and counter overflow, grace boundaries, out-of-order input, backward clocks, canonical tuples, limits, partial coverage, transactional rollback, explicit migration and aggregate snapshot/reopen equality. Later acceptance tests must interrupt promotion transactions, query across tier boundaries and measure sketch error alongside disk usage. Cross compilation checks build portability; native CI execution is required to validate platform behavior.

Primary references consulted for the storage spike: https://pkg.go.dev/modernc.org/sqlite@v1.60.1 , https://www.sqlite.org/lang_vacuum.html , https://www.sqlite.org/backup.html . Driver behavior is tested against the pinned dependency rather than inferred from an unpinned latest release.
