# RollupDB resume instructions

Checkpoint: Session 2, October 17, 2026. Pause after this checkpoint and resume on the user's command. RollupDB is a new standalone project at https://github.com/rollupdb/rollupdb . Future website: rollupdb.com . Its website repository has not been supplied and domain connection is deferred.

The checkpoint commit is the commit that last changed this file; resolve it with git log -1 --format=%H -- RESUME.md. Inspect branch/remote state and any AGENTS.md instructions before editing. Read PROJECT_PLAN.md, docs/API.md, docs/FORMAT.md and docs/CONTRACT.md. Original code is all rights reserved pending license selection before the first public release. No release or website deployment has been made.

## Completed

The Go library now exposes Create, Open, Migrate, Close, Info, CreateStream, Stream, Ingest, Query, Status and Snapshot. WithClock supplies an explicit clock for replay/tests; production uses time.Now. The CLI exposes init, migrate, inspect, create-stream, stream, stats, ingest, query, snapshot and help. It reads bounded strict JSON files; NDJSON and HTTP are not implemented.

Format revision 2 adds dimension_groups, buckets and stream_runtime to the original metadata and definitions. Open refuses revision 1 without modifying its schema; Migrate upgrades it transactionally and preserves definitions and file identity. New/migrated runtime state begins without historical coverage. Unknown formats are refused. Tests cover successful migration and full DDL/version rollback on failure.

Exact state in state.go includes checked int64 count, Neumaier-compensated sum, min, max, Welford mean and M2 with Chan-style merges. Public average is compensated sum/count; population and sample variances have explicit empty/singleton behavior. Nonfinite inputs or overflowing bucket arithmetic are rejected without mutation. Lifetime-counter overflow aborts the entire batch. Query merge overflow returns an error. No universal floating-point exactness claim is made; documented rational-reference tests specify acceptance tolerances.

Ingest accepts 1..10000 observations per batch and writes only tier 0. Values are grouped by the full JSON array of dimension values in definition order, with 1024-byte value and 4096-byte encoded-tuple limits. New group admission follows input order and rejects excess groups. It does not reclaim groups yet. A batch caches touched aggregate state and flushes it transactionally with counters, group metadata and a nondecreasing watermark. It samples the clock once, rejects future/pre-effective times, and enforces finest-bucket grace using the persisted watermark. Backward clocks do not reopen buckets. Retrying is new input, not deduplication.

Queries combine matching groups into a single summary over aligned finest-tier boundaries and exact declared-dimension filters. They expose coverage, missing intervals, retained resolution, definition version and selected statistics. No intake yet means null statistics; empty covered intervals have count/sum zero with other statistics null. Partial collection returns complete=false and explicit gaps. Active buckets may be included with coverage ending one nanosecond after the inclusive watermark. Reads do not advance intake coverage. Coverage describes recorded observations, not upstream-source completeness.

SQLite remains modernc.org/sqlite v1.60.1 with libc v1.77.1, one serialized connection, WAL and synchronous FULL. Snapshots use VACUUM INTO plus explicit output-file sync. Copy only successful snapshots; publication is not atomic and a killed export can leave an incomplete destination. Network filesystems and independent concurrent owners are unsupported.

## Validation

Local pure-Go tests, Linux race detection, vet, module verification and formatting checks pass. Windows amd64 and macOS arm64 pure-Go cross-builds pass. Native hosted CI passed Linux, macOS and Windows at code commit b78a9440e9cce003e3f0aef1a63a91763a9fb511: https://github.com/rollupdb/rollupdb/actions/runs/39036260475 . All platforms passed tests, vet, builds and the replay/snapshot example; Linux also passed race detection, formatting and module-tidy checks. The final verification-record commit changes documentation only and skips redundant CI.

Tests compare direct state and unequal partition merges with an independent big.Rat reference; exercise cancellation-sensitive sum, large offsets/small variance, overflow, grace boundaries, out-of-order input, backward-clock restart, dimension collisions/limits, malformed JSON, unknown fields, batch rollback under an injected SQL failure, count-only visibility, empty/unavailable/partial coverage, half-open minute buckets, migration and aggregate snapshot/copy/reopen equality. Concurrent ingestion must not lose updates.

The deterministic example is go run ./examples/replay. It ingests [10,20,20,20] across unequal groups/buckets and verifies count=4, sum=70, avg=17.5, population variance approximately 18.75 and sample variance approximately 25 after snapshot/copy/reopen. The initial revision-2 example snapshot was 45,056 bytes. This is a correctness example, not a compression benchmark.

## Next session: percentile state

Session 3 is approximately 2–3 hours of scope. Select one maintained Go mergeable quantile sketch after verifying its actual accuracy guarantee, supported parameter merges, deterministic serialization, license and maintenance. Prefer a documented rank-error model; do not invent an algorithm or promise relative value accuracy from a rank bound.

Extend initial stream configuration and validation with an explicit immutable sketch configuration, implement ingestion/serialization/merge/query paths and include algorithm identity, encoding version and settings in persistent state or an immutable definition reference. Add an explicit format revision/migration if the file schema changes; do not reinterpret revision 2 silently. Existing exact-only buckets cannot regenerate distributions, so an upgrade must leave their percentile coverage unavailable rather than synthesizing it. Definition edits are still unsupported; a new sketch-enabled stream is a valid first path. Keep the limitation explicit.

Test direct versus partitioned ingestion, restart/snapshot equivalence, skewed and heavy-tailed data, constants, duplicates, invalid quantiles and incompatible payload versions. Measure duplicate-aware rank discrepancy and observed value discrepancy against an independent sorted reference, and describe the algorithm's probabilistic or deterministic guarantee accurately. Do not compare serialized randomized sketches byte-for-byte unless the algorithm promises determinism. Unknown/unsupported encodings must fail without mutation. Keep exact-state behavior unchanged and retain the current corruption/rollback tests.

Do not start automatic promotion or expiry until Session 4. Retention is currently a validated declaration only: old buckets are neither moved nor deleted. Multi-tier query planning, persisted promotion frontiers, empty-interval advancement and group reclamation are future work. Top-K, cardinality, histograms, schema transitions, SQL, clustering and dashboards remain outside this checkpoint.

## Commands and environment

Use the Go version in go.mod (currently 1.27.2). Run CGO_ENABLED=0 go test ./..., go test -race ./..., go vet ./..., go mod verify and go run ./examples/replay. Cross-build the CLI with CGO_ENABLED=0 for Windows amd64 and Darwin arm64. CI runs native Linux/macOS/Windows tests, vet, builds and the replay example, plus Linux race, formatting and module-tidy checks.

In this workspace Go is not on PATH. The verified executable is /workspace/scratch/bda50f559789/tooling/go/bin/go and gofmt is alongside it. The checkout is /workspace/scratch/85ea80fdc7f7/rollupdb . These ephemeral paths are not product dependencies. Locate/install the declared toolchain if they no longer exist. Never place credentials in files or logs.

End each session with committed work, validation results, updated resume instructions and a concise report. Pause at the session boundary. A website repository or licensing decision is not required to continue the engine prototype.
