# RollupDB resume instructions

Checkpoint: Session 3, October 17, 2026. Pause here and resume on the user's command. This is the standalone RollupDB project at https://github.com/rollupdb/rollupdb . Future website: rollupdb.com . Website repository and domain connection are still deferred. Original code remains all rights reserved pending project-license selection before a public release. No release or website deployment has been made.

Resolve the checkpoint commit with git log -1 --format=%H -- RESUME.md. Check the remote branch, local changes and any AGENTS.md instructions first. Read PROJECT_PLAN.md, docs/CONTRACT.md, docs/FORMAT.md, docs/API.md and docs/SKETCHES.md before implementation.

## Current implementation

The Go library exposes Create, Open, Migrate, Close, Info, CreateStream, Stream, Ingest, Query, Status and Snapshot. WithClock supplies explicit replay/test time. The CLI supports init, migrate, inspect, create-stream, stream, stats, ingest, query, snapshot and help, using bounded strict JSON files. NDJSON and HTTP are not implemented.

Format revision 3 adds nullable buckets.sketch to the revision-2 exact/runtime schema. Migrate upgrades revision 1 or 2 to 3 in one transaction, preserving identity, definitions and any existing exact aggregate/runtime state. Open never silently upgrades. Old exact-only rows stay without sketches; their distribution cannot be reconstructed from moments. Stream-definition version 1 remains immutable. New streams may declare optional SketchConfig.

Exact statistics retain checked int64 count, compensated sum, min/max and Welford/Chan variance state. Public average uses compensated sum/count. Ingest supports 1..10000 observations, canonical bounded dimension tuples, deterministic group admission, one sampled clock per batch, a persistent nondecreasing watermark, per-observation outcomes and transactional lifetime counters. Rejected observations never update exact or sketch state. Retrying is new input. No raw-event table exists.

Sketch state uses Apache DataSketches Go v0.2.0 generic float64 KLL. Configuration is explicit: algorithm=kll_float64, encoding_version=1, M=8, K in [64,4096]. Only identical configurations merge. The 48-byte RDKL envelope records codec/type/K/M/length/SHA-256. kll_format.go validates bounded native layout, counts, indices, extrema and retained values before the pinned decoder. compactCapacity is adapted Apache-licensed code with retained notices under third_party/datasketches; original engine licensing remains pending. Do not bump the dependency or codec without reviewing its assumptions.

Query remains finest-tier only. It merges matching groups into one summary over aligned half-open ranges and exact declared-dimension filters, with explicit collection coverage. Quantiles requests support at most 32 distinct finite normalized ranks in [0,1]; JSON null ranks are refused. Quantile values and accuracy metadata are separate from exact statistics. Exact-only streams return not_retained, absent collection returns not_collected and covered empty periods return no_observations. Rank 0/1 use exact extrema. A configured populated bucket with missing/incompatible sketch data is an error.

KLL compaction is randomized. A query on one persisted bucket decodes it directly and is stable across restart/snapshot/copy. Merging multiple buckets may recompact, so repeated percentile values can differ while exact statistics remain unchanged. Do not reseed a global PRNG or use private deterministic test switches. K=200's single-sided model is about 0.0132947575 normalized-rank error, a fitted empirical 99% maximum-error model, not a deterministic bound or relative value-error promise. The inclusive nearest-rank convention includes upstream natural-rank rounding documented in docs/SKETCHES.md.

SQLite remains modernc.org/sqlite v1.60.1/libc v1.77.1, one serialized connection, WAL and synchronous FULL. Snapshot is VACUUM INTO plus output-file sync. Copy only successful snapshots; publication is not atomic and an interrupted export can leave partial output. One owning process and local filesystems are the support contract.

## Verification and measurements

Local pure-Go tests, Linux race checks, vet, module verification, formatting and Windows/macOS cross-builds pass. The bounded decoder completed a ten-second fuzz run without failure. Native Linux, macOS and Windows CI passed at code commit da36ac5ac0cf9cf6b5b420c26d76cb6385e2a47f: https://github.com/rollupdb/rollupdb/actions/runs/39039436672 . Each platform passed tests, vet, builds and both replay/accuracy examples; Linux also passed race detection, formatting and tidy-manifest checks. The final verification-record commit changes documentation only and skips redundant CI.

Session 3 tests codec round trips at K=64/200/4096, exact and estimated queries, direct/unequal partition accuracy, constants, duplicates, heavy tails, strict rank validation, filtered multi-bucket merges, decoded updates, checksum/version/type/parameter/layout corruption, refusal without mutation, explicit revision-1/2 migration and old-data non-backfill. Existing exact/rational, late-event, concurrency and rollback tests remain.

The internal/accuracy package is independent of the engine and sketch implementation. It sorts original values and computes duplicate-aware rank intervals, exact nearest-rank values and absolute/relative value error. Regression thresholds are deliberately conservative rather than asserting a stochastic 99% fit as a deterministic test guarantee.

The recorded experiment is docs/accuracy-session-3.json, generated with go run ./examples/accuracy -n 100000. Five deterministic datasets run in direct and seven-bucket layouts, totaling one million ingestion operations. Inputs were 7.3–8.94716 MB per case; snapshots were 49,152 or 73,728 bytes; ratios ranged about 99:1–182:1. Maximum observed requested-rank discrepancy was 0.00351. One heavy-tail p99 still had about 28.29% relative value error, illustrating the rank/value distinction. These short, one-group fixtures did not run retention and exclude peak working/WAL/reference storage. New runs use the same inputs but randomized sketches, so measurements vary.

## Next session: retention and mixed-resolution correctness

Session 4 is approximately 2–3 hours of scope. Add an explicit clock-driven advancement operation and transactional multi-tier promotion/expiry. Ingestion and reads currently do not run a scheduler. Keep the finest-tier late-arrival rule: no direct late writes into already coarsened tiers. Advance the same persisted nondecreasing watermark when finalizing intervals, and do not reopen them after clock rollback.

Use whole-parent-interval promotion as specified in docs/CONTRACT.md. A target interval may receive all its finalized child buckets only when its end plus the source retention/grace requirement is reached and all contributing lower-tier intervals are proven processed. Move state and delete sources atomically with persisted frontier advancement. Merge exact state and sketch state, not average/variance/p99 answers. Existing configuration permits multiple fixed UTC tiers with final zero retention meaning forever; retain that contract. Use subtraction for deadline comparisons to avoid timestamp-plus-100-year-duration overflow.

Persist contiguous per-definition/tier promotion frontiers, advancing over empty intervals too. Missing rows alone do not prove completeness. Work over ranges and actual buckets so an idle or sparse stream can advance through years without scanning every second. Parent completion must depend on lower-tier progress, not only wall time. A repeated or interrupted run must leave either authoritative sources or authoritative parents and must never merge a source twice.

Finite final-tier expiry must remove authoritative state and reclaim dimension keys only when no retained buckets reference them. Forever retention must preserve the final tier. Preserve original recorder coverage separately from retained availability so expired periods are reported as expired rather than empty zero-valued history.

Replace the finest-tier-only query scan with a disjoint plan across stored tiers. Check each actual bucket interval against requested boundaries; reject partial coarse-bucket cuts by default. Report actual resolution/coverage across the range rather than implying one-second precision for old hours. Keep dimension filters, schema identity, null availability and quantile metadata honest. Equal-config KLL merges must preserve current safety checks and disclose their stochastic estimates.

Bump the schema explicitly and provide revision-1/2/3 migration/reopen tests for any new frontier/availability tables. Test exact UTC boundaries, zero grace, out-of-order/rejected-late data, empty intervals, long jumps, partial coarse requests, multi-tier ranges, forever/finite retention, group reclamation, interruption/rollback, idempotent reruns and snapshot/restart after promotion. Compare counts/sums/extrema and variance within documented tolerances; evaluate quantile rank behavior independently after multiple merges.

Keep month buckets, definition-edit APIs, top-K, distinct counting, SQL, clustering and dashboards outside this session. NDJSON, HTTP, larger demonstrations and release work remain later sessions.

## Commands and execution environment

Use Go 1.27.2 from go.mod. Run CGO_ENABLED=0 go test ./..., go test -race ./..., go vet ./..., go mod verify, go run ./examples/replay and go run ./examples/accuracy -n 2000 -summary. Cross-build the CLI without CGo for Windows amd64 and Darwin arm64. CI runs native Linux/macOS/Windows tests, vet, builds and both examples, with Linux race/format/tidy checks.

Decoder fuzz command: go test -run '^$' -fuzz FuzzSketchDecoder -fuzztime=10s -parallel=2 . Keep reference input outside the engine when measuring accuracy. Do not treat random samples or exact serialized sketch bytes from different builds as deterministic.

The current checkout is /workspace/scratch/85ea80fdc7f7/rollupdb . Go is not on PATH; its verified executable is /workspace/scratch/bda50f559789/tooling/go/bin/go with gofmt alongside. These are ephemeral execution paths, not product dependencies. Never put credentials in files or logs.

Finish every session with committed work, verification results, updated resume instructions and a concise report. Pause at the boundary. A website repository or licensing decision is not required to continue the engine prototype.
