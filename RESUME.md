# RollupDB resume instructions

Checkpoint: Session 1, October 17, 2026. Pause after this checkpoint; resume only on the user's command. This is a new standalone project. Code repository: https://github.com/rollupdb/rollupdb . Future website: rollupdb.com . No website repository has been supplied and the domain is to be connected later.

The checkpoint commit is the commit that last changed this file. Resolve its exact SHA with `git log -1 --format=%H -- RESUME.md`; recording a commit's own SHA inside itself is not possible. The initial plan commit was 41eed48079aeeee6fa4569eef46503a17738e0c5. Read this file, PROJECT_PLAN.md and docs/CONTRACT.md before changing code. Check for AGENTS.md instructions and inspect the current branch and remote state before resuming.

## Completed

The Go module and CLI are in place. The root package implements Create, Open, Close, Info, CreateStream, Stream and Snapshot. Definitions are validated, version 1 is persisted atomically, and duplicate stream names are refused. On-disk schema revision 1 stores only metadata and stream definitions. No observations, exact aggregates, sketches, rollup jobs, HTTP service or query engine exist yet.

Storage is SQLite through modernc.org/sqlite v1.60.1 with its required modernc.org/libc v1.77.1. The local runtime reported SQLite 3.53.4. The library uses a single serialized connection, WAL, FULL synchronous mode and a five-second SQLite busy timeout. Create and Snapshot refuse overwrite; Open refuses missing or foreign databases. The snapshot command uses VACUUM INTO and explicitly syncs its standalone output. Snapshot output is visible during export and can remain incomplete after process termination; atomic publication and parent-directory durability are documented limitations.

The CLI supports init, inspect, create-stream, stream, snapshot and help. The example stream has four aligned UTC tiers. docs/ARCHITECTURE.md records the storage tradeoff; docs/CONTRACT.md defines the intended lateness, frontier, ownership, numerical, retry and query semantics; docs/FORMAT.md distinguishes existing schema from future requirements. Original code remains all rights reserved pending a project-license decision. Release artifacts must include required dependency notices. No release has been made.

## Validation

The local Go toolchain is Go 1.27.2. A pure-Go build and tests pass on Linux amd64. Tests verify persistence and restart, format rejection, invalid definitions, duplicate creation, concurrent callers, existing-file preservation, canceled-snapshot cleanup, a snapshot excluding an uncommitted write while including committed state, and reopening copied snapshot bytes without source journals. Windows amd64 and macOS arm64 pure-Go cross-builds pass. Race and final CI status are recorded in SESSION_LOG.md; do not confuse cross-build success with native execution.

A CLI smoke run created a stream, exported a snapshot, copied it and reopened it with the same file identity and one stream. The snapshot was 16,384 bytes and the Linux CLI was 11,442,385 bytes for the initial trimpath build. These are foundation-only observations, not compression or ingestion benchmarks. The full stream-statistics demo is future work.

## Next session: durable exact aggregation

Session 2 is approximately 2–3 hours of scope. Add checked count, sum, min, max, mean and M2 state with stable incremental and merge operations, keeping statistical code separate from storage. Define overflow, finite-input rules and comparison tolerances with an independent reference. Implement the declared distinctions between population variance, sample variance, empty covered intervals and unavailable statistics. Unequal bucket counts and large offsets with small variance need explicit tests.

Add canonical dimension-tuple encoding, bounded string sizes, group admission limits, group/bucket persistence and batched ingestion to the finest tier. Introduce a testable clock and a persisted nondecreasing ingestion watermark. Implement the finest-bucket grace cutoff independently of physical scheduling. Reject future event times and events before the definition's effective timestamp. Define batch outcomes and counters exactly as in docs/CONTRACT.md; return success only after commit and roll back all changes on storage failure. Resubmission counts again; no deduplication is promised.

The revision-1 schema has no bucket tables. Any storage expansion must be explicit: bump the schema revision and provide migration/reopen tests if revision-1 files are to remain readable. Do not silently change the schema underneath FormatVersion=1. Use the existing example definition and snapshot/reopen path to verify the new persisted state. Expose a minimal single-tier query through the library; CLI ingestion/query is useful if time permits, but correctness takes priority. Keep percentile sketches for Session 3 and multi-tier promotion for Session 4.

## Commands and environment

In a normal development environment: `go test ./...`, `go test -race ./...`, `go vet ./...`, `go mod verify`, and `CGO_ENABLED=0 go build ./cmd/rollupdb`. CI reads the Go version from go.mod, runs native tests on Linux/macOS/Windows, and performs Linux race, formatting and module-tidy checks. Dependencies are locked in go.sum.

In the current workspace, Go is not on PATH. The verified toolchain executable is /workspace/scratch/bda50f559789/tooling/go/bin/go and gofmt is alongside it. The checkout is /workspace/scratch/85ea80fdc7f7/rollupdb . These are ephemeral execution paths, not product dependencies. If unavailable on resume, install or locate the version declared in go.mod. Never store credentials in repository files or logs.

## Session boundary

Finish each future session with committed work, validation results, a short user report and updated resume instructions. Pause at the boundary. The website and licensing decision are tracked follow-up work, not reasons to block the engine prototype. Do not assume another project's repository, website template or license applies here.
