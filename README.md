# RollupDB

Keep years of useful statistics in one portable database file.

RollupDB is a new aggregate-database prototype. Its intended purpose is to ingest numeric observations, preserve declared mergeable statistics and progressively replace fine time buckets with coarse ones. Think of RRDtool rebuilt around statistical sketches and declared dimensions. This is a description of direction, not RRDtool file or API compatibility.

Session 4 implements explicit transactional rollups and expiry across fixed UTC tiers. It merges exact statistical state and optional KLL distributions, persists completion frontiers, and queries a disjoint set of retained resolutions. Counts are checked integers; sum, average and variance use floating-point arithmetic with documented numerical limits. Retention runs when the caller invokes `Advance` or the `advance` command. Reads and ingestion do not run a background scheduler. Size and accuracy are measured together on scoped fixtures.

The engine is a Go library with a small command-line interface. SQLite through the pure-Go modernc.org/sqlite driver provides transactions and snapshot export without CGo or a separately installed SQLite server. Apache DataSketches Go supplies KLL compaction and merging. Dependencies are pinned in go.mod and go.sum. The initial target matrix is Linux amd64, macOS arm64 and Windows amd64; native CI must pass before a platform is considered validated.

## Try the prototype

Use the Go version declared in `go.mod`. From a checkout:

```sh
CGO_ENABLED=0 go build -o bin/rollupdb ./cmd/rollupdb
bin/rollupdb init history.rollupdb
bin/rollupdb create-stream history.rollupdb examples/api_requests.json
bin/rollupdb stream history.rollupdb api_requests
bin/rollupdb inspect history.rollupdb
bin/rollupdb advance history.rollupdb api_requests
bin/rollupdb snapshot history.rollupdb backup.rollupdb
bin/rollupdb inspect backup.rollupdb
go run ./examples/replay
go run ./examples/accuracy -n 20000 -summary
go run ./examples/retention -n 20000
```

On Windows, build `bin/rollupdb.exe` and run it with the native shell's path and environment syntax. Creating the example stream does not itself ingest observations. The replay example uses an explicit clock, ingests four observations, verifies their statistics, exports a snapshot, copies one file and checks identical query results after reopening it. It uses temporary files and can be rerun on any day.

Use `ingest FILE BATCH.json`, `query FILE QUERY.json` and `stats FILE STREAM` for the exact-statistics path. The current CLI accepts bounded JSON batch files; NDJSON input is follow-up work. Ingestion returns per-observation outcomes after commit. Queries return declared statistics, actual coverage, missing intervals and retained resolution. See [API and numerical behavior](docs/API.md) for request shapes and the distinction between empty and unavailable history.

New databases use format revision 4. Open and ordinary CLI commands reject revision-1/2/3 files without changing their schema. Back up an old file first, then run migrate FILE to upgrade transactionally; identity, definitions and existing exact aggregates survive. Old exact-only buckets do not gain invented percentiles. Future formats are rejected. init and snapshot refuse to overwrite existing destinations; other commands refuse to create missing databases.

The optional sketch definition is shown in examples/api_latency_quantiles.json. Add a query quantiles array such as [0.5,0.95,0.99]; ranks are normalized fractions, not percentages. Results return quantile values separately from exact statistics, with availability and rank-error metadata. See [sketch configuration and measurement](docs/SKETCHES.md) and the [recorded experiment](docs/accuracy-session-3.json). KLL's error model concerns rank, not relative value error. Multi-bucket query merges may recompact randomly and yield slightly different approximate answers on repeated reads; individual stored sketches and exact statistics survive snapshot/reopen unchanged.

The [retention example](examples/retention/main.go) coarsens 20,000 observations from 120 second buckets into one daily bucket, advances the explicit clock five years, then snapshots, copies, reopens and verifies an idempotent rerun. The [recorded result](docs/retention-session-4.json) reduced a 299,008-byte fine snapshot to 53,248 bytes; serialized observation input was 1,602,117 bytes. Maximum observed requested-rank error was 0.0022, while p99 value error was about 4.83%. This is one group over two minutes followed by a clock jump, not five years of continuous telemetry. See [retention semantics](docs/RETENTION.md) for boundaries, expiry, limitations and reproduction.

For embedding, use `rollupdb.Create(ctx, path)` or `rollupdb.Open(ctx, path)`, then `CreateStream`, `Ingest`, `Advance`, `Query`, `Status`, `Stream`, `Info`, `Snapshot` and `Close`. `WithClock` provides an explicit clock for replay and tests; production defaults to the host clock. Close every handle. Run one owner process per database file. Calls through a shared handle are serialized and safe for concurrent Go callers; independent writers and network filesystems are outside the current support contract.

## One-file portability

A running database uses SQLite WAL and may have `-wal` and `-shm` sidecars. Do not copy the live main file alone. `snapshot` exports a consistent database to a new file and syncs the file before reporting success. Copy that completed snapshot anywhere on a supported local filesystem; it needs no sidecars from the source. A normal last-connection close ordinarily removes the source's sidecars, but the explicit snapshot path is the supported export procedure.

The snapshot destination is visible while it is being written. Use it only after success. On an ordinary export error the destination is removed; a killed process can leave an incomplete destination that must be removed before retrying. Snapshot publication is not yet atomic or guaranteed durable against loss of the parent-directory entry during power failure. Snapshotting temporarily needs space for another complete database. File identity is preserved in the snapshot so it can be traced back to its origin; it is not an exclusive instance identifier.

## Design and development

The [architecture](docs/ARCHITECTURE.md) explains the boundaries and storage decision. The [correctness contract](docs/CONTRACT.md) defines event time, grace windows, rollup ownership, statistical state and historical availability. The [format notes](docs/FORMAT.md) distinguish the implemented schema from future bucket/sketch storage. The [project plan](PROJECT_PLAN.md) describes the six work sessions, and [RESUME.md](RESUME.md) records the next concrete task.

```sh
CGO_ENABLED=0 go test ./...
go test -race ./...
go vet ./...
```

There is no SQL interface, HTTP server, background scheduler or public release yet. A future website is planned at rollupdb.com; its repository and domain connection are separate work. The project's license has not been selected. Original RollupDB code is all rights reserved pending that decision; see [NOTICE](NOTICE) and [dependency notes](docs/DEPENDENCIES.md).
