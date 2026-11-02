# RollupDB

Keep years of useful statistics in one portable database file.

RollupDB is a small aggregate database. It ingests numeric observations, keeps the mergeable statistics you declare and progressively replaces fine time buckets with coarse ones. Think of RRDtool rebuilt around statistical sketches and declared dimensions. This is a description of direction, not RRDtool file or API compatibility.

It offers bounded streaming NDJSON ingestion, a loopback HTTP service, transactional rollups and expiry across fixed UTC tiers, and a measured multi-year replay. It merges exact statistical state and optional KLL distributions, persists completion frontiers, and queries a disjoint set of retained resolutions. Counts are checked integers; sum, average and variance use floating-point arithmetic with documented numerical limits. Rollups run when the caller invokes `Advance` or the `advance` command, or on a schedule: `serve` rolls up every stream each minute by default, and `RunRetention` does the same for an embedding. Size and accuracy are measured together on scoped fixtures.

The engine is a Go library with command-line and local HTTP interfaces. SQLite through the pure-Go modernc.org/sqlite driver provides transactions and snapshot export without CGo or a separately installed SQLite server. Apache DataSketches Go supplies KLL compaction and merging. Dependencies are pinned in go.mod and go.sum. CI runs the full test suite natively on Linux amd64, macOS arm64 and Windows amd64; releases also carry Linux arm64 and macOS amd64 binaries, which are cross-built and not run in CI.

## Install

Download the archive for your platform from the [latest release](https://github.com/rollupdb/rollupdb/releases/latest), check it against `SHA256SUMS`, unpack it and put `rollupdb` on your PATH:

| Platform | Archive |
| --- | --- |
| Linux, x86-64 | rollupdb_linux_amd64.tar.gz |
| Linux, ARM64 | rollupdb_linux_arm64.tar.gz |
| macOS, Apple silicon | rollupdb_darwin_arm64.tar.gz |
| macOS, Intel | rollupdb_darwin_amd64.tar.gz |
| Windows, x86-64 | rollupdb_windows_amd64.zip |

```sh
curl -LO https://github.com/rollupdb/rollupdb/releases/latest/download/rollupdb_linux_amd64.tar.gz
curl -LO https://github.com/rollupdb/rollupdb/releases/latest/download/SHA256SUMS
sha256sum --ignore-missing -c SHA256SUMS
tar xzf rollupdb_linux_amd64.tar.gz
./rollupdb version
```

To embed it, `go get github.com/rollupdb/rollupdb`.

## Try it

To build from a checkout instead, use the Go version declared in `go.mod`:

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
go run ./examples/history -days 10 -slots 4 -repeats 4 -groups 1 -cardinality-factor 2
```

On Windows, build `bin/rollupdb.exe` and run it with the native shell's path and environment syntax. Creating the example stream does not itself ingest observations. The replay example uses an explicit clock, ingests four observations, verifies their statistics, exports a snapshot, copies one file and checks identical query results after reopening it. It uses temporary files and can be rerun on any day.

Use `ingest FILE BATCH.json`, `query FILE QUERY.json` and `stats FILE STREAM` for the exact-statistics path. The CLI accepts bounded JSON batches and streaming `ingest-ndjson FILE STREAM INPUT [--batch-size N] [--batch-id-prefix P]`, with `-` for stdin. Each committed batch emits a receipt; failures report partial progress and leave earlier batches committed. `serve FILE [--listen 127.0.0.1:7070]` exposes the same engine on a literal loopback address, and with `--remote-write MAP.json` it takes Prometheus remote write.

A batch can carry a `batch_id`. Sent again with the same observations, it returns the first result marked `"replayed": true` and changes nothing, so a retry after a timeout or a lost response is safe; the same ID with other observations is refused. `ingest-ndjson --batch-id-prefix` names each batch after its first line, so an interrupted import can be run again from the start. IDs are remembered for seven days (`WithBatchIDRetention`). See [streaming and HTTP contracts](docs/INTERFACES.md) for limits, errors, retry behavior and shutdown. Ingestion returns per-observation outcomes after commit. Queries return declared statistics, actual coverage, missing intervals and retained resolution. See [API and numerical behavior](docs/API.md) for request shapes and the distinction between empty and unavailable history.

New databases use format revision 5. Open and ordinary CLI commands reject revision-1/2/3/4 files without changing their schema; files made by RollupDB 0.4 and earlier are revision 4. Back up an old file first, then run migrate FILE to upgrade transactionally; identity, definitions and existing exact aggregates survive. Old exact-only buckets do not gain invented percentiles. Future formats are rejected. init and snapshot refuse to overwrite existing destinations; other commands refuse to create missing databases.

The optional sketch definition is shown in examples/api_latency_quantiles.json. Add a query quantiles array such as [0.5,0.95,0.99]; ranks are normalized fractions, not percentages. Results return quantile values separately from exact statistics, with availability and rank-error metadata. See [sketch configuration and measurement](docs/SKETCHES.md) and the [recorded experiment](docs/accuracy-session-3.json). KLL's error model concerns rank, not relative value error. Multi-bucket query merges may recompact randomly and yield slightly different approximate answers on repeated reads; individual stored sketches and exact statistics survive snapshot/reopen unchanged.

The [retention example](examples/retention/main.go) coarsens 20,000 observations from 120 second buckets into one daily bucket, advances the explicit clock five years, then snapshots, copies, reopens and verifies an idempotent rerun. The [recorded result](docs/retention-session-4.json) reduced a 299,008-byte fine snapshot to 53,248 bytes; serialized observation input was 1,602,117 bytes. Maximum observed requested-rank error was 0.0022, while p99 value error was about 4.83%. This is one group over two minutes followed by a clock jump, not five years of continuous telemetry. See [retention semantics](docs/RETENTION.md) for boundaries, expiry, limitations and reproduction.

The [history experiment](docs/HISTORY_DEMO.md) replays 5,606,400 observations across three regularly sampled five-year cases, retaining all five years. In the [recorded run](docs/history-session-5.json), doubling volume from 1.40 to 2.80 million observations at fixed groups/times changed compact size from 15.34 to 15.40 MB (about 0.37%). Increasing groups from two to eight at the baseline event count produced 15,888 buckets instead of 3,972 and a 16.31 MB snapshot. Maximum observed requested-rank error across the recorded checks was about 0.00500143; one p99 value error was about 9.57%. Raw input sizes, exact discrepancies, timings and sampled working disk appear beside the accuracy results. These are scoped lossy-aggregation measurements, not general compression or throughput promises.

For embedding, use `rollupdb.Create(ctx, path)` or `rollupdb.Open(ctx, path)`, then `CreateStream`, `Ingest`, `Advance`, `Query`, `Status`, `Stream`, `Info`, `Snapshot` and `Close`. `WithClock` provides an explicit clock for replay and tests; production defaults to the host clock. With the host clock, a reading more than 366 days past a stream's last write is refused as a probable clock error; `WithMaxClockStep` or the `ROLLUPDB_MAX_CLOCK_STEP` environment variable changes that. Close every handle. Run one owner process per database file. Calls through a shared handle are serialized and safe for concurrent Go callers; independent writers and network filesystems are outside the current support contract.

## Prometheus remote write

`serve FILE --remote-write MAP.json` accepts Prometheus remote write 1.0 at `POST /api/v1/write`. The map file names, for each metric, the stream it goes to and which label fills each of the stream's dimensions:

```json
{"metrics": [
  {"metric": "http_requests_total", "stream": "requests",
   "dimensions": {"service": "job", "host": "instance"}}
]}
```

Every sample of a mapped metric becomes one observation at the sample's time. Metrics not in the map and Prometheus stale markers are skipped and counted at `GET /api/v1/write/stats`. Batch IDs come from the request body, so when Prometheus retries a request after an error, samples that were already stored aren't counted again. Remote write 2.0 is refused with 415. Point Prometheus at it with:

```yaml
remote_write:
  - url: http://127.0.0.1:7070/api/v1/write
```

The server still listens on loopback only, so Prometheus has to run on the same machine. See [streaming and HTTP contracts](docs/INTERFACES.md).

## One-file portability

A running database uses SQLite WAL and may have `-wal` and `-shm` sidecars. Do not copy the live main file alone. `snapshot` exports a consistent database to a new file and syncs the file before reporting success. Copy that completed snapshot anywhere on a supported local filesystem; it needs no sidecars from the source. A normal last-connection close ordinarily removes the source's sidecars, but the explicit snapshot path is the supported export procedure.

A snapshot is written to a hidden temporary file beside the destination (`.NAME.*.tmp`), synced, and then published under the destination name, which is never overwritten. The folder is synced after that. So the destination either doesn't exist or is a complete database, even if the process is killed half way; a kill can leave the hidden temporary file (and its `-journal`), which is safe to delete. Snapshotting temporarily needs space for another complete database. File identity is preserved in the snapshot so it can be traced back to its origin; it is not an exclusive instance identifier.

## Design and development

The [architecture](docs/ARCHITECTURE.md) explains the boundaries and storage decision. The [correctness contract](docs/CONTRACT.md) defines event time, grace windows, rollup ownership, statistical state and historical availability. The [format notes](docs/FORMAT.md) distinguish the implemented schema from future bucket/sketch storage. [RELEASING.md](RELEASING.md) explains how releases go out, and [scope and limitations](docs/RELEASE_CANDIDATE.md) lists what RollupDB does not do.

```sh
CGO_ENABLED=0 go test ./...
go test -race ./...
go vet ./...
```

Read-only SQL over the retained buckets is available through `db.SQL`, `rollupdb sql` and `POST /v1/sql`; see [SQL](docs/SQL.md).

## License

RollupDB is licensed under the [Apache License 2.0](LICENSE). See [NOTICE](NOTICE), [third-party licenses](THIRD_PARTY_LICENSES.md) and the [dependency notes](docs/DEPENDENCIES.md).
