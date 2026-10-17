# RollupDB 0.1: what it does and where it stops

RollupDB is open source under the Apache License 2.0. Releases are built and published automatically; see [RELEASING.md](../RELEASING.md).

RollupDB is a Go embedded aggregate database storing declared numeric statistics in a portable SQLite-backed `.rollupdb` snapshot. It ingests bounded batches or NDJSON, offers a loopback-only HTTP interface, maintains mergeable exact aggregates and optional KLL quantiles, and explicitly advances fixed UTC retention tiers.

## Functionality

- Exact count, compensated sum, minimum, maximum, mean, population and sample variance, with documented floating-point limitations.
- Optional approximate quantiles with DataSketches KLL, exposing rank-error metadata; rank error is not a guarantee of relative value error.
- Fixed-width time tiers, grace-based lateness rejection, transactional promotion/expiry, and disjoint retained-resolution query coverage.
- Single-owner local database, explicit migration from earlier revisions to revision 4, consistent snapshot export, and copy/reopen of completed snapshots.
- CLI and trusted-local HTTP; no authentication, TLS or public listener. Batch retries are new observations; there is no event deduplication.
- Reproducible multi-year experiments, with independently evaluated exact state and approximate rank/value error.

## Scope and known limitations

The engine is single-writer, single-machine, and local-filesystem only. It does not expose SQL, clustering, remote authentication, dashboards, a background retention scheduler, definition edits or general-purpose raw-event search. Running SQLite WAL may create sidecar files: never copy only the live main database. Use `snapshot` and copy the completed export. Snapshot output is not atomically published, and a killed process can leave an incomplete destination. An explicit `Advance` may hold the engine mutex for a large backlog. Separate owner processes and network filesystems are unsupported. A timed-out or disconnected ingest request is not proof that its transaction rolled back. Retrying without an application-level reconciliation mechanism may double count.

## Known limits in 0.1

- **The clock only moves forward.** Each ingest and `Advance` stores the host clock as the stream's watermark. One reading far in the future (a bad NTP step, a wrong RTC) is kept: later events are then rejected as too late, and the next `Advance` promotes and expires tiers against that wrong time, which can't be undone. Keep the host clock synchronized, and treat a sudden jump as an incident.
- **Large HTTP snapshots can time out.** The HTTP service applies its 30-second request deadline to the snapshot export and its 60-second write timeout to the download. A database too big to export and send within those limits gets an error or a cut-off file. For large files, use the `snapshot` command on the host instead. HTTP snapshots are staged in the system temp directory, which may be small or memory-backed.
- **JSON field names match without regard to case.** Unknown fields are rejected, but Go's decoder accepts `"Value"` for `"value"`.

## Process-kill testing

The test suite kills the owning process with acknowledged definitions, acknowledged ingestion, open uncommitted transactions, a finished retention promotion and an `Advance` stopped just before its commit, then reopens the file and checks that exactly the acknowledged state survived. CI repeats those tests three times on Linux. They cover a killed process, not power loss.
