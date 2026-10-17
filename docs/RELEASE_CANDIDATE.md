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

The engine is single-writer, single-machine, and local-filesystem only. It does not expose SQL, clustering, remote authentication, dashboards or general-purpose raw-event search. Running SQLite WAL may create sidecar files: never copy only the live main database. Use `snapshot` and copy the completed export. Snapshot output is not atomically published, and a killed process can leave an incomplete destination. An explicit `Advance` may hold the engine mutex for a large backlog. Separate owner processes and network filesystems are unsupported. A timed-out or disconnected ingest request is not proof that its transaction rolled back. Retrying without an application-level reconciliation mechanism may double count.

## Known limits

- **JSON field names match without regard to case.** Unknown fields are rejected, but Go's decoder accepts `"Value"` for `"value"`.
- **HTTP snapshots are staged in the system temp directory,** which needs room for a full copy of the database and may be small or memory-backed. For very large files, run the `snapshot` command on the host.

Fixed in 0.1.1: a clock reading far ahead of a stream's watermark is now refused instead of becoming the permanent watermark (see [API](API.md)), and the HTTP snapshot route has its own 30-minute deadline instead of the 30-second request and 60-second write limits.

## Process-kill testing

The test suite kills the owning process with acknowledged definitions, acknowledged ingestion, open uncommitted transactions, a finished retention promotion and an `Advance` stopped just before its commit, then reopens the file and checks that exactly the acknowledged state survived. CI repeats those tests three times on Linux. They cover a killed process, not power loss.
