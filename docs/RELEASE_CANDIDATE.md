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

## Process-kill testing

The test suite kills the owning process with acknowledged definitions, acknowledged ingestion, open uncommitted transactions, a finished retention promotion and an `Advance` stopped just before its commit, then reopens the file and checks that exactly the acknowledged state survived. CI repeats those tests three times on Linux. They cover a killed process, not power loss.
