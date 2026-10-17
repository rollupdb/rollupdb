# RollupDB v0.1 candidate — internal release notes (draft)

Status: NOT RELEASED. No tag has been created. Original engine code remains all rights reserved until the owner chooses a license. This document is not a license or a publication approval.

RollupDB is a Go embedded aggregate database storing declared numeric statistics in a portable SQLite-backed `.rollupdb` snapshot. It ingests bounded batches or NDJSON, offers a loopback-only HTTP interface, maintains mergeable exact aggregates and optional KLL quantiles, and explicitly advances fixed UTC retention tiers.

## Candidate functionality

- Exact count, compensated sum, minimum, maximum, mean, population and sample variance, with documented floating-point limitations.
- Optional approximate quantiles with DataSketches KLL, exposing rank-error metadata; rank error is not a guarantee of relative value error.
- Fixed-width time tiers, grace-based lateness rejection, transactional promotion/expiry, and disjoint retained-resolution query coverage.
- Single-owner local database, explicit migration from earlier revisions to revision 4, consistent snapshot export, and copy/reopen of completed snapshots.
- CLI and trusted-local HTTP; no authentication, TLS or public listener. Batch retries are new observations; there is no event deduplication.
- Reproducible multi-year experiments, with independently evaluated exact state and approximate rank/value error.

## Scope and known limitations

The engine is single-writer, single-machine, and local-filesystem only. It does not expose SQL, clustering, remote authentication, dashboards, a background retention scheduler, definition edits or general-purpose raw-event search. Running SQLite WAL may create sidecar files: never copy only the live main database. Use `snapshot` and copy the completed export. Snapshot output is not atomically published, and a killed process can leave an incomplete destination. An explicit `Advance` may hold the engine mutex for a large backlog. Separate owner processes and network filesystems are unsupported. A timed-out or disconnected ingest request is not proof that its transaction rolled back. Retrying without an application-level reconciliation mechanism may double count.

## Release gates — not yet satisfied

1. Select and review an original-code license; preserve Apache DataSketches dependency notices independently.
2. Implement and run actual forced-process termination tests covering acknowledged ingestion, interrupted retention, reopen, counters, ownership frontiers, and persisted sketch validity. Distinguish process-kill testing from power-loss testing.
3. Run the candidate workflow on all three native platforms and inspect test, build, smoke and checksum artifacts. A workflow file existing is not a passed workflow.
4. Verify migration/format compatibility, full CLI/HTTP demos, cancellation and interrupted snapshot transfer.
5. Review security/operations wording and final release notes; only then decide whether to create a v0.1 tag and publish artifacts.

## Candidate build procedure

Use the manually dispatched `.github/workflows/release-candidate.yml` workflow. It verifies modules, runs tests, builds native CGO-free executables, smoke-tests init/inspect/snapshot/reopen, generates SHA256 files and uploads short-lived artifacts. It does not create tags or GitHub Releases. Recheck that workflow on the intended commit before distributing binaries.

## Recorded measurement context

The Session 5 report at `docs/history-session-5.json` covers three five-year replay cases totaling 5,606,400 observations. In the fixed-group comparison, 1,401,600 observations produced a 15,339,520-byte compact snapshot; doubling observations at unchanged groups/time produced 15,396,864 bytes. More groups increased bucket count and stored bytes. This is a scoped experiment, not a general compression claim. Consult `docs/HISTORY_DEMO.md` for conditions, exclusions, timing and error interpretation.
