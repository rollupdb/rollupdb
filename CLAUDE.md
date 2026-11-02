# Notes for Claude

This is the one file that says where the next session starts. Keep it short and
current. What comes next is agreed with the owner before it goes in here.

## How this project runs

RollupDB runs on the owner's low-maintenance workflow: Apache 2.0, CI on every
push to main, releases published automatically, a website at rollupdb.com.
The owner's part is the go-ahead, the GitHub org and repos, and granting
Claude push access. Claude does everything else.

Releases are automated. To release, raise `Version` in `doc.go` and push to
`main`; never ask the owner to publish one. [RELEASING.md](RELEASING.md) has
the whole procedure and the checks to run afterwards.

## Where it stands (October 17, 2026)

- **v0.1.0 is out** (Apache 2.0), published by the release workflow from
  commit 9b7af47 with five binaries and SHA256SUMS. Checked: the Linux
  archive matches its checksum, prints `rollupdb 0.1.0` and passes
  `scripts/smoke.sh`.
- Engine, CLI and local HTTP service: exact statistics, KLL quantiles, fixed
  UTC tiers with grace windows, `Advance` for promotion and expiry, disjoint
  queries across resolutions, snapshot export, format revision 4.
- CI (`test`) passes on Linux, macOS and Windows, including the process-kill
  recovery tests. Failing test lines show up as check-run annotations.
- An independent review found no blockers; its doc fixes are in.
- 0.1.1 (agreed with the owner): Ingest and Advance refuse a clock reading
  more than `WithMaxClockStep` past a stream's watermark (`ErrClockJump`,
  HTTP 409 `clock_jump`; default 366 days for the host clock, no limit for an
  injected clock; CLI and server read `ROLLUPDB_MAX_CLOCK_STEP`), and
  GET /v1/snapshot gets a 30-minute budget instead of the 30 s / 60 s limits.

- 0.2.0–0.4.0 (owner said "do them all"), each released by the workflow and
  checked on the downloaded Linux binary:
  - 0.2.0 scheduling: `serve --advance-every` (default 1m), `RunRetention`,
    `AdvanceAll`, `advance FILE` with no name, `POST /v1/advance`.
  - 0.3.0 stream versions: `UpdateStream`, `update-stream`,
    `/v1/streams/{name}/versions`. Only retention, grace, max_groups,
    statistics and adding a sketch may change; `SketchSince` marks where an
    added sketch starts. Data stays keyed at version 1; format revision 4.
  - 0.4.0 read-only SQL: `db.SQL`, `rollupdb sql`, `POST /v1/sql`, a temp
    view per stream and the rollup_variance, rollup_variance_sample and
    rollup_quantile aggregates. docs/SQL.md has the rules.
- 0.4.1 (owner approved "fix all" after the independent review of 0.2–0.4):
  SQL holds its own connection and always clears query_only, so a cancelled
  statement can't leave writes failing; Ingest refuses tier-0 time below the
  frontier, so a longer grace can't reopen rolled-up history; `SketchSince`
  starts after both the clock and the watermark; promotion checks each
  child's own sketch; views never shadow tables (`stream_<name>`);
  rollup_quantile validates literal sketches; `POST /v1/advance` keeps the
  results of streams that rolled up when another fails. Not changed: a
  backlog Advance still holds the handle lock for one transaction
  (documented in API.md).

## Done

- Test results are recorded in `test/results` by `scripts/record-tests.sh`.
  Refresh them by starting the `record` workflow
  (`gh api -X POST repos/rollupdb/rollupdb/actions/workflows/record.yml/dispatches -f ref=main`);
  it commits the files to main. Every number on the site comes from there,
  so rerun it after changes that affect them and update the site to match.
- The site is built at photozeitgeist/rollupdb.com on the standard Launch
  template, unchanged (content and config only): about, contact, quick
  start, download and reference pages; posts for the launch, the explainer,
  how it was tested and the name; four diagrams drawn into static/images.
  The quick start was run against the released 0.1.1 binary; its SQL step
  and the reference were updated for 0.4.0, with a release post and a fifth
  diagram (sql-query.png).

## In progress: RollupDB 0.5 (softdev-plan item 31, approved by YK 2026-11-02)

What and why: a retried batch is counted twice today (INTERFACES.md tells
users not to retry blindly); a killed snapshot leaves a half-written file
under the final name; and metrics people already have in Prometheus can't
reach RollupDB without a custom bridge.

Design decisions:
- Phase A1, idempotent batch IDs: `Batch.BatchID` (`batch_id`, optional,
  1..128 printable ASCII). The ID, a SHA-256 of the batch's content and its
  BatchResult are stored in a new `ingest_batches` table in the same
  transaction as the data. The same ID again with the same content returns
  the stored result with `"replayed": true` and changes nothing; with other
  content it is refused (`ErrBatchConflict`, HTTP 409 `batch_conflict`).
  IDs are kept 7 days by default (`WithBatchIDRetention`), pruned by
  Advance. CLI: `ingest-ndjson --batch-id-prefix P` names each batch
  `P:<from_line>`, so rerunning the same input skips what committed. HTTP:
  `batch_id` in the body or an `Idempotency-Key` header.
- New table means format revision 5: Open refuses revision 4 until
  `migrate` (the existing rule, now 1/2/3/4 to 5, one transaction).
- Phase A2, atomic snapshot: VACUUM INTO a hidden temp file beside the
  destination, fsync it, publish it with a hard link (refuses to overwrite,
  atomically), remove the temp, fsync the directory. Where hard links don't
  work, a rename after checking the name is still free. A killed snapshot
  leaves only the temp file, never a partial file under the final name.
- Phase B, Prometheus remote-write 1.0: `serve --remote-write MAP.json`
  enables `POST /api/v1/write` (snappy-compressed protobuf WriteRequest,
  decoded by hand, no new dependency). The map file names, per metric, the
  stream and which labels become which dimensions; unmapped metrics and
  stale markers are skipped and counted. Each request's batches get IDs
  from the body's SHA-256, so Prometheus's retries after a 5xx are not
  counted twice. 204 on success, 400 for bad input (no retry), 5xx when it
  should retry. Remote-write 2.0 gets 415.
- Upkeep: README install section; refresh the 0.1-era scope docs
  (RELEASE_CANDIDATE.md) and the target matrix line.
- Version 0.5.0.

Steps:
1. [x] A1 batch IDs, revision 5 and migrate, CLI and HTTP, tests (batchid_test.go, transport/batchid_test.go; CI green on 3 OS).
2. [x] A2 atomic snapshot, tests (snapshot_atomic_test.go: no temp left, a name appearing mid-snapshot is never replaced, killed after writing and killed mid-VACUUM leave no destination; a killed VACUUM also leaves the temp's -journal).
3. [x] B remote-write (transport/remotewrite.go, snappy.go): fixtures in
   transport/testdata/remotewrite written by the official protobuf runtime
   and cramjam's snappy (generate.py); stats at GET /api/v1/write/stats.
4. [x] Docs and upkeep (README install and remote-write sections, API, INTERFACES, FORMAT, CONTRACT, RELEASE_CANDIDATE).
5. [x] Independent review, two rounds, fixes. Round 1, no blockers; should-fix, all fixed with tests: a 3 MB remote-write body could decode into GBs of empty series (now 1M series and 1M samples per request, lazy label maps, two requests decoding at once); remote-write batch IDs used a global index, so a map that gained a stream between a 503 and the retry double counted (now per stream); the digest included Version, so a stream update between send and retry turned the retry into a conflict and Prometheus dropped the request (version left out); the digest dropped NaN/Inf batches to SHA-256 of nothing (now value bits, length-prefixed fields, nil and empty dimensions equal); inactive stream and clock jump gave 400. Nits fixed in docs: pruning only by Advance, rename fallback window, record numbering. Round 2, no blockers; two should-fix, fixed: 503 for an inactive stream stalled Prometheus forever for samples that could never be stored (now skipped and counted as `inactive`; clock jump stays 503, documented as stalling the queue); unused labels still amplified memory (only __name__ and mapped labels are decoded). Fresh test/results pending.
6. [ ] 0.5.0 released by the workflow, downloads checked.
7. [ ] rollupdb.com post with its own SVG, pages; softdevstudio.com note;
   softdev-plan log row.

## Working here

- Go version: whatever `go.mod` says. Build and test with `CGO_ENABLED=0`.
- `go test ./...`, `go test -race ./...`, `go vet ./...`, `gofmt -l .`
- Cloud sessions may not reach proxy.golang.org or modernc.org, so tests may
  only run in CI. Read failures through check-run annotations (see
  RELEASING.md) when job logs can't be downloaded. Never claim a test passed
  without a run that shows it.
- `third_party/datasketches` holds the license and notice for code adapted
  from Apache DataSketches Go; keep them.
