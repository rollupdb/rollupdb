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

## Next

Nothing agreed yet. Ask the owner before any feature work after 0.4.

## Working here

- Go version: whatever `go.mod` says. Build and test with `CGO_ENABLED=0`.
- `go test ./...`, `go test -race ./...`, `go vet ./...`, `gofmt -l .`
- Cloud sessions may not reach proxy.golang.org or modernc.org, so tests may
  only run in CI. Read failures through check-run annotations (see
  RELEASING.md) when job logs can't be downloaded. Never claim a test passed
  without a run that shows it.
- `third_party/datasketches` holds the license and notice for code adapted
  from Apache DataSketches Go; keep them.
