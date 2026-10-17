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
  The quick start was run against the released 0.1.1 binary.

## Next

Nothing agreed yet. Ask the owner before any feature work after 0.1.

## Working here

- Go version: whatever `go.mod` says. Build and test with `CGO_ENABLED=0`.
- `go test ./...`, `go test -race ./...`, `go vet ./...`, `gofmt -l .`
- Cloud sessions may not reach proxy.golang.org or modernc.org, so tests may
  only run in CI. Read failures through check-run annotations (see
  RELEASING.md) when job logs can't be downloaded. Never claim a test passed
  without a run that shows it.
- `third_party/datasketches` holds the license and notice for code adapted
  from Apache DataSketches Go; keep them.
