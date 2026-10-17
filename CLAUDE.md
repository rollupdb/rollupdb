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
- An independent review found no blockers. Its doc fixes are in. Two limits
  are written up in `docs/RELEASE_CANDIDATE.md` rather than fixed: a
  far-future clock reading is kept as the watermark for good, and large
  HTTP snapshots can hit the request and write timeouts.

## Next, agreed

1. Record test results into `test/results` with a script, and take every
   number used on the site from there.
2. Build the site at photozeitgeist/rollupdb.com (the owner creates the repo
   and grants access) with the standard Hugo Launch template, unchanged:
   about, contact (info@rollupdb.com), quick start, download and reference
   pages; posts for the launch, a plain-language explainer, how it was
   tested and the name.

Not decided yet, ask the owner first: fixing the two limits above (refuse a
clock reading too far ahead of the watermark; give `/v1/snapshot` its own
deadlines), and any feature work after 0.1.

## Working here

- Go version: whatever `go.mod` says. Build and test with `CGO_ENABLED=0`.
- `go test ./...`, `go test -race ./...`, `go vet ./...`, `gofmt -l .`
- Cloud sessions may not reach proxy.golang.org or modernc.org, so tests may
  only run in CI. Read failures through check-run annotations (see
  RELEASING.md) when job logs can't be downloaded. Never claim a test passed
  without a run that shows it.
- `third_party/datasketches` holds the license and notice for code adapted
  from Apache DataSketches Go; keep them.
