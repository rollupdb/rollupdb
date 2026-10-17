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

- Engine, CLI and local HTTP service are in place: exact statistics, KLL
  quantiles, fixed UTC tiers with grace windows, `Advance` for promotion and
  expiry, disjoint queries across resolutions, snapshot export, format
  revision 4. Docs are in `docs/`.
- Process-kill recovery tests cover acknowledged definitions and ingestion,
  open uncommitted transactions, a finished promotion and an `Advance` killed
  just before commit. They passed in CI before the last break.
- License chosen: Apache 2.0 (LICENSE, NOTICE, THIRD_PARTY_LICENSES.md).
- Release automation added: `test` and `release` workflows, `scripts/smoke.sh`,
  `scripts/third-party-licenses.sh`. `Version` is 0.1.0 and no tag exists, so
  the first green push to main publishes RollupDB v0.1.0.
- Fixed: `retention_cancel_commit_test.go` lost its `time` import, which broke
  compilation on every CI platform.

## Next, agreed

1. Get the pending commits onto main (needs Claude push access to the
   rollupdb org), then watch `test` and `release` and check v0.1.0 as
   RELEASING.md describes.
2. Record test results into `test/results` with a script, and take every
   number used on the site from there.
3. Build the site at photozeitgeist/rollupdb.com (the owner creates the repo)
   with the standard Hugo Launch template, unchanged: about, contact
   (info@rollupdb.com), quick start, download and reference pages; posts for
   the launch, a plain-language explainer, how it was tested and the name.

Feature work after 0.1.0 is not decided yet. Agree it with the owner first.

## Working here

- Go version: whatever `go.mod` says. Build and test with `CGO_ENABLED=0`.
- `go test ./...`, `go test -race ./...`, `go vet ./...`, `gofmt -l .`
- Cloud sessions may not reach proxy.golang.org or modernc.org, so tests may
  only run in CI. Read failures through check-run annotations (see
  RELEASING.md) when job logs can't be downloaded. Never claim a test passed
  without a run that shows it.
- `third_party/datasketches` holds the license and notice for code adapted
  from Apache DataSketches Go; keep them.
