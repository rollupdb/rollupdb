# Releasing RollupDB

Releases go out on their own. Nobody clicks Publish. This file is the record of
how that works, for whoever (person or Claude session) works on this repo next.

## The short version

1. Raise `const Version = "X.Y.Z"` in `doc.go`.
2. Write what changed into the commit message. The release notes list the files; the site's posts carry the story.
3. Push the commit to `main`.

That's it. When the `test` workflow passes on that commit, the `release`
workflow builds the binaries, smoke-tests them, then tags the commit `vX.Y.Z`
and publishes the release "RollupDB vX.Y.Z" with the archives and SHA256SUMS
attached. If any build or check fails, nothing is tagged and nothing goes
public. Fix it, push again, and the next green run picks it up.

Pushing without raising the version is fine. The release workflow sees that the
version already has a tag and stops quietly.

## What it does, step by step

The `release` workflow ([.github/workflows/release.yml](.github/workflows/release.yml))
is started by a finished `test` run, not by the push itself.

- **plan.** Goes on only if the `test` run passed, came from a push to `main`
  in this repo, and `main` still points at that commit (if `main` moved on, the
  run for the newer commit decides). Reads the version from `doc.go`.
  Stops quietly if `vX.Y.Z` is already a tag. Fails loudly if the version is
  lower than the newest tag.
- **build.** RollupDB is pure Go (no CGo), so each binary is built with
  `CGO_ENABLED=0` from that exact commit on its own platform: Linux x86-64 and
  ARM64, macOS Apple silicon and Intel, Windows x86-64. Every binary that can
  run on its runner (all but macOS Intel) runs `scripts/smoke.sh`: it creates a
  database, defines a stream, ingests, queries, advances, snapshots and
  queries the snapshot. Each archive gets the binary, LICENSE, NOTICE,
  README.md and a THIRD_PARTY_LICENSES.md written by
  `scripts/third-party-licenses.sh` for that platform's module set.
- **release.** Runs only after every build passed. Creates the tag and the
  release in one step with `gh release create --target <commit>`, on the
  repo's own `GITHUB_TOKEN` (`contents: write` on this job only). A
  `concurrency` group per tag means two runs for the same version can't
  publish twice.

Notes on the release come from the table in the workflow's "Checksums and notes" step.

## Files that go out

| System | File |
|---|---|
| Linux, x86-64 | rollupdb_linux_amd64.tar.gz |
| Linux, ARM64 | rollupdb_linux_arm64.tar.gz |
| macOS, Apple silicon | rollupdb_darwin_arm64.tar.gz |
| macOS, Intel | rollupdb_darwin_amd64.tar.gz |
| Windows, x86-64 | rollupdb_windows_amd64.zip |
| Checksums | SHA256SUMS |

Archive names carry no version on purpose. Links like
`https://github.com/rollupdb/rollupdb/releases/latest/download/rollupdb_linux_amd64.tar.gz`
keep working from one release to the next, so the site's download page never
needs editing for a release.

## After pushing: checking it went out

Claude's cloud sessions can't create tags or releases themselves, and the
`gh` GraphQL calls may be blocked there. The REST API works:

```sh
# the test run, then the release run, for the commit just pushed
gh api "repos/rollupdb/rollupdb/actions/workflows/test.yml/runs?per_page=1" \
  --jq '.workflow_runs[0] | "\(.status) \(.conclusion) \(.head_sha[0:7])"'
gh api "repos/rollupdb/rollupdb/actions/workflows/release.yml/runs?per_page=1" \
  --jq '.workflow_runs[0] | "\(.status) \(.conclusion) \(.head_sha[0:7])"'

# why a job failed, when the logs themselves can't be downloaded
gh api repos/rollupdb/rollupdb/check-runs/<job id>/annotations \
  --jq '.[] | "\(.path):\(.start_line) \(.message)"'

# the release and its files
gh api repos/rollupdb/rollupdb/releases/latest \
  --jq '.tag_name, .name, (.assets[].name)'
```

Then download one archive through the `latest/download` link, check it
against SHA256SUMS, unpack it and run `rollupdb version`. It should print the
new version. Only then is the release done.

If the release run failed: read its jobs
(`gh api repos/rollupdb/rollupdb/actions/runs/<id>/jobs`), fix the cause on
`main`, and push. Don't change the version unless the fix needs it; the tag
was never made, so the same version goes out on the next green run.

## Other ways in

- **A release published by hand** on GitHub still works. The workflow builds
  the binaries for that tag and attaches them.
- **Rebuilding an existing tag:** run the `release` workflow by hand
  (Actions, release, Run workflow) with the tag, such as `v0.1.0`. It rebuilds
  and replaces the files on that release.
- Releases made by the workflow don't start the workflow again, because
  events from `GITHUB_TOKEN` don't trigger other runs. No loop.

## Rules that don't change

- The owner doesn't publish releases, interim or final. Never ask him to. If
  something blocks a release, fix it in the repo.
- Every release is a full release ("RollupDB vX.Y.Z"), not a pre-release.
- Versions only go up. The plan step refuses a version below the newest tag.
- The CI workflow must keep the name `test`, because `release.yml` listens for
  `workflows: [test]`. Renaming one means renaming the other.
- `workflow_run` only fires for workflow files on the default branch, so
  changes to either workflow take effect once they're on `main`.
- `FormatVersion` in `db.go` is the on-disk format and moves on its own
  schedule; raising `Version` never changes it.
