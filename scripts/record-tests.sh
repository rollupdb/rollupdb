#!/bin/sh
# Copyright rollupdb.com 2026
# SPDX-License-Identifier: Apache-2.0
#
# Runs every test, the race detector and the measured examples, and writes the
# output to test/results/ with a note of the machine they ran on. Every number
# on rollupdb.com comes from these files.
#
#   sh scripts/record-tests.sh
#
# The record workflow (.github/workflows/record.yml) runs this on GitHub and
# commits the results, for sessions that can't download Go modules.
set -eu
cd "$(dirname "$0")/.."
out=test/results
mkdir -p "$out"
export CGO_ENABLED=0

header() {
	echo "# $1"
	echo "# recorded $(date -u '+%Y-%m-%d %H:%M UTC') with $(go version | cut -d' ' -f3-)"
	echo "# $(uname -sr), $(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | sed 's/^ //'), $(nproc 2>/dev/null || echo '?') CPUs"
	echo "# command: $2"
	echo
}

# record NAME TITLE COMMAND...: run the command, keep its output with a header.
record() {
	name=$1 title=$2
	shift 2
	{ header "$title" "$*"; "$@" 2>&1; } > "$out/$name"
}

go vet ./...

# Tests write their own progress lines; the CGo-free build is the shipped one.
record tests.txt "Every test, verbose" go test -count=1 -v ./...
record race.txt "Every test with the race detector" env CGO_ENABLED=1 go test -race -count=1 ./...
record kill.txt "Process-kill recovery tests, three runs" go test -count=3 -v -run '^TestKilledOwner' .

# The measured examples print JSON reports (after the header lines): sizes, timings and accuracy
# against references computed outside the database.
record accuracy.txt "Quantile accuracy: KLL estimates against exact sorted references" go run ./examples/accuracy -n 100000 -summary
record retention.txt "Retention: 20,000 observations rolled up, five years advanced, snapshot reopened" go run ./examples/retention -n 20000
record history.txt "Five years of history: volume and cardinality cases against an outside reference" go run ./examples/history

grep -hE '^(ok|FAIL|---)' "$out/tests.txt" "$out/race.txt" "$out/kill.txt" | grep -v -- '--- PASS' || true
echo "results written to $out/"
