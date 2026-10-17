#!/bin/sh
# Smoke test for a built rollupdb binary: create a database, define a stream,
# ingest three observations, query them back, advance, snapshot and reopen
# the snapshot. Used by the release workflow on every binary it can run.
#
# Usage: scripts/smoke.sh ./rollupdb
set -eu

bin=$1
case $bin in /*) ;; *) bin=$(pwd)/$bin ;; esac
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cd "$work"

"$bin" version
"$bin" --help > /dev/null

# A timestamp a few seconds in the past, in RFC 3339, on GNU and BSD date.
past=$(( $(date -u +%s) - 5 ))
if at=$(date -u -d "@$past" +%Y-%m-%dT%H:%M:%SZ 2> /dev/null); then :; else
	at=$(date -u -r "$past" +%Y-%m-%dT%H:%M:%SZ)
fi
until=$(( past + 1 ))
if to=$(date -u -d "@$until" +%Y-%m-%dT%H:%M:%SZ 2> /dev/null); then :; else
	to=$(date -u -r "$until" +%Y-%m-%dT%H:%M:%SZ)
fi

cat > definition.json << EOF
{"name":"smoke","version":1,"effective_at":"1970-01-01T00:00:00Z","dimensions":null,
 "statistics":["count","sum","avg"],"max_groups":1,
 "tiers":[{"width_seconds":1,"retain_seconds":0,"grace_seconds":3600}]}
EOF
cat > batch.json << EOF
{"stream":"smoke","version":1,"observations":[
 {"time":"$at","value":0},{"time":"$at","value":2},{"time":"$at","value":4}]}
EOF
cat > query.json << EOF
{"stream":"smoke","version":1,"from":"$at","to":"$to"}
EOF

"$bin" init smoke.rollupdb
"$bin" create-stream smoke.rollupdb definition.json
"$bin" ingest smoke.rollupdb batch.json | grep -q '"accepted":3'
"$bin" query smoke.rollupdb query.json > result.json
grep -q '"count":3' result.json
grep -q '"avg":2' result.json
"$bin" advance smoke.rollupdb smoke
"$bin" snapshot smoke.rollupdb copy.rollupdb
"$bin" inspect copy.rollupdb > /dev/null
"$bin" query copy.rollupdb query.json | grep -q '"count":3'

echo "smoke test passed"
