#!/usr/bin/env bash
# Prints the full license texts of every module compiled into the rollupdb
# command, plus the Go runtime. The release workflow puts the output into each
# archive as THIRD_PARTY_LICENSES.md. CI runs it on every push, so a new
# dependency without a license file fails there instead of at release time.
#
# Usage: scripts/third-party-licenses.sh > THIRD_PARTY_LICENSES.md
set -euo pipefail

cd "$(dirname "$0")/.."

mods=$(go list -deps -f '{{with .Module}}{{if not .Main}}{{.Path}}@{{.Version}}={{.Dir}}{{end}}{{end}}' ./cmd/rollupdb | sort -u)
if [ -z "$mods" ]; then
	echo "no dependency modules found" >&2
	exit 1
fi

echo "# Third-party licenses"
echo
echo "The rollupdb command and its ready-built binaries contain the modules below"
echo "and the Go runtime. RollupDB itself is under the Apache License 2.0; see"
echo "LICENSE and NOTICE."

while IFS='=' read -r mod dir; do
	files=$(find "$dir" -maxdepth 1 -type f \( -iname 'LICENSE*' -o -iname 'LICENCE*' -o -iname 'COPYING*' -o -iname 'NOTICE*' \) | sort)
	if [ -z "$files" ]; then
		echo "no license file in $mod ($dir)" >&2
		exit 1
	fi
	echo
	echo "## ${mod%@*} ${mod##*@}"
	for f in $files; do
		echo
		echo "$(basename "$f"):"
		echo
		echo '```'
		cat "$f"
		echo '```'
	done
done <<< "$mods"

echo
echo "## Go $(go env GOVERSION | sed 's/^go//')"
echo
echo "The Go runtime and standard library, compiled into every Go binary."
echo
echo '```'
cat "$(go env GOROOT)/LICENSE"
echo '```'
