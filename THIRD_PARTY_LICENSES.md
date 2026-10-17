# Third-party licenses

RollupDB is under the Apache License 2.0 (see [LICENSE](LICENSE) and
[NOTICE](NOTICE)). The rollupdb command and its ready-built binaries also
contain the Go modules below. Every release archive carries a
THIRD_PARTY_LICENSES.md with the full license text of each module actually
compiled in, written by
[scripts/third-party-licenses.sh](scripts/third-party-licenses.sh) from the
module sources at build time. Run that script to see the same texts for any
commit.

| Module | Version | License |
|---|---|---|
| github.com/apache/datasketches-go | v0.2.0 | Apache 2.0 |
| modernc.org/sqlite | v1.60.1 | BSD 3-Clause; the SQLite code it is translated from is in the public domain |
| modernc.org/libc | v1.77.1 | BSD 3-Clause, plus the third-party notices in its source |
| modernc.org/mathutil | v1.7.1 | BSD 3-Clause |
| modernc.org/memory | v1.12.1 | BSD 3-Clause |
| golang.org/x/sys | v0.48.0 | BSD 3-Clause |
| github.com/google/uuid | v1.6.0 | BSD 3-Clause |
| github.com/remyoudompheng/bigfft | v0.0.0-20230129092748 | BSD 3-Clause |
| github.com/twmb/murmur3 | v1.1.8 | BSD 3-Clause |
| github.com/dustin/go-humanize | v1.0.1 | MIT |
| github.com/mattn/go-isatty | v0.0.24 | MIT |
| github.com/ncruces/go-strftime | v1.0.0 | MIT |
| The Go runtime and standard library | 1.27 | BSD 3-Clause |

The list follows go.mod. A module that go.mod names but the command doesn't
compile in is left out of the archive's file.

## Apache DataSketches Go

Besides being a dependency, DataSketches is the source of the compactCapacity
function in kll_format.go, adapted under the Apache License 2.0. Its license
and notice are kept in [third_party/datasketches](third_party/datasketches).
