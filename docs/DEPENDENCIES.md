# Dependency decision

The direct runtime dependency is modernc.org/sqlite v1.60.1, pinned in go.mod and go.sum. It provides a Go SQLite driver without CGo. The resolved modernc.org/libc version is v1.77.1, matching the driver's requirement. SQLite is used as a transactional storage substrate; RollupDB semantics live in the Go library.

The graph includes modernc.org/libc, memory and mathutil, golang.org/x/sys, and small supporting modules. This is more code than a bespoke in-memory aggregate map, but replaces a substantial persistence and recovery implementation. Dependency installation and initial compilation require time and network access; no dependency downloads are required by a built executable.

The SQLite driver has its own license and third-party notices. Review dependency license texts and include required notices in binary distribution artifacts before the first release. This project has not selected its own distribution license. A percentile-sketch dependency is deliberately not selected until Session 3, when mergeability, serialization, accuracy model, license and maintenance can be checked together.
