# Session log

## Session 1 — October 17, 2026

Implemented the Go/SQLite storage foundation, versioned definition persistence, single-file snapshot command and CLI. Added architecture, format, dependency and correctness contracts. The design uses explicit event-time cutoffs, whole-parent-interval promotion and persisted coverage/frontier requirements for later implementation. Ingestion, statistics and retention remain future sessions.

Local validation: pure-Go Linux unit and integration tests passed; race detection, go vet, go mod verify and formatting checks passed; Windows amd64 and macOS arm64 pure-Go cross-builds passed. The CLI snapshot/copy/reopen smoke test passed with one retained stream and identical file identity. Metadata-only snapshot size was 16,384 bytes; the initial Linux trimpath executable was 11,442,385 bytes. These are not compression benchmarks. Native hosted CI is pending the checkpoint push.

The snapshot test held an uncommitted write on a second SQLite connection while exporting committed state, then copied only the snapshot main file to another directory and reopened it. Additional tests check malformed policy definitions, unknown formats, canceled exports, path escaping, concurrency and overwrite protection. CI is configured for native Linux, macOS and Windows tests, with a Linux race check. No release or website deployment was performed.

Next session: implement durable exact-statistical state, canonical dimension groups, atomic batched ingestion and single-tier queries, with an explicit schema migration from the foundation format.
