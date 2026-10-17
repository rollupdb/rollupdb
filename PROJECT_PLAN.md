RollupDB Project Plan

Status: Session 3 optional KLL quantiles, guarded sketch storage and measured accuracy implemented; see RESUME.md for checkpoint and validation status.
Created: October 17, 2026.
Code repository: https://github.com/rollupdb/rollupdb
Future website: https://rollupdb.com
RollupDB is a new, independent project. This plan defines its scope and has no dependency on another project.

Product

RollupDB is an embedded aggregate database that preserves declared statistics while progressively discarding time detail. The public explanation is: keep years of useful statistics in one portable database file. RRDtool rebuilt with mergeable sketches is the technical shorthand, not a claim of compatibility. Storage savings and approximation accuracy will be measured together on reproducible workloads.

The first users are developers retaining numeric telemetry from APIs, devices and AI applications. The first release targets a single machine and a single writer. It provides a Go library, a command-line executable and an optional local HTTP service using the same engine. SQL, clustering, dashboards, raw-event search and arbitrary historical regrouping are outside v0.1.

Storage and architecture

Use Go for the engine and executable. Evaluate SQLite-backed storage first rather than spending the prototype budget building a transactional file engine. The first session must verify dependency portability, build complexity, transaction behavior and backup behavior before locking that decision. This is a proposed choice, not an implemented architecture.

A closed database must be transferable as one .rollupdb file. Temporary transaction sidecars during operation are permissible and must be documented. Copying a live file is not a supported backup mechanism: provide a consistent snapshot command and demonstrate copying and reopening the resulting standalone file. A custom storage engine is a later option only if measurements identify a concrete limitation.

Separate statistical state, bucket storage, retention scheduling, query planning and transport. Persist stream definitions, canonical dimension keys, bucket state, definition versions, sketch metadata and operational counters. Acknowledge ingestion only after commit. Failed transactions must neither partially apply a batch nor delete its source buckets.

Stream contract

A stream declares an event timestamp, one finite numeric observation per event, named dimensions, retained statistics, dimension-combination limits, retention tiers and lateness settings. The prototype supports count, sum, min, max, average, population and sample variance, and approximate percentiles from one mergeable sketch family. Counts use integer state; floating-point sums and moments have documented numerical tolerances and overflow behavior. Reject invalid timestamps and nonfinite numeric values explicitly.

Store count, sum, extrema and stable mergeable variance state rather than materialized averages or variances. Select an existing percentile implementation after checking merge semantics, serialized format, dependency license and its actual error guarantee. Prefer a quantile sketch with a documented rank-error model. Do not write a new sketch algorithm for the prototype. Persist algorithm identity, serialization version and parameters with each bucket or through an immutable definition reference. Reject incompatible merges explicitly; allow supported parameter combinations only under documented accuracy rules.

Dimensions are a declared joint grouping. Queries may filter and combine retained groups but cannot reconstruct omitted dimensions. Limit dimension-combination growth; reject excess combinations with an observable counter rather than silently combining unrelated groups. An initial limit alone does not guarantee indefinitely bounded storage, so define when inactive dimension keys can be reclaimed and include metadata growth in measurements.

Time, lateness and rollups

Use UTC, half-open bucket intervals and an injectable clock. Start with aligned seconds, minutes, hours and days. Calendar months are deferred until the fixed-tier behavior is proven; they must eventually be real calendar intervals, not an assumed thirty-day duration. Validate policy nesting and ensure retention and grace settings permit safe promotion.

For v0.1, accept events only into their finest configured bucket while its grace window remains open. Reject and count arrivals after that bucket closes, even if a coarser tier could still accept them. This deliberately chooses one predictable lateness rule. Each tier has its own documented finalization/grace rule, but grace settings must not imply ingestion paths that are unsupported. Coarser-tier late ingestion and correction propagation are later work.

Finalize and promote buckets transactionally, merging each source exactly once into its parent before removing the source. Persist completion or ownership information sufficient for restart safety. Ensure a parent cannot finalize before all eligible child buckets have closed and been promoted. Scheduling delays and restart must not change which events are accepted: acceptance depends on the declared timestamp cutoff, not whether background cleanup has happened.

Queries must never count a child and its already represented parent together. Define a disjoint coverage plan and test boundaries explicitly. Return actual resolution, covered intervals, missing intervals, definition coverage and sketch accuracy metadata. Requests for unsupported fine resolution or partial coarse buckets must return an explicit limitation or require an explicit aligned-range option, rather than inventing sub-bucket values. Expired history and unavailable statistics are not zeros.

Definition changes

Use immutable stream-definition versions with effective timestamps. Adding a statistic applies only to future coverage unless it can be derived from already retained state. Historical queries return availability and coverage dates. Adding or changing a dimension creates a new version; queries requesting that dimension cannot treat older ungrouped observations as if they had it. For v0.1, restrict transitions to aligned bucket boundaries and reject unsupported transitions rather than merging ambiguous state.

API and retry contract

Provide commands for stream creation, batched NDJSON ingestion, query, inspect, retention advancement, snapshot and demo. Add a small JSON HTTP API only after the embedded path works. Return accepted, rejected-late and rejected-invalid counts with defined batch atomicity. Document that an ordinary resubmission is a new observation: automatic event deduplication is outside v0.1. Bind the optional service to localhost by default; remote authentication and multi-tenant hosting are outside this prototype.

Session 1: contracts and repository foundation, approximately 2–3 hours

Write the README, architecture and format contracts, package layout and initial resume instructions. Establish the Go module and basic CI. Spike the storage dependency and one-file snapshot/reopen path. Specify time boundaries, grace rules, query coverage, definition transitions and numerical behavior using worked examples. Exit when the architecture is concrete and a minimal database can be created and reopened. Record any remaining dependency decisions before proceeding.

Session 2: durable exact aggregation, approximately 2–3 hours

Implement versioned stream definitions, canonical dimension groups, batched ingestion, dimension limits and count/sum/extrema/variance state. Add basic inspection and queries over a single tier. Verify unequal bucket weights, large offsets with small variance, empty and single-observation cases, invalid input and reopen behavior. Exit when committed aggregates survive restart and match an independent reference within declared numerical tolerances.

Session 3: percentile state, approximately 2–3 hours

Integrate the selected sketch, serialization metadata and supported merge compatibility checks. Implement requested percentile queries and expose the actual error model. Compare direct ingestion and merged partitions on constant, skewed, duplicate-heavy and heavy-tailed datasets. Exit when persisted and merged sketches produce valid results and incompatible encodings fail clearly.

Session 4: retention and query correctness, approximately 2–3 hours

Implement the aligned tier scheduler, grace enforcement, transactional promotion and disjoint query coverage. Exercise late and out-of-order observations, exact boundary timestamps, interrupted promotion, repeated scheduler runs and schema-version boundaries. Include query ranges crossing tiers and partial coarse buckets. Exit when retention can run repeatedly and recover from interruption without losing acknowledged observations or double counting them.

Session 5: reproducible demonstration and interfaces, approximately 2–3 hours

Complete CLI operations, consistent snapshot export and the minimal local HTTP API. Build deterministic telemetry generators and a reference evaluator that runs outside the database. Simulate years of timestamps with the injectable clock. Report raw serialized input bytes, resulting standalone database bytes, peak working disk usage, ingest/query timings, retained group counts, numerical discrepancies and percentile error. Show rank error and observed value error separately where meaningful. Demonstrate that increased event volume within the same groups and buckets does not grow retained state proportionally, then separately show the effect of increasing group cardinality. Exit when the demo can be reproduced with documented commands, including copying a snapshot and reopening it.

Session 6: hardening and release preparation, approximately 2–3 hours

Run race and crash-recovery checks appropriate to the implementation, finish operational limits and format compatibility documentation, and exercise the full demo from a clean checkout. Add tag-driven release automation for an initially small supported platform matrix, checksums and automated binary smoke tests. Confirm the license before the first public release; do not assume a license from another project. Prepare a v0.1 candidate only after acceptance checks pass. Estimates are work-session targets, not guarantees: correctness failures take priority over the schedule.

Acceptance and measurement

The decisive demonstration ingests millions of reproducible observations, advances retention through multiple tiers, compares retained results against the reference, restarts, snapshots, copies one database file and queries it again. Exact counters must agree; floating-point aggregates must satisfy documented tolerances; percentile behavior must be evaluated against the sketch's stated error model. Include adversarial timing, high dimension cardinality, unavailable historical schema and interrupted transactions. Never present 100 GB to 50 MB as a guaranteed compression ratio.

Low-maintenance workflow

Keep dependencies small, tests deterministic and workflows limited to CI and tag-triggered releases. No cloud service is required to run the database. Each implementation session ends with committed work, a concise report, validation results and an updated RESUME.md containing current commit, completed work, open issues, exact next steps and reproduction commands. Pause after each session and resume on the user's command. Do not start implementation as part of this planning step.

Website

rollupdb.com will explain the product, show installation and a measured example, link documentation and releases, and include a contact page using info@rollupdb.com. Put Contact in both top navigation and footer. Website work starts when the website repository or hosting target is supplied; the code repository is not assumed to be the website repository. Domain connection is deferred. Publish only demonstrated functionality and measured results. An interactive browser demo is optional follow-up work, not a dependency for the engine prototype.

Resume

Current state: Session 3 adds optional KLL float64 state, guarded checksummed serialization, equal-configuration merges, percentile queries with error/availability metadata, explicit revision-1/2 migration and measured direct/partitioned fixtures. Existing exact aggregation and snapshot paths remain supported. Automatic retention, a release and the website remain future work. See RESUME.md for the current checkpoint.
Next action on an implementation command: inspect the current repository and any AGENTS.md instructions, read RESUME.md, then begin Session 4.
