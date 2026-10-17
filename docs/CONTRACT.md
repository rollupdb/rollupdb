# Correctness contract

This is the implementation target. Session 1 implements only initial-definition validation and persistence; rules for observations, statistics, rollups and queries below are specifications for subsequent sessions.

## Time and stream definitions

Event timestamps are instants normalized to UTC. Supported instants will be at or after 1970-01-01 and before 2200-01-01, avoiding signed nanosecond overflow. Buckets are half-open intervals [start, end). A timestamp exactly at end belongs to the next bucket. The initial supported widths are 1, 60, 3600 and 86400 seconds, aligned to the Unix epoch. Months are deferred, not represented by thirty-day durations.

A definition has a stream name, immutable version, effective timestamp, ordered dimension names, declared statistics, maximum retained dimension groups and a tier policy. Effective time must align to the coarsest configured width. Names use ASCII letters, digits and underscores, start with a letter or underscore, and have at most 64 characters. Up to sixteen distinct dimensions and one million retained groups are allowed by the prototype configuration validator. These are validation limits, not capacity promises.

Currently only creation of version 1 is available. Subsequent changes create new immutable versions rather than editing old definitions. New dimensions cannot be inferred historically. New statistics apply from their coverage date unless demonstrably derivable from retained state. Definition transitions must align to the coarsest tier boundary to avoid cross-version bucket ambiguity. Future ingestion before the first effective timestamp or against a mismatched definition is rejected.

The dimensions are one joint grouping, not independent marginals. Preserve exact dimension keys with bounded string sizes and a deterministic collision-free encoding; Session 2 will select and document that encoding. Queries may combine retained groups and filter declared dimensions. They cannot filter or group by information never retained. Reject new groups once the limit is reached; never collapse them into unrelated groups. Retain dimension keys while referenced by live buckets and reclaim keys only when their last bucket expires. Infinite final retention can make historical keys persist forever, so a configured group cap remains necessary.

## Retention and grace

Policy fields use whole seconds. Widths must increase and nest. Finite retention must be a multiple of width, at least one width, and at least the grace. Retention and grace do not decrease across tiers. Zero retention means forever and is allowed only on the final tier. Retention is measured from bucket end. Grace is also measured from bucket end. Grace and retention are limited to one hundred leap years in the initial validator.

An observation at 12:00:30 belongs to [12:00:30,12:00:31) when the finest tier is one second. With a 120-second grace, it may be accepted while the ingestion clock is strictly before 12:02:31. At exactly 12:02:31 it is rejected as late. The observation timestamp is not rewritten to arrival time. Rejections after the finest-tier cutoff are counted; v0.1 does not redirect them into coarse buckets. The decision uses the declared timestamp cutoff and one clock sample per batch, not whether a scheduler has physically moved the bucket yet. Clock rollback must not reopen previously finalized intervals; persist and enforce a nondecreasing ingestion watermark. Reject future timestamps rather than letting far-future observations consume arbitrary retained state. A manual or test clock must be explicit, and production assumes a reasonably synchronized host clock.

Grace closes updates; retention controls how long detail is kept. They are distinct. A closed fine bucket can remain readable until its retention boundary. Coarse buckets receive finalized child state through the engine, not direct user observations. Their grace is a lower bound on finalization; the engine must also establish that all contributing lower tiers are complete.

## Promotion and ownership

Prefer whole-parent-interval promotion for v0.1. For a minute target interval, merge every second bucket belonging to that interval in one transaction, then remove those source buckets in the same transaction. A whole parent interval becomes eligible only after parent_end + max(source_retention, source_grace). This may retain some fine buckets up to one extra parent width; retention is a minimum detail lifetime, not an exact deletion timer.

For example, seconds retained for 3600 seconds feeding the minute [12:00,12:01) cannot promote that interval before 13:01. All second-level grace windows must also have closed. Minute-to-hour promotion additionally requires proof that every contributing second interval has already been processed into the minute tier. Do not infer completeness merely from an absent bucket row: an empty interval and unprocessed work are different. Persist contiguous promotion frontiers per stream definition and tier, advance them over empty intervals as well as populated ones, and process tiers from finest upward. An idle stream must make progress without manufacturing observations. Sparse histories must use range-based advancement rather than scanning every possible second across years.

The source and target state changes and frontier advancement form one transaction. After interruption, either the sources remain authoritative or the target is authoritative; never both. A retry must not merge the same source twice. Final-tier expiry removes authoritative state and its no-longer-referenced dimension keys transactionally. Forever retention never expires the final tier. Aggregate rows from incompatible definition or sketch versions must never be combined silently.

This ownership rule avoids a partially built parent overlapping fine siblings for the same stream, group and interval. It does not remove the need for a query coverage plan: groups, definition versions and tier boundaries can differ across a requested range.

## Statistical state

Persist a checked integer count, floating-point sum, min, max, running mean and M2 (sum of squared deviations from the mean). Use Welford updates and Chan-style merges. Average is weighted by counts, never by averaging bucket averages. Population variance is M2/n; sample variance is M2/(n-1) and unavailable for fewer than two observations. Reject nonfinite inputs and overflow rather than persisting invalid state. Numerical tolerances, cancellation behavior and an accepted floating-point reference method must be specified and tested in Session 2. Mathematical exactness of floating-point sums is not promised.

An empty covered interval may have count zero and sum zero; min, max, average and variance are null. This differs from an interval outside collection coverage, expired history or a statistic unavailable under the historical definition. Those cases need availability metadata and cannot be encoded as all-zero observations. Statistics not selected for public retention are not exposed merely because internal state could derive them.

One mergeable percentile sketch will be selected in Session 3. Never store a list of per-bucket percentile answers as a substitute for a distribution summary. Each sketch's algorithm, encoding version and parameters must be recoverable from its bucket or immutable definition. Only documented compatible merges are supported. If merging different supported parameter sizes changes accuracy, disclose the resulting bound. A rank-error guarantee is not a relative value-error guarantee. Measure both rank discrepancy and observed value discrepancy against a reference, with duplicate-aware rank intervals and documented handling of quantile conventions.

## Queries and coverage

All reads of bucket state and coverage metadata belong to one consistent read transaction. Plan a disjoint set of authoritative buckets. Never count finer state in addition to the parent containing it. Responses must include requested and actual time coverage, actual bucket resolutions, definition coverage, statistic availability, missing intervals and approximation metadata. A coarse bucket that only partly intersects the requested range cannot be divided by proportion or treated as an exact answer for that fragment. Default to an explicit partial-bucket error; a future opt-in aligned-range query may widen the range and report its actual boundaries.

Test a query crossing the seconds-to-minutes frontier, a range beginning halfway through an hourly bucket, an empty but covered interval, expired history, and an interval spanning an added dimension. All are correctness cases, not display decisions. Schema coverage must distinguish never collected, no events and expired data.

## Batches, retries and durability

Session 2 will implement batches with atomic persistence of accepted observations and rejection counters. Reject structurally invalid request envelopes without writes. Within a valid envelope, invalid observations, too-late observations and group-limit rejections receive distinct outcomes; valid observations are committed together. A storage failure rolls back all state and counter changes and must not return a successful acceptance acknowledgment. Define integer overflow and group admission order deterministically. Resubmission counts as new input; no automatic event deduplication is promised. A client retry after an ambiguous network failure can duplicate observations.

A durable acceptance acknowledgment follows commit. Snapshot success follows completed export and explicit file sync. Ordinary failed exports are removed; process termination can leave partial output at the destination. Never tell a user that an arbitrary copy of a running main database file is a safe backup. One owner process and supported local filesystems are the initial operational contract; power-loss recovery and promotion fault injection are acceptance work, not claims already validated by the Session 1 unit tests.
