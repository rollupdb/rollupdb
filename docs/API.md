# API and numerical behavior

The embedded API is the engine. The CLI reads bounded UTF-8 JSON files or streaming NDJSON, and the loopback HTTP service calls the same methods. See docs/INTERFACES.md for transport limits, per-batch partial progress and HTTP routes. Stream definitions remain immutable version 1, with an optional sketch configuration for new streams. Adding a new definition version is future work. See docs/SKETCHES.md for the quantile contract and encoding.

## Ingestion

Call `db.Ingest(ctx, Batch{Stream: name, Version: 1, Observations: observations})`, or `rollupdb ingest FILE BATCH.json`. A batch contains between 1 and 10,000 observations. Each observation has time, value and dimensions. Time is an RFC3339 instant; value is one finite float64; dimensions must match all declared names exactly. Empty dimensions are allowed for scalar streams. JSON must explicitly supply non-null time and value, so a missing value is not silently interpreted as zero. Unknown JSON fields and additional trailing JSON objects are rejected. CLI batch files are limited to 64 MiB; definition and query files to 1 MiB.

```json
{
  "stream": "api_requests",
  "version": 1,
  "observations": [
    {"time": "2026-10-17T12:01:59Z", "value": 12.5, "dimensions": {"service": "web", "status_class": "2xx"}}
  ]
}
```

The timestamps above illustrate the shape, not a permanently live example. Production uses the host clock and rejects future or too-late events. `WithClock(func() time.Time)` makes replay explicit; `go run ./examples/replay` supplies a deterministic working example.

Every batch samples the clock once. Event timestamps must be at or after the definition's effective time, at or after 1970, before 2200, and no later than that raw clock sample. The closure watermark is max(previous committed watermark, raw clock sample), so moving the clock backward cannot reopen an old bucket. A bucket accepts updates only while watermark < bucket_end + grace. Arithmetic uses subtraction to avoid nanosecond overflow with long grace periods. Out-of-order events within that window are accepted. A clock before the definition's effective time rejects the request without writes.

The result contains accepted, rejected_invalid, rejected_late and rejected_group_limit counters for this batch, ordered per-observation outcomes, and observed_through_inclusive. Invalid observations carry timestamp, non_finite, dimensions or numeric_overflow reasons. Validation precedes lateness, which precedes group admission and state update. New groups are admitted in input order; a rejected observation does not consume a new group slot. Dimension values must be valid UTF-8 and at most 1024 bytes. Their JSON-array tuple, ordered by the definition, must be at most 4096 bytes. Escaping prevents delimiter collisions; the full tuple is retained rather than a lossy hash.

Accepted bucket state, group metadata, lifetime rejection counters and the watermark commit together. No observations are stored in a raw-event table. A SQL failure before commit leaves all of those unchanged, and an error returns no accepted-result acknowledgment. Lifetime counters and observation counts use checked int64 arithmetic. Counter overflow aborts the entire batch; a per-bucket arithmetic overflow rejects that observation as invalid without changing its state. Retrying is new input, not an idempotent operation. A commit-level I/O error may be indeterminate to the caller.

## Queries and availability

Call `db.Query(ctx, Query{Stream: name, Version: 1, From: from, To: to, Filter: filters})`, or `rollupdb query FILE QUERY.json`. From is inclusive and To exclusive. Both boundaries must align to the finest tier. To may equal 2200-01-01 solely as an exclusive endpoint; event times may not. Unknown dimensions and invalid definition versions return errors. Filters are exact matches on any subset of declared dimensions. The current response combines matching groups and buckets into one summary; grouped output is not yet exposed.

```json
{
  "stream": "api_requests",
  "version": 1,
  "from": "2026-10-17T12:01:58Z",
  "to": "2026-10-17T12:02:00Z",
  "filter": {"service": "web"}
}
```

Results include requested, covered, missing, resolutions, resolution_seconds, definition_version, available_statistics, statistics, complete and buckets_merged. Statistics are limited to the definition's selected names. Count is a JSON integer, not a float64 conversion; JavaScript clients must still account for JavaScript's own safe-integer limit. Queries read state and coverage in one transaction and merge matching state in deterministic timestamp/key order.

Before the first committed batch, coverage is absent and every selected statistic is null. The first committed valid-envelope batch, even one whose observations are all rejected, establishes recorder coverage starting at max(effective time, the earliest bucket that is still admissible at the watermark). Later batches extend it through the nondecreasing committed watermark. This means no observations were recorded in empty intervals; it does not certify upstream source completeness. Reads do not advance the watermark. Advance extends an existing recorder coverage interval while coarsening/expiring retained state. Before the first batch it sets a watermark without inventing a collection start. Original coverage_start remains unchanged when old state expires.

A covered interval with no observations returns count=0 and sum=0, with min, max, avg and variances null. A range outside retained collection coverage returns null statistics and not_collected, expired or not_observed_yet gaps. A partially covered request returns statistics only for the available observations and complete=false; callers must inspect coverage. The active bucket may be included in an aligned request, with actual coverage ending one nanosecond after the inclusive watermark and the unobserved suffix reported explicitly. This is a snapshot of incomplete collection, not proportional subdivision of a stored bucket. Requests with unaligned time boundaries return ErrPartialBucket.

db.Status(ctx, stream) and rollupdb stats FILE STREAM return lifetime counters, retained group and bucket counts, coverage_start and observed_through_inclusive. A query may additionally request up to 32 distinct normalized ranks in quantiles, each finite and in [0,1]. Sketch-enabled streams list quantiles in available_statistics and return a separate quantiles array plus quantile_accuracy. Exact-only streams return null quantile values with availability=not_retained. A configured stream without data returns not_collected or no_observations, as appropriate. Rank 0/1 use the preserved exact extrema. Definition changes are not implemented. Queries use persisted frontiers to select disjoint bands of fine and coarse state. Requests cutting a coarse bucket return ErrPartialBucket, including empty coarsened intervals and filters matching no groups; missing rows do not restore lost resolution. The resolutions array describes chronological retained coverage with tier and width_seconds. resolution_seconds is its maximum width, or zero when there is no retained coverage. It is not a claim that all history has that same width. A wholly expired sketch-enabled request returns null quantiles and availability=expired. A mixed available/expired range reports quantiles only for the available portion and complete=false.

## Retention advancement

Call `db.Advance(ctx, stream)` or `rollupdb advance FILE STREAM`. It samples WithClock/the host clock once and uses max(previous watermark, sample), like ingestion. No caller-supplied production timestamp or hidden query mutation is used. The result reports observed_through_inclusive, frontiers (tier, through_exclusive), source_buckets_removed, parent_buckets_created, expired_buckets and reclaimed_groups. These counts describe this transaction; one bucket can be created at one tier and consumed at the next during the same call. Lifetime ingestion counters do not change. A failed call returns a zero result and rolls back all writes before commit. Rerunning at the same watermark does no promotion/expiry work, and moving the clock backward cannot reopen finalized history.

Status also returns frontiers. For nonfinal tiers through_exclusive means promoted through; for the final tier it means expired through. Forever final retention has an empty expired prefix ending at effective_at. One transaction can process a long backlog and holds the handle lock until completion; use context deadlines. This first version has no bounded-work pagination or background scheduler. Deleting rows frees SQLite pages for reuse; shrinking the exported file requires Snapshot. See docs/RETENTION.md for the exact eligibility rule and worked example.

## Arithmetic and tested precision

Each bucket stores integer count, compensated sum and compensation, min, max, running mean and M2. Neumaier compensation retains the unit in the sequence [1e16, 1, -1e16], including across the tested partition merge. Public average is compensated sum/count. Welford updates and Chan-style weighted merges produce variance state; population variance is M2/n, sample variance M2/(n-1). One observation has population variance zero and unavailable sample variance. Empty intervals have neither variance.

Finite input does not guarantee finite intermediate arithmetic. If sum, compensation, mean, M2 or a checked count cannot be represented, ingestion rejects that observation without state mutation. Query merging returns ErrNumericOverflow if the combined state is unrepresentable. All exact state is maintained internally even when the public definition selects a subset, so internal arithmetic limits apply to count-only streams too. Extremely opposed magnitudes can be rejected conservatively even when some individual requested answers would be representable.

Tests compare direct ingestion and unequal-sized partition merges with an independent big.Rat reference. The large-offset fixture uses 257 exactly representable inputs near 1e12 with quarter-unit increments. Its mean tolerance is 0.0002 absolute, variance tolerance 0.0002 absolute plus 0.1% relative, and sum tolerance 1e-15 relative. Ordinary small-integer fixtures use 1e-12 absolute variance tolerance. These are acceptance tolerances for those documented workloads, not universal guarantees over arbitrary float64 sequences. Merge order can change rounding. KLL quantiles have a separate empirical rank-error model, not a float64 or relative-value guarantee; the observed errors are measured against an independent sorted reference in docs/accuracy-session-3.json.
