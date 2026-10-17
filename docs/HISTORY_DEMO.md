# Multi-year history, volume and cardinality experiment

Run `go run ./examples/history > history.json` for the default three-case replay. It ingests 5,606,400 observations in total across three independent databases, each spanning 1,825 fixed UTC days starting January 1, 2020. This is roughly five years; calendar years are not assumed to be 365 days in the database. Data arrives on 24 regular hourly slots per day, with multiple observations per slot/group. It is a regularly sampled history throughout the interval, not a claim of continuous per-second telemetry. The default final daily tier retains forever, so the main experiment's size ratios include all five years of declared statistics without expiry savings.

Baseline has two groups and 16 observations per slot/group (1,401,600 records). The volume case doubles repeats to 32 while preserving groups and timestamps (2,803,200 records). The cardinality case has eight groups and four observations per slot/group, preserving the baseline's total event count and timestamp grid (1,401,600 records). Values are deterministic bounded integers with periodic large outliers; each group's/repeat's values are generated explicitly, so the cardinality case is not a byte-identical reassignment of the baseline's source values. The report records input hashes and sizes for each case. This is a scoped demonstration of retained-state behavior, not a controlled production performance study.

The policies are second buckets retained one day, minutes retained one day, hours retained seven days, and daily buckets retained forever by default. Every tier has one-day grace, and ingestion occurs in daily batches of at most 10000 observations using an explicit day-end replay clock. Retention advances after each day. A final 12h34m advance creates a disjoint query crossing day/hour/minute/second ownership and extends recorder coverage through that idle period. There is no background scheduler or network clock override.

The generator writes actual NDJSON to an external source file outside the database directory, counting bytes and SHA-256. The engine receives only observation batches. After ingestion, the evaluator independently decodes the source file, selects the requested retained time range and dimension filter, accumulates exact integer sums/squared sums, computes mean/variance with big.Rat, and sorts original values for tie-aware quantile rank and value measurements. Counts, sums, extrema and public mean must match exactly for these bounded integer inputs. Variance is checked against 1e-8 absolute plus 1e-10 relative tolerance for this workload; this is not a universal float64 guarantee.

The example queries the mixed-resolution retained range and a single daily bucket filtered to g0. It closes/reopens the original database, exports/copies a standalone snapshot, opens that copy and checks the single stored sketch identically. It re-evaluates merged quantiles against source values after reopening, allowing legitimate randomized compaction to change point estimates. Same-time Advance reruns must do no promotion, expiry or reclamation work. A request cutting the daily bucket is explicitly refused. Gross rank regression checks use four times the empirical KLL model plus 1/N; they do not turn a 99% stochastic model into a deterministic bound.

For finite expiry, run the small native-CI command:

```sh
go run ./examples/history -days 10 -slots 4 -repeats 4 -groups 1 -cardinality-factor 2 -retain-days 8
```

This demonstrates all four resolutions, an independently calculated expiry frontier, null expired statistics/quantiles, retained-only reference comparisons, and stable snapshot/restart results. Finite input-to-snapshot ratios include intentionally expired input; do not describe those as aggregation-only compression. For no-expiry CI-sized replay, omit retain-days. The final-retention flag uses fixed UTC days, with zero meaning forever. Finite settings must be at least eight days so a complete retained daily bucket exists for the identity check. Generator settings are bounded and every case is capped at ten million observations/128 groups; those are experiment limits, not engine capacity promises.

## Measurement scope

The JSON report contains actual serialized input bytes/hash, completed standalone snapshot bytes, live main/WAL/shared-memory sizes before export, main-file size after clean close, retained group/bucket counts, exact discrepancies, individual rank/value errors and phase timings. Hardware context includes OS, architecture, Go version, logical CPU count and the Linux CPU model when available. Engine ingest/advance timings cover their calls only; query/snapshot timings cover those operations. Total case time includes generation, reference parsing/sorting, copies and checks. The shared execution environment can affect all timings, so these are not production throughput claims.

A sampler runs every 10ms and explicitly samples after original ingestion, advancement, export, copy and close. It sums apparent file lengths, including the database, WAL, shared-memory sidecar, snapshot and copied file within the experiment's database directory. peak_sampled_live_bytes includes the original main/WAL/SHM; peak_sampled_working_export_bytes includes all files in that directory. Directory scans are non-atomic: a sample can miss short-lived files or combine file lengths read at different instants. These maxima are approximate observed working sizes, not exact physical allocation, a strict bound or a guaranteed disk-space requirement. SQLite temporary files elsewhere, external source/reference storage and process/reference memory are excluded. reference_values_in_memory records the number of original values retained for evaluation, not total heap bytes. Working files are deleted after each case.

Deleting old rows can leave the live main file larger than its compact snapshot. The snapshot is the portable retained representation; the report keeps both sizes visible. Storage does not depend only on total input volume: group count, bucket count, KLL mode/settings, data distribution, metadata, indexes and retention all matter. Approximate values and sometimes sizes can vary between KLL runs. The recorded result is docs/history-session-5.json; use its exact settings and scope when describing it.

## Recorded Session 5 result

The recorded Linux amd64 run used Go 1.27.2, nine reported logical CPUs and an Intel Xeon Platinum 8370C at 2.80 GHz in the shared execution environment. All five years of declared statistics remained available, every main query was complete and covered all four resolution tiers, and all 5,606,400 accepted observations agreed with the external reference across the three cases.

| Case | Observations | Input bytes | Snapshot bytes | Input / snapshot | Groups | Buckets |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Baseline | 1,401,600 | 103,707,053 | 15,339,520 | 6.76:1 | 2 | 3,972 |
| Double volume, fixed groups/times | 2,803,200 | 207,414,097 | 15,396,864 | 13.47:1 | 2 | 3,972 |
| Four times groups, fixed event count/times | 1,401,600 | 103,707,074 | 16,310,272 | 6.36:1 | 8 | 15,888 |

Doubling event volume increased this run's compact snapshot by about 0.37%, with identical group/bucket counts. Raising groups from two to eight at the baseline event count quadrupled bucket count and increased compact size about 6.33%. The latter also changed per-group sample counts and KLL mode; it is not a claim that every fourfold cardinality change costs only 6.33%. All ratios describe lossy time/distribution aggregation of this specific input format, not lossless compression or a general storage guarantee.

Counts, sums, minima, maxima and public averages matched exactly for these bounded integer inputs. The largest recorded variance discrepancy was approximately 0.0000178516 absolute, within the workload's stated tolerance. Across initial mixed queries, copied mixed queries and single daily checks, the largest measured requested-rank discrepancy was about 0.00500143 (0.5001 percentage points). The K=200 empirical model is approximately 0.01329476. One copied mixed p99 still had about 9.57% relative value error; rank and value accuracy remain different. All single stored bucket queries were identical after original restart, snapshot copy and unchanged retention reruns. The separately run ten-day, eight-day-retention smoke replay passed expired-only null and retained-reference checks.

| Case | Engine ingest seconds | Advance seconds | Query ms | Snapshot ms | Sampled live bytes | Sampled working/export bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Baseline | 10.64 | 37.21 | 191.87 | 41.77 | 19,665,072 | 50,344,112 |
| Double volume | 12.38 | 39.50 | 111.31 | 43.32 | 19,685,576 | 50,479,304 |
| More groups | 32.54 | 117.84 | 335.99 | 75.05 | 28,284,016 | 60,904,560 |

Total case times, including generation/reference/copy checks, were about 55.67, 67.36 and 159.78 seconds. The higher-cardinality run spent much longer advancing more buckets despite equal event count. Timings can vary with caches, sketch compaction and other work in the execution environment. The table's sampled file totals follow the non-atomic apparent-size method above; they are not exact peak disk or memory requirements. The report retains individual measurements and live/closed/snapshot sizes rather than hiding those distinctions.
