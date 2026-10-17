# SQL

`db.SQL(ctx, statement)`, `rollupdb sql FILE STATEMENT` (or `-` to read the statement from stdin) and `POST /v1/sql` with `{"sql": "..."}` run one read-only SQL statement over the retained buckets. The answer is `{"columns": [...], "rows": [[...]], "truncated": false}`, with at most 10,000 rows; `truncated` says when there were more.

## Views

Every stream is a view named after it, with one row per retained bucket per dimension group:

| Column | Meaning |
|---|---|
| `start`, `end` | The bucket's interval in UTC, as RFC 3339 text, so string comparison orders them |
| `width_seconds`, `tier` | The bucket's width and its tier number |
| one per dimension | Named after the dimension; `dim_` is added in front if the name clashes with a column here |
| `count`, `sum`, `min`, `max`, `mean` | The bucket's exact state |
| `m2` | Sum of squared differences from the mean, for merging variance |
| `sketch` | The bucket's KLL sketch, or NULL for exact-only streams and buckets from before a sketch was added |

Retained buckets never overlap: a rollup removes the buckets it merges in the same transaction. So `sum(count)`, `sum(sum)`, `min(min)` and `max(max)` are correct over any set of rows, and the mean is `sum(sum) / sum(count)`. Averaging `mean` is not.

## Aggregates

| Function | Result |
|---|---|
| `rollup_variance(count, mean, m2)` | Population variance of all readings in the rows |
| `rollup_variance_sample(count, mean, m2)` | Sample variance |
| `rollup_quantile(sketch, rank)` | The percentile at `rank` (0 to 1) of the merged sketches; NULL if any row has no sketch. The rank must be the same on every row |

```sql
SELECT service,
       substr(start, 1, 10) AS day,
       sum(count) AS readings,
       sum(sum) / sum(count) AS mean_ms,
       max(max) AS worst_ms,
       rollup_quantile(sketch, 0.99) AS p99_ms
FROM latency
WHERE start >= '2026-10-01T00:00:00Z'
GROUP BY service, day
ORDER BY service, day;
```

Grouping by a period finer than the buckets in range repeats coarse buckets under their start time; group by periods at least as wide as the widest bucket you select, or filter on `width_seconds`.

## What SQL Doesn't Do

The statement sees what's retained now. It doesn't report coverage, missing intervals or expired history the way `Query` does, and it doesn't refuse a range that cuts a coarse bucket: the bucket is either in the `WHERE` or not. Use `Query` when the answer needs to say what it covers.

## Read-Only

Only one statement is accepted, and it must start with `SELECT`, `WITH` or `VALUES`; `PRAGMA`, `ATTACH`, `VACUUM` and anything after a second `;` are refused. The statement runs with the connection in SQLite's `query_only` mode inside a transaction that is always rolled back, so a `WITH ... DELETE` fails too. Refused or failing statements return `ErrSQL` (HTTP 400, code `sql`) and change nothing. The statement holds the database handle while it runs, like any other call.
