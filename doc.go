// Package rollupdb is a single-file aggregate database. It keeps the
// statistics a stream declares (count, sum, extrema, mean, variance and
// optional KLL quantiles) in time buckets, enforces ingestion grace windows,
// folds fine buckets into coarser tiers and expires them when Advance runs,
// answers queries across the retained resolutions and exports consistent
// snapshots.
package rollupdb

// Version is the RollupDB release version. Raising it on main releases it:
// see RELEASING.md.
const Version = "0.1.0"
