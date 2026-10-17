// Package rollupdb provides the storage foundation for a single-file aggregate
// database. It persists exact aggregate state, enforces ingestion grace windows,
// queries exact statistics and KLL quantiles from a single tier, and exports
// consistent snapshots. Automatic multi-tier rollups are not implemented yet.
package rollupdb
