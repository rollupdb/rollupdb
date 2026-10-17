// Package rollupdb provides the storage foundation for a single-file aggregate
// database. It persists exact aggregate state, enforces ingestion grace windows,
// queries a single tier, and exports consistent snapshots. Percentile sketches
// and automatic multi-tier rollups are not implemented yet.
package rollupdb
