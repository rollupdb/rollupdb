//go:build windows

package rollupdb

// syncDir is a no-op on Windows, which has no way to sync a folder; NTFS
// journals the new name itself.
func syncDir(string) error { return nil }
