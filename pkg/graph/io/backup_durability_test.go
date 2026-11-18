package io

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// BackupTo/BackupDeltaTo fsync'd the STAGED FILE (tmp.Sync())
// before publishing it, but renameNoClobber's publish step (os.Link the
// final name, os.Remove the temp name — both directory-entry mutations in
// the SAME containing directory) was never followed by a directory fsync.
// fsync on a file only guarantees the file's DATA is durable; the directory
// ENTRY that makes the data reachable under its final name is a SEPARATE
// durability domain on POSIX filesystems that don't implicitly journal
// directory metadata alongside file data — a crash immediately after a
// successful BackupTo/BackupDeltaTo return could lose the publish itself.
// fsyncDir closes that gap; these tests exercise it directly since
// crash-durability itself isn't observable in a unit test — the standard
// substitute is proving the helper is correctly wired for both its success
// and error paths, and relying on the untouched TestBackupTo_*/
// TestBackupDeltaTo_* end-to-end batteries (io_test package) to catch any
// wiring regression in renameNoClobber's unconditional call to it.

func TestFsyncDir_ExistingDirSucceeds(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := fsyncDir(dir); err != nil {
		t.Fatalf("fsyncDir(%q) = %v, want nil — regression", dir, err)
	}
}

func TestFsyncDir_NonexistentDirReturnsError(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "does-not-exist")
	err := fsyncDir(dir)
	if err == nil {
		t.Fatal("fsyncDir(nonexistent) = nil, want an error")
	}
	if errors.Is(err, ErrBackupExists) {
		t.Fatalf("fsyncDir(nonexistent) error = %v, want a plain os error, not ErrBackupExists", err)
	}
}

// TestRenameNoClobber_PublishesAndSyncsDir: on an ordinary writable directory
// renameNoClobber (called by both BackupTo and BackupDeltaTo) returns nil with
// the final name present and the temp name gone, so its directory fsync does
// not fail the publish. It cannot see whether fsyncDir ran: fsyncDir is a
// plain function with no seam, and a removed call would still pass here.
func TestRenameNoClobber_PublishesAndSyncsDir(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	tmpPath := filepath.Join(dir, "staged.tmp")
	if err := os.WriteFile(tmpPath, []byte("backup content"), 0o600); err != nil {
		t.Fatalf("stage file: %v", err)
	}
	finalPath := filepath.Join(dir, "backup-final.tkg")

	if err := renameNoClobber(tmpPath, finalPath); err != nil {
		t.Fatalf("renameNoClobber: %v — regression (dir fsync must not fail on an ordinary writable temp dir)", err)
	}
	if _, err := os.Stat(finalPath); err != nil {
		t.Fatalf("renameNoClobber: %s does not exist after a nil-error return: %v", finalPath, err)
	}
	if _, err := os.Stat(tmpPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("renameNoClobber: %s (temp name) still exists after publish", tmpPath)
	}
}
