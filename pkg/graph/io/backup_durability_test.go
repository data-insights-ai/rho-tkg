package io

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// BACKLOG 8d: BackupTo/BackupDeltaTo fsync'd the STAGED FILE (tmp.Sync())
// before publishing it, but renameNoClobber's publish step (os.Link the
// final name, os.Remove the temp name — both directory-entry mutations in
// the SAME containing directory) was never followed by a directory fsync.
// fsync on a file only guarantees the file's DATA is durable; the directory
// ENTRY that makes the data reachable under its final name is a SEPARATE
// durability domain on POSIX filesystems that don't implicitly journal
// directory metadata alongside file data — a crash immediately after a
// successful BackupTo/BackupDeltaTo return could lose the publish itself.
// fsyncDir (new) closes that gap; these tests exercise it directly since
// crash-durability itself isn't observable in a unit test — the standard
// substitute is proving the helper is correctly wired for both its success
// and error paths, and relying on the untouched TestBackupTo_*/
// TestBackupDeltaTo_* end-to-end batteries (io_test package) to catch any
// wiring regression in renameNoClobber's unconditional call to it.

func TestFsyncDir_ExistingDirSucceeds(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := fsyncDir(dir); err != nil {
		t.Fatalf("fsyncDir(%q) = %v, want nil — BACKLOG 8d regression", dir, err)
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

// TestRenameNoClobber_PublishesAndSyncsDir is the direct proof renameNoClobber
// (called by both BackupTo and BackupDeltaTo) reaches the new fsyncDir step
// on its normal success path, not just the pre-existing Link/Remove steps.
func TestRenameNoClobber_PublishesAndSyncsDir(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	tmpPath := filepath.Join(dir, "staged.tmp")
	if err := os.WriteFile(tmpPath, []byte("backup content"), 0o600); err != nil {
		t.Fatalf("stage file: %v", err)
	}
	finalPath := filepath.Join(dir, "backup-final.tkg")

	if err := renameNoClobber(tmpPath, finalPath); err != nil {
		t.Fatalf("renameNoClobber: %v — BACKLOG 8d regression (dir fsync must not fail on an ordinary writable temp dir)", err)
	}
	if _, err := os.Stat(finalPath); err != nil {
		t.Fatalf("renameNoClobber: %s does not exist after a nil-error return: %v", finalPath, err)
	}
	if _, err := os.Stat(tmpPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("renameNoClobber: %s (temp name) still exists after publish", tmpPath)
	}
}
