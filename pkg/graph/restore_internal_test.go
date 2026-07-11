package graph

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

// TestSafeBackupPath_RejectsEscapes is the adversarial containment test for
// the helper scanBackupDir uses to build fullPath/deltaPaths before any
// caller opens them (gosec G304 remediation): a crafted directory-entry
// name must never be able to resolve outside dir, whether via an embedded
// path separator or a ".." traversal component, on either separator style.
func TestSafeBackupPath_RejectsEscapes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	adversarial := []string{
		"../outside.tkg",
		"../../etc/passwd",
		"a/b",
		"sub/backup-00000000000000000001-full.tkg",
		`a\b`,
		"..",
		"/etc/passwd",
		"",
	}
	for _, name := range adversarial {
		if _, err := safeBackupPath(dir, name); err == nil {
			t.Fatalf("safeBackupPath(%q) = nil error, want rejection", name)
		}
	}
}

// TestSafeBackupPath_AcceptsLegitEntryNames proves the guard does not reject
// the real filenames scanBackupDir actually deals with, and that the
// returned path is the same dir-joined path the pre-fix code produced (no
// behavior change for the legitimate case).
func TestSafeBackupPath_AcceptsLegitEntryNames(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	legit := []string{
		"backup-00000000000000000001-full.tkg",
		"backup-00000000000000000001-to-00000000000000000002-delta.tkg",
	}
	for _, name := range legit {
		got, err := safeBackupPath(dir, name)
		if err != nil {
			t.Fatalf("safeBackupPath(%q) = %v, want no error", name, err)
		}
		want := filepath.Join(dir, name)
		if got != want {
			t.Fatalf("safeBackupPath(%q) = %q, want %q", name, got, want)
		}
	}
}

// TestSafeBackupPath_ErrorIsWrappedAndNamesTheEntry keeps the error message
// diagnosable — restore's chain-validation errors are matched by substring
// in restore_test.go, so a rejection here must still name the offending
// entry.
func TestSafeBackupPath_ErrorIsWrappedAndNamesTheEntry(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	_, err := safeBackupPath(dir, "../escape.tkg")
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "escape.tkg") {
		t.Fatalf("error %q does not name the offending entry", err.Error())
	}
	if !errors.Is(err, errUnsafeBackupEntryName) {
		t.Fatalf("error %v does not wrap errUnsafeBackupEntryName", err)
	}
}
