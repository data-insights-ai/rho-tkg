package tieredstore

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRegistryFile_SaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "registry.msgpack")

	labels := []string{"", "Case", "Signal", "User"}
	relTypes := []string{"", "RELATES_TO", "BELONGS_TO"}

	if err := saveRegistryFile(path, labels, relTypes); err != nil {
		t.Fatalf("save: %v", err)
	}

	gotLabels, gotRelTypes, err := loadRegistryFile(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if len(gotLabels) != len(labels) {
		t.Fatalf("labels len = %d, want %d", len(gotLabels), len(labels))
	}
	for i, l := range gotLabels {
		if l != labels[i] {
			t.Errorf("labels[%d] = %q, want %q", i, l, labels[i])
		}
	}

	if len(gotRelTypes) != len(relTypes) {
		t.Fatalf("relTypes len = %d, want %d", len(gotRelTypes), len(relTypes))
	}
	for i, rt := range gotRelTypes {
		if rt != relTypes[i] {
			t.Errorf("relTypes[%d] = %q, want %q", i, rt, relTypes[i])
		}
	}
}

func TestRegistryFile_LoadMissing(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nonexistent.msgpack")

	labels, relTypes, err := loadRegistryFile(path)
	if err != nil {
		t.Fatalf("load missing: %v", err)
	}
	if labels != nil || relTypes != nil {
		t.Errorf("load missing: got labels=%v relTypes=%v, want nil nil", labels, relTypes)
	}
}

func TestRegistryFile_EmptyRegistries(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "registry.msgpack")

	if err := saveRegistryFile(path, nil, nil); err != nil {
		t.Fatalf("save empty: %v", err)
	}

	labels, relTypes, err := loadRegistryFile(path)
	if err != nil {
		t.Fatalf("load empty: %v", err)
	}
	if len(labels) != 0 || len(relTypes) != 0 {
		t.Errorf("empty round-trip: labels=%v relTypes=%v", labels, relTypes)
	}
}

func TestRegistryFile_AtomicRename(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "registry.msgpack")

	if err := saveRegistryFile(path, []string{"", "A"}, []string{"", "B"}); err != nil {
		t.Fatalf("save: %v", err)
	}

	// Verify no tmp files left behind.
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if e.Name() != "registry.msgpack" {
			t.Errorf("unexpected file: %s", e.Name())
		}
	}
}
