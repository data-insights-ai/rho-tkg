package tiered

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
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

func TestRegistryFileLoadRejectsInvalidPersistedRegistries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data RegistryFileData
	}{
		{
			name: "labels missing reserved token",
			data: RegistryFileData{
				Labels:   []string{"Case"},
				RelTypes: []string{"", "RELATES_TO"},
			},
		},
		{
			name: "labels duplicate",
			data: RegistryFileData{
				Labels:   []string{"", "Case", "Case"},
				RelTypes: []string{"", "RELATES_TO"},
			},
		},
		{
			name: "labels whitespace",
			data: RegistryFileData{
				Labels:   []string{"", " "},
				RelTypes: []string{"", "RELATES_TO"},
			},
		},
		{
			name: "reltypes missing reserved token",
			data: RegistryFileData{
				Labels:   []string{"", "Case"},
				RelTypes: []string{"RELATES_TO"},
			},
		},
		{
			name: "reltypes duplicate",
			data: RegistryFileData{
				Labels:   []string{"", "Case"},
				RelTypes: []string{"", "RELATES_TO", "RELATES_TO"},
			},
		},
		{
			name: "reltypes whitespace",
			data: RegistryFileData{
				Labels:   []string{"", "Case"},
				RelTypes: []string{"", " "},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "registry.msgpack")
			encoded, err := msgpack.Marshal(&tc.data)
			if err != nil {
				t.Fatalf("marshal test registry: %v", err)
			}
			if err := os.WriteFile(path, encoded, 0o600); err != nil {
				t.Fatalf("write test registry: %v", err)
			}

			if _, _, err := loadRegistryFile(path); err == nil {
				t.Fatal("loadRegistryFile returned nil error for invalid persisted registry")
			}
		})
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

func TestRegistryFileRollbackRestoresPreviousBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.msgpack")
	original := []byte("previous registry bytes")
	if err := atomicWriteFile(path, original, "test registry setup"); err != nil {
		t.Fatalf("write original registry file: %v", err)
	}
	snapshot, err := snapshotRegistryFile(path)
	if err != nil {
		t.Fatalf("snapshotRegistryFile: %v", err)
	}
	if err := atomicWriteFile(path, []byte("changed registry bytes"), "test registry overwrite"); err != nil {
		t.Fatalf("write changed registry file: %v", err)
	}

	if err := restoreRegistryFile(snapshot); err != nil {
		t.Fatalf("restoreRegistryFile: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read restored registry file: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("restored registry bytes = %q, want %q", got, original)
	}
}

func TestRegistryFileRollbackRemovesNewFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.msgpack")
	snapshot, err := snapshotRegistryFile(path)
	if err != nil {
		t.Fatalf("snapshotRegistryFile: %v", err)
	}
	if err := atomicWriteFile(path, []byte("new registry bytes"), "test registry create"); err != nil {
		t.Fatalf("write new registry file: %v", err)
	}

	if err := restoreRegistryFile(snapshot); err != nil {
		t.Fatalf("restoreRegistryFile: %v", err)
	}
	if _, err := os.ReadFile(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("registry file after rollback error = %v, want os.ErrNotExist", err)
	}
}

func TestTieredStoreDeprecatedRegistrySavesRejectInvalidExistingOtherHalf(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data RegistryFileData
		run  func(*Store) error
	}{
		{
			name: "save labels rejects invalid existing reltypes",
			data: RegistryFileData{
				Labels:   []string{"", "ExistingLabel"},
				RelTypes: []string{"", "RELATES_TO", "RELATES_TO"},
			},
			run: func(ts *Store) error {
				reg := registrypkg.NewLabelRegistry()
				if _, err := reg.GetOrCreate("Case"); err != nil {
					return err
				}
				return ts.SaveLabelRegistry(reg)
			},
		},
		{
			name: "save reltypes rejects invalid existing labels",
			data: RegistryFileData{
				Labels:   []string{"", "Case", "Case"},
				RelTypes: []string{"", "EXISTING_REL"},
			},
			run: func(ts *Store) error {
				reg := registrypkg.NewRelTypeRegistry()
				if _, err := reg.GetOrCreate("RELATES_TO"); err != nil {
					return err
				}
				return ts.SaveRelTypeRegistry(reg)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ts := newDiskTestTieredStore(t)
			encoded, err := msgpack.Marshal(&tc.data)
			if err != nil {
				t.Fatalf("marshal test registry: %v", err)
			}
			if err := atomicWriteFile(ts.regFile, encoded, "test registry setup"); err != nil {
				t.Fatalf("write test registry: %v", err)
			}

			if err := tc.run(ts); err == nil {
				t.Fatal("deprecated registry save returned nil error for invalid existing other half")
			}
			got, err := os.ReadFile(ts.regFile)
			if err != nil {
				t.Fatalf("read registry file after rejected save: %v", err)
			}
			if !bytes.Equal(got, encoded) {
				t.Fatal("rejected registry save changed registry file bytes")
			}
		})
	}
}
