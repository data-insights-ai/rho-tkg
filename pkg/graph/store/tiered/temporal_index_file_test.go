package tiered

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func TestTemporalIndexFileRollbackRestoresPreviousBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "temporal_indexes.msgpack")
	original := []byte("previous temporal index bytes")
	if err := atomicWriteFile(path, original, "test temporal setup"); err != nil {
		t.Fatalf("write original temporal index file: %v", err)
	}
	snapshot, err := snapshotTemporalIndexFile(path)
	if err != nil {
		t.Fatalf("snapshotTemporalIndexFile: %v", err)
	}
	if err := atomicWriteFile(path, []byte("changed temporal index bytes"), "test temporal overwrite"); err != nil {
		t.Fatalf("write changed temporal index file: %v", err)
	}

	if err := restoreTemporalIndexFile(snapshot); err != nil {
		t.Fatalf("restoreTemporalIndexFile: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read restored temporal index file: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("restored temporal index bytes = %q, want %q", got, original)
	}
}

func TestTemporalIndexFileRollbackRemovesNewFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "temporal_indexes.msgpack")
	snapshot, err := snapshotTemporalIndexFile(path)
	if err != nil {
		t.Fatalf("snapshotTemporalIndexFile: %v", err)
	}
	if err := atomicWriteFile(path, []byte("new temporal index bytes"), "test temporal create"); err != nil {
		t.Fatalf("write new temporal index file: %v", err)
	}

	if err := restoreTemporalIndexFile(snapshot); err != nil {
		t.Fatalf("restoreTemporalIndexFile: %v", err)
	}
	if _, err := os.ReadFile(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporal index file after rollback error = %v, want os.ErrNotExist", err)
	}
}

func TestTemporalIndexFileSaveRejectsInvalidDefinitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data temporalIndexFileData
	}{
		{
			name: "zero temporal label",
			data: temporalIndexFileData{TemporalLabels: []uint16{0}},
		},
		{
			name: "duplicate temporal label",
			data: temporalIndexFileData{TemporalLabels: []uint16{1, 1}},
		},
		{
			name: "temporal and high-frequency conflict",
			data: temporalIndexFileData{
				TemporalLabels: []uint16{1},
				HighFrequency:  []tieredHFIdef{{LabelToken: 1, BucketSizeMillis: 3_600_000}},
			},
		},
		{
			name: "invalid high-frequency bucket",
			data: temporalIndexFileData{
				HighFrequency: []tieredHFIdef{{LabelToken: 1, BucketSizeMillis: 0}},
			},
		},
		{
			name: "duplicate high-frequency label",
			data: temporalIndexFileData{
				HighFrequency: []tieredHFIdef{
					{LabelToken: 1, BucketSizeMillis: 3_600_000},
					{LabelToken: 1, BucketSizeMillis: 3_600_000},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "temporal_indexes.msgpack")
			original := []byte("previous temporal index bytes")
			if err := atomicWriteFile(path, original, "test temporal setup"); err != nil {
				t.Fatalf("write original temporal index file: %v", err)
			}

			if err := saveTemporalIndexFile(path, tc.data); err == nil {
				t.Fatal("saveTemporalIndexFile returned nil for invalid definitions")
			}
			got, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read temporal index file after rejected save: %v", err)
			}
			if !bytes.Equal(got, original) {
				t.Fatal("rejected temporal index save changed file bytes")
			}
		})
	}
}

func TestTemporalIndexFileLoadRejectsInvalidDefinitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data temporalIndexFileData
	}{
		{
			name: "duplicate temporal label",
			data: temporalIndexFileData{TemporalLabels: []uint16{1, 1}},
		},
		{
			name: "temporal and high-frequency conflict",
			data: temporalIndexFileData{
				TemporalLabels: []uint16{1},
				HighFrequency:  []tieredHFIdef{{LabelToken: 1, BucketSizeMillis: 3_600_000}},
			},
		},
		{
			name: "duplicate high-frequency label",
			data: temporalIndexFileData{
				HighFrequency: []tieredHFIdef{
					{LabelToken: 1, BucketSizeMillis: 3_600_000},
					{LabelToken: 1, BucketSizeMillis: 3_600_000},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "temporal_indexes.msgpack")
			encoded, err := msgpack.Marshal(&tc.data)
			if err != nil {
				t.Fatalf("marshal temporal index file: %v", err)
			}
			if err := os.WriteFile(path, encoded, 0o600); err != nil {
				t.Fatalf("write temporal index file: %v", err)
			}

			if _, err := loadTemporalIndexFile(path); err == nil {
				t.Fatal("loadTemporalIndexFile returned nil for invalid definitions")
			}
		})
	}
}
