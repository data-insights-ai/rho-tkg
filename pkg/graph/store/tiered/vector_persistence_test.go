package tiered

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestTieredStore_VectorIndex_DefinitionSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	labelTok := createDiskTieredVectorIndex(t, dir)

	ts, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer ts.Close()

	results, err := ts.SearchNearestNodes(labelTok, "vec", []float32{1, 0, 0}, 1, QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes after reopen: %v", err)
	}
	if len(results) != 1 || results[0].ID() != types.NodeID(snowflake.ID(101)) {
		t.Fatalf("SearchNearestNodes after reopen = %#v, want node 101", results)
	}
}

func TestTieredStore_VectorIndex_DropDefinitionSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	labelTok := createDiskTieredVectorIndex(t, dir)

	ts1, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	if err := ts1.DropVectorIndex(labelTok, "vec"); err != nil {
		t.Fatalf("DropVectorIndex: %v", err)
	}
	if err := ts1.Close(); err != nil {
		t.Fatalf("close 2: %v", err)
	}

	ts2, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("open 3: %v", err)
	}
	defer ts2.Close()

	_, err = ts2.SearchNearestNodes(labelTok, "vec", []float32{1, 0, 0}, 1, QueryOpts{})
	if !errors.Is(err, ErrVectorIndexNotFound) {
		t.Fatalf("SearchNearestNodes after dropped reopen = %v, want ErrVectorIndexNotFound", err)
	}
}

func TestVectorIndexFileRollbackRestoresPreviousBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vector_indexes.msgpack")
	original := []byte("previous vector index bytes")
	if err := atomicWriteFile(path, original, "test vector setup"); err != nil {
		t.Fatalf("write original vector index file: %v", err)
	}
	snapshot, err := snapshotVectorIndexFile(path)
	if err != nil {
		t.Fatalf("snapshotVectorIndexFile: %v", err)
	}
	if err := atomicWriteFile(path, []byte("changed vector index bytes"), "test vector overwrite"); err != nil {
		t.Fatalf("write changed vector index file: %v", err)
	}

	if err := restoreVectorIndexFile(snapshot); err != nil {
		t.Fatalf("restoreVectorIndexFile: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read restored vector index file: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("restored vector index bytes = %q, want %q", got, original)
	}
}

func TestVectorIndexFileRollbackRemovesNewFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vector_indexes.msgpack")
	snapshot, err := snapshotVectorIndexFile(path)
	if err != nil {
		t.Fatalf("snapshotVectorIndexFile: %v", err)
	}
	if err := atomicWriteFile(path, []byte("new vector index bytes"), "test vector create"); err != nil {
		t.Fatalf("write new vector index file: %v", err)
	}

	if err := restoreVectorIndexFile(snapshot); err != nil {
		t.Fatalf("restoreVectorIndexFile: %v", err)
	}
	if _, err := os.ReadFile(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("vector index file after rollback error = %v, want os.ErrNotExist", err)
	}
}

func TestTieredStore_VectorIndex_ClearRemovesPersistedDefinition(t *testing.T) {
	dir := t.TempDir()
	labelTok := createDiskTieredVectorIndex(t, dir)

	ts1, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	if err := ts1.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if err := ts1.Close(); err != nil {
		t.Fatalf("close 2: %v", err)
	}

	ts2, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("open 3: %v", err)
	}
	defer ts2.Close()

	_, err = ts2.SearchNearestNodes(labelTok, "vec", []float32{1, 0, 0}, 1, QueryOpts{})
	if !errors.Is(err, ErrVectorIndexNotFound) {
		t.Fatalf("SearchNearestNodes after cleared reopen = %v, want ErrVectorIndexNotFound", err)
	}
}

func TestTieredStore_VectorIndex_PersistSkipsBuildingDefinition(t *testing.T) {
	dir := t.TempDir()
	ts, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer ts.Close()

	readyKey := indexpkg.VectorIndexKey{LabelToken: 1, PropertyKey: "ready_vec"}
	buildingKey := indexpkg.VectorIndexKey{LabelToken: 2, PropertyKey: "building_vec"}

	ts.vectorIdxMu.Lock()
	ts.vectorIndexes[readyKey] = &indexpkg.VectorIndex{Dims: 2, Metric: DistanceCosine}
	ts.vectorIndexes[buildingKey] = &indexpkg.VectorIndex{
		Dims:    2,
		Metric:  DistanceCosine,
		Mutated: make(map[snowflake.ID]struct{}),
	}
	err = ts.persistVectorIndexDefsLocked()
	ts.vectorIdxMu.Unlock()
	if err != nil {
		t.Fatalf("persistVectorIndexDefsLocked: %v", err)
	}

	defs, err := loadVectorIndexFile(ts.vectorIdxFile)
	if err != nil {
		t.Fatalf("loadVectorIndexFile: %v", err)
	}
	if len(defs) != 1 || defs[0].LabelToken != readyKey.LabelToken || defs[0].PropertyKey != readyKey.PropertyKey {
		t.Fatalf("persisted vector defs = %#v, want only ready definition", defs)
	}
}

func TestTieredStore_VectorIndex_LoadRejectsInvalidDefinition(t *testing.T) {
	dir := t.TempDir()
	metaDir := filepath.Join(dir, "meta")
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		t.Fatalf("mkdir meta: %v", err)
	}
	if err := saveVectorIndexFile(filepath.Join(metaDir, "vector_indexes.msgpack"), []vectorIdxDef{
		{LabelToken: 1, PropertyKey: "vec", Dims: 0, Metric: DistanceCosine},
	}); err != nil {
		t.Fatalf("write invalid vector index file: %v", err)
	}

	_, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if !errors.Is(err, ErrInvalidVectorIndexConfig) {
		t.Fatalf("open with invalid vector index definition = %v, want ErrInvalidVectorIndexConfig", err)
	}
}

func TestTieredStore_VectorIndex_LoadRejectsZeroLabelDefinition(t *testing.T) {
	dir := t.TempDir()
	metaDir := filepath.Join(dir, "meta")
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		t.Fatalf("mkdir meta: %v", err)
	}
	if err := saveVectorIndexFile(filepath.Join(metaDir, "vector_indexes.msgpack"), []vectorIdxDef{
		{LabelToken: 0, PropertyKey: "vec", Dims: 2, Metric: DistanceCosine},
	}); err != nil {
		t.Fatalf("write zero-label vector index file: %v", err)
	}

	_, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("open with zero-label vector index definition = %v, want ErrInvalidStoreMutation", err)
	}
}

func TestTieredStore_VectorIndex_LoadRejectsReservedPropertyDefinition(t *testing.T) {
	dir := t.TempDir()
	metaDir := filepath.Join(dir, "meta")
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		t.Fatalf("mkdir meta: %v", err)
	}
	if err := saveVectorIndexFile(filepath.Join(metaDir, "vector_indexes.msgpack"), []vectorIdxDef{
		{LabelToken: 1, PropertyKey: "tkg_hash", Dims: 2, Metric: DistanceCosine},
	}); err != nil {
		t.Fatalf("write reserved-key vector index file: %v", err)
	}

	_, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if !errors.Is(err, types.ErrReservedPrefix) {
		t.Fatalf("open with reserved-key vector index definition = %v, want ErrReservedPrefix", err)
	}
}

func TestTieredStore_VectorIndex_LoadRejectsConflictingDuplicateDefinition(t *testing.T) {
	dir := t.TempDir()
	metaDir := filepath.Join(dir, "meta")
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		t.Fatalf("mkdir meta: %v", err)
	}
	if err := saveVectorIndexFile(filepath.Join(metaDir, "vector_indexes.msgpack"), []vectorIdxDef{
		{LabelToken: 1, PropertyKey: "vec", Dims: 2, Metric: DistanceCosine},
		{LabelToken: 1, PropertyKey: "vec", Dims: 3, Metric: DistanceCosine},
	}); err != nil {
		t.Fatalf("write duplicate vector index file: %v", err)
	}

	_, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if !errors.Is(err, ErrVectorIndexExists) {
		t.Fatalf("open with conflicting duplicate vector index definition = %v, want ErrVectorIndexExists", err)
	}
}

func createDiskTieredVectorIndex(t *testing.T, dir string) uint16 {
	t.Helper()
	ts, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")

	n1 := types.NewNode(types.NodeID(snowflake.ID(101)), caseTok, nil)
	ps1, _ := types.NewPropertySlice(map[string]any{"vec": []float32{1, 0, 0}})
	n1.SetProperties(ps1)
	if err := ts.PutNode(n1); err != nil {
		t.Fatalf("PutNode 1: %v", err)
	}
	n2 := types.NewNode(types.NodeID(snowflake.ID(102)), caseTok, nil)
	ps2, _ := types.NewPropertySlice(map[string]any{"vec": []float32{0, 1, 0}})
	n2.SetProperties(ps2)
	if err := ts.PutNode(n2); err != nil {
		t.Fatalf("PutNode 2: %v", err)
	}
	if err := ts.CreateVectorIndex(caseTok, "vec", 3, DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}
	if err := ts.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}
	return caseTok
}
