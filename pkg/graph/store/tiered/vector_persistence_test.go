package tiered

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	"github.com/vmihailenco/msgpack/v5"
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

	// A definition created via plain CreateVectorIndex carries zero-value
	// (default HNSW) tuning; confirm that specifically survives the reopen
	// rather than just asserting search still resolves.
	gotOpts, ok := ts.VectorIndexOptionsForTest(labelTok, "vec")
	if !ok {
		t.Fatal("VectorIndexOptionsForTest after reopen: index not found")
	}
	if gotOpts != (storecontract.VectorIndexOptions{}) {
		t.Fatalf("VectorIndexOptionsForTest after reopen = %+v, want zero-value default tuning", gotOpts)
	}
}

// TestTieredStore_VectorIndex_NonDefaultOptionsSurviveRestart is the
// non-default-tuning counterpart to
// TestTieredStore_VectorIndex_DefinitionSurvivesRestart: it proves the
// documented contract (tiered persists UseBruteForce/M/EfConstruction/
// EfSearch, not just Dims/Metric, in the vector index definition file — see
// vectorIdxDef in vector_index_file.go and CLAUDE.md "Vector Indexes") by
// creating the index through CreateVectorIndexWithOptions with every tunable
// field set to a non-default, non-zero value, reopening the store, and
// asserting the SAME options come back via VectorIndexOptionsForTest — not
// merely that search still resolves the pre-existing definition.
func TestTieredStore_VectorIndex_NonDefaultOptionsSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	opts := storecontract.VectorIndexOptions{UseBruteForce: true, M: 8, EfConstruction: 50, EfSearch: 10}
	labelTok := createDiskTieredVectorIndexWithOptions(t, dir, opts)

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

	gotOpts, ok := ts.VectorIndexOptionsForTest(labelTok, "vec")
	if !ok {
		t.Fatal("VectorIndexOptionsForTest after reopen: index not found")
	}
	if gotOpts != opts {
		t.Fatalf("VectorIndexOptionsForTest after reopen = %+v, want %+v", gotOpts, opts)
	}

	results, err := ts.SearchNearestNodes(labelTok, "vec", []float32{1, 0, 0}, 1, QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes after reopen: %v", err)
	}
	if len(results) != 1 || results[0].ID() != types.NodeID(snowflake.ID(101)) {
		t.Fatalf("SearchNearestNodes after reopen (brute force tuning) = %#v, want node 101", results)
	}
}

func TestTieredStore_VectorIndex_LoadRejectsIndexedNodeDimensionMismatch(t *testing.T) {
	dir := t.TempDir()
	createDiskTieredVectorIndex(t, dir)
	vectorFile := filepath.Join(dir, "meta", "vector_indexes.msgpack")
	defs, err := loadVectorIndexFile(vectorFile)
	if err != nil {
		t.Fatalf("load vector index file: %v", err)
	}
	if len(defs) != 1 {
		t.Fatalf("vector index definitions = %#v, want one definition", defs)
	}
	defs[0].Dims = 2
	if err := saveVectorIndexFile(vectorFile, defs); err != nil {
		t.Fatalf("write mismatched vector index file: %v", err)
	}

	ts, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if ts != nil {
		_ = ts.Close()
	}
	if err == nil {
		t.Fatal("open with mismatched indexed vector returned nil")
	}
	if !errors.Is(err, ErrDimensionMismatch) {
		t.Fatalf("open with mismatched indexed vector = %v, want ErrDimensionMismatch", err)
	}
	if !strings.Contains(err.Error(), "vector index file: rebuild node ") {
		t.Fatalf("open with mismatched indexed vector = %v, want vector-index rebuild context", err)
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
	writeRawVectorIndexFile(t, filepath.Join(metaDir, "vector_indexes.msgpack"), []vectorIdxDef{
		{LabelToken: 1, PropertyKey: "vec", Dims: 0, Metric: DistanceCosine},
	})

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
	writeRawVectorIndexFile(t, filepath.Join(metaDir, "vector_indexes.msgpack"), []vectorIdxDef{
		{LabelToken: 0, PropertyKey: "vec", Dims: 2, Metric: DistanceCosine},
	})

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
	writeRawVectorIndexFile(t, filepath.Join(metaDir, "vector_indexes.msgpack"), []vectorIdxDef{
		{LabelToken: 1, PropertyKey: "tkg_hash", Dims: 2, Metric: DistanceCosine},
	})

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
	writeRawVectorIndexFile(t, filepath.Join(metaDir, "vector_indexes.msgpack"), []vectorIdxDef{
		{LabelToken: 1, PropertyKey: "vec", Dims: 2, Metric: DistanceCosine},
		{LabelToken: 1, PropertyKey: "vec", Dims: 3, Metric: DistanceCosine},
	})

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

func TestVectorIndexFileSaveRejectsInvalidDefinitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		defs []vectorIdxDef
	}{
		{
			name: "zero label",
			defs: []vectorIdxDef{{LabelToken: 0, PropertyKey: "vec", Dims: 2, Metric: DistanceCosine}},
		},
		{
			name: "reserved property",
			defs: []vectorIdxDef{{LabelToken: 1, PropertyKey: "tkg_hash", Dims: 2, Metric: DistanceCosine}},
		},
		{
			name: "invalid dimensions",
			defs: []vectorIdxDef{{LabelToken: 1, PropertyKey: "vec", Dims: 0, Metric: DistanceCosine}},
		},
		{
			name: "duplicate definition",
			defs: []vectorIdxDef{
				{LabelToken: 1, PropertyKey: "vec", Dims: 2, Metric: DistanceCosine},
				{LabelToken: 1, PropertyKey: "vec", Dims: 2, Metric: DistanceCosine},
			},
		},
		{
			name: "conflicting duplicate definition",
			defs: []vectorIdxDef{
				{LabelToken: 1, PropertyKey: "vec", Dims: 2, Metric: DistanceCosine},
				{LabelToken: 1, PropertyKey: "vec", Dims: 3, Metric: DistanceCosine},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "vector_indexes.msgpack")
			original := []byte("previous vector index bytes")
			if err := atomicWriteFile(path, original, "test vector setup"); err != nil {
				t.Fatalf("write original vector index file: %v", err)
			}

			if err := saveVectorIndexFile(path, tc.defs); err == nil {
				t.Fatal("saveVectorIndexFile returned nil for invalid definitions")
			}
			got, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read vector index file after rejected save: %v", err)
			}
			if !bytes.Equal(got, original) {
				t.Fatal("rejected vector index save changed file bytes")
			}
		})
	}
}

func TestVectorIndexFileLoadRejectsDuplicateDefinition(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "vector_indexes.msgpack")
	writeRawVectorIndexFile(t, path, []vectorIdxDef{
		{LabelToken: 1, PropertyKey: "vec", Dims: 2, Metric: DistanceCosine},
		{LabelToken: 1, PropertyKey: "vec", Dims: 2, Metric: DistanceCosine},
	})

	if _, err := loadVectorIndexFile(path); !errors.Is(err, ErrVectorIndexExists) {
		t.Fatalf("loadVectorIndexFile duplicate definition = %v, want ErrVectorIndexExists", err)
	}
}

func TestTieredStoreVectorIndexLoadRejectsDuplicateDefinition(t *testing.T) {
	dir := t.TempDir()
	metaDir := filepath.Join(dir, "meta")
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		t.Fatalf("mkdir meta: %v", err)
	}
	writeRawVectorIndexFile(t, filepath.Join(metaDir, "vector_indexes.msgpack"), []vectorIdxDef{
		{LabelToken: 1, PropertyKey: "vec", Dims: 2, Metric: DistanceCosine},
		{LabelToken: 1, PropertyKey: "vec", Dims: 2, Metric: DistanceCosine},
	})

	_, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if !errors.Is(err, ErrVectorIndexExists) {
		t.Fatalf("open with duplicate vector index definition = %v, want ErrVectorIndexExists", err)
	}
}

func createDiskTieredVectorIndex(t *testing.T, dir string) uint16 {
	t.Helper()
	return createDiskTieredVectorIndexWithOptions(t, dir, storecontract.VectorIndexOptions{})
}

// createDiskTieredVectorIndexWithOptions is createDiskTieredVectorIndex with
// control over the engine/tuning options passed to
// CreateVectorIndexWithOptions, so restart-persistence tests can verify a
// non-default choice (not just Dims/Metric) survives a reopen.
func createDiskTieredVectorIndexWithOptions(t *testing.T, dir string, opts storecontract.VectorIndexOptions) uint16 {
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
	if err := ts.CreateVectorIndexWithOptions(caseTok, "vec", 3, DistanceCosine, opts); err != nil {
		t.Fatalf("CreateVectorIndexWithOptions: %v", err)
	}
	if err := ts.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}
	return caseTok
}

func writeRawVectorIndexFile(t *testing.T, path string, defs []vectorIdxDef) {
	t.Helper()
	data, err := msgpack.Marshal(defs)
	if err != nil {
		t.Fatalf("marshal raw vector index definitions: %v", err)
	}
	if err := atomicWriteFile(path, data, "test raw vector index file"); err != nil {
		t.Fatalf("write raw vector index file: %v", err)
	}
}
