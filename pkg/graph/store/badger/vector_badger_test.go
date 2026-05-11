// Internal-package tests for Store implementations of
// RemoveNodeLabelToken, CreateVectorIndex, DropVectorIndex, SearchNearestNodes.
package badger

import (
	"errors"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	storeutil "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Store: RemoveNodeLabelToken ---

func TestBadgerStore_RemoveNodeLabelToken_Basic(t *testing.T) {
	bs := newTestBadgerStore(t)

	primary := uint16(1)
	extra := uint16(2)
	n := types.NewNode(types.NodeID(snowflake.ID(100)), primary, []uint16{extra})
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	id := n.ID()

	// Simulate RemoveLabelTokenRaw on a copy.
	copy := n.DeepCopy()
	copy.RemoveLabelTokenRaw(extra)

	if err := bs.RemoveNodeLabelToken(id, extra, copy); err != nil {
		t.Fatalf("RemoveNodeLabelToken: %v", err)
	}

	// Verify label index no longer contains id under extra token.
	set, hasSet := bs.LabelIndexForTest(extra)
	if hasSet {
		for _, sid := range set {
			if sid == id {
				t.Error("node still in label index after RemoveNodeLabelToken")
			}
		}
	}
}

func TestBadgerStore_RemoveNodeLabelToken_NotFound(t *testing.T) {
	bs := newTestBadgerStore(t)

	copy := types.NewNode(types.NodeID(snowflake.ID(999)), 1, nil)
	err := bs.RemoveNodeLabelToken(types.NodeID(999), 1, copy)
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("expected ErrNodeNotFound, got %v", err)
	}
}

// --- Store: CreateVectorIndex / DropVectorIndex / SearchNearestNodes ---

func TestBadgerStore_VectorIndex_CreateAndSearch(t *testing.T) {
	bs := newTestBadgerStore(t)
	labelTok := uint16(3)
	key := "vec"

	// Put two nodes with float32 vector properties.
	n1 := types.NewNode(types.NodeID(snowflake.ID(101)), labelTok, nil)
	ps1, _ := types.NewPropertySlice(map[string]any{key: []float32{1, 0, 0}})
	n1.SetProperties(ps1)
	bs.PutNode(n1)

	n2 := types.NewNode(types.NodeID(snowflake.ID(102)), labelTok, nil)
	ps2, _ := types.NewPropertySlice(map[string]any{key: []float32{0, 1, 0}})
	n2.SetProperties(ps2)
	bs.PutNode(n2)

	if err := bs.CreateVectorIndex(labelTok, key, 3, DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	results, err := bs.SearchNearestNodes(labelTok, key, []float32{1, 0, 0}, 2, QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected results, got none")
	}
	if results[0].ID() != n1.ID() {
		t.Error("expected n1 as closest to [1,0,0]")
	}
}

func TestBadgerStoreSearchNearestNodesAppliesCursorPagination(t *testing.T) {
	bs := newTestBadgerStore(t)
	labelTok := uint16(3)
	key := "vec"
	nodes := []*types.Node{
		types.NewNode(types.NodeID(snowflake.ID(101)), labelTok, nil),
		types.NewNode(types.NodeID(snowflake.ID(102)), labelTok, nil),
		types.NewNode(types.NodeID(snowflake.ID(103)), labelTok, nil),
	}
	vectors := [][]float32{{0, 0}, {1, 0}, {2, 0}}
	for i, n := range nodes {
		if err := n.SetProperty(key, vectors[i]); err != nil {
			t.Fatalf("SetProperty[%d]: %v", i, err)
		}
		if err := bs.PutNode(n); err != nil {
			t.Fatalf("PutNode[%d]: %v", i, err)
		}
	}
	if err := bs.CreateVectorIndex(labelTok, key, 2, DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	limited, err := bs.SearchNearestNodes(labelTok, key, []float32{0, 0}, 3, QueryOpts{Limit: 2})
	if err != nil {
		t.Fatalf("SearchNearestNodes limit: %v", err)
	}
	if len(limited) != 2 || limited[0].ID() != nodes[0].ID() || limited[1].ID() != nodes[1].ID() {
		t.Fatalf("limited result IDs = %v, want first two distance-ordered nodes", badgerNodeIDsForTest(limited))
	}

	afterFirst, err := bs.SearchNearestNodes(labelTok, key, []float32{0, 0}, 3, QueryOpts{
		After: types.EntityID(nodes[0].ID().SnowflakeID()),
		Limit: 1,
	})
	if err != nil {
		t.Fatalf("SearchNearestNodes after+limit: %v", err)
	}
	if len(afterFirst) != 1 || afterFirst[0].ID() != nodes[1].ID() {
		t.Fatalf("after+limit result IDs = %v, want second node", badgerNodeIDsForTest(afterFirst))
	}

	missingCursor, err := bs.SearchNearestNodes(labelTok, key, []float32{0, 0}, 3, QueryOpts{
		After: types.EntityID(snowflake.ID(999)),
	})
	if err != nil {
		t.Fatalf("SearchNearestNodes missing cursor: %v", err)
	}
	if len(missingCursor) != 0 {
		t.Fatalf("missing cursor result IDs = %v, want none", badgerNodeIDsForTest(missingCursor))
	}
}

func TestBadgerStoreSearchNearestNodesAppliesTemporalFilterBeforeHeap(t *testing.T) {
	bs := newTestBadgerStore(t)
	labelTok := uint16(3)
	key := "vec"
	tooNew := types.NewNode(types.NodeID(snowflake.ID(101)), labelTok, nil)
	tooNew.SetTemporal(&types.TemporalMetadata{ValidFrom: 100})
	eligible := types.NewNode(types.NodeID(snowflake.ID(102)), labelTok, nil)
	eligible.SetTemporal(&types.TemporalMetadata{ValidFrom: 1})

	for i, n := range []*types.Node{tooNew, eligible} {
		vec := []float32{float32(i * 10), 0}
		if err := n.SetProperty(key, vec); err != nil {
			t.Fatalf("SetProperty[%d]: %v", i, err)
		}
		if err := bs.PutNode(n); err != nil {
			t.Fatalf("PutNode[%d]: %v", i, err)
		}
	}
	if err := bs.CreateVectorIndex(labelTok, key, 2, DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	got, err := bs.SearchNearestNodes(labelTok, key, []float32{0, 0}, 1, QueryOpts{ValidAt: 50})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(got) != 1 || got[0].ID() != eligible.ID() {
		t.Fatalf("temporal result IDs = %v, want eligible farther node %d", badgerNodeIDsForTest(got), eligible.ID())
	}
}

func TestBadgerStoreQueryAPIsRejectInvalidDepth(t *testing.T) {
	bs := newTestBadgerStore(t)
	badOpts := QueryOpts{Depth: storepkg.ShardDepth(99)}
	cases := []struct {
		name string
		run  func() error
	}{
		{name: "NodesByLabel", run: func() error {
			_, err := bs.NodesByLabel(1, badOpts)
			return err
		}},
		{name: "RelationshipsByType", run: func() error {
			_, err := bs.RelationshipsByType(1, badOpts)
			return err
		}},
		{name: "AllNodeIDs", run: func() error {
			_, err := bs.AllNodeIDs(badOpts)
			return err
		}},
		{name: "AllRelIDs", run: func() error {
			_, err := bs.AllRelIDs(badOpts)
			return err
		}},
		{name: "AllNodes", run: func() error {
			_, err := bs.AllNodes(badOpts)
			return err
		}},
		{name: "AllRelationships", run: func() error {
			_, err := bs.AllRelationships(badOpts)
			return err
		}},
		{name: "NodesByLabelAndProperty", run: func() error {
			_, err := bs.NodesByLabelAndProperty(1, "name", "alice", badOpts)
			return err
		}},
		{name: "SearchNearestNodes", run: func() error {
			_, err := bs.SearchNearestNodes(1, "vec", []float32{1, 0}, 1, badOpts)
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.run(); !errors.Is(err, storepkg.ErrInvalidShardDepth) {
				t.Fatalf("%s err = %v, want ErrInvalidShardDepth", tc.name, err)
			}
		})
	}
}

func badgerNodeIDsForTest(nodes []*types.Node) []types.NodeID {
	ids := make([]types.NodeID, len(nodes))
	for i, n := range nodes {
		ids[i] = n.ID()
	}
	return ids
}

func TestBadgerStore_VectorIndex_CreateRejectsInvalidConfig(t *testing.T) {
	bs := newTestBadgerStore(t)

	cases := []struct {
		name   string
		dims   int
		metric DistanceMetric
	}{
		{name: "zero dims", dims: 0, metric: DistanceCosine},
		{name: "unsupported metric", dims: 2, metric: DistanceMetric(99)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := bs.CreateVectorIndex(3, "vec", tc.dims, tc.metric)
			if !errors.Is(err, ErrInvalidVectorIndexConfig) {
				t.Fatalf("CreateVectorIndex error = %v, want ErrInvalidVectorIndexConfig", err)
			}
			if !errors.Is(err, storepkg.ErrInvalidVectorIndexConfig) {
				t.Fatalf("CreateVectorIndex error = %v, want store.ErrInvalidVectorIndexConfig", err)
			}
			_, searchErr := bs.SearchNearestNodes(3, "vec", []float32{1, 0}, 1, QueryOpts{})
			if !errors.Is(searchErr, ErrVectorIndexNotFound) {
				t.Fatalf("SearchNearestNodes after failed create = %v, want ErrVectorIndexNotFound", searchErr)
			}
		})
	}
}

func TestBadgerStore_VectorIndex_LoadRejectsInvalidDefinition(t *testing.T) {
	dir := t.TempDir()

	bs1, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	defs := []vectorIdxDef{{LabelToken: 1, PropertyKey: "vec", Dims: 0, Metric: DistanceCosine}}
	data, err := msgpack.Marshal(defs)
	if err != nil {
		t.Fatalf("marshal defs: %v", err)
	}
	if err := bs1.DBForTest().Update(func(txn *badgerv4.Txn) error {
		return txn.Set(storeutil.VectorIndexDefsKey, data)
	}); err != nil {
		t.Fatalf("write invalid defs: %v", err)
	}
	if err := bs1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	_, err = New(Config{Dir: dir})
	if !errors.Is(err, ErrInvalidVectorIndexConfig) {
		t.Fatalf("open with invalid vector index definition = %v, want ErrInvalidVectorIndexConfig", err)
	}
}

func TestBadgerStore_VectorIndex_LoadRejectsConflictingDuplicateDefinition(t *testing.T) {
	cases := []struct {
		name    string
		defs    []vectorIdxDef
		wantErr bool
	}{
		{
			name: "identical duplicate",
			defs: []vectorIdxDef{
				{LabelToken: 1, PropertyKey: "vec", Dims: 2, Metric: DistanceCosine},
				{LabelToken: 1, PropertyKey: "vec", Dims: 2, Metric: DistanceCosine},
			},
		},
		{
			name: "conflicting duplicate",
			defs: []vectorIdxDef{
				{LabelToken: 1, PropertyKey: "vec", Dims: 2, Metric: DistanceCosine},
				{LabelToken: 1, PropertyKey: "vec", Dims: 3, Metric: DistanceCosine},
			},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			bs1, err := New(Config{Dir: dir})
			if err != nil {
				t.Fatalf("open 1: %v", err)
			}
			data, err := msgpack.Marshal(tc.defs)
			if err != nil {
				t.Fatalf("marshal defs: %v", err)
			}
			if err := bs1.DBForTest().Update(func(txn *badgerv4.Txn) error {
				return txn.Set(storeutil.VectorIndexDefsKey, data)
			}); err != nil {
				t.Fatalf("write duplicate defs: %v", err)
			}
			if err := bs1.Close(); err != nil {
				t.Fatalf("close 1: %v", err)
			}

			bs2, err := New(Config{Dir: dir})
			if tc.wantErr {
				if !errors.Is(err, ErrVectorIndexExists) {
					t.Fatalf("open with conflicting duplicate vector index definition = %v, want ErrVectorIndexExists", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("open with identical duplicate vector index definition: %v", err)
			}
			if err := bs2.Close(); err != nil {
				t.Fatalf("close 2: %v", err)
			}
		})
	}
}

func TestBadgerStoreIndexDefinitionLoadRejectsZeroLabelToken(t *testing.T) {
	cases := []struct {
		name string
		key  []byte
		defs any
	}{
		{
			name: "property",
			key:  storeutil.PropIndexDefsKey,
			defs: []propIdxDef{{LabelToken: 0, PropertyKey: "name"}},
		},
		{
			name: "temporal",
			key:  storeutil.TemporalIndexDefsKey,
			defs: []uint16{0},
		},
		{
			name: "high frequency",
			key:  storeutil.HighFrequencyIndexDefsKey,
			defs: []hfIdxDef{{LabelToken: 0, BucketSizeMillis: int64(time.Hour / time.Millisecond)}},
		},
		{
			name: "vector",
			key:  storeutil.VectorIndexDefsKey,
			defs: []vectorIdxDef{{LabelToken: 0, PropertyKey: "vec", Dims: 2, Metric: DistanceCosine}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			bs1, err := New(Config{Dir: dir})
			if err != nil {
				t.Fatalf("open 1: %v", err)
			}
			data, err := msgpack.Marshal(tc.defs)
			if err != nil {
				t.Fatalf("marshal defs: %v", err)
			}
			if err := bs1.DBForTest().Update(func(txn *badgerv4.Txn) error {
				return txn.Set(tc.key, data)
			}); err != nil {
				t.Fatalf("write defs: %v", err)
			}
			if err := bs1.Close(); err != nil {
				t.Fatalf("close 1: %v", err)
			}

			_, err = New(Config{Dir: dir})
			if !errors.Is(err, ErrInvalidStoreMutation) {
				t.Fatalf("open with zero-label %s index definition = %v, want ErrInvalidStoreMutation", tc.name, err)
			}
		})
	}
}

func TestBadgerStoreIndexDefinitionLoadRejectsReservedPropertyKey(t *testing.T) {
	cases := []struct {
		name string
		key  []byte
		defs any
	}{
		{
			name: "property",
			key:  storeutil.PropIndexDefsKey,
			defs: []propIdxDef{{LabelToken: 1, PropertyKey: "tkg_hash"}},
		},
		{
			name: "vector",
			key:  storeutil.VectorIndexDefsKey,
			defs: []vectorIdxDef{{LabelToken: 1, PropertyKey: "tkg_hash", Dims: 2, Metric: DistanceCosine}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			bs1, err := New(Config{Dir: dir})
			if err != nil {
				t.Fatalf("open 1: %v", err)
			}
			data, err := msgpack.Marshal(tc.defs)
			if err != nil {
				t.Fatalf("marshal defs: %v", err)
			}
			if err := bs1.DBForTest().Update(func(txn *badgerv4.Txn) error {
				return txn.Set(tc.key, data)
			}); err != nil {
				t.Fatalf("write defs: %v", err)
			}
			if err := bs1.Close(); err != nil {
				t.Fatalf("close 1: %v", err)
			}

			_, err = New(Config{Dir: dir})
			if !errors.Is(err, types.ErrReservedPrefix) {
				t.Fatalf("open with reserved-key %s index definition = %v, want ErrReservedPrefix", tc.name, err)
			}
		})
	}
}

func TestBadgerStoreIndexDefinitionLoadRejectsCorruptMsgpack(t *testing.T) {
	cases := []struct {
		name string
		key  []byte
	}{
		{name: "property", key: storeutil.PropIndexDefsKey},
		{name: "temporal", key: storeutil.TemporalIndexDefsKey},
		{name: "high frequency", key: storeutil.HighFrequencyIndexDefsKey},
		{name: "vector", key: storeutil.VectorIndexDefsKey},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			bs1, err := New(Config{Dir: dir})
			if err != nil {
				t.Fatalf("open 1: %v", err)
			}
			if err := bs1.DBForTest().Update(func(txn *badgerv4.Txn) error {
				return txn.Set(tc.key, []byte{0xc1}) // reserved MsgPack code
			}); err != nil {
				t.Fatalf("write corrupt defs: %v", err)
			}
			if err := bs1.Close(); err != nil {
				t.Fatalf("close 1: %v", err)
			}

			if _, err := New(Config{Dir: dir}); err == nil {
				t.Fatalf("open with corrupt %s index definitions returned nil", tc.name)
			}
		})
	}
}

func TestBadgerStorePersistIndexDefinitionsSkipBuildingIndexes(t *testing.T) {
	bs := newTestBadgerStore(t)

	readyProp := indexpkg.PropertyIndexKey{LabelToken: 1, PropertyKey: "ready"}
	buildingProp := indexpkg.PropertyIndexKey{LabelToken: 2, PropertyKey: "building"}
	readyVector := indexpkg.VectorIndexKey{LabelToken: 3, PropertyKey: "ready_vec"}
	buildingVector := indexpkg.VectorIndexKey{LabelToken: 4, PropertyKey: "building_vec"}

	bs.idxMu.Lock()
	bs.propertyIndexes[readyProp] = indexpkg.NewPropertyIndex()
	bs.propertyIndexes[buildingProp] = indexpkg.NewPropertyIndex()
	bs.propertyIndexes[buildingProp].Mutated = make(map[snowflake.ID]struct{})
	bs.persistPropertyIndexDefs()

	bs.temporalIndexes[5] = indexpkg.NewTemporalIndex()
	bs.temporalIndexes[6] = indexpkg.NewTemporalIndex()
	bs.temporalIndexes[6].Building = true
	bs.persistTemporalIndexDefs()

	bs.hfIndexes[7] = indexpkg.NewHighFrequencyIndex(time.Hour, 0)
	bs.hfIndexes[8] = indexpkg.NewHighFrequencyIndex(time.Hour, 0)
	bs.hfIndexes[8].Mutated = make(map[snowflake.ID]struct{})
	bs.persistHighFrequencyIndexDefs()

	bs.vectorIndexes[readyVector] = &indexpkg.VectorIndex{Dims: 2, Metric: DistanceCosine}
	bs.vectorIndexes[buildingVector] = &indexpkg.VectorIndex{
		Dims:    2,
		Metric:  DistanceCosine,
		Mutated: make(map[snowflake.ID]struct{}),
	}
	bs.persistVectorIndexDefs()
	bs.idxMu.Unlock()

	var propDefs []propIdxDef
	unmarshalPendingSetForTest(t, bs, storeutil.PropIndexDefsKey, &propDefs)
	if len(propDefs) != 1 || propDefs[0].LabelToken != readyProp.LabelToken || propDefs[0].PropertyKey != readyProp.PropertyKey {
		t.Fatalf("persisted property defs = %#v, want only ready definition", propDefs)
	}

	var temporalDefs []uint16
	unmarshalPendingSetForTest(t, bs, storeutil.TemporalIndexDefsKey, &temporalDefs)
	if len(temporalDefs) != 1 || temporalDefs[0] != 5 {
		t.Fatalf("persisted temporal defs = %#v, want only token 5", temporalDefs)
	}

	var hfDefs []hfIdxDef
	unmarshalPendingSetForTest(t, bs, storeutil.HighFrequencyIndexDefsKey, &hfDefs)
	if len(hfDefs) != 1 || hfDefs[0].LabelToken != 7 || hfDefs[0].BucketSizeMillis != int64(time.Hour/time.Millisecond) {
		t.Fatalf("persisted high-frequency defs = %#v, want only token 7", hfDefs)
	}

	var vectorDefs []vectorIdxDef
	unmarshalPendingSetForTest(t, bs, storeutil.VectorIndexDefsKey, &vectorDefs)
	if len(vectorDefs) != 1 || vectorDefs[0].LabelToken != readyVector.LabelToken || vectorDefs[0].PropertyKey != readyVector.PropertyKey {
		t.Fatalf("persisted vector defs = %#v, want only ready definition", vectorDefs)
	}
}

func unmarshalPendingSetForTest(t *testing.T, bs *Store, key []byte, dst any) {
	t.Helper()
	bs.wbMu.Lock()
	op, ok := bs.pending[string(key)]
	bs.wbMu.Unlock()
	if !ok {
		t.Fatalf("pending op for %q not found", string(key))
	}
	if op.opType != writeOpSet {
		t.Fatalf("pending op for %q type = %v, want set", string(key), op.opType)
	}
	if err := msgpack.Unmarshal(op.value, dst); err != nil {
		t.Fatalf("unmarshal pending op for %q: %v", string(key), err)
	}
}

func TestBadgerStore_VectorIndex_DefinitionSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	labelTok := uint16(3)
	key := "vec"

	bs1, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	n1 := types.NewNode(types.NodeID(snowflake.ID(101)), labelTok, nil)
	ps1, _ := types.NewPropertySlice(map[string]any{key: []float32{1, 0, 0}})
	n1.SetProperties(ps1)
	if err := bs1.PutNode(n1); err != nil {
		t.Fatalf("PutNode 1: %v", err)
	}
	n2 := types.NewNode(types.NodeID(snowflake.ID(102)), labelTok, nil)
	ps2, _ := types.NewPropertySlice(map[string]any{key: []float32{0, 1, 0}})
	n2.SetProperties(ps2)
	if err := bs1.PutNode(n2); err != nil {
		t.Fatalf("PutNode 2: %v", err)
	}
	if err := bs1.CreateVectorIndex(labelTok, key, 3, DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}
	if err := bs1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	bs2, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer bs2.Close()

	results, err := bs2.SearchNearestNodes(labelTok, key, []float32{1, 0, 0}, 1, QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes after reopen: %v", err)
	}
	if len(results) != 1 || results[0].ID() != n1.ID() {
		t.Fatalf("SearchNearestNodes after reopen = %#v, want n1", results)
	}
}

func TestBadgerStore_VectorIndex_DropDefinitionSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	bs1, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	if err := bs1.CreateVectorIndex(1, "v", 2, DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}
	if err := bs1.DropVectorIndex(1, "v"); err != nil {
		t.Fatalf("DropVectorIndex: %v", err)
	}
	if err := bs1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	bs2, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer bs2.Close()
	_, err = bs2.SearchNearestNodes(1, "v", []float32{1, 0}, 1, QueryOpts{})
	if !errors.Is(err, ErrVectorIndexNotFound) {
		t.Fatalf("SearchNearestNodes after dropped reopen = %v, want ErrVectorIndexNotFound", err)
	}
}

func TestBadgerStore_VectorIndex_CreatePropagatesBackfillCorruption(t *testing.T) {
	bs := newTestBadgerStore(t)
	labelTok := uint16(3)
	key := "vec"

	n := types.NewNode(types.NodeID(snowflake.ID(101)), labelTok, nil)
	ps, _ := types.NewPropertySlice(map[string]any{key: []float32{1, 0, 0}})
	n.SetProperties(ps)
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	bs.NodeCacheForTest().ResetForTest()
	if err := bs.DBForTest().Update(func(txn *badgerv4.Txn) error {
		return txn.Set(storeutil.NodeKey(n.ID().SnowflakeID()), []byte("corrupt"))
	}); err != nil {
		t.Fatalf("corrupt node row: %v", err)
	}

	err := bs.CreateVectorIndex(labelTok, key, 3, DistanceCosine)
	if err == nil {
		t.Fatal("CreateVectorIndex returned nil for corrupted node backfill")
	}
	if errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("CreateVectorIndex error = %v, want corruption/operational error", err)
	}
	_, searchErr := bs.SearchNearestNodes(labelTok, key, []float32{1, 0, 0}, 1, QueryOpts{})
	if !errors.Is(searchErr, ErrVectorIndexNotFound) {
		t.Fatalf("SearchNearestNodes after failed create = %v, want ErrVectorIndexNotFound", searchErr)
	}
}

func TestBadgerStore_VectorIndex_CreateRejectsBackfillDimensionMismatch(t *testing.T) {
	bs := newTestBadgerStore(t)
	labelTok := uint16(3)
	key := "vec"

	n := types.NewNode(types.NodeID(snowflake.ID(104)), labelTok, nil)
	ps, _ := types.NewPropertySlice(map[string]any{key: []float32{1, 0}})
	n.SetProperties(ps)
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	err := bs.CreateVectorIndex(labelTok, key, 3, DistanceCosine)
	if !errors.Is(err, ErrDimensionMismatch) {
		t.Fatalf("CreateVectorIndex error = %v, want ErrDimensionMismatch", err)
	}
	_, searchErr := bs.SearchNearestNodes(labelTok, key, []float32{1, 0, 0}, 1, QueryOpts{})
	if !errors.Is(searchErr, ErrVectorIndexNotFound) {
		t.Fatalf("SearchNearestNodes after failed create = %v, want ErrVectorIndexNotFound", searchErr)
	}
}

func TestBadgerStore_VectorIndex_PutNodeRejectsMaintenanceDimensionMismatch(t *testing.T) {
	bs := newTestBadgerStore(t)
	labelTok := uint16(3)
	key := "vec"
	if err := bs.CreateVectorIndex(labelTok, key, 3, DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	n := types.NewNode(types.NodeID(snowflake.ID(105)), labelTok, nil)
	ps, _ := types.NewPropertySlice(map[string]any{key: []float32{1, 0}})
	n.SetProperties(ps)

	err := bs.PutNode(n)
	if !errors.Is(err, ErrDimensionMismatch) {
		t.Fatalf("PutNode error = %v, want ErrDimensionMismatch", err)
	}
	if _, err := bs.GetNode(n.ID()); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNode after failed PutNode = %v, want ErrNodeNotFound", err)
	}
}

func TestBadgerStore_VectorIndex_SearchPropagatesCandidateCorruption(t *testing.T) {
	bs := newTestBadgerStore(t)
	labelTok := uint16(3)
	key := "vec"

	n := types.NewNode(types.NodeID(snowflake.ID(103)), labelTok, nil)
	ps, _ := types.NewPropertySlice(map[string]any{key: []float32{1, 0, 0}})
	n.SetProperties(ps)
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := bs.CreateVectorIndex(labelTok, key, 3, DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}
	corruptNodeRowAfterFlush(t, bs, n.ID().SnowflakeID())

	_, err := bs.SearchNearestNodes(labelTok, key, []float32{1, 0, 0}, 1, QueryOpts{})
	requireCorruptNodeReadError(t, "SearchNearestNodes", err)
}

func TestBadgerStore_VectorIndex_AlreadyExists(t *testing.T) {
	bs := newTestBadgerStore(t)
	bs.CreateVectorIndex(1, "v", 2, DistanceCosine)
	err := bs.CreateVectorIndex(1, "v", 2, DistanceCosine)
	if !errors.Is(err, ErrVectorIndexExists) {
		t.Errorf("expected ErrVectorIndexExists, got %v", err)
	}
}

func TestBadgerStore_VectorIndex_DropNotFound(t *testing.T) {
	bs := newTestBadgerStore(t)
	err := bs.DropVectorIndex(1, "missing")
	if !errors.Is(err, ErrVectorIndexNotFound) {
		t.Errorf("expected ErrVectorIndexNotFound, got %v", err)
	}
}

func TestBadgerStore_VectorIndex_Drop(t *testing.T) {
	bs := newTestBadgerStore(t)
	bs.CreateVectorIndex(1, "v", 2, DistanceCosine)
	if err := bs.DropVectorIndex(1, "v"); err != nil {
		t.Fatalf("DropVectorIndex: %v", err)
	}
	// After drop, SearchNearestNodes should return ErrVectorIndexNotFound.
	_, err := bs.SearchNearestNodes(1, "v", []float32{1, 0}, 1, QueryOpts{})
	if !errors.Is(err, ErrVectorIndexNotFound) {
		t.Errorf("expected ErrVectorIndexNotFound after drop, got %v", err)
	}
}

func TestBadgerStore_VectorIndex_SearchNotFound(t *testing.T) {
	bs := newTestBadgerStore(t)
	_, err := bs.SearchNearestNodes(1, "nonexistent", []float32{1, 0}, 1, QueryOpts{})
	if !errors.Is(err, ErrVectorIndexNotFound) {
		t.Errorf("expected ErrVectorIndexNotFound, got %v", err)
	}
}

func TestBadgerStore_VectorIndex_DimensionMismatch(t *testing.T) {
	bs := newTestBadgerStore(t)
	bs.CreateVectorIndex(1, "v", 3, DistanceCosine)
	_, err := bs.SearchNearestNodes(1, "v", []float32{1, 0}, 1, QueryOpts{})
	if !errors.Is(err, ErrDimensionMismatch) {
		t.Errorf("expected ErrDimensionMismatch, got %v", err)
	}
}
