package memory

import (
	"errors"
	"math"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/index"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

func TestMemoryStoreIndexAPIsRejectZeroLabelToken(t *testing.T) {
	t.Parallel()

	ms := New()
	cases := []struct {
		name string
		run  func() error
	}{
		{name: "create property", run: func() error { return ms.CreatePropertyIndex(0, "name") }},
		{name: "drop property", run: func() error { return ms.DropPropertyIndex(0, "name") }},
		{name: "create temporal", run: func() error { return ms.CreateTemporalIndex(0) }},
		{name: "drop temporal", run: func() error { return ms.DropTemporalIndex(0) }},
		{name: "create high frequency", run: func() error { return ms.CreateHighFrequencyIndex(0, time.Hour) }},
		{name: "drop high frequency", run: func() error { return ms.DropHighFrequencyIndex(0) }},
		{name: "create vector", run: func() error { return ms.CreateVectorIndex(0, "vec", 2, storepkg.DistanceCosine) }},
		{name: "drop vector", run: func() error { return ms.DropVectorIndex(0, "vec") }},
		{name: "search vector", run: func() error {
			_, err := ms.SearchNearestNodes(0, "vec", []float32{1, 0}, 1, QueryOpts{})
			return err
		}},
		{name: "search filtered vector", run: func() error {
			_, err := ms.SearchNearestFiltered(0, "vec", []float32{1, 0}, 1, nil)
			return err
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.run(); !errors.Is(err, ErrInvalidStoreMutation) {
				t.Fatalf("%s err = %v, want ErrInvalidStoreMutation", tc.name, err)
			}
		})
	}
}

func TestMemoryStoreIndexAPIsRejectReservedPropertyKey(t *testing.T) {
	t.Parallel()

	ms := New()
	cases := []struct {
		name string
		run  func() error
	}{
		{name: "create property", run: func() error { return ms.CreatePropertyIndex(1, "tkg_hash") }},
		{name: "drop property", run: func() error { return ms.DropPropertyIndex(1, "tkg_hash") }},
		{name: "query property", run: func() error {
			_, err := ms.NodesByLabelAndProperty(1, "tkg_hash", "x", QueryOpts{})
			return err
		}},
		{name: "create vector", run: func() error { return ms.CreateVectorIndex(1, "tkg_hash", 2, storepkg.DistanceCosine) }},
		{name: "drop vector", run: func() error { return ms.DropVectorIndex(1, "tkg_hash") }},
		{name: "search vector", run: func() error {
			_, err := ms.SearchNearestNodes(1, "tkg_hash", []float32{1, 0}, 1, QueryOpts{})
			return err
		}},
		{name: "search filtered vector", run: func() error {
			_, err := ms.SearchNearestFiltered(1, "tkg_hash", []float32{1, 0}, 1, nil)
			return err
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.run(); !errors.Is(err, types.ErrReservedPrefix) {
				t.Fatalf("%s err = %v, want ErrReservedPrefix", tc.name, err)
			}
		})
	}
}

func TestMemoryStoreNodesByLabelAndPropertyRejectsInvalidQueryValue(t *testing.T) {
	t.Parallel()

	ms := New()
	_, err := ms.NodesByLabelAndProperty(1, "name", struct{ Bad int }{Bad: 1}, QueryOpts{})
	if !errors.Is(err, types.ErrUnsupportedValueType) {
		t.Fatalf("NodesByLabelAndProperty invalid value = %v, want ErrUnsupportedValueType", err)
	}

	nodes, err := ms.NodesByLabelAndProperty(1, "name", []string{"valid", "unindexable"}, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabelAndProperty valid unindexable value: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("NodesByLabelAndProperty valid unindexable value returned %d nodes, want 0", len(nodes))
	}
}

func TestMemoryStoreQueryAPIsRejectInvalidDepth(t *testing.T) {
	t.Parallel()

	ms := New()
	badOpts := QueryOpts{Depth: storepkg.ShardDepth(99)}
	cases := []struct {
		name string
		run  func() error
	}{
		{name: "NodesByLabel", run: func() error {
			_, err := ms.NodesByLabel(1, badOpts)
			return err
		}},
		{name: "RelationshipsByType", run: func() error {
			_, err := ms.RelationshipsByType(1, badOpts)
			return err
		}},
		{name: "AllNodeIDs", run: func() error {
			_, err := ms.AllNodeIDs(badOpts)
			return err
		}},
		{name: "AllRelIDs", run: func() error {
			_, err := ms.AllRelIDs(badOpts)
			return err
		}},
		{name: "AllNodes", run: func() error {
			_, err := ms.AllNodes(badOpts)
			return err
		}},
		{name: "AllRelationships", run: func() error {
			_, err := ms.AllRelationships(badOpts)
			return err
		}},
		{name: "NodesByLabelAndProperty", run: func() error {
			_, err := ms.NodesByLabelAndProperty(1, "name", "alice", badOpts)
			return err
		}},
		{name: "SearchNearestNodes", run: func() error {
			_, err := ms.SearchNearestNodes(1, "vec", []float32{1, 0}, 1, badOpts)
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

func TestMemoryStore_VectorIndex_CreateRejectsBackfillDimensionMismatch(t *testing.T) {
	t.Parallel()

	ms := New()
	labelTok := uint16(3)
	key := "vec"

	n := types.NewNode(types.NodeID(snowflake.ID(101)), labelTok, nil)
	ps, _ := types.NewPropertySlice(map[string]any{key: []float32{1, 0}})
	n.SetProperties(ps)
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	err := ms.CreateVectorIndex(labelTok, key, 3, storepkg.DistanceCosine)
	if !errors.Is(err, ErrDimensionMismatch) {
		t.Fatalf("CreateVectorIndex error = %v, want ErrDimensionMismatch", err)
	}
	_, searchErr := ms.SearchNearestNodes(labelTok, key, []float32{1, 0, 0}, 1, QueryOpts{})
	if !errors.Is(searchErr, ErrVectorIndexNotFound) {
		t.Fatalf("SearchNearestNodes after failed create = %v, want ErrVectorIndexNotFound", searchErr)
	}
}

func TestMemoryStore_VectorIndex_RejectsNonFiniteVectors(t *testing.T) {
	t.Parallel()

	ms := New()
	labelTok := uint16(3)
	key := "vec"

	existing := types.NewNode(types.NodeID(snowflake.ID(105)), labelTok, nil)
	if err := existing.SetProperty(key, []float32{float32(math.NaN()), 0}); err != nil {
		t.Fatalf("SetProperty existing: %v", err)
	}
	if err := ms.PutNode(existing); err != nil {
		t.Fatalf("PutNode existing: %v", err)
	}

	err := ms.CreateVectorIndex(labelTok, key, 2, storepkg.DistanceEuclidean)
	if !errors.Is(err, ErrInvalidVectorValue) {
		t.Fatalf("CreateVectorIndex error = %v, want ErrInvalidVectorValue", err)
	}
	if !errors.Is(err, storepkg.ErrInvalidVectorValue) {
		t.Fatalf("CreateVectorIndex error = %v, want store.ErrInvalidVectorValue", err)
	}
	_, searchErr := ms.SearchNearestNodes(labelTok, key, []float32{0, 0}, 1, QueryOpts{})
	if !errors.Is(searchErr, ErrVectorIndexNotFound) {
		t.Fatalf("SearchNearestNodes after failed create = %v, want ErrVectorIndexNotFound", searchErr)
	}

	ms = New()
	if err := ms.CreateVectorIndex(labelTok, key, 2, storepkg.DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex empty: %v", err)
	}
	got, err := ms.SearchNearestNodes(labelTok, key, []float32{float32(math.NaN()), 0}, 0, QueryOpts{})
	if !errors.Is(err, ErrInvalidVectorValue) || got != nil {
		t.Fatalf("SearchNearestNodes non-finite query with k=0 = (%v, %v), want nil, ErrInvalidVectorValue", got, err)
	}
	bad := types.NewNode(types.NodeID(snowflake.ID(106)), labelTok, nil)
	if err := bad.SetProperty(key, []float32{float32(math.Inf(1)), 0}); err != nil {
		t.Fatalf("SetProperty bad: %v", err)
	}
	err = ms.PutNode(bad)
	if !errors.Is(err, ErrInvalidVectorValue) {
		t.Fatalf("PutNode indexed non-finite vector = %v, want ErrInvalidVectorValue", err)
	}
	if _, err := ms.GetNode(bad.ID()); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNode after failed PutNode = %v, want ErrNodeNotFound", err)
	}
}

func TestMemoryStore_VectorIndex_CreateBackfillsFromLabelIndexOnly(t *testing.T) {
	t.Parallel()

	ms := New()
	const labelTok = uint16(7)
	n := types.NewNode(types.NodeID(snowflake.ID(10)), labelTok, nil)
	if err := n.SetProperty("vec", []float32{1, 0}); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	ms.mu.Lock()
	delete(ms.labelIdx[labelTok], n.ID())
	ms.mu.Unlock()

	if err := ms.CreateVectorIndex(labelTok, "vec", 2, storepkg.DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}
	got, err := ms.SearchNearestNodes(labelTok, "vec", []float32{1, 0}, 1, QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("SearchNearestNodes returned %d nodes, want none for row absent from label index", len(got))
	}
}

func TestMemoryStore_VectorIndex_CreateRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	ms := New()

	cases := []struct {
		name   string
		dims   int
		metric DistanceMetric
	}{
		{name: "zero dims", dims: 0, metric: storepkg.DistanceCosine},
		{name: "unsupported metric", dims: 2, metric: storepkg.DistanceMetric(99)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ms.CreateVectorIndex(3, "vec", tc.dims, tc.metric)
			if !errors.Is(err, ErrInvalidVectorIndexConfig) {
				t.Fatalf("CreateVectorIndex error = %v, want ErrInvalidVectorIndexConfig", err)
			}
			if !errors.Is(err, storepkg.ErrInvalidVectorIndexConfig) {
				t.Fatalf("CreateVectorIndex error = %v, want store.ErrInvalidVectorIndexConfig", err)
			}
			_, searchErr := ms.SearchNearestNodes(3, "vec", []float32{1, 0}, 1, QueryOpts{})
			if !errors.Is(searchErr, ErrVectorIndexNotFound) {
				t.Fatalf("SearchNearestNodes after failed create = %v, want ErrVectorIndexNotFound", searchErr)
			}
		})
	}
}

func TestMemoryStoreSearchNearestNodesAppliesCursorPagination(t *testing.T) {
	t.Parallel()

	ms := New()
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
		if err := ms.PutNode(n); err != nil {
			t.Fatalf("PutNode[%d]: %v", i, err)
		}
	}
	if err := ms.CreateVectorIndex(labelTok, key, 2, storepkg.DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	limited, err := ms.SearchNearestNodes(labelTok, key, []float32{0, 0}, 3, QueryOpts{Limit: 2})
	if err != nil {
		t.Fatalf("SearchNearestNodes limit: %v", err)
	}
	if len(limited) != 2 || limited[0].ID() != nodes[0].ID() || limited[1].ID() != nodes[1].ID() {
		t.Fatalf("limited result IDs = %v, want first two distance-ordered nodes", nodeIDsForTest(limited))
	}

	afterFirst, err := ms.SearchNearestNodes(labelTok, key, []float32{0, 0}, 3, QueryOpts{
		After: types.EntityID(nodes[0].ID().SnowflakeID()),
		Limit: 1,
	})
	if err != nil {
		t.Fatalf("SearchNearestNodes after+limit: %v", err)
	}
	if len(afterFirst) != 1 || afterFirst[0].ID() != nodes[1].ID() {
		t.Fatalf("after+limit result IDs = %v, want second node", nodeIDsForTest(afterFirst))
	}

	missingCursor, err := ms.SearchNearestNodes(labelTok, key, []float32{0, 0}, 3, QueryOpts{
		After: types.EntityID(snowflake.ID(999)),
	})
	if err != nil {
		t.Fatalf("SearchNearestNodes missing cursor: %v", err)
	}
	if len(missingCursor) != 0 {
		t.Fatalf("missing cursor result IDs = %v, want none", nodeIDsForTest(missingCursor))
	}
}

func TestMemoryStoreSearchNearestNodesAppliesTemporalFilterBeforeHeap(t *testing.T) {
	t.Parallel()

	ms := New()
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
		if err := ms.PutNode(n); err != nil {
			t.Fatalf("PutNode[%d]: %v", i, err)
		}
	}
	if err := ms.CreateVectorIndex(labelTok, key, 2, storepkg.DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	got, err := ms.SearchNearestNodes(labelTok, key, []float32{0, 0}, 1, QueryOpts{ValidAt: 50})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(got) != 1 || got[0].ID() != eligible.ID() {
		t.Fatalf("temporal result IDs = %v, want eligible farther node %d", nodeIDsForTest(got), eligible.ID())
	}
}

func TestMemoryStoreSearchNearestNodesTemporalFilterSkipsStaleCurrentRowBeforeHeap(t *testing.T) {
	t.Parallel()

	ms := New()
	labelTok := uint16(3)
	keyName := "vec"
	stale := types.NewNode(types.NodeID(snowflake.ID(1301)), labelTok+1, nil)
	stale.SetTemporal(&types.TemporalMetadata{ValidFrom: 1})
	if err := stale.SetProperty(keyName, []float32{0, 0}); err != nil {
		t.Fatalf("SetProperty stale: %v", err)
	}
	live := types.NewNode(types.NodeID(snowflake.ID(1302)), labelTok, nil)
	live.SetTemporal(&types.TemporalMetadata{ValidFrom: 1})
	if err := live.SetProperty(keyName, []float32{10, 0}); err != nil {
		t.Fatalf("SetProperty live: %v", err)
	}
	for _, n := range []*types.Node{stale, live} {
		if err := ms.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}
	if err := ms.CreateVectorIndex(labelTok, keyName, 2, storepkg.DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	key := indexpkg.VectorIndexKey{LabelToken: labelTok, PropertyKey: keyName}
	ms.mu.Lock()
	if err := ms.vectorIndexes[key].Add(stale.ID().SnowflakeID(), []float32{0, 0}); err != nil {
		ms.mu.Unlock()
		t.Fatalf("seed stale vector entry: %v", err)
	}
	ms.mu.Unlock()

	got, err := ms.SearchNearestNodes(labelTok, keyName, []float32{0, 0}, 1, QueryOpts{ValidAt: 5})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(got) != 1 || got[0].ID() != live.ID() {
		t.Fatalf("SearchNearestNodes IDs = %v, want live node %d", nodeIDsForTest(got), live.ID())
	}
}

func TestMemoryStoreSearchNearestFilteredSkipsStaleCurrentRowBeforeHeap(t *testing.T) {
	t.Parallel()

	ms := New()
	labelTok := uint16(3)
	keyName := "vec"
	stale := types.NewNode(types.NodeID(snowflake.ID(1311)), labelTok+1, nil)
	if err := stale.SetProperty(keyName, []float32{0, 0}); err != nil {
		t.Fatalf("SetProperty stale: %v", err)
	}
	live := types.NewNode(types.NodeID(snowflake.ID(1312)), labelTok, nil)
	if err := live.SetProperty(keyName, []float32{10, 0}); err != nil {
		t.Fatalf("SetProperty live: %v", err)
	}
	for _, n := range []*types.Node{stale, live} {
		if err := ms.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}
	if err := ms.CreateVectorIndex(labelTok, keyName, 2, storepkg.DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	key := indexpkg.VectorIndexKey{LabelToken: labelTok, PropertyKey: keyName}
	ms.mu.Lock()
	if err := ms.vectorIndexes[key].Add(stale.ID().SnowflakeID(), []float32{0, 0}); err != nil {
		ms.mu.Unlock()
		t.Fatalf("seed stale vector entry: %v", err)
	}
	ms.mu.Unlock()

	filterCalls := make(map[snowflake.ID]int)
	ids, err := ms.SearchNearestFiltered(labelTok, keyName, []float32{0, 0}, 1, func(id snowflake.ID) bool {
		filterCalls[id]++
		return true
	})
	if err != nil {
		t.Fatalf("SearchNearestFiltered: %v", err)
	}
	if len(ids) != 1 || ids[0] != live.ID().SnowflakeID() {
		t.Fatalf("SearchNearestFiltered IDs = %v, want live node %d", ids, live.ID())
	}
	if filterCalls[stale.ID().SnowflakeID()] != 0 {
		t.Fatalf("SearchNearestFiltered called external filter for stale ID %d", stale.ID())
	}
	if filterCalls[live.ID().SnowflakeID()] != 1 {
		t.Fatalf("SearchNearestFiltered live filter calls = %d, want 1", filterCalls[live.ID().SnowflakeID()])
	}
}

func TestMemoryStoreVectorIndexReplaceNodePurgesStaleCrossLabelEntry(t *testing.T) {
	t.Parallel()

	ms := New()
	node := types.NewNode(types.NodeID(snowflake.ID(201)), 1, nil)
	if err := node.SetProperty("vec", []float32{1, 0}); err != nil {
		t.Fatalf("SetProperty node: %v", err)
	}
	if err := ms.PutNode(node); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := ms.CreateVectorIndex(2, "vec", 2, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	key := indexpkg.VectorIndexKey{LabelToken: 2, PropertyKey: "vec"}
	ms.mu.Lock()
	if err := ms.vectorIndexes[key].Add(node.ID().SnowflakeID(), []float32{1, 0}); err != nil {
		ms.mu.Unlock()
		t.Fatalf("seed stale vector entry: %v", err)
	}
	ms.mu.Unlock()

	updated := node.DeepCopy()
	if err := updated.SetProperty("name", "updated"); err != nil {
		t.Fatalf("SetProperty updated: %v", err)
	}
	if err := ms.ReplaceNode(updated); err != nil {
		t.Fatalf("ReplaceNode: %v", err)
	}

	ms.mu.RLock()
	rawIDs, rawErr := ms.vectorIndexes[key].SearchNearest([]float32{1, 0}, 1, nil)
	ms.mu.RUnlock()
	if rawErr != nil || len(rawIDs) != 0 {
		t.Fatalf("raw vector index after ReplaceNode = (%v, %v), want no stale entry", rawIDs, rawErr)
	}

	got, err := ms.SearchNearestNodes(2, "vec", []float32{1, 0}, 1, QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("SearchNearestNodes returned stale cross-label vector entry IDs = %v, want none", nodeIDsForTest(got))
	}
}

func TestMemoryStoreSearchNearestNodesSkipsStaleCurrentRowShape(t *testing.T) {
	t.Parallel()

	ms := New()
	node := types.NewNode(types.NodeID(snowflake.ID(202)), 1, nil)
	if err := node.SetProperty("vec", []float32{1, 0}); err != nil {
		t.Fatalf("SetProperty node: %v", err)
	}
	if err := ms.PutNode(node); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := ms.CreateVectorIndex(2, "vec", 2, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	key := indexpkg.VectorIndexKey{LabelToken: 2, PropertyKey: "vec"}
	ms.mu.Lock()
	if err := ms.vectorIndexes[key].Add(node.ID().SnowflakeID(), []float32{1, 0}); err != nil {
		ms.mu.Unlock()
		t.Fatalf("seed stale vector entry: %v", err)
	}
	ms.mu.Unlock()

	got, err := ms.SearchNearestNodes(2, "vec", []float32{1, 0}, 1, QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("SearchNearestNodes returned stale current-row shape IDs = %v, want none", nodeIDsForTest(got))
	}
}

func nodeIDsForTest(nodes []*types.Node) []types.NodeID {
	ids := make([]types.NodeID, len(nodes))
	for i, n := range nodes {
		ids[i] = n.ID()
	}
	return ids
}

func TestMemoryStore_VectorIndex_PutNodeRejectsMaintenanceDimensionMismatch(t *testing.T) {
	t.Parallel()

	ms := New()
	labelTok := uint16(3)
	key := "vec"
	if err := ms.CreateVectorIndex(labelTok, key, 3, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	n := types.NewNode(types.NodeID(snowflake.ID(102)), labelTok, nil)
	ps, _ := types.NewPropertySlice(map[string]any{key: []float32{1, 0}})
	n.SetProperties(ps)

	err := ms.PutNode(n)
	if !errors.Is(err, ErrDimensionMismatch) {
		t.Fatalf("PutNode error = %v, want ErrDimensionMismatch", err)
	}
	if _, err := ms.GetNode(n.ID()); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNode after failed PutNode = %v, want ErrNodeNotFound", err)
	}
}
