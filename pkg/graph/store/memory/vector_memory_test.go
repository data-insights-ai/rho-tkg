package memory

import (
	"errors"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
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
