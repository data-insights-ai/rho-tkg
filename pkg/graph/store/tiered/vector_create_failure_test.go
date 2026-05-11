package tiered

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
	storeutil "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestTieredStore_VectorIndex_CreateCleansPlaceholderOnScanError(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	key := "vec"

	n := types.NewNode(types.NodeID(snowflake.ID(101)), caseTok, nil)
	ps, _ := types.NewPropertySlice(map[string]any{key: []float32{1, 0, 0}})
	n.SetProperties(ps)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := ts.RefShardForTest().Flush(); err != nil {
		t.Fatalf("Flush ref shard: %v", err)
	}
	ts.RefShardForTest().NodeCacheForTest().ResetForTest()
	if err := ts.RefShardForTest().DBForTest().Update(func(txn *badgerv4.Txn) error {
		return txn.Set(storeutil.NodeKey(n.ID().SnowflakeID()), []byte("corrupt"))
	}); err != nil {
		t.Fatalf("corrupt node row: %v", err)
	}

	err := ts.CreateVectorIndex(caseTok, key, 3, DistanceCosine)
	if err == nil {
		t.Fatal("CreateVectorIndex returned nil for corrupted shard scan")
	}
	if errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("CreateVectorIndex error = %v, want corruption/operational error", err)
	}
	_, searchErr := ts.SearchNearestNodes(caseTok, key, []float32{1, 0, 0}, 1, QueryOpts{})
	if !errors.Is(searchErr, ErrVectorIndexNotFound) {
		t.Fatalf("SearchNearestNodes after failed create = %v, want ErrVectorIndexNotFound", searchErr)
	}
}

func TestTieredStore_VectorIndex_CreateRejectsInvalidConfig(t *testing.T) {
	ts := newTestTieredStore(t)

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
			err := ts.CreateVectorIndex(3, "vec", tc.dims, tc.metric)
			if !errors.Is(err, ErrInvalidVectorIndexConfig) {
				t.Fatalf("CreateVectorIndex error = %v, want ErrInvalidVectorIndexConfig", err)
			}
			_, searchErr := ts.SearchNearestNodes(3, "vec", []float32{1, 0}, 1, QueryOpts{})
			if !errors.Is(searchErr, ErrVectorIndexNotFound) {
				t.Fatalf("SearchNearestNodes after failed create = %v, want ErrVectorIndexNotFound", searchErr)
			}
		})
	}
}

func TestTieredStore_VectorIndex_SearchRejectsBuildingPlaceholder(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")

	n := types.NewNode(types.NodeID(snowflake.ID(106)), caseTok, nil)
	if err := n.SetProperty("vec", []float32{1, 0}); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	key := indexpkg.VectorIndexKey{LabelToken: caseTok, PropertyKey: "vec"}
	ts.vectorIdxMu.Lock()
	ts.vectorIndexes[key] = &indexpkg.VectorIndex{
		Dims:    2,
		Metric:  DistanceEuclidean,
		Mutated: make(map[snowflake.ID]struct{}),
	}
	ts.vectorIdxMu.Unlock()

	_, err := ts.SearchNearestNodes(caseTok, "vec", []float32{1, 0}, 1, QueryOpts{})
	if !errors.Is(err, ErrVectorIndexNotFound) {
		t.Fatalf("SearchNearestNodes with building vector index = %v, want ErrVectorIndexNotFound", err)
	}
}

func TestTieredStore_VectorIndex_CreateRejectsBackfillDimensionMismatch(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	key := "vec"

	n := types.NewNode(types.NodeID(snowflake.ID(102)), caseTok, nil)
	ps, _ := types.NewPropertySlice(map[string]any{key: []float32{1, 0}})
	n.SetProperties(ps)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	err := ts.CreateVectorIndex(caseTok, key, 3, DistanceCosine)
	if !errors.Is(err, ErrDimensionMismatch) {
		t.Fatalf("CreateVectorIndex error = %v, want ErrDimensionMismatch", err)
	}
	_, searchErr := ts.SearchNearestNodes(caseTok, key, []float32{1, 0, 0}, 1, QueryOpts{})
	if !errors.Is(searchErr, ErrVectorIndexNotFound) {
		t.Fatalf("SearchNearestNodes after failed create = %v, want ErrVectorIndexNotFound", searchErr)
	}
}

func TestTieredStore_VectorIndex_PutNodeRejectsMaintenanceDimensionMismatch(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	key := "vec"
	if err := ts.CreateVectorIndex(caseTok, key, 3, DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	n := types.NewNode(types.NodeID(snowflake.ID(105)), caseTok, nil)
	ps, _ := types.NewPropertySlice(map[string]any{key: []float32{1, 0}})
	n.SetProperties(ps)

	err := ts.PutNode(n)
	if !errors.Is(err, ErrDimensionMismatch) {
		t.Fatalf("PutNode error = %v, want ErrDimensionMismatch", err)
	}
	if _, err := ts.GetNode(n.ID()); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNode after failed PutNode = %v, want ErrNodeNotFound", err)
	}
}

func TestTieredStore_VectorIndex_SearchPropagatesCandidateCorruption(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	key := "vec"

	n := types.NewNode(types.NodeID(snowflake.ID(103)), caseTok, nil)
	ps, _ := types.NewPropertySlice(map[string]any{key: []float32{1, 0, 0}})
	n.SetProperties(ps)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := ts.CreateVectorIndex(caseTok, key, 3, DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}
	if err := ts.RefShardForTest().Flush(); err != nil {
		t.Fatalf("Flush ref shard: %v", err)
	}
	ts.RefShardForTest().NodeCacheForTest().ResetForTest()
	if err := ts.RefShardForTest().DBForTest().Update(func(txn *badgerv4.Txn) error {
		return txn.Set(storeutil.NodeKey(n.ID().SnowflakeID()), []byte("corrupt"))
	}); err != nil {
		t.Fatalf("corrupt node row: %v", err)
	}

	_, err := ts.SearchNearestNodes(caseTok, key, []float32{1, 0, 0}, 1, QueryOpts{})
	if err == nil {
		t.Fatal("SearchNearestNodes returned nil for corrupted vector candidate")
	}
	if errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("SearchNearestNodes error = %v, want corruption/operational error", err)
	}
}

func TestTieredStore_DeleteNodeWithHistoryPurgesVectorAfterCorruptShardCleanup(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	key := "vec"

	near := types.NewNode(types.NodeID(snowflake.ID(104)), caseTok, nil)
	nearProps, _ := types.NewPropertySlice(map[string]any{key: []float32{0, 0, 0}})
	near.SetProperties(nearProps)
	if err := ts.PutNode(near); err != nil {
		t.Fatalf("PutNode near: %v", err)
	}
	far := types.NewNode(types.NodeID(snowflake.ID(105)), caseTok, nil)
	farProps, _ := types.NewPropertySlice(map[string]any{key: []float32{10, 0, 0}})
	far.SetProperties(farProps)
	if err := ts.PutNode(far); err != nil {
		t.Fatalf("PutNode far: %v", err)
	}
	if err := ts.CreateVectorIndex(caseTok, key, 3, DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}
	if err := ts.RefShardForTest().Flush(); err != nil {
		t.Fatalf("Flush ref shard: %v", err)
	}
	ts.RefShardForTest().NodeCacheForTest().ResetForTest()
	if err := ts.RefShardForTest().DBForTest().Update(func(txn *badgerv4.Txn) error {
		return txn.Set(storeutil.NodeKey(near.ID().SnowflakeID()), []byte("corrupt"))
	}); err != nil {
		t.Fatalf("corrupt near node row: %v", err)
	}

	err := ts.DeleteNodeWithHistory(near.ID(), near.Version(), near.DeepCopy(), nil)
	if err == nil {
		t.Fatal("DeleteNodeWithHistory returned nil for corrupt node cleanup")
	}
	if errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("DeleteNodeWithHistory error = %v, want corruption/operational error", err)
	}

	nearest, err := ts.SearchNearestNodes(caseTok, key, []float32{0, 0, 0}, 1, QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes after corrupt delete cleanup: %v", err)
	}
	if len(nearest) != 1 || nearest[0].ID() != far.ID() {
		t.Fatalf("SearchNearestNodes after corrupt delete cleanup = %#v, want far node", nearest)
	}
}
