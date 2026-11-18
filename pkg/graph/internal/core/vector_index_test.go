package core

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// newVectorLimitGraph is a memory-backed graph whose property keys are capped
// at maxKeyLen bytes, for the validation tests.
func newVectorLimitGraph(t *testing.T, maxKeyLen int) *Core {
	t.Helper()
	g, err := New(Config{Validation: ValidationLimits{MaxPropertyKeyLength: maxKeyLen}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

func TestVectorIndex_CreateAndSearch_Cosine(t *testing.T) {
	g := newTestGraph(t)
	label := "Item"
	key := "embedding"

	n1, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{1, 0, 0}})
	n2, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{0, 1, 0}})
	n3, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{1, 0.1, 0}})

	if err := g.Index.CreateVector(label, key, 3, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	// Query closest to [1, 0, 0]: n1 and n3 should rank before n2.
	query := []float32{1, 0, 0}
	results, err := g.Index.SearchNearest(label, key, query, 3, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	// Cosine distance to [1,0,0]: n1 = 0 (same direction), n3 ~ 0.005, n2 = 1
	// (orthogonal).
	got := vectorTestNodeIDs(results)
	want := []types.NodeID{n1.ID(), n3.ID(), n2.ID()}
	if len(got) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("cosine ranking = %v, want %v (n1, n3, n2)", got, want)
	}
}

func TestVectorIndex_CreateAndSearch_Euclidean(t *testing.T) {
	g := newTestGraph(t)
	label := "Vec"
	key := "v"

	n1, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{0, 0}})
	n2, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{1, 1}})
	n3, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{10, 10}})

	if err := g.Index.CreateVector(label, key, 2, storepkg.DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	results, err := g.Index.SearchNearest(label, key, []float32{0.1, 0.1}, 2, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	// Closest to [0.1, 0.1]: n1=[0,0] d≈0.14, n2=[1,1] d≈1.27; n3=[10,10] is
	// cut by k=2.
	got := vectorTestNodeIDs(results)
	if len(got) != 2 || got[0] != n1.ID() || got[1] != n2.ID() {
		t.Fatalf("euclidean top-2 = %v, want [n1=%d n2=%d] (n3=%d cut by k)", got, n1.ID(), n2.ID(), n3.ID())
	}
}

// TestVectorIndex_SearchNearestScored covers the requirement: GraphRAG
// rerankers need the distance score, not just rank order. The scored door
// returns the SAME nodes in the SAME order as SearchNearest, each paired with
// its distance from the query under the index's metric.
func TestVectorIndex_SearchNearestScored(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	label := "Item"
	key := "embedding"

	n1, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{1, 0, 0}})
	n2, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{0, 1, 0}})

	if err := g.Index.CreateVector(label, key, 3, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVector: %v", err)
	}

	query := []float32{1, 0, 0}
	hits, err := g.Index.SearchNearestScored(label, key, query, 2, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestScored: %v", err)
	}
	if len(hits) != 2 {
		t.Fatalf("expected 2 hits, got %d", len(hits))
	}
	if hits[0].Node.ID() != n1.ID() || hits[1].Node.ID() != n2.ID() {
		t.Fatalf("hit order diverged from SearchNearest ranking")
	}
	// Cosine distance: exact match = 0, orthogonal = 1.
	if hits[0].Distance > 1e-9 {
		t.Errorf("exact-match distance = %v, want ~0", hits[0].Distance)
	}
	if d := hits[1].Distance; d < 1-1e-9 || d > 1+1e-9 {
		t.Errorf("orthogonal cosine distance = %v, want ~1", d)
	}
	// Ordered non-decreasing by distance.
	if hits[0].Distance > hits[1].Distance {
		t.Errorf("hits not ordered by distance: %v > %v", hits[0].Distance, hits[1].Distance)
	}

	// TxPin is refused identically to the unscored door.
	if _, err := g.Index.SearchNearestScored(label, key, query, 2, storepkg.QueryOpts{TxPin: 100}); !errors.Is(err, ErrVectorSearchTxPinUnsupported) {
		t.Errorf("SearchNearestScored{TxPin} = %v, want ErrVectorSearchTxPinUnsupported", err)
	}
	// Non-positive k mirrors SearchNearest's nil, nil contract.
	if hits, err := g.Index.SearchNearestScored(label, key, query, 0, storepkg.QueryOpts{}); hits != nil || err != nil {
		t.Errorf("SearchNearestScored{k=0} = (%v, %v), want (nil, nil)", hits, err)
	}

	// Validation parity with SearchNearest: bad label, bad key, NaN query.
	if _, err := g.Index.SearchNearestScored("", key, query, 1, storepkg.QueryOpts{}); err == nil {
		t.Error("SearchNearestScored with empty label: want error, got nil")
	}
	if _, err := g.Index.SearchNearestScored(label, "tkg_reserved", query, 1, storepkg.QueryOpts{}); err == nil {
		t.Error("SearchNearestScored with reserved key: want error, got nil")
	}
	nanQuery := []float32{float32(math.NaN()), 0, 0}
	if _, err := g.Index.SearchNearestScored(label, key, nanQuery, 1, storepkg.QueryOpts{}); !errors.Is(err, storepkg.ErrInvalidVectorValue) {
		t.Errorf("SearchNearestScored with NaN query = %v, want ErrInvalidVectorValue", err)
	}
	if _, err := g.Index.SearchNearestScored("Missing", key, query, 1, storepkg.QueryOpts{}); !errors.Is(err, storepkg.ErrVectorIndexNotFound) {
		t.Errorf("SearchNearestScored on unindexed label = %v, want ErrVectorIndexNotFound", err)
	}

	// Tx-side mirror returns the same scored hits.
	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.SearchNearestScored("", key, query, 1, storepkg.QueryOpts{}); err == nil {
		t.Error("tx.SearchNearestScored with empty label: want error, got nil")
	}
	if _, err := tx.SearchNearestScored(label, "tkg_reserved", query, 1, storepkg.QueryOpts{}); err == nil {
		t.Error("tx.SearchNearestScored with reserved key: want error, got nil")
	}
	txHits, err := tx.SearchNearestScored(label, key, query, 2, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("tx.SearchNearestScored: %v", err)
	}
	if len(txHits) != 2 || txHits[0].Node.ID() != n1.ID() || txHits[0].Distance != hits[0].Distance {
		t.Errorf("tx mirror diverged from standalone scored search")
	}
	if _, err := tx.SearchNearestScored(label, key, query, 2, storepkg.QueryOpts{TxPin: 100}); !errors.Is(err, ErrVectorSearchTxPinUnsupported) {
		t.Errorf("tx.SearchNearestScored{TxPin} = %v, want ErrVectorSearchTxPinUnsupported", err)
	}
}

// TestVectorIndex_TxPinRejected pins the SearchNearest × TxPin contract
// : the vector index holds only LATEST vectors and drops
// deleted nodes, so a belief-state (AS OF SYSTEM TIME) ranking is ill-defined
// — a node hard-deleted after the pin would be silently missing and distances
// would rank by post-pin vectors. The door rejects TxPin explicitly instead of
// returning a silently wrong belief state. Both doors (standalone + tx mirror)
// funnel through searchNearestLocked, so both must reject.
func TestVectorIndex_TxPinRejected(t *testing.T) {
	g := newTestGraph(t)
	label := "Doc"
	key := "embedding"

	if _, err := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{1, 0}}); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := g.Index.CreateVector(label, key, 2, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	pin, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx: %v", err)
	}

	_, err = g.Index.SearchNearest(label, key, []float32{1, 0}, 1, storepkg.QueryOpts{TxPin: pin})
	if !errors.Is(err, ErrVectorSearchTxPinUnsupported) {
		t.Fatalf("SearchNearest with TxPin: err = %v, want ErrVectorSearchTxPinUnsupported", err)
	}

	// TxPin combined with another temporal opt keeps the earlier, more specific
	// conflict error from the shared scan validation.
	_, err = g.Index.SearchNearest(label, key, []float32{1, 0}, 1, storepkg.QueryOpts{TxPin: pin, ValidAt: pin})
	if !errors.Is(err, ErrConflictingTemporalOpts) {
		t.Fatalf("SearchNearest with TxPin+ValidAt: err = %v, want ErrConflictingTemporalOpts", err)
	}

	// Tx-side mirror must reject identically.
	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	_, err = tx.SearchNearest(label, key, []float32{1, 0}, 1, storepkg.QueryOpts{TxPin: pin})
	if !errors.Is(err, ErrVectorSearchTxPinUnsupported) {
		t.Fatalf("tx.SearchNearest with TxPin: err = %v, want ErrVectorSearchTxPinUnsupported", err)
	}

	// A pin-free search on the same graph still works.
	results, err := g.Index.SearchNearest(label, key, []float32{1, 0}, 1, storepkg.QueryOpts{})
	if err != nil || len(results) != 1 {
		t.Fatalf("pin-free SearchNearest = (%d results, %v), want (1, nil)", len(results), err)
	}
}

// TestVectorIndex_Float64Embeddings_Indexed covers the requirement: a Go
// embedder storing []float64 embeddings must get an INDEXED vector (narrowed
// to float32), not a silently unindexed property. Exercises both index-build
// (node added before CreateVector) and auto-maintenance (node added after).
func TestVectorIndex_Float64Embeddings_Indexed(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	label := "Doc"
	key := "embedding"

	// Added BEFORE index creation — picked up by the index build.
	n1, err := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float64{1, 0, 0}})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	if err := g.Index.CreateVector(label, key, 3, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	// Added AFTER index creation — picked up by auto-maintenance.
	n2, err := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float64{0, 1, 0}})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	results, err := g.Index.SearchNearest(label, key, []float32{1, 0, 0}, 2, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearest: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected both []float64-embedded nodes indexed, got %d results", len(results))
	}
	if results[0].ID() != n1.ID() {
		t.Errorf("first result should be n1 (exact match), got other")
	}
	if results[1].ID() != n2.ID() {
		t.Errorf("second result should be n2 (orthogonal), got other")
	}
}

func TestVectorIndex_AutoMaintained_OnAdd(t *testing.T) {
	g := newTestGraph(t)
	label := "E"
	key := "v"

	// Register the label by adding a seed node without a vector, then create the index.
	_, _ = g.Nodes.Add(context.Background(), []string{label}, nil) // registers label; no vector property

	if err := g.Index.CreateVector(label, key, 2, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	// Add a node with a vector AFTER the index was created — should be auto-indexed.
	n, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{1, 0}})

	results, err := g.Index.SearchNearest(label, key, []float32{1, 0}, 1, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(results) != 1 || results[0].ID() != n.ID() {
		t.Error("node added after index creation should be searchable")
	}
}

func TestVectorIndex_AutoMaintained_OnDelete(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	label := "E"
	key := "v"

	n, _ := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{1, 0}})
	id := n.ID()

	if err := g.Index.CreateVector(label, key, 2, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}
	pre, err := g.Index.SearchNearest(label, key, []float32{1, 0}, 10, storepkg.QueryOpts{})
	if err != nil || len(pre) != 1 || pre[0].ID() != id {
		t.Fatalf("pre-delete search = (%v, %v), want [%d]", vectorTestNodeIDs(pre), err, id)
	}

	if err := g.Nodes.Delete(context.Background(), id); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	// The deleted node was the only vector: the index is empty afterwards.
	results, err := g.Index.SearchNearest(label, key, []float32{1, 0}, 10, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("post-delete search = %v, want empty (the only indexed node was deleted)", vectorTestNodeIDs(results))
	}
}

func TestVectorIndex_DimensionMismatch(t *testing.T) {
	g := newTestGraph(t)
	label := "X"
	key := "v"
	_, _ = g.Nodes.Add(context.Background(), []string{label}, nil) // register label

	if err := g.Index.CreateVector(label, key, 3, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	_, err := g.Index.SearchNearest(label, key, []float32{1, 0}, 1, storepkg.QueryOpts{})
	if !errors.Is(err, ErrDimensionMismatch) {
		t.Errorf("expected ErrDimensionMismatch for wrong query dims, got %v", err)
	}
}

func TestVectorIndex_IndexAlreadyExists(t *testing.T) {
	g := newTestGraph(t)
	label := "X"
	key := "v"
	_, _ = g.Nodes.Add(context.Background(), []string{label}, nil)

	_ = g.Index.CreateVector(label, key, 2, storepkg.DistanceCosine)
	err := g.Index.CreateVector(label, key, 2, storepkg.DistanceCosine)
	if !errors.Is(err, ErrVectorIndexExists) {
		t.Errorf("expected ErrVectorIndexExists, got %v", err)
	}
}

func TestVectorIndex_IndexNotFound(t *testing.T) {
	g := newTestGraph(t)
	label := "X"
	_, _ = g.Nodes.Add(context.Background(), []string{label}, nil)

	err := g.Index.DeleteVector(label, "nonexistent")
	if !errors.Is(err, ErrVectorIndexNotFound) {
		t.Errorf("expected ErrVectorIndexNotFound, got %v", err)
	}
}

func TestVectorIndex_CreateBeforeLabelExistsIndexesFutureNodes(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	label := "Ghost"
	key := "v"

	if err := g.Index.CreateVector(label, key, 2, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVector before label registration: %v", err)
	}

	n, err := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{1, 0}})
	if err != nil {
		t.Fatalf("Add future indexed node: %v", err)
	}

	results, err := g.Index.SearchNearest(label, key, []float32{1, 0}, 1, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearest after future node add: %v", err)
	}
	if len(results) != 1 || results[0].ID() != n.ID() {
		t.Fatalf("future node not indexed: got %v, want %d", results, n.ID().SnowflakeID())
	}
}

func TestIndexCreateRejectsInvalidLabelAndPropertyKey(t *testing.T) {
	t.Parallel()
	g := newVectorLimitGraph(t, 3)

	creates := []struct {
		name string
		fn   func() error
		want error
	}{
		{
			name: "property empty label",
			fn:   func() error { return g.Index.CreateProperty(" ", "key") },
			want: ErrEmptyName,
		},
		{
			name: "temporal empty label",
			fn:   func() error { return g.Index.CreateTemporal("") },
			want: ErrEmptyName,
		},
		{
			name: "high frequency empty label",
			fn:   func() error { return g.Index.CreateHighFrequency("\t", 0) },
			want: ErrEmptyName,
		},
		{
			name: "vector empty label",
			fn:   func() error { return g.Index.CreateVector("", "key", 2, storepkg.DistanceCosine) },
			want: ErrEmptyName,
		},
		{
			name: "high frequency non-positive bucket",
			fn:   func() error { return g.Index.CreateHighFrequency("Doc", 0) },
			want: ErrInvalidTemporalIndexConfig,
		},
		{
			name: "high frequency negative bucket",
			fn:   func() error { return g.Index.CreateHighFrequency("Doc", -time.Second) },
			want: ErrInvalidTemporalIndexConfig,
		},
		{
			name: "high frequency sub-millisecond bucket",
			fn:   func() error { return g.Index.CreateHighFrequency("Doc", time.Nanosecond) },
			want: ErrInvalidTemporalIndexConfig,
		},
		{
			name: "high frequency fractional millisecond bucket",
			fn:   func() error { return g.Index.CreateHighFrequency("Doc", 1500*time.Microsecond) },
			want: ErrInvalidTemporalIndexConfig,
		},
		{
			name: "property key too long",
			fn:   func() error { return g.Index.CreateProperty("Doc", "long") },
			want: ErrKeyTooLong,
		},
		{
			name: "property reserved key",
			fn:   func() error { return g.Index.CreateProperty("Doc", "tkg_hash") },
			want: types.ErrReservedPrefix,
		},
		{
			name: "vector property key too long",
			fn:   func() error { return g.Index.CreateVector("Doc", "long", 2, storepkg.DistanceCosine) },
			want: ErrKeyTooLong,
		},
		{
			name: "vector reserved property key",
			fn:   func() error { return g.Index.CreateVector("Doc", "tkg_hash", 2, storepkg.DistanceCosine) },
			want: types.ErrReservedPrefix,
		},
		{
			name: "vector zero dimensions",
			fn:   func() error { return g.Index.CreateVector("Doc", "v", 0, storepkg.DistanceCosine) },
			want: ErrInvalidVectorIndexConfig,
		},
		{
			name: "vector unsupported metric",
			fn:   func() error { return g.Index.CreateVector("Doc", "v", 2, storepkg.DistanceMetric(99)) },
			want: ErrInvalidVectorIndexConfig,
		},
	}
	for _, tc := range creates {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.fn(); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestVectorIndex_SearchEmpty_ReturnsNil(t *testing.T) {
	g := newTestGraph(t)
	label := "Empty"
	key := "v"
	_, _ = g.Nodes.Add(context.Background(), []string{label}, nil) // register label, no vector property

	if err := g.Index.CreateVector(label, key, 2, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	results, err := g.Index.SearchNearest(label, key, []float32{1, 0}, 5, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("expected nil error for empty index, got %v", err)
	}
	if results != nil {
		t.Errorf("expected nil results for empty index, got %v", results)
	}
}

func TestVectorIndex_SearchNotFound_ReturnsError(t *testing.T) {
	g := newTestGraph(t)
	label := "X"
	_, _ = g.Nodes.Add(context.Background(), []string{label}, nil)

	_, err := g.Index.SearchNearest(label, "missing", []float32{1, 0}, 1, storepkg.QueryOpts{})
	if !errors.Is(err, ErrVectorIndexNotFound) {
		t.Errorf("expected ErrVectorIndexNotFound when no index, got %v", err)
	}
}

func TestVectorIndex_SearchRejectsInvalidAndUnknownTargets(t *testing.T) {
	g := newVectorLimitGraph(t, 4)
	_, _ = g.Nodes.Add(context.Background(), []string{"Person"}, nil)

	tests := []struct {
		name string
		run  func() error
		want error
	}{
		{
			name: "empty label",
			run: func() error {
				_, err := g.Index.SearchNearest("", "vec", []float32{1, 0}, 1, storepkg.QueryOpts{})
				return err
			},
			want: ErrEmptyName,
		},
		{
			name: "property key too long",
			run: func() error {
				_, err := g.Index.SearchNearest("Person", "long-key", []float32{1, 0}, 1, storepkg.QueryOpts{})
				return err
			},
			want: ErrKeyTooLong,
		},
		{
			name: "reserved property key",
			run: func() error {
				_, err := g.Index.SearchNearest("Person", "tkg_hash", []float32{1, 0}, 1, storepkg.QueryOpts{})
				return err
			},
			want: types.ErrReservedPrefix,
		},
		{
			name: "unknown label",
			run: func() error {
				_, err := g.Index.SearchNearest("Missing", "vec", []float32{1, 0}, 1, storepkg.QueryOpts{})
				return err
			},
			want: ErrVectorIndexNotFound,
		},
		{
			name: "missing vector index",
			run: func() error {
				_, err := g.Index.SearchNearest("Person", "vec", []float32{1, 0}, 1, storepkg.QueryOpts{})
				return err
			},
			want: ErrVectorIndexNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.run(); !errors.Is(err, tc.want) {
				t.Fatalf("%s error = %v, want %v", tc.name, err, tc.want)
			}
		})
	}
}

func TestVectorIndex_SearchValidatesTargetsBeforeNonPositiveKShortcut(t *testing.T) {
	g := newVectorLimitGraph(t, 4)
	_, _ = g.Nodes.Add(context.Background(), []string{"Person"}, nil)

	tests := []struct {
		name string
		run  func() error
		want error
	}{
		{
			name: "empty label",
			run: func() error {
				_, err := g.Index.SearchNearest(" ", "vec", []float32{1, 0}, 0, storepkg.QueryOpts{})
				return err
			},
			want: ErrEmptyName,
		},
		{
			name: "reserved property key",
			run: func() error {
				_, err := g.Index.SearchNearest("Person", "tkg_hash", []float32{1, 0}, 0, storepkg.QueryOpts{})
				return err
			},
			want: types.ErrReservedPrefix,
		},
		{
			name: "unknown label",
			run: func() error {
				_, err := g.Index.SearchNearest("Missing", "vec", []float32{1, 0}, 0, storepkg.QueryOpts{})
				return err
			},
			want: ErrVectorIndexNotFound,
		},
		{
			name: "missing vector index",
			run: func() error {
				_, err := g.Index.SearchNearest("Person", "vec", []float32{1, 0}, 0, storepkg.QueryOpts{})
				return err
			},
			want: ErrVectorIndexNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.run(); !errors.Is(err, tc.want) {
				t.Fatalf("%s error = %v, want %v", tc.name, err, tc.want)
			}
		})
	}
}

func TestVectorIndex_DropAndRecreate(t *testing.T) {
	g := newTestGraph(t)
	label := "D"
	key := "v"
	_, _ = g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{1, 0}})

	if err := g.Index.CreateVector(label, key, 2, storepkg.DistanceCosine); err != nil {
		t.Fatalf("first CreateVectorIndex: %v", err)
	}

	if err := g.Index.DeleteVector(label, key); err != nil {
		t.Fatalf("DropVectorIndex: %v", err)
	}

	// Should be able to create again after drop.
	if err := g.Index.CreateVector(label, key, 2, storepkg.DistanceCosine); err != nil {
		t.Fatalf("second CreateVectorIndex after drop: %v", err)
	}
}

// TestVectorIndex_CreateWithOptions_AppliesTuning drives CreateVectorWithOptions
// through the real graph -> core -> store path (not a spy) with a non-default
// VectorIndexOptions and confirms it actually reached the backend: the
// options are readable back via the store's test-only accessor and the
// brute-force engine choice is observable in search results.
func TestVectorIndex_CreateWithOptions_AppliesTuning(t *testing.T) {
	g := newTestGraph(t)
	label := "Item"
	key := "embedding"

	n1, err := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{1, 0, 0}})
	if err != nil {
		t.Fatalf("Add n1: %v", err)
	}
	if _, err := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: []float32{0, 1, 0}}); err != nil {
		t.Fatalf("Add n2: %v", err)
	}

	opts := storepkg.VectorIndexOptions{UseBruteForce: true, M: 8, EfConstruction: 50, EfSearch: 10}
	if err := g.Index.CreateVectorWithOptions(label, key, 3, storepkg.DistanceCosine, opts); err != nil {
		t.Fatalf("CreateVectorWithOptions: %v", err)
	}

	tok, ok := g.labels.Lookup(label)
	if !ok {
		t.Fatal("label token not found after CreateVectorWithOptions")
	}
	ms, ok := g.store.(*memory.Store)
	if !ok {
		t.Fatalf("default store = %T, want *memory.Store", g.store)
	}
	got, ok := ms.VectorIndexOptionsForTest(tok, key)
	if !ok {
		t.Fatal("VectorIndexOptionsForTest: index not found")
	}
	if got != opts {
		t.Fatalf("VectorIndexOptionsForTest = %+v, want %+v", got, opts)
	}

	results, err := g.Index.SearchNearest(label, key, []float32{1, 0, 0}, 1, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearest: %v", err)
	}
	if len(results) != 1 || results[0].ID() != n1.ID() {
		t.Fatalf("SearchNearest (brute force tuning applied) = %#v, want n1", results)
	}
}

// TestVectorIndex_CreateWithOptions_ZeroValueMatchesPlainCreate confirms the
// documented equivalence: CreateVector == CreateVectorWithOptions with a
// zero-value VectorIndexOptions.
func TestVectorIndex_CreateWithOptions_ZeroValueMatchesPlainCreate(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	label := "Item"
	key := "embedding"

	if err := g.Index.CreateVector(label, key, 3, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVector: %v", err)
	}
	tok, ok := g.labels.Lookup(label)
	if !ok {
		t.Fatal("label token not found after CreateVector")
	}
	ms, ok := g.store.(*memory.Store)
	if !ok {
		t.Fatalf("default store = %T, want *memory.Store", g.store)
	}
	got, ok := ms.VectorIndexOptionsForTest(tok, key)
	if !ok {
		t.Fatal("VectorIndexOptionsForTest: index not found")
	}
	if got != (storepkg.VectorIndexOptions{}) {
		t.Fatalf("VectorIndexOptionsForTest after plain CreateVector = %+v, want zero value", got)
	}
}

func TestVectorIndex_toFloat32Slice_MixedAny(t *testing.T) {
	t.Parallel()
	// Test that []any containing float64 values is accepted by the index.
	g := newTestGraph(t)
	label := "M"
	key := "v"

	// Store as []any of float64 (what JSON unmarshaling produces).
	mixedVec := []any{float64(1), float64(0)}
	n, err := g.Nodes.Add(context.Background(), []string{label}, map[string]any{key: mixedVec})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	if err := g.Index.CreateVector(label, key, 2, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	results, err := g.Index.SearchNearest(label, key, []float32{1, 0}, 1, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(results) != 1 || results[0].ID() != n.ID() {
		t.Error("node with []any float64 vector should be indexed")
	}
}
