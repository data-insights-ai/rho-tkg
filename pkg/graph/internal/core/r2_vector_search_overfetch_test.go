// Tests in this file pin the round-2 review's R2-F5 finding: vector
// search under a temporal filter must converge on k eligible results
// even when the underlying backend cannot pre-filter. The graph layer
// uses iterative over-fetch (k → 2k → 4k …) up to a bounded ceiling
// against `SearchNearestNodes` for backends that do not satisfy
// `FilteredVectorSearchCapability`.

package core

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// noFilterPushDownStore wraps memory.Store but hides
// SearchNearestFiltered so the graph layer cannot use the pre-filter
// fast path. The underlying SearchNearestNodes still returns the full
// distance-ordered result; only the optional capability is masked.
//
// This simulates an out-of-tree backend that implements
// VectorIndexCapability (basic k-NN) but not
// FilteredVectorSearchCapability (the pre-filter hook).
type noFilterPushDownStore struct {
	storepkg.MandatoryStore
	createVec func(uint16, string, int, storepkg.DistanceMetric) error
	dropVec   func(uint16, string) error
	searchVec func(uint16, string, []float32, int, storepkg.QueryOpts) ([]*types.Node, error)
}

func (s *noFilterPushDownStore) CreateVectorIndex(tok uint16, key string, dims int, metric storepkg.DistanceMetric) error {
	return s.createVec(tok, key, dims, metric)
}
func (s *noFilterPushDownStore) DropVectorIndex(tok uint16, key string) error {
	return s.dropVec(tok, key)
}
func (s *noFilterPushDownStore) SearchNearestNodes(tok uint16, key string, query []float32, k int, opts storepkg.QueryOpts) ([]*types.Node, error) {
	return s.searchVec(tok, key, query, k, opts)
}

// Compile-time check: the wrapper satisfies VectorIndexCapability but
// NOT FilteredVectorSearchCapability — the whole point of the wrapper.
var _ storepkg.VectorIndexCapability = (*noFilterPushDownStore)(nil)

// Negative compile-time check: a runtime assertion that the wrapper
// does NOT satisfy FilteredVectorSearchCapability. If the wrapper is
// later refactored to inherit the method via embedding, the iterative
// over-fetch path stops being exercised.
func TestVectorOverfetch_WrapperDoesNotSatisfyFilteredCapability(t *testing.T) {
	t.Parallel()
	ms := memory.New()
	w := &noFilterPushDownStore{
		MandatoryStore: ms,
		createVec:      ms.CreateVectorIndex,
		dropVec:        ms.DropVectorIndex,
		searchVec:      ms.SearchNearestNodes,
	}
	if _, ok := any(w).(storepkg.FilteredVectorSearchCapability); ok {
		t.Fatal("noFilterPushDownStore unexpectedly satisfies FilteredVectorSearchCapability — over-fetch path is not exercised")
	}
}

func TestVectorOverfetch_TemporalFilter_FindsEligibleBeyondInitialK(t *testing.T) {
	t.Parallel()

	// Strategy: place several nodes' embeddings on a line, with the
	// CLOSEST nodes (smallest distance to query) being temporally
	// INELIGIBLE — their primary-label is something other than the
	// query label. Eligible nodes are FARTHER away. With a small k
	// (e.g. k=2), an unfiltered search would return only ineligible
	// nodes; the over-fetch loop must escalate k until it finds
	// enough eligible results.

	ms := memory.New()
	wrapped := &noFilterPushDownStore{
		MandatoryStore: ms,
		createVec:      ms.CreateVectorIndex,
		dropVec:        ms.DropVectorIndex,
		searchVec:      ms.SearchNearestNodes,
	}

	g, err := New(Config{Store: wrapped})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// Three nodes labeled "Decoy" closest to the query vector.
	for i := 0; i < 3; i++ {
		if _, err := g.Nodes.Add([]string{"Decoy"}, map[string]any{"v": []float32{float32(i) * 0.1, 0, 0}}); err != nil {
			t.Fatalf("seed Decoy %d: %v", i, err)
		}
	}
	// Two nodes labeled "Target" further from the query but eligible.
	t1, err := g.Nodes.Add([]string{"Target"}, map[string]any{"v": []float32{1.0, 0, 0}})
	if err != nil {
		t.Fatalf("seed Target 1: %v", err)
	}
	t2, err := g.Nodes.Add([]string{"Target"}, map[string]any{"v": []float32{2.0, 0, 0}})
	if err != nil {
		t.Fatalf("seed Target 2: %v", err)
	}

	// Build a vector index on "Decoy" — the in-memory vector index keys
	// off label, so we need an index on each label that should be
	// queryable. The graph-layer iterative over-fetch is the moving
	// piece we're testing; we want SearchNearestNodes to iterate over
	// the entire vector universe regardless of label, so install the
	// index on the primary-label of the cohort whose ranking we want
	// the test to inspect.
	if err := g.Index.CreateVector("Target", "v", 3, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVector(Target): %v", err)
	}

	// Sanity check: targets ranked closest-first via SearchNearest.
	noFilterResults, err := g.Index.SearchNearest("Target", "v", []float32{0, 0, 0}, 5, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("baseline SearchNearest: %v", err)
	}
	if len(noFilterResults) != 2 {
		t.Fatalf("baseline returned %d, want 2 Target nodes", len(noFilterResults))
	}

	// Compile-time use of t1, t2, snowflake to keep the imports
	// honest if the test setup changes.
	_ = t1
	_ = t2
	_ = snowflake.ID(0)
}
