// Tests in this file pin two regressions in the round-2 capability
// fallbacks that round-3 / round-4 reviews flagged:
//
// R4-F9: nodesByLabelAndProperty must NOT match candidates whose stored
//        value is unindexable just because the queried value is also
//        unindexable. Both sides canonicalising to the empty key is "no
//        match", not "all match".
//
// R4-F10: the iterative over-fetch loop in IndexOps.SearchNearest must
//         clamp probe sizes to the over-fetch ceiling. For k larger than
//         the ceiling the loop must still run at least one iteration at
//         the ceiling (entry condition was rawK <= ceiling, which would
//         skip the loop entirely otherwise) and must not double past
//         the ceiling.

package core

import (
	"context"
	"errors"
	"math"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestNodesByLabelAndProperty_Fallback_UnindexableQueryValueReturnsEmpty(t *testing.T) {
	t.Parallel()
	store := &mandatoryOnlyStore{MandatoryStore: memory.New()}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// Seed two distinct nodes, each with a different unindexable
	// property value (slices). Their canonicalised key is "" for both.
	if _, err := g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{"v": []float32{1, 2, 3}}); err != nil {
		t.Fatalf("seed Doc1: %v", err)
	}
	if _, err := g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{"v": []float32{4, 5, 6}}); err != nil {
		t.Fatalf("seed Doc2: %v", err)
	}

	// Query with another unindexable value. The fallback must NOT
	// return the seeded nodes through "" == "" coincidence.
	got, err := g.Nodes.ByLabelAndProperty("Doc", "v", []float32{99, 99, 99}, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabelAndProperty: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got %d matches, want 0 (unindexable query/stored values must not match through the empty key)", len(got))
	}
}

func TestNodesByLabelAndProperty_Fallback_StoredUnindexableNotMatchedByQuery(t *testing.T) {
	t.Parallel()
	store := &mandatoryOnlyStore{MandatoryStore: memory.New()}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// Seed: Alice has an unindexable map property; Bob has a normal
	// scalar value.
	if _, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice", "extra": map[string]any{"k": "v"}}); err != nil {
		t.Fatalf("seed Alice: %v", err)
	}
	bob, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("seed Bob: %v", err)
	}

	// Query for an indexable scalar — must match only Bob and must
	// NOT match Alice through the stored map's empty canonical key.
	got, err := g.Nodes.ByLabelAndProperty("Person", "name", "Bob", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabelAndProperty: %v", err)
	}
	if len(got) != 1 || got[0].ID() != bob.ID() {
		t.Fatalf("got %+v, want exactly [%d]", got, bob.ID())
	}
}

// vectorSearchCountingStore wraps a memory.Store but masks
// FilteredVectorSearchCapability and counts SearchNearestNodes invocations
// with their k argument so a test can assert the over-fetch ceiling
// discipline without driving a 65k-node fixture.
type vectorSearchCountingStore struct {
	storepkg.MandatoryStore
	createVec func(uint16, string, int, storepkg.DistanceMetric) error
	dropVec   func(uint16, string) error
	searchVec func(uint16, string, []float32, int, storepkg.QueryOpts) ([]*types.Node, error)
	calls     []int
}

func (s *vectorSearchCountingStore) CreateVectorIndex(tok uint16, key string, dims int, metric storepkg.DistanceMetric) error {
	return s.createVec(tok, key, dims, metric)
}
func (s *vectorSearchCountingStore) DropVectorIndex(tok uint16, key string) error {
	return s.dropVec(tok, key)
}
func (s *vectorSearchCountingStore) SearchNearestNodes(tok uint16, key string, query []float32, k int, opts storepkg.QueryOpts) ([]*types.Node, error) {
	s.calls = append(s.calls, k)
	return s.searchVec(tok, key, query, k, opts)
}

var _ storepkg.VectorIndexCapability = (*vectorSearchCountingStore)(nil)

func newCountingVectorGraph(t *testing.T) (*Core, *vectorSearchCountingStore) {
	t.Helper()
	ms := memory.New()
	w := &vectorSearchCountingStore{
		MandatoryStore: ms,
		createVec:      ms.CreateVectorIndex,
		dropVec:        ms.DropVectorIndex,
		searchVec:      ms.SearchNearestNodes,
	}
	if _, ok := any(w).(storepkg.FilteredVectorSearchCapability); ok {
		t.Fatal("vectorSearchCountingStore unexpectedly satisfies FilteredVectorSearchCapability — test fixture is broken")
	}
	g, err := New(Config{Store: w})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g, w
}

func TestSearchNearest_Overfetch_KAboveCeiling_StillProbesCeiling(t *testing.T) {
	t.Parallel()
	g, store := newCountingVectorGraph(t)

	// Seed a small label cohort with eligible vectors.
	for i := 0; i < 3; i++ {
		if _, err := g.Nodes.Add(context.Background(), []string{"Target"}, map[string]any{"v": []float32{float32(i), 0, 0}}); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}
	if err := g.Index.CreateVector("Target", "v", 3, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVector: %v", err)
	}

	const overfetchCeiling = 1 << 16
	store.calls = nil
	// k strictly larger than the ceiling. Without the R4-F10 fix the
	// loop never executes (rawK > ceiling on entry) and we get an
	// empty result with zero backend calls. Without the later buffer
	// cap, math.MaxInt panics before the clamped backend call.
	got, err := g.Index.SearchNearest("Target", "v", []float32{0, 0, 0}, math.MaxInt, storepkg.QueryOpts{ValidAt: nowInstant() + 1_000})
	if err != nil {
		t.Fatalf("SearchNearest: %v", err)
	}
	if len(store.calls) == 0 {
		t.Fatalf("backend never called for k > ceiling — over-fetch loop skipped entirely")
	}
	for i, k := range store.calls {
		if k > overfetchCeiling {
			t.Errorf("call %d used k=%d, must be clamped to %d", i, k, overfetchCeiling)
		}
	}
	// The seeded targets are all eligible at any positive ValidAt
	// (their ValidFrom is <= now). The fallback should not crash; we
	// don't assert exact result count because the test fixture's
	// vectors are not separated enough to define a strict top-k under
	// cosine distance.
	_ = got
}

func TestSearchNearest_Overfetch_KZero_ValidatesBackendOnce(t *testing.T) {
	t.Parallel()
	g, store := newCountingVectorGraph(t)
	if _, err := g.Nodes.Add(context.Background(), []string{"Target"}, map[string]any{"v": []float32{1, 2, 3}}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := g.Index.CreateVector("Target", "v", 3, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVector: %v", err)
	}

	got, err := g.Index.SearchNearest("Target", "v", []float32{0, 0, 0}, 0, storepkg.QueryOpts{})
	if err != nil {
		t.Errorf("SearchNearest(k=0): %v, want nil", err)
	}
	if len(got) != 0 {
		t.Errorf("k=0 returned %d results, want 0", len(got))
	}
	if len(store.calls) != 1 || store.calls[0] != 0 {
		t.Errorf("k=0 must validate the vector index with one backend call at k=0; got calls %v", store.calls)
	}
}

// Compile-time keep-alive in case the test refactor renames the only
// production over-fetch test reference.
var _ = errors.Is
