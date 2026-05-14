// Tests in this file pin the round-2 review's R2-F5 finding: vector
// search under a temporal filter must converge on k eligible results
// even when the underlying backend cannot pre-filter. The graph layer
// uses iterative over-fetch (k → 2k → 4k …) up to a bounded ceiling
// against `SearchNearestNodes` for backends that do not satisfy
// `FilteredVectorSearchCapability`.

package core

import (
	"context"
	"errors"
	"testing"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store/memory"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
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

type filteredHistoryFaultStore struct {
	*memory.Store
	failID types.NodeID
	err    error
}

func (s *filteredHistoryFaultStore) GetNodeHistory(id types.NodeID) ([]*types.Node, error) {
	if id == s.failID {
		return nil, s.err
	}
	return s.Store.GetNodeHistory(id)
}

type mandatoryHistoryFaultStore struct {
	storepkg.MandatoryStore
	failID types.NodeID
	err    error
}

func (s *mandatoryHistoryFaultStore) GetNodeHistory(id types.NodeID) ([]*types.Node, error) {
	if id == s.failID {
		return nil, s.err
	}
	return s.MandatoryStore.GetNodeHistory(id)
}

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

func TestSearchNearest_TemporalFilteredPath_PropagatesCandidateResolutionError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("history read failed")
	ms := memory.New()
	store := &filteredHistoryFaultStore{Store: ms, err: errBoom}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	node, err := g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{"embedding": []float32{1, 0}})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	store.failID = node.ID()
	if err := g.Index.CreateVector("Doc", "embedding", 2, storepkg.DistanceEuclidean); err != nil {
		t.Fatalf("CreateVector: %v", err)
	}

	_, err = g.Index.SearchNearest("Doc", "embedding", []float32{1, 0}, 1, storepkg.QueryOpts{ValidAt: nowInstant() + 1_000})
	if !errors.Is(err, errBoom) {
		t.Fatalf("SearchNearest filtered path error = %v, want %v", err, errBoom)
	}
}

func TestSearchNearest_TemporalOverfetchPath_PropagatesCandidateResolutionError(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("history read failed")
	ms := memory.New()
	fault := &mandatoryHistoryFaultStore{MandatoryStore: ms, err: errBoom}
	wrapped := &noFilterPushDownStore{
		MandatoryStore: fault,
		createVec:      ms.CreateVectorIndex,
		dropVec:        ms.DropVectorIndex,
		searchVec:      ms.SearchNearestNodes,
	}
	g, err := New(Config{Store: wrapped})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	node, err := g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{"embedding": []float32{1, 0}})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	fault.failID = node.ID()
	if err := g.Index.CreateVector("Doc", "embedding", 2, storepkg.DistanceEuclidean); err != nil {
		t.Fatalf("CreateVector: %v", err)
	}

	_, err = g.Index.SearchNearest("Doc", "embedding", []float32{1, 0}, 1, storepkg.QueryOpts{ValidAt: nowInstant() + 1_000})
	if !errors.Is(err, errBoom) {
		t.Fatalf("SearchNearest overfetch path error = %v, want %v", err, errBoom)
	}
}

func TestVectorOverfetch_TemporalFilter_FindsEligibleBeyondInitialK(t *testing.T) {
	t.Parallel()

	// Strategy: place several nodes' embeddings on a line, with the closest
	// current rows being temporally ineligible at t0. With k=2, an unfiltered
	// backend returns only those close-but-too-new candidates on the first
	// probe; the graph-layer fallback must over-fetch until it finds the two
	// farther rows that are valid at t0.

	ms := memory.New()
	var rawKs []int
	wrapped := &noFilterPushDownStore{
		MandatoryStore: ms,
		createVec:      ms.CreateVectorIndex,
		dropVec:        ms.DropVectorIndex,
		searchVec: func(tok uint16, key string, query []float32, k int, opts storepkg.QueryOpts) ([]*types.Node, error) {
			rawKs = append(rawKs, k)
			return ms.SearchNearestNodes(tok, key, query, k, opts)
		},
	}

	g, err := New(Config{Store: wrapped})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	const (
		tOld = types.Instant(1)
		t0   = types.Instant(100)
		tNew = types.Instant(200)
	)
	for i := 0; i < 3; i++ {
		if _, err := g.Nodes.Add(context.Background(), []string{"Target"}, map[string]any{
			"v":              []float32{float32(i) * 0.1, 0},
			"tkg_valid_from": tNew,
		}); err != nil {
			t.Fatalf("seed too-new Target %d: %v", i, err)
		}
	}
	t1, err := g.Nodes.Add(context.Background(), []string{"Target"}, map[string]any{
		"v":              []float32{10, 0},
		"tkg_valid_from": tOld,
	})
	if err != nil {
		t.Fatalf("seed Target 1: %v", err)
	}
	t2, err := g.Nodes.Add(context.Background(), []string{"Target"}, map[string]any{
		"v":              []float32{11, 0},
		"tkg_valid_from": tOld,
	})
	if err != nil {
		t.Fatalf("seed Target 2: %v", err)
	}

	if err := g.Index.CreateVector("Target", "v", 2, storepkg.DistanceEuclidean); err != nil {
		t.Fatalf("CreateVector(Target): %v", err)
	}

	noFilterResults, err := g.Index.SearchNearest("Target", "v", []float32{0, 0}, 2, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("baseline SearchNearest: %v", err)
	}
	if len(noFilterResults) != 2 {
		t.Fatalf("baseline returned %d, want 2 raw nearest nodes", len(noFilterResults))
	}
	for _, n := range noFilterResults {
		if n.ID() == t1.ID() || n.ID() == t2.ID() {
			t.Fatalf("baseline raw top-2 included eligible far node %d; setup no longer exercises over-fetch", n.ID())
		}
	}

	rawKs = rawKs[:0]
	results, err := g.Index.SearchNearest("Target", "v", []float32{0, 0}, 2, storepkg.QueryOpts{ValidAt: t0})
	if err != nil {
		t.Fatalf("temporal SearchNearest: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("temporal SearchNearest returned %d nodes, want 2 eligible nodes", len(results))
	}
	got := map[types.NodeID]struct{}{results[0].ID(): {}, results[1].ID(): {}}
	if _, ok := got[t1.ID()]; !ok {
		t.Fatalf("temporal SearchNearest missing eligible node %d; got %v", t1.ID(), vectorTestNodeIDs(results))
	}
	if _, ok := got[t2.ID()]; !ok {
		t.Fatalf("temporal SearchNearest missing eligible node %d; got %v", t2.ID(), vectorTestNodeIDs(results))
	}
	if len(rawKs) < 2 {
		t.Fatalf("temporal SearchNearest used raw k probes %v, want over-fetch beyond initial k", rawKs)
	}
	if rawKs[0] != 2 || rawKs[len(rawKs)-1] <= rawKs[0] {
		t.Fatalf("temporal SearchNearest raw k probes = %v, want growth beyond initial k=2", rawKs)
	}
}
