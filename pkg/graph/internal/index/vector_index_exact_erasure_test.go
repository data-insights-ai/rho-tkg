package index

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

func TestVectorIndexRemoveExactRebuildsAwayHNSWTombstone(t *testing.T) {
	vi := &VectorIndex{Dims: 2, Metric: storepkg.DistanceEuclidean}
	if err := vi.Add(snowflake.ID(1), []float32{123.5, 456.5}); err != nil {
		t.Fatal(err)
	}
	if err := vi.Add(snowflake.ID(2), []float32{1, 2}); err != nil {
		t.Fatal(err)
	}
	vi.mu.Lock()
	vi.ensurePositionsLocked()
	vi.ensureHNSWLocked()
	vi.mu.Unlock()

	vi.RemoveExact(snowflake.ID(1))
	vi.mu.RLock()
	defer vi.mu.RUnlock()
	if len(vi.entries) != 1 || vi.entries[0].id != snowflake.ID(2) {
		t.Fatalf("live entries after exact remove = %+v", vi.entries)
	}
	if vi.hnsw == nil || len(vi.hnsw.nodes) != 1 || vi.hnsw.nodes[0].extID != snowflake.ID(2) {
		t.Fatalf("HNSW retained erased vector node: %+v", vi.hnsw)
	}
	if vi.hnsw.tombstones != 0 {
		t.Fatalf("HNSW tombstones = %d, want 0", vi.hnsw.tombstones)
	}
}
