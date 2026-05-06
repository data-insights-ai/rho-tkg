package graph

import (
	"container/heap"
	"errors"
	"math"
	"sync"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// knnEntry is a candidate result for k-nearest-neighbor search.
type knnEntry struct {
	id   snowflake.ID
	dist float64
}

// knnHeap is a max-heap by dist: the root is the farthest of the k-best candidates.
// Keeping a max-heap of size k lets us evict the worst candidate in O(log k) time.
type knnHeap []knnEntry

func (h knnHeap) Len() int           { return len(h) }
func (h knnHeap) Less(i, j int) bool { return h[i].dist > h[j].dist } // max-heap
func (h knnHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *knnHeap) Push(x any)        { *h = append(*h, x.(knnEntry)) }
func (h *knnHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// DistanceMetric determines how similarity is measured between two vectors.
type DistanceMetric uint8

const (
	DistanceCosine DistanceMetric = iota + 1
	DistanceEuclidean
)

// vectorIndexKey uniquely identifies a vector index by label token and property key.
type vectorIndexKey struct {
	labelToken  uint16
	propertyKey string
}

// vectorEntry stores a node ID and its associated vector.
type vectorEntry struct {
	id  snowflake.ID
	vec []float32
}

// vectorIndex is an in-memory brute-force k-nearest-neighbor index.
// O(n × dims) per query. Not persisted — must be rebuilt on restart.
type vectorIndex struct {
	mu      sync.RWMutex
	entries []vectorEntry
	dims    int
	metric  DistanceMetric
}

// add inserts or updates a vector entry for the given ID.
// Returns ErrDimensionMismatch if the vector length differs from the index's expected dimensions.
func (vi *vectorIndex) add(id snowflake.ID, vec []float32) error {
	if vi.dims > 0 && len(vec) != vi.dims {
		return ErrDimensionMismatch
	}
	vi.mu.Lock()
	defer vi.mu.Unlock()

	// Replace existing entry if present.
	for i, e := range vi.entries {
		if e.id == id {
			cp := make([]float32, len(vec))
			copy(cp, vec)
			vi.entries[i].vec = cp
			return nil
		}
	}

	cp := make([]float32, len(vec))
	copy(cp, vec)
	vi.entries = append(vi.entries, vectorEntry{id: id, vec: cp})
	return nil
}

// remove deletes the entry for the given ID. No-op if not present.
func (vi *vectorIndex) remove(id snowflake.ID) {
	vi.mu.Lock()
	defer vi.mu.Unlock()

	for i, e := range vi.entries {
		if e.id == id {
			vi.entries[i] = vi.entries[len(vi.entries)-1]
			vi.entries = vi.entries[:len(vi.entries)-1]
			return
		}
	}
}

// searchNearest returns the IDs of the k nearest entries to query, ordered by
// ascending distance (closest first).
// Returns ErrDimensionMismatch if query length differs from the index's dimensions.
// Returns nil, nil if the index is empty (no entries) or k <= 0.
//
// filter is an optional eligibility predicate. When non-nil, only entries for
// which filter(id) returns true are considered for inclusion in the heap. This
// matters for top-k correctness: ineligible candidates that happen to be near
// the query must NOT crowd out farther but eligible candidates from the
// k-best set. By filtering BEFORE the heap insertion, the heap always
// contains the top-k of the eligible-only set.
func (vi *vectorIndex) searchNearest(query []float32, k int, filter func(snowflake.ID) bool) ([]snowflake.ID, error) {
	if vi.dims > 0 && len(query) != vi.dims {
		return nil, ErrDimensionMismatch
	}
	// Defensive guard: non-positive k yields no results. The public API
	// (Graph.SearchNearestNodes) also gates k <= 0, but a direct
	// store-level caller (or Store-interface consumer) might pass it
	// through. Returning nil here avoids a panic in make/heap[0].
	if k <= 0 {
		return nil, nil
	}

	vi.mu.RLock()
	defer vi.mu.RUnlock()

	if len(vi.entries) == 0 {
		return nil, nil
	}

	// Use a max-heap of size k to find the k nearest entries in O(N log k) time,
	// reducing the duration we hold vi.mu.RLock compared to O(N log N) sort.
	h := make(knnHeap, 0, k)
	heap.Init(&h)
	for _, e := range vi.entries {
		if filter != nil && !filter(e.id) {
			continue
		}
		var d float64
		switch vi.metric {
		case DistanceCosine:
			d = cosineDist(query, e.vec)
		default:
			d = euclideanDist(query, e.vec)
		}
		if h.Len() < k {
			heap.Push(&h, knnEntry{id: e.id, dist: d})
		} else if d < h[0].dist {
			// Replace the current farthest candidate.
			h[0] = knnEntry{id: e.id, dist: d}
			heap.Fix(&h, 0)
		}
	}

	// Drain heap in ascending distance order (closest first).
	ids := make([]snowflake.ID, h.Len())
	for i := len(ids) - 1; i >= 0; i-- {
		ids[i] = heap.Pop(&h).(knnEntry).id
	}
	return ids, nil
}

// cosineDist computes 1 - cosine_similarity (range [0, 2]).
func cosineDist(a, b []float32) float64 {
	var dot, normA, normB float64
	for i := range a {
		fa := float64(a[i])
		fb := float64(b[i])
		dot += fa * fb
		normA += fa * fa
		normB += fb * fb
	}
	if normA == 0 || normB == 0 {
		return 1.0 // maximum distance for zero vectors
	}
	return 1.0 - dot/(math.Sqrt(normA)*math.Sqrt(normB))
}

// euclideanDist computes the Euclidean (L2) distance.
func euclideanDist(a, b []float32) float64 {
	var sum float64
	for i := range a {
		d := float64(a[i]) - float64(b[i])
		sum += d * d
	}
	return math.Sqrt(sum)
}

// addNodeToVectorIndexes updates all vector indexes with the node's vector properties.
func addNodeToVectorIndexes(idxs map[vectorIndexKey]*vectorIndex, n *types.Node, id snowflake.ID) {
	for key, vi := range idxs {
		if !n.HasLabelTokenRaw(key.labelToken) {
			continue
		}
		val, ok := n.GetProperty(key.propertyKey)
		if !ok {
			continue
		}
		vec, ok := toFloat32Slice(val)
		if !ok {
			continue
		}
		_ = vi.add(id, vec) // ignore dimension mismatch for auto-maintenance
	}
}

// removeNodeFromVectorIndexes removes the node from all vector indexes.
func removeNodeFromVectorIndexes(idxs map[vectorIndexKey]*vectorIndex, n *types.Node, id snowflake.ID) {
	for key, vi := range idxs {
		if !n.HasLabelTokenRaw(key.labelToken) {
			continue
		}
		vi.remove(id)
	}
}

// purgeNodeFromAllVectorIndexes removes the node ID from every vector index
// without requiring the node object. Used during corrupt-node deletion.
// Caller must hold the store's write lock.
func purgeNodeFromAllVectorIndexes(idxs map[vectorIndexKey]*vectorIndex, id snowflake.ID) {
	for _, vi := range idxs {
		vi.remove(id)
	}
}

// toFloat32Slice converts any to []float32, supporting []float32 and []any (of float32 or float64).
// Slow path: the []any branch requires a type-switch per element.
// Prefer []float32 property values for high-frequency vector nodes.
func toFloat32Slice(val any) ([]float32, bool) {
	switch v := val.(type) {
	case []float32:
		return v, true
	case []any:
		out := make([]float32, len(v))
		for i, elem := range v {
			switch f := elem.(type) {
			case float32:
				out[i] = f
			case float64:
				out[i] = float32(f)
			default:
				return nil, false
			}
		}
		return out, true
	}
	return nil, false
}

// Sentinel errors for vector index operations.
var (
	ErrVectorIndexExists   = errors.New("graph: vector index already exists")
	ErrVectorIndexNotFound = errors.New("graph: vector index not found")
	ErrDimensionMismatch   = errors.New("graph: vector dimension mismatch")
)
