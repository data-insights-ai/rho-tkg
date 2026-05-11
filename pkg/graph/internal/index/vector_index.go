package index

import (
	"container/heap"
	"fmt"
	"math"
	"sync"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
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

// VectorIndexKey uniquely identifies a vector index by label token and property key.
type VectorIndexKey struct {
	LabelToken  uint16
	PropertyKey string
}

// vectorEntry stores a node ID and its associated vector.
type vectorEntry struct {
	id  snowflake.ID
	vec []float32
}

// VectorIndex is an in-memory brute-force k-nearest-neighbor index.
// O(n × dims) per query. CreateVectorIndex rebuilds entries from current node properties.
type VectorIndex struct {
	mu      sync.RWMutex
	entries []vectorEntry
	Dims    int
	Metric  storepkg.DistanceMetric
	Mutated map[snowflake.ID]struct{} // non-nil during index creation backfill
}

// ValidateVectorIndexConfig checks vector-index creation parameters.
func ValidateVectorIndexConfig(dims int, metric storepkg.DistanceMetric) error {
	if dims <= 0 {
		return fmt.Errorf("%w: dims must be > 0, got %d", ErrInvalidVectorIndexConfig, dims)
	}
	switch metric {
	case storepkg.DistanceCosine, storepkg.DistanceEuclidean:
		return nil
	default:
		return fmt.Errorf("%w: unsupported distance metric %d", ErrInvalidVectorIndexConfig, metric)
	}
}

// Add inserts or updates a vector entry for the given ID.
// Returns ErrDimensionMismatch if the vector length differs from the index's expected dimensions.
func (vi *VectorIndex) Add(id snowflake.ID, vec []float32) error {
	if vi == nil {
		return fmt.Errorf("%w: nil vector index", ErrInvalidVectorIndexConfig)
	}
	vi.mu.Lock()
	defer vi.mu.Unlock()
	if vi.Mutated != nil {
		vi.Mutated[id] = struct{}{}
	}
	if err := ValidateVectorIndexConfig(vi.Dims, vi.Metric); err != nil {
		return err
	}
	if len(vec) != vi.Dims {
		return ErrDimensionMismatch
	}

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

// Remove deletes the entry for the given ID. No-op if not present.
func (vi *VectorIndex) Remove(id snowflake.ID) {
	if vi == nil {
		return
	}
	vi.mu.Lock()
	defer vi.mu.Unlock()

	for i, e := range vi.entries {
		if e.id == id {
			vi.entries[i] = vi.entries[len(vi.entries)-1]
			vi.entries = vi.entries[:len(vi.entries)-1]
			if vi.Mutated != nil {
				vi.Mutated[id] = struct{}{}
			}
			return
		}
	}
	if vi.Mutated != nil {
		vi.Mutated[id] = struct{}{}
	}
}

// WasMutated reports whether id was touched while an index backfill was active.
func (vi *VectorIndex) WasMutated(id snowflake.ID) bool {
	if vi == nil {
		return false
	}
	vi.mu.RLock()
	defer vi.mu.RUnlock()
	if vi.Mutated == nil {
		return false
	}
	_, ok := vi.Mutated[id]
	return ok
}

// IsBuilding reports whether this index is still in its create/backfill phase.
func (vi *VectorIndex) IsBuilding() bool {
	if vi == nil {
		return false
	}
	vi.mu.RLock()
	defer vi.mu.RUnlock()
	return vi.Mutated != nil
}

// ClearMutationTracking stops tracking concurrent writes after index creation.
func (vi *VectorIndex) ClearMutationTracking() {
	if vi == nil {
		return
	}
	vi.mu.Lock()
	defer vi.mu.Unlock()
	vi.Mutated = nil
}

// IDs returns a snapshot of the IDs currently present in the index.
func (vi *VectorIndex) IDs() []snowflake.ID {
	if vi == nil {
		return nil
	}
	vi.mu.RLock()
	defer vi.mu.RUnlock()
	if len(vi.entries) == 0 {
		return nil
	}
	ids := make([]snowflake.ID, len(vi.entries))
	for i, entry := range vi.entries {
		ids[i] = entry.id
	}
	return ids
}

// SearchNearest returns the IDs of the k nearest entries to query, ordered by
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
func (vi *VectorIndex) SearchNearest(query []float32, k int, filter func(snowflake.ID) bool) ([]snowflake.ID, error) {
	if vi == nil {
		return nil, fmt.Errorf("%w: nil vector index", ErrInvalidVectorIndexConfig)
	}
	if err := ValidateVectorIndexConfig(vi.Dims, vi.Metric); err != nil {
		return nil, err
	}
	if len(query) != vi.Dims {
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

	// Use a max-heap of size min(k, entries) to find the k nearest entries in
	// O(N log k) time without letting an oversized caller k force an oversized
	// allocation when the index itself is small.
	heapCap := k
	if len(vi.entries) < heapCap {
		heapCap = len(vi.entries)
	}
	h := make(knnHeap, 0, heapCap)
	heap.Init(&h)
	for _, e := range vi.entries {
		if filter != nil && !filter(e.id) {
			continue
		}
		var d float64
		if vi.Metric == storepkg.DistanceCosine {
			d = cosineDist(query, e.vec)
		} else {
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

// ValidateNodeVectorIndexes verifies that every active vector index matching n
// can accept the node's vector property before the caller mutates store state.
func ValidateNodeVectorIndexes(idxs map[VectorIndexKey]*VectorIndex, n *types.Node, id snowflake.ID) error {
	for key, vi := range idxs {
		vec, ok := nodeVectorForIndex(n, key)
		if !ok {
			continue
		}
		if vi == nil {
			return fmt.Errorf("graph: vector index label %d property %q node %d: %w: nil vector index",
				key.LabelToken, key.PropertyKey, id, ErrInvalidVectorIndexConfig)
		}
		if err := ValidateVectorIndexConfig(vi.Dims, vi.Metric); err != nil {
			return err
		}
		if len(vec) != vi.Dims {
			return fmt.Errorf("graph: vector index label %d property %q node %d: %w",
				key.LabelToken, key.PropertyKey, id, ErrDimensionMismatch)
		}
	}
	return nil
}

// AddNodeToVectorIndexes updates all vector indexes with the node's vector properties.
func AddNodeToVectorIndexes(idxs map[VectorIndexKey]*VectorIndex, n *types.Node, id snowflake.ID) error {
	if err := ValidateNodeVectorIndexes(idxs, n, id); err != nil {
		return err
	}
	for key, vi := range idxs {
		vec, ok := nodeVectorForIndex(n, key)
		if !ok {
			continue
		}
		if vi == nil {
			return fmt.Errorf("graph: vector index label %d property %q node %d: %w: nil vector index",
				key.LabelToken, key.PropertyKey, id, ErrInvalidVectorIndexConfig)
		}
		if err := vi.Add(id, vec); err != nil {
			return fmt.Errorf("graph: vector index label %d property %q node %d: %w",
				key.LabelToken, key.PropertyKey, id, err)
		}
	}
	return nil
}

func nodeVectorForIndex(n *types.Node, key VectorIndexKey) ([]float32, bool) {
	if !n.HasLabelTokenRaw(key.LabelToken) {
		return nil, false
	}
	val, ok := n.GetProperty(key.PropertyKey)
	if !ok {
		return nil, false
	}
	return ToFloat32Slice(val)
}

// DeleteVectorIndexIfCurrent removes key only when it still points at expected.
func DeleteVectorIndexIfCurrent(idxs map[VectorIndexKey]*VectorIndex, key VectorIndexKey, expected *VectorIndex) {
	if idxs[key] == expected {
		delete(idxs, key)
	}
}

// RequireVectorIndexCurrentForCreate rejects stale CreateVectorIndex finalization
// after a concurrent DropVectorIndex or drop+recreate replaced its placeholder.
func RequireVectorIndexCurrentForCreate(idxs map[VectorIndexKey]*VectorIndex, key VectorIndexKey, expected *VectorIndex) error {
	current := idxs[key]
	if current == expected {
		return nil
	}
	if current == nil {
		return fmt.Errorf("graph: create vector index: index dropped during creation: %w", ErrVectorIndexNotFound)
	}
	return fmt.Errorf("graph: create vector index: index replaced during creation: %w", ErrVectorIndexExists)
}

// RemoveNodeFromVectorIndexes removes the node from all vector indexes.
func RemoveNodeFromVectorIndexes(idxs map[VectorIndexKey]*VectorIndex, n *types.Node, id snowflake.ID) {
	for key, vi := range idxs {
		if !n.HasLabelTokenRaw(key.LabelToken) {
			continue
		}
		if vi == nil {
			continue
		}
		vi.Remove(id)
	}
}

// PurgeNodeFromAllVectorIndexes removes the node ID from every vector index
// without requiring the node object. Used during corrupt-node deletion.
// Caller must hold the store's write lock.
func PurgeNodeFromAllVectorIndexes(idxs map[VectorIndexKey]*VectorIndex, id snowflake.ID) {
	for _, vi := range idxs {
		if vi != nil {
			vi.Remove(id)
		}
	}
}

// ToFloat32Slice converts any to []float32, supporting []float32 and []any (of float32 or float64).
// Slow path: the []any branch requires a type-switch per element.
// Prefer []float32 property values for high-frequency vector nodes.
func ToFloat32Slice(val any) ([]float32, bool) {
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
	ErrVectorIndexExists        = storepkg.ErrVectorIndexExists
	ErrVectorIndexNotFound      = storepkg.ErrVectorIndexNotFound
	ErrDimensionMismatch        = storepkg.ErrDimensionMismatch
	ErrInvalidVectorIndexConfig = storepkg.ErrInvalidVectorIndexConfig
)
