package index

import (
	"container/heap"
	"fmt"
	"math"
	"sync"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
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
func (h knnHeap) Less(i, j int) bool { return worseKNN(h[i], h[j]) } // max-heap
func (h knnHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *knnHeap) Push(x any)        { *h = append(*h, x.(knnEntry)) }
func (h *knnHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func worseKNN(a, b knnEntry) bool {
	if a.dist != b.dist {
		return a.dist > b.dist
	}
	return a.id > b.id
}

func betterKNN(a, b knnEntry) bool {
	if a.dist != b.dist {
		return a.dist < b.dist
	}
	return a.id < b.id
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
	mu        sync.RWMutex
	entries   []vectorEntry
	positions map[snowflake.ID]int
	Dims      int
	Metric    storepkg.DistanceMetric
	Mutated   map[snowflake.ID]struct{} // non-nil during index creation backfill
}

// NodeVectorIndexUpdate is a prevalidated vector-index write for one node.
// It lets store mutation paths validate all vector indexes before mutating
// store state, then apply the exact same prepared vectors without re-reading
// node properties.
type NodeVectorIndexUpdate struct {
	key VectorIndexKey
	idx *VectorIndex
	vec []float32
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

// ValidateVectorValues verifies that vector coordinates are safe for distance math.
func ValidateVectorValues(vec []float32) error {
	for i, v := range vec {
		f := float64(v)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return fmt.Errorf("%w: element %d is non-finite", ErrInvalidVectorValue, i)
		}
	}
	return nil
}

// Add inserts or updates a vector entry for the given ID.
// Returns ErrDimensionMismatch if the vector length differs from the index's expected dimensions.
func (vi *VectorIndex) Add(id snowflake.ID, vec []float32) error {
	if vi == nil {
		return fmt.Errorf("%w: nil vector index", ErrInvalidVectorIndexConfig)
	}
	vi.mu.Lock()
	defer vi.mu.Unlock()
	return vi.addLocked(id, vec, false)
}

// AddOwned inserts or updates a vector entry and takes ownership of vec.
// Callers must not mutate vec after calling AddOwned.
func (vi *VectorIndex) AddOwned(id snowflake.ID, vec []float32) error {
	if vi == nil {
		return fmt.Errorf("%w: nil vector index", ErrInvalidVectorIndexConfig)
	}
	vi.mu.Lock()
	defer vi.mu.Unlock()
	return vi.addLocked(id, vec, true)
}

func (vi *VectorIndex) addLocked(id snowflake.ID, vec []float32, owned bool) error {
	if vi.Mutated != nil {
		vi.Mutated[id] = struct{}{}
	}
	if err := ValidateVectorIndexConfig(vi.Dims, vi.Metric); err != nil {
		return err
	}
	if len(vec) != vi.Dims {
		return ErrDimensionMismatch
	}
	if err := ValidateVectorValues(vec); err != nil {
		return err
	}
	if !owned {
		cp := make([]float32, len(vec))
		copy(cp, vec)
		vec = cp
	}

	vi.ensurePositionsLocked()
	if i, ok := vi.positions[id]; ok {
		vi.entries[i].vec = vec
		return nil
	}

	vi.positions[id] = len(vi.entries)
	vi.entries = append(vi.entries, vectorEntry{id: id, vec: vec})
	return nil
}

func (vi *VectorIndex) ensurePositionsLocked() {
	if vi.positions != nil {
		return
	}
	vi.positions = make(map[snowflake.ID]int, len(vi.entries))
	originalLen := len(vi.entries)
	out := vi.entries[:0]
	for _, entry := range vi.entries {
		if i, exists := vi.positions[entry.id]; exists {
			out[i] = entry
			continue
		}
		vi.positions[entry.id] = len(out)
		out = append(out, entry)
	}
	clear(vi.entries[len(out):originalLen])
	vi.entries = out
}

// Remove deletes the entry for the given ID. No-op if not present.
func (vi *VectorIndex) Remove(id snowflake.ID) {
	if vi == nil {
		return
	}
	vi.mu.Lock()
	defer vi.mu.Unlock()

	vi.ensurePositionsLocked()
	if i, ok := vi.positions[id]; ok {
		lastIdx := len(vi.entries) - 1
		last := vi.entries[lastIdx]
		vi.entries[i] = last
		vi.entries[lastIdx] = vectorEntry{}
		vi.entries = vi.entries[:lastIdx]
		delete(vi.positions, id)
		if i != lastIdx {
			vi.positions[last.id] = i
		}
		if vi.Mutated != nil {
			vi.Mutated[id] = struct{}{}
		}
		return
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
//
// Non-nil filters are invoked after snapshotting entries, without holding the
// vector index lock. Store-backed filters may need store/index locks, and
// holding the vector lock while calling them would invert the mutation order
// used by backends (store lock -> vector lock).
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
	if err := ValidateVectorValues(query); err != nil {
		return nil, err
	}
	// Defensive guard: non-positive k yields no results. The public API
	// (Graph.SearchNearestNodes) also gates k <= 0, but a direct
	// store-level caller (or Store-interface consumer) might pass it
	// through. Returning nil here avoids a panic in make/heap[0].
	if k <= 0 {
		return nil, nil
	}

	vi.mu.RLock()
	if len(vi.entries) == 0 {
		vi.mu.RUnlock()
		return nil, nil
	}
	if filter == nil {
		ids := vi.searchNearestEntries(query, k, nil, vi.entries)
		vi.mu.RUnlock()
		return ids, nil
	}
	entries := make([]vectorEntry, len(vi.entries))
	copy(entries, vi.entries)
	vi.mu.RUnlock()

	return vi.searchNearestEntries(query, k, filter, entries), nil
}

func (vi *VectorIndex) searchNearestEntries(query []float32, k int, filter func(snowflake.ID) bool, entries []vectorEntry) []snowflake.ID {
	// Use a max-heap of size min(k, entries) to find the k nearest entries in
	// O(N log k) time without letting an oversized caller k force an oversized
	// allocation when the index itself is small.
	heapCap := k
	if len(entries) < heapCap {
		heapCap = len(entries)
	}
	h := make(knnHeap, 0, heapCap)
	heap.Init(&h)
	for _, e := range entries {
		if filter != nil && !filter(e.id) {
			continue
		}
		var d float64
		if vi.Metric == storepkg.DistanceCosine {
			d = cosineDist(query, e.vec)
		} else {
			d = euclideanDist(query, e.vec)
		}
		candidate := knnEntry{id: e.id, dist: d}
		if h.Len() < k {
			heap.Push(&h, candidate)
		} else if betterKNN(candidate, h[0]) {
			// Replace the current farthest candidate.
			h[0] = candidate
			heap.Fix(&h, 0)
		}
	}

	// Drain heap in ascending distance order (closest first).
	ids := make([]snowflake.ID, h.Len())
	for i := len(ids) - 1; i >= 0; i-- {
		ids[i] = heap.Pop(&h).(knnEntry).id
	}
	return ids
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

// PrepareNodeVectorIndexUpdates verifies that every active vector index
// matching n can accept the node's vector property, and returns the prepared
// writes so the caller can apply them after the surrounding store mutation.
func PrepareNodeVectorIndexUpdates(idxs map[VectorIndexKey]*VectorIndex, n *types.Node, id snowflake.ID) ([]NodeVectorIndexUpdate, error) {
	if len(idxs) == 0 {
		return nil, nil
	}
	var updates []NodeVectorIndexUpdate
	for key, vi := range idxs {
		vec, ok := nodeVectorForIndex(n, key)
		if !ok {
			continue
		}
		if vi == nil {
			return nil, fmt.Errorf("graph: vector index label %d property %q node %d: %w: nil vector index",
				key.LabelToken, key.PropertyKey, id, ErrInvalidVectorIndexConfig)
		}
		if err := ValidateVectorIndexConfig(vi.Dims, vi.Metric); err != nil {
			return nil, err
		}
		if len(vec) != vi.Dims {
			return nil, fmt.Errorf("graph: vector index label %d property %q node %d: %w",
				key.LabelToken, key.PropertyKey, id, ErrDimensionMismatch)
		}
		if err := ValidateVectorValues(vec); err != nil {
			return nil, fmt.Errorf("graph: vector index label %d property %q node %d: %w",
				key.LabelToken, key.PropertyKey, id, err)
		}
		updates = append(updates, NodeVectorIndexUpdate{key: key, idx: vi, vec: vec})
	}
	return updates, nil
}

// AddPreparedNodeToVectorIndexes applies updates returned by
// PrepareNodeVectorIndexUpdates. Callers must use updates prepared for the same
// node ID and under the same index-map lock.
func AddPreparedNodeToVectorIndexes(updates []NodeVectorIndexUpdate, id snowflake.ID) error {
	for _, update := range updates {
		if update.idx == nil {
			return fmt.Errorf("graph: vector index label %d property %q node %d: %w: nil vector index",
				update.key.LabelToken, update.key.PropertyKey, id, ErrInvalidVectorIndexConfig)
		}
		if err := update.idx.AddOwned(id, update.vec); err != nil {
			return fmt.Errorf("graph: vector index label %d property %q node %d: %w",
				update.key.LabelToken, update.key.PropertyKey, id, err)
		}
	}
	return nil
}

func nodeVectorForIndex(n *types.Node, key VectorIndexKey) ([]float32, bool) {
	if !n.HasLabelTokenRaw(key.LabelToken) {
		return nil, false
	}
	return n.Float32SlicePropertyCopy(key.PropertyKey)
}

// NodeMatchesVectorIndex reports whether the current node row still satisfies
// the label/property/vector-shape contract for a vector-index entry.
func NodeMatchesVectorIndex(n *types.Node, key VectorIndexKey, dims int) bool {
	vec, ok := nodeVectorForIndex(n, key)
	if !ok || len(vec) != dims {
		return false
	}
	return ValidateVectorValues(vec) == nil
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

// RemoveNodeFromVectorIndexes removes the node ID from every vector index.
// Callers re-add prepared entries for the current node shape after this purge.
func RemoveNodeFromVectorIndexes(idxs map[VectorIndexKey]*VectorIndex, _ *types.Node, id snowflake.ID) {
	PurgeNodeFromAllVectorIndexes(idxs, id)
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

// Sentinel errors for vector index operations.
var (
	ErrVectorIndexExists        = storepkg.ErrVectorIndexExists
	ErrVectorIndexNotFound      = storepkg.ErrVectorIndexNotFound
	ErrDimensionMismatch        = storepkg.ErrDimensionMismatch
	ErrInvalidVectorIndexConfig = storepkg.ErrInvalidVectorIndexConfig
	ErrInvalidVectorValue       = storepkg.ErrInvalidVectorValue
)
