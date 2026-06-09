package index

import (
	"sync"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// HighFrequencyIndex is a time-bucketed index providing O(1) amortized insertion.
// Designed for high-write-rate scenarios (thousands of event writes/sec).
//
// IDs are stored in buckets keyed by (validFrom - origin) / bucketSize.
// Insertion: O(1) amortized (map lookup + append).
// Point query: returns all IDs in the bucket containing t — O(avg_bucket_size).
// Range query: O(num_buckets_in_range * avg_bucket_size).
// Removal: O(n/num_buckets) amortized linear scan within a single bucket.
//
// The HFI stores validFrom buckets and returns candidates; exact interval
// semantics are handled by the filtering layer that consumes the candidates.
//
// In-memory: CreateHighFrequencyIndex rebuilds the buckets from current store state.
//
// Thread-safe via internal sync.RWMutex.
type HighFrequencyIndex struct {
	mu         sync.RWMutex
	bucketSize types.Instant // bucket width in milliseconds
	origin     types.Instant // epoch offset for bucket 0
	buckets    map[int64][]types.NodeID
	Mutated    map[snowflake.ID]struct{}
}

// NewHighFrequencyIndex creates a new HighFrequencyIndex.
// bucketSize is the time width of each bucket (e.g., time.Hour). Callers must
// validate that it is representable as a positive whole millisecond.
// origin is the baseline time for bucket 0 (typically 0 or the first event's time).
func NewHighFrequencyIndex(bucketSize time.Duration, origin types.Instant) *HighFrequencyIndex {
	return &HighFrequencyIndex{
		bucketSize: types.Instant(bucketSize.Milliseconds()),
		origin:     origin,
		buckets:    make(map[int64][]types.NodeID),
	}
}

// BucketSize returns the configured bucket width.
func (hfi *HighFrequencyIndex) BucketSize() time.Duration {
	if hfi == nil {
		return 0
	}
	hfi.mu.RLock()
	defer hfi.mu.RUnlock()
	return time.Duration(hfi.bucketSize) * time.Millisecond
}

// BucketFor returns the bucket index for a given validFrom instant.
// Instants before origin map to negative bucket indices.
func (hfi *HighFrequencyIndex) BucketFor(validFrom types.Instant) int64 {
	if hfi == nil {
		return 0
	}
	if hfi.bucketSize <= 0 {
		return 0
	}
	delta := int64(validFrom - hfi.origin)
	if delta < 0 {
		// Before origin: use floor division for correct negative bucket assignment.
		return (delta - int64(hfi.bucketSize) + 1) / int64(hfi.bucketSize)
	}
	return delta / int64(hfi.bucketSize)
}

// Add inserts id into the bucket for validFrom. O(1) amortized.
func (hfi *HighFrequencyIndex) Add(id types.NodeID, validFrom types.Instant) {
	if hfi == nil {
		return
	}
	b := hfi.BucketFor(validFrom)
	hfi.mu.Lock()
	hfi.ensureBucketsLocked()
	hfi.markMutatedLocked(id.SnowflakeID())
	hfi.buckets[b] = append(hfi.buckets[b], id)
	hfi.mu.Unlock()
}

// Remove removes id from the bucket for validFrom. O(n/num_buckets) amortized.
// No-op if id is not in the bucket.
func (hfi *HighFrequencyIndex) Remove(id types.NodeID, validFrom types.Instant) {
	if hfi == nil {
		return
	}
	b := hfi.BucketFor(validFrom)
	hfi.mu.Lock()
	defer hfi.mu.Unlock()
	hfi.markMutatedLocked(id.SnowflakeID())
	ids := hfi.buckets[b]
	out := ids[:0]
	for _, v := range ids {
		if v != id {
			out = append(out, v)
		}
	}
	if len(out) == 0 {
		delete(hfi.buckets, b)
		return
	}
	hfi.buckets[b] = out
}

// WasMutated reports whether id was touched while this index was being built.
func (hfi *HighFrequencyIndex) WasMutated(id snowflake.ID) bool {
	if hfi == nil {
		return false
	}
	hfi.mu.RLock()
	defer hfi.mu.RUnlock()
	_, ok := hfi.Mutated[id]
	return ok
}

// IsBuilding reports whether this index is still in its create/backfill phase.
func (hfi *HighFrequencyIndex) IsBuilding() bool {
	if hfi == nil {
		return false
	}
	hfi.mu.RLock()
	defer hfi.mu.RUnlock()
	return hfi.Mutated != nil
}

// ClearMutationTracking stops tracking concurrent mutations after creation.
func (hfi *HighFrequencyIndex) ClearMutationTracking() {
	if hfi == nil {
		return
	}
	hfi.mu.Lock()
	hfi.Mutated = nil
	hfi.mu.Unlock()
}

func (hfi *HighFrequencyIndex) ensureBucketsLocked() {
	if hfi.buckets == nil {
		hfi.buckets = make(map[int64][]types.NodeID)
	}
}

func (hfi *HighFrequencyIndex) markMutatedLocked(id snowflake.ID) {
	if hfi.Mutated != nil {
		hfi.Mutated[id] = struct{}{}
	}
}

// PointQuery returns candidate IDs in the bucket containing t.
// Exact temporal semantics are applied by the graph/store query layer because
// the HFI only stores ValidFrom bucket membership.
func (hfi *HighFrequencyIndex) PointQuery(t types.Instant) []types.NodeID {
	if hfi == nil {
		return nil
	}
	b := hfi.BucketFor(t)
	hfi.mu.RLock()
	ids := hfi.buckets[b]
	if len(ids) == 0 {
		hfi.mu.RUnlock()
		return nil
	}
	out := make([]types.NodeID, len(ids))
	copy(out, ids)
	hfi.mu.RUnlock()
	return out
}

// CandidatesUpTo returns all IDs whose valid-from bucket is not after t's
// bucket. Exact temporal semantics must still be applied by the caller, but
// the returned set is a safe superset for point-in-time and interval-overlap
// validity queries because nodes that started earlier may still be open-ended.
func (hfi *HighFrequencyIndex) CandidatesUpTo(t types.Instant) []types.NodeID {
	if hfi == nil {
		return nil
	}
	endBucket := hfi.BucketFor(t)
	hfi.mu.RLock()
	defer hfi.mu.RUnlock()

	var out []types.NodeID
	for b, ids := range hfi.buckets {
		if b <= endBucket {
			out = append(out, ids...)
		}
	}
	return out
}

// RangeQuery returns all candidate IDs in buckets that overlap [start, end).
//
// Iterates the actual bucket map rather than a numeric range to avoid a CPU hang
// when end is very large (e.g. math.MaxInt64): only non-empty buckets are visited.
func (hfi *HighFrequencyIndex) RangeQuery(start, end types.Instant) []types.NodeID {
	if hfi == nil || start >= end {
		return nil
	}
	startBucket := hfi.BucketFor(start)
	endBucket := hfi.BucketFor(end - 1)

	hfi.mu.RLock()
	defer hfi.mu.RUnlock()

	var out []types.NodeID
	for b, ids := range hfi.buckets {
		if b >= startBucket && b <= endBucket {
			out = append(out, ids...)
		}
	}
	return out
}

// AddNodeToHighFrequencyIndexes adds a node to all high-frequency indexes that
// cover any of the node's label tokens. Caller must hold the store's write lock.
func AddNodeToHighFrequencyIndexes(idxs map[uint16]*HighFrequencyIndex, n *types.Node, id snowflake.ID) {
	if len(idxs) == 0 {
		return
	}
	from, _ := NodeTemporalBounds(id, n.Temporal())
	nid := types.NodeID(id)
	for i := 0; i < n.LabelTokenCount(); i++ {
		if hfi, ok := idxs[n.LabelTokenRawAt(i)]; ok {
			hfi.Add(nid, from)
		}
	}
}

// RemoveNodeFromHighFrequencyIndexes removes a node from all high-frequency indexes.
// Caller must hold the store's write lock.
func RemoveNodeFromHighFrequencyIndexes(idxs map[uint16]*HighFrequencyIndex, n *types.Node, id snowflake.ID) {
	if len(idxs) == 0 {
		return
	}
	from, _ := NodeTemporalBounds(id, n.Temporal())
	nid := types.NodeID(id)
	for i := 0; i < n.LabelTokenCount(); i++ {
		if hfi, ok := idxs[n.LabelTokenRawAt(i)]; ok {
			hfi.Remove(nid, from)
		}
	}
}

// PurgeNodeFromAllHighFrequencyIndexes removes a node ID from every bucket of
// every high-frequency index. Used during corrupt-node deletion when label and
// temporal data are unavailable. Caller must hold the store's write lock.
func PurgeNodeFromAllHighFrequencyIndexes(idxs map[uint16]*HighFrequencyIndex, id snowflake.ID) {
	for _, hfi := range idxs {
		hfi.removeAll(types.NodeID(id))
	}
}

func (hfi *HighFrequencyIndex) removeAll(id types.NodeID) {
	if hfi == nil {
		return
	}
	hfi.mu.Lock()
	defer hfi.mu.Unlock()
	hfi.markMutatedLocked(id.SnowflakeID())
	for bucket, ids := range hfi.buckets {
		out := ids[:0]
		for _, existing := range ids {
			if existing != id {
				out = append(out, existing)
			}
		}
		if len(out) == 0 {
			delete(hfi.buckets, bucket)
			continue
		}
		hfi.buckets[bucket] = out
	}
}
