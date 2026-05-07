package index

import (
	"sync"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
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
// The HFI does NOT store ValidTo — it indexes validFrom only. Callers needing
// precise interval filtering must re-filter results against ValidTo.
//
// Not persisted: the index must be rebuilt via CreateHighFrequencyIndex after restart.
//
// Thread-safe via internal sync.RWMutex.
type HighFrequencyIndex struct {
	mu         sync.RWMutex
	bucketSize types.Instant // bucket width in milliseconds
	origin     types.Instant // epoch offset for bucket 0
	buckets    map[int64][]types.NodeID
}

// NewHighFrequencyIndex creates a new HighFrequencyIndex.
// bucketSize is the time width of each bucket (e.g., time.Hour).
// origin is the baseline time for bucket 0 (typically 0 or the first event's time).
func NewHighFrequencyIndex(bucketSize time.Duration, origin types.Instant) *HighFrequencyIndex {
	return &HighFrequencyIndex{
		bucketSize: types.Instant(bucketSize.Milliseconds()),
		origin:     origin,
		buckets:    make(map[int64][]types.NodeID),
	}
}

// BucketFor returns the bucket index for a given validFrom instant.
// Instants before origin map to negative bucket indices.
func (hfi *HighFrequencyIndex) BucketFor(validFrom types.Instant) int64 {
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
	b := hfi.BucketFor(validFrom)
	hfi.mu.Lock()
	hfi.buckets[b] = append(hfi.buckets[b], id)
	hfi.mu.Unlock()
}

// Remove removes id from the bucket for validFrom. O(n/num_buckets) amortized.
// No-op if id is not in the bucket.
func (hfi *HighFrequencyIndex) Remove(id types.NodeID, validFrom types.Instant) {
	b := hfi.BucketFor(validFrom)
	hfi.mu.Lock()
	defer hfi.mu.Unlock()
	ids := hfi.buckets[b]
	for i, v := range ids {
		if v == id {
			hfi.buckets[b] = append(ids[:i], ids[i+1:]...)
			return
		}
	}
}

// PointQuery returns all IDs in the bucket containing t.
// Returns candidates — the HFI does not store ValidTo, so callers must
// re-filter if exact interval matching is needed.
func (hfi *HighFrequencyIndex) PointQuery(t types.Instant) []types.NodeID {
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

// RangeQuery returns all IDs in buckets that overlap [start, end).
// Returns candidates — callers must re-filter if exact interval matching is needed.
//
// Iterates the actual bucket map rather than a numeric range to avoid a CPU hang
// when end is very large (e.g. math.MaxInt64): only non-empty buckets are visited.
func (hfi *HighFrequencyIndex) RangeQuery(start, end types.Instant) []types.NodeID {
	startBucket := hfi.BucketFor(start)
	endBucket := hfi.BucketFor(end)

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
