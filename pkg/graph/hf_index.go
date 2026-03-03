package graph

import (
	"sync"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

// highFrequencyIndex is a time-bucketed index providing O(1) amortized insertion.
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
type highFrequencyIndex struct {
	mu         sync.RWMutex
	bucketSize types.Instant             // bucket width in milliseconds
	origin     types.Instant             // epoch offset for bucket 0
	buckets    map[int64][]snowflake.ID
}

// newHighFrequencyIndex creates a new highFrequencyIndex.
// bucketSize is the time width of each bucket (e.g., time.Hour).
// origin is the baseline time for bucket 0 (typically 0 or the first event's time).
func newHighFrequencyIndex(bucketSize time.Duration, origin types.Instant) *highFrequencyIndex {
	return &highFrequencyIndex{
		bucketSize: types.Instant(bucketSize.Milliseconds()),
		origin:     origin,
		buckets:    make(map[int64][]snowflake.ID),
	}
}

// bucketFor returns the bucket index for a given validFrom instant.
// Instants before origin map to negative bucket indices.
func (hfi *highFrequencyIndex) bucketFor(validFrom types.Instant) int64 {
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

// add inserts id into the bucket for validFrom. O(1) amortized.
func (hfi *highFrequencyIndex) add(id snowflake.ID, validFrom types.Instant) {
	b := hfi.bucketFor(validFrom)
	hfi.mu.Lock()
	hfi.buckets[b] = append(hfi.buckets[b], id)
	hfi.mu.Unlock()
}

// remove removes id from the bucket for validFrom. O(n/num_buckets) amortized.
// No-op if id is not in the bucket.
func (hfi *highFrequencyIndex) remove(id snowflake.ID, validFrom types.Instant) {
	b := hfi.bucketFor(validFrom)
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

// pointQuery returns all IDs in the bucket containing t.
// Returns candidates — the HFI does not store ValidTo, so callers must
// re-filter if exact interval matching is needed.
func (hfi *highFrequencyIndex) pointQuery(t types.Instant) []snowflake.ID {
	b := hfi.bucketFor(t)
	hfi.mu.RLock()
	ids := hfi.buckets[b]
	if len(ids) == 0 {
		hfi.mu.RUnlock()
		return nil
	}
	out := make([]snowflake.ID, len(ids))
	copy(out, ids)
	hfi.mu.RUnlock()
	return out
}

// rangeQuery returns all IDs in buckets that overlap [start, end).
// Returns candidates — callers must re-filter if exact interval matching is needed.
func (hfi *highFrequencyIndex) rangeQuery(start, end types.Instant) []snowflake.ID {
	startBucket := hfi.bucketFor(start)
	endBucket := hfi.bucketFor(end)

	hfi.mu.RLock()
	defer hfi.mu.RUnlock()

	var out []snowflake.ID
	for b := startBucket; b <= endBucket; b++ {
		out = append(out, hfi.buckets[b]...)
	}
	return out
}
