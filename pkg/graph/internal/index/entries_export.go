package index

import (
	snowflake "github.com/bds421/rho-snowflake-2026"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ForEachBucketEntry calls fn for every (bucket start, id) entry, bucket by
// bucket. Add(id, start) puts the entry back into the same bucket, so a store
// can persist an index this way and reload it without the rows (badger's
// clean-close open state, backlog 54).
func (hfi *HighFrequencyIndex) ForEachBucketEntry(fn func(start types.Instant, id types.NodeID)) {
	if hfi == nil {
		return
	}
	hfi.mu.RLock()
	defer hfi.mu.RUnlock()
	for b, ids := range hfi.buckets {
		start := hfi.origin + types.Instant(b*int64(hfi.bucketSize))
		for _, id := range ids {
			fn(start, id)
		}
	}
}

// ForEachVector calls fn for every (id, vector) entry. vec is the index's own
// slice: fn must not keep or modify it.
func (vi *VectorIndex) ForEachVector(fn func(id snowflake.ID, vec []float32)) {
	if vi == nil {
		return
	}
	vi.mu.RLock()
	defer vi.mu.RUnlock()
	for _, e := range vi.entries {
		fn(e.id, e.vec)
	}
}

// ForEachEntry calls fn for every (id, from, to) envelope; AddKnownAbsent puts
// each back. Safe beside concurrent readers that sort the index.
func (ti *TemporalIndex) ForEachEntry(fn func(e IntervalEntry)) {
	if ti == nil {
		return
	}
	ti.sortMu.Lock()
	defer ti.sortMu.Unlock()
	for _, e := range ti.Entries {
		fn(e)
	}
}
