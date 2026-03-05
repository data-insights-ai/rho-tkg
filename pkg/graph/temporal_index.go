package graph

import (
	"sort"
	"sync"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

// intervalEntry is a single entry in the temporal index.
type intervalEntry struct {
	from types.Instant
	to   types.Instant // 0 = open-ended / still valid
	id   snowflake.ID
}

// temporalIndex is a sorted-slice interval index keyed by (from ASC, id ASC).
//
// add/remove are called under the store's write lock (ms.mu.Lock / bs.idxMu.Lock).
// queryAt/queryOverlap are called under the store's read lock (RLock), which allows
// multiple goroutines to query concurrently. sortIfDirty is protected by sortMu so
// that concurrent readers do not race on the sort transition.
//
// Complexity:
//   - add:          O(1) amortized append; sort deferred to first query after write
//   - remove:       O(n) linear scan
//   - queryAt:      O(n log n) sort (once per dirty batch) + O(log n) binary search + O(k)
//   - queryOverlap: same as queryAt
//
// The O(n) query bound is acceptable for v3 (small-to-medium label sets).
// A future version may augment with maxTo for O(log n + k) stabbing queries.
type temporalIndex struct {
	sortMu  sync.Mutex      // serialises concurrent sort transitions under RLock
	entries []intervalEntry // sorted by (from ASC, id ASC) when not dirty
	dirty   bool            // true when entries have been appended but not yet sorted
}

// newTemporalIndex allocates an empty temporal index.
func newTemporalIndex() *temporalIndex {
	return &temporalIndex{}
}

// add inserts or updates an entry for id with [from, to).
// If id already has an entry, it is removed first (replace semantics).
// Appends unsorted and marks dirty; sorting is deferred to the first query.
// Must be called under the store's write lock.
func (ti *temporalIndex) add(id snowflake.ID, from, to types.Instant) {
	// Remove any existing entry for id first.
	ti.remove(id)

	// Append unsorted — sort is deferred to queryAt/queryOverlap.
	ti.entries = append(ti.entries, intervalEntry{from: from, to: to, id: id})
	ti.dirty = true
}

// sortIfDirty sorts entries by (from ASC, id ASC) if the index has been
// modified since the last sort. Called at the start of every query.
// sortMu serialises concurrent callers holding only the store's read lock.
func (ti *temporalIndex) sortIfDirty() {
	ti.sortMu.Lock()
	defer ti.sortMu.Unlock()
	if !ti.dirty {
		return
	}
	sort.Slice(ti.entries, func(i, j int) bool {
		if ti.entries[i].from != ti.entries[j].from {
			return ti.entries[i].from < ti.entries[j].from
		}
		return ti.entries[i].id < ti.entries[j].id
	})
	ti.dirty = false
}

// remove deletes the entry for id. Linear scan — O(n).
// No-op if id is not present.
func (ti *temporalIndex) remove(id snowflake.ID) {
	for i, e := range ti.entries {
		if e.id == id {
			ti.entries = append(ti.entries[:i], ti.entries[i+1:]...)
			return
		}
	}
}

// queryAt returns IDs of all entries valid at instant t.
// Condition: from <= t AND (to == 0 OR to > t).
//
// Algorithm: binary search finds the rightmost position where from <= t,
// then scans leftward from that position collecting matches.
// Returns a sorted slice of IDs.
func (ti *temporalIndex) queryAt(t types.Instant) []snowflake.ID {
	if len(ti.entries) == 0 {
		return nil
	}
	ti.sortIfDirty()

	// Find the first index where from > t.
	// All entries at index < pos have from <= t.
	pos := sort.Search(len(ti.entries), func(i int) bool {
		return ti.entries[i].from > t
	})
	if pos == 0 {
		return nil // no entries with from <= t
	}

	var ids []snowflake.ID
	for i := 0; i < pos; i++ {
		e := ti.entries[i]
		if e.to == 0 || e.to > t {
			ids = append(ids, e.id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// queryOverlap returns IDs of all entries whose interval overlaps [start, end).
// Condition: from < end AND (to == 0 OR to > start).
//
// Returns a sorted slice of IDs.
func (ti *temporalIndex) queryOverlap(start, end types.Instant) []snowflake.ID {
	if len(ti.entries) == 0 {
		return nil
	}
	ti.sortIfDirty()

	// Find the first index where from >= end.
	// All entries at index < pos have from < end.
	pos := sort.Search(len(ti.entries), func(i int) bool {
		return ti.entries[i].from >= end
	})
	if pos == 0 {
		return nil
	}

	var ids []snowflake.ID
	for i := 0; i < pos; i++ {
		e := ti.entries[i]
		if e.to == 0 || e.to > start {
			ids = append(ids, e.id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// len returns the number of entries in the index.
func (ti *temporalIndex) len() int {
	return len(ti.entries)
}

// --- Store-level helpers ---

// nodeTemporalBounds returns the effective (from, to) for a node.
// Used when adding a node to a temporal index.
func nodeTemporalBounds(id snowflake.ID, tm *types.TemporalMetadata) (from, to types.Instant) {
	from = entityValidFrom(id, tm)
	if tm != nil {
		to = tm.ValidTo
	}
	return
}

// addNodeToTemporalIndexes adds a node to all temporal indexes that cover any of
// the node's label tokens. Caller must hold the store's write lock.
func addNodeToTemporalIndexes(idxs map[uint16]*temporalIndex, n *types.Node, id snowflake.ID) {
	if len(idxs) == 0 {
		return
	}
	from, to := nodeTemporalBounds(id, n.Temporal())
	for _, tok := range n.AllLabelTokens() {
		if ti, ok := idxs[tok.Value()]; ok {
			ti.add(id, from, to)
		}
	}
}

// removeNodeFromTemporalIndexes removes a node from all temporal indexes.
// Caller must hold the store's write lock.
func removeNodeFromTemporalIndexes(idxs map[uint16]*temporalIndex, n *types.Node, id snowflake.ID) {
	if len(idxs) == 0 {
		return
	}
	for _, tok := range n.AllLabelTokens() {
		if ti, ok := idxs[tok.Value()]; ok {
			ti.remove(id)
		}
	}
}

// purgeNodeFromAllTemporalIndexes removes a node ID from every temporal index.
// Used during corrupt-node deletion when label token data is unavailable.
// Caller must hold the store's write lock.
func purgeNodeFromAllTemporalIndexes(idxs map[uint16]*temporalIndex, id snowflake.ID) {
	for _, ti := range idxs {
		ti.remove(id)
	}
}
