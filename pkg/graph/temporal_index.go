package graph

import (
	"sort"

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
// NOT thread-safe — callers must hold the store's write or read lock.
//
// Complexity:
//   - add:          O(n) amortized (binary search insertion + slice shift)
//   - remove:       O(n) linear scan
//   - queryAt:      O(n) — scans all entries with from <= t, filters by to
//   - queryOverlap: O(n) — same scan approach
//
// The O(n) query bound is acceptable for v3 (small-to-medium label sets).
// A future version may augment with maxTo for O(log n + k) stabbing queries.
type temporalIndex struct {
	entries []intervalEntry // sorted by (from ASC, id ASC)
}

// newTemporalIndex allocates an empty temporal index.
func newTemporalIndex() *temporalIndex {
	return &temporalIndex{}
}

// add inserts or updates an entry for id with [from, to).
// If id already has an entry, it is removed first (replace semantics).
// Uses binary search to find the insertion point — preserves sorted order.
func (ti *temporalIndex) add(id snowflake.ID, from, to types.Instant) {
	// Remove any existing entry for id first.
	ti.remove(id)

	// Find the insertion position (sorted by from ASC, then id ASC).
	pos := sort.Search(len(ti.entries), func(i int) bool {
		if ti.entries[i].from != from {
			return ti.entries[i].from > from
		}
		return ti.entries[i].id >= id
	})

	// Insert at pos by growing the slice.
	ti.entries = append(ti.entries, intervalEntry{})
	copy(ti.entries[pos+1:], ti.entries[pos:])
	ti.entries[pos] = intervalEntry{from: from, to: to, id: id}
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
