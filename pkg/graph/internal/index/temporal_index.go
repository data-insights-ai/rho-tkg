package index

import (
	"sort"
	"sync"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// IntervalEntry is a single entry in the temporal index.
type IntervalEntry struct {
	From types.Instant
	To   types.Instant // 0 = open-ended / still valid
	ID   snowflake.ID
}

// TemporalIndex is a sorted-slice interval index keyed by (from ASC, id ASC).
//
// Add/Remove are called under the store's write lock (ms.mu.Lock / bs.idxMu.Lock).
// QueryAt/QueryOverlap are called under the store's read lock (RLock), which allows
// multiple goroutines to query concurrently. sortIfDirty is protected by sortMu so
// that concurrent readers do not race on the sort transition.
//
// Complexity:
//   - Add:          O(1) amortized append; sort deferred to first query after write
//   - Remove:       O(n) linear scan
//   - QueryAt:      O(n log n) sort (once per dirty batch) + O(log n) binary search + O(k)
//   - QueryOverlap: same as QueryAt
//
// The O(n) query bound is acceptable for v3 (small-to-medium label sets).
// A future version may augment with maxTo for O(log n + k) stabbing queries.
type TemporalIndex struct {
	sortMu   sync.Mutex      // serialises concurrent sort transitions under RLock
	Entries  []IntervalEntry // sorted by (From ASC, ID ASC) when not dirty
	dirty    bool            // true when entries have been appended but not yet sorted
	Building bool            // true while CreateTemporalIndex is still backfilling
	Mutated  map[snowflake.ID]struct{}
}

// NewTemporalIndex allocates an empty temporal index.
func NewTemporalIndex() *TemporalIndex {
	return &TemporalIndex{}
}

// Add inserts or updates an entry for id with [from, to).
// If id already has an entry, it is removed first (replace semantics).
// Appends unsorted and marks dirty; sorting is deferred to the first query.
// Must be called under the store's write lock.
func (ti *TemporalIndex) Add(id snowflake.ID, from, to types.Instant) {
	if ti == nil {
		return
	}
	// Remove any existing entry for id first.
	ti.Remove(id)

	// Append unsorted — sort is deferred to QueryAt/QueryOverlap.
	ti.Entries = append(ti.Entries, IntervalEntry{From: from, To: to, ID: id})
	ti.markMutated(id)
	ti.dirty = true
}

// AddKnownAbsent appends an entry without scanning for an existing ID.
// Caller must prove id is not already present in this index.
// Must be called under the store's write lock.
func (ti *TemporalIndex) AddKnownAbsent(id snowflake.ID, from, to types.Instant) {
	if ti == nil {
		return
	}
	ti.Entries = append(ti.Entries, IntervalEntry{From: from, To: to, ID: id})
	ti.dirty = true
}

func (ti *TemporalIndex) markMutated(id snowflake.ID) {
	if ti != nil && ti.Mutated != nil {
		ti.Mutated[id] = struct{}{}
	}
}

// WasMutated reports whether id was touched while this index was being built.
func (ti *TemporalIndex) WasMutated(id snowflake.ID) bool {
	if ti == nil || ti.Mutated == nil {
		return false
	}
	_, ok := ti.Mutated[id]
	return ok
}

// ClearMutationTracking stops tracking concurrent writes after index creation.
func (ti *TemporalIndex) ClearMutationTracking() {
	if ti == nil {
		return
	}
	ti.Mutated = nil
}

// sortIfDirty sorts entries by (From ASC, ID ASC) if the index has been
// modified since the last sort. Called at the start of every query.
// sortMu serialises concurrent callers holding only the store's read lock.
func (ti *TemporalIndex) sortIfDirty() {
	if ti == nil {
		return
	}
	ti.sortMu.Lock()
	defer ti.sortMu.Unlock()
	if !ti.dirty {
		return
	}
	sort.Slice(ti.Entries, func(i, j int) bool {
		if ti.Entries[i].From != ti.Entries[j].From {
			return ti.Entries[i].From < ti.Entries[j].From
		}
		return ti.Entries[i].ID < ti.Entries[j].ID
	})
	ti.dirty = false
}

// Remove deletes the entry for id. Linear scan — O(n).
// No-op if id is not present.
func (ti *TemporalIndex) Remove(id snowflake.ID) {
	if ti == nil {
		return
	}
	ti.markMutated(id)
	out := ti.Entries[:0]
	for _, e := range ti.Entries {
		if e.ID == id {
			continue
		}
		out = append(out, e)
	}
	ti.Entries = out
}

// QueryAt returns IDs of all entries valid at instant t.
// Condition: from <= t AND (to == 0 OR to > t).
//
// Algorithm: binary search finds the rightmost position where from <= t,
// then scans leftward from that position collecting matches.
// Returns a sorted slice of IDs.
func (ti *TemporalIndex) QueryAt(t types.Instant) []snowflake.ID {
	if ti == nil || len(ti.Entries) == 0 {
		return nil
	}
	ti.sortIfDirty()

	// Find the first index where From > t.
	// All entries at index < pos have From <= t.
	pos := sort.Search(len(ti.Entries), func(i int) bool {
		return ti.Entries[i].From > t
	})
	if pos == 0 {
		return nil // no entries with From <= t
	}

	var ids []snowflake.ID
	for i := 0; i < pos; i++ {
		e := ti.Entries[i]
		if e.To == 0 || e.To > t {
			ids = append(ids, e.ID)
		}
	}
	storepkg.SortSnowflakeIDs(ids)
	return ids
}

// QueryOverlap returns IDs of all entries whose interval overlaps [start, end).
// Condition: from < end AND (to == 0 OR to > start).
//
// Returns a sorted slice of IDs.
func (ti *TemporalIndex) QueryOverlap(start, end types.Instant) []snowflake.ID {
	if ti == nil || len(ti.Entries) == 0 || start >= end {
		return nil
	}
	ti.sortIfDirty()

	// Find the first index where From >= end.
	// All entries at index < pos have From < end.
	pos := sort.Search(len(ti.Entries), func(i int) bool {
		return ti.Entries[i].From >= end
	})
	if pos == 0 {
		return nil
	}

	var ids []snowflake.ID
	for i := 0; i < pos; i++ {
		e := ti.Entries[i]
		if e.To == 0 || e.To > start {
			ids = append(ids, e.ID)
		}
	}
	storepkg.SortSnowflakeIDs(ids)
	return ids
}

// Len returns the number of entries in the index.
func (ti *TemporalIndex) Len() int {
	if ti == nil {
		return 0
	}
	return len(ti.Entries)
}

// --- Store-level helpers ---

// NodeTemporalBounds returns the effective (from, to) for a node.
// Used when adding a node to a temporal index.
func NodeTemporalBounds(id snowflake.ID, tm *types.TemporalMetadata) (from, to types.Instant) {
	from = storepkg.EntityValidFrom(id, tm)
	if tm != nil {
		to = tm.ValidTo
	}
	return
}

// AddNodeToTemporalIndexes adds a node to all temporal indexes that cover any of
// the node's label tokens. Caller must hold the store's write lock.
func AddNodeToTemporalIndexes(idxs map[uint16]*TemporalIndex, n *types.Node, id snowflake.ID) {
	if len(idxs) == 0 {
		return
	}
	from, to := NodeTemporalBounds(id, n.Temporal())
	for i := 0; i < n.LabelTokenCount(); i++ {
		if ti, ok := idxs[n.LabelTokenRawAt(i)]; ok {
			ti.Add(id, from, to)
		}
	}
}

// RemoveNodeFromTemporalIndexes removes a node from all temporal indexes.
// Caller must hold the store's write lock.
func RemoveNodeFromTemporalIndexes(idxs map[uint16]*TemporalIndex, n *types.Node, id snowflake.ID) {
	if len(idxs) == 0 {
		return
	}
	for i := 0; i < n.LabelTokenCount(); i++ {
		if ti, ok := idxs[n.LabelTokenRawAt(i)]; ok {
			ti.Remove(id)
		}
	}
}

// PurgeNodeFromAllTemporalIndexes removes a node ID from every temporal index.
// Used during corrupt-node deletion when label token data is unavailable.
// Caller must hold the store's write lock.
func PurgeNodeFromAllTemporalIndexes(idxs map[uint16]*TemporalIndex, id snowflake.ID) {
	for _, ti := range idxs {
		if ti != nil {
			ti.Remove(id)
		}
	}
}
