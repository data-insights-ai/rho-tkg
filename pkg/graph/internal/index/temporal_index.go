package index

import (
	"math"
	"sort"
	"sync"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// maxInstant is the effective upper bound for an open-ended interval (To == 0).
// Treating "still valid" as +∞ lets the same comparison drive both finite and
// open intervals through the interval-tree pruning.
const maxInstant = types.Instant(math.MaxInt64)

// effTo maps a stored upper bound to its effective value: open-ended (0) is +∞.
func effTo(to types.Instant) types.Instant {
	if to == 0 {
		return maxInstant
	}
	return to
}

// IntervalEntry is a single entry in the temporal index.
type IntervalEntry struct {
	From types.Instant
	To   types.Instant // 0 = open-ended / still valid
	ID   snowflake.ID
}

// TemporalIndex is a maxTo-augmented interval index keyed by (from ASC, id ASC).
//
// Entries is kept sorted by (From ASC, ID ASC). On top of it the index treats the
// sorted slice as the in-order traversal of an implicit balanced BST (the root of
// any sub-range is its midpoint — no node allocation). subMax[i] holds the maximum
// effective upper bound (open-ended treated as +∞) over the subtree rooted at i.
// That augmentation lets a stabbing query prune any subtree whose intervals have
// all expired (maxTo <= probe) in O(1), giving output-sensitive O(log n + k) queries
// instead of the previous O(n) scan over every interval that started before the probe.
//
// Add/Remove are called under the store's write lock (ms.mu.Lock / bs.idxMu.Lock).
// QueryAt/QueryOverlap are called under the store's read lock (RLock), which allows
// multiple goroutines to query concurrently. sortIfDirty is protected by sortMu so
// that concurrent readers do not race on the sort/augmentation transition.
//
// Complexity:
//   - Add:          O(1) amortized append; sort + augmentation deferred to first query
//   - Remove:       O(n) linear scan
//   - QueryAt:      O(n log n) sort + O(n) augmentation (once per dirty batch),
//     then O(log n + k) per stabbing query
//   - QueryOverlap: same as QueryAt
type TemporalIndex struct {
	sortMu   sync.Mutex      // serialises concurrent sort/augmentation transitions under RLock
	Entries  []IntervalEntry // sorted by (From ASC, ID ASC) when not dirty
	subMax   []types.Instant // subMax[i] = max effective To over the implicit subtree rooted at i
	dirty    bool            // true when entries changed but slice not yet sorted/augmented
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

// sortIfDirty sorts entries by (From ASC, ID ASC) and rebuilds the maxTo
// augmentation if the index has been modified since the last query. Called at the
// start of every query. sortMu serialises concurrent callers holding only the
// store's read lock so that the sort/augmentation transition is observed atomically.
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
	ti.buildSubMax()
	ti.dirty = false
}

// buildSubMax populates subMax over the implicit balanced BST whose in-order
// traversal is the sorted Entries slice. Must be called with Entries already
// sorted and under sortMu.
func (ti *TemporalIndex) buildSubMax() {
	n := len(ti.Entries)
	if cap(ti.subMax) >= n {
		ti.subMax = ti.subMax[:n]
	} else {
		ti.subMax = make([]types.Instant, n)
	}
	if n > 0 {
		ti.fillSubMax(0, n)
	}
}

// fillSubMax computes and stores the maximum effective upper bound over the
// implicit subtree covering Entries[lo:hi] (root = midpoint), returning that max.
func (ti *TemporalIndex) fillSubMax(lo, hi int) types.Instant {
	if lo >= hi {
		return 0
	}
	mid := int(uint(lo+hi) >> 1)
	m := effTo(ti.Entries[mid].To)
	if l := ti.fillSubMax(lo, mid); l > m {
		m = l
	}
	if r := ti.fillSubMax(mid+1, hi); r > m {
		m = r
	}
	ti.subMax[mid] = m
	return m
}

// Remove deletes the entry for id. Linear scan — O(n).
// No-op if id is not present. The filtered slice stays sorted, but the maxTo
// augmentation now spans a stale length, so the index is marked dirty to force a
// rebuild on the next query.
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
	ti.dirty = true
}

// QueryAt returns IDs of all entries valid at instant t.
// Condition: from <= t AND (to == 0 OR to > t).
//
// Walks the implicit balanced BST, pruning whole subtrees whose intervals have all
// expired (subMax <= t) and right subtrees whose froms all exceed t. O(log n + k).
// Returns a sorted slice of IDs.
func (ti *TemporalIndex) QueryAt(t types.Instant) []snowflake.ID {
	if ti == nil || len(ti.Entries) == 0 {
		return nil
	}
	ti.sortIfDirty()

	var ids []snowflake.ID
	ti.stabAt(0, len(ti.Entries), t, &ids)
	if ids == nil {
		return nil
	}
	storepkg.SortSnowflakeIDs(ids)
	return ids
}

// stabAt collects entries containing point t (from <= t AND effTo > t) over the
// implicit subtree covering Entries[lo:hi].
func (ti *TemporalIndex) stabAt(lo, hi int, t types.Instant, out *[]snowflake.ID) {
	if lo >= hi {
		return
	}
	mid := int(uint(lo+hi) >> 1)
	if ti.subMax[mid] <= t {
		return // every interval in this subtree expired at/before t
	}
	ti.stabAt(lo, mid, t, out)
	e := ti.Entries[mid]
	if e.From <= t {
		if effTo(e.To) > t {
			*out = append(*out, e.ID)
		}
		// Right subtree froms are >= e.From <= t, so some may still satisfy from <= t.
		ti.stabAt(mid+1, hi, t, out)
	}
	// e.From > t: every right-subtree from is even larger — skip it.
}

// QueryOverlap returns IDs of all entries whose interval overlaps [start, end).
// Condition: from < end AND (to == 0 OR to > start).
//
// Walks the implicit balanced BST, pruning subtrees with subMax <= start (all end
// before the query) and right subtrees whose froms all reach end. O(log n + k).
// Returns a sorted slice of IDs.
func (ti *TemporalIndex) QueryOverlap(start, end types.Instant) []snowflake.ID {
	if ti == nil || len(ti.Entries) == 0 || start >= end {
		return nil
	}
	ti.sortIfDirty()

	var ids []snowflake.ID
	ti.stabOverlap(0, len(ti.Entries), start, end, &ids)
	if ids == nil {
		return nil
	}
	storepkg.SortSnowflakeIDs(ids)
	return ids
}

// stabOverlap collects entries overlapping [start, end) (from < end AND effTo > start)
// over the implicit subtree covering Entries[lo:hi].
func (ti *TemporalIndex) stabOverlap(lo, hi int, start, end types.Instant, out *[]snowflake.ID) {
	if lo >= hi {
		return
	}
	mid := int(uint(lo+hi) >> 1)
	if ti.subMax[mid] <= start {
		return // every interval in this subtree ends at/before start
	}
	ti.stabOverlap(lo, mid, start, end, out)
	e := ti.Entries[mid]
	if e.From < end {
		if effTo(e.To) > start {
			*out = append(*out, e.ID)
		}
		ti.stabOverlap(mid+1, hi, start, end, out)
	}
	// e.From >= end: every right-subtree from is >= e.From >= end — skip it.
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
