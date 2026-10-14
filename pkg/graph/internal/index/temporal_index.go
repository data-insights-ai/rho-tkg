package index

import (
	"cmp"
	"math"
	"slices"
	"sync"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// maxInstant is the effective upper bound for an open-ended interval (To == 0).
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
// Entries is sorted by (From, ID) and treated as the in-order traversal of an
// implicit balanced BST (root = midpoint); subMax[i] is the max effective upper
// bound (open = +inf) of the subtree at i, so stabbing queries prune expired
// subtrees in O(1): O(log n + k) per query.
//
// Add/Remove run under the store's write lock; QueryAt/QueryOverlap under the
// read lock. sortMu guards the lazy sort/augmentation for concurrent readers.
//
// Cost: Add and Remove O(n) (Add replaces via Remove); AddKnownAbsent and
// Extend O(1) amortized (posByID); first query after a mutation pays
// O(n log n) sort + O(n) augmentation, then O(log n + k).
type TemporalIndex struct {
	sortMu   sync.Mutex      // serialises concurrent sort/augmentation transitions under RLock
	Entries  []IntervalEntry // sorted by (From ASC, ID ASC) when not dirty
	subMax   []types.Instant // subMax[i] = max effective To over the implicit subtree rooted at i
	dirty    bool            // true when entries changed but slice not yet sorted/augmented
	Building bool            // true while CreateTemporalIndex is still backfilling
	Mutated  map[snowflake.ID]struct{}
	// DiskWritten, non-nil only while a disk-backed create backfills, holds the IDs
	// whose on-disk row a write added or removed since the build started: the create
	// then writes no row for them (the write's row is newer than the backfill's
	// fetch). Mutated cannot stand in for it (PutNodeVersion marks without a disk
	// row; label removal writes a disk delete without marking).
	DiskWritten map[snowflake.ID]struct{}
	// byID mirrors each id's envelope bounds for O(1) EnvelopeOf; bounds, not slice
	// positions, so re-sorts never invalidate it. Kept by every entry mutator.
	byID map[snowflake.ID]IntervalEntry
	// posByID mirrors each id's CURRENT slice position so Extend finds an existing
	// entry in O(1) (a linear scan made N Extends O(n^2)). Unlike byID it must be
	// rebuilt after every re-sort and after Remove shifts entries (backlog 16h).
	posByID map[snowflake.ID]int
}

// EnvelopeOf returns the per-node valid-time envelope [from, to) recorded for id,
// and ok=false when the index does not cover id. The core resolver prunes a
// candidate ONLY when ok and the envelope provably cannot overlap the query; an
// uncovered id is never pruned, so incomplete membership costs pruning, not correctness.
func (ti *TemporalIndex) EnvelopeOf(id snowflake.ID) (from, to types.Instant, ok bool) {
	if ti == nil || ti.byID == nil {
		return 0, 0, false
	}
	e, present := ti.byID[id]
	if !present {
		return 0, 0, false
	}
	return e.From, e.To, true
}

// setByID records id's current envelope bounds for EnvelopeOf.
func (ti *TemporalIndex) setByID(id snowflake.ID, from, to types.Instant) {
	if ti.byID == nil {
		ti.byID = make(map[snowflake.ID]IntervalEntry)
	}
	ti.byID[id] = IntervalEntry{From: from, To: to, ID: id}
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
	ti.setByID(id, from, to)
	ti.setPos(id, len(ti.Entries)-1)
	ti.markMutated(id)
	ti.dirty = true
}

// unionTo returns the wider (later-ending) of two valid-to bounds, treating 0 as
// open-ended (+infinity) — so unioning any bounded end with an open interval stays
// open. Mirrors effTo's 0-is-open convention.
func unionTo(a, b types.Instant) types.Instant {
	if a == 0 || b == 0 {
		return 0 // open-ended absorbs any bounded end
	}
	if b > a {
		return b
	}
	return a
}

// Extend inserts an entry for id, or UNIONs it into the existing entry:
// [min(From), unionTo(To)]. Unlike Add (replace), this GROWS a per-node valid-time
// ENVELOPE, so a node whose PAST version overlapped a probe stays a candidate
// after its current version moves off (rule 16); the resolver filters the
// over-inclusion. O(1) via posByID (backlog 16h). Caller holds the write lock.
func (ti *TemporalIndex) Extend(id snowflake.ID, from, to types.Instant) {
	if ti == nil {
		return
	}
	if i, ok := ti.posByID[id]; ok {
		if from < ti.Entries[i].From {
			ti.Entries[i].From = from
		}
		ti.Entries[i].To = unionTo(ti.Entries[i].To, to)
		ti.setByID(id, ti.Entries[i].From, ti.Entries[i].To)
		ti.markMutated(id)
		ti.dirty = true
		return
	}
	ti.Entries = append(ti.Entries, IntervalEntry{From: from, To: to, ID: id})
	ti.setByID(id, from, to)
	ti.setPos(id, len(ti.Entries)-1)
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
	ti.setByID(id, from, to)
	ti.setPos(id, len(ti.Entries)-1)
	ti.dirty = true
}

// setPos records id's current index into Entries. See the posByID field doc.
func (ti *TemporalIndex) setPos(id snowflake.ID, idx int) {
	if ti.posByID == nil {
		ti.posByID = make(map[snowflake.ID]int)
	}
	ti.posByID[id] = idx
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

// sortIfDirty sorts by (From, ID) and rebuilds the maxTo augmentation if modified
// since the last query. sortMu serialises concurrent callers holding only the read lock.
func (ti *TemporalIndex) sortIfDirty() {
	if ti == nil {
		return
	}
	ti.sortMu.Lock()
	defer ti.sortMu.Unlock()
	if !ti.dirty {
		return
	}
	slices.SortFunc(ti.Entries, func(a, b IntervalEntry) int {
		if a.From != b.From {
			if a.From < b.From {
				return -1
			}
			return 1
		}
		return cmp.Compare(a.ID, b.ID)
	})
	// posByID tracks slice position, so a re-sort must rebuild it (O(n), once per dirty batch).
	for i, e := range ti.Entries {
		ti.setPos(e.ID, i)
	}
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

// Remove deletes the entry for id (no-op if absent). O(n): a filter-copy shifts
// survivors; marks the index dirty because subMax now spans a stale length.
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
		// Every surviving entry's position shifts as out grows; record the new index now.
		ti.setPos(e.ID, len(out))
		out = append(out, e)
	}
	ti.Entries = out
	delete(ti.byID, id)
	delete(ti.posByID, id)
	ti.dirty = true
}

// QueryAt returns the sorted IDs of entries valid at instant t
// (from <= t AND (to == 0 OR to > t)). O(log n + k).
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

// QueryOverlap returns the sorted IDs of entries overlapping [start, end)
// (from < end AND (to == 0 OR to > start)). O(log n + k).
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

// AddNodeToTemporalIndexes folds the node's effective [from,to) into every
// temporal index covering one of its labels. Since B4 the index is a per-node
// valid-time ENVELOPE (sound superset), so this UNIONs; alias of
// ExtendNodeInTemporalIndexes. Caller holds the write lock.
func AddNodeToTemporalIndexes(idxs map[uint16]*TemporalIndex, n *types.Node, id snowflake.ID) {
	ExtendNodeInTemporalIndexes(idxs, n, id)
}

// ExtendNodeInTemporalIndexes UNIONs the node's effective [from,to) into the
// envelope of every temporal index covering one of its labels, keeping past-version
// intervals covered. Caller holds the write lock.
func ExtendNodeInTemporalIndexes(idxs map[uint16]*TemporalIndex, n *types.Node, id snowflake.ID) {
	if len(idxs) == 0 {
		return
	}
	from, to := NodeTemporalBounds(id, n.Temporal())
	for i := 0; i < n.LabelTokenCount(); i++ {
		if ti, ok := idxs[n.LabelTokenRawAt(i)]; ok {
			ti.Extend(id, from, to)
		}
	}
}

// RemoveNodeFromTemporalIndexes is a NO-OP since B4: the envelope is grow-only so
// a node's past versions (and a deleted node's history) stay candidates for
// historical temporal queries. Over-inclusion is filtered by nodesByLabelFromIDs and
// the core chain resolver. Shrinking happens only on index drop
// (DropTemporalIndex) or corrupt-node purge (PurgeNodeFromAllTemporalIndexes).
func RemoveNodeFromTemporalIndexes(_ map[uint16]*TemporalIndex, _ *types.Node, _ snowflake.ID) {
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

// --- Relationship-type temporal indexes (BACKLOG 21c) ---
//
// Mirror of the label-keyed helpers above, held in a SEPARATE map keyed by
// rel-type token; label and rel-type token namespaces are independent and must
// never share an index.

// RelTemporalBounds returns the effective (from, to) for a relationship.
// RelTemporalBounds returns the effective (from, to) for a relationship (same
// computation as NodeTemporalBounds).
func RelTemporalBounds(id snowflake.ID, tm *types.TemporalMetadata) (from, to types.Instant) {
	return NodeTemporalBounds(id, tm)
}

// AddRelToTemporalIndexes folds the relationship's effective [from,to) into the
// temporal index covering its type, if any. Alias of ExtendRelInTemporalIndexes.
// Caller holds the write lock.
func AddRelToTemporalIndexes(idxs map[uint16]*TemporalIndex, r *types.Relationship, id snowflake.ID) {
	ExtendRelInTemporalIndexes(idxs, r, id)
}

// ExtendRelInTemporalIndexes UNIONs the relationship's effective [from,to) into
// the envelope of the temporal index covering its type (one type, so at most one
// index). Caller holds the write lock.
func ExtendRelInTemporalIndexes(idxs map[uint16]*TemporalIndex, r *types.Relationship, id snowflake.ID) {
	if len(idxs) == 0 {
		return
	}
	if ti, ok := idxs[r.TypeToken().Value()]; ok {
		from, to := RelTemporalBounds(id, r.Temporal())
		ti.Extend(id, from, to)
	}
}

// RelCoveredInTemporalIndexes reports whether the temporal index covering r's
// type already holds an envelope for id. Caller must hold the store's lock.
func RelCoveredInTemporalIndexes(idxs map[uint16]*TemporalIndex, r *types.Relationship, id snowflake.ID) bool {
	ti, ok := idxs[r.TypeToken().Value()]
	if !ok {
		return false
	}
	_, _, covered := ti.EnvelopeOf(id)
	return covered
}

// RelTemporalIndexBuilding reports whether the temporal index covering r's
// type is still being created. Caller must hold the store's lock.
func RelTemporalIndexBuilding(idxs map[uint16]*TemporalIndex, r *types.Relationship) bool {
	ti, ok := idxs[r.TypeToken().Value()]
	return ok && ti.Building
}

// RemoveRelFromTemporalIndexes is a NO-OP, like RemoveNodeFromTemporalIndexes.
func RemoveRelFromTemporalIndexes(_ map[uint16]*TemporalIndex, _ *types.Relationship, _ snowflake.ID) {
}

// PurgeRelFromAllTemporalIndexes removes a relationship ID from every rel-type
// temporal index. Used during corrupt-relationship deletion when type token
// data is unavailable. Caller must hold the store's write lock.
func PurgeRelFromAllTemporalIndexes(idxs map[uint16]*TemporalIndex, id snowflake.ID) {
	for _, ti := range idxs {
		if ti != nil {
			ti.Remove(id)
		}
	}
}
