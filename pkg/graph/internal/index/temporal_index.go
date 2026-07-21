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
// Complexity (BACKLOG 16h corrected these — Add's prior doc claimed O(1)
// amortized, but it has ALWAYS called Remove first for replace semantics,
// so it was never actually O(1); Extend's O(n) scan is now O(1) via posByID):
//   - Add:            O(n) — Remove(id) first for replace semantics, then an
//     O(1) append
//   - AddKnownAbsent: O(1) amortized append (no existing-id check)
//   - Extend:         O(1) amortized — posByID lookup finds an existing
//     entry directly; O(1) append if absent
//   - Remove:         O(n) — must shift every surviving entry to preserve
//     order; inherent to a filter-copy, not something posByID changes
//   - QueryAt:        O(n log n) sort + O(n) augmentation + O(n) posByID
//     rebuild (once per dirty batch), then O(log n + k) per stabbing query
//   - QueryOverlap:   same as QueryAt
type TemporalIndex struct {
	sortMu   sync.Mutex      // serialises concurrent sort/augmentation transitions under RLock
	Entries  []IntervalEntry // sorted by (From ASC, ID ASC) when not dirty
	subMax   []types.Instant // subMax[i] = max effective To over the implicit subtree rooted at i
	dirty    bool            // true when entries changed but slice not yet sorted/augmented
	Building bool            // true while CreateTemporalIndex is still backfilling
	Mutated  map[snowflake.ID]struct{}
	// byID mirrors each id's current envelope bounds for O(1) EnvelopeOf lookups
	// (the B4 candidate-prune primitive). Entries stays sorted-by-From for stabbing
	// queries; byID stores BOUNDS (not slice positions), so a re-sort never
	// invalidates it. Maintained in every entry mutator (Add / AddKnownAbsent /
	// Extend / Remove).
	byID map[snowflake.ID]IntervalEntry
	// posByID mirrors each id's CURRENT index into Entries — BACKLOG 16h. Unlike
	// byID this DOES invalidate on reorder, so (unlike byID) it must be rebuilt
	// after every sortIfDirty re-sort and after Remove's filter-copy shifts
	// surviving entries down. It exists purely so Extend can find "does id
	// already have an entry, and if so where" in O(1) instead of an O(n) linear
	// scan — the O(n) scan on every call is what turns a bulk sequence of N
	// Extend calls (e.g. importing a node's full history, one Extend per
	// version) into O(n²) instead of O(n). Remove's own Entries rebuild stays
	// O(n) per call either way (it must shift every surviving entry to preserve
	// sort-adjacent order), so posByID does not change Remove's complexity —
	// only Extend's, which is the actual per-node-mutation hot path the finding
	// names.
	posByID map[snowflake.ID]int
}

// EnvelopeOf returns the per-node valid-time envelope [from, to) recorded for id,
// and ok=false when the index does not cover id. It is the B4 candidate-prune
// primitive: the core resolver drops a temporal candidate ONLY when the index
// vouches for it (ok) AND its envelope provably cannot overlap the query — an
// id the index does not cover (ok=false) is never pruned, so incomplete index
// membership costs recall of pruning, never correctness.
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

// Extend inserts an entry for id, or UNIONs it into the node's existing entry:
// [min(From), unionTo(To)]. Where Add REPLACES with the current version's interval,
// Extend GROWS a per-node VALID-TIME ENVELOPE across all versions. This is the
// sound-superset property the core resolver needs for predicate-anywhere temporal
// queries (rule 16): a node whose PAST version overlapped the probe must stay a
// candidate even after its current version moves off it — the envelope keeps
// covering the past interval, and the resolver filters the over-inclusion
// precisely. Must be called under the store's write lock.
//
// BACKLOG 16h: finds an existing entry via posByID in O(1) rather than a
// linear scan — the scan previously turned a bulk sequence of N Extend calls
// (e.g. importing a node's full history, one Extend per version) into
// O(n²), since Extend is called once PER NODE MUTATION to a
// temporally-indexed label.
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
	slices.SortFunc(ti.Entries, func(a, b IntervalEntry) int {
		if a.From != b.From {
			if a.From < b.From {
				return -1
			}
			return 1
		}
		return cmp.Compare(a.ID, b.ID)
	})
	// posByID (BACKLOG 16h) tracks slice POSITION, unlike byID's sort-invariant
	// bounds — a re-sort reorders every entry, so it must be rebuilt here. O(n),
	// same complexity class as the sort/augmentation this function already does
	// once per dirty batch, not once per entry.
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

// Remove deletes the entry for id. O(n) — a filter-copy that shifts every
// surviving entry left of the removed one, which posByID (BACKLOG 16h)
// intentionally does not change: preserving relative order without breaking
// the "posByID always reflects the current index" invariant needs the same
// O(n) work either way, and Remove is not the bulk-mutation hot path Extend
// is. No-op if id is not present. The filtered slice stays sorted, but the
// maxTo augmentation now spans a stale length, so the index is marked dirty
// to force a rebuild on the next query.
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
		// Every surviving entry's position shifts as out grows — record its
		// NEW index now rather than leaving posByID stale until the next sort.
		ti.setPos(e.ID, len(out))
		out = append(out, e)
	}
	ti.Entries = out
	delete(ti.byID, id)
	delete(ti.posByID, id)
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

// AddNodeToTemporalIndexes folds the node's current effective [from,to) into every
// temporal index covering one of its labels. Since B4 the temporal index is a
// per-node valid-time ENVELOPE (a SOUND SUPERSET for the core resolver's
// predicate-anywhere candidate narrowing), so this UNIONs (grows the envelope)
// rather than replacing with the current version — it is an alias of
// ExtendNodeInTemporalIndexes kept for the many existing maintenance call sites.
// Caller must hold the store's write lock.
func AddNodeToTemporalIndexes(idxs map[uint16]*TemporalIndex, n *types.Node, id snowflake.ID) {
	ExtendNodeInTemporalIndexes(idxs, n, id)
}

// ExtendNodeInTemporalIndexes UNIONs the node's current effective [from,to) into
// the per-node envelope of every temporal index covering one of its labels. Unlike
// AddNodeToTemporalIndexes (which REPLACES the entry with the current version's
// interval), this GROWS the envelope so a node's past-version interval stays
// covered — the sound-superset property the core resolver relies on for
// predicate-anywhere temporal queries. Caller must hold the store's write lock.
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

// RemoveNodeFromTemporalIndexes is a NO-OP since B4. The temporal index is now a
// per-node valid-time ENVELOPE that is APPEND-ONLY (grow-only) per node — mirroring
// the K1 label-tx-membership sidecar: an update or delete must NOT shrink the
// envelope, because a node's PAST version interval (and a deleted node's history)
// must remain a candidate for predicate-anywhere / historical temporal queries. The
// envelope is a SOUND SUPERSET; over-inclusion is filtered by the store's
// current-row predicate (nodesByLabelFromIDs) and by the core chain resolver, both
// of which stay authoritative. Removal that truly must shrink the index happens only
// on index DROP (DropTemporalIndex) or corrupt-node purge
// (PurgeNodeFromAllTemporalIndexes, which calls ti.Remove directly). Kept as a
// no-op — rather than deleting its ~34 maintenance call sites — so the create /
// update / delete / replace / batch write paths need no change.
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
// A relationship-type-keyed mirror of the label-keyed helpers above. The
// underlying TemporalIndex type is entity-agnostic (keyed by raw
// snowflake.ID), so relationship-type indexes reuse it directly against a
// SEPARATE map (keyed by rel-type token, never merged with the label-token
// map — the two token namespaces are independent registries and a numeric
// collision between a label token and a rel-type token must not cross-pollute
// their indexes).

// RelTemporalBounds returns the effective (from, to) for a relationship.
// Identical computation to NodeTemporalBounds (both delegate to the same
// entity-agnostic storepkg.EntityValidFrom); kept as a separate name for
// call-site clarity, matching this codebase's node/rel mirror convention.
func RelTemporalBounds(id snowflake.ID, tm *types.TemporalMetadata) (from, to types.Instant) {
	return NodeTemporalBounds(id, tm)
}

// AddRelToTemporalIndexes folds the relationship's current effective [from,to)
// into the temporal index covering its type, if one exists. Alias of
// ExtendRelInTemporalIndexes, mirroring AddNodeToTemporalIndexes. Caller must
// hold the store's write lock.
func AddRelToTemporalIndexes(idxs map[uint16]*TemporalIndex, r *types.Relationship, id snowflake.ID) {
	ExtendRelInTemporalIndexes(idxs, r, id)
}

// ExtendRelInTemporalIndexes UNIONs the relationship's current effective
// [from,to) into the per-relationship envelope of the temporal index covering
// its type (a relationship has exactly one type, unlike a node's multiple
// labels, so this indexes at most one map entry rather than looping). Grows
// the envelope rather than replacing it — the same sound-superset property
// AddNodeToTemporalIndexes relies on. Caller must hold the store's write lock.
func ExtendRelInTemporalIndexes(idxs map[uint16]*TemporalIndex, r *types.Relationship, id snowflake.ID) {
	if len(idxs) == 0 {
		return
	}
	if ti, ok := idxs[r.TypeToken().Value()]; ok {
		from, to := RelTemporalBounds(id, r.Temporal())
		ti.Extend(id, from, to)
	}
}

// RemoveRelFromTemporalIndexes is a NO-OP, mirroring RemoveNodeFromTemporalIndexes
// — see that function's doc comment for the full append-only-envelope rationale.
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
