package memory

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// NodeMutationEpoch returns the current global node-mutation epoch. The consumer
// re-checks it after a lock-free column scan: if a writer mutated any node during
// the scan the epoch advances, and the consumer discards the (now-torn) result and
// falls back. This is the Gate-2 staleness check (Pattern 37).
func (ms *Store) NodeMutationEpoch() uint64 {
	if ms == nil {
		return 0
	}
	return ms.nodeEpoch.Load()
}

// RelMutationEpoch returns the current global relationship-mutation epoch. The X5
// expand-aggregation column path reads ADJACENCY, so its Gate-2 re-check must
// sample this (before the first anchor) and re-verify it after the scan: a
// concurrent edge insert/delete advances it, and the consumer discards the
// (now-torn) aggregate and falls back. Distinct from NodeMutationEpoch.
func (ms *Store) RelMutationEpoch() uint64 {
	if ms == nil {
		return 0
	}
	return ms.relEpoch.Load()
}

// ForEachDocValues streams the requested property columns for a label in ordinal
// order, building/refreshing the immutable column snapshot under the write lock if
// stale, then iterating it lock-free.
//
//   - Returns ok=false (NOT an error) when the column path is unusable: the label
//     is empty, over the size cap, or any requested property is not a uniformly
//     numeric/string column. The caller falls back to the per-node path.
//   - Membership is the FULL label index (ms.labelIdx) — the same unfiltered set
//     the zero-QueryOpts scan returns. No valid-time predicate: a
//     non-temporal aggregation counts every label member, including logically
//     expired-but-not-deleted ones.
//   - The returned gen is the epoch the snapshot was built at; the caller re-checks
//     NodeMutationEpoch()==gen after consuming fn's results (Gate 2).
func (ms *Store) ForEachDocValues(labelToken uint16, propKeys []string,
	fn func(id types.NodeID, vals []any, present []bool) bool) (gen uint64, ok bool, err error) {
	if ms == nil {
		return 0, false, ErrNilStore
	}

	// Phase A: under the write lock (atomic vs every mutation, which also takes
	// the write lock), ensure a fresh column and grab the immutable pointer.
	ms.mu.Lock()
	if cerr := ms.checkOpenLocked(); cerr != nil {
		ms.mu.Unlock()
		return 0, false, cerr
	}
	set := ms.labelIdx[labelToken]
	if len(set) == 0 || len(set) > indexpkg.MaxDocValuesNodes {
		ms.mu.Unlock()
		return 0, false, nil // empty or over cap → fall back
	}
	cur := ms.nodeEpoch.Load()
	col := ms.docColumns[labelToken]
	if col == nil || col.Epoch() != cur || !col.HasAll(propKeys) {
		col = ms.refreshLabelColumnsLocked(labelToken, set, propKeys, ms.docColumns[labelToken], cur)
		ms.docColumns[labelToken] = col
	}
	ms.mu.Unlock()

	// Phase B: iterate the immutable snapshot lock-free. If a requested column was
	// not buildable, decline (the caller falls back for the whole query).
	if !col.HasAll(propKeys) {
		return col.Epoch(), false, nil
	}
	col.ForEachRow(propKeys, fn)
	return col.Epoch(), true, nil
}

// ForEachDocValuesMulti streams the requested property columns for a LABEL
// INTERSECTION (a multi-label pattern like (p:A:B)) in ordinal order, building/
// refreshing the immutable column snapshot under the write lock if stale, then
// iterating it lock-free. Same contract as ForEachDocValues:
//
//   - Returns ok=false (NOT an error) when unusable: an empty intersection, over
//     the size cap, or any requested property not a uniformly numeric/string
//     column. The caller falls back to the general (per-node) path.
//   - Membership is the INTERSECTION of the full label indexes — the same set a
//     multi-label scan returns (no valid-time filter). A node absent
//     from any one label is excluded; count(*) counts the surviving rows.
//   - The returned gen is the snapshot's epoch; the caller re-checks
//     NodeMutationEpoch()==gen after consuming fn's results (Gate 2).
func (ms *Store) ForEachDocValuesMulti(toks []uint16, propKeys []string,
	fn func(id types.NodeID, vals []any, present []bool) bool) (gen uint64, ok bool, err error) {
	if ms == nil {
		return 0, false, ErrNilStore
	}

	ms.mu.Lock()
	if cerr := ms.checkOpenLocked(); cerr != nil {
		ms.mu.Unlock()
		return 0, false, cerr
	}
	cur := ms.nodeEpoch.Load()
	key := indexpkg.MultiLabelKey(toks)
	col := ms.docColumnsMulti[key]
	// Compute the intersection ONLY when a (re)build is needed. A fresh cached
	// snapshot is trusted as-is: every label change bumps the epoch, so a matching
	// epoch guarantees the intersection membership is unchanged — re-intersecting
	// on every call would re-probe the smaller label set (O(min|label|)) for
	// nothing (the 50k-probe steady-state regression this avoids).
	if col == nil || col.Epoch() != cur || !col.HasAll(propKeys) {
		set := ms.intersectLabelsLocked(toks)
		if len(set) == 0 || len(set) > indexpkg.MaxDocValuesNodes {
			ms.mu.Unlock()
			return 0, false, nil // empty intersection or over cap → fall back
		}
		// Intersection membership, NOT single-label: a per-label append record cannot
		// say whether an appended node joins A∩B, so this path always rebuilds.
		col = ms.buildColumnsLocked(set, propKeys, col, cur)
		ms.docColumnsMulti[key] = col
	}
	ms.mu.Unlock()

	if !col.HasAll(propKeys) {
		return col.Epoch(), false, nil
	}
	col.ForEachRow(propKeys, fn)
	return col.Epoch(), true, nil
}

// intersectLabelsLocked returns the set of node IDs present in EVERY label token,
// driven by the smallest label set (so the probe count is O(min |label|)). Returns
// nil (empty) if any token is unknown/empty. Must hold ms.mu.
func (ms *Store) intersectLabelsLocked(toks []uint16) map[types.NodeID]struct{} {
	if len(toks) == 0 {
		return nil
	}
	var smallest map[types.NodeID]struct{}
	for _, t := range toks {
		s := ms.labelIdx[t]
		if len(s) == 0 {
			return nil // a label with no members → empty intersection
		}
		if smallest == nil || len(s) < len(smallest) {
			smallest = s
		}
	}
	out := make(map[types.NodeID]struct{}, len(smallest))
	for id := range smallest {
		inAll := true
		for _, t := range toks {
			if _, ok := ms.labelIdx[t][id]; !ok {
				inAll = false
				break
			}
		}
		if inAll {
			out[id] = struct{}{}
		}
	}
	return out
}

// DocValuesSnapshot returns a RANDOM-ACCESS point-lookup handle over a single
// label's column snapshot (the X5 expand-aggregation target side: look up b's
// properties by node ID without materializing b). Shares the same cached docColumns
// snapshot as ForEachDocValues, building/refreshing under the write lock if stale.
//
//   - ok=false (NOT an error) when unusable: empty/over-cap label, OR any requested
//     property is not a uniformly numeric/string column (the
//     consumer declines the whole query to the per-node path rather than reading an
//     unbuildable column as nulls).
//   - gen is the snapshot epoch; the caller re-checks NodeMutationEpoch()==gen AND
//     RelMutationEpoch()==relGen after the scan (Gate 2).
func (ms *Store) DocValuesSnapshot(labelToken uint16, propKeys []string) (snap types.NodeColumnReader, gen uint64, ok bool, err error) {
	if ms == nil {
		return nil, 0, false, ErrNilStore
	}
	ms.mu.Lock()
	if cerr := ms.checkOpenLocked(); cerr != nil {
		ms.mu.Unlock()
		return nil, 0, false, cerr
	}
	set := ms.labelIdx[labelToken]
	if len(set) == 0 || len(set) > indexpkg.MaxDocValuesNodes {
		ms.mu.Unlock()
		return nil, 0, false, nil
	}
	cur := ms.nodeEpoch.Load()
	col := ms.docColumns[labelToken]
	if col == nil || col.Epoch() != cur || !col.HasAll(propKeys) {
		col = ms.refreshLabelColumnsLocked(labelToken, set, propKeys, col, cur)
		ms.docColumns[labelToken] = col
	}
	ms.mu.Unlock()

	ps, pok := col.NewPointSnapshot(propKeys)
	if !pok {
		return nil, col.Epoch(), false, nil // an unbuildable key → decline (Trap B)
	}
	return ps, col.Epoch(), true, nil
}

// buildColumnsLocked builds a fresh immutable column snapshot at epoch `cur` over
// the given membership set, covering the union of the previously-built keys (old,
// may be nil) and the newly requested ones (so the columns stay at one epoch as
// queries request different property subsets). Shared by the single-label and
// label-intersection paths — only the membership set and the old snapshot differ.
// Must hold ms.mu (write lock).
func (ms *Store) buildColumnsLocked(set map[types.NodeID]struct{}, requested []string,
	old *indexpkg.LabelDocValues, cur uint64) *indexpkg.LabelDocValues {

	ids := make([]types.NodeID, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}

	keys := requested
	if old != nil {
		keys = indexpkg.UnionKeys(old.Keys(), requested)
	}

	getProp := func(id types.NodeID, key string) (any, bool) {
		n, ok := ms.nodes[id]
		if !ok {
			return nil, false
		}
		return n.GetProperty(key)
	}
	getTemporal := func(id types.NodeID) (int64, int64, bool) {
		n, ok := ms.nodes[id]
		if !ok {
			return 0, 0, false
		}
		f, t, has := n.ValidRange()
		// See core/docvalues.go: an unset ValidFrom resolves to the mint time.
		if !has || f == 0 {
			f = storeutil.SnowflakeInstant(id.SnowflakeID())
		}
		return int64(f), int64(t), true
	}
	ms.columnRebuilds.Add(1)
	return indexpkg.BuildLabelDocValues(cur, ids, keys, getProp, getTemporal)
}

// refreshLabelColumnsLocked returns a snapshot for one label at epoch cur, preferring
// an APPEND-EXTEND of the cached one over a full rebuild (R3). Falls back to
// buildColumnsLocked for anything that is not a clean append — including a poisoned
// record, an epoch that does not match, a snapshot missing a requested key (Extend
// adds rows, never columns), and every shape Extend itself refuses.
// Must hold ms.mu (write lock).
func (ms *Store) refreshLabelColumnsLocked(token uint16, set map[types.NodeID]struct{},
	requested []string, old *indexpkg.LabelDocValues, cur uint64) *indexpkg.LabelDocValues {

	if old != nil {
		keys := indexpkg.UnionKeys(old.Keys(), requested)
		if old.HasAll(keys) {
			if added, ok := ms.appendDeltaFor(token, cur, old.Epoch()); ok {
				getProp := func(id types.NodeID, key string) (any, bool) {
					n, present := ms.nodes[id]
					if !present {
						return nil, false
					}
					return n.GetProperty(key)
				}
				getTemporal := func(id types.NodeID) (int64, int64, bool) {
					n, present := ms.nodes[id]
					if !present {
						return 0, 0, false
					}
					f, t, has := n.ValidRange()
					if !has || f == 0 {
						f = storeutil.SnowflakeInstant(id.SnowflakeID())
					}
					return int64(f), int64(t), true
				}
				if ext := old.Extend(cur, added, getProp, getTemporal); ext != nil {
					ms.clearAppendDeltaFor(token)
					ms.columnExtends.Add(1)
					return ext
				}
			}
		}
	}
	col := ms.buildColumnsLocked(set, requested, old, cur)
	ms.clearAppendDeltaFor(token)
	return col
}
