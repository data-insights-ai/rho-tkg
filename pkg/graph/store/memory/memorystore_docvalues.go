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
	col, _, gen, ok, err := ms.labelDocColumns(labelToken, propKeys, false)
	if err != nil || !ok {
		return gen, false, err
	}
	// Phase B: iterate the immutable snapshot lock-free.
	col.ForEachRow(propKeys, fn)
	return gen, true, nil
}

// ForEachDocValuesOrdinal is ForEachDocValues with each row's node ordinal
// beside its ID (sigma-tkgd round 6 request 11): the ordinal the node's row
// carries, read under the lock that took the snapshot, once per snapshot.
func (ms *Store) ForEachDocValuesOrdinal(labelToken uint16, propKeys []string,
	fn func(id types.NodeID, ordinal uint32, vals []any, present []bool) bool) (gen uint64, ok bool, err error) {
	col, ords, gen, ok, err := ms.labelDocColumns(labelToken, propKeys, true)
	if err != nil || !ok {
		return gen, false, err
	}
	col.ForEachRowOrdinal(propKeys, ords, fn)
	return gen, true, nil
}

// labelDocColumns is ForEachDocValues' Phase A: under the write lock (atomic
// vs every mutation, which also takes it), a column fresh for the epoch and
// holding every requested key, with its rows' ordinals when withOrds.
// ok=false (gen as known) when the door declines: the label is empty, over
// the size cap, or a requested property is not a uniformly numeric/string
// column.
func (ms *Store) labelDocColumns(labelToken uint16, propKeys []string, withOrds bool) (col *indexpkg.LabelDocValues, ords []uint32, gen uint64, ok bool, err error) {
	if ms == nil {
		return nil, nil, 0, false, ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if cerr := ms.checkOpenLocked(); cerr != nil {
		return nil, nil, 0, false, cerr
	}
	set := ms.labelIdx[labelToken]
	if len(set) == 0 || len(set) > indexpkg.MaxDocValuesNodes {
		return nil, nil, 0, false, nil // empty or over cap → fall back
	}
	cur := ms.nodeEpoch.Load()
	col = ms.docColumns[labelToken]
	if col == nil || col.Epoch() != cur || !col.HasAll(propKeys) {
		col = ms.refreshLabelColumnsLocked(labelToken, set, propKeys, ms.docColumns[labelToken], cur)
		ms.docColumns[labelToken] = col
	}
	// A requested column that is not buildable declines (the caller falls
	// back for the whole query).
	if !col.HasAll(propKeys) {
		return nil, nil, col.Epoch(), false, nil
	}
	if withOrds {
		ords = ms.docOrdinals.label(labelToken, col, ms.snapshotOrdinalsLocked)
	}
	return col, ords, col.Epoch(), true, nil
}

// ForEachDocValuesMulti streams the requested property columns for a LABEL
// INTERSECTION (a multi-label pattern like (p:A:B)) in ordinal order, building/
// refreshing the immutable column snapshot under the write lock if stale, then
// iterating it lock-free. Same contract as ForEachDocValues:
//
//   - Returns ok=false (NOT an error) when unusable: over the size cap, or any
//     requested property not a uniformly numeric/string column. The caller
//     falls back to the general (per-node) path. An EMPTY intersection is
//     served (ok=true, no rows; round 5 request 3): the caller cannot tell it
//     from the labels' counts.
//   - Membership is the INTERSECTION of the full label indexes — the same set a
//     multi-label scan returns (no valid-time filter). A node absent
//     from any one label is excluded; count(*) counts the surviving rows.
//   - The returned gen is the snapshot's epoch; the caller re-checks
//     NodeMutationEpoch()==gen after consuming fn's results (Gate 2).
func (ms *Store) ForEachDocValuesMulti(toks []uint16, propKeys []string,
	fn func(id types.NodeID, vals []any, present []bool) bool) (gen uint64, ok bool, err error) {
	col, _, gen, ok, err := ms.multiDocColumns(toks, propKeys, false)
	if err != nil || !ok {
		return gen, false, err
	}
	col.ForEachRow(propKeys, fn)
	return gen, true, nil
}

// ForEachDocValuesMultiOrdinal is ForEachDocValuesMulti with each row's node
// ordinal (round 6 request 11), as ForEachDocValuesOrdinal.
func (ms *Store) ForEachDocValuesMultiOrdinal(toks []uint16, propKeys []string,
	fn func(id types.NodeID, ordinal uint32, vals []any, present []bool) bool) (gen uint64, ok bool, err error) {
	col, ords, gen, ok, err := ms.multiDocColumns(toks, propKeys, true)
	if err != nil || !ok {
		return gen, false, err
	}
	col.ForEachRowOrdinal(propKeys, ords, fn)
	return gen, true, nil
}

// multiDocColumns is labelDocColumns for a label intersection.
func (ms *Store) multiDocColumns(toks []uint16, propKeys []string, withOrds bool) (col *indexpkg.LabelDocValues, ords []uint32, gen uint64, ok bool, err error) {
	if ms == nil {
		return nil, nil, 0, false, ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if cerr := ms.checkOpenLocked(); cerr != nil {
		return nil, nil, 0, false, cerr
	}
	cur := ms.nodeEpoch.Load()
	key := indexpkg.MultiLabelKey(toks)
	col = ms.docColumnsMulti[key]
	// Compute the intersection ONLY when a (re)build is needed. A fresh cached
	// snapshot is trusted as-is: every label change bumps the epoch, so a matching
	// epoch guarantees the intersection membership is unchanged — re-intersecting
	// on every call would re-probe the smaller label set (O(min|label|)) for
	// nothing (the 50k-probe steady-state regression this avoids).
	if col == nil || col.Epoch() != cur || !col.HasAll(propKeys) {
		keys := propKeys
		if col != nil {
			keys = indexpkg.UnionKeys(col.Keys(), propKeys)
		}
		// Intersection membership, NOT single-label: a per-label append record cannot
		// say whether an appended node joins A∩B, so this path always rebuilds:
		// from a member label's fresh columns when one holds every key (round 7
		// request 6, no node read), else from the nodes.
		if sub := ms.multiColumnsFromMemberLocked(toks, cur, keys); sub != nil {
			ms.columnRebuilds.Add(1)
			ms.multiFromMember.Add(1)
			col = sub
		} else {
			ids := ms.intersectLabelsLocked(toks)
			if len(ids) > indexpkg.MaxDocValuesNodes {
				return nil, nil, 0, false, nil // over cap → fall back
			}
			// An empty intersection is served: an empty snapshot (round 5
			// request 3).
			col = ms.buildColumnsFromIDsLocked(ids, keys, nil, cur)
		}
		ms.docColumnsMulti[key] = col
	}
	if !col.HasAll(propKeys) {
		return nil, nil, col.Epoch(), false, nil
	}
	if withOrds {
		ords = ms.docOrdinals.multi(key, col, ms.snapshotOrdinalsLocked)
	}
	return col, ords, col.Epoch(), true, nil
}

// docOrdinalCache holds, per label and per label intersection, the node
// ordinals of the rows of the snapshot they were read for. Guarded by the
// store's mu.
type docOrdinalCache struct {
	labels map[uint16]snapshotOrdinals
	multis map[string]snapshotOrdinals
}

type snapshotOrdinals struct {
	col  *indexpkg.LabelDocValues
	ords []uint32
}

func (c *docOrdinalCache) label(tok uint16, col *indexpkg.LabelDocValues, read func(*indexpkg.LabelDocValues) []uint32) []uint32 {
	if e, ok := c.labels[tok]; ok && e.col == col {
		return e.ords
	}
	if c.labels == nil {
		c.labels = make(map[uint16]snapshotOrdinals)
	}
	ords := read(col)
	c.labels[tok] = snapshotOrdinals{col: col, ords: ords}
	return ords
}

func (c *docOrdinalCache) multi(key string, col *indexpkg.LabelDocValues, read func(*indexpkg.LabelDocValues) []uint32) []uint32 {
	if e, ok := c.multis[key]; ok && e.col == col {
		return e.ords
	}
	if c.multis == nil {
		c.multis = make(map[string]snapshotOrdinals)
	}
	ords := read(col)
	c.multis[key] = snapshotOrdinals{col: col, ords: ords}
	return ords
}

// snapshotOrdinalsLocked reads the ordinals of col's rows from the nodes'
// current rows (0 for a row the store no longer holds). Callers hold ms.mu,
// under which col was taken fresh, so every row is current.
func (ms *Store) snapshotOrdinalsLocked(col *indexpkg.LabelDocValues) []uint32 {
	ids := col.IDs()
	ords := make([]uint32, len(ids))
	for i, id := range ids {
		if n := ms.nodes[id]; n != nil {
			ords[i] = n.Ordinal()
		}
	}
	return ords
}

// intersectLabelsLocked returns the node IDs present in EVERY label token,
// driven by the smallest label set (so the probe count is O(min |label|)), as
// a slice (no intermediate set). Returns nil (empty) if any token is
// unknown/empty. Must hold ms.mu.
func (ms *Store) intersectLabelsLocked(toks []uint16) []types.NodeID {
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
	out := make([]types.NodeID, 0, len(smallest))
	for id := range smallest {
		if ms.inAllLabelsLocked(toks, id) {
			out = append(out, id)
		}
	}
	return out
}

// inAllLabelsLocked reports whether id carries every label of toks. Must hold
// ms.mu.
func (ms *Store) inAllLabelsLocked(toks []uint16, id types.NodeID) bool {
	for _, t := range toks {
		if _, ok := ms.labelIdx[t][id]; !ok {
			return false
		}
	}
	return true
}

// multiColumnsFromMemberLocked builds an intersection's snapshot at cur from
// the cached columns of its smallest member label that is fresh (built at
// cur) and holds every key with validity columns, taking that label's rows
// that carry every other label (indexpkg.SubsetDocValues). nil when no member
// qualifies or a key is not buildable on it. Must hold ms.mu (write lock).
func (ms *Store) multiColumnsFromMemberLocked(toks []uint16, cur uint64, keys []string) *indexpkg.LabelDocValues {
	var src *indexpkg.LabelDocValues
	for _, t := range toks {
		c := ms.docColumns[t]
		if c == nil || c.Epoch() != cur || !c.HasAll(keys) || !c.HasTemporal() {
			continue
		}
		if src == nil || c.Len() < src.Len() {
			src = c
		}
	}
	if src == nil {
		return nil
	}
	var ords []int
	for o, id := range src.IDs() {
		if ms.inAllLabelsLocked(toks, id) {
			ords = append(ords, o)
		}
	}
	sub, ok := indexpkg.SubsetDocValues(cur, src, ords, keys)
	if !ok {
		return nil
	}
	return sub
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
	return ms.buildColumnsFromIDsLocked(ids, requested, old, cur)
}

// buildColumnsFromIDsLocked is buildColumnsLocked over a membership slice.
// Must hold ms.mu (write lock).
func (ms *Store) buildColumnsFromIDsLocked(ids []types.NodeID, requested []string,
	old *indexpkg.LabelDocValues, cur uint64) *indexpkg.LabelDocValues {
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
	getTemporal := func(id types.NodeID) (int64, int64, int64, bool) {
		n, ok := ms.nodes[id]
		if !ok {
			return 0, 0, 0, false
		}
		// See core/docvalues.go: the effective start, one rule.
		_, t, _ := n.ValidRange()
		f := int64(storeutil.NodeValidFrom(ms.validAxis.Get(), n))
		return f, int64(t), f, true
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
				getTemporal := func(id types.NodeID) (int64, int64, int64, bool) {
					n, present := ms.nodes[id]
					if !present {
						return 0, 0, 0, false
					}
					_, t, _ := n.ValidRange()
					f := int64(storeutil.NodeValidFrom(ms.validAxis.Get(), n))
					return f, int64(t), f, true
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

// DocValuesSnapshotMulti is DocValuesSnapshot over a LABEL INTERSECTION
// (sigma-tkgd round 10): the snapshot ForEachDocValuesMulti reads, bound
// to propKeys, with ForEachDocValuesMulti's rows, order, gen and decline
// contract (an empty intersection is served: an empty snapshot).
func (ms *Store) DocValuesSnapshotMulti(toks []uint16, propKeys []string) (snap types.NodeColumnReader, gen uint64, ok bool, err error) {
	col, _, gen, ok, err := ms.multiDocColumns(toks, propKeys, false)
	if err != nil || !ok {
		return nil, gen, false, err
	}
	ps, pok := col.NewPointSnapshot(propKeys)
	if !pok {
		return nil, gen, false, nil
	}
	return ps, gen, true, nil
}

// DocValuesSnapshotOrdinal is DocValuesSnapshot whose chunks carry each
// member's node ordinal (types.ColumnChunk.Ordinals), the ordinals
// ForEachDocValuesOrdinal hands out (round 10).
func (ms *Store) DocValuesSnapshotOrdinal(labelToken uint16, propKeys []string) (snap types.NodeColumnChunkReader, gen uint64, ok bool, err error) {
	col, ords, gen, ok, err := ms.labelDocColumns(labelToken, propKeys, true)
	if err != nil || !ok {
		return nil, gen, false, err
	}
	os, sok := indexpkg.LabelOrdinalSnapshot(col, propKeys, ords)
	if !sok {
		return nil, gen, false, nil
	}
	return os, gen, true, nil
}

// DocValuesSnapshotMultiOrdinal is DocValuesSnapshotMulti with ordinals,
// as DocValuesSnapshotOrdinal.
func (ms *Store) DocValuesSnapshotMultiOrdinal(toks []uint16, propKeys []string) (snap types.NodeColumnChunkReader, gen uint64, ok bool, err error) {
	col, ords, gen, ok, err := ms.multiDocColumns(toks, propKeys, true)
	if err != nil || !ok {
		return nil, gen, false, err
	}
	os, sok := indexpkg.LabelOrdinalSnapshot(col, propKeys, ords)
	if !sok {
		return nil, gen, false, nil
	}
	return os, gen, true, nil
}
