package badger

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// NodeMutationEpoch returns the global node-mutation epoch — bumped on every node
// write (incl. deletes). The consumer re-checks it after a lock-free column scan
// (Gate 2): if it advanced, a writer interleaved and the result is discarded.
func (bs *Store) NodeMutationEpoch() uint64 {
	if bs == nil {
		return 0
	}
	return bs.nodeEpoch.Load()
}

// RelMutationEpoch returns the global relationship-mutation epoch — bumped on every
// edge write. The expand-aggregation column path samples it before the scan and
// re-checks it after (Gate 2): a concurrent edge insert/delete advances it and the
// consumer discards the torn aggregate. Distinct from NodeMutationEpoch.
func (bs *Store) RelMutationEpoch() uint64 {
	if bs == nil {
		return 0
	}
	return bs.relEpoch.Load()
}

// ForEachDocValues streams the requested property columns for a label's nodes in
// ordinal order from a cached columnar snapshot, building it if stale.
//
// Build is LOCK-FREE and keyed on the epoch read at its start: the column is
// stamped that epoch and only cached if the epoch did not advance during the
// build. Correctness rests on epoch monotonicity, not on holding a writer lock:
//   - a trusted result (the consumer's post-scan NodeMutationEpoch()==gen holds)
//     means no write occurred from build-start through the scan, so every value
//     read reflects one consistent snapshot;
//   - any interleaving write advances the epoch past the stamp, so the consumer
//     discards the (possibly torn) rows and falls back.
//
// Membership is the full in-RAM label index — the same unfiltered set the
// zero-QueryOpts scan returns (no valid-time filter). Declines
// (ok=false) in LabelIndexOnDisk mode (membership not in RAM), for an empty or
// over-cap label, or when a requested property is not a uniformly numeric/string
// column.
func (bs *Store) ForEachDocValues(labelToken uint16, propKeys []string,
	fn func(id types.NodeID, vals []any, present []bool) bool) (gen uint64, ok bool, err error) {
	if bs == nil {
		return 0, false, ErrStoreClosed
	}
	if err := bs.checkOpen(); err != nil {
		return 0, false, err
	}
	if bs.labelOnDisk {
		return 0, false, nil // membership not materialized in RAM → fall back
	}

	cur := bs.nodeEpoch.Load()
	bs.docMu.Lock()
	col := bs.docColumns[labelToken]
	bs.docMu.Unlock()

	if col == nil || col.Epoch() != cur || !col.HasAll(propKeys) {
		built, declined := bs.buildLabelColumns(labelToken, propKeys)
		if declined {
			return 0, false, nil // empty/over-cap label → fall back
		}
		col = built
	}

	if !col.HasAll(propKeys) {
		return col.Epoch(), false, nil
	}
	col.ForEachRow(propKeys, fn)
	return col.Epoch(), true, nil
}

// DocValuesSnapshot returns a RANDOM-ACCESS point-lookup handle over a single
// label's column snapshot (the expand-aggregation target side), building it if
// stale. Same lock-free epoch-keyed build and decline contract as ForEachDocValues;
// declines in LabelIndexOnDisk mode, on an empty/over-cap label, or when a requested
// property is not a uniformly numeric/string column. gen is the
// snapshot epoch for the consumer's Gate-2 (paired with RelMutationEpoch).
func (bs *Store) DocValuesSnapshot(labelToken uint16, propKeys []string) (snap types.NodeColumnReader, gen uint64, ok bool, err error) {
	if bs == nil {
		return nil, 0, false, ErrStoreClosed
	}
	if err := bs.checkOpen(); err != nil {
		return nil, 0, false, err
	}
	if bs.labelOnDisk {
		return nil, 0, false, nil
	}

	cur := bs.nodeEpoch.Load()
	bs.docMu.Lock()
	col := bs.docColumns[labelToken]
	bs.docMu.Unlock()

	if col == nil || col.Epoch() != cur || !col.HasAll(propKeys) {
		built, declined := bs.buildLabelColumns(labelToken, propKeys)
		if declined {
			return nil, 0, false, nil
		}
		col = built
	}

	ps, pok := col.NewPointSnapshot(propKeys)
	if !pok {
		return nil, col.Epoch(), false, nil // an unbuildable key → decline
	}
	return ps, col.Epoch(), true, nil
}

// ForEachDocValuesMulti streams the requested property columns for a LABEL
// INTERSECTION (a multi-label pattern like (p:A:B)) in ordinal order from a cached
// columnar snapshot, building it if stale. Same lock-free, epoch-keyed build and
// decline contract as ForEachDocValues; membership is the INTERSECTION of the
// in-RAM label indexes (no valid-time filter). Declines in
// LabelIndexOnDisk mode, on an empty intersection / over-cap, or for a
// non-numeric/string column.
func (bs *Store) ForEachDocValuesMulti(toks []uint16, propKeys []string,
	fn func(id types.NodeID, vals []any, present []bool) bool) (gen uint64, ok bool, err error) {
	if bs == nil {
		return 0, false, ErrStoreClosed
	}
	if err := bs.checkOpen(); err != nil {
		return 0, false, err
	}
	if bs.labelOnDisk {
		return 0, false, nil // membership not materialized in RAM → fall back
	}

	cur := bs.nodeEpoch.Load()
	key := indexpkg.MultiLabelKey(toks)
	bs.docMu.Lock()
	col := bs.docColumnsMulti[key]
	bs.docMu.Unlock()

	if col == nil || col.Epoch() != cur || !col.HasAll(propKeys) {
		built, declined := bs.buildMultiColumns(toks, key, propKeys)
		if declined {
			return 0, false, nil // empty intersection / over-cap → fall back
		}
		col = built
	}

	if !col.HasAll(propKeys) {
		return col.Epoch(), false, nil
	}
	col.ForEachRow(propKeys, fn)
	return col.Epoch(), true, nil
}

// buildMultiColumns builds a fresh immutable snapshot over a label intersection,
// mirroring buildLabelColumns (lock-free, epoch-stamped, cached only if the epoch
// held). declined=true means an empty intersection or over-cap.
func (bs *Store) buildMultiColumns(toks []uint16, key string, requested []string) (col *indexpkg.LabelDocValues, declined bool) {
	gen := bs.nodeEpoch.Load()

	bs.idxMu.RLock()
	set := bs.intersectLabelsLocked(toks)
	n := len(set)
	if n == 0 || n > indexpkg.MaxDocValuesNodes {
		bs.idxMu.RUnlock()
		return nil, true
	}
	ids := make([]types.NodeID, 0, n)
	for id := range set {
		ids = append(ids, id)
	}
	bs.idxMu.RUnlock()

	keys := requested
	bs.docMu.Lock()
	if old := bs.docColumnsMulti[key]; old != nil {
		keys = indexpkg.UnionKeys(old.Keys(), requested)
	}
	bs.docMu.Unlock()

	col = indexpkg.BuildLabelDocValues(gen, ids, keys, bs.bulkNodePropGetter(ids))

	bs.docMu.Lock()
	if bs.nodeEpoch.Load() == gen {
		if bs.docColumnsMulti == nil {
			bs.docColumnsMulti = make(map[string]*indexpkg.LabelDocValues)
		}
		bs.docColumnsMulti[key] = col
	}
	bs.docMu.Unlock()
	return col, false
}

// intersectLabelsLocked returns the node IDs present in EVERY label token, driven
// by the smallest set (O(min |label|) probes). Returns nil if any token is
// unknown/empty. Must hold bs.idxMu (read lock).
func (bs *Store) intersectLabelsLocked(toks []uint16) map[types.NodeID]struct{} {
	if len(toks) == 0 {
		return nil
	}
	var smallest map[types.NodeID]struct{}
	for _, t := range toks {
		s := bs.labelIdx[t]
		if len(s) == 0 {
			return nil
		}
		if smallest == nil || len(s) < len(smallest) {
			smallest = s
		}
	}
	out := make(map[types.NodeID]struct{}, len(smallest))
	for id := range smallest {
		inAll := true
		for _, t := range toks {
			if _, ok := bs.labelIdx[t][id]; !ok {
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

// buildLabelColumns builds a fresh immutable snapshot over the full label
// membership, reading each node's live value via the cache-backed point read. The
// snapshot is stamped the epoch read at the start and cached only if the epoch did
// not advance during the build (otherwise it is returned for the caller's
// fall-back, never trusted). declined=true means the label is empty or over cap.
func (bs *Store) buildLabelColumns(labelToken uint16, requested []string) (col *indexpkg.LabelDocValues, declined bool) {
	gen := bs.nodeEpoch.Load()

	bs.idxMu.RLock()
	set := bs.labelIdx[labelToken]
	n := len(set)
	if n == 0 || n > indexpkg.MaxDocValuesNodes {
		bs.idxMu.RUnlock()
		return nil, true
	}
	ids := make([]types.NodeID, 0, n)
	for id := range set {
		ids = append(ids, id)
	}
	bs.idxMu.RUnlock()

	keys := requested
	bs.docMu.Lock()
	if old := bs.docColumns[labelToken]; old != nil {
		keys = indexpkg.UnionKeys(old.Keys(), requested)
	}
	bs.docMu.Unlock()

	col = indexpkg.BuildLabelDocValues(gen, ids, keys, bs.bulkNodePropGetter(ids))

	bs.docMu.Lock()
	if bs.nodeEpoch.Load() == gen { // build saw a consistent snapshot — safe to cache
		if bs.docColumns == nil {
			bs.docColumns = make(map[uint16]*indexpkg.LabelDocValues)
		}
		bs.docColumns[labelToken] = col
	}
	bs.docMu.Unlock()
	return col, false
}

// bulkNodePropGetter returns a getProp closure for a column build over ids that
// decodes each node EXACTLY ONCE via a single bulk scan, instead of the naive
// column-major build's ~O(columns × 2 passes × N) per-(id,key) GetNode calls —
// each of which re-fetches the whole node and, on a label larger than the LRU,
// thrashes the cache with fill-on-miss (evicting hot point-read entries). The
// bulk scan is no-fill (scan discipline: cache hits served without promotion, so
// it does not pollute the LRU). On a bulk-scan error it falls back to per-node
// reads so the build still completes correctly.
func (bs *Store) bulkNodePropGetter(ids []types.NodeID) func(types.NodeID, string) (any, bool) {
	sorted := append([]types.NodeID(nil), ids...)
	storepkg.SortNodeIDs(sorted)
	mat := make(map[types.NodeID]*types.Node, len(sorted))
	if err := bs.forEachNodeBulk(sorted, func(nd *types.Node) bool {
		mat[nd.ID()] = nd
		return true
	}); err != nil {
		return func(id types.NodeID, key string) (any, bool) {
			nd, gerr := bs.GetNode(id)
			if gerr != nil || nd == nil {
				return nil, false
			}
			return nd.GetProperty(key)
		}
	}
	return func(id types.NodeID, key string) (any, bool) {
		nd := mat[id]
		if nd == nil {
			return nil, false // deleted between the membership snapshot and the scan
		}
		return nd.GetProperty(key)
	}
}
