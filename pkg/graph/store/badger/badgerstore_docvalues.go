package badger

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
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
// zero-QueryOpts scan returns (no valid-time filter; critique C1). Declines
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

	getProp := func(id types.NodeID, key string) (any, bool) {
		nd, err := bs.GetNode(id)
		if err != nil || nd == nil {
			return nil, false // node deleted between the membership snapshot and the read
		}
		return nd.GetProperty(key)
	}
	col = indexpkg.BuildLabelDocValues(gen, ids, keys, getProp)

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
