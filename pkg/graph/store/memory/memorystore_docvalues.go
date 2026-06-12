package memory

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
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

// ForEachDocValues streams the requested property columns for a label in ordinal
// order, building/refreshing the immutable column snapshot under the write lock if
// stale, then iterating it lock-free.
//
//   - Returns ok=false (NOT an error) when the column path is unusable: the label
//     is empty, over the size cap, or any requested property is not a uniformly
//     numeric/string column. The caller falls back to the per-node path.
//   - Membership is the FULL label index (ms.labelIdx) — the same unfiltered set
//     the zero-QueryOpts scan returns. No valid-time predicate (critique C1): a
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
		col = ms.buildLabelColumnsLocked(labelToken, set, propKeys, cur)
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

// buildLabelColumnsLocked builds a fresh immutable column snapshot at epoch `cur`
// over the full label membership, covering the union of the previously-built keys
// and the newly requested ones (so a label's columns stay at one epoch). Must hold
// ms.mu (write lock).
func (ms *Store) buildLabelColumnsLocked(labelToken uint16, set map[types.NodeID]struct{},
	requested []string, cur uint64) *indexpkg.LabelDocValues {

	ids := make([]types.NodeID, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}

	keys := requested
	if old := ms.docColumns[labelToken]; old != nil {
		keys = indexpkg.UnionKeys(old.Keys(), requested)
	}

	getProp := func(id types.NodeID, key string) (any, bool) {
		n, ok := ms.nodes[id]
		if !ok {
			return nil, false
		}
		return n.GetProperty(key)
	}
	return indexpkg.BuildLabelDocValues(cur, ids, keys, getProp)
}
