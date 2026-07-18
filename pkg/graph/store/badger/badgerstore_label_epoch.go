package badger

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// nodeLabelEpochStripes is the size of the sharded per-label epoch array. A power of
// two so token%stripes is a mask. 256 keeps false-sharing invalidation rare while the
// array stays tiny (2KB).
const nodeLabelEpochStripes = 256

// labelEpoch returns the per-label node-mutation epoch: the label's own stripe plus
// the global salt (bumped on label-less events). Monotonic. A DocValues column for
// token stamps this and is fresh iff it has not advanced. A hash collision on the
// stripe over-invalidates (safe); the salt invalidates every label.
func (bs *Store) labelEpoch(token uint16) uint64 {
	return bs.nodeLabelEpochs[token%nodeLabelEpochStripes].Load() + bs.nodeEpochSalt.Load()
}

// multiLabelEpoch returns the freshness stamp for a label-intersection column: the
// monotonic SUM of the member labels' epochs. Because each labelEpoch is monotonic,
// the sum is monotonic and unchanged iff NO member label changed — the exact
// invalidation condition for an intersection cache.
func (bs *Store) multiLabelEpoch(tokens []uint16) uint64 {
	var sum uint64
	for _, t := range tokens {
		sum += bs.labelEpoch(t)
	}
	return sum
}

// bumpNodeLabelEpochs advances the per-label epoch of every label the node carries —
// invalidating exactly those labels' cached DocValues columns. Called from the
// add/removeNodePropertyKeyCounts wrappers (the ungated seam every node-content write
// funnels through, always with the node); the remove-old + add-new pattern of a label
// change bumps BOTH the departed and acquired label sets.
func (bs *Store) bumpNodeLabelEpochs(n *types.Node) {
	if n == nil {
		return
	}
	count := n.LabelTokenCount()
	for i := 0; i < count; i++ {
		tok := n.LabelTokenRawAt(i)
		if tok == 0 {
			continue
		}
		bs.nodeLabelEpochs[tok%nodeLabelEpochStripes].Add(1)
	}
}

// NodeLabelMutationEpoch returns the per-label node-mutation epoch for the consumer's
// Gate-2 staleness re-check on a SINGLE-label DocValues result (BACKLOG 4b): it is the
// value a ForEachDocValues/DocValuesSnapshot over one label returns as gen, and it
// advances ONLY when a node carrying that label is written (or a label-less
// invalidation event fires) — NOT on writes to unrelated labels, unlike the global
// NodeMutationEpoch. A caller aggregating over one label should re-check against this,
// not NodeMutationEpoch, to avoid discarding a still-valid result after an
// unrelated-label write.
func (bs *Store) NodeLabelMutationEpoch(labelToken uint16) uint64 {
	return bs.labelEpoch(labelToken)
}
