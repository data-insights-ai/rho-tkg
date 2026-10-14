package badger

import (
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ColumnExtendCount / ColumnRebuildCount expose how a columnar snapshot was last
// refreshed. Both paths return identical data by construction, so no correctness
// test can tell them apart — these are what let a test prove the append fast path
// actually ran rather than silently never firing (which would pass every
// correctness assertion). They are also the builds-per-read telemetry the
// cache-versus-native-columnar decision needs.
func (bs *Store) ColumnExtendCount() uint64  { return bs.columnExtends.Load() }
func (bs *Store) ColumnRebuildCount() uint64 { return bs.columnRebuilds.Load() }

// ColumnPatchCount is ColumnExtendCount for the in-place-update path: how many
// columnar snapshots were PATCHED (rows of updated members re-read, the rest
// copied) instead of rebuilt (round 5 request 4).
func (bs *Store) ColumnPatchCount() uint64 { return bs.columnPatches.Load() }

// Append-delta bookkeeping (R3). A columnar snapshot is invalidated by epoch
// advance, so any write to a label makes the next read rebuild the WHOLE label —
// O(label size) even when the read wants a handful of rows. The dominant write
// shape is an APPEND (new nodes carrying labels that already exist), and an append
// needs no rebuild: LabelDocValues.Extend copies the existing rows and reads only
// the new ones, measured 5.5x cheaper.
//
// THE HARD PART IS KNOWING an append is all that happened, and the trick is that it
// needs no classification of write paths. Every node-content write funnels through
// exactly two ungated seams:
//
//	addNodePropertyKeyCounts    -> record the node's ID against each of its labels
//	removeNodePropertyKeyCounts -> POISON each of its labels
//
// An UPDATE is remove-then-add and a DELETE is remove, so both poison. Only a pure
// insert reaches `add` without `remove`. No call site has to be audited, and adding
// a new write path in future cannot silently opt into the fast path — it either
// funnels through these seams (and is handled) or does not bump the epoch at all.
//
// EXACTNESS. Recording alone is not enough: a write that bumps the epoch without
// being recorded (a stripe collision from an unrelated label, the global salt, a
// path that bumps directly) would leave the buffer describing less than the epoch
// delta. So each record stamps the label's epoch AFTER its bump, and the fast path
// runs only when that stamp still equals the epoch observed at read time. Anything
// else falls back to a rebuild.
//
// Every failure mode is a rebuild, never a wrong answer.

// appendDelta is the per-label record of what changed since its snapshot was built.
//
// Round 5 request 4: on a label with a cached snapshot (patchable) the removal
// seam records the node in changed instead of poisoning, so an UPDATE (remove
// half + add half) leaves both halves recorded. Every recorded event (an id in
// ids or in changed) bumped the label's epoch exactly once, so the accounting
// identity becomes gen - snapshotEpoch == len(ids) + len(changed). The read
// side then classifies the recorded IDs by their CURRENT membership
// (classifyLabelDelta) and patches or extends the snapshot, or rebuilds.
type appendDelta struct {
	ids      []types.NodeID // add-seam events since the cached snapshot, by arrival
	changed  []types.NodeID // removal-seam events on a patchable label, by arrival
	epoch    uint64         // labelEpoch immediately after the last recorded event
	poisoned bool           // an unrecorded removal touched this label — rebuild
}

// events is the number of recorded epoch bumps.
func (d *appendDelta) events() uint64 { return uint64(len(d.ids)) + uint64(len(d.changed)) }

// recordNodeAppend notes a pure insert against every label the node carries. Called
// AFTER bumpNodeLabelEpochs so the stamped epoch includes this write's own bump.
func (bs *Store) recordNodeAppend(n *types.Node) {
	if n == nil {
		return
	}
	count := n.LabelTokenCount()
	if count == 0 {
		return
	}
	id := n.ID()
	bs.appendMu.Lock()
	defer bs.appendMu.Unlock()
	if bs.appendDeltas == nil {
		bs.appendDeltas = make(map[uint16]*appendDelta)
	}
	for i := 0; i < count; i++ {
		tok := n.LabelTokenRawAt(i)
		if tok == 0 {
			continue
		}
		d := bs.appendDeltas[tok]
		if d == nil {
			d = &appendDelta{}
			bs.appendDeltas[tok] = d
		}
		if d.poisoned {
			continue // already void; recording more would not make it usable
		}
		// Bound the buffer: past a point, extending is no cheaper than rebuilding and
		// the buffer itself becomes the cost. Poison rather than grow without limit.
		if d.events() >= maxAppendDeltaIDs {
			d.poisoned, d.ids, d.changed = true, nil, nil
			continue
		}
		d.ids = append(d.ids, id)
		d.epoch = bs.labelEpoch(tok)
	}
}

// recordNodeRemoval is the removal seam's record (round 5 request 4): on a label
// with a cached snapshot (patchable) the node is recorded as changed — its row
// may differ or be gone — and stamped like an append; on any other label the
// append assumption is voided (poisoned), as before. Called AFTER
// bumpNodeLabelEpochs, from the removal seam an UPDATE also passes through.
func (bs *Store) recordNodeRemoval(n *types.Node) {
	if n == nil {
		return
	}
	count := n.LabelTokenCount()
	if count == 0 {
		return
	}
	id := n.ID()
	bs.appendMu.Lock()
	defer bs.appendMu.Unlock()
	if bs.appendDeltas == nil {
		bs.appendDeltas = make(map[uint16]*appendDelta)
	}
	for i := 0; i < count; i++ {
		tok := n.LabelTokenRawAt(i)
		if tok == 0 {
			continue
		}
		d := bs.appendDeltas[tok]
		if d == nil {
			d = &appendDelta{}
			bs.appendDeltas[tok] = d
		}
		if d.poisoned {
			continue
		}
		if _, ok := bs.patchable[tok]; !ok || d.events() >= maxAppendDeltaIDs {
			d.poisoned, d.ids, d.changed = true, nil, nil
			continue
		}
		d.changed = append(d.changed, id)
		d.epoch = bs.labelEpoch(tok)
	}
}

// poisonAllLabels voids every label's append assumption. Paired with the global
// nodeEpochSalt bump, which fires on label-LESS events (Clear, exact erasure,
// retention purge) that no per-label record can describe.
func (bs *Store) poisonAllLabels() {
	bs.appendMu.Lock()
	bs.appendDeltas = nil
	bs.patchable = nil
	bs.appendMu.Unlock()
}

// maxAppendDeltaIDs bounds a label's pending appends. Beyond this the extend is no
// longer clearly cheaper than a rebuild, and an unbounded buffer would itself be a
// leak on a write-only label nobody reads.
const maxAppendDeltaIDs = 50_000

// THE GUARD OF THE DELTA TAKES IS AN ACCOUNTING IDENTITY on top of the stamp:
//
//	gen - snapshotEpoch == recorded events
//
// Every recorded event bumped the label's epoch exactly once, so a balance
// proves EVERY bump since the snapshot was built was recorded. The stamp alone
// is weaker than it looks: poisonAllLabels DROPS the whole record map, so a
// subsequent write creates a FRESH, un-poisoned record stamped at the current
// epoch and the stamp check passes over a snapshot that a label-less removal
// already invalidated. That exact hole shipped on the relationship path and a
// probe caught it. For an intersection the identity sums the member labels'
// events: multiLabelEpoch is the SUM of their epochs, and a node carrying a
// label outside the tuple bumps that label too, which the sum does not count.

// takeLabelDelta returns every node recorded against a label since its
// snapshot was built (add and removal events, deduplicated), or ok=false when a
// rebuild is required: no record, a poisoned one, a stamp other than gen, or an
// accounting identity that does not balance (gen - snapshotEpoch must equal the
// recorded events, see above). Does not clear.
func (bs *Store) takeLabelDelta(token uint16, gen, snapshotEpoch uint64) ([]types.NodeID, bool) {
	bs.appendMu.Lock()
	defer bs.appendMu.Unlock()
	d := bs.appendDeltas[token]
	if d == nil || d.poisoned || d.events() == 0 || d.epoch != gen {
		return nil, false
	}
	if gen < snapshotEpoch || gen-snapshotEpoch != d.events() {
		return nil, false
	}
	return distinctDeltaIDs(d.ids, d.changed), true
}

// takeMultiLabelDelta is takeLabelDelta for a label intersection: the
// identity sums the member labels' events (multiLabelEpoch sums their epochs,
// see above).
func (bs *Store) takeMultiLabelDelta(toks []uint16, gen, snapshotEpoch uint64) ([]types.NodeID, bool) {
	if len(toks) == 0 {
		return nil, false
	}
	bs.appendMu.Lock()
	defer bs.appendMu.Unlock()
	var recorded uint64
	var lists [][]types.NodeID
	for _, tok := range toks {
		d := bs.appendDeltas[tok]
		if d == nil || d.poisoned {
			return nil, false
		}
		recorded += d.events()
		lists = append(lists, d.ids, d.changed)
	}
	if recorded == 0 || gen < snapshotEpoch || gen-snapshotEpoch != recorded {
		return nil, false
	}
	return distinctDeltaIDs(lists...), true
}

// distinctDeltaIDs returns the distinct IDs of lists, in first-seen order.
func distinctDeltaIDs(lists ...[]types.NodeID) []types.NodeID {
	seen := make(map[types.NodeID]struct{})
	var out []types.NodeID
	for _, l := range lists {
		for _, id := range l {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	return out
}

// markPatchable notes that a snapshot over toks is cached, so the removal seam
// records those labels' updates instead of poisoning them. Caller holds
// appendMu.
func (bs *Store) markPatchableLocked(toks ...uint16) {
	if bs.patchable == nil {
		bs.patchable = make(map[uint16]struct{})
	}
	for _, tok := range toks {
		bs.patchable[tok] = struct{}{}
	}
}

// clearMultiAppendDelta drops the pending record for every member label of an
// intersection once a snapshot covering those appends exists.
func (bs *Store) clearMultiAppendDelta(toks []uint16) {
	bs.appendMu.Lock()
	for _, tok := range toks {
		delete(bs.appendDeltas, tok)
	}
	bs.markPatchableLocked(toks...)
	bs.appendMu.Unlock()
}

// clearAppendDelta drops a label's pending record, called once a snapshot covering
// those appends has been built or extended.
func (bs *Store) clearAppendDelta(token uint16) {
	bs.appendMu.Lock()
	delete(bs.appendDeltas, token)
	bs.markPatchableLocked(token)
	bs.appendMu.Unlock()
}

// MultiColumnFromMemberCount reports how many label-intersection snapshots
// were taken from a member label's columns instead of the node rows (round 7
// request 6); telemetry beside ColumnRebuildCount, which counts them too.
func (bs *Store) MultiColumnFromMemberCount() uint64 { return bs.multiFromMember.Load() }
