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
type appendDelta struct {
	ids      []types.NodeID // appended since the cached snapshot, ascending by arrival
	epoch    uint64         // labelEpoch immediately after the last recorded append
	poisoned bool           // a removal touched this label — the append assumption is void
}

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
		if len(d.ids) >= maxAppendDeltaIDs {
			d.poisoned, d.ids = true, nil
			continue
		}
		d.ids = append(d.ids, id)
		d.epoch = bs.labelEpoch(tok)
	}
}

// poisonNodeLabels voids the append assumption for every label the node carries.
// Called from the removal seam, which an UPDATE also passes through (remove-old +
// add-new), so an update correctly forces a rebuild.
func (bs *Store) poisonNodeLabels(n *types.Node) {
	if n == nil {
		return
	}
	count := n.LabelTokenCount()
	if count == 0 {
		return
	}
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
		d.poisoned, d.ids = true, nil
	}
}

// poisonAllLabels voids every label's append assumption. Paired with the global
// nodeEpochSalt bump, which fires on label-LESS events (Clear, exact erasure,
// retention purge) that no per-label record can describe.
func (bs *Store) poisonAllLabels() {
	bs.appendMu.Lock()
	bs.appendDeltas = nil
	bs.appendMu.Unlock()
}

// maxAppendDeltaIDs bounds a label's pending appends. Beyond this the extend is no
// longer clearly cheaper than a rebuild, and an unbounded buffer would itself be a
// leak on a write-only label nobody reads.
const maxAppendDeltaIDs = 50_000

// takeAppendDelta returns the IDs appended since the label's snapshot was built, or
// ok=false if a rebuild is required. gen is the epoch the caller observed; the
// buffer is only usable if its stamp matches exactly, proving no unrecorded write
// intervened.
//
// Does NOT clear the buffer — the caller clears it only if the extend succeeds, so a
// refused extend still leaves the record intact for the rebuild path to discard.
func (bs *Store) takeAppendDelta(token uint16, gen uint64) ([]types.NodeID, bool) {
	bs.appendMu.Lock()
	defer bs.appendMu.Unlock()
	d := bs.appendDeltas[token]
	if d == nil || d.poisoned || len(d.ids) == 0 || d.epoch != gen {
		return nil, false
	}
	out := make([]types.NodeID, len(d.ids))
	copy(out, d.ids)
	return out, true
}

// clearAppendDelta drops a label's pending record, called once a snapshot covering
// those appends has been built or extended.
func (bs *Store) clearAppendDelta(token uint16) {
	bs.appendMu.Lock()
	delete(bs.appendDeltas, token)
	bs.appendMu.Unlock()
}
