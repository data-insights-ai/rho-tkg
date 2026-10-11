package core

import (
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The W5 oracle's generator for the caller-instant interval corrections
// (SetNodeVersionIntervalWithTx / SetRelVersionIntervalWithTx, backlog 34),
// each through a randomly chosen door (standalone, GraphTx, Batch, ingest
// strong, ingest concurrent; ivDoors, in the order of txbNodeFamilies).
//
// Per op, the oracle's independent half (t is the caller's number): every row
// the correction appends carries TxFrom = UpdatedAt = t and no retraction,
// every stored row is unchanged, pin t already answers the newest row (by
// version: the appended ones) through the as-of, as-of set and TxPin doors,
// and pin t-1 the belief before. A t at or below any TxFrom / TxTo /
// UpdatedAt / DeletedAt of the chain is refused on every door with ErrTxOrder
// and changes nothing. The sequence-level checks (rule 15 over every intent,
// the bitemporal oracle on the captured chains, cross-backend equality) run on
// these chains like on every other op.

// txbCascadeBound is the largest transaction stamp of the chain: every t above
// it is placeable for a correction, every t at or below it is refused.
func txbCascadeBound(e *oracleEntity) types.Instant {
	var b types.Instant
	for _, r := range e.rows {
		b = max(b, r.txFrom, r.txTo, r.updatedAt, r.deletedAt)
	}
	return b
}

// intervalStep runs, after a quarter of the steps, one interval correction at a
// caller instant through a random door, with every draw from o.ivRng.
func (o *txbOracle) intervalStep() {
	if o.ivRng.IntN(4) != 0 {
		return
	}
	o.tick()
	fam := o.ivRng.IntN(5)
	if o.ivRng.IntN(2) == 0 {
		o.nodeCallerInterval(fam)
	} else {
		o.relCallerInterval(fam)
	}
}

// txbIntervalTarget draws a correction interval (open-ended half the time).
func (o *txbOracle) txbIntervalTarget() (vf, vt types.Instant) {
	vf = types.Instant(500 + o.ivRng.IntN(int(o.vclock)))
	if o.ivRng.IntN(2) == 0 {
		vt = vf + types.Instant(100+o.ivRng.IntN(2000))
		if o.ivRng.IntN(8) == 0 {
			vt = vf + 1
		}
	}
	return vf, vt
}

// ivPickT is pickT on the interval stream: a placeable t in (lo, clock], or
// (one in five) a t at or below maxTx, which every door must refuse.
func (o *txbOracle) ivPickT(lo, maxTx types.Instant) (types.Instant, bool) {
	if o.ivRng.IntN(5) == 0 {
		return max(1, maxTx-types.Instant(o.ivRng.IntN(3))), false
	}
	if lo >= o.x {
		o.fail("fixture: entity bound %d not below the clock %d", lo, o.x)
	}
	return lo + 1 + types.Instant(o.ivRng.Int64N(int64(o.x-lo))), true
}

// txbCheckAppended asserts the correction's rows: at least one, every one
// stamped at t with no retraction, and no stored row changed. It returns the
// highest version of the chain after the op.
func (o *txbOracle) txbCheckAppended(desc string, before, after *oracleEntity, at types.Instant) uint32 {
	o.t.Helper()
	old := make(map[uint32]oracleRow, len(before.rows))
	for _, r := range before.rows {
		old[r.version] = r
	}
	var top uint32
	appended := 0
	for _, r := range after.rows {
		top = max(top, r.version)
		b, ok := old[r.version]
		if ok {
			if fmt.Sprint(b) != fmt.Sprint(r) {
				o.fail("%s: stored row v%d changed:\n before %+v\n after  %+v", desc, r.version, b, r)
			}
			continue
		}
		appended++
		if r.txFrom != at || r.updatedAt != at || r.txTo != 0 || r.deletedAt != 0 {
			o.fail("%s: appended v%d tx=%d..%d upd=%d del=%d; want TxFrom = UpdatedAt = t = %d, no retraction", desc, r.version, r.txFrom, r.txTo, r.updatedAt, r.deletedAt, at)
		}
	}
	if appended == 0 || len(after.rows) != len(before.rows)+appended {
		o.fail("%s: %d rows -> %d, %d appended", desc, len(before.rows), len(after.rows), appended)
	}
	return top
}

func (o *txbOracle) nodeCallerInterval(fam int) {
	alive := o.aliveNodes()
	if len(alive) == 0 {
		return
	}
	id := alive[o.ivRng.IntN(len(alive))]
	node := captureNode(o.t, o.g, id)
	o.noteAbove(node)
	name := txbNodeFamilies()[fam].name
	bound := txbCascadeBound(node)
	at, ok := o.ivPickT(bound, bound)
	vf, vt := o.txbIntervalTarget()
	desc := fmt.Sprintf("%s/SetNodeVersionIntervalWithTx(%v,[%d,%d),t=%d)", name, id, vf, vt, at)
	before := snapNode(o.t, o.g, id)
	beforeR := o.render(id)
	prev := o.nodeView(id, at-1)
	err := ivDoors()[fam].node(o.g, id, vf, vt, map[string]any{"k": o.seqno}, at)
	o.seqno++
	o.record(desc, err)
	if !ok {
		o.refusedOrder(desc, err)
		o.unchanged(desc, beforeR, o.render(id))
		assertNodeUnchanged(o.t, desc, before, snapNode(o.t, o.g, id))
		return
	}
	if err != nil {
		o.fail("%s: %v", desc, err)
	}
	o.accepted[name+"/node-interval"]++
	top := o.txbCheckAppended(desc, node, captureNode(o.t, o.g, id), at)
	o.checkIntent(txbIntent{desc: desc, isNode: true, nid: id, at: at}, prev, txbWantView(top))
}

func (o *txbOracle) relCallerInterval(fam int) {
	live := o.liveRels()
	if len(live) == 0 {
		return
	}
	id := live[o.ivRng.IntN(len(live))]
	rel := captureRel(o.t, o.g, id, "")
	o.noteAbove(rel)
	name := txbNodeFamilies()[fam].name
	bound := txbCascadeBound(rel)
	at, ok := o.ivPickT(bound, bound)
	vf, vt := o.txbIntervalTarget()
	desc := fmt.Sprintf("%s/SetRelVersionIntervalWithTx(%v,[%d,%d),t=%d)", name, id, vf, vt, at)
	before := snapRel(o.t, o.g, id)
	beforeR := o.render(0, id)
	prev := o.relView(id, at-1)
	err := ivDoors()[fam].rel(o.g, id, vf, vt, map[string]any{"k": o.seqno}, at)
	o.seqno++
	o.record(desc, err)
	if !ok {
		o.refusedOrder(desc, err)
		o.unchanged(desc, beforeR, o.render(0, id))
		assertRelUnchanged(o.t, desc, before, snapRel(o.t, o.g, id))
		return
	}
	if err != nil {
		o.fail("%s: %v", desc, err)
	}
	o.accepted[name+"/rel-interval"]++
	top := o.txbCheckAppended(desc, rel, captureRel(o.t, o.g, id, ""), at)
	o.checkIntent(txbIntent{desc: desc, rid: id, at: at}, prev, txbWantView(top))
}
