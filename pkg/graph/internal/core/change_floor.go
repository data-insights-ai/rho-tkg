package core

import (
	"context"
	"errors"
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Change floor (temporal.API.NodeChangeFloorCtx)
//
// Walk: the node's history newest version first (store.HistorySuffixCapability)
// with the current row merged in at its version. The history shows no
// DeletedAt and TxFrom never falling with the version (the store's ordered
// bit; when the store does not show it, the walk reads every row and the door
// checks them, historyOrdered), the current row has no delete stamp, and
// every row walked has a temporal block, a version below the previous one
// and a TxFrom at or below the previous one's; so belief order is version
// order over the whole chain. Rows with TxFrom > pin are skipped; the first
// row recorded by pin is top.Version; rows with after < TxFrom <= pin are D;
// the first row w with TxFrom <= after ends the walk (the "+1"; when w is the
// current row, one more history row, the witness, must be recorded no later
// than w, as the ordered bit covers history only). The walk may stop at a history row w above the current row (a
// cascade leaves the current row below the rows it appended): the rows above
// the current row are then D or w, and a rewrite of the current row in place
// is caught by its UpdatedAt.
//
// Proof. The chain at after is every row up to w (every row below w is
// recorded no later than w); the chain at pin is it plus D, appended above w.
// The mark proves the first is what the mark read: w is the mark's top
// version (no backfill since), the mark's current row was not rewritten in
// place (its UpdatedAt) and no wholesale rewrite ran (the generation). D rows
// are newer beliefs than every row at after and each covers only [its start,
// ...); a supersession cap a D row puts on an older row lies at that start.
// On the own-bounds arm every winner below min start(D) is therefore
// unchanged; on the positional arm (both chains tiling) only w's end moves,
// to D's first start. Rows written after the mark can switch the arm either
// way; whichever chain takes the positional arm answers as the own-bounds arm
// on it (gapReach; TestResolverArmsAgree; the door refuses the chains outside
// that rule: unmigrated, TxFrom falling), and the own-bounds answers below
// min start(D) are unchanged. So the floor is min start(D).
// =============================================================================

// changeFloorRowCheck is how many rows the walk visits between ctx checks.
const changeFloorRowCheck = 256

// noteRewrite moves the rewrite generation (ChainTop.Gen): a change to rows
// that no version, stamp or UpdatedAt shows (an import, a merge, an untimed
// conversion, a history compaction). Every mark taken before it answers
// unproven.
func (c *Core) noteRewrite() { c.rewriteGen.Add(2) }

// chainGen is the rewrite generation: odd, so never the zero mark's.
func (c *Core) chainGen() uint64 { return c.rewriteGen.Load() | 1 }

// NodeChangeFloorCtx is temporal.API.NodeChangeFloorCtx.
func (t *TempOps) NodeChangeFloorCtx(ctx context.Context, id types.NodeID, after types.Instant, afterTop temporal.ChainTop, pin types.Instant) (types.Instant, temporal.ChainTop, error) {
	const door = "NodeChangeFloor"
	c := t.c
	meter, err := c.beginTimeline(ctx, door)
	if err != nil {
		return types.UnboundedValidFrom, temporal.ChainTop{}, err
	}
	after, pin = recordedPin(after), recordedPin(pin)
	if err := c.checkOpen(); err != nil {
		return types.UnboundedValidFrom, temporal.ChainTop{}, err
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return types.UnboundedValidFrom, temporal.ChainTop{}, err
	}
	if after > pin {
		return types.UnboundedValidFrom, temporal.ChainTop{}, fmt.Errorf("%w: change floor after %d is above pin %d", ErrInvalidTimeRange, after, pin)
	}
	if err := c.validateEffectivePin(pin); err != nil {
		return types.UnboundedValidFrom, temporal.ChainTop{}, err
	}
	var tally *storepkg.ReadTally
	if meter != nil {
		if err := meter.AddCandidates(door, 1); err != nil {
			return types.UnboundedValidFrom, temporal.ChainTop{}, err
		}
		tally = meter.Tally(door)
	}
	floor, top := types.UnboundedValidFrom, temporal.ChainTop{}
	err = c.readUnderRLock(func() error {
		rd, release := c.meteredEntityReads(tally)
		defer release()
		var err error
		floor, top, err = c.nodeChangeFloor(ctx, rd, id, after, afterTop, pin)
		return err
	})
	if err != nil {
		return types.UnboundedValidFrom, temporal.ChainTop{}, err
	}
	return floor, top, nil
}

// floorWalk is one change-floor walk's state.
type floorWalk struct {
	c         *Core
	ctx       context.Context
	id        types.NodeID
	cur       *types.Node
	after     types.Instant
	pin       types.Instant
	rows      int
	err       error
	failed    bool                           // the walk's conditions fail: no floor (no top but when unordered)
	unordered bool                           // the store did not show the chain ordered: the walk read every row
	hist      []storepkg.VersionTemporalMeta // when unordered: every history row, newest first
	merged    bool                           // the current row was stepped
	moved     bool                           // a row was stepped (prevVer, prevTx hold)
	topSet    bool                           // top holds the highest version recorded by pin
	found     bool                           // w was reached
	prevV     uint32
	prevTx    types.Instant
	top       uint32
	w         *types.Node   // the newest row recorded by after
	d         []*types.Node // rows recorded in (after, pin], newest first
}

// step checks one row (hist: a history row, else the current row); false
// ends the walk. Delete and retraction stamps need no check here: the
// current row has none (nodeChangeFloor) and the ordered bit excludes them
// from history.
func (w *floorWalk) step(r *types.Node, hist bool) bool {
	tm := r.Temporal()
	v := r.Version()
	if tm == nil || (w.moved && (v >= w.prevV || tm.TxFrom > w.prevTx)) {
		w.failed = true
		return false
	}
	w.moved, w.prevV, w.prevTx = true, v, tm.TxFrom
	if w.found {
		return false // the witness under a current w: recorded no later than it
	}
	if tm.TxFrom > w.pin {
		return true // not recorded by the pin: by the TxFrom order a top run
	}
	if !w.topSet {
		w.topSet, w.top = true, v
	}
	if tm.TxFrom > w.after {
		w.d = append(w.d, r)
		return true
	}
	w.found, w.w = true, r
	// w the current row: the ordered bit covers history only, so the top
	// history row (the witness) must not be recorded after it.
	return !hist
}

// noteTop notes the highest version recorded by pin: the first such row by
// falling version.
func (w *floorWalk) noteTop(v uint32, tm *types.TemporalMetadata) {
	if !w.topSet && tm != nil && tm.TxFrom <= w.pin {
		w.topSet, w.top = true, v
	}
}

// visit is the store walk's visitor: the current row merged in at its
// version, then the history row m.
func (w *floorWalk) visit(m storepkg.VersionTemporalMeta, ordered bool) bool {
	if w.rows++; w.rows%changeFloorRowCheck == 0 {
		if w.err = checkCtx(w.ctx); w.err != nil {
			return false
		}
	}
	if !ordered {
		// Not proven by the store: read on to the end and keep the rows, so
		// the door checks the order itself (nodeChangeFloor) and a store
		// whose bit waits for a per-ID read resolves it from this walk
		// (badger). The top (the highest version recorded by pin) is noted
		// for the mark either way.
		w.failed, w.unordered = true, true
		w.hist = append(w.hist, m)
		if !w.merged && w.cur.Version() > m.Version {
			w.merged = true
			w.noteTop(w.cur.Version(), w.cur.Temporal())
		}
		w.noteTop(m.Version, m.Temporal)
		return true
	}
	if !w.merged && w.cur.Version() >= m.Version {
		w.merged = true
		if w.cur.Version() == m.Version {
			w.failed = true // a history row at the current row's version
			return false
		}
		if !w.step(w.cur, false) {
			return false
		}
	}
	s := types.NewNode(w.id, 0, nil)
	s.SetVersion(m.Version)
	if m.Temporal != nil {
		s.SetTemporal(m.Temporal)
	}
	return w.step(s, true)
}

// nodeChangeFloor is NodeChangeFloorCtx's body (c.mu held for reading).
func (c *Core) nodeChangeFloor(ctx context.Context, rd entityReads, id types.NodeID, after types.Instant, afterTop temporal.ChainTop, pin types.Instant) (types.Instant, temporal.ChainTop, error) {
	const unproven = types.UnboundedValidFrom
	none := temporal.ChainTop{}
	c.entityLocks.LockEntity(id.SnowflakeID())
	defer c.entityLocks.UnlockEntity(id.SnowflakeID())
	for _, p := range []types.Instant{after, pin} {
		if err := c.checkNodePointCompaction(id, p); err != nil {
			return unproven, none, err
		}
		if err := c.checkNodePointRetentionRd(rd, id, p); err != nil {
			return unproven, none, err
		}
	}
	current, err := rd.currentNode(id)
	if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
		return unproven, none, err
	}
	if current == nil {
		has, known, err := rd.hasNodeHistory(id)
		if err != nil {
			return unproven, none, err
		}
		if known && !has {
			return unproven, none, storepkg.ErrNodeNotFound
		}
		return unproven, none, nil // deleted: the chain ends in a delete stamp
	}
	if c.historySuffix == nil || c.temporalMetaHistory == nil || !c.bitemporalMigrated ||
		c.readOnlyReplica || (rd.tally != nil && !c.storeCountsReads) {
		return unproven, none, nil
	}
	if tm := current.Temporal(); tm == nil || tm.DeletedAt != 0 || tm.Retracted {
		return unproven, none, nil
	}
	gen := c.chainGen()
	w := &floorWalk{c: c, ctx: ctx, id: id, cur: current, after: after, pin: pin}
	_, err = c.historySuffix.NodeHistoryNewestFirst(id, rd.tally, w.visit)
	c.noteNodeMeta(w.rows)
	if errors.Is(err, storepkg.ErrHistorySpansShards) {
		return unproven, none, nil
	}
	if err != nil {
		return unproven, none, err
	}
	if w.err != nil {
		return unproven, none, w.err
	}
	if !w.failed && !w.merged && !w.found {
		w.merged = true // every history row lies above the current row
		w.step(current, false)
	}
	if w.unordered && historyOrdered(w.hist) {
		// The rows read show what the store's bit did not: walk them again.
		hist := w.hist
		w = &floorWalk{c: c, ctx: ctx, id: id, cur: current, after: after, pin: pin}
		for _, m := range hist {
			if !w.visit(m, true) {
				break
			}
		}
		if w.err != nil {
			return unproven, none, w.err
		}
		if !w.failed && !w.merged && !w.found {
			w.merged = true
			w.step(current, false)
		}
	}
	if w.unordered && !w.merged {
		w.noteTop(current.Version(), current.Temporal()) // every history row lies above the current row
	}
	switch {
	case w.unordered && w.topSet:
		// The store may show the chain ordered on the next call (this walk
		// resolved its bit): its mark is this top.
		return unproven, temporal.ChainTop{Updated: current.Temporal().UpdatedAt, Gen: gen, Version: w.top, Cur: current.Version()}, nil
	case w.failed:
		return unproven, none, nil // TxFrom falling with version among the rows walked
	}
	if !w.topSet {
		return unproven, none, nil // the pin records no row of the node
	}
	top := temporal.ChainTop{Updated: current.Temporal().UpdatedAt, Gen: gen, Version: w.top, Cur: current.Version()}
	floor := types.InfiniteValidTo
	for _, r := range w.d {
		floor = min(floor, c.nodeValidFrom(r))
	}
	if !w.found || afterTop.Gen == 0 || afterTop.Gen != gen || w.w.Version() != afterTop.Version {
		return unproven, top, nil // no mark, a rewrite, a backfill, or the mark's top row gone
	}
	if afterTop.Cur == current.Version() {
		if current.Temporal().UpdatedAt != afterTop.Updated {
			return unproven, top, nil // rewritten in place
		}
	} else {
		old, err := rd.nodeVersion(id, afterTop.Cur)
		if errors.Is(err, storepkg.ErrNodeNotFound) || errors.Is(err, storepkg.ErrVersionNotFound) {
			return unproven, top, nil
		}
		if err != nil {
			return unproven, none, err
		}
		if tm := old.Temporal(); tm == nil || tm.UpdatedAt != afterTop.Updated {
			return unproven, top, nil
		}
	}
	return floor, top, nil
}

// historyOrdered reports whether hist (every history row, newest first)
// shows what the store's ordered bit states: a temporal block on every row,
// no DeletedAt, and TxFrom never rising as the version falls.
func historyOrdered(hist []storepkg.VersionTemporalMeta) bool {
	for i, m := range hist {
		tm := m.Temporal
		if tm == nil || tm.DeletedAt != 0 || (i > 0 && tm.TxFrom > hist[i-1].Temporal.TxFrom) {
			return false
		}
	}
	return true
}
