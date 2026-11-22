package core

import (
	"math"
	"slices"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Cold tail reads (TAILCOLD)
//
// A head miss of NodeEffectiveTimelineFrom(id, validFrom, pin) reads the
// node's history newest version first (store.HistorySuffixCapability) and
// stops at the first visible row starting below validFrom, when the rows
// read prove that nothing older reaches validFrom: a cold process's first
// tail read of a fact costs the rows from validFrom on, not the fact's age.
//
// Walk: history skeletons by falling version with the current row merged in
// at its version. Every visited row has (a) a temporal block, no DeletedAt,
// no retraction; (b) a version below the previous one (no history row at the
// current row's version); (c) a TxFrom at or below the previous row's.
// Visible rows (TxFrom <= pin) also have: (d) the first is own-open (void
// allowed); (e) each next one r under q starts at or before q and is open or
// ends at or after q's start. The walk stops at the first visible row r1
// starting below validFrom at or below the current row's version; when r1 is
// the current row one more history row (the witness) must pass (a) to (c).
//
// Why it is the fold's answer: the store's ordered bit (no DeletedAt in
// history, TxFrom never falling with version) with (c) and the witness puts
// every unread row P below r1 in version and in TxFrom, without deletes: no
// life caps, no retracted lives, every read row a newer belief than every
// unread one. For t >= start(r1) the highest-version visible row rj starting
// at or before t covers t by (d) and (e); in the monotonic arm it is the
// highest position covering t, in the own-bounds arm the newest covering
// belief (a supersession cap on rj comes only from a newer row, which starts
// after t). So the winner on [start(r1), open) is the same in both arms and
// independent of P, and every piece P could win ends at or before start(r1),
// below validFrom: the tail equals tailOf(fold) at every pin.
//
// Any violated condition, or the rows running out, collects the rest of the
// history and folds it whole as before (no row is read twice).
// =============================================================================

// suffixFacts is what a proven suffix walk knows about the whole chain: the
// suffix entry's facts (noteNodeSuffixHead).
type suffixFacts struct {
	from      types.Instant // start(r1): the pieces are exact from here on
	maxTx     types.Instant // newest TxFrom of the chain (the top visited row's)
	maxVer    uint32        // highest version of the chain
	txToMax   types.Instant // newest TxTo among the rows read
	lastStart types.Instant // the highest-version row's start
	lastEnd   types.Instant // and its own ValidTo
	visible   bool          // every visited row was recorded by the pin
}

// suffixWalk is one cold tail read's walk state.
type suffixWalk struct {
	c         *Core
	a         types.ValidAxis
	id        types.NodeID
	cur       *types.Node
	validFrom types.Instant
	pin       types.Instant

	collected []storepkg.VersionTemporalMeta // history rows visited, newest first
	inS       int                            // collected[:inS] are the suffix (the witness is not)
	curMerged bool
	started   bool
	prevVer   uint32
	prevTx    types.Instant
	visible   bool // a visible row was seen
	qStart    types.Instant
	witness   bool // r1 was the current row: one more history row must pass (a) to (c)
	proven    bool
	failed    bool
	facts     suffixFacts
}

// step checks one row (history, or the current row when hist is false).
func (w *suffixWalk) step(r *types.Node, hist bool) {
	if hist {
		w.collected = append(w.collected, storepkg.VersionTemporalMeta{Version: r.Version(), Temporal: r.Temporal()})
	}
	if w.failed || w.proven {
		return
	}
	tm := r.Temporal()
	v := r.Version()
	if tm == nil || tm.DeletedAt != 0 || tm.Retracted ||
		(w.started && (v >= w.prevVer || tm.TxFrom > w.prevTx)) {
		w.failed = true
		return
	}
	if !w.started {
		w.facts.maxVer, w.facts.maxTx = v, tm.TxFrom
		w.facts.lastStart, w.facts.lastEnd = w.c.nodeValidFrom(r), tm.ValidTo
		w.facts.visible = true
	}
	w.started, w.prevVer, w.prevTx = true, v, tm.TxFrom
	w.facts.txToMax = max(w.facts.txToMax, tm.TxTo)
	if w.witness {
		w.proven = true
		return
	}
	if hist {
		w.inS = len(w.collected)
	}
	if tm.TxFrom > w.pin {
		w.facts.visible = false
		return // not recorded by the pin: by (c) the invisible rows are a top run
	}
	s, e := w.c.nodeOwnBounds(r)
	if !w.visible {
		if !w.a.Open(e) {
			w.failed = true // (d): a bounded newest row bounds nothing older rows cover after it
			return
		}
	} else if s > w.qStart || (!w.a.Open(e) && e < w.qStart) {
		w.failed = true // (e): a gap or an inverted start
		return
	}
	w.visible, w.qStart = true, s
	if s < w.validFrom && w.curMerged {
		w.facts.from = s
		if hist {
			w.proven = true
		} else {
			w.witness = true
		}
	}
}

// visit is the store walk's visitor: the current row merged in at its
// version, then the history row m. It asks for more rows until the walk is
// proven; a failed walk collects to the bottom.
func (w *suffixWalk) visit(m storepkg.VersionTemporalMeta) bool {
	if !w.curMerged && w.cur.Version() >= m.Version {
		w.curMerged = true
		if w.cur.Version() == m.Version {
			w.failed = true // (b): a history row at the current row's version
		} else {
			w.step(w.cur, false)
		}
	}
	s := types.NewNode(w.id, 0, nil)
	s.SetVersion(m.Version)
	if m.Temporal != nil {
		s.SetTemporal(m.Temporal)
	}
	w.step(s, true)
	return !w.proven
}

// nodeEffectiveViaSuffix answers a head miss of a tail read from the
// history's suffix (see the section comment); handled false: the read is not
// eligible, the caller folds as before. A failed walk folds the rows it
// collected (the whole history) with facts, as nodeEffectiveViaTemporalMeta.
func (c *Core) nodeEffectiveViaSuffix(rd entityReads, id types.NodeID, current *types.Node, validFrom, pin types.Instant, epoch uint64, facts *headFacts) ([]temporal.NodeSegment, bool, error) {
	if validFrom == types.UnboundedValidFrom || !rd.readsHead() || c.headCacheOff || c.headSuffixOff ||
		current == nil || c.historySuffix == nil || c.temporalMetaHistory == nil || !c.bitemporalMigrated ||
		(rd.tally != nil && !c.storeCountsReads) {
		return nil, false, nil
	}
	if tm := current.Temporal(); tm == nil || tm.DeletedAt != 0 {
		return nil, false, nil
	}
	ordered, err := c.historySuffix.NodeHistoryOrdered(id, rd.tally)
	if err != nil {
		return nil, true, err
	}
	if !ordered {
		return nil, false, nil
	}
	w := &suffixWalk{c: c, a: c.axis(), id: id, cur: current, validFrom: validFrom, pin: pin}
	err = c.historySuffix.NodeHistoryNewestFirst(id, rd.tally, w.visit)
	c.noteNodeMeta(len(w.collected))
	if err != nil {
		return nil, true, err
	}
	if !w.proven && !w.curMerged {
		w.curMerged = true // every history row lies above the current row
		w.step(current, false)
	}
	if !w.proven {
		// Failed, or the rows ran out: collected is the whole history.
		slices.Reverse(w.collected)
		return c.nodeEffectiveOnMetas(rd, id, current, w.collected, validFrom, pin, epoch, facts, nil)
	}
	suffix := slices.Clone(w.collected[:w.inS])
	slices.Reverse(suffix)
	var sf *suffixFacts
	if epoch != 0 && w.facts.visible {
		w.facts.maxVer = max(w.facts.maxVer, current.Version())
		w.facts.maxTx = max(w.facts.maxTx, current.Temporal().TxFrom)
		w.facts.txToMax = max(w.facts.txToMax, current.Temporal().TxTo)
		sf = &w.facts
	}
	return c.nodeEffectiveOnMetas(rd, id, current, suffix, validFrom, pin, epoch, nil, sf)
}

// noteNodeSuffixHead keeps a proven suffix walk's answer as a suffix entry:
// its pieces exact from start(r1) on (pieceFrom), no bounds (no correction
// plans from it), and the chain's newest stamps (the stamps sidecar's TxTo
// joins the rows read). It is not kept over a valid entry of a whole fold,
// which plans corrections, nor when the stamps do not confirm the walk's
// newest TxFrom.
func (c *Core) noteNodeSuffixHead(id types.NodeID, epoch uint64, current *types.Node, pieces []effPiece[*types.Node], f *suffixFacts) {
	if c.historyStamps == nil {
		return
	}
	k := headKey{id: id.SnowflakeID()}
	if e := c.headValid(k, epoch, current.Version(), temporalOf(current), f.maxTx); e != nil && !e.suffix {
		return
	}
	hf, ht, _, err := c.historyStamps.NodeHistoryStamps(id)
	if err != nil || hf > f.maxTx {
		return
	}
	e := &headEntry{epoch: epoch, curVer: current.Version(), curTM: temporalOf(current),
		maxTx: f.maxTx, maxTxTo: max(ht, f.txToMax), maxVer: f.maxVer,
		lastStart: f.lastStart, lastEnd: f.lastEnd, pieces: make([]headPiece, len(pieces)),
		pieceFrom: f.from, boundFrom: math.MaxInt64, suffix: true}
	for i, p := range pieces {
		e.pieces[i] = headPiece{from: p.from, to: p.to, version: p.row.Version()}
	}
	e.trim(c.headCaps())
	c.heads.put(k, e)
}
