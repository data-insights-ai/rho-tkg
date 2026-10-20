package core

import (
	"sort"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Corrections from the head entry (backlog 68b, sigma R10 write side)
//
// A correction (cascadeNode / cascadeRel) reads the pre-correction chain for
// five things: the highest version, the order rule of a caller instant, the
// resumption (the row valid at newVT and the first own bound after it), the
// correction pieces (the belief-winner over [newVF, newVT), cut where it
// changes) and the row that takes the current slot (the newest belief among
// the own-open, non-void rows). A head entry that is valid at the write's
// start (effective_head.go: the history token and the current row match)
// holds each of them for the whole chain: the pieces are the belief-winner at
// every instant (the fold at the head pin is the point resolver's answer
// pointwise, and with no delete in the chain the cascade's resolver and the
// fold's agree), maxVer / maxTx / maxTxTo are the chain's highest version and
// stamps, bounds are every row's own bounds, open is the newest open belief.
// So the correction takes them from the entry plus one point read per base
// row (and one for the resumption source) instead of the history, and builds
// byte-for-byte the rows the history read builds: the twin test
// (effective_head_write_test.go) runs both on twin graphs.
//
// The GraphTx rollback snapshot of such an entity is its current row and the
// trim point maxVer+1 (tx.go, snapshotCurrentNodeLocked): every row a
// transaction writes lies above maxVer or is the current row moved into
// history at its own version, which a valid entry proves was no history key
// before (its chain's versions are unique).
//
// Without a valid entry (none kept, a stale one, more than headMaxEntryBounds
// bounds, a caller instant the order rule refuses, a base row gone) the
// correction reads the history as before.
// =============================================================================

// headChain is what a correction reads of the pre-correction chain, taken
// from a head entry.
type headChain[T any] struct {
	maxVersion uint32
	src        T // the row valid at newVT (the resumption's source)
	hasSrc     bool
	srcEnd     types.Instant // the resumption's end: the first own bound after newVT (open by the graph's valid axis)
	segments   []correctionSegment[T]
	open       openBelief
	// voidFree: the chain holds no void row (wholeAxisSupersession), known
	// from the head entry. From the current row it stays false: that plan
	// has one changed segment and no gap after it, where voidFree is not
	// asked.
	voidFree bool
}

// chainFromHead returns the chain facts of a correction of [newVF, newVT) at
// at (0: the clock) from e, or ok false when the correction must read the
// history. current is the current row the entry was validated against
// (template and gap base); read point-reads a history row by version.
func chainFromHead[T interface {
	comparable
	storeutil.TemporalRow
}](c *Core, e *headEntry, current T, read func(uint32) (T, error), newVF, newVT, at types.Instant, splitGaps bool) (headChain[T], bool) {
	var hc headChain[T]
	if e == nil || e.bounds == nil || c.headWriteOff {
		return hc, false
	}
	if at != 0 && at <= max(e.maxTx, e.maxTxTo) {
		// The order rule refuses at: the history read names the stamp in
		// the error (cascadeInstant).
		return hc, false
	}
	var readV []uint32
	var readRows []T
	row := func(v uint32) (T, bool) {
		if v == current.Version() {
			return current, true
		}
		for i, w := range readV {
			if w == v {
				return readRows[i], true
			}
		}
		r, err := read(v)
		if err != nil {
			var zero T
			return zero, false
		}
		readV, readRows = append(readV, v), append(readRows, r)
		return r, true
	}
	a := c.axis()
	if !a.Open(newVT) {
		if v, ok := e.winnerAt(a, newVT); ok {
			src, ok := row(v)
			if !ok {
				return hc, false
			}
			hc.src, hc.hasSrc, hc.srcEnd = src, true, e.boundAfter(a, newVT)
		}
	}
	runs := e.correctionRuns(a, newVF, newVT, current.Version(), splitGaps)
	hc.segments = make([]correctionSegment[T], len(runs))
	for i, run := range runs {
		base, ok := row(run.version)
		if !ok {
			return hc, false
		}
		hc.segments[i] = correctionSegment[T]{from: run.from, to: run.to, base: base, gap: run.gap}
	}
	hc.maxVersion = max(e.maxVer, current.Version())
	hc.open = e.open
	hc.voidFree = !e.void
	return hc, true
}

// =============================================================================
// Appends from the current row (backlog 67, sigma-tkgd 2026-10-17)
//
// An open correction [newVF, +inf) at or after the current row's own start —
// a new version, no backdating — needs nothing of the chain but the current
// row once that row is the chain's top version and newest belief, open and
// live: then no other row wins any instant from the current row's start on
// (the cascade arm keeps the newest covering belief, and no row is newer; the
// monotonic arm tiles by version, and the top version starts last and is
// open), so the one piece is [newVF, +inf) on the current row, there is no
// resumption, the top version is the current row's, and the newest open
// belief is the current row. Each fact is read in O(1): the version probe
// above the current row (nodeHasHistoryAbove, exact, backlog 70) and the
// history's newest stamps (store.HistoryStampsCapability, backlog 30). The
// cascade then builds byte-for-byte the rows the history read builds (the
// twin tests, cascade_append_test.go and effective_head_write_twin_test.go).
//
// Everything else is planned from the head entry or the history as before: a
// bounded correction (its resumption ends at the chain's next own bound,
// which only the chain knows), a backdated one (it cuts earlier versions), an
// excise, a closed, void or deleted current row, a history row at or above
// the current row's version or with a stamp after its TxFrom, a caller
// instant the order rule refuses (the history read names the stamp), a store
// without the stamps capability, and a chain stored before the bitemporal
// migration (its starts can be inherited).
// =============================================================================

// chainFromCurrent returns the chain facts of an open correction from newVF
// at at (0: the clock) on an entity whose current row is current, or ok
// false when the conditions above do not hold. above probes for a history row
// above a version; stamps returns the history's newest TxFrom and TxTo (or
// DeletedAt), nil when the store does not keep them. A probe that fails falls
// back: the history read meets the error.
func chainFromCurrent[T interface {
	comparable
	storeutil.TemporalRow
}](c *Core, k *chainKernel[T], current T, above func(uint32) (bool, error),
	stamps func() (types.Instant, types.Instant, bool, error), newVF, newVT, at types.Instant, void bool) (headChain[T], bool) {
	var hc, zero headChain[T]
	var none T
	a := c.axis()
	if c.headWriteOff || !c.bitemporalMigrated || !a.Open(newVT) || void || current == none || stamps == nil {
		return zero, false
	}
	tm := current.Temporal()
	if tm == nil || !a.Open(tm.ValidTo) || tm.Void || tm.Retracted || tm.DeletedAt != 0 || tm.TxTo != 0 || tm.TxFrom == 0 {
		return zero, false
	}
	if start, _ := k.ownBounds(c, current); newVF < start {
		return zero, false // backdated: an earlier version is cut
	}
	if up, err := above(current.Version()); err != nil || up {
		return zero, false
	}
	hf, ht, _, err := stamps()
	if err != nil || max(hf, ht) > tm.TxFrom {
		return zero, false
	}
	if at != 0 && at <= tm.TxFrom {
		return zero, false // the order rule refuses at
	}
	hc.maxVersion = current.Version()
	hc.segments = []correctionSegment[T]{{from: newVF, to: a.OpenEnd(), base: current}}
	hc.open.note(beliefTx(tm), current.Version())
	return hc, true
}

// headRun is one correction piece by version (gap: no row valid there, the
// template's version).
type headRun struct {
	from, to types.Instant
	version  uint32
	gap      bool
}

// correctionRuns cuts [newVF, newVT) (newVT open by the valid axis a) where
// the winner changes, as correctionChain.segments does on the chain: maximal
// runs of one base, a gap's base being the template; with splitGaps a gap
// never merges with a piece the template wins.
func (e *headEntry) correctionRuns(a types.ValidAxis, newVF, newVT types.Instant, template uint32, splitGaps bool) []headRun {
	runs := make([]headRun, 0, 2)
	add := func(from, to types.Instant, version uint32, gap bool) {
		if n := len(runs); n > 0 && runs[n-1].version == version && (!splitGaps || runs[n-1].gap == gap) {
			runs[n-1].to = to
			return
		}
		runs = append(runs, headRun{from: from, to: to, version: version, gap: gap})
	}
	at := newVF
	for _, p := range e.pieces {
		if !a.Open(p.to) && p.to <= at {
			continue
		}
		if !a.Open(newVT) && p.from >= newVT {
			break
		}
		if p.from > at {
			add(at, p.from, template, true)
			at = p.from
		}
		end := p.to
		if !a.Open(newVT) && (a.Open(end) || end > newVT) {
			end = newVT
		}
		add(at, end, p.version, false)
		if a.Open(end) || end == newVT {
			return runs
		}
		at = end
	}
	add(at, newVT, template, true)
	return runs
}

// winnerAt returns the version that wins t, ok false in a gap (piece ends
// open by the valid axis a).
func (e *headEntry) winnerAt(a types.ValidAxis, t types.Instant) (uint32, bool) {
	i := sort.Search(len(e.pieces), func(i int) bool { return e.pieces[i].from > t }) - 1
	if i < 0 {
		return 0, false
	}
	if p := e.pieces[i]; a.Open(p.to) || p.to > t {
		return p.version, true
	}
	return 0, false
}

// boundAfter returns the smallest own bound strictly after t (the valid
// axis a's open end: none), as nodeResumptionEnd does on the chain.
func (e *headEntry) boundAfter(a types.ValidAxis, t types.Instant) types.Instant {
	i := sort.Search(len(e.bounds), func(i int) bool { return e.bounds[i] > t })
	if i == len(e.bounds) {
		return a.OpenEnd()
	}
	return e.bounds[i]
}

// nodeHeadTop returns the highest version of node id's chain from a valid
// head entry (current is the current row), ok false without one: the GraphTx
// rollback snapshot's trim point (tx.go).
func (c *Core) nodeHeadTop(id types.NodeID, current *types.Node) (uint32, bool) {
	if c.headWriteOff {
		return 0, false
	}
	e := c.nodeHeadBeforeWrite(id, current)
	if e == nil {
		return 0, false
	}
	return max(e.maxVer, current.Version()), true
}

// relHeadTop mirrors nodeHeadTop for relationships.
func (c *Core) relHeadTop(id types.RelID, current *types.Relationship) (uint32, bool) {
	if c.headWriteOff {
		return 0, false
	}
	e := c.relHeadBeforeWrite(id, current)
	if e == nil {
		return 0, false
	}
	return max(e.maxVer, current.Version()), true
}

// headNewCurrent returns the appended row that takes the current slot — the
// newest belief among the own-open, non-void rows of the chain (open) and
// appended — or the zero T when a chain row keeps it.
func headNewCurrent[T interface {
	comparable
	storeutil.TemporalRow
}](c *Core, k *chainKernel[T], open openBelief, appended []T) T {
	var cur T
	for _, r := range appended {
		if _, end := k.ownBounds(c, r); !c.axis().Open(end) || isVoidRow(r) {
			continue
		}
		before := open
		open.note(beliefTx(r.Temporal()), r.Version())
		if open != before {
			cur = r
		}
	}
	return cur
}
