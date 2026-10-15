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
	srcEnd     types.Instant // the resumption's end: the first own bound after newVT (0: open)
	segments   []correctionSegment[T]
	open       openBelief
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
	if newVT != 0 {
		if v, ok := e.winnerAt(newVT); ok {
			src, ok := row(v)
			if !ok {
				return hc, false
			}
			hc.src, hc.hasSrc, hc.srcEnd = src, true, e.boundAfter(newVT)
		}
	}
	runs := e.correctionRuns(newVF, newVT, current.Version(), splitGaps)
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
	return hc, true
}

// headRun is one correction piece by version (gap: no row valid there, the
// template's version).
type headRun struct {
	from, to types.Instant
	version  uint32
	gap      bool
}

// correctionRuns cuts [newVF, newVT) (newVT 0: open) where the winner
// changes, as nodeCorrectionSegments does on the chain: maximal runs of one
// base, a gap's base being the template; with splitGaps a gap never merges
// with a piece the template wins.
func (e *headEntry) correctionRuns(newVF, newVT types.Instant, template uint32, splitGaps bool) []headRun {
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
		if p.to != 0 && p.to <= at {
			continue
		}
		if newVT != 0 && p.from >= newVT {
			break
		}
		if p.from > at {
			add(at, p.from, template, true)
			at = p.from
		}
		end := p.to
		if newVT != 0 && (end == 0 || end > newVT) {
			end = newVT
		}
		add(at, end, p.version, false)
		if end == 0 || end == newVT {
			return runs
		}
		at = end
	}
	add(at, newVT, template, true)
	return runs
}

// winnerAt returns the version that wins t, ok false in a gap.
func (e *headEntry) winnerAt(t types.Instant) (uint32, bool) {
	i := sort.Search(len(e.pieces), func(i int) bool { return e.pieces[i].from > t }) - 1
	if i < 0 {
		return 0, false
	}
	if p := e.pieces[i]; p.to == 0 || p.to > t {
		return p.version, true
	}
	return 0, false
}

// boundAfter returns the smallest own bound strictly after t (0: none), as
// nodeResumptionEnd does on the chain.
func (e *headEntry) boundAfter(t types.Instant) types.Instant {
	i := sort.Search(len(e.bounds), func(i int) bool { return e.bounds[i] > t })
	if i == len(e.bounds) {
		return 0
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
		if _, end := k.ownBounds(c, r); end != 0 || isVoidRow(r) {
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
