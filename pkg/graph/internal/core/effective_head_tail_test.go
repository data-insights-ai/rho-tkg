package core

import (
	"fmt"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// tailSegments is the node's timeline from validFrom at the committed pin as
// (from, to, version) triples, read through the head entry (fold false) or by
// a fold of the chain (fold true: headCacheOff, the differential oracle).
func tailSegments(tb testing.TB, g *Core, e appendEntity, validFrom types.Instant, fold bool) []string {
	tb.Helper()
	g.headCacheOff = fold
	defer func() { g.headCacheOff = false }()
	pin, err := g.Temporal.CommittedTx()
	if err != nil {
		tb.Fatal(err)
	}
	segs, err := g.Temporal.NodeEffectiveTimelineFrom(e.nid, validFrom, pin)
	if err != nil {
		tb.Fatal(err)
	}
	out := make([]string, len(segs))
	for i, s := range segs {
		out[i] = fmt.Sprintf("[%d,%d) v%d", s.ValidFrom, s.ValidTo, s.Node.Version())
	}
	return out
}

// TestHeadTail_TrimmedEntryAnswersTailReads (CUTCOST): a fact that gets a new
// open version every cut (a count) passes the entry's piece cap after cap
// cuts; the entry then keeps its newest pieces. A read of the timeline's tail
// from an instant above the entry's watermark is answered from those pieces,
// equal to the fold's answer, with no read of the chain: before, every read of
// such a chain folded all of it, so a cut's cost grew with the fact's age. A
// read from at or below the watermark, and a whole read, still fold.
func TestHeadTail_TrimmedEntryAnswersTailReads(t *testing.T) {
	const pieceCap, cuts = 16, 80
	for _, be := range r10Backends() {
		t.Run(be.name, func(t *testing.T) {
			g, err := New(be.cfg())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = g.Close() }()
			g.headPieceCap, g.headBoundCap = pieceCap, 2*pieceCap
			e := newAppendEntity(t, g, false)
			at := func(k int) types.Instant { return e.base + types.Instant(1000*k) }
			reads := func(r headReason) uint64 { return g.headStats.reads[headDoorOne][r].Load() }
			var hits, notWhole uint64
			for k := 1; k <= cuts; k++ {
				// sigma's cut: the tail from past the previous instant, then the next version
				from := at(k-1) + 1
				h, n := reads(headHit), reads(headMissNotWhole)
				got := tailSegments(t, g, e, from, false)
				if k > cuts-8 {
					hits, notWhole = hits+reads(headHit)-h, notWhole+reads(headMissNotWhole)-n
					if want := tailSegments(t, g, e, from, true); !slices.Equal(got, want) || len(got) != 1 {
						t.Fatalf("cut %d: tail from %d = %v, the fold gives %v; want the open segment", k, from, got, want)
					}
				}
				if err := e.set(g, at(k), 0, k); err != nil {
					t.Fatalf("cut %d: %v", k, err)
				}
			}
			if hits != 8 || notWhole != 0 {
				t.Errorf("the last 8 tail reads: %d answered by the entry, %d refused as trimmed; want 8 and 0: a tail above the watermark is the entry's", hits, notWhole)
			}
			whole, oracle := tailSegments(t, g, e, types.UnboundedValidFrom, false), tailSegments(t, g, e, types.UnboundedValidFrom, true)
			if !slices.Equal(whole, oracle) || len(whole) != cuts+1 {
				t.Errorf("the whole timeline through a trimmed entry has %d segments, the fold %d, want %d equal ones", len(whole), len(oracle), cuts+1)
			}
			tailSegments(t, g, e, at(cuts)+1, false) // the whole read's fold filled the entry again: trim it by one more cut
			if err := e.set(g, at(cuts+1), 0, cuts+1); err != nil {
				t.Fatal(err)
			}
			n := reads(headMissNotWhole)
			low := at(2)
			if got, want := tailSegments(t, g, e, low, false), tailSegments(t, g, e, low, true); !slices.Equal(got, want) || reads(headMissNotWhole) == n {
				t.Errorf("a tail from below the watermark: %d segments, the fold gives %d, %d reads refused as trimmed; want equal answers by a fold (is the entry trimmed?)", len(got), len(want), reads(headMissNotWhole)-n)
			}
		})
	}
}
