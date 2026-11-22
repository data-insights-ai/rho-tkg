package core

import (
	"context"
	"fmt"
	"slices"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
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

// coldTail reads the node's timeline from validFrom at the committed pin under
// a read budget: the segments as (from, to, version) triples and the history
// rows the read examined (ReadMeter HistoryExamined).
func coldTail(tb testing.TB, g *Core, e appendEntity, validFrom types.Instant) ([]string, uint64) {
	tb.Helper()
	pin, err := g.Temporal.CommittedTx()
	if err != nil {
		tb.Fatal(err)
	}
	ctx, meter := storepkg.WithReadBudget(context.Background(), storepkg.ReadBudget{})
	segs, err := g.Temporal.NodeEffectiveTimelineFromCtx(ctx, e.nid, validFrom, pin)
	if err != nil {
		tb.Fatal(err)
	}
	out := make([]string, len(segs))
	for i, s := range segs {
		out[i] = fmt.Sprintf("[%d,%d) v%d", s.ValidFrom, s.ValidTo, s.Node.Version())
	}
	return out, meter.Work().HistoryExamined
}

// TestHeadTail_ColdReadExaminesItsSuffix (TAILCOLD): in a cold process (a
// reopened store, or memory with the entity's head entry dropped) the first
// tail read of a fact Sigma extends every cut, from just above the previous
// cut's instant, examines the history rows from there on: the same count
// after 32 and after 128 cuts, for a new open version per cut and for a
// bounded correction plus its open resumption per cut. Its answer is the
// fold's, the next read is a head hit, a cut written after it extends the
// entry (the read after it a hit), and a tail from below the entry's start
// and the whole timeline still equal the fold. Before, the first read folded
// the whole chain: its count grew with the cuts. Sharded offers no suffix
// read and folds as before (not gated).
func TestHeadTail_ColdReadExaminesItsSuffix(t *testing.T) {
	at := func(k int) types.Instant { return 1000 + types.Instant(1000*k) }
	shapes := []struct {
		name string
		cut  func(g *Core, e appendEntity, k int) error
	}{
		{"open version", func(g *Core, e appendEntity, k int) error { return e.set(g, at(k), 0, k) }},
		{"bounded correction and resumption", func(g *Core, e appendEntity, k int) error {
			if err := e.set(g, at(k), at(k)+500, 2*k); err != nil {
				return err
			}
			return e.set(g, at(k)+500, 0, 2*k+1)
		}},
	}
	for _, be := range hr2Backends() {
		for _, sh := range shapes {
			t.Run(be.name+"/"+sh.name, func(t *testing.T) {
				examined := map[int]uint64{}
				for _, cuts := range []int{32, 128} {
					dir := t.TempDir()
					g, shut := be.open(t, dir)
					e := newAppendEntity(t, g, false)
					for k := 1; k <= cuts; k++ {
						if err := sh.cut(g, e, k); err != nil {
							t.Fatalf("cut %d: %v", k, err)
						}
					}
					if be.persistent {
						shut()
						g, shut = be.open(t, dir)
					} else {
						g.heads.put(headKey{id: e.nid.SnowflakeID()}, nil)
					}
					from := at(cuts-1) + 1
					got, n := coldTail(t, g, e, from)
					examined[cuts] = n
					if want := tailSegments(t, g, e, from, true); !slices.Equal(got, want) {
						t.Fatalf("%d cuts: cold tail %v, the fold gives %v", cuts, got, want)
					}
					hits := g.headHits.Load()
					if again, _ := coldTail(t, g, e, from); !slices.Equal(again, got) || g.headHits.Load() != hits+1 {
						t.Errorf("%d cuts: the second read %v (%d hits); want %v from the entry", cuts, again, g.headHits.Load()-hits, got)
					}
					plans := g.headStats.missPlans.Load()
					for k := cuts + 1; k <= cuts+4; k++ {
						if err := sh.cut(g, e, k); err != nil {
							t.Fatal(err)
						}
						hits = g.headHits.Load()
						next := at(k-1) + 1
						if got, want := tailSegments(t, g, e, next, false), tailSegments(t, g, e, next, true); !slices.Equal(got, want) {
							t.Errorf("%d cuts: the tail after cut %d %v, the fold %v", cuts, k, got, want)
						}
						if be.name != "sharded" && g.headHits.Load() != hits+1 {
							t.Errorf("%d cuts: the read after cut %d was no head hit: the write did not extend the entry", cuts, k)
						}
					}
					if n := g.headStats.missPlans.Load() - plans; be.name != "sharded" && n > 1 {
						t.Errorf("%d cuts: 4 cuts after the cold read planned %d corrections on the whole history; want at most 1 (the first replaces the suffix entry with the whole answer)", cuts, n)
					}
					for _, low := range []types.Instant{at(2), types.UnboundedValidFrom} {
						if got, want := tailSegments(t, g, e, low, false), tailSegments(t, g, e, low, true); !slices.Equal(got, want) {
							t.Errorf("%d cuts: tail from %d = %d segments, the fold %d", cuts, low, len(got), len(want))
						}
					}
					shut()
				}
				t.Logf("history examined by the cold tail read: %d after 32 cuts, %d after 128", examined[32], examined[128])
				if be.name != "sharded" && (examined[32] != examined[128] || examined[128] > 16) {
					t.Errorf("the cold tail read examined %d history rows after 32 cuts and %d after 128; want equal and at most 16 (the rows from validFrom on; the 32-cut chain holds 32 or more)", examined[32], examined[128])
				}
			})
		}
	}
}
