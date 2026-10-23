package core

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Rows 87 and 88, consumer shape (sigma 2026-10-20 R19 (b) and (c), the
// Derived writer's `grow` shape, scratchpad r19 probe): the Derived node and
// its DERIVED_IN edge are created bounded; every cut k writes, in one GraphTx,
// SetNodeVersionInterval and SetRelVersionInterval over [t0, t0+(k+1)*1000)
// — the newest bounded end moves later, from the chain's start — and reads
// both effective timelines at the committed pin (believed, derivedIn).
//
// The correction appends one row per piece of [t0, t0+(k+1)*1000) (the
// bases differ: the previous cut's rows, then the template for the new
// stretch), so cut k appends k rows, the head answer has k pieces and the
// chain holds k²/2 rows. Past the entry's former cap of 512 pieces the entry
// was trimmed: a read needs a whole entry and folded the chain, and the write,
// starting below the trimmed watermark, read and sorted it — 232 ms per 40
// cuts at 480 cuts, 5.4 s at 520, 45 s at 600 (sigma's probe, memory). Now
// one entry keeps a share of its stripe (2,048 pieces), the appended rows paint
// over the pieces in one pass, and the first cuts that turn the monotonic
// chain non-monotonic keep the entry (its rows are all bounded), so every cut
// is planned from the entry and costs its own rows: linear in the cut, never
// in the chain.

const (
	growCap   = 128 // the entry's piece cap in this test (headPieceCap; bounds twice that), standing in for the real one
	growSmall = 24  // cuts: below the cap
	growLarge = 72  // cuts: past it, the shape that hit the former 512-piece cliff
	growK     = 3   // cuts per sample
)

// growCut runs cut k of the grow shape on the node and the relationship in
// one GraphTx, as sigma's writer does.
func growCut(tb testing.TB, g *Core, n, r appendEntity, k int) {
	tb.Helper()
	to := n.base + types.Instant(1000*(k+1))
	inTx(tb, g, func(tx *GraphTx) error {
		if err := n.setTx(tx, n.base, to, k); err != nil {
			return err
		}
		return r.setTx(tx, r.base, to, k)
	})
}

// growRound is one cut with sigma's reads around it: both timelines before
// and after the writes.
func growRound(tb testing.TB, g *Core, n, r appendEntity, k int) {
	tb.Helper()
	n.timeline(tb, g)
	r.timeline(tb, g)
	growCut(tb, g, n, r, k)
	n.timeline(tb, g)
	r.timeline(tb, g)
}

// growGraph is a graph whose node and relationship have seen `cuts` cuts.
func growGraph(tb testing.TB, be r10Backend, cuts int) (*Core, appendEntity, appendEntity) {
	tb.Helper()
	g, err := New(be.cfg())
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = g.Close() })
	g.headPieceCap, g.headBoundCap = growCap, 2*growCap // a short chain crosses the cap
	n := newCutEntity(tb, g, false, true)
	r := newCutEntity(tb, g, true, true)
	for k := 1; k <= cuts; k++ {
		growRound(tb, g, n, r, k)
	}
	return g, n, r
}

// growCost is the cheapest of five samples of growK rounds on a graph at
// `cuts` cuts: heap allocations and nanoseconds per cut, and the history
// reads of two or more rows (a fold or a correction read from the history;
// the entry's point reads report one row each) over all samples. Samples are
// 5 ms apart: a post-commit burst (badger, -race) outlasts back-to-back ones.
func growCost(tb testing.TB, g *Core, n, r appendEntity, cuts int) (allocs, ns float64, folds int) {
	tb.Helper()
	g.historyRead = func(rows int) {
		if rows > 1 {
			folds++
		}
	}
	defer func() { g.historyRead = nil }()
	allocs, ns = math.Inf(1), math.Inf(1)
	for i := range 5 {
		if i > 0 {
			time.Sleep(5 * time.Millisecond)
		}
		var dur time.Duration
		m := mallocs(func() {
			start := time.Now()
			for j := 1; j <= growK; j++ {
				growRound(tb, g, n, r, cuts+j)
			}
			dur = time.Since(start)
		})
		cuts += growK
		allocs = min(allocs, float64(m)/growK)
		ns = min(ns, float64(dur.Nanoseconds())/growK)
	}
	return allocs, ns, folds
}

// growSegments is the timeline of e as (from, to, version) triples, read
// from the entry (fold false) or by a fold of the chain (fold true:
// headCacheOff, the differential oracle's switch).
func growSegments(tb testing.TB, g *Core, e appendEntity, fold bool) []string {
	tb.Helper()
	g.headCacheOff = fold
	defer func() { g.headCacheOff = false }()
	pin, err := g.Temporal.CommittedTx()
	if err != nil {
		tb.Fatal(err)
	}
	var out []string
	if e.rel {
		segs, err := g.Temporal.RelEffectiveTimeline(e.rid, pin)
		if err != nil {
			tb.Fatal(err)
		}
		for _, s := range segs {
			out = append(out, fmt.Sprintf("[%d,%d) v%d", s.ValidFrom, s.ValidTo, s.Rel.Version()))
		}
		return out
	}
	segs, err := g.Temporal.NodeEffectiveTimeline(e.nid, pin)
	if err != nil {
		tb.Fatal(err)
	}
	for _, s := range segs {
		out = append(out, fmt.Sprintf("[%d,%d) v%d", s.ValidFrom, s.ValidTo, s.Node.Version()))
	}
	return out
}

// TestHeadGrow87_NoCliffPastFormerCap: on memory and badger, the grow shape
// at growLarge cuts (past growSmall, on the head entry under growCap) costs per appended
// row what it costs at growSmall cuts — a cut appends one row per piece, so
// the cost per cut is linear in the cut and the cost per row flat — and no
// cut of either sample reads the history (row 88: reads and writes after a
// grow write are planned from the entry). Before the fix the large sample
// folded and copied a chain of growLarge²/2 rows per read and per write.
func TestHeadGrow87_NoCliffPastFormerCap(t *testing.T) {
	// The real cap covers ai-soc's deepest grow chain (553 cuts, whole BA
	// 2026-10-20); the test runs the same path under growCap.
	if headMaxEntryPieces < 1024 || headMaxEntryBounds < 2*headMaxEntryPieces {
		t.Fatalf("head entry caps %d pieces / %d bounds: below a 1,024-cut grow chain", headMaxEntryPieces, headMaxEntryBounds)
	}
	for _, be := range r10Backends() {
		t.Run(be.name, func(t *testing.T) {
			g, n, r := growGraph(t, be, growSmall)
			// The entry's answer is the fold's (both entities, at the small
			// chain: a fold of growLarge²/2 rows is what this test retires).
			for _, e := range []appendEntity{n, r} {
				got, want := growSegments(t, g, e, false), growSegments(t, g, e, true)
				if fmt.Sprint(got) != fmt.Sprint(want) {
					t.Fatalf("rel=%v: head entry %v\nfold %v", e.rel, got, want)
				}
				if len(got) != growSmall {
					t.Fatalf("rel=%v: %d segments at %d cuts, want one per cut", e.rel, len(got), growSmall)
				}
			}
			a, na, fa := growCost(t, g, n, r, growSmall)
			for k := growSmall + 5*growK + 1; k <= growLarge; k++ {
				growRound(t, g, n, r, k)
			}
			b, nb, fb := growCost(t, g, n, r, growLarge)
			perRowA, perRowB := a/float64(growSmall), b/float64(growLarge)
			t.Logf("allocs per cut %.0f / %.0f (per appended row %.1f / %.1f), ms per cut %.2f / %.2f at %d / %d cuts; history reads %d / %d",
				a, b, perRowA, perRowB, na/1e6, nb/1e6, growSmall, growLarge, fa, fb)
			if fa != 0 || fb != 0 {
				t.Errorf("history read during a cut planned from the head entry: %d at %d cuts, %d at %d cuts", fa, growSmall, fb, growLarge)
			}
			// Linear in the cut: the allocations per appended row stay flat
			// (badger's own goroutines add a few objects per cut).
			if perRowB > perRowA*1.5+1 {
				t.Errorf("allocations per appended row grow with the chain: %.1f at %d cuts, %.1f at %d (x%.1f)", perRowA, growSmall, perRowB, growLarge, perRowB/perRowA)
			}
			if nb/float64(growLarge) > na/float64(growSmall)*4 {
				t.Errorf("time per appended row grows with the chain: %.1f µs at %d cuts, %.1f µs at %d",
					na/1e3/float64(growSmall), growSmall, nb/1e3/float64(growLarge), growLarge)
			}
		})
	}
}

// TestHeadGrow87_EntrySurvivesFirstCuts: the first cuts of the grow shape
// keep the entry (row 88): cut 1 is planned from the history (no entry yet:
// a chain of one row never fills one) and leaves it behind; cut 2 keeps the
// chain monotonic; cut 3 makes it non-monotonic (its first row starts below
// the newest row) and the entry switches to the own-bounds arm instead of
// being dropped (every row is bounded). From cut 2 on no cut or read touches
// the history, on memory and badger, and the entry's answer is the fold's
// after every cut.
func TestHeadGrow87_EntrySurvivesFirstCuts(t *testing.T) {
	for _, be := range r10Backends() {
		t.Run(be.name, func(t *testing.T) {
			g, err := New(be.cfg())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = g.Close() })
			n := newCutEntity(t, g, false, true)
			r := newCutEntity(t, g, true, true)
			growRound(t, g, n, r, 1)
			for k := 2; k <= 8; k++ {
				folds := 0
				g.historyRead = func(rows int) {
					if rows > 1 {
						folds++
					}
				}
				growRound(t, g, n, r, k)
				g.historyRead = nil
				if folds != 0 {
					t.Errorf("cut %d: %d history reads; the cut and its reads should be planned from the head entry", k, folds)
				}
				for _, e := range []appendEntity{n, r} {
					got, want := growSegments(t, g, e, false), growSegments(t, g, e, true)
					if fmt.Sprint(got) != fmt.Sprint(want) {
						t.Fatalf("cut %d rel=%v: head entry %v\nfold %v", k, e.rel, got, want)
					}
				}
			}
		})
	}
}
