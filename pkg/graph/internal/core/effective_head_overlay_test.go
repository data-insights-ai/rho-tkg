package core

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestHeadOverlay_MatchesFold drives the fold's head facts and headOverlay directly
// with chains the doors do not produce today (an appended row recorded at a
// replaced row's TxTo or below the newest TxFrom, a bounded row after an open
// newest row, an appended row starting before the newest one, rows with a
// delete, void rows): an entry must equal the fold, and wherever the overlay
// answers, its pieces must equal the fold of the grown chain.
func TestHeadOverlay_MatchesFold(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	if !g.bitemporalMigrated {
		t.Fatal("memory store not migrated")
	}
	rng := rand.New(rand.NewPCG(68, 0x0e))
	id := types.NodeID(1 << 40)
	grid := func() types.Instant { return types.Instant(100 * (1 + rng.IntN(12))) }
	row := func(v uint32, tx types.Instant) *types.Node {
		n := types.NewNode(id, 0, nil)
		n.SetVersion(v)
		tm := &types.TemporalMetadata{ValidFrom: grid(), TxFrom: tx}
		if rng.IntN(3) > 0 {
			tm.ValidTo = tm.ValidFrom + types.Instant(100*(1+rng.IntN(4)))
		}
		if rng.IntN(12) == 0 {
			tm.Void = true
			if tm.ValidTo == 0 && rng.IntN(2) == 0 { // an open void row: no door writes one (excise is bounded)
				tm.ValidTo = tm.ValidFrom + 100
			}
		}
		n.SetTemporal(tm)
		return n
	}
	toHead := func(ps []effPiece[*types.Node]) []headPiece {
		out := make([]headPiece, len(ps))
		for i, p := range ps {
			out[i] = headPiece{p.from, p.to, p.row.Version()}
		}
		return out
	}
	answered, declined, entries, corrections := 0, 0, 0, 0
	for iter := range 20000 {
		n := 2 + rng.IntN(5)
		chain := make([]*types.Node, 0, n+3)
		tx := types.Instant(10_000)
		for v := range n {
			tx += types.Instant(1 + rng.IntN(3))
			chain = append(chain, row(uint32(v), tx))
		}
		// A replacing write: a row ended at a later row's TxFrom.
		if rng.IntN(3) == 0 {
			i := rng.IntN(n - 1)
			chain[i].Temporal().TxTo = chain[i+1+rng.IntN(n-1-i)].Temporal().TxFrom
		}
		if rng.IntN(15) == 0 {
			chain[rng.IntN(n)].Temporal().DeletedAt = tx
		}
		// A chain stored before backlog 38: two rows share a version (a
		// segment cannot name its winner by version there).
		shared := rng.IntN(20) == 0
		if shared {
			i := 1 + rng.IntN(n-1)
			chain[i].SetVersion(chain[i-1].Version())
		}
		pin := tx
		for _, r := range chain {
			pin = max(pin, r.Temporal().TxTo)
		}
		cur := chain[len(chain)-1]
		var facts headFacts
		fold := effectivePiecesFacts(g, nodeKernel, chain, pin, &facts)
		e := headFromFold(1, cur, fold, &facts)
		if e == nil {
			continue
		}
		if shared {
			t.Fatalf("iter %d: an entry for a chain with a shared version%s", iter, headRows(chain))
		}
		entries++
		if got, want := e.pieces, toHead(fold); !slices.Equal(got, want) {
			t.Fatalf("iter %d: entry %v, fold %v", iter, got, want)
		}
		corrections += checkHeadCorrection(t, g, rng, e, chain, cur, iter)
		var appended []*types.Node
		for k := range 1 + rng.IntN(3) {
			atx := tx + types.Instant(1+k)
			switch rng.IntN(6) {
			case 0:
				atx = e.maxTxTo // at a replaced row's TxTo
			case 1:
				atx = e.maxTx - types.Instant(rng.IntN(2)) // at or below the newest TxFrom
			}
			appended = append(appended, row(uint32(n+k), atx))
		}
		grown := append(slices.Clone(chain), appended...)
		gpin := pin
		for _, r := range appended {
			gpin = max(gpin, r.Temporal().TxFrom)
		}
		ov := headOverlay(g, nodeKernel, e, appended)
		if ov == nil {
			declined++
			continue
		}
		answered++
		if got, want := ov.pieces, toHead(g.nodeEffectivePieces(grown, gpin)); !slices.Equal(got, want) {
			t.Fatalf("iter %d: overlay %v, fold %v\nchain%s\nappended%s", iter, got, want, headRows(chain), headRows(appended))
		}
		// The overlay keeps the correction facts (backlog 68b) the fold of
		// the grown chain gathers.
		var gf headFacts
		effectivePiecesFacts(g, nodeKernel, grown, gpin, &gf)
		if !gf.ok || !slices.Equal(ov.bounds, gf.bounds) || ov.open != gf.open || ov.maxVer != gf.maxVer {
			t.Fatalf("iter %d: overlay facts bounds %v open %+v maxVer %d, fold %v %+v %d (ok %v)\nchain%s\nappended%s", iter,
				ov.bounds, ov.open, ov.maxVer, gf.bounds, gf.open, gf.maxVer, gf.ok, headRows(chain), headRows(appended))
		}
	}
	t.Logf("%d entries, %d overlays checked, %d declined, %d corrections planned", entries, answered, declined, corrections)
	if answered < 1000 || declined < 1000 || corrections < 1000 {
		t.Fatalf("generator coverage: %d answered, %d declined, %d corrections", answered, declined, corrections)
	}
}

// checkHeadCorrection plans random corrections of chain from its entry e
// (chainFromHead, backlog 68b) and requires what the history read computes
// (temporal_cascade.go): the resumption source and end, the correction
// segments (bounds, base, gap) for a correction and an excise, and the
// newest open belief. Returns the corrections checked.
func checkHeadCorrection(t *testing.T, g *Core, rng *rand.Rand, e *headEntry, chain []*types.Node, cur *types.Node, iter int) int {
	t.Helper()
	pre := versionOrdered(slices.Clone(chain))
	byVersion := map[uint32]*types.Node{}
	for _, r := range chain {
		byVersion[r.Version()] = r
	}
	read := func(v uint32) (*types.Node, error) {
		if r, ok := byVersion[v]; ok {
			return r, nil
		}
		return nil, storepkg.ErrVersionNotFound
	}
	var open openBelief
	var top uint32
	for _, r := range pre {
		top = max(top, r.Version())
		if _, end := g.nodeOwnBounds(r); end == 0 && !isVoidRow(r) {
			open.note(beliefTx(r.Temporal()), r.Version())
		}
	}
	checked := 0
	for range 3 {
		vf := types.Instant(50 * (1 + rng.IntN(30)))
		var vt types.Instant
		if rng.IntN(4) > 0 {
			vt = vf + types.Instant(50*(1+rng.IntN(10)))
		}
		for _, void := range []bool{false, true} {
			hc, ok := chainFromHead(g, e, cur, read, vf, vt, 0, void)
			if !ok {
				t.Fatalf("iter %d: no plan from a valid entry", iter)
			}
			want := fmt.Sprintf("max %d open %+v", top, open)
			got := fmt.Sprintf("max %d open %+v", hc.maxVersion, hc.open)
			if vt != 0 {
				if src, err := g.resolveNodeVersionAt(slices.Clone(pre), vt); err == nil {
					want += fmt.Sprintf(" src v%d end %d", src.Version(), nodeResumptionEnd(g, pre, vt))
				}
				if hc.hasSrc {
					got += fmt.Sprintf(" src v%d end %d", hc.src.Version(), hc.srcEnd)
				}
			}
			segs, err := g.nodeCorrectionSegments(pre, cur, vf, vt, void)
			if err != nil {
				t.Fatal(err)
			}
			for _, s := range segs {
				want += fmt.Sprintf(" [%d,%d) v%d gap=%v", s.from, s.to, s.base.Version(), s.gap)
			}
			for _, s := range hc.segments {
				got += fmt.Sprintf(" [%d,%d) v%d gap=%v", s.from, s.to, s.base.Version(), s.gap)
			}
			if got != want {
				t.Fatalf("iter %d: correction [%d,%d) void=%v from the entry:\n %s\nfrom the chain:\n %s\nchain%s", iter, vf, vt, void, got, want, headRows(chain))
			}
			checked++
		}
	}
	// The current-slot choice against the chain's open belief: an appended
	// open row recorded before it (no door does: appended rows are the
	// newest beliefs) must not take the slot, one recorded after it must.
	for _, dtx := range []types.Instant{-1, 1} {
		r := types.NewNode(cur.ID(), 0, nil)
		r.SetVersion(top + 1)
		r.SetTemporal(&types.TemporalMetadata{ValidFrom: 100, TxFrom: open.tx + dtx})
		var want *types.Node
		for _, x := range append(slices.Clone(pre), r) {
			if _, end := g.nodeOwnBounds(x); end == 0 && !isVoidRow(x) && (want == nil || nodeBeliefNewerThan(x, want)) {
				want = x
			}
		}
		got := headNewCurrent(g, nodeKernel, e.open, []*types.Node{r})
		if (want == r) != (got == r) {
			t.Fatalf("iter %d: appended open row at tx %d: takes the slot %v from the entry, %v from the chain%s", iter, open.tx+dtx, got == r, want == r, headRows(chain))
		}
		checked++
	}
	return checked
}

func headRows(rows []*types.Node) string {
	s := ""
	for _, r := range rows {
		tm := r.Temporal()
		s += fmt.Sprintf(" v%d[%d,%d) tx %d-%d del %d void %v;", r.Version(), tm.ValidFrom, tm.ValidTo, tm.TxFrom, tm.TxTo, tm.DeletedAt, tm.Void)
	}
	return s
}
