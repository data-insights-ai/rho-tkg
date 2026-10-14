package core

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

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
			if tm.ValidTo == 0 {
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
	answered, declined, entries := 0, 0, 0
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
	}
	t.Logf("%d entries, %d overlays checked, %d declined", entries, answered, declined)
	if answered < 1000 || declined < 1000 {
		t.Fatalf("generator coverage: %d answered, %d declined", answered, declined)
	}
}

func headRows(rows []*types.Node) string {
	s := ""
	for _, r := range rows {
		tm := r.Temporal()
		s += fmt.Sprintf(" v%d[%d,%d) tx %d-%d del %d void %v;", r.Version(), tm.ValidFrom, tm.ValidTo, tm.TxFrom, tm.TxTo, tm.DeletedAt, tm.Void)
	}
	return s
}
