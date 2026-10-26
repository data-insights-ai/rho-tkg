package core

import (
	"fmt"
	"math"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TXSNAP's budget (lesson 89) on sigma's session stamp (sigma 26 Oct,
// overview_graph.go, v0.11.73): every WriteDerived looks its Session node up
// outside the transaction (ByLabelAndProperties, Limit 1), writes its Derived
// facts, and in the same Tx stamps the Session node once with
// SetNodeVersionInterval over the current row's own interval (its fixed start,
// open end) with new ov_ properties. The node gains one version per stamp, a
// chain of thousands over a day's run. A stamp is planned from the head entry,
// so it costs the same at 10 and 1,000 versions: neither the correction nor the
// Tx's rollback snapshot (nodeHeadTop) reads the history. Before sigma's
// v0.11.71 a shared rows node took this path per paged head; sigma's profile
// (58 % of CPU) was the snapshot's copy of the whole chain, which a missing
// entry costs (60-80x at 1,000 versions, measured with headWriteOff).
// Mutation: headWriteOff (corrections and snapshots read the whole history)
// breaks the budget and the history-read count.

// sessionStamp runs one WriteDerived of sigma's shape: the lookup, three
// Derived facts and the stamp, in one Tx.
func sessionStamp(tb testing.TB, g *Core, sid types.NodeID, k int) {
	tb.Helper()
	found, err := g.Nodes.ByLabelAndProperties("Session", map[string]any{"ov_namespace": "n", "ov_session": "s"}, storepkg.QueryOpts{Limit: 1})
	if err != nil || len(found) != 1 {
		tb.Fatalf("session lookup: %d nodes, %v", len(found), err)
	}
	inTx(tb, g, func(tx *GraphTx) error {
		for j := range 3 {
			if _, err := tx.AddNode([]string{"Derived"}, map[string]any{"ov_k": int64(3*k + j)}); err != nil {
				return err
			}
		}
		node, err := tx.GetNode(sid)
		if err != nil {
			return err
		}
		from, to, _ := node.ValidRange()
		_, err = tx.SetNodeVersionInterval(sid, from, to, map[string]any{
			"ov_namespace": "n", "ov_session": "s", "ov_through": int64(k), "ov_program": "p", "ov_coverage": int64(k % 7)})
		return err
	})
}

// sessionChain renders the node's current row and history (version, validity,
// ov_through) for a before/after comparison.
func sessionChain(tb testing.TB, g *Core, sid types.NodeID) string {
	tb.Helper()
	cur, err := g.getCurrentNode(sid)
	if err != nil {
		tb.Fatal(err)
	}
	history, err := g.getNodeHistory(sid)
	if err != nil {
		tb.Fatal(err)
	}
	row := func(n *types.Node) string {
		from, to, _ := n.ValidRange()
		v, _ := n.GetProperty("ov_through")
		return fmt.Sprintf("v%d [%d,%d) %v", n.Version(), from, to, v)
	}
	out := "current " + row(cur)
	for _, h := range history {
		out += "; " + row(h)
	}
	return out
}

func TestTxSnapSessionStampFlatInChainLength(t *testing.T) {
	if testing.Short() && !isRaceEnabled() {
		t.Skip("long tier: release gate")
	}
	const short, long = 10, 1000
	for _, be := range r10AllBackends(t) {
		t.Run(be.name, func(t *testing.T) {
			cost := map[int]float64{}
			for _, chain := range []int{short, long} {
				g, err := New(be.cfg())
				if err != nil {
					t.Fatal(err)
				}
				s, err := g.Nodes.Add(t.Context(), []string{"Session"}, map[string]any{
					"ov_namespace": "n", "ov_session": "s", "tkg_valid_from": int64(1_000_000)})
				if err != nil {
					t.Fatal(err)
				}
				sid := s.ID()
				k := 1
				for ; k < chain; k++ {
					sessionStamp(t, g, sid, k)
				}
				if cur, err := g.Nodes.Get(t.Context(), sid); err != nil || int(cur.Version()) < chain-1 {
					t.Fatalf("fixture: the session node's version is %v (%v), want a chain of %d", cur.Version(), err, chain)
				}
				reads := 0
				g.historyRead = func(rows int) {
					if rows > 1 {
						reads++
					}
				}
				best := math.Inf(1)
				for i := range 5 {
					kk := k + i
					best = min(best, float64(mallocs(func() { sessionStamp(t, g, sid, kk) })))
				}
				g.historyRead = nil
				cost[chain] = best
				if reads != 0 {
					t.Errorf("chain %d: %d history reads of two or more rows over 5 stamps, want 0", chain, reads)
				}
				// A rolled-back stamp leaves the chain as it was: the trim point
				// alone restores it (tiered and sharded rewrite the pre-tx rows).
				before := sessionChain(t, g, sid)
				tx, err := g.BeginTx()
				if err != nil {
					t.Fatal(err)
				}
				if _, err := tx.SetNodeVersionInterval(sid, 1_000_000, math.MaxInt64, map[string]any{"ov_through": int64(-1)}); err != nil {
					t.Fatal(err)
				}
				if _, err := tx.SetNodeVersionInterval(sid, 2_000_000, 3_000_000, map[string]any{"ov_through": int64(-2)}); err != nil {
					t.Fatal(err)
				}
				if err := tx.Rollback(); err != nil {
					t.Fatal(err)
				}
				if after := sessionChain(t, g, sid); after != before {
					t.Errorf("chain %d: rollback left\n%s\nwant\n%s", chain, after, before)
				}
				_ = g.Close()
			}
			t.Logf("allocs per WriteDerived: %.0f at %d versions, %.0f at %d (x%.2f)", cost[short], short, cost[long], long, cost[long]/cost[short])
			if cost[long] > 1.5*cost[short] {
				t.Errorf("a stamp at %d versions costs %.0f allocs, over 1.5x the %.0f at %d", long, cost[long], cost[short], short)
			}
		})
	}
}

// TestTxSnapRelStampFlatInChainLength is the session stamp's shape on a
// relationship (snapshotCurrentRelLocked): one SetRelVersionInterval per Tx
// over the current row's own interval costs the same at 10 and 1,000
// versions, without a history read, and a rolled-back stamp leaves the chain
// as it was. Mutation: the snapshot's trim point gated on the rollback trim
// capability (tiered and sharded copy the chain).
func TestTxSnapRelStampFlatInChainLength(t *testing.T) {
	if testing.Short() && !isRaceEnabled() {
		t.Skip("long tier: release gate")
	}
	const short, long = 10, 1000
	for _, be := range r10AllBackends(t) {
		t.Run(be.name, func(t *testing.T) {
			cost := map[int]float64{}
			for _, chain := range []int{short, long} {
				g, err := New(be.cfg())
				if err != nil {
					t.Fatal(err)
				}
				e := newCutEntity(t, g, true, false)
				stamp := func(k int) {
					inTx(t, g, func(tx *GraphTx) error {
						r, err := tx.GetRelationship(e.rid)
						if err != nil {
							return err
						}
						from, to, _ := r.ValidRange()
						return e.setTx(tx, from, to, k)
					})
				}
				k := 1
				for ; k < chain; k++ {
					stamp(k)
				}
				reads := 0
				g.historyRead = func(rows int) {
					if rows > 1 {
						reads++
					}
				}
				best := math.Inf(1)
				for i := range 5 {
					kk := k + i
					best = min(best, float64(mallocs(func() { stamp(kk) })))
				}
				g.historyRead = nil
				cost[chain] = best
				if reads != 0 {
					t.Errorf("chain %d: %d history reads of two or more rows over 5 stamps, want 0", chain, reads)
				}
				rows := func() string {
					cur, err := g.getCurrentRelationship(e.rid)
					if err != nil {
						t.Fatal(err)
					}
					history, err := g.getRelHistory(e.rid)
					if err != nil {
						t.Fatal(err)
					}
					w := func(r *types.Relationship) any { v, _ := r.GetProperty("w"); return v }
					out := fmt.Sprint(cur.Version(), w(cur))
					for _, h := range history {
						out += fmt.Sprint(";", h.Version(), w(h))
					}
					return out
				}
				before := rows()
				tx, err := g.BeginTx()
				if err != nil {
					t.Fatal(err)
				}
				if err := e.setTx(tx, e.base, math.MaxInt64, -1); err != nil {
					t.Fatal(err)
				}
				if err := tx.Rollback(); err != nil {
					t.Fatal(err)
				}
				if after := rows(); after != before {
					t.Errorf("chain %d: rollback left %s, want %s", chain, after, before)
				}
				_ = g.Close()
			}
			t.Logf("allocs per rel stamp: %.0f at %d versions, %.0f at %d (x%.2f)", cost[short], short, cost[long], long, cost[long]/cost[short])
			if cost[long] > 1.5*cost[short] {
				t.Errorf("a rel stamp at %d versions costs %.0f allocs, over 1.5x the %.0f at %d", long, cost[long], cost[short], short)
			}
		})
	}
}
