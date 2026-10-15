package graph_test

import (
	"context"
	"slices"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestUntimedRowsStartAtTxFrom (backlog 62, phase B): a row without a
// valid-from is valid from its TxFrom — for a create through Add, a batch, an
// ingest session and an AddWithTx backfill (TxFrom before the mint), and for a
// later version — on every valid-time door, on all four backends. The commit
// clock is advanced 60 s past the wall clock first, the burst sigma-tkgd hit:
// the ID's mint instant then lies 60 s before TxFrom, so a door that still
// starts a create at its mint answers at TxFrom-1. Break cases per entity:
// TxFrom-1 (not valid), TxFrom (valid), the timeline's first ValidFrom, the
// Allen interval start.
func TestUntimedRowsStartAtTxFrom(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, b := range allStoreBackendsWith(func(c *graphpkg.Config) { c.AllowTxBackfill = true }) {
		t.Run(b.name, func(t *testing.T) {
			t.Parallel()
			g := b.open(t)
			now, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Temporal().AdvanceClock(now + 60_000); err != nil {
				t.Fatal(err)
			}

			var nodes []types.NodeID
			add := func(n *types.Node, err error) *types.Node {
				t.Helper()
				if err != nil {
					t.Fatal(err)
				}
				nodes = append(nodes, n.ID())
				return n
			}
			a := add(g.Nodes().Add(ctx, []string{"N"}, map[string]any{"k": "add"}))
			add(g.Nodes().AddWithTx(ctx, []string{"N"}, map[string]any{"k": "backfill"}, now-30_000))
			bb, err := g.Batch().New()
			if err != nil {
				t.Fatal(err)
			}
			bn := add(bb.AddNode([]string{"N"}, map[string]any{"k": "batch"}))
			if _, err := bb.Execute(); err != nil {
				t.Fatal(err)
			}
			s, err := g.Ingest().NewSession(ingest.IngestOptions{Sync: true})
			if err != nil {
				t.Fatal(err)
			}
			add(s.AddNode([]string{"N"}, map[string]any{"k": "ingest"}))
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels().Add(ctx, "R", a, bn, nil)
			if err != nil {
				t.Fatal(err)
			}
			rb, err := g.Rels().AddWithTx(ctx, "R", bn, a, nil, now-20_000)
			if err != nil {
				t.Fatal(err)
			}
			rels := []types.RelID{r.ID(), rb.ID()}

			pin, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range nodes {
				n, err := g.Nodes().Get(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				tx := n.Temporal().TxFrom
				k, _ := n.GetProperty("k")
				if n.Temporal().ValidFrom != 0 || tx == 0 || tx == n.ID().MintInstant() {
					t.Fatalf("%v: setup: want an untimed row whose TxFrom (%d) differs from its mint (%d)", k, tx, n.ID().MintInstant())
				}
				for _, c := range []struct {
					at   types.Instant
					want bool
				}{{tx - 1, false}, {tx, true}} {
					_, err := g.Temporal().NodeAt(id, c.at)
					if got := err == nil; got != c.want {
						t.Errorf("%v NodeAt(TxFrom%+d) valid = %v (%v), want %v", k, c.at-tx, got, err, c.want)
					}
					_, err = g.Temporal().NodeAtTx(id, c.at, pin)
					if got := err == nil; got != c.want {
						t.Errorf("%v NodeAtTx(TxFrom%+d) valid = %v (%v), want %v", k, c.at-tx, got, err, c.want)
					}
					at, err := g.Temporal().NodesAt(c.at)
					if err != nil {
						t.Fatal(err)
					}
					if got := containsNode(at, id); got != c.want {
						t.Errorf("%v NodesAt(TxFrom%+d) lists it = %v, want %v", k, c.at-tx, got, c.want)
					}
					atTx, err := g.Temporal().NodesAtTx(c.at, pin)
					if err != nil {
						t.Fatal(err)
					}
					if got := containsNode(atTx, id); got != c.want {
						t.Errorf("%v NodesAtTx(TxFrom%+d) lists it = %v, want %v", k, c.at-tx, got, c.want)
					}
					byLabel, err := g.Nodes().ByLabel("N", storepkg.QueryOpts{ValidAt: c.at})
					if err != nil {
						t.Fatal(err)
					}
					if got := containsNode(byLabel, id); got != c.want {
						t.Errorf("%v ByLabel(ValidAt TxFrom%+d) lists it = %v, want %v", k, c.at-tx, got, c.want)
					}
					if got := g.Temporal().NodeMatchesValidTime(n, storepkg.QueryOpts{ValidAt: c.at}); got != c.want {
						t.Errorf("%v NodeMatchesValidTime(TxFrom%+d) = %v, want %v", k, c.at-tx, got, c.want)
					}
				}
				segs, err := g.Temporal().NodeEffectiveTimeline(id, pin)
				if err != nil || len(segs) == 0 || segs[0].ValidFrom != tx {
					t.Errorf("%v NodeEffectiveTimeline first ValidFrom = %v (%v), want TxFrom %d", k, segs, err, tx)
				}
			}
			for _, id := range rels {
				rel, err := g.Rels().Get(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				tx := rel.Temporal().TxFrom
				if rel.Temporal().ValidFrom != 0 || tx == rel.ID().MintInstant() {
					t.Fatalf("rel setup: want an untimed row whose TxFrom (%d) differs from its mint (%d)", tx, rel.ID().MintInstant())
				}
				for _, c := range []struct {
					at   types.Instant
					want bool
				}{{tx - 1, false}, {tx, true}} {
					_, err := g.Temporal().RelAt(id, c.at)
					if got := err == nil; got != c.want {
						t.Errorf("RelAt(TxFrom%+d) valid = %v (%v), want %v", c.at-tx, got, err, c.want)
					}
					_, err = g.Temporal().RelAtTx(id, c.at, pin)
					if got := err == nil; got != c.want {
						t.Errorf("RelAtTx(TxFrom%+d) valid = %v (%v), want %v", c.at-tx, got, err, c.want)
					}
					byType, err := g.Rels().ByType("R", storepkg.QueryOpts{ValidAt: c.at})
					if err != nil {
						t.Fatal(err)
					}
					if got := slices.ContainsFunc(byType, func(x *types.Relationship) bool { return x.ID() == id }); got != c.want {
						t.Errorf("ByType(ValidAt TxFrom%+d) lists it = %v, want %v", c.at-tx, got, c.want)
					}
					if got := g.Temporal().RelMatchesValidTime(rel, storepkg.QueryOpts{ValidAt: c.at}); got != c.want {
						t.Errorf("RelMatchesValidTime(TxFrom%+d) = %v, want %v", c.at-tx, got, c.want)
					}
				}
				segs, err := g.Temporal().RelEffectiveTimeline(id, pin)
				if err != nil || len(segs) == 0 || segs[0].ValidFrom != tx {
					t.Errorf("RelEffectiveTimeline first ValidFrom = %v (%v), want TxFrom %d", segs, err, tx)
				}
			}

			// The Allen classifier: an untimed start with an explicit end starts at
			// TxFrom, so it equals an explicit interval from that TxFrom.
			end := pin + 3_600_000
			e, err := g.Nodes().Add(ctx, []string{"N"}, map[string]any{"tkg_valid_to": end})
			if err != nil {
				t.Fatal(err)
			}
			eTx := e.Temporal().TxFrom
			twin, err := g.Nodes().Add(ctx, []string{"N"}, map[string]any{"tkg_valid_from": eTx, "tkg_valid_to": end})
			if err != nil {
				t.Fatal(err)
			}
			if s, _, err := g.Temporal().NodeInterval(e); err != nil || s != eTx {
				t.Errorf("NodeInterval start = %d (%v), want TxFrom %d", s, err, eTx)
			}
			if rel, err := g.Temporal().RelateNodes(e, twin); err != nil || rel != types.Equals {
				t.Errorf("RelateNodes(untimed, [TxFrom, end)) = %v (%v), want equals", rel, err)
			}
		})
	}
}

// TestOneBurstReadsConsistentlyAtOneInstant (backlog 62, the sigma-tkgd
// observation): with the commit clock ahead of the wall clock, node b is
// created, then updated, then node a is created. At every instant a valid-time
// read must order them as they were written: a, created after b's update, is
// never valid where b's update is not yet, and b's update is valid wherever a
// is. With the mint as a create's start, a was valid 60 s before b's update.
func TestOneBurstReadsConsistentlyAtOneInstant(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	forAllStoreBackends(t, func(t *testing.T, _ storeBackend, g *graphpkg.Graph) {
		now, err := g.Temporal().NowTx()
		if err != nil {
			t.Fatal(err)
		}
		b, err := g.Nodes().Add(ctx, []string{"N"}, map[string]any{"v": int64(0)})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := g.Temporal().AdvanceClock(now + 60_000); err != nil {
			t.Fatal(err)
		}
		bu, err := g.Nodes().Update(ctx, b.ID(), map[string]any{"v": int64(1)})
		if err != nil {
			t.Fatal(err)
		}
		a, err := g.Nodes().Add(ctx, []string{"N"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		update, create := bu.Temporal().TxFrom, a.Temporal().TxFrom
		for _, at := range []types.Instant{a.ID().MintInstant(), update - 1, update, create - 1, create} {
			_, aErr := g.Temporal().NodeAt(a.ID(), at)
			var v any
			if bRow, err := g.Temporal().NodeAt(b.ID(), at); err == nil {
				v, _ = bRow.GetProperty("v")
			}
			if aErr == nil && v != int64(1) {
				t.Errorf("at %d (update %+d, create %+d): a (created after b's update) is valid, b is still v=%v",
					at, at-update, at-create, v)
			}
		}
	})
}

func containsNode(ns []*types.Node, id types.NodeID) bool {
	return slices.ContainsFunc(ns, func(n *types.Node) bool { return n.ID() == id })
}
