package graph_test

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestReadBudgetPublicAsOfTailAdmission(t *testing.T) {
	for _, kind := range []string{"node", "rel"} {
		t.Run(kind, func(t *testing.T) {
			bs, err := badger.New(badger.Config{InMemory: true, MaxPendingWrites: -1, FlushInterval: 1<<63 - 1})
			if err != nil {
				t.Fatal(err)
			}
			g, err := graph.New(graph.Config{Store: bs})
			if err != nil {
				t.Fatal(err)
			}
			defer g.Close()
			n, err := g.Nodes().Add(t.Context(), []string{"wanted"}, map[string]any{"v": int64(0)})
			if err != nil {
				t.Fatal(err)
			}
			z, err := g.Nodes().Add(t.Context(), []string{"endpoint"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels().Add(t.Context(), "R", n, z, map[string]any{"v": int64(0)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes().Update(t.Context(), n.ID(), map[string]any{"v": int64(1)}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Rels().Update(t.Context(), r.ID(), map[string]any{"v": int64(1)}); err != nil {
				t.Fatal(err)
			}
			pin, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes().Update(t.Context(), n.ID(), map[string]any{"v": int64(2)}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Rels().Update(t.Context(), r.ID(), map[string]any{"v": int64(2)}); err != nil {
				t.Fatal(err)
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			// Preserve this test's reverse-walk boundary: an exact rewrite
			// invalidates the disk seek proof without changing the answer.
			if kind == "node" {
				h, e := bs.GetNodeVersion(n.ID(), 0)
				if e != nil {
					t.Fatal(e)
				}
				err = bs.PutNodeVersion(n.ID(), 0, h)
			} else {
				h, e := bs.GetRelVersion(r.ID(), 0)
				if e != nil {
					t.Fatal(e)
				}
				err = bs.PutRelVersion(r.ID(), 0, h)
			}
			if err := errors.Join(err, bs.Flush()); err != nil {
				t.Fatal(err)
			}
			read := func(b store.ReadBudget) (bool, int64, error) {
				ctx, _ := store.WithReadBudget(t.Context(), b)
				before := bs.HistoryMetaDecodesForTest()
				var present bool
				var err error
				if kind == "node" {
					var got *types.Node
					got, err = g.Temporal().NodeAsOfCtx(ctx, n.ID(), pin)
					present = got != nil
					if present && (got.ID() != n.ID() || got.Version() != 1 || got.PropertiesMap()["v"] != int64(1)) {
						t.Fatalf("wrong historical node: %v", got)
					}
				} else {
					var got *types.Relationship
					got, err = g.Temporal().RelAsOfCtx(ctx, r.ID(), pin)
					present = got != nil
					if present && (got.ID() != r.ID() || got.Version() != 1 || got.PropertiesMap()["v"] != int64(1)) {
						t.Fatalf("wrong historical relationship: %v", got)
					}
				}
				return present, bs.HistoryMetaDecodesForTest() - before, err
			}
			got, decodes, err := read(store.ReadBudget{DecodedBytes: 1})
			var refusal *store.ReadBudgetError
			if !errors.As(err, &refusal) || refusal.Limit != "decoded bytes" || got || decodes != 0 {
				t.Fatalf("limited: row %v native tail decodes %d err %v", got, decodes, err)
			}
			got, decodes, err = read(store.ReadBudget{})
			if err != nil || !got || decodes != 2 {
				t.Fatalf("exact control: row %v native tail decodes %d err %v", got, decodes, err)
			}
		})
	}
}

// New current rows leave history presence cold. A point read must not build a
// global presence table by walking unrelated pending current rows.
func TestReadBudgetPublicAsOfColdPresenceScope(t *testing.T) {
	for _, kind := range []string{"node", "rel"} {
		t.Run(kind, func(t *testing.T) {
			bs, err := badger.New(badger.Config{InMemory: true, MaxPendingWrites: -1, FlushInterval: 1<<63 - 1})
			if err != nil {
				t.Fatal(err)
			}
			g, err := graph.New(graph.Config{Store: bs})
			if err != nil {
				t.Fatal(err)
			}
			defer g.Close()
			n, err := g.Nodes().Add(t.Context(), []string{"wanted"}, map[string]any{"v": int64(7)})
			if err != nil {
				t.Fatal(err)
			}
			z, err := g.Nodes().Add(t.Context(), []string{"endpoint"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels().Add(t.Context(), "R", n, z, map[string]any{"v": int64(7)})
			if err != nil {
				t.Fatal(err)
			}
			for range 64 {
				if _, err := g.Nodes().Add(t.Context(), []string{"other"}, nil); err != nil {
					t.Fatal(err)
				}
			}
			pin, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			ctx, m := store.WithReadBudget(t.Context(), store.ReadBudget{Candidates: 8})
			visits, stop := badger.CountPendingKeysVisitedForTest()
			defer stop()
			var exact bool
			if kind == "node" {
				got, e := g.Temporal().NodeAsOfCtx(ctx, n.ID(), pin)
				err = e
				exact = got != nil && got.ID() == n.ID() && got.Version() == 0 && got.PropertiesMap()["v"] == int64(7)
			} else {
				got, e := g.Temporal().RelAsOfCtx(ctx, r.ID(), pin)
				err = e
				exact = got != nil && got.ID() == r.ID() && got.Version() == 0 && got.PropertiesMap()["v"] == int64(7)
			}
			stats := bs.HistoryPresenceStats()
			if err != nil || !exact || visits() != 0 || stats.NodesBuilt || stats.RelsBuilt {
				t.Fatalf("cold exact control: row %v pending visits %d presence %+v work %+v err %v", exact, visits(), stats, m.Work(), err)
			}
		})
	}
}
