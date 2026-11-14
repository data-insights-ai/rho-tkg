package graph_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The public point doors must reach the same native admission boundaries for
// nodes and relationships, including a delta winner's anchor copy.
func TestReadBudgetPublicAsOfNativeAdmission(t *testing.T) {
	for _, kind := range []string{"node", "rel"} {
		for _, flushed := range []bool{false, true} {
			t.Run(kind+map[bool]string{false: "/pending", true: "/flushed"}[flushed], func(t *testing.T) {
				bs, err := badger.New(badger.Config{InMemory: true, MaxPendingWrites: -1, HistoryDeltaEncoding: true, HistoryAnchorInterval: 8})
				if err != nil {
					t.Fatal(err)
				}
				g, err := graph.New(graph.Config{Store: bs})
				if err != nil {
					t.Fatal(err)
				}
				defer g.Close()
				payload := strings.Repeat("x", 8192)
				n, err := g.Nodes().Add(t.Context(), []string{"wanted"}, map[string]any{"payload": payload, "v": int64(0)})
				if err != nil {
					t.Fatal(err)
				}
				z, err := g.Nodes().Add(t.Context(), []string{"endpoint"}, nil)
				if err != nil {
					t.Fatal(err)
				}
				r, err := g.Rels().Add(t.Context(), "R", n, z, map[string]any{"payload": payload, "v": int64(0)})
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
				// Unrelated pending rows must never enter this entity's presence/overlay
				// capture. The graph writes have warmed presence; only the two history
				// values of the requested entity need capture.
				for range 32 {
					other, err := g.Nodes().Add(t.Context(), []string{"other"}, nil)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := g.Nodes().Update(t.Context(), other.ID(), map[string]any{"v": int64(1)}); err != nil {
						t.Fatal(err)
					}
					link, err := g.Rels().Add(t.Context(), "OTHER", other, z, nil)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := g.Rels().Update(t.Context(), link.ID(), map[string]any{"v": int64(1)}); err != nil {
						t.Fatal(err)
					}
				}
				if flushed {
					if err := bs.Flush(); err != nil {
						t.Fatal(err)
					}
				}
				read := func(b store.ReadBudget) (bool, store.ReadWork, int64, int64, error) {
					ctx, m := store.WithReadBudget(t.Context(), b)
					copies, end := badger.CountHistoryAnchorBytesCopiedForTest()
					defer end()
					visits, stop := badger.CountPendingKeysVisitedForTest()
					defer stop()
					var ok bool
					var err error
					if kind == "node" {
						var got *types.Node
						got, err = g.Temporal().NodeAsOfCtx(ctx, n.ID(), pin)
						ok = got != nil && got.ID() == n.ID() && got.Version() == 1 && got.PropertiesMap()["v"] == int64(1) && got.PropertiesMap()["payload"] == payload
					} else {
						var got *types.Relationship
						got, err = g.Temporal().RelAsOfCtx(ctx, r.ID(), pin)
						ok = got != nil && got.ID() == r.ID() && got.Version() == 1 && got.PropertiesMap()["v"] == int64(1) && got.PropertiesMap()["payload"] == payload
					}
					return ok, m.Work(), copies(), visits(), err
				}
				ok, total, copies, visits, err := read(store.ReadBudget{})
				wantVisits := int64(2)
				if flushed {
					wantVisits = 0
				}
				if err != nil || !ok || copies == 0 || visits != wantVisits || total.BufferBytes == 0 {
					t.Fatalf("control: exact row %v work %+v anchor bytes %d pending visits %d err %v", ok, total, copies, visits, err)
				}
				ok, w, copies, _, err := read(store.ReadBudget{BufferBytes: total.BufferBytes - 1})
				var refusal *store.ReadBudgetError
				if !errors.As(err, &refusal) || refusal.Limit != "buffer bytes" || ok || copies != 0 {
					t.Fatalf("limited: row %v work %+v anchor bytes %d err %v", ok, w, copies, err)
				}
			})
		}
	}
}
