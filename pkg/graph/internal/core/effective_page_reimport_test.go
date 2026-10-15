package core

import (
	"context"
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestEffectivePage_ReimportedIDEnvelope (backlog 75 review) — an entity
// deleted before its type's (label's) envelope index was created is
// uncovered; re-imported with a later valid-from, its envelope must still
// cover the deleted life's rows, or a page at a pin before the delete prunes
// it. The page at (tx before the delete, ValidAt inside the first life)
// equals the full walk on every backend (the memory store's relationship
// create doors extended the envelope by the new row alone).
func TestEffectivePage_ReimportedIDEnvelope(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, be := range epBackends() {
		for _, rel := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/rel=%v", be.name, rel), func(t *testing.T) {
				t.Parallel()
				g := be.open(t, false)
				clk := &switchableClock{}
				g.SetClockForTest(t, clk.Now)
				x := etX0()
				step := func() { x += 1000; clk.at.Store(int64(x)) }
				step()
				var s, e types.NodeID
				if rel {
					s, e = txbEndpoints(t, g)
				}
				props := func(from types.Instant) map[string]any {
					return map[string]any{epProp: "v", "tkg_valid_from": from}
				}
				var id int64
				if rel {
					r, err := g.Rels.AddByID(ctx, "T", s, e, props(x-10_000))
					if err != nil {
						t.Fatal(err)
					}
					id = int64(r.ID())
				} else {
					n, err := g.Nodes.Add(ctx, []string{"L"}, props(x-10_000))
					if err != nil {
						t.Fatal(err)
					}
					id = int64(n.ID())
				}
				step()
				tx1, _ := g.Temporal.NowTx()
				step()
				var err error
				if rel {
					err = g.Rels.Delete(ctx, types.RelID(id))
				} else {
					err = g.Nodes.Delete(ctx, types.NodeID(id))
				}
				if err != nil {
					t.Fatal(err)
				}
				step()
				if rel {
					err = g.Index.CreateRelTemporal("T")
				} else {
					err = g.Index.CreateTemporal("L")
				}
				if err != nil {
					t.Fatal(err)
				}
				step()
				if rel {
					sn, _ := g.Nodes.Get(ctx, s)
					en, _ := g.Nodes.Get(ctx, e)
					_, err = g.Rels.Import(ctx, types.RelID(id), "T", sn, en, props(x+50_000))
				} else {
					_, err = g.Nodes.Import(ctx, types.NodeID(id), []string{"L"}, props(x+50_000))
				}
				if err != nil {
					t.Fatalf("import: %v", err)
				}
				step()
				pin := temporal.Pin{Tx: tx1, ValidAt: x - 9_000}
				want := epOracle(t, g, rel, pin, "v")
				got, _, err := epPage(g, rel, pin, "v", 0, 10)
				if err != nil {
					t.Fatal(err)
				}
				if len(want) != 1 || want[0].id != id {
					t.Fatalf("oracle %s, want entity %d", epRender(want), id)
				}
				if a, b := epRender(got), epRender(want); a != b {
					t.Fatalf("page at the first life:\n got  %s\n want %s", a, b)
				}
			})
		}
	}
}
