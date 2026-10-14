package core

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestEffectivePage_CostFollowsPage — the indexed path's structural cost: with
// every relationship type (node label) indexed on estate, 3,000 entities over
// 10 estates and 3 types (labels), a page of 25 from the middle of one estate
// reads exactly 25 timelines on memory, badger and sharded; without the index
// the same page reads the candidates of every type above the cursor until 25
// match (more than 25). Both answer the same page.
func TestEffectivePage_CostFollowsPage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const n, limit = 3000, 25
	for _, be := range epBackends()[:3] {
		for _, rel := range []bool{true, false} {
			for _, indexed := range []bool{true, false} {
				t.Run(fmt.Sprintf("%s/rel=%v/indexed=%v", be.name, rel, indexed), func(t *testing.T) {
					t.Parallel()
					g := be.open(t, false)
					var s, e types.NodeID
					if rel {
						s, e = txbEndpoints(t, g)
					}
					names := epRelTypes
					if !rel {
						names = epLabels
					}
					if indexed {
						for _, name := range names {
							var err error
							if rel {
								err = g.Index.CreateRelProperty(name, epProp)
							} else {
								err = g.Index.CreateProperty(name, epProp)
							}
							if err != nil {
								t.Fatal(err)
							}
						}
					}
					var est3 []int64
					for i := 0; i < n; i++ {
						v := fmt.Sprintf("e%d", i%10)
						var id int64
						if rel {
							r, err := g.Rels.AddByID(ctx, names[i%3], s, e, map[string]any{epProp: v})
							if err != nil {
								t.Fatal(err)
							}
							id = int64(r.ID())
						} else {
							nd, err := g.Nodes.Add(ctx, []string{names[i%3]}, map[string]any{epProp: v})
							if err != nil {
								t.Fatal(err)
							}
							id = int64(nd.ID())
						}
						if v == "e3" {
							est3 = append(est3, id)
						}
					}
					pin, _ := g.Temporal.NowTx()
					p := temporal.Pin{Tx: pin}
					if _, _, err := epPage(g, rel, p, "e3", 0, 1); err != nil { // builds the sidecars, opens the views
						t.Fatal(err)
					}
					reads := 0
					g.scanReadTestHook = func() { reads++ }
					after := est3[len(est3)/2]
					page, next, err := epPage(g, rel, p, "e3", after, limit)
					g.scanReadTestHook = nil
					if err != nil {
						t.Fatal(err)
					}
					var ids []int64
					for _, en := range page {
						ids = append(ids, en.id)
					}
					i := slices.Index(est3, after) + 1
					if want := est3[i : i+limit]; !slices.Equal(ids, want) || next != want[limit-1] {
						t.Fatalf("page %v next %d, want %v", ids, next, want)
					}
					// The whole estate (300 entities) crosses several sidecar
					// fetches: the walk equals the full walk.
					// The whole estate (300 entities, 100 per type) crosses
					// several sidecar fetches within one page at limit 200.
					want := epShape(epOracle(t, g, rel, p, "e3"))
					for _, l := range []int{limit, 200} {
						if got := epShape(epWalk(t, g, rel, p, "e3", l, nil)); got != want {
							t.Fatalf("walk of e3 limit %d:\n got  %s\n want %s", l, got, want)
						}
					}
					t.Logf("page of %d read %d timelines", limit, reads)
					switch {
					case indexed && reads != limit:
						t.Errorf("indexed page read %d timelines, want %d", reads, limit)
					case !indexed && reads <= limit:
						t.Errorf("unindexed page read %d timelines, want more than %d (the type's candidates)", reads, limit)
					}
				})
			}
		}
	}
}
