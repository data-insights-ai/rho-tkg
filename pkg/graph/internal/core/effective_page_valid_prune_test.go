package core

import (
	"context"
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 75 (ai-soc 2026-10-15): a page with a valid filter prunes its
// candidates by the valid-time envelope index (CreateRelTemporal /
// CreateTemporal) before it reads their timelines.

// TestEffectivePage_ValidPruneCost — with an envelope index on every type
// (label), 300 entities over 3 types and 3 estates of which only every fifth
// is still open (the others closed long before the filter), a page with
// ValidAt, a closed or an open interval reads exactly the timelines of the
// candidates whose envelope meets the filter: the open ones of the estate when
// the property index feeds the candidates, the open ones of every estate when
// the type list does. Without the prune it reads every candidate. Where the
// store has no envelope prune (tiered nodes) the page reads every candidate.
// The answer equals the full walk at every limit.
func TestEffectivePage_ValidPruneCost(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const n = 300
	for _, be := range epBackends() {
		for _, rel := range []bool{true, false} {
			for _, propIdx := range []bool{true, false} {
				t.Run(fmt.Sprintf("%s/rel=%v/propIdx=%v", be.name, rel, propIdx), func(t *testing.T) {
					t.Parallel()
					g := be.open(t, false)
					clk := &switchableClock{}
					g.SetClockForTest(t, clk.Now)
					x := etX0()
					clk.at.Store(int64(x))
					var s, e types.NodeID
					names := epRelTypes
					if rel {
						s, e = txbEndpoints(t, g)
					} else {
						names = epLabels
					}
					propOK := propIdx
					for _, name := range names {
						var err error
						if rel {
							err = g.Index.CreateRelTemporal(name)
						} else {
							err = g.Index.CreateTemporal(name)
						}
						if err != nil {
							t.Fatal(err)
						}
						if !propIdx {
							continue
						}
						if rel {
							err = g.Index.CreateRelProperty(name, epProp)
						} else {
							err = g.Index.CreateProperty(name, epProp)
						}
						if err != nil {
							propOK = false
							t.Logf("property index %s: %v (backend declines; the type list feeds)", name, err)
						}
					}
					openAll, openE1, allE1 := 0, 0, 0
					for i := 0; i < n; i++ {
						v := fmt.Sprintf("e%d", (i/3)%3)
						props := map[string]any{epProp: v, "tkg_valid_from": x - 10_000}
						open := i%5 == 0
						if !open {
							props["tkg_valid_to"] = x - 9_000
						}
						var err error
						if rel {
							_, err = g.Rels.AddByID(ctx, names[i%3], s, e, props)
						} else {
							_, err = g.Nodes.Add(ctx, []string{names[i%3]}, props)
						}
						if err != nil {
							t.Fatal(err)
						}
						if open {
							openAll++
						}
						if v == "e1" {
							allE1++
							if open {
								openE1++
							}
						}
					}
					x += 1000
					clk.at.Store(int64(x))
					tx, _ := g.Temporal.NowTx()
					// The first page builds the sidecars and opens the views.
					if _, _, err := epPage(g, rel, temporal.Pin{Tx: tx}, "e1", 0, 1); err != nil {
						t.Fatal(err)
					}
					sidecar := propOK && g.nodePropTxPage != nil
					if rel {
						sidecar = propOK && g.relPropTxPage != nil
					}
					pruned := g.relTypeTemporalCandidates != nil
					if !rel {
						pruned = g.temporalCandidates != nil
					}
					for _, p := range []temporal.Pin{
						{Tx: tx, ValidAt: x},
						{Tx: tx, ValidStart: x - 100, ValidEnd: x + 100},
						{Tx: tx, ValidStart: x - 100}, // open-ended
					} {
						reads := 0
						g.scanReadTestHook = func() { reads++ }
						page, next, err := epPage(g, rel, p, "e1", 0, 1000)
						g.scanReadTestHook = nil
						if err != nil {
							t.Fatal(err)
						}
						want := epOracle(t, g, rel, p, "e1")
						if a, b := epRender(page), epRender(want); a != b || next != 0 || len(want) != openE1 {
							t.Fatalf("pin %+v: page (next %d) %s\n want (%d open) %s", p, next, a, openE1, b)
						}
						for _, l := range []int{1, 7} {
							if a, b := epRender(epWalk(t, g, rel, p, "e1", l, nil)), epRender(want); a != b {
								t.Fatalf("pin %+v walk limit %d:\n got  %s\n want %s", p, l, a, b)
							}
						}
						// The candidates the sources list: the estate's members
						// (property sidecar) or every member of the type (type
						// list), the envelope prune keeping the open ones.
						wantReads := n
						switch {
						case pruned && sidecar:
							wantReads = openE1
						case pruned:
							wantReads = openAll
						case sidecar:
							wantReads = allE1
						}
						t.Logf("pin %+v: %d timeline reads (pruned=%v sidecar=%v)", p, reads, pruned, sidecar)
						if reads != wantReads {
							t.Errorf("pin %+v: page read %d timelines, want %d", p, reads, wantReads)
						}
					}
				})
			}
		}
	}
}

// TestEffectivePage_ValidPruneDifferential — with envelope indexes on two of
// three types (labels), declared before or after the writes, every page walk
// at random pins, ValidAt, closed and open intervals equals the unpruned full
// walk, after adds, updates, deletes, closes, excises (void pieces), interval
// corrections and retractions, on every backend, on a TxFrom store and on an
// UntimedUnbounded store (untimed first versions start at -inf).
func TestEffectivePage_ValidPruneDifferential(t *testing.T) {
	t.Parallel()
	seeds, steps := 2, 90
	if testing.Short() {
		seeds, steps = 1, 60
	}
	if isRaceEnabled() {
		seeds, steps = 1, 40
	}
	for _, mode := range []UntimedValidStart{UntimedFromTxFrom, UntimedUnbounded} {
		for _, be := range txbBackends(func(c *Config) { c.UntimedValidStart = mode }) {
			for _, rel := range []bool{true, false} {
				for _, early := range []bool{true, false} {
					for seed := 0; seed < seeds; seed++ {
						t.Run(fmt.Sprintf("mode=%d/%s/rel=%v/early=%v/seed=%d", mode, be.name, rel, early, seed), func(t *testing.T) {
							t.Parallel()
							epDifferential(t, be, rel, early, true, seed, steps)
						})
					}
				}
			}
		}
	}
}
