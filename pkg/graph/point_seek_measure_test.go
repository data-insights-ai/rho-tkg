//go:build measure

// Measurement level (René 4 Nov, `make measure`): testing.AllocsPerRun reads
// the process-wide malloc count (badger's and the shards' goroutines
// included), so this comparison moves with load and runs by hand, not in a
// gate. The one store lookup per seek is gated by
// TestRound11PointSeekAsksTheStoreOnce (core), the answers by
// TestRound11FormsSeekMatchesOracle.

package graph_test

import (
	"context"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
)

// TestRound11PointSeekOneWalk: a seek of the number 1000 (ten forms) costs
// over a seek of a string with the same three matches (one form) the same
// on every backend: the forms' keys, not a walk per form. On tiered and
// sharded a walk reads every shard, so before round 11 (one store lookup per
// form) the numeric seek's extra allocations there were about 3.5 times
// memory's (139 vs 41 for nodes on tiered); the margin allows 12.
// TestRound11PointSeekAsksTheStoreOnce (core) pins the one lookup itself.
func TestRound11PointSeekOneWalk(t *testing.T) {
	extra := map[string]map[string]float64{} // kind -> backend -> numeric minus string allocations
	for _, b := range allStoreBackends() {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			r11SeekWorld(t, g, 40)
			nodes := func(key string, v any) func() {
				return func() {
					got, err := g.Nodes().ByLabelAndProperty("Ev", key, v, storepkg.QueryOpts{})
					if err != nil || len(got) != 3 {
						t.Fatalf("nodes %s=%v: %d rows, %v", key, v, len(got), err)
					}
				}
			}
			rels := func(key string, v any) func() {
				return func() {
					got, err := g.Rels().ByTypeAndProperty("E", key, v, storepkg.QueryOpts{})
					if err != nil || len(got) != 3 {
						t.Fatalf("rels %s=%v: %d rows, %v", key, v, len(got), err)
					}
				}
			}
			now, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			nodesAt := func(key string, v any) func() {
				return func() {
					got, err := g.Temporal().NodesByLabelPropertyAt("Ev", key, v, now)
					if err != nil || len(got) != 3 {
						t.Fatalf("nodes at %s=%v: %d rows, %v", key, v, len(got), err)
					}
				}
			}
			relsAt := func(key string, v any) func() {
				return func() {
					got, err := g.Temporal().RelsByTypePropertyAt("E", key, v, now)
					if err != nil || len(got) != 3 {
						t.Fatalf("rels at %s=%v: %d rows, %v", key, v, len(got), err)
					}
				}
			}
			page := func(key string, v any) func() {
				return func() {
					segs, _, err := g.Temporal().NodesEffectivePage(context.Background(), temporal.Pin{Tx: now}, temporal.Filter{Prop: key, Eq: v}, 0, 10)
					if err != nil || len(segs) == 0 {
						t.Fatalf("page %s=%v: %d segments, %v", key, v, len(segs), err)
					}
				}
			}
			for _, c := range []struct {
				kind     string
				num, str func()
			}{
				{"nodes page", page("k", int64(1000)), page("s", "v0")},
				{"nodes", nodes("k", int64(1000)), nodes("s", "v0")},
				{"rels", rels("k", int64(1000)), rels("s", "v0")},
				{"nodes at", nodesAt("k", int64(1000)), nodesAt("s", "v0")},
				{"rels at", relsAt("k", int64(1000)), relsAt("s", "v0")},
			} {
				num := testing.AllocsPerRun(20, c.num)
				str := testing.AllocsPerRun(20, c.str)
				t.Logf("%s: numeric seek %.0f allocs, string seek %.0f allocs", c.kind, num, str)
				if extra[c.kind] == nil {
					extra[c.kind] = map[string]float64{}
				}
				extra[c.kind][b.name] = num - str
			}
		})
	}
	for kind, byBackend := range extra {
		ref, ok := byBackend["memory"]
		if !ok {
			t.Fatalf("%s: no memory reference", kind)
		}
		for name, e := range byBackend {
			// A tiered page asks its paged membership once per numeric form
			// (one source per equal key, as on every backend), and each ask
			// fans out over the reference, archive and event shards: about 17
			// allocations a form, no walk. Until row 106 it walked the whole
			// label for every seek, so the margin held for that reason. That
			// no tiered page walks a label or type is pinned by
			// TestTieredRelsEffectivePage_NoWholeTypeScan (core).
			if kind == "nodes page" && name == "tiered" {
				continue
			}
			if e > ref+12 {
				t.Errorf("%s on %s: a numeric point seek costs %.0f allocations over a string seek, memory %.0f: the store is walked once per numeric form", kind, name, e, ref)
			}
		}
	}
}
