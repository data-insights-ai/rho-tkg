package graph_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestForEachByOrdinals (sigma-tkgd round 8 request 4): on every backend with
// ordinals, Nodes().ForEachByLabelOrdinals / Rels().ForEachByTypeOrdinals hand
// out exactly ForEachByLabel's / ForEachByType's rows (versions at a
// coordinate) whose entity's current ordinal is in the bitset, in ID order,
// current and at TxPin, ValidAt and all-valid-time-at-a-pin coordinates,
// through updates, deletes (no current row: never selected) and label moves;
// After and Limit apply; an early stop stops; tiered (no ordinals) is
// ErrCapabilityNotSupported.
func TestForEachByOrdinals(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		label := "Ref"
		if b.tiered {
			err := g.Nodes().ForEachByLabelOrdinals(label, []uint64{^uint64(0)}, storepkg.QueryOpts{}, func(*types.Node) bool { return true })
			if !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
				t.Fatalf("tiered: %v", err)
			}
			err = g.Rels().ForEachByTypeOrdinals("R", []uint64{^uint64(0)}, storepkg.QueryOpts{}, func(*types.Relationship) bool { return true })
			if !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
				t.Fatalf("tiered rels: %v", err)
			}
			return
		}
		rng := rand.New(rand.NewPCG(4, 8))
		hub, err := g.Nodes().Add(ctx, []string{"Hub"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		var nodes []*types.Node
		var rels []*types.Relationship
		var pins []types.Instant
		for i := range 60 {
			n, err := g.Nodes().Add(ctx, []string{label}, map[string]any{"v": int64(i), "tkg_valid_from": types.Instant(1000 + i)})
			if err != nil {
				t.Fatal(err)
			}
			nodes = append(nodes, n)
			r, err := g.Rels().Add(ctx, "R", hub, n, map[string]any{"w": int64(i), "tkg_valid_from": types.Instant(1000 + i)})
			if err != nil {
				t.Fatal(err)
			}
			rels = append(rels, r)
			f, _, _ := n.TxStamps()
			pins = append(pins, f)
		}
		for step := range 40 {
			i := rng.IntN(len(nodes))
			up, err := g.Nodes().Update(ctx, nodes[i].ID(), map[string]any{"v": int64(100 + step), "tkg_valid_from": types.Instant(5000 + step)})
			if err != nil {
				t.Fatal(err)
			}
			f, _, _ := up.TxStamps()
			pins = append(pins, f)
			if _, err := g.Rels().Update(ctx, rels[i].ID(), map[string]any{"w": int64(100 + step), "tkg_valid_from": types.Instant(5000 + step)}); err != nil {
				t.Fatal(err)
			}
		}
		for _, i := range []int{3, 17, 41} {
			if err := g.Nodes().AddLabel(ctx, nodes[i].ID(), "Keep"); err != nil {
				t.Fatal(err)
			}
			if err := g.Nodes().RemoveLabel(ctx, nodes[i].ID(), label); err != nil {
				t.Fatal(err)
			}
		}
		for _, i := range []int{5, 22, 50} {
			if err := g.Nodes().Delete(ctx, nodes[i].ID()); err != nil { // its relationship goes too
				t.Fatal(err)
			}
		}
		maxN, _, err := g.Nodes().MaxOrdinal()
		if err != nil {
			t.Fatal(err)
		}
		maxR, _, err := g.Rels().MaxOrdinal()
		if err != nil {
			t.Fatal(err)
		}
		bits := func(max uint32, keep func(uint32) bool) []uint64 {
			set := make([]uint64, max/64+1)
			for o := uint32(1); o <= max; o++ {
				if keep(o) {
					set[o/64] |= 1 << (o % 64)
				}
			}
			return set
		}
		nodeOrd := func(id types.NodeID) uint32 {
			cur, err := g.Nodes().Get(ctx, id)
			if err != nil {
				return 0
			}
			return cur.Ordinal()
		}
		relOrd := func(id types.RelID) uint32 {
			cur, err := g.Rels().Get(ctx, id)
			if err != nil {
				return 0
			}
			return cur.Ordinal()
		}
		sets := map[string][2][]uint64{
			"all":   {bits(maxN, func(uint32) bool { return true }), bits(maxR, func(uint32) bool { return true })},
			"none":  {nil, nil},
			"odd":   {bits(maxN, func(o uint32) bool { return o%2 == 1 }), bits(maxR, func(o uint32) bool { return o%2 == 1 })},
			"third": {bits(maxN, func(o uint32) bool { return o%3 == 0 }), bits(maxR, func(o uint32) bool { return o%3 == 0 })},
		}
		pin := pins[len(pins)/2]
		coords := []storepkg.QueryOpts{
			{},
			{After: types.EntityID(nodes[10].ID()), Limit: 7},
			{TxPin: pin},
			{ValidAt: 1030},
			{ValidStart: 1, ValidEnd: math.MaxInt64, TxAt: pin},
			{TxPin: pin, Limit: 4},
		}
		for setName, set := range sets {
			for ci, opts := range coords {
				name := fmt.Sprintf("%s set %s coord %d", b.name, setName, ci)
				var want, got []string
				scan := opts
				scan.Limit = 0
				if err := g.Nodes().ForEachByLabel(label, scan, func(n *types.Node) bool {
					if storepkg.OrdinalSetHas(set[0], nodeOrd(n.ID())) && (opts.Limit == 0 || len(want) < opts.Limit) {
						want = append(want, fmt.Sprintf("%d@%d", n.ID(), n.Version()))
					}
					return true
				}); err != nil {
					t.Fatal(err)
				}
				if err := g.Nodes().ForEachByLabelOrdinals(label, set[0], opts, func(n *types.Node) bool {
					got = append(got, fmt.Sprintf("%d@%d", n.ID(), n.Version()))
					return true
				}); err != nil {
					t.Fatalf("%s: %v", name, err)
				}
				if !slices.Equal(got, want) {
					t.Fatalf("%s nodes:\n got  %v\n want %v", name, got, want)
				}
				var wantR, gotR []string
				if err := g.Rels().ForEachByType("R", scan, func(r *types.Relationship) bool {
					if storepkg.OrdinalSetHas(set[1], relOrd(r.ID())) && (opts.Limit == 0 || len(wantR) < opts.Limit) {
						wantR = append(wantR, fmt.Sprintf("%d@%d", r.ID(), r.Version()))
					}
					return true
				}); err != nil {
					t.Fatal(err)
				}
				if err := g.Rels().ForEachByTypeOrdinals("R", set[1], opts, func(r *types.Relationship) bool {
					gotR = append(gotR, fmt.Sprintf("%d@%d", r.ID(), r.Version()))
					return true
				}); err != nil {
					t.Fatalf("%s rels: %v", name, err)
				}
				if !slices.Equal(gotR, wantR) {
					t.Fatalf("%s rels:\n got  %v\n want %v", name, gotR, wantR)
				}
			}
		}
		// An early stop stops; errors.
		calls := 0
		if err := g.Nodes().ForEachByLabelOrdinals(label, sets["all"][0], storepkg.QueryOpts{TxPin: pin}, func(*types.Node) bool {
			calls++
			return false
		}); err != nil || calls != 1 {
			t.Fatalf("early stop: %d calls, %v", calls, err)
		}
		if err := g.Nodes().ForEachByLabelOrdinals(label, nil, storepkg.QueryOpts{}, nil); err == nil {
			t.Fatal("nil fn: no error")
		}
		if err := g.Rels().ForEachByTypeOrdinals("R", nil, storepkg.QueryOpts{TxPin: 1, ValidAt: 1}, func(*types.Relationship) bool { return true }); err == nil {
			t.Fatal("conflicting opts: no error")
		}
		if err := g.Nodes().ForEachByLabelOrdinals("Nope", sets["all"][0], storepkg.QueryOpts{}, func(*types.Node) bool {
			t.Fatal("unknown label handed a row")
			return true
		}); err != nil {
			t.Fatal(err)
		}
	})
}
