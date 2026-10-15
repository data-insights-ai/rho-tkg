package core

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkEffectivePage (backlog 69, evidence 03-bench.txt): 100,000
// relationships over 10 estates and 3 types (each indexed on estate), one
// page of 500 of estate e3 from its middle, through RelsEffectivePage against
// the full walk ai-soc runs today (ForEachRelEffective filtered by estate and
// cursor, keeping the 500 smallest IDs). EP_BENCH_RELS overrides the size.
func BenchmarkEffectivePage(b *testing.B) {
	const n, limit = 100_000, 500
	ctx := context.Background()
	for _, be := range []string{"memory", "badger"} {
		g, err := New(Config{BadgerInMemory: be == "badger"})
		if err != nil {
			b.Fatal(err)
		}
		s, err := g.Nodes.Add(ctx, []string{"Host"}, nil)
		if err != nil {
			b.Fatal(err)
		}
		e, err := g.Nodes.Add(ctx, []string{"Host"}, nil)
		if err != nil {
			b.Fatal(err)
		}
		for _, typ := range epRelTypes {
			if err := g.Index.CreateRelProperty(typ, epProp); err != nil {
				b.Fatal(err)
			}
		}
		var est3 []types.RelID
		for i := 0; i < n; i++ {
			v := fmt.Sprintf("e%d", i%10)
			r, err := g.Rels.AddByID(ctx, epRelTypes[i%3], s.ID(), e.ID(), map[string]any{epProp: v})
			if err != nil {
				b.Fatal(err)
			}
			if v == "e3" {
				est3 = append(est3, r.ID())
			}
		}
		pin, _ := g.Temporal.NowTx()
		after := est3[len(est3)/2]
		want := est3[slices.Index(est3, after)+1:][:limit]
		check := func(b *testing.B, ids []types.RelID) {
			if !slices.Equal(ids, want) {
				b.Fatalf("page of %d IDs differs from the expected %d", len(ids), len(want))
			}
		}
		b.Run(be+"/page", func(b *testing.B) {
			var ids []types.RelID
			for b.Loop() {
				segs, _, err := g.Temporal.RelsEffectivePage(ctx, temporal.Pin{Tx: pin}, temporal.Filter{Prop: epProp, Eq: "e3"}, after, limit)
				if err != nil {
					b.Fatal(err)
				}
				ids = ids[:0]
				for _, sg := range segs {
					ids = append(ids, sg.Rel.ID())
				}
			}
			check(b, ids)
		})
		b.Run(be+"/fullwalk", func(b *testing.B) {
			var ids []types.RelID
			for b.Loop() {
				ids = ids[:0]
				if err := g.Temporal.ForEachRelEffective(pin, func(sg temporal.RelSegment) bool {
					if id := sg.Rel.ID(); id > after && sg.Rel.PropertiesMap()[epProp] == "e3" {
						ids = append(ids, id)
					}
					return true
				}); err != nil {
					b.Fatal(err)
				}
				slices.Sort(ids)
				ids = slices.Compact(ids)[:limit]
			}
			check(b, ids)
		})
		_ = g.Close()
	}
}
