package core

import (
	"fmt"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
)

// BenchmarkRelEffectiveTimelinesByProperties (row 105): 1,000 (estate, device)
// probes on 100k relationships (10 estates, 10k devices; memory) through one
// door call vs 1,000 pinned ByTypeAndProperty calls on the device (one version
// per relationship, not a timeline: the door does more per match and must
// still not cost more per probe).
func BenchmarkRelEffectiveTimelinesByProperties(b *testing.B) {
	const n, devices, probes = 100_000, 10_000, 1_000
	g := txbBackends()[0].open(b, false)
	ctx := b.Context()
	sn, err := g.Nodes.Add(ctx, []string{"Ref"}, nil)
	if err != nil {
		b.Fatal(err)
	}
	en, err := g.Nodes.Add(ctx, []string{"Ev"}, nil)
	if err != nil {
		b.Fatal(err)
	}
	s, e := sn.ID(), en.ID()
	for _, k := range rtbKeys {
		if err := g.Index.CreateRelProperty(rtbType, k); err != nil {
			b.Fatal(err)
		}
	}
	for i := range n {
		if _, err := g.Rels.AddByID(ctx, rtbType, s, e, map[string]any{"estate": fmt.Sprintf("e%d", i%10), "device": fmt.Sprintf("d%d", i%devices)}); err != nil {
			b.Fatal(err)
		}
	}
	tx, err := g.Temporal.NowTx()
	if err != nil {
		b.Fatal(err)
	}
	tuples := make([][]any, probes)
	for i := range tuples {
		tuples[i] = []any{fmt.Sprintf("e%d", i%10), fmt.Sprintf("d%d", i)}
	}
	// Warm-up: the first sidecar read builds the memory sidecar lazily.
	if _, err := g.Temporal.RelEffectiveTimelinesByProperties(ctx, rtbType, rtbKeys, tuples, temporal.Pin{Tx: tx}); err != nil {
		b.Fatal(err)
	}
	b.Run("door", func(b *testing.B) {
		for b.Loop() {
			if _, err := g.Temporal.RelEffectiveTimelinesByProperties(ctx, rtbType, rtbKeys, tuples, temporal.Pin{Tx: tx}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("ByTypeAndProperty", func(b *testing.B) {
		for b.Loop() {
			for _, tu := range tuples {
				if _, err := g.Rels.ByTypeAndProperty(rtbType, "device", tu[1], storepkg.QueryOpts{TxPin: tx}); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}
