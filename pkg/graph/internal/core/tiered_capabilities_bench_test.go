package core

import (
	"context"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
)

// BenchmarkTieredRelEffectiveByTypeFrom (backlog 58): a type with 2,000
// relationships over two event shards, all but 10 deleted before validFrom;
// one bounded scan per op. reads/op counts the timelines the door read.
func BenchmarkTieredRelEffectiveByTypeFrom(b *testing.B) {
	const total, live = 2_000, 10
	ctx := context.Background()
	ts, err := tiered.New(tiered.Config{InMemory: true, RefLabels: []string{"Host"},
		ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1})
	if err != nil {
		b.Fatal(err)
	}
	g, err := New(Config{Store: ts})
	if err != nil {
		_ = ts.Close()
		b.Fatal(err)
	}
	defer g.Close()
	host, err := g.Nodes.Add(ctx, []string{"Host"}, nil)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < total; i++ {
		if i == total/2 {
			ts.MuForTest().Lock()
			ts.HotShardForTest().SetTimeEndForTest(time.Now().Add(-time.Second))
			ts.MuForTest().Unlock()
			if err := ts.CheckRotationForTest(); err != nil {
				b.Fatal(err)
			}
			time.Sleep(2 * time.Millisecond)
		}
		ev, err := g.Nodes.Add(ctx, []string{"Event"}, nil)
		if err != nil {
			b.Fatal(err)
		}
		r, err := g.Rels.AddByID(ctx, "SEEN", host.ID(), ev.ID(), nil)
		if err != nil {
			b.Fatal(err)
		}
		if i >= live {
			if err := g.Rels.Delete(ctx, r.ID()); err != nil {
				b.Fatal(err)
			}
		}
	}
	pin, err := g.Temporal.NowTx()
	if err != nil {
		b.Fatal(err)
	}
	validFrom := pin
	reads := 0
	g.chainLoadTestHook = func() { reads++ }
	scan := func() int {
		n := 0
		if err := g.Temporal.ForEachRelEffectiveByTypeFrom("SEEN", pin, validFrom, func(temporal.RelSegment) bool { n++; return true }); err != nil {
			b.Fatal(err)
		}
		return n
	}
	if got := scan(); got != live {
		b.Fatalf("streamed %d segments, want %d", got, live)
	}
	reads = 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scan()
	}
	b.StopTimer()
	b.ReportMetric(float64(reads)/float64(b.N), "reads/op")
}

// BenchmarkTieredColdShards (backlog 58 review): 8 event shards, every one
// but the hot shard cold and closed on disk, MaxOpenColdShards 0 (each read
// of a cold shard opens and closes it); 10 relationships and nodes per shard,
// 2 of them live. Per op one ForEachRelEffectiveByTypeFrom (validFrom = now)
// and one temporal ByLabel (DepthAll, TxPin = now). fallback clears the
// core's capability handles (the path before backlog 58); capability uses
// the tiered fan-out.
func BenchmarkTieredColdShards(b *testing.B) {
	const shards, perShard, live = 8, 10, 2
	ctx := context.Background()
	ts, err := tiered.New(tiered.Config{DataDir: b.TempDir(), RefLabels: []string{"Host"},
		ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1})
	if err != nil {
		b.Fatal(err)
	}
	g, err := New(Config{Store: ts})
	if err != nil {
		_ = ts.Close()
		b.Fatal(err)
	}
	defer g.Close()
	host, err := g.Nodes.Add(ctx, []string{"Host"}, nil)
	if err != nil {
		b.Fatal(err)
	}
	for s := 0; s < shards; s++ {
		if s > 0 {
			ts.MuForTest().Lock()
			ts.HotShardForTest().SetTimeEndForTest(time.Now().Add(-time.Second))
			ts.MuForTest().Unlock()
			if err := ts.CheckRotationForTest(); err != nil {
				b.Fatal(err)
			}
			time.Sleep(2 * time.Millisecond)
		}
		for i := 0; i < perShard; i++ {
			ev, err := g.Nodes.Add(ctx, []string{"Event"}, map[string]any{"i": i})
			if err != nil {
				b.Fatal(err)
			}
			r, err := g.Rels.AddByID(ctx, "SEEN", host.ID(), ev.ID(), nil)
			if err != nil {
				b.Fatal(err)
			}
			if i >= live {
				if err := g.Rels.Delete(ctx, r.ID()); err != nil {
					b.Fatal(err)
				}
				if _, err := g.Nodes.Update(ctx, ev.ID(), map[string]any{"i": -i}); err != nil {
					b.Fatal(err)
				}
			}
		}
	}
	ts.MuForTest().Lock()
	hot := ts.HotShardForTest()
	var cold []*tiered.EventShard
	for _, es := range ts.EventShardsForTest() {
		if es != hot {
			es.SetTierForTest(tiered.TierCold)
			ts.CatalogForTest().UpdateShardTier(es.Name(), tiered.TierCold)
			cold = append(cold, es)
		}
	}
	ts.MuForTest().Unlock()
	for _, es := range cold {
		es.SetLastAccessForTest(0)
	}
	ts.SetIdleTimeoutForTest(time.Nanosecond)
	ts.CloseIdleShardsForTest()
	ts.SetIdleTimeoutForTest(time.Hour)
	for _, es := range cold {
		if es.Store() != nil {
			b.Fatalf("cold shard %s still open", es.Name())
		}
	}
	pin, err := g.Temporal.NowTx()
	if err != nil {
		b.Fatal(err)
	}
	type handles struct {
		lm  storepkg.LabelTxMembershipCapability
		rm  storepkg.RelTypeTxMembershipCapability
		ll  storepkg.LabelTxMemberLookupCapability
		rl  storepkg.RelTypeTxMemberLookupCapability
		rve storepkg.RelValidEndCapability
		nve storepkg.NodeValidEndCapability
	}
	native := handles{g.labelTxMembers, g.relTypeTxMembers, g.labelTxLookup, g.relTypeTxLookup, g.relValidEnds, g.nodeValidEnds}
	for _, mode := range []string{"capability", "fallback"} {
		h := native
		if mode == "fallback" {
			h = handles{}
		}
		g.labelTxMembers, g.relTypeTxMembers, g.labelTxLookup, g.relTypeTxLookup, g.relValidEnds, g.nodeValidEnds = h.lm, h.rm, h.ll, h.rl, h.rve, h.nve
		b.Run("RelFrom/"+mode, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				n := 0
				if err := g.Temporal.ForEachRelEffectiveByTypeFrom("SEEN", pin, pin, func(temporal.RelSegment) bool { n++; return true }); err != nil {
					b.Fatal(err)
				}
				if n != shards*live {
					b.Fatalf("streamed %d, want %d", n, shards*live)
				}
			}
		})
		b.Run("ByLabel/"+mode, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				nodes, err := g.Nodes.ByLabel("Event", storepkg.QueryOpts{TxPin: pin})
				if err != nil {
					b.Fatal(err)
				}
				if len(nodes) != shards*perShard {
					b.Fatalf("ByLabel = %d nodes, want %d", len(nodes), shards*perShard)
				}
			}
		})
	}
	g.labelTxMembers, g.relTypeTxMembers, g.labelTxLookup, g.relTypeTxLookup, g.relValidEnds, g.nodeValidEnds = native.lm, native.rm, native.ll, native.rl, native.rve, native.nve
}
