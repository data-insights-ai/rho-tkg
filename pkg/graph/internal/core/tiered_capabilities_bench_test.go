package core

import (
	"context"
	"testing"
	"time"

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
