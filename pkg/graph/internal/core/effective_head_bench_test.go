package core

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkR10HeadMiss: a head read whose kept answer is gone (dropped each
// iteration), against the fold without the cache (headCacheOff: the read as
// it was before backlog 68). The difference is what a miss adds.
func BenchmarkR10HeadMiss(b *testing.B) {
	for _, be := range r10Backends() {
		for _, versions := range []int{1, 100, 300} {
			for _, mode := range []string{"fold", "miss", "hit"} {
				b.Run(fmt.Sprintf("%s/versions=%d/%s", be.name, versions, mode), func(b *testing.B) {
					g, err := New(be.cfg())
					if err != nil {
						b.Fatal(err)
					}
					b.Cleanup(func() { _ = g.Close() })
					id := r10Node(b, g, versions)
					pin := g.peekNow()
					g.headCacheOff = mode == "fold"
					key := headKey{id: id.SnowflakeID()}
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						if mode == "miss" {
							g.heads.put(key, nil)
						}
						if _, err := g.Temporal.NodeEffectiveTimeline(id, pin); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}

// BenchmarkR10HeadParallel: head reads from all Ps at once, over 256 nodes of
// 100 versions each ("spread") or one node ("one"), with ("write") or without
// a writer correcting other nodes of the same graph meanwhile.
func BenchmarkR10HeadParallel(b *testing.B) {
	for _, be := range r10Backends() {
		for _, shape := range []string{"spread", "one", "spread+write"} {
			b.Run(fmt.Sprintf("%s/%s", be.name, shape), func(b *testing.B) {
				g, err := New(be.cfg())
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = g.Close() })
				n := 256
				if shape == "one" {
					n = 1
				}
				ids := make([]types.NodeID, n)
				for i := range ids {
					ids[i] = r10Node(b, g, 100)
				}
				others := []types.NodeID{r10Node(b, g, 2), r10Node(b, g, 2)}
				pin := g.peekNow()
				for _, id := range ids {
					if _, err := g.Temporal.NodeEffectiveTimeline(id, pin); err != nil {
						b.Fatal(err)
					}
				}
				stop := make(chan struct{})
				done := make(chan struct{})
				if shape == "spread+write" {
					go func() {
						defer close(done)
						ctx := context.Background()
						for i := 0; ; i++ {
							select {
							case <-stop:
								return
							default:
							}
							_, _ = g.Temporal.SetNodeVersionInterval(ctx, others[i%2], 1000, 2000, map[string]any{"w": int64(i)})
						}
					}()
				} else {
					close(done)
				}
				var next atomic.Uint64
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					i := next.Add(7919)
					for pb.Next() {
						i++
						if _, err := g.Temporal.NodeEffectiveTimeline(ids[i%uint64(len(ids))], pin); err != nil {
							b.Error(err)
							return
						}
					}
				})
				b.StopTimer()
				close(stop)
				<-done
			})
		}
	}
}
