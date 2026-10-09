package graph_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkLatestStamps measures Rels().LatestStamps on one relationship with
// 0 ("plain"), 100 and 10,000 history versions (updates, every tenth a past
// correction that appends rows above the head), memory and badger on disk,
// against the History scan it replaces (sigma-tkgd's latestStamps today: Get,
// HasHistory, History, fold; 8.3 ms at 10,000 versions in ai-soc's profile).
// The sidecar is built by a warm-up call; badger's current row is cached
// (the hit path; a cold row adds one point read).
//
//	go test ./pkg/graph/ -run '^$' -bench 'BenchmarkLatestStamps' -benchmem
func BenchmarkLatestStamps(b *testing.B) {
	for _, backend := range []string{"memory", "badger"} {
		for _, versions := range []int{0, 100, 10_000} {
			name := fmt.Sprintf("%s/versions=%d", backend, versions)
			if versions == 0 {
				name = backend + "/plain"
			}
			b.Run(name, func(b *testing.B) {
				g, id := latestStampsBenchGraph(b, backend, versions)
				from, to, _, err := g.Rels().LatestStamps(id) // builds the sidecar
				if err != nil {
					b.Fatal(err)
				}
				if f, t := historyScanStamps(b, g, id); f != from || t != to {
					b.Fatalf("LatestStamps (%d, %d) != History scan (%d, %d)", from, to, f, t)
				}
				b.Run("door", func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						if _, _, _, err := g.Rels().LatestStamps(id); err != nil {
							b.Fatal(err)
						}
					}
				})
				b.Run("history-scan", func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						historyScanStamps(b, g, id)
					}
				})
			})
		}
	}
}

// historyScanStamps is the History-based computation LatestStamps replaces.
func historyScanStamps(b *testing.B, g *graphpkg.Graph, id types.RelID) (from, to types.Instant) {
	ctx := context.Background()
	if head, err := g.Rels().Get(ctx, id); err == nil {
		if tm := head.Temporal(); tm != nil {
			from, to = storepkg.FoldTxStamps(from, to, tm.TxFrom, tm.TxTo, tm.DeletedAt)
		}
	}
	has, err := g.Rels().HasHistory(id)
	if err != nil {
		b.Fatal(err)
	}
	if !has {
		return from, to
	}
	rows, err := g.Rels().History(id)
	if err != nil {
		b.Fatal(err)
	}
	for _, r := range rows {
		if tm := r.Temporal(); tm != nil {
			from, to = storepkg.FoldTxStamps(from, to, tm.TxFrom, tm.TxTo, tm.DeletedAt)
		}
	}
	return from, to
}

func latestStampsBenchGraph(b *testing.B, backend string, versions int) (*graphpkg.Graph, types.RelID) {
	b.Helper()
	ctx := context.Background()
	var st storepkg.Store = memory.New()
	if backend == "badger" {
		bs, err := badger.New(badger.Config{Dir: b.TempDir(), FlushInterval: 50 * time.Millisecond})
		if err != nil {
			b.Fatal(err)
		}
		st = bs
	}
	g, err := graphpkg.New(graphpkg.Config{Store: st})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = g.Close() })
	n1, err := g.Nodes().Add(ctx, []string{"Host"}, nil)
	if err != nil {
		b.Fatal(err)
	}
	n2, err := g.Nodes().Add(ctx, []string{"Host"}, nil)
	if err != nil {
		b.Fatal(err)
	}
	r, err := g.Rels().AddByID(ctx, "SEEN", n1.ID(), n2.ID(), map[string]any{"w": int64(0)})
	if err != nil {
		b.Fatal(err)
	}
	rows := 0
	for v := 1; rows < versions; v++ {
		if v%64 == 0 || versions-rows < 8 {
			rows = len(benchRelHistory(b, g, r.ID()))
			if rows >= versions {
				break
			}
		}
		if v%10 == 0 {
			now, err := g.Temporal().NowTx()
			if err != nil {
				b.Fatal(err)
			}
			if _, err := g.Temporal().SetRelVersionInterval(ctx, r.ID(), now+10, now+20, map[string]any{"w": int64(-v)}); err != nil {
				b.Fatal(err)
			}
			if _, err := g.Temporal().SetRelVersionInterval(ctx, r.ID(), now+11, now+15, map[string]any{"w": int64(-v)}); err != nil {
				b.Fatal(err)
			}
			rows += 4
			continue
		}
		if _, err := g.Rels().Update(ctx, r.ID(), map[string]any{"w": int64(v)}); err != nil {
			b.Fatal(err)
		}
		rows++
	}
	return g, r.ID()
}

func benchRelHistory(b *testing.B, g *graphpkg.Graph, id types.RelID) []*types.Relationship {
	b.Helper()
	h, err := g.Rels().History(id)
	if err != nil {
		b.Fatal(err)
	}
	return h
}

// BenchmarkUpdateStampsMaintenance measures the write-path cost of the badger
// history-stamps sidecar: Rels().Update (one history row per call) before the
// first LatestStamps call (sidecar not tracking) and after it (every history
// write decodes its row's stamps under the write-buffer lock).
//
//	go test ./pkg/graph/ -run '^$' -bench 'BenchmarkUpdateStampsMaintenance' -benchmem -count 5
func BenchmarkUpdateStampsMaintenance(b *testing.B) {
	for _, built := range []bool{false, true} {
		name := "unbuilt"
		if built {
			name = "built"
		}
		b.Run("badger/"+name, func(b *testing.B) {
			ctx := context.Background()
			bs, err := badger.New(badger.Config{Dir: b.TempDir(), FlushInterval: 50 * time.Millisecond})
			if err != nil {
				b.Fatal(err)
			}
			g, err := graphpkg.New(graphpkg.Config{Store: bs})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = g.Close() })
			n1, err := g.Nodes().Add(ctx, []string{"Host"}, nil)
			if err != nil {
				b.Fatal(err)
			}
			n2, err := g.Nodes().Add(ctx, []string{"Host"}, nil)
			if err != nil {
				b.Fatal(err)
			}
			ids := make([]types.RelID, 0, 1000)
			for i := 0; i < 1000; i++ {
				r, err := g.Rels().AddByID(ctx, "SEEN", n1.ID(), n2.ID(), map[string]any{"w": int64(i)})
				if err != nil {
					b.Fatal(err)
				}
				ids = append(ids, r.ID())
			}
			if built {
				if _, _, _, err := g.Rels().LatestStamps(ids[0]); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := g.Rels().Update(ctx, ids[i%len(ids)], map[string]any{"w": int64(-i)}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
