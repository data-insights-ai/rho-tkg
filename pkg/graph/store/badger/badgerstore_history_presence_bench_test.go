package badger

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkRelHasHistory / BenchmarkNodeHasHistory measure HasRelHistory /
// HasNodeHistory on a reopened on-disk store of 200 K and 1 M entities where
// 0.1 % or 1 % of the entities hold three history rows (handover
// effective-read-cost 1b; target <= 0.1 us and 0 allocs on a built set):
//
//   - hit / miss: an entity with / without history, set already built;
//   - build: the one-time cost of the first call (key-only scan of the
//     history keyspace plus the install), the set reset before each call,
//     and setB/id, the live heap the built set retains per ID (GC before and
//     after one build).
//
// The 1 M fixtures are skipped under -short.
//
//	go test ./pkg/graph/store/badger/ -run '^$' -bench 'Benchmark(Rel|Node)HasHistory$' -benchmem
func BenchmarkRelHasHistory(b *testing.B)  { benchHasHistory(b, false) }
func BenchmarkNodeHasHistory(b *testing.B) { benchHasHistory(b, true) }

func benchHasHistory(b *testing.B, node bool) {
	for _, size := range []int{200_000, 1_000_000} {
		for _, d := range []struct {
			name   string
			stride int
		}{{"density=0.1%", 1000}, {"density=1%", 100}} {
			b.Run(fmt.Sprintf("entities=%dK/%s", size/1000, d.name), func(b *testing.B) {
				if size > 200_000 && testing.Short() {
					b.Skip("1 M fixture skipped under -short")
				}
				bs := seedHistoryBenchStoreN(b, node, d.stride, size)
				p := &bs.histRelPresence
				has := func(id int64) (bool, error) { return bs.HasRelHistory(types.RelID(snowflake.ID(id))) }
				if node {
					p = &bs.histNodePresence
					has = func(id int64) (bool, error) { return bs.HasNodeHistory(types.NodeID(snowflake.ID(id))) }
				}
				b.Run("build", func(b *testing.B) {
					// Retained heap of the built set: live heap after GC around
					// one build, per ID in the set (scan garbage collected).
					resetHistoryPresenceForBench(bs, p)
					var before, after runtime.MemStats
					runtime.GC()
					runtime.ReadMemStats(&before)
					if got, err := has(int64(d.stride)); err != nil || !got {
						b.Fatalf("build: %v, %v", got, err)
					}
					runtime.GC()
					runtime.ReadMemStats(&after)
					st := bs.HistoryPresenceStats()
					setIDs := st.NodeIDs + st.RelIDs
					setBytesPerID := float64(int64(after.HeapAlloc)-int64(before.HeapAlloc)) / float64(setIDs)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						b.StopTimer()
						resetHistoryPresenceForBench(bs, p)
						b.StartTimer()
						if got, err := has(int64(d.stride)); err != nil || !got {
							b.Fatalf("build: %v, %v", got, err)
						}
					}
					b.ReportMetric(float64(setIDs), "ids")
					b.ReportMetric(setBytesPerID, "setB/id") // after the loop: ResetTimer drops reported metrics
				})
				hits := make([]int64, 0, 1024)
				for id := d.stride; id <= size && len(hits) < 1024; id += d.stride {
					hits = append(hits, int64(id))
				}
				misses := plainHistoryBenchIDsN(d.stride, size)
				for _, tc := range []struct {
					name string
					ids  []int64
					want bool
				}{{"hit", hits, true}, {"miss", misses, false}} {
					b.Run(tc.name, func(b *testing.B) {
						if got, err := has(tc.ids[0]); err != nil || got != tc.want {
							b.Fatalf("warm-up: %v, %v", got, err)
						}
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							got, err := has(tc.ids[i%len(tc.ids)])
							if err != nil || got != tc.want {
								b.Fatalf("id %d: %v, %v; want %v", tc.ids[i%len(tc.ids)], got, err, tc.want)
							}
						}
					})
				}
			})
		}
	}
}

// resetHistoryPresenceForBench drops a built set so the next call builds it
// again (benchmark only; no writer runs concurrently).
func resetHistoryPresenceForBench(bs *Store, p *historyPresence) {
	p.buildMu.Lock()
	bs.wbMu.Lock()
	p.mu.Lock()
	p.tracking = false
	p.has, p.unknown = nil, nil
	p.built.Store(false)
	p.mu.Unlock()
	bs.wbMu.Unlock()
	p.buildMu.Unlock()
}

// seedHistoryBenchStoreN is seedHistoryBenchStore for an entity count other
// than historyBenchEntities.
func seedHistoryBenchStoreN(b *testing.B, node bool, stride, entities int) *Store {
	b.Helper()
	dir := b.TempDir()
	bs, err := New(Config{Dir: dir})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	if err := seedHistoryBenchRows(bs, node, stride, entities); err != nil {
		b.Fatal(err)
	}
	if err := bs.Close(); err != nil {
		b.Fatalf("close: %v", err)
	}
	bs, err = New(Config{Dir: dir, FlushInterval: time.Hour})
	if err != nil {
		b.Fatalf("reopen: %v", err)
	}
	b.Cleanup(func() { _ = bs.Close() })
	return bs
}

// plainHistoryBenchIDsN is plainHistoryBenchIDs over entities.
func plainHistoryBenchIDsN(stride, entities int) []int64 {
	ids := make([]int64, 0, 1024)
	for j := int64(0); len(ids) < 1024; j++ {
		id := 1 + j*int64(entities/1024)
		if stride > 0 && id%int64(stride) == 0 {
			id++
		}
		ids = append(ids, id)
	}
	return ids
}
