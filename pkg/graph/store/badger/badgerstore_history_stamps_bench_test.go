package badger

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkRelHistoryStamps / BenchmarkNodeHistoryStamps measure the
// history-stamps sidecar on a reopened on-disk store of 200 K and 1 M
// entities where 1 % hold three stamped history rows (backlog 30):
//
//   - build: the one-time cost of the first call (a scan of every history
//     value's temporal fields plus the install), the sidecar reset before
//     each call, and setB/id, the live heap the built sidecar retains per ID;
//   - hit / miss: an entity with / without history on a built sidecar.
//
// The 1 M fixtures are skipped under -short.
//
//	go test ./pkg/graph/store/badger/ -run '^$' -bench 'Benchmark(Rel|Node)HistoryStamps$' -benchmem
func BenchmarkRelHistoryStamps(b *testing.B)  { benchHistoryStamps(b, false) }
func BenchmarkNodeHistoryStamps(b *testing.B) { benchHistoryStamps(b, true) }

func benchHistoryStamps(b *testing.B, node bool) {
	const stride = 100 // 1 %
	for _, size := range []int{200_000, 1_000_000} {
		b.Run(fmt.Sprintf("entities=%dK/density=1%%", size/1000), func(b *testing.B) {
			if size > 200_000 && testing.Short() {
				b.Skip("1 M fixture skipped under -short")
			}
			bs := seedStampsBenchStore(b, node, stride, size)
			p := &bs.histRelStamps
			stamps := func(id int64) (types.Instant, bool, error) {
				f, _, has, err := bs.RelHistoryStamps(types.RelID(snowflake.ID(id)))
				return f, has, err
			}
			if node {
				p = &bs.histNodeStamps
				stamps = func(id int64) (types.Instant, bool, error) {
					f, _, has, err := bs.NodeHistoryStamps(types.NodeID(snowflake.ID(id)))
					return f, has, err
				}
			}
			b.Run("build", func(b *testing.B) {
				resetHistoryStampsForBench(bs, p)
				var before, after runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&before)
				if f, has, err := stamps(stride); err != nil || !has || f == 0 {
					b.Fatalf("build: %d, %v, %v", f, has, err)
				}
				runtime.GC()
				runtime.ReadMemStats(&after)
				st := bs.HistoryStampsStats()
				ids := st.NodeIDs + st.RelIDs
				perID := float64(int64(after.HeapAlloc)-int64(before.HeapAlloc)) / float64(ids)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					resetHistoryStampsForBench(bs, p)
					b.StartTimer()
					if _, has, err := stamps(stride); err != nil || !has {
						b.Fatalf("build: %v, %v", has, err)
					}
				}
				b.ReportMetric(float64(ids), "ids")
				b.ReportMetric(perID, "setB/id")
			})
			hits := make([]int64, 0, 1024)
			for id := stride; id <= size && len(hits) < 1024; id += stride {
				hits = append(hits, int64(id))
			}
			misses := plainHistoryBenchIDsN(stride, size)
			for _, tc := range []struct {
				name string
				ids  []int64
				want bool
			}{{"hit", hits, true}, {"miss", misses, false}} {
				b.Run(tc.name, func(b *testing.B) {
					if _, has, err := stamps(tc.ids[0]); err != nil || has != tc.want {
						b.Fatalf("warm-up: %v, %v", has, err)
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if _, has, err := stamps(tc.ids[i%len(tc.ids)]); err != nil || has != tc.want {
							b.Fatalf("id %d: %v, %v", tc.ids[i%len(tc.ids)], has, err)
						}
					}
				})
			}
		})
	}
}

// resetHistoryStampsForBench drops a built sidecar so the next call builds it
// again (benchmark only; no writer runs concurrently).
func resetHistoryStampsForBench(bs *Store, p *historyStamps) {
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

// seedStampsBenchStore writes entities current rows and, for every stride-th
// one, three history rows with distinct temporal stamps, then reopens.
func seedStampsBenchStore(b *testing.B, node bool, stride, entities int) *Store {
	b.Helper()
	dir := b.TempDir()
	bs, err := New(Config{Dir: dir})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	if err := seedHistoryBenchRows(bs, node, 0, entities); err != nil { // current rows only
		b.Fatal(err)
	}
	for i := stride; i <= entities; i += stride {
		for ver := uint32(0); ver < 3; ver++ {
			tm := &types.TemporalMetadata{ValidFrom: types.Instant(i), TxFrom: types.Instant(1_000_000 + i*10 + int(ver)), TxTo: types.Instant(1_000_000 + i*10 + int(ver) + 1)}
			if node {
				n := types.NewNode(types.NodeID(snowflake.ID(i)), 1, nil)
				n.SetVersion(ver)
				n.SetTemporal(tm)
				if err := bs.PutNodeVersion(n.ID(), ver, n); err != nil {
					b.Fatal(err)
				}
				continue
			}
			r := types.NewRelationship(types.RelID(snowflake.ID(i)), 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
			r.SetVersion(ver)
			r.SetTemporal(tm)
			if err := bs.PutRelVersion(r.ID(), ver, r); err != nil {
				b.Fatal(err)
			}
		}
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
