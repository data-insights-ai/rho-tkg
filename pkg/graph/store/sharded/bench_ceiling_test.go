package sharded_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Ceiling benchmarks — the theoretical maximum insert rate when the CORE layer
// (validation, ID mint, hashing, locking, events, change-log, defensive copies)
// is stripped away and only the STORE write path runs, in a single process. Run:
//
//	go test -run '^$' -bench 'BenchmarkCeiling' -benchmem -count=5 \
//	    ./pkg/graph/store/sharded/
//
// These answer "how fast could ingestion possibly go on this box" and bound how
// much the core layer + allocation work can ever recover.

const ceilBatch = 256

// makeNodes builds n fully-formed nodes with a single property, IDs offset so
// distinct goroutines never collide. Construction is OUTSIDE the timed region.
func makeNodes(n, idBase int) []*types.Node {
	out := make([]*types.Node, n)
	for i := 0; i < n; i++ {
		id := types.NodeID(int64(idBase + i + 1))
		nd := types.NewNode(id, 1, nil)
		ps, _ := types.NewPropertySlice(map[string]any{"i": int64(i)})
		_ = nd.SetProperties(ps)
		out[i] = nd
	}
	return out
}

// BenchmarkCeilingBareShardedMap is the absolute floor of "put a *Node
// somewhere addressable": a lane-striped Go map, no store, no wire, no locks
// beyond one mutex per stripe. This is the hardware/allocator ceiling — nothing
// real can beat it.
func BenchmarkCeilingBareShardedMap(b *testing.B) {
	const stripes = 16
	type shard struct {
		mu sync.Mutex
		m  map[types.NodeID]*types.Node
	}
	shards := make([]*shard, stripes)
	for i := range shards {
		shards[i] = &shard{m: make(map[types.NodeID]*types.Node, b.N/stripes+1)}
	}
	nodes := makeNodes(b.N, 0)

	b.ResetTimer()
	var wg sync.WaitGroup
	for w := 0; w < stripes; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := w; i < len(nodes); i += stripes {
				sh := shards[int(uint64(nodes[i].ID()))%stripes]
				sh.mu.Lock()
				sh.m[nodes[i].ID()] = nodes[i]
				sh.mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	b.StopTimer()
	if s := b.Elapsed().Seconds(); s > 0 {
		b.ReportMetric(float64(b.N)/s, "inserts/s")
	}
}

// BenchmarkCeilingMemoryStoreP is the memory.Store's own write ceiling (Go maps
// + freeze + defensive copy under ONE store mutex), driven by p goroutines with
// NO core layer. p>1 reveals the single-store write-mutex wall directly.
func BenchmarkCeilingMemoryStoreP(b *testing.B) {
	for _, p := range []int{1, 8} {
		b.Run(fmt.Sprintf("p-%d", p), func(b *testing.B) {
			st := memory.New()
			defer func() { _ = st.Close() }()
			batches := makeBatches(b.N, p)
			b.ResetTimer()
			runStoreBatches(b, p, batches, func(nodes []*types.Node) error {
				return st.PutNodesBatch(nodes)
			})
		})
	}
}

// BenchmarkCeilingSingleBadgerP is a single in-memory badger store's write
// ceiling (LSM skiplist / memtable arena) under p goroutines, NO core layer.
func BenchmarkCeilingSingleBadgerP(b *testing.B) {
	for _, p := range []int{1, 8} {
		b.Run(fmt.Sprintf("p-%d", p), func(b *testing.B) {
			st, err := badger.New(badger.Config{InMemory: true})
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = st.Close() }()
			batches := makeBatches(b.N, p)
			b.ResetTimer()
			runStoreBatches(b, p, batches, func(nodes []*types.Node) error {
				return st.PutNodesBatch(nodes)
			})
		})
	}
}

// BenchmarkCeilingShardedBadger is the "perfect sharding" store ceiling: p
// INDEPENDENT in-memory badger stores, one goroutine each, NO core layer and NO
// cross-store coordination. This is the most a p-way badger deployment could do
// if the core layer were free.
func BenchmarkCeilingShardedBadger(b *testing.B) {
	const p = 8
	stores := make([]*badger.Store, p)
	for i := range stores {
		st, err := badger.New(badger.Config{InMemory: true})
		if err != nil {
			b.Fatal(err)
		}
		stores[i] = st
	}
	defer func() {
		for _, st := range stores {
			_ = st.Close()
		}
	}()
	batches := makeBatches(b.N, p)
	b.ResetTimer()
	var wg sync.WaitGroup
	for w := 0; w < p; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for _, nb := range batches[w] {
				if err := stores[w].PutNodesBatch(nb); err != nil {
					b.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	b.StopTimer()
	if s := b.Elapsed().Seconds(); s > 0 {
		b.ReportMetric(float64(b.N)/s, "inserts/s")
	}
}

// makeBatches splits b.N nodes into p goroutines' worth of ceilBatch-sized
// batches, with disjoint ID ranges so no two goroutines/stores collide.
func makeBatches(total, p int) [][][]*types.Node {
	out := make([][][]*types.Node, p)
	per := total / p
	rem := total % p
	idBase := 0
	for w := 0; w < p; w++ {
		count := per
		if w < rem {
			count++
		}
		all := makeNodes(count, idBase)
		idBase += count + 1
		for i := 0; i < len(all); i += ceilBatch {
			end := i + ceilBatch
			if end > len(all) {
				end = len(all)
			}
			out[w] = append(out[w], all[i:end])
		}
	}
	return out
}

func runStoreBatches(b *testing.B, p int, batches [][][]*types.Node, put func([]*types.Node) error) {
	var wg sync.WaitGroup
	for w := 0; w < p; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for _, nb := range batches[w] {
				if err := put(nb); err != nil {
					b.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	b.StopTimer()
	if s := b.Elapsed().Seconds(); s > 0 {
		b.ReportMetric(float64(b.N)/s, "inserts/s")
	}
}
