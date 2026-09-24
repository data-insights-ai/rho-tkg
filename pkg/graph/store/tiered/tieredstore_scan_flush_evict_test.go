package tiered

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestTieredNodesByLabel_ConcurrentReplaceAndFlush_NoDropNoStale runs the badger
// flush-evict scan race through the tiered store: every shard is a badger Store
// and a tiered label scan is a label scan per shard. A writer replaces rows
// across two shards, a goroutine flushes in a loop, and the per-shard cache is
// far smaller than the data. Each scan must return every row, none older than
// the version committed before the scan began.
func TestTieredNodesByLabel_ConcurrentReplaceAndFlush_NoDropNoStale(t *testing.T) {
	const rows = 2048
	const rounds = 12
	ts, err := New(Config{
		InMemory:      true,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1, // the test flushes
		CacheCapacity: 128,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	_, _, signalTok := installDefaultTestLabelRegistry(t, ts)

	mk := func(id types.NodeID, v int64) *types.Node {
		n := types.NewNode(id, signalTok, nil)
		if err := n.SetProperty("v", v); err != nil {
			t.Fatal(err)
		}
		return n
	}
	gen := tieredNodeGen(t)
	ids := make([]types.NodeID, rows)
	index := make(map[types.NodeID]int, rows)
	for i := range ids {
		if i == rows/2 {
			time.Sleep(2 * time.Millisecond)
			if err := ts.RotateHotShard(); err != nil { // second shard
				t.Fatalf("RotateHotShard: %v", err)
			}
		}
		ids[i] = types.NodeID(gen.Generate())
		index[ids[i]] = i
		if err := ts.PutNode(mk(ids[i], 1)); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
	}
	if err := ts.Flush(); err != nil {
		t.Fatal(err)
	}

	latest := make([]atomic.Int64, rows)
	for i := range latest {
		latest[i].Store(1)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var bgErr atomic.Value
	wg.Add(2)
	go func() {
		defer wg.Done()
		for k := 0; ; k++ {
			select {
			case <-stop:
				return
			default:
			}
			i := (k * 7919) % rows
			v := latest[i].Load() + 1
			if err := ts.ReplaceNode(mk(ids[i], v)); err != nil {
				bgErr.Store(err)
				return
			}
			latest[i].Store(v)
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := ts.Flush(); err != nil {
				bgErr.Store(err)
				return
			}
			runtime.Gosched()
		}
	}()
	defer func() {
		close(stop)
		wg.Wait()
		if err, _ := bgErr.Load().(error); err != nil {
			t.Fatalf("writer/flusher: %v", err)
		}
	}()

	for round := 0; round < rounds; round++ {
		floor := make([]int64, rows)
		for i := range floor {
			floor[i] = latest[i].Load()
		}
		nodes, err := ts.NodesByLabel(signalTok, QueryOpts{})
		if err != nil {
			t.Fatalf("round %d: NodesByLabel: %v", round, err)
		}
		stale := 0
		for _, n := range nodes {
			v, _ := n.GetProperty("v")
			if v.(int64) < floor[index[n.ID()]] {
				stale++
			}
		}
		if len(nodes) != rows || stale != 0 {
			t.Fatalf("round %d: NodesByLabel returned %d of %d rows, %d older than the version committed before the scan began", round, len(nodes), rows, stale)
		}
	}
}
