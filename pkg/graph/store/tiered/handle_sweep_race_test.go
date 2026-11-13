//go:build race

package tiered

import (
	"sync"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestHandleSweep_RaceStress pins R3 T-f under the race detector: rotations
// (each one a kick), lock-free reads of warm and demoted shards, owner writes
// into them and sweep passes run together for a fixed number of steps. Every
// node reads back with its last write at the end, and the detector reports no
// unsynchronised handle access (es.store read beside the swap's write).
func TestHandleSweep_RaceStress(t *testing.T) {
	cfg := Config{DataDir: t.TempDir(), RefLabels: []string{"Case"}, ShardWindow: 24 * time.Hour,
		FlushInterval: 1<<63 - 1, WarmMemTableSize: 8 << 20, ColdAfter: 30 * time.Millisecond, MaxOpenColdShards: 2}
	ts, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts.Close() }()
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	tok, _ := reg.GetOrCreate("Signal")
	gen := tieredNodeGen(t)

	const rotations, perShard, writers, readers, steps = 6, 4, 2, 4, 150
	var nodes []*types.Node
	for r := 0; r < rotations; r++ {
		for i := 0; i < perShard; i++ {
			n := types.NewNode(types.NodeID(gen.Generate()), tok, nil)
			if err := ts.PutNode(n); err != nil {
				t.Fatal(err)
			}
			nodes = append(nodes, n)
		}
		if err := ts.RotateHotShard(); err != nil {
			t.Fatal(err)
		}
	}

	errs := make(chan error, 64)
	var wg sync.WaitGroup
	run := func(f func(i int) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < steps; i++ {
				if err := f(i); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	run(func(i int) error { // rotations: each demotes, each kicks the sweep
		if i%25 != 0 {
			return nil
		}
		time.Sleep(time.Millisecond)
		return ts.RotateHotShard()
	})
	run(func(int) error { ts.SweepHandlesForTest(); return nil })
	for r := 0; r < readers; r++ {
		run(func(i int) error {
			_, err := ts.GetNode(nodes[(i*7+r)%len(nodes)].ID())
			return err
		})
	}
	// Each writer owns its own nodes, so the last value per node is known.
	last := make([]int, len(nodes))
	for w := 0; w < writers; w++ {
		run(func(i int) error {
			k := (i*writers + w) % len(nodes)
			if k%writers != w {
				return nil
			}
			n := types.NewNode(nodes[k].ID(), tok, nil)
			if err := n.SetProperty("v", int64(i)); err != nil {
				return err
			}
			if err := ts.ReplaceNode(n); err != nil {
				return err
			}
			last[k] = i
			return nil
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("stress step: %v", err)
	}
	ts.SweepHandlesForTest()
	for k, n := range nodes {
		got, err := ts.GetNode(n.ID())
		if err != nil {
			t.Fatalf("node %d unreadable after the stress: %v", k, err)
		}
		if last[k] == 0 {
			continue
		}
		if v, _ := got.GetProperty("v"); v != int64(last[k]) {
			t.Fatalf("node %d holds v=%v, want its last write %d", k, v, last[k])
		}
	}
	swaps, closes := ts.HandleSweepCountsForTest()
	t.Logf("swaps %d, closes %d", swaps, closes)
}
