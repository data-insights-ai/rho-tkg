package tiered

import (
	"fmt"
	"slices"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestTieredConfig_LabelIndexOnDisk_Propagates pins TIERLABELDISK (sigma, 24
// Oct): tiered.Config.LabelIndexOnDisk reaches the reference, hot,
// rotation-created, warm, archive and a lazily opened cold shard, each of
// which then keeps no label->nodes map in RAM; with the flag off every shard
// keeps it. Catches a shard kind opened without the flag.
func TestTieredConfig_LabelIndexOnDisk_Propagates(t *testing.T) {
	t.Parallel()
	for _, on := range []bool{true, false} {
		t.Run(fmt.Sprintf("flag=%v", on), func(t *testing.T) {
			t.Parallel()
			cfg := Config{
				DataDir:          t.TempDir(),
				RefLabels:        []string{"Case"},
				ShardWindow:      7 * 24 * time.Hour,
				FlushInterval:    1<<63 - 1,
				LabelIndexOnDisk: on,
			}
			ts, err := New(cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			reg := registrypkg.NewLabelRegistry()
			ts.SetLabelRegistry(reg)
			caseTok, _ := reg.GetOrCreate("Case")
			sigTok, _ := reg.GetOrCreate("Signal")
			gen := tieredNodeGen(t)
			put := func(tok uint16) {
				t.Helper()
				if err := ts.PutNode(types.NewNode(types.NodeID(gen.Generate()), tok, nil)); err != nil {
					t.Fatalf("PutNode: %v", err)
				}
			}
			check := func(name string, s *BadgerStore) {
				t.Helper()
				if s == nil {
					t.Fatalf("%s shard not open", name)
				}
				got, labelN := s.LabelIndexOnDiskForTest()
				if got != on {
					t.Errorf("%s shard LabelIndexOnDisk = %v, want %v", name, got, on)
				}
				if on && labelN != 0 {
					t.Errorf("%s shard holds %d label map entries in disk mode", name, labelN)
				}
			}
			put(caseTok)
			put(sigTok)
			check("reference", ts.RefShardForTest())
			ts.mu.RLock()
			first := ts.hotShard.name
			check("hot", ts.hotShard.Store())
			ts.mu.RUnlock()
			forceRotation(t, ts)
			put(sigTok)
			ts.mu.RLock()
			check("rotation-created hot", ts.hotShard.Store())
			ts.mu.RUnlock()
			check("warm", eventShardNamed(t, ts, first).store)
			if err := ts.EnsureRefArchiveForTest(); err != nil {
				t.Fatalf("archive: %v", err)
			}
			check("archive", ts.refArchive.Load())
			if err := ts.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			time.Sleep(5 * time.Millisecond)
			coldCfg := cfg
			coldCfg.ColdAfter = time.Millisecond
			ts, err = New(coldCfg)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer func() { _ = ts.Close() }()
			s, release, err := ts.ResolveShardStoreForTest(first)
			if err != nil {
				t.Fatalf("lazy open: %v", err)
			}
			check("lazy cold", s)
			release()
		})
	}
}

// TestTieredLabelIndexOnDisk_Parity: the same writes over a rotation answer the
// label doors (NodesByLabel, NodeCountByLabel, AllNodeIDs) the same with the
// flag on as off.
func TestTieredLabelIndexOnDisk_Parity(t *testing.T) {
	t.Parallel()
	answers := func(on bool) []string {
		ts, err := New(Config{DataDir: t.TempDir(), RefLabels: []string{"Case"},
			ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1, LabelIndexOnDisk: on})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		defer func() { _ = ts.Close() }()
		reg := registrypkg.NewLabelRegistry()
		ts.SetLabelRegistry(reg)
		caseTok, _ := reg.GetOrCreate("Case")
		sigTok, _ := reg.GetOrCreate("Signal")
		gen := tieredNodeGen(t)
		var ids []types.NodeID
		for round := 0; round < 2; round++ {
			for i := 0; i < 5; i++ {
				for _, tok := range []uint16{caseTok, sigTok} {
					n := types.NewNode(types.NodeID(gen.Generate()), tok, nil)
					if err := ts.PutNode(n); err != nil {
						t.Fatalf("PutNode: %v", err)
					}
					ids = append(ids, n.ID())
				}
			}
			if err := ts.DeleteNode(ids[len(ids)-1]); err != nil {
				t.Fatalf("DeleteNode: %v", err)
			}
			forceRotation(t, ts)
		}
		var out []string
		for _, tok := range []uint16{caseTok, sigTok} {
			nodes, err := ts.NodesByLabel(tok, QueryOpts{})
			if err != nil {
				t.Fatalf("NodesByLabel: %v", err)
			}
			var got []int64
			for _, n := range nodes {
				got = append(got, int64(n.ID()))
			}
			slices.Sort(got)
			count, err := ts.NodeCountByLabel(tok)
			if err != nil {
				t.Fatalf("NodeCountByLabel: %v", err)
			}
			out = append(out, fmt.Sprintf("label %d: %d nodes, count %d", tok, len(got), count))
		}
		all, err := ts.AllNodeIDs(QueryOpts{})
		if err != nil {
			t.Fatalf("AllNodeIDs: %v", err)
		}
		return append(out, fmt.Sprintf("all: %d", len(all)))
	}
	off, on := answers(false), answers(true)
	if !slices.Equal(off, on) {
		t.Fatalf("label doors differ: off %v, on %v", off, on)
	}
}
