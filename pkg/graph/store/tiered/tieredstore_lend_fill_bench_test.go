package tiered

import (
	"strings"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkTieredLendSortedRepeat (LENDCOST round 2): tiered LendNodesSorted
// of 4 or 64 IDs 100 rows apart on the hot shard (16 Ki flushed nodes of
// ~512 B, in memory). "cold": the shard caches emptied before every call
// (untimed); "repeat": the same IDs every call, sigma's re-lend of its
// blocks each round, which the cache serves once the first call filled it.
func BenchmarkTieredLendSortedRepeat(b *testing.B) {
	ts, err := New(mintCfg("", false))
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = ts.Close() }()
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	signal := uint16(0)
	for _, l := range []string{"Case", "User", "Signal"} {
		if signal, err = reg.GetOrCreate(l); err != nil {
			b.Fatal(err)
		}
	}
	gen := newTestGen(b, 0)
	const n = 16 << 10
	all := make([]types.NodeID, n)
	fill := strings.Repeat("x", 512)
	for i := range all {
		all[i] = types.NodeID(gen.Generate())
		nd := types.NewNode(all[i], signal, nil)
		if err := nd.SetProperty("p", fill); err != nil {
			b.Fatal(err)
		}
		if err := ts.PutNode(nd); err != nil {
			b.Fatal(err)
		}
	}
	if err := ts.Flush(); err != nil {
		b.Fatal(err)
	}
	reset := func() {
		for _, s := range []*BadgerStore{ts.refShard, ts.hotShard.store} {
			s.NodeCacheForTest().ResetForTest()
		}
	}
	for _, k := range []int{4, 64} {
		ids := make([]types.NodeID, k)
		for i := range ids {
			ids[i] = all[100*i]
		}
		out := make([]*types.Node, k)
		name := map[int]string{4: "4", 64: "64"}[k]
		b.Run("cold"+name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				reset()
				b.StartTimer()
				if err := ts.LendNodesSorted(ids, out); err != nil || out[k-1] == nil {
					b.Fatal(err)
				}
			}
		})
		reset()
		b.Run("repeat"+name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := ts.LendNodesSorted(ids, out); err != nil || out[k-1] == nil {
					b.Fatal(err)
				}
			}
		})
	}
}
