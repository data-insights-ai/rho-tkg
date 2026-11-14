package tiered

import (
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestColdOpen_OverCapHandleOpensTransient (R3 qualify, measure.txt section
// H): with MaxOpenColdShards > 0 a read of a closed cold shard that finds the
// cap full closes its handle again at check-in, every read, as a cap-0 handle
// does. It must open as cheaply: the small block cache and the lazy label maps
// and ID sets (coldReadCfg's transient form), not the full config a retained
// handle gets. The cap is filled by counting one more open cold shard, which is
// what another retained shard does to the count. A handle that fits under the
// cap keeps the full config, and the over-cap handle answers the same and
// closes at check-in.
func TestColdOpen_OverCapHandleOpensTransient(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name      string
		occupied  int64 // other cold shards already open
		wantCache int64
		wantLazy  bool
		wantKept  bool
	}{
		{"under the cap: retained, full config", 0, 64 << 20, false, true},
		{"cap full: transient config, closed at check-in", 1, transientCache, true, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			ts, name, n, _ := coldOpenFixture(t, func(cfg *Config) {
				cfg.MaxOpenColdShards = 1
				cfg.BlockCacheSize = 64 << 20
			})
			ts.openColdShards.Add(c.occupied)
			t.Cleanup(func() { ts.openColdShards.Add(-c.occupied) })
			store, release := readOnlyColdHandle(t, ts, name)
			cache := store.DBForTest().Opts().BlockCacheSize
			lazy, _ := store.LabelMapStateForTest()
			ids, err := store.NodeIDsByLabel(mintSignalTok)
			release()
			if cache != c.wantCache || lazy != c.wantLazy {
				t.Fatalf("read-only cold handle: block cache %d, lazy label maps %v; want %d, %v", cache, lazy, c.wantCache, c.wantLazy)
			}
			if err != nil || !slices.Equal(ids, []types.NodeID{n}) {
				t.Fatalf("NodeIDsByLabel = %v, %v; want [%d]", ids, err, n)
			}
			if kept, _ := ts.EventShardsForTest()[name].StoreStateForTest(); (kept != nil) != c.wantKept {
				t.Fatalf("after check-in the handle is open %v, want %v", kept != nil, c.wantKept)
			}
		})
	}
}
