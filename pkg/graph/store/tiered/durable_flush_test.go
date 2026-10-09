package tiered

import (
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Catches: a DurableFlush built on Flush / forEachOpenShard, which lazy-opens
// every closed cold shard on each commit (a durable commit would then cost one
// Badger open per historical shard). A closed cold shard has no pending writes,
// so it must be skipped. MaxOpenColdShards keeps a lazily opened shard open, so
// such an open stays observable after the call.
func TestDurableFlush_DoesNotOpenClosedColdShard(t *testing.T) {
	ts, err := New(Config{
		DataDir:           t.TempDir(),
		RefLabels:         []string{"Case"},
		ShardWindow:       7 * 24 * time.Hour,
		FlushInterval:     1<<63 - 1,
		MaxOpenColdShards: 8,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	signalTok, _ := reg.GetOrCreate("Signal")
	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), signalTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	coldName := ts.HotShardForTest().Name()
	forceRotation(t, ts)
	demoteToCold(ts, coldName)
	cold := ts.EventShardsForTest()[coldName]
	cold.LockShardMuForTest()
	if cold.Store() != nil {
		if err := cold.Store().Close(); err != nil {
			cold.UnlockShardMuForTest()
			t.Fatalf("close cold store: %v", err)
		}
		cold.SetStoreForTest(nil)
	}
	cold.UnlockShardMuForTest()

	if err := ts.DurableFlush(); err != nil {
		t.Fatalf("DurableFlush: %v", err)
	}
	assertColdShardClosedForTest(t, cold, "DurableFlush")
}
