package tiered

import (
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// An in-memory shard's BadgerStore is the only copy of its rows: closing it
// and lazy-opening the path again yields an empty store. A demoted cold
// shard of an InMemory store must therefore never be closed — not by the
// idle sweep, not by the open-handle cap — or its rows vanish (the twin
// differential's "close idle cold shards" step, seed 26800, lost a node on
// the twin whose sweep came a millisecond later). A disk store still closes
// the shard and reads the rows back from disk.
func TestTieredStore_ColdShardClose_KeepsRows(t *testing.T) {
	for _, tc := range []struct {
		name      string
		open      func(t *testing.T) *Store
		wantClose bool
	}{
		{"in-memory", newTestTieredStore, false},
		{"disk", newDiskTestTieredStore, true},
	} {
		for _, sweep := range []string{"idle", "cap"} {
			t.Run(tc.name+"/"+sweep, func(t *testing.T) {
				ts := tc.open(t)
				_, _, signalTok := installDefaultTestLabelRegistry(t, ts)
				gen := tieredNodeGen(t)
				var ids []types.NodeID
				var cold []*EventShard
				// Two demoted shards, so the cap of one has a shard to evict.
				for range 2 {
					id := types.NodeID(gen.Generate())
					if err := ts.PutNode(types.NewNode(id, signalTok, nil)); err != nil {
						t.Fatal(err)
					}
					ids = append(ids, id)
					ts.MuForTest().RLock()
					hot := ts.HotShardForTest()
					ts.MuForTest().RUnlock()
					time.Sleep(2 * time.Millisecond)
					if err := ts.RotateHotShard(); err != nil {
						t.Fatal(err)
					}
					demoteToCold(ts, hot.Name())
					cold = append(cold, hot)
				}
				for _, es := range cold {
					es.SetLastAccessForTest(0)
				}
				switch sweep {
				case "idle":
					ts.SetIdleTimeoutForTest(0)
					ts.CloseIdleShardsForTest()
				case "cap":
					ts.maxOpenColdShards = 1
					ts.openColdShards.Store(2)
					ts.enforceColdShardCap()
				}
				closed := false
				for _, es := range cold {
					es.LockShardMuForTest()
					closed = closed || es.Store() == nil
					es.UnlockShardMuForTest()
				}
				if closed != tc.wantClose {
					t.Errorf("a cold shard closed = %v, want %v", closed, tc.wantClose)
				}
				for _, id := range ids {
					if _, err := ts.GetNode(id); err != nil {
						t.Fatalf("GetNode(%v) after the %s sweep: %v", id, sweep, err)
					}
				}
			})
		}
	}
}
