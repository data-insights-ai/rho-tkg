package tiered

import (
	"testing"

	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestRebuildCatalog_CountsClosedColdEventShard(t *testing.T) {
	ts := newDiskTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	hotName := ts.HotShardForTest().Name()
	if err := ts.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}
	demoteToCold(ts, hotName)
	if !ts.CatalogForTest().UpdateShardStats(hotName, 0, 0) {
		t.Fatalf("UpdateShardStats(%q) returned false", hotName)
	}

	cold := ts.EventShardsForTest()[hotName]
	cold.LockShardMuForTest()
	if cold.Store() != nil {
		if err := cold.Store().Close(); err != nil {
			cold.UnlockShardMuForTest()
			t.Fatalf("close cold store: %v", err)
		}
		cold.SetStoreForTest(nil)
	}
	cold.UnlockShardMuForTest()

	if err := ts.RebuildCatalog(); err != nil {
		t.Fatalf("RebuildCatalog: %v", err)
	}
	entry, ok := ts.CatalogForTest().GetShard(hotName)
	if !ok {
		t.Fatalf("GetShard(%q) returned false", hotName)
	}
	if entry.ApproxNodes != 1 || entry.ApproxRels != 0 {
		t.Fatalf("rebuilt cold shard stats = nodes %d rels %d, want 1/0", entry.ApproxNodes, entry.ApproxRels)
	}
}
