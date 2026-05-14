package core

import (
	"context"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/tiered"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func newDiskTieredGraph(t *testing.T) (*Core, *tiered.Store) {
	t.Helper()
	ts, err := tiered.New(tiered.Config{
		DataDir:       t.TempDir(),
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	g, err := New(Config{
		SnowflakeNodeID: 0,
		Store:           ts,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g, ts
}

func newTieredGraphWithClosedColdShard(t *testing.T) (*Core, *tiered.Store, *tiered.EventShard) {
	t.Helper()
	g, ts := newTestTieredGraph(t)

	ts.MuForTest().RLock()
	originName := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()

	time.Sleep(2 * time.Millisecond)
	if err := ts.RotateHotShard(); err != nil {
		t.Fatal(err)
	}

	demoteToCold(ts, originName)
	cold := eventShardByName(t, ts, originName)
	closeEventShardStore(t, cold)
	return g, ts, cold
}

func eventShardByName(t *testing.T, ts *tiered.Store, name string) *tiered.EventShard {
	t.Helper()
	ts.MuForTest().RLock()
	defer ts.MuForTest().RUnlock()
	es := ts.EventShardsForTest()[name]
	if es == nil {
		t.Fatalf("event shard %q not found", name)
	}
	return es
}

func closeEventShardStore(t *testing.T, es *tiered.EventShard) {
	t.Helper()
	es.LockShardMuForTest()
	defer es.UnlockShardMuForTest()
	if es.Store() == nil {
		return
	}
	if err := es.Store().Close(); err != nil {
		t.Fatalf("close cold shard store: %v", err)
	}
	es.SetStoreForTest(nil)
}

func assertColdShardStillClosed(t *testing.T, es *tiered.EventShard, op string) {
	t.Helper()
	es.LockShardMuForTest()
	open := es.Store() != nil
	es.UnlockShardMuForTest()
	if open {
		t.Fatalf("%s opened a cold shard that should not be probed", op)
	}
}

func containsRelIDSlice(ids []snowflake.ID, want snowflake.ID) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

func containsNodeIDValue(ids []types.NodeID, want types.NodeID) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

func containsRelIDValue(ids []types.RelID, want types.RelID) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

func containsNodeEntity(nodes []*types.Node, want types.NodeID) bool {
	for _, n := range nodes {
		if n.InternalID() == want {
			return true
		}
	}
	return false
}

func containsRelEntity(rels []*types.Relationship, want types.RelID) bool {
	for _, r := range rels {
		if r.InternalID() == want {
			return true
		}
	}
	return false
}

func mustArchivedRelationshipFixture(t *testing.T) (*tiered.Store, types.RelID, uint16) {
	t.Helper()

	ts := newTestTieredStore(t)
	g, err := New(Config{
		SnowflakeNodeID: 0,
		Store:           ts,
		Validation:      ValidationLimits{AllowSelfLoops: true},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	node, err := g.Nodes.Add(context.Background(), []string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	rel, err := g.Rels.Add(context.Background(), "KNOWS", node, node, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	relID := rel.InternalID()

	if err := ts.ArchiveNode(node.InternalID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	archive := ts.RefArchiveForTest().Load()
	if archive == nil {
		t.Fatal("ArchiveNode left refArchive nil")
	}
	if !archive.HasRelID(relID.SnowflakeID()) {
		t.Fatal("setup: ArchiveNode did not move self-loop relationship into refArchive")
	}

	knowsTok, ok := g.Resolve.LookupRelType("KNOWS")
	if !ok {
		t.Fatal("KNOWS reltype not registered")
	}
	return ts, relID, knowsTok
}
