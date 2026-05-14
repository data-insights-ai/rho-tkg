package tiered

import (
	"errors"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestRollbackArchiveNodeStateRemovesDestinationNode(t *testing.T) {
	shard, err := NewBadgerStore(BadgerStoreConfig{InMemory: true})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
	}
	t.Cleanup(func() { _ = shard.Close() })

	nodeID := types.NodeID(901)
	node := types.NewNode(nodeID, 1, nil)
	if err := shard.PutNode(node); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	if err := rollbackArchiveNodeState(nil, shard, nodeID); err != nil {
		t.Fatalf("rollbackArchiveNodeState: %v", err)
	}
	if _, err := shard.GetNode(nodeID); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNode after rollback = %v, want ErrNodeNotFound", err)
	}
	if err := rollbackArchiveNodeState(nil, shard, nodeID); err != nil {
		t.Fatalf("rollbackArchiveNodeState missing node: %v", err)
	}
}

func TestAppendMissingNodeBucketSelectsOnlyMissingSnapshots(t *testing.T) {
	shard, err := NewBadgerStore(BadgerStoreConfig{InMemory: true})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
	}
	t.Cleanup(func() { _ = shard.Close() })

	missingA := types.NewNode(types.NodeID(1001), 1, nil)
	existing := types.NewNode(types.NodeID(1002), 1, nil)
	missingB := types.NewNode(types.NodeID(1003), 1, nil)
	if err := shard.PutNode(existing); err != nil {
		t.Fatalf("PutNode existing: %v", err)
	}

	nodesByID := map[types.NodeID]*types.Node{
		missingA.ID(): missingA,
		existing.ID(): existing,
		missingB.ID(): missingB,
	}
	buckets := appendMissingNodeBucket(nil, shard, []types.NodeID{
		missingA.ID(),
		existing.ID(),
		missingB.ID(),
		types.NodeID(1004),
	}, nodesByID)

	if len(buckets) != 1 {
		t.Fatalf("bucket count = %d, want 1", len(buckets))
	}
	if buckets[0].shard != shard {
		t.Fatal("bucket shard does not match input shard")
	}
	if got := nodeIDsOf(buckets[0].nodes); !sameNodeIDs(got, []types.NodeID{missingA.ID(), missingB.ID()}) {
		t.Fatalf("bucket node ids = %v, want [%d %d]", got, missingA.ID(), missingB.ID())
	}

	unchanged := appendMissingNodeBucket(buckets, shard, []types.NodeID{existing.ID(), types.NodeID(1004)}, nodesByID)
	if len(unchanged) != len(buckets) {
		t.Fatalf("all-present/unknown append changed bucket count from %d to %d", len(buckets), len(unchanged))
	}
}

func TestAppendMissingRelationshipsSelectsOnlyMissingSnapshots(t *testing.T) {
	shard, err := NewBadgerStore(BadgerStoreConfig{InMemory: true})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
	}
	t.Cleanup(func() { _ = shard.Close() })

	start := types.NewNode(types.NodeID(2001), 1, nil)
	end := types.NewNode(types.NodeID(2002), 1, nil)
	if err := shard.PutNode(start); err != nil {
		t.Fatalf("PutNode start: %v", err)
	}
	if err := shard.PutNode(end); err != nil {
		t.Fatalf("PutNode end: %v", err)
	}

	missingA := types.NewRelationship(types.RelID(3001), 1, start.ID(), end.ID())
	existing := types.NewRelationship(types.RelID(3002), 1, start.ID(), end.ID())
	missingB := types.NewRelationship(types.RelID(3003), 1, start.ID(), end.ID())
	if err := shard.PutRelationship(existing); err != nil {
		t.Fatalf("PutRelationship existing: %v", err)
	}

	relByID := map[types.RelID]*types.Relationship{
		missingA.ID(): missingA,
		existing.ID(): existing,
		missingB.ID(): missingB,
	}
	got := appendMissingRelationships(nil, []types.RelID{
		missingA.ID(),
		existing.ID(),
		missingB.ID(),
		types.RelID(3004),
	}, relByID, shard)

	if gotIDs := relIDsOf(got); !sameRelIDs(gotIDs, []types.RelID{missingA.ID(), missingB.ID()}) {
		t.Fatalf("relationship ids = %v, want [%d %d]", gotIDs, missingA.ID(), missingB.ID())
	}

	unchanged := appendMissingRelationships(got, []types.RelID{existing.ID(), types.RelID(3004)}, relByID, shard)
	if len(unchanged) != len(got) {
		t.Fatalf("all-present/unknown append changed relationship count from %d to %d", len(got), len(unchanged))
	}
}

func nodeIDsOf(nodes []*types.Node) []types.NodeID {
	out := make([]types.NodeID, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n.ID())
	}
	return out
}

func relIDsOf(rels []*types.Relationship) []types.RelID {
	out := make([]types.RelID, 0, len(rels))
	for _, r := range rels {
		out = append(out, r.ID())
	}
	return out
}

func sameNodeIDs(got, want []types.NodeID) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func sameRelIDs(got, want []types.RelID) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
