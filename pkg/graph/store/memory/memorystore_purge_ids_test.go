package memory

import (
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestMemoryPurgeNodesByIDs (PURGEIDS): the named nodes leave with their edges
// and history (a duplicate and a missing ID are skipped), a third node of the
// label stays, and more than PurgeByIDsChunk IDs is refused.
func TestMemoryPurgeNodesByIDs(t *testing.T) {
	t.Parallel()
	ms := New()
	const label, relType = uint16(10), uint16(5)
	hub := types.NodeID(idAtTimeField(1000, 7))
	if err := ms.PutNode(types.NewNode(hub, 11, nil)); err != nil {
		t.Fatal(err)
	}
	var ids []types.NodeID
	for i := uint64(1); i <= 3; i++ {
		id := types.NodeID(idAtTimeField(1000, i))
		cur := types.NewNode(id, label, nil)
		cur.SetVersion(1)
		if err := ms.PutNode(cur); err != nil {
			t.Fatal(err)
		}
		hist := types.NewNode(id, label, nil)
		if err := ms.PutNodeVersion(id, 0, hist); err != nil {
			t.Fatal(err)
		}
		if err := ms.PutRelationship(types.NewRelationship(types.RelID(idAtTimeField(1000, 100+i)), relType, id, hub)); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	res, err := ms.PurgeNodesByIDs([]types.NodeID{ids[0], ids[1], ids[0], types.NodeID(idAtTimeField(5000, 1))}, nil)
	if err != nil || res.NodesPurged != 2 || res.RelsPurged != 2 {
		t.Fatalf("purge: %+v %v, want 2 nodes and 2 edges", res, err)
	}
	for _, id := range ids[:2] {
		if _, err := ms.GetNode(id); err == nil {
			t.Fatal("purged node survived")
		}
		if h, _ := ms.GetNodeHistory(id); len(h) != 0 {
			t.Fatalf("purged node keeps %d history rows", len(h))
		}
	}
	if _, err := ms.GetNode(ids[2]); err != nil {
		t.Fatalf("third node: %v", err)
	}
	if _, err := ms.PurgeNodesByIDs(make([]types.NodeID, storecontract.PurgeByIDsChunk+1), nil); err == nil {
		t.Fatal("PurgeByIDsChunk+1 IDs accepted")
	}
}
