package graph

import (
	"errors"
	"testing"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- TestDeleteRelWithHistory ---

func TestDeleteRelWithHistory_HistoryAndLiveConsistent(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	defer ms.Close() //nolint:errcheck

	nA := types.NewNode(types.NodeID(10), 1, nil)
	nB := types.NewNode(types.NodeID(20), 2, nil)
	if err := ms.PutNode(nA); err != nil {
		t.Fatalf("PutNode A: %v", err)
	}
	if err := ms.PutNode(nB); err != nil {
		t.Fatalf("PutNode B: %v", err)
	}

	r := types.NewRelationship(types.RelID(100), 1, types.NodeID(10), types.NodeID(20))
	if err := ms.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	// Build tombstone.
	now := types.Instant(time.Now().UnixMilli())
	tombR := r.DeepCopy()
	tm := tombR.Temporal()
	if tm == nil {
		tm = &types.TemporalMetadata{}
		tombR.SetTemporal(tm)
	}
	tm.DeletedAt = now
	tm.ValidTo = now
	tm.TxFrom = now
	tm.TxTo = now

	prevVersion := r.Version()
	if err := ms.DeleteRelWithHistory(types.RelID(100), prevVersion, tombR); err != nil {
		t.Fatalf("DeleteRelWithHistory: %v", err)
	}

	// (a) Rel must be gone from live store.
	if _, err := ms.GetRelationship(types.RelID(100)); !errors.Is(err, ErrRelNotFound) {
		t.Errorf("GetRelationship after delete: got %v, want ErrRelNotFound", err)
	}

	// (b) History entry must exist with tombstone fields.
	hist, err := ms.GetRelVersion(types.RelID(100), prevVersion)
	if err != nil {
		t.Fatalf("GetRelVersion: %v", err)
	}
	if hist == nil {
		t.Fatal("GetRelVersion returned nil")
	}
	htm := hist.Temporal()
	if htm == nil {
		t.Fatal("tombstone Temporal is nil")
	}
	if htm.DeletedAt == 0 {
		t.Error("tombstone DeletedAt not set")
	}
	if htm.ValidTo == 0 {
		t.Error("tombstone ValidTo not set")
	}
}

func TestDeleteRelWithHistory_NotFound(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	defer ms.Close() //nolint:errcheck

	r := types.NewRelationship(types.RelID(999), 1, types.NodeID(1), types.NodeID(2))
	tombR := r.DeepCopy()

	err := ms.DeleteRelWithHistory(types.RelID(999), 0, tombR)
	if !errors.Is(err, ErrRelNotFound) {
		t.Errorf("DeleteRelWithHistory on missing rel: got %v, want ErrRelNotFound", err)
	}
}

// --- TestDeleteNodeWithHistory ---

func TestDeleteNodeWithHistory_HistoryAndLiveConsistent(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	defer ms.Close() //nolint:errcheck

	// Node to delete.
	nA := types.NewNode(types.NodeID(10), 1, nil)
	// Two connected nodes.
	nB := types.NewNode(types.NodeID(20), 2, nil)
	nC := types.NewNode(types.NodeID(30), 3, nil)
	for _, n := range []*types.Node{nA, nB, nC} {
		if err := ms.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
	}

	r1 := types.NewRelationship(types.RelID(100), 1, types.NodeID(10), types.NodeID(20))
	r2 := types.NewRelationship(types.RelID(200), 1, types.NodeID(30), types.NodeID(10))
	for _, r := range []*types.Relationship{r1, r2} {
		if err := ms.PutRelationship(r); err != nil {
			t.Fatalf("PutRelationship: %v", err)
		}
	}

	now := types.Instant(time.Now().UnixMilli())

	// Build rel tombstones.
	relTombstones := make([]RelTombstone, 0, 2)
	for _, r := range []*types.Relationship{r1, r2} {
		tombR := r.DeepCopy()
		tm := tombR.Temporal()
		if tm == nil {
			tm = &types.TemporalMetadata{}
			tombR.SetTemporal(tm)
		}
		tm.DeletedAt = now
		tm.ValidTo = now
		tm.TxFrom = now
		tm.TxTo = now
		relTombstones = append(relTombstones, RelTombstone{
			ID:          r.ID(),
			PrevVersion: r.Version(),
			Tombstone:   tombR,
		})
	}

	// Build node tombstone.
	tombN := nA.DeepCopy()
	tmN := tombN.Temporal()
	if tmN == nil {
		tmN = &types.TemporalMetadata{}
		tombN.SetTemporal(tmN)
	}
	tmN.DeletedAt = now
	tmN.ValidTo = now
	tmN.TxFrom = now
	tmN.TxTo = now

	prevNodeVersion := nA.Version()
	if err := ms.DeleteNodeWithHistory(types.NodeID(10), prevNodeVersion, tombN, relTombstones); err != nil {
		t.Fatalf("DeleteNodeWithHistory: %v", err)
	}

	// Node must be gone.
	if _, err := ms.GetNode(types.NodeID(10)); !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("GetNode after delete: got %v, want ErrNodeNotFound", err)
	}
	// Both rels must be gone.
	for _, id := range []types.RelID{100, 200} {
		if _, err := ms.GetRelationship(id); !errors.Is(err, ErrRelNotFound) {
			t.Errorf("GetRelationship(%d) after delete: got %v, want ErrRelNotFound", id, err)
		}
	}

	// Node history entry must exist.
	nodeHist, err := ms.GetNodeVersion(types.NodeID(10), prevNodeVersion)
	if err != nil {
		t.Fatalf("GetNodeVersion: %v", err)
	}
	if nodeHist.Temporal() == nil || nodeHist.Temporal().DeletedAt == 0 {
		t.Error("node tombstone DeletedAt not set in history")
	}

	// Rel history entries must exist.
	for _, rt := range relTombstones {
		relHist, err := ms.GetRelVersion(rt.ID, rt.PrevVersion)
		if err != nil {
			t.Fatalf("GetRelVersion(%d): %v", rt.ID, err)
		}
		if relHist.Temporal() == nil || relHist.Temporal().DeletedAt == 0 {
			t.Errorf("rel tombstone %d DeletedAt not set in history", rt.ID)
		}
	}
}

func TestDeleteNodeWithHistory_EmptyRelTombstones(t *testing.T) {
	t.Parallel()

	ms := NewMemoryStore()
	defer ms.Close() //nolint:errcheck

	n := types.NewNode(types.NodeID(10), 1, nil)
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	now := types.Instant(time.Now().UnixMilli())
	tombN := n.DeepCopy()
	tmN := tombN.Temporal()
	if tmN == nil {
		tmN = &types.TemporalMetadata{}
		tombN.SetTemporal(tmN)
	}
	tmN.DeletedAt = now
	tmN.ValidTo = now
	tmN.TxFrom = now
	tmN.TxTo = now

	// nil relTombstones — node has no connected rels.
	if err := ms.DeleteNodeWithHistory(types.NodeID(10), n.Version(), tombN, nil); err != nil {
		t.Fatalf("DeleteNodeWithHistory with nil relTombstones: %v", err)
	}

	if _, err := ms.GetNode(types.NodeID(10)); !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("GetNode after delete: got %v, want ErrNodeNotFound", err)
	}
	hist, err := ms.GetNodeVersion(types.NodeID(10), n.Version())
	if err != nil {
		t.Fatalf("GetNodeVersion: %v", err)
	}
	if hist.Temporal() == nil || hist.Temporal().DeletedAt == 0 {
		t.Error("node tombstone DeletedAt not set in history")
	}
}

func TestDeleteNodeWithHistory_BadgerStore(t *testing.T) {
	t.Parallel()

	bs, err := NewBadgerStore(BadgerStoreConfig{InMemory: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
	}
	defer bs.Close() //nolint:errcheck

	nA := putTestNode(t, bs, 10, 1, nil)
	nB := putTestNode(t, bs, 20, 2, nil)
	nC := putTestNode(t, bs, 30, 3, nil)

	r1 := putTestRel(t, bs, 100, 1, 10, 20)
	r2 := putTestRel(t, bs, 200, 1, int64(nC.ID()), 10)

	now := types.Instant(time.Now().UnixMilli())

	// Build rel tombstones.
	relTombstones := make([]RelTombstone, 0, 2)
	for _, r := range []*types.Relationship{r1, r2} {
		tombR := r.DeepCopy()
		tm := tombR.Temporal()
		if tm == nil {
			tm = &types.TemporalMetadata{}
			tombR.SetTemporal(tm)
		}
		tm.DeletedAt = now
		tm.ValidTo = now
		tm.TxFrom = now
		tm.TxTo = now
		relTombstones = append(relTombstones, RelTombstone{
			ID:          r.ID(),
			PrevVersion: r.Version(),
			Tombstone:   tombR,
		})
	}

	tombN := nA.DeepCopy()
	tmN := tombN.Temporal()
	if tmN == nil {
		tmN = &types.TemporalMetadata{}
		tombN.SetTemporal(tmN)
	}
	tmN.DeletedAt = now
	tmN.ValidTo = now
	tmN.TxFrom = now
	tmN.TxTo = now

	prevNodeVersion := nA.Version()
	if err := bs.DeleteNodeWithHistory(nA.ID(), prevNodeVersion, tombN, relTombstones); err != nil {
		t.Fatalf("DeleteNodeWithHistory: %v", err)
	}

	// SyncWrites=true — no explicit flush needed.
	// Node gone.
	if _, err := bs.GetNode(nA.ID()); !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("GetNode after delete: got %v, want ErrNodeNotFound", err)
	}
	// Rels gone.
	for _, id := range []types.RelID{100, 200} {
		if _, err := bs.GetRelationship(id); !errors.Is(err, ErrRelNotFound) {
			t.Errorf("GetRelationship(%d) after delete: got %v, want ErrRelNotFound", id, err)
		}
	}
	// Node history present.
	nodeHist, err := bs.GetNodeVersion(nA.ID(), prevNodeVersion)
	if err != nil {
		t.Fatalf("GetNodeVersion: %v", err)
	}
	if nodeHist.Temporal() == nil || nodeHist.Temporal().DeletedAt == 0 {
		t.Error("node tombstone DeletedAt not set in history")
	}
	// Rel history present.
	for _, rt := range relTombstones {
		relHist, err := bs.GetRelVersion(rt.ID, rt.PrevVersion)
		if err != nil {
			t.Fatalf("GetRelVersion(%d): %v", rt.ID, err)
		}
		if relHist.Temporal() == nil || relHist.Temporal().DeletedAt == 0 {
			t.Errorf("rel tombstone %d DeletedAt not set in history", rt.ID)
		}
	}

	// nB and nC must still be alive.
	if _, err := bs.GetNode(nB.ID()); err != nil {
		t.Errorf("GetNode(nB) should still exist: %v", err)
	}
	if _, err := bs.GetNode(nC.ID()); err != nil {
		t.Errorf("GetNode(nC) should still exist: %v", err)
	}
}

// TestDeleteNodeWithHistory_TieredStore is a smoke test via the Graph API.
// DeleteNode internally calls DeleteNodeWithHistory; this verifies that after
// a Graph-layer delete on a TieredStore:
//   - live entities are gone
//   - history tombstones are present in the underlying shard's Badger DB
//
// Note: TieredStore.GetNodeVersion/GetRelVersion use shardForNodeID/shardForRelID
// which resolve shards via in-memory presence. After deletion, the shard cannot be
// resolved via the high-level routing. We access the underlying shard directly
// (refShard for "User" reference entities) to verify history was written.
func TestDeleteNodeWithHistory_TieredStore(t *testing.T) {
	t.Parallel()

	g, ts := newTestTieredGraph(t)
	defer g.Close() //nolint:errcheck

	// Add reference nodes (go to refShard) and a rel between them.
	nA, err := g.AddNode([]string{"User"}, map[string]any{"name": "alice"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	nB, err := g.AddNode([]string{"User"}, map[string]any{"name": "bob"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	r, err := g.AddRelationship("KNOWS", nA, nB, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	nodeID := nA.ID()
	relID := r.ID()
	nodeVersion := nA.Version()
	relVersion := r.Version()

	// Delete nA — cascades the rel.
	if err := g.DeleteNode(nodeID); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	// nA and the rel must be gone from live store.
	if _, err := ts.GetNode(nodeID); !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("GetNode after delete: got %v, want ErrNodeNotFound", err)
	}
	if _, err := ts.GetRelationship(relID); !errors.Is(err, ErrRelNotFound) {
		t.Errorf("GetRelationship after delete: got %v, want ErrRelNotFound", err)
	}

	// History lives in refShard (both nA and r are ref-label entities: "User" + KNOWS).
	// Access the underlying BadgerStore directly — TieredStore high-level GetNodeVersion
	// uses shardForNodeID which resolves via live presence (unavailable post-delete).
	if err := ts.refShard.Flush(); err != nil {
		t.Fatalf("Flush refShard: %v", err)
	}

	nodeHist, err := ts.refShard.GetNodeVersion(nodeID, nodeVersion)
	if err != nil {
		t.Fatalf("refShard.GetNodeVersion: %v", err)
	}
	if nodeHist.Temporal() == nil || nodeHist.Temporal().DeletedAt == 0 {
		t.Error("node tombstone DeletedAt not set in TieredStore/refShard history")
	}

	relHist, err := ts.refShard.GetRelVersion(relID, relVersion)
	if err != nil {
		t.Fatalf("refShard.GetRelVersion: %v", err)
	}
	if relHist.Temporal() == nil || relHist.Temporal().DeletedAt == 0 {
		t.Error("rel tombstone DeletedAt not set in TieredStore/refShard history")
	}

	// nB must still be alive.
	if _, err := ts.GetNode(nB.ID()); err != nil {
		t.Errorf("GetNode(nB) should still exist: %v", err)
	}
}
