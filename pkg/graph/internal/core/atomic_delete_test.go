package core

import (
	"errors"
	"testing"
	"time"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/badger"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestDeleteNodeWithHistory_BadgerStore(t *testing.T) {
	t.Parallel()

	bs, err := badger.New(badger.Config{InMemory: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("badger.New: %v", err)
	}
	defer bs.Close() //nolint:errcheck

	nA := putTestNode(t, bs, 10, 1, nil)
	nB := putTestNode(t, bs, 20, 2, nil)
	nC := putTestNode(t, bs, 30, 3, nil)

	r1 := putTestRel(t, bs, 100, 1, 10, 20)
	r2 := putTestRel(t, bs, 200, 1, int64(nC.ID()), 10)

	now := types.Instant(time.Now().UnixMilli())

	// Build rel tombstones.
	relTombstones := make([]storepkg.RelTombstone, 0, 2)
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
		relTombstones = append(relTombstones, storepkg.RelTombstone{
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
	if _, err := bs.GetNode(nA.ID()); !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Errorf("GetNode after delete: got %v, want storepkg.ErrNodeNotFound", err)
	}
	// Rels gone.
	for _, id := range []types.RelID{100, 200} {
		if _, err := bs.GetRelationship(id); !errors.Is(err, storepkg.ErrRelNotFound) {
			t.Errorf("GetRelationship(%d) after delete: got %v, want storepkg.ErrRelNotFound", id, err)
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
// a Graph-layer delete on a tiered.Store:
//   - live entities are gone
//   - history tombstones are present in the underlying shard's Badger DB
//
// Note: tiered.Store.Nodes.GetVersion/GetRelVersion use shardForNodeID/shardForRelID
// which resolve shards via in-memory presence. After deletion, the shard cannot be
// resolved via the high-level routing. We access the underlying shard directly
// (refShard for "User" reference entities) to verify history was written.
func TestDeleteNodeWithHistory_TieredStore(t *testing.T) {
	t.Parallel()

	g, ts := newTestTieredGraph(t)
	defer g.Close() //nolint:errcheck

	// Add reference nodes (go to refShard) and a rel between them.
	nA, err := g.Nodes.Add([]string{"User"}, map[string]any{"name": "alice"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	nB, err := g.Nodes.Add([]string{"User"}, map[string]any{"name": "bob"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	r, err := g.Rels.Add("KNOWS", nA, nB, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	nodeID := nA.ID()
	relID := r.ID()
	nodeVersion := nA.Version()
	relVersion := r.Version()

	// Delete nA — cascades the rel.
	if err := g.Nodes.Delete(nodeID); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	// nA and the rel must be gone from live store.
	if _, err := ts.GetNode(nodeID); !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Errorf("GetNode after delete: got %v, want storepkg.ErrNodeNotFound", err)
	}
	if _, err := ts.GetRelationship(relID); !errors.Is(err, storepkg.ErrRelNotFound) {
		t.Errorf("GetRelationship after delete: got %v, want storepkg.ErrRelNotFound", err)
	}

	// History lives in refShard (both nA and r are ref-label entities: "User" + KNOWS).
	// Access the underlying badger.Store directly — tiered.Store high-level GetNodeVersion
	// uses shardForNodeID which resolves via live presence (unavailable post-delete).
	if err := ts.RefShardForTest().Flush(); err != nil {
		t.Fatalf("Flush refShard: %v", err)
	}

	nodeHist, err := ts.RefShardForTest().GetNodeVersion(nodeID, nodeVersion)
	if err != nil {
		t.Fatalf("refShard.Nodes.GetVersion: %v", err)
	}
	if nodeHist.Temporal() == nil || nodeHist.Temporal().DeletedAt == 0 {
		t.Error("node tombstone DeletedAt not set in tiered.Store/refShard history")
	}

	relHist, err := ts.RefShardForTest().GetRelVersion(relID, relVersion)
	if err != nil {
		t.Fatalf("refShard.GetRelVersion: %v", err)
	}
	if relHist.Temporal() == nil || relHist.Temporal().DeletedAt == 0 {
		t.Error("rel tombstone DeletedAt not set in tiered.Store/refShard history")
	}

	// nB must still be alive.
	if _, err := ts.GetNode(nB.ID()); err != nil {
		t.Errorf("GetNode(nB) should still exist: %v", err)
	}
}
