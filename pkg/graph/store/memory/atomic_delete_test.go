package memory

import (
	"errors"
	"testing"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func assertAsOfTemporalBeforeDelete(t *testing.T, tm *types.TemporalMetadata) {
	t.Helper()
	if tm == nil {
		t.Fatal("as-of entity has nil temporal metadata")
	}
	if tm.TxTo != 0 {
		t.Fatalf("as-of TxTo = %d, want 0 before delete transaction", tm.TxTo)
	}
	if tm.ValidTo != 0 {
		t.Fatalf("as-of ValidTo = %d, want 0 before delete transaction", tm.ValidTo)
	}
	if tm.DeletedAt != 0 {
		t.Fatalf("as-of DeletedAt = %d, want 0 before delete transaction", tm.DeletedAt)
	}
}

// --- TestDeleteRelWithHistory ---

func TestDeleteRelWithHistory_HistoryAndLiveConsistent(t *testing.T) {
	t.Parallel()

	ms := New()
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

func TestDeleteRelWithHistory_RelAsOfBeforeDeleteHidesFutureDeleteMarkers(t *testing.T) {
	t.Parallel()

	ms := New()
	defer ms.Close() //nolint:errcheck

	nA := types.NewNode(types.NodeID(10), 1, nil)
	nB := types.NewNode(types.NodeID(20), 2, nil)
	if err := ms.PutNode(nA); err != nil {
		t.Fatalf("PutNode A: %v", err)
	}
	if err := ms.PutNode(nB); err != nil {
		t.Fatalf("PutNode B: %v", err)
	}

	r := types.NewRelationship(types.RelID(100), 1, nA.ID(), nB.ID())
	r.SetTemporal(&types.TemporalMetadata{TxFrom: 100})
	if err := ms.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	tomb := r.DeepCopy()
	tm := tomb.Temporal()
	tm.DeletedAt = 200
	tm.ValidTo = 200
	tm.TxTo = 200
	if err := ms.DeleteRelWithHistory(r.ID(), r.Version(), tomb); err != nil {
		t.Fatalf("DeleteRelWithHistory: %v", err)
	}

	got, err := ms.RelAsOf(r.ID(), 100)
	if err != nil {
		t.Fatalf("RelAsOf before delete: %v", err)
	}
	assertAsOfTemporalBeforeDelete(t, got.Temporal())
	rels, err := ms.RelsAsOf(100)
	if err != nil {
		t.Fatalf("RelsAsOf before delete: %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("RelsAsOf before delete returned %d relationships, want 1", len(rels))
	}
	assertAsOfTemporalBeforeDelete(t, rels[0].Temporal())

	if _, err := ms.RelAsOf(r.ID(), 200); !errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("RelAsOf at delete = %v, want ErrVersionNotFound", err)
	}
}

func TestDeleteRelWithHistory_NotFound(t *testing.T) {
	t.Parallel()

	ms := New()
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

	ms := New()
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

func TestDeleteNodeWithHistory_NodeAsOfBeforeDeleteHidesFutureDeleteMarkers(t *testing.T) {
	t.Parallel()

	ms := New()
	defer ms.Close() //nolint:errcheck

	n := types.NewNode(types.NodeID(10), 1, nil)
	n.SetTemporal(&types.TemporalMetadata{TxFrom: 100})
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	tomb := n.DeepCopy()
	tm := tomb.Temporal()
	tm.DeletedAt = 200
	tm.ValidTo = 200
	tm.TxTo = 200
	if err := ms.DeleteNodeWithHistory(n.ID(), n.Version(), tomb, nil); err != nil {
		t.Fatalf("DeleteNodeWithHistory: %v", err)
	}

	got, err := ms.NodeAsOf(n.ID(), 100)
	if err != nil {
		t.Fatalf("NodeAsOf before delete: %v", err)
	}
	assertAsOfTemporalBeforeDelete(t, got.Temporal())
	nodes, err := ms.NodesAsOf(100)
	if err != nil {
		t.Fatalf("NodesAsOf before delete: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("NodesAsOf before delete returned %d nodes, want 1", len(nodes))
	}
	assertAsOfTemporalBeforeDelete(t, nodes[0].Temporal())

	if _, err := ms.NodeAsOf(n.ID(), 200); !errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("NodeAsOf at delete = %v, want ErrVersionNotFound", err)
	}
}

func TestNodesAsOfReturnsSortedByID(t *testing.T) {
	t.Parallel()

	ms := New()
	defer ms.Close() //nolint:errcheck

	for i := 64; i >= 1; i-- {
		n := types.NewNode(types.NodeID(i), 1, nil)
		n.SetTemporal(&types.TemporalMetadata{TxFrom: 1})
		if err := ms.PutNode(n); err != nil {
			t.Fatalf("PutNode %d: %v", i, err)
		}
	}

	nodes, err := ms.NodesAsOf(1)
	if err != nil {
		t.Fatalf("NodesAsOf: %v", err)
	}
	if len(nodes) != 64 {
		t.Fatalf("len(NodesAsOf) = %d, want 64", len(nodes))
	}
	for i := 1; i < len(nodes); i++ {
		if nodes[i-1].ID() > nodes[i].ID() {
			t.Fatalf("NodesAsOf order[%d:%d] = %d > %d", i-1, i, nodes[i-1].ID(), nodes[i].ID())
		}
	}
}

func TestRelsAsOfReturnsSortedByID(t *testing.T) {
	t.Parallel()

	ms := New()
	defer ms.Close() //nolint:errcheck

	a := types.NewNode(types.NodeID(1), 1, nil)
	b := types.NewNode(types.NodeID(2), 1, nil)
	a.SetTemporal(&types.TemporalMetadata{TxFrom: 1})
	b.SetTemporal(&types.TemporalMetadata{TxFrom: 1})
	if err := ms.PutNode(a); err != nil {
		t.Fatalf("PutNode a: %v", err)
	}
	if err := ms.PutNode(b); err != nil {
		t.Fatalf("PutNode b: %v", err)
	}
	for i := 64; i >= 1; i-- {
		r := types.NewRelationship(types.RelID(i), 1, a.ID(), b.ID())
		r.SetTemporal(&types.TemporalMetadata{TxFrom: 1})
		if err := ms.PutRelationship(r); err != nil {
			t.Fatalf("PutRelationship %d: %v", i, err)
		}
	}

	rels, err := ms.RelsAsOf(1)
	if err != nil {
		t.Fatalf("RelsAsOf: %v", err)
	}
	if len(rels) != 64 {
		t.Fatalf("len(RelsAsOf) = %d, want 64", len(rels))
	}
	for i := 1; i < len(rels); i++ {
		if rels[i-1].ID() > rels[i].ID() {
			t.Fatalf("RelsAsOf order[%d:%d] = %d > %d", i-1, i, rels[i-1].ID(), rels[i].ID())
		}
	}
}

func TestDeleteNodeWithHistory_EmptyRelTombstones(t *testing.T) {
	t.Parallel()

	ms := New()
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

func TestDeleteNodeWithHistoryRejectsRelTombstoneIndexedFieldMutation(t *testing.T) {
	t.Parallel()

	ms := New()
	for _, n := range []*types.Node{
		types.NewNode(types.NodeID(10), 1, nil),
		types.NewNode(types.NodeID(20), 2, nil),
		types.NewNode(types.NodeID(30), 3, nil),
	} {
		if err := ms.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
	}
	rel := types.NewRelationship(types.RelID(100), 1, types.NodeID(10), types.NodeID(20))
	if err := ms.PutRelationship(rel); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	nodeTombstone := types.NewNode(types.NodeID(10), 1, nil)
	badRelTombstone := types.NewRelationship(types.RelID(100), 1, types.NodeID(10), types.NodeID(30))
	err := ms.DeleteNodeWithHistory(types.NodeID(10), 0, nodeTombstone, []RelTombstone{{
		ID:          rel.ID(),
		PrevVersion: rel.Version(),
		Tombstone:   badRelTombstone,
	}})
	if !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteNodeWithHistory bad rel tombstone = %v, want ErrInvalidStoreMutation", err)
	}
	if _, err := ms.GetNode(types.NodeID(10)); err != nil {
		t.Fatalf("node deleted after rejected tombstone: %v", err)
	}
	gotRel, err := ms.GetRelationship(types.RelID(100))
	if err != nil {
		t.Fatalf("relationship deleted after rejected tombstone: %v", err)
	}
	if gotRel.EndNodeID() != types.NodeID(20) {
		t.Fatalf("relationship tombstone changed live endpoint: got %d, want 20", gotRel.EndNodeID())
	}
	if hist, err := ms.GetRelHistory(types.RelID(100)); err != nil || len(hist) != 0 {
		t.Fatalf("relationship history after rejected tombstone = len %d err %v, want empty nil", len(hist), err)
	}
}

func TestDeleteNodeWithHistoryPurgesOrphanAdjacency(t *testing.T) {
	t.Parallel()

	ms := New()
	defer ms.Close() //nolint:errcheck

	nA := types.NewNode(types.NodeID(10), 1, nil)
	nB := types.NewNode(types.NodeID(20), 1, nil)
	if err := ms.PutNode(nA); err != nil {
		t.Fatalf("PutNode A: %v", err)
	}
	if err := ms.PutNode(nB); err != nil {
		t.Fatalf("PutNode B: %v", err)
	}

	orphan := types.RelID(999)
	ms.mu.Lock()
	ms.outIdx[nA.ID()] = map[types.RelID]struct{}{orphan: {}}
	ms.inIdx[nB.ID()] = map[types.RelID]struct{}{orphan: {}}
	ms.typeIdx[7] = map[types.RelID]struct{}{orphan: {}}
	ms.mu.Unlock()

	tombN := nA.DeepCopy()
	tm := &types.TemporalMetadata{DeletedAt: types.Instant(time.Now().UnixMilli())}
	tombN.SetTemporal(tm)
	if err := ms.DeleteNodeWithHistory(nA.ID(), nA.Version(), tombN, nil); err != nil {
		t.Fatalf("DeleteNodeWithHistory: %v", err)
	}

	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if _, ok := ms.outIdx[nA.ID()][orphan]; ok {
		t.Fatal("orphan rel remained in outgoing adjacency after delete-with-history")
	}
	if _, ok := ms.inIdx[nB.ID()][orphan]; ok {
		t.Fatal("orphan rel remained in incoming adjacency after delete-with-history")
	}
	if _, ok := ms.typeIdx[7][orphan]; ok {
		t.Fatal("orphan rel remained in type index after delete-with-history")
	}
}
