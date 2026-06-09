package tiered

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func newArchiveWriteTestStore(t *testing.T) (*Store, uint16, uint16) {
	t.Helper()
	ts := newTestTieredStore(t)
	caseTok, _, signalTok := installDefaultTestLabelRegistry(t, ts)
	return ts, caseTok, signalTok
}

func mustArchiveStore(t *testing.T, ts *Store) *BadgerStore {
	t.Helper()
	if err := ts.ensureRefArchive(); err != nil {
		t.Fatalf("ensureRefArchive: %v", err)
	}
	archive := ts.refArchive.Load()
	if archive == nil {
		t.Fatal("ensureRefArchive left refArchive nil")
	}
	return archive
}

func newArchiveWriteRel(t *testing.T, start, end types.NodeID) *types.Relationship {
	t.Helper()
	return types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, start, end)
}

func newArchiveWriteNodePair(t *testing.T) (types.NodeID, types.NodeID) {
	t.Helper()
	gen := tieredNodeGen(t)
	return types.NodeID(gen.Generate()), types.NodeID(gen.Generate())
}

func TestMigrateRelationshipPlacement_EntityOnly(t *testing.T) {
	ts := newTestTieredStore(t)
	archive := mustArchiveStore(t, ts)

	start, end := newArchiveWriteNodePair(t)
	rel := newArchiveWriteRel(t, start, end)

	if err := ts.refShard.PutRelEntityAndOut(rel); err != nil {
		t.Fatalf("PutRelEntityAndOut old: %v", err)
	}

	move := relationshipPlacementMove{
		rel:       rel,
		oldEntity: ts.refShard,
		oldIn:     ts.hotShard.store,
		newEntity: archive,
		newIn:     ts.hotShard.store,
	}
	if err := migrateRelationshipPlacement(move); err != nil {
		t.Fatalf("migrateRelationshipPlacement: %v", err)
	}

	if ts.refShard.HasRelID(rel.ID().SnowflakeID()) {
		t.Fatal("old entity shard still owns rel after entity-only migration")
	}
	if !archive.HasRelID(rel.ID().SnowflakeID()) {
		t.Fatal("new entity shard does not own rel after entity-only migration")
	}
	if got := archive.OutgoingRelIDs(start.SnowflakeID()); len(got) != 1 || got[0] != rel.ID().SnowflakeID() {
		t.Fatalf("new entity shard outgoing index = %v, want [%d]", got, rel.ID().SnowflakeID())
	}
}

func TestMigrateRelationshipPlacement_NoMove(t *testing.T) {
	ts := newTestTieredStore(t)

	start, end := newArchiveWriteNodePair(t)
	rel := newArchiveWriteRel(t, start, end)

	move := relationshipPlacementMove{
		rel:       rel,
		oldEntity: ts.refShard,
		oldIn:     ts.hotShard.store,
		newEntity: ts.refShard,
		newIn:     ts.hotShard.store,
	}
	if err := migrateRelationshipPlacement(move); err != nil {
		t.Fatalf("migrateRelationshipPlacement no-op: %v", err)
	}
}

func TestMigrateRelationshipPlacement_IncomingOnly(t *testing.T) {
	ts := newTestTieredStore(t)
	archive := mustArchiveStore(t, ts)

	start, end := newArchiveWriteNodePair(t)
	rel := newArchiveWriteRel(t, start, end)
	startID := start.SnowflakeID()
	endID := end.SnowflakeID()
	relID := rel.ID().SnowflakeID()
	relType := rel.TypeToken().Value()

	if err := ts.refShard.PutRelEntityAndOut(rel); err != nil {
		t.Fatalf("PutRelEntityAndOut: %v", err)
	}
	if err := ts.refShard.PutRelIncoming(endID, startID, relType, relID); err != nil {
		t.Fatalf("PutRelIncoming old: %v", err)
	}

	move := relationshipPlacementMove{
		rel:       rel,
		oldEntity: ts.refShard,
		oldIn:     ts.refShard,
		newEntity: ts.refShard,
		newIn:     archive,
	}
	if err := migrateRelationshipPlacement(move); err != nil {
		t.Fatalf("migrateRelationshipPlacement: %v", err)
	}

	if hasIncomingEntry(ts.refShard, endID, relID) {
		t.Fatal("old incoming shard still has rel after incoming-only migration")
	}
	if !hasIncomingEntry(archive, endID, relID) {
		t.Fatal("new incoming shard does not have rel after incoming-only migration")
	}
	if !ts.refShard.HasRelID(relID) {
		t.Fatal("entity shard should be unchanged for incoming-only migration")
	}
}

func TestMigrateRelationshipPlacement_DuplicateNewEntityLeavesOldPlacement(t *testing.T) {
	ts := newTestTieredStore(t)
	archive := mustArchiveStore(t, ts)

	start, end := newArchiveWriteNodePair(t)
	rel := newArchiveWriteRel(t, start, end)

	if err := ts.refShard.PutRelEntityAndOut(rel); err != nil {
		t.Fatalf("PutRelEntityAndOut old: %v", err)
	}
	if err := archive.PutRelEntityAndOut(rel); err != nil {
		t.Fatalf("PutRelEntityAndOut duplicate: %v", err)
	}

	move := relationshipPlacementMove{
		rel:       rel,
		oldEntity: ts.refShard,
		oldIn:     ts.hotShard.store,
		newEntity: archive,
		newIn:     ts.hotShard.store,
	}
	if err := migrateRelationshipPlacement(move); !errors.Is(err, ErrRelExists) {
		t.Fatalf("migrateRelationshipPlacement error = %v, want ErrRelExists", err)
	}
	if !ts.refShard.HasRelID(rel.ID().SnowflakeID()) {
		t.Fatal("old entity shard lost rel after duplicate-target failure")
	}
}

func TestMigrateRelationshipPlacement_RollsBackNewWritesWhenOldEntityDeleteFails(t *testing.T) {
	ts := newTestTieredStore(t)
	archive := mustArchiveStore(t, ts)

	start, end := newArchiveWriteNodePair(t)
	rel := newArchiveWriteRel(t, start, end)
	startID := start.SnowflakeID()
	endID := end.SnowflakeID()
	relID := rel.ID().SnowflakeID()
	relType := rel.TypeToken().Value()

	if err := ts.refShard.PutRelIncoming(endID, startID, relType, relID); err != nil {
		t.Fatalf("PutRelIncoming old: %v", err)
	}

	move := relationshipPlacementMove{
		rel:       rel,
		oldEntity: ts.refShard,
		oldIn:     ts.refShard,
		newEntity: archive,
		newIn:     ts.hotShard.store,
	}
	if err := migrateRelationshipPlacement(move); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("migrateRelationshipPlacement error = %v, want ErrRelNotFound", err)
	}

	if !hasIncomingEntry(ts.refShard, endID, relID) {
		t.Fatal("old incoming entry was not restored after old entity delete failure")
	}
	if archive.HasRelID(relID) {
		t.Fatal("new entity shard kept rel after rollback")
	}
	if hasIncomingEntry(ts.hotShard.store, endID, relID) {
		t.Fatal("new incoming shard kept rel after rollback")
	}
}

func TestRollbackRelationshipPlacementMoves_RestoresCommittedMove(t *testing.T) {
	ts := newTestTieredStore(t)
	archive := mustArchiveStore(t, ts)

	start, end := newArchiveWriteNodePair(t)
	rel := newArchiveWriteRel(t, start, end)
	startID := start.SnowflakeID()
	endID := end.SnowflakeID()
	relID := rel.ID().SnowflakeID()
	relType := rel.TypeToken().Value()

	if err := ts.refShard.PutRelEntityAndOut(rel); err != nil {
		t.Fatalf("PutRelEntityAndOut old: %v", err)
	}
	if err := ts.refShard.PutRelIncoming(endID, startID, relType, relID); err != nil {
		t.Fatalf("PutRelIncoming old: %v", err)
	}

	move := relationshipPlacementMove{
		rel:       rel,
		oldEntity: ts.refShard,
		oldIn:     ts.refShard,
		newEntity: archive,
		newIn:     ts.hotShard.store,
	}
	if err := migrateRelationshipPlacement(move); err != nil {
		t.Fatalf("migrateRelationshipPlacement: %v", err)
	}
	if err := rollbackRelationshipPlacementMoves([]relationshipPlacementMove{move}); err != nil {
		t.Fatalf("rollbackRelationshipPlacementMoves: %v", err)
	}

	if !ts.refShard.HasRelID(relID) {
		t.Fatal("rollback did not restore rel entity to old shard")
	}
	if !hasIncomingEntry(ts.refShard, endID, relID) {
		t.Fatal("rollback did not restore old incoming entry")
	}
	if archive.HasRelID(relID) {
		t.Fatal("rollback left rel entity in new shard")
	}
	if hasIncomingEntry(ts.hotShard.store, endID, relID) {
		t.Fatal("rollback left incoming entry in new shard")
	}
}

func TestRollbackRelationshipPlacementMoves_ReturnsFirstFailure(t *testing.T) {
	ts := newTestTieredStore(t)
	archive := mustArchiveStore(t, ts)

	start, end := newArchiveWriteNodePair(t)
	rel := newArchiveWriteRel(t, start, end)

	if err := ts.refShard.PutRelEntityAndOut(rel); err != nil {
		t.Fatalf("PutRelEntityAndOut old duplicate: %v", err)
	}
	if err := archive.PutRelEntityAndOut(rel); err != nil {
		t.Fatalf("PutRelEntityAndOut new: %v", err)
	}

	move := relationshipPlacementMove{
		rel:       rel,
		oldEntity: ts.refShard,
		oldIn:     ts.refShard,
		newEntity: archive,
		newIn:     archive,
	}
	if err := rollbackRelationshipPlacementMoves([]relationshipPlacementMove{move}); !errors.Is(err, ErrRelExists) {
		t.Fatalf("rollbackRelationshipPlacementMoves error = %v, want ErrRelExists", err)
	}
}

func TestPlanRelationshipPlacementMoves_MissingEndpointBranches(t *testing.T) {
	t.Run("old start missing", func(t *testing.T) {
		ts := newTestTieredStore(t)
		archive := mustArchiveStore(t, ts)
		gen := tieredNodeGen(t)
		movingEnd := types.NodeID(gen.Generate())
		missingStart := types.NodeID(gen.Generate())
		rel := newArchiveWriteRel(t, missingStart, movingEnd)
		if err := ts.refShard.PutRelEntityAndOut(rel); err != nil {
			t.Fatalf("PutRelEntityAndOut: %v", err)
		}
		ts.closed.Store(true)

		_, release, err := ts.planRelationshipPlacementMoves(movingEnd, []snowflake.ID{rel.ID().SnowflakeID()}, ts.refShard, archive)
		if release != nil {
			release()
		}
		if !errors.Is(err, ErrStoreClosed) {
			t.Fatalf("planRelationshipPlacementMoves error = %v, want ErrStoreClosed", err)
		}
	})

	t.Run("old end missing", func(t *testing.T) {
		ts, caseTok, _ := newArchiveWriteTestStore(t)
		archive := mustArchiveStore(t, ts)
		gen := tieredNodeGen(t)
		movingStart := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
		missingEnd := types.NodeID(gen.Generate())
		if err := ts.PutNode(movingStart); err != nil {
			t.Fatalf("PutNode moving start: %v", err)
		}
		rel := newArchiveWriteRel(t, movingStart.ID(), missingEnd)
		if err := ts.refShard.PutRelEntityAndOut(rel); err != nil {
			t.Fatalf("PutRelEntityAndOut: %v", err)
		}
		ts.closed.Store(true)

		_, release, err := ts.planRelationshipPlacementMoves(movingStart.ID(), []snowflake.ID{rel.ID().SnowflakeID()}, ts.refShard, archive)
		if release != nil {
			release()
		}
		if !errors.Is(err, ErrStoreClosed) {
			t.Fatalf("planRelationshipPlacementMoves error = %v, want ErrStoreClosed", err)
		}
	})
}

func TestPlanRelationshipPlacementMoves_CachesPeerEndpointShardPins(t *testing.T) {
	ts, caseTok, signalTok := newArchiveWriteTestStore(t)
	archive := mustArchiveStore(t, ts)
	gen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	moving := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	peer := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	for _, node := range []*types.Node{moving, peer} {
		if err := ts.PutNode(node); err != nil {
			t.Fatalf("PutNode(%d): %v", node.ID(), err)
		}
	}

	const relCount = 5
	relIDs := make([]snowflake.ID, 0, relCount)
	for range relCount {
		rel := types.NewRelationship(types.RelID(relGen.Generate()), 1, moving.ID(), peer.ID())
		if err := ts.PutRelationship(rel); err != nil {
			t.Fatalf("PutRelationship(%d): %v", rel.ID(), err)
		}
		relIDs = append(relIDs, rel.ID().SnowflakeID())
	}

	hot := ts.HotShardForTest()
	moves, release, err := ts.planRelationshipPlacementMoves(moving.ID(), relIDs, ts.refShard, archive)
	released := false
	if release != nil {
		defer func() {
			if !released {
				release()
			}
		}()
	}
	if err != nil {
		t.Fatalf("planRelationshipPlacementMoves: %v", err)
	}
	if len(moves) != relCount {
		t.Fatalf("planned moves = %d, want %d", len(moves), relCount)
	}
	if got := hot.ActiveReqsForTest().Load(); got != 1 {
		t.Fatalf("hot shard active checkouts while moves are pinned = %d, want 1 cached peer endpoint checkout", got)
	}
	release()
	released = true
	if got := hot.ActiveReqsForTest().Load(); got != 0 {
		t.Fatalf("hot shard active checkouts after release = %d, want 0", got)
	}
}

func TestMergeUniqueRelIDs_DeduplicatesBothInputs(t *testing.T) {
	a := snowflake.ID(1)
	b := snowflake.ID(2)
	got := mergeUniqueRelIDs([]snowflake.ID{a, a, b}, []snowflake.ID{b, a})
	want := []snowflake.ID{a, b}
	if len(got) != len(want) {
		t.Fatalf("mergeUniqueRelIDs length = %d, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("mergeUniqueRelIDs[%d] = %d, want %d; got %v", i, got[i], want[i], got)
		}
	}
}

func TestArchiveNode_ErrorBranches(t *testing.T) {
	t.Run("invalid id", func(t *testing.T) {
		ts, _, _ := newArchiveWriteTestStore(t)
		for _, id := range []types.NodeID{0, -1} {
			err := ts.ArchiveNode(id)
			if !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
				t.Fatalf("ArchiveNode(%d) error = %v, want ErrInvalidStoreMutation", id, err)
			}
		}
	})

	t.Run("node not found", func(t *testing.T) {
		ts, _, _ := newArchiveWriteTestStore(t)
		err := ts.ArchiveNode(types.NodeID(tieredNodeGen(t).Generate()))
		if !errors.Is(err, ErrNodeNotFound) {
			t.Fatalf("ArchiveNode error = %v, want ErrNodeNotFound", err)
		}
	})

	t.Run("event node", func(t *testing.T) {
		ts, _, signalTok := newArchiveWriteTestStore(t)
		signal := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), signalTok, nil)
		if err := ts.PutNode(signal); err != nil {
			t.Fatalf("PutNode signal: %v", err)
		}

		err := ts.ArchiveNode(signal.ID())
		if !errors.Is(err, ErrNotReferenceEntity) {
			t.Fatalf("ArchiveNode error = %v, want ErrNotReferenceEntity", err)
		}
		if ts.refArchive.Load() != nil {
			t.Fatal("ArchiveNode opened archive for rejected event node")
		}
		if _, err := ts.GetNode(signal.ID()); err != nil {
			t.Fatalf("event node moved or lost after rejected archive: %v", err)
		}
	})

	t.Run("read node error", func(t *testing.T) {
		ts, caseTok, _ := newArchiveWriteTestStore(t)
		n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, nil)
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
		if !ts.refShard.NodeCacheForTest().EvictForTest(n.ID().SnowflakeID()) {
			t.Fatal("failed to evict ref node cache entry")
		}

		err := ts.ArchiveNode(n.ID())
		if !errors.Is(err, ErrNodeNotFound) {
			t.Fatalf("ArchiveNode error = %v, want ErrNodeNotFound", err)
		}
	})

	t.Run("misplaced event node in ref shard", func(t *testing.T) {
		ts, _, signalTok := newArchiveWriteTestStore(t)
		signal := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), signalTok, nil)
		if err := ts.refShard.PutNode(signal); err != nil {
			t.Fatalf("PutNode misplaced signal: %v", err)
		}

		err := ts.ArchiveNode(signal.ID())
		if !errors.Is(err, ErrNotReferenceEntity) {
			t.Fatalf("ArchiveNode error = %v, want ErrNotReferenceEntity", err)
		}
		if ts.refArchive.Load() != nil {
			t.Fatal("ArchiveNode opened archive for misplaced event node")
		}
		if !ts.refShard.HasNodeID(signal.ID().SnowflakeID()) {
			t.Fatal("ArchiveNode removed misplaced event node from ref shard")
		}
	})

	t.Run("closed store", func(t *testing.T) {
		ts, caseTok, _ := newArchiveWriteTestStore(t)
		n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, nil)
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
		if err := ts.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		err := ts.ArchiveNode(n.ID())
		if !errors.Is(err, ErrStoreClosed) {
			t.Fatalf("ArchiveNode error = %v, want ErrStoreClosed", err)
		}
	})

	t.Run("stale incoming adjacency", func(t *testing.T) {
		ts, caseTok, _ := newArchiveWriteTestStore(t)
		n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, nil)
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
		staleRelID := tieredRelGen(t).Generate()
		if err := ts.refShard.PutRelIncoming(n.ID().SnowflakeID(), snowflake.ID(1), 1, staleRelID); err != nil {
			t.Fatalf("PutRelIncoming stale: %v", err)
		}

		if err := ts.ArchiveNode(n.ID()); err != nil {
			t.Fatalf("ArchiveNode with stale incoming adjacency: %v", err)
		}
		archive := ts.refArchive.Load()
		if archive == nil || !archive.HasNodeID(n.ID().SnowflakeID()) {
			t.Fatal("ArchiveNode did not move node to archive")
		}
		if ts.refShard.HasNodeID(n.ID().SnowflakeID()) {
			t.Fatal("ArchiveNode left node in ref shard")
		}
		if got := ts.refShard.IncomingRelIDs(n.ID().SnowflakeID(), 0); len(got) != 0 {
			t.Fatalf("ArchiveNode left stale incoming adjacency in ref shard: %v", got)
		}
	})

	t.Run("stale incoming adjacency in destination", func(t *testing.T) {
		ts, caseTok, _ := newArchiveWriteTestStore(t)
		n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, nil)
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
		archive := mustArchiveStore(t, ts)
		staleRelID := tieredRelGen(t).Generate()
		if err := archive.PutRelIncoming(n.ID().SnowflakeID(), snowflake.ID(1), 1, staleRelID); err != nil {
			t.Fatalf("PutRelIncoming stale destination: %v", err)
		}

		if err := ts.ArchiveNode(n.ID()); err != nil {
			t.Fatalf("ArchiveNode with stale destination adjacency: %v", err)
		}
		if !archive.HasNodeID(n.ID().SnowflakeID()) {
			t.Fatal("ArchiveNode did not move node to archive")
		}
		if got := archive.IncomingRelIDs(n.ID().SnowflakeID(), 0); len(got) != 0 {
			t.Fatalf("ArchiveNode left stale incoming adjacency in archive: %v", got)
		}
	})

	t.Run("live incoming adjacency in destination", func(t *testing.T) {
		ts, caseTok, _ := newArchiveWriteTestStore(t)
		gen := tieredNodeGen(t)
		n := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
		peer := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
		for _, node := range []*types.Node{n, peer} {
			if err := ts.PutNode(node); err != nil {
				t.Fatalf("PutNode(%d): %v", node.ID(), err)
			}
		}
		rel := newArchiveWriteRel(t, peer.ID(), n.ID())
		if err := ts.PutRelationship(rel); err != nil {
			t.Fatalf("PutRelationship: %v", err)
		}
		archive := mustArchiveStore(t, ts)
		if err := archive.PutRelIncoming(n.ID().SnowflakeID(), peer.ID().SnowflakeID(), rel.TypeToken().Value(), rel.ID().SnowflakeID()); err != nil {
			t.Fatalf("PutRelIncoming live destination: %v", err)
		}

		err := ts.ArchiveNode(n.ID())
		if !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
			t.Fatalf("ArchiveNode error = %v, want ErrInvalidStoreMutation", err)
		}
		if !ts.refShard.HasNodeID(n.ID().SnowflakeID()) {
			t.Fatal("ArchiveNode removed ref node after live destination adjacency rejection")
		}
		if archive.HasNodeID(n.ID().SnowflakeID()) {
			t.Fatal("ArchiveNode wrote node to archive after live destination adjacency rejection")
		}
		if got := archive.IncomingRelIDs(n.ID().SnowflakeID(), 0); len(got) != 1 || got[0] != rel.ID().SnowflakeID() {
			t.Fatalf("ArchiveNode purged live destination adjacency = %v, want [%d]", got, rel.ID().SnowflakeID())
		}
	})

	t.Run("archive already has node", func(t *testing.T) {
		ts, caseTok, _ := newArchiveWriteTestStore(t)
		n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, nil)
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode ref: %v", err)
		}
		archive := mustArchiveStore(t, ts)
		if err := archive.PutNode(n); err != nil {
			t.Fatalf("PutNode archive duplicate: %v", err)
		}

		err := ts.ArchiveNode(n.ID())
		if !errors.Is(err, ErrNodeExists) {
			t.Fatalf("ArchiveNode error = %v, want ErrNodeExists", err)
		}
	})

	t.Run("relationship duplicate in archive", func(t *testing.T) {
		ts, caseTok, signalTok := newArchiveWriteTestStore(t)
		gen := tieredNodeGen(t)
		caseNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
		signal := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
		if err := ts.PutNode(caseNode); err != nil {
			t.Fatalf("PutNode case: %v", err)
		}
		if err := ts.PutNode(signal); err != nil {
			t.Fatalf("PutNode signal: %v", err)
		}
		rel := newArchiveWriteRel(t, caseNode.ID(), signal.ID())
		if err := ts.PutRelationship(rel); err != nil {
			t.Fatalf("PutRelationship: %v", err)
		}
		archive := mustArchiveStore(t, ts)
		if err := archive.PutRelEntityAndOut(rel); err != nil {
			t.Fatalf("PutRelEntityAndOut archive duplicate: %v", err)
		}

		err := ts.ArchiveNode(caseNode.ID())
		if !errors.Is(err, ErrRelExists) {
			t.Fatalf("ArchiveNode error = %v, want ErrRelExists", err)
		}
		if archive.HasNodeID(caseNode.ID().SnowflakeID()) {
			t.Fatal("ArchiveNode left node in archive after relationship migration failure")
		}
		if !ts.refShard.HasNodeID(caseNode.ID().SnowflakeID()) {
			t.Fatal("ArchiveNode removed ref node after relationship migration failure")
		}
		if !ts.refShard.HasRelID(rel.ID().SnowflakeID()) {
			t.Fatal("ArchiveNode removed old rel after relationship migration failure")
		}
	})
}

func TestRestoreNode_ErrorBranches(t *testing.T) {
	t.Run("invalid id", func(t *testing.T) {
		ts, _, _ := newArchiveWriteTestStore(t)
		for _, id := range []types.NodeID{0, -1} {
			err := ts.RestoreNode(id)
			if !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
				t.Fatalf("RestoreNode(%d) error = %v, want ErrInvalidStoreMutation", id, err)
			}
		}
	})

	t.Run("node not found", func(t *testing.T) {
		ts, _, _ := newArchiveWriteTestStore(t)
		mustArchiveStore(t, ts)
		err := ts.RestoreNode(types.NodeID(tieredNodeGen(t).Generate()))
		if !errors.Is(err, ErrNodeNotFound) {
			t.Fatalf("RestoreNode error = %v, want ErrNodeNotFound", err)
		}
	})

	t.Run("closed store", func(t *testing.T) {
		ts, _, _ := newArchiveWriteTestStore(t)
		if err := ts.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		err := ts.RestoreNode(types.NodeID(tieredNodeGen(t).Generate()))
		if !errors.Is(err, ErrStoreClosed) {
			t.Fatalf("RestoreNode error = %v, want ErrStoreClosed", err)
		}
	})

	t.Run("read node error", func(t *testing.T) {
		ts, caseTok, _ := newArchiveWriteTestStore(t)
		archive := mustArchiveStore(t, ts)
		n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, nil)
		if err := archive.PutNode(n); err != nil {
			t.Fatalf("PutNode archive: %v", err)
		}
		if !archive.NodeCacheForTest().EvictForTest(n.ID().SnowflakeID()) {
			t.Fatal("failed to evict archive node cache entry")
		}

		err := ts.RestoreNode(n.ID())
		if !errors.Is(err, ErrNodeNotFound) {
			t.Fatalf("RestoreNode error = %v, want ErrNodeNotFound", err)
		}
	})

	t.Run("event node in archive", func(t *testing.T) {
		ts, _, signalTok := newArchiveWriteTestStore(t)
		archive := mustArchiveStore(t, ts)
		signal := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), signalTok, nil)
		if err := archive.PutNode(signal); err != nil {
			t.Fatalf("PutNode archive signal: %v", err)
		}

		err := ts.RestoreNode(signal.ID())
		if !errors.Is(err, ErrNotReferenceEntity) {
			t.Fatalf("RestoreNode error = %v, want ErrNotReferenceEntity", err)
		}
		if !archive.HasNodeID(signal.ID().SnowflakeID()) {
			t.Fatal("RestoreNode removed rejected event node from archive")
		}
		if ts.refShard.HasNodeID(signal.ID().SnowflakeID()) {
			t.Fatal("RestoreNode wrote rejected event node to ref shard")
		}
	})

	t.Run("stale incoming adjacency", func(t *testing.T) {
		ts, caseTok, _ := newArchiveWriteTestStore(t)
		archive := mustArchiveStore(t, ts)
		n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, nil)
		if err := archive.PutNode(n); err != nil {
			t.Fatalf("PutNode archive: %v", err)
		}
		staleRelID := tieredRelGen(t).Generate()
		if err := archive.PutRelIncoming(n.ID().SnowflakeID(), snowflake.ID(1), 1, staleRelID); err != nil {
			t.Fatalf("PutRelIncoming stale: %v", err)
		}

		if err := ts.RestoreNode(n.ID()); err != nil {
			t.Fatalf("RestoreNode with stale incoming adjacency: %v", err)
		}
		if !ts.refShard.HasNodeID(n.ID().SnowflakeID()) {
			t.Fatal("RestoreNode did not move node to ref shard")
		}
		if archive.HasNodeID(n.ID().SnowflakeID()) {
			t.Fatal("RestoreNode left node in archive")
		}
		if got := archive.IncomingRelIDs(n.ID().SnowflakeID(), 0); len(got) != 0 {
			t.Fatalf("RestoreNode left stale incoming adjacency in archive: %v", got)
		}
	})

	t.Run("stale incoming adjacency in destination", func(t *testing.T) {
		ts, caseTok, _ := newArchiveWriteTestStore(t)
		archive := mustArchiveStore(t, ts)
		n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, nil)
		if err := archive.PutNode(n); err != nil {
			t.Fatalf("PutNode archive: %v", err)
		}
		staleRelID := tieredRelGen(t).Generate()
		if err := ts.refShard.PutRelIncoming(n.ID().SnowflakeID(), snowflake.ID(1), 1, staleRelID); err != nil {
			t.Fatalf("PutRelIncoming stale destination: %v", err)
		}

		if err := ts.RestoreNode(n.ID()); err != nil {
			t.Fatalf("RestoreNode with stale destination adjacency: %v", err)
		}
		if !ts.refShard.HasNodeID(n.ID().SnowflakeID()) {
			t.Fatal("RestoreNode did not move node to ref shard")
		}
		if got := ts.refShard.IncomingRelIDs(n.ID().SnowflakeID(), 0); len(got) != 0 {
			t.Fatalf("RestoreNode left stale incoming adjacency in ref shard: %v", got)
		}
	})

	t.Run("live incoming adjacency in destination", func(t *testing.T) {
		ts, caseTok, _ := newArchiveWriteTestStore(t)
		archive := mustArchiveStore(t, ts)
		gen := tieredNodeGen(t)
		n := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
		peer := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
		if err := archive.PutNode(n); err != nil {
			t.Fatalf("PutNode archive: %v", err)
		}
		if err := ts.PutNode(peer); err != nil {
			t.Fatalf("PutNode peer: %v", err)
		}
		rel := newArchiveWriteRel(t, n.ID(), peer.ID())
		if err := archive.PutRelEntityAndOut(rel); err != nil {
			t.Fatalf("PutRelEntityAndOut archive: %v", err)
		}
		if err := ts.refShard.PutRelIncoming(peer.ID().SnowflakeID(), n.ID().SnowflakeID(), rel.TypeToken().Value(), rel.ID().SnowflakeID()); err != nil {
			t.Fatalf("PutRelIncoming ref endpoint: %v", err)
		}
		if err := ts.refShard.PutRelIncoming(n.ID().SnowflakeID(), peer.ID().SnowflakeID(), rel.TypeToken().Value(), rel.ID().SnowflakeID()); err != nil {
			t.Fatalf("PutRelIncoming live destination: %v", err)
		}

		err := ts.RestoreNode(n.ID())
		if !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
			t.Fatalf("RestoreNode error = %v, want ErrInvalidStoreMutation", err)
		}
		if !archive.HasNodeID(n.ID().SnowflakeID()) {
			t.Fatal("RestoreNode removed archive node after live destination adjacency rejection")
		}
		if ts.refShard.HasNodeID(n.ID().SnowflakeID()) {
			t.Fatal("RestoreNode wrote node to ref after live destination adjacency rejection")
		}
		if got := ts.refShard.IncomingRelIDs(n.ID().SnowflakeID(), 0); len(got) != 1 || got[0] != rel.ID().SnowflakeID() {
			t.Fatalf("RestoreNode purged live destination adjacency = %v, want [%d]", got, rel.ID().SnowflakeID())
		}
	})

	t.Run("ref already has node", func(t *testing.T) {
		ts, caseTok, _ := newArchiveWriteTestStore(t)
		archive := mustArchiveStore(t, ts)
		n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, nil)
		if err := archive.PutNode(n); err != nil {
			t.Fatalf("PutNode archive: %v", err)
		}
		if err := ts.refShard.PutNode(n); err != nil {
			t.Fatalf("PutNode ref duplicate: %v", err)
		}

		err := ts.RestoreNode(n.ID())
		if !errors.Is(err, ErrNodeExists) {
			t.Fatalf("RestoreNode error = %v, want ErrNodeExists", err)
		}
	})

	t.Run("relationship duplicate in ref", func(t *testing.T) {
		ts, caseTok, signalTok := newArchiveWriteTestStore(t)
		gen := tieredNodeGen(t)
		caseNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
		signal := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
		archive := mustArchiveStore(t, ts)
		if err := archive.PutNode(caseNode); err != nil {
			t.Fatalf("PutNode archive case: %v", err)
		}
		if err := ts.PutNode(signal); err != nil {
			t.Fatalf("PutNode signal: %v", err)
		}
		rel := newArchiveWriteRel(t, caseNode.ID(), signal.ID())
		if err := archive.PutRelEntityAndOut(rel); err != nil {
			t.Fatalf("PutRelEntityAndOut archive: %v", err)
		}
		if err := ts.hotShard.store.PutRelIncoming(signal.ID().SnowflakeID(), caseNode.ID().SnowflakeID(), rel.TypeToken().Value(), rel.ID().SnowflakeID()); err != nil {
			t.Fatalf("PutRelIncoming hot: %v", err)
		}
		if err := ts.refShard.PutRelEntityAndOut(rel); err != nil {
			t.Fatalf("PutRelEntityAndOut ref duplicate: %v", err)
		}

		err := ts.RestoreNode(caseNode.ID())
		if !errors.Is(err, ErrRelExists) {
			t.Fatalf("RestoreNode error = %v, want ErrRelExists", err)
		}
		if ts.refShard.HasNodeID(caseNode.ID().SnowflakeID()) {
			t.Fatal("RestoreNode left node in ref after relationship migration failure")
		}
		if !archive.HasNodeID(caseNode.ID().SnowflakeID()) {
			t.Fatal("RestoreNode removed archive node after relationship migration failure")
		}
		if !archive.HasRelID(rel.ID().SnowflakeID()) {
			t.Fatal("RestoreNode removed old rel after relationship migration failure")
		}
	})
}
