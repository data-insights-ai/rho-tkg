package tiered

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"
	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
	storeutil "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestTieredStore_Repair_OrphanedIncoming(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	sigTok, _ := reg.GetOrCreate("Signal")
	relTok, _ := registrypkg.NewRelTypeRegistry().GetOrCreate("TRIGGERED")

	gen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	// Create a ref node (Case) and an event node (Signal).
	refNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(refNode); err != nil {
		t.Fatalf("PutNode ref: %v", err)
	}
	evtNode := types.NewNode(types.NodeID(gen.Generate()), sigTok, nil)
	if err := ts.PutNode(evtNode); err != nil {
		t.Fatalf("PutNode evt: %v", err)
	}

	// Manually create an orphaned in/ entry on refShard pointing to a non-existent rel.
	fakeRelID := relGen.Generate()
	if err := ts.RefShardForTest().PutRelIncoming(
		refNode.ID().SnowflakeID(),
		evtNode.ID().SnowflakeID(),
		relTok,
		fakeRelID,
	); err != nil {
		t.Fatalf("PutRelIncoming: %v", err)
	}

	// Verify the orphaned in/ entry exists.
	inIDs := ts.RefShardForTest().IncomingRelIDs(refNode.ID().SnowflakeID(), 0)
	if len(inIDs) != 1 {
		t.Fatalf("expected 1 incoming rel, got %d", len(inIDs))
	}

	// Run repair.
	result, err := ts.RunRepair()
	if err != nil {
		t.Fatalf("RunRepair: %v", err)
	}
	if result.OrphanedInEntries != 1 {
		t.Errorf("OrphanedInEntries = %d, want 1", result.OrphanedInEntries)
	}

	// Verify the orphaned in/ entry was removed.
	inIDs = ts.RefShardForTest().IncomingRelIDs(refNode.ID().SnowflakeID(), 0)
	if len(inIDs) != 0 {
		t.Errorf("expected 0 incoming rels after repair, got %d", len(inIDs))
	}
}

func TestTieredStore_Repair_OrphanedIncomingMissingEndNode(t *testing.T) {
	ts := newTestTieredStore(t)
	relTok, _ := registrypkg.NewRelTypeRegistry().GetOrCreate("TRIGGERED")

	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)
	missingEndID := nodeGen.Generate()
	startID := nodeGen.Generate()
	fakeRelID := relGen.Generate()

	if err := ts.RefShardForTest().PutRelIncoming(missingEndID, startID, relTok, fakeRelID); err != nil {
		t.Fatalf("PutRelIncoming: %v", err)
	}
	if got := ts.RefShardForTest().IncomingRelIDs(missingEndID, 0); len(got) != 1 || got[0] != fakeRelID {
		t.Fatalf("incoming before repair = %v, want [%d]", got, fakeRelID)
	}

	result, err := ts.RunRepair()
	if err != nil {
		t.Fatalf("RunRepair: %v", err)
	}
	if result.OrphanedInEntries != 1 {
		t.Fatalf("OrphanedInEntries = %d, want 1", result.OrphanedInEntries)
	}
	if got := ts.RefShardForTest().IncomingRelIDs(missingEndID, 0); len(got) != 0 {
		t.Fatalf("incoming after repair = %v, want empty", got)
	}
}

func TestTieredStoreRepairPurgesStaleTypeIndexWithOrphanedIncoming(t *testing.T) {
	relID := snowflake.ID(777)
	startID := snowflake.ID(10)
	endID := snowflake.ID(20)
	relType := uint16(7)
	stale := newBadgerStoreWithStaleRelTypeAndIncomingIndex(t, relID, startID, endID, relType)
	if stale.HasRelID(relID) {
		t.Fatal("Badger loadIndexes rebuilt relIDs from a stale type index without an entity row")
	}
	if got := stale.IncomingRelIDs(endID, 0); len(got) != 1 || got[0] != relID {
		t.Fatalf("incoming setup = %v, want [%d]", got, relID)
	}

	ts := &Store{}
	result, err := ts.runRepairStores([]namedStore{{name: "stale", store: stale}})
	if err != nil {
		t.Fatalf("runRepairStores: %v", err)
	}
	if result.OrphanedInEntries != 1 {
		t.Fatalf("OrphanedInEntries = %d, want 1", result.OrphanedInEntries)
	}
	if got := stale.IncomingRelIDs(endID, 0); len(got) != 0 {
		t.Fatalf("incoming after repair = %v, want empty", got)
	}
	if stale.HasRelID(relID) {
		t.Fatal("repair left stale relIDs/type index after purging orphan incoming")
	}
	if got, err := stale.RelationshipCount(); err != nil || got != 0 {
		t.Fatalf("RelationshipCount = %d, %v; want 0, nil", got, err)
	}
	if err := stale.Flush(); err != nil {
		t.Fatalf("Flush after repair: %v", err)
	}
	err = stale.DBForTest().View(func(txn *badgerv4.Txn) error {
		_, err := txn.Get(storeutil.RelTypeIndexKey(relType, relID))
		return err
	})
	if !errors.Is(err, badgerv4.ErrKeyNotFound) {
		t.Fatalf("stale reltype key after repair = %v, want ErrKeyNotFound", err)
	}
}

func TestTieredStore_Repair_MissingIncoming(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	sigTok, _ := reg.GetOrCreate("Signal")
	relTypeTok, _ := registrypkg.NewRelTypeRegistry().GetOrCreate("TRIGGERED")

	gen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	// Create a ref node and an event node.
	refNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(refNode); err != nil {
		t.Fatalf("PutNode ref: %v", err)
	}
	evtNode := types.NewNode(types.NodeID(gen.Generate()), sigTok, nil)
	if err := ts.PutNode(evtNode); err != nil {
		t.Fatalf("PutNode evt: %v", err)
	}

	// Create a cross-shard relationship (E→R) but ONLY the entity+out side.
	// This simulates a partial write failure where the in/ write didn't happen.
	relID := relGen.Generate()
	r := types.NewRelationship(types.RelID(relID), relTypeTok,
		evtNode.ID(),
		refNode.ID())

	// Write only entity+out to the event shard (hotShard).
	ts.MuForTest().RLock()
	hotStore := ts.HotShardForTest().Store()
	ts.MuForTest().RUnlock()
	if err := hotStore.PutRelEntityAndOut(r); err != nil {
		t.Fatalf("PutRelEntityAndOut: %v", err)
	}

	// Verify the in/ entry is missing on refShard.
	inIDs := ts.RefShardForTest().IncomingRelIDs(refNode.ID().SnowflakeID(), 0)
	if len(inIDs) != 0 {
		t.Fatalf("expected 0 incoming rels before repair, got %d", len(inIDs))
	}

	// Run repair.
	result, err := ts.RunRepair()
	if err != nil {
		t.Fatalf("RunRepair: %v", err)
	}
	if result.MissingInEntries != 1 {
		t.Errorf("MissingInEntries = %d, want 1", result.MissingInEntries)
	}

	// Verify the in/ entry was created.
	inIDs = ts.RefShardForTest().IncomingRelIDs(refNode.ID().SnowflakeID(), 0)
	if len(inIDs) != 1 {
		t.Errorf("expected 1 incoming rel after repair, got %d", len(inIDs))
	}
}

func TestTieredStoreRepairPurgesStaleIncomingTypeAndRecreatesCorrectEntry(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	sigTok, _ := reg.GetOrCreate("Signal")
	relTypeTok, _ := registrypkg.NewRelTypeRegistry().GetOrCreate("TRIGGERED")
	staleTypeTok := relTypeTok + 1

	gen := tieredNodeGen(t)
	refNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(refNode); err != nil {
		t.Fatalf("PutNode ref: %v", err)
	}
	evtNode := types.NewNode(types.NodeID(gen.Generate()), sigTok, nil)
	if err := ts.PutNode(evtNode); err != nil {
		t.Fatalf("PutNode evt: %v", err)
	}

	relID := tieredRelGen(t).Generate()
	r := types.NewRelationship(types.RelID(relID), relTypeTok, evtNode.ID(), refNode.ID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}
	if err := ts.RefShardForTest().PutRelIncoming(refNode.ID().SnowflakeID(), evtNode.ID().SnowflakeID(), staleTypeTok, relID); err != nil {
		t.Fatalf("PutRelIncoming stale type: %v", err)
	}
	if got := ts.RefShardForTest().IncomingRelIDs(refNode.ID().SnowflakeID(), staleTypeTok); len(got) != 1 || got[0] != relID {
		t.Fatalf("stale incoming setup = %v, want [%d]", got, relID)
	}

	result, err := ts.RunRepair()
	if err != nil {
		t.Fatalf("RunRepair: %v", err)
	}
	if result.StaleInEntries != 1 {
		t.Fatalf("StaleInEntries = %d, want 1", result.StaleInEntries)
	}
	if result.MissingInEntries != 1 {
		t.Fatalf("MissingInEntries = %d, want 1", result.MissingInEntries)
	}
	if got := ts.RefShardForTest().IncomingRelIDs(refNode.ID().SnowflakeID(), staleTypeTok); len(got) != 0 {
		t.Fatalf("stale incoming after repair = %v, want empty", got)
	}
	if got := ts.RefShardForTest().IncomingRelIDs(refNode.ID().SnowflakeID(), relTypeTok); len(got) != 1 || got[0] != relID {
		t.Fatalf("correct incoming after repair = %v, want [%d]", got, relID)
	}
	if err := ts.RefShardForTest().Flush(); err != nil {
		t.Fatalf("Flush after repair: %v", err)
	}
	staleKey := storeutil.InKey(refNode.ID().SnowflakeID(), staleTypeTok, evtNode.ID().SnowflakeID(), relID)
	err = ts.RefShardForTest().DBForTest().View(func(txn *badgerv4.Txn) error {
		_, err := txn.Get(staleKey)
		return err
	})
	if !errors.Is(err, badgerv4.ErrKeyNotFound) {
		t.Fatalf("stale incoming key after repair = %v, want ErrKeyNotFound", err)
	}
}

func TestTieredStoreRepairPurgesStaleIncomingWrongEnd(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	sigTok, _ := reg.GetOrCreate("Signal")
	relTypeTok, _ := registrypkg.NewRelTypeRegistry().GetOrCreate("TRIGGERED")

	gen := tieredNodeGen(t)
	caseA := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	caseB := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	signal := types.NewNode(types.NodeID(gen.Generate()), sigTok, nil)
	for _, n := range []*types.Node{caseA, caseB, signal} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}

	relID := tieredRelGen(t).Generate()
	r := types.NewRelationship(types.RelID(relID), relTypeTok, signal.ID(), caseA.ID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}
	if err := ts.RefShardForTest().PutRelIncoming(caseB.ID().SnowflakeID(), signal.ID().SnowflakeID(), relTypeTok, relID); err != nil {
		t.Fatalf("PutRelIncoming stale wrong end: %v", err)
	}

	result, err := ts.RunRepair()
	if err != nil {
		t.Fatalf("RunRepair: %v", err)
	}
	if result.StaleInEntries != 1 {
		t.Fatalf("StaleInEntries = %d, want 1", result.StaleInEntries)
	}
	if result.MissingInEntries != 0 {
		t.Fatalf("MissingInEntries = %d, want 0", result.MissingInEntries)
	}
	if got := ts.RefShardForTest().IncomingRelIDs(caseB.ID().SnowflakeID(), relTypeTok); len(got) != 0 {
		t.Fatalf("wrong-end incoming after repair = %v, want empty", got)
	}
	if got := ts.RefShardForTest().IncomingRelIDs(caseA.ID().SnowflakeID(), relTypeTok); len(got) != 1 || got[0] != relID {
		t.Fatalf("correct incoming after repair = %v, want [%d]", got, relID)
	}
}

func TestTieredStoreRepairPropagatesCorruptEndpointRows(t *testing.T) {
	for _, tc := range []struct {
		name        string
		corruptNode func(start, end *types.Node) types.NodeID
	}{
		{
			name: "start",
			corruptNode: func(start, end *types.Node) types.NodeID {
				return start.ID()
			},
		},
		{
			name: "end",
			corruptNode: func(start, end *types.Node) types.NodeID {
				return end.ID()
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := newTestTieredStore(t)
			reg := registrypkg.NewLabelRegistry()
			ts.SetLabelRegistry(reg)

			caseTok, _ := reg.GetOrCreate("Case")
			signalTok, _ := reg.GetOrCreate("Signal")
			relTypeTok, _ := registrypkg.NewRelTypeRegistry().GetOrCreate("TRIGGERED")
			gen := tieredNodeGen(t)
			start := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
			end := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
			if err := ts.PutNode(start); err != nil {
				t.Fatalf("PutNode start: %v", err)
			}
			if err := ts.PutNode(end); err != nil {
				t.Fatalf("PutNode end: %v", err)
			}
			rel := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), relTypeTok, start.ID(), end.ID())
			if err := ts.PutRelationship(rel); err != nil {
				t.Fatalf("PutRelationship: %v", err)
			}

			corruptID := tc.corruptNode(start, end)
			shard, checkin, err := ts.shardForNodeIDChecked(corruptID)
			if err != nil {
				t.Fatalf("shardForNodeIDChecked corrupt endpoint: %v", err)
			}
			defer checkin()
			corruptTieredNodeRowAfterFlush(t, shard, corruptID)

			_, err = ts.RunRepair()
			if err == nil {
				t.Fatal("RunRepair returned nil for corrupt endpoint row")
			}
			if errors.Is(err, ErrNodeNotFound) {
				t.Fatalf("RunRepair returned not-found for corrupt endpoint row: %v", err)
			}
		})
	}
}

func TestTieredStore_Repair_MissingIncomingUsesPinnedArchiveSnapshot(t *testing.T) {
	ts, caseTok, signalTok := newArchiveWriteTestStore(t)
	relTypeTok, _ := registrypkg.NewRelTypeRegistry().GetOrCreate("TRIGGERED")

	gen := tieredNodeGen(t)
	caseNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(caseNode); err != nil {
		t.Fatalf("PutNode case: %v", err)
	}
	signal := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(signal); err != nil {
		t.Fatalf("PutNode signal: %v", err)
	}
	if err := ts.ArchiveNode(caseNode.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	archive := ts.RefArchiveForTest().Load()
	if archive == nil {
		t.Fatal("ArchiveNode left refArchive nil")
	}

	relID := tieredRelGen(t).Generate()
	r := types.NewRelationship(types.RelID(relID), relTypeTok, signal.ID(), caseNode.ID())
	if err := ts.HotShardForTest().Store().PutRelEntityAndOut(r); err != nil {
		t.Fatalf("PutRelEntityAndOut: %v", err)
	}
	if hasIncomingEntry(archive, caseNode.ID().SnowflakeID(), relID) {
		t.Fatal("incoming entry unexpectedly exists before repair")
	}

	stores, release, err := ts.allShardStoresWithLazyOpen()
	if err != nil {
		t.Fatalf("allShardStoresWithLazyOpen: %v", err)
	}
	wasClosed := ts.closed.Load()
	ts.closed.Store(true)
	ts.refArchive.Store(nil)
	defer func() {
		ts.refArchive.Store(archive)
		ts.closed.Store(wasClosed)
		release()
	}()

	result, err := ts.runRepairStores(stores)
	if err != nil {
		t.Fatalf("runRepairStores: %v", err)
	}
	if result.MissingInEntries != 1 {
		t.Fatalf("MissingInEntries = %d, want 1", result.MissingInEntries)
	}
	if !hasIncomingEntry(archive, caseNode.ID().SnowflakeID(), relID) {
		t.Fatal("repair did not recreate archive incoming entry")
	}
}
