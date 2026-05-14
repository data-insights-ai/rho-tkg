package badger

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"
	snowflakepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/snowflake"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/storeutil"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

// newTestBadgerStoreInMemory creates an in-memory Store for testing.
func newTestBadgerStoreInMemory(t *testing.T) *Store {
	t.Helper()
	bs, err := New(Config{
		InMemory:      true,
		FlushInterval: 1<<63 - 1, // effectively disable periodic flush
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	return bs
}

func TestPutRelEntityAndOut_CreatesEntityButNotInIdx(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)

	// Create two nodes.
	gen := newTestGen(t, 0)
	n1 := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	n2 := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := bs.PutNode(n1); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(n2); err != nil {
		t.Fatal(err)
	}

	// Create a relationship using partial write.
	relGen := newTestGen(t, 1)
	relID := relGen.Generate()
	r := types.NewRelationship(types.RelID(relID), 1, n1.ID(), n2.ID())
	if err := bs.PutRelEntityAndOut(r); err != nil {
		t.Fatal(err)
	}

	// Entity should exist.
	if !bs.HasRelID(relID) {
		t.Error("HasRelID should be true after PutRelEntityAndOut")
	}

	// GetRelationship should work.
	got, err := bs.GetRelationship(types.RelID(relID))
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if got.ID().SnowflakeID() != relID {
		t.Error("relationship ID mismatch")
	}

	// outIdx should contain the rel.
	outIDs := bs.OutgoingRelIDs(n1.ID().SnowflakeID())
	if len(outIDs) != 1 || outIDs[0] != relID {
		t.Errorf("OutgoingRelIDs = %v, want [%d]", outIDs, relID)
	}

	// inIdx should NOT contain the rel (partial write skips in/).
	inIDs := bs.IncomingRelIDs(n2.ID().SnowflakeID(), 0)
	if len(inIDs) != 0 {
		t.Errorf("IncomingRelIDs should be empty after PutRelEntityAndOut, got %v", inIDs)
	}
}

func TestPutRelEntityAndOutRejectsInvalidPayload(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)

	tests := []struct {
		name string
		rel  *types.Relationship
	}{
		{
			name: "nil",
			rel:  nil,
		},
		{
			name: "zero rel ID",
			rel:  types.NewRelationship(types.RelID(0), 1, types.NodeID(1), types.NodeID(2)),
		},
		{
			name: "zero start",
			rel:  types.NewRelationship(types.RelID(100), 1, types.NodeID(0), types.NodeID(2)),
		},
		{
			name: "zero end",
			rel:  types.NewRelationship(types.RelID(100), 1, types.NodeID(1), types.NodeID(0)),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := bs.PutRelEntityAndOut(tc.rel); !errors.Is(err, ErrInvalidStoreMutation) {
				t.Fatalf("PutRelEntityAndOut(%s) = %v, want ErrInvalidStoreMutation", tc.name, err)
			}
		})
	}

	if count, err := bs.RelationshipCount(); err != nil || count != 0 {
		t.Fatalf("RelationshipCount after rejected partial writes = %d, %v; want 0, nil", count, err)
	}
}

func TestPutRelIncoming_CreatesInIdxOnly(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)

	// Create one node (the endpoint).
	gen := newTestGen(t, 0)
	endNode := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := bs.PutNode(endNode); err != nil {
		t.Fatal(err)
	}

	startID := snowflake.ID(999999)
	relID := snowflake.ID(888888)
	var relType uint16 = 1

	if err := bs.PutRelIncoming(endNode.ID().SnowflakeID(), startID, relType, relID); err != nil {
		t.Fatal(err)
	}

	// inIdx should contain the rel.
	inIDs := bs.IncomingRelIDs(endNode.ID().SnowflakeID(), 0)
	if len(inIDs) != 1 || inIDs[0] != relID {
		t.Errorf("IncomingRelIDs = %v, want [%d]", inIDs, relID)
	}

	// Rel entity should NOT exist (only the in/ index was written).
	if bs.HasRelID(relID) {
		t.Error("HasRelID should be false — PutRelIncoming doesn't store entity")
	}
}

func TestPutRelIncomingRejectsZeroIndexFields(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)

	tests := []struct {
		name    string
		endID   snowflake.ID
		startID snowflake.ID
		relType uint16
		relID   snowflake.ID
	}{
		{name: "zero end", endID: 0, startID: 2, relType: 1, relID: 3},
		{name: "zero start", endID: 1, startID: 0, relType: 1, relID: 3},
		{name: "zero type", endID: 1, startID: 2, relType: 0, relID: 3},
		{name: "zero rel", endID: 1, startID: 2, relType: 1, relID: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := bs.PutRelIncoming(tc.endID, tc.startID, tc.relType, tc.relID)
			if !errors.Is(err, ErrInvalidStoreMutation) {
				t.Fatalf("PutRelIncoming(%s) = %v, want ErrInvalidStoreMutation", tc.name, err)
			}
		})
	}

	if got := bs.IncomingRelIDs(1, 0); len(got) != 0 {
		t.Fatalf("incoming index after rejected partial writes = %v, want empty", got)
	}
}

func TestDeleteRelEntityAndOut_RemovesEntityButNotInIdx(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)

	gen := newTestGen(t, 0)
	n1 := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	n2 := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := bs.PutNode(n1); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(n2); err != nil {
		t.Fatal(err)
	}

	// Use full PutRelationship first, then partial delete.
	relGen := newTestGen(t, 1)
	relID := relGen.Generate()
	r := types.NewRelationship(types.RelID(relID), 1, n1.ID(), n2.ID())
	if err := bs.PutRelationship(r); err != nil {
		t.Fatal(err)
	}

	info, err := bs.DeleteRelEntityAndOut(relID)
	if err != nil {
		t.Fatalf("DeleteRelEntityAndOut: %v", err)
	}

	// Entity should be gone.
	if bs.HasRelID(relID) {
		t.Error("HasRelID should be false after DeleteRelEntityAndOut")
	}

	// outIdx should be empty.
	outIDs := bs.OutgoingRelIDs(n1.ID().SnowflakeID())
	if len(outIDs) != 0 {
		t.Errorf("OutgoingRelIDs should be empty, got %v", outIDs)
	}

	// inIdx should STILL contain the rel (partial delete doesn't touch in/).
	inIDs := bs.IncomingRelIDs(n2.ID().SnowflakeID(), 0)
	if len(inIDs) != 1 || inIDs[0] != relID {
		t.Errorf("IncomingRelIDs should still contain rel, got %v", inIDs)
	}

	// Verify info is populated correctly.
	if info.ID != relID {
		t.Errorf("info.ID = %d, want %d", info.ID, relID)
	}
	if info.StartID != n1.ID().SnowflakeID() {
		t.Error("info.StartID mismatch")
	}
	if info.EndID != n2.ID().SnowflakeID() {
		t.Error("info.EndID mismatch")
	}
}

func TestDeleteRelEntityAndOutRejectsStaleEntityRowNotInLiveSet(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)

	gen := newTestGen(t, 0)
	n1 := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	n2 := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := bs.PutNode(n1); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(n2); err != nil {
		t.Fatal(err)
	}

	relGen := newTestGen(t, 1)
	relID := relGen.Generate()
	r := types.NewRelationship(types.RelID(relID), 1, n1.ID(), n2.ID())
	if err := bs.PutRelationship(r); err != nil {
		t.Fatal(err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush setup: %v", err)
	}

	bs.idxMu.Lock()
	delete(bs.relIDs, r.InternalID())
	bs.relCache.ResetForTest()
	bs.idxMu.Unlock()

	_, err := bs.DeleteRelEntityAndOut(relID)
	if !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("DeleteRelEntityAndOut with stale row = %v, want ErrRelNotFound", err)
	}

	if got := bs.OutgoingRelIDs(n1.ID().SnowflakeID()); len(got) != 1 || got[0] != relID {
		t.Fatalf("OutgoingRelIDs after rejected stale delete = %v, want [%d]", got, relID)
	}
	if got := bs.IncomingRelIDs(n2.ID().SnowflakeID(), 0); len(got) != 1 || got[0] != relID {
		t.Fatalf("IncomingRelIDs after rejected stale delete = %v, want [%d]", got, relID)
	}
	count, err := bs.RelationshipCount()
	if err != nil {
		t.Fatalf("RelationshipCount: %v", err)
	}
	if count != 1 {
		t.Fatalf("RelationshipCount after rejected stale delete = %d, want 1", count)
	}
}

func TestDeleteRelEntityAndOutRejectsInvalidID(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)

	tests := []struct {
		name string
		id   snowflake.ID
	}{
		{name: "zero", id: 0},
		{name: "negative", id: snowflake.ID(-1)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := bs.DeleteRelEntityAndOut(tc.id); !errors.Is(err, ErrInvalidStoreMutation) {
				t.Fatalf("DeleteRelEntityAndOut(%s) = %v, want ErrInvalidStoreMutation", tc.name, err)
			}
		})
	}
}

func TestDeleteRelIncoming_RemovesInIdxOnly(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)

	gen := newTestGen(t, 0)
	n1 := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	n2 := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := bs.PutNode(n1); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(n2); err != nil {
		t.Fatal(err)
	}

	// Full PutRelationship.
	relGen := newTestGen(t, 1)
	relID := relGen.Generate()
	r := types.NewRelationship(types.RelID(relID), 1, n1.ID(), n2.ID())
	if err := bs.PutRelationship(r); err != nil {
		t.Fatal(err)
	}

	info := RelDeleteInfo{
		ID:      relID,
		RelType: r.TypeToken().Value(),
		StartID: n1.ID().SnowflakeID(),
		EndID:   n2.ID().SnowflakeID(),
	}
	if err := bs.DeleteRelIncoming(info); err != nil {
		t.Fatal(err)
	}

	// inIdx should be empty.
	inIDs := bs.IncomingRelIDs(n2.ID().SnowflakeID(), 0)
	if len(inIDs) != 0 {
		t.Errorf("IncomingRelIDs should be empty after DeleteRelIncoming, got %v", inIDs)
	}

	// Entity should STILL exist.
	if !bs.HasRelID(relID) {
		t.Error("HasRelID should be true — DeleteRelIncoming doesn't remove entity")
	}

	// outIdx should still have the rel.
	outIDs := bs.OutgoingRelIDs(n1.ID().SnowflakeID())
	if len(outIDs) != 1 || outIDs[0] != relID {
		t.Errorf("OutgoingRelIDs should still contain rel, got %v", outIDs)
	}
}

func TestDeleteRelIncomingRejectsMismatchedRelTypeWithoutDroppingMemory(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)

	endID := snowflake.ID(1001)
	startID := snowflake.ID(2002)
	relID := snowflake.ID(3003)
	if err := bs.PutRelIncoming(endID, startID, 7, relID); err != nil {
		t.Fatalf("PutRelIncoming: %v", err)
	}

	err := bs.DeleteRelIncoming(RelDeleteInfo{
		ID:      relID,
		RelType: 8,
		StartID: startID,
		EndID:   endID,
	})
	if !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("DeleteRelIncoming with mismatched type = %v, want ErrRelNotFound", err)
	}
	if got := bs.IncomingRelIDs(endID, 0); len(got) != 1 || got[0] != relID {
		t.Fatalf("IncomingRelIDs after rejected mismatched delete = %v, want [%d]", got, relID)
	}
	if got := bs.IncomingRelIDs(endID, 7); len(got) != 1 || got[0] != relID {
		t.Fatalf("IncomingRelIDs(type=7) after rejected mismatched delete = %v, want [%d]", got, relID)
	}
}

func TestDeleteRelIncomingRejectsZeroIndexFields(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)

	tests := []struct {
		name string
		info RelDeleteInfo
	}{
		{
			name: "zero end",
			info: RelDeleteInfo{ID: 3, RelType: 1, StartID: 2, EndID: 0},
		},
		{
			name: "zero start",
			info: RelDeleteInfo{ID: 3, RelType: 1, StartID: 0, EndID: 1},
		},
		{
			name: "zero type",
			info: RelDeleteInfo{ID: 3, RelType: 0, StartID: 2, EndID: 1},
		},
		{
			name: "zero rel",
			info: RelDeleteInfo{ID: 0, RelType: 1, StartID: 2, EndID: 1},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := bs.DeleteRelIncoming(tc.info)
			if !errors.Is(err, ErrInvalidStoreMutation) {
				t.Fatalf("DeleteRelIncoming(%s) = %v, want ErrInvalidStoreMutation", tc.name, err)
			}
		})
	}

	if got := bs.IncomingRelIDs(1, 0); len(got) != 0 {
		t.Fatalf("incoming index after rejected delete helpers = %v, want empty", got)
	}
}

func TestScanAndDeleteIncoming_DeletesPersistedInKey(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)

	endID := snowflake.ID(1001)
	startID := snowflake.ID(2002)
	relID := snowflake.ID(3003)
	relType := uint16(7)
	if err := bs.PutRelIncoming(endID, startID, relType, relID); err != nil {
		t.Fatalf("PutRelIncoming: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush setup: %v", err)
	}

	key := storepkg.InKey(endID, relType, startID, relID)
	if err := bs.db.View(func(txn *badgerv4.Txn) error {
		_, err := txn.Get(key)
		return err
	}); err != nil {
		t.Fatalf("persisted incoming key missing before scan delete: %v", err)
	}

	if err := bs.ScanAndDeleteIncoming(endID, relID); err != nil {
		t.Fatalf("ScanAndDeleteIncoming: %v", err)
	}
	if got := bs.IncomingRelIDs(endID, 0); len(got) != 0 {
		t.Fatalf("IncomingRelIDs after scan delete = %v, want empty", got)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush delete: %v", err)
	}

	err := bs.db.View(func(txn *badgerv4.Txn) error {
		_, err := txn.Get(key)
		return err
	})
	if !errors.Is(err, badgerv4.ErrKeyNotFound) {
		t.Fatalf("persisted incoming key after scan delete = %v, want ErrKeyNotFound", err)
	}
}

func TestDeleteIncomingByRelID_MatchesPendingEndNode(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)

	relID := snowflake.ID(3003)
	endA := snowflake.ID(1001)
	endB := snowflake.ID(2002)
	keyA := storepkg.InKey(endA, 7, snowflake.ID(4004), relID)
	keyB := storepkg.InKey(endB, 7, snowflake.ID(5005), relID)

	if err := bs.PutRelIncoming(endA, snowflake.ID(4004), 7, relID); err != nil {
		t.Fatalf("PutRelIncoming A: %v", err)
	}
	if err := bs.PutRelIncoming(endB, snowflake.ID(5005), 7, relID); err != nil {
		t.Fatalf("PutRelIncoming B: %v", err)
	}
	if err := bs.DeleteIncomingByRelID(endA, relID); err != nil {
		t.Fatalf("DeleteIncomingByRelID: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	errA := bs.db.View(func(txn *badgerv4.Txn) error {
		_, err := txn.Get(keyA)
		return err
	})
	if !errors.Is(errA, badgerv4.ErrKeyNotFound) {
		t.Fatalf("endA key after delete = %v, want ErrKeyNotFound", errA)
	}
	if err := bs.db.View(func(txn *badgerv4.Txn) error {
		_, err := txn.Get(keyB)
		return err
	}); err != nil {
		t.Fatalf("endB key after delete = %v, want present", err)
	}
}

func TestDeleteIncomingByRelIDDeletesDuplicateIncomingKeys(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)

	endID := snowflake.ID(1001)
	relID := snowflake.ID(3003)
	startA := snowflake.ID(4004)
	startB := snowflake.ID(5005)
	keyA := storepkg.InKey(endID, 7, startA, relID)
	keyB := storepkg.InKey(endID, 8, startB, relID)

	if err := bs.PutRelIncoming(endID, startA, 7, relID); err != nil {
		t.Fatalf("PutRelIncoming A: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush A: %v", err)
	}
	if err := bs.PutRelIncoming(endID, startB, 8, relID); err != nil {
		t.Fatalf("PutRelIncoming B: %v", err)
	}

	if err := bs.DeleteIncomingByRelID(endID, relID); err != nil {
		t.Fatalf("DeleteIncomingByRelID: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush delete: %v", err)
	}
	if got := bs.IncomingRelIDs(endID, 0); len(got) != 0 {
		t.Fatalf("IncomingRelIDs after duplicate delete = %v, want empty", got)
	}
	for name, key := range map[string][]byte{"persisted": keyA, "pending": keyB} {
		err := bs.db.View(func(txn *badgerv4.Txn) error {
			_, err := txn.Get(key)
			return err
		})
		if !errors.Is(err, badgerv4.ErrKeyNotFound) {
			t.Fatalf("%s incoming key after duplicate delete = %v, want ErrKeyNotFound", name, err)
		}
	}
}

func TestDeleteIncomingByRelIDDeletesPersistedKeyWhenMemoryMissing(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)

	endID := snowflake.ID(1001)
	startID := snowflake.ID(4004)
	relID := snowflake.ID(3003)
	relType := uint16(7)
	key := storepkg.InKey(endID, relType, startID, relID)

	if err := bs.PutRelIncoming(endID, startID, relType, relID); err != nil {
		t.Fatalf("PutRelIncoming: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush setup: %v", err)
	}

	bs.idxMu.Lock()
	delete(bs.inIdx[types.NodeID(endID)], types.RelID(relID))
	if len(bs.inIdx[types.NodeID(endID)]) == 0 {
		delete(bs.inIdx, types.NodeID(endID))
	}
	bs.idxMu.Unlock()

	if err := bs.DeleteIncomingByRelID(endID, relID); err != nil {
		t.Fatalf("DeleteIncomingByRelID: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush delete: %v", err)
	}

	err := bs.db.View(func(txn *badgerv4.Txn) error {
		_, err := txn.Get(key)
		return err
	})
	if !errors.Is(err, badgerv4.ErrKeyNotFound) {
		t.Fatalf("persisted incoming key after memory-missing delete = %v, want ErrKeyNotFound", err)
	}
}

func TestScanAndDeleteIncoming_MatchesPendingEndNode(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)

	relID := snowflake.ID(3003)
	endA := snowflake.ID(1001)
	endB := snowflake.ID(2002)
	keyA := storepkg.InKey(endA, 7, snowflake.ID(4004), relID)
	keyB := storepkg.InKey(endB, 7, snowflake.ID(5005), relID)

	if err := bs.PutRelIncoming(endA, snowflake.ID(4004), 7, relID); err != nil {
		t.Fatalf("PutRelIncoming A: %v", err)
	}
	if err := bs.PutRelIncoming(endB, snowflake.ID(5005), 7, relID); err != nil {
		t.Fatalf("PutRelIncoming B: %v", err)
	}
	if err := bs.ScanAndDeleteIncoming(endA, relID); err != nil {
		t.Fatalf("ScanAndDeleteIncoming: %v", err)
	}
	if got := bs.IncomingRelIDs(endA, 0); len(got) != 0 {
		t.Fatalf("endA IncomingRelIDs after scan delete = %v, want empty", got)
	}
	if got := bs.IncomingRelIDs(endB, 0); len(got) != 1 || got[0] != relID {
		t.Fatalf("endB IncomingRelIDs after scan delete = %v, want [%d]", got, relID)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	errA := bs.db.View(func(txn *badgerv4.Txn) error {
		_, err := txn.Get(keyA)
		return err
	})
	if !errors.Is(errA, badgerv4.ErrKeyNotFound) {
		t.Fatalf("endA key after scan delete = %v, want ErrKeyNotFound", errA)
	}
	if err := bs.db.View(func(txn *badgerv4.Txn) error {
		_, err := txn.Get(keyB)
		return err
	}); err != nil {
		t.Fatalf("endB key after scan delete = %v, want present", err)
	}
}

func TestDeleteIncomingByRelIDRejectsInvalidFields(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)

	tests := []struct {
		name  string
		endID snowflake.ID
		relID snowflake.ID
	}{
		{name: "zero end", endID: 0, relID: 3003},
		{name: "zero rel", endID: 1001, relID: 0},
		{name: "negative end", endID: snowflake.ID(-1), relID: 3003},
		{name: "negative rel", endID: 1001, relID: snowflake.ID(-1)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := bs.DeleteIncomingByRelID(tc.endID, tc.relID)
			if !errors.Is(err, ErrInvalidStoreMutation) {
				t.Fatalf("DeleteIncomingByRelID(%s) = %v, want ErrInvalidStoreMutation", tc.name, err)
			}
		})
	}
}

func TestScanAndDeleteIncomingRejectsInvalidFields(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)

	tests := []struct {
		name  string
		endID snowflake.ID
		relID snowflake.ID
	}{
		{name: "zero end", endID: 0, relID: 3003},
		{name: "zero rel", endID: 1001, relID: 0},
		{name: "negative end", endID: snowflake.ID(-1), relID: 3003},
		{name: "negative rel", endID: 1001, relID: snowflake.ID(-1)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := bs.ScanAndDeleteIncoming(tc.endID, tc.relID)
			if !errors.Is(err, ErrInvalidStoreMutation) {
				t.Fatalf("ScanAndDeleteIncoming(%s) = %v, want ErrInvalidStoreMutation", tc.name, err)
			}
		})
	}
}

func TestHasNodeID_HasRelID(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)

	gen := newTestGen(t, 0)
	nodeID := gen.Generate()
	n := types.NewNode(types.NodeID(nodeID), 1, nil)
	if err := bs.PutNode(n); err != nil {
		t.Fatal(err)
	}

	if !bs.HasNodeID(nodeID) {
		t.Error("HasNodeID should be true")
	}
	if bs.HasNodeID(snowflake.ID(999)) {
		t.Error("HasNodeID should be false for unknown ID")
	}
	if bs.HasRelID(snowflake.ID(999)) {
		t.Error("HasRelID should be false for unknown ID")
	}
}

func TestOutgoingRelIDs_Sorted(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)

	gen := newTestGen(t, 0)
	n1 := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	n2 := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := bs.PutNode(n1); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(n2); err != nil {
		t.Fatal(err)
	}

	relGen := newTestGen(t, 1)
	for i := 0; i < 5; i++ {
		relID := relGen.Generate()
		r := types.NewRelationship(types.RelID(relID), 1, n1.ID(), n2.ID())
		if err := bs.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
	}

	outIDs := bs.OutgoingRelIDs(n1.ID().SnowflakeID())
	if len(outIDs) != 5 {
		t.Fatalf("OutgoingRelIDs count = %d, want 5", len(outIDs))
	}
	for i := 1; i < len(outIDs); i++ {
		if outIDs[i] <= outIDs[i-1] {
			t.Errorf("OutgoingRelIDs not sorted: [%d]=%d >= [%d]=%d", i-1, outIDs[i-1], i, outIDs[i])
		}
	}
}

func TestIncomingRelIDs_TypeFilter(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)

	gen := newTestGen(t, 0)
	n1 := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	n2 := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := bs.PutNode(n1); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(n2); err != nil {
		t.Fatal(err)
	}

	relGen := newTestGen(t, 1)

	// Type 1 rel.
	r1 := types.NewRelationship(types.RelID(relGen.Generate()), 1, n1.ID(), n2.ID())
	if err := bs.PutRelationship(r1); err != nil {
		t.Fatal(err)
	}

	// Type 2 rel.
	r2 := types.NewRelationship(types.RelID(relGen.Generate()), 2, n1.ID(), n2.ID())
	if err := bs.PutRelationship(r2); err != nil {
		t.Fatal(err)
	}

	// All types.
	all := bs.IncomingRelIDs(n2.ID().SnowflakeID(), 0)
	if len(all) != 2 {
		t.Fatalf("IncomingRelIDs(0) = %d, want 2", len(all))
	}

	// Filter type 1.
	type1 := bs.IncomingRelIDs(n2.ID().SnowflakeID(), 1)
	if len(type1) != 1 {
		t.Fatalf("IncomingRelIDs(1) = %d, want 1", len(type1))
	}

	// Filter type 2.
	type2 := bs.IncomingRelIDs(n2.ID().SnowflakeID(), 2)
	if len(type2) != 1 {
		t.Fatalf("IncomingRelIDs(2) = %d, want 1", len(type2))
	}

	// Filter unknown type.
	type99 := bs.IncomingRelIDs(n2.ID().SnowflakeID(), 99)
	if len(type99) != 0 {
		t.Errorf("IncomingRelIDs(99) = %d, want 0", len(type99))
	}
}

func TestIncomingIndexEntries_IncludesEntriesWithoutEndNode(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)

	endB := snowflake.ID(2002)
	endA := snowflake.ID(1001)
	if err := bs.PutRelIncoming(endB, snowflake.ID(3003), 9, snowflake.ID(5005)); err != nil {
		t.Fatalf("PutRelIncoming B: %v", err)
	}
	if err := bs.PutRelIncoming(endA, snowflake.ID(4004), 7, snowflake.ID(6006)); err != nil {
		t.Fatalf("PutRelIncoming A: %v", err)
	}

	entries := bs.IncomingIndexEntries()
	if len(entries) != 2 {
		t.Fatalf("IncomingIndexEntries = %d, want 2", len(entries))
	}
	if entries[0].EndID != endA || entries[0].RelID != snowflake.ID(6006) || entries[0].RelType != 7 {
		t.Fatalf("entry[0] = %+v, want end=%d rel=6006 type=7", entries[0], endA)
	}
	if entries[1].EndID != endB || entries[1].RelID != snowflake.ID(5005) || entries[1].RelType != 9 {
		t.Fatalf("entry[1] = %+v, want end=%d rel=5005 type=9", entries[1], endB)
	}
}

// newTestGen creates a snowflake generator for testing.
func newTestGen(t *testing.T, nodeID int64) *snowflake.Node {
	t.Helper()
	gen, err := snowflake.NewNode(nodeID,
		snowflake.WithEpoch(snowflakepkg.Epoch),
		snowflake.WithMicroseconds(),
		snowflake.WithNodeBits(5),
		snowflake.WithStepBits(10),
	)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return gen
}
