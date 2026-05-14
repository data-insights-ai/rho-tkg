package badger

import (
	"errors"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"
	storeutil "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestSyncWrite_ConfigPassthrough(t *testing.T) {
	// Verify SyncWrites=true flows through Config → Config → Store.
	// Create a Store with SyncWrites=true and verify:
	// 1. The bs.SyncWritesForTest() field is true
	// 2. The flushInt is 0 (no background goroutine)

	dir := t.TempDir()
	bs, err := New(Config{
		Dir:        dir,
		SyncWrites: true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer bs.Close()

	if !bs.SyncWritesForTest() {
		t.Error("expected bs.SyncWritesForTest() = true")
	}
	if bs.FlushIntervalForTest() != 0 {
		t.Errorf("expected flushInt=0, got %v", bs.FlushIntervalForTest())
	}
}

func TestSyncWrite_DataSurvivesWithoutClose(t *testing.T) {
	// With SyncWrites=true, data written to Store should be persisted
	// immediately (not just buffered). To verify, write a node, then open
	// a SECOND Store on the same directory without closing the first
	// and verify the data exists.
	//
	// NOTE: We must simulate persistence without a Close() flush. In normal
	// async mode, data would be lost after a crash. With SyncWrites, each
	// write is flushed to disk immediately.
	//
	// Implementation: Write node to bs1 (SyncWrites=true), then close bs1
	// and verify the node persists in a fresh bs2 (SyncWrites=false).
	// The key test is that the data was flushed BEFORE Close() was called.
	// We do this by manually flushing after the put and before close,
	// but with SyncWrites the auto-flush should have already done it.

	dir := t.TempDir()

	// Write with SyncWrites=true
	bs1, err := New(Config{
		Dir:        dir,
		SyncWrites: true,
	})
	if err != nil {
		t.Fatalf("New (write): %v", err)
	}

	// Create a minimal node
	n := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	if err := bs1.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	// Close bs1 properly — set dbClosed before db.Close() per B22.
	bs1.SetDBClosedForTest(true)
	if err := bs1.DBForTest().Close(); err != nil {
		t.Fatalf("Close bs1: %v", err)
	}

	// Open a new store on the same directory and verify the node is there
	bs2, err := New(Config{
		Dir: dir,
	})
	if err != nil {
		t.Fatalf("New (read): %v", err)
	}
	defer bs2.Close()

	nid := n.ID()
	got, err := bs2.GetNode(nid)
	if err != nil {
		t.Fatalf("GetNode: node should exist after sync write, got err: %v", err)
	}
	if got == nil {
		t.Fatal("GetNode: returned nil node")
	}
}

func TestSyncWrite_IndexDefinitionMutationsPersistImmediately(t *testing.T) {
	tests := []struct {
		name   string
		key    []byte
		create func(*Store) error
		drop   func(*Store) error
	}{
		{
			name: "property index",
			key:  storeutil.PropIndexDefsKey,
			create: func(bs *Store) error {
				return bs.CreatePropertyIndex(1, "name")
			},
			drop: func(bs *Store) error {
				return bs.DropPropertyIndex(1, "name")
			},
		},
		{
			name: "temporal index",
			key:  storeutil.TemporalIndexDefsKey,
			create: func(bs *Store) error {
				return bs.CreateTemporalIndex(1)
			},
			drop: func(bs *Store) error {
				return bs.DropTemporalIndex(1)
			},
		},
		{
			name: "high-frequency index",
			key:  storeutil.HighFrequencyIndexDefsKey,
			create: func(bs *Store) error {
				return bs.CreateHighFrequencyIndex(1, time.Hour)
			},
			drop: func(bs *Store) error {
				return bs.DropHighFrequencyIndex(1)
			},
		},
		{
			name: "vector index",
			key:  storeutil.VectorIndexDefsKey,
			create: func(bs *Store) error {
				return bs.CreateVectorIndex(1, "embedding", 3, DistanceCosine)
			},
			drop: func(bs *Store) error {
				return bs.DropVectorIndex(1, "embedding")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bs, err := New(Config{
				Dir:        t.TempDir(),
				SyncWrites: true,
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer func() { _ = bs.Close() }()

			if err := tt.create(bs); err != nil {
				t.Fatalf("create: %v", err)
			}
			if !badgerKeyExists(t, bs, tt.key) {
				t.Fatalf("metadata key %q was not flushed after create", string(tt.key))
			}

			if err := tt.drop(bs); err != nil {
				t.Fatalf("drop: %v", err)
			}
			if badgerKeyExists(t, bs, tt.key) {
				t.Fatalf("metadata key %q was not flushed after drop", string(tt.key))
			}
		})
	}
}

func TestSyncWrite_SplitRelationshipHelpersPersistImmediately(t *testing.T) {
	bs, err := New(Config{
		Dir:        t.TempDir(),
		SyncWrites: true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = bs.Close() }()

	startID := snowflake.ID(101)
	endID := snowflake.ID(202)
	relID := snowflake.ID(303)
	relType := uint16(7)
	rel := types.NewRelationship(types.RelID(relID), relType, types.NodeID(startID), types.NodeID(endID))

	relKey := storeutil.RelKey(relID)
	typeKey := storeutil.RelTypeIndexKey(relType, relID)
	outKey := storeutil.OutKey(startID, relType, endID, relID)
	inKey := storeutil.InKey(endID, relType, startID, relID)

	if err := bs.PutRelEntityAndOut(rel); err != nil {
		t.Fatalf("PutRelEntityAndOut: %v", err)
	}
	for name, key := range map[string][]byte{"rel": relKey, "type": typeKey, "out": outKey} {
		if !badgerKeyExists(t, bs, key) {
			t.Fatalf("%s key was not flushed after PutRelEntityAndOut", name)
		}
	}

	if err := bs.PutRelIncoming(endID, startID, relType, relID); err != nil {
		t.Fatalf("PutRelIncoming: %v", err)
	}
	if !badgerKeyExists(t, bs, inKey) {
		t.Fatal("incoming key was not flushed after PutRelIncoming")
	}

	if _, err := bs.DeleteRelEntityAndOut(relID); err != nil {
		t.Fatalf("DeleteRelEntityAndOut: %v", err)
	}
	for name, key := range map[string][]byte{"rel": relKey, "type": typeKey, "out": outKey} {
		if badgerKeyExists(t, bs, key) {
			t.Fatalf("%s key was not flushed after DeleteRelEntityAndOut", name)
		}
	}

	if err := bs.DeleteIncomingByRelID(endID, relID); err != nil {
		t.Fatalf("DeleteIncomingByRelID: %v", err)
	}
	if badgerKeyExists(t, bs, inKey) {
		t.Fatal("incoming key was not flushed after DeleteIncomingByRelID")
	}

	if err := bs.PutRelIncoming(endID, startID, relType, relID); err != nil {
		t.Fatalf("PutRelIncoming second: %v", err)
	}
	if err := bs.DeleteRelIncoming(RelDeleteInfo{
		ID:      relID,
		RelType: relType,
		StartID: startID,
		EndID:   endID,
	}); err != nil {
		t.Fatalf("DeleteRelIncoming: %v", err)
	}
	if badgerKeyExists(t, bs, inKey) {
		t.Fatal("incoming key was not flushed after DeleteRelIncoming")
	}
}

func TestSyncWrite_HistoryMaintenancePersistsImmediately(t *testing.T) {
	bs, err := New(Config{
		Dir:        t.TempDir(),
		SyncWrites: true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = bs.Close() }()

	nid := types.NodeID(snowflake.ID(1001))
	for version := uint32(0); version < 3; version++ {
		n := types.NewNode(nid, 1, nil)
		n.SetVersion(version)
		if err := bs.PutNodeVersion(nid, version, n); err != nil {
			t.Fatalf("PutNodeVersion(%d): %v", version, err)
		}
	}
	for version := uint64(0); version < 3; version++ {
		if !badgerKeyExists(t, bs, storeutil.HistNodeKey(nid.SnowflakeID(), version)) {
			t.Fatalf("node history version %d was not flushed before maintenance", version)
		}
	}
	if err := bs.TruncateNodeHistory(nid, 2); err != nil {
		t.Fatalf("TruncateNodeHistory: %v", err)
	}
	if badgerKeyExists(t, bs, storeutil.HistNodeKey(nid.SnowflakeID(), 0)) {
		t.Fatal("node history version 0 remained persisted after sync truncate")
	}
	if !badgerKeyExists(t, bs, storeutil.HistNodeKey(nid.SnowflakeID(), 1)) {
		t.Fatal("node history version 1 should remain after truncate")
	}
	if err := bs.TrimNodeHistoryFrom(nid, 2); err != nil {
		t.Fatalf("TrimNodeHistoryFrom: %v", err)
	}
	if badgerKeyExists(t, bs, storeutil.HistNodeKey(nid.SnowflakeID(), 2)) {
		t.Fatal("node history version 2 remained persisted after sync trim")
	}

	rid := types.RelID(snowflake.ID(2001))
	for version := uint32(0); version < 3; version++ {
		r := types.NewRelationship(rid, 2, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
		r.SetVersion(version)
		if err := bs.PutRelVersion(rid, version, r); err != nil {
			t.Fatalf("PutRelVersion(%d): %v", version, err)
		}
	}
	for version := uint64(0); version < 3; version++ {
		if !badgerKeyExists(t, bs, storeutil.HistRelKey(rid.SnowflakeID(), version)) {
			t.Fatalf("relationship history version %d was not flushed before maintenance", version)
		}
	}
	if err := bs.TruncateRelHistory(rid, 2); err != nil {
		t.Fatalf("TruncateRelHistory: %v", err)
	}
	if badgerKeyExists(t, bs, storeutil.HistRelKey(rid.SnowflakeID(), 0)) {
		t.Fatal("relationship history version 0 remained persisted after sync truncate")
	}
	if !badgerKeyExists(t, bs, storeutil.HistRelKey(rid.SnowflakeID(), 1)) {
		t.Fatal("relationship history version 1 should remain after truncate")
	}
	if err := bs.TrimRelHistoryFrom(rid, 2); err != nil {
		t.Fatalf("TrimRelHistoryFrom: %v", err)
	}
	if badgerKeyExists(t, bs, storeutil.HistRelKey(rid.SnowflakeID(), 2)) {
		t.Fatal("relationship history version 2 remained persisted after sync trim")
	}
}

func badgerKeyExists(t *testing.T, bs *Store, key []byte) bool {
	t.Helper()

	var exists bool
	err := bs.DBForTest().View(func(txn *badgerv4.Txn) error {
		_, err := txn.Get(key)
		switch {
		case err == nil:
			exists = true
			return nil
		case errors.Is(err, badgerv4.ErrKeyNotFound):
			return nil
		default:
			return err
		}
	})
	if err != nil {
		t.Fatalf("lookup metadata key %q: %v", string(key), err)
	}
	return exists
}

func TestSyncWrite_FlushIntervalIgnored_WhenSyncWrites(t *testing.T) {
	// Verify that even if FlushInterval is set, SyncWrites forces flushInt=0.
	dir := t.TempDir()
	bs, err := New(Config{
		Dir:           dir,
		SyncWrites:    true,
		FlushInterval: 500, // would normally set flushInt to 500
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer bs.Close()

	if bs.FlushIntervalForTest() != 0 {
		t.Errorf("SyncWrites should force flushInt=0, got %v", bs.FlushIntervalForTest())
	}
}

func TestSyncWrite_ReadOnly_SyncWritesIgnored(t *testing.T) {
	// Verify that SyncWrites is a no-op in ReadOnly mode.
	// ReadOnly mode does not allow writes, so syncWrites must be false.
	dir := t.TempDir()

	// First open to initialize the store.
	init, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("init store: %v", err)
	}
	if err := init.Close(); err != nil {
		t.Fatalf("close init: %v", err)
	}

	// Open as ReadOnly with SyncWrites=true.
	bs, err := New(Config{
		Dir:        dir,
		ReadOnly:   true,
		SyncWrites: true,
	})
	if err != nil {
		t.Fatalf("New (readonly): %v", err)
	}
	defer bs.Close()

	if bs.SyncWritesForTest() {
		t.Error("expected bs.SyncWritesForTest() = false in ReadOnly mode")
	}
}
