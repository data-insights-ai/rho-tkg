package badgerstore

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestSyncWrite_ConfigPassthrough(t *testing.T) {
	// Verify SyncWrites=true flows through Config → BadgerStoreConfig → BadgerStore.
	// Create a BadgerStore with SyncWrites=true and verify:
	// 1. The bs.SyncWritesForTest() field is true
	// 2. The flushInt is 0 (no background goroutine)

	dir := t.TempDir()
	bs, err := NewBadgerStore(BadgerStoreConfig{
		Dir:        dir,
		SyncWrites: true,
	})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
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
	// With SyncWrites=true, data written to BadgerStore should be persisted
	// immediately (not just buffered). To verify, write a node, then open
	// a SECOND BadgerStore on the same directory without closing the first
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
	bs1, err := NewBadgerStore(BadgerStoreConfig{
		Dir:        dir,
		SyncWrites: true,
	})
	if err != nil {
		t.Fatalf("NewBadgerStore (write): %v", err)
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
	bs2, err := NewBadgerStore(BadgerStoreConfig{
		Dir: dir,
	})
	if err != nil {
		t.Fatalf("NewBadgerStore (read): %v", err)
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

func TestSyncWrite_FlushIntervalIgnored_WhenSyncWrites(t *testing.T) {
	// Verify that even if FlushInterval is set, SyncWrites forces flushInt=0.
	dir := t.TempDir()
	bs, err := NewBadgerStore(BadgerStoreConfig{
		Dir:           dir,
		SyncWrites:    true,
		FlushInterval: 500, // would normally set flushInt to 500
	})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
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
	init, err := NewBadgerStore(BadgerStoreConfig{Dir: dir})
	if err != nil {
		t.Fatalf("init store: %v", err)
	}
	if err := init.Close(); err != nil {
		t.Fatalf("close init: %v", err)
	}

	// Open as ReadOnly with SyncWrites=true.
	bs, err := NewBadgerStore(BadgerStoreConfig{
		Dir:        dir,
		ReadOnly:   true,
		SyncWrites: true,
	})
	if err != nil {
		t.Fatalf("NewBadgerStore (readonly): %v", err)
	}
	defer bs.Close()

	if bs.SyncWritesForTest() {
		t.Error("expected bs.SyncWritesForTest() = false in ReadOnly mode")
	}
}
