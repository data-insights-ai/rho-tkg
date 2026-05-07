package graph

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/dgraph-io/badger/v4/options"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestCompression_BadgerStore_None(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewBadgerStore(BadgerStoreConfig{
		Dir:         dir,
		Compression: options.None,
	})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
	}
	defer bs.Close()

	// Write and read back to verify the store works with no compression.
	n := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	got, err := bs.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got == nil {
		t.Fatal("GetNode returned nil")
	}
}

func TestCompression_BadgerStore_Snappy(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewBadgerStore(BadgerStoreConfig{
		Dir:         dir,
		Compression: options.Snappy,
	})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
	}
	defer bs.Close()

	n := types.NewNode(types.NodeID(snowflake.ID(2)), 1, nil)
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	got, err := bs.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got == nil {
		t.Fatal("GetNode returned nil")
	}
}

func TestCompression_BadgerStore_ZSTD(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewBadgerStore(BadgerStoreConfig{
		Dir:                  dir,
		Compression:          options.ZSTD,
		ZSTDCompressionLevel: 3,
	})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
	}
	defer bs.Close()

	n := types.NewNode(types.NodeID(snowflake.ID(3)), 1, nil)
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	got, err := bs.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got == nil {
		t.Fatal("GetNode returned nil")
	}
}

func TestCompression_BadgerStore_ZeroKeepsDefault(t *testing.T) {
	// Zero Compression value should not call WithCompression (keeps Badger default = Snappy).
	dir := t.TempDir()
	bs, err := NewBadgerStore(BadgerStoreConfig{
		Dir: dir,
	})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
	}
	defer bs.Close()

	n := types.NewNode(types.NodeID(snowflake.ID(4)), 1, nil)
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	got, err := bs.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got == nil {
		t.Fatal("GetNode returned nil")
	}
}

func TestCompression_BadgerStore_InMemory(t *testing.T) {
	// Compression should work with InMemory mode.
	bs, err := NewBadgerStore(BadgerStoreConfig{
		InMemory:    true,
		Compression: options.None,
	})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
	}
	defer bs.Close()

	n := types.NewNode(types.NodeID(snowflake.ID(5)), 1, nil)
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	got, err := bs.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got == nil {
		t.Fatal("GetNode returned nil")
	}
}

func TestCompression_Graph_ConfigPassthrough(t *testing.T) {
	dir := t.TempDir()
	g, err := New(Config{
		BadgerDir:            dir,
		Compression:          options.ZSTD,
		ZSTDCompressionLevel: 3,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// Verify we can write and read through the graph.
	node, err := g.AddNode([]string{"Test"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	got, err := g.GetNode(node.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got == nil {
		t.Fatal("GetNode returned nil")
	}
}

func TestCompression_TieredStore_Passthrough(t *testing.T) {
	ts, err := NewTieredStore(TieredStoreConfig{
		InMemory:             true,
		RefLabels:            []string{"Ref"},
		Compression:          options.ZSTD,
		ZSTDCompressionLevel: 1,
	})
	if err != nil {
		t.Fatalf("NewTieredStore: %v", err)
	}
	defer ts.Close()

	if ts.CompressionForTest() != options.ZSTD {
		t.Errorf("expected compression=ZSTD, got %v", ts.CompressionForTest())
	}
	if ts.ZSTDLevelForTest() != 1 {
		t.Errorf("expected zstdLevel=1, got %d", ts.ZSTDLevelForTest())
	}
}

func TestCompression_ZSTD_DataSurvivesReopen(t *testing.T) {
	dir := t.TempDir()

	// Write with ZSTD compression.
	bs1, err := NewBadgerStore(BadgerStoreConfig{
		Dir:                  dir,
		Compression:          options.ZSTD,
		ZSTDCompressionLevel: 3,
	})
	if err != nil {
		t.Fatalf("NewBadgerStore (write): %v", err)
	}

	n := types.NewNode(types.NodeID(snowflake.ID(6)), 1, nil)
	if err := bs1.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := bs1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify data is readable.
	bs2, err := NewBadgerStore(BadgerStoreConfig{
		Dir:                  dir,
		Compression:          options.ZSTD,
		ZSTDCompressionLevel: 3,
	})
	if err != nil {
		t.Fatalf("NewBadgerStore (read): %v", err)
	}
	defer bs2.Close()

	got, err := bs2.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got == nil {
		t.Fatal("GetNode returned nil")
	}
}
