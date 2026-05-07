package badger

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/dgraph-io/badger/v4/options"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestCompression_BadgerStore_None(t *testing.T) {
	dir := t.TempDir()
	bs, err := New(Config{
		Dir:         dir,
		Compression: options.None,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
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
	bs, err := New(Config{
		Dir:         dir,
		Compression: options.Snappy,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
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
	bs, err := New(Config{
		Dir:                  dir,
		Compression:          options.ZSTD,
		ZSTDCompressionLevel: 3,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
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
	bs, err := New(Config{
		Dir: dir,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
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
	bs, err := New(Config{
		InMemory:    true,
		Compression: options.None,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
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

func TestCompression_ZSTD_DataSurvivesReopen(t *testing.T) {
	dir := t.TempDir()

	// Write with ZSTD compression.
	bs1, err := New(Config{
		Dir:                  dir,
		Compression:          options.ZSTD,
		ZSTDCompressionLevel: 3,
	})
	if err != nil {
		t.Fatalf("New (write): %v", err)
	}

	n := types.NewNode(types.NodeID(snowflake.ID(6)), 1, nil)
	if err := bs1.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := bs1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify data is readable.
	bs2, err := New(Config{
		Dir:                  dir,
		Compression:          options.ZSTD,
		ZSTDCompressionLevel: 3,
	})
	if err != nil {
		t.Fatalf("New (read): %v", err)
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
