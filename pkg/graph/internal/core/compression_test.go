package core

import (
	"testing"

	"github.com/dgraph-io/badger/v4/options"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/tiered"
)

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
	node, err := g.Nodes.Add([]string{"Test"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	got, err := g.Nodes.Get(node.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got == nil {
		t.Fatal("GetNode returned nil")
	}
}

func TestCompression_TieredStore_Passthrough(t *testing.T) {
	ts, err := tiered.New(tiered.Config{
		InMemory:             true,
		RefLabels:            []string{"Ref"},
		Compression:          options.ZSTD,
		ZSTDCompressionLevel: 1,
	})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	defer ts.Close()

	if ts.CompressionForTest() != options.ZSTD {
		t.Errorf("expected compression=ZSTD, got %v", ts.CompressionForTest())
	}
	if ts.ZSTDLevelForTest() != 1 {
		t.Errorf("expected zstdLevel=1, got %d", ts.ZSTDLevelForTest())
	}
}
