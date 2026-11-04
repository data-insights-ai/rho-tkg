package core

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	badgerv4 "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
)

// TestNoCompression_Graph_ConfigPassthrough pins that Config.NoCompression
// reaches the store built from BadgerDir: after Close every table in the
// manifest is options.None (badger records the codec per table,
// manifest.go:62-66). Mutation: dropping the passthrough in New writes Snappy.
func TestNoCompression_Graph_ConfigPassthrough(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	g, err := New(Config{BadgerDir: dir, NoCompression: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for range 3 {
		if _, err := g.Nodes.Add(context.Background(), []string{"Test"}, nil); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	fp, err := os.Open(filepath.Join(dir, badgerv4.ManifestFilename))
	if err != nil {
		t.Fatalf("open MANIFEST: %v", err)
	}
	defer fp.Close()
	m, _, err := badgerv4.ReplayManifestFile(fp, 0, badgerv4.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("ReplayManifestFile: %v", err)
	}
	if len(m.Tables) == 0 {
		t.Fatal("manifest holds no tables: Close did not flush the memtable")
	}
	for id, tm := range m.Tables {
		if tm.Compression != options.None {
			t.Errorf("table %d written with codec %v, want options.None", id, tm.Compression)
		}
	}
}

// TestNoCompression_TieredStore_Passthrough pins that tiered.Config.NoCompression
// reaches badgerCfg, the single source of every shard's options, and the
// reference shard's badger.
func TestNoCompression_TieredStore_Passthrough(t *testing.T) {
	t.Parallel()
	ts, err := tiered.New(tiered.Config{
		InMemory:      true,
		RefLabels:     []string{"Ref"},
		NoCompression: true,
	})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	defer ts.Close()

	if !ts.NoCompressionForTest() {
		t.Error("NoCompressionForTest = false, want true")
	}
	if !ts.ShardConfigForTest("hot", false).NoCompression {
		t.Error("badgerCfg dropped NoCompression")
	}
	if got := ts.RefShardForTest().DBForTest().Opts().Compression; got != options.None {
		t.Errorf("reference shard's badger codec = %v, want options.None", got)
	}
}

func TestCompression_Graph_ConfigPassthrough(t *testing.T) {
	t.Parallel()
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
	node, err := g.Nodes.Add(context.Background(), []string{"Test"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	got, err := g.Nodes.Get(context.Background(), node.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got == nil {
		t.Fatal("GetNode returned nil")
	}
}

func TestCompression_TieredStore_Passthrough(t *testing.T) {
	t.Parallel()
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
