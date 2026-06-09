package core

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
)

func TestSyncWrite_Graph_ConfigPassthrough(t *testing.T) {
	// Verify that Config.SyncWrites flows through to the backing badger.Store.
	dir := t.TempDir()
	g, err := New(Config{
		BadgerDir:  dir,
		SyncWrites: true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	bs, ok := g.store.(*badger.Store)
	if !ok {
		t.Fatal("expected store to be *badger.Store")
	}
	if !bs.SyncWritesForTest() {
		t.Error("expected bs.SyncWritesForTest() = true when Config.SyncWrites=true")
	}
	if bs.FlushIntervalForTest() != 0 {
		t.Errorf("expected flushInt=0, got %v", bs.FlushIntervalForTest())
	}
}
