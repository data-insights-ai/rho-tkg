package core

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
)

// TestCoreConfigFootprintKnobsReachBadger covers the top-level passthrough layer
// that has no other test: graph.Config (== core.Config) -> the badger.Config that
// core.New constructs from BadgerDir/BadgerInMemory. A dropped field in that
// struct literal (core.go) would silently leave a graph.New(...) store at
// Badger's stock footprint. Verified against the live DB options, so it cannot
// pass with an un-applied knob.
func TestCoreConfigFootprintKnobsReachBadger(t *testing.T) {
	const (
		vlog       = 64 << 20
		memtable   = 16 << 20
		cache      = 32 << 20
		compactors = 3
	)
	g, err := New(Config{
		BadgerDir:        t.TempDir(),
		ValueLogFileSize: vlog,
		MemTableSize:     memtable,
		BlockCacheSize:   cache,
		NumCompactors:    compactors,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	bs, ok := g.store.(*badger.Store)
	if !ok {
		t.Fatalf("expected *badger.Store from BadgerDir config, got %T", g.store)
	}
	opts := bs.DBForTest().Opts()
	if opts.ValueLogFileSize != vlog {
		t.Errorf("graph.Config ValueLogFileSize reached Badger as %d, want %d (core.go passthrough dropped it?)", opts.ValueLogFileSize, vlog)
	}
	if opts.MemTableSize != memtable {
		t.Errorf("graph.Config MemTableSize reached Badger as %d, want %d (core.go passthrough dropped it?)", opts.MemTableSize, memtable)
	}
	if opts.BlockCacheSize != cache {
		t.Errorf("graph.Config BlockCacheSize reached Badger as %d, want %d (core.go passthrough dropped it?)", opts.BlockCacheSize, cache)
	}
	if opts.NumCompactors != compactors {
		t.Errorf("graph.Config NumCompactors reached Badger as %d, want %d (core.go passthrough dropped it?)", opts.NumCompactors, compactors)
	}
}
