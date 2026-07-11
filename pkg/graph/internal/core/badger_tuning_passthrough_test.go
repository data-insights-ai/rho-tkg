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
		indexCache = 8 << 20
		compactors = 3
	)
	g, err := New(Config{
		BadgerDir:        t.TempDir(),
		ValueLogFileSize: vlog,
		MemTableSize:     memtable,
		BlockCacheSize:   cache,
		IndexCacheSize:   indexCache,
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
	if opts.IndexCacheSize != indexCache {
		t.Errorf("graph.Config IndexCacheSize reached Badger as %d, want %d (core.go passthrough dropped it?)", opts.IndexCacheSize, indexCache)
	}
	if opts.NumCompactors != compactors {
		t.Errorf("graph.Config NumCompactors reached Badger as %d, want %d (core.go passthrough dropped it?)", opts.NumCompactors, compactors)
	}
}

// TestCoreConfigEncryptionKnobsReachBadger covers the same passthrough layer
// for the encryption knobs: graph.Config.EncryptionKey / EncryptionKeyRotation
// must reach the badger.Config core.New constructs, not just the tuning knobs
// above. A dropped field here would silently leave a graph.New(...) store
// unencrypted despite the caller setting EncryptionKey.
func TestCoreConfigEncryptionKnobsReachBadger(t *testing.T) {
	const (
		cache      = 1 << 20
		indexCache = 1 << 20
		rotation   = 3 * 24 * 60 * 60 * 1_000_000_000 // 3 days in time.Duration units (ns)
	)
	key := []byte("0123456789abcdef0123456789abcdef") // 32 bytes -> AES-256
	g, err := New(Config{
		BadgerDir:             t.TempDir(),
		EncryptionKey:         key,
		EncryptionKeyRotation: rotation,
		BlockCacheSize:        cache,
		IndexCacheSize:        indexCache,
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
	if len(opts.EncryptionKey) != len(key) {
		t.Errorf("graph.Config EncryptionKey reached Badger as %d bytes, want %d (core.go passthrough dropped it?)", len(opts.EncryptionKey), len(key))
	}
	if opts.EncryptionKeyRotationDuration != rotation {
		t.Errorf("graph.Config EncryptionKeyRotation reached Badger as %v, want %v (core.go passthrough dropped it?)", opts.EncryptionKeyRotationDuration, rotation)
	}
}

// TestCoreConfigEncryptionValidationSurfacesAtNew confirms an invalid
// encryption config (bad key length, or a key without the required caches)
// is rejected at graph.New(), not silently ignored or left to panic deep
// inside Badger.
func TestCoreConfigEncryptionValidationSurfacesAtNew(t *testing.T) {
	_, err := New(Config{
		BadgerDir:     t.TempDir(),
		EncryptionKey: []byte("too-short"),
	})
	if err == nil {
		t.Fatal("expected graph.New to reject a bad EncryptionKey length, got nil")
	}

	_, err = New(Config{
		BadgerDir:     t.TempDir(),
		EncryptionKey: []byte("0123456789abcdef0123456789abcdef"),
	})
	if err == nil {
		t.Fatal("expected graph.New to reject EncryptionKey without BlockCacheSize/IndexCacheSize, got nil")
	}
}
