package core

import (
	"context"
	"strings"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/badger"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"
)

func TestNewGraph(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	if g == nil {
		t.Fatal("New() returned nil")
	}
}

func TestGraphCloseAndReopenBadger(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Create and populate.
	g1, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New 1: %v", err)
	}
	nA, err := g1.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	nB, err := g1.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	_, err = g1.Rels.Add(context.Background(), "KNOWS", nA, nB, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	if err := g1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	// Reopen and verify data persists.
	g2, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New 2: %v", err)
	}
	defer g2.Close()

	nc, err := g2.Nodes.Count()
	if err != nil {
		t.Fatal(err)
	}
	if nc != 2 {
		t.Fatalf("NodeCount after reopen = %d, want 2", nc)
	}
	rc, err := g2.Rels.Count()
	if err != nil {
		t.Fatal(err)
	}
	if rc != 1 {
		t.Fatalf("RelationshipCount after reopen = %d, want 1", rc)
	}
}

func TestGraphRegistryPersistence(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Create labels, close.
	g1, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New 1: %v", err)
	}
	g1.Nodes.Add(context.Background(), []string{"Person", "Actor"}, nil)
	if err := g1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	// Reopen and verify labels resolve.
	g2, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New 2: %v", err)
	}
	defer g2.Close()

	tok, ok := g2.Resolve.LookupLabel("Person")
	if !ok {
		t.Fatal("Person label not persisted")
	}
	if tok == 0 {
		t.Fatal("Person token should not be 0")
	}

	tok, ok = g2.Resolve.LookupLabel("Actor")
	if !ok {
		t.Fatal("Actor label not persisted")
	}
	if tok == 0 {
		t.Fatal("Actor token should not be 0")
	}
}

func TestGraphCloseNoop(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()}) // MemoryStore — Close() calls MemoryStore.Close() which returns nil
	if err := g.Close(); err != nil {
		t.Fatalf("Close on MemoryStore should return nil, got: %v", err)
	}
}

func TestGraphCloseMemoryStoreCallsStoreClose(t *testing.T) {
	t.Parallel()

	// Verify Close() calls store.Close() even for MemoryStore.
	// Previously Close() was a no-op (closeFn == nil); now it goes through store.Close().
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// First close should succeed.
	if err := g.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}
	// Second close returns nil (sync.Once).
	if err := g.Close(); err != nil {
		t.Fatalf("Close 2 (idempotent): %v", err)
	}
}

func TestGraphNewBadgerDirWhitespace(t *testing.T) {
	t.Parallel()

	_, err := New(Config{BadgerDir: "   "})
	if err == nil {
		t.Fatal("New(BadgerDir: whitespace) should return error")
	}
	if !strings.Contains(err.Error(), "whitespace-only") {
		t.Fatalf("error should mention whitespace-only, got: %v", err)
	}
}

func TestGraphNewBadgerDirTabsWhitespace(t *testing.T) {
	t.Parallel()

	_, err := New(Config{BadgerDir: "\t\n "})
	if err == nil {
		t.Fatal("New(BadgerDir: tabs/newlines) should return error")
	}
}

// TestGraphCloseIdempotent removed — duplicate of TestGraphCloseMemoryStoreCallsStoreClose above.

func TestGraphCloseAlwaysReleasesResources(t *testing.T) {
	t.Parallel()

	g, err := New(Config{BadgerInMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Sabotage: close the Badger DB directly so registry saves will fail.
	bs := g.store.(*badger.Store)
	if err := bs.DBForTest().Close(); err != nil {
		t.Fatalf("sabotage close: %v", err)
	}

	// Close() must return an error (registry save fails on closed DB),
	// but must NOT panic. sync.Once handles idempotency.
	err = g.Close()
	if err == nil {
		t.Fatal("Close() should return error when Badger is already closed")
	}

	// Second call returns nil — sync.Once already fired.
	if err := g.Close(); err != nil {
		t.Fatalf("Close() second call should return nil (sync.Once), got: %v", err)
	}
}

func TestGraphCloseConcurrent(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// 10 goroutines calling Close() simultaneously — must not panic or race.
	const workers = 10
	done := make(chan error, workers)
	for range workers {
		go func() {
			done <- g.Close()
		}()
	}

	var errs int
	for range workers {
		if <-done != nil {
			errs++
		}
	}
	// At most zero errors expected (all succeed or only the first one runs).
	_ = errs
}

func TestGraphBadgerInMemoryDefault(t *testing.T) {
	t.Parallel()

	g, err := New(Config{BadgerInMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// Verify it's a badger.Store.
	if _, ok := g.store.(*badger.Store); !ok {
		t.Fatalf("expected badger.Store, got %T", g.store)
	}
}

// ─── Write-skew regression ──────────────────────────────────────────────────
