package core

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
)

func TestGraphCloseAndReopenBadger(t *testing.T) {
	if testing.Short() && !isRaceEnabled() {
		t.Skip("long tier: release gate")
	}
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
	if testing.Short() && !isRaceEnabled() {
		t.Skip("long tier: release gate")
	}
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

// closeCountingStore counts store.Close calls so a test can see that Graph
// Close reaches the store exactly once.
type closeCountingStore struct {
	storepkg.MandatoryStore
	closes atomic.Int32
}

func (s *closeCountingStore) Close() error {
	s.closes.Add(1)
	return s.MandatoryStore.Close()
}

// Graph Close closes the store once; a second Close is a no-op returning nil.
func TestGraphCloseMemoryStoreCallsStoreClose(t *testing.T) {
	t.Parallel()

	st := &closeCountingStore{MandatoryStore: memory.New()}
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := g.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}
	if n := st.closes.Load(); n != 1 {
		t.Fatalf("store.Close calls after Close = %d, want 1", n)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close 2 (idempotent): %v", err)
	}
	if n := st.closes.Load(); n != 1 {
		t.Fatalf("store.Close calls after a second Close = %d, want still 1", n)
	}
}

func TestGraphNewBadgerDirWhitespace(t *testing.T) {
	t.Parallel()

	for _, dir := range []string{"   ", "\t\n "} {
		_, err := New(Config{BadgerDir: dir})
		if err == nil || !strings.Contains(err.Error(), "whitespace-only") {
			t.Fatalf("New(BadgerDir: %q) = %v, want the whitespace-only error", dir, err)
		}
	}
}

func TestGraphCloseAlwaysReleasesResources(t *testing.T) {
	testtier.Slow(t)
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

	st := &closeCountingStore{MandatoryStore: memory.New()}
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// 10 goroutines calling Close() simultaneously: none may fail, panic or
	// race, and the store is closed exactly once.
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
	if errs != 0 {
		t.Fatalf("concurrent Close returned %d errors, want 0", errs)
	}
	if n := st.closes.Load(); n != 1 {
		t.Fatalf("store.Close calls = %d, want 1", n)
	}
}

func TestGraphBadgerInMemoryDefault(t *testing.T) {
	testtier.Slow(t)
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
