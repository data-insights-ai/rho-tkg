package core

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/integrity"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

type verifyHistoryPageTrackingStore struct {
	storepkg.MandatoryStore
	historyPager        storepkg.HistoryVersionPageCapability
	nodeHistoryPageHits atomic.Int64
	relHistoryPageHits  atomic.Int64
}

func (s *verifyHistoryPageTrackingStore) NodeHistoryVersionsFrom(id types.NodeID, startVersion uint32, limit int) ([]*types.Node, error) {
	s.nodeHistoryPageHits.Add(1)
	return s.historyPager.NodeHistoryVersionsFrom(id, startVersion, limit)
}

func (s *verifyHistoryPageTrackingStore) RelHistoryVersionsFrom(id types.RelID, startVersion uint32, limit int) ([]*types.Relationship, error) {
	s.relHistoryPageHits.Add(1)
	return s.historyPager.RelHistoryVersionsFrom(id, startVersion, limit)
}

type verifyNilHistoryPager struct{}

func (verifyNilHistoryPager) NodeHistoryVersionsFrom(id types.NodeID, startVersion uint32, limit int) ([]*types.Node, error) {
	return []*types.Node{nil}, nil
}

func (verifyNilHistoryPager) RelHistoryVersionsFrom(id types.RelID, startVersion uint32, limit int) ([]*types.Relationship, error) {
	return []*types.Relationship{nil}, nil
}

var errHashInvalidIDProbeStoreTouched = errors.New("hash invalid-id probe touched store")

type hashInvalidIDProbeStore struct {
	storepkg.MandatoryStore
	nodeReads atomic.Int64
	relReads  atomic.Int64
}

func (s *hashInvalidIDProbeStore) GetNode(types.NodeID) (*types.Node, error) {
	s.nodeReads.Add(1)
	return nil, errHashInvalidIDProbeStoreTouched
}

func (s *hashInvalidIDProbeStore) GetRelationship(types.RelID) (*types.Relationship, error) {
	s.relReads.Add(1)
	return nil, errHashInvalidIDProbeStoreTouched
}

// --- ComputeNodeHash unit tests ---

func TestComputeNodeHashDeterministic(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	personTok, _ := g.Resolve.GetOrCreateLabel("Person")

	n := types.NewNode(types.NodeID(snowflake.ID(100)), personTok, nil)
	_ = n.SetProperty("name", "Alice")
	n.SetVersion(1)

	h1 := integrity.ComputeNodeHash(n, []string{"Person"})
	h2 := integrity.ComputeNodeHash(n, []string{"Person"})

	if h1 != h2 {
		t.Fatalf("same input produced different hashes: %q vs %q", h1, h2)
	}
	if len(h1) != 64 {
		t.Fatalf("hash length = %d, want 64 hex chars", len(h1))
	}
}

func TestComputeNodeHashChangesWithProperties(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	personTok, _ := g.Resolve.GetOrCreateLabel("Person")

	n1 := types.NewNode(types.NodeID(snowflake.ID(100)), personTok, nil)
	_ = n1.SetProperty("name", "Alice")

	n2 := types.NewNode(types.NodeID(snowflake.ID(100)), personTok, nil)
	_ = n2.SetProperty("name", "Bob")

	h1 := integrity.ComputeNodeHash(n1, []string{"Person"})
	h2 := integrity.ComputeNodeHash(n2, []string{"Person"})

	if h1 == h2 {
		t.Fatal("different properties produced same hash")
	}
}

func TestComputeNodeHashChangesWithVersion(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	personTok, _ := g.Resolve.GetOrCreateLabel("Person")

	n1 := types.NewNode(types.NodeID(snowflake.ID(100)), personTok, nil)
	n1.SetVersion(0)

	n2 := types.NewNode(types.NodeID(snowflake.ID(100)), personTok, nil)
	n2.SetVersion(1)

	h1 := integrity.ComputeNodeHash(n1, []string{"Person"})
	h2 := integrity.ComputeNodeHash(n2, []string{"Person"})

	if h1 == h2 {
		t.Fatal("different versions produced same hash")
	}
}

func TestComputeNodeHashChangesWithLabels(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	personTok, _ := g.Resolve.GetOrCreateLabel("Person")

	n := types.NewNode(types.NodeID(snowflake.ID(100)), personTok, nil)

	h1 := integrity.ComputeNodeHash(n, []string{"Person"})
	h2 := integrity.ComputeNodeHash(n, []string{"Employee"})

	if h1 == h2 {
		t.Fatal("different labels produced same hash")
	}
}

func TestComputeNodeHashLabelOrderIndependent(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	personTok, _ := g.Resolve.GetOrCreateLabel("Person")
	actorTok, _ := g.Resolve.GetOrCreateLabel("Actor")

	n := types.NewNode(types.NodeID(snowflake.ID(100)), personTok, []uint16{actorTok})

	h1 := integrity.ComputeNodeHash(n, []string{"Person", "Actor"})
	h2 := integrity.ComputeNodeHash(n, []string{"Actor", "Person"})

	if h1 != h2 {
		t.Fatalf("label order affected hash: %q vs %q", h1, h2)
	}
}

// --- ComputeRelHash unit tests ---

func TestComputeRelHashDeterministic(t *testing.T) {
	t.Parallel()

	r := types.NewRelationship(types.RelID(snowflake.ID(200)), 1, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	_ = r.SetProperty("weight", int64(5))
	r.SetVersion(1)

	h1 := integrity.ComputeRelHash(r, "KNOWS")
	h2 := integrity.ComputeRelHash(r, "KNOWS")

	if h1 != h2 {
		t.Fatalf("same input produced different hashes: %q vs %q", h1, h2)
	}
	if len(h1) != 64 {
		t.Fatalf("hash length = %d, want 64 hex chars", len(h1))
	}
}

func TestComputeRelHashChangesWithProperties(t *testing.T) {
	t.Parallel()

	r1 := types.NewRelationship(types.RelID(snowflake.ID(200)), 1, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	_ = r1.SetProperty("weight", int64(5))

	r2 := types.NewRelationship(types.RelID(snowflake.ID(200)), 1, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	_ = r2.SetProperty("weight", int64(10))

	h1 := integrity.ComputeRelHash(r1, "KNOWS")
	h2 := integrity.ComputeRelHash(r2, "KNOWS")

	if h1 == h2 {
		t.Fatal("different properties produced same hash")
	}
}

func TestComputeRelHashChangesWithVersion(t *testing.T) {
	t.Parallel()

	r1 := types.NewRelationship(types.RelID(snowflake.ID(200)), 1, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	r1.SetVersion(0)

	r2 := types.NewRelationship(types.RelID(snowflake.ID(200)), 1, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	r2.SetVersion(1)

	h1 := integrity.ComputeRelHash(r1, "KNOWS")
	h2 := integrity.ComputeRelHash(r2, "KNOWS")

	if h1 == h2 {
		t.Fatal("different versions produced same hash")
	}
}

func TestComputeRelHashChangesWithType(t *testing.T) {
	t.Parallel()

	r := types.NewRelationship(types.RelID(snowflake.ID(200)), 1, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))

	h1 := integrity.ComputeRelHash(r, "KNOWS")
	h2 := integrity.ComputeRelHash(r, "LIKES")

	if h1 == h2 {
		t.Fatal("different type names produced same hash")
	}
}

func TestComputeRelHashChangesWithEndpoints(t *testing.T) {
	t.Parallel()

	r1 := types.NewRelationship(types.RelID(snowflake.ID(200)), 1, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	r2 := types.NewRelationship(types.RelID(snowflake.ID(200)), 1, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(30)))

	h1 := integrity.ComputeRelHash(r1, "KNOWS")
	h2 := integrity.ComputeRelHash(r2, "KNOWS")

	if h1 == h2 {
		t.Fatal("different endpoints produced same hash")
	}
}

// --- Hash determinism and type distinction tests (Issue 3) ---

func TestHashMapPropertyDeterministic(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	personTok, _ := g.Resolve.GetOrCreateLabel("Person")

	n := types.NewNode(types.NodeID(snowflake.ID(100)), personTok, nil)
	_ = n.SetProperty("meta", map[string]any{
		"z": "last",
		"a": "first",
		"m": int64(42),
	})

	// Compute 1000 times — map iteration order is randomized by Go,
	// so a non-deterministic implementation would fail.
	first := integrity.ComputeNodeHash(n, []string{"Person"})
	for i := 0; i < 1000; i++ {
		h := integrity.ComputeNodeHash(n, []string{"Person"})
		if h != first {
			t.Fatalf("iteration %d: hash differs (non-deterministic map hashing)", i)
		}
	}
}

func TestHashTypeDistinction(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	personTok, _ := g.Resolve.GetOrCreateLabel("Person")

	// int(1) vs string("1") must produce different hashes.
	n1 := types.NewNode(types.NodeID(snowflake.ID(100)), personTok, nil)
	_ = n1.SetProperty("val", int(1))

	n2 := types.NewNode(types.NodeID(snowflake.ID(100)), personTok, nil)
	_ = n2.SetProperty("val", "1")

	h1 := integrity.ComputeNodeHash(n1, []string{"Person"})
	h2 := integrity.ComputeNodeHash(n2, []string{"Person"})

	if h1 == h2 {
		t.Fatal("int(1) and string(\"1\") produced same hash — type distinction failed")
	}
}

func TestHashNestedMapDeterministic(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	personTok, _ := g.Resolve.GetOrCreateLabel("Person")

	n := types.NewNode(types.NodeID(snowflake.ID(100)), personTok, nil)
	_ = n.SetProperty("nested", []any{
		map[string]any{"b": int64(2), "a": int64(1)},
		"hello",
		int64(42),
	})

	first := integrity.ComputeNodeHash(n, []string{"Person"})
	for i := 0; i < 1000; i++ {
		h := integrity.ComputeNodeHash(n, []string{"Person"})
		if h != first {
			t.Fatalf("iteration %d: nested map hash differs", i)
		}
	}
}

// --- VerifyNodeChain tests ---

func TestVerifyNodeChain_GenesisOnly(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})

	valid, err := g.Hash.VerifyNodeChain(n.ID())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !valid {
		t.Fatal("genesis-only chain should be valid")
	}
}

func TestVerifyNodeChain_MultipleUpdates(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	id := n.ID()

	g.Nodes.Update(context.Background(), id, map[string]any{"name": "Bob"})
	g.Nodes.Update(context.Background(), id, map[string]any{"name": "Charlie"})
	g.Nodes.Update(context.Background(), id, map[string]any{"age": int64(30)})

	valid, err := g.Hash.VerifyNodeChain(id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !valid {
		t.Fatal("multi-update chain should be valid")
	}
}

func TestVerifyNodeChain_TamperedHash(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	id := n.ID()

	// Tamper with the stored hash.
	current, _ := g.store.GetNode(id)
	current.SetIntegrity(&types.NodeIntegrity{Hash: "tampered", PrevHash: ""})
	_ = g.store.ReplaceNode(current)

	valid, err := g.Hash.VerifyNodeChain(id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if valid {
		t.Fatal("tampered hash should be detected as invalid")
	}
}

func TestVerifyNodeChain_BrokenPrevHash(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	id := n.ID()

	g.Nodes.Update(context.Background(), id, map[string]any{"name": "Bob"})

	// Break the PrevHash link on the current version.
	current, _ := g.store.GetNode(id)
	ig := current.Integrity()
	current.SetIntegrity(&types.NodeIntegrity{Hash: ig.Hash, PrevHash: "broken"})
	_ = g.store.ReplaceNode(current)

	valid, err := g.Hash.VerifyNodeChain(id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if valid {
		t.Fatal("broken PrevHash should be detected as invalid")
	}
}

func TestVerifyNodeChain_NonExistent(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})

	_, err := g.Hash.VerifyNodeChain(types.NodeID(999))
	if !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("expected storepkg.ErrNodeNotFound, got %v", err)
	}
}

func TestVerifyChain_InvalidIDsRejectedBeforeStoreRead(t *testing.T) {
	t.Parallel()

	store := &hashInvalidIDProbeStore{MandatoryStore: memory.New()}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	for _, id := range []types.NodeID{0, types.NodeID(-1)} {
		valid, err := g.Hash.VerifyNodeChain(id)
		if valid || !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
			t.Fatalf("VerifyNodeChain(%d) = (%v, %v), want (false, ErrInvalidStoreMutation)", id, valid, err)
		}
		valid, err = g.verifyNodeChainLocked(id)
		if valid || !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
			t.Fatalf("verifyNodeChainLocked(%d) = (%v, %v), want (false, ErrInvalidStoreMutation)", id, valid, err)
		}
	}
	for _, id := range []types.RelID{0, types.RelID(-1)} {
		valid, err := g.Hash.VerifyRelChain(id)
		if valid || !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
			t.Fatalf("VerifyRelChain(%d) = (%v, %v), want (false, ErrInvalidStoreMutation)", id, valid, err)
		}
		valid, err = g.verifyRelChainLocked(id)
		if valid || !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
			t.Fatalf("verifyRelChainLocked(%d) = (%v, %v), want (false, ErrInvalidStoreMutation)", id, valid, err)
		}
	}
	if got := store.nodeReads.Load(); got != 0 {
		t.Fatalf("invalid node hash verification touched store %d time(s)", got)
	}
	if got := store.relReads.Load(); got != 0 {
		t.Fatalf("invalid relationship hash verification touched store %d time(s)", got)
	}
}

func TestVerifyNodeChain_NilIntegrity(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	id := n.ID()

	// Clear integrity metadata.
	current, _ := g.store.GetNode(id)
	current.SetIntegrity(nil)
	_ = g.store.ReplaceNode(current)

	valid, err := g.Hash.VerifyNodeChain(id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if valid {
		t.Fatal("nil integrity should be detected as invalid")
	}
}

func TestVerifyNodeChain_PropertyChange(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	id := n.ID()

	g.Nodes.Update(context.Background(), id, map[string]any{"name": "Bob"})
	g.Nodes.Update(context.Background(), id, map[string]any{"name": nil, "age": int64(25)})

	valid, err := g.Hash.VerifyNodeChain(id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !valid {
		t.Fatal("property-mutation chain should be valid")
	}
}

// --- VerifyRelChain tests ---

func TestVerifyRelChain_GenesisOnly(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	a, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", a, b, map[string]any{"since": int64(2020)})

	valid, err := g.Hash.VerifyRelChain(r.ID())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !valid {
		t.Fatal("genesis-only rel chain should be valid")
	}
}

func TestVerifyRelChain_MultipleUpdates(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	a, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", a, b, map[string]any{"weight": int64(1)})
	id := r.ID()

	g.Rels.Update(context.Background(), id, map[string]any{"weight": int64(2)})
	g.Rels.Update(context.Background(), id, map[string]any{"weight": int64(3)})
	g.Rels.Update(context.Background(), id, map[string]any{"note": "old friends"})

	valid, err := g.Hash.VerifyRelChain(id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !valid {
		t.Fatal("multi-update rel chain should be valid")
	}
}

func TestVerifyRelChain_TamperedHash(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	a, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", a, b, nil)
	id := r.ID()

	current, _ := g.store.GetRelationship(id)
	current.SetIntegrity(&types.RelIntegrity{Hash: "tampered", PrevHash: ""})
	_ = g.store.ReplaceRelationship(current)

	valid, err := g.Hash.VerifyRelChain(id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if valid {
		t.Fatal("tampered rel hash should be detected as invalid")
	}
}

func TestVerifyRelChain_BrokenPrevHash(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	a, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", a, b, nil)
	id := r.ID()

	g.Rels.Update(context.Background(), id, map[string]any{"weight": int64(5)})

	current, _ := g.store.GetRelationship(id)
	ig := current.Integrity()
	current.SetIntegrity(&types.RelIntegrity{Hash: ig.Hash, PrevHash: "broken"})
	_ = g.store.ReplaceRelationship(current)

	valid, err := g.Hash.VerifyRelChain(id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if valid {
		t.Fatal("broken rel PrevHash should be detected as invalid")
	}
}

func TestVerifyRelChain_NonExistent(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})

	_, err := g.Hash.VerifyRelChain(types.RelID(999))
	if !errors.Is(err, storepkg.ErrRelNotFound) {
		t.Fatalf("expected storepkg.ErrRelNotFound, got %v", err)
	}
}

func TestVerifyRelChain_NilIntegrity(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	a, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", a, b, nil)
	id := r.ID()

	current, _ := g.store.GetRelationship(id)
	current.SetIntegrity(nil)
	_ = g.store.ReplaceRelationship(current)

	valid, err := g.Hash.VerifyRelChain(id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if valid {
		t.Fatal("nil rel integrity should be detected as invalid")
	}
}

func TestVerifyRelChain_PropertyChange(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	a, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", a, b, map[string]any{"weight": int64(1)})
	id := r.ID()

	g.Rels.Update(context.Background(), id, map[string]any{"weight": int64(10)})
	g.Rels.Update(context.Background(), id, map[string]any{"weight": nil, "note": "test"})

	valid, err := g.Hash.VerifyRelChain(id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !valid {
		t.Fatal("property-mutation rel chain should be valid")
	}
}

func TestVerifyChain_HistoryPagesVersionsWhenCapabilityAvailable(t *testing.T) {
	t.Parallel()

	base := memory.New()
	store := &verifyHistoryPageTrackingStore{MandatoryStore: base, historyPager: base}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	start, err := g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{"name": "a"})
	if err != nil {
		t.Fatalf("Add start: %v", err)
	}
	end, err := g.Nodes.Add(context.Background(), []string{"Doc"}, nil)
	if err != nil {
		t.Fatalf("Add end: %v", err)
	}
	rel, err := g.Rels.Add(context.Background(), "LINKS", start, end, map[string]any{"weight": int64(1)})
	if err != nil {
		t.Fatalf("Add relationship: %v", err)
	}
	if _, err := g.Nodes.Update(context.Background(), start.ID(), map[string]any{"name": "b"}); err != nil {
		t.Fatalf("Update node: %v", err)
	}
	if _, err := g.Rels.Update(context.Background(), rel.ID(), map[string]any{"weight": int64(2)}); err != nil {
		t.Fatalf("Update relationship: %v", err)
	}

	if valid, err := g.Hash.VerifyNodeChain(start.ID()); err != nil || !valid {
		t.Fatalf("VerifyNodeChain = (%v, %v), want (true, nil)", valid, err)
	}
	if valid, err := g.Hash.VerifyRelChain(rel.ID()); err != nil || !valid {
		t.Fatalf("VerifyRelChain = (%v, %v), want (true, nil)", valid, err)
	}

	if got := store.nodeHistoryPageHits.Load(); got == 0 {
		t.Fatal("VerifyNodeChain did not call NodeHistoryVersionsFrom")
	}
	if got := store.relHistoryPageHits.Load(); got == 0 {
		t.Fatal("VerifyRelChain did not call RelHistoryVersionsFrom")
	}
}

func TestVerifyChain_IgnoresInheritedNativeHistoryPagerOnWrapper(t *testing.T) {
	t.Parallel()

	store := &exportEmbeddedNativeHistoryWrapper{Store: memory.New()}
	if _, ok := any(store).(storepkg.HistoryVersionPageCapability); !ok {
		t.Fatal("test wrapper no longer inherits HistoryVersionPageCapability")
	}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	start, err := g.Nodes.Add(context.Background(), []string{"Doc"}, map[string]any{"name": "a"})
	if err != nil {
		t.Fatalf("Add start: %v", err)
	}
	end, err := g.Nodes.Add(context.Background(), []string{"Doc"}, nil)
	if err != nil {
		t.Fatalf("Add end: %v", err)
	}
	rel, err := g.Rels.Add(context.Background(), "LINKS", start, end, map[string]any{"weight": int64(1)})
	if err != nil {
		t.Fatalf("Add relationship: %v", err)
	}
	if _, err := g.Nodes.Update(context.Background(), start.ID(), map[string]any{"name": "b"}); err != nil {
		t.Fatalf("Update node: %v", err)
	}
	if _, err := g.Rels.Update(context.Background(), rel.ID(), map[string]any{"weight": int64(2)}); err != nil {
		t.Fatalf("Update relationship: %v", err)
	}

	if valid, err := g.Hash.VerifyNodeChain(start.ID()); err != nil || !valid {
		t.Fatalf("VerifyNodeChain = (%v, %v), want (true, nil)", valid, err)
	}
	if valid, err := g.Hash.VerifyRelChain(rel.ID()); err != nil || !valid {
		t.Fatalf("VerifyRelChain = (%v, %v), want (true, nil)", valid, err)
	}

	if got := store.getNodeHistoryCalls.Load(); got == 0 {
		t.Fatal("VerifyNodeChain used inherited native NodeHistoryVersionsFrom instead of wrapper GetNodeHistory")
	}
	if got := store.getRelHistoryCalls.Load(); got == 0 {
		t.Fatal("VerifyRelChain used inherited native RelHistoryVersionsFrom instead of wrapper GetRelHistory")
	}
}

func TestVerifyChainRejectsNilHistoryRows(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	if valid, err := g.verifyNodeChainRowsLocked(nil, []*types.Node{nil}, nil); !errors.Is(err, ErrNilNode) || valid {
		t.Fatalf("verifyNodeChainRowsLocked nil history = (%v, %v), want (false, ErrNilNode)", valid, err)
	}
	if valid, err := g.verifyRelChainRowsLocked(nil, []*types.Relationship{nil}, nil); !errors.Is(err, ErrNilRelationship) || valid {
		t.Fatalf("verifyRelChainRowsLocked nil history = (%v, %v), want (false, ErrNilRelationship)", valid, err)
	}

	pager := verifyNilHistoryPager{}
	if valid, err := g.verifyNodeChainPagedLocked(types.NodeID(1), nil, pager, nil); !errors.Is(err, ErrNilNode) || valid {
		t.Fatalf("verifyNodeChainPagedLocked nil history = (%v, %v), want (false, ErrNilNode)", valid, err)
	}
	if valid, err := g.verifyRelChainPagedLocked(types.RelID(2), nil, pager, nil); !errors.Is(err, ErrNilRelationship) || valid {
		t.Fatalf("verifyRelChainPagedLocked nil history = (%v, %v), want (false, ErrNilRelationship)", valid, err)
	}
}

// --- Truncation resilience tests ---

func TestVerifyNodeChain_AfterTruncation(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	id := n.ID()

	// Create 3 updates → versions 0, 1, 2, 3 (current is v3).
	g.Nodes.Update(context.Background(), id, map[string]any{"name": "Bob"})
	g.Nodes.Update(context.Background(), id, map[string]any{"name": "Charlie"})
	g.Nodes.Update(context.Background(), id, map[string]any{"name": "Diana"})

	// Verify before truncation.
	valid, err := g.Hash.VerifyNodeChain(id)
	if err != nil {
		t.Fatalf("pre-truncate: unexpected error: %v", err)
	}
	if !valid {
		t.Fatal("pre-truncate: chain should be valid")
	}

	// Truncate to keep only 1 history version.
	if err := g.store.TruncateNodeHistory(id, 1); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	// Verify after truncation — chain[0] is no longer genesis.
	valid, err = g.Hash.VerifyNodeChain(id)
	if err != nil {
		t.Fatalf("post-truncate: unexpected error: %v", err)
	}
	if !valid {
		t.Fatal("post-truncate: truncated chain should still verify (content integrity)")
	}
}

func TestVerifyRelChain_AfterTruncation(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	a, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", a, b, map[string]any{"weight": int64(1)})
	id := r.ID()

	// Create 3 updates → versions 0, 1, 2, 3 (current is v3).
	g.Rels.Update(context.Background(), id, map[string]any{"weight": int64(2)})
	g.Rels.Update(context.Background(), id, map[string]any{"weight": int64(3)})
	g.Rels.Update(context.Background(), id, map[string]any{"weight": int64(4)})

	// Verify before truncation.
	valid, err := g.Hash.VerifyRelChain(id)
	if err != nil {
		t.Fatalf("pre-truncate: unexpected error: %v", err)
	}
	if !valid {
		t.Fatal("pre-truncate: rel chain should be valid")
	}

	// Truncate to keep only 1 history version.
	if err := g.store.TruncateRelHistory(id, 1); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	// Verify after truncation — chain[0] is no longer genesis.
	valid, err = g.Hash.VerifyRelChain(id)
	if err != nil {
		t.Fatalf("post-truncate: unexpected error: %v", err)
	}
	if !valid {
		t.Fatal("post-truncate: truncated rel chain should still verify (content integrity)")
	}
}
