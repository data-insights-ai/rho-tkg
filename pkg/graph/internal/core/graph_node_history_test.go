package core

import (
	"context"
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

type staleFirstNodeSnapshotStore struct {
	storepkg.MandatoryStore
	target types.NodeID
	stale  *types.Node
	gets   int
}

func (s *staleFirstNodeSnapshotStore) GetNode(id types.NodeID) (*types.Node, error) {
	n, err := s.MandatoryStore.GetNode(id)
	if err != nil {
		return nil, err
	}
	if id == s.target {
		s.gets++
		if s.gets == 1 && s.stale != nil {
			return s.stale.DeepCopy(), nil
		}
	}
	return n, nil
}

func TestGraphBadgerUpdateNodePersistence(t *testing.T) {
	if testing.Short() && !isRaceEnabled() {
		t.Skip("long tier: release gate")
	}
	t.Parallel()
	dir := t.TempDir()

	// Create, update, close.
	g1, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New 1: %v", err)
	}
	n, _ := g1.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	id := n.ID()

	_, err = g1.Nodes.Update(context.Background(), id, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}
	if err := g1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	// Reopen and verify updated value persisted.
	g2, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New 2: %v", err)
	}
	defer g2.Close()

	got, err := g2.Nodes.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("GetNode after reopen: %v", err)
	}
	v, ok := got.GetProperty("name")
	if !ok || v != "Bob" {
		t.Fatalf("persisted name = %v, want Bob", v)
	}
	if got.Version() != 1 {
		t.Fatalf("persisted version = %d, want 1", got.Version())
	}
}

func TestGraphUpdateNodeSavesHistory(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	defer g.Close()

	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	id := n.ID()

	// Update: should save version 0 (pre-mutation) to history.
	_, err := g.Nodes.Update(context.Background(), id, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	history, err := g.Nodes.History(id)
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(history) != 1 {
		t.Fatalf("len(history) = %d, want 1", len(history))
	}
	if history[0].Version() != 0 {
		t.Errorf("history[0].Version() = %d, want 0", history[0].Version())
	}
	hv, ok := history[0].GetProperty("name")
	if !ok || hv != "Alice" {
		t.Fatalf("history[0] name = %v, want Alice", hv)
	}
}

func TestGraphDeleteNodeReReadsNodeForTombstoneHistory(t *testing.T) {
	t.Parallel()
	store := &staleFirstNodeSnapshotStore{MandatoryStore: memory.New()}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	n, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "stale"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	stale := n.DeepCopy()
	updated, err := g.Nodes.Update(context.Background(), n.ID(), map[string]any{"name": "fresh"})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	store.target = n.ID()
	store.stale = stale
	if err := g.Nodes.Delete(context.Background(), n.ID()); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	tombstone, err := g.store.GetNodeVersion(n.ID(), updated.Version())
	if err != nil {
		t.Fatalf("GetNodeVersion(%d): %v", updated.Version(), err)
	}
	got, ok := tombstone.GetProperty("name")
	if !ok || got != "fresh" {
		t.Fatalf("delete tombstone name = %v, want fresh", got)
	}
	if tombstone.Temporal() == nil || tombstone.Temporal().DeletedAt == 0 {
		t.Fatal("delete tombstone missing DeletedAt")
	}
}

func TestGraphUpdateNodeHistoryAscendingOrder(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	defer g.Close()

	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "v0"})
	id := n.ID()

	for i := 1; i <= 5; i++ {
		if _, err := g.Nodes.Update(context.Background(), id, map[string]any{"name": fmt.Sprintf("v%d", i)}); err != nil {
			t.Fatalf("UpdateNode %d: %v", i, err)
		}
	}

	// Five updates leave the five superseded versions v0..v4, oldest first.
	history, err := g.Nodes.History(id)
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(history) != 5 {
		t.Fatalf("len(history) = %d, want 5", len(history))
	}
	for i, h := range history {
		if h.Version() != uint32(i) {
			t.Fatalf("history[%d].Version() = %d, want %d (ascending)", i, h.Version(), i)
		}
		if name, _ := h.GetProperty("name"); name != fmt.Sprintf("v%d", i) {
			t.Fatalf("history[%d] name = %v, want v%d", i, name, i)
		}
	}
}

func TestGraphDeleteNodePreservesHistory(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	defer g.Close()

	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "v0"})
	id := n.ID()

	g.Nodes.Update(context.Background(), id, map[string]any{"name": "v1"})
	g.Nodes.Update(context.Background(), id, map[string]any{"name": "v2"})

	if err := g.Nodes.Delete(context.Background(), id); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	// History is preserved — v0 pre-mutation, v1 pre-mutation, + tombstone at v2.
	history, err := g.Nodes.History(id)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(history) != 3 {
		t.Fatalf("len(history) = %d, want 3 (v0, v1, tombstone v2)", len(history))
	}
	for i, h := range history {
		if h.Version() != uint32(i) {
			t.Fatalf("history[%d].Version() = %d, want %d", i, h.Version(), i)
		}
	}
	if tm := history[2].Temporal(); tm == nil || tm.DeletedAt == 0 {
		t.Fatal("history[2] is not the delete tombstone (DeletedAt unset)")
	}
}

// ─── Version history — Badger persistence ───────────────────────────────────

func TestGraphBadgerDeleteNodePreservesHistory(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	dir := t.TempDir()

	g1, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New 1: %v", err)
	}
	n, _ := g1.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "v0"})
	id := n.ID()

	g1.Nodes.Update(context.Background(), id, map[string]any{"name": "v1"})

	if err := g1.Nodes.Delete(context.Background(), id); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	if err := g1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	g2, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New 2: %v", err)
	}
	defer g2.Close()

	// History is preserved after delete (v0 pre-mutation + tombstone v1).
	history, err := g2.Nodes.History(id)
	if err != nil {
		t.Fatalf("History after reopen: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("len(history) after reopen = %d, want 2 (v0, tombstone v1)", len(history))
	}
	if tm := history[1].Temporal(); tm == nil || tm.DeletedAt == 0 {
		t.Fatal("history[1] after reopen is not the delete tombstone (DeletedAt unset)")
	}
}
