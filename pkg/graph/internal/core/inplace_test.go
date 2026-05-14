package core

import (
	"context"
	"testing"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestUpdateNodeInPlace_NoHistoryEntry(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"Thing"}, map[string]any{"x": int64(1)})
	id := n.ID()

	_, err := g.Nodes.UpdateInPlace(context.Background(), id, map[string]any{"x": int64(2)})
	if err != nil {
		t.Fatalf("UpdateNodeInPlace: %v", err)
	}

	hist, err := g.Nodes.History(id)
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(hist) != 0 {
		t.Errorf("expected no history entry, got %d", len(hist))
	}
}

func TestUpdateNodeInPlace_VersionUnchanged(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"Thing"}, map[string]any{"v": int64(0)})
	id := n.ID()
	origVersion := n.Version()

	updated, err := g.Nodes.UpdateInPlace(context.Background(), id, map[string]any{"v": int64(1)})
	if err != nil {
		t.Fatalf("UpdateNodeInPlace: %v", err)
	}
	if updated.Version() != origVersion {
		t.Errorf("version changed: got %d, want %d", updated.Version(), origVersion)
	}
}

func TestUpdateNodeInPlace_PropertiesUpdated(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"Thing"}, map[string]any{"a": "old"})
	id := n.ID()

	updated, err := g.Nodes.UpdateInPlace(context.Background(), id, map[string]any{"a": "new"})
	if err != nil {
		t.Fatalf("UpdateNodeInPlace: %v", err)
	}

	val, ok := updated.GetProperty("a")
	if !ok || val != "new" {
		t.Errorf("property 'a' = %v %v, want 'new' true", val, ok)
	}
}

func TestUpdateNodeInPlace_NoOp(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"Thing"}, nil)
	id := n.ID()

	// Empty updates — should return current node without any write.
	result, err := g.Nodes.UpdateInPlace(context.Background(), id, map[string]any{})
	if err != nil {
		t.Fatalf("UpdateNodeInPlace empty: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil node")
	}
}

func TestUpdateNodeInPlace_PublishesEvent(t *testing.T) {
	g, _ := New(Config{})
	bus := eventspkg.NewEventBus()
	_ = g.Events.SetSync(bus)

	n, _ := g.Nodes.Add(context.Background(), []string{"Thing"}, nil)
	id := n.ID()

	var got []eventspkg.Event
	bus.Subscribe(func(e eventspkg.Event) {
		got = append(got, e)
	})

	// Clear events from AddNode.
	got = nil

	_, err := g.Nodes.UpdateInPlace(context.Background(), id, map[string]any{"k": "v"})
	if err != nil {
		t.Fatalf("UpdateNodeInPlace: %v", err)
	}

	if len(got) == 0 {
		t.Fatal("expected eventspkg.EventNodeUpdate, got none")
	}
	if got[0].Type != eventspkg.EventNodeUpdate {
		t.Errorf("event type = %v, want eventspkg.EventNodeUpdate", got[0].Type)
	}
}

func TestUpdateNodeInPlace_WithContext_Cancelled(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"Thing"}, nil)
	id := n.ID()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := g.Nodes.UpdateInPlace(ctx, id, map[string]any{"k": "v"})
	if err == nil {
		t.Error("expected error for cancelled context")
	}
}

// --- Relationship mirrors ---

func TestUpdateRelInPlace_NoHistoryEntry(t *testing.T) {
	g, _ := New(Config{})
	a, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", a, b, map[string]any{"x": int64(1)})
	id := r.ID()

	_, err := g.Rels.UpdateInPlace(context.Background(), id, map[string]any{"x": int64(2)})
	if err != nil {
		t.Fatalf("UpdateRelInPlace: %v", err)
	}

	hist, err := g.Rels.History(id)
	if err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	}
	if len(hist) != 0 {
		t.Errorf("expected no history entry, got %d", len(hist))
	}
}

func TestUpdateRelInPlace_VersionUnchanged(t *testing.T) {
	g, _ := New(Config{})
	a, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", a, b, map[string]any{"v": int64(0)})
	id := r.ID()
	origVersion := r.Version()

	updated, err := g.Rels.UpdateInPlace(context.Background(), id, map[string]any{"v": int64(1)})
	if err != nil {
		t.Fatalf("UpdateRelInPlace: %v", err)
	}
	if updated.Version() != origVersion {
		t.Errorf("version changed: got %d, want %d", updated.Version(), origVersion)
	}
}

func TestUpdateRelInPlace_PropertiesUpdated(t *testing.T) {
	g, _ := New(Config{})
	a, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", a, b, map[string]any{"a": "old"})
	id := r.ID()

	updated, err := g.Rels.UpdateInPlace(context.Background(), id, map[string]any{"a": "new"})
	if err != nil {
		t.Fatalf("UpdateRelInPlace: %v", err)
	}

	val, ok := updated.GetProperty("a")
	if !ok || val != "new" {
		t.Errorf("property 'a' = %v %v, want 'new' true", val, ok)
	}
}

func TestUpdateRelInPlace_NoOp(t *testing.T) {
	g, _ := New(Config{})
	a, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", a, b, nil)
	id := r.ID()

	result, err := g.Rels.UpdateInPlace(context.Background(), id, map[string]any{})
	if err != nil {
		t.Fatalf("UpdateRelInPlace empty: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil relationship")
	}
}

func TestUpdateRelInPlace_PublishesEvent(t *testing.T) {
	g, _ := New(Config{})
	bus := eventspkg.NewEventBus()
	_ = g.Events.SetSync(bus)

	a, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", a, b, nil)
	id := r.ID()

	var got []eventspkg.Event
	bus.Subscribe(func(e eventspkg.Event) {
		got = append(got, e)
	})
	got = nil // clear AddNode/AddRel events

	_, err := g.Rels.UpdateInPlace(context.Background(), id, map[string]any{"k": "v"})
	if err != nil {
		t.Fatalf("UpdateRelInPlace: %v", err)
	}

	if len(got) == 0 {
		t.Fatal("expected eventspkg.EventRelUpdate, got none")
	}
	if got[0].Type != eventspkg.EventRelUpdate {
		t.Errorf("event type = %v, want eventspkg.EventRelUpdate", got[0].Type)
	}
}

// Verify that UpdateNodeInPlace shares the opNodeUpdates counter.
func TestUpdateNodeInPlace_CountedAsUpdate(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.Nodes.Add(context.Background(), []string{"Thing"}, nil)
	id := n.ID()

	beforeSnap, _ := g.Stats.Get()

	before := beforeSnap.NodesUpdated
	_, _ = g.Nodes.UpdateInPlace(context.Background(), id, map[string]any{"k": "v"})
	afterSnap, _ := g.Stats.Get()
	after := afterSnap.NodesUpdated
	if after != before+1 {
		t.Errorf("NodesUpdated: got %d, want %d", after, before+1)
	}
}

// Compile-time check: ensure types.Node and types.Relationship are used correctly.
var _ *types.Node = (*types.Node)(nil)
var _ *types.Relationship = (*types.Relationship)(nil)
