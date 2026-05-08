package core

import (
	"errors"
	"testing"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"
)

func TestRemoveNodeLabel_ExtraLabel(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.Nodes.Add([]string{"Person", "Employee"}, nil)
	id := n.ID()

	if err := g.Nodes.RemoveLabel(id, "Employee"); err != nil {
		t.Fatalf("RemoveNodeLabel: %v", err)
	}

	updated, _ := g.Nodes.Get(id)
	if g.Nodes.HasLabel(updated, "Employee") {
		t.Error("label 'Employee' still present after removal")
	}
	if !g.Nodes.HasLabel(updated, "Person") {
		t.Error("primary label 'Person' should remain")
	}
}

func TestRemoveNodeLabel_PrimaryPromotesExtra(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.Nodes.Add([]string{"Primary", "Secondary"}, nil)
	id := n.ID()

	if err := g.Nodes.RemoveLabel(id, "Primary"); err != nil {
		t.Fatalf("RemoveNodeLabel primary: %v", err)
	}

	updated, _ := g.Nodes.Get(id)
	// After removing primary, "Secondary" should be promoted.
	labels := g.Nodes.Labels(updated)
	if len(labels) != 1 || labels[0] != "Secondary" {
		t.Errorf("labels after primary removal = %v, want [Secondary]", labels)
	}
}

func TestRemoveNodeLabel_LastLabelError(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.Nodes.Add([]string{"Solo"}, nil)
	id := n.ID()

	err := g.Nodes.RemoveLabel(id, "Solo")
	if !errors.Is(err, ErrLastLabel) {
		t.Errorf("expected ErrLastLabel, got %v", err)
	}
}

func TestRemoveNodeLabel_LabelNotFoundError(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.Nodes.Add([]string{"Person"}, nil)
	id := n.ID()

	err := g.Nodes.RemoveLabel(id, "Ghost")
	if !errors.Is(err, ErrLabelNotFound) {
		t.Errorf("expected ErrLabelNotFound, got %v", err)
	}
}

func TestRemoveNodeLabel_NodeNotFoundError(t *testing.T) {
	g, _ := New(Config{})

	// Register the label so Lookup() succeeds.
	_, _ = g.Resolve.GetOrCreateLabel("Person")

	err := g.Nodes.RemoveLabel(999, "Person")
	if !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Errorf("expected storepkg.ErrNodeNotFound, got %v", err)
	}
}

func TestRemoveNodeLabel_HashUpdated(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.Nodes.Add([]string{"A", "B"}, nil)
	id := n.ID()
	origHash := ""
	if ig := n.Integrity(); ig != nil {
		origHash = ig.Hash
	}

	if err := g.Nodes.RemoveLabel(id, "B"); err != nil {
		t.Fatalf("RemoveNodeLabel: %v", err)
	}

	updated, _ := g.Nodes.Get(id)
	newHash := ""
	if ig := updated.Integrity(); ig != nil {
		newHash = ig.Hash
	}
	if newHash == "" {
		t.Error("hash should be non-empty after removal")
	}
	if newHash == origHash {
		t.Error("hash should differ after label removal")
	}
}

func TestRemoveNodeLabel_NodesByLabelUpdated(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.Nodes.Add([]string{"Thing", "Tag"}, nil)
	id := n.ID()

	before, _ := g.Nodes.ByLabel("Tag", storepkg.QueryOpts{})
	if len(before) == 0 {
		t.Fatal("expected node in Tag label index before removal")
	}

	if err := g.Nodes.RemoveLabel(id, "Tag"); err != nil {
		t.Fatalf("RemoveNodeLabel: %v", err)
	}

	after, _ := g.Nodes.ByLabel("Tag", storepkg.QueryOpts{})
	for _, node := range after {
		if node.ID() == id {
			t.Error("node still found in 'Tag' label index after removal")
		}
	}
}

func TestRemoveNodeLabel_PublishesEvent(t *testing.T) {
	g, _ := New(Config{})
	bus := eventspkg.NewEventBus()
	g.Events.SetSync(bus)

	n, _ := g.Nodes.Add([]string{"A", "B"}, nil)
	id := n.ID()

	var events []eventspkg.Event
	bus.Subscribe(func(e eventspkg.Event) {
		events = append(events, e)
	})
	events = nil // clear AddNode event

	if err := g.Nodes.RemoveLabel(id, "B"); err != nil {
		t.Fatalf("RemoveNodeLabel: %v", err)
	}

	if len(events) == 0 {
		t.Fatal("expected eventspkg.EventNodeUpdate, got none")
	}
	if events[0].Type != eventspkg.EventNodeUpdate {
		t.Errorf("event type = %v, want eventspkg.EventNodeUpdate", events[0].Type)
	}
}
