package graph

import (
	"errors"
	"testing"
)

func TestRemoveNodeLabel_ExtraLabel(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Person", "Employee"}, nil)
	id := n.ID()

	if err := g.RemoveNodeLabel(id, "Employee"); err != nil {
		t.Fatalf("RemoveNodeLabel: %v", err)
	}

	updated, _ := g.GetNode(id)
	if g.NodeHasLabel(updated, "Employee") {
		t.Error("label 'Employee' still present after removal")
	}
	if !g.NodeHasLabel(updated, "Person") {
		t.Error("primary label 'Person' should remain")
	}
}

func TestRemoveNodeLabel_PrimaryPromotesExtra(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Primary", "Secondary"}, nil)
	id := n.ID()

	if err := g.RemoveNodeLabel(id, "Primary"); err != nil {
		t.Fatalf("RemoveNodeLabel primary: %v", err)
	}

	updated, _ := g.GetNode(id)
	// After removing primary, "Secondary" should be promoted.
	labels := g.NodeLabels(updated)
	if len(labels) != 1 || labels[0] != "Secondary" {
		t.Errorf("labels after primary removal = %v, want [Secondary]", labels)
	}
}

func TestRemoveNodeLabel_LastLabelError(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Solo"}, nil)
	id := n.ID()

	err := g.RemoveNodeLabel(id, "Solo")
	if !errors.Is(err, ErrLastLabel) {
		t.Errorf("expected ErrLastLabel, got %v", err)
	}
}

func TestRemoveNodeLabel_LabelNotFoundError(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Person"}, nil)
	id := n.ID()

	err := g.RemoveNodeLabel(id, "Ghost")
	if !errors.Is(err, ErrLabelNotFound) {
		t.Errorf("expected ErrLabelNotFound, got %v", err)
	}
}

func TestRemoveNodeLabel_NodeNotFoundError(t *testing.T) {
	g, _ := New(Config{})

	// Register the label so Lookup() succeeds.
	_, _ = g.GetOrCreateLabel("Person")

	err := g.RemoveNodeLabel(999, "Person")
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("expected ErrNodeNotFound, got %v", err)
	}
}

func TestRemoveNodeLabel_HashUpdated(t *testing.T) {
	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"A", "B"}, nil)
	id := n.ID()
	origHash := ""
	if ig := n.Integrity(); ig != nil {
		origHash = ig.Hash
	}

	if err := g.RemoveNodeLabel(id, "B"); err != nil {
		t.Fatalf("RemoveNodeLabel: %v", err)
	}

	updated, _ := g.GetNode(id)
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
	n, _ := g.AddNode([]string{"Thing", "Tag"}, nil)
	id := n.ID()

	before, _ := g.NodesByLabel("Tag", QueryOpts{})
	if len(before) == 0 {
		t.Fatal("expected node in Tag label index before removal")
	}

	if err := g.RemoveNodeLabel(id, "Tag"); err != nil {
		t.Fatalf("RemoveNodeLabel: %v", err)
	}

	after, _ := g.NodesByLabel("Tag", QueryOpts{})
	for _, node := range after {
		if node.ID() == id {
			t.Error("node still found in 'Tag' label index after removal")
		}
	}
}

func TestRemoveNodeLabel_PublishesEvent(t *testing.T) {
	g, _ := New(Config{})
	bus := NewEventBus()
	g.SetEventBus(bus)

	n, _ := g.AddNode([]string{"A", "B"}, nil)
	id := n.ID()

	var events []Event
	bus.Subscribe(func(e Event) {
		events = append(events, e)
	})
	events = nil // clear AddNode event

	if err := g.RemoveNodeLabel(id, "B"); err != nil {
		t.Fatalf("RemoveNodeLabel: %v", err)
	}

	if len(events) == 0 {
		t.Fatal("expected EventNodeUpdate, got none")
	}
	if events[0].Type != EventNodeUpdate {
		t.Errorf("event type = %v, want EventNodeUpdate", events[0].Type)
	}
}
