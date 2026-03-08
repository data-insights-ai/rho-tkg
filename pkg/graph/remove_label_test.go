package graph_test

import (
	"errors"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph"
)

func TestRemoveNodeLabel_ExtraLabel(t *testing.T) {
	g, _ := graph.New(graph.Config{})
	n, _ := g.AddNode([]string{"Person", "Employee"}, nil)
	id := n.InternalID().SnowflakeID()

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
	g, _ := graph.New(graph.Config{})
	n, _ := g.AddNode([]string{"Primary", "Secondary"}, nil)
	id := n.InternalID().SnowflakeID()

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
	g, _ := graph.New(graph.Config{})
	n, _ := g.AddNode([]string{"Solo"}, nil)
	id := n.InternalID().SnowflakeID()

	err := g.RemoveNodeLabel(id, "Solo")
	if !errors.Is(err, graph.ErrLastLabel) {
		t.Errorf("expected ErrLastLabel, got %v", err)
	}
}

func TestRemoveNodeLabel_LabelNotFoundError(t *testing.T) {
	g, _ := graph.New(graph.Config{})
	n, _ := g.AddNode([]string{"Person"}, nil)
	id := n.InternalID().SnowflakeID()

	err := g.RemoveNodeLabel(id, "Ghost")
	if !errors.Is(err, graph.ErrLabelNotFound) {
		t.Errorf("expected ErrLabelNotFound, got %v", err)
	}
}

func TestRemoveNodeLabel_NodeNotFoundError(t *testing.T) {
	g, _ := graph.New(graph.Config{})

	// Register the label so Lookup() succeeds.
	_, _ = g.GetOrCreateLabel("Person")

	err := g.RemoveNodeLabel(999, "Person")
	if !errors.Is(err, graph.ErrNodeNotFound) {
		t.Errorf("expected ErrNodeNotFound, got %v", err)
	}
}

func TestRemoveNodeLabel_HashUpdated(t *testing.T) {
	g, _ := graph.New(graph.Config{})
	n, _ := g.AddNode([]string{"A", "B"}, nil)
	id := n.InternalID().SnowflakeID()
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
	g, _ := graph.New(graph.Config{})
	n, _ := g.AddNode([]string{"Thing", "Tag"}, nil)
	id := n.InternalID().SnowflakeID()

	before, _ := g.NodesByLabel("Tag", graph.QueryOpts{})
	if len(before) == 0 {
		t.Fatal("expected node in Tag label index before removal")
	}

	if err := g.RemoveNodeLabel(id, "Tag"); err != nil {
		t.Fatalf("RemoveNodeLabel: %v", err)
	}

	after, _ := g.NodesByLabel("Tag", graph.QueryOpts{})
	for _, node := range after {
		if node.InternalID().SnowflakeID() == id {
			t.Error("node still found in 'Tag' label index after removal")
		}
	}
}

func TestRemoveNodeLabel_PublishesEvent(t *testing.T) {
	g, _ := graph.New(graph.Config{})
	bus := graph.NewEventBus()
	g.SetEventBus(bus)

	n, _ := g.AddNode([]string{"A", "B"}, nil)
	id := n.InternalID().SnowflakeID()

	var events []graph.Event
	bus.Subscribe(func(e graph.Event) {
		events = append(events, e)
	})
	events = nil // clear AddNode event

	if err := g.RemoveNodeLabel(id, "B"); err != nil {
		t.Fatalf("RemoveNodeLabel: %v", err)
	}

	if len(events) == 0 {
		t.Fatal("expected EventNodeUpdate, got none")
	}
	if events[0].Type != graph.EventNodeUpdate {
		t.Errorf("event type = %v, want EventNodeUpdate", events[0].Type)
	}
}
