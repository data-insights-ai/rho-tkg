package graph

import (
	"context"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func containsNodeID(nodes []*types.Node, id snowflake.ID) bool {
	for _, n := range nodes {
		if n.InternalID().SnowflakeID() == id {
			return true
		}
	}
	return false
}

func TestVerifyNodeHashChain_LabelMutations(t *testing.T) {
	g := newTestGraph(t)

	n, err := g.AddNode([]string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.InternalID().SnowflakeID()

	if err := g.AddNodeLabel(id, "B"); err != nil {
		t.Fatalf("AddNodeLabel: %v", err)
	}
	valid, err := g.VerifyNodeHashChain(id)
	if err != nil {
		t.Fatalf("VerifyNodeHashChain after add: %v", err)
	}
	if !valid {
		t.Fatal("hash chain should verify after adding a label")
	}

	if err := g.RemoveNodeLabel(id, "B"); err != nil {
		t.Fatalf("RemoveNodeLabel: %v", err)
	}
	valid, err = g.VerifyNodeHashChain(id)
	if err != nil {
		t.Fatalf("VerifyNodeHashChain after remove: %v", err)
	}
	if !valid {
		t.Fatal("hash chain should verify after removing a label")
	}
}

func TestGetNodesByLabelValidAt_UsesHistoricalLabelVersion(t *testing.T) {
	g := newTestGraph(t)

	n, err := g.AddNode([]string{"Person", "Legacy"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.InternalID().SnowflakeID()
	queryTime := g.nodeValidFrom(n)

	time.Sleep(2 * time.Millisecond)
	if err := g.RemoveNodeLabel(id, "Legacy"); err != nil {
		t.Fatalf("RemoveNodeLabel: %v", err)
	}

	nodes, err := g.GetNodesByLabelValidAt("Legacy", queryTime)
	if err != nil {
		t.Fatalf("GetNodesByLabelValidAt: %v", err)
	}
	if !containsNodeID(nodes, id) {
		t.Fatalf("historical Legacy label missing at %d; got %d nodes", queryTime, len(nodes))
	}
}

func TestNodesByLabelPropertyTemporalQueries_UseHistoricalPropertyVersion(t *testing.T) {
	g := newTestGraph(t)

	n, err := g.AddNode([]string{"Person"}, map[string]any{"status": "draft"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.InternalID().SnowflakeID()
	queryTime := g.nodeValidFrom(n)

	time.Sleep(2 * time.Millisecond)
	updated, err := g.UpdateNode(id, map[string]any{"status": "published"})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	nodes, err := g.NodesByLabelPropertyAndTime("Person", "status", "draft", queryTime)
	if err != nil {
		t.Fatalf("NodesByLabelPropertyAndTime: %v", err)
	}
	if !containsNodeID(nodes, id) {
		t.Fatalf("historical property value missing at %d; got %d nodes", queryTime, len(nodes))
	}

	end := updated.Temporal().UpdatedAt
	nodes, err = g.NodesByLabelPropertyDuring("Person", "status", "draft", queryTime, end)
	if err != nil {
		t.Fatalf("NodesByLabelPropertyDuring: %v", err)
	}
	if !containsNodeID(nodes, id) {
		t.Fatalf("historical property value missing during [%d, %d); got %d nodes", queryTime, end, len(nodes))
	}
}

func TestGetNeighborsValidAt_UsesHistoricalRelationships(t *testing.T) {
	g := newTestGraph(t)

	a, err := g.AddNode([]string{"Person"}, map[string]any{"name": "A"})
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	b, err := g.AddNode([]string{"Person"}, map[string]any{"name": "B"})
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}
	r, err := g.AddRelationship("KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	queryTime := g.relValidFrom(r)

	time.Sleep(2 * time.Millisecond)
	if err := g.DeleteRelationship(r.InternalID().SnowflakeID()); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}

	neighbors, err := g.GetNeighborsValidAt(a.InternalID().SnowflakeID(), queryTime)
	if err != nil {
		t.Fatalf("GetNeighborsValidAt: %v", err)
	}
	if !containsNodeID(neighbors, b.InternalID().SnowflakeID()) {
		t.Fatalf("historical neighbor missing at %d; got %d neighbors", queryTime, len(neighbors))
	}
}

func TestLabelMutations_UpdateTransactionTimeBounds(t *testing.T) {
	tests := []struct {
		name   string
		labels []string
		mutate func(*Graph, snowflake.ID) error
	}{
		{
			name:   "add",
			labels: []string{"A"},
			mutate: func(g *Graph, id snowflake.ID) error {
				return g.AddNodeLabel(id, "B")
			},
		},
		{
			name:   "remove",
			labels: []string{"A", "B"},
			mutate: func(g *Graph, id snowflake.ID) error {
				return g.RemoveNodeLabel(id, "B")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := newTestGraph(t)
			n, err := g.AddNode(tt.labels, nil)
			if err != nil {
				t.Fatalf("AddNode: %v", err)
			}
			id := n.InternalID().SnowflakeID()
			origTxFrom := n.Temporal().TxFrom

			time.Sleep(2 * time.Millisecond)
			if err := tt.mutate(g, id); err != nil {
				t.Fatalf("label mutation: %v", err)
			}

			current, err := g.GetNode(id)
			if err != nil {
				t.Fatalf("GetNode: %v", err)
			}
			currentTM := current.Temporal()
			if currentTM == nil || currentTM.TxFrom <= origTxFrom {
				t.Fatalf("current TxFrom = %v, want > original %v", currentTM, origTxFrom)
			}

			hist, err := g.GetNodeHistory(id)
			if err != nil {
				t.Fatalf("GetNodeHistory: %v", err)
			}
			if len(hist) != 1 {
				t.Fatalf("history entries = %d, want 1", len(hist))
			}
			histTM := hist[0].Temporal()
			if histTM == nil || histTM.TxTo == 0 {
				t.Fatalf("history TxTo = %v, want non-zero", histTM)
			}
			if histTM.TxTo > currentTM.TxFrom {
				t.Fatalf("history TxTo %d should be <= current TxFrom %d", histTM.TxTo, currentTM.TxFrom)
			}
		})
	}
}

func TestNoOpMutations_DoNotPublishUpdateEvents(t *testing.T) {
	t.Run("idempotent add label", func(t *testing.T) {
		g := newTestGraphForEvents(t)
		n, err := g.AddNode([]string{"A", "B"}, nil)
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		events := collectEvents(g, EventNodeUpdate)

		if err := g.AddNodeLabel(n.InternalID().SnowflakeID(), "B"); err != nil {
			t.Fatalf("AddNodeLabel: %v", err)
		}
		if got := drain(events); len(got) != 0 {
			t.Fatalf("idempotent AddNodeLabel published %d events: %v", len(got), got)
		}
	})

	t.Run("empty node update", func(t *testing.T) {
		g := newTestGraphForEvents(t)
		n, err := g.AddNode([]string{"A"}, nil)
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		events := collectEvents(g, EventNodeUpdate)

		if _, err := g.UpdateNode(n.InternalID().SnowflakeID(), map[string]any{}); err != nil {
			t.Fatalf("UpdateNode: %v", err)
		}
		if got := drain(events); len(got) != 0 {
			t.Fatalf("empty UpdateNode published %d events: %v", len(got), got)
		}
	})

	t.Run("empty relationship update", func(t *testing.T) {
		g := newTestGraphForEvents(t)
		a, _ := g.AddNode([]string{"A"}, nil)
		b, _ := g.AddNode([]string{"B"}, nil)
		r, err := g.AddRelationship("REL", a, b, nil)
		if err != nil {
			t.Fatalf("AddRelationship: %v", err)
		}
		events := collectEvents(g, EventRelUpdate)

		if _, err := g.UpdateRelationship(r.InternalID().SnowflakeID(), map[string]any{}); err != nil {
			t.Fatalf("UpdateRelationship: %v", err)
		}
		if got := drain(events); len(got) != 0 {
			t.Fatalf("empty UpdateRelationship published %d events: %v", len(got), got)
		}
	})

	t.Run("empty node in-place update", func(t *testing.T) {
		g := newTestGraphForEvents(t)
		n, err := g.AddNode([]string{"A"}, nil)
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		events := collectEvents(g, EventNodeUpdate)

		if _, err := g.UpdateNodeInPlace(n.InternalID().SnowflakeID(), map[string]any{}); err != nil {
			t.Fatalf("UpdateNodeInPlace: %v", err)
		}
		if got := drain(events); len(got) != 0 {
			t.Fatalf("empty UpdateNodeInPlace published %d events: %v", len(got), got)
		}
	})

	t.Run("empty relationship in-place update", func(t *testing.T) {
		g := newTestGraphForEvents(t)
		a, _ := g.AddNode([]string{"A"}, nil)
		b, _ := g.AddNode([]string{"B"}, nil)
		r, err := g.AddRelationship("REL", a, b, nil)
		if err != nil {
			t.Fatalf("AddRelationship: %v", err)
		}
		events := collectEvents(g, EventRelUpdate)

		if _, err := g.UpdateRelInPlace(r.InternalID().SnowflakeID(), map[string]any{}); err != nil {
			t.Fatalf("UpdateRelInPlace: %v", err)
		}
		if got := drain(events); len(got) != 0 {
			t.Fatalf("empty UpdateRelInPlace published %d events: %v", len(got), got)
		}
	})

	t.Run("compare-and-set absent delete", func(t *testing.T) {
		g := newTestGraphForEvents(t)
		n, err := g.AddNode([]string{"A"}, nil)
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		events := collectEvents(g, EventNodeUpdate)

		ok, err := g.CompareAndSetProperty(n.InternalID().SnowflakeID(), "missing", nil, nil)
		if err != nil {
			t.Fatalf("CompareAndSetProperty: %v", err)
		}
		if !ok {
			t.Fatal("CompareAndSetProperty should report a matched absent property")
		}
		if got := drain(events); len(got) != 0 {
			t.Fatalf("absent-property CAS delete published %d events: %v", len(got), got)
		}
	})
}

func TestImportNodeWithID_MatchesAddNodeMetadataEventsAndStats(t *testing.T) {
	g := newTestGraphForEvents(t)
	events := collectEvents(g, EventNodeCreate)
	before := g.Stats()

	n, err := g.ImportNodeWithID(context.Background(), snowflake.ID(12345), []string{"Person"}, map[string]any{
		"name":           "Alice",
		"tkg_valid_from": int64(1000),
		"tkg_created_at": int64(900),
	})
	if err != nil {
		t.Fatalf("ImportNodeWithID: %v", err)
	}

	tm := n.Temporal()
	if tm == nil {
		t.Fatal("imported node missing temporal metadata")
	}
	if tm.TxFrom == 0 {
		t.Fatal("imported node TxFrom should be set")
	}
	if tm.ValidFrom != 1000 || tm.CreatedAt != 900 {
		t.Fatalf("temporal metadata = %+v, want ValidFrom=1000 CreatedAt=900", tm)
	}
	if _, ok := n.GetProperty("tkg_valid_from"); ok {
		t.Fatal("reserved temporal key should not be stored as a normal property")
	}
	if got := drain(events); len(got) != 1 || got[0].Type != EventNodeCreate || got[0].EntityID != n.InternalID().SnowflakeID() {
		t.Fatalf("expected one EventNodeCreate for imported node, got %v", got)
	}
	after := g.Stats()
	if after.NodesAdded != before.NodesAdded+1 {
		t.Fatalf("NodesAdded = %d, want %d", after.NodesAdded, before.NodesAdded+1)
	}
}

func TestImportRelationshipWithID_MatchesAddRelationshipMetadataEventsAndStats(t *testing.T) {
	g := newTestGraphForEvents(t)
	a, err := g.AddNode([]string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	b, err := g.AddNode([]string{"B"}, nil)
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}

	events := collectEvents(g, EventRelCreate)
	before := g.Stats()

	r, err := g.ImportRelationshipWithID(context.Background(), snowflake.ID(54321), "REL", a, b, map[string]any{
		"weight":         int64(1),
		"tkg_valid_from": int64(2000),
		"tkg_created_at": int64(1900),
	})
	if err != nil {
		t.Fatalf("ImportRelationshipWithID: %v", err)
	}

	tm := r.Temporal()
	if tm == nil {
		t.Fatal("imported relationship missing temporal metadata")
	}
	if tm.TxFrom == 0 {
		t.Fatal("imported relationship TxFrom should be set")
	}
	if tm.ValidFrom != 2000 || tm.CreatedAt != 1900 {
		t.Fatalf("temporal metadata = %+v, want ValidFrom=2000 CreatedAt=1900", tm)
	}
	if _, ok := r.GetProperty("tkg_valid_from"); ok {
		t.Fatal("reserved temporal key should not be stored as a normal property")
	}
	if got := drain(events); len(got) != 1 || got[0].Type != EventRelCreate || got[0].EntityID != r.InternalID().SnowflakeID() {
		t.Fatalf("expected one EventRelCreate for imported relationship, got %v", got)
	}
	after := g.Stats()
	if after.RelsAdded != before.RelsAdded+1 {
		t.Fatalf("RelsAdded = %d, want %d", after.RelsAdded, before.RelsAdded+1)
	}
}
