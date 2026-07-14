package sharded_test

import (
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
)

// TestGraphLevelSmoke wires a sharded.Store into graph.New and exercises the
// basic Add/Get/Update/Delete/ByLabel + relationship flow.
//
// Core still mints legacy dual-generator IDs (SnowflakeNodeID*2 for nodes,
// *2+1 for rels — S4 lane minting is not wired), so a graph-level deployment
// must claim slots covering BOTH raw values. With SnowflakeNodeID=0, nodes land
// on slot 0 and rels on slot 1, so BaseSlot=0, SlotCount=2 claims both. Every
// relationship is therefore cross-shard from its endpoints — exercising the
// adjacency fold + cross-shard endpoint validation end to end.
func TestGraphLevelSmoke(t *testing.T) {
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	g, err := graph.New(graph.Config{Store: st, SnowflakeNodeID: 0})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	defer func() { _ = g.Close() }()

	ctx := context.Background()

	alice, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("Add alice: %v", err)
	}
	bob, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("Add bob: %v", err)
	}

	// Get.
	got, err := g.Nodes().Get(ctx, alice.ID())
	if err != nil {
		t.Fatalf("Get alice: %v", err)
	}
	if name, _ := got.PropertiesMap()["name"].(string); name != "Alice" {
		t.Fatalf("alice name = %q, want Alice", name)
	}

	// Update.
	if _, err := g.Nodes().Update(ctx, alice.ID(), map[string]any{"name": "Alicia"}); err != nil {
		t.Fatalf("Update alice: %v", err)
	}
	got2, _ := g.Nodes().Get(ctx, alice.ID())
	if name, _ := got2.PropertiesMap()["name"].(string); name != "Alicia" {
		t.Fatalf("post-update alice name = %q, want Alicia", name)
	}

	// Relationship (cross-shard: nodes on slot 0, rel on slot 1).
	rel, err := g.Rels().Add(ctx, "KNOWS", alice, bob, map[string]any{"since": int64(2020)})
	if err != nil {
		t.Fatalf("Add rel: %v", err)
	}
	if _, err := g.Rels().Get(ctx, rel.ID()); err != nil {
		t.Fatalf("Get rel: %v", err)
	}

	// Adjacency through the graph layer (folds cross-shard).
	out, err := g.Rels().Outgoing(alice.ID(), "")
	if err != nil {
		t.Fatalf("Outgoing: %v", err)
	}
	if len(out) != 1 || out[0].ID() != rel.ID() {
		t.Fatalf("Outgoing = %d rels, want 1 (the KNOWS rel)", len(out))
	}

	// ByLabel.
	people, err := g.Nodes().ByLabel("Person", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabel: %v", err)
	}
	if len(people) != 2 {
		t.Fatalf("ByLabel(Person) = %d, want 2", len(people))
	}

	// Delete bob (cascade removes the KNOWS rel too).
	if err := g.Nodes().Delete(ctx, bob.ID()); err != nil {
		t.Fatalf("Delete bob: %v", err)
	}
	people2, _ := g.Nodes().ByLabel("Person", storepkg.QueryOpts{})
	if len(people2) != 1 {
		t.Fatalf("post-delete ByLabel(Person) = %d, want 1", len(people2))
	}
	// The rel is gone.
	if _, err := g.Rels().Get(ctx, rel.ID()); err == nil {
		t.Fatalf("expected KNOWS rel deleted with bob")
	}
}
