package core

import (
	"context"
	"testing"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"
)

func TestAbsentPropertyDeleteVersionedUpdatesAreReadOnlyNoOps(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	_ = g.Events.SetSync(eventspkg.NewEventBus())

	a, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "KNOWS", a, b, map[string]any{"since": int64(2026)})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	events := collectEvents(g, eventspkg.EventNodeUpdate)
	before, _ := g.Stats.Get()

	updatedNode, err := g.Nodes.Update(context.Background(), a.ID(), map[string]any{"missing": nil})
	if err != nil {
		t.Fatalf("UpdateNode absent delete: %v", err)
	}
	if updatedNode.Version() != 0 {
		t.Fatalf("node version = %d, want 0", updatedNode.Version())
	}
	if got, ok := updatedNode.GetProperty("name"); !ok || got != "Alice" {
		t.Fatalf("node name = (%v, %v), want (Alice, true)", got, ok)
	}
	if _, ok := updatedNode.GetProperty("missing"); ok {
		t.Fatal("absent node property became present")
	}

	updatedRel, err := g.Rels.Update(context.Background(), r.ID(), map[string]any{"missing": nil})
	if err != nil {
		t.Fatalf("UpdateRelationship absent delete: %v", err)
	}
	if updatedRel.Version() != 0 {
		t.Fatalf("relationship version = %d, want 0", updatedRel.Version())
	}
	if got, ok := updatedRel.GetProperty("since"); !ok || got != int64(2026) {
		t.Fatalf("relationship since = (%v, %v), want (2026, true)", got, ok)
	}
	if _, ok := updatedRel.GetProperty("missing"); ok {
		t.Fatal("absent relationship property became present")
	}

	after, _ := g.Stats.Get()
	if after.NodesUpdated != before.NodesUpdated || after.RelsUpdated != before.RelsUpdated {
		t.Fatalf("update counters changed for absent deletes: before=%+v after=%+v", before, after)
	}
	if after.NodesRead != before.NodesRead+1 || after.RelsRead != before.RelsRead+1 {
		t.Fatalf("read counters after absent deletes = nodes %d rels %d, want %d/%d",
			after.NodesRead, after.RelsRead, before.NodesRead+1, before.RelsRead+1)
	}
	if gotEvents := drain(events); len(gotEvents) != 0 {
		t.Fatalf("absent delete no-ops published events: %+v", gotEvents)
	}

	nodeHistory, err := g.Nodes.History(a.ID())
	if err != nil {
		t.Fatalf("Node history: %v", err)
	}
	if len(nodeHistory) != 0 {
		t.Fatalf("node history len = %d, want 0", len(nodeHistory))
	}
	relHistory, err := g.Rels.History(r.ID())
	if err != nil {
		t.Fatalf("Relationship history: %v", err)
	}
	if len(relHistory) != 0 {
		t.Fatalf("relationship history len = %d, want 0", len(relHistory))
	}
}

func TestAbsentPropertyDeleteInPlaceUpdatesAreReadOnlyNoOps(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	_ = g.Events.SetSync(eventspkg.NewEventBus())

	a, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "KNOWS", a, b, map[string]any{"since": int64(2026)})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	events := collectEvents(g, eventspkg.EventNodeUpdate)
	before, _ := g.Stats.Get()

	updatedNode, err := g.Nodes.UpdateInPlace(context.Background(), a.ID(), map[string]any{"missing": nil})
	if err != nil {
		t.Fatalf("UpdateNodeInPlace absent delete: %v", err)
	}
	if updatedNode.Version() != 0 {
		t.Fatalf("node version = %d, want 0", updatedNode.Version())
	}

	updatedRel, err := g.Rels.UpdateInPlace(context.Background(), r.ID(), map[string]any{"missing": nil})
	if err != nil {
		t.Fatalf("UpdateRelationshipInPlace absent delete: %v", err)
	}
	if updatedRel.Version() != 0 {
		t.Fatalf("relationship version = %d, want 0", updatedRel.Version())
	}

	after, _ := g.Stats.Get()
	if after.NodesUpdated != before.NodesUpdated || after.RelsUpdated != before.RelsUpdated {
		t.Fatalf("update counters changed for in-place absent deletes: before=%+v after=%+v", before, after)
	}
	if after.NodesRead != before.NodesRead+1 || after.RelsRead != before.RelsRead+1 {
		t.Fatalf("read counters after in-place absent deletes = nodes %d rels %d, want %d/%d",
			after.NodesRead, after.RelsRead, before.NodesRead+1, before.RelsRead+1)
	}
	if gotEvents := drain(events); len(gotEvents) != 0 {
		t.Fatalf("in-place absent delete no-ops published events: %+v", gotEvents)
	}
}

func TestSameValuePropertyUpdatesAreReadOnlyNoOps(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	_ = g.Events.SetSync(eventspkg.NewEventBus())

	a, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "KNOWS", a, b, map[string]any{"since": int64(2026)})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	events := collectEvents(g, eventspkg.EventNodeUpdate)
	before, _ := g.Stats.Get()

	updatedNode, err := g.Nodes.Update(context.Background(), a.ID(), map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("UpdateNode same value: %v", err)
	}
	if updatedNode.Version() != 0 {
		t.Fatalf("node version after same-value update = %d, want 0", updatedNode.Version())
	}
	updatedRel, err := g.Rels.Update(context.Background(), r.ID(), map[string]any{"since": int64(2026)})
	if err != nil {
		t.Fatalf("UpdateRelationship same value: %v", err)
	}
	if updatedRel.Version() != 0 {
		t.Fatalf("relationship version after same-value update = %d, want 0", updatedRel.Version())
	}

	updatedNode, err = g.Nodes.UpdateInPlace(context.Background(), a.ID(), map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("UpdateNodeInPlace same value: %v", err)
	}
	if updatedNode.Version() != 0 {
		t.Fatalf("node version after same-value in-place update = %d, want 0", updatedNode.Version())
	}
	updatedRel, err = g.Rels.UpdateInPlace(context.Background(), r.ID(), map[string]any{"since": int64(2026)})
	if err != nil {
		t.Fatalf("UpdateRelationshipInPlace same value: %v", err)
	}
	if updatedRel.Version() != 0 {
		t.Fatalf("relationship version after same-value in-place update = %d, want 0", updatedRel.Version())
	}

	if ok, err := g.Nodes.CompareAndSetProperty(context.Background(), a.ID(), "name", "Alice", "Alice"); err != nil || !ok {
		t.Fatalf("Node CAS same value = (%v, %v), want (true, nil)", ok, err)
	}
	if ok, err := g.Rels.CompareAndSetProperty(context.Background(), r.ID(), "since", int64(2026), int64(2026)); err != nil || !ok {
		t.Fatalf("Relationship CAS same value = (%v, %v), want (true, nil)", ok, err)
	}

	after, _ := g.Stats.Get()
	if after.NodesUpdated != before.NodesUpdated || after.RelsUpdated != before.RelsUpdated {
		t.Fatalf("same-value update counters changed: before=%+v after=%+v", before, after)
	}
	if after.NodesRead != before.NodesRead+2 || after.RelsRead != before.RelsRead+2 {
		t.Fatalf("read counters after same-value updates = nodes %d rels %d, want %d/%d",
			after.NodesRead, after.RelsRead, before.NodesRead+2, before.RelsRead+2)
	}
	if gotEvents := drain(events); len(gotEvents) != 0 {
		t.Fatalf("same-value no-ops published events: %+v", gotEvents)
	}

	nodeHistory, err := g.Nodes.History(a.ID())
	if err != nil {
		t.Fatalf("Node history: %v", err)
	}
	if len(nodeHistory) != 0 {
		t.Fatalf("node history len = %d, want 0", len(nodeHistory))
	}
	relHistory, err := g.Rels.History(r.ID())
	if err != nil {
		t.Fatalf("Relationship history: %v", err)
	}
	if len(relHistory) != 0 {
		t.Fatalf("relationship history len = %d, want 0", len(relHistory))
	}
}

func TestBatchAbsentPropertyDeleteUpdatesAreReadOnlyNoOps(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	_ = g.Events.SetSync(eventspkg.NewEventBus())

	a, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "KNOWS", a, b, map[string]any{"since": int64(2026)})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	events := collectEvents(g, eventspkg.EventNodeUpdate)

	batch, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	if err := batch.UpdateNode(a.ID(), map[string]any{"missing": nil}); err != nil {
		t.Fatalf("UpdateNode queue: %v", err)
	}
	if err := batch.UpdateRelationship(r.ID(), map[string]any{"missing": nil}); err != nil {
		t.Fatalf("UpdateRelationship queue: %v", err)
	}

	result, err := batch.Execute()
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Updated != 0 || result.Failed != 0 {
		t.Fatalf("result = %+v, want Updated=0 Failed=0", result)
	}
	if gotEvents := drain(events); len(gotEvents) != 0 {
		t.Fatalf("batch absent delete no-ops published events: %+v", gotEvents)
	}

	gotNode, err := g.Nodes.Get(context.Background(), a.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if gotNode.Version() != 0 {
		t.Fatalf("node version = %d, want 0", gotNode.Version())
	}
	gotRel, err := g.Rels.Get(context.Background(), r.ID())
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if gotRel.Version() != 0 {
		t.Fatalf("relationship version = %d, want 0", gotRel.Version())
	}
}

func TestTxAbsentPropertyDeleteUpdatesAreReadOnlyNoOps(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	_ = g.Events.SetSync(eventspkg.NewEventBus())

	a, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "KNOWS", a, b, map[string]any{"since": int64(2026)})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	events := collectEvents(g, eventspkg.EventNodeUpdate)
	before, _ := g.Stats.Get()

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.UpdateNode(a.ID(), map[string]any{"missing": nil}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("UpdateNode tx absent delete: %v", err)
	}
	if _, err := tx.UpdateRelationship(r.ID(), map[string]any{"missing": nil}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("UpdateRelationship tx absent delete: %v", err)
	}
	if len(tx.updatedNodes) != 0 || len(tx.updatedRels) != 0 {
		_ = tx.Rollback()
		t.Fatalf("tx no-op updates recorded rollback snapshots: nodes=%d rels=%d", len(tx.updatedNodes), len(tx.updatedRels))
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	after, _ := g.Stats.Get()
	if after.NodesUpdated != before.NodesUpdated || after.RelsUpdated != before.RelsUpdated {
		t.Fatalf("tx update counters changed for absent deletes: before=%+v after=%+v", before, after)
	}
	if gotEvents := drain(events); len(gotEvents) != 0 {
		t.Fatalf("tx absent delete no-ops published events: %+v", gotEvents)
	}

	gotNode, err := g.Nodes.Get(context.Background(), a.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if gotNode.Version() != 0 {
		t.Fatalf("node version = %d, want 0", gotNode.Version())
	}
	gotRel, err := g.Rels.Get(context.Background(), r.ID())
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if gotRel.Version() != 0 {
		t.Fatalf("relationship version = %d, want 0", gotRel.Version())
	}
}

func TestBatchAndTxSameValuePropertyUpdatesAreReadOnlyNoOps(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	_ = g.Events.SetSync(eventspkg.NewEventBus())

	a, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "KNOWS", a, b, map[string]any{"since": int64(2026)})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	events := collectEvents(g, eventspkg.EventNodeUpdate)
	before, _ := g.Stats.Get()

	batch, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	if err := batch.UpdateNode(a.ID(), map[string]any{"name": "Alice"}); err != nil {
		t.Fatalf("Batch UpdateNode queue: %v", err)
	}
	if err := batch.UpdateRelationship(r.ID(), map[string]any{"since": int64(2026)}); err != nil {
		t.Fatalf("Batch UpdateRelationship queue: %v", err)
	}
	result, err := batch.Execute()
	if err != nil {
		t.Fatalf("Batch Execute: %v", err)
	}
	if result.Updated != 0 || result.Failed != 0 {
		t.Fatalf("batch result = %+v, want Updated=0 Failed=0", result)
	}

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.UpdateNode(a.ID(), map[string]any{"name": "Alice"}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("Tx UpdateNode same value: %v", err)
	}
	if _, err := tx.UpdateRelationship(r.ID(), map[string]any{"since": int64(2026)}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("Tx UpdateRelationship same value: %v", err)
	}
	if len(tx.updatedNodes) != 0 || len(tx.updatedRels) != 0 {
		_ = tx.Rollback()
		t.Fatalf("tx same-value no-ops recorded rollback snapshots: nodes=%d rels=%d", len(tx.updatedNodes), len(tx.updatedRels))
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	after, _ := g.Stats.Get()
	if after.NodesUpdated != before.NodesUpdated || after.RelsUpdated != before.RelsUpdated {
		t.Fatalf("same-value batch/tx counters changed: before=%+v after=%+v", before, after)
	}
	if gotEvents := drain(events); len(gotEvents) != 0 {
		t.Fatalf("same-value batch/tx no-ops published events: %+v", gotEvents)
	}

	gotNode, err := g.Nodes.Get(context.Background(), a.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if gotNode.Version() != 0 {
		t.Fatalf("node version = %d, want 0", gotNode.Version())
	}
	gotRel, err := g.Rels.Get(context.Background(), r.ID())
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if gotRel.Version() != 0 {
		t.Fatalf("relationship version = %d, want 0", gotRel.Version())
	}
}

func TestExistingNilValuedPropertyDeleteStillMutates(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"drop": nil})
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "KNOWS", a, b, map[string]any{"drop": nil})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	before, _ := g.Stats.Get()
	updatedNode, err := g.Nodes.Update(context.Background(), a.ID(), map[string]any{"drop": nil})
	if err != nil {
		t.Fatalf("UpdateNode delete nil-valued property: %v", err)
	}
	if _, ok := updatedNode.GetProperty("drop"); ok {
		t.Fatal("node property drop still present")
	}
	if updatedNode.Version() != 1 {
		t.Fatalf("node version = %d, want 1", updatedNode.Version())
	}

	updatedRel, err := g.Rels.Update(context.Background(), r.ID(), map[string]any{"drop": nil})
	if err != nil {
		t.Fatalf("UpdateRelationship delete nil-valued property: %v", err)
	}
	if _, ok := updatedRel.GetProperty("drop"); ok {
		t.Fatal("relationship property drop still present")
	}
	if updatedRel.Version() != 1 {
		t.Fatalf("relationship version = %d, want 1", updatedRel.Version())
	}

	after, _ := g.Stats.Get()
	if after.NodesUpdated != before.NodesUpdated+1 || after.RelsUpdated != before.RelsUpdated+1 {
		t.Fatalf("delete existing property counters = nodes %d rels %d, want %d/%d",
			after.NodesUpdated, after.RelsUpdated, before.NodesUpdated+1, before.RelsUpdated+1)
	}
}
