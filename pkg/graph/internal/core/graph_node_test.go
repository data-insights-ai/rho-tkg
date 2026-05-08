package core

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestGraphNodePrimaryLabel(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	personTok, _ := g.Resolve.GetOrCreateLabel("Person")

	n := types.NewNode(types.NodeID(snowflake.ID(1)), personTok, nil)
	got := g.Nodes.PrimaryLabel(n)
	if got != "Person" {
		t.Errorf("NodePrimaryLabel = %q, want \"Person\"", got)
	}
}

func TestGraphNodeHasLabel(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	personTok, _ := g.Resolve.GetOrCreateLabel("Person")
	actorTok, _ := g.Resolve.GetOrCreateLabel("Actor")

	// Single-label node: primary label hit.
	n := types.NewNode(types.NodeID(snowflake.ID(1)), personTok, nil)
	if !g.Nodes.HasLabel(n, "Person") {
		t.Error("NodeHasLabel(\"Person\") = false, want true (primary)")
	}
	if g.Nodes.HasLabel(n, "Animal") {
		t.Error("NodeHasLabel(\"Animal\") = true, want false (unregistered)")
	}

	// Multi-label node: extra label hit.
	n2 := types.NewNode(types.NodeID(snowflake.ID(2)), personTok, []uint16{actorTok})
	if !g.Nodes.HasLabel(n2, "Actor") {
		t.Error("NodeHasLabel(\"Actor\") = false, want true (extra label)")
	}
	if !g.Nodes.HasLabel(n2, "Person") {
		t.Error("NodeHasLabel(\"Person\") = false, want true (primary on multi-label)")
	}
}

func TestGraphNodeRelIDValueUniqueness(t *testing.T) {
	t.Parallel()

	g, err := New(Config{SnowflakeNodeID: 0, Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Generate node and rel IDs concurrently. The even/odd node field
	// guarantees no value collision even within the same millisecond.
	const count = 1000
	all := make(map[snowflake.ID]string, count*2)

	for range count {
		nid := g.Nodes.NextID().SnowflakeID()
		if prev, dup := all[nid]; dup {
			t.Fatalf("node ID %d collides with %s", nid, prev)
		}
		all[nid] = "node"

		rid := g.Rels.NextID().SnowflakeID()
		if prev, dup := all[rid]; dup {
			t.Fatalf("rel ID %d collides with %s", rid, prev)
		}
		all[rid] = "rel"
	}
}

func TestGraphAddNode(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	n, err := g.Nodes.Add([]string{"Person", "Actor"}, map[string]any{"name": "Alice", "age": 30})
	if err != nil {
		t.Fatalf("AddNode() returned error: %v", err)
	}

	// Verify labels.
	labels := g.Nodes.Labels(n)
	if len(labels) != 2 || labels[0] != "Person" || labels[1] != "Actor" {
		t.Errorf("NodeLabels = %v, want [Person Actor]", labels)
	}

	// Verify properties.
	name, ok := n.GetProperty("name")
	if !ok || name != "Alice" {
		t.Errorf("GetProperty(\"name\") = (%v, %v), want (\"Alice\", true)", name, ok)
	}
	age, ok := n.GetProperty("age")
	if !ok || age != 30 {
		t.Errorf("GetProperty(\"age\") = (%v, %v), want (30, true)", age, ok)
	}

	// Verify retrievable from store.
	got, err := g.Nodes.Get(n.ID())
	if err != nil {
		t.Fatalf("GetNode() returned error: %v", err)
	}
	if got.ID() != n.ID() {
		t.Fatal("GetNode() returned node with different ID")
	}
	gotName, _ := got.GetProperty("name")
	if gotName != "Alice" {
		t.Fatalf("GetNode() property name = %v, want Alice", gotName)
	}
}

func TestGraphAddNodeNoLabels(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	_, err := g.Nodes.Add(nil, nil)
	if err == nil {
		t.Fatal("AddNode(nil labels) should return error")
	}
	if !errors.Is(err, ErrNoLabels) {
		t.Errorf("errors.Is(err, ErrNoLabels) = false; err = %v", err)
	}

	_, err = g.Nodes.Add([]string{}, nil)
	if !errors.Is(err, ErrNoLabels) {
		t.Errorf("errors.Is(err, ErrNoLabels) for empty slice = false; err = %v", err)
	}
}

func TestGraphAddNodeInvalidProperty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	_, err := g.Nodes.Add([]string{"Person"}, map[string]any{"tkg_hack": "bad"})
	if err == nil {
		t.Fatal("AddNode with tkg_ property should return error")
	}
}

func TestGraphAddNodeBulkProperties(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	props := make(map[string]any, 50)
	for i := range 50 {
		props[fmt.Sprintf("prop_%02d", i)] = i
	}

	n, err := g.Nodes.Add([]string{"Test"}, props)
	if err != nil {
		t.Fatalf("AddNode() returned error: %v", err)
	}

	// Verify all properties exist and are sorted.
	ps := n.Properties()
	if ps.Len() != 50 {
		t.Fatalf("Properties() len = %d, want 50", ps.Len())
	}
	for i := 1; i < ps.Len(); i++ {
		if ps[i-1].Key >= ps[i].Key {
			t.Fatalf("Properties not sorted: %q >= %q", ps[i-1].Key, ps[i].Key)
		}
	}
}

func TestGraphAddNodeUniqueIDs(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	seen := make(map[snowflake.ID]struct{}, 100)

	for range 100 {
		n, err := g.Nodes.Add([]string{"X"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		id := n.ID().SnowflakeID()
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate node ID: %d", id)
		}
		seen[id] = struct{}{}
	}
}

func TestGraphAddNodeNilProperties(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	n, err := g.Nodes.Add([]string{"Empty"}, nil)
	if err != nil {
		t.Fatalf("AddNode(nil props) returned error: %v", err)
	}
	if n.Properties().Len() != 0 {
		t.Errorf("Properties() len = %d, want 0", n.Properties().Len())
	}
}

func TestGraphDeleteNode(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add([]string{"Person"}, nil)
	id := n.ID()

	if err := g.Nodes.Delete(id); err != nil {
		t.Fatalf("DeleteNode() returned error: %v", err)
	}

	_, err := g.Nodes.Get(id)
	if !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Errorf("GetNode after delete: errors.Is(err, storepkg.ErrNodeNotFound) = false; err = %v", err)
	}
}

func TestGraphDeleteNodeCascadesRelationships(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add([]string{"Person"}, nil)
	nB, _ := g.Nodes.Add([]string{"Person"}, nil)
	nC, _ := g.Nodes.Add([]string{"Person"}, nil)

	// A → B
	rAB, _ := g.Rels.Add("KNOWS", nA, nB, nil)
	// C → A (incoming to A)
	rCA, _ := g.Rels.Add("FOLLOWS", nC, nA, nil)

	// Delete A — both relationships should be cascade-deleted.
	if err := g.Nodes.Delete(nA.ID()); err != nil {
		t.Fatalf("DeleteNode() returned error: %v", err)
	}

	// Verify node A is gone.
	if _, err := g.Nodes.Get(nA.ID()); !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Error("Node A should be deleted")
	}

	// Verify relationships are gone.
	if _, err := g.Rels.Get(rAB.ID()); !errors.Is(err, storepkg.ErrRelNotFound) {
		t.Error("Rel A→B should be cascade-deleted")
	}
	if _, err := g.Rels.Get(rCA.ID()); !errors.Is(err, storepkg.ErrRelNotFound) {
		t.Error("Rel C→A should be cascade-deleted")
	}

	// Verify B and C still exist.
	if _, err := g.Nodes.Get(nB.ID()); err != nil {
		t.Errorf("Node B should still exist: %v", err)
	}
	if _, err := g.Nodes.Get(nC.ID()); err != nil {
		t.Errorf("Node C should still exist: %v", err)
	}
}

func TestGraphDeleteNodeCascadeBothDirections(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add([]string{"X"}, nil)
	nB, _ := g.Nodes.Add([]string{"X"}, nil)

	// A → B and B → A (both directions).
	g.Rels.Add("OUT", nA, nB, nil)
	g.Rels.Add("IN", nB, nA, nil)

	rc, err := g.Rels.Count()
	if err != nil {
		t.Fatal(err)
	}
	if rc != 2 {
		t.Fatalf("RelationshipCount before delete = %d, want 2", rc)
	}

	// Delete A — both relationships should be gone.
	g.Nodes.Delete(nA.ID())
	rc, err = g.Rels.Count()
	if err != nil {
		t.Fatal(err)
	}
	if rc != 0 {
		t.Errorf("RelationshipCount after cascade delete = %d, want 0", rc)
	}
}

func TestGraphNodesByLabel(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Nodes.Add([]string{"Person"}, nil)
	g.Nodes.Add([]string{"Person"}, nil)
	g.Nodes.Add([]string{"Animal"}, nil)

	persons, err := g.Nodes.ByLabel("Person", storepkg.QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(persons) != 2 {
		t.Fatalf("NodesByLabel(\"Person\") = %d, want 2", len(persons))
	}

	animals, err := g.Nodes.ByLabel("Animal", storepkg.QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(animals) != 1 {
		t.Fatalf("NodesByLabel(\"Animal\") = %d, want 1", len(animals))
	}

	// Unregistered label.
	unknown, err := g.Nodes.ByLabel("Robot", storepkg.QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(unknown) != 0 {
		t.Fatalf("NodesByLabel(\"Robot\") = %d, want 0", len(unknown))
	}
}

func TestGraphNodeCount(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nc, err := g.Nodes.Count()
	if err != nil {
		t.Fatal(err)
	}
	if nc != 0 {
		t.Fatalf("empty NodeCount() = %d, want 0", nc)
	}
	g.Nodes.Add([]string{"X"}, nil)
	g.Nodes.Add([]string{"X"}, nil)
	nc, err = g.Nodes.Count()
	if err != nil {
		t.Fatal(err)
	}
	if nc != 2 {
		t.Fatalf("NodeCount() = %d, want 2", nc)
	}
}

func TestGraphDeleteNodeCascadeToleratesPreDeletedOutgoingRel(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nA, _ := g.Nodes.Add([]string{"X"}, nil)
	nB, _ := g.Nodes.Add([]string{"X"}, nil)

	r, _ := g.Rels.Add("R", nA, nB, nil)

	// Simulate a concurrent delete: remove the outgoing relationship before
	// cascade-deleting the node. Without the storepkg.ErrRelNotFound guard in the
	// outgoing loop, DeleteNode would return an error and leave the node stranded.
	if err := g.Rels.Delete(r.ID()); err != nil {
		t.Fatalf("pre-delete rel: %v", err)
	}

	// DeleteNode must succeed — the outgoing loop must tolerate storepkg.ErrRelNotFound.
	if err := g.Nodes.Delete(nA.ID()); err != nil {
		t.Fatalf("DeleteNode() after pre-deleted outgoing rel: %v", err)
	}

	// Node A should be gone.
	if _, err := g.Nodes.Get(nA.ID()); !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Error("Node A should be deleted")
	}
}

func TestGraphDeleteNodeSelfLoopCascade(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Validation: ValidationLimits{AllowSelfLoops: true}})
	nA, _ := g.Nodes.Add([]string{"X"}, nil)

	// Self-loop: A → A. Appears in both outgoing and incoming lists.
	_, err := g.Rels.Add("SELF", nA, nA, nil)
	if err != nil {
		t.Fatalf("AddRelationship self-loop: %v", err)
	}

	rc, err := g.Rels.Count()
	if err != nil {
		t.Fatal(err)
	}
	if rc != 1 {
		t.Fatalf("RelationshipCount before delete = %d, want 1", rc)
	}

	// DeleteNode must handle the self-loop appearing in both loops without error.
	if err := g.Nodes.Delete(nA.ID()); err != nil {
		t.Fatalf("DeleteNode() with self-loop: %v", err)
	}

	nc, err := g.Nodes.Count()
	if err != nil {
		t.Fatal(err)
	}
	if nc != 0 {
		t.Errorf("NodeCount after delete = %d, want 0", nc)
	}
	rc, err = g.Rels.Count()
	if err != nil {
		t.Fatal(err)
	}
	if rc != 0 {
		t.Errorf("RelationshipCount after delete = %d, want 0", rc)
	}
}

// ─── Badger integration ──────────────────────────────────────────────────────

// TestGraphWithBadgerInMemory removed — redundant CRUD already covered by MemoryStore tests.

func TestGraphDeleteNodeCascade_MemStore(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	nA, _ := g.Nodes.Add([]string{"Person"}, nil)
	nB, _ := g.Nodes.Add([]string{"Person"}, nil)
	nC, _ := g.Nodes.Add([]string{"Person"}, nil)

	g.Rels.Add("KNOWS", nA, nB, nil)
	g.Rels.Add("KNOWS", nA, nC, nil)
	g.Rels.Add("FOLLOWS", nB, nA, nil)

	rc, err := g.Rels.Count()
	if err != nil {
		t.Fatal(err)
	}
	if rc != 3 {
		t.Fatalf("RelationshipCount = %d, want 3", rc)
	}

	// Cascade delete nA: should remove all 3 relationships.
	if err := g.Nodes.Delete(nA.ID()); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	nc, err := g.Nodes.Count()
	if err != nil {
		t.Fatal(err)
	}
	if nc != 2 {
		t.Fatalf("NodeCount = %d, want 2", nc)
	}
	rc, err = g.Rels.Count()
	if err != nil {
		t.Fatal(err)
	}
	if rc != 0 {
		t.Fatalf("RelationshipCount = %d, want 0", rc)
	}
}

func TestGraphAddRelDeleteNodeConcurrency(t *testing.T) {
	t.Parallel()

	// Regression test: concurrent AddRelationship(→X) + DeleteNode(X) must not
	// produce a dangling edge. Entity locks at the Graph layer serialize these
	// operations on overlapping entities.
	const iterations = 100
	for i := range iterations {
		t.Run(fmt.Sprintf("iter_%d", i), func(t *testing.T) {
			t.Parallel()
			g, err := New(Config{Store: memory.New()})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer g.Close()

			// Create 3 nodes: A, B, target.
			nodeA, _ := g.Nodes.Add([]string{"A"}, nil)
			nodeB, _ := g.Nodes.Add([]string{"B"}, nil)
			target, _ := g.Nodes.Add([]string{"Target"}, nil)
			targetID := target.ID()

			// Race: AddRelationship(A→target) vs DeleteNode(target)
			done := make(chan struct{}, 2)
			var addErr error
			var delErr error

			go func() {
				defer func() { done <- struct{}{} }()
				_, addErr = g.Rels.Add("KNOWS", nodeA, target, nil)
			}()

			go func() {
				defer func() { done <- struct{}{} }()
				delErr = g.Nodes.Delete(targetID)
			}()

			<-done
			<-done

			// Exactly one of two valid outcomes:
			// 1. AddRel succeeded first → DeleteNode cascade removes it → graph clean
			// 2. DeleteNode succeeded first → AddRel fails with storepkg.ErrNodeNotFound
			if addErr != nil && delErr != nil {
				// Both failed — unexpected.
				t.Fatalf("both failed: addErr=%v, delErr=%v", addErr, delErr)
			}

			// Invariant: no dangling edges. Every rel's endpoints must exist.
			nc, _ := g.Nodes.Count()
			rc, _ := g.Rels.Count()

			if addErr != nil {
				// AddRel failed → target deleted first → only A, B remain, 0 rels.
				if nc != 2 {
					t.Errorf("addErr case: NodeCount=%d, want 2", nc)
				}
				if rc != 0 {
					t.Errorf("addErr case: RelCount=%d, want 0", rc)
				}
			} else {
				// AddRel succeeded → either target still exists with the rel,
				// or target was deleted (cascade removed the rel).
				if delErr != nil {
					// Delete failed → target exists, rel exists.
					if nc != 3 {
						t.Errorf("delErr case: NodeCount=%d, want 3", nc)
					}
					if rc != 1 {
						t.Errorf("delErr case: RelCount=%d, want 1", rc)
					}
				} else {
					// Both succeeded → AddRel then DeleteNode cascade.
					// Target is gone, rel is cascade-deleted.
					if nc != 2 {
						t.Errorf("both ok case: NodeCount=%d, want 2", nc)
					}
					if rc != 0 {
						t.Errorf("both ok case: RelCount=%d, want 0", rc)
					}
				}
			}

			// Final invariant: verify no rels reference non-existent nodes.
			// Check nodeB outgoing (should be empty).
			bID := nodeB.ID()
			outB, _ := g.Rels.Outgoing(bID, "")
			if len(outB) != 0 {
				t.Errorf("nodeB should have no outgoing rels, got %d", len(outB))
			}
		})
	}
}

func TestGraphUpdateNode(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice", "age": 30})
	id := n.ID()

	updated, err := g.Nodes.Update(id, map[string]any{"name": "Bob", "age": 31})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}
	v, ok := updated.GetProperty("name")
	if !ok || v != "Bob" {
		t.Fatalf("name = %v, want Bob", v)
	}
	v, ok = updated.GetProperty("age")
	if !ok || v != 31 {
		t.Fatalf("age = %v, want 31", v)
	}

	// Verify persisted.
	got, _ := g.Nodes.Get(id)
	v, _ = got.GetProperty("name")
	if v != "Bob" {
		t.Fatalf("persisted name = %v, want Bob", v)
	}
}

func TestGraphUpdateNodeAddProperty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.ID()

	_, err := g.Nodes.Update(id, map[string]any{"email": "alice@example.com"})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	got, _ := g.Nodes.Get(id)
	v, ok := got.GetProperty("email")
	if !ok || v != "alice@example.com" {
		t.Fatalf("email = %v, want alice@example.com", v)
	}
	// Original property still present.
	v, ok = got.GetProperty("name")
	if !ok || v != "Alice" {
		t.Fatalf("name = %v, want Alice", v)
	}
}

func TestGraphUpdateNodeDeleteProperty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice", "age": 30})
	id := n.ID()

	_, err := g.Nodes.Update(id, map[string]any{"age": nil})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	got, _ := g.Nodes.Get(id)
	_, ok := got.GetProperty("age")
	if ok {
		t.Fatal("age should be deleted")
	}
	v, ok := got.GetProperty("name")
	if !ok || v != "Alice" {
		t.Fatalf("name = %v, want Alice (unchanged)", v)
	}
}

func TestGraphUpdateNodeMixed(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice", "age": 30, "city": "NYC"})
	id := n.ID()

	// Add email, modify name, delete city — all in one call.
	_, err := g.Nodes.Update(id, map[string]any{
		"email": "alice@example.com",
		"name":  "Bob",
		"city":  nil,
	})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	got, _ := g.Nodes.Get(id)
	v, _ := got.GetProperty("name")
	if v != "Bob" {
		t.Fatalf("name = %v, want Bob", v)
	}
	v, ok := got.GetProperty("email")
	if !ok || v != "alice@example.com" {
		t.Fatalf("email = %v, want alice@example.com", v)
	}
	_, ok = got.GetProperty("city")
	if ok {
		t.Fatal("city should be deleted")
	}
	v, ok = got.GetProperty("age")
	if !ok || v != 30 {
		t.Fatalf("age = %v, want 30 (unchanged)", v)
	}
}

func TestGraphUpdateNodeNotFound(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	_, err := g.Nodes.Update(types.NodeID(999), map[string]any{"name": "Alice"})
	if !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("UpdateNode(nonexistent): errors.Is(err, storepkg.ErrNodeNotFound) = false; err = %v", err)
	}
}

func TestGraphUpdateNodeInvalidProperty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add([]string{"Person"}, nil)
	id := n.ID()

	_, err := g.Nodes.Update(id, map[string]any{"tkg_hack": "bad"})
	if !errors.Is(err, types.ErrReservedPrefix) {
		t.Fatalf("UpdateNode(tkg_ key): errors.Is(err, ErrReservedPrefix) = false; err = %v", err)
	}
}

func TestGraphUpdateNodeInvalidValue(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add([]string{"Person"}, nil)
	id := n.ID()

	type badStruct struct{ X int }
	_, err := g.Nodes.Update(id, map[string]any{"bad": badStruct{42}})
	if !errors.Is(err, types.ErrUnsupportedValueType) {
		t.Fatalf("UpdateNode(bad value): errors.Is(err, ErrUnsupportedValueType) = false; err = %v", err)
	}
}

func TestGraphUpdateNodeVersionIncrement(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.ID()

	if n.Version() != 0 {
		t.Fatalf("initial version = %d, want 0", n.Version())
	}

	updated1, _ := g.Nodes.Update(id, map[string]any{"name": "Bob"})
	if updated1.Version() != 1 {
		t.Fatalf("version after first update = %d, want 1", updated1.Version())
	}

	updated2, _ := g.Nodes.Update(id, map[string]any{"name": "Charlie"})
	if updated2.Version() != 2 {
		t.Fatalf("version after second update = %d, want 2", updated2.Version())
	}
}

func TestGraphUpdateNodeEmptyUpdates(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.ID()

	got, err := g.Nodes.Update(id, map[string]any{})
	if err != nil {
		t.Fatalf("UpdateNode(empty): %v", err)
	}
	if got.Version() != 0 {
		t.Fatalf("version after empty update = %d, want 0 (no bump)", got.Version())
	}
	v, _ := got.GetProperty("name")
	if v != "Alice" {
		t.Fatalf("name = %v, want Alice (unchanged)", v)
	}
}

func TestGraphUpdateNodeConcurrentSameNode(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"counter": 0})
	id := n.ID()

	const workers = 50
	var wg sync.WaitGroup
	wg.Add(workers)

	for i := range workers {
		go func(val int) {
			defer wg.Done()
			// Each goroutine reads and increments a different property to avoid lost updates.
			g.Nodes.Update(id, map[string]any{fmt.Sprintf("worker_%d", val): val})
		}(i)
	}
	wg.Wait()

	got, _ := g.Nodes.Get(id)
	// All 50 properties should be present (serialized updates, no lost writes).
	for i := range workers {
		key := fmt.Sprintf("worker_%d", i)
		v, ok := got.GetProperty(key)
		if !ok {
			t.Errorf("property %s missing (lost update)", key)
		}
		if v != i {
			t.Errorf("property %s = %v, want %d", key, v, i)
		}
	}
	// Version should be workers (one bump per update).
	if got.Version() != uint32(workers) {
		t.Errorf("version = %d, want %d", got.Version(), workers)
	}
}

func TestGraphSetNodeProperty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add([]string{"Person"}, nil)
	id := n.ID()

	if err := g.Nodes.SetProperty(id, "name", "Alice"); err != nil {
		t.Fatalf("SetNodeProperty: %v", err)
	}

	got, _ := g.Nodes.Get(id)
	v, ok := got.GetProperty("name")
	if !ok || v != "Alice" {
		t.Fatalf("name = %v, want Alice", v)
	}
}

func TestGraphDeleteNodeProperty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.ID()

	if err := g.Nodes.DeleteProperty(id, "name"); err != nil {
		t.Fatalf("DeleteNodeProperty: %v", err)
	}

	got, _ := g.Nodes.Get(id)
	_, ok := got.GetProperty("name")
	if ok {
		t.Fatal("name should be deleted")
	}
}

func TestGraphUpdateNodeWithMemStore(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.ID()

	updated, err := g.Nodes.Update(id, map[string]any{"name": "Bob", "age": 30})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}
	v, _ := updated.GetProperty("name")
	if v != "Bob" {
		t.Fatalf("name = %v, want Bob", v)
	}

	got, _ := g.Nodes.Get(id)
	v, _ = got.GetProperty("name")
	if v != "Bob" {
		t.Fatalf("persisted name = %v, want Bob", v)
	}
	v, ok := got.GetProperty("age")
	if !ok || v != 30 {
		t.Fatalf("age = %v, want 30", v)
	}
}

func TestGraphConcurrentDeleteNodeOverlappingRels(t *testing.T) {
	t.Parallel()

	const iterations = 50
	for i := range iterations {
		t.Run(fmt.Sprintf("iter_%d", i), func(t *testing.T) {
			t.Parallel()
			g, err := New(Config{Store: memory.New()})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer g.Close()

			nA, _ := g.Nodes.Add([]string{"A"}, nil)
			nB, _ := g.Nodes.Add([]string{"B"}, nil)
			nC, _ := g.Nodes.Add([]string{"C"}, nil)

			// A→B, B→C, A→C — deleting A and B concurrently overlaps on A→B.
			g.Rels.Add("R", nA, nB, nil)
			g.Rels.Add("R", nB, nC, nil)
			g.Rels.Add("R", nA, nC, nil)

			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				g.Nodes.Delete(nA.ID())
			}()
			go func() {
				defer wg.Done()
				g.Nodes.Delete(nB.ID())
			}()
			wg.Wait()

			nc, _ := g.Nodes.Count()
			if nc != 1 {
				t.Fatalf("NodeCount = %d, want 1 (only C)", nc)
			}
			rc, _ := g.Rels.Count()
			if rc != 0 {
				t.Fatalf("RelationshipCount = %d, want 0", rc)
			}
		})
	}
}

func TestGraphAllNodes(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Bob"})
	g.Nodes.Add([]string{"City"}, map[string]any{"name": "Vienna"})

	got, err := g.Nodes.All(storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("AllNodes() returned error: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("AllNodes() = %d nodes, want 3", len(got))
	}
}

func TestGraphAllNodesEmpty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	got, err := g.Nodes.All(storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("AllNodes() returned error: %v", err)
	}
	if got != nil {
		t.Fatalf("AllNodes() on empty graph = %v, want nil", got)
	}
}

func TestGraphGetNodesByIDs(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	n1, _ := g.Nodes.Add([]string{"Person"}, nil)
	n2, _ := g.Nodes.Add([]string{"Person"}, nil)
	g.Nodes.Add([]string{"Person"}, nil) // n3 — not requested

	ids := []types.NodeID{
		n1.ID(),
		types.NodeID(999), // missing
		n2.ID(),
	}

	got, err := g.Nodes.GetByIDs(ids)
	if err != nil {
		t.Fatalf("GetNodesByIDs() returned error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("GetNodesByIDs() = %d nodes, want 2", len(got))
	}
}

func TestGraphGetNodesByIDsEmpty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	got, err := g.Nodes.GetByIDs(nil)
	if err != nil {
		t.Fatalf("GetNodesByIDs(nil) returned error: %v", err)
	}
	if got != nil {
		t.Fatalf("GetNodesByIDs(nil) = %v, want nil", got)
	}
}

func TestNodeCountByLabel_Empty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	// Register label but add no nodes.
	g.Resolve.GetOrCreateLabel("Person")

	count, err := g.Nodes.CountByLabel("Person")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 0 {
		t.Fatalf("NodeCountByLabel = %d, want 0", count)
	}
}

func TestNodeCountByLabel_UnregisteredLabel(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	count, err := g.Nodes.CountByLabel("NeverRegistered")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 0 {
		t.Fatalf("NodeCountByLabel = %d, want 0", count)
	}
}

func TestNodeCountByLabel_SingleNode(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})

	count, err := g.Nodes.CountByLabel("Person")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 1 {
		t.Fatalf("NodeCountByLabel = %d, want 1", count)
	}
}

func TestNodeCountByLabel_MultipleNodes(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Bob"})
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Charlie"})

	count, err := g.Nodes.CountByLabel("Person")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 3 {
		t.Fatalf("NodeCountByLabel = %d, want 3", count)
	}
}

func TestNodeCountByLabel_AfterDelete(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	n1, _ := g.Nodes.Add([]string{"Person"}, nil)
	g.Nodes.Add([]string{"Person"}, nil)

	g.Nodes.Delete(n1.ID())

	count, err := g.Nodes.CountByLabel("Person")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 1 {
		t.Fatalf("NodeCountByLabel after delete = %d, want 1", count)
	}
}

func TestNodeCountByLabel_BatchAdd(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})

	batch := NewBatchBuilder(g)
	batch.AddNode([]string{"Animal"}, nil)
	batch.AddNode([]string{"Animal"}, nil)
	batch.AddNode([]string{"Animal", "Pet"}, nil)
	batch.Execute()

	c, err := g.Nodes.CountByLabel("Animal")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c != 3 {
		t.Fatalf("Animal count = %d, want 3", c)
	}
	cp, _ := g.Nodes.CountByLabel("Pet")
	if cp != 1 {
		t.Fatalf("Pet count = %d, want 1", cp)
	}
}

func TestGraphAddNode_DuplicateLabelsHashVerifies(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	// Pass duplicate labels — NewNode deduplicates internally.
	n, err := g.Nodes.Add([]string{"Person", "Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	// Hash chain must verify — the hash was computed from canonical (deduplicated) labels.
	valid, err := g.Hash.VerifyNodeChain(n.ID())
	if err != nil {
		t.Fatalf("VerifyNodeChain: %v", err)
	}
	if !valid {
		t.Fatal("hash chain verification failed — hash was computed from raw labels, not canonical")
	}
}

func TestGraphAddNode_DuplicateLabelsOnlyOneStored(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	n, err := g.Nodes.Add([]string{"Person", "Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	labels := g.Nodes.Labels(n)
	if len(labels) != 1 {
		t.Fatalf("expected 1 label after dedup, got %d: %v", len(labels), labels)
	}
	if labels[0] != "Person" {
		t.Fatalf("expected 'Person', got %q", labels[0])
	}
}

// ─── CompareAndSetProperty ───────────────────────────────────────────────────
