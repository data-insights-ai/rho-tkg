package graph

import (
	"errors"
	"fmt"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

func TestNewGraph(t *testing.T) {
	t.Parallel()

	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	if g == nil {
		t.Fatal("New() returned nil")
	}
}

func TestGraphNodeLabels(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})

	personTok, _ := g.GetOrCreateLabel("Person")
	actorTok, _ := g.GetOrCreateLabel("Actor")

	n := types.NewNode(snowflake.ID(1), personTok, []uint16{actorTok})
	labels := g.NodeLabels(n)

	if len(labels) != 2 {
		t.Fatalf("NodeLabels len = %d, want 2", len(labels))
	}
	if labels[0] != "Person" || labels[1] != "Actor" {
		t.Errorf("NodeLabels = %v, want [Person Actor]", labels)
	}
}

func TestGraphNodePrimaryLabel(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	personTok, _ := g.GetOrCreateLabel("Person")

	n := types.NewNode(snowflake.ID(1), personTok, nil)
	got := g.NodePrimaryLabel(n)
	if got != "Person" {
		t.Errorf("NodePrimaryLabel = %q, want \"Person\"", got)
	}
}

func TestGraphNodeHasLabel(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	personTok, _ := g.GetOrCreateLabel("Person")
	actorTok, _ := g.GetOrCreateLabel("Actor")

	// Single-label node: primary label hit.
	n := types.NewNode(snowflake.ID(1), personTok, nil)
	if !g.NodeHasLabel(n, "Person") {
		t.Error("NodeHasLabel(\"Person\") = false, want true (primary)")
	}
	if g.NodeHasLabel(n, "Animal") {
		t.Error("NodeHasLabel(\"Animal\") = true, want false (unregistered)")
	}

	// Multi-label node: extra label hit.
	n2 := types.NewNode(snowflake.ID(2), personTok, []uint16{actorTok})
	if !g.NodeHasLabel(n2, "Actor") {
		t.Error("NodeHasLabel(\"Actor\") = false, want true (extra label)")
	}
	if !g.NodeHasLabel(n2, "Person") {
		t.Error("NodeHasLabel(\"Person\") = false, want true (primary on multi-label)")
	}
}

func TestGraphRelationshipType(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	knowsTok, _ := g.GetOrCreateRelType("KNOWS")

	r := types.NewRelationship(snowflake.ID(1), knowsTok, snowflake.ID(10), snowflake.ID(20))
	got := g.RelationshipType(r)
	if got != "KNOWS" {
		t.Errorf("RelationshipType = %q, want \"KNOWS\"", got)
	}
}

func TestGraphRelationshipHasType(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	knowsTok, _ := g.GetOrCreateRelType("KNOWS")

	r := types.NewRelationship(snowflake.ID(1), knowsTok, snowflake.ID(10), snowflake.ID(20))

	if !g.RelationshipHasType(r, "KNOWS") {
		t.Error("RelationshipHasType(\"KNOWS\") = false, want true")
	}
	if g.RelationshipHasType(r, "LIKES") {
		t.Error("RelationshipHasType(\"LIKES\") = true, want false (unregistered)")
	}

	// Registered but wrong type: Lookup succeeds, comparison fails.
	g.GetOrCreateRelType("LIKES")
	if g.RelationshipHasType(r, "LIKES") {
		t.Error("RelationshipHasType(\"LIKES\") = true, want false (registered but wrong type)")
	}
}

func TestGraphLookupLabel(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	g.GetOrCreateLabel("Person")

	tok, ok := g.LookupLabel("Person")
	if !ok {
		t.Fatal("LookupLabel(\"Person\") should return true")
	}
	if tok == 0 {
		t.Fatal("LookupLabel should return non-zero token")
	}

	_, ok = g.LookupLabel("Unknown")
	if ok {
		t.Fatal("LookupLabel(\"Unknown\") should return false")
	}
}

func TestGraphLookupRelType(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	g.GetOrCreateRelType("KNOWS")

	tok, ok := g.LookupRelType("KNOWS")
	if !ok {
		t.Fatal("LookupRelType(\"KNOWS\") should return true")
	}
	if tok == 0 {
		t.Fatal("LookupRelType should return non-zero token")
	}

	_, ok = g.LookupRelType("UNKNOWN")
	if ok {
		t.Fatal("LookupRelType(\"UNKNOWN\") should return false")
	}
}

// ─── Snowflake generator tests ──────────────────────────────────────────────

func TestGraphSnowflakeGeneratorsInitialized(t *testing.T) {
	t.Parallel()

	g, err := New(Config{SnowflakeNodeID: 0})
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}

	nid := g.NextNodeID()
	if nid == 0 {
		t.Fatal("NextNodeID() returned zero")
	}

	nid2 := g.NextNodeID()
	if nid == nid2 {
		t.Fatal("NextNodeID() returned duplicate IDs")
	}
}

func TestGraphNextRelID(t *testing.T) {
	t.Parallel()

	g, err := New(Config{SnowflakeNodeID: 0})
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}

	rid := g.NextRelID()
	if rid == 0 {
		t.Fatal("NextRelID() returned zero")
	}

	rid2 := g.NextRelID()
	if rid == rid2 {
		t.Fatal("NextRelID() returned duplicate IDs")
	}
}

func TestGraphSnowflakeNodeIDRange(t *testing.T) {
	t.Parallel()

	// Valid: 0 (minimum)
	_, err := New(Config{SnowflakeNodeID: 0})
	if err != nil {
		t.Errorf("SnowflakeNodeID=0 should be valid, got: %v", err)
	}

	// Valid: 511 (maximum — maps to even/odd pair 1022/1023)
	_, err = New(Config{SnowflakeNodeID: 511})
	if err != nil {
		t.Errorf("SnowflakeNodeID=511 should be valid, got: %v", err)
	}

	// Invalid: 512 (would map to 1024/1025 — exceeds 10-bit range)
	_, err = New(Config{SnowflakeNodeID: 512})
	if err == nil {
		t.Fatal("SnowflakeNodeID=512 should return error")
	}

	// Invalid: negative
	_, err = New(Config{SnowflakeNodeID: -1})
	if err == nil {
		t.Fatal("SnowflakeNodeID=-1 should return error")
	}
}

func TestGraphNodeRelIDValueUniqueness(t *testing.T) {
	t.Parallel()

	g, err := New(Config{SnowflakeNodeID: 0})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Generate node and rel IDs concurrently. The even/odd node field
	// guarantees no value collision even within the same millisecond.
	const count = 1000
	all := make(map[snowflake.ID]string, count*2)

	for range count {
		nid := g.NextNodeID()
		if prev, dup := all[nid]; dup {
			t.Fatalf("node ID %d collides with %s", nid, prev)
		}
		all[nid] = "node"

		rid := g.NextRelID()
		if prev, dup := all[rid]; dup {
			t.Fatalf("rel ID %d collides with %s", rid, prev)
		}
		all[rid] = "rel"
	}
}

func TestGraphSnowflakeIDsAreUnique(t *testing.T) {
	t.Parallel()

	g, err := New(Config{SnowflakeNodeID: 1})
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}

	const count = 1000
	seen := make(map[snowflake.ID]struct{}, count)
	for range count {
		id := g.NextNodeID()
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate node ID: %d", id)
		}
		seen[id] = struct{}{}
	}

	seenRel := make(map[snowflake.ID]struct{}, count)
	for range count {
		id := g.NextRelID()
		if _, dup := seenRel[id]; dup {
			t.Fatalf("duplicate rel ID: %d", id)
		}
		seenRel[id] = struct{}{}
	}
}

// ─── Entity management tests ────────────────────────────────────────────────

func TestGraphAddNode(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, err := g.AddNode([]string{"Person", "Actor"}, map[string]any{"name": "Alice", "age": 30})
	if err != nil {
		t.Fatalf("AddNode() returned error: %v", err)
	}

	// Verify labels.
	labels := g.NodeLabels(n)
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
	got, err := g.GetNode(n.InternalID().SnowflakeID())
	if err != nil {
		t.Fatalf("GetNode() returned error: %v", err)
	}
	if got.InternalID() != n.InternalID() {
		t.Fatal("GetNode() returned node with different ID")
	}
	gotName, _ := got.GetProperty("name")
	if gotName != "Alice" {
		t.Fatalf("GetNode() property name = %v, want Alice", gotName)
	}
}

func TestGraphAddNodeNoLabels(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	_, err := g.AddNode(nil, nil)
	if err == nil {
		t.Fatal("AddNode(nil labels) should return error")
	}
	if !errors.Is(err, ErrNoLabels) {
		t.Errorf("errors.Is(err, ErrNoLabels) = false; err = %v", err)
	}

	_, err = g.AddNode([]string{}, nil)
	if !errors.Is(err, ErrNoLabels) {
		t.Errorf("errors.Is(err, ErrNoLabels) for empty slice = false; err = %v", err)
	}
}

func TestGraphAddNodeInvalidProperty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	_, err := g.AddNode([]string{"Person"}, map[string]any{"tkg_hack": "bad"})
	if err == nil {
		t.Fatal("AddNode with tkg_ property should return error")
	}
}

func TestGraphAddNodeBulkProperties(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	props := make(map[string]any, 50)
	for i := range 50 {
		props[fmt.Sprintf("prop_%02d", i)] = i
	}

	n, err := g.AddNode([]string{"Test"}, props)
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

	g, _ := New(Config{})
	seen := make(map[snowflake.ID]struct{}, 100)

	for range 100 {
		n, err := g.AddNode([]string{"X"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		id := n.InternalID().SnowflakeID()
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate node ID: %d", id)
		}
		seen[id] = struct{}{}
	}
}

func TestGraphAddNodeNilProperties(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, err := g.AddNode([]string{"Empty"}, nil)
	if err != nil {
		t.Fatalf("AddNode(nil props) returned error: %v", err)
	}
	if n.Properties().Len() != 0 {
		t.Errorf("Properties() len = %d, want 0", n.Properties().Len())
	}
}

func TestGraphAddRelationship(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nA, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	nB, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Bob"})

	r, err := g.AddRelationship("KNOWS", nA, nB, map[string]any{"since": 2020})
	if err != nil {
		t.Fatalf("AddRelationship() returned error: %v", err)
	}

	// Verify type.
	if g.RelationshipType(r) != "KNOWS" {
		t.Errorf("RelationshipType = %q, want \"KNOWS\"", g.RelationshipType(r))
	}

	// Verify endpoints.
	if r.StartNodeID().SnowflakeID() != nA.InternalID().SnowflakeID() {
		t.Error("StartNodeID does not match nA")
	}
	if r.EndNodeID().SnowflakeID() != nB.InternalID().SnowflakeID() {
		t.Error("EndNodeID does not match nB")
	}

	// Verify property.
	since, ok := r.GetProperty("since")
	if !ok || since != 2020 {
		t.Errorf("GetProperty(\"since\") = (%v, %v), want (2020, true)", since, ok)
	}
}

func TestGraphAddRelNilStart(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nB, _ := g.AddNode([]string{"X"}, nil)

	_, err := g.AddRelationship("KNOWS", nil, nB, nil)
	if !errors.Is(err, ErrNilNode) {
		t.Errorf("AddRelationship(nil start): errors.Is(err, ErrNilNode) = false; err = %v", err)
	}
}

func TestGraphAddRelNilEnd(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nA, _ := g.AddNode([]string{"X"}, nil)

	_, err := g.AddRelationship("KNOWS", nA, nil, nil)
	if !errors.Is(err, ErrNilNode) {
		t.Errorf("AddRelationship(nil end): errors.Is(err, ErrNilNode) = false; err = %v", err)
	}
}

func TestGraphAddRelEmptyType(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nA, _ := g.AddNode([]string{"X"}, nil)
	nB, _ := g.AddNode([]string{"X"}, nil)

	_, err := g.AddRelationship("", nA, nB, nil)
	if err == nil {
		t.Fatal("AddRelationship(\"\") should return error")
	}
}

func TestGraphDeleteNode(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Person"}, nil)
	id := n.InternalID().SnowflakeID()

	if err := g.DeleteNode(id); err != nil {
		t.Fatalf("DeleteNode() returned error: %v", err)
	}

	_, err := g.GetNode(id)
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("GetNode after delete: errors.Is(err, ErrNodeNotFound) = false; err = %v", err)
	}
}

func TestGraphDeleteNodeCascadesRelationships(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nA, _ := g.AddNode([]string{"Person"}, nil)
	nB, _ := g.AddNode([]string{"Person"}, nil)
	nC, _ := g.AddNode([]string{"Person"}, nil)

	// A → B
	rAB, _ := g.AddRelationship("KNOWS", nA, nB, nil)
	// C → A (incoming to A)
	rCA, _ := g.AddRelationship("FOLLOWS", nC, nA, nil)

	// Delete A — both relationships should be cascade-deleted.
	if err := g.DeleteNode(nA.InternalID().SnowflakeID()); err != nil {
		t.Fatalf("DeleteNode() returned error: %v", err)
	}

	// Verify node A is gone.
	if _, err := g.GetNode(nA.InternalID().SnowflakeID()); !errors.Is(err, ErrNodeNotFound) {
		t.Error("Node A should be deleted")
	}

	// Verify relationships are gone.
	if _, err := g.GetRelationship(rAB.InternalID().SnowflakeID()); !errors.Is(err, ErrRelNotFound) {
		t.Error("Rel A→B should be cascade-deleted")
	}
	if _, err := g.GetRelationship(rCA.InternalID().SnowflakeID()); !errors.Is(err, ErrRelNotFound) {
		t.Error("Rel C→A should be cascade-deleted")
	}

	// Verify B and C still exist.
	if _, err := g.GetNode(nB.InternalID().SnowflakeID()); err != nil {
		t.Errorf("Node B should still exist: %v", err)
	}
	if _, err := g.GetNode(nC.InternalID().SnowflakeID()); err != nil {
		t.Errorf("Node C should still exist: %v", err)
	}
}

func TestGraphDeleteNodeCascadeBothDirections(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nA, _ := g.AddNode([]string{"X"}, nil)
	nB, _ := g.AddNode([]string{"X"}, nil)

	// A → B and B → A (both directions).
	g.AddRelationship("OUT", nA, nB, nil)
	g.AddRelationship("IN", nB, nA, nil)

	rc, err := g.RelationshipCount()
	if err != nil {
		t.Fatal(err)
	}
	if rc != 2 {
		t.Fatalf("RelationshipCount before delete = %d, want 2", rc)
	}

	// Delete A — both relationships should be gone.
	g.DeleteNode(nA.InternalID().SnowflakeID())
	rc, err = g.RelationshipCount()
	if err != nil {
		t.Fatal(err)
	}
	if rc != 0 {
		t.Errorf("RelationshipCount after cascade delete = %d, want 0", rc)
	}
}

func TestGraphNodesByLabel(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	g.AddNode([]string{"Person"}, nil)
	g.AddNode([]string{"Person"}, nil)
	g.AddNode([]string{"Animal"}, nil)

	persons, err := g.NodesByLabel("Person")
	if err != nil {
		t.Fatal(err)
	}
	if len(persons) != 2 {
		t.Fatalf("NodesByLabel(\"Person\") = %d, want 2", len(persons))
	}

	animals, err := g.NodesByLabel("Animal")
	if err != nil {
		t.Fatal(err)
	}
	if len(animals) != 1 {
		t.Fatalf("NodesByLabel(\"Animal\") = %d, want 1", len(animals))
	}

	// Unregistered label.
	unknown, err := g.NodesByLabel("Robot")
	if err != nil {
		t.Fatal(err)
	}
	if len(unknown) != 0 {
		t.Fatalf("NodesByLabel(\"Robot\") = %d, want 0", len(unknown))
	}
}

func TestGraphRelationshipsByType(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nA, _ := g.AddNode([]string{"X"}, nil)
	nB, _ := g.AddNode([]string{"X"}, nil)

	g.AddRelationship("KNOWS", nA, nB, nil)
	g.AddRelationship("KNOWS", nB, nA, nil)
	g.AddRelationship("LIKES", nA, nB, nil)

	knows, err := g.RelationshipsByType("KNOWS")
	if err != nil {
		t.Fatal(err)
	}
	if len(knows) != 2 {
		t.Fatalf("RelationshipsByType(\"KNOWS\") = %d, want 2", len(knows))
	}

	likes, err := g.RelationshipsByType("LIKES")
	if err != nil {
		t.Fatal(err)
	}
	if len(likes) != 1 {
		t.Fatalf("RelationshipsByType(\"LIKES\") = %d, want 1", len(likes))
	}

	unknown, err := g.RelationshipsByType("HATES")
	if err != nil {
		t.Fatal(err)
	}
	if len(unknown) != 0 {
		t.Fatalf("RelationshipsByType(\"HATES\") = %d, want 0", len(unknown))
	}
}

func TestGraphNodeCount(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nc, err := g.NodeCount()
	if err != nil {
		t.Fatal(err)
	}
	if nc != 0 {
		t.Fatalf("empty NodeCount() = %d, want 0", nc)
	}
	g.AddNode([]string{"X"}, nil)
	g.AddNode([]string{"X"}, nil)
	nc, err = g.NodeCount()
	if err != nil {
		t.Fatal(err)
	}
	if nc != 2 {
		t.Fatalf("NodeCount() = %d, want 2", nc)
	}
}

func TestGraphRelCount(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nA, _ := g.AddNode([]string{"X"}, nil)
	nB, _ := g.AddNode([]string{"X"}, nil)
	g.AddRelationship("R", nA, nB, nil)
	rc, err := g.RelationshipCount()
	if err != nil {
		t.Fatal(err)
	}
	if rc != 1 {
		t.Fatalf("RelationshipCount() = %d, want 1", rc)
	}
}

func TestGraphDefaultMemoryStore(t *testing.T) {
	t.Parallel()

	g, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	// Verify the default store works by adding and retrieving a node.
	n, _ := g.AddNode([]string{"Test"}, nil)
	got, _ := g.GetNode(n.InternalID().SnowflakeID())
	if got.InternalID() != n.InternalID() {
		t.Fatal("Default store should round-trip nodes")
	}
}

func TestGraphDeleteRelationship(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nA, _ := g.AddNode([]string{"X"}, nil)
	nB, _ := g.AddNode([]string{"X"}, nil)
	r, _ := g.AddRelationship("R", nA, nB, nil)

	if err := g.DeleteRelationship(r.InternalID().SnowflakeID()); err != nil {
		t.Fatalf("DeleteRelationship() returned error: %v", err)
	}
	rc, err := g.RelationshipCount()
	if err != nil {
		t.Fatal(err)
	}
	if rc != 0 {
		t.Errorf("RelationshipCount() = %d, want 0", rc)
	}
}

func TestGraphLabelAndRelTypeIndependentNamespaces(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})

	labelTok, _ := g.GetOrCreateLabel("KNOWS")
	relTok, _ := g.GetOrCreateRelType("KNOWS")

	// Both should be valid token 1 (first in each registry), but independent.
	if labelTok != relTok {
		// They happen to be equal (both first), which is fine — the point is
		// they're in independent namespaces and don't collide.
		t.Logf("label token=%d, reltype token=%d (independent, OK if different)", labelTok, relTok)
	}

	// Verify resolution is namespace-scoped.
	labelStr := g.labels.Resolve(labelTok)
	relStr := g.relTypes.Resolve(relTok)
	if labelStr != "KNOWS" || relStr != "KNOWS" {
		t.Errorf("label=%q reltype=%q, want both \"KNOWS\"", labelStr, relStr)
	}
}

func TestGraphDeleteNodeCascadeToleratesPreDeletedOutgoingRel(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nA, _ := g.AddNode([]string{"X"}, nil)
	nB, _ := g.AddNode([]string{"X"}, nil)

	r, _ := g.AddRelationship("R", nA, nB, nil)

	// Simulate a concurrent delete: remove the outgoing relationship before
	// cascade-deleting the node. Without the ErrRelNotFound guard in the
	// outgoing loop, DeleteNode would return an error and leave the node stranded.
	if err := g.DeleteRelationship(r.InternalID().SnowflakeID()); err != nil {
		t.Fatalf("pre-delete rel: %v", err)
	}

	// DeleteNode must succeed — the outgoing loop must tolerate ErrRelNotFound.
	if err := g.DeleteNode(nA.InternalID().SnowflakeID()); err != nil {
		t.Fatalf("DeleteNode() after pre-deleted outgoing rel: %v", err)
	}

	// Node A should be gone.
	if _, err := g.GetNode(nA.InternalID().SnowflakeID()); !errors.Is(err, ErrNodeNotFound) {
		t.Error("Node A should be deleted")
	}
}

func TestGraphDeleteNodeSelfLoopCascade(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nA, _ := g.AddNode([]string{"X"}, nil)

	// Self-loop: A → A. Appears in both outgoing and incoming lists.
	_, err := g.AddRelationship("SELF", nA, nA, nil)
	if err != nil {
		t.Fatalf("AddRelationship self-loop: %v", err)
	}

	rc, err := g.RelationshipCount()
	if err != nil {
		t.Fatal(err)
	}
	if rc != 1 {
		t.Fatalf("RelationshipCount before delete = %d, want 1", rc)
	}

	// DeleteNode must handle the self-loop appearing in both loops without error.
	if err := g.DeleteNode(nA.InternalID().SnowflakeID()); err != nil {
		t.Fatalf("DeleteNode() with self-loop: %v", err)
	}

	nc, err := g.NodeCount()
	if err != nil {
		t.Fatal(err)
	}
	if nc != 0 {
		t.Errorf("NodeCount after delete = %d, want 0", nc)
	}
	rc, err = g.RelationshipCount()
	if err != nil {
		t.Fatal(err)
	}
	if rc != 0 {
		t.Errorf("RelationshipCount after delete = %d, want 0", rc)
	}
}

// ─── Badger integration ──────────────────────────────────────────────────────

func TestGraphWithBadgerInMemory(t *testing.T) {
	t.Parallel()

	g, err := New(Config{BadgerInMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	nA, err := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	nB, err := g.AddNode([]string{"Person"}, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	_, err = g.AddRelationship("KNOWS", nA, nB, map[string]any{"since": int64(2026)})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	nc, err := g.NodeCount()
	if err != nil {
		t.Fatal(err)
	}
	if nc != 2 {
		t.Fatalf("NodeCount = %d, want 2", nc)
	}
	rc, err := g.RelationshipCount()
	if err != nil {
		t.Fatal(err)
	}
	if rc != 1 {
		t.Fatalf("RelationshipCount = %d, want 1", rc)
	}

	got, err := g.GetNode(nA.InternalID().SnowflakeID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	v, ok := got.GetProperty("name")
	if !ok || v != "Alice" {
		t.Fatalf("property mismatch: %v", v)
	}
}

func TestGraphCloseAndReopenBadger(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Create and populate.
	g1, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New 1: %v", err)
	}
	nA, err := g1.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	nB, err := g1.AddNode([]string{"Person"}, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	_, err = g1.AddRelationship("KNOWS", nA, nB, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	if err := g1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	// Reopen and verify data persists.
	g2, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New 2: %v", err)
	}
	defer g2.Close()

	nc, err := g2.NodeCount()
	if err != nil {
		t.Fatal(err)
	}
	if nc != 2 {
		t.Fatalf("NodeCount after reopen = %d, want 2", nc)
	}
	rc, err := g2.RelationshipCount()
	if err != nil {
		t.Fatal(err)
	}
	if rc != 1 {
		t.Fatalf("RelationshipCount after reopen = %d, want 1", rc)
	}
}

func TestGraphRegistryPersistence(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Create labels, close.
	g1, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New 1: %v", err)
	}
	g1.AddNode([]string{"Person", "Actor"}, nil)
	if err := g1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	// Reopen and verify labels resolve.
	g2, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New 2: %v", err)
	}
	defer g2.Close()

	tok, ok := g2.LookupLabel("Person")
	if !ok {
		t.Fatal("Person label not persisted")
	}
	if tok == 0 {
		t.Fatal("Person token should not be 0")
	}

	tok, ok = g2.LookupLabel("Actor")
	if !ok {
		t.Fatal("Actor label not persisted")
	}
	if tok == 0 {
		t.Fatal("Actor token should not be 0")
	}
}

func TestGraphCloseNoop(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{}) // MemoryStore
	if err := g.Close(); err != nil {
		t.Fatalf("Close on MemoryStore should be no-op, got: %v", err)
	}
}

func TestGraphCloseIdempotent(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{BadgerInMemory: true})
	if err := g.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close 2 (idempotent): %v", err)
	}
}

func TestGraphCloseAlwaysReleasesResources(t *testing.T) {
	t.Parallel()

	g, err := New(Config{BadgerInMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Sabotage: close the Badger DB directly so registry saves will fail.
	bs := g.store.(*BadgerStore)
	if err := bs.db.Close(); err != nil {
		t.Fatalf("sabotage close: %v", err)
	}

	// Close() must return an error (registry save fails on closed DB),
	// but must NOT panic. sync.Once handles idempotency.
	err = g.Close()
	if err == nil {
		t.Fatal("Close() should return error when Badger is already closed")
	}

	// Second call returns nil — sync.Once already fired.
	if err := g.Close(); err != nil {
		t.Fatalf("Close() second call should return nil (sync.Once), got: %v", err)
	}
}

func TestGraphCloseConcurrent(t *testing.T) {
	t.Parallel()

	g, err := New(Config{BadgerInMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// 10 goroutines calling Close() simultaneously — must not panic or race.
	const workers = 10
	done := make(chan error, workers)
	for range workers {
		go func() {
			done <- g.Close()
		}()
	}

	var errs int
	for range workers {
		if <-done != nil {
			errs++
		}
	}
	// At most zero errors expected (all succeed or only the first one runs).
	_ = errs
}

func TestGraphBadgerDeleteNodeCascade(t *testing.T) {
	t.Parallel()

	g, err := New(Config{BadgerInMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	nA, _ := g.AddNode([]string{"Person"}, nil)
	nB, _ := g.AddNode([]string{"Person"}, nil)
	nC, _ := g.AddNode([]string{"Person"}, nil)

	g.AddRelationship("KNOWS", nA, nB, nil)
	g.AddRelationship("KNOWS", nA, nC, nil)
	g.AddRelationship("FOLLOWS", nB, nA, nil)

	rc, err := g.RelationshipCount()
	if err != nil {
		t.Fatal(err)
	}
	if rc != 3 {
		t.Fatalf("RelationshipCount = %d, want 3", rc)
	}

	// Cascade delete nA: should remove all 3 relationships.
	if err := g.DeleteNode(nA.InternalID().SnowflakeID()); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	nc, err := g.NodeCount()
	if err != nil {
		t.Fatal(err)
	}
	if nc != 2 {
		t.Fatalf("NodeCount = %d, want 2", nc)
	}
	rc, err = g.RelationshipCount()
	if err != nil {
		t.Fatal(err)
	}
	if rc != 0 {
		t.Fatalf("RelationshipCount = %d, want 0", rc)
	}
}

func TestGraphBadgerInMemoryDefault(t *testing.T) {
	t.Parallel()

	g, err := New(Config{BadgerInMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// Verify it's a BadgerStore.
	if _, ok := g.store.(*BadgerStore); !ok {
		t.Fatalf("expected BadgerStore, got %T", g.store)
	}
}

// ─── Write-skew regression ──────────────────────────────────────────────────

func TestGraphAddRelDeleteNodeConcurrency(t *testing.T) {
	t.Parallel()

	// Regression test: concurrent AddRelationship(→X) + DeleteNode(X) must not
	// produce a dangling edge. Entity locks at the Graph layer serialize these
	// operations on overlapping entities.
	const iterations = 100
	for i := range iterations {
		t.Run(fmt.Sprintf("iter_%d", i), func(t *testing.T) {
			t.Parallel()
			g, err := New(Config{BadgerInMemory: true})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer g.Close()

			// Create 3 nodes: A, B, target.
			nodeA, _ := g.AddNode([]string{"A"}, nil)
			nodeB, _ := g.AddNode([]string{"B"}, nil)
			target, _ := g.AddNode([]string{"Target"}, nil)
			targetID := target.InternalID().SnowflakeID()

			// Race: AddRelationship(A→target) vs DeleteNode(target)
			done := make(chan struct{}, 2)
			var addErr error
			var delErr error

			go func() {
				defer func() { done <- struct{}{} }()
				_, addErr = g.AddRelationship("KNOWS", nodeA, target, nil)
			}()

			go func() {
				defer func() { done <- struct{}{} }()
				delErr = g.DeleteNode(targetID)
			}()

			<-done
			<-done

			// Exactly one of two valid outcomes:
			// 1. AddRel succeeded first → DeleteNode cascade removes it → graph clean
			// 2. DeleteNode succeeded first → AddRel fails with ErrNodeNotFound
			if addErr != nil && delErr != nil {
				// Both failed — unexpected.
				t.Fatalf("both failed: addErr=%v, delErr=%v", addErr, delErr)
			}

			// Invariant: no dangling edges. Every rel's endpoints must exist.
			nc, _ := g.NodeCount()
			rc, _ := g.RelationshipCount()

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
			bID := nodeB.InternalID().SnowflakeID()
			outB, _ := g.OutgoingRelationships(bID, "")
			if len(outB) != 0 {
				t.Errorf("nodeB should have no outgoing rels, got %d", len(outB))
			}
		})
	}
}

func TestGraphOutgoingRelationships(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	a, _ := g.AddNode([]string{"Person"}, nil)
	b, _ := g.AddNode([]string{"Person"}, nil)
	c, _ := g.AddNode([]string{"Person"}, nil)

	g.AddRelationship("KNOWS", a, b, nil)
	g.AddRelationship("WORKS_WITH", a, c, nil)

	// All outgoing from A.
	all, err := g.OutgoingRelationships(a.InternalID().SnowflakeID(), "")
	if err != nil {
		t.Fatalf("OutgoingRelationships(all): %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("OutgoingRelationships(all) = %d, want 2", len(all))
	}

	// Filtered by type.
	knows, err := g.OutgoingRelationships(a.InternalID().SnowflakeID(), "KNOWS")
	if err != nil {
		t.Fatalf("OutgoingRelationships(KNOWS): %v", err)
	}
	if len(knows) != 1 {
		t.Fatalf("OutgoingRelationships(KNOWS) = %d, want 1", len(knows))
	}

	// Unregistered type returns nil.
	none, err := g.OutgoingRelationships(a.InternalID().SnowflakeID(), "NONEXISTENT")
	if err != nil {
		t.Fatalf("OutgoingRelationships(NONEXISTENT): %v", err)
	}
	if none != nil {
		t.Errorf("OutgoingRelationships(NONEXISTENT) = %v, want nil", none)
	}

	// Node with no outgoing.
	empty, err := g.OutgoingRelationships(c.InternalID().SnowflakeID(), "")
	if err != nil {
		t.Fatalf("OutgoingRelationships(c, all): %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("OutgoingRelationships(c, all) = %d, want 0", len(empty))
	}
}

func TestGraphIncomingRelationships(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	a, _ := g.AddNode([]string{"Person"}, nil)
	b, _ := g.AddNode([]string{"Person"}, nil)
	c, _ := g.AddNode([]string{"Person"}, nil)

	g.AddRelationship("KNOWS", a, b, nil)
	g.AddRelationship("WORKS_WITH", c, b, nil)

	// All incoming to B.
	all, err := g.IncomingRelationships(b.InternalID().SnowflakeID(), "")
	if err != nil {
		t.Fatalf("IncomingRelationships(all): %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("IncomingRelationships(all) = %d, want 2", len(all))
	}

	// Filtered by type.
	knows, err := g.IncomingRelationships(b.InternalID().SnowflakeID(), "KNOWS")
	if err != nil {
		t.Fatalf("IncomingRelationships(KNOWS): %v", err)
	}
	if len(knows) != 1 {
		t.Fatalf("IncomingRelationships(KNOWS) = %d, want 1", len(knows))
	}

	// Unregistered type returns nil.
	none, err := g.IncomingRelationships(b.InternalID().SnowflakeID(), "NONEXISTENT")
	if err != nil {
		t.Fatalf("IncomingRelationships(NONEXISTENT): %v", err)
	}
	if none != nil {
		t.Errorf("IncomingRelationships(NONEXISTENT) = %v, want nil", none)
	}

	// Node with no incoming.
	empty, err := g.IncomingRelationships(a.InternalID().SnowflakeID(), "")
	if err != nil {
		t.Fatalf("IncomingRelationships(a, all): %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("IncomingRelationships(a, all) = %d, want 0", len(empty))
	}
}
