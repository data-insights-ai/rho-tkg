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

	// Valid: 1023 (maximum for 10-bit node)
	_, err = New(Config{SnowflakeNodeID: 1023})
	if err != nil {
		t.Errorf("SnowflakeNodeID=1023 should be valid, got: %v", err)
	}

	// Invalid: 1024 (exceeds 10-bit range)
	_, err = New(Config{SnowflakeNodeID: 1024})
	if err == nil {
		t.Fatal("SnowflakeNodeID=1024 should return error")
	}

	// Invalid: negative
	_, err = New(Config{SnowflakeNodeID: -1})
	if err == nil {
		t.Fatal("SnowflakeNodeID=-1 should return error")
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
	if got != n {
		t.Fatal("GetNode() returned different pointer than AddNode()")
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

	if g.RelationshipCount() != 2 {
		t.Fatalf("RelationshipCount before delete = %d, want 2", g.RelationshipCount())
	}

	// Delete A — both relationships should be gone.
	g.DeleteNode(nA.InternalID().SnowflakeID())
	if g.RelationshipCount() != 0 {
		t.Errorf("RelationshipCount after cascade delete = %d, want 0", g.RelationshipCount())
	}
}

func TestGraphNodesByLabel(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	g.AddNode([]string{"Person"}, nil)
	g.AddNode([]string{"Person"}, nil)
	g.AddNode([]string{"Animal"}, nil)

	persons := g.NodesByLabel("Person")
	if len(persons) != 2 {
		t.Fatalf("NodesByLabel(\"Person\") = %d, want 2", len(persons))
	}

	animals := g.NodesByLabel("Animal")
	if len(animals) != 1 {
		t.Fatalf("NodesByLabel(\"Animal\") = %d, want 1", len(animals))
	}

	// Unregistered label.
	unknown := g.NodesByLabel("Robot")
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

	knows := g.RelationshipsByType("KNOWS")
	if len(knows) != 2 {
		t.Fatalf("RelationshipsByType(\"KNOWS\") = %d, want 2", len(knows))
	}

	likes := g.RelationshipsByType("LIKES")
	if len(likes) != 1 {
		t.Fatalf("RelationshipsByType(\"LIKES\") = %d, want 1", len(likes))
	}

	unknown := g.RelationshipsByType("HATES")
	if len(unknown) != 0 {
		t.Fatalf("RelationshipsByType(\"HATES\") = %d, want 0", len(unknown))
	}
}

func TestGraphNodeCount(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	if g.NodeCount() != 0 {
		t.Fatalf("empty NodeCount() = %d, want 0", g.NodeCount())
	}
	g.AddNode([]string{"X"}, nil)
	g.AddNode([]string{"X"}, nil)
	if g.NodeCount() != 2 {
		t.Fatalf("NodeCount() = %d, want 2", g.NodeCount())
	}
}

func TestGraphRelCount(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nA, _ := g.AddNode([]string{"X"}, nil)
	nB, _ := g.AddNode([]string{"X"}, nil)
	g.AddRelationship("R", nA, nB, nil)
	if g.RelationshipCount() != 1 {
		t.Fatalf("RelationshipCount() = %d, want 1", g.RelationshipCount())
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
	if got != n {
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
	if g.RelationshipCount() != 0 {
		t.Errorf("RelationshipCount() = %d, want 0", g.RelationshipCount())
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
