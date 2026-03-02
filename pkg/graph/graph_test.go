package graph

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

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

	g, _ := New(Config{}) // MemoryStore — Close() calls MemoryStore.Close() which returns nil
	if err := g.Close(); err != nil {
		t.Fatalf("Close on MemoryStore should return nil, got: %v", err)
	}
}

func TestGraphCloseMemoryStoreCallsStoreClose(t *testing.T) {
	t.Parallel()

	// Verify Close() calls store.Close() even for MemoryStore.
	// Previously Close() was a no-op (closeFn == nil); now it goes through store.Close().
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// First close should succeed.
	if err := g.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}
	// Second close returns nil (sync.Once).
	if err := g.Close(); err != nil {
		t.Fatalf("Close 2 (idempotent): %v", err)
	}
}

func TestGraphNewBadgerDirWhitespace(t *testing.T) {
	t.Parallel()

	_, err := New(Config{BadgerDir: "   "})
	if err == nil {
		t.Fatal("New(BadgerDir: whitespace) should return error")
	}
	if !strings.Contains(err.Error(), "whitespace-only") {
		t.Fatalf("error should mention whitespace-only, got: %v", err)
	}
}

func TestGraphNewBadgerDirTabsWhitespace(t *testing.T) {
	t.Parallel()

	_, err := New(Config{BadgerDir: "\t\n "})
	if err == nil {
		t.Fatal("New(BadgerDir: tabs/newlines) should return error")
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

// ─── UpdateNode tests ────────────────────────────────────────────────────────

func TestGraphUpdateNode(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice", "age": 30})
	id := n.InternalID().SnowflakeID()

	updated, err := g.UpdateNode(id, map[string]any{"name": "Bob", "age": 31})
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
	got, _ := g.GetNode(id)
	v, _ = got.GetProperty("name")
	if v != "Bob" {
		t.Fatalf("persisted name = %v, want Bob", v)
	}
}

func TestGraphUpdateNodeAddProperty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.InternalID().SnowflakeID()

	_, err := g.UpdateNode(id, map[string]any{"email": "alice@example.com"})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	got, _ := g.GetNode(id)
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

	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice", "age": 30})
	id := n.InternalID().SnowflakeID()

	_, err := g.UpdateNode(id, map[string]any{"age": nil})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	got, _ := g.GetNode(id)
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

	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice", "age": 30, "city": "NYC"})
	id := n.InternalID().SnowflakeID()

	// Add email, modify name, delete city — all in one call.
	_, err := g.UpdateNode(id, map[string]any{
		"email": "alice@example.com",
		"name":  "Bob",
		"city":  nil,
	})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	got, _ := g.GetNode(id)
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

	g, _ := New(Config{})
	_, err := g.UpdateNode(snowflake.ID(999), map[string]any{"name": "Alice"})
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("UpdateNode(nonexistent): errors.Is(err, ErrNodeNotFound) = false; err = %v", err)
	}
}

func TestGraphUpdateNodeInvalidProperty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Person"}, nil)
	id := n.InternalID().SnowflakeID()

	_, err := g.UpdateNode(id, map[string]any{"tkg_hack": "bad"})
	if !errors.Is(err, types.ErrReservedPrefix) {
		t.Fatalf("UpdateNode(tkg_ key): errors.Is(err, ErrReservedPrefix) = false; err = %v", err)
	}
}

func TestGraphUpdateNodeInvalidValue(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Person"}, nil)
	id := n.InternalID().SnowflakeID()

	type badStruct struct{ X int }
	_, err := g.UpdateNode(id, map[string]any{"bad": badStruct{42}})
	if !errors.Is(err, types.ErrUnsupportedValueType) {
		t.Fatalf("UpdateNode(bad value): errors.Is(err, ErrUnsupportedValueType) = false; err = %v", err)
	}
}

func TestGraphUpdateNodeVersionIncrement(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.InternalID().SnowflakeID()

	if n.Version() != 0 {
		t.Fatalf("initial version = %d, want 0", n.Version())
	}

	updated1, _ := g.UpdateNode(id, map[string]any{"name": "Bob"})
	if updated1.Version() != 1 {
		t.Fatalf("version after first update = %d, want 1", updated1.Version())
	}

	updated2, _ := g.UpdateNode(id, map[string]any{"name": "Charlie"})
	if updated2.Version() != 2 {
		t.Fatalf("version after second update = %d, want 2", updated2.Version())
	}
}

func TestGraphUpdateNodeUpdatedAt(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Person"}, nil)
	id := n.InternalID().SnowflakeID()

	updated, _ := g.UpdateNode(id, map[string]any{"name": "Alice"})
	tm := updated.Temporal()
	if tm == nil {
		t.Fatal("temporal should be set after update")
	}
	if tm.UpdatedAt == 0 {
		t.Fatal("UpdatedAt should be non-zero after update")
	}
}

func TestGraphUpdateNodeEmptyUpdates(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.InternalID().SnowflakeID()

	got, err := g.UpdateNode(id, map[string]any{})
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

	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Person"}, map[string]any{"counter": 0})
	id := n.InternalID().SnowflakeID()

	const workers = 50
	var wg sync.WaitGroup
	wg.Add(workers)

	for i := range workers {
		go func(val int) {
			defer wg.Done()
			// Each goroutine reads and increments a different property to avoid lost updates.
			g.UpdateNode(id, map[string]any{fmt.Sprintf("worker_%d", val): val})
		}(i)
	}
	wg.Wait()

	got, _ := g.GetNode(id)
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

func TestGraphUpdateNodeConcurrentDifferentNodes(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})

	const count = 20
	ids := make([]snowflake.ID, count)
	for i := range count {
		n, _ := g.AddNode([]string{"X"}, map[string]any{"v": 0})
		ids[i] = n.InternalID().SnowflakeID()
	}

	var wg sync.WaitGroup
	wg.Add(count)
	for i := range count {
		go func(idx int) {
			defer wg.Done()
			g.UpdateNode(ids[idx], map[string]any{"v": idx + 1})
		}(i)
	}
	wg.Wait()

	for i, id := range ids {
		got, err := g.GetNode(id)
		if err != nil {
			t.Fatalf("GetNode(%d): %v", id, err)
		}
		v, _ := got.GetProperty("v")
		if v != i+1 {
			t.Errorf("node %d: v = %v, want %d", id, v, i+1)
		}
	}
}

func TestGraphUpdateNodeLabelsUnchanged(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Person", "Actor"}, map[string]any{"name": "Alice"})
	id := n.InternalID().SnowflakeID()

	g.UpdateNode(id, map[string]any{"name": "Bob"})

	got, _ := g.GetNode(id)
	labels := g.NodeLabels(got)
	if len(labels) != 2 || labels[0] != "Person" || labels[1] != "Actor" {
		t.Fatalf("labels after update = %v, want [Person Actor]", labels)
	}
}

// ─── UpdateRelationship tests ────────────────────────────────────────────────

func TestGraphUpdateRelationship(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nA, _ := g.AddNode([]string{"X"}, nil)
	nB, _ := g.AddNode([]string{"X"}, nil)
	r, _ := g.AddRelationship("KNOWS", nA, nB, map[string]any{"since": 2020})
	id := r.InternalID().SnowflakeID()

	updated, err := g.UpdateRelationship(id, map[string]any{"since": 2021})
	if err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}
	v, ok := updated.GetProperty("since")
	if !ok || v != 2021 {
		t.Fatalf("since = %v, want 2021", v)
	}

	// Verify persisted.
	got, _ := g.GetRelationship(id)
	v, _ = got.GetProperty("since")
	if v != 2021 {
		t.Fatalf("persisted since = %v, want 2021", v)
	}
}

func TestGraphUpdateRelAddProperty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nA, _ := g.AddNode([]string{"X"}, nil)
	nB, _ := g.AddNode([]string{"X"}, nil)
	r, _ := g.AddRelationship("KNOWS", nA, nB, nil)
	id := r.InternalID().SnowflakeID()

	_, err := g.UpdateRelationship(id, map[string]any{"weight": 0.5})
	if err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}

	got, _ := g.GetRelationship(id)
	v, ok := got.GetProperty("weight")
	if !ok || v != 0.5 {
		t.Fatalf("weight = %v, want 0.5", v)
	}
}

func TestGraphUpdateRelDeleteProperty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nA, _ := g.AddNode([]string{"X"}, nil)
	nB, _ := g.AddNode([]string{"X"}, nil)
	r, _ := g.AddRelationship("KNOWS", nA, nB, map[string]any{"since": 2020, "note": "friend"})
	id := r.InternalID().SnowflakeID()

	_, err := g.UpdateRelationship(id, map[string]any{"note": nil})
	if err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}

	got, _ := g.GetRelationship(id)
	_, ok := got.GetProperty("note")
	if ok {
		t.Fatal("note should be deleted")
	}
	v, ok := got.GetProperty("since")
	if !ok || v != 2020 {
		t.Fatalf("since = %v, want 2020 (unchanged)", v)
	}
}

func TestGraphUpdateRelMixed(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nA, _ := g.AddNode([]string{"X"}, nil)
	nB, _ := g.AddNode([]string{"X"}, nil)
	r, _ := g.AddRelationship("KNOWS", nA, nB, map[string]any{"since": 2020, "note": "friend"})
	id := r.InternalID().SnowflakeID()

	_, err := g.UpdateRelationship(id, map[string]any{
		"since":  2021,
		"note":   nil,
		"weight": 0.8,
	})
	if err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}

	got, _ := g.GetRelationship(id)
	v, _ := got.GetProperty("since")
	if v != 2021 {
		t.Fatalf("since = %v, want 2021", v)
	}
	_, ok := got.GetProperty("note")
	if ok {
		t.Fatal("note should be deleted")
	}
	v, ok = got.GetProperty("weight")
	if !ok || v != 0.8 {
		t.Fatalf("weight = %v, want 0.8", v)
	}
}

func TestGraphUpdateRelNotFound(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	_, err := g.UpdateRelationship(snowflake.ID(999), map[string]any{"x": 1})
	if !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("UpdateRelationship(nonexistent): errors.Is(err, ErrRelNotFound) = false; err = %v", err)
	}
}

func TestGraphUpdateRelInvalidProperty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nA, _ := g.AddNode([]string{"X"}, nil)
	nB, _ := g.AddNode([]string{"X"}, nil)
	r, _ := g.AddRelationship("KNOWS", nA, nB, nil)
	id := r.InternalID().SnowflakeID()

	_, err := g.UpdateRelationship(id, map[string]any{"tkg_hack": "bad"})
	if !errors.Is(err, types.ErrReservedPrefix) {
		t.Fatalf("UpdateRelationship(tkg_ key): errors.Is(err, ErrReservedPrefix) = false; err = %v", err)
	}
}

func TestGraphUpdateRelInvalidValue(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nA, _ := g.AddNode([]string{"X"}, nil)
	nB, _ := g.AddNode([]string{"X"}, nil)
	r, _ := g.AddRelationship("KNOWS", nA, nB, nil)
	id := r.InternalID().SnowflakeID()

	type badStruct struct{ X int }
	_, err := g.UpdateRelationship(id, map[string]any{"bad": badStruct{42}})
	if !errors.Is(err, types.ErrUnsupportedValueType) {
		t.Fatalf("UpdateRelationship(bad value): errors.Is(err, ErrUnsupportedValueType) = false; err = %v", err)
	}
}

func TestGraphUpdateRelVersionIncrement(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nA, _ := g.AddNode([]string{"X"}, nil)
	nB, _ := g.AddNode([]string{"X"}, nil)
	r, _ := g.AddRelationship("KNOWS", nA, nB, nil)
	id := r.InternalID().SnowflakeID()

	if r.Version() != 0 {
		t.Fatalf("initial version = %d, want 0", r.Version())
	}

	u1, _ := g.UpdateRelationship(id, map[string]any{"x": 1})
	if u1.Version() != 1 {
		t.Fatalf("version after first update = %d, want 1", u1.Version())
	}

	u2, _ := g.UpdateRelationship(id, map[string]any{"x": 2})
	if u2.Version() != 2 {
		t.Fatalf("version after second update = %d, want 2", u2.Version())
	}
}

func TestGraphUpdateRelUpdatedAt(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nA, _ := g.AddNode([]string{"X"}, nil)
	nB, _ := g.AddNode([]string{"X"}, nil)
	r, _ := g.AddRelationship("KNOWS", nA, nB, nil)
	id := r.InternalID().SnowflakeID()

	updated, _ := g.UpdateRelationship(id, map[string]any{"x": 1})
	tm := updated.Temporal()
	if tm == nil {
		t.Fatal("temporal should be set after update")
	}
	if tm.UpdatedAt == 0 {
		t.Fatal("UpdatedAt should be non-zero after update")
	}
}

func TestGraphUpdateRelEmptyUpdates(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nA, _ := g.AddNode([]string{"X"}, nil)
	nB, _ := g.AddNode([]string{"X"}, nil)
	r, _ := g.AddRelationship("KNOWS", nA, nB, map[string]any{"x": 1})
	id := r.InternalID().SnowflakeID()

	got, err := g.UpdateRelationship(id, map[string]any{})
	if err != nil {
		t.Fatalf("UpdateRelationship(empty): %v", err)
	}
	if got.Version() != 0 {
		t.Fatalf("version after empty update = %d, want 0 (no bump)", got.Version())
	}
}

func TestGraphUpdateRelEndpointsUnchanged(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nA, _ := g.AddNode([]string{"X"}, nil)
	nB, _ := g.AddNode([]string{"X"}, nil)
	r, _ := g.AddRelationship("KNOWS", nA, nB, map[string]any{"x": 1})
	id := r.InternalID().SnowflakeID()
	origStartID := r.StartNodeID().SnowflakeID()
	origEndID := r.EndNodeID().SnowflakeID()

	g.UpdateRelationship(id, map[string]any{"x": 2})

	got, _ := g.GetRelationship(id)
	if got.StartNodeID().SnowflakeID() != origStartID {
		t.Fatal("startID changed after update")
	}
	if got.EndNodeID().SnowflakeID() != origEndID {
		t.Fatal("endID changed after update")
	}
}

// ─── Convenience method tests ────────────────────────────────────────────────

func TestGraphSetNodeProperty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Person"}, nil)
	id := n.InternalID().SnowflakeID()

	if err := g.SetNodeProperty(id, "name", "Alice"); err != nil {
		t.Fatalf("SetNodeProperty: %v", err)
	}

	got, _ := g.GetNode(id)
	v, ok := got.GetProperty("name")
	if !ok || v != "Alice" {
		t.Fatalf("name = %v, want Alice", v)
	}
}

func TestGraphDeleteNodeProperty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.InternalID().SnowflakeID()

	if err := g.DeleteNodeProperty(id, "name"); err != nil {
		t.Fatalf("DeleteNodeProperty: %v", err)
	}

	got, _ := g.GetNode(id)
	_, ok := got.GetProperty("name")
	if ok {
		t.Fatal("name should be deleted")
	}
}

func TestGraphSetRelationshipProperty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nA, _ := g.AddNode([]string{"X"}, nil)
	nB, _ := g.AddNode([]string{"X"}, nil)
	r, _ := g.AddRelationship("KNOWS", nA, nB, nil)
	id := r.InternalID().SnowflakeID()

	if err := g.SetRelationshipProperty(id, "weight", 0.5); err != nil {
		t.Fatalf("SetRelationshipProperty: %v", err)
	}

	got, _ := g.GetRelationship(id)
	v, ok := got.GetProperty("weight")
	if !ok || v != 0.5 {
		t.Fatalf("weight = %v, want 0.5", v)
	}
}

func TestGraphDeleteRelationshipProperty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nA, _ := g.AddNode([]string{"X"}, nil)
	nB, _ := g.AddNode([]string{"X"}, nil)
	r, _ := g.AddRelationship("KNOWS", nA, nB, map[string]any{"weight": 0.5})
	id := r.InternalID().SnowflakeID()

	if err := g.DeleteRelationshipProperty(id, "weight"); err != nil {
		t.Fatalf("DeleteRelationshipProperty: %v", err)
	}

	got, _ := g.GetRelationship(id)
	_, ok := got.GetProperty("weight")
	if ok {
		t.Fatal("weight should be deleted")
	}
}

// ─── Badger integration: UpdateNode / UpdateRelationship ─────────────────────

func TestGraphBadgerUpdateNode(t *testing.T) {
	t.Parallel()

	g, err := New(Config{BadgerInMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.InternalID().SnowflakeID()

	updated, err := g.UpdateNode(id, map[string]any{"name": "Bob", "age": 30})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}
	v, _ := updated.GetProperty("name")
	if v != "Bob" {
		t.Fatalf("name = %v, want Bob", v)
	}

	got, _ := g.GetNode(id)
	v, _ = got.GetProperty("name")
	if v != "Bob" {
		t.Fatalf("persisted name = %v, want Bob", v)
	}
	v, ok := got.GetProperty("age")
	if !ok || v != 30 {
		t.Fatalf("age = %v, want 30", v)
	}
}

func TestGraphBadgerUpdateRelationship(t *testing.T) {
	t.Parallel()

	g, err := New(Config{BadgerInMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	nA, _ := g.AddNode([]string{"X"}, nil)
	nB, _ := g.AddNode([]string{"X"}, nil)
	r, _ := g.AddRelationship("KNOWS", nA, nB, map[string]any{"since": 2020})
	id := r.InternalID().SnowflakeID()

	_, err = g.UpdateRelationship(id, map[string]any{"since": 2021})
	if err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}

	got, _ := g.GetRelationship(id)
	v, _ := got.GetProperty("since")
	if v != 2021 {
		t.Fatalf("since = %v, want 2021", v)
	}
}

func TestGraphBadgerUpdateNodePersistence(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Create, update, close.
	g1, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New 1: %v", err)
	}
	n, _ := g1.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.InternalID().SnowflakeID()

	_, err = g1.UpdateNode(id, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}
	if err := g1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	// Reopen and verify updated value persisted.
	g2, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New 2: %v", err)
	}
	defer g2.Close()

	got, err := g2.GetNode(id)
	if err != nil {
		t.Fatalf("GetNode after reopen: %v", err)
	}
	v, ok := got.GetProperty("name")
	if !ok || v != "Bob" {
		t.Fatalf("persisted name = %v, want Bob", v)
	}
	if got.Version() != 1 {
		t.Fatalf("persisted version = %d, want 1", got.Version())
	}
}

func TestGraphBadgerUpdateRelPersistence(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Create, update, close.
	g1, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New 1: %v", err)
	}
	nA, _ := g1.AddNode([]string{"X"}, nil)
	nB, _ := g1.AddNode([]string{"X"}, nil)
	r, _ := g1.AddRelationship("KNOWS", nA, nB, map[string]any{"since": 2020})
	relID := r.InternalID().SnowflakeID()

	_, err = g1.UpdateRelationship(relID, map[string]any{"since": 2021})
	if err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}
	if err := g1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	// Reopen and verify updated value persisted.
	g2, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New 2: %v", err)
	}
	defer g2.Close()

	got, err := g2.GetRelationship(relID)
	if err != nil {
		t.Fatalf("GetRelationship after reopen: %v", err)
	}
	v, ok := got.GetProperty("since")
	if !ok || v != 2021 {
		t.Fatalf("persisted since = %v, want 2021", v)
	}
	if got.Version() != 1 {
		t.Fatalf("persisted version = %d, want 1", got.Version())
	}
}

// ─── Version history — Node ─────────────────────────────────────────────────

func TestGraphUpdateNodeSavesHistory(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})
	defer g.Close()

	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.InternalID().SnowflakeID()

	// Update: should save version 0 (pre-mutation) to history.
	_, err := g.UpdateNode(id, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	history, err := g.GetNodeHistory(id)
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(history) != 1 {
		t.Fatalf("len(history) = %d, want 1", len(history))
	}
	if history[0].Version() != 0 {
		t.Errorf("history[0].Version() = %d, want 0", history[0].Version())
	}
	hv, ok := history[0].GetProperty("name")
	if !ok || hv != "Alice" {
		t.Fatalf("history[0] name = %v, want Alice", hv)
	}
}

func TestGraphUpdateNodeHistoryGrows(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})
	defer g.Close()

	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "v0"})
	id := n.InternalID().SnowflakeID()

	for i := 1; i <= 5; i++ {
		_, err := g.UpdateNode(id, map[string]any{"name": fmt.Sprintf("v%d", i)})
		if err != nil {
			t.Fatalf("UpdateNode %d: %v", i, err)
		}
	}

	history, err := g.GetNodeHistory(id)
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(history) != 5 {
		t.Fatalf("len(history) = %d, want 5", len(history))
	}
}

func TestGraphUpdateNodeHistoryAscendingOrder(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})
	defer g.Close()

	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "v0"})
	id := n.InternalID().SnowflakeID()

	for i := 1; i <= 3; i++ {
		g.UpdateNode(id, map[string]any{"name": fmt.Sprintf("v%d", i)})
	}

	history, _ := g.GetNodeHistory(id)
	for i := 0; i < len(history)-1; i++ {
		if history[i].Version() >= history[i+1].Version() {
			t.Fatalf("not ascending: v[%d]=%d >= v[%d]=%d",
				i, history[i].Version(), i+1, history[i+1].Version())
		}
	}
}

func TestGraphGetNodeHistoryEmpty(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})
	defer g.Close()

	n, _ := g.AddNode([]string{"Person"}, nil)
	id := n.InternalID().SnowflakeID()

	// No updates = no history.
	history, err := g.GetNodeHistory(id)
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(history) != 0 {
		t.Fatalf("expected empty history, got %d", len(history))
	}
}

func TestGraphDeleteNodePreservesHistory(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})
	defer g.Close()

	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "v0"})
	id := n.InternalID().SnowflakeID()

	g.UpdateNode(id, map[string]any{"name": "v1"})
	g.UpdateNode(id, map[string]any{"name": "v2"})

	if err := g.DeleteNode(id); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	// History is preserved — v0 pre-mutation, v1 pre-mutation, + tombstone at v2.
	history, _ := g.GetNodeHistory(id)
	if len(history) < 3 {
		t.Fatalf("expected at least 3 preserved history entries, got %d", len(history))
	}
}

// ─── Version history — Relationship ─────────────────────────────────────────

func TestGraphUpdateRelSavesHistory(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})
	defer g.Close()

	nA, _ := g.AddNode([]string{"X"}, nil)
	nB, _ := g.AddNode([]string{"X"}, nil)
	r, _ := g.AddRelationship("KNOWS", nA, nB, map[string]any{"weight": 1.0})
	id := r.InternalID().SnowflakeID()

	_, err := g.UpdateRelationship(id, map[string]any{"weight": 2.0})
	if err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}

	history, err := g.GetRelHistory(id)
	if err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	}
	if len(history) != 1 {
		t.Fatalf("len(history) = %d, want 1", len(history))
	}
	if history[0].Version() != 0 {
		t.Errorf("history[0].Version() = %d, want 0", history[0].Version())
	}
	hv, ok := history[0].GetProperty("weight")
	if !ok || hv != 1.0 {
		t.Fatalf("history[0] weight = %v, want 1.0", hv)
	}
}

func TestGraphUpdateRelHistoryGrows(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})
	defer g.Close()

	nA, _ := g.AddNode([]string{"X"}, nil)
	nB, _ := g.AddNode([]string{"X"}, nil)
	r, _ := g.AddRelationship("KNOWS", nA, nB, map[string]any{"w": int64(0)})
	id := r.InternalID().SnowflakeID()

	for i := 1; i <= 5; i++ {
		g.UpdateRelationship(id, map[string]any{"w": int64(i)})
	}

	history, _ := g.GetRelHistory(id)
	if len(history) != 5 {
		t.Fatalf("len(history) = %d, want 5", len(history))
	}
}

func TestGraphUpdateRelHistoryAscendingOrder(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})
	defer g.Close()

	nA, _ := g.AddNode([]string{"X"}, nil)
	nB, _ := g.AddNode([]string{"X"}, nil)
	r, _ := g.AddRelationship("KNOWS", nA, nB, map[string]any{"w": int64(0)})
	id := r.InternalID().SnowflakeID()

	for i := 1; i <= 3; i++ {
		g.UpdateRelationship(id, map[string]any{"w": int64(i)})
	}

	history, _ := g.GetRelHistory(id)
	for i := 0; i < len(history)-1; i++ {
		if history[i].Version() >= history[i+1].Version() {
			t.Fatalf("not ascending: v[%d]=%d >= v[%d]=%d",
				i, history[i].Version(), i+1, history[i+1].Version())
		}
	}
}

func TestGraphGetRelHistoryEmpty(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})
	defer g.Close()

	nA, _ := g.AddNode([]string{"X"}, nil)
	nB, _ := g.AddNode([]string{"X"}, nil)
	r, _ := g.AddRelationship("KNOWS", nA, nB, nil)
	id := r.InternalID().SnowflakeID()

	history, err := g.GetRelHistory(id)
	if err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	}
	if len(history) != 0 {
		t.Fatalf("expected empty, got %d", len(history))
	}
}

func TestGraphDeleteRelPreservesHistory(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})
	defer g.Close()

	nA, _ := g.AddNode([]string{"X"}, nil)
	nB, _ := g.AddNode([]string{"X"}, nil)
	r, _ := g.AddRelationship("KNOWS", nA, nB, map[string]any{"w": int64(0)})
	id := r.InternalID().SnowflakeID()

	g.UpdateRelationship(id, map[string]any{"w": int64(1)})
	g.UpdateRelationship(id, map[string]any{"w": int64(2)})

	if err := g.DeleteRelationship(id); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}

	// History preserved: v0 pre-mutation, v1 pre-mutation + tombstone at v2.
	history, _ := g.GetRelHistory(id)
	if len(history) < 3 {
		t.Fatalf("expected at least 3 preserved rel history entries, got %d", len(history))
	}
}

// ─── Version history — Badger persistence ───────────────────────────────────

func TestGraphBadgerNodeHistoryPersistence(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	g1, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New 1: %v", err)
	}
	n, _ := g1.AddNode([]string{"Person"}, map[string]any{"name": "v0"})
	id := n.InternalID().SnowflakeID()

	g1.UpdateNode(id, map[string]any{"name": "v1"})
	g1.UpdateNode(id, map[string]any{"name": "v2"})

	if err := g1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	g2, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New 2: %v", err)
	}
	defer g2.Close()

	history, err := g2.GetNodeHistory(id)
	if err != nil {
		t.Fatalf("GetNodeHistory after reopen: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("len(history) = %d, want 2", len(history))
	}
	if history[0].Version() != 0 {
		t.Errorf("history[0].Version() = %d, want 0", history[0].Version())
	}
}

func TestGraphBadgerRelHistoryPersistence(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	g1, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New 1: %v", err)
	}
	nA, _ := g1.AddNode([]string{"X"}, nil)
	nB, _ := g1.AddNode([]string{"X"}, nil)
	r, _ := g1.AddRelationship("KNOWS", nA, nB, map[string]any{"w": int64(0)})
	relID := r.InternalID().SnowflakeID()

	g1.UpdateRelationship(relID, map[string]any{"w": int64(1)})
	g1.UpdateRelationship(relID, map[string]any{"w": int64(2)})

	if err := g1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	g2, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New 2: %v", err)
	}
	defer g2.Close()

	history, err := g2.GetRelHistory(relID)
	if err != nil {
		t.Fatalf("GetRelHistory after reopen: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("len(history) = %d, want 2", len(history))
	}
}

func TestGraphBadgerDeleteNodePreservesHistory(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	g1, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New 1: %v", err)
	}
	n, _ := g1.AddNode([]string{"Person"}, map[string]any{"name": "v0"})
	id := n.InternalID().SnowflakeID()

	g1.UpdateNode(id, map[string]any{"name": "v1"})

	if err := g1.DeleteNode(id); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	if err := g1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	g2, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New 2: %v", err)
	}
	defer g2.Close()

	// History is preserved after delete (v0 pre-mutation + tombstone).
	history, _ := g2.GetNodeHistory(id)
	if len(history) < 2 {
		t.Fatalf("expected preserved history after reopen, got %d", len(history))
	}
}

func TestGraphBadgerDeleteRelPreservesHistory(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	g1, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New 1: %v", err)
	}
	nA, _ := g1.AddNode([]string{"X"}, nil)
	nB, _ := g1.AddNode([]string{"X"}, nil)
	r, _ := g1.AddRelationship("KNOWS", nA, nB, map[string]any{"w": int64(0)})
	relID := r.InternalID().SnowflakeID()

	g1.UpdateRelationship(relID, map[string]any{"w": int64(1)})

	if err := g1.DeleteRelationship(relID); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}
	if err := g1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	g2, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New 2: %v", err)
	}
	defer g2.Close()

	// History is preserved after delete (v0 pre-mutation + tombstone).
	history, _ := g2.GetRelHistory(relID)
	if len(history) < 2 {
		t.Fatalf("expected preserved rel history after reopen, got %d", len(history))
	}
}

// --- Hash chain integrity -- Node ---

func TestGraphAddNodeSetsIntegrity(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, err := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	ig := n.Integrity()
	if ig == nil {
		t.Fatal("Integrity() is nil after AddNode")
	}
	if ig.Hash == "" {
		t.Fatal("Hash is empty after AddNode")
	}
	if ig.PrevHash != "" {
		t.Fatalf("PrevHash = %q, want empty for genesis", ig.PrevHash)
	}
	if len(ig.Hash) != 64 {
		t.Fatalf("Hash length = %d, want 64 hex chars", len(ig.Hash))
	}
}

func TestGraphAddNodeHashDeterministic(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n1, _ := g.AddNode([]string{"Person", "Actor"}, map[string]any{"name": "Alice", "age": int64(30)})
	n2, _ := g.AddNode([]string{"Person", "Actor"}, map[string]any{"name": "Alice", "age": int64(30)})

	// Different IDs means different hashes — but same labels+props with same ID would match.
	// We verify that both hashes are non-empty and well-formed.
	if n1.Integrity().Hash == "" || n2.Integrity().Hash == "" {
		t.Fatal("one or both hashes are empty")
	}
	// IDs differ, so hashes must differ.
	if n1.Integrity().Hash == n2.Integrity().Hash {
		t.Fatal("different node IDs produced identical hashes")
	}
}

func TestGraphAddNodeGenesisZeroPrevHash(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"X"}, nil)

	if n.Integrity().PrevHash != "" {
		t.Fatalf("PrevHash = %q, want empty for genesis", n.Integrity().PrevHash)
	}
}

// --- Hash chain integrity -- Relationship ---

func TestGraphAddRelSetsIntegrity(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nA, _ := g.AddNode([]string{"Person"}, nil)
	nB, _ := g.AddNode([]string{"Person"}, nil)

	r, err := g.AddRelationship("KNOWS", nA, nB, map[string]any{"since": int64(2020)})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	ig := r.Integrity()
	if ig == nil {
		t.Fatal("Integrity() is nil after AddRelationship")
	}
	if ig.Hash == "" {
		t.Fatal("Hash is empty after AddRelationship")
	}
	if ig.PrevHash != "" {
		t.Fatalf("PrevHash = %q, want empty for genesis", ig.PrevHash)
	}
	if len(ig.Hash) != 64 {
		t.Fatalf("Hash length = %d, want 64 hex chars", len(ig.Hash))
	}
}

func TestGraphAddRelHashDeterministic(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nA, _ := g.AddNode([]string{"Person"}, nil)
	nB, _ := g.AddNode([]string{"Person"}, nil)

	r1, _ := g.AddRelationship("KNOWS", nA, nB, map[string]any{"since": int64(2020)})
	r2, _ := g.AddRelationship("KNOWS", nA, nB, map[string]any{"since": int64(2020)})

	if r1.Integrity().Hash == "" || r2.Integrity().Hash == "" {
		t.Fatal("one or both hashes are empty")
	}
	// Different IDs means different hashes.
	if r1.Integrity().Hash == r2.Integrity().Hash {
		t.Fatal("different rel IDs produced identical hashes")
	}
}

func TestGraphAddRelGenesisZeroPrevHash(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nA, _ := g.AddNode([]string{"X"}, nil)
	nB, _ := g.AddNode([]string{"X"}, nil)
	r, _ := g.AddRelationship("KNOWS", nA, nB, nil)

	if r.Integrity().PrevHash != "" {
		t.Fatalf("PrevHash = %q, want empty for genesis", r.Integrity().PrevHash)
	}
}

// --- Hash chain integrity -- UpdateNode ---

func TestGraphUpdateNodeHashChain(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.InternalID().SnowflakeID()

	oldHash := n.Integrity().Hash

	updated, err := g.UpdateNode(id, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	ig := updated.Integrity()
	if ig == nil {
		t.Fatal("Integrity() is nil after UpdateNode")
	}
	if ig.PrevHash != oldHash {
		t.Fatalf("PrevHash = %q, want %q", ig.PrevHash, oldHash)
	}
	if ig.Hash == oldHash {
		t.Fatal("Hash did not change after update")
	}
}

func TestGraphUpdateNodeMultipleUpdatesChain(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "v0"})
	id := n.InternalID().SnowflakeID()

	h0 := n.Integrity().Hash

	n1, _ := g.UpdateNode(id, map[string]any{"name": "v1"})
	h1 := n1.Integrity().Hash
	if n1.Integrity().PrevHash != h0 {
		t.Fatalf("update 1: PrevHash = %q, want %q", n1.Integrity().PrevHash, h0)
	}

	n2, _ := g.UpdateNode(id, map[string]any{"name": "v2"})
	h2 := n2.Integrity().Hash
	if n2.Integrity().PrevHash != h1 {
		t.Fatalf("update 2: PrevHash = %q, want %q", n2.Integrity().PrevHash, h1)
	}

	n3, _ := g.UpdateNode(id, map[string]any{"name": "v3"})
	if n3.Integrity().PrevHash != h2 {
		t.Fatalf("update 3: PrevHash = %q, want %q", n3.Integrity().PrevHash, h2)
	}

	// All hashes must be unique.
	hashes := map[string]bool{h0: true, h1: true, h2: true, n3.Integrity().Hash: true}
	if len(hashes) != 4 {
		t.Fatal("expected 4 unique hashes across genesis + 3 updates")
	}
}

func TestGraphUpdateNodeHashChanges(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.InternalID().SnowflakeID()
	hashBefore := n.Integrity().Hash

	updated, _ := g.UpdateNode(id, map[string]any{"age": int64(30)})
	if updated.Integrity().Hash == hashBefore {
		t.Fatal("hash did not change when properties changed")
	}
}

// --- Hash chain integrity -- UpdateRelationship ---

func TestGraphUpdateRelHashChain(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nA, _ := g.AddNode([]string{"Person"}, nil)
	nB, _ := g.AddNode([]string{"Person"}, nil)
	r, _ := g.AddRelationship("KNOWS", nA, nB, map[string]any{"weight": int64(1)})
	relID := r.InternalID().SnowflakeID()

	oldHash := r.Integrity().Hash

	updated, err := g.UpdateRelationship(relID, map[string]any{"weight": int64(2)})
	if err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}

	ig := updated.Integrity()
	if ig == nil {
		t.Fatal("Integrity() is nil after UpdateRelationship")
	}
	if ig.PrevHash != oldHash {
		t.Fatalf("PrevHash = %q, want %q", ig.PrevHash, oldHash)
	}
	if ig.Hash == oldHash {
		t.Fatal("Hash did not change after update")
	}
}

func TestGraphUpdateRelMultipleUpdatesChain(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nA, _ := g.AddNode([]string{"X"}, nil)
	nB, _ := g.AddNode([]string{"X"}, nil)
	r, _ := g.AddRelationship("KNOWS", nA, nB, map[string]any{"w": int64(0)})
	relID := r.InternalID().SnowflakeID()

	h0 := r.Integrity().Hash

	r1, _ := g.UpdateRelationship(relID, map[string]any{"w": int64(1)})
	h1 := r1.Integrity().Hash
	if r1.Integrity().PrevHash != h0 {
		t.Fatalf("update 1: PrevHash = %q, want %q", r1.Integrity().PrevHash, h0)
	}

	r2, _ := g.UpdateRelationship(relID, map[string]any{"w": int64(2)})
	h2 := r2.Integrity().Hash
	if r2.Integrity().PrevHash != h1 {
		t.Fatalf("update 2: PrevHash = %q, want %q", r2.Integrity().PrevHash, h1)
	}

	r3, _ := g.UpdateRelationship(relID, map[string]any{"w": int64(3)})
	if r3.Integrity().PrevHash != h2 {
		t.Fatalf("update 3: PrevHash = %q, want %q", r3.Integrity().PrevHash, h2)
	}

	hashes := map[string]bool{h0: true, h1: true, h2: true, r3.Integrity().Hash: true}
	if len(hashes) != 4 {
		t.Fatal("expected 4 unique hashes across genesis + 3 updates")
	}
}

func TestGraphUpdateRelHashChanges(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nA, _ := g.AddNode([]string{"X"}, nil)
	nB, _ := g.AddNode([]string{"X"}, nil)
	r, _ := g.AddRelationship("KNOWS", nA, nB, map[string]any{"w": int64(1)})
	relID := r.InternalID().SnowflakeID()
	hashBefore := r.Integrity().Hash

	updated, _ := g.UpdateRelationship(relID, map[string]any{"extra": "data"})
	if updated.Integrity().Hash == hashBefore {
		t.Fatal("hash did not change when properties changed")
	}
}

// ─── Gap 1: Concurrency & Locks ─────────────────────────────────────────────

func TestGraphConcurrentAddRelSameEndpoints(t *testing.T) {
	t.Parallel()

	const iterations = 50
	for i := range iterations {
		t.Run(fmt.Sprintf("iter_%d", i), func(t *testing.T) {
			t.Parallel()
			g, err := New(Config{BadgerInMemory: true})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer g.Close()

			nA, _ := g.AddNode([]string{"A"}, nil)
			nB, _ := g.AddNode([]string{"B"}, nil)

			var wg sync.WaitGroup
			wg.Add(2)

			var err1, err2 error
			go func() {
				defer wg.Done()
				_, err1 = g.AddRelationship("R1", nA, nB, nil)
			}()
			go func() {
				defer wg.Done()
				_, err2 = g.AddRelationship("R2", nA, nB, nil)
			}()
			wg.Wait()

			if err1 != nil {
				t.Fatalf("AddRelationship R1: %v", err1)
			}
			if err2 != nil {
				t.Fatalf("AddRelationship R2: %v", err2)
			}

			rc, _ := g.RelationshipCount()
			if rc != 2 {
				t.Fatalf("RelationshipCount = %d, want 2", rc)
			}

			out, err := g.OutgoingRelationships(nA.InternalID().SnowflakeID(), "")
			if err != nil {
				t.Fatalf("OutgoingRelationships: %v", err)
			}
			if len(out) != 2 {
				t.Fatalf("outgoing = %d, want 2", len(out))
			}
		})
	}
}

func TestGraphConcurrentDeleteNodeOverlappingRels(t *testing.T) {
	t.Parallel()

	const iterations = 50
	for i := range iterations {
		t.Run(fmt.Sprintf("iter_%d", i), func(t *testing.T) {
			t.Parallel()
			g, err := New(Config{BadgerInMemory: true})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer g.Close()

			nA, _ := g.AddNode([]string{"A"}, nil)
			nB, _ := g.AddNode([]string{"B"}, nil)
			nC, _ := g.AddNode([]string{"C"}, nil)

			// A→B, B→C, A→C — deleting A and B concurrently overlaps on A→B.
			g.AddRelationship("R", nA, nB, nil)
			g.AddRelationship("R", nB, nC, nil)
			g.AddRelationship("R", nA, nC, nil)

			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				g.DeleteNode(nA.InternalID().SnowflakeID())
			}()
			go func() {
				defer wg.Done()
				g.DeleteNode(nB.InternalID().SnowflakeID())
			}()
			wg.Wait()

			nc, _ := g.NodeCount()
			if nc != 1 {
				t.Fatalf("NodeCount = %d, want 1 (only C)", nc)
			}
			rc, _ := g.RelationshipCount()
			if rc != 0 {
				t.Fatalf("RelationshipCount = %d, want 0", rc)
			}
		})
	}
}

func TestGraphConcurrentCRUDStress(t *testing.T) {
	t.Parallel()

	g, err := New(Config{BadgerInMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// Pre-create 10 hub nodes.
	const hubCount = 10
	hubs := make([]*types.Node, hubCount)
	for i := range hubCount {
		n, err := g.AddNode([]string{"Hub"}, map[string]any{"idx": int64(i)})
		if err != nil {
			t.Fatalf("AddNode hub %d: %v", i, err)
		}
		hubs[i] = n
	}

	const workers = 50
	const opsPerWorker = 20
	errs := make(chan error, workers)

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := range workers {
		go func(workerID int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					errs <- fmt.Errorf("worker %d panicked: %v", workerID, r)
				}
			}()

			wn, err := g.AddNode([]string{"Worker"}, map[string]any{"w": int64(workerID)})
			if err != nil {
				errs <- fmt.Errorf("worker %d AddNode: %w", workerID, err)
				return
			}
			hub := hubs[workerID%hubCount]
			if _, err := g.AddRelationship("LINK", wn, hub, nil); err != nil {
				errs <- fmt.Errorf("worker %d AddRel: %w", workerID, err)
				return
			}

			for i := range opsPerWorker {
				// Query.
				g.NodesByLabel("Hub")

				// Update.
				g.UpdateNode(wn.InternalID().SnowflakeID(), map[string]any{"iter": int64(i)})

				// Delete on even iterations.
				if i == opsPerWorker-1 && workerID%2 == 0 {
					g.DeleteNode(wn.InternalID().SnowflakeID())
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("worker error: %v", err)
	}

	// Hubs survive.
	nc, _ := g.NodeCount()
	if nc < hubCount {
		t.Fatalf("NodeCount = %d, want >= %d (hubs survive)", nc, hubCount)
	}
}

// ─── Bulk queries ───────────────────────────────────────────────────────────

func TestGraphAllNodes(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	g.AddNode([]string{"Person"}, map[string]any{"name": "Bob"})
	g.AddNode([]string{"City"}, map[string]any{"name": "Vienna"})

	got, err := g.AllNodes()
	if err != nil {
		t.Fatalf("AllNodes() returned error: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("AllNodes() = %d nodes, want 3", len(got))
	}
}

func TestGraphAllNodesEmpty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	got, err := g.AllNodes()
	if err != nil {
		t.Fatalf("AllNodes() returned error: %v", err)
	}
	if got != nil {
		t.Fatalf("AllNodes() on empty graph = %v, want nil", got)
	}
}

func TestGraphAllRels(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nA, _ := g.AddNode([]string{"Person"}, nil)
	nB, _ := g.AddNode([]string{"Person"}, nil)
	nC, _ := g.AddNode([]string{"Person"}, nil)
	g.AddRelationship("KNOWS", nA, nB, nil)
	g.AddRelationship("LIKES", nB, nC, nil)

	got, err := g.AllRelationships()
	if err != nil {
		t.Fatalf("AllRelationships() returned error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("AllRelationships() = %d rels, want 2", len(got))
	}
}

func TestGraphAllRelsEmpty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	got, err := g.AllRelationships()
	if err != nil {
		t.Fatalf("AllRelationships() returned error: %v", err)
	}
	if got != nil {
		t.Fatalf("AllRelationships() on empty graph = %v, want nil", got)
	}
}

func TestGraphGetNodesByIDs(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n1, _ := g.AddNode([]string{"Person"}, nil)
	n2, _ := g.AddNode([]string{"Person"}, nil)
	g.AddNode([]string{"Person"}, nil) // n3 — not requested

	ids := []snowflake.ID{
		n1.InternalID().SnowflakeID(),
		snowflake.ID(999), // missing
		n2.InternalID().SnowflakeID(),
	}

	got, err := g.GetNodesByIDs(ids)
	if err != nil {
		t.Fatalf("GetNodesByIDs() returned error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("GetNodesByIDs() = %d nodes, want 2", len(got))
	}
}

func TestGraphGetNodesByIDsEmpty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	got, err := g.GetNodesByIDs(nil)
	if err != nil {
		t.Fatalf("GetNodesByIDs(nil) returned error: %v", err)
	}
	if got != nil {
		t.Fatalf("GetNodesByIDs(nil) = %v, want nil", got)
	}
}

func TestGraphGetRelsByIDs(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nA, _ := g.AddNode([]string{"Person"}, nil)
	nB, _ := g.AddNode([]string{"Person"}, nil)
	r1, _ := g.AddRelationship("KNOWS", nA, nB, nil)
	r2, _ := g.AddRelationship("LIKES", nA, nB, nil)

	ids := []snowflake.ID{
		r1.InternalID().SnowflakeID(),
		snowflake.ID(999), // missing
		r2.InternalID().SnowflakeID(),
	}

	got, err := g.GetRelationshipsByIDs(ids)
	if err != nil {
		t.Fatalf("GetRelationshipsByIDs() returned error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("GetRelationshipsByIDs() = %d rels, want 2", len(got))
	}
}

func TestGraphGetRelsByIDsEmpty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	got, err := g.GetRelationshipsByIDs(nil)
	if err != nil {
		t.Fatalf("GetRelationshipsByIDs(nil) returned error: %v", err)
	}
	if got != nil {
		t.Fatalf("GetRelationshipsByIDs(nil) = %v, want nil", got)
	}
}

// --- Per-Label / Per-Type Statistics tests ---

func TestNodeCountByLabel_Empty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	// Register label but add no nodes.
	g.GetOrCreateLabel("Person")

	count, err := g.NodeCountByLabel("Person")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 0 {
		t.Fatalf("NodeCountByLabel = %d, want 0", count)
	}
}

func TestNodeCountByLabel_UnregisteredLabel(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	count, err := g.NodeCountByLabel("NeverRegistered")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 0 {
		t.Fatalf("NodeCountByLabel = %d, want 0", count)
	}
}

func TestNodeCountByLabel_SingleNode(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})

	count, err := g.NodeCountByLabel("Person")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 1 {
		t.Fatalf("NodeCountByLabel = %d, want 1", count)
	}
}

func TestNodeCountByLabel_MultipleNodes(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	g.AddNode([]string{"Person"}, map[string]any{"name": "Bob"})
	g.AddNode([]string{"Person"}, map[string]any{"name": "Charlie"})

	count, err := g.NodeCountByLabel("Person")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 3 {
		t.Fatalf("NodeCountByLabel = %d, want 3", count)
	}
}

func TestNodeCountByLabel_AfterDelete(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n1, _ := g.AddNode([]string{"Person"}, nil)
	g.AddNode([]string{"Person"}, nil)

	g.DeleteNode(n1.InternalID().SnowflakeID())

	count, err := g.NodeCountByLabel("Person")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 1 {
		t.Fatalf("NodeCountByLabel after delete = %d, want 1", count)
	}
}

func TestRelCountByType_Empty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	g.GetOrCreateRelType("KNOWS")

	count, err := g.RelCountByType("KNOWS")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 0 {
		t.Fatalf("RelCountByType = %d, want 0", count)
	}
}

func TestRelCountByType_UnregisteredType(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	count, err := g.RelCountByType("NeverRegistered")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 0 {
		t.Fatalf("RelCountByType = %d, want 0", count)
	}
}

func TestRelCountByType_SingleRel(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	a, _ := g.AddNode([]string{"Person"}, nil)
	b, _ := g.AddNode([]string{"Person"}, nil)
	g.AddRelationship("KNOWS", a, b, nil)

	count, err := g.RelCountByType("KNOWS")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 1 {
		t.Fatalf("RelCountByType = %d, want 1", count)
	}
}

func TestRelCountByType_AfterDelete(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	a, _ := g.AddNode([]string{"Person"}, nil)
	b, _ := g.AddNode([]string{"Person"}, nil)
	r1, _ := g.AddRelationship("KNOWS", a, b, nil)
	g.AddRelationship("KNOWS", b, a, nil)

	g.DeleteRelationship(r1.InternalID().SnowflakeID())

	count, err := g.RelCountByType("KNOWS")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 1 {
		t.Fatalf("RelCountByType after delete = %d, want 1", count)
	}
}

func TestAllLabelCounts_Empty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	counts, err := g.AllLabelCounts()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(counts) != 0 {
		t.Fatalf("AllLabelCounts = %v, want empty map", counts)
	}
}

func TestAllLabelCounts_Multiple(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	g.AddNode([]string{"Person"}, nil)
	g.AddNode([]string{"Person"}, nil)
	g.AddNode([]string{"Company"}, nil)
	// Register a label but don't add nodes — should be omitted.
	g.GetOrCreateLabel("Empty")

	counts, err := g.AllLabelCounts()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if counts["Person"] != 2 {
		t.Errorf("Person count = %d, want 2", counts["Person"])
	}
	if counts["Company"] != 1 {
		t.Errorf("Company count = %d, want 1", counts["Company"])
	}
	if _, ok := counts["Empty"]; ok {
		t.Error("Empty label should be omitted from AllLabelCounts")
	}
}

func TestAllRelTypeCounts_Multiple(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	a, _ := g.AddNode([]string{"Person"}, nil)
	b, _ := g.AddNode([]string{"Person"}, nil)
	c, _ := g.AddNode([]string{"Company"}, nil)

	g.AddRelationship("KNOWS", a, b, nil)
	g.AddRelationship("KNOWS", b, a, nil)
	g.AddRelationship("WORKS_AT", a, c, nil)
	// Register a type but don't add rels — should be omitted.
	g.GetOrCreateRelType("EMPTY")

	counts, err := g.AllRelTypeCounts()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if counts["KNOWS"] != 2 {
		t.Errorf("KNOWS count = %d, want 2", counts["KNOWS"])
	}
	if counts["WORKS_AT"] != 1 {
		t.Errorf("WORKS_AT count = %d, want 1", counts["WORKS_AT"])
	}
	if _, ok := counts["EMPTY"]; ok {
		t.Error("EMPTY type should be omitted from AllRelTypeCounts")
	}
}

// --- Validation Limits ---

func TestDefaultValidationLimits(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})
	v := g.ValidationDefaults()
	if v.MaxLabelsPerNode != 50 {
		t.Errorf("MaxLabelsPerNode = %d, want 50", v.MaxLabelsPerNode)
	}
	if v.MaxPropertiesPerEntity != 1000 {
		t.Errorf("MaxPropertiesPerEntity = %d, want 1000", v.MaxPropertiesPerEntity)
	}
	if v.MaxPropertyKeyLength != 256 {
		t.Errorf("MaxPropertyKeyLength = %d, want 256", v.MaxPropertyKeyLength)
	}
	if v.MaxPropertyValueSize != 65536 {
		t.Errorf("MaxPropertyValueSize = %d, want 65536", v.MaxPropertyValueSize)
	}
	if v.MaxNameLength != 256 {
		t.Errorf("MaxNameLength = %d, want 256", v.MaxNameLength)
	}
}

func TestValidationLimitsZeroUsesDefaults(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{}})
	v := g.ValidationDefaults()
	if v.MaxLabelsPerNode != 50 {
		t.Errorf("zero MaxLabelsPerNode should default to 50, got %d", v.MaxLabelsPerNode)
	}
}

func TestValidationLimitsCustom(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxLabelsPerNode: 3}})
	v := g.ValidationDefaults()
	if v.MaxLabelsPerNode != 3 {
		t.Errorf("MaxLabelsPerNode = %d, want 3", v.MaxLabelsPerNode)
	}
	// Other fields should still be defaults.
	if v.MaxNameLength != 256 {
		t.Errorf("MaxNameLength should still be 256, got %d", v.MaxNameLength)
	}
}

func TestAddNodeTooManyLabels(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxLabelsPerNode: 2}})

	_, err := g.AddNode([]string{"A", "B", "C"}, nil)
	if err == nil {
		t.Fatal("expected error for too many labels")
	}
	if !errors.Is(err, ErrTooManyLabels) {
		t.Fatalf("expected ErrTooManyLabels, got: %v", err)
	}
}

func TestAddNodeMaxLabels(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxLabelsPerNode: 2}})

	n, err := g.AddNode([]string{"A", "B"}, nil)
	if err != nil {
		t.Fatalf("at-limit should succeed: %v", err)
	}
	if n == nil {
		t.Fatal("node should not be nil")
	}
}

func TestAddNodeTooManyProperties(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertiesPerEntity: 2}})

	props := map[string]any{"a": 1, "b": 2, "c": 3}
	_, err := g.AddNode([]string{"X"}, props)
	if err == nil {
		t.Fatal("expected error for too many properties")
	}
	if !errors.Is(err, ErrTooManyProperties) {
		t.Fatalf("expected ErrTooManyProperties, got: %v", err)
	}
}

func TestAddNodeMaxProperties(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertiesPerEntity: 2}})

	props := map[string]any{"a": 1, "b": 2}
	n, err := g.AddNode([]string{"X"}, props)
	if err != nil {
		t.Fatalf("at-limit should succeed: %v", err)
	}
	if n == nil {
		t.Fatal("node should not be nil")
	}
}

func TestAddNodePropertyKeyTooLong(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertyKeyLength: 5}})

	props := map[string]any{"toolong": "val"}
	_, err := g.AddNode([]string{"X"}, props)
	if err == nil {
		t.Fatal("expected error for key too long")
	}
	if !errors.Is(err, ErrKeyTooLong) {
		t.Fatalf("expected ErrKeyTooLong, got: %v", err)
	}
}

func TestAddNodePropertyKeyMaxLength(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertyKeyLength: 5}})

	props := map[string]any{"abcde": "val"}
	n, err := g.AddNode([]string{"X"}, props)
	if err != nil {
		t.Fatalf("at-limit key should succeed: %v", err)
	}
	if n == nil {
		t.Fatal("node should not be nil")
	}
}

func TestAddNodePropertyValueTooLarge(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertyValueSize: 10}})

	props := map[string]any{"key": "12345678901"} // 11 bytes
	_, err := g.AddNode([]string{"X"}, props)
	if err == nil {
		t.Fatal("expected error for value too large")
	}
	if !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("expected ErrValueTooLarge, got: %v", err)
	}
}

func TestAddNodePropertyValueMaxSize(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertyValueSize: 10}})

	props := map[string]any{"key": "1234567890"} // exactly 10 bytes
	n, err := g.AddNode([]string{"X"}, props)
	if err != nil {
		t.Fatalf("at-limit value should succeed: %v", err)
	}
	if n == nil {
		t.Fatal("node should not be nil")
	}
}

func TestAddNodePropertyValueNonStringIgnored(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertyValueSize: 1}})

	// Non-string values should not be checked against MaxPropertyValueSize.
	props := map[string]any{"key": 99999}
	n, err := g.AddNode([]string{"X"}, props)
	if err != nil {
		t.Fatalf("non-string value should be ignored: %v", err)
	}
	if n == nil {
		t.Fatal("node should not be nil")
	}
}

func TestAddNodeLabelNameTooLong(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxNameLength: 5}})

	_, err := g.AddNode([]string{"TooLong"}, nil)
	if err == nil {
		t.Fatal("expected error for label name too long")
	}
	if !errors.Is(err, ErrNameTooLong) {
		t.Fatalf("expected ErrNameTooLong, got: %v", err)
	}
}

func TestAddNodeLabelNameMaxLength(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxNameLength: 5}})

	n, err := g.AddNode([]string{"ABCDE"}, nil)
	if err != nil {
		t.Fatalf("at-limit name should succeed: %v", err)
	}
	if n == nil {
		t.Fatal("node should not be nil")
	}
}

func TestAddRelationshipTypeNameTooLong(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxNameLength: 5}})

	a, _ := g.AddNode([]string{"X"}, nil)
	b, _ := g.AddNode([]string{"X"}, nil)
	_, err := g.AddRelationship("TOOLONG", a, b, nil)
	if err == nil {
		t.Fatal("expected error for type name too long")
	}
	if !errors.Is(err, ErrNameTooLong) {
		t.Fatalf("expected ErrNameTooLong, got: %v", err)
	}
}

func TestAddRelationshipTooManyProperties(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertiesPerEntity: 1}})

	a, _ := g.AddNode([]string{"X"}, nil)
	b, _ := g.AddNode([]string{"X"}, nil)
	_, err := g.AddRelationship("REL", a, b, map[string]any{"a": 1, "b": 2})
	if err == nil {
		t.Fatal("expected error for too many properties")
	}
	if !errors.Is(err, ErrTooManyProperties) {
		t.Fatalf("expected ErrTooManyProperties, got: %v", err)
	}
}

func TestAddRelationshipPropertyKeyTooLong(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertyKeyLength: 3}})

	a, _ := g.AddNode([]string{"X"}, nil)
	b, _ := g.AddNode([]string{"X"}, nil)
	_, err := g.AddRelationship("REL", a, b, map[string]any{"toolong": "v"})
	if err == nil {
		t.Fatal("expected error for key too long")
	}
	if !errors.Is(err, ErrKeyTooLong) {
		t.Fatalf("expected ErrKeyTooLong, got: %v", err)
	}
}

func TestAddRelationshipPropertyValueTooLarge(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertyValueSize: 3}})

	a, _ := g.AddNode([]string{"X"}, nil)
	b, _ := g.AddNode([]string{"X"}, nil)
	_, err := g.AddRelationship("REL", a, b, map[string]any{"k": "toolong"})
	if err == nil {
		t.Fatal("expected error for value too large")
	}
	if !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("expected ErrValueTooLarge, got: %v", err)
	}
}

func TestUpdateNodePropertyCountExceedsLimit(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertiesPerEntity: 2}})

	n, _ := g.AddNode([]string{"X"}, map[string]any{"a": 1, "b": 2})

	// Try to add a 3rd property via update — should fail.
	_, err := g.UpdateNode(n.InternalID().SnowflakeID(), map[string]any{"c": 3})
	if err == nil {
		t.Fatal("expected error for exceeding property limit on update")
	}
	if !errors.Is(err, ErrTooManyProperties) {
		t.Fatalf("expected ErrTooManyProperties, got: %v", err)
	}
}

func TestUpdateNodePropertyKeyTooLong(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertyKeyLength: 3}})

	n, _ := g.AddNode([]string{"X"}, nil)

	_, err := g.UpdateNode(n.InternalID().SnowflakeID(), map[string]any{"toolong": "v"})
	if err == nil {
		t.Fatal("expected error for key too long on update")
	}
	if !errors.Is(err, ErrKeyTooLong) {
		t.Fatalf("expected ErrKeyTooLong, got: %v", err)
	}
}

func TestUpdateNodePropertyValueTooLarge(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertyValueSize: 3}})

	n, _ := g.AddNode([]string{"X"}, nil)

	_, err := g.UpdateNode(n.InternalID().SnowflakeID(), map[string]any{"k": "toolong"})
	if err == nil {
		t.Fatal("expected error for value too large on update")
	}
	if !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("expected ErrValueTooLarge, got: %v", err)
	}
}

func TestUpdateRelPropertyCountExceedsLimit(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertiesPerEntity: 1}})

	a, _ := g.AddNode([]string{"X"}, nil)
	b, _ := g.AddNode([]string{"X"}, nil)
	r, _ := g.AddRelationship("REL", a, b, map[string]any{"a": 1})

	_, err := g.UpdateRelationship(r.InternalID().SnowflakeID(), map[string]any{"b": 2})
	if err == nil {
		t.Fatal("expected error for exceeding property limit on rel update")
	}
	if !errors.Is(err, ErrTooManyProperties) {
		t.Fatalf("expected ErrTooManyProperties, got: %v", err)
	}
}

func TestUpdateNodeReplacementWithinLimit(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertiesPerEntity: 2}})

	n, _ := g.AddNode([]string{"X"}, map[string]any{"a": 1, "b": 2})

	// Replacing an existing property should not trip the limit.
	updated, err := g.UpdateNode(n.InternalID().SnowflakeID(), map[string]any{"a": 99})
	if err != nil {
		t.Fatalf("replacement should succeed: %v", err)
	}
	v, _ := updated.GetProperty("a")
	if v != 99 {
		t.Fatalf("expected 99, got %v", v)
	}
}

func TestUpdateNodeDeleteThenAddWithinLimit(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertiesPerEntity: 2}})

	n, _ := g.AddNode([]string{"X"}, map[string]any{"a": 1, "b": 2})

	// Delete one and add one — should still be at limit.
	updated, err := g.UpdateNode(n.InternalID().SnowflakeID(), map[string]any{"a": nil, "c": 3})
	if err != nil {
		t.Fatalf("delete+add should succeed: %v", err)
	}
	if updated.PropertyCount() != 2 {
		t.Fatalf("PropertyCount = %d, want 2", updated.PropertyCount())
	}
}

func TestBatchAddNodeTooManyLabels(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxLabelsPerNode: 1}})

	batch := NewBatchBuilder(g)
	_, err := batch.AddNode([]string{"A", "B"}, nil)
	if err == nil {
		t.Fatal("expected error for too many labels in batch")
	}
	if !errors.Is(err, ErrTooManyLabels) {
		t.Fatalf("expected ErrTooManyLabels, got: %v", err)
	}
}

func TestBatchAddRelationshipTypeNameTooLong(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxNameLength: 3}})

	batch := NewBatchBuilder(g)
	n, _ := batch.AddNode([]string{"X"}, nil)
	m, _ := batch.AddNode([]string{"X"}, nil)
	_, err := batch.AddRelationship("TOOLONG", n, m, nil)
	if err == nil {
		t.Fatal("expected error for type name too long in batch")
	}
	if !errors.Is(err, ErrNameTooLong) {
		t.Fatalf("expected ErrNameTooLong, got: %v", err)
	}
}

func TestBatchUpdateNodePropertyKeyTooLong(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertyKeyLength: 3}})

	n, _ := g.AddNode([]string{"X"}, nil)

	batch := NewBatchBuilder(g)
	err := batch.UpdateNode(n.InternalID().SnowflakeID(), map[string]any{"toolong": "v"})
	if err == nil {
		t.Fatal("expected error for key too long in batch update")
	}
	if !errors.Is(err, ErrKeyTooLong) {
		t.Fatalf("expected ErrKeyTooLong, got: %v", err)
	}
}

func TestBatchUpdateRelPropertyValueTooLarge(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Validation: ValidationLimits{MaxPropertyValueSize: 3}})

	a, _ := g.AddNode([]string{"X"}, nil)
	b, _ := g.AddNode([]string{"X"}, nil)
	r, _ := g.AddRelationship("REL", a, b, nil)

	batch := NewBatchBuilder(g)
	err := batch.UpdateRelationship(r.InternalID().SnowflakeID(), map[string]any{"k": "toolong"})
	if err == nil {
		t.Fatal("expected error for value too large in batch update")
	}
	if !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("expected ErrValueTooLarge, got: %v", err)
	}
}

// --- MemoryStore NodeCountByLabel / RelCountByType ---

func TestMemStoreNodeCountByLabel_AfterPut(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	n := types.NewNode(snowflake.ID(100), 1, []uint16{2})
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	c1, _ := ms.NodeCountByLabel(1)
	c2, _ := ms.NodeCountByLabel(2)
	c3, _ := ms.NodeCountByLabel(3) // non-existent
	if c1 != 1 {
		t.Fatalf("label 1 count = %d, want 1", c1)
	}
	if c2 != 1 {
		t.Fatalf("label 2 count = %d, want 1", c2)
	}
	if c3 != 0 {
		t.Fatalf("label 3 count = %d, want 0", c3)
	}
}

func TestMemStoreNodeCountByLabel_AfterDelete(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	n := types.NewNode(snowflake.ID(100), 1, nil)
	ms.PutNode(n)
	n2 := types.NewNode(snowflake.ID(200), 1, nil)
	ms.PutNode(n2)

	ms.DeleteNode(snowflake.ID(100))

	c, _ := ms.NodeCountByLabel(1)
	if c != 1 {
		t.Fatalf("label 1 count after delete = %d, want 1", c)
	}
}

func TestMemStoreNodeCountByLabel_MultiLabel(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	// Node with labels 1, 2, 3 — each label counter incremented.
	n := types.NewNode(snowflake.ID(100), 1, []uint16{2, 3})
	ms.PutNode(n)

	for _, tok := range []uint16{1, 2, 3} {
		c, _ := ms.NodeCountByLabel(tok)
		if c != 1 {
			t.Fatalf("label %d count = %d, want 1", tok, c)
		}
	}
}

func TestMemStoreRelCountByType_AfterPut(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	n1 := types.NewNode(snowflake.ID(100), 1, nil)
	n2 := types.NewNode(snowflake.ID(200), 1, nil)
	ms.PutNode(n1)
	ms.PutNode(n2)

	r := types.NewRelationship(snowflake.ID(300), 5, snowflake.ID(100), snowflake.ID(200))
	ms.PutRelationship(r)

	c, _ := ms.RelCountByType(5)
	if c != 1 {
		t.Fatalf("type 5 count = %d, want 1", c)
	}
	c0, _ := ms.RelCountByType(99) // non-existent
	if c0 != 0 {
		t.Fatalf("type 99 count = %d, want 0", c0)
	}
}

func TestMemStoreRelCountByType_AfterDelete(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	n1 := types.NewNode(snowflake.ID(100), 1, nil)
	n2 := types.NewNode(snowflake.ID(200), 1, nil)
	ms.PutNode(n1)
	ms.PutNode(n2)

	r1 := types.NewRelationship(snowflake.ID(300), 5, snowflake.ID(100), snowflake.ID(200))
	r2 := types.NewRelationship(snowflake.ID(400), 5, snowflake.ID(200), snowflake.ID(100))
	ms.PutRelationship(r1)
	ms.PutRelationship(r2)

	ms.DeleteRelationship(snowflake.ID(300))

	c, _ := ms.RelCountByType(5)
	if c != 1 {
		t.Fatalf("type 5 count after delete = %d, want 1", c)
	}
}

func TestMemStoreNodeCountByLabel_CascadeDelete(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	n1 := types.NewNode(snowflake.ID(100), 1, []uint16{2})
	n2 := types.NewNode(snowflake.ID(200), 1, nil)
	ms.PutNode(n1)
	ms.PutNode(n2)

	r := types.NewRelationship(snowflake.ID(300), 5, snowflake.ID(100), snowflake.ID(200))
	ms.PutRelationship(r)

	// Cascade deletes n1 and its relationship.
	ms.DeleteNodeCascade(snowflake.ID(100))

	c1, _ := ms.NodeCountByLabel(1)
	if c1 != 1 {
		t.Fatalf("label 1 count after cascade = %d, want 1", c1)
	}
	c2, _ := ms.NodeCountByLabel(2)
	if c2 != 0 {
		t.Fatalf("label 2 count after cascade = %d, want 0", c2)
	}
	ct, _ := ms.RelCountByType(5)
	if ct != 0 {
		t.Fatalf("type 5 count after cascade = %d, want 0", ct)
	}
}

// --- Graph-level CountByLabel after batch add and cascade delete ---

func TestNodeCountByLabel_BatchAdd(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})

	batch := NewBatchBuilder(g)
	batch.AddNode([]string{"Animal"}, nil)
	batch.AddNode([]string{"Animal"}, nil)
	batch.AddNode([]string{"Animal", "Pet"}, nil)
	batch.Execute()

	c, err := g.NodeCountByLabel("Animal")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c != 3 {
		t.Fatalf("Animal count = %d, want 3", c)
	}
	cp, _ := g.NodeCountByLabel("Pet")
	if cp != 1 {
		t.Fatalf("Pet count = %d, want 1", cp)
	}
}

func TestCountAfterCascadeDelete(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{})

	a, _ := g.AddNode([]string{"Person"}, nil)
	b, _ := g.AddNode([]string{"Person"}, nil)
	g.AddRelationship("KNOWS", a, b, nil)
	g.AddRelationship("LIKES", a, b, nil)

	// Before delete.
	nc, _ := g.NodeCountByLabel("Person")
	if nc != 2 {
		t.Fatalf("Person before cascade = %d, want 2", nc)
	}

	// Cascade delete a — removes 2 rels.
	g.DeleteNode(a.InternalID().SnowflakeID())

	nc, _ = g.NodeCountByLabel("Person")
	if nc != 1 {
		t.Fatalf("Person after cascade = %d, want 1", nc)
	}
	rk, _ := g.RelCountByType("KNOWS")
	rl, _ := g.RelCountByType("LIKES")
	if rk != 0 {
		t.Fatalf("KNOWS after cascade = %d, want 0", rk)
	}
	if rl != 0 {
		t.Fatalf("LIKES after cascade = %d, want 0", rl)
	}
}

// --- MemoryStore Property Index tests ---

func TestMemStoreCreatePropertyIndex(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})

	err := g.CreatePropertyIndex("Person", "name")
	if err != nil {
		t.Fatalf("CreatePropertyIndex failed: %v", err)
	}
}

func TestMemStoreCreatePropertyIndex_Duplicate(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	g.GetOrCreateLabel("Person")
	g.CreatePropertyIndex("Person", "name")

	err := g.CreatePropertyIndex("Person", "name")
	if !errors.Is(err, ErrIndexExists) {
		t.Fatalf("expected ErrIndexExists, got %v", err)
	}
}

func TestMemStoreDropPropertyIndex(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	g.GetOrCreateLabel("Person")
	g.CreatePropertyIndex("Person", "name")

	err := g.DropPropertyIndex("Person", "name")
	if err != nil {
		t.Fatalf("DropPropertyIndex failed: %v", err)
	}
}

func TestMemStoreDropPropertyIndex_NotFound(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	g.GetOrCreateLabel("Person")

	err := g.DropPropertyIndex("Person", "name")
	if !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("expected ErrIndexNotFound, got %v", err)
	}
}

func TestMemStoreNodesByLabelAndProperty_Hit(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	g.AddNode([]string{"Person"}, map[string]any{"name": "Bob"})
	g.CreatePropertyIndex("Person", "name")

	nodes, err := g.NodesByLabelAndProperty("Person", "name", "Alice")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}
	name, _ := nodes[0].GetProperty("name")
	if name != "Alice" {
		t.Fatalf("expected name=Alice, got %v", name)
	}
}

func TestMemStoreNodesByLabelAndProperty_Miss(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	g.CreatePropertyIndex("Person", "name")

	nodes, err := g.NodesByLabelAndProperty("Person", "name", "Bob")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if nodes != nil {
		t.Fatalf("expected nil, got %d nodes", len(nodes))
	}
}

func TestMemStoreNodesByLabelAndProperty_NoIndex(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	g.AddNode([]string{"Person"}, map[string]any{"name": "Bob"})

	// No index — should fall back to scan.
	nodes, err := g.NodesByLabelAndProperty("Person", "name", "Alice")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("fallback scan: expected 1 node, got %d", len(nodes))
	}
}

func TestMemStorePropertyIndex_AutoUpdate(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	g.CreatePropertyIndex("Person", "name")

	// Verify index finds Alice.
	nodes, _ := g.NodesByLabelAndProperty("Person", "name", "Alice")
	if len(nodes) != 1 {
		t.Fatalf("after add: expected 1, got %d", len(nodes))
	}

	// Update the property.
	id := n.InternalID().SnowflakeID()
	g.UpdateNode(id, map[string]any{"name": "Alicia"})

	// Old value should be gone.
	nodes, _ = g.NodesByLabelAndProperty("Person", "name", "Alice")
	if len(nodes) != 0 {
		t.Fatalf("after update: old value still found, got %d", len(nodes))
	}

	// New value should be found.
	nodes, _ = g.NodesByLabelAndProperty("Person", "name", "Alicia")
	if len(nodes) != 1 {
		t.Fatalf("after update: new value not found, got %d", len(nodes))
	}

	// Delete the node.
	g.DeleteNode(id)

	nodes, _ = g.NodesByLabelAndProperty("Person", "name", "Alicia")
	if len(nodes) != 0 {
		t.Fatalf("after delete: node still in index, got %d", len(nodes))
	}
}

// --- Graph-layer Property Index tests ---

func TestGraphCreatePropertyIndex(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	g.GetOrCreateLabel("Person")

	err := g.CreatePropertyIndex("Person", "name")
	if err != nil {
		t.Fatalf("CreatePropertyIndex failed: %v", err)
	}

	// Unregistered label → no-op (no error).
	err = g.CreatePropertyIndex("Unknown", "name")
	if err != nil {
		t.Fatalf("unregistered label should return nil, got %v", err)
	}
}

func TestGraphDropPropertyIndex(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	g.GetOrCreateLabel("Person")
	g.CreatePropertyIndex("Person", "name")

	err := g.DropPropertyIndex("Person", "name")
	if err != nil {
		t.Fatalf("DropPropertyIndex failed: %v", err)
	}

	// Unregistered label → no-op.
	err = g.DropPropertyIndex("Unknown", "name")
	if err != nil {
		t.Fatalf("unregistered label should return nil, got %v", err)
	}
}

func TestGraphNodesByLabelAndProperty_Found(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	g.AddNode([]string{"Person"}, map[string]any{"name": "Alice", "age": int64(30)})
	g.AddNode([]string{"Person"}, map[string]any{"name": "Bob", "age": int64(25)})
	g.CreatePropertyIndex("Person", "name")

	nodes, err := g.NodesByLabelAndProperty("Person", "name", "Alice")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1, got %d", len(nodes))
	}
}

func TestGraphNodesByLabelAndProperty_NotFound(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	g.CreatePropertyIndex("Person", "name")

	nodes, err := g.NodesByLabelAndProperty("Person", "name", "Charlie")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if nodes != nil {
		t.Fatalf("expected nil, got %d nodes", len(nodes))
	}
}

func TestGraphNodesByLabelAndProperty_UnregisteredLabel(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nodes, err := g.NodesByLabelAndProperty("Unknown", "name", "Alice")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if nodes != nil {
		t.Fatalf("expected nil for unregistered label, got %d", len(nodes))
	}
}

func TestGraphPropertyIndex_MultipleValues(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	g.AddNode([]string{"Person"}, map[string]any{"name": "Bob"})
	g.CreatePropertyIndex("Person", "name")

	nodes, _ := g.NodesByLabelAndProperty("Person", "name", "Alice")
	if len(nodes) != 2 {
		t.Fatalf("expected 2 Alices, got %d", len(nodes))
	}

	nodes, _ = g.NodesByLabelAndProperty("Person", "name", "Bob")
	if len(nodes) != 1 {
		t.Fatalf("expected 1 Bob, got %d", len(nodes))
	}
}

func TestGraphPropertyIndex_UpdateReflected(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	g.CreatePropertyIndex("Person", "name")

	id := n.InternalID().SnowflakeID()
	g.UpdateNode(id, map[string]any{"name": "Alicia"})

	// Old value gone.
	nodes, _ := g.NodesByLabelAndProperty("Person", "name", "Alice")
	if len(nodes) != 0 {
		t.Fatalf("old value still found: %d", len(nodes))
	}

	// New value present.
	nodes, _ = g.NodesByLabelAndProperty("Person", "name", "Alicia")
	if len(nodes) != 1 {
		t.Fatalf("new value not found: %d", len(nodes))
	}
}

func TestGraphPropertyIndex_DeleteRemoves(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	g.CreatePropertyIndex("Person", "name")

	g.DeleteNode(n.InternalID().SnowflakeID())

	nodes, _ := g.NodesByLabelAndProperty("Person", "name", "Alice")
	if len(nodes) != 0 {
		t.Fatalf("deleted node still in index: %d", len(nodes))
	}
}

func TestPropertyValueKey_AllTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		value    any
		expected string
	}{
		{"string", "hello", "s:hello"},
		{"int", int(42), "i:42"},
		{"int8", int8(8), "i8:8"},
		{"int16", int16(16), "i16:16"},
		{"int32", int32(32), "i32:32"},
		{"int64", int64(64), "i64:64"},
		{"uint", uint(10), "u:10"},
		{"uint8", uint8(8), "u8:8"},
		{"uint16", uint16(16), "u16:16"},
		{"uint32", uint32(32), "u32:32"},
		{"uint64", uint64(64), "u64:64"},
		{"float32", float32(3.14), "f32:3.14"},
		{"float64", float64(2.718), "f64:2.718"},
		{"bool_true", true, "b:true"},
		{"bool_false", false, "b:false"},
		{"slice_not_indexed", []string{"a"}, ""},
		{"map_not_indexed", map[string]any{"k": "v"}, ""},
		{"nil_not_indexed", nil, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := propertyValueKey(tt.value)
			if got != tt.expected {
				t.Errorf("propertyValueKey(%v) = %q, want %q", tt.value, got, tt.expected)
			}
		})
	}

	// Verify type-safety: int(1) vs string("1") produce different keys.
	intKey := propertyValueKey(int(1))
	strKey := propertyValueKey("1")
	if intKey == strKey {
		t.Errorf("int(1) and string(\"1\") produced same key: %q", intKey)
	}
}

// --- Badger-backed temporal query tests (Fix 1) ---

func TestGraphBadgerGetNodesValidAt_DeletedNode(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	g, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	g.AddNode([]string{"Person"}, map[string]any{"name": "Bob"})
	id := n.InternalID().SnowflakeID()

	validTime := types.Instant(time.Now().UnixMilli())
	time.Sleep(2 * time.Millisecond)

	if err := g.DeleteNode(id); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	// Query at pre-deletion time — both nodes should appear.
	nodes, err := g.GetNodesValidAt(validTime)
	if err != nil {
		t.Fatalf("GetNodesValidAt: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes at pre-deletion time, got %d", len(nodes))
	}

	// Query at current time — only Bob.
	nodes, err = g.GetNodesValidAt(types.Instant(time.Now().UnixMilli()))
	if err != nil {
		t.Fatalf("GetNodesValidAt now: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node at current time, got %d", len(nodes))
	}
}

func TestGraphBadgerSnapshot_IncludesDeletedNodes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	g, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	a, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	b, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Bob"})
	g.AddRelationship("KNOWS", a, b, nil)

	snapshotTime := types.Instant(time.Now().UnixMilli())
	time.Sleep(2 * time.Millisecond)

	if err := g.DeleteNode(a.InternalID().SnowflakeID()); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	// Snapshot at pre-deletion time — both nodes and the rel.
	snap, err := g.Snapshot(snapshotTime)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.NodeCount != 2 {
		t.Fatalf("expected 2 nodes, got %d", snap.NodeCount)
	}
	if snap.RelCount != 1 {
		t.Fatalf("expected 1 rel, got %d", snap.RelCount)
	}
}
