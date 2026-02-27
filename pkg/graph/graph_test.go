package graph

import (
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"

	snowflake "gitlab2024.bds421-cloud.com/bds421/rho/snowflake-2026"
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
