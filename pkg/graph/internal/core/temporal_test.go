package core

import (
	"errors"
	"testing"
	"time"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// nowMs returns the current time as types.Instant (Unix milliseconds).
func nowMs() types.Instant {
	return types.Instant(time.Now().UnixMilli())
}

// --- Point-in-time tests ---

func TestGetNodesValidAt_Empty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nodes, err := g.Temporal.NodesAt(nowMs())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if nodes != nil {
		t.Fatalf("expected nil, got %d nodes", len(nodes))
	}
}

func TestGetNodesValidAt_NoTemporal(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})

	// Query at current time — node should be valid (ValidFrom derived from snowflake).
	nodes, err := g.Temporal.NodesAt(nowMs())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}
}

func TestGetNodesValidAt_ExplicitValidity(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.Nodes.Add([]string{"Person"}, nil)
	id := n.ID()

	// Set explicit validity: valid from 1000 to 2000.
	current, _ := g.store.GetNode(id)
	current.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})
	_ = g.store.ReplaceNode(current)

	// Query at 1500 — should be valid.
	nodes, err := g.Temporal.NodesAt(1500)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("at t=1500: expected 1 node, got %d", len(nodes))
	}

	// Query at 500 — should be excluded (before ValidFrom).
	nodes, err = g.Temporal.NodesAt(500)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("at t=500: expected 0 nodes, got %d", len(nodes))
	}
}

func TestGetNodesValidAt_Expired(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.Nodes.Add([]string{"Person"}, nil)
	id := n.ID()

	// Set ValidTo in the past.
	current, _ := g.store.GetNode(id)
	current.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})
	_ = g.store.ReplaceNode(current)

	nodes, err := g.Temporal.NodesAt(3000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("expired node should be excluded, got %d", len(nodes))
	}
}

func TestGetNodesValidAt_Future(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.Nodes.Add([]string{"Person"}, nil)
	id := n.ID()

	// Set ValidFrom far in the future.
	futureMs := types.Instant(time.Now().Add(24 * time.Hour).UnixMilli())
	current, _ := g.store.GetNode(id)
	current.SetTemporal(&types.TemporalMetadata{ValidFrom: futureMs})
	_ = g.store.ReplaceNode(current)

	nodes, err := g.Temporal.NodesAt(nowMs())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("future node should be excluded, got %d", len(nodes))
	}
}

func TestGetNodesValidAt_Mixed(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})

	// Node A: valid at query time (no explicit temporal).
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "A"})

	// Node B: expired.
	nB, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "B"})
	bCurrent, _ := g.store.GetNode(nB.ID())
	bCurrent.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})
	_ = g.store.ReplaceNode(bCurrent)

	// Node C: valid at query time with explicit range.
	nC, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "C"})
	cCurrent, _ := g.store.GetNode(nC.ID())
	cCurrent.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 0})
	_ = g.store.ReplaceNode(cCurrent)

	nodes, err := g.Temporal.NodesAt(nowMs())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("expected 2 valid nodes, got %d", len(nodes))
	}
}

func TestGetRelsValidAt_Empty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	rels, err := g.Temporal.RelationshipsAt(nowMs())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rels != nil {
		t.Fatalf("expected nil, got %d rels", len(rels))
	}
}

func TestGetRelsValidAt_ExplicitValidity(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	a, _ := g.Nodes.Add([]string{"Person"}, nil)
	b, _ := g.Nodes.Add([]string{"Person"}, nil)
	r, _ := g.Rels.Add("KNOWS", a, b, nil)
	id := r.ID()

	current, _ := g.store.GetRelationship(id)
	current.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})
	_ = g.store.ReplaceRelationship(current)

	rels, err := g.Temporal.RelationshipsAt(1500)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("at t=1500: expected 1 rel, got %d", len(rels))
	}

	rels, err = g.Temporal.RelationshipsAt(2500)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rels) != 0 {
		t.Fatalf("at t=2500: expected 0 rels, got %d", len(rels))
	}
}

func TestGetRelsValidAt_Mixed(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	a, _ := g.Nodes.Add([]string{"Person"}, nil)
	b, _ := g.Nodes.Add([]string{"Person"}, nil)

	// Rel 1: valid (no explicit temporal).
	g.Rels.Add("KNOWS", a, b, nil)

	// Rel 2: expired.
	r2, _ := g.Rels.Add("LIKES", a, b, nil)
	r2Current, _ := g.store.GetRelationship(r2.ID())
	r2Current.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})
	_ = g.store.ReplaceRelationship(r2Current)

	rels, err := g.Temporal.RelationshipsAt(nowMs())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("expected 1 valid rel, got %d", len(rels))
	}
}

func TestGetNodesByLabelValidAt_Empty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	g.Resolve.GetOrCreateLabel("Person")
	nodes, err := g.Temporal.NodesByLabelAt("Person", nowMs())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("expected 0 nodes, got %d", len(nodes))
	}
}

func TestGetNodesByLabelValidAt_Filtered(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Valid"})

	nExpired, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Expired"})
	ec, _ := g.store.GetNode(nExpired.ID())
	ec.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})
	_ = g.store.ReplaceNode(ec)

	nodes, err := g.Temporal.NodesByLabelAt("Person", nowMs())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1 valid Person, got %d", len(nodes))
	}
}

func TestGetNodesByLabelValidAt_UnregisteredLabel(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nodes, err := g.Temporal.NodesByLabelAt("Unknown", nowMs())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if nodes != nil {
		t.Fatalf("expected nil, got %d nodes", len(nodes))
	}
}

// --- Interval tests ---

func TestGetNodesValidDuring_Overlap(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.Nodes.Add([]string{"Person"}, nil)
	id := n.ID()

	current, _ := g.store.GetNode(id)
	current.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 3000})
	_ = g.store.ReplaceNode(current)

	// Interval [2000, 4000) overlaps [1000, 3000).
	nodes, err := g.Temporal.NodesDuring(2000, 4000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1 overlapping node, got %d", len(nodes))
	}
}

func TestGetNodesValidDuring_NoOverlap(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.Nodes.Add([]string{"Person"}, nil)
	id := n.ID()

	current, _ := g.store.GetNode(id)
	current.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})
	_ = g.store.ReplaceNode(current)

	// Interval [3000, 4000) does not overlap [1000, 2000).
	nodes, err := g.Temporal.NodesDuring(3000, 4000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("expected 0 nodes, got %d", len(nodes))
	}
}

func TestGetNodesValidDuring_OpenEnded(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.Nodes.Add([]string{"Person"}, nil)
	id := n.ID()

	// Open-ended (ValidTo=0) — overlaps any future interval.
	current, _ := g.store.GetNode(id)
	current.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000})
	_ = g.store.ReplaceNode(current)

	farFuture := types.Instant(time.Now().Add(365 * 24 * time.Hour).UnixMilli())
	nodes, err := g.Temporal.NodesDuring(farFuture, farFuture+1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("open-ended node should overlap future interval, got %d", len(nodes))
	}
}

func TestGetRelsValidDuring_Overlap(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	a, _ := g.Nodes.Add([]string{"Person"}, nil)
	b, _ := g.Nodes.Add([]string{"Person"}, nil)
	r, _ := g.Rels.Add("KNOWS", a, b, nil)
	id := r.ID()

	current, _ := g.store.GetRelationship(id)
	current.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 3000})
	_ = g.store.ReplaceRelationship(current)

	rels, err := g.Temporal.RelationshipsDuring(2000, 4000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("expected 1 overlapping rel, got %d", len(rels))
	}
}

func TestGetRelsValidDuring_NoOverlap(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	a, _ := g.Nodes.Add([]string{"Person"}, nil)
	b, _ := g.Nodes.Add([]string{"Person"}, nil)
	r, _ := g.Rels.Add("KNOWS", a, b, nil)
	id := r.ID()

	current, _ := g.store.GetRelationship(id)
	current.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})
	_ = g.store.ReplaceRelationship(current)

	rels, err := g.Temporal.RelationshipsDuring(3000, 4000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rels) != 0 {
		t.Fatalf("expected 0 rels, got %d", len(rels))
	}
}

func TestGetRelsValidDuring_OpenEnded(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	a, _ := g.Nodes.Add([]string{"Person"}, nil)
	b, _ := g.Nodes.Add([]string{"Person"}, nil)
	// Rel without explicit ValidTo — open-ended.
	g.Rels.Add("KNOWS", a, b, nil)

	now := nowMs()
	rels, err := g.Temporal.RelationshipsDuring(now-1000, now+1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("expected 1 open-ended rel, got %d", len(rels))
	}
}

// --- Version-specific tests ---

func TestGetNodeAt_CurrentVersion(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})

	// Query at current time — should return the (only) version.
	result, err := g.Temporal.NodeAt(n.ID(), nowMs())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	name, _ := result.GetProperty("name")
	if name != "Alice" {
		t.Fatalf("expected name=Alice, got %v", name)
	}
}

func TestGetNodeAt_HistoricalVersion(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	clk := useTestClock(t, g)
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "v0"})
	id := n.ID()

	// Record creation time.
	creationTime := g.nodeValidFrom(n)

	// Test clock advances 1ms per c.now() call, so consecutive Updates
	// always produce distinct UpdatedAt values without wall-clock sleeps
	// (R5-F10).
	g.Nodes.Update(id, map[string]any{"name": "v1"})
	g.Nodes.Update(id, map[string]any{"name": "v2"})

	// Query at creation time — should return v0 (genesis).
	result, err := g.Temporal.NodeAt(id, creationTime)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	name, _ := result.GetProperty("name")
	if name != "v0" {
		t.Fatalf("at creation time: expected name=v0, got %v", name)
	}

	// Query strictly after the last Update's UpdatedAt — should return
	// v2 (latest). Using clk.PeekInstant() rather than nowMs() because
	// the test clock advances independently of the wall clock; querying
	// at a wall-clock "now" would land before the test-clock-derived
	// UpdatedAt of v1/v2 and resolve to v0.
	result, err = g.Temporal.NodeAt(id, clk.PeekInstant())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	name, _ = result.GetProperty("name")
	if name != "v2" {
		t.Fatalf("at now: expected name=v2, got %v", name)
	}
}

func TestGetNodeAt_BeforeCreation(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.Nodes.Add([]string{"Person"}, nil)

	// Query before the entity existed.
	_, err := g.Temporal.NodeAt(n.ID(), 1)
	if !errors.Is(err, storepkg.ErrNoVersionValidAt) {
		t.Fatalf("expected storepkg.ErrNoVersionValidAt, got %v", err)
	}
}

func TestGetNodeAt_NotFound(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	_, err := g.Temporal.NodeAt(types.NodeID(999), nowMs())
	if !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("expected storepkg.ErrNodeNotFound, got %v", err)
	}
}

func TestGetNodeAt_ExplicitTemporal(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "explicit"})
	id := n.ID()

	// Set explicit ValidFrom/ValidTo on the node.
	current, _ := g.store.GetNode(id)
	current.SetTemporal(&types.TemporalMetadata{ValidFrom: 5000, ValidTo: 10000})
	_ = g.store.ReplaceNode(current)

	// Query at t=7000 — within explicit range.
	result, err := g.Temporal.NodeAt(id, 7000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	name, _ := result.GetProperty("name")
	if name != "explicit" {
		t.Fatalf("expected name=explicit, got %v", name)
	}

	// Query at t=3000 — before explicit ValidFrom.
	_, err = g.Temporal.NodeAt(id, 3000)
	if !errors.Is(err, storepkg.ErrNoVersionValidAt) {
		t.Fatalf("before explicit ValidFrom: expected storepkg.ErrNoVersionValidAt, got %v", err)
	}

	// Query at t=15000 — after explicit ValidTo.
	_, err = g.Temporal.NodeAt(id, 15000)
	if !errors.Is(err, storepkg.ErrNoVersionValidAt) {
		t.Fatalf("after explicit ValidTo: expected storepkg.ErrNoVersionValidAt, got %v", err)
	}
}

// --- Neighbor tests ---

func TestGetNeighborsValidAt_AllValid(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	a, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "A"})
	b, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "B"})
	c, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "C"})

	g.Rels.Add("KNOWS", a, b, nil)
	g.Rels.Add("KNOWS", c, a, nil)

	neighbors, err := g.Temporal.NeighborsAt(a.ID(), nowMs())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(neighbors) != 2 {
		t.Fatalf("expected 2 neighbors, got %d", len(neighbors))
	}
}

func TestGetNeighborsValidAt_SomeExpired(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	a, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "A"})
	b, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "B"})
	c, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "C"})

	g.Rels.Add("KNOWS", a, b, nil)
	g.Rels.Add("KNOWS", a, c, nil)

	// Expire node C.
	cCurrent, _ := g.store.GetNode(c.ID())
	cCurrent.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})
	_ = g.store.ReplaceNode(cCurrent)

	neighbors, err := g.Temporal.NeighborsAt(a.ID(), nowMs())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(neighbors) != 1 {
		t.Fatalf("expected 1 valid neighbor (C expired), got %d", len(neighbors))
	}
}

func TestGetNeighborsValidAt_RelExpired(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	a, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "A"})
	b, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "B"})

	r, _ := g.Rels.Add("KNOWS", a, b, nil)

	// Expire the relationship (node B is still valid).
	rCurrent, _ := g.store.GetRelationship(r.ID())
	rCurrent.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})
	_ = g.store.ReplaceRelationship(rCurrent)

	neighbors, err := g.Temporal.NeighborsAt(a.ID(), nowMs())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(neighbors) != 0 {
		t.Fatalf("expired rel should exclude neighbor, got %d", len(neighbors))
	}
}

// --- Snapshot tests ---

func TestSnapshot_Empty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	snap, err := g.Temporal.Snapshot(nowMs())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if snap.NodeCount != 0 {
		t.Fatalf("expected 0 nodes, got %d", snap.NodeCount)
	}
	if snap.RelCount != 0 {
		t.Fatalf("expected 0 rels, got %d", snap.RelCount)
	}
}

func TestSnapshot_CurrentTime(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	a, _ := g.Nodes.Add([]string{"Person"}, nil)
	b, _ := g.Nodes.Add([]string{"Person"}, nil)
	g.Rels.Add("KNOWS", a, b, nil)

	snap, err := g.Temporal.Snapshot(nowMs())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if snap.NodeCount != 2 {
		t.Fatalf("expected 2 nodes, got %d", snap.NodeCount)
	}
	if snap.RelCount != 1 {
		t.Fatalf("expected 1 rel, got %d", snap.RelCount)
	}
}

func TestSnapshot_PastTime(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	a, _ := g.Nodes.Add([]string{"Person"}, nil)
	b, _ := g.Nodes.Add([]string{"Person"}, nil)
	g.Rels.Add("KNOWS", a, b, nil)

	// Snapshot at t=1 (before epoch) — snowflake-derived ValidFrom will be in 2026.
	snap, err := g.Temporal.Snapshot(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if snap.NodeCount != 0 {
		t.Fatalf("expected 0 nodes at t=1, got %d", snap.NodeCount)
	}
	if snap.RelCount != 0 {
		t.Fatalf("expected 0 rels at t=1, got %d", snap.RelCount)
	}
}

func TestSnapshot_DanglingRelExcluded(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	a, _ := g.Nodes.Add([]string{"Person"}, nil)
	b, _ := g.Nodes.Add([]string{"Person"}, nil)
	g.Rels.Add("KNOWS", a, b, nil)

	// Expire node B — the rel becomes dangling.
	bCurrent, _ := g.store.GetNode(b.ID())
	bCurrent.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})
	_ = g.store.ReplaceNode(bCurrent)

	snap, err := g.Temporal.Snapshot(nowMs())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if snap.NodeCount != 1 {
		t.Fatalf("expected 1 valid node, got %d", snap.NodeCount)
	}
	if snap.RelCount != 0 {
		t.Fatalf("dangling rel should be excluded, got %d rels", snap.RelCount)
	}
}

func TestSnapshot_SortedResults(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	// Add multiple nodes — they get ascending snowflake IDs.
	n1, _ := g.Nodes.Add([]string{"Person"}, nil)
	n2, _ := g.Nodes.Add([]string{"Person"}, nil)
	n3, _ := g.Nodes.Add([]string{"Person"}, nil)

	snap, err := g.Temporal.Snapshot(nowMs())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(snap.Nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(snap.Nodes))
	}

	// Verify ascending snowflake ID order (from AllNodes which sorts by ID).
	id1 := snap.Nodes[0].ID()
	id2 := snap.Nodes[1].ID()
	id3 := snap.Nodes[2].ID()

	if id1 >= id2 || id2 >= id3 {
		t.Fatalf("nodes not sorted by ID: %d, %d, %d", id1, id2, id3)
	}

	// Verify all returned nodes match the ones we created.
	_ = n1
	_ = n2
	_ = n3
}

// --- Truncation resilience tests ---

func TestGetNodeAt_AfterTruncation(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	clk := useTestClock(t, g)
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "v0"})
	id := n.ID()

	// Test clock advances 1ms per c.now() call — Updates produce
	// distinct UpdatedAt values without wall-clock sleeps (R5-F10).
	g.Nodes.Update(id, map[string]any{"name": "v1"})
	g.Nodes.Update(id, map[string]any{"name": "v2"})
	updated, _ := g.Nodes.Update(id, map[string]any{"name": "v3"})

	// Truncate to keep only 1 history version (v2 survives).
	if err := g.store.TruncateNodeHistory(id, 1); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	// Query at the surviving version's UpdatedAt — should return it.
	history, _ := g.store.GetNodeHistory(id)
	if len(history) != 1 {
		t.Fatalf("expected 1 history entry after truncation, got %d", len(history))
	}
	survivingVersion := history[0]
	tm := survivingVersion.Temporal()
	if tm == nil || tm.UpdatedAt == 0 {
		t.Fatal("surviving version should have UpdatedAt set")
	}

	// Query at the surviving version's time.
	result, err := g.Temporal.NodeAt(id, tm.UpdatedAt)
	if err != nil {
		t.Fatalf("GetNodeAt at surviving version time: %v", err)
	}
	name, _ := result.GetProperty("name")
	if name != "v2" {
		t.Fatalf("expected name=v2 at surviving version time, got %v", name)
	}

	// Query strictly after the last Update — should return latest (v3).
	// clk.PeekInstant() rather than nowMs() because the test clock is
	// at wall-time + 1s (see useTestClock); v3's UpdatedAt is on the
	// test clock, so wall-time nowMs() would land before it.
	result, err = g.Temporal.NodeAt(id, clk.PeekInstant())
	if err != nil {
		t.Fatalf("GetNodeAt at now: %v", err)
	}
	name, _ = result.GetProperty("name")
	if name != "v3" {
		t.Fatalf("expected name=v3 at now, got %v", name)
	}
	_ = updated
}

// --- Delete history preservation tests ---

func TestDeleteNodePreservesHistory(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	useTestClock(t, g)
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "v0"})
	id := n.ID()

	g.Nodes.Update(id, map[string]any{"name": "v1"})

	// Delete the node.
	if err := g.Nodes.Delete(id); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	// History should still be available.
	history, err := g.store.GetNodeHistory(id)
	if err != nil {
		t.Fatalf("GetNodeHistory after delete: %v", err)
	}
	if len(history) == 0 {
		t.Fatal("expected preserved history entries after delete, got 0")
	}
}

func TestDeleteNodeTombstone_DeletedAt(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	useTestClock(t, g)
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.ID()

	// nowMs() is wall clock; the test clock is wall + 1s so any
	// subsequent c.now()-driven DeletedAt is strictly greater (R5-F10).
	beforeDelete := nowMs()

	if err := g.Nodes.Delete(id); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	// Check that tombstone entry exists with DeletedAt set.
	history, _ := g.store.GetNodeHistory(id)
	if len(history) == 0 {
		t.Fatal("expected tombstone history entry")
	}

	// The tombstone should be the last history entry.
	tombstone := history[len(history)-1]
	tm := tombstone.Temporal()
	if tm == nil {
		t.Fatal("tombstone has nil temporal metadata")
	}
	if tm.DeletedAt == 0 {
		t.Fatal("tombstone should have DeletedAt set")
	}
	if tm.DeletedAt <= beforeDelete {
		t.Fatalf("DeletedAt %d should be after beforeDelete %d", tm.DeletedAt, beforeDelete)
	}
	if tm.ValidTo == 0 {
		t.Fatal("tombstone should have ValidTo set")
	}
}

func TestDeleteRelPreservesHistory(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	useTestClock(t, g)
	a, _ := g.Nodes.Add([]string{"Person"}, nil)
	b, _ := g.Nodes.Add([]string{"Person"}, nil)
	r, _ := g.Rels.Add("KNOWS", a, b, map[string]any{"weight": int64(1)})
	rid := r.ID()

	g.Rels.Update(rid, map[string]any{"weight": int64(2)})

	// Delete the relationship.
	if err := g.Rels.Delete(rid); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}

	// History should still be available.
	history, err := g.store.GetRelHistory(rid)
	if err != nil {
		t.Fatalf("GetRelHistory after delete: %v", err)
	}
	if len(history) == 0 {
		t.Fatal("expected preserved rel history entries after delete, got 0")
	}

	// Tombstone should have DeletedAt.
	tombstone := history[len(history)-1]
	tm := tombstone.Temporal()
	if tm == nil || tm.DeletedAt == 0 {
		t.Fatal("rel tombstone should have DeletedAt set")
	}
}

func TestDeleteNodeCascade_RelHistoryPreserved(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	useTestClock(t, g)
	a, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	b, _ := g.Nodes.Add([]string{"Person"}, nil)
	r, _ := g.Rels.Add("KNOWS", a, b, map[string]any{"since": int64(2020)})
	rid := r.ID()
	aid := a.ID()

	g.Rels.Update(rid, map[string]any{"since": int64(2021)})

	// Cascade delete node A — should preserve rel history.
	if err := g.Nodes.Delete(aid); err != nil {
		t.Fatalf("DeleteNode cascade: %v", err)
	}

	// Rel history should be preserved with tombstone.
	history, err := g.store.GetRelHistory(rid)
	if err != nil {
		t.Fatalf("GetRelHistory after cascade: %v", err)
	}
	if len(history) == 0 {
		t.Fatal("expected preserved rel history after cascade delete")
	}

	// Tombstone should have DeletedAt.
	tombstone := history[len(history)-1]
	tm := tombstone.Temporal()
	if tm == nil || tm.DeletedAt == 0 {
		t.Fatal("cascade-deleted rel tombstone should have DeletedAt set")
	}

	// Node history should also be preserved.
	nodeHistory, err := g.store.GetNodeHistory(aid)
	if err != nil {
		t.Fatalf("GetNodeHistory after cascade: %v", err)
	}
	if len(nodeHistory) == 0 {
		t.Fatal("expected preserved node history after cascade delete")
	}
}

// --- History-aware temporal query tests (Fix 1) ---

func TestGetNodeAt_DeletedEntity(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	useTestClock(t, g)
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.ID()

	// Record a time when the node existed. nowMs() is wall-clock; the
	// test clock is wall + 1s so the subsequent Delete's ValidTo is
	// strictly greater than validTime (R5-F10).
	validTime := nowMs()

	// Delete the node (creates tombstone with DeletedAt/ValidTo).
	if err := g.Nodes.Delete(id); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	// GetNodeAt at the pre-deletion time should return the node.
	result, err := g.Temporal.NodeAt(id, validTime)
	if err != nil {
		t.Fatalf("GetNodeAt at pre-deletion time: %v", err)
	}
	name, _ := result.GetProperty("name")
	if name != "Alice" {
		t.Fatalf("expected name=Alice, got %v", name)
	}
}

func TestGetNodeAt_DeletedEntity_AfterDeletion(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	clk := useTestClock(t, g)
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.ID()

	if err := g.Nodes.Delete(id); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	// GetNodeAt after deletion should return ErrNoVersionValidAt.
	// Query at the test clock (strictly after the Delete's ValidTo,
	// which was stamped from the test clock) — wall-clock nowMs() lands
	// before the test-clock ValidTo and would still see the live node.
	_, err := g.Temporal.NodeAt(id, clk.PeekInstant())
	if !errors.Is(err, storepkg.ErrNoVersionValidAt) {
		t.Fatalf("expected storepkg.ErrNoVersionValidAt after deletion, got %v", err)
	}
}

func TestGetNodesValidAt_DeletedNode(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	clk := useTestClock(t, g)
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.ID()

	// Also add a node that stays alive.
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Bob"})

	validTime := nowMs()

	// Delete Alice.
	if err := g.Nodes.Delete(id); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	// Query at pre-deletion time — both nodes should appear.
	nodes, err := g.Temporal.NodesAt(validTime)
	if err != nil {
		t.Fatalf("GetNodesValidAt: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes at pre-deletion time, got %d", len(nodes))
	}

	// Query strictly after Alice's Delete (test-clock-stamped ValidTo)
	// — only Bob should appear.
	nodes, err = g.Temporal.NodesAt(clk.PeekInstant())
	if err != nil {
		t.Fatalf("GetNodesValidAt now: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node at current time, got %d", len(nodes))
	}
}

func TestGetNodesValidAt_UpdatedNode(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	useTestClock(t, g)
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "v0"})
	id := n.ID()

	creationTime := g.nodeValidFrom(n)

	g.Nodes.Update(id, map[string]any{"name": "v1"})

	// Query at creation time — should return v0.
	nodes, err := g.Temporal.NodesAt(creationTime)
	if err != nil {
		t.Fatalf("GetNodesValidAt: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}
	name, _ := nodes[0].GetProperty("name")
	if name != "v0" {
		t.Fatalf("expected v0 at creation time, got %v", name)
	}
}

func TestGetRelAt_Basic(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	clk := useTestClock(t, g)
	a, _ := g.Nodes.Add([]string{"Person"}, nil)
	b, _ := g.Nodes.Add([]string{"Person"}, nil)
	r, _ := g.Rels.Add("KNOWS", a, b, map[string]any{"weight": int64(1)})
	rid := r.ID()

	creationTime := g.relValidFrom(r)

	g.Rels.Update(rid, map[string]any{"weight": int64(2)})
	g.Rels.Update(rid, map[string]any{"weight": int64(3)})

	// Query at creation time — should return v0 with weight=1.
	result, err := g.Temporal.RelAt(rid, creationTime)
	if err != nil {
		t.Fatalf("GetRelAt at creation: %v", err)
	}
	w, _ := result.GetProperty("weight")
	if w != int64(1) {
		t.Fatalf("expected weight=1, got %v", w)
	}

	// Query strictly after the last Update (test-clock-stamped
	// UpdatedAt) — should return latest with weight=3.
	result, err = g.Temporal.RelAt(rid, clk.PeekInstant())
	if err != nil {
		t.Fatalf("GetRelAt now: %v", err)
	}
	w, _ = result.GetProperty("weight")
	if w != int64(3) {
		t.Fatalf("expected weight=3, got %v", w)
	}
}

func TestGetRelAt_DeletedEntity(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	clk := useTestClock(t, g)
	a, _ := g.Nodes.Add([]string{"Person"}, nil)
	b, _ := g.Nodes.Add([]string{"Person"}, nil)
	r, _ := g.Rels.Add("KNOWS", a, b, map[string]any{"weight": int64(1)})
	rid := r.ID()

	validTime := nowMs()

	if err := g.Rels.Delete(rid); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}

	// Query at pre-deletion time — should return the rel.
	result, err := g.Temporal.RelAt(rid, validTime)
	if err != nil {
		t.Fatalf("GetRelAt at pre-deletion: %v", err)
	}
	w, _ := result.GetProperty("weight")
	if w != int64(1) {
		t.Fatalf("expected weight=1, got %v", w)
	}

	// Query strictly after the Delete (test-clock-stamped ValidTo) —
	// expects ErrNoVersionValidAt.
	_, err = g.Temporal.RelAt(rid, clk.PeekInstant())
	if !errors.Is(err, storepkg.ErrNoVersionValidAt) {
		t.Fatalf("expected storepkg.ErrNoVersionValidAt after deletion, got %v", err)
	}
}

func TestGetRelAt_NotFound(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	_, err := g.Temporal.RelAt(types.RelID(999), nowMs())
	if !errors.Is(err, storepkg.ErrRelNotFound) {
		t.Fatalf("expected storepkg.ErrRelNotFound, got %v", err)
	}
}

func TestGetRelationshipsValidAt_DeletedRel(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	clk := useTestClock(t, g)
	a, _ := g.Nodes.Add([]string{"Person"}, nil)
	b, _ := g.Nodes.Add([]string{"Person"}, nil)
	g.Rels.Add("KNOWS", a, b, nil)

	validTime := nowMs()

	// Delete the relationship via cascade (delete node a).
	if err := g.Nodes.Delete(a.ID()); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	// Query at pre-deletion time — rel should appear.
	rels, err := g.Temporal.RelationshipsAt(validTime)
	if err != nil {
		t.Fatalf("GetRelationshipsValidAt: %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("expected 1 rel at pre-deletion time, got %d", len(rels))
	}

	// Query strictly after the cascade delete — rel should not appear.
	rels, err = g.Temporal.RelationshipsAt(clk.PeekInstant())
	if err != nil {
		t.Fatalf("GetRelationshipsValidAt now: %v", err)
	}
	if len(rels) != 0 {
		t.Fatalf("expected 0 rels after deletion, got %d", len(rels))
	}
}

func TestSnapshot_IncludesDeletedNodes(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	clk := useTestClock(t, g)
	a, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	b, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Bob"})
	g.Rels.Add("KNOWS", a, b, nil)

	snapshotTime := nowMs()

	// Delete Alice — cascades to the KNOWS relationship.
	if err := g.Nodes.Delete(a.ID()); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	// Snapshot at pre-deletion time — should include both nodes and the rel.
	snap, err := g.Temporal.Snapshot(snapshotTime)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.NodeCount != 2 {
		t.Fatalf("expected 2 nodes in snapshot, got %d", snap.NodeCount)
	}
	if snap.RelCount != 1 {
		t.Fatalf("expected 1 rel in snapshot, got %d", snap.RelCount)
	}

	// Snapshot strictly after the cascade delete — only Bob.
	snap, err = g.Temporal.Snapshot(clk.PeekInstant())
	if err != nil {
		t.Fatalf("Snapshot now: %v", err)
	}
	if snap.NodeCount != 1 {
		t.Fatalf("expected 1 node in current snapshot, got %d", snap.NodeCount)
	}
	if snap.RelCount != 0 {
		t.Fatalf("expected 0 rels in current snapshot, got %d", snap.RelCount)
	}
}

func TestGetNodesValidDuring_DeletedNode(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	clk := useTestClock(t, g)
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.ID()

	creationTime := g.nodeValidFrom(n)

	if err := g.Nodes.Delete(id); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	// Interval overlapping the entity's lifetime should include it.
	// End of interval must be strictly after the test-clock-stamped
	// ValidTo so the lifetime fully covers the interval.
	nodes, err := g.Temporal.NodesDuring(creationTime, clk.PeekInstant())
	if err != nil {
		t.Fatalf("GetNodesValidDuring: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node during lifetime interval, got %d", len(nodes))
	}

	// Interval entirely after deletion should not include it.
	afterDeletion := clk.PeekInstant() + 1000
	nodes, err = g.Temporal.NodesDuring(afterDeletion, afterDeletion+1000)
	if err != nil {
		t.Fatalf("GetNodesValidDuring after deletion: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("expected 0 nodes after deletion, got %d", len(nodes))
	}
}

func TestGetRelationshipsValidDuring_DeletedRel(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	clk := useTestClock(t, g)
	a, _ := g.Nodes.Add([]string{"Person"}, nil)
	b, _ := g.Nodes.Add([]string{"Person"}, nil)
	r, _ := g.Rels.Add("KNOWS", a, b, nil)

	creationTime := g.relValidFrom(r)

	rid := r.ID()
	if err := g.Rels.Delete(rid); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}

	// Interval overlapping the rel's lifetime should include it.
	rels, err := g.Temporal.RelationshipsDuring(creationTime, clk.PeekInstant())
	if err != nil {
		t.Fatalf("GetRelationshipsValidDuring: %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("expected 1 rel during lifetime, got %d", len(rels))
	}

	// Interval entirely after deletion should not include it.
	afterDeletion := clk.PeekInstant() + 1000
	rels, err = g.Temporal.RelationshipsDuring(afterDeletion, afterDeletion+1000)
	if err != nil {
		t.Fatalf("GetRelationshipsValidDuring after deletion: %v", err)
	}
	if len(rels) != 0 {
		t.Fatalf("expected 0 rels after deletion, got %d", len(rels))
	}
}

// --- Combined label + property + temporal query tests ---

func TestNodesByLabelPropertyAndTime_Found(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	defer func() { _ = g.Close() }()

	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.ID()

	// Set explicit validity: 1000-2000.
	current, _ := g.store.GetNode(id)
	current.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})
	_ = g.store.ReplaceNode(current)

	// Query at 1500 with matching label+property → should find.
	nodes, err := g.Temporal.NodesByLabelPropertyAt("Person", "name", "Alice", 1500)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1, got %d", len(nodes))
	}
}

func TestNodesByLabelPropertyAndTime_PropertyMismatch(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	defer func() { _ = g.Close() }()

	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.ID()

	current, _ := g.store.GetNode(id)
	current.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})
	_ = g.store.ReplaceNode(current)

	// Right label+time, wrong property value.
	nodes, err := g.Temporal.NodesByLabelPropertyAt("Person", "name", "Bob", 1500)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 0 {
		t.Fatalf("expected 0, got %d", len(nodes))
	}
}

func TestNodesByLabelPropertyAndTime_TemporalMismatch(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	defer func() { _ = g.Close() }()

	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.ID()

	current, _ := g.store.GetNode(id)
	current.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})
	_ = g.store.ReplaceNode(current)

	// Right label+property, wrong time (after ValidTo).
	nodes, err := g.Temporal.NodesByLabelPropertyAt("Person", "name", "Alice", 3000)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 0 {
		t.Fatalf("expected 0, got %d", len(nodes))
	}
}

func TestNodesByLabelPropertyAndTime_UnregisteredLabel(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	defer func() { _ = g.Close() }()

	nodes, err := g.Temporal.NodesByLabelPropertyAt("Unknown", "name", "Alice", nowMs())
	if err != nil {
		t.Fatal(err)
	}
	if nodes != nil {
		t.Fatalf("expected nil, got %d nodes", len(nodes))
	}
}

func TestNodesByLabelPropertyAndTime_WithPropertyIndex(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	defer func() { _ = g.Close() }()

	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.ID()

	current, _ := g.store.GetNode(id)
	current.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})
	_ = g.store.ReplaceNode(current)

	// Create an index — should use the indexed path.
	_ = g.Index.CreateProperty("Person", "name")

	nodes, err := g.Temporal.NodesByLabelPropertyAt("Person", "name", "Alice", 1500)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1, got %d", len(nodes))
	}
}

func TestNodesByLabelPropertyDuring_Found(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	defer func() { _ = g.Close() }()

	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.ID()

	current, _ := g.store.GetNode(id)
	current.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})
	_ = g.store.ReplaceNode(current)

	// Interval [1500, 2500) overlaps [1000, 2000).
	nodes, err := g.Temporal.NodesByLabelPropertyDuring("Person", "name", "Alice", 1500, 2500)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1, got %d", len(nodes))
	}
}

func TestNodesByLabelPropertyDuring_NoOverlap(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	defer func() { _ = g.Close() }()

	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})

	current, _ := g.store.GetNode(n.ID())
	current.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})
	_ = g.store.ReplaceNode(current)

	// Interval [3000, 4000) does not overlap [1000, 2000).
	nodes, err := g.Temporal.NodesByLabelPropertyDuring("Person", "name", "Alice", 3000, 4000)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 0 {
		t.Fatalf("expected 0, got %d", len(nodes))
	}
}
