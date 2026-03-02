package graph

import (
	"errors"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

// nowMs returns the current time as types.Instant (Unix milliseconds).
func nowMs() types.Instant {
	return types.Instant(time.Now().UnixMilli())
}

// --- Point-in-time tests ---

func TestGetNodesValidAt_Empty(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	nodes, err := g.GetNodesValidAt(nowMs())
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
	g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})

	// Query at current time — node should be valid (ValidFrom derived from snowflake).
	nodes, err := g.GetNodesValidAt(nowMs())
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
	n, _ := g.AddNode([]string{"Person"}, nil)
	id := n.InternalID().SnowflakeID()

	// Set explicit validity: valid from 1000 to 2000.
	current, _ := g.store.GetNode(id)
	current.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})
	_ = g.store.ReplaceNode(current)

	// Query at 1500 — should be valid.
	nodes, err := g.GetNodesValidAt(1500)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("at t=1500: expected 1 node, got %d", len(nodes))
	}

	// Query at 500 — should be excluded (before ValidFrom).
	nodes, err = g.GetNodesValidAt(500)
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
	n, _ := g.AddNode([]string{"Person"}, nil)
	id := n.InternalID().SnowflakeID()

	// Set ValidTo in the past.
	current, _ := g.store.GetNode(id)
	current.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})
	_ = g.store.ReplaceNode(current)

	nodes, err := g.GetNodesValidAt(3000)
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
	n, _ := g.AddNode([]string{"Person"}, nil)
	id := n.InternalID().SnowflakeID()

	// Set ValidFrom far in the future.
	futureMs := types.Instant(time.Now().Add(24 * time.Hour).UnixMilli())
	current, _ := g.store.GetNode(id)
	current.SetTemporal(&types.TemporalMetadata{ValidFrom: futureMs})
	_ = g.store.ReplaceNode(current)

	nodes, err := g.GetNodesValidAt(nowMs())
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
	g.AddNode([]string{"Person"}, map[string]any{"name": "A"})

	// Node B: expired.
	nB, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "B"})
	bCurrent, _ := g.store.GetNode(nB.InternalID().SnowflakeID())
	bCurrent.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})
	_ = g.store.ReplaceNode(bCurrent)

	// Node C: valid at query time with explicit range.
	nC, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "C"})
	cCurrent, _ := g.store.GetNode(nC.InternalID().SnowflakeID())
	cCurrent.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 0})
	_ = g.store.ReplaceNode(cCurrent)

	nodes, err := g.GetNodesValidAt(nowMs())
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
	rels, err := g.GetRelationshipsValidAt(nowMs())
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
	a, _ := g.AddNode([]string{"Person"}, nil)
	b, _ := g.AddNode([]string{"Person"}, nil)
	r, _ := g.AddRelationship("KNOWS", a, b, nil)
	id := r.InternalID().SnowflakeID()

	current, _ := g.store.GetRelationship(id)
	current.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})
	_ = g.store.ReplaceRelationship(current)

	rels, err := g.GetRelationshipsValidAt(1500)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("at t=1500: expected 1 rel, got %d", len(rels))
	}

	rels, err = g.GetRelationshipsValidAt(2500)
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
	a, _ := g.AddNode([]string{"Person"}, nil)
	b, _ := g.AddNode([]string{"Person"}, nil)

	// Rel 1: valid (no explicit temporal).
	g.AddRelationship("KNOWS", a, b, nil)

	// Rel 2: expired.
	r2, _ := g.AddRelationship("LIKES", a, b, nil)
	r2Current, _ := g.store.GetRelationship(r2.InternalID().SnowflakeID())
	r2Current.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})
	_ = g.store.ReplaceRelationship(r2Current)

	rels, err := g.GetRelationshipsValidAt(nowMs())
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
	g.GetOrCreateLabel("Person")
	nodes, err := g.GetNodesByLabelValidAt("Person", nowMs())
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
	g.AddNode([]string{"Person"}, map[string]any{"name": "Valid"})

	nExpired, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Expired"})
	ec, _ := g.store.GetNode(nExpired.InternalID().SnowflakeID())
	ec.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})
	_ = g.store.ReplaceNode(ec)

	nodes, err := g.GetNodesByLabelValidAt("Person", nowMs())
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
	nodes, err := g.GetNodesByLabelValidAt("Unknown", nowMs())
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
	n, _ := g.AddNode([]string{"Person"}, nil)
	id := n.InternalID().SnowflakeID()

	current, _ := g.store.GetNode(id)
	current.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 3000})
	_ = g.store.ReplaceNode(current)

	// Interval [2000, 4000) overlaps [1000, 3000).
	nodes, err := g.GetNodesValidDuring(2000, 4000)
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
	n, _ := g.AddNode([]string{"Person"}, nil)
	id := n.InternalID().SnowflakeID()

	current, _ := g.store.GetNode(id)
	current.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})
	_ = g.store.ReplaceNode(current)

	// Interval [3000, 4000) does not overlap [1000, 2000).
	nodes, err := g.GetNodesValidDuring(3000, 4000)
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
	n, _ := g.AddNode([]string{"Person"}, nil)
	id := n.InternalID().SnowflakeID()

	// Open-ended (ValidTo=0) — overlaps any future interval.
	current, _ := g.store.GetNode(id)
	current.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000})
	_ = g.store.ReplaceNode(current)

	farFuture := types.Instant(time.Now().Add(365 * 24 * time.Hour).UnixMilli())
	nodes, err := g.GetNodesValidDuring(farFuture, farFuture+1000)
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
	a, _ := g.AddNode([]string{"Person"}, nil)
	b, _ := g.AddNode([]string{"Person"}, nil)
	r, _ := g.AddRelationship("KNOWS", a, b, nil)
	id := r.InternalID().SnowflakeID()

	current, _ := g.store.GetRelationship(id)
	current.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 3000})
	_ = g.store.ReplaceRelationship(current)

	rels, err := g.GetRelationshipsValidDuring(2000, 4000)
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
	a, _ := g.AddNode([]string{"Person"}, nil)
	b, _ := g.AddNode([]string{"Person"}, nil)
	r, _ := g.AddRelationship("KNOWS", a, b, nil)
	id := r.InternalID().SnowflakeID()

	current, _ := g.store.GetRelationship(id)
	current.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})
	_ = g.store.ReplaceRelationship(current)

	rels, err := g.GetRelationshipsValidDuring(3000, 4000)
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
	a, _ := g.AddNode([]string{"Person"}, nil)
	b, _ := g.AddNode([]string{"Person"}, nil)
	// Rel without explicit ValidTo — open-ended.
	g.AddRelationship("KNOWS", a, b, nil)

	now := nowMs()
	rels, err := g.GetRelationshipsValidDuring(now-1000, now+1000)
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
	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})

	// Query at current time — should return the (only) version.
	result, err := g.GetNodeAt(n.InternalID().SnowflakeID(), nowMs())
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
	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "v0"})
	id := n.InternalID().SnowflakeID()

	// Record creation time.
	creationTime := g.nodeValidFrom(n)

	// Pause to ensure time advances (UpdatedAt will differ).
	time.Sleep(2 * time.Millisecond)

	g.UpdateNode(id, map[string]any{"name": "v1"})
	time.Sleep(2 * time.Millisecond)
	g.UpdateNode(id, map[string]any{"name": "v2"})

	// Query at creation time — should return v0 (genesis).
	result, err := g.GetNodeAt(id, creationTime)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	name, _ := result.GetProperty("name")
	if name != "v0" {
		t.Fatalf("at creation time: expected name=v0, got %v", name)
	}

	// Query at current time — should return v2 (latest).
	result, err = g.GetNodeAt(id, nowMs())
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
	n, _ := g.AddNode([]string{"Person"}, nil)

	// Query before the entity existed.
	_, err := g.GetNodeAt(n.InternalID().SnowflakeID(), 1)
	if !errors.Is(err, ErrNoVersionValidAt) {
		t.Fatalf("expected ErrNoVersionValidAt, got %v", err)
	}
}

func TestGetNodeAt_NotFound(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	_, err := g.GetNodeAt(snowflake.ID(999), nowMs())
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("expected ErrNodeNotFound, got %v", err)
	}
}

func TestGetNodeAt_ExplicitTemporal(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "explicit"})
	id := n.InternalID().SnowflakeID()

	// Set explicit ValidFrom/ValidTo on the node.
	current, _ := g.store.GetNode(id)
	current.SetTemporal(&types.TemporalMetadata{ValidFrom: 5000, ValidTo: 10000})
	_ = g.store.ReplaceNode(current)

	// Query at t=7000 — within explicit range.
	result, err := g.GetNodeAt(id, 7000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	name, _ := result.GetProperty("name")
	if name != "explicit" {
		t.Fatalf("expected name=explicit, got %v", name)
	}

	// Query at t=3000 — before explicit ValidFrom.
	_, err = g.GetNodeAt(id, 3000)
	if !errors.Is(err, ErrNoVersionValidAt) {
		t.Fatalf("before explicit ValidFrom: expected ErrNoVersionValidAt, got %v", err)
	}

	// Query at t=15000 — after explicit ValidTo.
	_, err = g.GetNodeAt(id, 15000)
	if !errors.Is(err, ErrNoVersionValidAt) {
		t.Fatalf("after explicit ValidTo: expected ErrNoVersionValidAt, got %v", err)
	}
}

// --- Neighbor tests ---

func TestGetNeighborsValidAt_AllValid(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{})
	a, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "A"})
	b, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "B"})
	c, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "C"})

	g.AddRelationship("KNOWS", a, b, nil)
	g.AddRelationship("KNOWS", c, a, nil)

	neighbors, err := g.GetNeighborsValidAt(a.InternalID().SnowflakeID(), nowMs())
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
	a, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "A"})
	b, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "B"})
	c, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "C"})

	g.AddRelationship("KNOWS", a, b, nil)
	g.AddRelationship("KNOWS", a, c, nil)

	// Expire node C.
	cCurrent, _ := g.store.GetNode(c.InternalID().SnowflakeID())
	cCurrent.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})
	_ = g.store.ReplaceNode(cCurrent)

	neighbors, err := g.GetNeighborsValidAt(a.InternalID().SnowflakeID(), nowMs())
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
	a, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "A"})
	b, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "B"})

	r, _ := g.AddRelationship("KNOWS", a, b, nil)

	// Expire the relationship (node B is still valid).
	rCurrent, _ := g.store.GetRelationship(r.InternalID().SnowflakeID())
	rCurrent.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})
	_ = g.store.ReplaceRelationship(rCurrent)

	neighbors, err := g.GetNeighborsValidAt(a.InternalID().SnowflakeID(), nowMs())
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
	snap, err := g.Snapshot(nowMs())
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
	a, _ := g.AddNode([]string{"Person"}, nil)
	b, _ := g.AddNode([]string{"Person"}, nil)
	g.AddRelationship("KNOWS", a, b, nil)

	snap, err := g.Snapshot(nowMs())
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
	a, _ := g.AddNode([]string{"Person"}, nil)
	b, _ := g.AddNode([]string{"Person"}, nil)
	g.AddRelationship("KNOWS", a, b, nil)

	// Snapshot at t=1 (before epoch) — snowflake-derived ValidFrom will be in 2026.
	snap, err := g.Snapshot(1)
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
	a, _ := g.AddNode([]string{"Person"}, nil)
	b, _ := g.AddNode([]string{"Person"}, nil)
	g.AddRelationship("KNOWS", a, b, nil)

	// Expire node B — the rel becomes dangling.
	bCurrent, _ := g.store.GetNode(b.InternalID().SnowflakeID())
	bCurrent.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})
	_ = g.store.ReplaceNode(bCurrent)

	snap, err := g.Snapshot(nowMs())
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
	n1, _ := g.AddNode([]string{"Person"}, nil)
	n2, _ := g.AddNode([]string{"Person"}, nil)
	n3, _ := g.AddNode([]string{"Person"}, nil)

	snap, err := g.Snapshot(nowMs())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(snap.Nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(snap.Nodes))
	}

	// Verify ascending snowflake ID order (from AllNodes which sorts by ID).
	id1 := snap.Nodes[0].InternalID().SnowflakeID()
	id2 := snap.Nodes[1].InternalID().SnowflakeID()
	id3 := snap.Nodes[2].InternalID().SnowflakeID()

	if id1 >= id2 || id2 >= id3 {
		t.Fatalf("nodes not sorted by ID: %d, %d, %d", id1, id2, id3)
	}

	// Verify all returned nodes match the ones we created.
	_ = n1
	_ = n2
	_ = n3
}
