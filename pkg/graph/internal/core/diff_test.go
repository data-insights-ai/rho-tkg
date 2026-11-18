package core

import (
	"context"
	"errors"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	temporalpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// newDiffGraph returns a Graph for diff tests.
func newDiffGraph(t *testing.T) *Core {
	t.Helper()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

type orderedDiffStore struct {
	storepkg.Store
	nodeOrder []types.NodeID
	relOrder  []types.RelID
}

func (s *orderedDiffStore) ForEachNodeID(fn func(types.NodeID) bool) error {
	if len(s.nodeOrder) == 0 {
		return s.Store.ForEachNodeID(fn)
	}
	for _, id := range s.nodeOrder {
		if !fn(id) {
			return nil
		}
	}
	return nil
}

func (s *orderedDiffStore) ForEachRelID(fn func(types.RelID) bool) error {
	if len(s.relOrder) == 0 {
		return s.Store.ForEachRelID(fn)
	}
	for _, id := range s.relOrder {
		if !fn(id) {
			return nil
		}
	}
	return nil
}

func sortedNodeIDsDescending(ids ...types.NodeID) []types.NodeID {
	out := append([]types.NodeID(nil), ids...)
	sort.Slice(out, func(i, j int) bool { return out[i] > out[j] })
	return out
}

func sortedRelIDsDescending(ids ...types.RelID) []types.RelID {
	out := append([]types.RelID(nil), ids...)
	sort.Slice(out, func(i, j int) bool { return out[i] > out[j] })
	return out
}

func assertDiffNodesSorted(t *testing.T, name string, nodes []*types.Node) {
	t.Helper()
	for i := 1; i < len(nodes); i++ {
		if nodes[i-1].ID() > nodes[i].ID() {
			t.Fatalf("%s not sorted by ID: %d before %d", name, nodes[i-1].ID(), nodes[i].ID())
		}
	}
}

func assertDiffRelsSorted(t *testing.T, name string, rels []*types.Relationship) {
	t.Helper()
	for i := 1; i < len(rels); i++ {
		if rels[i-1].ID() > rels[i].ID() {
			t.Fatalf("%s not sorted by ID: %d before %d", name, rels[i-1].ID(), rels[i].ID())
		}
	}
}

func assertDiffNodeUpdatesSorted(t *testing.T, updates []temporalpkg.NodeUpdate) {
	t.Helper()
	for i := 1; i < len(updates); i++ {
		if updates[i-1].After.ID() > updates[i].After.ID() {
			t.Fatalf("NodesUpdated not sorted by ID: %d before %d", updates[i-1].After.ID(), updates[i].After.ID())
		}
	}
}

func assertDiffRelUpdatesSorted(t *testing.T, updates []temporalpkg.RelUpdate) {
	t.Helper()
	for i := 1; i < len(updates); i++ {
		if updates[i-1].After.ID() > updates[i].After.ID() {
			t.Fatalf("RelsUpdated not sorted by ID: %d before %d", updates[i-1].After.ID(), updates[i].After.ID())
		}
	}
}

func advanceClockPast(t *testing.T, clk *testClock, target types.Instant) {
	t.Helper()
	now := clk.PeekInstant()
	if now >= target {
		return
	}
	clk.Advance(time.Duration(target-now) * time.Millisecond)
}

func requireDiffLen(t *testing.T, name string, got, want int) {
	t.Helper()
	if got != want {
		t.Fatalf("%s length = %d, want %d", name, got, want)
	}
}

// setNodeTemporal directly sets ValidFrom/ValidTo on a stored node.
// This gives tests precise control over temporal visibility without relying
// on snowflake timestamp resolution.
func setNodeTemporal(t *testing.T, g *Core, id types.NodeID, validFrom, validTo types.Instant) {
	t.Helper()
	n, err := g.store.GetNode(id)
	if err != nil {
		t.Fatalf("setNodeTemporal GetNode: %v", err)
	}
	tm := n.Temporal()
	if tm == nil {
		tm = &types.TemporalMetadata{}
		n.SetTemporal(tm)
	}
	tm.ValidFrom = validFrom
	tm.ValidTo = validTo
	if err := g.store.ReplaceNode(n); err != nil {
		t.Fatalf("setNodeTemporal ReplaceNode: %v", err)
	}
}

// setRelTemporal directly sets ValidFrom/ValidTo on a stored relationship.
func setRelTemporal(t *testing.T, g *Core, id types.RelID, validFrom, validTo types.Instant) {
	t.Helper()
	r, err := g.store.GetRelationship(id)
	if err != nil {
		t.Fatalf("setRelTemporal GetRelationship: %v", err)
	}
	tm := r.Temporal()
	if tm == nil {
		tm = &types.TemporalMetadata{}
		r.SetTemporal(tm)
	}
	tm.ValidFrom = validFrom
	tm.ValidTo = validTo
	if err := g.store.ReplaceRelationship(r); err != nil {
		t.Fatalf("setRelTemporal ReplaceRelationship: %v", err)
	}
}

func TestDiffSnapshots_ReturnsSortedChangeSlices(t *testing.T) {
	t.Parallel()
	store := &orderedDiffStore{Store: memory.New()}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	clk := useTestClock(t, g)

	addNode := func(label string, props map[string]any) *types.Node {
		t.Helper()
		n, err := g.Nodes.Add(context.Background(), []string{label}, props)
		if err != nil {
			t.Fatalf("AddNode %s: %v", label, err)
		}
		return n
	}
	addRel := func(typeName string, props map[string]any, start, end *types.Node) *types.Relationship {
		t.Helper()
		r, err := g.Rels.Add(context.Background(), typeName, start, end, props)
		if err != nil {
			t.Fatalf("AddRelationship %s: %v", typeName, err)
		}
		return r
	}

	endA := addNode("EndpointA", nil)
	endB := addNode("EndpointB", nil)

	const perKind = 8
	createdNodes := make([]*types.Node, 0, perKind)
	deletedNodes := make([]*types.Node, 0, perKind)
	updatedNodes := make([]*types.Node, 0, perKind)
	createdRels := make([]*types.Relationship, 0, perKind)
	deletedRels := make([]*types.Relationship, 0, perKind)
	updatedRels := make([]*types.Relationship, 0, perKind)

	for i := 0; i < perKind; i++ {
		createdNodes = append(createdNodes, addNode("Created", map[string]any{"seq": int64(i)}))
		deletedNodes = append(deletedNodes, addNode("Deleted", map[string]any{"seq": int64(i)}))
		updatedNodes = append(updatedNodes, addNode("Updated", map[string]any{"seq": int64(i), "state": "before"}))
		createdRels = append(createdRels, addRel("CREATED_SORT", map[string]any{"seq": int64(i)}, endA, endB))
		deletedRels = append(deletedRels, addRel("DELETED_SORT", map[string]any{"seq": int64(i)}, endA, endB))
		updatedRels = append(updatedRels, addRel("UPDATED_SORT", map[string]any{"seq": int64(i), "state": "before"}, endA, endB))
	}

	t1 := clk.PeekInstant() + 20
	t2 := t1 + 10_000
	validBeforeT1 := t1 - 20
	validAfterT1 := t1 + 1

	setNodeTemporal(t, g, endA.ID(), validBeforeT1, 0)
	setNodeTemporal(t, g, endB.ID(), validBeforeT1, 0)
	for i := 0; i < perKind; i++ {
		setNodeTemporal(t, g, createdNodes[i].ID(), validAfterT1, 0)
		setNodeTemporal(t, g, deletedNodes[i].ID(), validBeforeT1, validAfterT1)
		setNodeTemporal(t, g, updatedNodes[i].ID(), validBeforeT1, 0)
		setRelTemporal(t, g, createdRels[i].ID(), validAfterT1, 0)
		setRelTemporal(t, g, deletedRels[i].ID(), validBeforeT1, validAfterT1)
		setRelTemporal(t, g, updatedRels[i].ID(), validBeforeT1, 0)
	}

	advanceClockPast(t, clk, t1+10)
	for i, n := range updatedNodes {
		if _, err := g.Nodes.Update(context.Background(), n.ID(), map[string]any{"state": "after", "rank": int64(i)}); err != nil {
			t.Fatalf("UpdateNode %d: %v", i, err)
		}
	}
	for i, r := range updatedRels {
		if _, err := g.Rels.Update(context.Background(), r.ID(), map[string]any{"state": "after", "rank": int64(i)}); err != nil {
			t.Fatalf("UpdateRelationship %d: %v", i, err)
		}
	}

	nodeOrder := make([]types.NodeID, 0, 2+3*perKind)
	nodeOrder = append(nodeOrder, endA.ID(), endB.ID())
	for i := 0; i < perKind; i++ {
		nodeOrder = append(nodeOrder, createdNodes[i].ID(), deletedNodes[i].ID(), updatedNodes[i].ID())
	}
	relOrder := make([]types.RelID, 0, 3*perKind)
	for i := 0; i < perKind; i++ {
		relOrder = append(relOrder, createdRels[i].ID(), deletedRels[i].ID(), updatedRels[i].ID())
	}
	store.nodeOrder = sortedNodeIDsDescending(nodeOrder...)
	store.relOrder = sortedRelIDsDescending(relOrder...)

	diff, err := g.Temporal.Diff(t1, t2)
	if err != nil {
		t.Fatalf("DiffSnapshots: %v", err)
	}

	requireDiffLen(t, "NodesCreated", len(diff.NodesCreated), perKind)
	requireDiffLen(t, "NodesDeleted", len(diff.NodesDeleted), perKind)
	requireDiffLen(t, "NodesUpdated", len(diff.NodesUpdated), perKind)
	requireDiffLen(t, "RelsCreated", len(diff.RelsCreated), perKind)
	requireDiffLen(t, "RelsDeleted", len(diff.RelsDeleted), perKind)
	requireDiffLen(t, "RelsUpdated", len(diff.RelsUpdated), perKind)

	assertDiffNodesSorted(t, "NodesCreated", diff.NodesCreated)
	assertDiffNodesSorted(t, "NodesDeleted", diff.NodesDeleted)
	assertDiffNodeUpdatesSorted(t, diff.NodesUpdated)
	assertDiffRelsSorted(t, "RelsCreated", diff.RelsCreated)
	assertDiffRelsSorted(t, "RelsDeleted", diff.RelsDeleted)
	assertDiffRelUpdatesSorted(t, diff.RelsUpdated)
}

// TestDiffSnapshots_InvalidRange verifies that invalid time ranges return ErrInvalidTimeRange.
func TestDiffSnapshots_InvalidRange(t *testing.T) {
	t.Parallel()
	g := newDiffGraph(t)

	now := types.Instant(time.Now().UnixMilli())
	cases := []struct {
		name   string
		t1, t2 types.Instant
	}{
		{"t1==t2", now, now},
		{"t1>t2", now + 1, now},
		{"t1==0", 0, now},
		{"t2==0", now, 0},
		{"both_zero", 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := g.Temporal.Diff(tc.t1, tc.t2)
			if !errors.Is(err, ErrInvalidTimeRange) {
				t.Fatalf("expected ErrInvalidTimeRange, got %v", err)
			}
		})
	}
}

// TestDiffSnapshots_EmptyGraph verifies that diffing an empty graph returns
// an empty (non-nil) diff without panicking.
func TestDiffSnapshots_EmptyGraph(t *testing.T) {
	t.Parallel()
	g := newDiffGraph(t)
	t1 := types.Instant(time.Now().UnixMilli()) - 200
	t2 := t1 + 100

	diff, err := g.Temporal.Diff(t1, t2)
	if err != nil {
		t.Fatalf("DiffSnapshots empty graph: %v", err)
	}
	if diff == nil {
		t.Fatal("DiffSnapshots returned nil diff")
	}
	if len(diff.NodesCreated)+len(diff.NodesUpdated)+len(diff.NodesDeleted) != 0 {
		t.Fatalf("expected empty node diff, got: created=%d updated=%d deleted=%d",
			len(diff.NodesCreated), len(diff.NodesUpdated), len(diff.NodesDeleted))
	}
	if len(diff.RelsCreated)+len(diff.RelsUpdated)+len(diff.RelsDeleted) != 0 {
		t.Fatalf("expected empty rel diff")
	}
}

// TestDiffSnapshots_Created verifies that a node added between t1 and t2 appears
// in NodesCreated.
func TestDiffSnapshots_Created(t *testing.T) {
	t.Parallel()
	g := newDiffGraph(t)

	base := types.Instant(time.Now().UnixMilli())
	t1 := base + 100
	t2 := base + 300

	n, err := g.Nodes.Add(context.Background(), []string{"Thing"}, map[string]any{"k": "v"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	nid := n.ID()

	// ValidFrom between t1 and t2 → not visible at t1, visible at t2.
	setNodeTemporal(t, g, nid, base+150, 0)

	diff, err := g.Temporal.Diff(t1, t2)
	if err != nil {
		t.Fatalf("DiffSnapshots: %v", err)
	}
	if len(diff.NodesCreated) != 1 {
		t.Fatalf("expected 1 created node, got %d (deleted=%d, updated=%d)",
			len(diff.NodesCreated), len(diff.NodesDeleted), len(diff.NodesUpdated))
	}
	if diff.NodesCreated[0].ID() != nid {
		t.Fatal("created node ID mismatch")
	}
}

// TestDiffSnapshots_Deleted verifies that a node that expires between t1 and t2
// appears in NodesDeleted.
func TestDiffSnapshots_Deleted(t *testing.T) {
	t.Parallel()
	g := newDiffGraph(t)

	base := types.Instant(time.Now().UnixMilli())
	t1 := base + 100
	t2 := base + 300

	n, err := g.Nodes.Add(context.Background(), []string{"Event"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	nid := n.ID()

	// ValidFrom before t1, ValidTo between t1 and t2 → visible at t1, gone at t2.
	setNodeTemporal(t, g, nid, base+50, base+200)

	diff, err := g.Temporal.Diff(t1, t2)
	if err != nil {
		t.Fatalf("DiffSnapshots: %v", err)
	}
	if len(diff.NodesDeleted) != 1 {
		t.Fatalf("expected 1 deleted node, got %d (created=%d, updated=%d)",
			len(diff.NodesDeleted), len(diff.NodesCreated), len(diff.NodesUpdated))
	}
	if diff.NodesDeleted[0].ID() != nid {
		t.Fatal("deleted node ID mismatch")
	}
}

// TestDiffSnapshots_Updated verifies that a node whose properties changed between
// t1 and t2 appears in NodesUpdated.
func TestDiffSnapshots_Updated(t *testing.T) {
	t.Parallel()
	g := newDiffGraph(t)
	clk := useTestClock(t, g)

	n, err := g.Nodes.Add(context.Background(), []string{"User"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	nid := n.ID()

	// t1 captures state BEFORE the update. Advance the clock so the
	// Update's UpdatedAt is strictly greater than t1.
	t1 := clk.PeekInstant()
	clk.Advance(2 * time.Millisecond)

	// Update the node (its hash changes).
	if _, err := g.Nodes.Update(context.Background(), nid, map[string]any{"name": "Alice Updated"}); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	t2 := clk.PeekInstant() + 10

	diff, err := g.Temporal.Diff(t1, t2)
	if err != nil {
		t.Fatalf("DiffSnapshots: %v", err)
	}
	if len(diff.NodesUpdated) != 1 {
		t.Fatalf("expected 1 updated node, got %d (created=%d, deleted=%d)",
			len(diff.NodesUpdated), len(diff.NodesCreated), len(diff.NodesDeleted))
	}
	upd := diff.NodesUpdated[0]
	beforeName, _ := upd.Before.GetProperty("name")
	afterName, _ := upd.After.GetProperty("name")
	if beforeName != "Alice" {
		t.Fatalf("Before.name: expected Alice, got %v", beforeName)
	}
	if afterName != "Alice Updated" {
		t.Fatalf("After.name: expected Alice Updated, got %v", afterName)
	}
}

// TestDiffSnapshots_Unchanged verifies that a node not changed between t1 and t2
// does not appear in any diff list.
func TestDiffSnapshots_Unchanged(t *testing.T) {
	t.Parallel()
	g := newDiffGraph(t)

	base := types.Instant(time.Now().UnixMilli())
	t1 := base + 100
	t2 := base + 300

	n, err := g.Nodes.Add(context.Background(), []string{"Static"}, map[string]any{"x": int64(1)})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	// Explicitly set ValidFrom before t1 so the node is visible at both t1 and t2.
	// No updates → hash identical at both snapshots → should not appear in any list.
	setNodeTemporal(t, g, n.ID(), base+50, 0)

	diff, err := g.Temporal.Diff(t1, t2)
	if err != nil {
		t.Fatalf("DiffSnapshots: %v", err)
	}
	if len(diff.NodesCreated)+len(diff.NodesUpdated)+len(diff.NodesDeleted) != 0 {
		t.Fatalf("expected empty diff for unchanged node, got: created=%d updated=%d deleted=%d",
			len(diff.NodesCreated), len(diff.NodesUpdated), len(diff.NodesDeleted))
	}
}

// TestDiffSnapshots_RelCreated verifies that a relationship added between t1 and t2
// appears in RelsCreated.
func TestDiffSnapshots_RelCreated(t *testing.T) {
	t.Parallel()
	g := newDiffGraph(t)

	base := types.Instant(time.Now().UnixMilli())
	t1 := base + 100
	t2 := base + 300

	a, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"B"}, nil)

	// Both nodes valid at t1.
	setNodeTemporal(t, g, a.ID(), base+50, 0)
	setNodeTemporal(t, g, b.ID(), base+50, 0)

	r, err := g.Rels.Add(context.Background(), "LINKS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()

	// Rel ValidFrom between t1 and t2 → not visible at t1, visible at t2.
	setRelTemporal(t, g, rid, base+150, 0)

	diff, err := g.Temporal.Diff(t1, t2)
	if err != nil {
		t.Fatalf("DiffSnapshots: %v", err)
	}
	if len(diff.RelsCreated) != 1 {
		t.Fatalf("expected 1 created rel, got %d (deleted=%d, updated=%d)",
			len(diff.RelsCreated), len(diff.RelsDeleted), len(diff.RelsUpdated))
	}
	if diff.RelsCreated[0].ID() != rid {
		t.Fatal("created rel ID mismatch")
	}
}

// TestDiffSnapshots_RelDeleted verifies that a relationship expiring between t1
// and t2 appears in RelsDeleted.
func TestDiffSnapshots_RelDeleted(t *testing.T) {
	t.Parallel()
	g := newDiffGraph(t)

	base := types.Instant(time.Now().UnixMilli())
	t1 := base + 100
	t2 := base + 300

	a, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"B"}, nil)

	setNodeTemporal(t, g, a.ID(), base+50, 0)
	setNodeTemporal(t, g, b.ID(), base+50, 0)

	r, err := g.Rels.Add(context.Background(), "LINKS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()

	// Rel ValidFrom before t1, ValidTo between t1 and t2.
	setRelTemporal(t, g, rid, base+50, base+200)

	diff, err := g.Temporal.Diff(t1, t2)
	if err != nil {
		t.Fatalf("DiffSnapshots: %v", err)
	}
	if len(diff.RelsDeleted) != 1 {
		t.Fatalf("expected 1 deleted rel, got %d (created=%d, updated=%d)",
			len(diff.RelsDeleted), len(diff.RelsCreated), len(diff.RelsUpdated))
	}
	if diff.RelsDeleted[0].ID() != rid {
		t.Fatal("deleted rel ID mismatch")
	}
}

// TestDiffSnapshots_RelUpdated verifies that a relationship whose properties changed
// between t1 and t2 appears in RelsUpdated.
func TestDiffSnapshots_RelUpdated(t *testing.T) {
	t.Parallel()
	g := newDiffGraph(t)
	clk := useTestClock(t, g)

	a, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	r, err := g.Rels.Add(context.Background(), "LINKS", a, b, map[string]any{"w": int64(1)})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	rid := r.ID()

	t1 := clk.PeekInstant()
	clk.Advance(2 * time.Millisecond)

	if _, err := g.Rels.Update(context.Background(), rid, map[string]any{"w": int64(2)}); err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}

	t2 := clk.PeekInstant() + 10

	diff, err := g.Temporal.Diff(t1, t2)
	if err != nil {
		t.Fatalf("DiffSnapshots: %v", err)
	}
	if len(diff.RelsUpdated) != 1 {
		t.Fatalf("expected 1 updated rel, got %d (created=%d, deleted=%d)",
			len(diff.RelsUpdated), len(diff.RelsCreated), len(diff.RelsDeleted))
	}
	upd := diff.RelsUpdated[0]
	beforeW, _ := upd.Before.GetProperty("w")
	afterW, _ := upd.After.GetProperty("w")
	if beforeW != int64(1) {
		t.Fatalf("Before.w: expected 1, got %v", beforeW)
	}
	if afterW != int64(2) {
		t.Fatalf("After.w: expected 2, got %v", afterW)
	}
}

// TestNodeHash_NilIntegrity verifies that nodeHash returns the empty string
// when integrity is unset, so DiffSnapshotsCallback treats two such versions
// as identical (no spurious Updated entry).
func TestNodeHash_NilIntegrity(t *testing.T) {
	t.Parallel()
	g := newDiffGraph(t)

	n, _ := g.Nodes.Add(context.Background(), []string{"Shared"}, nil)
	n.SetIntegrity(nil) // exercise nodeHash nil branch

	if got := nodeHash(n); got != "" {
		t.Fatalf("nodeHash(nil-integrity) = %q, want empty", got)
	}
}

// TestRelHash_NilIntegrity verifies the nil-integrity path for relationships.
func TestRelHash_NilIntegrity(t *testing.T) {
	t.Parallel()
	g := newDiffGraph(t)
	a, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	r, _ := g.Rels.Add(context.Background(), "E", a, b, nil)
	r.SetIntegrity(nil) // exercise relHash nil branch

	if got := relHash(r); got != "" {
		t.Fatalf("relHash(nil-integrity) = %q, want empty", got)
	}
}

// TestDiffSnapshots_MixedScenario verifies a mix of created, updated, deleted,
// and unchanged entities each appear in the correct diff category.
func TestDiffSnapshots_MixedScenario(t *testing.T) {
	t.Parallel()
	g := newDiffGraph(t)
	clk := useTestClock(t, g)

	// "unchanged" — created before the diff window, not modified.
	_, err := g.Nodes.Add(context.Background(), []string{"Unchanged"}, map[string]any{"v": "same"})
	if err != nil {
		t.Fatalf("AddNode unchanged: %v", err)
	}

	// "updated" — exists at t1, properties change before t2.
	toUpdate, err := g.Nodes.Add(context.Background(), []string{"Updated"}, map[string]any{"v": "before"})
	if err != nil {
		t.Fatalf("AddNode toUpdate: %v", err)
	}

	// t1 captured before Update; clock advance widens the gap so the
	// Update's UpdatedAt > t1.
	t1 := clk.PeekInstant()
	clk.Advance(2 * time.Millisecond)

	// Perform the update between t1 and t2.
	if _, err := g.Nodes.Update(context.Background(), toUpdate.ID(), map[string]any{"v": "after"}); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	// "created" — explicit ValidFrom past t1 so the Diff classifies it
	// as created within the [t1, t2) window. Without explicit
	// ValidFrom the snowflake-derived value is wall-clock-now which is
	// before the test-clock-derived t1.
	createdValidFrom := clk.PeekInstant()
	created, err := g.Nodes.Add(context.Background(), []string{"Created"}, map[string]any{"tkg_valid_from": createdValidFrom})
	if err != nil {
		t.Fatalf("AddNode created: %v", err)
	}

	t2 := clk.PeekInstant() + 10

	diff, err := g.Temporal.Diff(t1, t2)
	if err != nil {
		t.Fatalf("DiffSnapshots: %v", err)
	}

	if len(diff.NodesCreated) != 1 {
		t.Fatalf("expected 1 created, got %d", len(diff.NodesCreated))
	}
	if diff.NodesCreated[0].ID() != created.ID() {
		t.Fatal("wrong node in Created list")
	}
	if len(diff.NodesUpdated) != 1 {
		t.Fatalf("expected 1 updated, got %d", len(diff.NodesUpdated))
	}
	if diff.NodesUpdated[0].Before.ID() != toUpdate.ID() {
		t.Fatal("wrong node in Updated list")
	}
	if len(diff.NodesDeleted) != 0 {
		t.Fatalf("expected 0 deleted, got %d", len(diff.NodesDeleted))
	}
}

// diffParkStore runs a hook on the first store read of each of two seeded
// nodes once armed: onFirst for whichever the scan reads first, onSecond for
// the other. Reads of any other node pass straight through.
type diffParkStore struct {
	storepkg.Store
	seeded   map[types.NodeID]bool
	armed    atomic.Bool
	mu       sync.Mutex
	seen     map[types.NodeID]bool
	onFirst  func()
	onSecond func()
}

func (s *diffParkStore) GetNode(id types.NodeID) (*types.Node, error) {
	if s.armed.Load() && s.seeded[id] {
		s.mu.Lock()
		hook := func() {}
		if !s.seen[id] {
			s.seen[id] = true
			if len(s.seen) == 1 {
				hook = s.onFirst
			} else {
				hook = s.onSecond
			}
		}
		s.mu.Unlock()
		hook()
	}
	return s.Store.GetNode(id)
}

// TestDiffSnapshots_DoesNotBlockWrites: Diff takes the graph read lock per
// entity, not once for the whole scan, so a transaction commit (which takes
// the graph write lock) runs between two entity reads. While Diff reads the
// first of two nodes (read lock held), a transaction starts and the hook waits
// until its commit is queued on the write lock. When Diff reads the second
// node, the commit must already be done: Diff released the read lock between
// the two entities and the queued writer went first. A Diff holding one read
// lock across the scan fails the check (or deadlocks on its nested read, which
// the hang detector reports).
func TestDiffSnapshots_DoesNotBlockWrites(t *testing.T) {
	t.Parallel()
	st := &diffParkStore{Store: memory.New(), seen: map[types.NodeID]bool{}}
	g, err := New(Config{Store: st})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Close only when Diff returned: after a hang, Close would wait on the
	// stuck lock and turn a failure into a test-binary timeout.
	closeGraph := true
	defer func() {
		if closeGraph {
			_ = g.Close()
		}
	}()

	a, err := g.Nodes.Add(context.Background(), []string{"Thing"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Thing"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	setNodeTemporal(t, g, a.ID(), 1000, 0)
	setNodeTemporal(t, g, b.ID(), 1000, 0)
	st.seeded = map[types.NodeID]bool{a.ID(): true, b.ID(): true}

	// writerQueued reports whether a writer holds or waits for the graph
	// write lock: stripe 0 then refuses a new reader.
	writerQueued := func() bool {
		if g.mu.stripes[0].mu.TryRLock() {
			g.mu.stripes[0].mu.RUnlock()
			return false
		}
		return true
	}

	writerDone := make(chan error, 1)
	var hookErr error
	st.onFirst = func() {
		go func() {
			tx, err := g.BeginTx()
			if err != nil {
				writerDone <- err
				return
			}
			if _, err := tx.AddNode([]string{"Concurrent"}, nil); err != nil {
				_ = tx.Rollback()
				writerDone <- err
				return
			}
			writerDone <- tx.Commit()
		}()
		deadline := time.Now().Add(10 * time.Second) // hang detector
		for !writerQueued() {
			if time.Now().After(deadline) {
				hookErr = errors.New("the transaction commit never queued on the graph write lock")
				return
			}
			runtime.Gosched()
		}
	}
	st.onSecond = func() {
		if hookErr != nil {
			return
		}
		select {
		case err := <-writerDone:
			if err != nil {
				hookErr = err
			}
		default:
			hookErr = errors.New("the transaction commit had not run between the two entity reads: Diff held the read lock across the scan")
		}
	}
	st.armed.Store(true)

	type result struct {
		diff *temporalpkg.SnapshotDiff
		err  error
	}
	done := make(chan result, 1)
	go func() {
		d, err := g.Temporal.Diff(500, 2000)
		done <- result{d, err}
	}()
	var res result
	select {
	case res = <-done:
	case <-time.After(20 * time.Second): // hang detector
		closeGraph = false
		t.Fatal("Diff did not return while a transaction commit was queued")
	}
	if res.err != nil {
		t.Fatalf("Diff: %v", res.err)
	}
	if hookErr != nil {
		t.Fatal(hookErr)
	}
	if len(st.seen) != 2 {
		t.Fatalf("Diff read %d of the 2 seeded nodes, want both", len(st.seen))
	}
	if len(res.diff.NodesCreated) != 2 {
		t.Fatalf("NodesCreated = %d, want the 2 seeded nodes", len(res.diff.NodesCreated))
	}
}

// TestDiffGracefulUnderConcurrentClose guards the corrected Diff doc
// comment: because Diff takes c.mu.RLock per entity (readUnderRLock) rather
// than once for the whole scan, Close can run between entity reads. The
// claimed safety property is that Diff never observes a torn/closing store —
// it either finishes normally (if it wins the race) or fails closed with
// ErrGraphClosed (if Close wins), never a partial result alongside a nil
// error and never a panic/hang. A large entity count widens the window for
// Close to land mid-scan across repeated runs.
func TestDiffGracefulUnderConcurrentClose(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()

	for iter := 0; iter < 20; iter++ {
		g, err := New(Config{})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		ctx := context.Background()
		for i := 0; i < 200; i++ {
			n, err := g.Nodes.Add(ctx, []string{"Thing"}, nil)
			if err != nil {
				t.Fatalf("AddNode: %v", err)
			}
			setNodeTemporal(t, g, n.ID(), 1000, 0)
		}

		var wg sync.WaitGroup
		wg.Add(2)
		var diff *temporalpkg.SnapshotDiff
		var diffErr error
		go func() {
			defer wg.Done()
			diff, diffErr = g.Temporal.Diff(types.Instant(500), types.Instant(2000))
		}()
		go func() {
			defer wg.Done()
			_ = g.Close()
		}()

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("Diff + concurrent Close did not complete within 10s")
		}

		if diffErr != nil {
			if !errors.Is(diffErr, ErrGraphClosed) {
				t.Fatalf("iter %d: Diff error = %v, want nil or ErrGraphClosed", iter, diffErr)
			}
			if diff != nil {
				t.Fatalf("iter %d: Diff returned a non-nil result alongside an error", iter)
			}
		}
	}
}

// TestDiffConcurrentStandaloneWritesNeverProduceInconsistentResult guards
// that Diff's per-entity c.mu.RLock (not one atomic global snapshot,
// see the doc comment on TempOps.Diff) is an HONESTLY DISCLOSED
// accepted tradeoff — a concurrent standalone backdated write MAY appear as
// a spurious Created/Deleted entry, and this test does not try to eliminate
// that (it's by design, not a bug). What was untested is the STRONGER
// invariant the tradeoff must still uphold even under a torrent of
// concurrent writes hitting the exact entities Diff is scanning: the result
// must never be internally CORRUPTED — no node ID appearing in both
// NodesCreated and NodesDeleted, no duplicate ID within either slice, and
// Diff itself must never panic or return an unexpected error. Many rounds,
// many concurrent writers per round, to shake out ordering-dependent bugs.
func TestDiffConcurrentStandaloneWritesNeverProduceInconsistentResult(t *testing.T) {
	const rounds = 15
	const entities = 20
	const writersPerEntity = 3

	for round := 0; round < rounds; round++ {
		g := newDiffGraph(t)
		ctx := context.Background()

		ids := make([]types.NodeID, entities)
		for i := 0; i < entities; i++ {
			n, err := g.Nodes.Add(ctx, []string{"Thing"}, nil)
			if err != nil {
				t.Fatalf("round %d: AddNode %d: %v", round, i, err)
			}
			setNodeTemporal(t, g, n.ID(), 1000, 0)
			ids[i] = n.ID()
		}

		t1 := types.Instant(500)
		t2 := types.Instant(3000)

		start := make(chan struct{})
		var wg sync.WaitGroup

		// Concurrent writers: each entity gets a few racing goroutines doing
		// a mix of update/delete, some with an explicit backdated
		// tkg_valid_from — exactly the write shape the doc comment calls out
		// as able to produce a spurious Created/Deleted entry.
		for i := 0; i < entities; i++ {
			id := ids[i]
			for w := 0; w < writersPerEntity; w++ {
				w := w
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					switch w % 3 {
					case 0:
						_, _ = g.Nodes.Update(ctx, id, map[string]any{"tkg_valid_from": types.Instant(700), "k": int64(1)})
					case 1:
						_ = g.Nodes.Delete(ctx, id)
					default:
						_, _ = g.Nodes.Update(ctx, id, map[string]any{"k": int64(2)})
					}
				}()
			}
		}

		var diff *temporalpkg.SnapshotDiff
		var diffErr error
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			diff, diffErr = g.Temporal.Diff(t1, t2)
		}()

		close(start)
		wg.Wait()

		if diffErr != nil {
			t.Fatalf("round %d: Diff: %v", round, diffErr)
		}
		if diff == nil {
			t.Fatalf("round %d: Diff returned nil result with nil error", round)
		}

		created := make(map[types.NodeID]bool, len(diff.NodesCreated))
		for _, n := range diff.NodesCreated {
			if created[n.ID()] {
				t.Fatalf("round %d: NodesCreated contains duplicate ID %v", round, n.ID())
			}
			created[n.ID()] = true
		}
		deleted := make(map[types.NodeID]bool, len(diff.NodesDeleted))
		for _, n := range diff.NodesDeleted {
			if deleted[n.ID()] {
				t.Fatalf("round %d: NodesDeleted contains duplicate ID %v", round, n.ID())
			}
			deleted[n.ID()] = true
			if created[n.ID()] {
				t.Fatalf("round %d: node %v appears in BOTH NodesCreated and NodesDeleted — corrupted result", round, n.ID())
			}
		}
		seenUpdated := make(map[types.NodeID]bool, len(diff.NodesUpdated))
		for _, u := range diff.NodesUpdated {
			id := u.Before.ID()
			if seenUpdated[id] {
				t.Fatalf("round %d: NodesUpdated contains duplicate ID %v", round, id)
			}
			seenUpdated[id] = true
			if created[id] || deleted[id] {
				t.Fatalf("round %d: node %v appears in NodesUpdated AND Created/Deleted — corrupted result", round, id)
			}
		}
	}
}
