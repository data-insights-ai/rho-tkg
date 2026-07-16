package core

import (
	"context"
	"errors"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// OPT10 — NodesRelating / RelsRelating: the Allen-predicate query doors.
//
// These are the ONLY temporal doors that surface NON-overlapping relations
// (Before / After / Meets / MetBy): an entity whose valid-interval ended before
// the query window still matches {Before}. That is exactly the case the B4
// envelope-overlap prune would wrongly drop — so the Relating doors deliberately
// do NOT apply it (the During doors do; see nodesByLabelPropertyDuringLocked).
//
// Query window b = [100, 200) throughout. Every node/rel below carries a distinct
// CLOSED valid-interval chosen so its Allen relation to b is unique, plus one
// OPEN-ended entity (vEnd == 0 == +∞) to exercise the RelateOpen path.

// vfvt builds a {tkg_valid_from, tkg_valid_to} props map (a closed interval).
func vfvt(from, to types.Instant) map[string]any {
	return map[string]any{"tkg_valid_from": from, "tkg_valid_to": to}
}

// relatingFixture creates 14 nodes covering all 13 Allen relations against
// [100,200), returning them keyed by the relation each realizes ("Contains" maps
// to two: one closed, one open-ended).
func relatingFixture(t *testing.T, g *Core) map[string][]types.NodeID {
	t.Helper()
	ctx := context.Background()
	add := func(from, to types.Instant) types.NodeID {
		t.Helper()
		n, err := g.Nodes.Add(ctx, []string{"R"}, vfvt(from, to))
		if err != nil {
			t.Fatalf("add [%d,%d): %v", from, to, err)
		}
		return n.ID()
	}
	addOpen := func(from types.Instant) types.NodeID {
		t.Helper()
		n, err := g.Nodes.Add(ctx, []string{"R"}, vf(from, nil)) // open end
		if err != nil {
			t.Fatalf("add [%d,∞): %v", from, err)
		}
		return n.ID()
	}
	return map[string][]types.NodeID{
		"Before":       {add(10, 50)},               // aEnd 50 < bStart 100
		"Meets":        {add(10, 100)},              // aEnd 100 == bStart 100
		"Overlaps":     {add(50, 150)},              // aStart<bStart, 100<aEnd<bEnd
		"Starts":       {add(100, 150)},             // same start, aEnd<bEnd
		"During":       {add(120, 180)},             // strictly inside
		"Equals":       {add(100, 200)},             // identical
		"Finishes":     {add(150, 200)},             // same end, aStart>bStart
		"Contains":     {add(50, 250), addOpen(10)}, // envelopes b (closed AND open)
		"After":        {add(250, 300)},             // bEnd 200 < aStart 250
		"MetBy":        {add(200, 250)},             // aStart 200 == bEnd 200
		"OverlappedBy": {add(150, 250)},             // bStart<aStart<bEnd<aEnd
		"StartedBy":    {add(100, 250)},             // same start, aEnd>bEnd
		"FinishedBy":   {add(50, 200)},              // same end, aStart<bStart
	}
}

func flattenIDs(m map[string][]types.NodeID, keys ...string) []types.NodeID {
	var out []types.NodeID
	for _, k := range keys {
		out = append(out, m[k]...)
	}
	return out
}

func TestNodesRelating_AllThirteenRelations(t *testing.T) {
	g := newTxTimeGraph(t)
	fix := relatingFixture(t, g)

	relating := func(set types.AllenRelationSet) []*types.Node {
		got, err := g.Temporal.NodesRelating(100, 200, set)
		if err != nil {
			t.Fatalf("NodesRelating: %v", err)
		}
		return got
	}

	// One relation at a time: exact-set (rule 16) — no over-report, no omission.
	for _, rel := range []types.AllenRelation{
		types.Before, types.Meets, types.Overlaps, types.Starts, types.During,
		types.Equals, types.Finishes, types.Contains, types.After, types.MetBy,
		types.OverlappedBy, types.StartedBy, types.FinishedBy,
	} {
		assertNodeSet(t, "Relating{"+rel.String()+"}",
			relating(rel.Set()), fix[rel.String()])
	}

	// The four NON-overlapping relations together — the set envelope-overlap
	// pruning would wrongly drop. Must return exactly the four disjoint entities.
	nonOverlap := types.Before.Set().Add(types.After).Add(types.Meets).Add(types.MetBy)
	assertNodeSet(t, "Relating{non-overlap}", relating(nonOverlap),
		flattenIDs(fix, "Before", "After", "Meets", "MetBy"))

	// Contains must include BOTH the closed and the open-ended envelope.
	assertNodeSet(t, "Relating{Contains}(closed+open)", relating(types.Contains.Set()),
		fix["Contains"])

	// The full 13-relation set returns every entity exactly once.
	all := types.AllRelations()
	assertNodeSet(t, "Relating{all}", relating(all),
		flattenIDs(fix, "Before", "Meets", "Overlaps", "Starts", "During", "Equals",
			"Finishes", "Contains", "After", "MetBy", "OverlappedBy", "StartedBy", "FinishedBy"))

	// Empty relation set → empty result, no error.
	if got := relating(0); len(got) != 0 {
		t.Fatalf("Relating{empty} = %d nodes, want 0", len(got))
	}
}

// TestNodesRelating_OpenQueryInterval asserts a to==0 query interval is treated
// as +∞ (not pre-resolved to a concrete "now+" bound), so Before/Meets classify
// exactly against the open right edge.
func TestNodesRelating_OpenQueryInterval(t *testing.T) {
	g := newTxTimeGraph(t)
	fix := relatingFixture(t, g)

	got, err := g.Temporal.NodesRelating(100, 0, types.Before.Set()) // b = [100,∞)
	if err != nil {
		t.Fatalf("NodesRelating open: %v", err)
	}
	// Only [10,50) is Before [100,∞); [10,100) Meets, everything else overlaps ∞.
	assertNodeSet(t, "Relating{Before}[100,∞)", got, fix["Before"])

	// Meets against the open query: [10,100) abuts bStart 100.
	gotM, err := g.Temporal.NodesRelating(100, 0, types.Meets.Set())
	if err != nil {
		t.Fatalf("NodesRelating open Meets: %v", err)
	}
	assertNodeSet(t, "Relating{Meets}[100,∞)", gotM, fix["Meets"])
}

// TestNodesRelating_PredicateAnywhere is the two-phase history proof (rule 15):
// a node whose CURRENT (head) version does NOT relate as {Before} still matches
// because an EARLIER version's interval did. If history were not retained the
// query would return empty.
func TestNodesRelating_PredicateAnywhere(t *testing.T) {
	g := newTxTimeGraph(t)
	ctx := context.Background()

	// Phase 1: [10,∞) — Contains [100,200), NOT Before it.
	n, err := g.Nodes.Add(ctx, []string{"H"}, vf(10, nil))
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	before := func(set types.AllenRelationSet) []*types.Node {
		got, err := g.Temporal.NodesRelating(100, 200, set)
		if err != nil {
			t.Fatalf("NodesRelating: %v", err)
		}
		return got
	}
	// No version is Before [100,200) yet.
	assertNodeSet(t, "phase1 {Before}", before(types.Before.Set()), nil)
	assertNodeSet(t, "phase1 {Contains}", before(types.Contains.Set()), []types.NodeID{n.ID()})

	// Phase 2: update so genesis tiles to [10,60) (Before [100,200)) and the head
	// becomes [60,∞) (still Contains).
	if _, err := g.Nodes.Update(ctx, n.ID(), vf(60, map[string]any{"v": int64(2)})); err != nil {
		t.Fatalf("update: %v", err)
	}
	// The retained older tile [10,60) is Before [100,200) → predicate-anywhere hit.
	assertNodeSet(t, "phase2 {Before}(older tile)", before(types.Before.Set()),
		[]types.NodeID{n.ID()})
	// The head [60,∞) still Contains.
	assertNodeSet(t, "phase2 {Contains}(head)", before(types.Contains.Set()),
		[]types.NodeID{n.ID()})
	// During matches no version → absent.
	assertNodeSet(t, "phase2 {During}(none)", before(types.During.Set()), nil)
}

// TestRelsRelating_AllThirteenRelations is the relationship mirror (rule 2).
func TestRelsRelating_AllThirteenRelations(t *testing.T) {
	g := newTxTimeGraph(t)
	ctx := context.Background()
	a, _ := g.Nodes.Add(ctx, []string{"N"}, nil)
	b, _ := g.Nodes.Add(ctx, []string{"N"}, nil)

	add := func(from, to types.Instant) types.RelID {
		t.Helper()
		r, err := g.Rels.AddByID(ctx, "KNOWS", a.ID(), b.ID(), vfvt(from, to))
		if err != nil {
			t.Fatalf("add rel [%d,%d): %v", from, to, err)
		}
		return r.ID()
	}
	addOpen := func(from types.Instant) types.RelID {
		t.Helper()
		r, err := g.Rels.AddByID(ctx, "KNOWS", a.ID(), b.ID(), vf(from, nil))
		if err != nil {
			t.Fatalf("add rel [%d,∞): %v", from, err)
		}
		return r.ID()
	}
	fix := map[string][]types.RelID{
		"Before":       {add(10, 50)},
		"Meets":        {add(10, 100)},
		"Overlaps":     {add(50, 150)},
		"Starts":       {add(100, 150)},
		"During":       {add(120, 180)},
		"Equals":       {add(100, 200)},
		"Finishes":     {add(150, 200)},
		"Contains":     {add(50, 250), addOpen(10)},
		"After":        {add(250, 300)},
		"MetBy":        {add(200, 250)},
		"OverlappedBy": {add(150, 250)},
		"StartedBy":    {add(100, 250)},
		"FinishedBy":   {add(50, 200)},
	}

	relating := func(set types.AllenRelationSet) []*types.Relationship {
		got, err := g.Temporal.RelsRelating(100, 200, set)
		if err != nil {
			t.Fatalf("RelsRelating: %v", err)
		}
		return got
	}
	for _, rel := range []types.AllenRelation{
		types.Before, types.Meets, types.Overlaps, types.Starts, types.During,
		types.Equals, types.Finishes, types.Contains, types.After, types.MetBy,
		types.OverlappedBy, types.StartedBy, types.FinishedBy,
	} {
		assertRelSet(t, "RelRelating{"+rel.String()+"}", relating(rel.Set()), fix[rel.String()])
	}
	if got := relating(0); len(got) != 0 {
		t.Fatalf("RelRelating{empty} = %d, want 0", len(got))
	}
}

// TestRelating_InvalidRange asserts the query-interval guard: an open START or a
// closed range with from >= to is rejected; an open END (to==0) is accepted.
func TestRelating_InvalidRange(t *testing.T) {
	g := newTxTimeGraph(t)
	if _, err := g.Temporal.NodesRelating(0, 200, types.Before.Set()); !errors.Is(err, storepkg.ErrInvalidTimeRange) {
		t.Errorf("open start: want ErrInvalidTimeRange, got %v", err)
	}
	if _, err := g.Temporal.NodesRelating(200, 100, types.Before.Set()); !errors.Is(err, storepkg.ErrInvalidTimeRange) {
		t.Errorf("inverted: want ErrInvalidTimeRange, got %v", err)
	}
	if _, err := g.Temporal.RelsRelating(0, 200, types.Before.Set()); !errors.Is(err, storepkg.ErrInvalidTimeRange) {
		t.Errorf("rel open start: want ErrInvalidTimeRange, got %v", err)
	}
	// Open end is valid.
	if _, err := g.Temporal.NodesRelating(100, 0, types.Before.Set()); err != nil {
		t.Errorf("open end must be valid: %v", err)
	}
}
