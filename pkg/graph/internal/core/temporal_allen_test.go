package core

import (
	"errors"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Error propagation ---

func TestNodeInterval_OpenEnded(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	n, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	// Freshly created node has ValidTo == 0 (open-ended).
	_, _, err = g.Temporal.NodeInterval(n)
	if !errors.Is(err, types.ErrOpenInterval) {
		t.Errorf("NodeInterval on open-ended node: got err=%v, want ErrOpenInterval", err)
	}
}

func TestRelInterval_OpenEnded(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	b, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	r, err := g.Rels.Add("KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	_, _, err = g.Temporal.RelInterval(r)
	if !errors.Is(err, types.ErrOpenInterval) {
		t.Errorf("RelInterval on open-ended rel: got err=%v, want ErrOpenInterval", err)
	}
}

func TestNodeInterval_Resolved(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	n, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	// Set explicit ValidFrom and ValidTo.
	n.SetTemporal(&types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000})

	start, end, err := g.Temporal.NodeInterval(n)
	if err != nil {
		t.Fatalf("NodeInterval: %v", err)
	}
	if start != 1000 {
		t.Errorf("start = %d, want 1000", start)
	}
	if end != 2000 {
		t.Errorf("end = %d, want 2000", end)
	}
}

func TestNodeInterval_SnowflakeDerived(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	n, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	// Set only ValidTo (no explicit ValidFrom) — start should come from snowflake ID.
	n.SetTemporal(&types.TemporalMetadata{ValidTo: 9999999999999})

	start, end, err := g.Temporal.NodeInterval(n)
	if err != nil {
		t.Fatalf("NodeInterval: %v", err)
	}
	if start == 0 {
		t.Error("start should be derived from snowflake ID, got 0")
	}
	if end != 9999999999999 {
		t.Errorf("end = %d, want 9999999999999", end)
	}
	if start >= end {
		t.Errorf("start (%d) should be < end (%d)", start, end)
	}
}

func TestNodeInterval_NilTemporal(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	n, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	n.SetTemporal(nil)

	_, _, err = g.Temporal.NodeInterval(n)
	if !errors.Is(err, types.ErrOpenInterval) {
		t.Errorf("NodeInterval with nil temporal: got err=%v, want ErrOpenInterval", err)
	}
}

func TestNodeInterval_NilNode(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	_, _, err := g.Temporal.NodeInterval(nil)
	if !errors.Is(err, ErrNilNode) {
		t.Fatalf("NodeInterval(nil): got err=%v, want ErrNilNode", err)
	}
}

func TestRelInterval_NilRelationship(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	_, _, err := g.Temporal.RelInterval(nil)
	if !errors.Is(err, ErrNilRelationship) {
		t.Fatalf("RelInterval(nil): got err=%v, want ErrNilRelationship", err)
	}
}

// --- Relation classification through Graph ---

// makeFiniteNode creates a node via the graph and sets its interval to [from, to).
func makeFiniteNode(t *testing.T, g *Core, from, to types.Instant) *types.Node {
	t.Helper()
	n, err := g.Nodes.Add([]string{"Event"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	n.SetTemporal(&types.TemporalMetadata{ValidFrom: from, ValidTo: to})
	return n
}

func makeFiniteRel(t *testing.T, g *Core, from, to types.Instant) *types.Relationship {
	t.Helper()
	a, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	b, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	r, err := g.Rels.Add("KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	r.SetTemporal(&types.TemporalMetadata{ValidFrom: from, ValidTo: to})
	return r
}

func TestRelateNodes_AllRelations(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	cases := []struct {
		name       string
		aFrom, aTo types.Instant
		bFrom, bTo types.Instant
		want       types.AllenRelation
	}{
		{"Before", 100, 200, 300, 400, types.Before},
		{"After", 300, 400, 100, 200, types.After},
		{"Meets", 100, 200, 200, 300, types.Meets},
		{"MetBy", 200, 300, 100, 200, types.MetBy},
		{"Overlaps", 100, 250, 200, 400, types.Overlaps},
		{"OverlappedBy", 200, 400, 100, 250, types.OverlappedBy},
		{"Starts", 100, 200, 100, 400, types.Starts},
		{"StartedBy", 100, 400, 100, 200, types.StartedBy},
		{"During", 200, 300, 100, 400, types.During},
		{"Contains", 100, 400, 200, 300, types.Contains},
		{"Finishes", 300, 400, 100, 400, types.Finishes},
		{"FinishedBy", 100, 400, 300, 400, types.FinishedBy},
		{"Equals", 100, 400, 100, 400, types.Equals},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			a := makeFiniteNode(t, g, tc.aFrom, tc.aTo)
			b := makeFiniteNode(t, g, tc.bFrom, tc.bTo)

			got, err := g.Temporal.RelateNodes(a, b)
			if err != nil {
				t.Fatalf("RelateNodes: %v", err)
			}
			if got != tc.want {
				t.Errorf("RelateNodes = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestRelateNodes_NilNode(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	n := makeFiniteNode(t, g, 100, 200)

	if _, err := g.Temporal.RelateNodes(nil, n); !errors.Is(err, ErrNilNode) {
		t.Fatalf("RelateNodes(nil, n): got err=%v, want ErrNilNode", err)
	}
	if _, err := g.Temporal.RelateNodes(n, nil); !errors.Is(err, ErrNilNode) {
		t.Fatalf("RelateNodes(n, nil): got err=%v, want ErrNilNode", err)
	}
}

func TestRelateNodes_InverseConsistency(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	pairs := [][4]types.Instant{
		{100, 200, 300, 400}, // Before/After
		{100, 200, 200, 300}, // Meets/MetBy
		{100, 250, 200, 400}, // Overlaps/OverlappedBy
		{100, 200, 100, 400}, // Starts/StartedBy
		{200, 300, 100, 400}, // During/Contains
		{300, 400, 100, 400}, // Finishes/FinishedBy
		{100, 400, 100, 400}, // Equals/Equals
	}
	for _, p := range pairs {
		a := makeFiniteNode(t, g, p[0], p[1])
		b := makeFiniteNode(t, g, p[2], p[3])

		ab, err := g.Temporal.RelateNodes(a, b)
		if err != nil {
			t.Fatalf("RelateNodes(a,b): %v", err)
		}
		ba, err := g.Temporal.RelateNodes(b, a)
		if err != nil {
			t.Fatalf("RelateNodes(b,a): %v", err)
		}
		if ab.Inverse() != ba {
			t.Errorf("RelateNodes(a,b)=%v, Inverse=%v, RelateNodes(b,a)=%v — mismatch",
				ab, ab.Inverse(), ba)
		}
	}
}

func TestRelateRels_Before(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	b, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	r1, err := g.Rels.Add("KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	r2, err := g.Rels.Add("KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	r1.SetTemporal(&types.TemporalMetadata{ValidFrom: 100, ValidTo: 200})
	r2.SetTemporal(&types.TemporalMetadata{ValidFrom: 300, ValidTo: 400})

	got, err := g.Temporal.RelateRels(r1, r2)
	if err != nil {
		t.Fatalf("RelateRels: %v", err)
	}
	if got != types.Before {
		t.Errorf("RelateRels = %v, want Before", got)
	}
}

func TestRelateRels_NilRelationship(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	r := makeFiniteRel(t, g, 100, 200)

	if _, err := g.Temporal.RelateRels(nil, r); !errors.Is(err, ErrNilRelationship) {
		t.Fatalf("RelateRels(nil, r): got err=%v, want ErrNilRelationship", err)
	}
	if _, err := g.Temporal.RelateRels(r, nil); !errors.Is(err, ErrNilRelationship) {
		t.Fatalf("RelateRels(r, nil): got err=%v, want ErrNilRelationship", err)
	}
}

func TestRelateNodes_OneOpenEnded(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a := makeFiniteNode(t, g, 100, 200)
	b, err := g.Nodes.Add([]string{"Event"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	// b has ValidTo == 0 (open-ended)

	_, err = g.Temporal.RelateNodes(a, b)
	if !errors.Is(err, types.ErrOpenInterval) {
		t.Errorf("RelateNodes with one open-ended: got err=%v, want ErrOpenInterval", err)
	}

	// Also test the reverse direction.
	_, err = g.Temporal.RelateNodes(b, a)
	if !errors.Is(err, types.ErrOpenInterval) {
		t.Errorf("RelateNodes (reversed) with one open-ended: got err=%v, want ErrOpenInterval", err)
	}
}

func TestRelateRels_OneOpenEnded(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	b, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	r1, err := g.Rels.Add("KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	r2, err := g.Rels.Add("KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	r1.SetTemporal(&types.TemporalMetadata{ValidFrom: 100, ValidTo: 200})
	// r2 stays open-ended (no ValidTo set by graph layer on creation)

	_, err = g.Temporal.RelateRels(r1, r2)
	if !errors.Is(err, types.ErrOpenInterval) {
		t.Errorf("RelateRels with one open-ended: got err=%v, want ErrOpenInterval", err)
	}
}
