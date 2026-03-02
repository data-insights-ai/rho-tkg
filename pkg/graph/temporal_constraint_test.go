package graph

import (
	"context"
	"errors"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

// --- ConstraintSet unit tests ---

func TestTemporalConstraintSet_Add_Len_Items(t *testing.T) {
	t.Parallel()

	cs := NewConstraintSet()
	if cs.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", cs.Len())
	}
	if items := cs.Items(); items != nil {
		t.Fatalf("Items() = %v, want nil", items)
	}

	c1 := TemporalConstraint{Kind: ConstraintRelWithinEndpoints}
	cs2 := cs.Add(c1)

	// Original unchanged.
	if cs.Len() != 0 {
		t.Errorf("original Len() = %d after Add, want 0", cs.Len())
	}
	// New set has one item.
	if cs2.Len() != 1 {
		t.Errorf("cs2.Len() = %d, want 1", cs2.Len())
	}
	items := cs2.Items()
	if len(items) != 1 || items[0] != c1 {
		t.Errorf("cs2.Items() = %v, want [%v]", items, c1)
	}

	// Items returns a copy — mutation does not affect the set.
	items[0] = TemporalConstraint{Kind: 99}
	if cs2.Items()[0] != c1 {
		t.Error("Items() returned a non-copy; modifying it changed the set")
	}
}

func TestNewConstraintSet(t *testing.T) {
	t.Parallel()

	c := TemporalConstraint{Kind: ConstraintRelWithinEndpoints}
	cs := NewConstraintSet(c)
	if cs.Len() != 1 {
		t.Errorf("Len() = %d, want 1", cs.Len())
	}
}

// --- TestSetTemporalConstraints_Replace ---

func TestSetTemporalConstraints_Replace(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	c := TemporalConstraint{Kind: ConstraintRelWithinEndpoints}
	g.AddTemporalConstraint(c)
	if g.TemporalConstraints().Len() != 1 {
		t.Fatalf("Len() = %d after AddTemporalConstraint, want 1", g.TemporalConstraints().Len())
	}

	// Replace with a set of two constraints.
	cs := NewConstraintSet(c, c)
	g.SetTemporalConstraints(cs)
	if g.TemporalConstraints().Len() != 2 {
		t.Errorf("Len() = %d after SetTemporalConstraints(2), want 2", g.TemporalConstraints().Len())
	}

	// Replace with empty — clears all constraints.
	g.SetTemporalConstraints(ConstraintSet{})
	if g.TemporalConstraints().Len() != 0 {
		t.Errorf("Len() = %d after SetTemporalConstraints(empty), want 0", g.TemporalConstraints().Len())
	}
}

// --- Graph-level constraint tests ---

func TestConstraintRelWithinEndpoints_NoConstraint(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	// No constraint added.

	a, err := g.AddNode([]string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.AddNode([]string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Even with an expired start node, no constraint means no error.
	a.SetTemporal(&types.TemporalMetadata{ValidTo: 1})

	_, err = g.AddRelationship("LINK", a, b, nil)
	if err != nil {
		t.Errorf("unexpected error without constraints: %v", err)
	}
}

func TestConstraintRelWithinEndpoints_Valid(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	g.AddTemporalConstraint(TemporalConstraint{Kind: ConstraintRelWithinEndpoints})

	// Open-ended nodes, open-ended rel — always valid.
	a, err := g.AddNode([]string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.AddNode([]string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = g.AddRelationship("LINK", a, b, nil)
	if err != nil {
		t.Errorf("AddRelationship with open-ended nodes: %v", err)
	}
}

func TestConstraintRelWithinEndpoints_RelBeforeStartNode(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	g.AddTemporalConstraint(TemporalConstraint{Kind: ConstraintRelWithinEndpoints})

	a, err := g.AddNode([]string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.AddNode([]string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Set start node's ValidFrom to the far future — rel's snowflake-derived from
	// is approximately "now" relative to the epoch (~few billion ms), which is less
	// than 9_999_999_999_999.
	a.SetTemporal(&types.TemporalMetadata{ValidFrom: 9_999_999_999_999})

	_, err = g.AddRelationship("LINK", a, b, nil)
	if err == nil {
		t.Fatal("expected ErrRelBeforeStartNode, got nil")
	}
	if !errors.Is(err, ErrTemporalConstraint) {
		t.Errorf("errors.Is(err, ErrTemporalConstraint) = false; err = %v", err)
	}
	if !errors.Is(err, ErrRelBeforeStartNode) {
		t.Errorf("errors.Is(err, ErrRelBeforeStartNode) = false; err = %v", err)
	}
}

func TestConstraintRelWithinEndpoints_RelBeforeEndNode(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	g.AddTemporalConstraint(TemporalConstraint{Kind: ConstraintRelWithinEndpoints})

	a, err := g.AddNode([]string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.AddNode([]string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// End node valid from the far future.
	b.SetTemporal(&types.TemporalMetadata{ValidFrom: 9_999_999_999_999})

	_, err = g.AddRelationship("LINK", a, b, nil)
	if err == nil {
		t.Fatal("expected ErrRelBeforeEndNode, got nil")
	}
	if !errors.Is(err, ErrTemporalConstraint) {
		t.Errorf("errors.Is(err, ErrTemporalConstraint) = false; err = %v", err)
	}
	if !errors.Is(err, ErrRelBeforeEndNode) {
		t.Errorf("errors.Is(err, ErrRelBeforeEndNode) = false; err = %v", err)
	}
}

func TestConstraintRelWithinEndpoints_RelAfterStartNodeExpiry(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	g.AddTemporalConstraint(TemporalConstraint{Kind: ConstraintRelWithinEndpoints})

	a, err := g.AddNode([]string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.AddNode([]string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Start node expired at t=1ms. Rel's snowflake-derived from is ~now >> 1.
	a.SetTemporal(&types.TemporalMetadata{ValidTo: 1})

	_, err = g.AddRelationship("LINK", a, b, nil)
	if err == nil {
		t.Fatal("expected ErrRelAfterStartNode, got nil")
	}
	if !errors.Is(err, ErrTemporalConstraint) {
		t.Errorf("errors.Is(err, ErrTemporalConstraint) = false; err = %v", err)
	}
	if !errors.Is(err, ErrRelAfterStartNode) {
		t.Errorf("errors.Is(err, ErrRelAfterStartNode) = false; err = %v", err)
	}
}

func TestConstraintRelWithinEndpoints_RelAfterEndNodeExpiry(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	g.AddTemporalConstraint(TemporalConstraint{Kind: ConstraintRelWithinEndpoints})

	a, err := g.AddNode([]string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.AddNode([]string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// End node expired at t=1ms.
	b.SetTemporal(&types.TemporalMetadata{ValidTo: 1})

	_, err = g.AddRelationship("LINK", a, b, nil)
	if err == nil {
		t.Fatal("expected ErrRelAfterEndNode, got nil")
	}
	if !errors.Is(err, ErrTemporalConstraint) {
		t.Errorf("errors.Is(err, ErrTemporalConstraint) = false; err = %v", err)
	}
	if !errors.Is(err, ErrRelAfterEndNode) {
		t.Errorf("errors.Is(err, ErrRelAfterEndNode) = false; err = %v", err)
	}
}

func TestConstraintRelWithinEndpoints_RelExceedsNodeValidity(t *testing.T) {
	// Checks (5)/(6): rel.ValidTo > node.ValidTo (both explicit).
	// Since AddRelationship doesn't set rel temporal, test via the internal method directly.
	t.Parallel()
	g := newTestGraph(t)
	g.AddTemporalConstraint(TemporalConstraint{Kind: ConstraintRelWithinEndpoints})

	a, err := g.AddNode([]string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.AddNode([]string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Derive the rel's effective from time so we can set node ranges around it.
	relID := g.NextRelID()
	relFrom := entityValidFrom(relID, nil)

	// Both endpoint nodes: valid from before rel, expires after rel.
	a.SetTemporal(&types.TemporalMetadata{
		ValidFrom: relFrom - 1000,
		ValidTo:   relFrom + 500, // node expires at relFrom+500
	})
	b.SetTemporal(&types.TemporalMetadata{
		ValidFrom: relFrom - 1000,
		ValidTo:   relFrom + 500,
	})

	// Manually create a relationship with ValidTo > node.ValidTo.
	rtok, _ := g.relTypes.GetOrCreate("LINK")
	startID := a.InternalID().SnowflakeID()
	endID := b.InternalID().SnowflakeID()
	r := types.NewRelationship(relID, rtok, startID, endID)
	r.SetTemporal(&types.TemporalMetadata{ValidTo: relFrom + 1000}) // exceeds node ValidTo of relFrom+500

	err = g.checkTemporalConstraints(r, a, b)
	if err == nil {
		t.Fatal("expected ErrRelExceedsStartNodeValidity, got nil")
	}
	if !errors.Is(err, ErrTemporalConstraint) {
		t.Errorf("errors.Is(err, ErrTemporalConstraint) = false; err = %v", err)
	}
	if !errors.Is(err, ErrRelExceedsStartNodeValidity) {
		t.Errorf("errors.Is(err, ErrRelExceedsStartNodeValidity) = false; err = %v", err)
	}
}

func TestConstraintRelWithinEndpoints_RelExceedsEndNodeValidity(t *testing.T) {
	// rel.ValidTo > endNode.ValidTo — start node has no ValidTo but end node does.
	t.Parallel()
	g := newTestGraph(t)
	g.AddTemporalConstraint(TemporalConstraint{Kind: ConstraintRelWithinEndpoints})

	a, err := g.AddNode([]string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.AddNode([]string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	relID := g.NextRelID()
	relFrom := entityValidFrom(relID, nil)

	// Start node open-ended, end node expires at relFrom+500.
	// Start node: no ValidTo — checks (5) won't fire.
	// End node: ValidTo = relFrom+500 — check (6) fires.
	b.SetTemporal(&types.TemporalMetadata{
		ValidFrom: relFrom - 1000,
		ValidTo:   relFrom + 500,
	})

	rtok, _ := g.relTypes.GetOrCreate("LINK")
	startID := a.InternalID().SnowflakeID()
	endID := b.InternalID().SnowflakeID()
	r := types.NewRelationship(relID, rtok, startID, endID)
	r.SetTemporal(&types.TemporalMetadata{ValidTo: relFrom + 1000}) // exceeds endNode.ValidTo

	err = g.checkTemporalConstraints(r, a, b)
	if err == nil {
		t.Fatal("expected ErrRelExceedsEndNodeValidity, got nil")
	}
	if !errors.Is(err, ErrTemporalConstraint) {
		t.Errorf("errors.Is(err, ErrTemporalConstraint) = false; err = %v", err)
	}
	if !errors.Is(err, ErrRelExceedsEndNodeValidity) {
		t.Errorf("errors.Is(err, ErrRelExceedsEndNodeValidity) = false; err = %v", err)
	}
}

func TestConstraintRelWithinEndpoints_ErrorsIs(t *testing.T) {
	// errors.Is must work on both the outer sentinel and specific leaf error.
	t.Parallel()
	g := newTestGraph(t)
	g.AddTemporalConstraint(TemporalConstraint{Kind: ConstraintRelWithinEndpoints})

	a, err := g.AddNode([]string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.AddNode([]string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Start node expired.
	a.SetTemporal(&types.TemporalMetadata{ValidTo: 1})

	_, err = g.AddRelationship("LINK", a, b, nil)
	if err == nil {
		t.Fatal("expected constraint error")
	}

	if !errors.Is(err, ErrTemporalConstraint) {
		t.Errorf("errors.Is(err, ErrTemporalConstraint) = false; err = %v", err)
	}
	if !errors.Is(err, ErrRelAfterStartNode) {
		t.Errorf("errors.Is(err, ErrRelAfterStartNode) = false; err = %v", err)
	}
}

// TestConstraintRelWithinEndpoints_ImportRel ensures the constraint hook also fires
// in ImportRelationshipWithID.
func TestConstraintRelWithinEndpoints_ImportRel(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	g.AddTemporalConstraint(TemporalConstraint{Kind: ConstraintRelWithinEndpoints})

	a, err := g.AddNode([]string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.AddNode([]string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// End node expired.
	b.SetTemporal(&types.TemporalMetadata{ValidTo: 1})

	relID := g.NextRelID()
	_, err = g.ImportRelationshipWithID(context.Background(), relID, "LINK", a, b, nil)
	if err == nil {
		t.Fatal("expected ErrRelAfterEndNode from ImportRelationshipWithID, got nil")
	}
	if !errors.Is(err, ErrTemporalConstraint) {
		t.Errorf("errors.Is(err, ErrTemporalConstraint) = false; err = %v", err)
	}
	if !errors.Is(err, ErrRelAfterEndNode) {
		t.Errorf("errors.Is(err, ErrRelAfterEndNode) = false; err = %v", err)
	}
}
