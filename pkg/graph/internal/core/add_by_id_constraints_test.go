// Tests in this file pin that endpoint-hash capture holds on the ByID
// relationship-create doors, with and without constraints. The ByID variants
// accept endpoint IDs instead of node objects, but their relationship state
// must match Rels.Add. Constraint rejection through every door is pinned by
// TestConstraintRelWithinEndpointsAllDoors.
package core

import (
	"context"
	"errors"
	"testing"

	temporalpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestAddByID_CapturesEndpointHashes: ByID is only an input-shape variant.
// With and without configured constraints it fetches the live endpoints and
// records their integrity hashes on the relationship, exactly the hashes the
// endpoint nodes carry, so the relationship integrity shape matches Rels.Add.
func TestAddByID_CapturesEndpointHashes(t *testing.T) {
	t.Parallel()
	for _, constrained := range []bool{false, true} {
		g := newTestGraph(t)
		if constrained {
			addRelWithinEndpointsConstraint(t, g)
		}
		ctx := context.Background()
		a, err := g.Nodes.Add(ctx, []string{"Item"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		b, err := g.Nodes.Add(ctx, []string{"Item"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		r, err := g.Rels.AddByID(ctx, "LINK", a.ID(), b.ID(), nil)
		if err != nil {
			t.Fatalf("constrained=%v: AddByID: %v", constrained, err)
		}
		ig := r.Integrity()
		if ig == nil {
			t.Fatalf("constrained=%v: rel has nil Integrity", constrained)
		}
		wantFrom, wantTo := a.Integrity().Hash, b.Integrity().Hash
		if wantFrom == "" || wantTo == "" {
			t.Fatalf("fixture: endpoint hashes %q, %q", wantFrom, wantTo)
		}
		if ig.FromNodeHash != wantFrom || ig.ToNodeHash != wantTo {
			t.Errorf("constrained=%v: endpoint hashes From=%q To=%q; want %q, %q", constrained, ig.FromNodeHash, ig.ToNodeHash, wantFrom, wantTo)
		}
	}
}

// R5-F7: with ConstraintRelWithinEndpoints set on the graph,
// Rels.AddByID must reject a relationship whose endpoint has already
// expired. Pre-fix the ByID path would silently commit because it
// never fetched the endpoint to check the constraint.
func TestR5_AddByID_EnforcesTemporalConstraint(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	addRelWithinEndpointsConstraint(t, g)

	a, err := g.Nodes.Add(context.Background(), []string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Persist b with a deterministic already-expired interval so the
	// constraint check on the rel must reject as after-expiry rather
	// than before-validity.
	b, err := g.Nodes.Add(context.Background(), []string{"Item"}, map[string]any{
		"tkg_valid_from": types.Instant(1),
		"tkg_valid_to":   types.Instant(2),
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = g.Rels.AddByID(context.Background(), "LINK", a.ID(), b.ID(), nil)
	if !errors.Is(err, temporalpkg.ErrTemporalConstraint) {
		t.Fatalf("AddByID with expired endpoint: got %v, want errors.Is(ErrTemporalConstraint)", err)
	}
}

// R5-F7: same expectation for Rels.AddByIDIfAbsent.
func TestR5_AddByIDIfAbsent_EnforcesTemporalConstraint(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	addRelWithinEndpointsConstraint(t, g)

	a, err := g.Nodes.Add(context.Background(), []string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Item"}, map[string]any{
		"tkg_valid_from": types.Instant(1),
		"tkg_valid_to":   types.Instant(2),
	})
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = g.Rels.AddByIDIfAbsent(context.Background(), "LINK", a.ID(), b.ID(), nil)
	if !errors.Is(err, temporalpkg.ErrTemporalConstraint) {
		t.Fatalf("AddByIDIfAbsent with expired endpoint: got %v, want errors.Is(ErrTemporalConstraint)", err)
	}
}
