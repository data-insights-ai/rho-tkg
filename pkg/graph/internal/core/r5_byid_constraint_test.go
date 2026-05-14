// Tests in this file pin R5-F7 from the 2026-05-09 maintainability
// review and the later R9 parity fix: graph-level temporal constraints
// and endpoint-hash capture must be enforced by every relationship-create
// entry point. The ByID variants accept endpoint IDs instead of node
// objects, but their relationship state must match Rels.Add.
package core

import (
	"context"
	"errors"
	"testing"

	temporalpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/temporal"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

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

// ByID is only an input-shape variant. Even without configured
// constraints it must fetch live endpoints and capture endpoint hashes
// so the relationship integrity shape matches Rels.Add.
func TestR9_AddByID_CapturesEndpointHashesWithoutConstraints(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	// No constraints configured.

	a, err := g.Nodes.Add(context.Background(), []string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	r, err := g.Rels.AddByID(context.Background(), "LINK", a.ID(), b.ID(), nil)
	if err != nil {
		t.Fatalf("AddByID with no constraints: %v", err)
	}
	ig := r.Integrity()
	if ig == nil {
		t.Fatal("rel has nil Integrity")
	}
	if ig.FromNodeHash == "" || ig.ToNodeHash == "" {
		t.Errorf("ByID missed endpoint hashes without constraints — From=%q To=%q", ig.FromNodeHash, ig.ToNodeHash)
	}
}

// R5-F7 sister test: with constraints configured, the constrained
// path captures endpoint hashes (so existing R4-F5 invariants
// continue to hold for the ByID path too).
func TestR5_AddByID_ConstrainedPathCapturesEndpointHashes(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	addRelWithinEndpointsConstraint(t, g)

	a, err := g.Nodes.Add(context.Background(), []string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	r, err := g.Rels.AddByID(context.Background(), "LINK", a.ID(), b.ID(), nil)
	if err != nil {
		t.Fatalf("AddByID with constraints: %v", err)
	}
	ig := r.Integrity()
	if ig == nil {
		t.Fatal("rel has nil Integrity")
	}
	if ig.FromNodeHash == "" || ig.ToNodeHash == "" {
		t.Errorf("constrained path missed endpoint hashes — From=%q To=%q", ig.FromNodeHash, ig.ToNodeHash)
	}
}
