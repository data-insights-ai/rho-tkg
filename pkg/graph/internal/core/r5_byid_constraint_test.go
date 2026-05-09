// Tests in this file pin R5-F7 from the 2026-05-09 maintainability
// review: graph-level temporal constraints must be enforced by every
// relationship-creation entry point. The ByID variants previously
// skipped the live-endpoint fetch and silently bypassed
// ConstraintRelWithinEndpoints — silent bypass is gone now: when a
// constraint is configured, the ByID path transparently fetches
// endpoints and runs the same check that Rels.Add runs.
package core

import (
	"context"
	"errors"
	"testing"

	temporalpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/temporal"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// R5-F7: with ConstraintRelWithinEndpoints set on the graph,
// Rels.AddByID must reject a relationship whose endpoint has already
// expired. Pre-fix the ByID path would silently commit because it
// never fetched the endpoint to check the constraint.
func TestR5_AddByID_EnforcesTemporalConstraint(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	g.Constraints.Add(temporalpkg.TemporalConstraint{Kind: temporalpkg.ConstraintRelWithinEndpoints})

	a, err := g.Nodes.Add([]string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Persist b with an already-expired ValidTo so the constraint
	// check on the rel must reject.
	b, err := g.Nodes.Add([]string{"Item"}, map[string]any{
		"tkg_valid_to": types.Instant(1),
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = g.Rels.AddByIDWithContext(context.Background(), "LINK", a.ID(), b.ID(), nil)
	if !errors.Is(err, temporalpkg.ErrTemporalConstraint) {
		t.Fatalf("AddByID with expired endpoint: got %v, want errors.Is(ErrTemporalConstraint)", err)
	}
}

// R5-F7: same expectation for Rels.AddByIDIfAbsent.
func TestR5_AddByIDIfAbsent_EnforcesTemporalConstraint(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	g.Constraints.Add(temporalpkg.TemporalConstraint{Kind: temporalpkg.ConstraintRelWithinEndpoints})

	a, err := g.Nodes.Add([]string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add([]string{"Item"}, map[string]any{
		"tkg_valid_to": types.Instant(1),
	})
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = g.Rels.AddByIDIfAbsentWithContext(context.Background(), "LINK", a.ID(), b.ID(), nil)
	if !errors.Is(err, temporalpkg.ErrTemporalConstraint) {
		t.Fatalf("AddByIDIfAbsent with expired endpoint: got %v, want errors.Is(ErrTemporalConstraint)", err)
	}
}

// R5-F7 fast-path preservation: when no constraints are configured,
// AddByID must NOT fetch endpoints — the original high-throughput
// trade-off is preserved when the user has not opted into constraints.
// We verify by ensuring AddByID still succeeds when the endpoint
// nodes don't exist in the store at all (they couldn't if the path
// were silently fetching them).
//
// The test uses ImportNodeWithID-style setup: create endpoint IDs
// without persisting them, then call AddByID. With no constraints
// the call must succeed (snowflake IDs accepted, no fetch attempted);
// with constraints it would fail with a fetch error.
//
// Implementation note: getting "endpoint IDs that don't exist" is
// awkward through the public API. A simpler proxy: with no
// constraints, AddByID succeeds *and* leaves FromNodeHash/ToNodeHash
// empty (they'd be populated if the endpoints had been fetched). This
// pins the fast path's observable side effect.
func TestR5_AddByID_FastPathSkipsEndpointFetchWhenNoConstraints(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	// No constraints configured.

	a, err := g.Nodes.Add([]string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add([]string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	r, err := g.Rels.AddByIDWithContext(context.Background(), "LINK", a.ID(), b.ID(), nil)
	if err != nil {
		t.Fatalf("AddByID with no constraints: %v", err)
	}
	ig := r.Integrity()
	if ig == nil {
		t.Fatal("rel has nil Integrity")
	}
	if ig.FromNodeHash != "" || ig.ToNodeHash != "" {
		t.Errorf("fast path captured endpoint hashes — From=%q To=%q; want empty pair when no constraints configured", ig.FromNodeHash, ig.ToNodeHash)
	}
}

// R5-F7 sister test: with constraints configured, the constrained
// path captures endpoint hashes (so existing R4-F5 invariants
// continue to hold for the ByID path too).
func TestR5_AddByID_ConstrainedPathCapturesEndpointHashes(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	g.Constraints.Add(temporalpkg.TemporalConstraint{Kind: temporalpkg.ConstraintRelWithinEndpoints})

	a, err := g.Nodes.Add([]string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add([]string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	r, err := g.Rels.AddByIDWithContext(context.Background(), "LINK", a.ID(), b.ID(), nil)
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
