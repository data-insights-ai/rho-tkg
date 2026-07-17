package graph_test

import (
	"context"
	"errors"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	constraintspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/constraints"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	temporalpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
)

// TestDryRunValidate_Unique proves the dry-run door reports unique violations
// (committed-current, intra-set duplicate) without mutating the graph.
func TestDryRunValidate_Unique(t *testing.T) {
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	if err := g.Constraints().CreateUnique(ctx, "User", "email"); err != nil {
		t.Fatalf("CreateUnique: %v", err)
	}
	if _, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "a@x.com"}); err != nil {
		t.Fatalf("add existing user: %v", err)
	}

	facts := constraintspkg.DryRunFacts{
		Nodes: []constraintspkg.DryRunNode{
			{Ref: "dup-existing", Labels: []string{"User"}, Properties: map[string]any{"email": "a@x.com"}}, // violates committed
			{Ref: "fresh", Labels: []string{"User"}, Properties: map[string]any{"email": "b@x.com"}},        // ok
			{Ref: "intra-1", Labels: []string{"User"}, Properties: map[string]any{"email": "c@x.com"}},      // ok (first claim)
			{Ref: "intra-2", Labels: []string{"User"}, Properties: map[string]any{"email": "c@x.com"}},      // violates intra-set
		},
	}
	violations, err := g.Constraints().DryRunValidate(ctx, facts)
	if err != nil {
		t.Fatalf("DryRunValidate: %v", err)
	}

	got := map[string]bool{}
	for _, v := range violations {
		if !errors.Is(v.Err, graphpkg.ErrUniqueViolation) {
			t.Fatalf("violation %q err=%v, want ErrUniqueViolation", v.Ref, v.Err)
		}
		got[v.Ref] = true
	}
	if !got["dup-existing"] || !got["intra-2"] {
		t.Fatalf("expected violations for dup-existing + intra-2, got %v", got)
	}
	if got["fresh"] || got["intra-1"] {
		t.Fatalf("unexpected violation for a valid fact: %v", got)
	}

	// The dry run mutated NOTHING — still exactly one User, and "b@x.com" is free.
	users, _ := g.Nodes().ByLabel("User", storepkg.QueryOpts{})
	if len(users) != 1 {
		t.Fatalf("dry run mutated the graph: %d users, want 1", len(users))
	}
	if _, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "b@x.com"}); err != nil {
		t.Fatalf("b@x.com should be free after a dry run: %v", err)
	}
}

// TestDryRunValidate_ForeverLeavesNoGhostClaim is the key correctness win over
// the Tx+rollback emulation: a dry run of a UniqueForever value must NOT claim
// it, so a later REAL assert of that value still succeeds. (Tx+rollback would
// leave a permanent claim and wrongly bar the value.)
func TestDryRunValidate_ForeverLeavesNoGhostClaim(t *testing.T) {
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	if err := g.Constraints().CreateUniqueForever(ctx, "Account", "handle"); err != nil {
		t.Fatalf("CreateUniqueForever: %v", err)
	}

	// Dry-run a fresh handle — must pass (nobody owns it yet).
	facts := constraintspkg.DryRunFacts{Nodes: []constraintspkg.DryRunNode{
		{Ref: "n", Labels: []string{"Account"}, Properties: map[string]any{"handle": "neo"}},
	}}
	violations, err := g.Constraints().DryRunValidate(ctx, facts)
	if err != nil {
		t.Fatalf("DryRunValidate: %v", err)
	}
	if len(violations) != 0 {
		t.Fatalf("fresh forever value dry-run reported %d violations, want 0", len(violations))
	}

	// A REAL assert of the same value must still succeed — the dry run left no claim.
	if _, err := g.Nodes().Add(ctx, []string{"Account"}, map[string]any{"handle": "neo"}); err != nil {
		t.Fatalf("real add after dry run failed — ghost forever claim: %v", err)
	}
}

// TestDryRunValidate_Temporal proves the door reports temporal (rel-within-
// endpoints) violations for a proposed relationship without asserting it.
func TestDryRunValidate_Temporal(t *testing.T) {
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	if err := g.Constraints().Add(temporalpkg.TemporalConstraint{Kind: temporalpkg.ConstraintRelWithinEndpoints}); err != nil {
		t.Fatalf("Add constraint: %v", err)
	}

	// Start node valid from 1000; a rel proposed to start at 500 is BEFORE it.
	start, err := g.Nodes().Add(ctx, []string{"N"}, map[string]any{"tkg_valid_from": int64(1000)})
	if err != nil {
		t.Fatalf("add start: %v", err)
	}
	end, err := g.Nodes().Add(ctx, []string{"N"}, map[string]any{"tkg_valid_from": int64(1000)})
	if err != nil {
		t.Fatalf("add end: %v", err)
	}

	facts := constraintspkg.DryRunFacts{Rels: []constraintspkg.DryRunRel{
		{Ref: "bad", TypeName: "R", StartID: start.ID(), EndID: end.ID(), ValidFrom: 500},   // before start's validity
		{Ref: "good", TypeName: "R", StartID: start.ID(), EndID: end.ID(), ValidFrom: 2000}, // within
	}}
	violations, err := g.Constraints().DryRunValidate(ctx, facts)
	if err != nil {
		t.Fatalf("DryRunValidate: %v", err)
	}
	if len(violations) != 1 || violations[0].Ref != "bad" {
		t.Fatalf("want exactly one temporal violation for 'bad', got %+v", violations)
	}
	if !errors.Is(violations[0].Err, temporalpkg.ErrTemporalConstraint) {
		t.Fatalf("violation err=%v, want ErrTemporalConstraint", violations[0].Err)
	}
}

// TestDryRunValidate_CoProposedEndpoint proves a relationship whose START is a
// node PROPOSED in the SAME fact set (not yet asserted) is validated against that
// proposed node's interval — not reported as "start node not found".
func TestDryRunValidate_CoProposedEndpoint(t *testing.T) {
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	if err := g.Constraints().Add(temporalpkg.TemporalConstraint{Kind: temporalpkg.ConstraintRelWithinEndpoints}); err != nil {
		t.Fatalf("Add constraint: %v", err)
	}
	// Live end node, valid from 1000, no upper bound.
	b, err := g.Nodes().Add(ctx, []string{"N"}, map[string]any{"tkg_valid_from": int64(1000)})
	if err != nil {
		t.Fatalf("add b: %v", err)
	}

	// Proposed start node A (co-proposed, ID assigned by caller, valid from 2000).
	const proposedA = 999
	facts := constraintspkg.DryRunFacts{
		Nodes: []constraintspkg.DryRunNode{
			{Ref: "A", ID: proposedA, Labels: []string{"N"}, Properties: map[string]any{"tkg_valid_from": int64(2000)}},
		},
		Rels: []constraintspkg.DryRunRel{
			{Ref: "r-before", TypeName: "R", StartID: proposedA, EndID: b.ID(), ValidFrom: 1500}, // before A's 2000 → temporal violation
			{Ref: "r-within", TypeName: "R", StartID: proposedA, EndID: b.ID(), ValidFrom: 2500}, // within → ok
		},
	}
	violations, err := g.Constraints().DryRunValidate(ctx, facts)
	if err != nil {
		t.Fatalf("DryRunValidate: %v", err)
	}

	if len(violations) != 1 {
		t.Fatalf("want exactly one violation, got %d: %+v", len(violations), violations)
	}
	v := violations[0]
	if v.Ref != "r-before" {
		t.Fatalf("violation ref=%q, want r-before", v.Ref)
	}
	// CRUCIAL: it must be a TEMPORAL violation (A resolved from the proposed set),
	// NOT an "invalid: start node not found" (A is not a live node).
	if v.Kind != "temporal" || !errors.Is(v.Err, temporalpkg.ErrTemporalConstraint) {
		t.Fatalf("violation kind=%q err=%v, want a temporal violation (co-proposed endpoint not resolved?)", v.Kind, v.Err)
	}
}
