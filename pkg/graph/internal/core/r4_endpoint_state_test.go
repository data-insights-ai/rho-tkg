// Tests in this file pin R4-F5 / R4-F6 / R4-F7 from the 2026-05-08
// maintainability review:
//
//   - R4-F5: addRelationshipInternal must use canonical (live store)
//     endpoint state for the temporal-constraint check, not whatever the
//     caller happened to pass in. Local pointer mutations to a *types.Node
//     must NOT bypass ConstraintRelWithinEndpoints.
//
//   - R4-F6: BatchBuilder.runRels must run the same temporal-constraint
//     check that addRelationshipInternal runs, otherwise queue-then-Execute
//     becomes a constraint-bypass door.
//
//   - R4-F7: updateRelationshipInternal must lock both endpoints (in
//     addition to the rel) before refreshing FromNodeHash / ToNodeHash so a
//     concurrent UpdateNode cannot interleave between read and persist.
package core

import (
	"context"
	"errors"
	"sync"
	"testing"

	temporalpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/temporal"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

// R4-F5: caller-mutated stale endpoint pointers must not bypass the
// temporal-constraint check. The store sees a normal endpoint, but the
// caller hands in a pointer with ValidTo=1 — pre-R4-F5 the constraint
// check used the stale pointer (and rejected); post-R4-F5 it uses the
// live store state (and accepts). This test pins the post-R4-F5 contract:
// the live state wins.
func TestR4_AddRel_UsesLiveEndpointStateForConstraintCheck(t *testing.T) {
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

	// Stale local mutation — must NOT influence the constraint decision.
	a.SetTemporal(&types.TemporalMetadata{ValidTo: 1})

	if _, err := g.Rels.Add(context.Background(), "LINK", a, b, nil); err != nil {
		t.Fatalf("AddRelationship rejected: caller pointer mutation must not influence the constraint check (R4-F5): %v", err)
	}
}

// R4-F6: the batch path enforces the same temporal-constraint check as
// addRelationshipInternal. Persist a node with ValidTo=1 in the store,
// then queue a batched AddRelationship pointing at it — Execute must
// surface the constraint violation as a per-rel BatchError, not silently
// commit a constraint-violating relationship.
func TestR4_BatchAddRel_EnforcesTemporalConstraints(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	addRelWithinEndpointsConstraint(t, g)

	a, err := g.Nodes.Add(context.Background(), []string{"Item"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Persist b with a deterministic expired interval in the store so the
	// live state seen by the batch's constraint check is genuinely
	// past-its-expiry.
	b, err := g.Nodes.Add(context.Background(), []string{"Item"}, map[string]any{
		"tkg_valid_from": types.Instant(1),
		"tkg_valid_to":   types.Instant(2),
	})
	if err != nil {
		t.Fatal(err)
	}

	bb, _ := NewBatchBuilder(g)
	if _, err := bb.AddRelationship("LINK", a, b, nil); err != nil {
		t.Fatalf("BatchBuilder.AddRelationship: %v", err)
	}
	res, err := bb.Execute()
	if !errors.Is(err, ErrBatchFailed) {
		t.Fatalf("Batch.Execute error = %v, want ErrBatchFailed", err)
	}
	if res.Created != 0 {
		t.Errorf("res.Created = %d, want 0 (constraint violation must skip the rel)", res.Created)
	}
	if res.Failed != 1 {
		t.Fatalf("res.Failed = %d, want 1; Errors=%v", res.Failed, res.Errors)
	}
	if len(res.Errors) != 1 {
		t.Fatalf("len(res.Errors) = %d, want 1", len(res.Errors))
	}
	be := res.Errors[0]
	if be.Op != "AddRelationship" {
		t.Errorf("BatchError.Op = %q, want %q", be.Op, "AddRelationship")
	}
	if !errors.Is(be.Err, temporalpkg.ErrTemporalConstraint) {
		t.Errorf("errors.Is(BatchError.Err, ErrTemporalConstraint) = false; err = %v", be.Err)
	}
	if !errors.Is(be.Err, temporalpkg.ErrRelAfterEndNode) {
		t.Errorf("errors.Is(BatchError.Err, ErrRelAfterEndNode) = false; err = %v", be.Err)
	}
}

// R4-F7: updateRelationshipInternal must hold endpoint locks while
// refreshing endpoint hashes. We can't deterministically race-test that,
// but we can confirm the new lock acquisition (rel + start + end via
// LockMany) is deadlock-free under heavy interleaved updates of both the
// rel and an endpoint.
func TestR4_RelUpdate_NoDeadlockWithEndpointUpdate(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	a, err := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels.Add(context.Background(), "LINK", a, b, nil)
	if err != nil {
		t.Fatal(err)
	}

	const iterations = 50
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			if _, err := g.Rels.Update(context.Background(), r.ID(), map[string]any{"v": i}); err != nil {
				t.Errorf("Rels.Update[%d]: %v", i, err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			if _, err := g.Nodes.Update(context.Background(), a.ID(), map[string]any{"v": i}); err != nil {
				t.Errorf("Nodes.Update[%d]: %v", i, err)
				return
			}
		}
	}()
	wg.Wait()
}
