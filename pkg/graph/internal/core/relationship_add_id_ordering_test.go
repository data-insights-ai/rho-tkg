package core

import (
	"context"
	"errors"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	temporalpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// newTestGraphBadgerNoEndpointHashWrite returns a Core over a badger store.
// nativeRelationshipEndpointHashWrite only wires the fast-path
// endpointHashWrite capability for *memory.Store/*tiered.Store (core.go:875-889),
// so a badger-backed Core always takes relEndpointHashLadder's TRUE default
// liveEndpointHashes branch — the one this fix's existence-check-before-mint
// ordering actually protects. (memory.Store's endpointHashWrite fast path
// skips the ladder's own existence check entirely — a separate, pre-existing,
// out-of-scope gap; existence there is caught later by the store write.)
func newTestGraphBadgerNoEndpointHashWrite(t *testing.T) *Core {
	t.Helper()
	bs, err := badger.New(badger.Config{InMemory: true})
	if err != nil {
		t.Fatalf("badger.New: %v", err)
	}
	g, err := New(Config{Store: bs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { g.Close() })
	return g
}

// BACKLOG 9d: createRelationshipLocked / addRelationshipByIDIfAbsentInternal
// used to call c.nextRelID() BEFORE relEndpointHashLadder confirmed both
// endpoints exist — violating the Design Rules invariant "validate before
// generating IDs" (AddNode/AddRelationship's genesis path already honors
// it). A create attempt against a missing endpoint burned a freshly minted
// snowflake ID for nothing. The fix threads ID minting INTO the ladder as a
// lazily-invoked mintID callback, called only after each branch's own
// endpoint-existence check has already passed.
//
// These tests exercise relEndpointHashLadder directly with a call-counting
// spy in place of c.nextRelID — the precise mechanism the fix changes —
// covering both the no-constraints (liveEndpointHashes) and
// constraints-configured (liveEndpointNodes + temporal check) branches.

func TestRelEndpointHashLadder_DoesNotMintIDWhenEndpointMissing(t *testing.T) {
	g := newTestGraphBadgerNoEndpointHashWrite(t)
	ctx := context.Background()

	start, err := g.Nodes.Add(ctx, []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("Add start: %v", err)
	}
	missingEnd := types.NodeID(999_999_999) // never created

	mintCalls := 0
	mint := func() types.RelID {
		mintCalls++
		return types.RelID(12345)
	}

	_, _, _, _, err = g.relEndpointHashLadder(mint, start.ID(), missingEnd, 0, 0, 0)
	if err == nil {
		t.Fatal("relEndpointHashLadder with a missing endpoint = nil error, want a fetch error")
	}
	if mintCalls != 0 {
		t.Fatalf("mintID called %d times on a failed endpoint validation, want 0 — BACKLOG 9d regression (ID minted before validation)", mintCalls)
	}
}

func TestRelEndpointHashLadder_MintsIDAfterValidationPasses(t *testing.T) {
	g := newTestGraphBadgerNoEndpointHashWrite(t)
	ctx := context.Background()

	start, err := g.Nodes.Add(ctx, []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("Add start: %v", err)
	}
	end, err := g.Nodes.Add(ctx, []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("Add end: %v", err)
	}

	mintCalls := 0
	const wantID = types.RelID(777)
	mint := func() types.RelID {
		mintCalls++
		return wantID
	}

	id, _, _, _, err := g.relEndpointHashLadder(mint, start.ID(), end.ID(), 0, 0, 0)
	if err != nil {
		t.Fatalf("relEndpointHashLadder with valid endpoints: %v", err)
	}
	if mintCalls != 1 {
		t.Fatalf("mintID called %d times on a successful validation, want exactly 1", mintCalls)
	}
	if id != wantID {
		t.Fatalf("returned id = %v, want the minted %v", id, wantID)
	}
}

// TestRelEndpointHashLadder_ConstraintsBranch_DoesNotMintIDWhenEndpointMissing
// covers the c.constraints.Len() > 0 branch, which fetches endpoints via
// liveEndpointNodes (a different code path from the default
// liveEndpointHashes branch) and additionally needs the minted id to build
// the temporal-constraint probe — the ordering fix must delay minting past
// THIS branch's own existence check too.
func TestRelEndpointHashLadder_ConstraintsBranch_DoesNotMintIDWhenEndpointMissing(t *testing.T) {
	g := newTestGraph(t)
	ctx := context.Background()

	if err := g.Constraints.Add(temporalpkg.TemporalConstraint{Kind: temporalpkg.ConstraintRelWithinEndpoints}); err != nil {
		t.Fatalf("Constraints.Add: %v", err)
	}

	start, err := g.Nodes.Add(ctx, []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("Add start: %v", err)
	}
	missingEnd := types.NodeID(999_999_998)

	mintCalls := 0
	mint := func() types.RelID {
		mintCalls++
		return types.RelID(1)
	}

	_, _, _, _, err = g.relEndpointHashLadder(mint, start.ID(), missingEnd, 0, 0, 0)
	if err == nil {
		t.Fatal("relEndpointHashLadder (constraints branch) with a missing endpoint = nil error, want a fetch error")
	}
	if !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("error = %v, want wrapping ErrNodeNotFound", err)
	}
	if mintCalls != 0 {
		t.Fatalf("mintID called %d times on a failed endpoint validation (constraints branch), want 0 — BACKLOG 9d regression", mintCalls)
	}
}

// TestAddByID_MissingEndpointStillReturnsExpectedError is the end-to-end
// non-regression counterpart via the public door: the reordering must not
// change the observable error for a missing endpoint.
func TestAddByID_MissingEndpointStillReturnsExpectedError(t *testing.T) {
	g := newTestGraph(t)
	ctx := context.Background()

	start, err := g.Nodes.Add(ctx, []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("Add start: %v", err)
	}
	missingEnd := types.NodeID(999_999_997)

	if _, err := g.Rels.AddByID(ctx, "KNOWS", start.ID(), missingEnd, nil); !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("AddByID with missing end = %v, want ErrNodeNotFound", err)
	}
}
