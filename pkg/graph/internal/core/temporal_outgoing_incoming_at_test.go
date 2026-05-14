package core

import (
	"context"
	"errors"
	"testing"
	"time"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// TestOutgoingRelsAt_HappyPath asserts the basic case: at time t the function
// returns the rels whose start endpoint is the queried node and were valid at t.
func TestOutgoingRelsAt_HappyPath(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	useTestClock(t, g)
	ctx := context.Background()

	a, _ := g.Nodes.Add(ctx, []string{"Person"}, nil)
	b, _ := g.Nodes.Add(ctx, []string{"Person"}, nil)
	c, _ := g.Nodes.Add(ctx, []string{"Person"}, nil)
	r1, _ := g.Rels.Add(ctx, "KNOWS", a, b, nil)
	r2, _ := g.Rels.Add(ctx, "FOLLOWS", a, c, nil)
	rIn, _ := g.Rels.Add(ctx, "LIKES", b, a, nil) // incoming, not outgoing

	at := g.relValidFrom(r2)
	got, err := g.Temporal.OutgoingRelsAt(a.ID(), at)
	if err != nil {
		t.Fatalf("OutgoingRelsAt: %v", err)
	}

	wantIDs := map[types.RelID]bool{r1.ID(): true, r2.ID(): true}
	if len(got) != len(wantIDs) {
		t.Fatalf("len = %d, want %d (rels=%v)", len(got), len(wantIDs), relIDsOf(got))
	}
	for _, r := range got {
		if !wantIDs[r.ID()] {
			t.Fatalf("unexpected rel %d in outgoing set", r.ID())
		}
		if r.StartNodeID() != a.ID() {
			t.Fatalf("rel %d Start=%d, want %d", r.ID(), r.StartNodeID(), a.ID())
		}
	}
	// Sort invariant.
	for i := 1; i < len(got); i++ {
		if got[i].ID() <= got[i-1].ID() {
			t.Fatalf("rels not sorted ascending by ID: %v", relIDsOf(got))
		}
	}
	_ = rIn
}

// TestOutgoingRelsAt_HistoryAfterDelete (two-phase) — rel r is valid at t0
// pointing a→b; we delete it after t0; querying at t0 must still return r.
// This proves the function consults history, not just current adjacency.
func TestOutgoingRelsAt_HistoryAfterDelete(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	useTestClock(t, g)
	ctx := context.Background()

	a, _ := g.Nodes.Add(ctx, []string{"Person"}, nil)
	b, _ := g.Nodes.Add(ctx, []string{"Person"}, nil)
	r, _ := g.Rels.Add(ctx, "KNOWS", a, b, nil)
	t0 := g.relValidFrom(r)

	if err := g.Rels.Delete(ctx, r.ID()); err != nil {
		t.Fatalf("DeleteRel: %v", err)
	}

	got, err := g.Temporal.OutgoingRelsAt(a.ID(), t0)
	if err != nil {
		t.Fatalf("OutgoingRelsAt at t0 after delete: %v", err)
	}
	if len(got) != 1 || got[0].ID() != r.ID() {
		t.Fatalf("history-only outgoing rels at t0 = %v, want [%d]", relIDsOf(got), r.ID())
	}
}

// TestOutgoingRelsAt_FuturePoint asserts a query in the future (after delete)
// returns the empty set — the deleted rel must NOT leak through as current.
func TestOutgoingRelsAt_FuturePoint(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	clk := useTestClock(t, g)
	ctx := context.Background()

	a, _ := g.Nodes.Add(ctx, []string{"Person"}, nil)
	b, _ := g.Nodes.Add(ctx, []string{"Person"}, nil)
	r, _ := g.Rels.Add(ctx, "KNOWS", a, b, nil)

	clk.Advance(2 * time.Second)
	if err := g.Rels.Delete(ctx, r.ID()); err != nil {
		t.Fatalf("DeleteRel: %v", err)
	}
	tFuture := clk.PeekInstant() + 1

	got, err := g.Temporal.OutgoingRelsAt(a.ID(), tFuture)
	if err != nil {
		t.Fatalf("OutgoingRelsAt at future: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("future-point outgoing rels = %v, want empty", relIDsOf(got))
	}
}

// TestOutgoingRelsAt_PhantomNode asserts a zero/invalid node returns an error.
func TestOutgoingRelsAt_PhantomNode(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	useTestClock(t, g)
	at := types.Instant(1)
	if got, err := g.Temporal.OutgoingRelsAt(types.NodeID(0), at); err == nil || got != nil {
		t.Fatalf("phantom zero NodeID = (%v, %v), want (nil, error)", got, err)
	}
	if got, err := g.Temporal.OutgoingRelsAt(types.NodeID(99999), at); !errors.Is(err, storepkg.ErrNodeNotFound) || got != nil {
		t.Fatalf("nonexistent node = (%v, %v), want (nil, ErrNodeNotFound)", got, err)
	}
}

// TestIncomingRelsAt_HappyPath mirrors OutgoingRelsAt_HappyPath. Rels with
// end-endpoint matching the queried node must be returned; outgoing rels and
// rels touching the node not as End must not.
func TestIncomingRelsAt_HappyPath(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	useTestClock(t, g)
	ctx := context.Background()

	a, _ := g.Nodes.Add(ctx, []string{"Person"}, nil)
	b, _ := g.Nodes.Add(ctx, []string{"Person"}, nil)
	c, _ := g.Nodes.Add(ctx, []string{"Person"}, nil)
	rIn1, _ := g.Rels.Add(ctx, "KNOWS", b, a, nil)
	rIn2, _ := g.Rels.Add(ctx, "FOLLOWS", c, a, nil)
	rOut, _ := g.Rels.Add(ctx, "LIKES", a, b, nil) // outgoing, not incoming

	at := g.relValidFrom(rIn2)
	got, err := g.Temporal.IncomingRelsAt(a.ID(), at)
	if err != nil {
		t.Fatalf("IncomingRelsAt: %v", err)
	}
	wantIDs := map[types.RelID]bool{rIn1.ID(): true, rIn2.ID(): true}
	if len(got) != len(wantIDs) {
		t.Fatalf("len = %d, want %d (rels=%v)", len(got), len(wantIDs), relIDsOf(got))
	}
	for _, r := range got {
		if !wantIDs[r.ID()] {
			t.Fatalf("unexpected rel %d in incoming set", r.ID())
		}
		if r.EndNodeID() != a.ID() {
			t.Fatalf("rel %d End=%d, want %d", r.ID(), r.EndNodeID(), a.ID())
		}
	}
	_ = rOut
}

// TestIncomingRelsAt_HistoryAfterDelete (two-phase) — incoming rel deleted
// after t0, query at t0 must still see it.
func TestIncomingRelsAt_HistoryAfterDelete(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	useTestClock(t, g)
	ctx := context.Background()

	a, _ := g.Nodes.Add(ctx, []string{"Person"}, nil)
	b, _ := g.Nodes.Add(ctx, []string{"Person"}, nil)
	r, _ := g.Rels.Add(ctx, "KNOWS", b, a, nil)
	t0 := g.relValidFrom(r)

	if err := g.Rels.Delete(ctx, r.ID()); err != nil {
		t.Fatalf("DeleteRel: %v", err)
	}

	got, err := g.Temporal.IncomingRelsAt(a.ID(), t0)
	if err != nil {
		t.Fatalf("IncomingRelsAt at t0: %v", err)
	}
	if len(got) != 1 || got[0].ID() != r.ID() {
		t.Fatalf("history-only incoming rels = %v, want [%d]", relIDsOf(got), r.ID())
	}
}

// TestOutgoingIncomingRelsAt_ClosedGraph asserts the lifecycle gate.
func TestOutgoingIncomingRelsAt_ClosedGraph(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	ctx := context.Background()
	a, _ := g.Nodes.Add(ctx, []string{"Person"}, nil)
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := g.Temporal.OutgoingRelsAt(a.ID(), 1); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("OutgoingRelsAt after Close = %v, want ErrGraphClosed", err)
	}
	if _, err := g.Temporal.IncomingRelsAt(a.ID(), 1); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("IncomingRelsAt after Close = %v, want ErrGraphClosed", err)
	}
}

func relIDsOf(rels []*types.Relationship) []types.RelID {
	out := make([]types.RelID, len(rels))
	for i, r := range rels {
		out[i] = r.ID()
	}
	return out
}

