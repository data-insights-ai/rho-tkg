package core

import (
	"context"
	"errors"
	"testing"
	"time"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"
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

// TestOutgoingRelsAt_HistoryReturnsT0PropertyValues (S7) is the stronger
// two-phase test: an Update after t0 changes the rel's properties; querying
// at t0 must return the pre-update property value, not the current value.
// This proves the function returns the version-at-t, not the most-recent
// version that happens to have an endpoint at this node.
func TestOutgoingRelsAt_HistoryReturnsT0PropertyValues(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	useTestClock(t, g)
	ctx := context.Background()

	a, _ := g.Nodes.Add(ctx, []string{"Person"}, nil)
	b, _ := g.Nodes.Add(ctx, []string{"Person"}, nil)
	r, err := g.Rels.Add(ctx, "KNOWS", a, b, map[string]any{"strength": "weak"})
	if err != nil {
		t.Fatalf("Add rel: %v", err)
	}
	t0 := g.relValidFrom(r)

	// Phase 2: mutate the property after t0.
	if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"strength": "strong"}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	got, err := g.Temporal.OutgoingRelsAt(a.ID(), t0)
	if err != nil {
		t.Fatalf("OutgoingRelsAt: %v", err)
	}
	if len(got) != 1 || got[0].ID() != r.ID() {
		t.Fatalf("got = %v, want [%d]", relIDsOf(got), r.ID())
	}
	if v, ok := got[0].GetProperty("strength"); !ok || v != "weak" {
		t.Fatalf("rel property at t0 = (%v, %v), want (\"weak\", true) — function returned post-mutation state instead of t0 state", v, ok)
	}
}

// TestIncomingRelsAt_FuturePoint mirrors TestOutgoingRelsAt_FuturePoint —
// pin Node/Rel parity (rule 2) and force the symmetric audit path
// through directionalRelsAt(outgoing=false) (rule 17).
func TestIncomingRelsAt_FuturePoint(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	clk := useTestClock(t, g)
	ctx := context.Background()

	a, _ := g.Nodes.Add(ctx, []string{"Person"}, nil)
	b, _ := g.Nodes.Add(ctx, []string{"Person"}, nil)
	r, _ := g.Rels.Add(ctx, "KNOWS", b, a, nil) // incoming on a

	clk.Advance(2 * time.Second)
	if err := g.Rels.Delete(ctx, r.ID()); err != nil {
		t.Fatalf("DeleteRel: %v", err)
	}
	tFuture := clk.PeekInstant() + 1

	got, err := g.Temporal.IncomingRelsAt(a.ID(), tFuture)
	if err != nil {
		t.Fatalf("IncomingRelsAt at future: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("future-point incoming rels = %v, want empty", relIDsOf(got))
	}
}

// TestIncomingRelsAt_HistoryReturnsT0PropertyValues mirrors the property
// preservation guarantee for the incoming direction (S7).
func TestIncomingRelsAt_HistoryReturnsT0PropertyValues(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	useTestClock(t, g)
	ctx := context.Background()

	a, _ := g.Nodes.Add(ctx, []string{"Person"}, nil)
	b, _ := g.Nodes.Add(ctx, []string{"Person"}, nil)
	r, _ := g.Rels.Add(ctx, "KNOWS", b, a, map[string]any{"trust": "low"})
	t0 := g.relValidFrom(r)
	if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"trust": "high"}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got, err := g.Temporal.IncomingRelsAt(a.ID(), t0)
	if err != nil {
		t.Fatalf("IncomingRelsAt: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got = %v, want exactly r", relIDsOf(got))
	}
	if v, _ := got[0].GetProperty("trust"); v != "low" {
		t.Fatalf("rel property at t0 = %v, want \"low\"", v)
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
// relCandidateSpyStore wraps a memory.Store and counts which iteration path
// the graph layer dispatches to for the deleted-rel candidate fold. All four
// methods (ForEachDeletedNodeID, ForEachDeletedRelID, ForEachNodeHistoryID,
// ForEachRelHistoryID) are source-declared on the wrapper so the wrapper
// detection in deletedIterationCapability + depthHistoryIterationCapability
// recognises the wrapper as a real capability provider (the check requires
// every named method to be source-backed, not autogenerated from embedding).
type relCandidateSpyStore struct {
	*memory.Store
	deletedNodeCalls atomicCount
	deletedRelCalls  atomicCount
	nodeHistCalls    atomicCount
	relHistCalls     atomicCount
}

type atomicCount struct{ n int64 }

func (a *atomicCount) Inc()        { a.n++ }
func (a *atomicCount) Get() int64  { return a.n }

func (s *relCandidateSpyStore) ForEachDeletedNodeID(fn func(types.NodeID) bool) error {
	s.deletedNodeCalls.Inc()
	return s.Store.ForEachDeletedNodeID(fn)
}

func (s *relCandidateSpyStore) ForEachDeletedRelID(fn func(types.RelID) bool) error {
	s.deletedRelCalls.Inc()
	return s.Store.ForEachDeletedRelID(fn)
}

func (s *relCandidateSpyStore) ForEachNodeHistoryID(fn func(types.NodeID) bool) error {
	s.nodeHistCalls.Inc()
	return s.Store.ForEachNodeHistoryID(fn)
}

func (s *relCandidateSpyStore) ForEachRelHistoryID(fn func(types.RelID) bool) error {
	s.relHistCalls.Inc()
	return s.Store.ForEachRelHistoryID(fn)
}

// TestOutgoingRelsAt_DispatchesToDeletedIteration pins the B2 wiring: when
// the store implements DeletedIterationCapability, the adjacency-at-t fold
// uses ForEachDeletedRelID (cost O(deleted_count)), NOT the wider
// ForEachRelHistoryID. Without this test a future wiring regression that
// silently falls back to the all-history path would be invisible — the
// query results would still be correct.
func TestOutgoingRelsAt_DispatchesToDeletedIteration(t *testing.T) {
	t.Parallel()
	spy := &relCandidateSpyStore{Store: memory.New()}
	g, err := New(Config{Store: spy})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { g.Close() })
	useTestClock(t, g)
	ctx := context.Background()

	a, _ := g.Nodes.Add(ctx, []string{"Person"}, nil)
	b, _ := g.Nodes.Add(ctx, []string{"Person"}, nil)
	r, _ := g.Rels.Add(ctx, "KNOWS", a, b, nil)
	t0 := g.relValidFrom(r)
	if err := g.Rels.Delete(ctx, r.ID()); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Reset counters AFTER setup mutations (which may also touch iteration paths).
	spy.deletedRelCalls = atomicCount{}
	spy.relHistCalls = atomicCount{}

	if _, err := g.Temporal.OutgoingRelsAt(a.ID(), t0); err != nil {
		t.Fatalf("OutgoingRelsAt: %v", err)
	}

	if spy.deletedRelCalls.Get() == 0 {
		t.Errorf("ForEachDeletedRelID was not called — adjacency fold fell back to history scan")
	}
	if spy.relHistCalls.Get() != 0 {
		t.Errorf("ForEachRelHistoryID was called %d times — adjacency fold should use deleted-only path", spy.relHistCalls.Get())
	}
}

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

