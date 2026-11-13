package temporal

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// byIDsCtxSpy is an Ops that also offers the by-ID Ctx doors.
type byIDsCtxSpy struct {
	temporalOpsSpy
	gotCtx    context.Context
	gotIDs    []types.NodeID
	gotValid  types.Instant
	gotTxAt   types.Instant
	nodeReply *types.Node
}

func (s *byIDsCtxSpy) NodesEffectiveByIDsCtx(ctx context.Context, ids []types.NodeID, pin types.Instant, fn func(NodeSegment) bool) error {
	s.record("NodesEffectiveByIDsCtx")
	s.gotCtx, s.gotIDs, s.lastPin = ctx, ids, pin
	return s.err
}

func (s *byIDsCtxSpy) NodeAtTxCtx(ctx context.Context, id types.NodeID, validAt, txAt types.Instant) (*types.Node, error) {
	s.record("NodeAtTxCtx")
	s.gotCtx, s.lastNodeID, s.gotValid, s.gotTxAt = ctx, id, validAt, txAt
	return s.nodeReply, s.err
}

func (s *byIDsCtxSpy) NodesAtTxByIDsCtx(ctx context.Context, ids []types.NodeID, validAt, txAt types.Instant) ([]*types.Node, error) {
	s.record("NodesAtTxByIDsCtx")
	s.gotCtx, s.gotIDs, s.gotValid, s.gotTxAt = ctx, ids, validAt, txAt
	return []*types.Node{s.nodeReply}, s.err
}

func (s *byIDsCtxSpy) NodesLowestAnsweredPinsCtx(ctx context.Context, ids []types.NodeID) ([]types.Instant, error) {
	s.record("NodesLowestAnsweredPinsCtx")
	s.gotCtx, s.gotIDs = ctx, ids
	return []types.Instant{4}, s.err
}

// byIDsCtxCalls runs every by-ID Ctx wrapper of api under ctx.
func byIDsCtxCalls(api *API, ctx context.Context) map[string]timelineResult {
	ids := []types.NodeID{3, 5}
	out := map[string]timelineResult{}
	out["NodesEffectiveByIDs"] = timelineResult{api.NodesEffectiveByIDsCtx(ctx, ids, 7, func(NodeSegment) bool { return true }), false}
	n, err := api.NodeAtTxCtx(ctx, 3, 6, 7)
	out["NodeAtTx"] = timelineResult{err, n != nil}
	ns, err := api.NodesAtTxByIDsCtx(ctx, ids, 6, 7)
	out["NodesAtTxByIDs"] = timelineResult{err, ns != nil}
	ps, err := api.NodesLowestAnsweredPinsCtx(ctx, ids)
	out["NodesLowestAnsweredPins"] = timelineResult{err, ps != nil}
	return out
}

// TestReadBudgetByIDsWrappers: the by-ID Ctx wrappers forward the context and
// arguments to an Ops that has the Ctx doors and return its answer and error
// unchanged; for an Ops without them a context with no budget reads as the
// plain door and a context with a budget is refused (Limit "unsupported", the
// door's name) before any call; a cancelled context and a nil API are refused
// before anything is called. Not parallel: the spies record into package
// variables.
func TestReadBudgetByIDsWrappers(t *testing.T) {
	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, 1)
	bctx, _ := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 5})
	cctx, cancel := context.WithCancel(ctx)
	cancel()

	for door, r := range byIDsCtxCalls(nil, ctx) {
		if !errors.Is(r.err, grapherr.ErrNilGraph) || r.got {
			t.Fatalf("%s on a nil API = %v, result %v; want ErrNilGraph and none", door, r.err, r.got)
		}
	}

	spy := &byIDsCtxSpy{nodeReply: types.NewNode(3, 0, nil)}
	for door, r := range byIDsCtxCalls(New(spy), bctx) {
		if r.err != nil {
			t.Fatalf("%s = %v; want the Ops' answer", door, r.err)
		}
	}
	want := []string{"NodeAtTxCtx", "NodesAtTxByIDsCtx", "NodesEffectiveByIDsCtx", "NodesLowestAnsweredPinsCtx"}
	got := slices.Clone(spy.calls)
	slices.Sort(got)
	if !slices.Equal(got, want) || spy.gotCtx != bctx || !slices.Equal(spy.gotIDs, []types.NodeID{3, 5}) || spy.gotValid != 6 || spy.gotTxAt != 7 || spy.lastPin != 7 || spy.lastNodeID != 3 {
		t.Fatalf("forwarded %v (ctx kept %v, ids %v, validAt %d, txAt %d, pin %d, node %d); want %v unchanged", got, spy.gotCtx == bctx, spy.gotIDs, spy.gotValid, spy.gotTxAt, spy.lastPin, spy.lastNodeID, want)
	}
	boom := errors.New("boom")
	for door, r := range byIDsCtxCalls(New(&byIDsCtxSpy{temporalOpsSpy: temporalOpsSpy{err: boom}}), ctx) {
		if !errors.Is(r.err, boom) {
			t.Fatalf("%s = %v; want the Ops' error", door, r.err)
		}
	}

	plain := &temporalOpsSpy{}
	for door, r := range byIDsCtxCalls(New(plain), ctx) {
		if r.err != nil {
			t.Fatalf("%s with no budget = %v; want the plain door", door, r.err)
		}
	}
	wantPlain := []string{"NodeAtTx", "NodesAtTxByIDs", "NodesEffectiveByIDs", "NodesLowestAnsweredPins"}
	gotPlain := slices.Clone(plain.calls)
	slices.Sort(gotPlain)
	if !slices.Equal(gotPlain, wantPlain) {
		t.Fatalf("plain doors called %v; want %v", gotPlain, wantPlain)
	}

	plain = &temporalOpsSpy{}
	for door, r := range byIDsCtxCalls(New(plain), bctx) {
		var rb *storepkg.ReadBudgetError
		if !errors.Is(r.err, storepkg.ErrReadBudget) || !errors.As(r.err, &rb) || rb.Limit != "unsupported" || rb.Door != door || r.got {
			t.Fatalf("%s under a budget = %v; want ErrReadBudget %q from that door and no result", door, r.err, "unsupported")
		}
	}
	for door, r := range byIDsCtxCalls(New(plain), cctx) {
		if !errors.Is(r.err, context.Canceled) {
			t.Fatalf("%s on a cancelled ctx = %v; want context.Canceled", door, r.err)
		}
	}
	if len(plain.calls) != 0 {
		t.Fatalf("a refused budget or a cancelled ctx still called %v", plain.calls)
	}
}
