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

// pointCtxSpy is an Ops that also offers Sigma's group C Ctx doors.
type pointCtxSpy struct {
	temporalOpsSpy
	gotCtx  context.Context
	gotAt   types.Instant
	nodeRow *types.Node
}

func (s *pointCtxSpy) NodeAtCtx(ctx context.Context, id types.NodeID, at types.Instant) (*types.Node, error) {
	s.record("NodeAtCtx")
	s.gotCtx, s.lastNodeID, s.gotAt = ctx, id, at
	return s.nodeRow, s.err
}

// pointCtxCalls runs every group C Ctx wrapper of api under ctx.
func pointCtxCalls(api *API, ctx context.Context) map[string]timelineResult {
	out := map[string]timelineResult{}
	n, err := api.NodeAtCtx(ctx, 3, 6)
	out["NodeAt"] = timelineResult{err, n != nil}
	return out
}

// TestReadBudgetPointWrappers: NodeAtCtx forwards the context and arguments to
// an Ops that has the Ctx door and returns its answer and error unchanged; for
// an Ops without it a context with no budget reads as the plain door and a
// context with a budget is refused (Limit "unsupported", the door's name)
// before any call; a cancelled context and a nil API are refused before
// anything is called. Not parallel: the spies record into package variables.
func TestReadBudgetPointWrappers(t *testing.T) {
	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, 1)
	bctx, _ := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 5})
	cctx, cancel := context.WithCancel(ctx)
	cancel()

	for door, r := range pointCtxCalls(nil, ctx) {
		if !errors.Is(r.err, grapherr.ErrNilGraph) || r.got {
			t.Fatalf("%s on a nil API = %v, result %v; want ErrNilGraph and none", door, r.err, r.got)
		}
	}

	spy := &pointCtxSpy{nodeRow: types.NewNode(3, 0, nil)}
	for door, r := range pointCtxCalls(New(spy), bctx) {
		if r.err != nil || !r.got {
			t.Fatalf("%s = %v, row %v; want the Ops' answer", door, r.err, r.got)
		}
	}
	if !slices.Equal(spy.calls, []string{"NodeAtCtx"}) || spy.gotCtx != bctx || spy.lastNodeID != 3 || spy.gotAt != 6 {
		t.Fatalf("forwarded %v (ctx kept %v, node %d, at %d); want NodeAtCtx unchanged", spy.calls, spy.gotCtx == bctx, spy.lastNodeID, spy.gotAt)
	}
	boom := errors.New("boom")
	for door, r := range pointCtxCalls(New(&pointCtxSpy{temporalOpsSpy: temporalOpsSpy{err: boom}}), ctx) {
		if !errors.Is(r.err, boom) {
			t.Fatalf("%s = %v; want the Ops' error", door, r.err)
		}
	}

	plain := &temporalOpsSpy{}
	for door, r := range pointCtxCalls(New(plain), ctx) {
		if r.err != nil {
			t.Fatalf("%s with no budget = %v; want the plain door", door, r.err)
		}
	}
	if !slices.Equal(plain.calls, []string{"NodeAt"}) {
		t.Fatalf("plain doors called %v; want NodeAt", plain.calls)
	}

	plain = &temporalOpsSpy{}
	for door, r := range pointCtxCalls(New(plain), bctx) {
		var rb *storepkg.ReadBudgetError
		if !errors.Is(r.err, storepkg.ErrReadBudget) || !errors.As(r.err, &rb) || rb.Limit != "unsupported" || rb.Door != door || r.got {
			t.Fatalf("%s under a budget = %v; want ErrReadBudget %q from that door and no result", door, r.err, "unsupported")
		}
	}
	for door, r := range pointCtxCalls(New(plain), cctx) {
		if !errors.Is(r.err, context.Canceled) {
			t.Fatalf("%s on a cancelled ctx = %v; want context.Canceled", door, r.err)
		}
	}
	if len(plain.calls) != 0 {
		t.Fatalf("a refused budget or a cancelled ctx still called %v", plain.calls)
	}
}
