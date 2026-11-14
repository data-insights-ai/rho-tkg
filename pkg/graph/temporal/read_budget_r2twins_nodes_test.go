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

// nodeTwinsCtxSpy is an Ops that also offers the last two node Ctx doors of
// R2-TWINS.
type nodeTwinsCtxSpy struct {
	temporalOpsSpy
	gotCtx   context.Context
	gotIDs   []types.NodeID
	gotOpts  storepkg.QueryOpts
	gotValid types.Instant
	gotTxAt  types.Instant
	nodeRow  *types.Node
}

func (s *nodeTwinsCtxSpy) NodesAtOptsByIDsCtx(ctx context.Context, ids []types.NodeID, opts storepkg.QueryOpts) ([]*types.Node, error) {
	s.record("NodesAtOptsByIDsCtx")
	s.gotCtx, s.gotIDs, s.gotOpts = ctx, ids, opts
	return []*types.Node{s.nodeRow}, s.err
}

func (s *nodeTwinsCtxSpy) NodesAtTxCtx(ctx context.Context, validAt, txAt types.Instant) ([]*types.Node, error) {
	s.record("NodesAtTxCtx")
	s.gotCtx, s.gotValid, s.gotTxAt = ctx, validAt, txAt
	return []*types.Node{s.nodeRow}, s.err
}

// nodeTwinsCtxCalls runs both node twin wrappers of api under ctx.
func nodeTwinsCtxCalls(api *API, ctx context.Context, opts storepkg.QueryOpts) map[string]timelineResult {
	out := map[string]timelineResult{}
	ns, err := api.NodesAtOptsByIDsCtx(ctx, []types.NodeID{3, 5}, opts)
	out["NodesAtOptsByIDs"] = timelineResult{err, ns != nil}
	ns, err = api.NodesAtTxCtx(ctx, 6, 7)
	out["NodesAtTx"] = timelineResult{err, ns != nil}
	return out
}

// TestReadBudgetNodeTwinsWrappers: NodesAtOptsByIDsCtx and NodesAtTxCtx forward
// the context and arguments to an Ops that has the Ctx doors and return its
// answer and error unchanged; for an Ops without them a context with no budget
// reads as the plain door and a context with a budget is refused (Limit
// "unsupported", the door's name) before any call; a cancelled context, a nil one
// and a nil API are refused before anything is called. Not parallel: the spies
// record into package variables.
func TestReadBudgetNodeTwinsWrappers(t *testing.T) {
	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, 1)
	bctx, _ := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 5})
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	var nilCtx context.Context
	opts := storepkg.QueryOpts{ValidAt: 9, ValidAtSet: true, TxAt: 8}

	for door, r := range nodeTwinsCtxCalls(nil, ctx, opts) {
		if !errors.Is(r.err, grapherr.ErrNilGraph) || r.got {
			t.Fatalf("%s on a nil API = %v, result %v; want ErrNilGraph and none", door, r.err, r.got)
		}
	}

	spy := &nodeTwinsCtxSpy{nodeRow: types.NewNode(3, 0, nil)}
	for door, r := range nodeTwinsCtxCalls(New(spy), bctx, opts) {
		if r.err != nil {
			t.Fatalf("%s = %v; want the Ops' answer", door, r.err)
		}
	}
	want := []string{"NodesAtOptsByIDsCtx", "NodesAtTxCtx"}
	got := slices.Clone(spy.calls)
	slices.Sort(got)
	if !slices.Equal(got, want) || spy.gotCtx != bctx || !slices.Equal(spy.gotIDs, []types.NodeID{3, 5}) || spy.gotOpts != opts || spy.gotValid != 6 || spy.gotTxAt != 7 {
		t.Fatalf("forwarded %v (ctx kept %v, ids %v, opts %+v, validAt %d, txAt %d); want %v unchanged", got, spy.gotCtx == bctx, spy.gotIDs, spy.gotOpts, spy.gotValid, spy.gotTxAt, want)
	}
	boom := errors.New("boom")
	for door, r := range nodeTwinsCtxCalls(New(&nodeTwinsCtxSpy{temporalOpsSpy: temporalOpsSpy{err: boom}}), ctx, opts) {
		if !errors.Is(r.err, boom) {
			t.Fatalf("%s = %v; want the Ops' error", door, r.err)
		}
	}

	plain := &temporalOpsSpy{}
	for door, r := range nodeTwinsCtxCalls(New(plain), ctx, opts) {
		if r.err != nil {
			t.Fatalf("%s with no budget = %v; want the plain door", door, r.err)
		}
	}
	wantPlain := []string{"NodesAtOptsByIDs", "NodesAtTx"}
	gotPlain := slices.Clone(plain.calls)
	slices.Sort(gotPlain)
	if !slices.Equal(gotPlain, wantPlain) {
		t.Fatalf("plain doors called %v; want %v", gotPlain, wantPlain)
	}

	plain = &temporalOpsSpy{}
	for door, r := range nodeTwinsCtxCalls(New(plain), bctx, opts) {
		var rb *storepkg.ReadBudgetError
		if !errors.Is(r.err, storepkg.ErrReadBudget) || !errors.As(r.err, &rb) || rb.Limit != "unsupported" || rb.Door != door || r.got {
			t.Fatalf("%s under a budget = %v; want ErrReadBudget %q from that door and no result", door, r.err, "unsupported")
		}
	}
	for door, r := range nodeTwinsCtxCalls(New(plain), cctx, opts) {
		if !errors.Is(r.err, context.Canceled) {
			t.Fatalf("%s on a cancelled ctx = %v; want context.Canceled", door, r.err)
		}
	}
	for door, r := range nodeTwinsCtxCalls(New(plain), nilCtx, opts) {
		if !errors.Is(r.err, grapherr.ErrNilContext) {
			t.Fatalf("%s on a nil ctx = %v; want ErrNilContext", door, r.err)
		}
	}
	if len(plain.calls) != 0 {
		t.Fatalf("a refused budget, a cancelled or a nil ctx still called %v", plain.calls)
	}
}
