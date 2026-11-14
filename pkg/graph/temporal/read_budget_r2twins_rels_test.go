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

// relTwinsCtxSpy is an Ops that also offers the relationship Ctx doors of
// R2-TWINS.
type relTwinsCtxSpy struct {
	temporalOpsSpy
	gotCtx   context.Context
	gotRels  []types.RelID
	gotValid types.Instant
	gotTxAt  types.Instant
	relRow   *types.Relationship
}

func (s *relTwinsCtxSpy) RelAtTxCtx(ctx context.Context, id types.RelID, validAt, txAt types.Instant) (*types.Relationship, error) {
	s.record("RelAtTxCtx")
	s.gotCtx, s.lastRelID, s.gotValid, s.gotTxAt = ctx, id, validAt, txAt
	return s.relRow, s.err
}

func (s *relTwinsCtxSpy) RelAtCtx(ctx context.Context, id types.RelID, at types.Instant) (*types.Relationship, error) {
	s.record("RelAtCtx")
	s.gotCtx, s.lastRelID, s.gotValid = ctx, id, at
	return s.relRow, s.err
}

func (s *relTwinsCtxSpy) RelsAtTxByIDsCtx(ctx context.Context, ids []types.RelID, validAt, txAt types.Instant) ([]*types.Relationship, error) {
	s.record("RelsAtTxByIDsCtx")
	s.gotCtx, s.gotRels, s.gotValid, s.gotTxAt = ctx, ids, validAt, txAt
	return []*types.Relationship{s.relRow}, s.err
}

func (s *relTwinsCtxSpy) RelsEffectiveByIDsCtx(ctx context.Context, ids []types.RelID, pin types.Instant, fn func(RelSegment) bool) error {
	s.record("RelsEffectiveByIDsCtx")
	s.gotCtx, s.gotRels, s.lastPin = ctx, ids, pin
	return s.err
}

func (s *relTwinsCtxSpy) RelLowestAnsweredPinCtx(ctx context.Context, id types.RelID) (types.Instant, error) {
	s.record("RelLowestAnsweredPinCtx")
	s.gotCtx, s.lastRelID = ctx, id
	return 4, s.err
}

// relTwinsCtxCalls runs every relationship twin wrapper of api under ctx.
func relTwinsCtxCalls(api *API, ctx context.Context) map[string]timelineResult {
	ids := []types.RelID{3, 5}
	out := map[string]timelineResult{}
	r, err := api.RelAtTxCtx(ctx, 3, 6, 7)
	out["RelAtTx"] = timelineResult{err, r != nil}
	r, err = api.RelAtCtx(ctx, 3, 6)
	out["RelAt"] = timelineResult{err, r != nil}
	rs, err := api.RelsAtTxByIDsCtx(ctx, ids, 6, 7)
	out["RelsAtTxByIDs"] = timelineResult{err, rs != nil}
	out["RelsEffectiveByIDs"] = timelineResult{api.RelsEffectiveByIDsCtx(ctx, ids, 7, func(RelSegment) bool { return true }), false}
	p, err := api.RelLowestAnsweredPinCtx(ctx, 3)
	out["RelLowestAnsweredPin"] = timelineResult{err, p != 0}
	return out
}

// TestReadBudgetRelTwinsWrappers: the relationship twin wrappers forward the
// context and arguments to an Ops that has the Ctx doors and return its answer
// and error unchanged; for an Ops without them a context with no budget reads as
// the plain door and a context with a budget is refused (Limit "unsupported", the
// door's name) before any call; a cancelled context, a nil one and a nil API are
// refused before anything is called. Not parallel: the spies record into package
// variables.
func TestReadBudgetRelTwinsWrappers(t *testing.T) {
	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, 1)
	bctx, _ := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 5})
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	var nilCtx context.Context

	for door, r := range relTwinsCtxCalls(nil, ctx) {
		if !errors.Is(r.err, grapherr.ErrNilGraph) || r.got {
			t.Fatalf("%s on a nil API = %v, result %v; want ErrNilGraph and none", door, r.err, r.got)
		}
	}

	spy := &relTwinsCtxSpy{relRow: types.NewRelationship(3, 0, 1, 2)}
	for door, r := range relTwinsCtxCalls(New(spy), bctx) {
		if r.err != nil {
			t.Fatalf("%s = %v; want the Ops' answer", door, r.err)
		}
	}
	want := []string{"RelAtCtx", "RelAtTxCtx", "RelLowestAnsweredPinCtx", "RelsAtTxByIDsCtx", "RelsEffectiveByIDsCtx"}
	got := slices.Clone(spy.calls)
	slices.Sort(got)
	if !slices.Equal(got, want) || spy.gotCtx != bctx || !slices.Equal(spy.gotRels, []types.RelID{3, 5}) || spy.gotValid != 6 || spy.gotTxAt != 7 || spy.lastPin != 7 || spy.lastRelID != 3 {
		t.Fatalf("forwarded %v (ctx kept %v, ids %v, validAt %d, txAt %d, pin %d, rel %d); want %v unchanged", got, spy.gotCtx == bctx, spy.gotRels, spy.gotValid, spy.gotTxAt, spy.lastPin, spy.lastRelID, want)
	}
	boom := errors.New("boom")
	for door, r := range relTwinsCtxCalls(New(&relTwinsCtxSpy{temporalOpsSpy: temporalOpsSpy{err: boom}}), ctx) {
		if !errors.Is(r.err, boom) {
			t.Fatalf("%s = %v; want the Ops' error", door, r.err)
		}
	}

	plain := &temporalOpsSpy{}
	for door, r := range relTwinsCtxCalls(New(plain), ctx) {
		if r.err != nil {
			t.Fatalf("%s with no budget = %v; want the plain door", door, r.err)
		}
	}
	wantPlain := []string{"RelAt", "RelAtTx", "RelLowestAnsweredPin", "RelsAtTxByIDs", "RelsEffectiveByIDs"}
	gotPlain := slices.Clone(plain.calls)
	slices.Sort(gotPlain)
	if !slices.Equal(gotPlain, wantPlain) {
		t.Fatalf("plain doors called %v; want %v", gotPlain, wantPlain)
	}

	plain = &temporalOpsSpy{}
	for door, r := range relTwinsCtxCalls(New(plain), bctx) {
		var rb *storepkg.ReadBudgetError
		if !errors.Is(r.err, storepkg.ErrReadBudget) || !errors.As(r.err, &rb) || rb.Limit != "unsupported" || rb.Door != door || r.got {
			t.Fatalf("%s under a budget = %v; want ErrReadBudget %q from that door and no result", door, r.err, "unsupported")
		}
	}
	for door, r := range relTwinsCtxCalls(New(plain), cctx) {
		if !errors.Is(r.err, context.Canceled) {
			t.Fatalf("%s on a cancelled ctx = %v; want context.Canceled", door, r.err)
		}
	}
	for door, r := range relTwinsCtxCalls(New(plain), nilCtx) {
		if !errors.Is(r.err, grapherr.ErrNilContext) {
			t.Fatalf("%s on a nil ctx = %v; want ErrNilContext", door, r.err)
		}
	}
	if len(plain.calls) != 0 {
		t.Fatalf("a refused budget, a cancelled or a nil ctx still called %v", plain.calls)
	}
}
