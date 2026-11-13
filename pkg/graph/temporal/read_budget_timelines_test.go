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

// timelineCtxSpy is an Ops that also offers the timeline Ctx doors.
type timelineCtxSpy struct {
	temporalOpsSpy
	gotCtx context.Context
}

func (s *timelineCtxSpy) NodeEffectiveTimelineCtx(ctx context.Context, id types.NodeID, pin types.Instant) ([]NodeSegment, error) {
	s.record("NodeEffectiveTimelineCtx")
	s.gotCtx, s.lastNodeID, s.lastPin = ctx, id, pin
	return s.nodeSegs, s.err
}

func (s *timelineCtxSpy) NodeEffectiveTimelineFromCtx(ctx context.Context, id types.NodeID, validFrom, pin types.Instant) ([]NodeSegment, error) {
	s.record("NodeEffectiveTimelineFromCtx")
	s.gotCtx, s.lastNodeID, s.lastPin, s.lastValidFrom = ctx, id, pin, validFrom
	return s.nodeSegs, s.err
}

func (s *timelineCtxSpy) NodeEffectiveTimelinesCtx(ctx context.Context, id types.NodeID, pins []types.Instant) ([][]NodeSegment, error) {
	s.record("NodeEffectiveTimelinesCtx")
	s.gotCtx, s.lastNodeID, s.lastPins = ctx, id, pins
	return [][]NodeSegment{s.nodeSegs}, s.err
}

func (s *timelineCtxSpy) RelEffectiveTimelineCtx(ctx context.Context, id types.RelID, pin types.Instant) ([]RelSegment, error) {
	s.record("RelEffectiveTimelineCtx")
	s.gotCtx, s.lastRelID, s.lastPin = ctx, id, pin
	return s.relSegs, s.err
}

func (s *timelineCtxSpy) RelEffectiveTimelinesCtx(ctx context.Context, id types.RelID, pins []types.Instant) ([][]RelSegment, error) {
	s.record("RelEffectiveTimelinesCtx")
	s.gotCtx, s.lastRelID, s.lastPins = ctx, id, pins
	return [][]RelSegment{s.relSegs}, s.err
}

// timelineResult is one wrapper's error and whether it returned a result.
type timelineResult struct {
	err error
	got bool
}

// timelineCtxCalls runs every timeline Ctx wrapper of api under ctx.
func timelineCtxCalls(api *API, ctx context.Context) map[string]timelineResult {
	out := map[string]timelineResult{}
	pins := []types.Instant{5, 9}
	n1, e1 := api.NodeEffectiveTimelineCtx(ctx, 11, 7)
	out["NodeEffectiveTimeline"] = timelineResult{e1, n1 != nil}
	n2, e2 := api.NodeEffectiveTimelineFromCtx(ctx, 11, 3, 7)
	out["NodeEffectiveTimelineFrom"] = timelineResult{e2, n2 != nil}
	n3, e3 := api.NodeEffectiveTimelinesCtx(ctx, 11, pins)
	out["NodeEffectiveTimelines"] = timelineResult{e3, n3 != nil}
	r1, e4 := api.RelEffectiveTimelineCtx(ctx, 12, 7)
	out["RelEffectiveTimeline"] = timelineResult{e4, r1 != nil}
	r2, e5 := api.RelEffectiveTimelinesCtx(ctx, 12, pins)
	out["RelEffectiveTimelines"] = timelineResult{e5, r2 != nil}
	return out
}

// TestReadBudgetTimelineWrappers: the timeline Ctx wrappers forward the context
// and arguments to an Ops that has the Ctx doors and return its answer and
// error unchanged; for an Ops without them a context with no budget reads as
// the plain door and a context with a budget is refused (Limit "unsupported",
// the door's name) before any call; a cancelled context and a nil API are
// refused before anything is called. Not parallel: the spies record into
// package variables.
func TestReadBudgetTimelineWrappers(t *testing.T) {
	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, 1)
	bctx, _ := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{HistoryExamined: 5})
	cctx, cancel := context.WithCancel(ctx)
	cancel()

	for door, r := range timelineCtxCalls(nil, ctx) {
		if !errors.Is(r.err, grapherr.ErrNilGraph) || r.got {
			t.Fatalf("%s on a nil API = %v, result %v; want ErrNilGraph and none", door, r.err, r.got)
		}
	}

	segs := []NodeSegment{{ValidFrom: 1, ValidTo: 2}}
	spy := &timelineCtxSpy{temporalOpsSpy: temporalOpsSpy{nodeSegs: segs, relSegs: []RelSegment{{ValidFrom: 1, ValidTo: 2}}}}
	for door, r := range timelineCtxCalls(New(spy), bctx) {
		if r.err != nil || !r.got {
			t.Fatalf("%s = %v, result %v; want the Ops' answer", door, r.err, r.got)
		}
	}
	want := []string{"NodeEffectiveTimelineCtx", "NodeEffectiveTimelineFromCtx", "NodeEffectiveTimelinesCtx", "RelEffectiveTimelineCtx", "RelEffectiveTimelinesCtx"}
	got := slices.Clone(spy.calls)
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) || spy.gotCtx != bctx || !slices.Equal(spy.lastPins, []types.Instant{5, 9}) || spy.lastRelID != 12 || spy.lastNodeID != 11 || spy.lastValidFrom != 3 {
		t.Fatalf("forwarded %v (ctx kept %v, pins %v, node %d, rel %d, validFrom %d); want %v unchanged", got, spy.gotCtx == bctx, spy.lastPins, spy.lastNodeID, spy.lastRelID, spy.lastValidFrom, want)
	}
	boom := errors.New("boom")
	for door, r := range timelineCtxCalls(New(&timelineCtxSpy{temporalOpsSpy: temporalOpsSpy{err: boom}}), ctx) {
		if !errors.Is(r.err, boom) {
			t.Fatalf("%s = %v; want the Ops' error", door, r.err)
		}
	}

	plain := &temporalOpsSpy{}
	for door, r := range timelineCtxCalls(New(plain), ctx) {
		if r.err != nil {
			t.Fatalf("%s with no budget = %v; want the plain door", door, r.err)
		}
	}
	wantPlain := []string{"NodeEffectiveTimeline", "NodeEffectiveTimelineFrom", "NodeEffectiveTimelines", "RelEffectiveTimeline", "RelEffectiveTimelines"}
	gotPlain := slices.Clone(plain.calls)
	slices.Sort(gotPlain)
	slices.Sort(wantPlain)
	if !slices.Equal(gotPlain, wantPlain) {
		t.Fatalf("plain doors called %v; want %v", gotPlain, wantPlain)
	}

	plain = &temporalOpsSpy{}
	for door, r := range timelineCtxCalls(New(plain), bctx) {
		var rb *storepkg.ReadBudgetError
		if !errors.Is(r.err, storepkg.ErrReadBudget) || !errors.As(r.err, &rb) || rb.Limit != "unsupported" || rb.Door != door || r.got {
			t.Fatalf("%s under a budget = %v; want ErrReadBudget %q from that door and no result", door, r.err, "unsupported")
		}
	}
	for door, r := range timelineCtxCalls(New(plain), cctx) {
		if !errors.Is(r.err, context.Canceled) {
			t.Fatalf("%s on a cancelled ctx = %v; want context.Canceled", door, r.err)
		}
	}
	if len(plain.calls) != 0 {
		t.Fatalf("a refused budget or a cancelled ctx still called %v", plain.calls)
	}
}
