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

// scanCtxSpy is an Ops that also offers the scan Ctx doors: it records the
// door and the context it was handed.
type scanCtxSpy struct {
	temporalOpsSpy
	gotCtx context.Context
}

func (s *scanCtxSpy) ForEachNodeEffectiveByLabelCtx(ctx context.Context, label string, pin types.Instant, fn func(NodeSegment) bool) error {
	s.record("ForEachNodeEffectiveByLabelCtx")
	s.gotCtx, s.lastLabel, s.lastPin = ctx, label, pin
	return s.err
}

func (s *scanCtxSpy) ForEachNodeEffectiveByLabelsCtx(ctx context.Context, labels []string, pin types.Instant, fn func(NodeSegment) bool) error {
	s.record("ForEachNodeEffectiveByLabelsCtx")
	s.gotCtx, s.lastLabels, s.lastPin = ctx, labels, pin
	return s.err
}

func (s *scanCtxSpy) ForEachNodeEffectiveByLabelsFromCtx(ctx context.Context, labels []string, pin, validFrom types.Instant, fn func(NodeSegment) bool) error {
	s.record("ForEachNodeEffectiveByLabelsFromCtx")
	s.gotCtx, s.lastLabels, s.lastPin, s.lastValidFrom = ctx, labels, pin, validFrom
	return s.err
}

func (s *scanCtxSpy) ForEachRelEffectiveByTypeCtx(ctx context.Context, typeName string, pin types.Instant, fn func(RelSegment) bool) error {
	s.record("ForEachRelEffectiveByTypeCtx")
	s.gotCtx, s.lastRelType, s.lastPin = ctx, typeName, pin
	return s.err
}

func (s *scanCtxSpy) ForEachRelEffectiveByTypeFromCtx(ctx context.Context, typeName string, pin, validFrom types.Instant, fn func(RelSegment) bool) error {
	s.record("ForEachRelEffectiveByTypeFromCtx")
	s.gotCtx, s.lastRelType, s.lastPin, s.lastValidFrom = ctx, typeName, pin, validFrom
	return s.err
}

// scanCtxCalls runs every scan Ctx wrapper of api under ctx.
func scanCtxCalls(api *API, ctx context.Context) map[string]error {
	nf := func(NodeSegment) bool { return true }
	rf := func(RelSegment) bool { return true }
	return map[string]error{
		"ForEachNodeEffectiveByLabel":      api.ForEachNodeEffectiveByLabelCtx(ctx, "D", 7, nf),
		"ForEachNodeEffectiveByLabels":     api.ForEachNodeEffectiveByLabelsCtx(ctx, []string{"D", "E"}, 7, nf),
		"ForEachNodeEffectiveByLabelsFrom": api.ForEachNodeEffectiveByLabelsFromCtx(ctx, []string{"D"}, 7, 3, nf),
		"ForEachRelEffectiveByType":        api.ForEachRelEffectiveByTypeCtx(ctx, "NET", 7, rf),
		"ForEachRelEffectiveByTypeFrom":    api.ForEachRelEffectiveByTypeFromCtx(ctx, "NET", 7, 3, rf),
	}
}

// TestReadBudgetScanWrappers: the scan Ctx wrappers forward the context and
// the arguments to an Ops that has the Ctx doors; for an Ops without them a
// context with no budget reads as the plain door and a context with a budget is
// refused (*store.ReadBudgetError, Limit "unsupported", the door's name)
// without any read, rather than read unbounded; a cancelled context and a nil
// API are refused before anything is called. Not parallel: the spies record
// into package variables.
func TestReadBudgetScanWrappers(t *testing.T) {
	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, 1)
	bctx, _ := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 5})
	cctx, cancel := context.WithCancel(ctx)
	cancel()

	t.Run("nil API", func(t *testing.T) {
		var api *API
		for door, err := range scanCtxCalls(api, ctx) {
			if !errors.Is(err, grapherr.ErrNilGraph) {
				t.Fatalf("%s on a nil API = %v; want ErrNilGraph", door, err)
			}
		}
	})

	t.Run("an Ops with the Ctx doors", func(t *testing.T) {
		spy := &scanCtxSpy{}
		scanCtxCalls(New(spy), bctx)
		want := []string{"ForEachNodeEffectiveByLabelCtx", "ForEachNodeEffectiveByLabelsCtx", "ForEachNodeEffectiveByLabelsFromCtx", "ForEachRelEffectiveByTypeCtx", "ForEachRelEffectiveByTypeFromCtx"}
		got := slices.Clone(spy.calls)
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) || spy.gotCtx != bctx || spy.lastPin != 7 || spy.lastValidFrom != 3 || spy.lastRelType != "NET" {
			t.Fatalf("forwarded %v (ctx kept %v, pin %d, validFrom %d, type %q); want %v with the context and arguments unchanged", got, spy.gotCtx == bctx, spy.lastPin, spy.lastValidFrom, spy.lastRelType, want)
		}
		boom := errors.New("boom")
		spy = &scanCtxSpy{temporalOpsSpy: temporalOpsSpy{err: boom}}
		for door, err := range scanCtxCalls(New(spy), ctx) {
			if !errors.Is(err, boom) {
				t.Fatalf("%s = %v; want the Ops' error", door, err)
			}
		}
	})

	t.Run("an Ops without them", func(t *testing.T) {
		spy := &temporalOpsSpy{}
		for door, err := range scanCtxCalls(New(spy), ctx) {
			if err != nil {
				t.Fatalf("%s with no budget = %v; want the plain door's nil", door, err)
			}
		}
		want := []string{"ForEachNodeEffectiveByLabel", "ForEachNodeEffectiveByLabels", "ForEachNodeEffectiveByLabelsFrom", "ForEachRelEffectiveByType", "ForEachRelEffectiveByTypeFrom"}
		got := slices.Clone(spy.calls)
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Fatalf("plain doors called %v; want %v", got, want)
		}

		spy = &temporalOpsSpy{}
		for door, err := range scanCtxCalls(New(spy), bctx) {
			var rb *storepkg.ReadBudgetError
			if !errors.Is(err, storepkg.ErrReadBudget) || !errors.As(err, &rb) || rb.Limit != "unsupported" || rb.Door != door {
				t.Fatalf("%s under a budget = %v; want ErrReadBudget %q from that door", door, err, "unsupported")
			}
		}
		if len(spy.calls) != 0 {
			t.Fatalf("a refused budget still called %v", spy.calls)
		}

		for door, err := range scanCtxCalls(New(spy), cctx) {
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("%s on a cancelled ctx = %v; want context.Canceled", door, err)
			}
		}
		if len(spy.calls) != 0 {
			t.Fatalf("a cancelled ctx still called %v", spy.calls)
		}
	})
}
