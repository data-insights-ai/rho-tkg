package temporal

import (
	"context"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The per-entity effective timelines under a read budget (store.WithReadBudget,
// RH list R2, sigma's group B). Each is the plain door's rule and answer, or
// *store.ReadBudgetError and no segments (a slice-returning door is exact or
// refused). ctx is checked first; without a budget each is its plain door.

// timelineCtxOps is the optional part of Ops the timeline Ctx doors reach (the
// graph's own ops implement it; Ops itself stays unchanged). An Ops without it
// answers only a context without a budget.
type timelineCtxOps interface {
	NodeEffectiveTimelineCtx(ctx context.Context, id types.NodeID, pin types.Instant) ([]NodeSegment, error)
	NodeEffectiveTimelineFromCtx(ctx context.Context, id types.NodeID, validFrom, pin types.Instant) ([]NodeSegment, error)
	NodeEffectiveTimelinesCtx(ctx context.Context, id types.NodeID, pins []types.Instant) ([][]NodeSegment, error)
	RelEffectiveTimelineCtx(ctx context.Context, id types.RelID, pin types.Instant) ([]RelSegment, error)
	RelEffectiveTimelinesCtx(ctx context.Context, id types.RelID, pins []types.Instant) ([][]RelSegment, error)
}

// NodeEffectiveTimelineCtx is NodeEffectiveTimeline under ctx's read budget
// (store.WithReadBudget): the same segments at the same pin, or
// *store.ReadBudgetError and no segments. It counts one candidate (the ID),
// the history versions the read looks at (HistoryExamined), the rows and
// skeletons it decodes (DecodedBytes), on tiered each closed shard it is about
// to open (ShardOpens: a node live nowhere is searched on every shard), and
// each returned segment (Segments, and its size as BufferBytes), the segments
// after the read and before they are returned. The checks, the gates and the
// errors are NodeEffectiveTimeline's. Without a budget it is
// NodeEffectiveTimeline.
func (a *API) NodeEffectiveTimelineCtx(ctx context.Context, id types.NodeID, pin types.Instant) ([]NodeSegment, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	if m, ok := ops.(timelineCtxOps); ok {
		return m.NodeEffectiveTimelineCtx(ctx, id, pin)
	}
	if err := asOfWithoutBudget(ctx, "NodeEffectiveTimeline"); err != nil {
		return nil, err
	}
	return ops.NodeEffectiveTimeline(id, pin)
}

// NodeEffectiveTimelineFromCtx is NodeEffectiveTimelineFrom under ctx's read
// budget, as NodeEffectiveTimelineCtx: only the segments that reach validFrom
// count and are returned.
func (a *API) NodeEffectiveTimelineFromCtx(ctx context.Context, id types.NodeID, validFrom, pin types.Instant) ([]NodeSegment, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	if m, ok := ops.(timelineCtxOps); ok {
		return m.NodeEffectiveTimelineFromCtx(ctx, id, validFrom, pin)
	}
	if err := asOfWithoutBudget(ctx, "NodeEffectiveTimelineFrom"); err != nil {
		return nil, err
	}
	return ops.NodeEffectiveTimelineFrom(id, validFrom, pin)
}

// NodeEffectiveTimelinesCtx is NodeEffectiveTimelines under ctx's read budget,
// as NodeEffectiveTimelineCtx: one candidate for the ID, one walk of its chain,
// and every segment of every pin's answer counted before the result is
// returned.
func (a *API) NodeEffectiveTimelinesCtx(ctx context.Context, id types.NodeID, pins []types.Instant) ([][]NodeSegment, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	if m, ok := ops.(timelineCtxOps); ok {
		return m.NodeEffectiveTimelinesCtx(ctx, id, pins)
	}
	if err := asOfWithoutBudget(ctx, "NodeEffectiveTimelines"); err != nil {
		return nil, err
	}
	return ops.NodeEffectiveTimelines(id, pins)
}

// RelEffectiveTimelineCtx is RelEffectiveTimeline under ctx's read budget, as
// NodeEffectiveTimelineCtx.
func (a *API) RelEffectiveTimelineCtx(ctx context.Context, id types.RelID, pin types.Instant) ([]RelSegment, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	if m, ok := ops.(timelineCtxOps); ok {
		return m.RelEffectiveTimelineCtx(ctx, id, pin)
	}
	if err := asOfWithoutBudget(ctx, "RelEffectiveTimeline"); err != nil {
		return nil, err
	}
	return ops.RelEffectiveTimeline(id, pin)
}

// RelEffectiveTimelinesCtx is RelEffectiveTimelines under ctx's read budget, as
// NodeEffectiveTimelinesCtx.
func (a *API) RelEffectiveTimelinesCtx(ctx context.Context, id types.RelID, pins []types.Instant) ([][]RelSegment, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	if m, ok := ops.(timelineCtxOps); ok {
		return m.RelEffectiveTimelinesCtx(ctx, id, pins)
	}
	if err := asOfWithoutBudget(ctx, "RelEffectiveTimelines"); err != nil {
		return nil, err
	}
	return ops.RelEffectiveTimelines(id, pins)
}
