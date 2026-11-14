package temporal

import (
	"context"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The windowed effective scans under a read budget (store.WithReadBudget, RH
// list R2, R2-SCANS). Each streams the plain door's segments in its order and
// charges before each entity it reads, as the scans of read_budget_scans.go:
// it stops at the first refusal with *store.ReadBudgetError, the callbacks made
// before it stand and none follows. ctx is checked first; without a budget each
// is its plain door.

// windowScanCtxOps is the optional part of Ops the label and type Window Ctx
// doors reach (the graph's own ops implement it; Ops itself stays unchanged).
// An Ops without it answers only a context without a budget.
type windowScanCtxOps interface {
	ForEachNodeEffectiveByLabelsWindowCtx(ctx context.Context, labels []string, pin types.Instant, window Window, fn func(NodeSegment) bool) error
	ForEachRelEffectiveByTypeWindowCtx(ctx context.Context, typeName string, pin types.Instant, window Window, fn func(RelSegment) bool) error
}

// ForEachNodeEffectiveByLabelsWindowCtx is ForEachNodeEffectiveByLabelsWindow
// under ctx's read budget, as ForEachNodeEffectiveByLabelsFromCtx: the segments
// that meet window, in the plain door's order, the callbacks made before a
// refusal standing. The candidates are the store's valid-end index's from
// window[0].From where it keeps one (memory, badger, tiered), so the entities
// that ended before the window are not read; its first call for a label after an
// open reads every ever-member's temporal fields, and that read is counted as
// Candidates (one per entity), HistoryExamined and DecodedBytes, before it is
// done, and on tiered each closed shard it opens as ShardOpens. A store
// without the index (sharded, a wrapper) reads the candidates of
// ForEachNodeEffectiveByLabels, each counted. Without a budget it is
// ForEachNodeEffectiveByLabelsWindow.
func (a *API) ForEachNodeEffectiveByLabelsWindowCtx(ctx context.Context, labels []string, pin types.Instant, window Window, fn func(NodeSegment) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	if m, ok := ops.(windowScanCtxOps); ok {
		return m.ForEachNodeEffectiveByLabelsWindowCtx(ctx, labels, pin, window, fn)
	}
	if err := asOfWithoutBudget(ctx, "ForEachNodeEffectiveByLabelsWindow"); err != nil {
		return err
	}
	return ops.ForEachNodeEffectiveByLabelsWindow(labels, pin, window, fn)
}

// ForEachRelEffectiveByTypeWindowCtx is ForEachRelEffectiveByTypeWindow under
// ctx's read budget, as ForEachNodeEffectiveByLabelsWindowCtx.
func (a *API) ForEachRelEffectiveByTypeWindowCtx(ctx context.Context, typeName string, pin types.Instant, window Window, fn func(RelSegment) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	if m, ok := ops.(windowScanCtxOps); ok {
		return m.ForEachRelEffectiveByTypeWindowCtx(ctx, typeName, pin, window, fn)
	}
	if err := asOfWithoutBudget(ctx, "ForEachRelEffectiveByTypeWindow"); err != nil {
		return err
	}
	return ops.ForEachRelEffectiveByTypeWindow(typeName, pin, window, fn)
}
