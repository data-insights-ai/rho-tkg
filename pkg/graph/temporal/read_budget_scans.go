package temporal

import (
	"context"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The effective scans under a read budget (store.WithReadBudget, RH list R2,
// sigma's group B). A scan streams: it charges before each entity it reads and
// stops at the first refusal with *store.ReadBudgetError, and the callbacks it
// made before that refusal stand (a streaming scan has no partial answer to
// withhold). ctx is checked first; without a budget each is its plain door.

// scanCtxOps is the optional part of Ops the scan Ctx doors reach (the
// graph's own ops implement it; Ops itself stays unchanged). An Ops without
// it answers only a context without a budget.
type scanCtxOps interface {
	ForEachNodeEffectiveByLabelCtx(ctx context.Context, label string, pin types.Instant, fn func(NodeSegment) bool) error
	ForEachNodeEffectiveByLabelsCtx(ctx context.Context, labels []string, pin types.Instant, fn func(NodeSegment) bool) error
	ForEachNodeEffectiveByLabelsFromCtx(ctx context.Context, labels []string, pin, validFrom types.Instant, fn func(NodeSegment) bool) error
	ForEachRelEffectiveByTypeCtx(ctx context.Context, typeName string, pin types.Instant, fn func(RelSegment) bool) error
	ForEachRelEffectiveByTypeFromCtx(ctx context.Context, typeName string, pin, validFrom types.Instant, fn func(RelSegment) bool) error
}

// ForEachNodeEffectiveByLabelCtx is ForEachNodeEffectiveByLabel under ctx's
// read budget (store.WithReadBudget): the same segments in the same order, or
// *store.ReadBudgetError at the first limit the scan's work passes. Before each
// entity it reads it counts one candidate (Candidates), and what the entity's
// read decodes (DecodedBytes), looks at (HistoryExamined) and opens (ShardOpens,
// tiered: the label's member enumeration fans out over every shard too) counts
// before it is done; each segment counts (Segments) before it is handed to fn;
// the candidate list the scan holds counts as BufferBytes, 8 per ID, once the
// gather has built it. The scan is a stream: fn has been called for every
// entity read before the refusal and is not called after it, so a refused
// scan has handed out a prefix of the plain scan's segments (exact-or-refused
// holds for slice-returning doors, not for a scan; a caller that must not act
// on a prefix collects the segments and discards them on the error). Under a
// Segments or BufferBytes bound the scan reads one entity at a time, so a
// refusal comes before the next entity is read. The meter is cumulative over
// the context and spent after a refusal. Without a budget it is
// ForEachNodeEffectiveByLabel.
func (a *API) ForEachNodeEffectiveByLabelCtx(ctx context.Context, label string, pin types.Instant, fn func(NodeSegment) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	if m, ok := ops.(scanCtxOps); ok {
		return m.ForEachNodeEffectiveByLabelCtx(ctx, label, pin, fn)
	}
	if err := asOfWithoutBudget(ctx, "ForEachNodeEffectiveByLabel"); err != nil {
		return err
	}
	return ops.ForEachNodeEffectiveByLabel(label, pin, fn)
}

// ForEachNodeEffectiveByLabelsCtx is ForEachNodeEffectiveByLabels under ctx's
// read budget, as ForEachNodeEffectiveByLabelCtx.
func (a *API) ForEachNodeEffectiveByLabelsCtx(ctx context.Context, labels []string, pin types.Instant, fn func(NodeSegment) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	if m, ok := ops.(scanCtxOps); ok {
		return m.ForEachNodeEffectiveByLabelsCtx(ctx, labels, pin, fn)
	}
	if err := asOfWithoutBudget(ctx, "ForEachNodeEffectiveByLabels"); err != nil {
		return err
	}
	return ops.ForEachNodeEffectiveByLabels(labels, pin, fn)
}

// ForEachNodeEffectiveByLabelsFromCtx is ForEachNodeEffectiveByLabelsFrom under
// ctx's read budget, as ForEachNodeEffectiveByLabelCtx. Its candidates are the
// store's valid-end index's (memory, badger, tiered: only the entities that can
// reach validFrom), each counted as the scan reads it; the index's first call
// for a label after an open reads every ever-member's temporal fields, and that
// read is counted before it is done (one candidate per entity, its history
// versions and stored bytes, on tiered each closed shard it opens), so a budget
// can refuse inside it. A store without the index (sharded, a wrapper) reads
// the candidates of ForEachNodeEffectiveByLabels, each counted. The segments and
// their order are the plain From door's.
func (a *API) ForEachNodeEffectiveByLabelsFromCtx(ctx context.Context, labels []string, pin, validFrom types.Instant, fn func(NodeSegment) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	if m, ok := ops.(scanCtxOps); ok {
		return m.ForEachNodeEffectiveByLabelsFromCtx(ctx, labels, pin, validFrom, fn)
	}
	if err := asOfWithoutBudget(ctx, "ForEachNodeEffectiveByLabelsFrom"); err != nil {
		return err
	}
	return ops.ForEachNodeEffectiveByLabelsFrom(labels, pin, validFrom, fn)
}

// ForEachRelEffectiveByTypeCtx is ForEachRelEffectiveByType under ctx's read
// budget, as ForEachNodeEffectiveByLabelCtx (the type's member enumeration
// fans out over every shard on tiered).
func (a *API) ForEachRelEffectiveByTypeCtx(ctx context.Context, typeName string, pin types.Instant, fn func(RelSegment) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	if m, ok := ops.(scanCtxOps); ok {
		return m.ForEachRelEffectiveByTypeCtx(ctx, typeName, pin, fn)
	}
	if err := asOfWithoutBudget(ctx, "ForEachRelEffectiveByType"); err != nil {
		return err
	}
	return ops.ForEachRelEffectiveByType(typeName, pin, fn)
}

// ForEachRelEffectiveByTypeFromCtx is ForEachRelEffectiveByTypeFrom under
// ctx's read budget, as ForEachNodeEffectiveByLabelsFromCtx (the valid-end
// index's candidates, its first call counted; ForEachRelEffectiveByType's where
// the store has none).
func (a *API) ForEachRelEffectiveByTypeFromCtx(ctx context.Context, typeName string, pin, validFrom types.Instant, fn func(RelSegment) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	if m, ok := ops.(scanCtxOps); ok {
		return m.ForEachRelEffectiveByTypeFromCtx(ctx, typeName, pin, validFrom, fn)
	}
	if err := asOfWithoutBudget(ctx, "ForEachRelEffectiveByTypeFrom"); err != nil {
		return err
	}
	return ops.ForEachRelEffectiveByTypeFrom(typeName, pin, validFrom, fn)
}
