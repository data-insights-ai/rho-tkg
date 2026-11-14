package temporal

import (
	"context"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
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

// atNodesCtxOps is the optional part of Ops the by-endpoint Ctx doors reach
// (the graph's own ops implement it). An Ops without it answers only a context
// without a budget.
type atNodesCtxOps interface {
	ForEachRelEffectiveAtNodesCtx(ctx context.Context, nodeIDs []types.NodeID, dir Direction, relTypes []string, pin types.Instant, fn func(RelSegment) bool) error
	ForEachRelEffectiveAtNodesWindowCtx(ctx context.Context, nodeIDs []types.NodeID, dir Direction, relTypes []string, pin types.Instant, window Window, fn func(RelSegment) bool) error
	ScanRelEffectiveColumnsAtNodesCtx(ctx context.Context, nodeIDs []types.NodeID, dir Direction, relTypes []string, pin types.Instant, window Window, props []string, fn func(*storepkg.RelColumnBatch) bool) error
}

// ForEachRelEffectiveAtNodesCtx is ForEachRelEffectiveAtNodes under ctx's read
// budget (store.WithReadBudget): the same segments in the same order, or
// *store.ReadBudgetError at the first limit the work passes, the callbacks made
// before it standing and none after. The candidates are gathered under the
// budget, each step counted before it is done: one candidate per seed probed
// (its current row, decoded bytes counted), each entry of the seeds' current
// adjacency before it is collected (a RAM index counts a node's whole set before
// the list is built; the on-disk adjacency index, AdjacencyIndexOnDisk, each
// entry as its walk produces it; store.AdjacencyMeteredCapability), and each
// entry of their ex-adjacency as it is produced; on tiered each closed shard
// these reads open counts as ShardOpens before it opens, and a tiered store
// without an ex-adjacency covering every relationship refuses a ShardOpens
// budget before the fold that would open every shard (Limit "unsupported"). Then,
// as the scans of ForEachNodeEffectiveByLabelCtx, one candidate per relationship
// before its timeline is read (so an entity the gather listed counts twice:
// a budget reads high, never low), its history versions, decoded bytes and
// shard opens, and one segment per segment before fn gets it. BufferBytes counts
// the seed list and the candidate list, 8 per ID, once each is built. Without a
// budget it is ForEachRelEffectiveAtNodes.
func (a *API) ForEachRelEffectiveAtNodesCtx(ctx context.Context, nodeIDs []types.NodeID, dir Direction, relTypes []string, pin types.Instant, fn func(RelSegment) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	if m, ok := ops.(atNodesCtxOps); ok {
		return m.ForEachRelEffectiveAtNodesCtx(ctx, nodeIDs, dir, relTypes, pin, fn)
	}
	if err := asOfWithoutBudget(ctx, "ForEachRelEffectiveAtNodes"); err != nil {
		return err
	}
	return ops.ForEachRelEffectiveAtNodes(nodeIDs, dir, relTypes, pin, fn)
}

// ForEachRelEffectiveAtNodesWindowCtx is ForEachRelEffectiveAtNodesWindow under
// ctx's read budget, as ForEachRelEffectiveAtNodesCtx: the segments that meet
// window (Segments counts those handed to fn, not those the window drops).
func (a *API) ForEachRelEffectiveAtNodesWindowCtx(ctx context.Context, nodeIDs []types.NodeID, dir Direction, relTypes []string, pin types.Instant, window Window, fn func(RelSegment) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	if m, ok := ops.(atNodesCtxOps); ok {
		return m.ForEachRelEffectiveAtNodesWindowCtx(ctx, nodeIDs, dir, relTypes, pin, window, fn)
	}
	if err := asOfWithoutBudget(ctx, "ForEachRelEffectiveAtNodesWindow"); err != nil {
		return err
	}
	return ops.ForEachRelEffectiveAtNodesWindow(nodeIDs, dir, relTypes, pin, window, fn)
}

// ScanRelEffectiveColumnsAtNodesCtx is ScanRelEffectiveColumnsAtNodes under
// ctx's read budget: ForEachRelEffectiveAtNodesWindowCtx's gather and counts,
// and the call holds the segments it keeps before its first batch, so each kept
// segment also counts 24 bytes of BufferBytes (its relationship pointer and
// valid span) before it is held. The call is exact or refused: a refusal comes
// before fn has seen a batch.
func (a *API) ScanRelEffectiveColumnsAtNodesCtx(ctx context.Context, nodeIDs []types.NodeID, dir Direction, relTypes []string, pin types.Instant, window Window, props []string, fn func(*storepkg.RelColumnBatch) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	if m, ok := ops.(atNodesCtxOps); ok {
		return m.ScanRelEffectiveColumnsAtNodesCtx(ctx, nodeIDs, dir, relTypes, pin, window, props, fn)
	}
	if err := asOfWithoutBudget(ctx, "ScanRelEffectiveColumnsAtNodes"); err != nil {
		return err
	}
	return ops.ScanRelEffectiveColumnsAtNodes(nodeIDs, dir, relTypes, pin, window, props, fn)
}
