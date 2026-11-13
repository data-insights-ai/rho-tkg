package temporal

import (
	"context"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The by-ID and bitemporal point doors under a read budget (store.WithReadBudget,
// RH list R2, sigma's group B). The slice- and row-returning doors are exact or
// refused: the plain door's answer, or *store.ReadBudgetError and nothing. The
// streaming NodesEffectiveByIDsCtx is a scan: the callbacks made before a
// refusal stand. ctx is checked first; without a budget each is its plain door.

// byIDsCtxOps is the optional part of Ops the by-ID Ctx doors reach (the
// graph's own ops implement it; Ops itself stays unchanged). An Ops without it
// answers only a context without a budget.
type byIDsCtxOps interface {
	NodesEffectiveByIDsCtx(ctx context.Context, ids []types.NodeID, pin types.Instant, fn func(NodeSegment) bool) error
	NodeAtTxCtx(ctx context.Context, id types.NodeID, validAt, txAt types.Instant) (*types.Node, error)
	NodesAtTxByIDsCtx(ctx context.Context, ids []types.NodeID, validAt, txAt types.Instant) ([]*types.Node, error)
	NodesLowestAnsweredPinsCtx(ctx context.Context, ids []types.NodeID) ([]types.Instant, error)
}

// NodesEffectiveByIDsCtx is NodesEffectiveByIDs under ctx's read budget
// (store.WithReadBudget): the same segments in the order of ids, or
// *store.ReadBudgetError at the first limit the stream's work passes. It is a
// stream, as the label and type scans are (ForEachNodeEffectiveByLabelCtx): it
// counts the ID list it holds (BufferBytes, 8 per ID, before it copies it), one
// candidate before each distinct ID it reads (Candidates), and what each read
// looks at, decodes and opens (HistoryExamined, DecodedBytes, ShardOpens on
// tiered) and each segment before fn gets it (Segments); the callbacks made
// before a refusal stand and none follows it. Under a Segments or BufferBytes
// bound it reads one entity at a time.
func (a *API) NodesEffectiveByIDsCtx(ctx context.Context, ids []types.NodeID, pin types.Instant, fn func(NodeSegment) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	if m, ok := ops.(byIDsCtxOps); ok {
		return m.NodesEffectiveByIDsCtx(ctx, ids, pin, fn)
	}
	if err := asOfWithoutBudget(ctx, "NodesEffectiveByIDs"); err != nil {
		return err
	}
	return ops.NodesEffectiveByIDs(ids, pin, fn)
}

// NodeAtTxCtx is NodeAtTx under ctx's read budget: the same row at the same
// pins, or *store.ReadBudgetError and no row. It counts one candidate (the ID),
// and the rows decoded (DecodedBytes), the history versions looked at
// (HistoryExamined) and, on tiered, the closed shards opened (ShardOpens: a node
// live nowhere is searched on every shard), each before it is done. The checks,
// gates and errors are NodeAtTx's. Without a budget it is NodeAtTx.
func (a *API) NodeAtTxCtx(ctx context.Context, id types.NodeID, validAt, txAt types.Instant) (*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	if m, ok := ops.(byIDsCtxOps); ok {
		return m.NodeAtTxCtx(ctx, id, validAt, txAt)
	}
	if err := asOfWithoutBudget(ctx, "NodeAtTx"); err != nil {
		return nil, err
	}
	return ops.NodeAtTx(id, validAt, txAt)
}

// NodesAtTxByIDsCtx is NodesAtTxByIDs under ctx's read budget: the same
// aligned answer, or *store.ReadBudgetError and no rows. It validates as
// NodesAtTxByIDs does, then counts one candidate per ID and 8 buffer bytes per
// ID for the result slice before any ID is read (a repeated ID counts at each
// position: a budget reads high, never low), and the rows decoded, history
// looked at and shards opened by each distinct ID's read before they are done.
// Without a budget it is NodesAtTxByIDs.
func (a *API) NodesAtTxByIDsCtx(ctx context.Context, ids []types.NodeID, validAt, txAt types.Instant) ([]*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	if m, ok := ops.(byIDsCtxOps); ok {
		return m.NodesAtTxByIDsCtx(ctx, ids, validAt, txAt)
	}
	if err := asOfWithoutBudget(ctx, "NodesAtTxByIDs"); err != nil {
		return nil, err
	}
	return ops.NodesAtTxByIDs(ids, validAt, txAt)
}

// NodesLowestAnsweredPinsCtx is NodesLowestAnsweredPins under ctx's read
// budget: the same aligned answer, or *store.ReadBudgetError and no result. The
// door reads no row (it consults the compaction boundaries and retention
// watermarks held in memory), so it counts one candidate per ID and 8 buffer
// bytes per ID for the result before it computes any, and nothing else.
func (a *API) NodesLowestAnsweredPinsCtx(ctx context.Context, ids []types.NodeID) ([]types.Instant, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	if m, ok := ops.(byIDsCtxOps); ok {
		return m.NodesLowestAnsweredPinsCtx(ctx, ids)
	}
	if err := asOfWithoutBudget(ctx, "NodesLowestAnsweredPins"); err != nil {
		return nil, err
	}
	return ops.NodesLowestAnsweredPins(ids)
}
