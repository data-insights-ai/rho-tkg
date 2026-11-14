package temporal

import (
	"context"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
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

// nodeAtCtxOps is the optional part of Ops NodeAtCtx reaches (the graph's own
// ops implement it; Ops itself stays unchanged).
type nodeAtCtxOps interface {
	NodeAtCtx(ctx context.Context, id types.NodeID, at types.Instant) (*types.Node, error)
}

// NodeAtCtx is NodeAt under ctx's read budget: the same row at the same valid
// instant, or *store.ReadBudgetError and no row, with NodeAtTxCtx's counts and
// no transaction filter: one candidate (the ID), and the rows decoded
// (DecodedBytes), the history versions looked at (HistoryExamined) and, on
// tiered, the closed shards opened (ShardOpens: a node live nowhere is searched
// on every shard), each before it is done. The checks, gates and errors are
// NodeAt's. Without a budget it is NodeAt.
func (a *API) NodeAtCtx(ctx context.Context, id types.NodeID, at types.Instant) (*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	if m, ok := ops.(nodeAtCtxOps); ok {
		return m.NodeAtCtx(ctx, id, at)
	}
	if err := asOfWithoutBudget(ctx, "NodeAt"); err != nil {
		return nil, err
	}
	return ops.NodeAt(id, at)
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
// budget: the same aligned answer, or *store.ReadBudgetError and no result. It
// counts one candidate and 8 buffer bytes per ID (the result) before it computes
// any answer; with a retention watermark set it then reads each node's current
// row and a deleted one's history for the retention gate, and counts those
// reads (DecodedBytes, HistoryExamined and, on tiered, ShardOpens) before they
// are done; without a watermark it reads no row and counts nothing else.
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

// relsLowestPinsCtxOps is the optional part of Ops RelsLowestAnsweredPinsCtx
// reaches (the graph's own ops implement it; Ops itself stays unchanged).
type relsLowestPinsCtxOps interface {
	RelsLowestAnsweredPinsCtx(ctx context.Context, ids []types.RelID) ([]types.Instant, error)
}

// RelsLowestAnsweredPinsCtx is RelsLowestAnsweredPins under ctx's read budget,
// as NodesLowestAnsweredPinsCtx: the same aligned answer, or
// *store.ReadBudgetError and no result; one candidate and 8 buffer bytes per ID,
// and with a retention watermark set each relationship's current row and a
// deleted one's history, counted before they are read.
func (a *API) RelsLowestAnsweredPinsCtx(ctx context.Context, ids []types.RelID) ([]types.Instant, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	if m, ok := ops.(relsLowestPinsCtxOps); ok {
		return m.RelsLowestAnsweredPinsCtx(ctx, ids)
	}
	if err := asOfWithoutBudget(ctx, "RelsLowestAnsweredPins"); err != nil {
		return nil, err
	}
	return ops.RelsLowestAnsweredPins(ids)
}

// The relationship twins of the doors above (R2-TWINS,
// RH list R2): the same shape, each a thin wrapper over the graph's own ops,
// which implement them; an Ops without them answers only a context without a
// budget (and a cancelled or nil context is refused first).

// relTwinsCtxOps is the optional part of Ops the relationship Ctx doors reach
// (the graph's own ops implement it; Ops itself stays unchanged).
type relTwinsCtxOps interface {
	RelAtTxCtx(ctx context.Context, id types.RelID, validAt, txAt types.Instant) (*types.Relationship, error)
	RelAtCtx(ctx context.Context, id types.RelID, at types.Instant) (*types.Relationship, error)
	RelsAtTxByIDsCtx(ctx context.Context, ids []types.RelID, validAt, txAt types.Instant) ([]*types.Relationship, error)
	RelsEffectiveByIDsCtx(ctx context.Context, ids []types.RelID, pin types.Instant, fn func(RelSegment) bool) error
	RelLowestAnsweredPinCtx(ctx context.Context, id types.RelID) (types.Instant, error)
}

// RelAtTxCtx is RelAtTx under ctx's read budget: the same row at the same
// pins, or *store.ReadBudgetError and no row. It counts one candidate (the ID),
// and the rows decoded (DecodedBytes), the history versions looked at
// (HistoryExamined) and, on tiered, the closed shards opened (ShardOpens), each
// before it is done; the retention gate's row reads count too. The checks,
// gates and errors are RelAtTx's. Without a budget it is RelAtTx.
func (a *API) RelAtTxCtx(ctx context.Context, id types.RelID, validAt, txAt types.Instant) (*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	if m, ok := ops.(relTwinsCtxOps); ok {
		return m.RelAtTxCtx(ctx, id, validAt, txAt)
	}
	if err := asOfWithoutBudget(ctx, "RelAtTx"); err != nil {
		return nil, err
	}
	return ops.RelAtTx(id, validAt, txAt)
}

// RelAtCtx is RelAt under ctx's read budget: RelAtTxCtx with no transaction
// filter, counted under the door name "RelAt" (the twin of NodeAtCtx). Without
// a budget it is RelAt.
func (a *API) RelAtCtx(ctx context.Context, id types.RelID, at types.Instant) (*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	if m, ok := ops.(relTwinsCtxOps); ok {
		return m.RelAtCtx(ctx, id, at)
	}
	if err := asOfWithoutBudget(ctx, "RelAt"); err != nil {
		return nil, err
	}
	return ops.RelAt(id, at)
}

// RelsAtTxByIDsCtx is RelsAtTxByIDs under ctx's read budget: the same aligned
// answer, or *store.ReadBudgetError and no rows. It validates as RelsAtTxByIDs
// does, then counts one candidate per ID and 8 buffer bytes per ID for the
// result slice before any ID is read (a repeated ID counts at each position: a
// budget reads high, never low), and the rows decoded, history looked at and
// shards opened by each distinct ID's read, the retention gate's included,
// before they are done. Without a budget it is RelsAtTxByIDs.
func (a *API) RelsAtTxByIDsCtx(ctx context.Context, ids []types.RelID, validAt, txAt types.Instant) ([]*types.Relationship, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	if m, ok := ops.(relTwinsCtxOps); ok {
		return m.RelsAtTxByIDsCtx(ctx, ids, validAt, txAt)
	}
	if err := asOfWithoutBudget(ctx, "RelsAtTxByIDs"); err != nil {
		return nil, err
	}
	return ops.RelsAtTxByIDs(ids, validAt, txAt)
}

// RelsEffectiveByIDsCtx is RelsEffectiveByIDs under ctx's read budget: the
// segments of the plain door in the order of ids, as NodesEffectiveByIDsCtx
// streams them (the callbacks made before a refusal stand, none follows it): 8
// buffer bytes per ID of the list before it is copied, one candidate before each
// distinct ID it reads, what each read looks at, decodes and opens, and each
// segment before fn gets it. Under a retention watermark the per-relationship
// retention gate, which the plain door runs for every ID before the first
// segment, counts its row reads and one candidate per distinct ID first, and a
// refusal there hands out no segment. Without a budget it is RelsEffectiveByIDs.
func (a *API) RelsEffectiveByIDsCtx(ctx context.Context, ids []types.RelID, pin types.Instant, fn func(RelSegment) bool) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	if m, ok := ops.(relTwinsCtxOps); ok {
		return m.RelsEffectiveByIDsCtx(ctx, ids, pin, fn)
	}
	if err := asOfWithoutBudget(ctx, "RelsEffectiveByIDs"); err != nil {
		return err
	}
	return ops.RelsEffectiveByIDs(ids, pin, fn)
}

// RelLowestAnsweredPinCtx is RelLowestAnsweredPin under ctx's read budget, as
// RelsLowestAnsweredPinsCtx for one ID: one candidate and 8 buffer bytes, and
// with a retention watermark set the relationship's current row and, if
// deleted, its history, counted before they are read. Without a budget it is
// RelLowestAnsweredPin.
func (a *API) RelLowestAnsweredPinCtx(ctx context.Context, id types.RelID) (types.Instant, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, err
	}
	if m, ok := ops.(relTwinsCtxOps); ok {
		return m.RelLowestAnsweredPinCtx(ctx, id)
	}
	if err := asOfWithoutBudget(ctx, "RelLowestAnsweredPin"); err != nil {
		return 0, err
	}
	return ops.RelLowestAnsweredPin(id)
}

// nodeTwinsCtxOps is the optional part of Ops the last two node Ctx doors reach
// (the graph's own ops implement it; Ops itself stays unchanged).
type nodeTwinsCtxOps interface {
	NodesAtOptsByIDsCtx(ctx context.Context, ids []types.NodeID, opts storepkg.QueryOpts) ([]*types.Node, error)
	NodesAtTxCtx(ctx context.Context, validAt, txAt types.Instant) ([]*types.Node, error)
}

// NodesAtOptsByIDsCtx is NodesAtOptsByIDs under ctx's read budget: the same
// aligned answer, or *store.ReadBudgetError and no rows. It validates as
// NodesAtOptsByIDs does (IDs, the opts' point form, a pin above the clock), then
// counts as NodesAtTxByIDsCtx: one candidate and 8 buffer bytes per ID before
// any ID is read, then the rows decoded, history looked at and shards opened by
// each distinct ID's read. Without a budget it is NodesAtOptsByIDs.
func (a *API) NodesAtOptsByIDsCtx(ctx context.Context, ids []types.NodeID, opts storepkg.QueryOpts) ([]*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	if m, ok := ops.(nodeTwinsCtxOps); ok {
		return m.NodesAtOptsByIDsCtx(ctx, ids, opts)
	}
	if err := asOfWithoutBudget(ctx, "NodesAtOptsByIDs"); err != nil {
		return nil, err
	}
	return ops.NodesAtOptsByIDs(ids, opts)
}

// NodesAtTxCtx is NodesAtTx under ctx's read budget: the same rows in ID order,
// or *store.ReadBudgetError and no rows. Every node ever stored is a candidate
// (the door has no index to narrow it): the store's ID walks (the current IDs
// and the history IDs) count each distinct ID as a candidate, and 24 buffer
// bytes for the set entry, before it joins the candidate set, so a budget stops
// the walk at the ID that passes it; then 8 bytes per ID for the ID list and 8
// for the result slice before any node is read, then the rows decoded, history
// looked at and, on tiered, shards opened by each node's read (the metered read
// snapshot). On tiered the ID walks open every shard and cannot count the
// opens: a ShardOpens budget is refused before the walk with Limit
// "unsupported". Without a budget it is NodesAtTx.
func (a *API) NodesAtTxCtx(ctx context.Context, validAt, txAt types.Instant) ([]*types.Node, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	if m, ok := ops.(nodeTwinsCtxOps); ok {
		return m.NodesAtTxCtx(ctx, validAt, txAt)
	}
	if err := asOfWithoutBudget(ctx, "NodesAtTx"); err != nil {
		return nil, err
	}
	return ops.NodesAtTx(validAt, txAt)
}
