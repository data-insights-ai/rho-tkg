package core

import (
	"context"
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The by-ID and bitemporal point doors under a read budget (storepkg.WithReadBudget,
// RH list R2, sigma's group B). NodeAtTxCtx, NodesAtTxByIDsCtx and
// NodesLowestAnsweredPinsCtx are exact or refused (the plain door's answer, or
// *store.ReadBudgetError and nothing); NodesEffectiveByIDsCtx is a stream
// (read_budget_scans.go): the callbacks made before a refusal stand. ctx is
// checked first, then a budget the store cannot count is refused; the call's own
// checks (open, IDs, pins, callback) run before anything is counted. Without a
// budget each is its plain door.

// NodesEffectiveByIDsCtx is NodesEffectiveByIDs under ctx's read budget
// (temporal.API.NodesEffectiveByIDsCtx).
func (t *TempOps) NodesEffectiveByIDsCtx(ctx context.Context, ids []types.NodeID, pin types.Instant, fn func(temporal.NodeSegment) bool) error {
	run, err := t.c.scanRunFor(ctx, "NodesEffectiveByIDs")
	if err != nil {
		return err
	}
	return t.c.nodesEffectiveByIDsRun(run, ids, recordedPin(pin), fn)
}

// NodeAtTxCtx is NodeAtTx under ctx's read budget: one candidate, then the
// rows decoded, history versions looked at and closed shards opened by the
// read, each counted before it happens (the point read of NodeAsOfCtx).
func (t *TempOps) NodeAtTxCtx(ctx context.Context, id types.NodeID, validAt, txAt types.Instant) (*types.Node, error) {
	const door = "NodeAtTx"
	c := t.c
	meter, err := c.beginTimeline(ctx, door)
	if err != nil {
		return nil, err
	}
	if meter == nil {
		return t.NodeAtTx(id, validAt, txAt)
	}
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, err
	}
	if err := meter.AddCandidates(door, 1); err != nil {
		return nil, err
	}
	var result *types.Node
	err = c.readUnderRLock(func() error {
		rd, release := c.meteredEntityReads(meter.Tally(door))
		defer release()
		n, err := c.nodeAtLockedTxRd(rd, id, validAt, txAt)
		result = n
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// NodesAtTxByIDsCtx is NodesAtTxByIDs under ctx's read budget: after the
// plain door's checks it counts one candidate and 8 buffer bytes per ID (the
// result slice) before any ID is read, then the rows decoded, history looked at
// and shards opened by each distinct ID's read before they happen.
func (t *TempOps) NodesAtTxByIDsCtx(ctx context.Context, ids []types.NodeID, validAt, txAt types.Instant) ([]*types.Node, error) {
	const door = "NodesAtTxByIDs"
	c := t.c
	meter, err := c.beginTimeline(ctx, door)
	if err != nil {
		return nil, err
	}
	if meter == nil {
		return t.NodesAtTxByIDs(ids, validAt, txAt)
	}
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	for _, id := range ids {
		if err := storepkg.ValidateNodeID(id); err != nil {
			return nil, err
		}
	}
	if txAt == 0 {
		return nil, fmt.Errorf("%w: transaction-time pin 0 (a pin is required; a pin at the instant 0: NodesAtOptsByIDs with TxAtSet)", ErrInvalidTimeRange)
	}
	if err := c.validateEffectivePin(txAt); err != nil {
		return nil, err
	}
	if err := meter.AddCandidates(door, len(ids)); err != nil {
		return nil, err
	}
	if err := meter.AddBufferBytes(door, len(ids)*bufSliceRef); err != nil {
		return nil, err
	}
	return c.nodesAtTxByIDsTallied(meter.Tally(door), ids, validAt, txAt)
}

// NodesLowestAnsweredPinsCtx is NodesLowestAnsweredPins under ctx's read
// budget: it reads no row, so it counts one candidate and 8 buffer bytes per ID
// before computing any answer, and nothing else.
func (t *TempOps) NodesLowestAnsweredPinsCtx(ctx context.Context, ids []types.NodeID) ([]types.Instant, error) {
	const door = "NodesLowestAnsweredPins"
	c := t.c
	meter, err := c.beginTimeline(ctx, door)
	if err != nil {
		return nil, err
	}
	if meter == nil {
		return t.NodesLowestAnsweredPins(ids)
	}
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	for _, id := range ids {
		if err := storepkg.ValidateNodeID(id); err != nil {
			return nil, err
		}
	}
	if err := meter.AddCandidates(door, len(ids)); err != nil {
		return nil, err
	}
	if err := meter.AddBufferBytes(door, len(ids)*bufSliceRef); err != nil {
		return nil, err
	}
	return t.NodesLowestAnsweredPins(ids)
}
