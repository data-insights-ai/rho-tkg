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
	return t.nodeAtCtx(ctx, "NodeAtTx", id, validAt, txAt, func() (*types.Node, error) { return t.NodeAtTx(id, validAt, txAt) })
}

// NodeAtCtx is NodeAt under ctx's read budget: NodeAtTxCtx with no transaction
// filter (NodeAt reads at txAt 0), counted under the door name "NodeAt".
func (t *TempOps) NodeAtCtx(ctx context.Context, id types.NodeID, at types.Instant) (*types.Node, error) {
	return t.nodeAtCtx(ctx, "NodeAt", id, at, 0, func() (*types.Node, error) { return t.NodeAt(id, at) })
}

// nodeAtCtx is the body of the node point-read Ctx doors; plain is the door's
// plain form, which a context without a budget reads as.
func (t *TempOps) nodeAtCtx(ctx context.Context, door string, id types.NodeID, validAt, txAt types.Instant, plain func() (*types.Node, error)) (*types.Node, error) {
	c := t.c
	meter, err := c.beginTimeline(ctx, door)
	if err != nil {
		return nil, err
	}
	if meter == nil {
		return plain()
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
// budget: it counts one candidate and 8 buffer bytes per ID before computing any
// answer, then the rows its retention gate reads: with a retention watermark set
// (below the pins' reach no row is read) each node's current row and, for a
// deleted one, its history, through the metered read snapshot (decoded bytes,
// history versions and, on tiered, closed shards opened), each counted before
// it is done. The compaction stubs and the watermarks are small meta reads,
// not counted.
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
	return t.nodesLowestAnsweredPins(ids, meter.Tally(door))
}

// RelsLowestAnsweredPinsCtx is RelsLowestAnsweredPins under ctx's read budget,
// as NodesLowestAnsweredPinsCtx: one candidate and 8 buffer bytes per ID before
// any answer, then, with a retention watermark set, each relationship's current
// row and a deleted one's history through the metered read snapshot, each
// counted before it is done.
func (t *TempOps) RelsLowestAnsweredPinsCtx(ctx context.Context, ids []types.RelID) ([]types.Instant, error) {
	return t.relsLowestAnsweredPinsCtx(ctx, "RelsLowestAnsweredPins", ids)
}

// RelLowestAnsweredPinCtx is RelLowestAnsweredPin under ctx's read budget:
// RelsLowestAnsweredPinsCtx for one ID, counted under the door name
// "RelLowestAnsweredPin" (one candidate and 8 buffer bytes, then, with a
// retention watermark set, the relationship's current row and, if deleted, its
// history through the metered read snapshot).
func (t *TempOps) RelLowestAnsweredPinCtx(ctx context.Context, id types.RelID) (types.Instant, error) {
	out, err := t.relsLowestAnsweredPinsCtx(ctx, "RelLowestAnsweredPin", []types.RelID{id})
	if err != nil {
		return 0, err
	}
	return out[0], nil
}

// relsLowestAnsweredPinsCtx is the body of the relationship lowest-answered-pin
// Ctx doors, counted under door.
func (t *TempOps) relsLowestAnsweredPinsCtx(ctx context.Context, door string, ids []types.RelID) ([]types.Instant, error) {
	c := t.c
	meter, err := c.beginTimeline(ctx, door)
	if err != nil {
		return nil, err
	}
	if meter == nil {
		return t.RelsLowestAnsweredPins(ids)
	}
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	for _, id := range ids {
		if err := storepkg.ValidateRelID(id); err != nil {
			return nil, err
		}
	}
	if err := meter.AddCandidates(door, len(ids)); err != nil {
		return nil, err
	}
	if err := meter.AddBufferBytes(door, len(ids)*bufSliceRef); err != nil {
		return nil, err
	}
	return t.relsLowestAnsweredPins(ids, meter.Tally(door))
}

// The relationship twins (R2-TWINS): the node doors above for relationships,
// with the relationship family's retention gate (checkRelTypePointRetention)
// reading through the budgeted read's metered snapshot as the node gate does.

// RelAtTxCtx is RelAtTx under ctx's read budget: one candidate, then the rows
// decoded, history versions looked at and closed shards opened by the read, each
// counted before it is done, the retention gate's row read included (the twin of
// NodeAtTxCtx).
func (t *TempOps) RelAtTxCtx(ctx context.Context, id types.RelID, validAt, txAt types.Instant) (*types.Relationship, error) {
	return t.relAtCtx(ctx, "RelAtTx", id, validAt, txAt, func() (*types.Relationship, error) { return t.RelAtTx(id, validAt, txAt) })
}

// RelAtCtx is RelAt under ctx's read budget: RelAtTxCtx with no transaction
// filter (RelAt reads at txAt 0), counted under the door name "RelAt" (the twin
// of NodeAtCtx).
func (t *TempOps) RelAtCtx(ctx context.Context, id types.RelID, at types.Instant) (*types.Relationship, error) {
	return t.relAtCtx(ctx, "RelAt", id, at, 0, func() (*types.Relationship, error) { return t.RelAt(id, at) })
}

// relAtCtx is the body of the relationship point-read Ctx doors; plain is the
// door's plain form, which a context without a budget reads as.
func (t *TempOps) relAtCtx(ctx context.Context, door string, id types.RelID, validAt, txAt types.Instant, plain func() (*types.Relationship, error)) (*types.Relationship, error) {
	c := t.c
	meter, err := c.beginTimeline(ctx, door)
	if err != nil {
		return nil, err
	}
	if meter == nil {
		return plain()
	}
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := storepkg.ValidateRelID(id); err != nil {
		return nil, err
	}
	if err := meter.AddCandidates(door, 1); err != nil {
		return nil, err
	}
	var result *types.Relationship
	err = c.readUnderRLock(func() error {
		rd, release := c.meteredEntityReads(meter.Tally(door))
		defer release()
		if err := c.checkRelTypePointRetentionRd(rd, id, txAt); err != nil {
			return err
		}
		r, err := c.relAtLockedTxRd(rd, id, validAt, txAt)
		result = r
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// RelsAtTxByIDsCtx is RelsAtTxByIDs under ctx's read budget: after the plain
// door's checks it counts one candidate and 8 buffer bytes per ID (the result
// slice) before any ID is read, then the rows decoded, history looked at and
// shards opened by each distinct ID's read before they happen, the retention
// gate's row reads among them (the twin of NodesAtTxByIDsCtx).
func (t *TempOps) RelsAtTxByIDsCtx(ctx context.Context, ids []types.RelID, validAt, txAt types.Instant) ([]*types.Relationship, error) {
	const door = "RelsAtTxByIDs"
	c := t.c
	meter, err := c.beginTimeline(ctx, door)
	if err != nil {
		return nil, err
	}
	if meter == nil {
		return t.RelsAtTxByIDs(ids, validAt, txAt)
	}
	if err := c.relsAtTxByIDsGates(ids, txAt); err != nil {
		return nil, err
	}
	if err := meter.AddCandidates(door, len(ids)); err != nil {
		return nil, err
	}
	if err := meter.AddBufferBytes(door, len(ids)*bufSliceRef); err != nil {
		return nil, err
	}
	return c.relsAtTxByIDsTallied(meter.Tally(door), ids, validAt, txAt)
}

// RelsEffectiveByIDsCtx is RelsEffectiveByIDs under ctx's read budget
// (temporal.API.RelsEffectiveByIDsCtx), the stream twin of NodesEffectiveByIDsCtx.
func (t *TempOps) RelsEffectiveByIDsCtx(ctx context.Context, ids []types.RelID, pin types.Instant, fn func(temporal.RelSegment) bool) error {
	run, err := t.c.scanRunFor(ctx, "RelsEffectiveByIDs")
	if err != nil {
		return err
	}
	return t.c.relsEffectiveByIDsRun(run, ids, recordedPin(pin), fn)
}
