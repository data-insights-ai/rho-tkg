package core

import (
	"context"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The entity history doors under a read budget (storepkg.WithReadBudget, RH
// list R2, Sigma doors group C): HistoryCtx of a node and of a relationship
// are the plain doors' rows in their order, or *store.ReadBudgetError and no
// rows. ctx is checked first, then a budget the store cannot count is refused;
// the call's own checks (open, ID) run before anything is counted. Without a
// budget each is its plain door.

// HistoryCtx is History under ctx's read budget.
func (n *NodeOps) HistoryCtx(ctx context.Context, id types.NodeID) ([]*types.Node, error) {
	const door = "History"
	c := n.c
	meter, err := c.beginTimeline(ctx, door)
	if err != nil {
		return nil, err
	}
	if meter == nil {
		return n.History(id)
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
	var history []*types.Node
	err = c.readUnderRLock(func() error {
		rd, release := c.meteredEntityReads(meter.Tally(door))
		defer release()
		var err error
		history, err = rd.nodeHistory(id)
		return err
	})
	if err != nil {
		return nil, err
	}
	return history, nil
}

// HistoryCtx is History under ctx's read budget.
func (r *RelOps) HistoryCtx(ctx context.Context, id types.RelID) ([]*types.Relationship, error) {
	const door = "History"
	c := r.c
	meter, err := c.beginTimeline(ctx, door)
	if err != nil {
		return nil, err
	}
	if meter == nil {
		return r.History(id)
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
	var history []*types.Relationship
	err = c.readUnderRLock(func() error {
		rd, release := c.meteredEntityReads(meter.Tally(door))
		defer release()
		var err error
		history, err = rd.relHistory(id)
		return err
	})
	if err != nil {
		return nil, err
	}
	return history, nil
}
