package core

import (
	"context"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The by-ID doors under a read budget (storepkg.WithReadBudget, RH list R2):
// one candidate per ID examined, counted before any read; past a limit
// *storepkg.ReadBudgetError and no answer. ctx is checked first. Without a
// budget each is its plain door.

// GetByIDsCtx is GetByIDs under ctx's read budget.
func (n *NodeOps) GetByIDsCtx(ctx context.Context, ids []types.NodeID) ([]*types.Node, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	if err := n.c.budgetSupported(ctx, "GetByIDs"); err != nil {
		return nil, err
	}
	meter := storepkg.ReadMeterFrom(ctx)
	if err := meter.AddCandidates("GetByIDs", len(ids)); err != nil {
		return nil, err
	}
	if meter == nil {
		return n.GetByIDs(ids)
	}
	return n.getByIDsTallied(ids, meter.Tally("GetByIDs"))
}

// getByIDsTallied is GetByIDs counting the bytes the store decodes in tally.
func (n *NodeOps) getByIDsTallied(ids []types.NodeID, tally *storepkg.ReadTally) ([]*types.Node, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	for _, id := range ids {
		if err := storepkg.ValidateNodeID(id); err != nil {
			return nil, err
		}
	}
	var result []*types.Node
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.nodesByIDsTalliedLocked(ids, tally)
		return err
	})
	return result, err
}

// HasHistoryCtx is HasHistory under ctx's read budget.
func (n *NodeOps) HasHistoryCtx(ctx context.Context, id types.NodeID) (bool, error) {
	if err := checkCtx(ctx); err != nil {
		return false, err
	}
	if err := n.c.budgetSupported(ctx, "HasHistory"); err != nil { // a wrapper store cannot count its opens
		return false, err
	}
	meter := storepkg.ReadMeterFrom(ctx)
	if err := meter.AddCandidates("HasHistory", 1); err != nil {
		return false, err
	}
	m, ok := n.c.store.(storepkg.NodeStampsMeteredCapability)
	if meter == nil || !ok {
		return n.HasHistory(id)
	}
	// A store that opens shards counts each closed one before it opens
	// (ShardOpens, READBUDGET).
	c := n.c
	if err := c.checkOpen(); err != nil {
		return false, err
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return false, err
	}
	var has bool
	err := c.readUnderRLock(func() error {
		var err error
		has, err = m.HasNodeHistoryMetered(id, meter.Tally("HasHistory"))
		return err
	})
	return has, err
}

// LatestStampsCtx is LatestStamps under ctx's read budget.
func (n *NodeOps) LatestStampsCtx(ctx context.Context, id types.NodeID) (txFrom, txTo types.Instant, deleted bool, err error) {
	if err := checkCtx(ctx); err != nil {
		return 0, 0, false, err
	}
	if err := n.c.budgetSupported(ctx, "LatestStamps"); err != nil { // a wrapper store cannot count its opens
		return 0, 0, false, err
	}
	meter := storepkg.ReadMeterFrom(ctx)
	if err := meter.AddCandidates("LatestStamps", 1); err != nil {
		return 0, 0, false, err
	}
	if _, ok := n.c.store.(storepkg.NodeStampsMeteredCapability); !ok || meter == nil {
		return n.LatestStamps(id)
	}
	return n.latestStampsTallied(id, meter.Tally("LatestStamps"))
}

// GetCtx is Get under ctx's read budget.
func (n *NodeOps) GetCtx(ctx context.Context, id types.NodeID) (*types.Node, error) {
	const door = "Get"
	c := n.c
	meter, err := c.beginTimeline(ctx, door)
	if err != nil {
		return nil, err
	}
	if meter == nil {
		return n.Get(ctx, id)
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, err
	}
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := meter.AddCandidates(door, 1); err != nil {
		return nil, err
	}
	var node *types.Node
	err = c.readUnderRLock(func() error {
		rd, release := c.meteredEntityReads(meter.Tally(door))
		defer release()
		var err error
		node, err = rd.currentNode(id)
		return err
	})
	if err != nil {
		return nil, err
	}
	if err := checkCtx(ctx); err != nil { // as Get: a context cancelled during the read
		return nil, err
	}
	c.opNodeReads.Add(1)
	return node, nil
}
