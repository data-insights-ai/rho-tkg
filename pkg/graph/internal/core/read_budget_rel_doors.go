package core

import (
	"context"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The relationship by-ID doors under a read budget (RH list R2), the mirror of
// the node doors in read_budget_doors.go: one candidate per ID examined,
// counted before any read; past a limit *storepkg.ReadBudgetError and no
// answer. ctx is checked first. Without a budget each is its plain door.

// GetByIDsCtx is GetByIDs under ctx's read budget: the candidates, then the
// stored bytes of each row the store decodes and, on tiered, each closed shard
// it opens (store.RelsByIDsMeteredCapability).
func (r *RelOps) GetByIDsCtx(ctx context.Context, ids []types.RelID) ([]*types.Relationship, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	if err := r.c.budgetSupported(ctx, "GetByIDs"); err != nil {
		return nil, err
	}
	meter := storepkg.ReadMeterFrom(ctx)
	if err := meter.AddCandidates("GetByIDs", len(ids)); err != nil {
		return nil, err
	}
	if meter == nil {
		return r.GetByIDs(ids)
	}
	return r.getByIDsTallied(ids, meter.Tally("GetByIDs"))
}

// getByIDsTallied is GetByIDs counting the bytes the store decodes and the
// shards it opens in tally.
func (r *RelOps) getByIDsTallied(ids []types.RelID, tally *storepkg.ReadTally) ([]*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	for _, id := range ids {
		if err := storepkg.ValidateRelID(id); err != nil {
			return nil, err
		}
	}
	var result []*types.Relationship
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.relsByIDsTalliedLocked(ids, tally)
		return err
	})
	return result, err
}

// HasHistoryCtx is HasHistory under ctx's read budget: one candidate, and on a
// store that opens shards each closed one the walk is about to open, counted
// before the open (store.RelStampsMeteredCapability).
func (r *RelOps) HasHistoryCtx(ctx context.Context, id types.RelID) (bool, error) {
	if err := checkCtx(ctx); err != nil {
		return false, err
	}
	if err := r.c.budgetSupported(ctx, "HasHistory"); err != nil { // a wrapper store cannot count its opens
		return false, err
	}
	meter := storepkg.ReadMeterFrom(ctx)
	if err := meter.AddCandidates("HasHistory", 1); err != nil {
		return false, err
	}
	c := r.c
	m, ok := c.store.(storepkg.RelStampsMeteredCapability)
	if meter == nil {
		return r.HasHistory(id)
	}
	if !ok || !c.storeCountsReads { // a wrapper keeps its overrides as the read; one that hides its capabilities folds the history, whose versions count
		return r.hasHistoryTallied(id, meter.Tally("HasHistory"))
	}
	if err := c.checkOpen(); err != nil {
		return false, err
	}
	if err := storepkg.ValidateRelID(id); err != nil {
		return false, err
	}
	var has bool
	err := c.readUnderRLock(func() error {
		var err error
		has, err = m.HasRelHistoryMetered(id, meter.Tally("HasHistory"))
		return err
	})
	return has, err
}

// LatestStampsCtx is LatestStamps under ctx's read budget: one candidate, and
// on a store that opens shards each closed one the reads are about to open,
// counted before the open (store.RelStampsMeteredCapability).
func (r *RelOps) LatestStampsCtx(ctx context.Context, id types.RelID) (txFrom, txTo types.Instant, deleted bool, err error) {
	if err := checkCtx(ctx); err != nil {
		return 0, 0, false, err
	}
	if err := r.c.budgetSupported(ctx, "LatestStamps"); err != nil { // a wrapper store cannot count its opens
		return 0, 0, false, err
	}
	meter := storepkg.ReadMeterFrom(ctx)
	if err := meter.AddCandidates("LatestStamps", 1); err != nil {
		return 0, 0, false, err
	}
	if meter == nil {
		return r.LatestStamps(id)
	}
	return r.latestStampsTallied(id, meter.Tally("LatestStamps"))
}

// OutgoingCtx is Outgoing under ctx's read budget: the node's adjacency
// entries count as candidates before they are collected (a RAM index: the
// node's whole set, before the list is built; a disk index: each entry as the
// walk produces it), then the stored bytes of each row decoded and, on tiered,
// each closed shard opened (a reference node's mint-placed rels included),
// counted before the open (store.AdjacencyMeteredCapability). A store that
// cannot count its own reads has the node's degree counted before the read
// where it has one, else the rows after. Past a limit
// *storepkg.ReadBudgetError and no rows; without a budget it is Outgoing.
func (r *RelOps) OutgoingCtx(ctx context.Context, nodeID types.NodeID, typeName string) ([]*types.Relationship, error) {
	return r.adjacentCtx(ctx, nodeID, typeName, false)
}

// IncomingCtx is OutgoingCtx for the incoming adjacency.
func (r *RelOps) IncomingCtx(ctx context.Context, nodeID types.NodeID, typeName string) ([]*types.Relationship, error) {
	return r.adjacentCtx(ctx, nodeID, typeName, true)
}

func (r *RelOps) adjacentCtx(ctx context.Context, nodeID types.NodeID, typeName string, incoming bool) ([]*types.Relationship, error) {
	door := "Outgoing"
	if incoming {
		door = "Incoming"
	}
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	if err := r.c.budgetSupported(ctx, door); err != nil {
		return nil, err
	}
	meter := storepkg.ReadMeterFrom(ctx)
	if meter == nil {
		if incoming {
			return r.Incoming(nodeID, typeName)
		}
		return r.Outgoing(nodeID, typeName)
	}
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if typeName != "" {
		if err := c.validateRelTypeQueryName(typeName); err != nil {
			return nil, err
		}
	}
	if err := storepkg.ValidateNodeID(nodeID); err != nil {
		return nil, err
	}
	var result []*types.Relationship
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.adjacentRelsTalliedLocked(nodeID, typeName, incoming, storepkg.DepthAll, meter.Tally(door))
		return err
	})
	return result, err
}

// GetCtx is Get under ctx's read budget (the twin of Nodes().GetCtx): the same
// row or error as Get, or *storepkg.ReadBudgetError and no row. It counts one
// candidate (the ID) and, each before it is done, the stored bytes of the row it
// decodes (DecodedBytes: a row served from the entity cache decodes nothing and
// counts 0) and, on tiered, each closed shard it is about to open (ShardOpens).
// The call's own checks (the ID, an open graph) run before the candidate is
// counted. Without a budget it is Get.
func (r *RelOps) GetCtx(ctx context.Context, id types.RelID) (*types.Relationship, error) {
	const door = "Get"
	c := r.c
	meter, err := c.beginTimeline(ctx, door)
	if err != nil {
		return nil, err
	}
	if meter == nil {
		return r.Get(ctx, id)
	}
	if err := storepkg.ValidateRelID(id); err != nil {
		return nil, err
	}
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := meter.AddCandidates(door, 1); err != nil {
		return nil, err
	}
	var rel *types.Relationship
	err = c.readUnderRLock(func() error {
		rd, release := c.meteredEntityReads(meter.Tally(door))
		defer release()
		var err error
		rel, err = rd.currentRel(id)
		return err
	})
	if err != nil {
		return nil, err
	}
	if err := checkCtx(ctx); err != nil { // as Get: a context cancelled during the read
		return nil, err
	}
	c.opRelReads.Add(1)
	return rel, nil
}
