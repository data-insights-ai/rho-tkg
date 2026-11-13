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
	if meter == nil || !ok || !c.storeCountsReads { // a wrapper keeps its overrides as the read
		return r.HasHistory(id)
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
