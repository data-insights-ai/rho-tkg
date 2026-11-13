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
	if err := storepkg.ReadMeterFrom(ctx).AddCandidates("GetByIDs", len(ids)); err != nil {
		return nil, err
	}
	return n.GetByIDs(ids)
}

// HasHistoryCtx is HasHistory under ctx's read budget.
func (n *NodeOps) HasHistoryCtx(ctx context.Context, id types.NodeID) (bool, error) {
	if err := checkCtx(ctx); err != nil {
		return false, err
	}
	if err := storepkg.ReadMeterFrom(ctx).AddCandidates("HasHistory", 1); err != nil {
		return false, err
	}
	return n.HasHistory(id)
}

// LatestStampsCtx is LatestStamps under ctx's read budget.
func (n *NodeOps) LatestStampsCtx(ctx context.Context, id types.NodeID) (txFrom, txTo types.Instant, deleted bool, err error) {
	if err := checkCtx(ctx); err != nil {
		return 0, 0, false, err
	}
	if err := storepkg.ReadMeterFrom(ctx).AddCandidates("LatestStamps", 1); err != nil {
		return 0, 0, false, err
	}
	return n.LatestStamps(id)
}
