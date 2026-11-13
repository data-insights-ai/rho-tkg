package core

import (
	"context"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The property lookup doors Sigma reads under a read budget (RH list R2, Sigma
// doors group A): Rels().ByTypeAndPropertyCtx and
// Nodes().IDsByLabelAndPropertyValuesCtx, with the contract of the node
// property doors (graph_property_query.go): ctx checked first, a budget the
// store cannot count refused (budgetSupported), one candidate per index match
// (or per node of the label, per rel of the type, for a key without an index)
// counted inside the store before the row decode, no partial answer, and
// without a budget exactly the plain door's.

// ByTypeAndPropertyCtx is ByTypeAndProperty under ctx's read budget: each
// index match (or, for a key without an index, each relationship of the type)
// counts as a candidate inside the store before its row is decoded, then the
// stored bytes of every row decoded and, on tiered, each closed shard opened.
//
// A read at a time coordinate (TxAt, TxPin, ValidAt, the valid interval) counts
// as the node twin, ByLabelAndPropertyCtx, does (Sigma doors group D): each
// membership candidate as the index produces it (a disk membership admitted as
// its fold grows), the current lookup's matches, the history versions looked at
// and the stored bytes decoded to resolve each candidate, on tiered each closed
// shard before it opens; a fold with no usable index counts each candidate
// before it is resolved and, on tiered, refuses a ShardOpens budget before its
// walk (Limit "unsupported", it cannot count the opens). The answer is the
// plain door's, in its order.
func (r *RelOps) ByTypeAndPropertyCtx(ctx context.Context, typeName, key string, value any, opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	if err := r.c.budgetSupported(ctx, "ByTypeAndProperty"); err != nil {
		return nil, err
	}
	return r.ByTypeAndProperty(typeName, key, value, opts.WithReadMeter(storepkg.ReadMeterFrom(ctx), "ByTypeAndProperty"))
}

// IDsByLabelAndPropertyValuesCtx is IDsByLabelAndPropertyValues under ctx's
// read budget.
func (n *NodeOps) IDsByLabelAndPropertyValuesCtx(ctx context.Context, label, key string, values []any, opts storepkg.QueryOpts) ([]types.NodeID, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	if err := n.c.budgetSupported(ctx, "IDsByLabelAndPropertyValues"); err != nil {
		return nil, err
	}
	return n.IDsByLabelAndPropertyValues(label, key, values, opts.WithReadMeter(storepkg.ReadMeterFrom(ctx), "IDsByLabelAndPropertyValues"))
}
