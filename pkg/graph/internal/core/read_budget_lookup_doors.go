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

// ByLabelCtx is ByLabel under ctx's read budget (R2, Sigma doors group D): the
// label's current members. Each member counts as a candidate before its row is
// read, where the store lists it (store.NodesByLabelMeteredCapability: a label
// index in RAM counts its whole set before the list is built, one kept on disk
// each entry as its walk produces it, so a budget of 1 over many members stops
// at the listing, not after it), then the stored bytes of each row decoded and,
// on tiered, each closed shard opened, counted before the open. A store that
// cannot count its own reads (memory, a wrapper embedding a native store) has
// the label's count charged before the read. Past a limit
// *storepkg.ReadBudgetError and no rows; without a budget it is ByLabel.
//
// A read at a time coordinate (TxAt, TxPin, ValidAt, the valid interval) is the
// label's history and counts as the property doors' time reads do (R2-SCANS; it
// was refused under a budget, Limit "unsupported"): the label's current members
// are counted where the store lists them, as above, each other node that ever
// carried the label (the membership sidecar's ever-members, else the history
// fold) counts one candidate before its chain is read, and every candidate is
// resolved through a metered read snapshot, so its rows and history versions
// are counted (DecodedBytes, HistoryExamined and, on tiered, ShardOpens: the
// membership gather and the reads charge each closed shard before they open it)
// before they are decoded. BufferBytes counts 8 per listed candidate, once the
// list is built, and 8 per answered row. A tiered store with no usable
// membership sidecar folds the history IDs of every shard, which cannot count
// its opens: under a ShardOpens budget it refuses before the fold (Limit
// "unsupported"). The answer is the plain door's, paged the same.
func (n *NodeOps) ByLabelCtx(ctx context.Context, label string, opts storepkg.QueryOpts) ([]*types.Node, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	if err := n.c.budgetSupported(ctx, "ByLabel"); err != nil {
		return nil, err
	}
	return n.ByLabel(label, opts.WithReadMeter(storepkg.ReadMeterFrom(ctx), "ByLabel"))
}

// storeNodesByLabel is the store's current-members read of label token tok
// (c.mu held). Under a read budget in opts a native store counts each member
// it lists before it reads a row (store.NodesByLabelMeteredCapability); one
// that cannot (memory, a wrapper) has the label's count (StatsCapability, which
// every store has) charged first, so a budget under it refuses before the read.
func (c *Core) storeNodesByLabel(tok uint16, opts storepkg.QueryOpts) ([]*types.Node, error) {
	m, _ := opts.ReadMeter()
	if m == nil {
		return c.store.NodesByLabel(tok, opts)
	}
	if ms, ok := c.store.(storepkg.NodesByLabelMeteredCapability); ok && c.storeCountsReads {
		return ms.NodesByLabelMetered(tok, opts)
	}
	if err := c.meterLabelScan(tok, opts); err != nil {
		return nil, err
	}
	return c.store.NodesByLabel(tok, opts)
}
