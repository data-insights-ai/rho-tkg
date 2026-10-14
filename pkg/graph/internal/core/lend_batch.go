package core

import (
	"cmp"
	"context"
	"errors"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// lendBatchChunk is the most IDs LendByIDs reads under one hold of the core
// read lock (one store read, one badger snapshot); the context is checked
// before each chunk.
const lendBatchChunk = 4096

// LendByIDs lends the current row of each node in ids, as Lend does (the
// store's frozen row, no copy on memory, badger and tiered; a frozen copy
// elsewhere), and calls fn(i, row) for each ID that names a node, i its
// position in ids. An ID with no node is skipped: fn is not called for it.
// The calls come in ascending ID order (equal IDs in ids' order), not in ids'
// order; fn returning false stops. fn runs with no lock held and may call
// back into the graph.
//
// The IDs are read in chunks of 4,096 in ascending order, each under one
// hold of the graph's read lock: badger reads a chunk's rows missing from
// its cache through one snapshot, its iterator moving forward, and does not
// cache them; memory reads a chunk under one store lock; other stores lend
// per ID. Each row is a snapshot at its chunk's read. The context is checked
// before each chunk. Safe for concurrent calls on any ID sets.
//
// Errors: an error for a nil fn, ErrInvalidStoreMutation for a non-positive ID
// (before any read), ErrGraphClosed, the context's error, a store read error
// (for example a corrupt row); fn may have been called for earlier chunks.
func (n *NodeOps) LendByIDs(ctx context.Context, ids []types.NodeID, fn func(i int, n *types.Node) bool) error {
	c := n.c
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	for _, id := range ids {
		if err := storepkg.ValidateNodeID(id); err != nil {
			return err
		}
	}
	batch, _ := c.store.(storepkg.EntityLendBatchCapability)
	lender, _ := c.store.(storepkg.EntityLendCapability)
	if !c.storeRowsTrust {
		batch, lender = nil, nil
	}
	read := func(sorted []types.NodeID, out []*types.Node) error {
		if batch != nil {
			return batch.LendNodesSorted(sorted, out)
		}
		for j, id := range sorted {
			var row *types.Node
			var err error
			if lender != nil {
				row, err = lender.LendNode(id)
			} else if row, err = c.getCurrentNode(id); err == nil {
				row.Freeze()
			}
			if errors.Is(err, storepkg.ErrNodeNotFound) {
				row, err = nil, nil
			}
			if err != nil {
				return err
			}
			out[j] = row
		}
		return nil
	}
	return lendByIDs(ctx, c, ids, read, fn, &c.opNodeReads)
}

// LendByIDs is NodeOps.LendByIDs for relationships (types.ErrFrozenRelationship;
// on memory a relationship of a sealed segment is decoded per call).
func (r *RelOps) LendByIDs(ctx context.Context, ids []types.RelID, fn func(i int, r *types.Relationship) bool) error {
	c := r.c
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	for _, id := range ids {
		if err := storepkg.ValidateRelID(id); err != nil {
			return err
		}
	}
	batch, _ := c.store.(storepkg.EntityLendBatchCapability)
	lender, _ := c.store.(storepkg.EntityLendCapability)
	if !c.storeRowsTrust {
		batch, lender = nil, nil
	}
	read := func(sorted []types.RelID, out []*types.Relationship) error {
		if batch != nil {
			return batch.LendRelationshipsSorted(sorted, out)
		}
		for j, id := range sorted {
			var row *types.Relationship
			var err error
			if lender != nil {
				row, err = lender.LendRelationship(id)
			} else if row, err = c.getCurrentRelationship(id); err == nil {
				row.Freeze()
			}
			if errors.Is(err, storepkg.ErrRelNotFound) {
				row, err = nil, nil
			}
			if err != nil {
				return err
			}
			out[j] = row
		}
		return nil
	}
	return lendByIDs(ctx, c, ids, read, fn, &c.opRelReads)
}

// reads is the operation counter a lend door adds its lent rows to.
type reads interface{ Add(int64) int64 }

// lendByIDs is the shared body of the LendByIDs doors: order ids ascending
// (a permutation, so fn's index refers to the caller's slice), read each
// chunk under the core read lock, call fn outside it.
func lendByIDs[ID ~int64, Row any](ctx context.Context, c *Core, ids []ID,
	read func(sorted []ID, out []*Row) error, fn func(i int, row *Row) bool, counter reads) error {
	if err := checkCtx(ctx); err != nil {
		return err
	}
	var order []int32
	if !slices.IsSorted(ids) {
		order = make([]int32, len(ids))
		for i := range order {
			order[i] = int32(i) // #nosec G115 -- a slice index; the caller's slice is far below 2^31 IDs
		}
		slices.SortStableFunc(order, func(a, b int32) int { return cmp.Compare(ids[a], ids[b]) })
	}
	size := min(len(ids), lendBatchChunk)
	sorted := make([]ID, size)
	out := make([]*Row, size)
	for lo := 0; lo < len(ids); lo += lendBatchChunk {
		if err := checkCtx(ctx); err != nil {
			return err
		}
		hi := min(lo+lendBatchChunk, len(ids))
		chunk, rows := sorted[:hi-lo], out[:hi-lo]
		for j := range chunk {
			if order != nil {
				chunk[j] = ids[order[lo+j]]
			} else {
				chunk[j] = ids[lo+j]
			}
		}
		c.mu.RLock()
		if c.closed.Load() {
			c.mu.RUnlock()
			return ErrGraphClosed
		}
		err := read(chunk, rows)
		c.mu.RUnlock()
		if err != nil {
			return err
		}
		lent := int64(0)
		for j, row := range rows {
			if row == nil {
				continue
			}
			lent++
			i := lo + j
			if order != nil {
				i = int(order[i])
			}
			if !fn(i, row) {
				counter.Add(lent)
				return nil
			}
		}
		counter.Add(lent)
		clear(rows)
	}
	return nil
}
