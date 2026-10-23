package core

import (
	"context"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// NodeStoredSizes reports a node's stored rows (store.NodeStoredSizeCapability,
// row 121): a measurement surface, read under the node's entity lock so a
// concurrent write is seen whole or not at all.
func (a *AdminOps) NodeStoredSizes(id types.NodeID) ([]storepkg.StoredRow, error) {
	c := a.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, err
	}
	if c.storedSizes == nil {
		return nil, storepkg.ErrCapabilityNotSupported
	}
	var rows []storepkg.StoredRow
	err := c.readUnderRLock(func() error {
		var err error
		rows, err = c.nodeStoredSizesLocked(id)
		return err
	})
	return rows, err
}

func (c *Core) nodeStoredSizesLocked(id types.NodeID) ([]storepkg.StoredRow, error) {
	c.entityLocks.LockEntity(id.SnowflakeID())
	defer c.entityLocks.UnlockEntity(id.SnowflakeID())
	return c.storedSizes.NodeStoredSizes(id)
}

// ForEachNodeStoredSize hands fn every current member of label (from the
// shards depth selects) with its stored rows, ascending by ID, effScanChunk
// nodes per hold of the graph read lock; fn runs without graph locks and
// returning false stops. ctx is checked between nodes.
func (a *AdminOps) ForEachNodeStoredSize(ctx context.Context, label string, depth storepkg.ShardDepth, fn func(types.NodeID, []storepkg.StoredRow) bool) error {
	c := a.c
	if err := checkCtx(ctx); err != nil {
		return err
	}
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if err := c.validateTemporalQueryOptsScan(storepkg.QueryOpts{Depth: depth}, scopeNodes); err != nil {
		return err
	}
	if err := c.validateIndexLabel(label); err != nil {
		return err
	}
	if c.storedSizes == nil {
		return storepkg.ErrCapabilityNotSupported
	}
	var ids []types.NodeID
	if err := c.readUnderRLock(func() error {
		tok, ok := c.labels.Lookup(label)
		if !ok {
			return nil
		}
		var err error
		if depth == storepkg.DepthAll {
			ids, err = c.nodeLabelCurrentIDs(tok)
			return err
		}
		rows, err := c.store.NodesByLabel(tok, storepkg.QueryOpts{Depth: depth})
		if err != nil {
			return err
		}
		ids, err = c.nodeIDsFromLabelRows(tok, rows)
		return err
	}); err != nil {
		return err
	}
	ids = ascending(ids)
	type entry struct {
		id   types.NodeID
		rows []storepkg.StoredRow
	}
	buf := make([]entry, 0, min(len(ids), effScanChunk))
	for start := 0; start < len(ids); start += effScanChunk {
		chunk := ids[start:min(start+effScanChunk, len(ids))]
		buf = buf[:0]
		if err := c.readUnderRLock(func() error {
			for _, id := range chunk {
				rows, err := c.nodeStoredSizesLocked(id)
				if err != nil {
					return err
				}
				buf = append(buf, entry{id, rows})
			}
			return nil
		}); err != nil {
			return err
		}
		for _, e := range buf {
			if err := checkCtx(ctx); err != nil {
				return err
			}
			if !fn(e.id, e.rows) {
				return nil
			}
		}
	}
	return nil
}
