package core

import (
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Lowest answered pin (row 104; sigma O1-3 P1)
//
// NodeLowestAnsweredPin(id) is the lowest transaction pin at which the
// singular point reads of the node (NodeEffectiveTimeline, NodeAsOf, NodeAtTx,
// NodeEffectiveTimelines) answer without ErrHistoryCompacted or
// ErrRetentionExpired: the max of the floors of exactly the gates those reads
// run (compactionFloor, nodeRetentionFloor; for a relationship
// relTypeRetentionFloor and relNodeFamilyFloor as well), so it agrees with
// them by construction. Both gates fall monotonically with the pin: every
// positive pin at or above the result is answered, every pin below it is
// refused. sigma found it by binary search over the reads (~50 calls per
// expired entity).
// =============================================================================

// NodeLowestAnsweredPin returns the lowest transaction pin at which the
// node's point reads answer without ErrHistoryCompacted / ErrRetentionExpired:
// max(the entity's compaction stub LastTrimmedTxTo under the graph
// compaction watermark, the highest retention watermark of its labels). 0
// means every positive pin answers (never compacted, no watermark on its
// labels). An unknown or purged id has no stub or labels to consult: 0, and
// the reads answer ErrNodeNotFound at every pin (a retention purge removes
// the whole node, so pins that answered it before answer not found). The
// result may lie above the commit clock (a purge boundary in the future).
// Pins at or below 0 read "nothing recorded yet" and are not covered.
// Errors: ErrGraphClosed, ErrInvalidStoreMutation (invalid ID).
func (t *TempOps) NodeLowestAnsweredPin(id types.NodeID) (types.Instant, error) {
	out, err := t.NodesLowestAnsweredPins([]types.NodeID{id})
	if err != nil {
		return 0, err
	}
	return out[0], nil
}

// NodesLowestAnsweredPins is NodeLowestAnsweredPin for every id: a slice
// aligned with ids (unsorted and repeated ids allowed), computed under one
// graph read lock. Every id is validated first; one invalid id refuses the
// whole call with no result. Empty ids: an empty result.
func (t *TempOps) NodesLowestAnsweredPins(ids []types.NodeID) ([]types.Instant, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	for _, id := range ids {
		if err := storepkg.ValidateNodeID(id); err != nil {
			return nil, err
		}
	}
	out := make([]types.Instant, len(ids))
	err := c.readUnderRLock(func() error {
		for i, id := range ids {
			f, err := c.nodeLowestAnsweredPinLocked(id)
			if err != nil {
				return err
			}
			out[i] = f
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RelLowestAnsweredPin is NodeLowestAnsweredPin for a relationship: max(its
// compaction stub floor, its type's retention watermark — the relationship
// max when the relationship is gone, nothing when it was minted at or after
// every relationship watermark — and the node-family retention max, which
// every relationship point read checks). An unknown or purged relationship
// minted below the relationship max gets that max (its type is unknown);
// at and above it the reads answer ErrRelNotFound.
func (t *TempOps) RelLowestAnsweredPin(id types.RelID) (types.Instant, error) {
	out, err := t.RelsLowestAnsweredPins([]types.RelID{id})
	if err != nil {
		return 0, err
	}
	return out[0], nil
}

// RelsLowestAnsweredPins is NodesLowestAnsweredPins for relationships.
func (t *TempOps) RelsLowestAnsweredPins(ids []types.RelID) ([]types.Instant, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	for _, id := range ids {
		if err := storepkg.ValidateRelID(id); err != nil {
			return nil, err
		}
	}
	out := make([]types.Instant, len(ids))
	err := c.readUnderRLock(func() error {
		for i, id := range ids {
			f, err := c.relLowestAnsweredPinLocked(id)
			if err != nil {
				return err
			}
			out[i] = f
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// nodeLowestAnsweredPinLocked reads the node's gates under its entity lock,
// as the timeline door does (c.mu held).
func (c *Core) nodeLowestAnsweredPinLocked(id types.NodeID) (types.Instant, error) {
	c.entityLocks.LockEntity(id.SnowflakeID())
	defer c.entityLocks.UnlockEntity(id.SnowflakeID())
	comp, gated, err := c.compactionFloor(func() (compactionStub, bool, error) { return c.loadNodeCompactionStub(id) })
	if err != nil {
		return 0, err
	}
	if !gated {
		comp = 0
	}
	ret, err := c.nodeRetentionFloor(id)
	if err != nil {
		return 0, err
	}
	return types.Instant(max(comp, ret)), nil
}

// relLowestAnsweredPinLocked mirrors nodeLowestAnsweredPinLocked with the
// relationship gates (c.mu held).
func (c *Core) relLowestAnsweredPinLocked(id types.RelID) (types.Instant, error) {
	c.entityLocks.LockEntity(id.SnowflakeID())
	defer c.entityLocks.UnlockEntity(id.SnowflakeID())
	var floor int64
	comp, gated, err := c.compactionFloor(func() (compactionStub, bool, error) { return c.loadRelCompactionStub(id) })
	if err != nil {
		return 0, err
	}
	if gated {
		floor = max(floor, comp)
	}
	typ, gated, err := c.relTypeRetentionFloor(id)
	if err != nil {
		return 0, err
	}
	if gated {
		floor = max(floor, typ)
	}
	if fam, gated := c.relNodeFamilyFloor(); gated {
		floor = max(floor, fam)
	}
	return types.Instant(floor), nil
}
