package core

import (
	"context"
	"errors"
	"fmt"
	"math"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	eventspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/integrity"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- Version chain navigation ---

// VersionBefore returns the version immediately before the given version.
// Returns nil, nil if the predecessor does not exist for a known entity.
func (n *NodeOps) VersionBefore(id types.NodeID, version uint32) (*types.Node, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, err
	}
	var node *types.Node
	err := c.readUnderRLock(func() error {
		if version == 0 {
			return c.requireKnownNodeUnlocked(id)
		}
		var err error
		node, err = c.getNodeVersion(id, version-1)
		if errors.Is(err, storepkg.ErrVersionNotFound) {
			if err := c.requireKnownNodeUnlocked(id); err != nil {
				return err
			}
			node = nil
			return nil
		}
		return err
	})
	return node, err
}

// VersionAfter returns the version immediately after the given version.
// Returns nil, nil if no newer version exists (the given version IS the current tip).
// Checks history first, then falls back to the current node (which may be version+1).
func (n *NodeOps) VersionAfter(id types.NodeID, version uint32) (*types.Node, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, err
	}
	var node *types.Node
	err := c.readUnderRLock(func() error {
		if version == math.MaxUint32 {
			return c.requireKnownNodeUnlocked(id)
		}
		var err error
		node, err = c.getNodeVersion(id, version+1)
		if err == nil {
			return nil
		}
		if !errors.Is(err, storepkg.ErrVersionNotFound) {
			return err
		}
		// Not in history: the current node itself may be version+1.
		current, err2 := c.getCurrentNode(id)
		if err2 != nil {
			if errors.Is(err2, storepkg.ErrNodeNotFound) {
				if err := c.requireNodeHistoryUnlocked(id); err != nil {
					return err
				}
				node = nil
				return nil
			}
			return err2
		}
		if current.Version() == version+1 {
			node = current
			return nil
		}
		node = nil
		return nil
	})
	return node, err
}

// CloseVersion sets ValidTo on the current node to t, marking it temporally
// expired without deleting it or incrementing its version number.
// Acquires c.mu.RLock for transaction isolation — blocked while a tx holds c.mu.Lock.
func (n *NodeOps) CloseVersion(ctx context.Context, id types.NodeID, t types.Instant) error {
	c := n.c
	if err := c.checkWritable(); err != nil {
		return err
	}
	if err := checkCtx(ctx); err != nil {
		return err
	}
	var err error
	ep, closeErr := c.runUnderRLock(func() {
		err = c.closeNodeVersionInternal(id, t)
	})
	if closeErr != nil {
		return closeErr
	}
	if err == nil {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventNodeUpdate, EntityID: types.EntityID(id), Timestamp: c.now(), Priority: eventspkg.PriorityNormal})
	}
	return err
}

// closeNodeVersionInternal is the lock-free implementation of CloseNodeVersion.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
func (c *Core) closeNodeVersionInternal(id types.NodeID, t types.Instant) error {
	if err := storepkg.ValidateNodeID(id); err != nil {
		return err
	}
	if t == 0 {
		return fmt.Errorf("%w: close time must be non-zero", ErrInvalidTimeRange)
	}
	c.entityLocks.LockEntity(id.SnowflakeID())
	defer c.entityLocks.UnlockEntity(id.SnowflakeID())

	// GetNode returns a deep copy (Store contract). Mutations below are safe.
	current, err := c.getCurrentNode(id)
	if err != nil {
		return err
	}

	if tm := current.Temporal(); tm != nil && tm.ValidTo != 0 {
		return ErrAlreadyClosed
	}
	if start := c.nodeValidFrom(current); t <= start {
		return fmt.Errorf("%w: close time %d must be after node valid-from %d", ErrInvalidTimeRange, t, start)
	}
	if err := c.checkNodeCloseTemporalConstraints(id, current, t); err != nil {
		return err
	}
	if err := c.checkpointDirtyRegistriesBeforeMutation("close node version"); err != nil {
		return err
	}

	prevVersion := current.Version()
	prevState := current.DeepCopy()
	now := c.nodeVersionUpdateInstant(current)
	if ptm := prevState.Temporal(); ptm == nil {
		ptm = &types.TemporalMetadata{}
		prevState.SetTemporal(ptm)
		ptm.TxTo = now
	} else {
		ptm.TxTo = now
	}

	tm := current.Temporal()
	if tm == nil {
		tm = &types.TemporalMetadata{}
		current.SetTemporal(tm)
	}
	tm.ValidTo = t
	tm.UpdatedAt = now
	tm.TxFrom = now

	// Preserve existing chain position; recompute hash after temporal change.
	prevHash := ""
	if ig := current.Integrity(); ig != nil {
		prevHash = ig.PrevHash
	}
	nodeLabels := c.nodeLabelsUnlocked(current)
	hash, err := integrity.ComputeNodeHashChecked(current, nodeLabels)
	if err != nil {
		return fmt.Errorf("graph: compute node hash: %w", err)
	}
	current.SetIntegrity(nodeIntegrityWithHash(current.Integrity(), hash, prevHash))

	if err := c.store.ReplaceNodeWithHistory(current, prevVersion, prevState); err != nil {
		return err
	}
	c.opNodeUpdates.Add(1)
	return nil
}

// VersionBefore returns the version immediately before the given version.
// Returns nil, nil if the predecessor does not exist for a known entity.
func (r *RelOps) VersionBefore(id types.RelID, version uint32) (*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := storepkg.ValidateRelID(id); err != nil {
		return nil, err
	}
	var rel *types.Relationship
	err := c.readUnderRLock(func() error {
		if version == 0 {
			return c.requireKnownRelUnlocked(id)
		}
		var err error
		rel, err = c.getRelVersion(id, version-1)
		if errors.Is(err, storepkg.ErrVersionNotFound) {
			if err := c.requireKnownRelUnlocked(id); err != nil {
				return err
			}
			rel = nil
			return nil
		}
		return err
	})
	return rel, err
}

// VersionAfter returns the version immediately after the given version.
// Returns nil, nil if no newer version exists (the given version IS the current tip).
// Checks history first, then falls back to the current relationship (which may be version+1).
func (r *RelOps) VersionAfter(id types.RelID, version uint32) (*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := storepkg.ValidateRelID(id); err != nil {
		return nil, err
	}
	var rel *types.Relationship
	err := c.readUnderRLock(func() error {
		if version == math.MaxUint32 {
			return c.requireKnownRelUnlocked(id)
		}
		var err error
		rel, err = c.getRelVersion(id, version+1)
		if err == nil {
			return nil
		}
		if !errors.Is(err, storepkg.ErrVersionNotFound) {
			return err
		}
		// Not in history: the current rel itself may be version+1.
		current, err2 := c.getCurrentRelationship(id)
		if err2 != nil {
			if errors.Is(err2, storepkg.ErrRelNotFound) {
				if err := c.requireRelHistoryUnlocked(id); err != nil {
					return err
				}
				rel = nil
				return nil
			}
			return err2
		}
		if current.Version() == version+1 {
			rel = current
			return nil
		}
		rel = nil
		return nil
	})
	return rel, err
}

func (c *Core) requireKnownNodeUnlocked(id types.NodeID) error {
	if err := storepkg.ValidateNodeID(id); err != nil {
		return err
	}
	if _, err := c.getCurrentNode(id); err == nil {
		return nil
	} else if !errors.Is(err, storepkg.ErrNodeNotFound) {
		return err
	}
	return c.requireNodeHistoryUnlocked(id)
}

func (c *Core) requireNodeHistoryUnlocked(id types.NodeID) error {
	if err := storepkg.ValidateNodeID(id); err != nil {
		return err
	}
	history, err := c.getNodeHistory(id)
	if err != nil {
		return err
	}
	if len(history) == 0 {
		return storepkg.ErrNodeNotFound
	}
	return nil
}

func (c *Core) requireKnownRelUnlocked(id types.RelID) error {
	if err := storepkg.ValidateRelID(id); err != nil {
		return err
	}
	if _, err := c.getCurrentRelationship(id); err == nil {
		return nil
	} else if !errors.Is(err, storepkg.ErrRelNotFound) {
		return err
	}
	return c.requireRelHistoryUnlocked(id)
}

func (c *Core) requireRelHistoryUnlocked(id types.RelID) error {
	if err := storepkg.ValidateRelID(id); err != nil {
		return err
	}
	history, err := c.getRelHistory(id)
	if err != nil {
		return err
	}
	if len(history) == 0 {
		return storepkg.ErrRelNotFound
	}
	return nil
}

// CloseVersion sets ValidTo on the current relationship to t, marking it
// temporally expired without deleting it or incrementing its version number.
// Acquires c.mu.RLock for transaction isolation — blocked while a tx holds c.mu.Lock.
func (r *RelOps) CloseVersion(ctx context.Context, id types.RelID, t types.Instant) error {
	c := r.c
	if err := c.checkWritable(); err != nil {
		return err
	}
	if err := checkCtx(ctx); err != nil {
		return err
	}
	var err error
	ep, closeErr := c.runUnderRLock(func() {
		err = c.closeRelVersionInternal(id, t)
	})
	if closeErr != nil {
		return closeErr
	}
	if err == nil {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventRelUpdate, EntityID: types.EntityID(id), Timestamp: c.now(), Priority: eventspkg.PriorityNormal})
	}
	return err
}

// closeRelVersionInternal is the lock-free implementation of CloseRelVersion.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
func (c *Core) closeRelVersionInternal(id types.RelID, t types.Instant) error {
	if err := storepkg.ValidateRelID(id); err != nil {
		return err
	}
	if t == 0 {
		return fmt.Errorf("%w: close time must be non-zero", ErrInvalidTimeRange)
	}

	current, startID, endID, err := c.lockRelationshipCurrentEndpoints(context.Background(), id)
	if err != nil {
		return err
	}
	defer c.entityLocks.UnlockThree(id.SnowflakeID(), startID.SnowflakeID(), endID.SnowflakeID())

	if tm := current.Temporal(); tm != nil && tm.ValidTo != 0 {
		return ErrAlreadyClosed
	}
	if start := c.relValidFrom(current); t <= start {
		return fmt.Errorf("%w: close time %d must be after relationship valid-from %d", ErrInvalidTimeRange, t, start)
	}
	if c.constraints.Len() > 0 {
		liveStart, liveEnd, err := c.liveEndpointNodes(startID, endID)
		if err != nil {
			return err
		}
		if err := c.checkTemporalConstraints(relWithValidTo(current, t), liveStart, liveEnd); err != nil {
			return err
		}
	}
	if err := c.checkpointDirtyRegistriesBeforeMutation("close relationship version"); err != nil {
		return err
	}

	prevVersion := current.Version()
	prevState := current.DeepCopy()
	now := c.relVersionUpdateInstant(current)
	if ptm := prevState.Temporal(); ptm == nil {
		ptm = &types.TemporalMetadata{}
		prevState.SetTemporal(ptm)
		ptm.TxTo = now
	} else {
		ptm.TxTo = now
	}

	tm := current.Temporal()
	if tm == nil {
		tm = &types.TemporalMetadata{}
		current.SetTemporal(tm)
	}
	tm.ValidTo = t
	tm.UpdatedAt = now
	tm.TxFrom = now

	// Preserve existing chain position; recompute hash after temporal change.
	prevHash := ""
	if ig := current.Integrity(); ig != nil {
		prevHash = ig.PrevHash
	}
	relTypeName := c.relTypeUnlocked(current)
	hash, err := integrity.ComputeRelHashChecked(current, relTypeName)
	if err != nil {
		return fmt.Errorf("graph: compute relationship hash: %w", err)
	}
	current.SetIntegrity(relIntegrityWithHash(current.Integrity(), hash, prevHash))

	if err := c.store.ReplaceRelWithHistory(current, prevVersion, prevState); err != nil {
		return err
	}
	c.opRelUpdates.Add(1)
	return nil
}
