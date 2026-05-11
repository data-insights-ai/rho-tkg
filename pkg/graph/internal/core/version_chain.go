package core

import (
	"errors"
	"fmt"
	"math"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/integrity"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Version chain navigation ---

// PreviousVersion returns the version immediately before the given version.
// Returns nil, nil if the predecessor does not exist for a known entity.
func (n *NodeOps) PreviousVersion(id types.NodeID, version uint32) (*types.Node, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	var node *types.Node
	err := c.readUnderRLock(func() error {
		if version == 0 {
			return c.requireKnownNodeUnlocked(id)
		}
		var err error
		node, err = c.store.GetNodeVersion(id, version-1)
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

// NextVersion returns the version immediately after the given version.
// Returns nil, nil if no newer version exists (the given version IS the current tip).
// Checks history first, then falls back to the current node (which may be version+1).
func (n *NodeOps) NextVersion(id types.NodeID, version uint32) (*types.Node, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	var node *types.Node
	err := c.readUnderRLock(func() error {
		if version == math.MaxUint32 {
			return c.requireKnownNodeUnlocked(id)
		}
		var err error
		node, err = c.store.GetNodeVersion(id, version+1)
		if err == nil {
			return nil
		}
		if !errors.Is(err, storepkg.ErrVersionNotFound) {
			return err
		}
		// Not in history: the current node itself may be version+1.
		current, err2 := c.store.GetNode(id)
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
func (n *NodeOps) CloseVersion(id types.NodeID, t types.Instant) error {
	c := n.c
	if err := c.checkOpen(); err != nil {
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
	current, err := c.store.GetNode(id)
	if err != nil {
		return err
	}

	if tm := current.Temporal(); tm != nil && tm.ValidTo != 0 {
		return ErrAlreadyClosed
	}

	tm := current.Temporal()
	if tm == nil {
		tm = &types.TemporalMetadata{}
		current.SetTemporal(tm)
	}
	tm.ValidTo = t

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

	if err := c.store.ReplaceNode(current); err != nil {
		return err
	}
	c.opNodeUpdates.Add(1)
	return nil
}

// PreviousVersion returns the version immediately before the given version.
// Returns nil, nil if the predecessor does not exist for a known entity.
func (r *RelOps) PreviousVersion(id types.RelID, version uint32) (*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	var rel *types.Relationship
	err := c.readUnderRLock(func() error {
		if version == 0 {
			return c.requireKnownRelUnlocked(id)
		}
		var err error
		rel, err = c.store.GetRelVersion(id, version-1)
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

// NextVersion returns the version immediately after the given version.
// Returns nil, nil if no newer version exists (the given version IS the current tip).
// Checks history first, then falls back to the current relationship (which may be version+1).
func (r *RelOps) NextVersion(id types.RelID, version uint32) (*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	var rel *types.Relationship
	err := c.readUnderRLock(func() error {
		if version == math.MaxUint32 {
			return c.requireKnownRelUnlocked(id)
		}
		var err error
		rel, err = c.store.GetRelVersion(id, version+1)
		if err == nil {
			return nil
		}
		if !errors.Is(err, storepkg.ErrVersionNotFound) {
			return err
		}
		// Not in history: the current rel itself may be version+1.
		current, err2 := c.store.GetRelationship(id)
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
	if _, err := c.store.GetNode(id); err == nil {
		return nil
	} else if !errors.Is(err, storepkg.ErrNodeNotFound) {
		return err
	}
	return c.requireNodeHistoryUnlocked(id)
}

func (c *Core) requireNodeHistoryUnlocked(id types.NodeID) error {
	history, err := c.store.GetNodeHistory(id)
	if err != nil {
		return err
	}
	if len(history) == 0 {
		return storepkg.ErrNodeNotFound
	}
	return nil
}

func (c *Core) requireKnownRelUnlocked(id types.RelID) error {
	if _, err := c.store.GetRelationship(id); err == nil {
		return nil
	} else if !errors.Is(err, storepkg.ErrRelNotFound) {
		return err
	}
	return c.requireRelHistoryUnlocked(id)
}

func (c *Core) requireRelHistoryUnlocked(id types.RelID) error {
	history, err := c.store.GetRelHistory(id)
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
func (r *RelOps) CloseVersion(id types.RelID, t types.Instant) error {
	c := r.c
	if err := c.checkOpen(); err != nil {
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
	c.entityLocks.LockEntity(id.SnowflakeID())
	defer c.entityLocks.UnlockEntity(id.SnowflakeID())

	// GetRelationship returns a deep copy (Store contract). Mutations below are safe.
	current, err := c.store.GetRelationship(id)
	if err != nil {
		return err
	}

	if tm := current.Temporal(); tm != nil && tm.ValidTo != 0 {
		return ErrAlreadyClosed
	}

	tm := current.Temporal()
	if tm == nil {
		tm = &types.TemporalMetadata{}
		current.SetTemporal(tm)
	}
	tm.ValidTo = t

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

	if err := c.store.ReplaceRelationship(current); err != nil {
		return err
	}
	c.opRelUpdates.Add(1)
	return nil
}
