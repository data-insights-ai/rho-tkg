package core

import (
	"errors"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/integrity"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Version chain navigation ---

// GetPreviousNodeVersion returns the version immediately before the given version.
// Returns nil, nil if version == 0 or if the predecessor does not exist in history.
func (c *Core) GetPreviousNodeVersion(id types.NodeID, version uint32) (*types.Node, error) {
	if version == 0 {
		return nil, nil
	}
	n, err := c.store.GetNodeVersion(id, version-1)
	if errors.Is(err, storepkg.ErrVersionNotFound) {
		return nil, nil
	}
	return n, err
}

// GetNextNodeVersion returns the version immediately after the given version.
// Returns nil, nil if no newer version exists (the given version IS the current tip).
// Checks history first, then falls back to the current node (which may be version+1).
func (c *Core) GetNextNodeVersion(id types.NodeID, version uint32) (*types.Node, error) {
	n, err := c.store.GetNodeVersion(id, version+1)
	if err == nil {
		return n, nil
	}
	if !errors.Is(err, storepkg.ErrVersionNotFound) {
		return nil, err
	}
	// Not in history: the current node itself may be version+1.
	current, err2 := c.store.GetNode(id)
	if err2 != nil {
		if errors.Is(err2, storepkg.ErrNodeNotFound) {
			return nil, nil
		}
		return nil, err2
	}
	if current.Version() == version+1 {
		return current, nil
	}
	return nil, nil
}

// CloseNodeVersion sets ValidTo on the current node to t, marking it temporally
// expired without deleting it or incrementing its version number.
// Acquires c.mu.RLock for transaction isolation — blocked while a tx holds c.mu.Lock.
func (c *Core) CloseNodeVersion(id types.NodeID, t types.Instant) error {
	c.mu.RLock()
	err := c.closeNodeVersionInternal(id, t)
	ep := c.events
	c.mu.RUnlock()
	if err == nil {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventNodeUpdate, EntityID: types.EntityID(id), Timestamp: nowInstant(), Priority: eventspkg.PriorityNormal})
	}
	return err
}

// closeNodeVersionInternal is the lock-free implementation of CloseNodeVersion.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
func (c *Core) closeNodeVersionInternal(id types.NodeID, t types.Instant) error {
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
	nodeLabels := c.NodeLabels(current)
	hash := integrity.ComputeNodeHash(current, nodeLabels)
	current.SetIntegrity(&types.NodeIntegrity{Hash: hash, PrevHash: prevHash})

	if err := c.store.ReplaceNode(current); err != nil {
		return err
	}
	return nil
}

// GetPreviousRelVersion returns the version immediately before the given version.
// Returns nil, nil if version == 0 or if the predecessor does not exist in history.
func (c *Core) GetPreviousRelVersion(id types.RelID, version uint32) (*types.Relationship, error) {
	if version == 0 {
		return nil, nil
	}
	r, err := c.store.GetRelVersion(id, version-1)
	if errors.Is(err, storepkg.ErrVersionNotFound) {
		return nil, nil
	}
	return r, err
}

// GetNextRelVersion returns the version immediately after the given version.
// Returns nil, nil if no newer version exists (the given version IS the current tip).
// Checks history first, then falls back to the current relationship (which may be version+1).
func (c *Core) GetNextRelVersion(id types.RelID, version uint32) (*types.Relationship, error) {
	r, err := c.store.GetRelVersion(id, version+1)
	if err == nil {
		return r, nil
	}
	if !errors.Is(err, storepkg.ErrVersionNotFound) {
		return nil, err
	}
	// Not in history: the current rel itself may be version+1.
	current, err2 := c.store.GetRelationship(id)
	if err2 != nil {
		if errors.Is(err2, storepkg.ErrRelNotFound) {
			return nil, nil
		}
		return nil, err2
	}
	if current.Version() == version+1 {
		return current, nil
	}
	return nil, nil
}

// CloseRelVersion sets ValidTo on the current relationship to t, marking it
// temporally expired without deleting it or incrementing its version number.
// Acquires c.mu.RLock for transaction isolation — blocked while a tx holds c.mu.Lock.
func (c *Core) CloseRelVersion(id types.RelID, t types.Instant) error {
	c.mu.RLock()
	err := c.closeRelVersionInternal(id, t)
	ep := c.events
	c.mu.RUnlock()
	if err == nil {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventRelUpdate, EntityID: types.EntityID(id), Timestamp: nowInstant(), Priority: eventspkg.PriorityNormal})
	}
	return err
}

// closeRelVersionInternal is the lock-free implementation of CloseRelVersion.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
func (c *Core) closeRelVersionInternal(id types.RelID, t types.Instant) error {
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
	relTypeName := c.RelationshipType(current)
	hash := integrity.ComputeRelHash(current, relTypeName)
	current.SetIntegrity(&types.RelIntegrity{Hash: hash, PrevHash: prevHash})

	if err := c.store.ReplaceRelationship(current); err != nil {
		return err
	}
	return nil
}
