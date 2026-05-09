package core

import (
	"errors"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/integrity"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Version chain navigation ---

// PreviousVersion returns the version immediately before the given version.
// Returns nil, nil if version == 0 or if the predecessor does not exist in history.
func (n *NodeOps) PreviousVersion(id types.NodeID, version uint32) (*types.Node, error) {
	c := n.c
	if version == 0 {
		return nil, nil
	}
	node, err := c.store.GetNodeVersion(id, version-1)
	if errors.Is(err, storepkg.ErrVersionNotFound) {
		return nil, nil
	}
	return node, err
}

// NextVersion returns the version immediately after the given version.
// Returns nil, nil if no newer version exists (the given version IS the current tip).
// Checks history first, then falls back to the current node (which may be version+1).
func (n *NodeOps) NextVersion(id types.NodeID, version uint32) (*types.Node, error) {
	c := n.c
	node, err := c.store.GetNodeVersion(id, version+1)
	if err == nil {
		return node, nil
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

// CloseVersion sets ValidTo on the current node to t, marking it temporally
// expired without deleting it or incrementing its version number.
// Acquires c.mu.RLock for transaction isolation — blocked while a tx holds c.mu.Lock.
func (n *NodeOps) CloseVersion(id types.NodeID, t types.Instant) error {
	c := n.c
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
	nodeLabels := c.Nodes.Labels(current)
	hash := integrity.ComputeNodeHash(current, nodeLabels)
	current.SetIntegrity(&types.NodeIntegrity{Hash: hash, PrevHash: prevHash})

	if err := c.store.ReplaceNode(current); err != nil {
		return err
	}
	return nil
}

// PreviousVersion returns the version immediately before the given version.
// Returns nil, nil if version == 0 or if the predecessor does not exist in history.
func (r *RelOps) PreviousVersion(id types.RelID, version uint32) (*types.Relationship, error) {
	c := r.c
	if version == 0 {
		return nil, nil
	}
	rel, err := c.store.GetRelVersion(id, version-1)
	if errors.Is(err, storepkg.ErrVersionNotFound) {
		return nil, nil
	}
	return rel, err
}

// NextVersion returns the version immediately after the given version.
// Returns nil, nil if no newer version exists (the given version IS the current tip).
// Checks history first, then falls back to the current relationship (which may be version+1).
func (r *RelOps) NextVersion(id types.RelID, version uint32) (*types.Relationship, error) {
	c := r.c
	rel, err := c.store.GetRelVersion(id, version+1)
	if err == nil {
		return rel, nil
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

// CloseVersion sets ValidTo on the current relationship to t, marking it
// temporally expired without deleting it or incrementing its version number.
// Acquires c.mu.RLock for transaction isolation — blocked while a tx holds c.mu.Lock.
func (r *RelOps) CloseVersion(id types.RelID, t types.Instant) error {
	c := r.c
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
	relTypeName := c.Rels.Type(current)
	hash := integrity.ComputeRelHash(current, relTypeName)
	current.SetIntegrity(&types.RelIntegrity{Hash: hash, PrevHash: prevHash})

	if err := c.store.ReplaceRelationship(current); err != nil {
		return err
	}
	return nil
}
