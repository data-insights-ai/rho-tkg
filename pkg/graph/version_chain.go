package graph

import (
	"errors"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Version chain navigation ---

// GetPreviousNodeVersion returns the version immediately before the given version.
// Returns nil, nil if version == 0 or if the predecessor does not exist in history.
func (g *Graph) GetPreviousNodeVersion(id types.NodeID, version uint32) (*types.Node, error) {
	if version == 0 {
		return nil, nil
	}
	n, err := g.store.GetNodeVersion(id, version-1)
	if errors.Is(err, ErrVersionNotFound) {
		return nil, nil
	}
	return n, err
}

// GetNextNodeVersion returns the version immediately after the given version.
// Returns nil, nil if no newer version exists (the given version IS the current tip).
// Checks history first, then falls back to the current node (which may be version+1).
func (g *Graph) GetNextNodeVersion(id types.NodeID, version uint32) (*types.Node, error) {
	n, err := g.store.GetNodeVersion(id, version+1)
	if err == nil {
		return n, nil
	}
	if !errors.Is(err, ErrVersionNotFound) {
		return nil, err
	}
	// Not in history: the current node itself may be version+1.
	current, err2 := g.store.GetNode(id)
	if err2 != nil {
		if errors.Is(err2, ErrNodeNotFound) {
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
// Acquires g.mu.RLock for transaction isolation — blocked while a tx holds g.mu.Lock.
func (g *Graph) CloseNodeVersion(id types.NodeID, t types.Instant) error {
	g.mu.RLock()
	err := g.closeNodeVersionInternal(id, t)
	ep := g.events
	g.mu.RUnlock()
	if err == nil {
		dispatchEvent(ep, Event{Type: EventNodeUpdate, EntityID: types.EntityID(id), Timestamp: nowInstant(), Priority: PriorityNormal})
	}
	return err
}

// closeNodeVersionInternal is the lock-free implementation of CloseNodeVersion.
// Callers must hold g.mu.RLock (standalone) or g.mu.Lock (tx/batch).
func (g *Graph) closeNodeVersionInternal(id types.NodeID, t types.Instant) error {
	g.entityLocks.LockEntity(id.SnowflakeID())
	defer g.entityLocks.UnlockEntity(id.SnowflakeID())

	// GetNode returns a deep copy (Store contract). Mutations below are safe.
	current, err := g.store.GetNode(id)
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
	nodeLabels := g.NodeLabels(current)
	hash := ComputeNodeHash(current, nodeLabels)
	current.SetIntegrity(&types.NodeIntegrity{Hash: hash, PrevHash: prevHash})

	if err := g.store.ReplaceNode(current); err != nil {
		return err
	}
	return nil
}

// GetPreviousRelVersion returns the version immediately before the given version.
// Returns nil, nil if version == 0 or if the predecessor does not exist in history.
func (g *Graph) GetPreviousRelVersion(id types.RelID, version uint32) (*types.Relationship, error) {
	if version == 0 {
		return nil, nil
	}
	r, err := g.store.GetRelVersion(id, version-1)
	if errors.Is(err, ErrVersionNotFound) {
		return nil, nil
	}
	return r, err
}

// GetNextRelVersion returns the version immediately after the given version.
// Returns nil, nil if no newer version exists (the given version IS the current tip).
// Checks history first, then falls back to the current relationship (which may be version+1).
func (g *Graph) GetNextRelVersion(id types.RelID, version uint32) (*types.Relationship, error) {
	r, err := g.store.GetRelVersion(id, version+1)
	if err == nil {
		return r, nil
	}
	if !errors.Is(err, ErrVersionNotFound) {
		return nil, err
	}
	// Not in history: the current rel itself may be version+1.
	current, err2 := g.store.GetRelationship(id)
	if err2 != nil {
		if errors.Is(err2, ErrRelNotFound) {
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
// Acquires g.mu.RLock for transaction isolation — blocked while a tx holds g.mu.Lock.
func (g *Graph) CloseRelVersion(id types.RelID, t types.Instant) error {
	g.mu.RLock()
	err := g.closeRelVersionInternal(id, t)
	ep := g.events
	g.mu.RUnlock()
	if err == nil {
		dispatchEvent(ep, Event{Type: EventRelUpdate, EntityID: types.EntityID(id), Timestamp: nowInstant(), Priority: PriorityNormal})
	}
	return err
}

// closeRelVersionInternal is the lock-free implementation of CloseRelVersion.
// Callers must hold g.mu.RLock (standalone) or g.mu.Lock (tx/batch).
func (g *Graph) closeRelVersionInternal(id types.RelID, t types.Instant) error {
	g.entityLocks.LockEntity(id.SnowflakeID())
	defer g.entityLocks.UnlockEntity(id.SnowflakeID())

	// GetRelationship returns a deep copy (Store contract). Mutations below are safe.
	current, err := g.store.GetRelationship(id)
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
	relTypeName := g.RelationshipType(current)
	hash := ComputeRelHash(current, relTypeName)
	current.SetIntegrity(&types.RelIntegrity{Hash: hash, PrevHash: prevHash})

	if err := g.store.ReplaceRelationship(current); err != nil {
		return err
	}
	return nil
}
