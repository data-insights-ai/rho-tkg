package graph

import (
	"fmt"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/integrity"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// AddNodeLabel adds the given label to an existing node.
// Idempotent: returns nil without bumping version or writing history if the
// node already has the label. Validates label name length and enforces
// MaxLabelsPerNode. Returns ErrNodeNotFound if the node does not exist.
// Acquires g.mu.RLock for transaction isolation — blocked while a tx holds g.mu.Lock.
func (g *Graph) AddNodeLabel(id types.NodeID, label string) error {
	g.mu.RLock()
	mutated, err := g.addNodeLabelInternal(id, label)
	ep := g.events
	g.mu.RUnlock()
	if err == nil && mutated {
		dispatchEvent(ep, Event{Type: EventNodeUpdate, EntityID: types.EntityID(id), Timestamp: nowInstant(), Priority: PriorityNormal})
	}
	return err
}

// addNodeLabelInternal is the lock-free implementation of AddNodeLabel.
// Callers must hold g.mu.RLock (standalone) or g.mu.Lock (tx/batch).
// Returns mutated=false if the node already has the label.
func (g *Graph) addNodeLabelInternal(id types.NodeID, label string) (bool, error) {
	if err := g.validateName(label); err != nil {
		return false, err
	}

	g.entityLocks.LockEntity(id.SnowflakeID())
	defer g.entityLocks.UnlockEntity(id.SnowflakeID())

	current, err := g.store.GetNode(id)
	if err != nil {
		return false, err
	}

	// Resolve or create the token only after confirming the node exists, so
	// we don't pollute the registry for unknown IDs.
	tok, err := g.labels.GetOrCreate(label)
	if err != nil {
		return false, fmt.Errorf("graph: add label: %w", err)
	}

	// Idempotent: node already carries the label — no mutation, no history.
	if current.HasLabelTokenRaw(tok) {
		return false, nil
	}

	// Enforce MaxLabelsPerNode against the post-addition count.
	if current.LabelTokenCount()+1 > g.validation.MaxLabelsPerNode {
		return false, fmt.Errorf("%w: %d > %d", ErrTooManyLabels, current.LabelTokenCount()+1, g.validation.MaxLabelsPerNode)
	}

	// Capture pre-mutation state for version history (before any modification).
	prevVersion := current.Version()
	prevState := current.DeepCopy()

	copy := current.DeepCopy()
	if !copy.AddLabelTokenRaw(tok) {
		// Defensive: should never hit — idempotence check above already handled presence.
		return false, nil
	}
	copy.SetVersion(prevVersion + 1)

	// Advance hash chain: PrevHash = current Hash (link new version back to current).
	prevHash := ""
	if ig := current.Integrity(); ig != nil {
		prevHash = ig.Hash
	}
	nodeLabels := g.NodeLabels(copy)
	hash := integrity.ComputeNodeHash(copy, nodeLabels)
	copy.SetIntegrity(&types.NodeIntegrity{Hash: hash, PrevHash: prevHash})

	// Set transaction/update time on both sides of the version boundary.
	now := types.Instant(time.Now().UnixMilli())
	if ptm := prevState.Temporal(); ptm == nil {
		ptm2 := &types.TemporalMetadata{}
		prevState.SetTemporal(ptm2)
		ptm2.TxTo = now
	} else {
		ptm.TxTo = now
	}
	tm := copy.Temporal()
	if tm == nil {
		tm = &types.TemporalMetadata{}
		copy.SetTemporal(tm)
	}
	tm.UpdatedAt = now
	tm.TxFrom = now

	// Atomic: write history entry + add label index + persist updated node in one call.
	if err := g.store.AddNodeLabelTokenWithHistory(id, tok, copy, prevVersion, prevState); err != nil {
		return false, err
	}
	g.opNodeUpdates.Add(1)
	return true, nil
}

// RemoveNodeLabel removes the given label from an existing node.
// Acquires g.mu.RLock for transaction isolation — blocked while a tx holds g.mu.Lock.
func (g *Graph) RemoveNodeLabel(id types.NodeID, label string) error {
	g.mu.RLock()
	err := g.removeNodeLabelInternal(id, label)
	ep := g.events
	g.mu.RUnlock()
	if err == nil {
		dispatchEvent(ep, Event{Type: EventNodeUpdate, EntityID: types.EntityID(id), Timestamp: nowInstant(), Priority: PriorityNormal})
	}
	return err
}

// removeNodeLabelInternal is the lock-free implementation of RemoveNodeLabel.
// Callers must hold g.mu.RLock (standalone) or g.mu.Lock (tx/batch).
func (g *Graph) removeNodeLabelInternal(id types.NodeID, label string) error {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return ErrLabelNotFound
	}

	g.entityLocks.LockEntity(id.SnowflakeID())
	defer g.entityLocks.UnlockEntity(id.SnowflakeID())

	current, err := g.store.GetNode(id)
	if err != nil {
		return err
	}

	if !current.HasLabelTokenRaw(tok) {
		return ErrLabelNotFound
	}
	if current.LabelTokenCount() == 1 {
		return ErrLastLabel
	}

	// Capture pre-mutation state for version history (before any modification).
	prevVersion := current.Version()
	prevState := current.DeepCopy()

	copy := current.DeepCopy()
	copy.RemoveLabelTokenRaw(tok)
	copy.SetVersion(prevVersion + 1)

	// Advance hash chain: PrevHash = current Hash (link new version back to current).
	prevHash := ""
	if ig := current.Integrity(); ig != nil {
		prevHash = ig.Hash
	}
	nodeLabels := g.NodeLabels(copy)
	hash := integrity.ComputeNodeHash(copy, nodeLabels)
	copy.SetIntegrity(&types.NodeIntegrity{Hash: hash, PrevHash: prevHash})

	// Set transaction/update time on both sides of the version boundary.
	now := types.Instant(time.Now().UnixMilli())
	if ptm := prevState.Temporal(); ptm == nil {
		ptm2 := &types.TemporalMetadata{}
		prevState.SetTemporal(ptm2)
		ptm2.TxTo = now
	} else {
		ptm.TxTo = now
	}
	tm := copy.Temporal()
	if tm == nil {
		tm = &types.TemporalMetadata{}
		copy.SetTemporal(tm)
	}
	tm.UpdatedAt = now
	tm.TxFrom = now

	// Atomic: write history entry + remove label index + persist updated node in one call.
	if err := g.store.RemoveNodeLabelTokenWithHistory(id, tok, copy, prevVersion, prevState); err != nil {
		return err
	}
	g.opNodeUpdates.Add(1)
	return nil
}
