package core

import (
	"fmt"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/integrity"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// AddLabel adds the given label to an existing node.
// Idempotent: returns nil without bumping version or writing history if the
// node already has the label. Validates label name length and enforces
// MaxLabelsPerNode. Returns storepkg.ErrNodeNotFound if the node does not exist.
// Acquires c.mu.RLock for transaction isolation — blocked while a tx holds c.mu.Lock.
func (n *NodeOps) AddLabel(id types.NodeID, label string) error {
	c := n.c
	var (
		mutated bool
		err     error
	)
	ep, closeErr := c.runUnderRLock(func() {
		mutated, err = c.addNodeLabelInternal(id, label)
	})
	if closeErr != nil {
		return closeErr
	}
	if err == nil && mutated {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventNodeUpdate, EntityID: types.EntityID(id), Timestamp: c.now(), Priority: eventspkg.PriorityNormal})
	}
	return err
}

// addNodeLabelInternal is the lock-free implementation of AddNodeLabel.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
// Returns mutated=false if the node already has the label.
func (c *Core) addNodeLabelInternal(id types.NodeID, label string) (bool, error) {
	if err := c.validateName(label); err != nil {
		return false, err
	}

	c.entityLocks.LockEntity(id.SnowflakeID())
	defer c.entityLocks.UnlockEntity(id.SnowflakeID())

	current, err := c.store.GetNode(id)
	if err != nil {
		return false, err
	}

	// Resolve or create the token only after confirming the node exists, so
	// we don't pollute the registry for unknown IDs.
	tok, err := c.labels.GetOrCreate(label)
	if err != nil {
		return false, fmt.Errorf("graph: add label: %w", err)
	}

	// Idempotent: node already carries the label — no mutation, no history.
	if current.HasLabelTokenRaw(tok) {
		return false, nil
	}

	// Enforce MaxLabelsPerNode against the post-addition count.
	if current.LabelTokenCount()+1 > c.validation.MaxLabelsPerNode {
		return false, fmt.Errorf("%w: %d > %d", ErrTooManyLabels, current.LabelTokenCount()+1, c.validation.MaxLabelsPerNode)
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
	nodeLabels := c.Nodes.Labels(copy)
	hash := integrity.ComputeNodeHash(copy, nodeLabels)
	copy.SetIntegrity(&types.NodeIntegrity{Hash: hash, PrevHash: prevHash})

	// Set transaction/update time on both sides of the version boundary.
	now := c.now()
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
	if err := c.store.AddNodeLabelTokenWithHistory(id, tok, copy, prevVersion, prevState); err != nil {
		return false, err
	}
	c.opNodeUpdates.Add(1)
	return true, nil
}

// RemoveLabel removes the given label from an existing node.
// Acquires c.mu.RLock for transaction isolation — blocked while a tx holds c.mu.Lock.
func (n *NodeOps) RemoveLabel(id types.NodeID, label string) error {
	c := n.c
	var err error
	ep, closeErr := c.runUnderRLock(func() {
		err = c.removeNodeLabelInternal(id, label)
	})
	if closeErr != nil {
		return closeErr
	}
	if err == nil {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventNodeUpdate, EntityID: types.EntityID(id), Timestamp: c.now(), Priority: eventspkg.PriorityNormal})
	}
	return err
}

// removeNodeLabelInternal is the lock-free implementation of RemoveNodeLabel.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
func (c *Core) removeNodeLabelInternal(id types.NodeID, label string) error {
	tok, ok := c.labels.Lookup(label)
	if !ok {
		return ErrLabelNotFound
	}

	c.entityLocks.LockEntity(id.SnowflakeID())
	defer c.entityLocks.UnlockEntity(id.SnowflakeID())

	current, err := c.store.GetNode(id)
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
	nodeLabels := c.Nodes.Labels(copy)
	hash := integrity.ComputeNodeHash(copy, nodeLabels)
	copy.SetIntegrity(&types.NodeIntegrity{Hash: hash, PrevHash: prevHash})

	// Set transaction/update time on both sides of the version boundary.
	now := c.now()
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
	if err := c.store.RemoveNodeLabelTokenWithHistory(id, tok, copy, prevVersion, prevState); err != nil {
		return err
	}
	c.opNodeUpdates.Add(1)
	return nil
}
