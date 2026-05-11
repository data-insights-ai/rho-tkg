package core

import (
	"fmt"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

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
	if err := c.checkOpen(); err != nil {
		return err
	}
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
	if err == nil && mutated && ep != nil {
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
	if err := storepkg.ValidateNodeID(id); err != nil {
		return false, err
	}

	c.entityLocks.LockEntity(id.SnowflakeID())
	defer c.entityLocks.UnlockEntity(id.SnowflakeID())

	current, err := c.store.GetNode(id)
	if err != nil {
		return false, err
	}

	tok, known := c.lookupLabelLocked(label)
	if known {
		// Idempotent: node already carries the label — no mutation, no history.
		if current.HasLabelTokenRaw(tok) {
			return false, nil
		}
	}

	// Enforce MaxLabelsPerNode against the post-addition count.
	if current.LabelTokenCount()+1 > c.validation.MaxLabelsPerNode {
		return false, fmt.Errorf("%w: %d > %d", ErrTooManyLabels, current.LabelTokenCount()+1, c.validation.MaxLabelsPerNode)
	}

	prevVersion := current.Version()
	nextVersion, err := nextEntityVersion(prevVersion)
	if err != nil {
		return false, err
	}

	finishLabel := func(err error) error {
		return err
	}
	labelFinished := true
	if !known {
		var err error
		var labelSnapshot []string
		var allocatedLabel bool
		tok, labelSnapshot, allocatedLabel, err = c.getOrCreateLabelWithSnapshot(label)
		if err != nil {
			return false, fmt.Errorf("graph: add label: %w", err)
		}
		labelFinished = false
		finishLabel = func(err error) error {
			labelFinished = true
			return c.restoreNewLabelOnError(labelSnapshot, allocatedLabel, label, err)
		}
		defer func() {
			if !labelFinished {
				_ = c.restoreNewLabelOnError(labelSnapshot, allocatedLabel, label, fmt.Errorf("panic during add label"))
			}
		}()
	}

	// Capture pre-mutation state for version history (before any modification).
	prevState := current.DeepCopy()

	copy := current.DeepCopy()
	if !copy.AddLabelTokenRaw(tok) {
		// Defensive: should never hit — idempotence check above already handled presence.
		return false, nil
	}
	copy.SetVersion(nextVersion)

	// Advance hash chain: PrevHash = current Hash (link new version back to current).
	prevHash := ""
	if ig := current.Integrity(); ig != nil {
		prevHash = ig.Hash
	}
	nodeLabels := c.appendNodeLabelsUnlocked(make([]string, 0, current.LabelTokenCount()+1), current)
	nodeLabels = append(nodeLabels, label)
	hash, err := integrity.ComputeNodeHashChecked(copy, nodeLabels)
	if err != nil {
		return false, finishLabel(fmt.Errorf("graph: compute node hash: %w", err))
	}
	copy.SetIntegrity(nodeIntegrityWithHash(current.Integrity(), hash, prevHash))

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
		return false, finishLabel(err)
	}
	if err := finishLabel(nil); err != nil {
		return false, err
	}
	c.opNodeUpdates.Add(1)
	return true, nil
}

// RemoveLabel removes the given label from an existing node.
// Acquires c.mu.RLock for transaction isolation — blocked while a tx holds c.mu.Lock.
func (n *NodeOps) RemoveLabel(id types.NodeID, label string) error {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	var err error
	ep, closeErr := c.runUnderRLock(func() {
		err = c.removeNodeLabelInternal(id, label)
	})
	if closeErr != nil {
		return closeErr
	}
	if err == nil && ep != nil {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventNodeUpdate, EntityID: types.EntityID(id), Timestamp: c.now(), Priority: eventspkg.PriorityNormal})
	}
	return err
}

// removeNodeLabelInternal is the lock-free implementation of RemoveNodeLabel.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
func (c *Core) removeNodeLabelInternal(id types.NodeID, label string) error {
	if err := c.validateName(label); err != nil {
		return err
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return err
	}

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
	nextVersion, err := nextEntityVersion(prevVersion)
	if err != nil {
		return err
	}
	prevState := current.DeepCopy()

	copy := current.DeepCopy()
	copy.RemoveLabelTokenRaw(tok)
	copy.SetVersion(nextVersion)

	// Advance hash chain: PrevHash = current Hash (link new version back to current).
	prevHash := ""
	if ig := current.Integrity(); ig != nil {
		prevHash = ig.Hash
	}
	nodeLabels := c.nodeLabelsWithoutTokenUnlocked(current, tok)
	hash, err := integrity.ComputeNodeHashChecked(copy, nodeLabels)
	if err != nil {
		return fmt.Errorf("graph: compute node hash: %w", err)
	}
	copy.SetIntegrity(nodeIntegrityWithHash(current.Integrity(), hash, prevHash))

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

func (c *Core) nodeLabelsWithoutTokenUnlocked(node *types.Node, tok uint16) []string {
	count := node.LabelTokenCount()
	if count <= 1 {
		return nil
	}
	labels := make([]string, 0, count-1)
	for i := 0; i < count; i++ {
		labelTok := node.LabelTokenRawAt(i)
		if labelTok != tok {
			labels = append(labels, c.labels.Resolve(labelTok))
		}
	}
	return labels
}
