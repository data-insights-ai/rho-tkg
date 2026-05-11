package core

import (
	"context"
	"errors"
	"fmt"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/integrity"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// =============================================================================
// Node — Add (Create / Import)
// =============================================================================

// AddWithContext creates a new node with the given labels and properties.
// Acquires c.mu.RLock (panic-safe) for transaction isolation — blocked
// while a tx holds c.mu.Lock.
func (n *NodeOps) AddWithContext(ctx context.Context, labels []string, props map[string]any) (*types.Node, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	var (
		node *types.Node
		err  error
	)
	ep, closeErr := c.runUnderRLock(func() {
		node, err = c.addNodeInternal(ctx, labels, props)
	})
	if closeErr != nil {
		return nil, closeErr
	}
	if err == nil {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventNodeCreate, EntityID: types.EntityID(node.ID()), Timestamp: c.now(), Priority: eventspkg.PriorityHigh})
	}
	return node, err
}

// addNodeInternal is the lock-free implementation of NodeOps.AddWithContext.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
func (c *Core) addNodeInternal(ctx context.Context, labels []string, props map[string]any) (*types.Node, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// Extract reserved provenance fields before validation so they are never
	// seen by PropertySlice.Set (which rejects the tkg_ prefix).
	authorID, sig, authorizedBy, authLevel, props, err := extractProvenance(props)
	if err != nil {
		return nil, err
	}

	// Extract reserved temporal fields (tkg_valid_from, tkg_valid_to, tkg_created_at).
	validFrom, validTo, createdAt, props, err := extractTemporal(props)
	if err != nil {
		return nil, err
	}

	if len(labels) == 0 {
		return nil, ErrNoLabels
	}

	// Validation limits.
	if len(labels) > c.validation.MaxLabelsPerNode {
		return nil, fmt.Errorf("%w: %d > %d", ErrTooManyLabels, len(labels), c.validation.MaxLabelsPerNode)
	}
	for _, label := range labels {
		if err := c.validateName(label); err != nil {
			return nil, err
		}
	}
	if err := c.validateProperties(props); err != nil {
		return nil, err
	}

	// Bulk-build properties first — fail fast before generating an ID.
	ps, err := types.NewPropertySlice(props)
	if err != nil {
		return nil, fmt.Errorf("graph: node properties: %w", err)
	}

	// Resolve labels to tokens.
	primaryToken, extraTokens, labelSnapshot, allocatedLabels, labelsLocked, err := c.getOrCreateLabelsWithSnapshot(labels)
	if err != nil {
		return nil, err
	}
	labelsFinished := !labelsLocked
	finishLabels := func(err error) error {
		if !labelsLocked {
			return err
		}
		labelsFinished = true
		return c.restoreNewLabelsOnError(labelSnapshot, allocatedLabels, err)
	}
	defer func() {
		if !labelsFinished {
			_ = c.restoreNewLabelsOnError(labelSnapshot, allocatedLabels, fmt.Errorf("panic during node create"))
		}
	}()

	id := c.nextNodeID()
	n := types.NewNode(id, primaryToken, extraTokens)
	if err := n.SetProperties(ps); err != nil {
		return nil, finishLabels(fmt.Errorf("graph: node properties: %w", err))
	}

	// Hash from canonical (deduplicated) labels, not raw user input.
	// NewNode deduplicates tokens; NodeLabels resolves the canonical set.
	canonicalLabels := c.nodeLabelsUnlocked(n)
	hash, err := integrity.ComputeNodeHashChecked(n, canonicalLabels)
	if err != nil {
		return nil, finishLabels(fmt.Errorf("graph: compute node hash: %w", err))
	}
	n.SetIntegrity(&types.NodeIntegrity{
		Hash:               hash,
		PrevHash:           "",
		AuthorID:           authorID,
		Signature:          sig,
		AuthorizedBy:       authorizedBy,
		AuthorizationLevel: authLevel,
	})

	// Set transaction time + merge caller-provided temporal metadata.
	// TxFrom/TxTo are NOT hashed — must be set AFTER hash computation.
	{
		txNow := c.now()
		ntm := n.Temporal()
		if ntm == nil {
			ntm = &types.TemporalMetadata{}
			n.SetTemporal(ntm)
		}
		ntm.TxFrom = txNow
		if validFrom != 0 {
			ntm.ValidFrom = validFrom
		}
		if validTo != 0 {
			ntm.ValidTo = validTo
		}
		if createdAt != 0 {
			ntm.CreatedAt = createdAt
		}
	}

	if err := checkCtx(ctx); err != nil {
		return nil, finishLabels(err)
	}

	if err := putGeneratedNode(c.store, n); err != nil {
		return nil, finishLabels(err)
	}
	if err := finishLabels(nil); err != nil {
		return n, err
	}

	c.opNodeAdds.Add(1)
	return n, nil
}

// Import creates a node with a caller-specified snowflake ID.
// Acquires c.mu.RLock (panic-safe) for transaction isolation — blocked
// while a tx holds c.mu.Lock.
func (n *NodeOps) Import(ctx context.Context, id types.NodeID, labels []string, props map[string]any) (*types.Node, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	var (
		node *types.Node
		err  error
	)
	ep, closeErr := c.runUnderRLock(func() {
		node, err = c.importNodeWithIDInternal(ctx, id, labels, props)
	})
	if closeErr != nil {
		return nil, closeErr
	}
	if err == nil {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventNodeCreate, EntityID: types.EntityID(id), Timestamp: c.now(), Priority: eventspkg.PriorityHigh})
	}
	return node, err
}

// importNodeWithIDInternal is the lock-free implementation of ImportNodeWithID.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
func (c *Core) importNodeWithIDInternal(ctx context.Context, id types.NodeID, labels []string, props map[string]any) (*types.Node, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	authorID, sig, authorizedBy, authLevel, props, err := extractProvenance(props)
	if err != nil {
		return nil, err
	}
	validFrom, validTo, createdAt, props, err := extractTemporal(props)
	if err != nil {
		return nil, err
	}

	if id == 0 {
		return nil, ErrZeroID
	}
	if id < 0 {
		return nil, ErrInvalidID
	}

	if len(labels) == 0 {
		return nil, ErrNoLabels
	}

	if len(labels) > c.validation.MaxLabelsPerNode {
		return nil, fmt.Errorf("%w: %d > %d", ErrTooManyLabels, len(labels), c.validation.MaxLabelsPerNode)
	}
	for _, label := range labels {
		if err := c.validateName(label); err != nil {
			return nil, err
		}
	}
	if err := c.validateProperties(props); err != nil {
		return nil, err
	}

	ps, err := types.NewPropertySlice(props)
	if err != nil {
		return nil, fmt.Errorf("graph: node properties: %w", err)
	}

	// Check for collision BEFORE allocating registry tokens (R4-F14).
	// Allocating tokens up-front pollutes the registry on duplicate-ID
	// rejection. Probe must surface non-not-found errors instead of
	// silently treating them as absence (R4-F15) — operational store
	// failures must not be hidden by the import path.
	if _, err := c.store.GetNode(id); err == nil {
		return nil, storepkg.ErrNodeExists
	} else if !errors.Is(err, storepkg.ErrNodeNotFound) {
		return nil, fmt.Errorf("graph: node-id collision probe: %w", err)
	}

	primaryToken, extraTokens, labelSnapshot, allocatedLabels, labelsLocked, err := c.getOrCreateLabelsWithSnapshot(labels)
	if err != nil {
		return nil, err
	}
	labelsFinished := !labelsLocked
	finishLabels := func(err error) error {
		if !labelsLocked {
			return err
		}
		labelsFinished = true
		return c.restoreNewLabelsOnError(labelSnapshot, allocatedLabels, err)
	}
	defer func() {
		if !labelsFinished {
			_ = c.restoreNewLabelsOnError(labelSnapshot, allocatedLabels, fmt.Errorf("panic during node import"))
		}
	}()

	n := types.NewNode(id, primaryToken, extraTokens)
	if err := n.SetProperties(ps); err != nil {
		return nil, finishLabels(fmt.Errorf("graph: node properties: %w", err))
	}

	canonicalLabels := c.nodeLabelsUnlocked(n)
	hash, err := integrity.ComputeNodeHashChecked(n, canonicalLabels)
	if err != nil {
		return nil, finishLabels(fmt.Errorf("graph: compute node hash: %w", err))
	}
	n.SetIntegrity(&types.NodeIntegrity{
		Hash:               hash,
		PrevHash:           "",
		AuthorID:           authorID,
		Signature:          sig,
		AuthorizedBy:       authorizedBy,
		AuthorizationLevel: authLevel,
	})

	txNow := c.now()
	tm := n.Temporal()
	if tm == nil {
		tm = &types.TemporalMetadata{}
		n.SetTemporal(tm)
	}
	tm.TxFrom = txNow
	if validFrom != 0 {
		tm.ValidFrom = validFrom
	}
	if validTo != 0 {
		tm.ValidTo = validTo
	}
	if createdAt != 0 {
		tm.CreatedAt = createdAt
	}

	if err := checkCtx(ctx); err != nil {
		return nil, finishLabels(err)
	}

	if err := c.store.PutNode(n); err != nil {
		return nil, finishLabels(err)
	}
	if err := finishLabels(nil); err != nil {
		return n, err
	}

	c.opNodeAdds.Add(1)
	return n, nil
}
