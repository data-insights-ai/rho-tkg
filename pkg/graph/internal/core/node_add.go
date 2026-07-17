package core

import (
	"context"
	"errors"
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	eventspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/integrity"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func (c *Core) restoreNewLabelsCreateOnError(snapshot, allocated []string, err error, cleanup func() error, partialLive *bool) error {
	if err != nil && cleanup != nil {
		if cleanupErr := runRollbackCleanup(cleanup); cleanupErr != nil && !errors.Is(cleanupErr, storepkg.ErrNodeNotFound) {
			if partialLive != nil {
				*partialLive = true
			}
			err = fmt.Errorf("%w; additionally failed to remove partial node after create failure: %v", err, cleanupErr)
			if len(allocated) > 0 {
				if persistErr := c.persistRegistries(); persistErr != nil {
					err = fmt.Errorf("%w; additionally failed to persist retained label registry after partial node cleanup failure: %v", err, persistErr)
				}
				c.registryMu.Unlock()
				return err
			}
		}
	}
	if len(allocated) == 0 {
		return err
	}
	return c.restoreNewLabelsOnError(snapshot, allocated, err)
}

func (c *Core) deletePartialNodeForRollback(n *types.Node) error {
	if n == nil {
		return storepkg.ErrNodeNotFound
	}
	return c.store.DeleteNode(n.ID())
}

// =============================================================================
// Node — Add (Create / Import)
// =============================================================================

// Add creates a new node with the given labels and properties.
// Acquires c.mu.RLock (panic-safe) for transaction isolation — blocked
// while a tx holds c.mu.Lock.
func (n *NodeOps) Add(ctx context.Context, labels []string, props map[string]any) (*types.Node, error) {
	c := n.c
	if err := c.checkWritable(); err != nil {
		return nil, err
	}
	if err := checkCtx(ctx); err != nil {
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
	if node != nil && ep != nil {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventNodeCreate, EntityID: types.EntityID(node.ID()), Timestamp: c.now(), Priority: eventspkg.PriorityHigh})
	}
	return node, err
}

// AddWithTx is the privileged transaction-time backfill door. It creates a node
// exactly like Add but stamps the caller-supplied txFrom as the node's TxFrom
// (its Erkenntniszeit / knowledge time) instead of the system clock, so a
// re-ingest can reproduce a documented historical knowledge state addressable
// via AS OF SYSTEM TIME (§4.1). Requires the graph to be opened with
// Config.AllowTxBackfill — otherwise ErrTxBackfillDisabled. txFrom must be a
// positive instant (ErrInvalidTxFrom otherwise) and overrides any tkg_tx_from
// in props. Backfill affects TxFrom only; set tkg_valid_from / tkg_created_at
// in props to backdate world- and creation-time too.
func (n *NodeOps) AddWithTx(ctx context.Context, labels []string, props map[string]any, txFrom types.Instant) (*types.Node, error) {
	if txFrom <= 0 {
		return nil, ErrInvalidTxFrom
	}
	return n.Add(ctx, labels, withBackfillTxFrom(props, txFrom))
}

// addNodeInternal is the lock-free implementation of NodeOps.Add.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
//
// heldStripes lists value-lock stripes the caller ALREADY holds (GetOrCreateByKey
// holds the keyed value's stripe across the create); the unique kernel skips
// re-locking those to avoid a non-reentrant self-deadlock, still checking their
// values under the caller's held lock. Standalone / tx callers pass none.
func (c *Core) addNodeInternal(ctx context.Context, labels []string, props map[string]any, heldStripes ...uint8) (*types.Node, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if err := c.validateNodeCreateLabels(labels); err != nil {
		return nil, err
	}

	authorID, sig, authorizedBy, authLevel, props, err := extractProvenance(props)
	if err != nil {
		return nil, err
	}

	validFrom, validTo, createdAt, txFromOverride, props, err := extractTemporal(props)
	if err != nil {
		return nil, err
	}
	txFromOverride, err = c.resolveBackfillTxFrom(txFromOverride)
	if err != nil {
		return nil, err
	}
	if err := c.validateProperties(props); err != nil {
		return nil, err
	}

	// Bulk-build properties first — fail fast before generating an ID.
	ps, err := types.NewOwnedPropertySlice(props)
	if err != nil {
		return nil, fmt.Errorf("graph: node properties: %w", err)
	}

	// Resolve labels to tokens.
	primaryToken, extraTokens, labelSnapshot, allocatedLabels, labelsLocked, err := c.getOrCreateLabelsWithSnapshot(labels)
	if err != nil {
		return nil, err
	}
	createFinished := false
	finishLabels := func(err error) error {
		createFinished = true
		if !labelsLocked {
			return err
		}
		return c.restoreNewLabelsOnError(labelSnapshot, allocatedLabels, err)
	}
	var n *types.Node
	finishNodeCreateError := func(err error) (error, bool) {
		createFinished = true
		if !labelsLocked {
			partialLive := false
			if cleanupErr := runRollbackCleanup(func() error {
				return c.deletePartialNodeForRollback(n)
			}); cleanupErr != nil && !errors.Is(cleanupErr, storepkg.ErrNodeNotFound) {
				partialLive = true
				err = fmt.Errorf("%w; additionally failed to remove partial node after create failure: %v", err, cleanupErr)
			}
			return err, partialLive
		}
		partialLive := false
		err = c.restoreNewLabelsCreateOnError(labelSnapshot, allocatedLabels, err, func() error {
			return c.deletePartialNodeForRollback(n)
		}, &partialLive)
		return err, partialLive
	}
	defer func() {
		if !createFinished {
			_ = c.restoreNewLabelsCreateOnError(labelSnapshot, allocatedLabels, fmt.Errorf("panic during node create"), func() error {
				return c.deletePartialNodeForRollback(n)
			}, nil)
		}
	}()

	id := c.nextNodeID()
	n = types.NewNode(id, primaryToken, extraTokens)
	if err := n.SetOwnedProperties(ps); err != nil {
		return nil, finishLabels(fmt.Errorf("graph: node properties: %w", err))
	}

	// Hash from canonical (deduplicated) labels, not raw user input.
	// NewNode deduplicates tokens; NodeLabels resolves the canonical set.
	canonicalLabels := labels
	if len(labels) != 1 {
		canonicalLabels = c.nodeLabelsUnlocked(n)
	}
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
	// A privileged backfill (txFromOverride != 0, gated above) stamps the
	// caller's transaction time instead of the system clock (§4.1).
	{
		txNow := c.now()
		if txFromOverride != 0 {
			txNow = txFromOverride
		}
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

	// Unique-constraint enforcement (standalone create door). Hold the value
	// stripe(s) across the index check + store write so concurrent same-value
	// creates serialize to exactly one winner.
	uniqueRelease, uniqueErr := c.enforceUniqueForNodeHeld(n, nil, id, heldStripes)
	if uniqueErr != nil {
		return nil, finishLabels(uniqueErr)
	}
	defer uniqueRelease()

	if err := c.putGeneratedNode(n); err != nil {
		err, partialLive := finishNodeCreateError(err)
		if partialLive {
			c.opNodeAdds.Add(1)
			return n, err
		}
		return nil, err
	}
	if err := finishLabels(nil); err != nil {
		c.opNodeAdds.Add(1)
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
	if err := c.checkWritable(); err != nil {
		return nil, err
	}
	if err := checkCtx(ctx); err != nil {
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
	if node != nil && ep != nil {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventNodeCreate, EntityID: types.EntityID(node.ID()), Timestamp: c.now(), Priority: eventspkg.PriorityHigh})
	}
	return node, err
}

// AddByIDIfAbsent is the idempotent counterpart of Import: if a node with
// the supplied id already exists, the existing node is returned with
// created=false and no error; otherwise a new node is created exactly as
// Import would create it. Mirrors the shape of RelOps.AddByIDIfAbsent so
// Node and Rel sub-APIs share the same "ensure-exists" verb.
//
// Returns ErrZeroID / ErrInvalidID for invalid IDs (same as Import).
// Acquires c.mu.RLock (panic-safe) for transaction isolation — blocked
// while a tx holds c.mu.Lock.
func (n *NodeOps) AddByIDIfAbsent(ctx context.Context, id types.NodeID, labels []string, props map[string]any) (*types.Node, bool, error) {
	c := n.c
	if err := c.checkWritable(); err != nil {
		return nil, false, err
	}
	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}
	var (
		node    *types.Node
		created bool
		err     error
	)
	ep, closeErr := c.runUnderRLock(func() {
		node, created, err = c.addNodeByIDIfAbsentInternal(ctx, id, labels, props)
	})
	if closeErr != nil {
		return nil, false, closeErr
	}
	if created && node != nil && ep != nil {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventNodeCreate, EntityID: types.EntityID(node.ID()), Timestamp: c.now(), Priority: eventspkg.PriorityHigh})
	}
	return node, created, err
}

// addNodeByIDIfAbsentInternal is the lock-free implementation of
// AddByIDIfAbsent. Callers must hold c.mu.RLock (standalone) or
// c.mu.Lock (tx/batch). Differs from importNodeWithIDInternal only in
// the existence-check branch: instead of returning ErrNodeExists, we
// return the existing node with created=false.
func (c *Core) addNodeByIDIfAbsentInternal(ctx context.Context, id types.NodeID, labels []string, props map[string]any) (*types.Node, bool, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}
	if id == 0 {
		return nil, false, ErrZeroID
	}
	if id < 0 {
		return nil, false, ErrInvalidID
	}
	// Fast pre-flight existence check (lock-free read). The actual
	// importNodeWithIDInternal will re-check under entity lock and return
	// ErrNodeExists if a racing writer beat us — translate that into the
	// "found, not created" shape for callers.
	if existing, err := c.getCurrentNode(id); err == nil {
		c.opNodeReads.Add(1)
		return existing, false, nil
	} else if !errors.Is(err, storepkg.ErrNodeNotFound) {
		return nil, false, fmt.Errorf("graph: node-id collision probe: %w", err)
	}
	node, err := c.importNodeWithIDInternal(ctx, id, labels, props)
	if err != nil {
		if errors.Is(err, storepkg.ErrNodeExists) {
			// Race: another writer created the node between our pre-flight
			// and the import's internal collision check. Re-read.
			existing, getErr := c.getCurrentNode(id)
			if getErr != nil {
				return nil, false, fmt.Errorf("graph: AddByIDIfAbsent race re-read: %w", getErr)
			}
			c.opNodeReads.Add(1)
			return existing, false, nil
		}
		return nil, false, err
	}
	return node, true, nil
}

// importNodeWithIDInternal is the lock-free implementation of ImportNodeWithID.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
func (c *Core) importNodeWithIDInternal(ctx context.Context, id types.NodeID, labels []string, props map[string]any) (*types.Node, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if id == 0 {
		return nil, ErrZeroID
	}
	if id < 0 {
		return nil, ErrInvalidID
	}

	if err := c.validateNodeCreateLabels(labels); err != nil {
		return nil, err
	}
	authorID, sig, authorizedBy, authLevel, props, err := extractProvenance(props)
	if err != nil {
		return nil, err
	}
	validFrom, validTo, createdAt, txFromOverride, props, err := extractTemporal(props)
	if err != nil {
		return nil, err
	}
	txFromOverride, err = c.resolveBackfillTxFrom(txFromOverride)
	if err != nil {
		return nil, err
	}
	if err := c.validateProperties(props); err != nil {
		return nil, err
	}

	ps, err := types.NewOwnedPropertySlice(props)
	if err != nil {
		return nil, fmt.Errorf("graph: node properties: %w", err)
	}

	// Caller-supplied node IDs can be reused after deletion, unlike generated
	// creates. Hold the node entity lock across the collision probe and final
	// store write so same-ID import/delete/update interleavings serialize.
	c.entityLocks.LockEntity(id.SnowflakeID())
	defer c.entityLocks.UnlockEntity(id.SnowflakeID())

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// Check for collision BEFORE allocating registry tokens.
	// Allocating tokens up-front pollutes the registry on duplicate-ID
	// rejection. Probe must surface non-not-found errors instead of
	// silently treating them as absence — operational store
	// failures must not be hidden by the import path.
	if _, err := c.getCurrentNode(id); err == nil {
		return nil, storepkg.ErrNodeExists
	} else if !errors.Is(err, storepkg.ErrNodeNotFound) {
		return nil, fmt.Errorf("graph: node-id collision probe: %w", err)
	}

	primaryToken, extraTokens, labelSnapshot, allocatedLabels, labelsLocked, err := c.getOrCreateLabelsWithSnapshot(labels)
	if err != nil {
		return nil, err
	}
	createFinished := false
	finishLabels := func(err error) error {
		createFinished = true
		if !labelsLocked {
			return err
		}
		return c.restoreNewLabelsOnError(labelSnapshot, allocatedLabels, err)
	}
	var n *types.Node
	finishNodeCreateError := func(err error) (error, bool) {
		createFinished = true
		if !labelsLocked {
			partialLive := false
			if cleanupErr := runRollbackCleanup(func() error {
				return c.deletePartialNodeForRollback(n)
			}); cleanupErr != nil && !errors.Is(cleanupErr, storepkg.ErrNodeNotFound) {
				partialLive = true
				err = fmt.Errorf("%w; additionally failed to remove partial node after create failure: %v", err, cleanupErr)
			}
			return err, partialLive
		}
		partialLive := false
		err = c.restoreNewLabelsCreateOnError(labelSnapshot, allocatedLabels, err, func() error {
			return c.deletePartialNodeForRollback(n)
		}, &partialLive)
		return err, partialLive
	}
	defer func() {
		if !createFinished {
			_ = c.restoreNewLabelsCreateOnError(labelSnapshot, allocatedLabels, fmt.Errorf("panic during node import"), func() error {
				return c.deletePartialNodeForRollback(n)
			}, nil)
		}
	}()

	n = types.NewNode(id, primaryToken, extraTokens)
	if err := n.SetOwnedProperties(ps); err != nil {
		return nil, finishLabels(fmt.Errorf("graph: node properties: %w", err))
	}

	canonicalLabels := labels
	if len(labels) != 1 {
		canonicalLabels = c.nodeLabelsUnlocked(n)
	}
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
	if txFromOverride != 0 {
		txNow = txFromOverride
	}
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

	// Unique-constraint enforcement (standalone import / AddByIDIfAbsent create
	// door). The entity lock is already held above; take value stripe(s) after
	// it (entity -> value order) across the check + PutNode.
	uniqueRelease, uniqueErr := c.enforceUniqueForNode(n, nil, id)
	if uniqueErr != nil {
		return nil, finishLabels(uniqueErr)
	}
	defer uniqueRelease()

	if err := c.store.PutNode(n); err != nil {
		err, partialLive := finishNodeCreateError(err)
		if partialLive {
			c.opNodeAdds.Add(1)
			return n, err
		}
		return nil, err
	}
	if err := finishLabels(nil); err != nil {
		c.opNodeAdds.Add(1)
		return n, err
	}

	c.opNodeAdds.Add(1)
	return n, nil
}
