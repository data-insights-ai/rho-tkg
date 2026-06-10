package core

import (
	"context"
	"errors"
	"fmt"

	eventspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

const nonZeroRelTypeProbeToken uint16 = 1

func validateRelationshipEndpointIDs(startID, endID types.NodeID) error {
	if err := storepkg.ValidateNodeID(startID); err != nil {
		return err
	}
	if err := storepkg.ValidateNodeID(endID); err != nil {
		return err
	}
	return nil
}

func (c *Core) restoreNewRelTypeCreateOnError(snapshot []string, allocated bool, typeName string, err error, cleanup func() error, partialLive *bool) error {
	if err != nil && cleanup != nil {
		if cleanupErr := runRollbackCleanup(cleanup); cleanupErr != nil && !errors.Is(cleanupErr, storepkg.ErrRelNotFound) {
			if partialLive != nil {
				*partialLive = true
			}
			err = fmt.Errorf("%w; additionally failed to remove partial relationship for reltype %q after create failure: %v", err, typeName, cleanupErr)
			if allocated {
				if persistErr := c.persistRegistries(); persistErr != nil {
					err = fmt.Errorf("%w; additionally failed to persist retained reltype registry after partial relationship cleanup failure: %v", err, persistErr)
				}
				c.registryMu.Unlock()
				return err
			}
		}
	}
	return c.restoreNewRelTypeOnError(snapshot, allocated, typeName, err)
}

func (c *Core) deletePartialRelationshipForRollback(r *types.Relationship) error {
	if r == nil {
		return storepkg.ErrRelNotFound
	}
	return c.store.DeleteRelationship(r.ID())
}

// =============================================================================
// Relationship — Add (Create / IfAbsent / ByID)
//
// All three doors share the create kernel in relationship_create_kernel.go;
// each door contributes only its input form (node pointers vs IDs) and, for
// IfAbsent, the under-lock existence probe.
// =============================================================================

// Add creates a new directed relationship between two nodes.
// Acquires c.mu.RLock for transaction isolation — blocked while a tx holds c.mu.Lock.
func (r *RelOps) Add(ctx context.Context, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	var (
		rel *types.Relationship
		err error
	)
	ep, closeErr := c.runUnderRLock(func() {
		rel, err = c.addRelationshipInternal(ctx, typeName, startNode, endNode, props)
	})
	if closeErr != nil {
		return nil, closeErr
	}
	if rel != nil && ep != nil {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventRelCreate, EntityID: types.EntityID(rel.ID()), Timestamp: c.now(), Priority: eventspkg.PriorityHigh})
	}
	return rel, err
}

// addRelationshipInternal is the lock-free implementation of RelOps.Add.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
func (c *Core) addRelationshipInternal(ctx context.Context, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	if startNode == nil || endNode == nil {
		return nil, ErrNilNode
	}
	return c.createRelationshipLocked(ctx, typeName, startNode.ID(), endNode.ID(), props)
}

// AddByID creates a relationship using endpoint snowflake IDs.
// It has the same live-endpoint verification, endpoint-hash capture, and
// constraint enforcement semantics as RelOps.Add; it only changes
// the input form so callers do not need to carry endpoint node objects.
func (r *RelOps) AddByID(ctx context.Context, typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	var (
		rel *types.Relationship
		err error
	)
	ep, closeErr := c.runUnderRLock(func() {
		rel, err = c.addRelationshipByIDInternal(ctx, typeName, startID, endID, props)
	})
	if closeErr != nil {
		return nil, closeErr
	}
	if rel != nil && ep != nil {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventRelCreate, EntityID: types.EntityID(rel.ID()), Timestamp: c.now(), Priority: eventspkg.PriorityHigh})
	}
	return rel, err
}

// addRelationshipByIDInternal is the lock-free implementation of RelOps.AddByID.
// Unlike addRelationshipInternal, it does NOT require pre-fetched endpoint nodes.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
func (c *Core) addRelationshipByIDInternal(ctx context.Context, typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	return c.createRelationshipLocked(ctx, typeName, startID, endID, props)
}

// createRelationshipLocked is the shared standalone create door: validate,
// take the endpoint locks, run the endpoint-hash/constraint ladder, then
// hand off to the persistence kernel. Callers must hold c.mu.RLock
// (standalone) or c.mu.Lock (tx/batch).
func (c *Core) createRelationshipLocked(ctx context.Context, typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, error) {
	prep, err := c.prepareRelCreate(typeName, props, startID, endID)
	if err != nil {
		return nil, err
	}

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// Lock both endpoints to prevent write-skew with concurrent DeleteNode.
	// Lock ordering: ascending shard index — deadlock-free.
	c.entityLocks.LockTwo(startID.SnowflakeID(), endID.SnowflakeID())
	defer c.entityLocks.UnlockTwo(startID.SnowflakeID(), endID.SnowflakeID())

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	id := c.nextRelID()
	fromHash, toHash, useEndpointHashWrite, err := c.relEndpointHashLadder(id, startID, endID, prep.validFrom, prep.validTo, prep.createdAt)
	if err != nil {
		return nil, err
	}
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// R5-F6: the kernel allocates the rel-type token only now — after the
	// endpoint locks are held and every endpoint-fetch failure path has
	// passed — so an operational failure does not leave a permanent
	// rel-type registration.
	spec := relCreateSpec{relCreatePrep: prep, id: id, fromHash: fromHash, toHash: toHash}
	rel, err := c.createRelWithTypeRollback(typeName, useEndpointHashWrite, c.buildRelFromSpec(ctx, spec))
	if rel != nil {
		c.opRelAdds.Add(1)
	}
	return rel, err
}

// AddByIDIfAbsent atomically creates a relationship using
// endpoint snowflake IDs only if no relationship of the same type between the same
// endpoints already exists. Returns (rel, created, err) where created is true if a
// new relationship was created, false if an existing one was returned.
//
// The existence check and creation are serialized under entity locks, preventing
// the TOCTOU race inherent in separate check-then-create calls.
//
// Endpoint and constraint behaviour matches AddByID.
func (r *RelOps) AddByIDIfAbsent(ctx context.Context, typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, bool, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, false, err
	}
	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}
	var (
		rel     *types.Relationship
		created bool
		err     error
	)
	ep, closeErr := c.runUnderRLock(func() {
		rel, created, err = c.addRelationshipByIDIfAbsentInternal(ctx, typeName, startID, endID, props)
	})
	if closeErr != nil {
		return nil, false, closeErr
	}
	if rel != nil && created && ep != nil {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventRelCreate, EntityID: types.EntityID(rel.ID()), Timestamp: c.now(), Priority: eventspkg.PriorityHigh})
	}
	return rel, created, err
}

// addRelationshipByIDIfAbsentInternal is the lock-free implementation of
// AddRelationshipByIDIfAbsent. Under entity locks it checks for an
// existing relationship before creating, making the operation atomic.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
func (c *Core) addRelationshipByIDIfAbsentInternal(ctx context.Context, typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, bool, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}

	prep, err := c.prepareRelCreate(typeName, props, startID, endID)
	if err != nil {
		return nil, false, err
	}

	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}

	// Lock both endpoints — serializes with concurrent Add/Delete on same endpoints.
	c.entityLocks.LockTwo(startID.SnowflakeID(), endID.SnowflakeID())
	defer c.entityLocks.UnlockTwo(startID.SnowflakeID(), endID.SnowflakeID())

	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}

	// R5-F6: probe the registry for an existing token via Lookup
	// (zero side effect) before committing to GetOrCreate. If the type
	// was never registered, no relationship of this type can exist for
	// any endpoint pair, so the absence check is vacuous and we can
	// skip OutgoingRelationships entirely. This avoids polluting the
	// rel-type registry when a "duplicate-relationship" check that
	// would have hit the store-failure path returns early.
	if existingTok, ok := c.lookupRelTypeLocked(typeName); ok {
		existing, err := c.store.OutgoingRelationships(startID, existingTok)
		if err != nil {
			return nil, false, fmt.Errorf("graph: check existing relationships: %w", err)
		}
		if !c.storeRowsTrust {
			if err := c.validateOutgoingRelationshipRows(startID, existingTok, existing); err != nil {
				return nil, false, fmt.Errorf("graph: check existing relationships: %w", err)
			}
		}
		if err := checkCtx(ctx); err != nil {
			return nil, false, err
		}
		for _, r := range existing {
			if r.EndNodeID() == endID {
				if !c.storeRowsTrust {
					return r.DeepCopy(), false, nil
				}
				return r, false, nil
			}
		}
	}

	id := c.nextRelID()
	fromHash, toHash, useEndpointHashWrite, err := c.relEndpointHashLadder(id, startID, endID, prep.validFrom, prep.validTo, prep.createdAt)
	if err != nil {
		return nil, false, err
	}
	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}

	// Not found — the kernel allocates the token now and creates, rolling
	// back a newly created type token if the final store write fails.
	spec := relCreateSpec{relCreatePrep: prep, id: id, fromHash: fromHash, toHash: toHash}
	rel, err := c.createRelWithTypeRollback(typeName, useEndpointHashWrite, c.buildRelFromSpec(ctx, spec))
	if rel != nil {
		c.opRelAdds.Add(1)
		return rel, true, err
	}
	return nil, false, err
}

func (c *Core) newRelConstraintProbe(id types.RelID, startID, endID types.NodeID, validFrom, validTo, createdAt types.Instant) *types.Relationship {
	r := types.NewRelationship(id, nonZeroRelTypeProbeToken, startID, endID)
	c.applyRelCreateTemporal(r, validFrom, validTo, createdAt)
	return r
}

func (c *Core) applyRelCreateTemporal(r *types.Relationship, validFrom, validTo, createdAt types.Instant) {
	rtm := r.Temporal()
	if rtm == nil {
		rtm = &types.TemporalMetadata{}
		r.SetTemporal(rtm)
	}
	rtm.TxFrom = c.now()
	if validFrom != 0 {
		rtm.ValidFrom = validFrom
	}
	if validTo != 0 {
		rtm.ValidTo = validTo
	}
	if createdAt != 0 {
		rtm.CreatedAt = createdAt
	}
}

func (c *Core) liveEndpointNodes(startID, endID types.NodeID) (*types.Node, *types.Node, error) {
	liveStart, err := c.getCurrentNode(startID)
	if err != nil {
		return nil, nil, fmt.Errorf("graph: live start-node fetch under endpoint lock: %w", err)
	}
	if startID == endID {
		return liveStart, liveStart, nil
	}
	liveEnd, err := c.getCurrentNode(endID)
	if err != nil {
		return nil, nil, fmt.Errorf("graph: live end-node fetch under endpoint lock: %w", err)
	}
	return liveStart, liveEnd, nil
}

func (c *Core) liveEndpointHashes(startID, endID types.NodeID) (string, string, error) {
	if c.endpointHash != nil {
		fromHash, toHash, err := c.endpointHash.EndpointIntegrityHashes(startID, endID)
		if err != nil {
			return "", "", fmt.Errorf("graph: live endpoint integrity fetch under endpoint lock: %w", err)
		}
		if startID == endID {
			toHash = fromHash
		}
		return fromHash, toHash, nil
	}
	if c.nodeHash != nil {
		fromHash, err := c.nodeHash.NodeIntegrityHash(startID)
		if err != nil {
			return "", "", fmt.Errorf("graph: live start-node integrity fetch under endpoint lock: %w", err)
		}
		if startID == endID {
			return fromHash, fromHash, nil
		}
		toHash, err := c.nodeHash.NodeIntegrityHash(endID)
		if err != nil {
			return "", "", fmt.Errorf("graph: live end-node integrity fetch under endpoint lock: %w", err)
		}
		return fromHash, toHash, nil
	}

	liveStart, liveEnd, err := c.liveEndpointNodes(startID, endID)
	if err != nil {
		return "", "", err
	}
	return nodeIntegrityHash(liveStart), nodeIntegrityHash(liveEnd), nil
}

func nodeIntegrityHash(n *types.Node) string {
	if ig := n.Integrity(); ig != nil {
		return ig.Hash
	}
	return ""
}
