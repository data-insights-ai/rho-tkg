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

// =============================================================================
// Relationship — Import (caller-specified ID)
// =============================================================================

// Import creates a relationship with a caller-specified snowflake ID.
// Acquires c.mu.RLock for transaction isolation — blocked while a tx holds c.mu.Lock.
func (r *RelOps) Import(ctx context.Context, id types.RelID, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	c := r.c
	if err := c.checkWritable(); err != nil {
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
		rel, err = c.importRelWithIDInternal(ctx, id, typeName, startNode, endNode, props)
	})
	if closeErr != nil {
		return nil, closeErr
	}
	if rel != nil && ep != nil {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventRelCreate, EntityID: types.EntityID(rel.ID()), Timestamp: c.now(), Priority: eventspkg.PriorityHigh})
	}
	return rel, err
}

// importRelWithIDInternal is the lock-free implementation of RelOps.Import.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
func (c *Core) importRelWithIDInternal(ctx context.Context, id types.RelID, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if id == 0 {
		return nil, ErrZeroID
	}
	if id < 0 {
		return nil, ErrInvalidID
	}

	if startNode == nil || endNode == nil {
		return nil, ErrNilNode
	}

	if err := c.validateName(typeName); err != nil {
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
		return nil, fmt.Errorf("graph: relationship properties: %w", err)
	}

	startID := startNode.ID()
	endID := endNode.ID()
	if err := validateRelationshipEndpointIDs(startID, endID); err != nil {
		return nil, err
	}

	if startID == endID && !c.validation.AllowSelfLoops {
		return nil, ErrSelfLoop
	}

	// defer registry-token allocation until after the
	// cheap validation gates (self-loop, ID==0, nil endpoints), the
	// collision probe, AND the live-endpoint fetches. Every operational
	// failure path that can reject the import must run before we touch
	// the rel-type registry, so a rejected import never leaves a
	// permanent type registration behind.

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// Lock the caller-supplied relationship ID together with both endpoints.
	// Unlike generated creates, this path can reuse a previously deleted rel ID;
	// the rel lock serializes same-ID import/delete/update interleavings while
	// endpoint locks still prevent write-skew with concurrent DeleteNode.
	c.entityLocks.LockThree(id.SnowflakeID(), startID.SnowflakeID(), endID.SnowflakeID())
	defer c.entityLocks.UnlockThree(id.SnowflakeID(), startID.SnowflakeID(), endID.SnowflakeID())

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// Check for collision. Probe BEFORE allocating
	// the rel-type token so a duplicate import never pollutes the
	// registry. Probe must surface non-not-found errors instead of
	// silently treating them as absence — operational store failures
	// must not be hidden by the import path.
	if _, err := c.getCurrentRelationship(id); err == nil {
		return nil, storepkg.ErrRelExists
	} else if !errors.Is(err, storepkg.ErrRelNotFound) {
		return nil, fmt.Errorf("graph: rel-id collision probe: %w", err)
	}

	var fromHash, toHash string
	if c.constraints.Len() > 0 {
		// Fetch live endpoints under the endpoint locks. The
		// caller-supplied `startNode`/`endNode` pointers are advisory —
		// only their IDs are load-bearing for routing. Constraint checks
		// must use the current persisted state.
		liveStart, liveEnd, err := c.liveEndpointNodes(startID, endID)
		if err != nil {
			return nil, err
		}
		probe := c.newRelConstraintProbe(id, startID, endID, validFrom, validTo, createdAt)
		if err := c.checkTemporalConstraints(probe, liveStart, liveEnd); err != nil {
			return nil, err
		}
		fromHash = nodeIntegrityHash(liveStart)
		toHash = nodeIntegrityHash(liveEnd)
	} else {
		fromHash, toHash, err = c.liveEndpointHashes(startID, endID)
		if err != nil {
			return nil, err
		}
	}
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// only allocate the rel-type token now that every cheap,
	// operational, and temporal-constraint rejection gate has been cleared.
	// The type-token-allocate / build / persist / rollback sequence itself
	// (including the panic-safety net) is the shared kernel
	// (relationship_create_kernel.go) — BACKLOG 9l: this door used to
	// hand-roll a byte-for-byte copy of that bookkeeping, plus its own
	// inline duplicate of applyRelCreateTemporal's temporal-stamping logic.
	// relPersistImport is the one kernel persist mode Import needs that no
	// other create door does: a direct c.store.PutRelationship, never
	// putGeneratedRelationship/FreshGraphID, since id here may be a
	// previously-deleted (reused) caller-specified ID, not freshly minted.
	build := func(typeToken uint16) (*types.Relationship, *types.RelIntegrity, error) {
		r := types.NewRelationship(id, typeToken, startID, endID)
		if err := r.SetOwnedProperties(ps); err != nil {
			return nil, nil, fmt.Errorf("graph: relationship import properties: %w", err)
		}
		hash, err := integrity.ComputeRelHashChecked(r, typeName)
		if err != nil {
			return nil, nil, fmt.Errorf("graph: compute relationship hash: %w", err)
		}
		ig := &types.RelIntegrity{
			Hash:               hash,
			PrevHash:           "",
			AuthorID:           authorID,
			Signature:          sig,
			AuthorizedBy:       authorizedBy,
			AuthorizationLevel: authLevel,
		}
		ig.FromNodeHash = fromHash
		ig.ToNodeHash = toHash
		r.SetIntegrity(ig)
		c.applyRelCreateTemporal(r, validFrom, validTo, createdAt, txFromOverride)
		if err := checkCtx(ctx); err != nil {
			return nil, nil, err
		}
		return r, ig, nil
	}

	rel, err := c.createRelWithTypeRollback(ctx, typeName, relPersistImport, build)
	if rel != nil {
		c.opRelAdds.Add(1)
	}
	return rel, err
}
