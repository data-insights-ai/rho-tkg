package core

import (
	"context"
	"errors"
	"fmt"

	eventspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/integrity"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Cross-machine (foreign-endpoint) relationship creation — ADR-0010.
//
// A relationship whose END node lives on a slot owned by ANOTHER machine cannot
// go through the normal create doors: the endpoint-hash ladder resolves both
// endpoints from the local store, and a foreign end fails closed with
// ErrSlotNotLocal. AddByIDForeignEnd is the door for that case. The caller
// (which has RPC'd the owning machine — a sigma-tkgd concern) supplies the
// foreign end as a store.ForeignEndpoint descriptor carrying the attested
// tkg_to_hash and its provenance; rho-tkg locks + hashes only the LOCAL start,
// stamps the attested to-hash, and persists via the partitioned store's
// foreign-endpoint capability. The rel is minted in the (local) start's slot per
// ADR-0007 §2, so a foreign START is never seen here — it is executed on the
// start's own machine as an ordinary local-start create (ADR-0010 §3.4).
var (
	// ErrForeignEndpointUnsupported is returned when AddByIDForeignEnd is called
	// on a store with no foreign partition (any non-sharded backend). Cross-
	// machine edges require a partitioned store.
	ErrForeignEndpointUnsupported = errors.New("graph: cross-machine (foreign-endpoint) relationships require a partitioned store")
	// ErrForeignEndpointConstraint is returned when temporal constraints are
	// configured: they need the LIVE foreign end node, which is not locally
	// available, so they cannot be enforced on a cross-machine edge (ADR-0010
	// §4.1). The create fails closed rather than silently skipping the check.
	ErrForeignEndpointConstraint = errors.New("graph: temporal constraints cannot be enforced on a cross-machine edge")
)

// AddByIDForeignEnd creates startID -typeName-> foreignEnd where foreignEnd's
// node lives on a foreign partition (ADR-0010). The relationship is minted in
// the start's slot and lands entirely on this machine; tkg_from_hash is captured
// from the LOCAL start under its entity lock, tkg_to_hash is taken from
// foreignEnd.Hash (NOT fetched locally), and the foreign end's existence is
// ATTESTED by the caller, not verified here.
func (r *RelOps) AddByIDForeignEnd(ctx context.Context, typeName string, startID types.NodeID, foreignEnd storepkg.ForeignEndpoint, props map[string]any) (*types.Relationship, error) {
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
		rel, err = c.addRelationshipByIDForeignEndInternal(ctx, typeName, startID, foreignEnd, props)
	})
	if closeErr != nil {
		return nil, closeErr
	}
	if rel != nil && ep != nil {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventRelCreate, EntityID: types.EntityID(rel.ID()), Timestamp: c.now(), Priority: eventspkg.PriorityHigh})
	}
	return rel, err
}

// addRelationshipByIDForeignEndInternal is the lock-free implementation.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
func (c *Core) addRelationshipByIDForeignEndInternal(ctx context.Context, typeName string, startID types.NodeID, fe storepkg.ForeignEndpoint, props map[string]any) (*types.Relationship, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	if c.foreignEndpointRel == nil {
		return nil, ErrForeignEndpointUnsupported
	}
	if err := fe.Validate(); err != nil {
		return nil, err
	}
	// Temporal constraints need the live foreign end node (unavailable here).
	if c.constraints.Len() > 0 {
		return nil, ErrForeignEndpointConstraint
	}

	// Reuse the canonical validation order (type name, provenance/temporal
	// extraction, property validation, endpoint-ID validation, self-loop policy).
	prep, err := c.prepareRelCreate(typeName, props, startID, fe.NodeID)
	if err != nil {
		return nil, err
	}
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// Lock ONLY the local start endpoint — the foreign end lives on another
	// machine and cannot (and need not) be locked here; its hash is attested.
	c.entityLocks.LockEntity(startID.SnowflakeID())
	defer c.entityLocks.UnlockEntity(startID.SnowflakeID())

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// tkg_from_hash: the local start under its lock (full endpoint-hash ladder on the local
	// half). This also validates that the local start exists.
	fromHash, err := c.localEndpointHash(startID)
	if err != nil {
		return nil, err
	}
	toHash := fe.Hash

	id := c.nextRelID()
	spec := relCreateSpec{relCreatePrep: prep, id: id, fromHash: fromHash, toHash: toHash}
	rel, err := c.createRelWithTypeRollback(ctx, typeName, relPersistForeignEnd, c.buildRelFromSpec(ctx, spec))
	if rel != nil {
		c.opRelAdds.Add(1)
	}
	return rel, err
}

// RecordForeignIncoming records a cross-machine edge as an incoming half-edge
// STUB on the END node's machine (ADR-0010 Model A) so IncomingRelationships(END)
// is locally complete. The caller (sigma), having created the authoritative edge
// on the START node's machine, extracts the edge fields into a ForeignIncomingEdge
// and RPCs them here. The type is re-tokenized in THIS machine's registry; the
// content hash is RECOMPUTED from the same inputs, so the stub's tkg_hash is
// byte-identical to the authoritative edge's. Requires a partitioned store
// (ErrForeignEndpointUnsupported otherwise). Idempotent — a duplicate stub is a
// no-op (the store returns ErrRelExists, tolerated by the replica apply path).
func (r *RelOps) RecordForeignIncoming(ctx context.Context, edge storepkg.ForeignIncomingEdge) error {
	c := r.c
	if err := c.checkWritable(); err != nil {
		return err
	}
	if err := checkCtx(ctx); err != nil {
		return err
	}
	var (
		rel *types.Relationship
		err error
	)
	ep, closeErr := c.runUnderRLock(func() {
		rel, err = c.recordForeignIncomingInternal(ctx, edge)
	})
	if closeErr != nil {
		return closeErr
	}
	if rel != nil && ep != nil {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventRelCreate, EntityID: types.EntityID(rel.ID()), Timestamp: c.now(), Priority: eventspkg.PriorityHigh})
	}
	return err
}

// recordForeignIncomingInternal is the lock-free implementation. Callers must
// hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
func (c *Core) recordForeignIncomingInternal(ctx context.Context, edge storepkg.ForeignIncomingEdge) (*types.Relationship, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	if c.foreignIncomingRel == nil {
		return nil, ErrForeignEndpointUnsupported
	}
	if err := edge.Validate(); err != nil {
		return nil, err
	}
	// RecordForeignIncoming is a DIRECTLY CALLABLE public door (unlike
	// apply_record.go's replica-apply path, which has a documented exemption
	// because it reproduces a PRIMARY's already-validated state verbatim) — a
	// caller on a machine with looser ValidationLimits could otherwise inject
	// a type name or property set exceeding THIS machine's configured caps.
	// Mirror prepareRelCreate's validation order (validateName, then
	// validateProperties, before the property slice is built).
	if err := c.validateName(edge.TypeName); err != nil {
		return nil, err
	}
	if err := c.validateProperties(edge.Properties); err != nil {
		return nil, err
	}
	ps, err := types.NewOwnedPropertySlice(edge.Properties)
	if err != nil {
		return nil, fmt.Errorf("graph: foreign-incoming properties: %w", err)
	}

	// Lock the LOCAL end node (it hosts the stub); the foreign start is elsewhere.
	c.entityLocks.LockEntity(edge.EndID.SnowflakeID())
	defer c.entityLocks.UnlockEntity(edge.EndID.SnowflakeID())
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	build := func(typeToken uint16) (*types.Relationship, *types.RelIntegrity, error) {
		stub := types.NewRelationship(edge.RelID, typeToken, edge.StartID, edge.EndID)
		if err := stub.SetOwnedProperties(ps); err != nil {
			return nil, nil, fmt.Errorf("graph: foreign-incoming properties: %w", err)
		}
		// Recompute the content hash from the same inputs (keyed on the type NAME,
		// not the local token) → byte-identical to the authoritative edge's tkg_hash.
		hash, err := integrity.ComputeRelHashChecked(stub, edge.TypeName)
		if err != nil {
			return nil, nil, fmt.Errorf("graph: compute foreign-incoming hash: %w", err)
		}
		ig := &types.RelIntegrity{
			Hash:         hash,
			PrevHash:     edge.PrevHash,
			FromNodeHash: edge.FromHash,
			ToNodeHash:   edge.ToHash,
		}
		stub.SetIntegrity(ig)
		// Temporal carried verbatim from the authoritative edge (edge.TxFrom is
		// always non-zero, so it overrides the fresh-clock default).
		c.applyRelCreateTemporal(stub, edge.ValidFrom, edge.ValidTo, edge.CreatedAt, edge.TxFrom)
		stub.SetVersion(edge.Version)
		if err := checkCtx(ctx); err != nil {
			return nil, nil, err
		}
		return stub, ig, nil
	}

	rel, err := c.createRelWithTypeRollback(ctx, edge.TypeName, relPersistForeignIncoming, build)
	if rel != nil {
		c.opRelAdds.Add(1)
	}
	return rel, err
}

// localEndpointHash returns a LOCAL node's integrity hash under its entity lock,
// erroring if the node is absent (a foreign-endpoint create still requires a
// live LOCAL start). The partitioned store that backs this door does not
// implement NodeIntegrityHashCapability, so it always reads the full node — a
// single fetch per create, negligible against the store write.
func (c *Core) localEndpointHash(id types.NodeID) (string, error) {
	n, err := c.getCurrentNode(id)
	if err != nil {
		return "", fmt.Errorf("graph: local start-node fetch under lock: %w", err)
	}
	return nodeIntegrityHash(n), nil
}
