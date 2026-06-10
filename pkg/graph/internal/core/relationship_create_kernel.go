package core

import (
	"context"
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/generatedcreate"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/integrity"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// This file is the single relationship-creation kernel shared by every
// create door: RelOps.Add, RelOps.AddByID, RelOps.AddByIDIfAbsent, their tx
// mirrors, and BatchBuilder.Execute. Before the kernel existed each door
// carried its own copy of the validate → endpoint-hash ladder → type-token
// allocation → build → persist → rollback sequence, and an invariant added
// to one door could silently miss the others (lesson 17; the MR protocol's
// "check batch paths" rule existed to compensate by process). Changes to
// creation semantics belong HERE, not in the doors.

// relCreatePrep carries the validated, shadow-stripped inputs for a
// relationship create. Produced by prepareRelCreate before any lock or ID
// allocation so every door fails fast on bad input in the same order.
type relCreatePrep struct {
	typeName     string
	startID      types.NodeID
	endID        types.NodeID
	ps           types.OwnedPropertySlice
	authorID     string
	sig          []byte
	authorizedBy string
	authLevel    uint8
	validFrom    types.Instant
	validTo      types.Instant
	createdAt    types.Instant
}

// prepareRelCreate validates the create inputs in the canonical order:
// type name, provenance extraction, temporal extraction, property
// validation, bulk property construction, endpoint-ID validation, self-loop
// policy. Callers must not reorder these — error precedence is part of the
// public behavior.
func (c *Core) prepareRelCreate(typeName string, props map[string]any, startID, endID types.NodeID) (relCreatePrep, error) {
	var prep relCreatePrep
	if err := c.validateName(typeName); err != nil {
		return prep, err
	}
	authorID, sig, authorizedBy, authLevel, props, err := extractProvenance(props)
	if err != nil {
		return prep, err
	}
	validFrom, validTo, createdAt, props, err := extractTemporal(props)
	if err != nil {
		return prep, err
	}
	if err := c.validateProperties(props); err != nil {
		return prep, err
	}
	// Bulk-build properties first — fail fast before generating an ID.
	ps, err := types.NewOwnedPropertySlice(props)
	if err != nil {
		return prep, fmt.Errorf("graph: relationship properties: %w", err)
	}
	if err := validateRelationshipEndpointIDs(startID, endID); err != nil {
		return prep, err
	}
	if startID == endID && !c.validation.AllowSelfLoops {
		return prep, ErrSelfLoop
	}
	return relCreatePrep{
		typeName:     typeName,
		startID:      startID,
		endID:        endID,
		ps:           ps,
		authorID:     authorID,
		sig:          sig,
		authorizedBy: authorizedBy,
		authLevel:    authLevel,
		validFrom:    validFrom,
		validTo:      validTo,
		createdAt:    createdAt,
	}, nil
}

// relEndpointHashLadder resolves FromNodeHash/ToNodeHash for a new
// relationship. Must be called under the endpoint entity locks so the hashes
// (and the temporal-constraint check, when constraints are configured)
// reflect the committed endpoint state at creation time, not whatever the
// caller happened to hold (R4-F5).
//
// Returns useEndpointHashWrite=true when hash capture should instead be
// delegated to the store write (endpointHashWrite capability available and
// no constraints configured) — in that case the returned hashes are empty
// and the kernel refreshes them from the store's reply.
func (c *Core) relEndpointHashLadder(id types.RelID, startID, endID types.NodeID, validFrom, validTo, createdAt types.Instant) (fromHash, toHash string, useEndpointHashWrite bool, err error) {
	if c.constraints.Len() > 0 {
		// Fetch live endpoints from the store under the endpoint locks so
		// hash refresh and temporal-constraint checks see the current state.
		liveStart, liveEnd, err := c.liveEndpointNodes(startID, endID)
		if err != nil {
			return "", "", false, err
		}
		probe := c.newRelConstraintProbe(id, startID, endID, validFrom, validTo, createdAt)
		if err := c.checkTemporalConstraints(probe, liveStart, liveEnd); err != nil {
			return "", "", false, err
		}
		return nodeIntegrityHash(liveStart), nodeIntegrityHash(liveEnd), false, nil
	}
	if c.endpointHashWrite != nil {
		return "", "", true, nil
	}
	fromHash, toHash, err = c.liveEndpointHashes(startID, endID)
	if err != nil {
		return "", "", false, err
	}
	return fromHash, toHash, false, nil
}

// relCreateSpec carries everything the standalone build closure needs to
// assemble the new relationship once the type token is allocated.
type relCreateSpec struct {
	relCreatePrep
	id       types.RelID
	fromHash string
	toHash   string
}

// buildRelFromSpec returns the build closure used by the standalone create
// doors: construct the relationship, install properties, compute the content
// hash, attach integrity + provenance + endpoint hashes, and apply create
// temporal metadata. A non-nil error rolls back the type-token allocation in
// the kernel.
func (c *Core) buildRelFromSpec(ctx context.Context, spec relCreateSpec) func(typeToken uint16) (*types.Relationship, *types.RelIntegrity, error) {
	return func(typeToken uint16) (*types.Relationship, *types.RelIntegrity, error) {
		r := types.NewRelationship(spec.id, typeToken, spec.startID, spec.endID)
		if err := r.SetOwnedProperties(spec.ps); err != nil {
			return nil, nil, fmt.Errorf("graph: relationship properties: %w", err)
		}
		hash, err := integrity.ComputeRelHashChecked(r, spec.typeName)
		if err != nil {
			return nil, nil, fmt.Errorf("graph: compute relationship hash: %w", err)
		}
		// Capture endpoint hashes at creation time for cross-validation.
		// FromNodeHash/ToNodeHash are NOT part of ComputeRelHash to avoid
		// cascading hash invalidation whenever endpoint nodes are updated.
		ig := &types.RelIntegrity{
			Hash:               hash,
			PrevHash:           "",
			AuthorID:           spec.authorID,
			Signature:          spec.sig,
			AuthorizedBy:       spec.authorizedBy,
			AuthorizationLevel: spec.authLevel,
		}
		ig.FromNodeHash = spec.fromHash
		ig.ToNodeHash = spec.toHash
		r.SetIntegrity(ig)
		c.applyRelCreateTemporal(r, spec.validFrom, spec.validTo, spec.createdAt)
		if err := checkCtx(ctx); err != nil {
			return nil, nil, err
		}
		return r, ig, nil
	}
}

// createRelWithTypeRollback is the persistence kernel: allocate the rel-type
// token (snapshotting the registry so a later failure can roll back a fresh
// allocation), invoke build(token) to produce the relationship, persist it —
// via the endpoint-hash-capture capability when useEndpointHashWrite is true,
// else putGeneratedRelationship — and finalize or roll back the registry
// state exactly once on every path, including panics.
//
// Contract: getOrCreateRelTypeWithSnapshot returns with c.registryMu HELD
// when it allocated a new token (snapshot != nil); the restore helpers
// release it. The kernel therefore must call exactly one restore function on
// every exit path.
//
// Returns (rel, err) where rel != nil means the row IS live in the store —
// either a clean success (err == nil) or a partial-live outcome (the write
// landed but post-write cleanup/registry persistence failed; err != nil).
// Callers must count the create and surface the row whenever rel != nil.
func (c *Core) createRelWithTypeRollback(typeName string, useEndpointHashWrite bool, build func(typeToken uint16) (*types.Relationship, *types.RelIntegrity, error)) (*types.Relationship, error) {
	typeToken, relTypeSnapshot, allocatedRelType, err := c.getOrCreateRelTypeWithSnapshot(typeName)
	if err != nil {
		return nil, fmt.Errorf("graph: relationship type: %w", err)
	}
	relTypeFinished := false
	var r *types.Relationship
	defer func() {
		if !relTypeFinished {
			_ = c.restoreNewRelTypeCreateOnError(relTypeSnapshot, allocatedRelType, typeName, fmt.Errorf("panic during relationship create"), func() error {
				return c.deletePartialRelationshipForRollback(r)
			}, nil)
		}
	}()

	built, ig, err := build(typeToken)
	if err != nil {
		relTypeFinished = true
		return nil, c.restoreNewRelTypeOnError(relTypeSnapshot, allocatedRelType, typeName, err)
	}
	r = built

	persistFailed := func(err error) (*types.Relationship, error) {
		relTypeFinished = true
		partialLive := false
		err = c.restoreNewRelTypeCreateOnError(relTypeSnapshot, allocatedRelType, typeName, err, func() error {
			return c.deletePartialRelationshipForRollback(r)
		}, &partialLive)
		if partialLive {
			return r, err
		}
		return nil, err
	}

	if useEndpointHashWrite {
		fromHash, toHash, err := c.endpointHashWrite.PutRelationshipGeneratedIDWithEndpointHashes(r, generatedcreate.FreshGraphID)
		if err != nil {
			return persistFailed(err)
		}
		ig.FromNodeHash = fromHash
		ig.ToNodeHash = toHash
	} else {
		if err := c.putGeneratedRelationship(r); err != nil {
			return persistFailed(err)
		}
	}

	relTypeFinished = true
	if err := c.restoreNewRelTypeOnError(relTypeSnapshot, allocatedRelType, typeName, nil); err != nil {
		// The row is live; only the registry persistence failed. Surface the
		// error but hand back the live relationship so the caller counts it.
		return r, err
	}
	c.rememberRelType(typeName, typeToken)
	return r, nil
}
