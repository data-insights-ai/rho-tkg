package core

import (
	"context"
	"errors"
	"fmt"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// RelCreate is one element of GraphTx.AddRelationships: the endpoints by ID
// and the relationship's properties (shadow keys as AddRelationshipByID
// accepts them).
type RelCreate struct {
	StartID types.NodeID
	EndID   types.NodeID
	Props   map[string]any
}

// AddRelationships creates one relationship of typeName per element of rels
// inside the transaction and returns them in rels' order. It is
// AddRelationshipByID called once per element, with the work that does not
// depend on the element done once: the type is resolved once, the endpoints'
// locks are taken once, and the relationships reach the store in one batch
// write (Store.PutRelationshipsBatch) instead of one write each. Every
// relationship gets its own ID, hash, endpoint hashes, transaction time
// (strictly increasing in rels' order) and create event; each is tracked for
// rollback, so Rollback removes all of them.
//
// All or nothing: a validation error in any element (type name, provenance or
// temporal shadow keys, property limits, endpoint IDs, the self-loop policy)
// or an endpoint without a current row returns it before an ID is minted, and
// a failed store write removes the relationships it may have written; the
// returned slice is then nil. Where a relationship needs a per-relationship
// check the batch cannot make (temporal constraints are configured, or the
// transaction runs under a scoped write token), the call runs
// AddRelationshipByID's path per element instead: same results, none of the
// saving, and an error stops at the failing element with the earlier
// relationships kept (the transaction's rollback removes them).
//
// An empty rels creates nothing and returns (nil, nil).
func (tx *GraphTx) AddRelationships(typeName string, rels []RelCreate) ([]*types.Relationship, error) {
	if err := tx.lockActiveCoreWrite(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCoreWrite()

	created, err := tx.g.addRelationshipsInternal(tx.doorCtx(), typeName, rels)
	for _, r := range created {
		tx.noteRelCreateResultLocked(r)
	}
	return created, err
}

// canBatchRelCreates reports whether a relationship create may skip the
// per-relationship kernel: no temporal constraint needs the live endpoints
// checked against each relationship, and no scoped write token routes the
// write.
func (c *Core) canBatchRelCreates(ctx context.Context) bool {
	if c.constraints.Len() > 0 {
		return false
	}
	if token, ok := scopeTokenFrom(ctx); ok && token != 0 {
		return false
	}
	return true
}

// addRelationshipsInternal is the lock-free body of GraphTx.AddRelationships.
// Callers hold the transaction's core write section.
func (c *Core) addRelationshipsInternal(ctx context.Context, typeName string, rels []RelCreate) ([]*types.Relationship, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	if len(rels) == 0 {
		return nil, nil
	}
	if !c.canBatchRelCreates(ctx) {
		return c.addRelationshipsOneByOne(ctx, typeName, rels)
	}

	specs := make([]relCreateSpec, len(rels))
	for i, rc := range rels {
		prep, err := c.prepareRelCreate(typeName, rc.Props, rc.StartID, rc.EndID)
		if err != nil {
			return nil, fmt.Errorf("graph: relationship %d of %d: %w", i, len(rels), err)
		}
		specs[i].relCreatePrep = prep
	}
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// Lock every endpoint once (LockMany sorts and deduplicates: deadlock-free
	// with every other door), then read each endpoint's hash under the locks —
	// what the single door reads, or lets the store read, under LockTwo.
	lockIDs := make([]snowflake.ID, 0, 2*len(specs))
	for i := range specs {
		lockIDs = append(lockIDs, specs[i].startID.SnowflakeID(), specs[i].endID.SnowflakeID())
	}
	c.entityLocks.LockMany(lockIDs)
	defer c.entityLocks.UnlockMany(lockIDs)

	for i := range specs {
		fromHash, toHash, err := c.liveEndpointHashes(specs[i].startID, specs[i].endID)
		if err != nil {
			return nil, fmt.Errorf("graph: relationship %d of %d: %w", i, len(rels), err)
		}
		specs[i].fromHash, specs[i].toHash = fromHash, toHash
	}
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	typeToken, typeSnapshot, allocatedType, err := c.getOrCreateRelTypeWithSnapshot(typeName)
	if err != nil {
		return nil, fmt.Errorf("graph: relationship type: %w", err)
	}
	var built []*types.Relationship
	finished := false
	finish := func(err error) error {
		finished = true
		if err != nil {
			err = c.removePartialRelationships(built, err)
		}
		return c.restoreNewRelTypeOnError(typeSnapshot, allocatedType, typeName, err)
	}
	defer func() {
		if !finished {
			_ = finish(fmt.Errorf("panic during relationship batch create"))
		}
	}()

	built = make([]*types.Relationship, 0, len(specs))
	for i := range specs {
		specs[i].id = c.nextRelID()
		r, _, err := c.buildRelFromSpec(ctx, specs[i])(typeToken)
		if err != nil {
			built = nil
			return nil, finish(err)
		}
		built = append(built, r)
	}
	if err := checkCtx(ctx); err != nil {
		built = nil
		return nil, finish(err)
	}
	if err := c.store.PutRelationshipsBatch(built); err != nil {
		return nil, finish(err)
	}
	if err := finish(nil); err != nil {
		// The rows are live; only the registry persistence failed.
		c.opRelAdds.Add(int64(len(built)))
		return built, err
	}
	c.rememberRelType(typeName, typeToken)
	c.opRelAdds.Add(int64(len(built)))
	return built, nil
}

// addRelationshipsOneByOne is AddRelationships' per-element path (see
// AddRelationships).
func (c *Core) addRelationshipsOneByOne(ctx context.Context, typeName string, rels []RelCreate) ([]*types.Relationship, error) {
	created := make([]*types.Relationship, 0, len(rels))
	for _, rc := range rels {
		r, err := c.createRelationshipLocked(ctx, typeName, rc.StartID, rc.EndID, rc.Props)
		if r != nil {
			created = append(created, r)
		}
		if err != nil {
			return created, err
		}
	}
	return created, nil
}

// removePartialRelationships deletes the relationships a failed batch write
// may have stored and adds any cleanup failure to err.
func (c *Core) removePartialRelationships(rels []*types.Relationship, err error) error {
	for _, r := range rels {
		if cleanupErr := runRollbackCleanup(func() error {
			return c.deletePartialRelationshipForRollback(r)
		}); cleanupErr != nil && !errors.Is(cleanupErr, storepkg.ErrRelNotFound) {
			err = fmt.Errorf("%w; additionally failed to remove partial relationship %d after create failure: %v", err, r.ID().SnowflakeID(), cleanupErr)
		}
	}
	return err
}
