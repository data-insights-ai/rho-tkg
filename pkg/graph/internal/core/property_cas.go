package core

import (
	"context"
	"fmt"

	eventspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/integrity"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// CompareAndSetProperty atomically compares and swaps a single node property.
// Returns (true, nil) on match+update, (false, nil) on mismatch, (false, error) on real error.
// expected == nil means "property must not exist". newVal == nil means "delete the property".
// Value comparison is exact-type property equality (int(42) != int64(42)); NaN property values match NaN.
// Acquires c.mu.RLock for transaction isolation — blocked while a tx holds c.mu.Lock.
func (n *NodeOps) CompareAndSetProperty(ctx context.Context, id types.NodeID, key string, expected, newVal any) (bool, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return false, err
	}
	if err := checkCtx(ctx); err != nil {
		return false, err
	}
	var (
		ok      bool
		mutated bool
		err     error
	)
	ep, closeErr := c.runUnderRLock(func() {
		ok, mutated, err = c.compareAndSetPropertyInternal(ctx, id, key, expected, newVal)
	})
	if closeErr != nil {
		return false, closeErr
	}
	if ok && mutated && err == nil {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventNodeUpdate, EntityID: types.EntityID(id), Timestamp: c.now(), Priority: eventspkg.PriorityNormal})
	}
	return ok, err
}

// compareAndSetPropertyInternal is the lock-free implementation.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
func (c *Core) compareAndSetPropertyInternal(ctx context.Context, id types.NodeID, key string, expected, newVal any) (bool, bool, error) {
	if err := checkCtx(ctx); err != nil {
		return false, false, err
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return false, false, err
	}

	// Reject reserved keys.
	if types.IsShadowKey(key) {
		return false, false, fmt.Errorf("graph: compare-and-set: %w: %q", types.ErrReservedPrefix, key)
	}

	// Pre-validate compare and replacement values before taking locks or
	// reading the target row. Malformed expected values are caller input bugs,
	// not CAS mismatches.
	if expected != nil {
		if err := types.ValidatePropertyValue(expected); err != nil {
			return false, false, fmt.Errorf("graph: compare-and-set expected property %q: %w", key, err)
		}
		if err := c.validatePropertyEntryLimits(key, expected); err != nil {
			return false, false, err
		}
	}

	// Pre-validate newVal if non-nil.
	if newVal != nil {
		if err := types.ValidatePropertyValue(newVal); err != nil {
			return false, false, fmt.Errorf("graph: compare-and-set property %q: %w", key, err)
		}
		if err := c.validatePropertyEntryLimits(key, newVal); err != nil {
			return false, false, err
		}
	} else {
		// Even for deletions, check key length.
		if err := c.validatePropertyKeyLength(key); err != nil {
			return false, false, err
		}
	}
	// Entity lock → read-modify-write under serialization.
	c.entityLocks.LockEntity(id.SnowflakeID())
	defer c.entityLocks.UnlockEntity(id.SnowflakeID())

	if err := checkCtx(ctx); err != nil {
		return false, false, err
	}

	current, err := c.getCurrentNode(id)
	if err != nil {
		return false, false, err
	}
	if err := checkCtx(ctx); err != nil {
		return false, false, err
	}

	// --- Compare gate ---
	found, equal := current.PropertyValueEqual(key, expected)
	if expected == nil {
		// "property must not exist"
		if found {
			return false, false, nil
		}
	} else {
		// "property must exist and match"
		if !found || !equal {
			return false, false, nil
		}
	}

	// --- No-op check: deleting an absent property ---
	if newVal == nil && !found {
		return true, false, nil
	}
	if newVal != nil {
		_, equalNew := current.PropertyValueEqual(key, newVal)
		if equalNew {
			return true, false, nil
		}
	}
	if err := rejectClosedNodeMutation(current); err != nil {
		return false, false, err
	}
	if err := c.checkpointDirtyRegistriesBeforeMutation("compare-and-set node property"); err != nil {
		return false, false, err
	}

	// --- Capture pre-mutation state ---
	prevVersion := current.Version()
	nextVersion, err := nextEntityVersion(prevVersion)
	if err != nil {
		return false, false, err
	}
	prevState := current.DeepCopy()
	prevHash := ""
	if ig := current.Integrity(); ig != nil {
		prevHash = ig.Hash
	}

	// --- Mutate ---
	if newVal == nil {
		if _, err := current.DeleteProperty(key); err != nil {
			return false, false, fmt.Errorf("graph: compare-and-set property %q: %w", key, err)
		}
	} else {
		if err := current.SetProperty(key, newVal); err != nil {
			return false, false, fmt.Errorf("graph: compare-and-set property %q: %w", key, err)
		}
	}

	// Check final property count after mutations (under entity lock, before persist).
	if current.PropertyCount() > c.validation.MaxPropertiesPerEntity {
		return false, false, fmt.Errorf("%w: %d > %d", ErrTooManyProperties, current.PropertyCount(), c.validation.MaxPropertiesPerEntity)
	}

	current.SetVersion(nextVersion)

	now := c.nodeVersionUpdateInstant(current)
	tm := current.Temporal()
	if tm == nil {
		tm = &types.TemporalMetadata{}
		current.SetTemporal(tm)
	}
	// Lesson 33, property-mutation door: the new version inherited the
	// previous version's world-time claim via the in-place mutation of the
	// current row. Clear it — property mutations accept no caller-supplied
	// valid time, and an inherited ValidFrom would let this version cover
	// the previous version's interval in historical queries.
	tm.ValidFrom = 0
	tm.ValidTo = 0
	tm.UpdatedAt = now

	// TxTo on previous version.
	if ptm := prevState.Temporal(); ptm == nil {
		ptm2 := &types.TemporalMetadata{}
		prevState.SetTemporal(ptm2)
		ptm2.TxTo = now
	} else {
		ptm.TxTo = now
	}
	tm.TxFrom = now

	nodeLabels := c.nodeLabelsUnlocked(current)
	hash, err := integrity.ComputeNodeHashChecked(current, nodeLabels)
	if err != nil {
		return false, false, fmt.Errorf("graph: compute node hash: %w", err)
	}
	current.SetIntegrity(nodeIntegrityWithHash(current.Integrity(), hash, prevHash))

	if err := checkCtx(ctx); err != nil {
		return false, false, err
	}

	// Atomic replace + history.
	if err := c.store.ReplaceNodeWithHistory(current, prevVersion, prevState); err != nil {
		return false, false, err
	}

	c.opNodeUpdates.Add(1)
	return true, true, nil
}

// CompareAndSetProperty atomically compares and swaps a single relationship property.
// Returns (true, nil) on match+update, (false, nil) on mismatch, (false, error) on real error.
// expected == nil means "property must not exist". newVal == nil means "delete the property".
// Value comparison is exact-type property equality (int(42) != int64(42)); NaN property values match NaN.
// Acquires c.mu.RLock for transaction isolation — blocked while a tx holds c.mu.Lock.
func (r *RelOps) CompareAndSetProperty(ctx context.Context, id types.RelID, key string, expected, newVal any) (bool, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return false, err
	}
	if err := checkCtx(ctx); err != nil {
		return false, err
	}
	var (
		ok      bool
		mutated bool
		err     error
	)
	ep, closeErr := c.runUnderRLock(func() {
		ok, mutated, err = c.compareAndSetRelPropertyInternal(ctx, id, key, expected, newVal)
	})
	if closeErr != nil {
		return false, closeErr
	}
	if ok && mutated && err == nil {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventRelUpdate, EntityID: types.EntityID(id), Timestamp: c.now(), Priority: eventspkg.PriorityNormal})
	}
	return ok, err
}

// compareAndSetRelPropertyInternal is the lock-free relationship CAS implementation.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
func (c *Core) compareAndSetRelPropertyInternal(ctx context.Context, id types.RelID, key string, expected, newVal any) (bool, bool, error) {
	if err := checkCtx(ctx); err != nil {
		return false, false, err
	}
	if err := storepkg.ValidateRelID(id); err != nil {
		return false, false, err
	}

	if types.IsShadowKey(key) {
		return false, false, fmt.Errorf("graph: compare-and-set relationship: %w: %q", types.ErrReservedPrefix, key)
	}

	if expected != nil {
		if err := types.ValidatePropertyValue(expected); err != nil {
			return false, false, fmt.Errorf("graph: compare-and-set relationship expected property %q: %w", key, err)
		}
		if err := c.validatePropertyEntryLimits(key, expected); err != nil {
			return false, false, err
		}
	}

	if newVal != nil {
		if err := types.ValidatePropertyValue(newVal); err != nil {
			return false, false, fmt.Errorf("graph: compare-and-set relationship property %q: %w", key, err)
		}
		if err := c.validatePropertyEntryLimits(key, newVal); err != nil {
			return false, false, err
		}
	} else {
		if err := c.validatePropertyKeyLength(key); err != nil {
			return false, false, err
		}
	}
	if err := checkCtx(ctx); err != nil {
		return false, false, err
	}

	current, startID, endID, err := c.lockRelationshipCurrentEndpoints(ctx, id)
	if err != nil {
		return false, false, err
	}
	defer c.entityLocks.UnlockThree(id.SnowflakeID(), startID.SnowflakeID(), endID.SnowflakeID())

	if err := checkCtx(ctx); err != nil {
		return false, false, err
	}

	found, equal := current.PropertyValueEqual(key, expected)
	if expected == nil {
		if found {
			return false, false, nil
		}
	} else if !found || !equal {
		return false, false, nil
	}

	if newVal == nil && !found {
		return true, false, nil
	}
	if newVal != nil {
		_, equalNew := current.PropertyValueEqual(key, newVal)
		if equalNew {
			return true, false, nil
		}
	}
	if err := rejectClosedRelMutation(current); err != nil {
		return false, false, err
	}
	if err := c.checkpointDirtyRegistriesBeforeMutation("compare-and-set relationship property"); err != nil {
		return false, false, err
	}

	prevVersion := current.Version()
	nextVersion, err := nextEntityVersion(prevVersion)
	if err != nil {
		return false, false, err
	}
	prevState := current.DeepCopy()
	prevHash := ""
	if ig := current.Integrity(); ig != nil {
		prevHash = ig.Hash
	}

	if newVal == nil {
		if _, err := current.DeleteProperty(key); err != nil {
			return false, false, fmt.Errorf("graph: compare-and-set relationship property %q: %w", key, err)
		}
	} else {
		if err := current.SetProperty(key, newVal); err != nil {
			return false, false, fmt.Errorf("graph: compare-and-set relationship property %q: %w", key, err)
		}
	}

	if current.PropertyCount() > c.validation.MaxPropertiesPerEntity {
		return false, false, fmt.Errorf("%w: %d > %d", ErrTooManyProperties, current.PropertyCount(), c.validation.MaxPropertiesPerEntity)
	}

	current.SetVersion(nextVersion)

	now := c.relVersionUpdateInstant(current)
	tm := current.Temporal()
	if tm == nil {
		tm = &types.TemporalMetadata{}
		current.SetTemporal(tm)
	}
	// Lesson 33, property-mutation door: the new version inherited the
	// previous version's world-time claim via the in-place mutation of the
	// current row. Clear it — property mutations accept no caller-supplied
	// valid time, and an inherited ValidFrom would let this version cover
	// the previous version's interval in historical queries.
	tm.ValidFrom = 0
	tm.ValidTo = 0
	tm.UpdatedAt = now
	if ptm := prevState.Temporal(); ptm == nil {
		ptm2 := &types.TemporalMetadata{}
		prevState.SetTemporal(ptm2)
		ptm2.TxTo = now
	} else {
		ptm.TxTo = now
	}
	tm.TxFrom = now

	relTypeName := c.relTypeUnlocked(current)
	hash, err := integrity.ComputeRelHashChecked(current, relTypeName)
	if err != nil {
		return false, false, fmt.Errorf("graph: compute relationship hash: %w", err)
	}
	relIG := relIntegrityWithHash(current.Integrity(), hash, prevHash)
	if err := c.refreshRelationshipEndpointHashes(current, relIG); err != nil {
		return false, false, err
	}
	current.SetIntegrity(relIG)

	if err := checkCtx(ctx); err != nil {
		return false, false, err
	}

	if err := c.store.ReplaceRelWithHistory(current, prevVersion, prevState); err != nil {
		return false, false, err
	}

	c.opRelUpdates.Add(1)
	return true, true, nil
}

func propertyCASValueEqual(cur, expected any) bool {
	return types.PropertyValueEqual(cur, expected)
}
