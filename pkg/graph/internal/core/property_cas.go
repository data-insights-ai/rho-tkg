package core

import (
	"context"
	"fmt"

	eventspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// CompareAndSetProperty atomically compares and swaps a single node property.
// Returns (true, nil) on match+update, (false, nil) on mismatch, (false, error) on real error.
// expected == nil means "property must not exist". newVal == nil means "delete the property".
// A numeric expected compares under the property equality (types.PropertyEqual):
// int64(42) matches a stored int(42), a scalar NaN matches nothing. Strings,
// booleans, lists and maps compare by value identity (types.PropertyValueEqual):
// exact types, and a NaN inside a list or map matches NaN.
// Acquires c.mu.RLock for transaction isolation — blocked while a tx holds c.mu.Lock.
func (n *NodeOps) CompareAndSetProperty(ctx context.Context, id types.NodeID, key string, expected, newVal any) (bool, error) {
	c := n.c
	if err := c.checkWritable(); err != nil {
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

	// Under UntimedWholeAxis first: the compare reads the current row, one
	// segment of a segmented node (a per-segment compare needs a store door).
	if err := c.checkWholeAxisVersionedNode(id, current, "compare-and-set"); err != nil {
		return false, false, err
	}

	// --- Compare gate ---
	found, equal := casGate(current.GetProperty, current.PropertyValueEqual, key, expected)
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
	if err := c.rejectClosedNodeMutation(current); err != nil {
		return false, false, err
	}
	if err := c.checkpointDirtyRegistriesBeforeMutation("compare-and-set node property"); err != nil {
		return false, false, err
	}

	// --- Capture pre-mutation state ---
	prevVersion := current.Version()
	nextVersion, err := c.nextNodeVersion(id, current)
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

	at := c.nodeVersionUpdateInstant(current)
	now := at.tx
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
	stampValidFrom(c.axis(), tm, 0, false) // the whole axis under UntimedWholeAxis (checkWholeAxis*), else no claim
	tm.ValidTo = c.axis().OpenEnd()
	tm.UpdatedAt = now

	// TxTo on previous version.
	if ptm := prevState.Temporal(); ptm == nil {
		ptm2 := &types.TemporalMetadata{}
		prevState.SetTemporal(ptm2)
		ptm2.TxTo = now
	} else {
		ptm.TxTo = now
	}
	at.stamp(tm)

	nodeLabels := c.nodeLabelsUnlocked(current)
	hash, err := c.nodeContentHash(current, nodeLabels)
	if err != nil {
		return false, false, fmt.Errorf("graph: compute node hash: %w", err)
	}
	current.SetIntegrity(nodeIntegrityWithHash(current.Integrity(), hash, prevHash))

	if err := checkCtx(ctx); err != nil {
		return false, false, err
	}

	// Unique-constraint enforcement (standalone CAS door). Entity lock held;
	// take value stripe(s) across the check + write. prevState holds the old
	// value so a changed constrained value also holds the freed value's stripe.
	uniqueHold, uniqueErr := c.enforceUniqueForNode(current, prevState, id)
	if uniqueErr != nil {
		return false, false, uniqueErr
	}
	defer uniqueHold.release()

	// Atomic replace + history.
	if err := c.store.ReplaceNodeWithHistory(current, prevVersion, prevState); err != nil {
		return false, false, uniqueHold.storeWriteFailed(err)
	}

	c.opNodeUpdates.Add(1)
	return true, true, nil
}

// CompareAndSetProperty atomically compares and swaps a single relationship property.
// Returns (true, nil) on match+update, (false, nil) on mismatch, (false, error) on real error.
// expected == nil means "property must not exist". newVal == nil means "delete the property".
// A numeric expected compares under the property equality (types.PropertyEqual):
// int64(42) matches a stored int(42), a scalar NaN matches nothing. Strings,
// booleans, lists and maps compare by value identity (types.PropertyValueEqual):
// exact types, and a NaN inside a list or map matches NaN.
// Acquires c.mu.RLock for transaction isolation — blocked while a tx holds c.mu.Lock.
func (r *RelOps) CompareAndSetProperty(ctx context.Context, id types.RelID, key string, expected, newVal any) (bool, error) {
	c := r.c
	if err := c.checkWritable(); err != nil {
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

	// Under UntimedWholeAxis first, as the node door.
	if err := c.checkWholeAxisVersionedRel(id, current, "compare-and-set"); err != nil {
		return false, false, err
	}

	found, equal := casGate(current.GetProperty, current.PropertyValueEqual, key, expected)
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
	if err := c.rejectClosedRelMutation(current); err != nil {
		return false, false, err
	}
	if err := c.checkpointDirtyRegistriesBeforeMutation("compare-and-set relationship property"); err != nil {
		return false, false, err
	}

	prevVersion := current.Version()
	nextVersion, err := c.nextRelVersion(id, current)
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

	at := c.relVersionUpdateInstant(current)
	now := at.tx
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
	stampValidFrom(c.axis(), tm, 0, false) // the whole axis under UntimedWholeAxis (checkWholeAxis*), else no claim
	tm.ValidTo = c.axis().OpenEnd()
	tm.UpdatedAt = now
	if ptm := prevState.Temporal(); ptm == nil {
		ptm2 := &types.TemporalMetadata{}
		prevState.SetTemporal(ptm2)
		ptm2.TxTo = now
	} else {
		ptm.TxTo = now
	}
	at.stamp(tm)

	relTypeName := c.relTypeUnlocked(current)
	hash, err := c.relContentHash(current, relTypeName)
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
	return types.PropertyEqual(cur, expected)
}

// casGate reports whether key exists and its value equals expected under the
// property equality (types.PropertyEqual): a numeric expected reads the
// stored scalar, any other compares in place without a copy.
func casGate(get func(string) (any, bool), same func(string, any) (bool, bool), key string, expected any) (found, equal bool) {
	if _, num := types.NumericFloat64(expected); !num {
		return same(key, expected)
	}
	cur, found := get(key)
	return found, found && propertyCASValueEqual(cur, expected)
}
