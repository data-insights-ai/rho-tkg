package core

import (
	"context"
	"fmt"
	"reflect"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/integrity"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// CompareAndSetProperty atomically compares and swaps a single node property.
// Returns (true, nil) on match+update, (false, nil) on mismatch, (false, error) on real error.
// expected == nil means "property must not exist". newVal == nil means "delete the property".
// Value comparison uses reflect.DeepEqual — type must match exactly (int(42) != int64(42)).
func (n *NodeOps) CompareAndSetProperty(id types.NodeID, key string, expected, newVal any) (bool, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return false, err
	}
	return c.Nodes.CompareAndSetPropertyWithContext(context.Background(), id, key, expected, newVal)
}

// CompareAndSetPropertyWithContext is the context-aware variant of CompareAndSetProperty.
// Acquires c.mu.RLock for transaction isolation — blocked while a tx holds c.mu.Lock.
func (n *NodeOps) CompareAndSetPropertyWithContext(ctx context.Context, id types.NodeID, key string, expected, newVal any) (bool, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
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

	// Reject reserved keys.
	if types.IsShadowKey(key) {
		return false, false, fmt.Errorf("graph: compare-and-set: %w: %q", types.ErrReservedPrefix, key)
	}

	// Pre-validate newVal if non-nil.
	if newVal != nil {
		if err := types.ValidatePropertyValue(newVal); err != nil {
			return false, false, fmt.Errorf("graph: compare-and-set property %q: %w", key, err)
		}
		if err := c.validatePropertyEntry(key, newVal); err != nil {
			return false, false, err
		}
	} else {
		// Even for deletions, check key length.
		if len(key) > c.validation.MaxPropertyKeyLength {
			return false, false, fmt.Errorf("%w: %q (%d > %d)", ErrKeyTooLong, key, len(key), c.validation.MaxPropertyKeyLength)
		}
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return false, false, err
	}

	// Entity lock → read-modify-write under serialization.
	c.entityLocks.LockEntity(id.SnowflakeID())
	defer c.entityLocks.UnlockEntity(id.SnowflakeID())

	if err := checkCtx(ctx); err != nil {
		return false, false, err
	}

	current, err := c.store.GetNode(id)
	if err != nil {
		return false, false, err
	}

	// --- Compare gate ---
	cur, found := current.GetProperty(key)
	if expected == nil {
		// "property must not exist"
		if found {
			return false, false, nil
		}
	} else {
		// "property must exist and match"
		if !found || !reflect.DeepEqual(cur, expected) {
			return false, false, nil
		}
	}

	// --- No-op check: deleting an absent property ---
	if newVal == nil && !found {
		return true, false, nil
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

	now := c.now()
	tm := current.Temporal()
	if tm == nil {
		tm = &types.TemporalMetadata{}
		current.SetTemporal(tm)
	}
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

	// Atomic replace + history.
	if err := c.store.ReplaceNodeWithHistory(current, prevVersion, prevState); err != nil {
		return false, false, err
	}

	c.opNodeUpdates.Add(1)
	return true, true, nil
}
