package graph

import (
	"context"
	"fmt"
	"reflect"
	"time"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/integrity"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// CompareAndSetProperty atomically compares and swaps a single node property.
// Returns (true, nil) on match+update, (false, nil) on mismatch, (false, error) on real error.
// expected == nil means "property must not exist". newVal == nil means "delete the property".
// Value comparison uses reflect.DeepEqual — type must match exactly (int(42) != int64(42)).
func (g *Graph) CompareAndSetProperty(id types.NodeID, key string, expected, newVal any) (bool, error) {
	return g.CompareAndSetPropertyWithContext(context.Background(), id, key, expected, newVal)
}

// CompareAndSetPropertyWithContext is the context-aware variant of CompareAndSetProperty.
// Acquires g.mu.RLock for transaction isolation — blocked while a tx holds g.mu.Lock.
func (g *Graph) CompareAndSetPropertyWithContext(ctx context.Context, id types.NodeID, key string, expected, newVal any) (bool, error) {
	g.mu.RLock()
	ok, mutated, err := g.compareAndSetPropertyInternal(ctx, id, key, expected, newVal)
	ep := g.events
	g.mu.RUnlock()
	if ok && mutated && err == nil {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventNodeUpdate, EntityID: types.EntityID(id), Timestamp: nowInstant(), Priority: eventspkg.PriorityNormal})
	}
	return ok, err
}

// compareAndSetPropertyInternal is the lock-free implementation.
// Callers must hold g.mu.RLock (standalone) or g.mu.Lock (tx/batch).
func (g *Graph) compareAndSetPropertyInternal(ctx context.Context, id types.NodeID, key string, expected, newVal any) (bool, bool, error) {
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
		if err := g.validatePropertyEntry(key, newVal); err != nil {
			return false, false, err
		}
	} else {
		// Even for deletions, check key length.
		if len(key) > g.validation.MaxPropertyKeyLength {
			return false, false, fmt.Errorf("%w: %q (%d > %d)", ErrKeyTooLong, key, len(key), g.validation.MaxPropertyKeyLength)
		}
	}

	// Entity lock → read-modify-write under serialization.
	g.entityLocks.LockEntity(id.SnowflakeID())
	defer g.entityLocks.UnlockEntity(id.SnowflakeID())

	if err := checkCtx(ctx); err != nil {
		return false, false, err
	}

	current, err := g.store.GetNode(id)
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

	current.SetVersion(current.Version() + 1)

	now := types.Instant(time.Now().UnixMilli())
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

	nodeLabels := g.NodeLabels(current)
	hash := integrity.ComputeNodeHash(current, nodeLabels)
	current.SetIntegrity(&types.NodeIntegrity{
		Hash:     hash,
		PrevHash: prevHash,
	})

	// Atomic replace + history.
	if err := g.store.ReplaceNodeWithHistory(current, prevVersion, prevState); err != nil {
		return false, false, err
	}

	g.opNodeUpdates.Add(1)
	return true, true, nil
}
