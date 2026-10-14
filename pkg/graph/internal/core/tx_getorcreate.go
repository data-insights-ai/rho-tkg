package core

import (
	"context"
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// GetOrCreateByKey is the transaction-scoped sibling of NodeOps.GetOrCreateByKey
// (node_getorcreate.go): same semantics (one value stripe shared with the
// standalone door and any active unique constraint; exactly one create under
// concurrency; bool reports whether THIS call created; same value/extraProps
// rules, ErrUniqueUnsupportedType for floats and non-scalars), but the create
// participates in this tx: visible to later reads on the tx, its EventNodeCreate
// buffered until Commit, undone by Rollback (so MERGE in a statement tx can roll
// back). Holds tx.mu for the whole call, see GraphTx.AddNode.
func (tx *GraphTx) GetOrCreateByKey(label, propertyKey string, value any, extraProps map[string]any) (*types.Node, bool, error) {
	if err := tx.lockActiveCoreWrite(); err != nil {
		return nil, false, err
	}
	defer tx.unlockActiveCoreWrite()
	return tx.getOrCreateByKeyLocked(tx.doorCtx(), label, propertyKey, value, extraProps)
}

// getOrCreateByKeyLocked is the lock-free body: the caller holds tx.mu AND c.mu
// (via lockActiveCoreWrite), so it must NOT re-enter runUnderRLock (deadlock under
// the exclusive log-scope path); it uses the *Internal store doors and routes event
// + rollback bookkeeping through noteNodeCreateResultLocked.
func (tx *GraphTx) getOrCreateByKeyLocked(ctx context.Context, label, propertyKey string, value any, extraProps map[string]any) (*types.Node, bool, error) {
	c := tx.g
	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}
	if err := c.validateName(label); err != nil {
		return nil, false, err
	}
	if err := c.validateIndexPropertyKey(propertyKey); err != nil {
		return nil, false, err
	}

	// Reject float (bit-pattern equality trap, lesson 25) and non-scalar values up front.
	valueKey := types.IndexablePropertyValueKey(value)
	if valueKey == "" {
		return nil, false, fmt.Errorf("%w: GetOrCreateByKey value for key %q is not an indexable scalar", ErrUniqueUnsupportedType, propertyKey)
	}
	if isFloatValueKey(valueKey) {
		return nil, false, fmt.Errorf("%w: GetOrCreateByKey does not support float values (key %q)", ErrUniqueUnsupportedType, propertyKey)
	}

	// Resolve/persist the label token so the value stripe is stable; the tx registry
	// snapshot (BeginTx) undoes it on Rollback, as for tx.AddNode.
	labelTok, err := c.getOrCreateLabelPersisted(label)
	if err != nil {
		return nil, false, err
	}
	if err := c.refuseNodeElementsIndex(labelTok, propertyKey, "GetOrCreateByKey"); err != nil {
		return nil, false, err
	}
	stripe := uniqueValueStripe(labelTok, propertyKey, valueKey)

	// Hold the value stripe across lookup AND create; it is the separate lock class
	// that serializes standalone writers running under c.mu.RLock. The tx holds c.mu.
	ordered := c.valueLocks.LockStripes([]uint8{stripe})
	defer c.valueLocks.UnlockStripes(ordered)

	matches, err := c.nodesByLabelAndProperty(labelTok, propertyKey, value, storepkg.QueryOpts{})
	if err != nil {
		return nil, false, fmt.Errorf("graph: get-or-create lookup: %w", err)
	}
	if len(matches) > 0 {
		// Hit: an earlier create in THIS tx is already in the store's property index
		// (ghost-read consistency). Return a mutable copy of the lowest-ID match.
		node := matches[0].DeepCopy()
		c.opNodeReads.Add(1)
		return node, false, nil
	}

	// Miss: create under the SAME held stripe, routed through the tx's event/rollback
	// bookkeeping instead of an immediate dispatch.
	props := make(map[string]any, len(extraProps)+1)
	for k, v := range extraProps {
		props[k] = v
	}
	props[propertyKey] = value
	node, err := c.addNodeInternal(ctx, []string{label}, props, stripe)
	if err != nil {
		return nil, false, err
	}
	tx.noteNodeCreateResultLocked(node)
	return node, true, nil
}
