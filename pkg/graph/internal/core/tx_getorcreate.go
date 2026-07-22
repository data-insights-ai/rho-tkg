package core

import (
	"context"
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// GetOrCreateByKey is the transaction-scoped sibling of NodeOps.GetOrCreateByKey
// (node_getorcreate.go). It atomically returns the CURRENT node carrying value
// for (label, propertyKey) — or creates one if none exists — but the create
// PARTICIPATES in this transaction rather than auto-committing: it is visible to
// later reads on the same tx (ghost-read consistency), its EventNodeCreate is
// buffered and published on Commit, and it is undone by Rollback. This closes the
// MERGE-inside-a-statement-tx correctness gap: the standalone g.Nodes() door
// commits its create immediately, so a later clause failing could not roll the
// create back (sigma V412-1).
//
// Semantics match the standalone door exactly. A single value stripe (shared with
// the standalone door AND any active unique constraint on (label, propertyKey))
// is held across the lookup and the create, so a storm of concurrent callers of
// the same key — whether on this tx, another tx, or the standalone path —
// produces exactly one create; the returned bool reports whether THIS call made
// it. value must be an indexable scalar (string / int / bool / Instant); float
// and non-scalar values are rejected with ErrUniqueUnsupportedType. extraProps
// seed a freshly created node only (the keyed property always wins); they are
// ignored on a hit. The returned node is a mutable, independent copy.
//
// Holds tx.mu for the whole call — see GraphTx.AddNode.
func (tx *GraphTx) GetOrCreateByKey(label, propertyKey string, value any, extraProps map[string]any) (*types.Node, bool, error) {
	if err := tx.lockActiveCoreWrite(); err != nil {
		return nil, false, err
	}
	defer tx.unlockActiveCoreWrite()
	return tx.getOrCreateByKeyLocked(tx.doorCtx(), label, propertyKey, value, extraProps)
}

// getOrCreateByKeyLocked is the lock-free body: the caller already holds tx.mu
// AND c.mu (via lockActiveCoreWrite), so unlike Core.getOrCreateNodeByKey it must
// NOT re-enter runUnderRLock (a same-goroutine re-lock would deadlock under the
// exclusive log-scope path), reaches the store through the *Internal doors, and
// routes the create's event + rollback bookkeeping through
// noteNodeCreateResultLocked so the create is part of the tx.
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

	// The value must be an indexable scalar — that is the lookup key. Reject
	// float (bit-pattern equality trap) and non-scalar values up front.
	valueKey := types.IndexablePropertyValueKey(value)
	if valueKey == "" {
		return nil, false, fmt.Errorf("%w: GetOrCreateByKey value for key %q is not an indexable scalar", ErrUniqueUnsupportedType, propertyKey)
	}
	if isFloatValueKey(valueKey) {
		return nil, false, fmt.Errorf("%w: GetOrCreateByKey does not support float values (key %q)", ErrUniqueUnsupportedType, propertyKey)
	}

	// Resolve/persist the label token so the value stripe is stable and a create
	// binds to it. A token allocated here is captured by the tx's registry
	// snapshot (BeginTx) and undone on Rollback, exactly as tx.AddNode's is.
	labelTok, err := c.getOrCreateLabelPersisted(label)
	if err != nil {
		return nil, false, err
	}
	stripe := uniqueValueStripe(labelTok, propertyKey, valueKey)

	// Hold the value stripe across the lookup AND the create so the whole
	// check-then-create is atomic against every other caller of the same key
	// (standalone writers proceed under c.mu.RLock in parallel; the stripe, a
	// separate lock class, is what serializes them). The tx already holds c.mu.
	ordered := c.valueLocks.LockStripes([]uint8{stripe})
	defer c.valueLocks.UnlockStripes(ordered)

	matches, err := c.nodesByLabelAndProperty(labelTok, propertyKey, value, storepkg.QueryOpts{})
	if err != nil {
		return nil, false, fmt.Errorf("graph: get-or-create lookup: %w", err)
	}
	if len(matches) > 0 {
		// Hit — an earlier create in THIS tx is already visible here (the tx
		// applies mutations immediately to the store's property index), giving
		// ghost-read consistency. Return a mutable copy of the lowest-ID match.
		node := matches[0].DeepCopy()
		c.opNodeReads.Add(1)
		return node, false, nil
	}

	// Miss — create under the SAME held stripe (passed so the create's own
	// unique kernel does not re-lock it) and route the create through the tx's
	// event/rollback bookkeeping instead of an immediate dispatch.
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
