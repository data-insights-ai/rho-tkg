package core

import (
	"context"
	"fmt"

	eventspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// GetOrCreateByKey atomically returns the CURRENT node carrying value for
// (label, propertyKey), or creates one if none exists — under a single value
// lock so a storm of concurrent callers with the same key produces exactly one
// create and every other caller returns that same node (returned bool reports
// whether THIS call created it).
//
// The value lock alone makes the check-then-create atomic, so GetOrCreateByKey
// works WITHOUT an active unique constraint on (label, propertyKey) (ADR-0002).
// When such a constraint IS active it is still honoured (the created node passes
// the same kernel), and the two mechanisms share the same stripe so they
// serialize together.
//
// BACKLOG 9s — scope of the guarantee: atomicity holds against every caller
// that participates in the value-stripe protocol — other GetOrCreateByKey
// calls, and any write whose value is under an ACTIVE unique constraint
// (enforceUniqueForNode/enforceUniqueForNodeHeld take the same stripe). It
// does NOT serialize against a PLAIN concurrent Add/Update writing the same
// value when NO unique constraint exists on (label, propertyKey): value-
// stripe locking is unique-constraint-enforcement machinery, so a plain
// write with nothing constrained never touches the stripe and can land a
// second node holding the value even while GetOrCreateByKey believes its
// own check-then-create was atomic. Create a unique constraint on
// (label, propertyKey) if protection against EVERY writer (not just other
// GetOrCreateByKey callers) is required.
//
// value must be an indexable scalar (string / int / bool / Instant). Float
// values are rejected with ErrUniqueUnsupportedType (bit-pattern equality is a
// trap — lesson 25). extraProps are set on a freshly created node (the keyed
// property always wins over any same key in extraProps); they are ignored on a
// hit. The returned node is a mutable, independent copy (Get semantics).
func (n *NodeOps) GetOrCreateByKey(ctx context.Context, label, propertyKey string, value any, extraProps map[string]any) (*types.Node, bool, error) {
	return n.c.getOrCreateNodeByKey(ctx, label, propertyKey, value, extraProps)
}

func (c *Core) getOrCreateNodeByKey(ctx context.Context, label, propertyKey string, value any, extraProps map[string]any) (*types.Node, bool, error) {
	if err := c.checkWritable(); err != nil {
		return nil, false, err
	}
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
	// binds to it. Minting a token for a not-yet-used label is harmless.
	labelTok, err := c.getOrCreateLabelPersisted(label)
	if err != nil {
		return nil, false, err
	}
	stripe := uniqueValueStripe(labelTok, propertyKey, valueKey)

	var (
		node    *types.Node
		created bool
		opErr   error
	)
	ep, closeErr := c.runUnderRLock(func() {
		// Hold the value stripe across the lookup AND the create so the whole
		// check-then-create is atomic against every other caller of the same key.
		ordered := c.valueLocks.LockStripes([]uint8{stripe})
		defer c.valueLocks.UnlockStripes(ordered)

		matches, err := c.nodesByLabelAndProperty(labelTok, propertyKey, value, storepkg.QueryOpts{})
		if err != nil {
			opErr = fmt.Errorf("graph: get-or-create lookup: %w", err)
			return
		}
		if len(matches) > 0 {
			// Hit — return a mutable copy of the lowest-ID match (scan rows are
			// frozen and sorted by ID; determinism picks the first).
			node = matches[0].DeepCopy()
			c.opNodeReads.Add(1)
			return
		}

		// Miss — create under the SAME held stripe. Pass the held stripe so the
		// create's own unique kernel does not try to re-lock it (non-reentrant).
		props := make(map[string]any, len(extraProps)+1)
		for k, v := range extraProps {
			props[k] = v
		}
		props[propertyKey] = value
		node, opErr = c.addNodeInternal(ctx, []string{label}, props, stripe)
		created = opErr == nil
	})
	if closeErr != nil {
		return nil, false, closeErr
	}
	if opErr != nil {
		return nil, false, opErr
	}
	if created && node != nil && ep != nil {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventNodeCreate, EntityID: types.EntityID(node.ID()), Timestamp: c.now(), Priority: eventspkg.PriorityHigh})
	}
	return node, created, nil
}
