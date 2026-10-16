package core

import (
	"context"
	"fmt"

	eventspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// GetOrCreateByKey atomically returns the CURRENT node carrying value for
// (label, propertyKey), or creates one if none exists, under a single value lock:
// concurrent callers with the same key produce exactly one create. The bool
// reports whether THIS call created it. Works WITHOUT a unique constraint
// (ADR-0002); an active one is still honoured and shares the same stripe.
//
// Scope (backlog 9s): atomic only against callers in the value-stripe protocol
// (other GetOrCreateByKey calls, writes under an ACTIVE unique constraint). A
// PLAIN concurrent Add/Update of the same value with no constraint never touches
// the stripe and can land a second node; create a unique constraint on
// (label, propertyKey) to protect against every writer.
//
// value must be an indexable scalar (string / int / bool / Instant); floats are
// rejected with ErrUniqueUnsupportedType (lesson 25). extraProps are set on a
// created node (the keyed property wins) and ignored on a hit. The returned node
// is a mutable, independent copy (Get semantics).
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

	// Reject float (bit-pattern equality trap, lesson 25) and non-scalar values up front.
	valueKey := types.CanonicalIndexKey(types.IndexablePropertyValueKey(value))
	if valueKey == "" {
		return nil, false, fmt.Errorf("%w: GetOrCreateByKey value for key %q is not an indexable scalar", ErrUniqueUnsupportedType, propertyKey)
	}
	if isFloatValueKey(valueKey) {
		return nil, false, fmt.Errorf("%w: GetOrCreateByKey does not support float values (key %q)", ErrUniqueUnsupportedType, propertyKey)
	}

	// Resolve/persist the label token so the value stripe is stable.
	labelTok, err := c.getOrCreateLabelPersisted(label)
	if err != nil {
		return nil, false, err
	}
	if err := c.refuseNodeElementsIndex(labelTok, propertyKey, "GetOrCreateByKey"); err != nil {
		return nil, false, err
	}
	stripe := uniqueValueStripe(labelTok, propertyKey, valueKey)

	var (
		node    *types.Node
		created bool
		opErr   error
	)
	ep, closeErr := c.runUnderRLock(func() {
		// Hold the value stripe across lookup AND create (atomic check-then-create).
		ordered := c.valueLocks.LockStripes([]uint8{stripe})
		defer c.valueLocks.UnlockStripes(ordered)

		matches, err := c.nodesByLabelAndProperty(labelTok, propertyKey, value, storepkg.QueryOpts{})
		if err != nil {
			opErr = fmt.Errorf("graph: get-or-create lookup: %w", err)
			return
		}
		if len(matches) > 0 {
			// Hit: mutable copy of the lowest-ID match (scan rows are sorted by ID and frozen).
			node = matches[0].DeepCopy()
			c.opNodeReads.Add(1)
			return
		}

		// Miss: create under the SAME held stripe, passed down because it is non-reentrant.
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
