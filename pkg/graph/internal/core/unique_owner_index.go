package core

import (
	"errors"
	"fmt"

	constraintspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/constraints"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Unique keys of tiered event labels (SHARD step 10, UNIQIDX). An event
// label's rows live on the shard of their day, so its unique check, its
// GetOrCreateByKey and its equality lookup cannot read one shard's property
// index the way a reference label's do. The store's owner index
// (storepkg.UniqueOwnerIndexCapability) holds the ID that last claimed each
// value. The graph declares it with the constraint, seeds it from the rows
// that exist, marks it ready, and claims a value under the value lock before
// the row holding it is written (enforceUniqueForNodeHeld). A lookup reads the
// claim and then the claimant's row, and ignores a claim the row does not back
// (a crash between claim and row, a rollback, a later change, a delete, a
// dropped shard), so the index never misses a holder and needs no replay.

// tieredEventOwnerIndex returns the owner index a unique constraint on label
// uses, or nil when the label needs none (not a tiered event label). It
// refuses UniqueForever on an event label (its claims live in one map in
// MetaKV) and a tiered store without the index.
func (c *Core) tieredEventOwnerIndex(label string, scope constraintspkg.UniqueScope) (storepkg.UniqueOwnerIndexCapability, error) {
	ts, ok := c.store.(*tiered.Store)
	if !ok || ts.IsReferenceLabel(label) {
		return nil, nil
	}
	if scope == constraintspkg.UniqueForever {
		return nil, fmt.Errorf("%w: label %q: UniqueForever on an event label", ErrUniqueEventLabelUnsupported, label)
	}
	oi, ok := c.store.(storepkg.UniqueOwnerIndexCapability)
	if !ok {
		return nil, fmt.Errorf("%w: label %q (values span unbounded time shards)", ErrUniqueEventLabelUnsupported, label)
	}
	return oi, nil
}

// ownerIndexFor is the store's owner index when (labelTok, key) is declared
// on it: writers claim there.
func (c *Core) ownerIndexFor(labelTok uint16, key string) (storepkg.UniqueOwnerIndexCapability, bool) {
	oi, ok := c.store.(storepkg.UniqueOwnerIndexCapability)
	if !ok || !oi.UniqueOwnerIndexDeclared(labelTok, key) {
		return nil, false
	}
	return oi, true
}

// readyOwnerIndex is ownerIndexFor when the index may answer lookups.
func (c *Core) readyOwnerIndex(labelTok uint16, key string) (storepkg.UniqueOwnerIndexCapability, bool) {
	oi, ok := c.ownerIndexFor(labelTok, key)
	if !ok || !oi.UniqueOwnerIndexReady(labelTok, key) {
		return nil, false
	}
	return oi, true
}

// nodesByOwnerIndex answers the current equality lookup of (tok, key) over
// every shard from the owner index: for each value form, the claimant whose
// current row carries tok and the value. Same rows as the label-wide lookup
// (each holder has the latest claim of its value), in ID order, paged by
// opts.After / opts.Limit.
func (c *Core) nodesByOwnerIndex(oi storepkg.UniqueOwnerIndexCapability, tok uint16, key string, forms []any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	seen := make(map[string]struct{}, len(forms))
	var out []*types.Node
	for _, f := range forms {
		vk := types.CanonicalIndexKey(types.IndexablePropertyValueKey(f))
		if _, dup := seen[vk]; dup || vk == "" {
			continue
		}
		seen[vk] = struct{}{}
		id, ok, err := oi.UniqueOwner(tok, key, vk)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		n, err := c.store.GetNode(id)
		if errors.Is(err, storepkg.ErrNodeNotFound) {
			continue // the claimant is gone: the claim backs nothing
		}
		if err != nil {
			return nil, err
		}
		if got, _ := uniqueValueKey(n, key); !n.HasLabelTokenRaw(tok) || got != vk {
			continue // the claimant's row no longer holds the value
		}
		out = append(out, n)
	}
	return uniteNodes(out, opts), nil
}

// seedOwnerIndex claims, for every current row of (labelTok, key), its value
// where no claim exists yet, then marks the index ready. Runs after the
// existing-data validation, beside writers that claim on their own.
func (c *Core) seedOwnerIndex(oi storepkg.UniqueOwnerIndexCapability, labelTok uint16, key string) error {
	var nodes []*types.Node
	if err := c.readUnderRLock(func() error {
		var err error
		nodes, err = c.store.NodesByLabel(labelTok, storepkg.QueryOpts{})
		return err
	}); err != nil {
		return fmt.Errorf("graph: seed owner index: %w", err)
	}
	for _, n := range nodes {
		vk, ok := uniqueValueKey(n, key)
		if !ok || vk == "" {
			continue
		}
		if err := oi.SeedUniqueOwner(labelTok, key, vk, n.ID()); err != nil {
			return err
		}
	}
	return oi.MarkUniqueOwnerIndexReady(labelTok, key)
}
