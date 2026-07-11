package core

import (
	"errors"
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ByTypeAndProperty returns relationships matching the rel type and property
// value — the relationship mirror of NodeOps.ByLabelAndProperty. Resolves the
// type name to a token; returns nil if the type is not registered.
//
// Without a temporal filter, the call falls through to the store-level rel
// property index for O(matches) lookup when the backend implements
// RelPropertyIndexCapability; otherwise the graph layer falls back to a
// type-scan + property filter over the mandatory RelationshipsByType surface
// (so the query works on every backend, including the tiered store, which
// declines rel-property-index CREATION). When opts carries a temporal filter,
// the candidate set is the union of (rels currently matching type+property) and
// every known history ID; each candidate is resolved to its version overlapping
// the requested time and the predicate re-checked against that version, so a rel
// whose type and property held at the requested time is included even if a later
// version no longer matches.
func (r *RelOps) ByTypeAndProperty(typeName, key string, value any, opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := c.validateTemporalQueryOptsScan(opts); err != nil {
		return nil, err
	}
	if err := c.validateRelTypeQueryName(typeName); err != nil {
		return nil, err
	}
	if err := c.validateIndexPropertyKey(key); err != nil {
		return nil, err
	}
	if err := types.ValidatePropertyValue(value); err != nil {
		return nil, fmt.Errorf("graph: relationships by type and property value: %w", err)
	}
	var result []*types.Relationship
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.relsByTypeAndPropertyLocked(typeName, key, value, opts)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// relsByTypeAndPropertyLocked is the lock-free body of RelOps.ByTypeAndProperty.
// Callers must hold c.mu (R or W).
func (c *Core) relsByTypeAndPropertyLocked(typeName, key string, value any, opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	temporal := hasTemporalFilter(opts)
	var targetKey string
	if temporal {
		targetKey = indexpkg.PropertyValueKey(value)
	}
	tok, ok := c.lookupRelTypeQueryToken(typeName)
	if !ok {
		return nil, nil
	}
	if !temporal {
		return c.relsByTypeAndProperty(tok, key, value, opts)
	}
	if targetKey == "" {
		return nil, nil
	}
	currentMatching, err := c.relsByTypeAndProperty(tok, key, value, storepkg.QueryOpts{Depth: opts.Depth})
	if err != nil {
		return nil, err
	}
	currentIDs, err := c.relIDsFromTypeRows(tok, currentMatching)
	if err != nil {
		return nil, err
	}

	var result []*types.Relationship
	pred := func(r *types.Relationship) bool {
		if !r.HasTypeTokenRaw(tok) {
			return false
		}
		gotKey, found := r.IndexablePropertyValueKey(key)
		return found && gotKey == targetKey
	}
	if err := c.forEachRelCandidateIDByDepth(currentIDs, opts.Depth, func(id types.RelID) error {
		rel, err := c.findRelVersionForOpts(id, opts, pred)
		if err != nil {
			if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrRelNotFound) {
				return nil
			}
			return err
		}
		result = append(result, rel)
		return nil
	}); err != nil {
		return nil, err
	}
	storeutil.SortRelsByID(result)
	result = storeutil.PaginateRels(result, opts.After, opts.Limit)
	return result, nil
}

// relsByTypeAndProperty answers the (rel-type-token, property-key, value) query
// whether or not the store implements the accelerated rel property index. Exact
// in-tree stores and direct external implementations use
// RelationshipsByTypeAndProperty; wrappers and mandatory-only backends fall back
// to a type scan + property filter (the optional capability is index
// management/acceleration, not query correctness). Mirror of
// nodesByLabelAndProperty; see its empty-key contract note.
func (c *Core) relsByTypeAndProperty(tok uint16, key string, value any, opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	wantKey := indexpkg.PropertyValueKey(value)
	if wantKey == "" {
		// The query value itself is not canonicalisable — by contract, no matches.
		return nil, nil
	}
	if c.relPropertyQuery != nil {
		rels, err := c.relPropertyQuery.RelationshipsByTypeAndProperty(tok, key, value, opts)
		if err != nil {
			return nil, err
		}
		if !c.relPropertyQueryTrust {
			if err := validateRelsByTypeAndProperty(tok, key, wantKey, opts, rels); err != nil {
				return nil, err
			}
			rels = copyRelationshipRows(rels)
		}
		return rels, nil
	}
	// Fallback: type scan + property filter. Pagination is applied after
	// filtering (the property predicate can drop arbitrary elements).
	pageOpts := opts
	pageOpts.Limit = 0
	candidates, err := c.store.RelationshipsByType(tok, pageOpts)
	if err != nil {
		return nil, err
	}
	if !c.storeRowsTrust {
		if err := c.validateRelationshipsByTypePage(tok, pageOpts, candidates); err != nil {
			return nil, err
		}
	}
	out := make([]*types.Relationship, 0, len(candidates))
	for _, r := range candidates {
		gotKey, found := r.IndexablePropertyValueKey(key)
		if !found || gotKey == "" {
			continue
		}
		if gotKey != wantKey {
			continue
		}
		out = append(out, r)
	}
	out = storeutil.PaginateRels(out, opts.After, opts.Limit)
	if !c.storeRowsTrust {
		out = copyRelationshipRows(out)
	}
	return out, nil
}
