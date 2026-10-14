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
// value (rel mirror of NodeOps.ByLabelAndProperty); nil if the type is not
// registered.
//
// Without a temporal filter it uses the store rel property index when the backend
// implements RelPropertyIndexCapability, else a type scan + filter over
// RelationshipsByType (so it works on every backend, including tiered). With a
// temporal filter the candidates are the current matches plus the rels whose rows
// ever carried the value (membership sidecar, backlog 8; else every known history
// ID), each re-checked at its version at the requested time.
//
// DECLARED view under a temporal filter: each relationship's own asserted validity
// only, NOT masked by endpoint validity. Use Temporal().Snapshot / NeighborsAt /
// OutgoingRelsAt for the EFFECTIVE graph view.
func (r *RelOps) ByTypeAndProperty(typeName, key string, value any, opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := c.validateTemporalQueryOptsScan(opts, c.relTypeRetentionScope(typeName)); err != nil {
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

	elements, err := c.relIndexElements(tok, key)
	if err != nil {
		return nil, err
	}
	var result []*types.Relationship
	pred := func(r *types.Relationship) bool {
		return r.HasTypeTokenRaw(tok) && r.HasPropertyIndexKey(key, elements, targetKey)
	}
	resolveOpts := c.normalizeTxAtOnlyOpts(opts)
	if err := c.forEachRelPropertyCandidateID(tok, key, targetKey, currentIDs, opts, func(id types.RelID) error {
		rel, err := c.findRelVersionForOpts(id, resolveOpts, pred)
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
// with or without the accelerated index: RelationshipsByTypeAndProperty where
// implemented, else type scan + filter. Exact in-tree stores use
// RelationshipsByTypeAndProperty; wrappers and mandatory-only backends fall back to
// a type scan + filter. Mirror of nodesByLabelAndProperty.
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
			elements, err := c.relIndexElements(tok, key)
			if err != nil {
				return nil, err
			}
			if err := validateRelsByTypeAndProperty(tok, key, wantKey, elements, opts, rels); err != nil {
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
	elements, err := c.relIndexElements(tok, key)
	if err != nil {
		return nil, err
	}
	out := make([]*types.Relationship, 0, len(candidates))
	for _, r := range candidates {
		if r.HasPropertyIndexKey(key, elements, wantKey) {
			out = append(out, r)
		}
	}
	out = storeutil.PaginateRels(out, opts.After, opts.Limit)
	if !c.storeRowsTrust {
		out = copyRelationshipRows(out)
	}
	return out, nil
}

// relPropertyCurrentIDs returns the IDs of relationships whose CURRENT row matches
// (type, key = value), the seed of the temporal property doors. A current rel that
// does not match now and has no other row can match at no time; the rest come from
// history (forEachRelPropertyCandidateID).
func (c *Core) relPropertyCurrentIDs(tok uint16, key string, value any) ([]types.RelID, error) {
	current, err := c.relsByTypeAndProperty(tok, key, value, storepkg.QueryOpts{})
	if err != nil {
		return nil, err
	}
	return c.relIDsFromTypeRows(tok, current)
}
