package memory

import (
	"log/slog"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- Relationship property indexes (K3b) ---
//
// The relationship mirror of memorystore_index.go's node property indexes,
// keyed by rel-type token instead of label token. RAM-only (v1): rebuilt from
// current relationships on open (memory has no persistence). Maintained at every
// rel mutation door (see AddRelToPropertyIndexes / RemoveRelFromPropertyIndexes /
// PurgeRelFromAllPropertyIndexes call sites in memorystore_rel.go and
// memorystore_history.go).

// CreateRelPropertyIndex creates a property index for the given rel-type token
// and property key. Scans all existing relationships with that type to populate
// the index. Returns ErrIndexExists if the index already exists.
func (ms *Store) CreateRelPropertyIndex(relTypeToken uint16, propertyKey string) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelTypeToken(relTypeToken); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return err
	}

	key := indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propertyKey}
	if _, exists := ms.relPropertyIndexes[key]; exists {
		return ErrIndexExists
	}

	idx := indexpkg.NewPropertyIndex()

	// Populate from existing relationships with this type.
	if relIDs, ok := ms.typeIdx[relTypeToken]; ok {
		for relID := range relIDs {
			r := ms.rels[relID]
			if r == nil {
				continue
			}
			if valueKey, found := r.IndexablePropertyValueKey(propertyKey); found {
				idx.AddKey(relID.SnowflakeID(), valueKey)
			}
		}
	}

	ms.relPropertyIndexes[key] = idx
	return nil
}

// DropRelPropertyIndex removes a relationship property index.
// Returns ErrIndexNotFound if the index does not exist.
func (ms *Store) DropRelPropertyIndex(relTypeToken uint16, propertyKey string) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelTypeToken(relTypeToken); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return err
	}

	key := indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propertyKey}
	if _, exists := ms.relPropertyIndexes[key]; !exists {
		return ErrIndexNotFound
	}

	delete(ms.relPropertyIndexes, key)
	return nil
}

// RelationshipsByTypeAndProperty returns relationships matching the type and
// property value, with optional pagination and temporal filtering. Uses the
// rel property index if one exists; falls back to a type scan + property filter.
// Results are sorted by snowflake.ID for deterministic output.
func (ms *Store) RelationshipsByTypeAndProperty(relTypeToken uint16, propKey string, value any, opts QueryOpts) ([]*types.Relationship, error) {
	if ms == nil {
		return nil, ErrNilStore
	}

	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelTypeToken(relTypeToken); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propKey); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return nil, err
	}
	if err := types.ValidatePropertyValue(value); err != nil {
		return nil, err
	}
	targetKey := indexpkg.PropertyValueKey(value)
	if targetKey == "" {
		return nil, nil
	}

	key := indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propKey}
	if idx, ok := ms.relPropertyIndexes[key]; ok {
		ids := idx.RelIDs(value)
		if len(ids) == 0 {
			return nil, nil
		}
		storepkg.SortRelIDs(ids)
		return ms.relsByTypePropertyFromIDs(relTypeToken, propKey, targetKey, ids, opts), nil
	}

	// Fallback: type scan + property filter.
	slog.Debug("graph: RelationshipsByTypeAndProperty using full type scan (no rel property index)",
		"relTypeToken", relTypeToken, "propertyKey", propKey)
	typeIDs := ms.typeIdx[relTypeToken]
	if len(typeIDs) == 0 {
		return nil, nil
	}
	matchIDs := make([]types.RelID, 0, len(typeIDs))
	for id := range typeIDs {
		matchIDs = append(matchIDs, id)
	}
	if len(matchIDs) == 0 {
		return nil, nil
	}
	storepkg.SortRelIDs(matchIDs)
	return ms.relsByTypePropertyFromIDs(relTypeToken, propKey, targetKey, matchIDs, opts), nil
}

func (ms *Store) relsByTypePropertyFromIDs(relTypeToken uint16, propKey, targetKey string, ids []types.RelID, opts QueryOpts) []*types.Relationship {
	ids = storepkg.PaginateRelIDs(ids, opts.After, 0)
	if len(ids) == 0 {
		return nil
	}

	hasTemporal := storepkg.HasTemporalFilter(opts)
	capHint := len(ids)
	if opts.Limit > 0 && opts.Limit < capHint {
		capHint = opts.Limit
	}
	result := make([]*types.Relationship, 0, capHint)
	for _, id := range ids {
		if r, ok := ms.rels[id]; ok {
			if !r.HasTypeTokenRaw(relTypeToken) {
				continue
			}
			if valueKey, found := r.IndexablePropertyValueKey(propKey); !found || valueKey != targetKey {
				continue
			}
			if hasTemporal && !storepkg.MatchesTemporalFilter(id.SnowflakeID(), r.Temporal(), opts) {
				continue
			}
			result = append(result, r)
			if opts.Limit > 0 && len(result) >= opts.Limit {
				break
			}
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// ForEachRelByTypePropertyRange streams relationships carrying relTypeToken whose
// NUMERIC propKey value lies within [min, max] (per the inclusivity flags), in
// snowflake-ID order. Candidates come from the rel property index's ordered
// numeric view, which OVER-SELECTS by design (float64 sort keys, ulp-widened
// bounds): fn must re-check its predicate with exact comparison semantics.
// Returns ErrIndexNotFound when no rel property index with a usable ordered view
// exists for (relType, propKey) — the caller falls back to a type scan.
func (ms *Store) ForEachRelByTypePropertyRange(relTypeToken uint16, propKey string, min, max float64, inclMin, inclMax bool, opts QueryOpts, fn func(*types.Relationship) bool) error {
	if ms == nil {
		return ErrNilStore
	}
	if fn == nil {
		return storecontract.ErrInvalidStoreMutation
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelTypeToken(relTypeToken); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propKey); err != nil {
		return err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return err
	}

	key := indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propKey}
	idx, ok := ms.relPropertyIndexes[key]
	if !ok {
		return ErrIndexNotFound
	}
	ids, supported := idx.RangeRelIDs(min, max, inclMin, inclMax)
	if !supported {
		return ErrIndexNotFound
	}
	if len(ids) == 0 {
		return nil
	}
	storepkg.SortRelIDs(ids)
	ids = storepkg.PaginateRelIDs(ids, opts.After, 0)

	hasTemporal := storepkg.HasTemporalFilter(opts)
	emitted := 0
	for _, id := range ids {
		r, ok := ms.rels[id]
		if !ok {
			continue
		}
		if !r.HasTypeTokenRaw(relTypeToken) {
			continue
		}
		if hasTemporal && !storepkg.MatchesTemporalFilter(id.SnowflakeID(), r.Temporal(), opts) {
			continue
		}
		if !fn(r) {
			return nil
		}
		emitted++
		if opts.Limit > 0 && emitted >= opts.Limit {
			return nil
		}
	}
	return nil
}
