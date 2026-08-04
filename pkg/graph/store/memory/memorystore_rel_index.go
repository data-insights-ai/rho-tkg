package memory

import (
	"fmt"
	"log/slog"

	snowflake "github.com/bds421/rho-snowflake-2026"
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
// and property key. Three-phase approach mirroring node CreatePropertyIndex
// (BACKLOG 17h) — see its doc comment for the full rationale; the rel mirror
// snapshots the type's relationship IDs instead of a label's node IDs, and
// Phase 2 reads *types.Relationship rows (also frozen/immutable-once-cached,
// same safety argument). Returns ErrIndexExists if the index already exists.
func (ms *Store) CreateRelPropertyIndex(relTypeToken uint16, propertyKey string) error {
	if ms == nil {
		return ErrNilStore
	}

	// Phase 1. checkOpenLocked before argument validation — a closed store
	// must report ErrStoreClosed even for otherwise-invalid arguments,
	// mirroring node CreatePropertyIndex's lifecycle-before-validation order.
	ms.mu.Lock()
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.Unlock()
		return err
	}
	if err := storecontract.ValidateRelTypeToken(relTypeToken); err != nil {
		ms.mu.Unlock()
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		ms.mu.Unlock()
		return err
	}
	key := indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propertyKey}
	if _, exists := ms.relPropertyIndexes[key]; exists {
		ms.mu.Unlock()
		return ErrIndexExists
	}
	liveIdx := indexpkg.NewPropertyIndex()
	liveIdx.Mutated = make(map[snowflake.ID]struct{})
	ms.relPropertyIndexes[key] = liveIdx
	var rids []types.RelID
	if idSet, ok := ms.typeIdx[relTypeToken]; ok {
		rids = make([]types.RelID, 0, len(idSet))
		for rid := range idSet {
			rids = append(rids, rid)
		}
	}
	ms.mu.Unlock()

	// Phase 2.
	backfill := indexpkg.NewPropertyIndex()
	for _, rid := range rids {
		ms.mu.RLock()
		r, ok := ms.rels[rid]
		ms.mu.RUnlock()
		phase2Yield() // test seam: ms.mu is provably unheld here
		if !ok {
			continue // deleted between snapshot and fetch
		}
		if valueKey, found := r.IndexablePropertyValueKey(propertyKey); found {
			backfill.AddKey(rid.SnowflakeID(), valueKey)
		}
	}

	// Phase 3.
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if err := requireRelPropertyIndexCurrentForCreate(ms.relPropertyIndexes, key, liveIdx); err != nil {
		return err
	}
	for vk, idSet := range backfill.Entries {
		for id := range idSet {
			if _, mutated := liveIdx.Mutated[id]; mutated {
				continue // concurrent write handled this ID during Phase 2
			}
			if _, alive := ms.rels[types.RelID(id)]; !alive {
				continue // relationship deleted during Phase 2
			}
			liveIdx.AddKey(id, vk)
		}
	}
	liveIdx.Mutated = nil // stop tracking — index creation complete
	return nil
}

// requireRelPropertyIndexCurrentForCreate mirrors node CreatePropertyIndex's
// requirePropertyIndexCurrentForCreate for the rel-type-keyed index map.
func requireRelPropertyIndexCurrentForCreate(idxs map[indexpkg.RelPropertyIndexKey]*indexpkg.PropertyIndex, key indexpkg.RelPropertyIndexKey, expected *indexpkg.PropertyIndex) error {
	current := idxs[key]
	if current == expected {
		return nil
	}
	if current == nil {
		return fmt.Errorf("graph: create rel property index: index dropped during creation: %w", ErrIndexNotFound)
	}
	return fmt.Errorf("graph: create rel property index: index replaced during creation: %w", ErrIndexExists)
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
//
// Isolation mirrors ForEachRelByTypePropertyRangeOrdered (BACKLOG 17b): the
// candidate ID set is snapshotted under one brief store RLock, then each row is
// looked up under its OWN brief RLock, and fn runs with NO lock held — fn may
// freely call back into the store. Every sibling streaming method already
// followed this two-phase pattern (matching the documented IterationCapability
// contract and sync.RWMutex's non-reentrancy); this was the one door that held
// the lock across the whole callback loop, a self-deadlock hazard if fn ever
// re-entered any Store read method while a writer was waiting for the RLock
// (a waiting writer blocks NEW readers too, so a reentrant RLock from inside fn
// would never acquire).
func (ms *Store) ForEachRelByTypePropertyRange(relTypeToken uint16, propKey string, min, max float64, inclMin, inclMax bool, opts QueryOpts, fn func(*types.Relationship) bool) error {
	if ms == nil {
		return ErrNilStore
	}
	if fn == nil {
		return storecontract.ErrInvalidStoreMutation
	}

	ms.mu.RLock()
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.RUnlock()
		return err
	}
	if err := storecontract.ValidateRelTypeToken(relTypeToken); err != nil {
		ms.mu.RUnlock()
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propKey); err != nil {
		ms.mu.RUnlock()
		return err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		ms.mu.RUnlock()
		return err
	}

	key := indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propKey}
	idx, ok := ms.relPropertyIndexes[key]
	if !ok {
		ms.mu.RUnlock()
		return ErrIndexNotFound
	}
	ids, supported := idx.RangeRelIDs(min, max, inclMin, inclMax)
	ms.mu.RUnlock()
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
		ms.mu.RLock()
		r, ok := ms.rels[id]
		ms.mu.RUnlock()
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
