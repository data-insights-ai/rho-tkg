package memory

import (
	"fmt"
	"iter"
	"log/slog"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- Relationship property indexes (K3b) ---
//
// The rel mirror of memorystore_index.go's node indexes, keyed by rel-type token.
// RAM-only, rebuilt from current relationships on open; maintained at every rel
// mutation door (memorystore_rel.go, memorystore_history.go).

// CreateRelPropertyIndex creates a property index for the rel-type token and
// property key, in three phases as node CreatePropertyIndex (BACKLOG 17h), over
// the type's relationship IDs. Returns ErrIndexExists if it already exists.
func (ms *Store) CreateRelPropertyIndex(relTypeToken uint16, propertyKey string) error {
	return ms.CreateRelPropertyIndexWithOptions(relTypeToken, propertyKey, storecontract.PropertyIndexOptions{})
}

// CreateRelPropertyIndexWithOptions is CreateRelPropertyIndex with
// storecontract.PropertyIndexOptions.
func (ms *Store) CreateRelPropertyIndexWithOptions(relTypeToken uint16, propertyKey string, opts storecontract.PropertyIndexOptions) error {
	if ms == nil {
		return ErrNilStore
	}

	// Phase 1. checkOpenLocked before argument validation: a closed store reports
	// ErrStoreClosed even for invalid arguments (as node CreatePropertyIndex).
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
	if err := storecontract.ValidatePropertyIndexOptions(opts); err != nil {
		ms.mu.Unlock()
		return err
	}
	key := indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propertyKey}
	if _, exists := ms.relPropertyIndexes[key]; exists {
		ms.mu.Unlock()
		return ErrIndexExists
	}
	liveIdx := indexpkg.NewPropertyIndexWith(opts.RangeCounts, opts.Elements)
	liveIdx.Mutated = make(map[snowflake.ID]struct{})
	ms.relPropertyIndexes[key] = liveIdx
	var rids []types.RelID
	if idSet, ok := ms.typeIdx[relTypeToken]; ok {
		rids = make([]types.RelID, 0, len(idSet))
		for rid := range idSet {
			rids = append(rids, rid)
		}
	}
	refs, err := ms.appendSealedRefsLocked(nil, ms.segTypes[relTypeToken]) // ADR-0011: sealed rows too
	if err != nil {
		delete(ms.relPropertyIndexes, key)
		ms.mu.Unlock()
		return err
	}
	for _, ref := range refs {
		rids = append(rids, ref.id)
	}
	ms.mu.Unlock()

	// Phase 2.
	backfill := indexpkg.NewPostingsBuild(opts.Elements)
	for _, rid := range rids {
		ms.mu.RLock()
		r, ok, err := ms.relLocked(rid)
		ms.mu.RUnlock()
		phase2Yield() // test seam: ms.mu is provably unheld here
		if err != nil {
			ms.mu.Lock()
			if ms.relPropertyIndexes[key] == liveIdx {
				delete(ms.relPropertyIndexes, key)
			}
			ms.mu.Unlock()
			return err
		}
		if !ok {
			continue // deleted between snapshot and fetch
		}
		backfill.AddEntityKeys(rid.SnowflakeID(), propertyKey, r.ForEachPropertyIndexKey)
	}

	// Phase 3.
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if err := requireRelPropertyIndexCurrentForCreate(ms.relPropertyIndexes, key, liveIdx); err != nil {
		return err
	}
	return liveIdx.FinishBuild(backfill, func(id snowflake.ID) (bool, error) {
		return ms.relExistsLocked(types.RelID(id)) // a rel deleted during Phase 2 is skipped
	})
}

// requireRelPropertyIndexCurrentForCreate mirrors requirePropertyIndexCurrentForCreate
// for the rel-type-keyed index map.
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
	delete(ms.relPropTxMembers, key) // its membership sidecar goes with it
	return nil
}

// RelationshipsByTypeAndProperty returns relationships matching the type and
// property value (pagination, temporal filtering), sorted by snowflake.ID; uses
// the rel property index if one exists, else a type scan + property filter.
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
	if idx, ok := ms.relPropertyIndexes[key]; ok && idx.Mutated == nil {
		// The value's sorted posting list from opts.After on (backlog 72).
		ids := idx.Postings(value)
		if ids.Len() == 0 {
			return nil, nil
		}
		elements := idx.Elements()
		if ms.anySegmentsLocked() {
			rows, err := ms.relsFromSeqLocked(ids.After(opts.After.SnowflakeID()), opts.Limit, func(r *types.Relationship) bool {
				if !r.HasTypeTokenRaw(relTypeToken) {
					return false
				}
				if !r.HasPropertyIndexKey(propKey, elements, targetKey) {
					return false
				}
				return !opts.HasValidTimeFilter() || storepkg.MatchesTemporalFilter(r.ID().SnowflakeID(), r.Temporal(), opts)
			})
			if err != nil || len(rows) == 0 {
				return nil, err
			}
			return rows, nil
		}
		return ms.relsByTypePropertyFrom(relTypeToken, propKey, targetKey, elements, ids.After(opts.After.SnowflakeID()), ids.Len(), opts), nil
	}

	// Fallback: type scan + property filter, also while the index is still
	// building (its postings are partial), with the declared index's match.
	slog.Debug("graph: RelationshipsByTypeAndProperty using full type scan (no rel property index)",
		"relTypeToken", relTypeToken, "propertyKey", propKey)
	scanElements := ms.relPropertyIndexes[key].Elements()
	if ms.hasSegmentsLocked(relTypeToken) {
		rows, err := ms.typeRowsByIDLocked(relTypeToken, opts.After, opts.Limit, func(r *types.Relationship) bool {
			if !r.HasPropertyIndexKey(propKey, scanElements, targetKey) {
				return false
			}
			return !opts.HasValidTimeFilter() || storepkg.MatchesTemporalFilter(r.ID().SnowflakeID(), r.Temporal(), opts)
		})
		if err != nil || len(rows) == 0 {
			return nil, err
		}
		return rows, nil
	}
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
	return ms.relsByTypePropertyFrom(relTypeToken, propKey, targetKey, scanElements, idsAfter(matchIDs, opts.After), len(matchIDs), opts), nil
}

// relsByTypePropertyFrom is the rel mirror of nodesByLabelPropertyFrom.
func (ms *Store) relsByTypePropertyFrom(relTypeToken uint16, propKey, targetKey string, elements bool, ids iter.Seq[snowflake.ID], n int, opts QueryOpts) []*types.Relationship {
	hasTemporal := opts.HasValidTimeFilter()
	result := make([]*types.Relationship, 0, resultCap(opts.Limit, n))
	for raw := range ids {
		id := types.RelID(raw)
		if r, ok := ms.rels[id]; ok {
			if !r.HasTypeTokenRaw(relTypeToken) {
				continue
			}
			if !r.HasPropertyIndexKey(propKey, elements, targetKey) {
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

// ForEachRelByTypePropertyRange streams relationships of relTypeToken whose numeric
// propKey value lies within [min, max] (per the inclusivity flags), in snowflake-ID
// order. Candidates come from the index's ordered numeric view, which over-selects
// (float64 sort keys, ulp-widened bounds): fn must re-check exactly. Returns
// ErrIndexNotFound without a usable ordered view (callers fall back to a type scan).
//
// Isolation as ForEachRelByTypePropertyRangeOrdered (BACKLOG 17b): the candidate
// IDs are snapshotted under one brief RLock, each row is looked up under its own
// brief RLock, and fn runs with no lock held, so it may call back into the store.
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

	hasTemporal := opts.HasValidTimeFilter()
	emitted := 0
	for _, id := range ids {
		ms.mu.RLock()
		r, ok, err := ms.relLocked(id) // ADR-0011 union view
		ms.mu.RUnlock()
		if err != nil {
			return err
		}
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
