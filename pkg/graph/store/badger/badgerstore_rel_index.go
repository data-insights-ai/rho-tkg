package badger

import (
	"errors"
	"fmt"
	"log/slog"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/vmihailenco/msgpack/v5"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Relationship property indexes (K3b) — the rel mirror of badgerstore_index.go's
// node property indexes, keyed by rel-type token. RAM-only v1: the value maps
// live entirely in RAM (bs.relPropertyIndexes), only the DEFINITIONS are
// persisted (RelPropIndexDefsKey), and the maps are rebuilt from current
// relationship state at open (loadIndexes). There is no on-disk value keyspace;
// a 0x0C rel keyspace disk mode mirroring the node 0x0A PropertyIndexOnDisk mode
// is a documented follow-up.

// relPropIdxDef is the serialization format for relationship property index
// definitions (mirror of propIdxDef).
type relPropIdxDef struct {
	RelTypeToken uint16 `msgpack:"t"`
	PropertyKey  string `msgpack:"p"`
}

// CreateRelPropertyIndex creates a relationship property index for the given
// rel-type token and property key. Three-phase (mirror of CreatePropertyIndex)
// so slow backfill I/O does not block concurrent rel writes:
//
//	Phase 1 (idxMu.Lock): install an empty live index, snapshot the type's rel IDs.
//	Phase 2 (no lock): prefetch relationship data to build a backfill set.
//	Phase 3 (idxMu.Lock): merge backfill, skipping IDs a concurrent write already
//	  handled during Phase 2 and rels deleted meanwhile.
//
// Returns ErrIndexExists if the index already exists.
func (bs *Store) CreateRelPropertyIndex(relTypeToken uint16, propertyKey string) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelTypeToken(relTypeToken); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return err
	}

	// Phase 1: install empty live index + snapshot rel IDs under idxMu.Lock.
	bs.idxMu.Lock()
	key := indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propertyKey}
	if _, exists := bs.relPropertyIndexes[key]; exists {
		bs.idxMu.Unlock()
		return ErrIndexExists
	}
	liveIdx := indexpkg.NewPropertyIndex()
	liveIdx.Mutated = make(map[snowflake.ID]struct{})
	bs.relPropertyIndexes[key] = liveIdx
	rids := bs.relTypeRelIDsSnapshotLocked(relTypeToken)
	bs.idxMu.Unlock()

	// Phase 2: fetch relationship data OUTSIDE any lock.
	backfill := indexpkg.NewPropertyIndex()
	for _, rid := range rids {
		r, err := bs.prefetchRelScan(rid)
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue // deleted between snapshot and fetch
			}
			bs.idxMu.Lock()
			deleteRelPropertyIndexIfCurrent(bs.relPropertyIndexes, key, liveIdx)
			bs.idxMu.Unlock()
			return fmt.Errorf("graph: create relationship property index: %w", err)
		}
		valueKey, found := r.IndexablePropertyValueKey(propertyKey)
		if !found {
			continue
		}
		backfill.AddKey(rid.SnowflakeID(), valueKey)
	}

	// Phase 3: merge backfill into live index under idxMu.Lock.
	bs.idxMu.Lock()
	if err := requireRelPropertyIndexCurrentForCreate(bs.relPropertyIndexes, key, liveIdx); err != nil {
		bs.idxMu.Unlock()
		return err
	}
	for vk, idSet := range backfill.Entries {
		for id := range idSet {
			if _, mutated := liveIdx.Mutated[id]; mutated {
				continue // concurrent write handled this ID during Phase 2
			}
			if _, alive := bs.relIDs[types.RelID(id)]; !alive {
				continue // rel deleted during Phase 2
			}
			liveIdx.AddKey(id, vk)
		}
	}
	liveIdx.Mutated = nil // stop tracking — index creation complete
	bs.persistRelPropertyIndexDefs()
	bs.idxMu.Unlock()
	return bs.flushIfNeeded()
}

// DropRelPropertyIndex removes a relationship property index.
// Returns ErrIndexNotFound if the index does not exist.
func (bs *Store) DropRelPropertyIndex(relTypeToken uint16, propertyKey string) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelTypeToken(relTypeToken); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return err
	}

	bs.idxMu.Lock()
	key := indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propertyKey}
	if _, exists := bs.relPropertyIndexes[key]; !exists {
		bs.idxMu.Unlock()
		return ErrIndexNotFound
	}
	delete(bs.relPropertyIndexes, key)
	bs.persistRelPropertyIndexDefs()
	bs.idxMu.Unlock()
	return bs.flushIfNeeded()
}

// RelationshipsByTypeAndProperty returns relationships with the given rel-type
// token and property value, applying temporal/pagination options. Uses the rel
// property index if one exists; falls back to a type scan + property filter.
func (bs *Store) RelationshipsByTypeAndProperty(relTypeToken uint16, propKey string, value any, opts QueryOpts) ([]*types.Relationship, error) {
	if err := bs.checkOpen(); err != nil {
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

	bs.idxMu.RLock()
	key := indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propKey}
	if idx, ok := bs.relPropertyIndexes[key]; ok && idx.Mutated == nil {
		rids := idx.RelIDs(value)
		bs.idxMu.RUnlock()
		if len(rids) == 0 {
			return nil, nil
		}
		storepkg.SortRelIDs(rids)
		rids = bs.filterRelIDsByTemporalPeek(rids, opts)
		return bs.fetchRelsByTypePropertyIDs(relTypeToken, propKey, targetKey, rids, opts)
	}

	// Fallback: snapshot type IDs, release lock, then scan properties.
	set := bs.typeIdx[relTypeToken]
	rids := make([]types.RelID, 0, len(set))
	for id := range set {
		rids = append(rids, id)
	}
	bs.idxMu.RUnlock()
	if len(rids) == 0 {
		return nil, nil
	}
	storepkg.SortRelIDs(rids)
	return bs.fetchRelsByTypePropertyIDs(relTypeToken, propKey, targetKey, rids, opts)
}

func (bs *Store) fetchRelsByTypePropertyIDs(relTypeToken uint16, propKey, targetKey string, rids []types.RelID, opts QueryOpts) ([]*types.Relationship, error) {
	rids = storepkg.PaginateRelIDs(rids, opts.After, 0)
	if len(rids) == 0 {
		return nil, nil
	}
	hasTemporal := storepkg.HasTemporalFilter(opts)
	rels := make([]*types.Relationship, 0, capForLimit(opts.Limit))
	for _, rid := range rids {
		r, err := bs.prefetchRelScan(rid)
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue // orphaned index entry
			}
			return nil, fmt.Errorf("graph: query relationship %d: %w", rid.SnowflakeID(), err)
		}
		if !r.HasTypeTokenRaw(relTypeToken) {
			continue
		}
		if valueKey, found := r.IndexablePropertyValueKey(propKey); !found || valueKey != targetKey {
			continue
		}
		if hasTemporal && !storepkg.MatchesTemporalFilter(rid.SnowflakeID(), r.Temporal(), opts) {
			continue
		}
		rels = append(rels, r)
		if opts.Limit > 0 && len(rels) >= opts.Limit {
			break
		}
	}
	if len(rels) == 0 {
		return nil, nil
	}
	return rels, nil
}

// ForEachRelByTypePropertyRange streams the type's relationships whose NUMERIC
// propKey value lies within [min, max] (per the inclusivity flags) to fn in
// snowflake-ID order — the rel range-scan capability. Candidates come from the
// rel property index's ordered numeric view, which OVER-SELECTS by design, so
// fn receives CANDIDATES and MUST re-check the predicate with exact comparison
// semantics. fn returning false stops early. Returns ErrIndexNotFound when no
// usable rel property index exists for (relType, propKey) — callers fall back to
// a type scan.
func (bs *Store) ForEachRelByTypePropertyRange(relTypeToken uint16, propKey string, min, max float64, inclMin, inclMax bool, opts QueryOpts, fn func(*types.Relationship) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return storecontract.ErrInvalidStoreMutation
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return err
	}

	bs.idxMu.RLock()
	idx, ok := bs.relPropertyIndexes[indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propKey}]
	var rids []types.RelID
	supported := false
	if ok && idx.Mutated == nil {
		rids, supported = idx.RangeRelIDs(min, max, inclMin, inclMax)
	}
	bs.idxMu.RUnlock()
	if !ok || !supported {
		return storecontract.ErrIndexNotFound
	}
	if len(rids) == 0 {
		return nil
	}
	storepkg.SortRelIDs(rids)
	rids = storepkg.PaginateRelIDs(rids, opts.After, 0)

	hasTemporal := storepkg.HasTemporalFilter(opts)
	emitted := 0
	for _, rid := range rids {
		r, err := bs.prefetchRelScan(rid)
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue
			}
			return fmt.Errorf("graph: range-scan relationship %d: %w", rid.SnowflakeID(), err)
		}
		if !r.HasTypeTokenRaw(relTypeToken) {
			continue
		}
		if hasTemporal && !storepkg.MatchesTemporalFilter(rid.SnowflakeID(), r.Temporal(), opts) {
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

// relTypeRelIDsSnapshotLocked returns a snapshot of the type's current rel IDs.
// Caller holds idxMu. Unlike labels, the rel-type index is always a RAM map.
func (bs *Store) relTypeRelIDsSnapshotLocked(relTypeToken uint16) []types.RelID {
	set := bs.typeIdx[relTypeToken]
	if len(set) == 0 {
		return nil
	}
	rids := make([]types.RelID, 0, len(set))
	for id := range set {
		rids = append(rids, id)
	}
	return rids
}

// persistRelPropertyIndexDefs serializes the current rel property index
// definitions to Badger. Caller holds idxMu. Mirror of persistPropertyIndexDefs.
func (bs *Store) persistRelPropertyIndexDefs() {
	var defs []relPropIdxDef
	for key, idx := range bs.relPropertyIndexes {
		if idx == nil || idx.Mutated != nil {
			continue // still being created (Phase 2) — not yet durable
		}
		defs = append(defs, relPropIdxDef{RelTypeToken: key.RelTypeToken, PropertyKey: key.PropertyKey})
	}
	if len(defs) == 0 {
		bs.appendOps(writeOp{opType: writeOpDelete, key: storepkg.RelPropIndexDefsKey})
		return
	}
	data, err := msgpack.Marshal(defs)
	if err != nil {
		slog.Error("graph: persist relationship property index defs: marshal failed", "error", err)
		return // index still works in-memory; will retry on next change
	}
	bs.appendOps(writeOp{opType: writeOpSet, key: storepkg.RelPropIndexDefsKey, value: data})
}

// maintainRelPropertyIndexesAdd / Remove are the write-path maintenance entry
// points every rel-mutation door calls. RAM-only: they mutate the value maps
// directly under idxMu (the caller already holds it). No disk ops (unlike the
// node property index's disk mode).
func (bs *Store) maintainRelPropertyIndexesAdd(r *types.Relationship, id snowflake.ID) {
	indexpkg.AddRelToPropertyIndexes(bs.relPropertyIndexes, r, id)
}

func (bs *Store) maintainRelPropertyIndexesRemove(r *types.Relationship, id snowflake.ID) {
	indexpkg.RemoveRelFromPropertyIndexes(bs.relPropertyIndexes, r, id)
}

// maintainRelPropertyIndexesPurge is the shared-seam brute-force removal
// (deleteRelByInfo carries no property values). Caller holds idxMu.
func (bs *Store) maintainRelPropertyIndexesPurge(id snowflake.ID) {
	indexpkg.PurgeRelFromAllPropertyIndexes(bs.relPropertyIndexes, id)
}

func deleteRelPropertyIndexIfCurrent(idxs map[indexpkg.RelPropertyIndexKey]*indexpkg.PropertyIndex, key indexpkg.RelPropertyIndexKey, expected *indexpkg.PropertyIndex) {
	if cur, ok := idxs[key]; ok && cur == expected {
		delete(idxs, key)
	}
}

func requireRelPropertyIndexCurrentForCreate(idxs map[indexpkg.RelPropertyIndexKey]*indexpkg.PropertyIndex, key indexpkg.RelPropertyIndexKey, expected *indexpkg.PropertyIndex) error {
	cur, ok := idxs[key]
	if !ok || cur != expected {
		// The index was dropped (or replaced) during Phase 2 — abort the create.
		return ErrIndexNotFound
	}
	return nil
}
