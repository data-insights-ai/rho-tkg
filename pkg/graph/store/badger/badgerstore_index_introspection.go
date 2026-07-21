package badger

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// HasPropertyIndex satisfies the optional
// store.PropertyIndexIntrospectionCapability (BACKLOG 21b). Unregistered
// labels return false, not an error.
func (bs *Store) HasPropertyIndex(labelToken uint16, propertyKey string) (bool, error) {
	if err := bs.checkOpen(); err != nil {
		return false, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return false, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return false, err
	}
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	_, ok := bs.propertyIndexes[indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}]
	return ok, nil
}

// HasTemporalIndex satisfies the optional
// store.TemporalIndexIntrospectionCapability (BACKLOG 21b). Reports the
// interval-index KIND specifically — a high-frequency index on the same
// label does not count (see store.TemporalIndexIntrospectionCapability doc).
func (bs *Store) HasTemporalIndex(labelToken uint16) (bool, error) {
	if err := bs.checkOpen(); err != nil {
		return false, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return false, err
	}
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	_, ok := bs.temporalIndexes[labelToken]
	return ok, nil
}

// VectorIndexInfo satisfies the optional
// store.VectorIndexIntrospectionCapability (BACKLOG 21b): the declared
// dims/metric/engine/tuning of the vector index on (labelToken,
// propertyKey), or (zero value, false, nil) if none exists. Reflects the
// live in-memory VectorIndex's config fields — on badger these are restored
// from the persisted definition at open (CLAUDE.md "Vector Indexes"), so
// this is correct across restarts too, not just within one process.
func (bs *Store) VectorIndexInfo(labelToken uint16, propertyKey string) (storecontract.VectorIndexInfo, bool, error) {
	if err := bs.checkOpen(); err != nil {
		return storecontract.VectorIndexInfo{}, false, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return storecontract.VectorIndexInfo{}, false, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return storecontract.VectorIndexInfo{}, false, err
	}
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	vi, ok := bs.vectorIndexes[indexpkg.VectorIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}]
	if !ok || vi == nil {
		return storecontract.VectorIndexInfo{}, false, nil
	}
	return storecontract.VectorIndexInfo{
		Dims:   vi.Dims,
		Metric: vi.Metric,
		Options: storecontract.VectorIndexOptions{
			UseBruteForce:  vi.BruteForce,
			M:              vi.HNSWM,
			EfConstruction: vi.HNSWEfConstruction,
			EfSearch:       vi.HNSWEfSearch,
		},
	}, true, nil
}

// HasRelPropertyIndex satisfies the optional
// store.RelPropertyIndexIntrospectionCapability (BACKLOG 21b). Unregistered
// relationship types return false, not an error.
func (bs *Store) HasRelPropertyIndex(relTypeToken uint16, propertyKey string) (bool, error) {
	if err := bs.checkOpen(); err != nil {
		return false, err
	}
	if err := storecontract.ValidateRelTypeToken(relTypeToken); err != nil {
		return false, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return false, err
	}
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	_, ok := bs.relPropertyIndexes[indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propertyKey}]
	return ok, nil
}
