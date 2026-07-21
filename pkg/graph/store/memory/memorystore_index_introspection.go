package memory

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// HasPropertyIndex satisfies the optional
// store.PropertyIndexIntrospectionCapability (BACKLOG 21b). Unregistered
// labels return false, not an error.
func (ms *Store) HasPropertyIndex(labelToken uint16, propertyKey string) (bool, error) {
	if ms == nil {
		return false, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return false, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return false, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return false, err
	}
	_, ok := ms.propertyIndexes[indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}]
	return ok, nil
}

// HasTemporalIndex satisfies the optional
// store.TemporalIndexIntrospectionCapability (BACKLOG 21b). Reports the
// interval-index KIND specifically — a high-frequency index on the same
// label does not count (see store.TemporalIndexIntrospectionCapability doc).
func (ms *Store) HasTemporalIndex(labelToken uint16) (bool, error) {
	if ms == nil {
		return false, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return false, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return false, err
	}
	_, ok := ms.temporalIndexes[labelToken]
	return ok, nil
}

// VectorIndexInfo satisfies the optional
// store.VectorIndexIntrospectionCapability (BACKLOG 21b): the declared
// dims/metric/engine/tuning of the vector index on (labelToken,
// propertyKey), or (zero value, false, nil) if none exists.
func (ms *Store) VectorIndexInfo(labelToken uint16, propertyKey string) (storecontract.VectorIndexInfo, bool, error) {
	if ms == nil {
		return storecontract.VectorIndexInfo{}, false, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return storecontract.VectorIndexInfo{}, false, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return storecontract.VectorIndexInfo{}, false, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return storecontract.VectorIndexInfo{}, false, err
	}
	vi, ok := ms.vectorIndexes[indexpkg.VectorIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}]
	if !ok || vi == nil {
		return storecontract.VectorIndexInfo{}, false, nil
	}
	return vectorIndexInfoOf(vi), true, nil
}

// vectorIndexInfoOf snapshots an *indexpkg.VectorIndex's declared
// configuration into the public store.VectorIndexInfo shape. Shared by every
// backend that stores the SAME indexpkg.VectorIndex type directly
// (memory/badger/tiered — see CLAUDE.md "Vector Indexes").
func vectorIndexInfoOf(vi *indexpkg.VectorIndex) storecontract.VectorIndexInfo {
	return storecontract.VectorIndexInfo{
		Dims:   vi.Dims,
		Metric: vi.Metric,
		Options: storecontract.VectorIndexOptions{
			UseBruteForce:  vi.BruteForce,
			M:              vi.HNSWM,
			EfConstruction: vi.HNSWEfConstruction,
			EfSearch:       vi.HNSWEfSearch,
		},
	}
}

// HasRelPropertyIndex satisfies the optional
// store.RelPropertyIndexIntrospectionCapability (BACKLOG 21b). Unregistered
// relationship types return false, not an error.
func (ms *Store) HasRelPropertyIndex(relTypeToken uint16, propertyKey string) (bool, error) {
	if ms == nil {
		return false, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return false, err
	}
	if err := storecontract.ValidateRelTypeToken(relTypeToken); err != nil {
		return false, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return false, err
	}
	_, ok := ms.relPropertyIndexes[indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propertyKey}]
	return ok, nil
}
