package memory

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// HasPropertyIndex satisfies store.PropertyIndexIntrospectionCapability (BACKLOG 21b).
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

// HasTemporalIndex satisfies store.TemporalIndexIntrospectionCapability (BACKLOG 21b);
// the interval kind only, a high-frequency index on the label does not count.
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

// VectorIndexInfo satisfies store.VectorIndexIntrospectionCapability (BACKLOG 21b):
// the declared config of the vector index, or (zero, false, nil).
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

// vectorIndexInfoOf snapshots an *indexpkg.VectorIndex's declared configuration;
// shared by every backend that stores that type directly.
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

// HasRelPropertyIndex satisfies store.RelPropertyIndexIntrospectionCapability (BACKLOG 21b).
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

var (
	_ storecontract.PropertyIndexOptionsCapability    = (*Store)(nil)
	_ storecontract.RelPropertyIndexOptionsCapability = (*Store)(nil)
)

// PropertyIndexOptions returns the options the index on (labelToken, propertyKey)
// was created with; ok=false when there is none.
func (ms *Store) PropertyIndexOptions(labelToken uint16, propertyKey string) (storecontract.PropertyIndexOptions, bool, error) {
	if ms == nil {
		return storecontract.PropertyIndexOptions{}, false, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return storecontract.PropertyIndexOptions{}, false, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return storecontract.PropertyIndexOptions{}, false, err
	}
	idx, ok := ms.propertyIndexes[indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}]
	return storecontract.PropertyIndexOptions{RangeCounts: idx.RangeCounts(), Elements: idx.Elements()}, ok, nil
}

// RelPropertyIndexOptions is the relationship mirror of PropertyIndexOptions.
func (ms *Store) RelPropertyIndexOptions(relTypeToken uint16, propertyKey string) (storecontract.PropertyIndexOptions, bool, error) {
	if ms == nil {
		return storecontract.PropertyIndexOptions{}, false, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return storecontract.PropertyIndexOptions{}, false, err
	}
	if err := storecontract.ValidateRelTypeToken(relTypeToken); err != nil {
		return storecontract.PropertyIndexOptions{}, false, err
	}
	idx, ok := ms.relPropertyIndexes[indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propertyKey}]
	return storecontract.PropertyIndexOptions{RangeCounts: idx.RangeCounts(), Elements: idx.Elements()}, ok, nil
}
