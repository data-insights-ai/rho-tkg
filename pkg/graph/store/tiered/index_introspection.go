package tiered

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// Query-planner index introspection (BACKLOG 21b) for property indexes on reference
// labels, temporal and vector indexes; composite introspection lives in
// shard_index_fanout.go. Relationship property indexes are not implemented (tiered
// declines them), so callers get store.ErrCapabilityNotSupported.

var (
	_ storecontract.PropertyIndexIntrospectionCapability = (*Store)(nil)
	_ storecontract.TemporalIndexIntrospectionCapability = (*Store)(nil)
	_ storecontract.VectorIndexIntrospectionCapability   = (*Store)(nil)
)

// HasPropertyIndex reports whether a property index exists on (labelToken,
// propertyKey); property indexes live only on the reference shard.
func (ts *Store) HasPropertyIndex(labelToken uint16, propertyKey string) (bool, error) {
	if err := ts.checkOpen(); err != nil {
		return false, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return false, err
	}
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return false, err
	}
	defer refCheckin()
	return ref.HasPropertyIndex(labelToken, propertyKey)
}

// PropertyIndexOptions returns the options of the index on (labelToken,
// propertyKey) from the reference shard.
func (ts *Store) PropertyIndexOptions(labelToken uint16, propertyKey string) (storecontract.PropertyIndexOptions, bool, error) {
	if err := ts.checkOpen(); err != nil {
		return storecontract.PropertyIndexOptions{}, false, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return storecontract.PropertyIndexOptions{}, false, err
	}
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return storecontract.PropertyIndexOptions{}, false, err
	}
	defer refCheckin()
	return ref.PropertyIndexOptions(labelToken, propertyKey)
}

// HasTemporalIndex reports whether a temporal interval index exists on labelToken,
// from the store-level label list (membership is tracked once, store-wide).
func (ts *Store) HasTemporalIndex(labelToken uint16) (bool, error) {
	if err := ts.checkOpen(); err != nil {
		return false, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return false, err
	}
	ts.tempIdxMu.Lock()
	defer ts.tempIdxMu.Unlock()
	for _, tok := range ts.tempIdxLabels {
		if tok == labelToken {
			return true, nil
		}
	}
	return false, nil
}

// VectorIndexInfo returns the declared configuration of the vector index on
// (labelToken, propertyKey); vector indexes are store-level on tiered.
func (ts *Store) VectorIndexInfo(labelToken uint16, propertyKey string) (storecontract.VectorIndexInfo, bool, error) {
	if err := ts.checkOpen(); err != nil {
		return storecontract.VectorIndexInfo{}, false, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return storecontract.VectorIndexInfo{}, false, err
	}
	ts.vectorIdxMu.RLock()
	defer ts.vectorIdxMu.RUnlock()
	vi, ok := ts.vectorIndexes[indexpkg.VectorIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}]
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
