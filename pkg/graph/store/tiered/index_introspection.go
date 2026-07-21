package tiered

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// Query-planner index-existence/config introspection (BACKLOG 21b) for the
// three index kinds the tiered store actually supports (property indexes on
// reference labels, temporal indexes, vector indexes). Relationship property
// indexes and composite indexes are NOT implemented here: tiered declines
// both entirely (see RelPropertyIndexCapability / CompositeIndexCapability
// doc comments), so it correctly does not satisfy
// RelPropertyIndexIntrospectionCapability or
// CompositeIndexIntrospectionCapability — the core-level type assertion
// fails and callers get store.ErrCapabilityNotSupported, consistent with
// Create/DropRelPropertyIndex's own decline.

var (
	_ storecontract.PropertyIndexIntrospectionCapability = (*Store)(nil)
	_ storecontract.TemporalIndexIntrospectionCapability = (*Store)(nil)
	_ storecontract.VectorIndexIntrospectionCapability   = (*Store)(nil)
)

// HasPropertyIndex reports whether a property index exists on (labelToken,
// propertyKey). Property indexes on tiered live only on the reference shard
// (CreatePropertyIndex rejects event labels), so this delegates there.
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

// HasTemporalIndex reports whether a temporal interval index exists on
// labelToken, reading the store-level label list CreateTemporalIndex
// maintains (the index itself fans out across every shard, but membership is
// tracked once, store-wide).
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
// (labelToken, propertyKey). Vector indexes are STORE-LEVEL on tiered (not
// per-shard), so this reads ts.vectorIndexes directly.
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
