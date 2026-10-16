package badger

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// PropertyIndexDefs returns every finished node property index definition this
// store holds, label token -> property key -> options (an index still
// backfilling is not a definition yet). Caller-owned. The tiered store reads
// its reference shard's definitions through it to install them on the other
// shards (WP-31).
func (bs *Store) PropertyIndexDefs() (map[uint16]map[string]storecontract.PropertyIndexOptions, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	out := make(map[uint16]map[string]storecontract.PropertyIndexOptions, len(bs.propertyIndexes))
	for k, idx := range bs.propertyIndexes {
		addIndexDef(out, k.LabelToken, k.PropertyKey, idx)
	}
	return out, nil
}

// RelPropertyIndexDefs is PropertyIndexDefs for relationship property indexes,
// rel-type token -> property key -> options.
func (bs *Store) RelPropertyIndexDefs() (map[uint16]map[string]storecontract.PropertyIndexOptions, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	out := make(map[uint16]map[string]storecontract.PropertyIndexOptions, len(bs.relPropertyIndexes))
	for k, idx := range bs.relPropertyIndexes {
		addIndexDef(out, k.RelTypeToken, k.PropertyKey, idx)
	}
	return out, nil
}

func addIndexDef(out map[uint16]map[string]storecontract.PropertyIndexOptions, tok uint16, key string, idx *indexpkg.PropertyIndex) {
	if idx == nil || idx.Mutated != nil {
		return
	}
	if out[tok] == nil {
		out[tok] = make(map[string]storecontract.PropertyIndexOptions)
	}
	out[tok][key] = storecontract.PropertyIndexOptions{RangeCounts: idx.RangeCounts(), Elements: idx.Elements()}
}
