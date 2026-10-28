package memory

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

var _ storecontract.PropertyIndexStatsCapability = (*Store)(nil)

// PropertyIndexStats implements store.PropertyIndexStatsCapability (IDXSTATS):
// every property, relationship property and composite index, with its
// membership sidecar when built, walked under ms.mu's read lock.
func (ms *Store) PropertyIndexStats() ([]storecontract.PropertyIndexStat, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	out := make([]storecontract.PropertyIndexStat, 0, len(ms.propertyIndexes)+len(ms.relPropertyIndexes)+len(ms.compositeIndexes))
	for key, idx := range ms.propertyIndexes {
		out = append(out, indexStat(key.LabelToken, 0, []string{key.PropertyKey}, false, idx.OnDisk(), idx.Footprint(), ms.nodePropTxMembers[key].Footprint()))
	}
	for key, idx := range ms.relPropertyIndexes {
		out = append(out, indexStat(0, key.RelTypeToken, []string{key.PropertyKey}, false, idx.OnDisk(), idx.Footprint(), ms.relPropTxMembers[key].Footprint()))
	}
	for key, idx := range ms.compositeIndexes {
		out = append(out, indexStat(key.LabelToken, 0, append([]string(nil), idx.Keys...), true, false, idx.Footprint(), indexpkg.Footprint{}))
	}
	storepkg.SortPropertyIndexStats(out)
	return out, nil
}

// indexStat is the PropertyIndexStat of a definition whose entries take f and
// whose membership sidecar (zero when not built) takes members.
func indexStat(labelToken, relTypeToken uint16, keys []string, composite, onDisk bool, f, members indexpkg.Footprint) storecontract.PropertyIndexStat {
	return storecontract.PropertyIndexStat{
		LabelToken:     labelToken,
		RelTypeToken:   relTypeToken,
		Keys:           keys,
		Composite:      composite,
		OnDisk:         onDisk,
		Values:         f.Values,
		Postings:       f.Postings,
		MemberPostings: members.Postings,
		RAMBytes:       f.Bytes + members.Bytes,
	}
}
