package badger

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

var _ storecontract.PropertyIndexStatsCapability = (*Store)(nil)

// PropertyIndexStats implements store.PropertyIndexStatsCapability (IDXSTATS):
// every property, relationship property and composite index, with its
// membership sidecar when built (a definition kept on disk has neither in RAM),
// walked under idxMu's read lock.
func (bs *Store) PropertyIndexStats() ([]storecontract.PropertyIndexStat, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	out := make([]storecontract.PropertyIndexStat, 0, len(bs.propertyIndexes)+len(bs.relPropertyIndexes)+len(bs.compositeIndexes))
	for key, idx := range bs.propertyIndexes {
		out = append(out, indexStat(key.LabelToken, 0, []string{key.PropertyKey}, false, idx.OnDisk(), idx.Footprint(), sidecarFootprint(bs.nodePropTx[key])))
	}
	for key, idx := range bs.relPropertyIndexes {
		out = append(out, indexStat(0, key.RelTypeToken, []string{key.PropertyKey}, false, idx.OnDisk(), idx.Footprint(), sidecarFootprint(bs.relPropTx[key])))
	}
	for key, idx := range bs.compositeIndexes {
		out = append(out, indexStat(key.LabelToken, 0, append([]string(nil), idx.Keys...), true, false, idx.Footprint(), indexpkg.Footprint{}))
	}
	storepkg.SortPropertyIndexStats(out)
	return out, nil
}

// sidecarFootprint is sc's members' footprint, zero without a sidecar (one a
// pass is still filling counts what it holds so far).
func sidecarFootprint[ID interface{ ~int64 }](sc *propTxSidecar[ID]) indexpkg.Footprint {
	if sc == nil {
		return indexpkg.Footprint{}
	}
	return sc.members.Footprint()
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
