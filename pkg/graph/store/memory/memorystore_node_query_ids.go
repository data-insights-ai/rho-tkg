package memory

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// NodeIDsByLabelAndPropertyKeys implements
// storecontract.NodePropertyKeysIDsCapability: NodesByLabelAndPropertyKeys'
// IDs from the index, without a row lookup or recheck (RHOMEMIDS). It
// answers when the lookup names no time coordinate and the (label, key) index
// is published (not building: Phase 2's postings are partial). The index
// holds exactly the keys its definition writes (an Elements definition each
// element's key, a plain one the value's key), one index per (label, key)
// with no shared keyspace, so its postings are the rows' match. A posting
// counts when the node carries the label (labelIdx; a deleted node carries
// none). Paged by opts.After and opts.Limit.
func (ms *Store) NodeIDsByLabelAndPropertyKeys(labelToken uint16, propKey string, valueKeys []string, opts QueryOpts) ([]types.NodeID, bool, error) {
	if ms == nil {
		return nil, false, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return nil, false, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return nil, false, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propKey); err != nil {
		return nil, false, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return nil, false, err
	}
	if opts.HasTimeCoordinate() {
		return nil, false, nil
	}
	idx, ok := ms.propertyIndexes[indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propKey}]
	if !ok || idx.Mutated != nil {
		return nil, false, nil
	}
	targetKeys := storepkg.DistinctIndexKeyMatch(valueKeys).Keys()
	if len(targetKeys) == 0 {
		return nil, true, nil
	}
	labelled := ms.labelIdx[labelToken]
	var nids []types.NodeID
	for id := range idx.PostingsOfKeys(targetKeys).After(opts.After.SnowflakeID()) {
		nid := types.NodeID(id)
		if _, ok := labelled[nid]; !ok {
			continue
		}
		nids = append(nids, nid)
		if opts.Limit > 0 && len(nids) >= opts.Limit {
			break
		}
	}
	return nids, true, nil
}
