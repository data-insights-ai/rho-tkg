package badger

import (
	"sync/atomic"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// typeClassCounters holds the per-(label, property key) exact node counts by
// types.PropertyTypeClass. Lock-free (mirrors the propertyKeyCounts
// atomic-counter pattern); each class slot is adjusted on the SAME
// node-mutation call as the presence counter (adjustNodePropertyKeyCounts),
// so exactness is inherited door-for-door — including the loadIndexes
// rebuild at open, which routes every current row through the same add door.
type typeClassCounters struct {
	classes [types.NumPropertyTypeClasses]atomic.Int64
}

func (bs *Store) getOrCreateTypeClassCounters(labelToken uint16, propertyKey string) *typeClassCounters {
	key := indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	if v, ok := bs.propertyTypeClassCounts.Load(key); ok {
		return v.(*typeClassCounters)
	}
	v, _ := bs.propertyTypeClassCounts.LoadOrStore(key, &typeClassCounters{})
	return v.(*typeClassCounters)
}

// adjustNodePropertyTypeClassCounts runs the full-property class sweep for one
// entering (delta>0) or leaving (delta<0) node row. Called ONLY from
// adjustNodePropertyKeyCounts — the single choke point every node-mutation
// door already funnels through.
func (bs *Store) adjustNodePropertyTypeClassCounts(n *types.Node, delta int64) {
	labelCount := n.LabelTokenCount()
	n.ForEachPropertyTypeClass(func(propertyKey string, class types.PropertyTypeClass) bool {
		if class >= types.NumPropertyTypeClasses {
			return true
		}
		for i := 0; i < labelCount; i++ {
			tok := n.LabelTokenRawAt(i)
			if tok == 0 {
				continue
			}
			bs.getOrCreateTypeClassCounters(tok, propertyKey).classes[class].Add(delta)
		}
		return true
	})
}

// NodePropertyTypeClassCounts satisfies the optional
// store.NodePropertyTypeClassCountsCapability — the exact O(1) per-(label,
// property key) type-class partition. Missing is always 0 at this boundary
// (graph-layer computed). Unregistered pairs return the zero value, not an
// error, mirroring NodeCountByLabelAndPropertyKey. Negative intermediate
// reads (a concurrent remove observed before its paired add) clamp to 0.
func (bs *Store) NodePropertyTypeClassCounts(labelToken uint16, propertyKey string) (storecontract.PropertyTypeClassCounts, error) {
	if err := bs.checkOpen(); err != nil {
		return storecontract.PropertyTypeClassCounts{}, err
	}
	if bs.disablePlannerStats {
		return storecontract.PropertyTypeClassCounts{}, storecontract.ErrCapabilityNotSupported
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return storecontract.PropertyTypeClassCounts{}, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return storecontract.PropertyTypeClassCounts{}, err
	}
	key := indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	v, ok := bs.propertyTypeClassCounts.Load(key)
	if !ok {
		return storecontract.PropertyTypeClassCounts{}, nil
	}
	c := v.(*typeClassCounters)
	load := func(class types.PropertyTypeClass) int64 {
		n := c.classes[class].Load()
		if n < 0 {
			return 0
		}
		return n
	}
	return storecontract.PropertyTypeClassCounts{
		Numeric: load(types.ClassNumeric),
		NaN:     load(types.ClassNaN),
		String:  load(types.ClassString),
		Bool:    load(types.ClassBool),
		Other:   load(types.ClassOther),
	}, nil
}

// ListCompositePropertyIndexes satisfies the optional
// store.CompositeIndexIntrospectionCapability: the declared, order-preserving
// key tuple of every composite definition under labelToken. Caller-owned
// copies; unregistered labels return an empty slice.
func (bs *Store) ListCompositePropertyIndexes(labelToken uint16) ([][]string, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return nil, err
	}
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	defs := bs.compositeIndexesByLabel[labelToken]
	out := make([][]string, 0, len(defs))
	for _, defKey := range defs {
		idx := bs.compositeIndexes[defKey]
		if idx == nil {
			continue
		}
		keys := make([]string, len(idx.Keys))
		copy(keys, idx.Keys)
		out = append(out, keys)
	}
	return out, nil
}
