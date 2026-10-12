package badger

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func (bs *Store) getOrCreateRelTypeClassCounters(relTypeToken uint16, propertyKey string) *typeClassCounters {
	key := indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propertyKey}
	if v, ok := bs.relPropertyTypeClassCounts.Load(key); ok {
		return v.(*typeClassCounters)
	}
	v, _ := bs.relPropertyTypeClassCounts.LoadOrStore(key, &typeClassCounters{})
	return v.(*typeClassCounters)
}

// addRelPropertyTypeClassCounts counts a relationship row entering the current
// set into the per-(relType, propKey) type-class counters — the relationship
// mirror of adjustNodePropertyTypeClassCounts(+1); called at every door that
// makes a row current AND by the loadIndexes rebuild.
func (bs *Store) addRelPropertyTypeClassCounts(r *types.Relationship) {
	bs.adjustRelPropertyTypeClassCounts(r, 1)
}

// removeRelPropertyTypeClassCounts takes a row leaving the current set back
// out of the counters. The caller passes the row it removes (the delete seam's
// old row, the replace path's previous row), so the decrement is exactly the
// increment that row made and no per-relationship memo is kept (backlog 53).
func (bs *Store) removeRelPropertyTypeClassCounts(r *types.Relationship) {
	bs.adjustRelPropertyTypeClassCounts(r, -1)
}

func (bs *Store) adjustRelPropertyTypeClassCounts(r *types.Relationship, delta int64) {
	if bs.disablePlannerStats || r == nil {
		return
	}
	relType := r.TypeToken().Value()
	if relType == 0 {
		return
	}
	r.ForEachPropertyTypeClass(func(propertyKey string, class types.PropertyTypeClass) bool {
		if class >= types.NumPropertyTypeClasses {
			return true
		}
		bs.getOrCreateRelTypeClassCounters(relType, propertyKey).classes[class].Add(delta)
		return true
	})
}

// RelPropertyTypeClassCounts satisfies the optional
// store.RelPropertyTypeClassCountsCapability — the exact per-(relType, property key)
// partition of the type's current relationships by value class. The relationship
// mirror of NodePropertyTypeClassCounts (rule 2, BACKLOG 5B): the correctness gate for
// the rel ORDER BY r.prop LIMIT k push-down (ordering is sound only when the ordered
// class is unambiguous). Missing is 0 at this boundary (graph-layer computed).
// Unregistered pairs return the zero value, not an error. Negative intermediate reads
// (a concurrent remove observed before its paired add) clamp to 0.
func (bs *Store) RelPropertyTypeClassCounts(relTypeToken uint16, propertyKey string) (storecontract.PropertyTypeClassCounts, error) {
	if err := bs.checkOpen(); err != nil {
		return storecontract.PropertyTypeClassCounts{}, err
	}
	if bs.disablePlannerStats {
		return storecontract.PropertyTypeClassCounts{}, storecontract.ErrCapabilityNotSupported
	}
	if err := storecontract.ValidateRelTypeToken(relTypeToken); err != nil {
		return storecontract.PropertyTypeClassCounts{}, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return storecontract.PropertyTypeClassCounts{}, err
	}
	key := indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propertyKey}
	v, ok := bs.relPropertyTypeClassCounts.Load(key)
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
