package badger

import (
	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// relClassEntry is one property's classification for a relationship, memoized by rel
// ID (relTypeClassContrib) so the read-free deleteRelByInfo — which carries no
// property values — can decrement the type-class counters precisely by ID.
type relClassEntry struct {
	key   string
	class types.PropertyTypeClass
}

func (bs *Store) getOrCreateRelTypeClassCounters(relTypeToken uint16, propertyKey string) *typeClassCounters {
	key := indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propertyKey}
	if v, ok := bs.relPropertyTypeClassCounts.Load(key); ok {
		return v.(*typeClassCounters)
	}
	v, _ := bs.relPropertyTypeClassCounts.LoadOrStore(key, &typeClassCounters{})
	return v.(*typeClassCounters)
}

// addRelPropertyTypeClassCounts classifies a relationship's properties, increments the
// per-(relType, propKey) type-class counters, and MEMOIZES the classification by rel
// ID so a later delete-by-ID (deleteRelByInfo) can decrement precisely. The
// relationship mirror of adjustNodePropertyTypeClassCounts(+1); called at every
// full-rel-write ADD site AND the loadIndexes rebuild.
func (bs *Store) addRelPropertyTypeClassCounts(r *types.Relationship) {
	if bs.disablePlannerStats || r == nil {
		return
	}
	relType := r.TypeToken().Value()
	if relType == 0 {
		return
	}
	var contrib []relClassEntry
	r.ForEachPropertyTypeClass(func(propertyKey string, class types.PropertyTypeClass) bool {
		if class >= types.NumPropertyTypeClasses {
			return true
		}
		bs.getOrCreateRelTypeClassCounters(relType, propertyKey).classes[class].Add(1)
		contrib = append(contrib, relClassEntry{key: propertyKey, class: class})
		return true
	})
	if contrib != nil {
		bs.relTypeClassContrib.Store(r.ID().SnowflakeID(), contrib)
	}
}

// removeRelPropertyTypeClassCountsByID decrements the counters for the relationship
// identified by relID (relType from the caller's RelDeleteInfo / old row), using the
// memoized contribution — the SINGLE decrement seam every delete path funnels through
// (deleteRelByInfo) plus the replace-old path. A rel with no memoized contribution
// (no classifiable properties, or a sharded/tiered partial write that never populated
// it — those backends decline this capability) is a no-op.
func (bs *Store) removeRelPropertyTypeClassCountsByID(relID snowflake.ID, relType uint16) {
	if bs.disablePlannerStats {
		return
	}
	v, ok := bs.relTypeClassContrib.LoadAndDelete(relID)
	if !ok {
		return
	}
	for _, e := range v.([]relClassEntry) {
		bs.getOrCreateRelTypeClassCounters(relType, e.key).classes[e.class].Add(-1)
	}
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
