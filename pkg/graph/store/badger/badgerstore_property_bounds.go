package badger

import (
	"sync/atomic"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// NodePropertyBounds satisfies store.NodePropertyBoundsCapability: the
// numeric bounds of (label, key) from the RAM property index's ordered view
// (first and last key), the type-class counters and the label count, read
// under idxMu's read lock (every node write moves the three under its write
// lock, so they agree). ok=false with PropertyIndexOnDisk (no RAM index),
// without an index on the pair, for an index that cannot state the bounds
// exactly (PropertyIndex.NumericBounds) and with planner statistics
// disabled. Deferred planner statistics are built first, as every
// statistics read does.
func (bs *Store) NodePropertyBounds(labelToken uint16, propertyKey string) (storecontract.PropertyBounds, bool, error) {
	if err := bs.checkOpen(); err != nil {
		return storecontract.PropertyBounds{}, false, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return storecontract.PropertyBounds{}, false, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return storecontract.PropertyBounds{}, false, err
	}
	if bs.disablePlannerStats {
		return storecontract.PropertyBounds{}, false, nil
	}
	if err := bs.ensurePlannerStats(); err != nil {
		return storecontract.PropertyBounds{}, false, err
	}
	key := indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	idx := bs.propertyIndexes[key]
	if idx == nil {
		return storecontract.PropertyBounds{}, false, nil
	}
	var classes storecontract.PropertyTypeClassCounts
	if v, ok := bs.propertyTypeClassCounts.Load(key); ok {
		classes = loadTypeClassCounts(v.(*typeClassCounters))
	}
	var total int64
	if v, ok := bs.labelCounts.Load(labelToken); ok {
		total = v.(*atomic.Int64).Load()
	}
	out, ok := indexpkg.PropertyBoundsOf(idx, classes, total)
	return out, ok, nil
}

// RelPropertyBounds is NodePropertyBounds over a relationship type's property
// index (in RAM in every mode), type-class counters and type count.
func (bs *Store) RelPropertyBounds(relTypeToken uint16, propertyKey string) (storecontract.PropertyBounds, bool, error) {
	if err := bs.checkOpen(); err != nil {
		return storecontract.PropertyBounds{}, false, err
	}
	if err := storecontract.ValidateRelTypeToken(relTypeToken); err != nil {
		return storecontract.PropertyBounds{}, false, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return storecontract.PropertyBounds{}, false, err
	}
	if bs.disablePlannerStats {
		return storecontract.PropertyBounds{}, false, nil
	}
	if err := bs.ensurePlannerStats(); err != nil {
		return storecontract.PropertyBounds{}, false, err
	}
	key := indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propertyKey}
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	idx := bs.relPropertyIndexes[key]
	if idx == nil {
		return storecontract.PropertyBounds{}, false, nil
	}
	var classes storecontract.PropertyTypeClassCounts
	if v, ok := bs.relPropertyTypeClassCounts.Load(key); ok {
		classes = loadTypeClassCounts(v.(*typeClassCounters))
	}
	var total int64
	if v, ok := bs.typeCounts.Load(relTypeToken); ok {
		total = v.(*atomic.Int64).Load()
	}
	out, ok := indexpkg.PropertyBoundsOf(idx, classes, total)
	return out, ok, nil
}

// loadTypeClassCounts reads one pair's class counters (a negative count,
// never expected under idxMu, reads as 0).
func loadTypeClassCounts(c *typeClassCounters) storecontract.PropertyTypeClassCounts {
	load := func(class types.PropertyTypeClass) int64 { return max(c.classes[class].Load(), 0) }
	return storecontract.PropertyTypeClassCounts{
		Numeric: load(types.ClassNumeric),
		NaN:     load(types.ClassNaN),
		String:  load(types.ClassString),
		Bool:    load(types.ClassBool),
		Other:   load(types.ClassOther),
	}
}

// StatesPropertyBounds implements store.PropertyBoundsStatedCapability: the
// configuration that switches the bounds off (planner statistics disabled;
// for nodes also the on-disk property index). A label in
// PropertyIndexOnDiskLabels declines per call (its index states none).
func (bs *Store) StatesPropertyBounds() (nodes, rels bool) {
	if bs == nil {
		return false, false
	}
	return !bs.disablePlannerStats && !bs.propIdxOnDisk, !bs.disablePlannerStats
}
