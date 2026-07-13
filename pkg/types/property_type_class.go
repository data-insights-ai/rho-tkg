package types

import "math"

// PropertyTypeClass partitions every storable property value into exactly one
// of five classes. The rule is deliberately COARSE and STABLE — it exists so a
// query planner can prove ordering-soundness facts ("every present value on
// this (label, key) is a finite number", "the gap between label count and
// numeric count is nulls only") from exact maintained counters, without
// scanning values:
//
//   - ClassNumeric — every integer kind (int, int8..64, uint, uint8..64) and
//     float32/float64 values that are NOT NaN. ±Inf is Numeric: it is
//     IEEE-orderable and sorts at the extremes, unlike NaN.
//   - ClassNaN — float32/float64 NaN. Split from Numeric because NaN is
//     unorderable; a numeric fast path that tolerates ±Inf still cannot
//     tolerate NaN.
//   - ClassString — string.
//   - ClassBool — bool.
//   - ClassOther — everything else the property allowlist admits: slices
//     (including []float32 and []byte), maps, and registered struct types.
//
// Absence ("Missing") is deliberately NOT a class — it is the difference
// between a label's node count and the sum of present classes, computed by the
// graph layer, never stored.
type PropertyTypeClass uint8

// The five classes, in counter-array order — see the PropertyTypeClass doc
// for the exact classification rule each one covers.
const (
	ClassNumeric PropertyTypeClass = iota
	ClassNaN
	ClassString
	ClassBool
	ClassOther
	// NumPropertyTypeClasses is the array size for per-class counters.
	NumPropertyTypeClasses
)

// classifyPropertyValue implements the PropertyTypeClass rule. It must stay
// total: every value the property allowlist admits maps to exactly one class.
func classifyPropertyValue(v any) PropertyTypeClass {
	switch x := v.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return ClassNumeric
	case float64:
		if math.IsNaN(x) {
			return ClassNaN
		}
		return ClassNumeric
	case float32:
		if math.IsNaN(float64(x)) {
			return ClassNaN
		}
		return ClassNumeric
	case string:
		return ClassString
	case bool:
		return ClassBool
	default:
		return ClassOther
	}
}

// forEachPropertyTypeClass calls fn once per property with its type class,
// stopping early when fn returns false. No value reference escapes — only the
// key and the class — so callers (store-side counter maintenance) never see
// internal state.
func (p PropertySlice) forEachPropertyTypeClass(fn func(key string, class PropertyTypeClass) bool) {
	for i := range p {
		if !fn(p[i].Key, classifyPropertyValue(p[i].Value)) {
			return
		}
	}
}

// ForEachPropertyTypeClass calls fn for EVERY property the node carries
// (indexable or not) with the property's PropertyTypeClass, stopping early
// when fn returns false. Unlike ForEachIndexablePropertyValueKey it skips
// nothing: non-indexable values (slices, maps, structs) report ClassOther.
// Only the key and the class are exposed — never the value.
func (n *Node) ForEachPropertyTypeClass(fn func(key string, class PropertyTypeClass) bool) {
	if n == nil {
		return
	}
	n.properties.forEachPropertyTypeClass(fn)
}

// ForEachPropertyTypeClass is the Relationship mirror of
// Node.ForEachPropertyTypeClass (structural mirrors — Testing Rule 2).
func (r *Relationship) ForEachPropertyTypeClass(fn func(key string, class PropertyTypeClass) bool) {
	if r == nil {
		return
	}
	r.properties.forEachPropertyTypeClass(fn)
}
