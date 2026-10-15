package store

// PropertyBounds is the answer of the statistics door Stats().PropertyBounds
// (and its relationship mirror): the smallest and largest numeric value of a
// property key over a label's (or relationship type's) current entities, with
// the exact type-class partition of the same entities, all read at one moment.
//
// Min and Max are the ordered index's sort keys: the value itself for every
// integer (|v| <= 2^53) and float64, ±Inf included; a float32 sorts and is
// reported by its shortest decimal rendering (float32(0.1) is 0.1, as in every
// ordered numeric read). NaN takes no part (Classes.NaN counts it). MinValue
// and MaxValue are a stored value at Min and Max in its stored type; where
// values of several numeric types share the key (int64 3 and float64 3.0),
// the first present of float64, float32, int64, int, int32, int16, int8,
// uint64, uint, uint32, uint16, uint8. With no numeric value
// (Classes.Numeric == 0) Min, Max, MinValue and MaxValue are zero and nil.
type PropertyBounds struct {
	Min, Max           float64
	MinValue, MaxValue any
	// Classes is the exact partition of the label's (type's) current entities
	// by the class of the key's value, Missing included (entities without the
	// key), as Stats().PropertyTypeClassCounts states it.
	Classes PropertyTypeClassCounts
}

// HasNumeric reports whether at least one entity holds an orderable number
// (Min and Max are set).
func (b PropertyBounds) HasNumeric() bool { return b.Classes.Numeric > 0 }

// NumericOnly reports whether every entity that holds the key holds a number
// (NaN counts as a number; no string, bool, list, map or other value). True
// when no entity holds the key.
func (b PropertyBounds) NumericOnly() bool {
	return b.Classes.String == 0 && b.Classes.Bool == 0 && b.Classes.Other == 0
}

// NullFree reports whether every entity of the label (type) holds the key.
func (b PropertyBounds) NullFree() bool { return b.Classes.Missing == 0 }

// NodePropertyBoundsCapability is OPTIONAL: PropertyBounds for (label token,
// property key) from the store's ordered property index (its first and last
// numeric key) and its type-class and label counters, read under one lock so
// the parts agree. ok=false (no error) where the store cannot state the
// bounds exactly: no property index on the pair, an index being built, an
// Elements index, an index holding an integer of magnitude past 2^53, no RAM
// index (badger PropertyIndexOnDisk) or planner statistics disabled. memory
// and badger implement it.
type NodePropertyBoundsCapability interface {
	NodePropertyBounds(labelToken uint16, propertyKey string) (PropertyBounds, bool, error)
}

// RelPropertyBoundsCapability is the relationship mirror of
// NodePropertyBoundsCapability over a relationship type's property index
// (memory and badger).
type RelPropertyBoundsCapability interface {
	RelPropertyBounds(relTypeToken uint16, propertyKey string) (PropertyBounds, bool, error)
}
