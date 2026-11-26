package types

// NullProbe is the type of Null.
type NullProbe struct{}

// Null is the probe value of a relationship property equality read
// (ByTypeAndProperty, its Ctx door and the temporal reads) that matches the
// relationships whose property is absent or nil. A relationship property
// index posts those under NullIndexKey, so the read goes through the index
// and its valid-window seek like any value. A plain nil still matches
// nothing. Null is never a stored value: a write carrying it is refused
// (ValidatePropertyValue).
var Null NullProbe

// NullIndexKey is the index key of Null: no IndexablePropertyValueKey of a
// stored value has its prefix.
const NullIndexKey = "null:"
