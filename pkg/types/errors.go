package types

import "errors"

// ErrNilNode is returned when a method with an error channel receives a nil *Node.
var ErrNilNode = errors.New("graph: node must not be nil")

// ErrNilRelationship is returned when a method with an error channel receives a nil *Relationship.
var ErrNilRelationship = errors.New("graph: relationship must not be nil")

// ErrNilPropertySlice is returned when a pointer-mutating PropertySlice method
// is called on a nil *PropertySlice.
var ErrNilPropertySlice = errors.New("types: property slice must not be nil")

// ErrFrozenNode is returned when an error-returning mutator is called on a
// frozen node. Frozen entities are shared store-cache entries; thaw with
// DeepCopy before mutating. Void/bool mutators panic instead.
var ErrFrozenNode = errors.New("types: node is frozen — DeepCopy it before mutating")

// ErrFrozenRelationship is the relationship counterpart of ErrFrozenNode.
var ErrFrozenRelationship = errors.New("types: relationship is frozen — DeepCopy it before mutating")
