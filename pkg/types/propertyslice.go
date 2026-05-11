package types

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
)

// ErrReservedPrefix is returned when a property key uses the reserved "tkg_" prefix.
var ErrReservedPrefix = errors.New("types: property key uses reserved tkg_ prefix")

// ErrUnsupportedValueType is returned when a property value contains a type
// not on the exact allowlist consumed by hashing, copying, and wire encoding.
// Graph databases store data, not application memory references.
var ErrUnsupportedValueType = errors.New("types: unsupported property value type")

// ErrUnsupportedMapType is returned when a property value is a map whose
// key/value type is not supported by hashing and wire serialization. Only
// map[string]any and map[string]string are accepted; everything else would
// either panic at hash time or fail to round-trip through msgpack export.
var ErrUnsupportedMapType = errors.New("types: unsupported map type — only map[string]any and map[string]string are supported")

// ErrMaxDepthExceeded is returned when a property value exceeds the maximum
// nesting depth. This prevents stack overflow from self-referential or
// excessively deep structures.
var ErrMaxDepthExceeded = errors.New("types: property value exceeds maximum nesting depth")

// maxPropertyDepth is the maximum nesting depth for property values.
// Validation and deep-copy functions stop recursing beyond this limit.
const maxPropertyDepth = 32

var (
	propertyTypeBool    = reflect.TypeOf(true)
	propertyTypeString  = reflect.TypeOf("")
	propertyTypeInt     = reflect.TypeOf(int(0))
	propertyTypeInt8    = reflect.TypeOf(int8(0))
	propertyTypeInt16   = reflect.TypeOf(int16(0))
	propertyTypeInt32   = reflect.TypeOf(int32(0))
	propertyTypeInt64   = reflect.TypeOf(int64(0))
	propertyTypeUint    = reflect.TypeOf(uint(0))
	propertyTypeUint8   = reflect.TypeOf(uint8(0))
	propertyTypeUint16  = reflect.TypeOf(uint16(0))
	propertyTypeUint32  = reflect.TypeOf(uint32(0))
	propertyTypeUint64  = reflect.TypeOf(uint64(0))
	propertyTypeFloat32 = reflect.TypeOf(float32(0))
	propertyTypeFloat64 = reflect.TypeOf(float64(0))

	propertyTypeSliceString  = reflect.TypeOf([]string(nil))
	propertyTypeSliceInt     = reflect.TypeOf([]int(nil))
	propertyTypeSliceInt64   = reflect.TypeOf([]int64(nil))
	propertyTypeSliceFloat32 = reflect.TypeOf([]float32(nil))
	propertyTypeSliceFloat64 = reflect.TypeOf([]float64(nil))
	propertyTypeSliceByte    = reflect.TypeOf([]byte(nil))
	propertyTypeSliceBool    = reflect.TypeOf([]bool(nil))
	propertyTypeSliceAny     = reflect.TypeOf([]any(nil))

	propertyTypeMapStringAny    = reflect.TypeOf(map[string]any(nil))
	propertyTypeMapStringString = reflect.TypeOf(map[string]string(nil))
)

// Property is a single key-value property entry.
type Property struct {
	Key   string
	Value any
}

// PropertySlice is a sorted slice of Property entries.
// The sorted invariant is maintained by Set; never modify entries directly.
type PropertySlice []Property

// OwnedPropertySlice is an already validated, canonical property slice whose
// ownership can be transferred into an entity without another defensive copy.
// The unexported field prevents callers from fabricating one around unchecked
// data; use NewOwnedPropertySlice to construct it.
type OwnedPropertySlice struct {
	ps PropertySlice
}

// Set inserts or overwrites the property at key.
// Returns an error if key has the reserved "tkg_" prefix.
// Recursively validates the value using an exact allowlist — only the concrete
// scalar, slice, and map types handled by hashing/copy/wire are accepted. All
// other types (pointers, structs, arrays, channels, functions, aliases, etc.)
// are rejected at any nesting depth unless explicitly registered as custom
// property struct types.
// Returns ErrMaxDepthExceeded if nesting exceeds maxPropertyDepth (32).
// Stores a deep copy of accepted values and maintains the sorted-by-key invariant.
//
// For bulk construction, prefer NewPropertySlice (O(N log N)) over repeated
// Set calls (O(N) per call due to binary-search insertion into a sorted slice).
func (ps *PropertySlice) Set(key string, value any) error {
	if ps == nil {
		return ErrNilPropertySlice
	}
	if IsShadowKey(key) {
		return fmt.Errorf("%w: %q", ErrReservedPrefix, key)
	}
	if err := ValidatePropertyValue(value); err != nil {
		return fmt.Errorf("%w: %q (got %T)", err, key, value)
	}
	copied, err := copyValidatedPropertyValue(key, value)
	if err != nil {
		return err
	}
	i := sort.Search(len(*ps), func(i int) bool {
		return (*ps)[i].Key >= key
	})
	if i < len(*ps) && (*ps)[i].Key == key {
		(*ps)[i].Value = copied
		return nil
	}
	*ps = append(*ps, Property{})
	copy((*ps)[i+1:], (*ps)[i:])
	(*ps)[i] = Property{Key: key, Value: copied}
	return nil
}

func copyValidatedPropertyValue(key string, value any) (copied any, err error) {
	if isScalarPropertyValue(value) {
		return value, nil
	}
	defer func() {
		if r := recover(); r != nil {
			copied = nil
			err = fmt.Errorf("%w: %q deep copy panic: %v", ErrUnsupportedValueType, key, r)
		}
	}()
	copied = deepCopyValue(value, 0)
	if err := ValidatePropertyValue(copied); err != nil {
		return nil, fmt.Errorf("%w: %q after deep copy (got %T)", err, key, copied)
	}
	return copied, nil
}

func isScalarPropertyValue(v any) bool {
	switch v.(type) {
	case nil,
		bool, string,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return true
	default:
		return false
	}
}

// Get returns the value for key and whether it was found.
// Uses binary search on the sorted slice.
func (ps PropertySlice) Get(key string) (any, bool) {
	i := sort.Search(len(ps), func(i int) bool {
		return ps[i].Key >= key
	})
	if i < len(ps) && ps[i].Key == key {
		return ps[i].Value, true
	}
	return nil, false
}

// DeepCopy returns a copy of the slice (independent from the original).
// Reference-type values (slices, maps) are cloned so the copy is fully
// independent. Primitives, strings, and other value types are safe as-is.
func (ps PropertySlice) DeepCopy() PropertySlice {
	if ps == nil {
		return nil
	}
	cp := make(PropertySlice, len(ps))
	for i, p := range ps {
		cp[i] = Property{Key: p.Key, Value: deepCopyValue(p.Value, 0)}
	}
	return cp
}

// ValidatePropertyValue recursively checks that v contains only allowlisted
// exact types at any nesting depth. Slices, maps, and interface wrappers are
// traversed.
// Rejects at maxPropertyDepth to prevent stack overflow from deep/cyclic input.
func ValidatePropertyValue(v any) error {
	if v == nil {
		return nil
	}
	return validateReflectValue(reflect.ValueOf(v), 0)
}

// validateReflectValue uses an allowlist to accept only types that downstream
// hashing, deep-copy, and wire reconstruction handle without type-erasure loss.
// Containers (Slice, Map, Interface) are recursed with a depth counter to
// prevent stack overflow from deep/cyclic structures.
// Everything else (Ptr, Struct, Array, Chan, Func, UnsafePointer, etc.) is rejected.
func validateReflectValue(rv reflect.Value, depth int) error {
	if depth > maxPropertyDepth {
		return ErrMaxDepthExceeded
	}

	switch rv.Kind() {
	// Exact scalar types supported by hash/copy/wire.
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.String:
		if isAllowedPropertyScalarType(rv.Type()) {
			return nil
		}
		return fmt.Errorf("%w: %s", ErrUnsupportedValueType, rv.Type())

	// Containers — recurse only where the supported shape can hide any values.
	case reflect.Interface:
		if rv.IsNil() {
			return nil
		}
		// Interface unwrapping is not a nesting level — just type unwrapping.
		return validateReflectValue(rv.Elem(), depth)

	case reflect.Slice:
		if !isAllowedPropertySliceType(rv.Type()) {
			return fmt.Errorf("%w: %s", ErrUnsupportedValueType, rv.Type())
		}
		if rv.IsNil() {
			return nil
		}
		if rv.Type() != propertyTypeSliceAny {
			return nil
		}
		for i := range rv.Len() {
			if err := validateReflectValue(rv.Index(i), depth+1); err != nil {
				return err
			}
		}
		return nil

	case reflect.Map:
		mt := rv.Type()
		if mt != propertyTypeMapStringAny && mt != propertyTypeMapStringString {
			return fmt.Errorf("%w: %s", ErrUnsupportedMapType, mt)
		}
		if rv.IsNil() {
			return nil
		}
		if mt == propertyTypeMapStringAny {
			iter := rv.MapRange()
			for iter.Next() {
				if err := validateReflectValue(iter.Value(), depth+1); err != nil {
					return err
				}
			}
		}
		return nil

	// Pointer and Struct are accepted if the type has been registered via
	// RegisterPropertyStructType (e.g. for spatial geometry types). This is
	// the opt-in extension point; unregistered structs/pointers are rejected.
	case reflect.Pointer, reflect.Struct:
		if isRegisteredPropertyStructType(rv) {
			return nil
		}
		return ErrUnsupportedValueType

	// Everything else (Array, Chan, Func, UnsafePointer, etc.) is rejected.
	default:
		return ErrUnsupportedValueType
	}
}

func isAllowedPropertyScalarType(t reflect.Type) bool {
	switch t {
	case propertyTypeBool,
		propertyTypeString,
		propertyTypeInt,
		propertyTypeInt8,
		propertyTypeInt16,
		propertyTypeInt32,
		propertyTypeInt64,
		propertyTypeUint,
		propertyTypeUint8,
		propertyTypeUint16,
		propertyTypeUint32,
		propertyTypeUint64,
		propertyTypeFloat32,
		propertyTypeFloat64:
		return true
	default:
		return false
	}
}

func isAllowedPropertySliceType(t reflect.Type) bool {
	switch t {
	case propertyTypeSliceString,
		propertyTypeSliceInt,
		propertyTypeSliceInt64,
		propertyTypeSliceFloat32,
		propertyTypeSliceFloat64,
		propertyTypeSliceByte,
		propertyTypeSliceBool,
		propertyTypeSliceAny:
		return true
	default:
		return false
	}
}

// deepCopyValue clones known reference types so that mutations to the copy
// never affect the original. Unknown types fall through to shallow copy,
// which is safe for primitives, strings, and other immutable values.
// Stops recursing at maxPropertyDepth to prevent stack overflow.
func deepCopyValue(v any, depth int) any {
	if v == nil {
		return nil
	}
	if depth > maxPropertyDepth {
		return v // safety fallback: return as-is
	}

	// Custom registered types own their own deep-copy semantics — the
	// registry enforces DeepCopier at registration time, so any registered
	// struct/pointer reaching this path delegates to its DeepCopyValue.
	// Probed before the type switch so the slice/map fast paths don't get
	// invoked with an arbitrary user struct.
	if dc, ok := v.(DeepCopier); ok {
		return preserveDeepCopyPointerShape(v, dc.DeepCopyValue())
	}

	switch val := v.(type) {
	// Scalar types — immutable, return as-is without reflect overhead.
	case bool, string,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return val

	// Common slice types.
	case []string:
		cp := make([]string, len(val))
		copy(cp, val)
		return cp
	case []int:
		cp := make([]int, len(val))
		copy(cp, val)
		return cp
	case []int64:
		cp := make([]int64, len(val))
		copy(cp, val)
		return cp
	case []float32:
		cp := make([]float32, len(val))
		copy(cp, val)
		return cp
	case []float64:
		cp := make([]float64, len(val))
		copy(cp, val)
		return cp
	case []byte:
		cp := make([]byte, len(val))
		copy(cp, val)
		return cp
	case []bool:
		cp := make([]bool, len(val))
		copy(cp, val)
		return cp
	case []any:
		cp := make([]any, len(val))
		for i, elem := range val {
			cp[i] = deepCopyValue(elem, depth+1)
		}
		return cp

	// Common map types.
	case map[string]any:
		cp := make(map[string]any, len(val))
		for k, elem := range val {
			cp[k] = deepCopyValue(elem, depth+1)
		}
		return cp
	case map[string]string:
		cp := make(map[string]string, len(val))
		for k, elem := range val {
			cp[k] = elem
		}
		return cp

	default:
		// Reflect fallback for any other slice or map type.
		return reflectCopyValue(v, depth)
	}
}

func preserveDeepCopyPointerShape(original, copied any) any {
	if original == nil || copied == nil {
		return copied
	}
	origT := reflect.TypeOf(original)
	copiedT := reflect.TypeOf(copied)
	if origT.Kind() != reflect.Pointer {
		if copiedT.Kind() != reflect.Pointer {
			return copied
		}
		copiedV := reflect.ValueOf(copied)
		if copiedV.IsNil() || !copiedT.Elem().AssignableTo(origT) {
			return copied
		}
		return copiedV.Elem().Interface()
	}
	if copiedT == origT {
		return copied
	}
	elemT := origT.Elem()
	if !copiedT.AssignableTo(elemT) {
		return copied
	}
	ptr := reflect.New(elemT)
	ptr.Elem().Set(reflect.ValueOf(copied))
	return ptr.Interface()
}

// reflectCopyValue uses reflection to clone unknown slice and map types.
// Elements are recursively deep-copied via deepCopyValue so that nested
// reference types (e.g., map[int][]string) are fully independent.
// Returns v unchanged for primitives, strings, and other immutable values.
// Note: unsupported value types are rejected by Set() at insertion time
// and will never reach this function.
func reflectCopyValue(v any, depth int) any {
	if depth > maxPropertyDepth {
		return v // safety fallback: return as-is
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice:
		if rv.IsNil() {
			return v
		}
		cp := reflect.MakeSlice(rv.Type(), rv.Len(), rv.Len())
		for i := range rv.Len() {
			cp.Index(i).Set(reflect.ValueOf(deepCopyValue(rv.Index(i).Interface(), depth+1)))
		}
		return cp.Interface()
	case reflect.Map:
		if rv.IsNil() {
			return v
		}
		cp := reflect.MakeMapWithSize(rv.Type(), rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			elem := deepCopyValue(iter.Value().Interface(), depth+1)
			if elem == nil {
				// reflect.ValueOf(nil) is a zero Value — SetMapIndex with zero
				// Value deletes the key. Use the typed zero instead.
				cp.SetMapIndex(iter.Key(), reflect.Zero(rv.Type().Elem()))
			} else {
				cp.SetMapIndex(iter.Key(), reflect.ValueOf(elem))
			}
		}
		return cp.Interface()
	default:
		return v
	}
}

// Delete removes the property at key.
// Returns true if the key was found and removed, false if it was not present.
// Returns an error if key has the reserved "tkg_" prefix.
func (ps *PropertySlice) Delete(key string) (bool, error) {
	if ps == nil {
		return false, ErrNilPropertySlice
	}
	if IsShadowKey(key) {
		return false, fmt.Errorf("%w: %q", ErrReservedPrefix, key)
	}
	i := sort.Search(len(*ps), func(i int) bool {
		return (*ps)[i].Key >= key
	})
	if i >= len(*ps) || (*ps)[i].Key != key {
		return false, nil
	}
	*ps = append((*ps)[:i], (*ps)[i+1:]...)
	return true, nil
}

// ToMap converts the PropertySlice to a map.
// Values are deep-copied so mutations to the map never affect internal state.
func (ps PropertySlice) ToMap() map[string]any {
	m := make(map[string]any, len(ps))
	for _, p := range ps {
		m[p.Key] = deepCopyValue(p.Value, 0)
	}
	return m
}

// Len returns the number of properties.
func (ps PropertySlice) Len() int {
	return len(ps)
}

// canonicalPropertySlice validates, de-duplicates, sorts, and deep-copies a
// caller-provided PropertySlice before it is installed on an entity.
func canonicalPropertySlice(ps PropertySlice) (PropertySlice, error) {
	if len(ps) == 0 {
		return nil, nil
	}

	out := make(PropertySlice, len(ps))
	sortedUnique := true
	for i, p := range ps {
		if IsShadowKey(p.Key) {
			return nil, fmt.Errorf("%w: %q", ErrReservedPrefix, p.Key)
		}
		if err := ValidatePropertyValue(p.Value); err != nil {
			return nil, fmt.Errorf("%w: %q (got %T)", err, p.Key, p.Value)
		}
		copied, err := copyValidatedPropertyValue(p.Key, p.Value)
		if err != nil {
			return nil, err
		}
		out[i] = Property{Key: p.Key, Value: copied}
		if i > 0 && p.Key <= ps[i-1].Key {
			sortedUnique = false
		}
	}
	if sortedUnique {
		return out, nil
	}

	latest := make(map[string]any, len(out))
	for _, p := range out {
		latest[p.Key] = p.Value
	}
	keys := make([]string, 0, len(latest))
	for key := range latest {
		keys = append(keys, key)
	}
	slices.Sort(keys)

	out = make(PropertySlice, len(keys))
	for i, key := range keys {
		out[i] = Property{Key: key, Value: latest[key]}
	}
	return out, nil
}

func validateCanonicalPropertySlice(ps PropertySlice) error {
	for i, p := range ps {
		if IsShadowKey(p.Key) {
			return fmt.Errorf("%w: %q", ErrReservedPrefix, p.Key)
		}
		if err := ValidatePropertyValue(p.Value); err != nil {
			return fmt.Errorf("%w: %q (got %T)", err, p.Key, p.Value)
		}
		if i > 0 && p.Key <= ps[i-1].Key {
			return fmt.Errorf("%w: non-canonical property order at %q", ErrUnsupportedValueType, p.Key)
		}
	}
	return nil
}

// NewPropertySlice creates a PropertySlice from a map in O(N log N) time.
// Allocates the result once, validates all values, deep-copies accepted
// reference values, then sorts once — no per-key memmoves.
// Returns nil, nil for nil or empty maps.
// Returns ErrReservedPrefix if any key starts with "tkg_".
// Returns ErrUnsupportedValueType if any value fails allowlist validation.
func NewPropertySlice(m map[string]any) (PropertySlice, error) {
	if len(m) == 0 {
		return nil, nil
	}
	ps := make(PropertySlice, 0, len(m))
	for k, v := range m {
		if IsShadowKey(k) {
			return nil, fmt.Errorf("%w: %q", ErrReservedPrefix, k)
		}
		if err := ValidatePropertyValue(v); err != nil {
			return nil, fmt.Errorf("%w: %q (got %T)", err, k, v)
		}
		copied, err := copyValidatedPropertyValue(k, v)
		if err != nil {
			return nil, err
		}
		ps = append(ps, Property{Key: k, Value: copied})
	}
	slices.SortFunc(ps, func(a, b Property) int {
		if a.Key < b.Key {
			return -1
		}
		if a.Key > b.Key {
			return 1
		}
		return 0
	})
	return ps, nil
}

// NewOwnedPropertySlice creates a validated, canonical property slice for
// ownership transfer into a newly constructed entity.
func NewOwnedPropertySlice(m map[string]any) (OwnedPropertySlice, error) {
	ps, err := NewPropertySlice(m)
	if err != nil {
		return OwnedPropertySlice{}, err
	}
	return OwnedPropertySlice{ps: ps}, nil
}
