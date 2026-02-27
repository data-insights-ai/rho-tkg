package types

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
)

// ErrReservedPrefix is returned when a property key uses the reserved "tkg_" prefix.
var ErrReservedPrefix = errors.New("types: property key uses reserved tkg_ prefix")

// ErrUnsupportedValueType is returned when a property value contains a type
// not on the allowlist (only primitives, slices, and maps are accepted).
// Graph databases store data, not application memory references.
var ErrUnsupportedValueType = errors.New("types: unsupported property value type")

// ErrMaxDepthExceeded is returned when a property value exceeds the maximum
// nesting depth. This prevents stack overflow from self-referential or
// excessively deep structures.
var ErrMaxDepthExceeded = errors.New("types: property value exceeds maximum nesting depth")

// maxPropertyDepth is the maximum nesting depth for property values.
// Validation and deep-copy functions stop recursing beyond this limit.
const maxPropertyDepth = 32

// Property is a single key-value property entry.
type Property struct {
	Key   string
	Value any
}

// PropertySlice is a sorted slice of Property entries.
// The sorted invariant is maintained by Set; never modify entries directly.
type PropertySlice []Property

// Set inserts or overwrites the property at key.
// Returns an error if key has the reserved "tkg_" prefix.
// Recursively validates the value using an allowlist — only primitives, slices,
// and maps are accepted. All other types (pointers, structs, arrays, channels,
// functions, etc.) are rejected at any nesting depth.
// Returns ErrMaxDepthExceeded if nesting exceeds maxPropertyDepth (32).
// Maintains the sorted-by-key invariant.
func (ps *PropertySlice) Set(key string, value any) error {
	if IsShadowKey(key) {
		return fmt.Errorf("%w: %q", ErrReservedPrefix, key)
	}
	if err := validatePropertyValue(value); err != nil {
		return fmt.Errorf("%w: %q (got %T)", err, key, value)
	}
	i := sort.Search(len(*ps), func(i int) bool {
		return (*ps)[i].Key >= key
	})
	if i < len(*ps) && (*ps)[i].Key == key {
		(*ps)[i].Value = value
		return nil
	}
	*ps = append(*ps, Property{})
	copy((*ps)[i+1:], (*ps)[i:])
	(*ps)[i] = Property{Key: key, Value: value}
	return nil
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

// validatePropertyValue recursively checks that v contains only allowlisted types
// at any nesting depth. Slices, maps, and interface wrappers are traversed.
// Rejects at maxPropertyDepth to prevent stack overflow from deep/cyclic input.
func validatePropertyValue(v any) error {
	if v == nil {
		return nil
	}
	return validateReflectValue(reflect.ValueOf(v), 0)
}

// validateReflectValue uses an allowlist to accept only safe kinds.
// Primitives pass directly. Containers (Slice, Map, Interface) are recursed
// with a depth counter to prevent stack overflow from deep/cyclic structures.
// Everything else (Ptr, Struct, Array, Chan, Func, UnsafePointer, etc.) is rejected.
func validateReflectValue(rv reflect.Value, depth int) error {
	if depth > maxPropertyDepth {
		return ErrMaxDepthExceeded
	}

	switch rv.Kind() {
	// Primitives — always safe.
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.String:
		return nil

	// Containers — recurse into elements.
	case reflect.Interface:
		if rv.IsNil() {
			return nil
		}
		// Interface unwrapping is not a nesting level — just type unwrapping.
		return validateReflectValue(rv.Elem(), depth)

	case reflect.Slice:
		if rv.IsNil() {
			return nil
		}
		for i := range rv.Len() {
			if err := validateReflectValue(rv.Index(i), depth+1); err != nil {
				return err
			}
		}
		return nil

	case reflect.Map:
		if rv.IsNil() {
			return nil
		}
		iter := rv.MapRange()
		for iter.Next() {
			if err := validateReflectValue(iter.Key(), depth+1); err != nil {
				return err
			}
			if err := validateReflectValue(iter.Value(), depth+1); err != nil {
				return err
			}
		}
		return nil

	// Everything else (Ptr, Struct, Array, Chan, Func, UnsafePointer, etc.) is rejected.
	default:
		return ErrUnsupportedValueType
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

	switch val := v.(type) {
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
