package types

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
)

// ErrReservedPrefix is returned when a property key uses the reserved "tkg_" prefix.
var ErrReservedPrefix = errors.New("types: property key uses reserved tkg_ prefix")

// ErrUnsupportedValueType is returned when a property value is a pointer or struct.
// Graph databases store data, not application memory references.
var ErrUnsupportedValueType = errors.New("types: pointer and struct values are not supported")

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
// Recursively validates the value — pointers and structs are rejected at any
// nesting depth (inside slices, maps, and interface wrappers).
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
		cp[i] = Property{Key: p.Key, Value: deepCopyValue(p.Value)}
	}
	return cp
}

// validatePropertyValue recursively checks that v contains no pointers or structs
// at any nesting depth. Slices, maps, and interface wrappers are traversed.
func validatePropertyValue(v any) error {
	if v == nil {
		return nil
	}
	return validateReflectValue(reflect.ValueOf(v))
}

// validateReflectValue traverses rv to reject pointer and struct kinds.
func validateReflectValue(rv reflect.Value) error {
	switch rv.Kind() {
	case reflect.Ptr, reflect.Struct:
		return ErrUnsupportedValueType
	case reflect.Interface:
		if rv.IsNil() {
			return nil
		}
		return validateReflectValue(rv.Elem())
	case reflect.Slice:
		if rv.IsNil() {
			return nil
		}
		for i := range rv.Len() {
			if err := validateReflectValue(rv.Index(i)); err != nil {
				return err
			}
		}
	case reflect.Map:
		if rv.IsNil() {
			return nil
		}
		iter := rv.MapRange()
		for iter.Next() {
			if err := validateReflectValue(iter.Key()); err != nil {
				return err
			}
			if err := validateReflectValue(iter.Value()); err != nil {
				return err
			}
		}
	}
	return nil
}

// deepCopyValue clones known reference types so that mutations to the copy
// never affect the original. Unknown types fall through to shallow copy,
// which is safe for primitives, strings, and other immutable values.
func deepCopyValue(v any) any {
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
			cp[i] = deepCopyValue(elem)
		}
		return cp

	// Common map types.
	case map[string]any:
		cp := make(map[string]any, len(val))
		for k, elem := range val {
			cp[k] = deepCopyValue(elem)
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
		return reflectCopyValue(v)
	}
}

// reflectCopyValue uses reflection to clone unknown slice and map types.
// Elements are recursively deep-copied via deepCopyValue so that nested
// reference types (e.g., map[int][]string) are fully independent.
// Returns v unchanged for primitives, strings, and other immutable values.
// Note: pointer and struct values are rejected by Set() at insertion time
// and will never reach this function.
func reflectCopyValue(v any) any {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice:
		if rv.IsNil() {
			return v
		}
		cp := reflect.MakeSlice(rv.Type(), rv.Len(), rv.Len())
		for i := range rv.Len() {
			cp.Index(i).Set(reflect.ValueOf(deepCopyValue(rv.Index(i).Interface())))
		}
		return cp.Interface()
	case reflect.Map:
		if rv.IsNil() {
			return v
		}
		cp := reflect.MakeMapWithSize(rv.Type(), rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			elem := deepCopyValue(iter.Value().Interface())
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
		m[p.Key] = deepCopyValue(p.Value)
	}
	return m
}

// Len returns the number of properties.
func (ps PropertySlice) Len() int {
	return len(ps)
}
