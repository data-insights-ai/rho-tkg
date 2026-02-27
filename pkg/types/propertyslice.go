package types

import (
	"fmt"
	"sort"
)

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
// Maintains the sorted-by-key invariant.
func (ps *PropertySlice) Set(key string, value any) error {
	if IsShadowKey(key) {
		return fmt.Errorf("property key %q uses reserved tkg_ prefix", key)
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

// deepCopyValue clones known reference types so that mutations to the copy
// never affect the original. Unknown types fall through to shallow copy,
// which is safe for primitives, strings, and other immutable values.
func deepCopyValue(v any) any {
	switch val := v.(type) {
	case []string:
		cp := make([]string, len(val))
		copy(cp, val)
		return cp
	case []any:
		cp := make([]any, len(val))
		for i, elem := range val {
			cp[i] = deepCopyValue(elem)
		}
		return cp
	case map[string]any:
		cp := make(map[string]any, len(val))
		for k, elem := range val {
			cp[k] = deepCopyValue(elem)
		}
		return cp
	default:
		return v
	}
}

// ToMap converts the PropertySlice to a map.
func (ps PropertySlice) ToMap() map[string]any {
	m := make(map[string]any, len(ps))
	for _, p := range ps {
		m[p.Key] = p.Value
	}
	return m
}

// Len returns the number of properties.
func (ps PropertySlice) Len() int {
	return len(ps)
}
