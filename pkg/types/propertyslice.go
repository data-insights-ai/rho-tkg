package types

import (
	"fmt"
	"sort"
	"strings"
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
	if strings.HasPrefix(key, "tkg_") {
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
func (ps PropertySlice) DeepCopy() PropertySlice {
	if ps == nil {
		return nil
	}
	cp := make(PropertySlice, len(ps))
	copy(cp, ps)
	return cp
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
