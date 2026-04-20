package types

import (
	"reflect"
	"sort"
	"sync"
)

// propertyStructRegistry holds struct types that external packages have
// registered as acceptable property values. By default, PropertySlice rejects
// anything outside primitives, slices, and maps — spatial geometry, custom
// domain types, and similar need an explicit opt-in via RegisterPropertyStructType.
//
// This is a package-level registry on purpose. Calls happen at init() time
// of the registering package (e.g. tkgd/pkg/spatial), so they are effectively
// one-shot and visible to every subsequent PropertySlice.Set call.
var (
	propertyStructRegistryMu sync.RWMutex
	propertyStructRegistry   = make(map[reflect.Type]struct{})
)

// RegisterPropertyStructType declares that values of v's type (and pointer-to
// that type) are valid property values. Call from an init() in packages that
// ship first-class custom types, typically alongside a msgpack extension
// registration so the values also round-trip through storage.
//
// Pass either a zero value or a pointer — both forms (value and pointer-to)
// become acceptable. Registering the same type twice is a no-op.
//
// Intended for tkgd-bundled packages like pkg/spatial. Third-party callers
// are welcome, but review carefully: accepting arbitrary struct types in
// properties widens the trust surface (serialisation, hashing, deep-copy
// semantics must all hold).
func RegisterPropertyStructType(v any) {
	if v == nil {
		return
	}
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	propertyStructRegistryMu.Lock()
	propertyStructRegistry[t] = struct{}{}
	propertyStructRegistryMu.Unlock()
}

// isRegisteredPropertyStructType reports whether rv's type (or the element
// type if rv is a pointer) has been registered via RegisterPropertyStructType.
func isRegisteredPropertyStructType(rv reflect.Value) bool {
	t := rv.Type()
	if t.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return false
		}
		t = t.Elem()
	}
	propertyStructRegistryMu.RLock()
	_, ok := propertyStructRegistry[t]
	propertyStructRegistryMu.RUnlock()
	return ok
}

// RegisteredPropertyStructTypes returns the names of all registered custom
// property types in lexicographic order. For admin and diagnostic use.
func RegisteredPropertyStructTypes() []string {
	propertyStructRegistryMu.RLock()
	out := make([]string, 0, len(propertyStructRegistry))
	for t := range propertyStructRegistry {
		out = append(out, t.String())
	}
	propertyStructRegistryMu.RUnlock()
	sort.Strings(out)
	return out
}
