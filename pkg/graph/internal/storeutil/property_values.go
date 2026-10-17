package storeutil

import (
	"fmt"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ValueIndexKeys is the key list of a single-value equality lookup
// (NodesByLabelAndProperty and twins): value validated
// (types.ValidatePropertyValue, the error wrapped as "graph: <door> value"),
// its index key alone (empty for a value without one: it matches nothing).
func ValueIndexKeys(value any, door string) ([]string, error) {
	if err := types.ValidatePropertyValue(value); err != nil {
		return nil, fmt.Errorf("graph: %s value: %w", door, err)
	}
	return []string{types.IndexablePropertyValueKey(value)}, nil
}

// distinctKeysByScan is the key count up to which DistinctIndexKeys checks
// repeats by a scan (a number has at most 12 equal keys) instead of a set.
const distinctKeysByScan = 16

// DistinctIndexKeys returns the keys of an equality lookup over several index
// keys (store.PropertyKeysLookupCapability) without the empty key and without
// repeats, in first-occurrence order: keys itself when it has neither.
func DistinctIndexKeys(keys []string) []string {
	if len(keys) > distinctKeysByScan {
		seen := make(map[string]struct{}, len(keys))
		out := make([]string, 0, len(keys))
		for _, k := range keys {
			if _, dup := seen[k]; k != "" && !dup {
				seen[k] = struct{}{}
				out = append(out, k)
			}
		}
		return out
	}
	clean := true
	for i, k := range keys {
		if k == "" || slices.Contains(keys[:i], k) {
			clean = false
			break
		}
	}
	if clean {
		return keys
	}
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		if k != "" && !slices.Contains(out, k) {
			out = append(out, k)
		}
	}
	return out
}

// NodeHasAnyIndexKey reports whether n's property prop carries one of keys
// (a list's elements on an Elements index): the match of a lookup over
// several values, n.HasPropertyIndexKey for each key.
func NodeHasAnyIndexKey(n *types.Node, prop string, elements bool, keys []string) bool {
	for _, k := range keys {
		if n.HasPropertyIndexKey(prop, elements, k) {
			return true
		}
	}
	return false
}

// RelHasAnyIndexKey is NodeHasAnyIndexKey for a relationship.
func RelHasAnyIndexKey(r *types.Relationship, prop string, elements bool, keys []string) bool {
	for _, k := range keys {
		if r.HasPropertyIndexKey(prop, elements, k) {
			return true
		}
	}
	return false
}
