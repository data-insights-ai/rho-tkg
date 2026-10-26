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

// IndexKeyMatch is the key list of an equality lookup over several index keys,
// prepared once per call (ROWPROBE): a row matches when one of its property's
// own index keys (one for a scalar, each element's with elements) is in the
// list, so a row costs its own keys, built once, not one key per value
// (sigma's paged-row probe: 1,000 values in one call cost 48x per value what
// 16 cost). Above distinctKeysByScan keys a set answers the membership.
type IndexKeyMatch struct {
	keys []string
	set  map[string]struct{}
}

// NewIndexKeyMatch prepares keys (DistinctIndexKeys' output) for row matches.
func NewIndexKeyMatch(keys []string) IndexKeyMatch {
	m := IndexKeyMatch{keys: keys}
	if len(keys) > distinctKeysByScan {
		m.set = make(map[string]struct{}, len(keys))
		for _, k := range keys {
			m.set[k] = struct{}{}
		}
	}
	return m
}

func (m IndexKeyMatch) has(vk string) bool {
	if m.set != nil {
		_, ok := m.set[vk]
		return ok
	}
	return slices.Contains(m.keys, vk)
}

// Node reports whether n's property prop carries one of the keys (elements:
// a list element's key counts, as for an Elements index).
func (m IndexKeyMatch) Node(n *types.Node, prop string, elements bool) bool {
	found := false
	n.ForEachPropertyIndexKey(prop, elements, func(vk string) bool {
		found = m.has(vk)
		return !found
	})
	return found
}

// Rel is Node for a relationship.
func (m IndexKeyMatch) Rel(r *types.Relationship, prop string, elements bool) bool {
	found := false
	r.ForEachPropertyIndexKey(prop, elements, func(vk string) bool {
		found = m.has(vk)
		return !found
	})
	return found
}
