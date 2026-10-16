package core

import (
	"slices"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The property equality of every lookup door (ByLabelAndProperty /
// ByTypeAndProperty current and temporal, the named *At / *During doors,
// ByLabelAndProperties, the paged effective doors' filter) is the one of
// types.EqualIndexValues, which the equality counts read from the index too: a
// query value stands for its forms (one typed value per index key: 3 is 3,
// int8(3), ..., 3.0, float32(3)). The stores answer one exact key per call;
// the doors unite the forms.

// equalKeys are the index keys of a query value's forms
// (types.EqualIndexKeys); nil matches nothing.
type equalKeys []string

// node reports whether n's property prop carries one of ks (a list's
// elements on an Elements index).
func (ks equalKeys) node(n *types.Node, prop string, elements bool) bool {
	found := false
	n.ForEachPropertyIndexKey(prop, elements, func(vk string) bool {
		found = slices.Contains(ks, vk)
		return !found
	})
	return found
}

// rel is node for a relationship.
func (ks equalKeys) rel(r *types.Relationship, prop string, elements bool) bool {
	found := false
	r.ForEachPropertyIndexKey(prop, elements, func(vk string) bool {
		found = slices.Contains(ks, vk)
		return !found
	})
	return found
}

// keysOf are the index keys of a value's forms (types.EqualIndexValues).
func keysOf(forms []any) equalKeys {
	keys := make(equalKeys, len(forms))
	for i, f := range forms {
		keys[i] = types.IndexablePropertyValueKey(f)
	}
	return keys
}

// equalKeysOf are the equal keys of each (key, value) pair.
func equalKeysOf(values map[string]any) map[string]equalKeys {
	out := make(map[string]equalKeys, len(values))
	for k, v := range values {
		out[k] = types.EqualIndexKeys(v)
	}
	return out
}

// nodeMatchesAllEqual reports whether n carries, for every pair of want, a
// scalar value with one of its keys (the composite lookup's predicate).
func nodeMatchesAllEqual(n *types.Node, want map[string]equalKeys) bool {
	for k, ks := range want {
		vk, found := n.IndexablePropertyValueKey(k)
		if !found || vk == "" || !slices.Contains(ks, vk) {
			return false
		}
	}
	return true
}

// equalForms are the forms of every pair (types.EqualIndexValues), and
// whether some pair has more than one; nil when a pair has none (it matches
// nothing).
func equalForms(values map[string]any) (map[string][]any, bool) {
	out := make(map[string][]any, len(values))
	multi := false
	for k, v := range values {
		forms := types.EqualIndexValues(v)
		if len(forms) == 0 {
			return nil, false
		}
		multi = multi || len(forms) > 1
		out[k] = forms
	}
	return out, multi
}

// forEachFormCombination calls fn with every combination of one form per
// pair (sorted keys, so the order is deterministic).
func forEachFormCombination(forms map[string][]any, fn func(map[string]any) error) error {
	keys := make([]string, 0, len(forms))
	for k := range forms {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	combo := make(map[string]any, len(keys))
	var walk func(i int) error
	walk = func(i int) error {
		if i == len(keys) {
			return fn(combo)
		}
		for _, f := range forms[keys[i]] {
			combo[keys[i]] = f
			if err := walk(i + 1); err != nil {
				return err
			}
		}
		return nil
	}
	return walk(0)
}

// nodePropertyIndexed reports whether a property index on (tok, key) serves
// the store's single-value lookup (false where the store cannot say).
func (c *Core) nodePropertyIndexed(tok uint16, key string) (bool, error) {
	cap, ok := c.store.(storepkg.PropertyIndexIntrospectionCapability)
	if !ok {
		return false, nil
	}
	return cap.HasPropertyIndex(tok, key)
}

// relPropertyIndexed is nodePropertyIndexed for a relationship type.
func (c *Core) relPropertyIndexed(tok uint16, key string) (bool, error) {
	cap, ok := c.store.(storepkg.RelPropertyIndexIntrospectionCapability)
	if !ok {
		return false, nil
	}
	return cap.HasRelPropertyIndex(tok, key)
}

// compositeIndexed reports whether a composite index over exactly the keys
// of values serves the store's composite lookup.
func (c *Core) compositeIndexed(tok uint16, values map[string]any) (bool, error) {
	cap, ok := c.store.(storepkg.CompositeIndexIntrospectionCapability)
	if !ok {
		return false, nil
	}
	defs, err := cap.ListCompositePropertyIndexes(tok)
	if err != nil {
		return false, err
	}
	for _, def := range defs {
		if len(def) != len(values) {
			continue
		}
		all := true
		for _, k := range def {
			if _, ok := values[k]; !ok {
				all = false
				break
			}
		}
		if all {
			return true, nil
		}
	}
	return false, nil
}

// uniteNodes sorts rows gathered from several form lookups by ID, drops
// repeats (a list holding two forms on an Elements index) and pages them.
func uniteNodes(nodes []*types.Node, opts storepkg.QueryOpts) []*types.Node {
	storeutil.SortNodesByID(nodes)
	nodes = slices.CompactFunc(nodes, func(a, b *types.Node) bool { return a.ID() == b.ID() })
	return storeutil.PaginateNodes(nodes, opts.After, opts.Limit)
}

// uniteRels is uniteNodes for relationships.
func uniteRels(rels []*types.Relationship, opts storepkg.QueryOpts) []*types.Relationship {
	storeutil.SortRelsByID(rels)
	rels = slices.CompactFunc(rels, func(a, b *types.Relationship) bool { return a.ID() == b.ID() })
	return storeutil.PaginateRels(rels, opts.After, opts.Limit)
}
