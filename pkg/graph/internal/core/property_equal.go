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

// match is ks as a matcher built once (a set past a few keys): a value list's
// keys checked per row stay linear in rows plus keys (ROWPROBE).
func (ks equalKeys) match() storeutil.IndexKeyMatch { return storeutil.NewIndexKeyMatch(ks) }

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

// unionEqualForms is the equal forms of every value (types.EqualIndexValues):
// the forms a lookup by any of the values matches (VALUES). A form two values
// share appears twice; the lookups count a repeated key once.
func unionEqualForms(values []any) []any {
	var forms []any
	for _, v := range values {
		forms = append(forms, types.EqualIndexValues(v)...)
	}
	return forms
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

// fewestForms is the pair with the fewest forms (the smallest key on a tie).
func fewestForms(forms map[string][]any) string {
	best := ""
	for k, f := range forms {
		if best == "" || len(f) < len(forms[best]) || (len(f) == len(forms[best]) && k < best) {
			best = k
		}
	}
	return best
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
