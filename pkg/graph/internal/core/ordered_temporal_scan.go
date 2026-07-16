package core

import (
	"cmp"
	"sort"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Temporal ordered / prefix scans (Stage B).
//
// The current-state ordered property view is valid-time-AGNOSTIC: it reflects
// each entity's live value, so it cannot answer a value-ordered query pinned to a
// past belief/valid state (a node in range at t may be out of range now, and vice
// versa — the index would both miss and over-report). Rather than DECLINE such a
// query (the Stage-A behaviour, ErrOrderedScanTemporal), these folds serve it
// SOUNDLY: resolve every label/type member to its version at the temporal pin
// (via the same chain resolver + B4 valid-time prune the temporal ByLabel/ByType
// door uses), apply the value predicate to the value-AT-t, then sort by that
// value and emit in contractual order.
//
// Cost: O(N log N) in the label/type's temporal membership. Value-at-t is not
// indexed, so — unlike the current-state door — the scan cannot early-stop by
// value; it must resolve and score every candidate before the sort. fn's early
// stop still bounds emission (and any downstream per-row work) once a top-k caller
// has enough rows.

// forEachNodeValueOrderedTemporal gathers every node carrying label whose version
// at the temporal pin in opts passes matchKey, then emits them to fn ordered by
// the returned key (ascending, or descending when desc; ties by node ID
// ascending). matchKey extracts the sort key from a resolved (at-t) node and
// reports whether it satisfies the value predicate.
func forEachNodeValueOrderedTemporal[K cmp.Ordered](
	c *Core, label string, opts storepkg.QueryOpts, desc bool,
	matchKey func(n *types.Node) (K, bool),
	fn func(*types.Node) bool,
) error {
	// Gather every label member resolved to its version at the temporal pin.
	// nodesByLabelLocked applies the chain resolver AND the B4 valid-time prune;
	// strip pagination so value ordering + the caller's fn limit happen AFTER the
	// value sort, never by ID pagination.
	gopts := opts
	gopts.Limit = 0
	gopts.After = 0
	var nodes []*types.Node
	if err := c.readUnderRLock(func() error {
		var e error
		nodes, e = c.nodesByLabelLocked(label, gopts)
		return e
	}); err != nil {
		return err
	}

	type scored struct {
		key K
		n   *types.Node
	}
	matched := make([]scored, 0, len(nodes))
	for _, nd := range nodes {
		if k, ok := matchKey(nd); ok {
			matched = append(matched, scored{key: k, n: nd})
		}
	}
	sort.Slice(matched, func(i, j int) bool {
		if matched[i].key != matched[j].key {
			if desc {
				return matched[i].key > matched[j].key
			}
			return matched[i].key < matched[j].key
		}
		return int64(matched[i].n.ID().SnowflakeID()) < int64(matched[j].n.ID().SnowflakeID())
	})
	for _, m := range matched {
		if !fn(m.n) {
			return nil
		}
	}
	return nil
}

// forEachRelValueOrderedTemporal is the relationship mirror: gather every rel of
// typeName resolved to its version at the temporal pin, keep those passing
// matchKey, emit ordered by key (ties by rel ID ascending).
func forEachRelValueOrderedTemporal[K cmp.Ordered](
	c *Core, typeName string, opts storepkg.QueryOpts, desc bool,
	matchKey func(r *types.Relationship) (K, bool),
	fn func(*types.Relationship) bool,
) error {
	gopts := opts
	gopts.Limit = 0
	gopts.After = 0
	var rels []*types.Relationship
	if err := c.readUnderRLock(func() error {
		var e error
		rels, e = c.relsByTypeLocked(typeName, gopts)
		return e
	}); err != nil {
		return err
	}

	type scored struct {
		key K
		r   *types.Relationship
	}
	matched := make([]scored, 0, len(rels))
	for _, rl := range rels {
		if k, ok := matchKey(rl); ok {
			matched = append(matched, scored{key: k, r: rl})
		}
	}
	sort.Slice(matched, func(i, j int) bool {
		if matched[i].key != matched[j].key {
			if desc {
				return matched[i].key > matched[j].key
			}
			return matched[i].key < matched[j].key
		}
		return int64(matched[i].r.ID().SnowflakeID()) < int64(matched[j].r.ID().SnowflakeID())
	})
	for _, m := range matched {
		if !fn(m.r) {
			return nil
		}
	}
	return nil
}

// numericInRange reports whether f lies in [min,max] under the inclusivity flags.
func numericInRange(f, min, max float64, inclMin, inclMax bool) bool {
	if f < min || f > max {
		return false
	}
	if !inclMin && f == min {
		return false
	}
	if !inclMax && f == max {
		return false
	}
	return true
}

// coerceFloat64 converts an indexable numeric property value to float64. ok=false
// for non-numeric values (matching the ordered numeric view, which indexes only
// numeric sort keys).
func coerceFloat64(v any) (float64, bool) {
	switch x := v.(type) {
	case int:
		return float64(x), true
	case int8:
		return float64(x), true
	case int16:
		return float64(x), true
	case int32:
		return float64(x), true
	case int64:
		return float64(x), true
	case uint:
		return float64(x), true
	case uint8:
		return float64(x), true
	case uint16:
		return float64(x), true
	case uint32:
		return float64(x), true
	case uint64:
		return float64(x), true
	case float32:
		return float64(x), true
	case float64:
		return x, true
	default:
		return 0, false
	}
}
