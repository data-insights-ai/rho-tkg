// Package stats is a sub-API accessor for graph counter / count-by-label
// statistics. The full GraphStats struct (atomic operation counters + cache
// metrics) is reachable via `g.Stats().Get()`; the count helpers below answer
// label/type-scoped questions without materialising the full GraphStats
// snapshot.
package stats

import (
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// GraphStats holds operation counters and optional cache metrics for a Graph.
// Cache metrics are populated only when the underlying store is a BadgerStore;
// they are zero for MemoryStore and tiered.Store.
type GraphStats struct {
	// Operation counters — incremented on every successful operation.
	NodesAdded   int64
	NodesRead    int64
	NodesUpdated int64
	NodesDeleted int64
	RelsAdded    int64
	RelsRead     int64
	RelsUpdated  int64
	RelsDeleted  int64

	// Cache metrics — populated for BadgerStore only, zero otherwise.
	// Both cacheHit and cacheDeleted (tombstone) results count as hits,
	// because both avoid a Badger read. cacheMiss counts as a miss.
	NodeCacheHits   int64
	NodeCacheMisses int64
	RelCacheHits    int64
	RelCacheMisses  int64
}

// Ops is the subset of *core.StatOps the stats sub-API forwards to.
//
// The full-snapshot accessor returns the individual counters as a 12-tuple via
// SnapshotCounters() so the local Ops interface stays decoupled from any
// particular GraphStats struct definition (the implementation in
// pkg/graph/internal/core ships the same struct shape under a different
// declaration; assembling the result here keeps the interface narrow).
type Ops interface {
	NodeCount() (int, error)
	RelCount() (int, error)
	NodeCountByLabel(label string) (int, error)
	NodeCountByLabelAndPropertyKey(label, propertyKey string) (int, error)
	PropertyTypeClassCounts(label, propertyKey string) (storepkg.PropertyTypeClassCounts, error)
	RelPropertyTypeClassCounts(typeName, propertyKey string) (storepkg.PropertyTypeClassCounts, error)
	PropertyStats(label, propertyKey string) (storepkg.PropertyStats, error)
	RelCountByType(typeName string) (int, error)
	RangeCardinality(label, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts) (int64, bool, error)
	RelRangeCardinality(typeName, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts) (int64, bool, error)
	AllLabelCounts() (map[string]int, error)
	AllRelTypeCounts() (map[string]int, error)
	SnapshotCounters() (
		nodesAdded, nodesRead, nodesUpdated, nodesDeleted int64,
		relsAdded, relsRead, relsUpdated, relsDeleted int64,
		nodeCacheHits, nodeCacheMisses, relCacheHits, relCacheMisses int64,
		err error,
	)
}

// API is the stats sub-API accessor.
type API struct {
	ops Ops
	ok  bool
}

// New constructs a stats sub-API.
func New(ops Ops) *API { return &API{ops: ops, ok: !grapherr.IsNil(ops)} }

func (a *API) ready() (Ops, error) {
	if a == nil || !a.ok {
		return nil, grapherr.ErrNilGraph
	}
	return a.ops, nil
}

// Get returns a snapshot of graph operation counters and optional cache metrics.
// Cache metrics are populated only when the underlying store implements the
// store-stats interface (currently BadgerStore only); all cache fields are zero
// for MemoryStore and tiered.Store.
//
// Returns ErrNilGraph if called on a zero-value or nil-backed API, or
// ErrGraphClosed if the graph has been closed (the counter snapshot is still
// populated in the closed case so callers can observe the final state). The
// error shape matches every other Stats method for caller-side uniformity.
func (a *API) Get() (GraphStats, error) {
	ops, err := a.ready()
	if err != nil {
		return GraphStats{}, err
	}
	na, nr, nu, nd, ra, rr, ru, rd, nch, ncm, rch, rcm, snapErr := ops.SnapshotCounters()
	return GraphStats{
		NodesAdded:      na,
		NodesRead:       nr,
		NodesUpdated:    nu,
		NodesDeleted:    nd,
		RelsAdded:       ra,
		RelsRead:        rr,
		RelsUpdated:     ru,
		RelsDeleted:     rd,
		NodeCacheHits:   nch,
		NodeCacheMisses: ncm,
		RelCacheHits:    rch,
		RelCacheMisses:  rcm,
	}, snapErr
}

// NodeCount returns the total node count.
func (a *API) NodeCount() (int, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, err
	}
	return ops.NodeCount()
}

// RelCount returns the total relationship count.
func (a *API) RelCount() (int, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, err
	}
	return ops.RelCount()
}

// NodeCountByLabel returns the count of nodes carrying the label.
func (a *API) NodeCountByLabel(label string) (int, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, err
	}
	return ops.NodeCountByLabel(label)
}

// NodeCountByLabelAndPropertyKey returns the count of current nodes carrying
// label with an indexable scalar propertyKey value.
func (a *API) NodeCountByLabelAndPropertyKey(label, propertyKey string) (int, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, err
	}
	return ops.NodeCountByLabelAndPropertyKey(label, propertyKey)
}

// PropertyTypeClassCounts returns the EXACT partition of label's current
// nodes by the type class of propertyKey's value — Numeric (finite numbers
// and ±Inf), NaN, String, Bool, Other (slices/maps/structs), plus Missing
// (nodes carrying the label WITHOUT the key, computed as NodeCountByLabel
// minus the present classes). O(1): maintained counters adjusted on the same
// mutation call as NodeCountByLabelAndPropertyKey, so exactness is a
// correctness guarantee a planner's ordering-soundness gate may rely on —
// e.g. "every present value is orderable-numeric" is Numeric == Present, and
// "the gap is nulls only" is Present == Numeric with Missing free to be
// nonzero. Note the difference from NodeCountByLabelAndPropertyKey: that
// counter covers INDEXABLE SCALAR values only, while this partition covers
// EVERY value (a []int64 property counts under Other here but is invisible
// there). Unregistered labels return the zero value. Backends without
// store.NodePropertyTypeClassCountsCapability return
// store.ErrCapabilityNotSupported; the in-tree memory, badger, and tiered
// (cross-shard fold) backends all implement it.
func (a *API) PropertyTypeClassCounts(label, propertyKey string) (storepkg.PropertyTypeClassCounts, error) {
	ops, err := a.ready()
	if err != nil {
		return storepkg.PropertyTypeClassCounts{}, err
	}
	return ops.PropertyTypeClassCounts(label, propertyKey)
}

// RelPropertyTypeClassCounts is the relationship mirror of PropertyTypeClassCounts
// (rule 2, BACKLOG 5B): the EXACT per-(relType, property key) partition of the type's
// current relationships by value class — the correctness gate for the rel ORDER BY
// r.prop LIMIT k push-down (ordering is sound only when the ordered class is
// unambiguous). Backends without store.RelPropertyTypeClassCountsCapability
// (tiered/sharded — rel property indexes are RAM-only) return
// store.ErrCapabilityNotSupported.
func (a *API) RelPropertyTypeClassCounts(typeName, propertyKey string) (storepkg.PropertyTypeClassCounts, error) {
	ops, err := a.ready()
	if err != nil {
		return storepkg.PropertyTypeClassCounts{}, err
	}
	return ops.RelPropertyTypeClassCounts(typeName, propertyKey)
}

// PropertyStats returns NDV (estimated distinct-value count via a
// HyperLogLog sketch), exact Min/Max (for scalar-ordered value families —
// numeric and string, see store.NodePropertyStatsCapability), and Count (the
// same presence count NodeCountByLabelAndPropertyKey returns) for label's
// current nodes carrying propertyKey.
//
// Missing/unpopulated (label, propertyKey) pairs return a zero-value
// PropertyStats, not an error — matching NodeCountByLabelAndPropertyKey's
// "unregistered → 0" convention. All three in-tree backends (memory,
// badger, tiered) implement store.NodePropertyStatsCapability — tiered
// folds NDV via a register-max HyperLogLog merge across shards and Min/Max
// via min-of-mins/max-of-maxes (see docs/query-planners.md "Tiered NDV
// fold"). A backend WITHOUT the optional capability returns
// store.ErrCapabilityNotSupported; check with errors.Is.
func (a *API) PropertyStats(label, propertyKey string) (storepkg.PropertyStats, error) {
	ops, err := a.ready()
	if err != nil {
		return storepkg.PropertyStats{}, err
	}
	return ops.PropertyStats(label, propertyKey)
}

// RelCountByType returns the count of relationships of the given type.
func (a *API) RelCountByType(typeName string) (int, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, err
	}
	return ops.RelCountByType(typeName)
}

// RangeCardinality is an additive alias forwarding to the SAME core op
// Nodes().RangeCardinality uses (core.NodeOps.RangeCardinality) — identical
// signature and semantics, so a query planner reading only g.Stats() need
// not also import the nodes sub-API for this one statistic.
//
// It returns the count of the label's nodes whose numeric propKey value lies
// within [min,max] (inclusivity per flags), summed directly from the
// property index's sorted per-value bucket sizes — O(distinct values in
// range), NO node scan. The second return is exact: false means the fast
// path declined (no store capability, no/poisoned index, or a temporal
// filter set in opts — the index is valid-time agnostic) and the caller must
// scan-and-count instead (e.g. via ForEachByLabel). The bounds MUST already
// capture the whole predicate. See core.NodeOps.RangeCardinality and
// nodes.API.RangeCardinality — Nodes().RangeCardinality itself is untouched
// by this alias.
func (a *API) RangeCardinality(label, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts) (int64, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, false, err
	}
	return ops.RangeCardinality(label, propKey, min, max, inclMin, inclMax, opts)
}

// RelRangeCardinality is the relationship mirror of RangeCardinality (rule 2): an
// O(distinct values in range) bucket-sum count from the REL property index, with
// exact=false when the fast path declines (no capability — rel indexes are RAM-only,
// so tiered/sharded decline; no/poisoned index; or a temporal filter in opts). The
// rel ordering-soundness primitive for the ORDER BY r.prop LIMIT k push-down.
func (a *API) RelRangeCardinality(typeName, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts) (int64, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, false, err
	}
	return ops.RelRangeCardinality(typeName, propKey, min, max, inclMin, inclMax, opts)
}

// AllLabelCounts returns counts per label.
func (a *API) AllLabelCounts() (map[string]int, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	counts, err := ops.AllLabelCounts()
	if err != nil {
		return nil, err
	}
	return cloneCounts(counts), nil
}

// AllRelTypeCounts returns counts per relationship type.
func (a *API) AllRelTypeCounts() (map[string]int, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	counts, err := ops.AllRelTypeCounts()
	if err != nil {
		return nil, err
	}
	return cloneCounts(counts), nil
}

func cloneCounts(counts map[string]int) map[string]int {
	if counts == nil {
		return nil
	}
	out := make(map[string]int, len(counts))
	for key, count := range counts {
		out[key] = count
	}
	return out
}
