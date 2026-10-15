// Package stats is the sub-API for graph counters and label/type-scoped count
// statistics. The full GraphStats snapshot is reachable via g.Stats().Get().
package stats

import (
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/apiutil"
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

// Ops is the subset of *core.StatOps the stats sub-API forwards to; counters come
// back as a tuple via SnapshotCounters() to keep the interface narrow.
type Ops interface {
	NodeCount() (int, error)
	RelCount() (int, error)
	NodeCountByLabel(label string) (int, error)
	NodeCountByLabelAndPropertyKey(label, propertyKey string) (int, error)
	PropertyTypeClassCounts(label, propertyKey string) (storepkg.PropertyTypeClassCounts, error)
	RelPropertyTypeClassCounts(typeName, propertyKey string) (storepkg.PropertyTypeClassCounts, error)
	PropertyStats(label, propertyKey string) (storepkg.PropertyStats, error)
	RelPropertyStats(typeName, propertyKey string) (storepkg.PropertyStats, error)
	PropertyNDV(label, propertyKey string) (storepkg.PropertyNDV, error)
	RelPropertyNDV(typeName, propertyKey string) (storepkg.PropertyNDV, error)
	PropertyBounds(label, propertyKey string, opts storepkg.QueryOpts) (storepkg.PropertyBounds, bool, error)
	RelPropertyBounds(typeName, propertyKey string, opts storepkg.QueryOpts) (storepkg.PropertyBounds, bool, error)
	LabelTimeStats(label string) (storepkg.MemberTimeStats, bool, error)
	RelTypeTimeStats(typeName string) (storepkg.MemberTimeStats, bool, error)
	RelCountByType(typeName string) (int, error)
	RelTypeDegreeStats(typeName string) (storepkg.RelTypeDegreeStats, error)
	TrackRelTypeDegrees(typeName string) error
	UntrackRelTypeDegrees(typeName string) error
	ReadCosts() (storepkg.ReadCosts, bool, error)
	HistoryCounts() (storepkg.HistoryCounts, bool, error)
	RelHistoryCountByType(typeName string) (int, bool, error)
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

// Get returns a snapshot of operation counters and optional cache metrics (cache
// fields are populated for BadgerStore only). Returns ErrNilGraph on a zero-value
// API, or ErrGraphClosed after close (the snapshot is still populated).
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

// PropertyTypeClassCounts returns the EXACT partition of label's current nodes by
// the type class of propertyKey's value: Numeric (finite and +-Inf), NaN, String,
// Bool, Other, plus Missing (label count minus present classes). O(1) maintained
// counters; unlike NodeCountByLabelAndPropertyKey (indexable scalars only) it
// covers EVERY value. Unregistered labels return zero. Backends without
// store.NodePropertyTypeClassCountsCapability return store.ErrCapabilityNotSupported
// (memory, badger, tiered implement it).
func (a *API) PropertyTypeClassCounts(label, propertyKey string) (storepkg.PropertyTypeClassCounts, error) {
	ops, err := a.ready()
	if err != nil {
		return storepkg.PropertyTypeClassCounts{}, err
	}
	return ops.PropertyTypeClassCounts(label, propertyKey)
}

// RelPropertyTypeClassCounts is the relationship mirror of PropertyTypeClassCounts:
// the EXACT per-(relType, key) partition, the gate for the rel ORDER BY ... LIMIT k
// push-down. Memory, badger and sharded implement it; others return
// store.ErrCapabilityNotSupported.
func (a *API) RelPropertyTypeClassCounts(typeName, propertyKey string) (storepkg.PropertyTypeClassCounts, error) {
	ops, err := a.ready()
	if err != nil {
		return storepkg.PropertyTypeClassCounts{}, err
	}
	return ops.RelPropertyTypeClassCounts(typeName, propertyKey)
}

// RelTypeDegreeStats returns the largest out- and in-degree among a type's current
// relationships (with the holding nodes), the relationship count and distinct start
// and end node counts; typeName "" means every relationship. Every backend.
// Derived: the first call after a write scans once, then it is cached against the
// mutation epoch; Exact reports that no write moved the epoch during the count.
// Unregistered types return zero stats.
func (a *API) RelTypeDegreeStats(typeName string) (storepkg.RelTypeDegreeStats, error) {
	ops, err := a.ready()
	if err != nil {
		return storepkg.RelTypeDegreeStats{}, err
	}
	return ops.RelTypeDegreeStats(typeName)
}

// TrackRelTypeDegrees opts a relationship type in to maintained degree counts
// (opt-in, per type): after its next count, RelTypeDegreeStats applies the
// relationships created since (every create door but a foreign-endpoint one)
// instead of counting the type again after each write to it; a delete, an
// update, a rollback or a write through another door counts again. Cost only
// for a tracked type: two per-node count maps (about 40-50 B per distinct
// endpoint), up to 65,536 recorded creates between reads, a short lock per
// create, the graph's write lock for O(creates since) at a read. Other types
// pay one atomic load per create. Counts again as without tracking where the
// store keeps no per-type stamp (tiered, sharded, external stores). Errors: a
// malformed type name, ErrGraphClosed.
func (a *API) TrackRelTypeDegrees(typeName string) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.TrackRelTypeDegrees(typeName)
}

// UntrackRelTypeDegrees drops a type's maintained degree counts (a no-op for
// an untracked type).
func (a *API) UntrackRelTypeDegrees(typeName string) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.UntrackRelTypeDegrees(typeName)
}

// ReadCosts returns the store's nominal read costs in ns per row (by-ID Lend,
// streaming scan, adjacency walk) for RAM-held and backing-store rows, plus how many
// rows stay held (store.ReadCosts). ok=false when the store states none.
func (a *API) ReadCosts() (storepkg.ReadCosts, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return storepkg.ReadCosts{}, false, err
	}
	return ops.ReadCosts()
}

// HistoryCounts returns how many nodes and relationships have history rows
// (store.HistoryCounts), exact and without a pass per call (memory, badger,
// sharded). ok=false when the store states none (tiered).
func (a *API) HistoryCounts() (storepkg.HistoryCounts, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return storepkg.HistoryCounts{}, false, err
	}
	return ops.HistoryCounts()
}

// RelHistoryCountByType returns how many relationships of typeName have history
// rows (deleted ones included). Exact unless a relationship ID was reused across
// types (then an upper bound), so 0 always means none. Memory, badger and tiered
// answer it (O(relationships with history); tiered opens closed cold shards);
// ok=false otherwise. An unknown type is 0.
func (a *API) RelHistoryCountByType(typeName string) (int, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, false, err
	}
	return ops.RelHistoryCountByType(typeName)
}

// PropertyStats returns NDV (HyperLogLog estimate), exact Min/Max (numeric and
// string families) and Count for label's current nodes carrying propertyKey.
// Missing pairs return a zero PropertyStats, not an error. Memory, badger and
// tiered implement store.NodePropertyStatsCapability (tiered folds shards, see
// docs/query-planners.md "Tiered NDV fold"); others return
// store.ErrCapabilityNotSupported (check with errors.Is).
func (a *API) PropertyStats(label, propertyKey string) (storepkg.PropertyStats, error) {
	ops, err := a.ready()
	if err != nil {
		return storepkg.PropertyStats{}, err
	}
	return ops.PropertyStats(label, propertyKey)
}

// RelPropertyStats is the relationship mirror of PropertyStats (backlog 21a):
// NDV, exact Min/Max and Count of the type's current relationships carrying
// propertyKey with an indexable scalar value. Missing pairs return zero stats.
// Memory and badger implement store.RelPropertyStatsCapability; tiered (rel property
// indexes are RAM-only per shard) and others return store.ErrCapabilityNotSupported.
func (a *API) RelPropertyStats(typeName, propertyKey string) (storepkg.PropertyStats, error) {
	ops, err := a.ready()
	if err != nil {
		return storepkg.PropertyStats{}, err
	}
	return ops.RelPropertyStats(typeName, propertyKey)
}

// PropertyNDV returns PropertyStats' NDV and Count without Min/Max, skipping the
// rescan PropertyStats runs after a write removed the min or max value. Missing
// pairs return zero. Memory, badger and tiered implement
// store.NodePropertyNDVCapability; others (or planner statistics disabled) return
// store.ErrCapabilityNotSupported.
func (a *API) PropertyNDV(label, propertyKey string) (storepkg.PropertyNDV, error) {
	ops, err := a.ready()
	if err != nil {
		return storepkg.PropertyNDV{}, err
	}
	return ops.PropertyNDV(label, propertyKey)
}

// RelPropertyNDV is the relationship mirror of PropertyNDV (memory and
// badger; tiered returns store.ErrCapabilityNotSupported, as for
// RelPropertyStats).
func (a *API) RelPropertyNDV(typeName, propertyKey string) (storepkg.PropertyNDV, error) {
	ops, err := a.ready()
	if err != nil {
		return storepkg.PropertyNDV{}, err
	}
	return ops.RelPropertyNDV(typeName, propertyKey)
}

// PropertyBounds returns the smallest and largest numeric value of
// propertyKey over label's current nodes (store.PropertyBounds: Min, Max, a
// stored value at each, and the exact type-class partition with Missing, so
// NumericOnly and NullFree) read at one moment, O(1) from the property
// index's first and last key and maintained counters. ok=false with no error
// where it cannot be stated exactly, never answered by a walk: no property
// index on the pair, an index being built, an Elements index, an integer of
// magnitude past 2^53 in the index, badger PropertyIndexOnDisk, planner
// statistics disabled, tiered, sharded and external stores, an unknown label,
// a temporal opts. Read-only: no write cost. Errors: a malformed label or
// key, ErrGraphClosed.
func (a *API) PropertyBounds(label, propertyKey string, opts storepkg.QueryOpts) (storepkg.PropertyBounds, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return storepkg.PropertyBounds{}, false, err
	}
	return ops.PropertyBounds(label, propertyKey, opts)
}

// RelPropertyBounds is the relationship mirror of PropertyBounds over a
// relationship type's property index (memory and badger).
func (a *API) RelPropertyBounds(typeName, propertyKey string, opts storepkg.QueryOpts) (storepkg.PropertyBounds, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return storepkg.PropertyBounds{}, false, err
	}
	return ops.RelPropertyBounds(typeName, propertyKey, opts)
}

// LabelTimeStats returns store.MemberTimeStats for label: current members,
// those whose row carries an explicit valid time (tkg_valid_from or
// tkg_valid_to), and LastEnded, the latest transaction instant a row of an
// entity whose rows ever carried the label was ended (HistoryAfter(pin):
// whether a read pinned there may see a non-current version; Eternal: neither).
// No write cost: the explicit count is counted on the first call after a
// write to the label (one pass over its current rows) and then served in O(1)
// against the label's mutation epoch; LastEnded is O(1) on a store without
// node history rows, else one RAM stamp lookup per ever-member. exact=false
// when writers kept moving the epoch across three counts, or the store keeps
// no epoch (tiered, external stores: counted per call). An unknown label is
// zero stats. Errors: a malformed label, ErrGraphClosed.
func (a *API) LabelTimeStats(label string) (storepkg.MemberTimeStats, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return storepkg.MemberTimeStats{}, false, err
	}
	return ops.LabelTimeStats(label)
}

// RelTypeTimeStats is LabelTimeStats for a relationship type.
func (a *API) RelTypeTimeStats(typeName string) (storepkg.MemberTimeStats, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return storepkg.MemberTimeStats{}, false, err
	}
	return ops.RelTypeTimeStats(typeName)
}

// RelCountByType returns the count of relationships of the given type.
func (a *API) RelCountByType(typeName string) (int, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, err
	}
	return ops.RelCountByType(typeName)
}

// RangeCardinality forwards to core.NodeOps.RangeCardinality (same signature and
// semantics as Nodes().RangeCardinality): the count of label's nodes whose numeric
// propKey lies within [min,max] (inclusivity per flags), summed from the property
// index bucket sizes, no node scan. exact=false means the fast path declined (no
// capability, no/poisoned index, or a temporal filter in opts) and the caller must
// scan-and-count. The bounds MUST capture the whole predicate.
func (a *API) RangeCardinality(label, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts) (int64, bool, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, false, err
	}
	return ops.RangeCardinality(label, propKey, min, max, inclMin, inclMax, opts)
}

// RelRangeCardinality is the relationship mirror of RangeCardinality: a bucket-sum
// count from the REL property index, exact=false when the fast path declines (no
// capability, e.g. tiered; no/poisoned index; temporal filter in opts).
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
	return apiutil.CloneMap(counts), nil
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
	return apiutil.CloneMap(counts), nil
}
