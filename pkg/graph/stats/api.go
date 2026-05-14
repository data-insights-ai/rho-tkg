// Package stats is a sub-API accessor for graph counter / count-by-label
// statistics. The full GraphStats struct (atomic operation counters + cache
// metrics) is reachable via `g.Stats.Get()`; the count helpers below answer
// label/type-scoped questions without materialising the full GraphStats
// snapshot.
package stats

import "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/grapherr"

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
	RelCountByType(typeName string) (int, error)
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

// RelCountByType returns the count of relationships of the given type.
func (a *API) RelCountByType(typeName string) (int, error) {
	ops, err := a.ready()
	if err != nil {
		return 0, err
	}
	return ops.RelCountByType(typeName)
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
