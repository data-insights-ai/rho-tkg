package core

import (
	"fmt"

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

// StoreStats is an optional interface for stores that expose LRU cache metrics.
// `(*Core).Stats()` (reachable from a *Graph via `g.Stats.Get()`)
// type-asserts the underlying store to this interface — implementing it does
// not affect the Store interface contract.
type StoreStats interface {
	NodeCacheHits() int64
	NodeCacheMisses() int64
	RelCacheHits() int64
	RelCacheMisses() int64
}

// Get returns a snapshot of graph operation counters and optional cache metrics.
// Cache metrics are populated only when the underlying store implements StoreStats
// (currently BadgerStore only); all cache fields are zero for MemoryStore and tiered.Store.
//
// Returns ErrGraphClosed if the graph has been closed; the counter snapshot
// is still returned in that case. The error shape matches every other Stats
// method for caller-side uniformity (API 4.0).
func (s *StatOps) Get() (GraphStats, error) {
	c := s.c
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.statsLocked()
}

// statsLocked is the lock-free body of StatOps.Get.
func (c *Core) statsLocked() (GraphStats, error) {
	out := GraphStats{
		NodesAdded:   c.opNodeAdds.Load(),
		NodesRead:    c.opNodeReads.Load(),
		NodesUpdated: c.opNodeUpdates.Load(),
		NodesDeleted: c.opNodeDeletes.Load(),
		RelsAdded:    c.opRelAdds.Load(),
		RelsRead:     c.opRelReads.Load(),
		RelsUpdated:  c.opRelUpdates.Load(),
		RelsDeleted:  c.opRelDeletes.Load(),
	}
	if c.closed.Load() {
		return out, ErrGraphClosed
	}
	if ss, ok := c.store.(StoreStats); ok {
		out.NodeCacheHits = ss.NodeCacheHits()
		out.NodeCacheMisses = ss.NodeCacheMisses()
		out.RelCacheHits = ss.RelCacheHits()
		out.RelCacheMisses = ss.RelCacheMisses()
	}
	return out, nil
}

// SnapshotCounters returns the same operation counters and cache metrics as
// Get but as discrete int64 return values plus the lifecycle error, so
// consumers (e.g. the pkg/graph/stats sub-API) can satisfy a local Ops
// interface without importing core's GraphStats struct. The error is the
// same one Get would return — propagating it lets the public wrapper honour
// the fail-closed contract (ErrGraphClosed surfaces to callers).
func (s *StatOps) SnapshotCounters() (
	nodesAdded, nodesRead, nodesUpdated, nodesDeleted int64,
	relsAdded, relsRead, relsUpdated, relsDeleted int64,
	nodeCacheHits, nodeCacheMisses, relCacheHits, relCacheMisses int64,
	err error,
) {
	g, getErr := s.Get()
	return g.NodesAdded, g.NodesRead, g.NodesUpdated, g.NodesDeleted,
		g.RelsAdded, g.RelsRead, g.RelsUpdated, g.RelsDeleted,
		g.NodeCacheHits, g.NodeCacheMisses, g.RelCacheHits, g.RelCacheMisses,
		getErr
}

// NodeCount forwards to Core.Nodes.Count until NodeOps migration moves it.
func (s *StatOps) NodeCount() (int, error) { return s.c.Nodes.Count() }

// RelCount forwards to Core.Rels.Count until RelOps migration moves it.
func (s *StatOps) RelCount() (int, error) { return s.c.Rels.Count() }

// NodeCountByLabel forwards to Core.Nodes.CountByLabel.
func (s *StatOps) NodeCountByLabel(label string) (int, error) { return s.c.Nodes.CountByLabel(label) }

// NodeCountByLabelAndPropertyKey returns the number of current nodes carrying
// label with an indexable scalar propertyKey value. Missing labels return 0.
func (s *StatOps) NodeCountByLabelAndPropertyKey(label, propertyKey string) (int, error) {
	c := s.c
	if err := c.validateIndexLabel(label); err != nil {
		return 0, err
	}
	if err := storepkg.ValidateIndexPropertyKey(propertyKey); err != nil {
		return 0, err
	}
	c.mu.RLock()
	if c.closed.Load() {
		c.mu.RUnlock()
		return 0, ErrGraphClosed
	}
	tok, ok := c.labels.Lookup(label)
	if !ok {
		c.mu.RUnlock()
		return 0, nil
	}
	count, err := c.nodeCountByLabelAndPropertyKey(tok, propertyKey)
	c.mu.RUnlock()
	return count, err
}

// PropertyTypeClassCounts returns the EXACT per-(label, propertyKey)
// partition of the label's current nodes by the type class of the key's value
// — see types.PropertyTypeClass for the classification rule. O(1): maintained
// counters, adjusted on the same node-mutation call as the presence counter,
// so exactness is a correctness guarantee, not a planner estimate (an
// ordering-soundness gate may rely on it). Missing is computed here as
// NodeCountByLabel − Present (nodes carrying the label WITHOUT the key).
// Unregistered labels return the zero value. Backends without
// store.NodePropertyTypeClassCountsCapability return
// storepkg.ErrCapabilityNotSupported.
func (s *StatOps) PropertyTypeClassCounts(label, propertyKey string) (storepkg.PropertyTypeClassCounts, error) {
	c := s.c
	var zero storepkg.PropertyTypeClassCounts
	if err := c.validateIndexLabel(label); err != nil {
		return zero, err
	}
	if err := storepkg.ValidateIndexPropertyKey(propertyKey); err != nil {
		return zero, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() {
		return zero, ErrGraphClosed
	}
	cap, ok := c.store.(storepkg.NodePropertyTypeClassCountsCapability)
	if !ok {
		return zero, fmt.Errorf("%w: NodePropertyTypeClassCountsCapability", storepkg.ErrCapabilityNotSupported)
	}
	tok, ok := c.labels.Lookup(label)
	if !ok {
		return zero, nil
	}
	counts, err := cap.NodePropertyTypeClassCounts(tok, propertyKey)
	if err != nil {
		return zero, err
	}
	labelCount, err := c.nodeCountByLabel(tok)
	if err != nil {
		return zero, err
	}
	if missing := int64(labelCount) - counts.Present(); missing > 0 {
		counts.Missing = missing
	}
	return counts, nil
}

// PropertyStats returns NDV/min/max/count planner statistics for
// (label, propertyKey). Missing labels return a zero-value PropertyStats
// (Count 0, NDV 0, Min/Max nil), matching NodeCountByLabelAndPropertyKey's
// "unregistered label → 0" convention. Backends without
// store.NodePropertyStatsCapability return storepkg.ErrCapabilityNotSupported
// (memory, badger, and tiered all implement it — see
// docs/adr/0005-tiered-parity.md §3.1).
func (s *StatOps) PropertyStats(label, propertyKey string) (storepkg.PropertyStats, error) {
	c := s.c
	if err := c.validateIndexLabel(label); err != nil {
		return storepkg.PropertyStats{}, err
	}
	if err := storepkg.ValidateIndexPropertyKey(propertyKey); err != nil {
		return storepkg.PropertyStats{}, err
	}
	c.mu.RLock()
	if c.closed.Load() {
		c.mu.RUnlock()
		return storepkg.PropertyStats{}, ErrGraphClosed
	}
	tok, ok := c.labels.Lookup(label)
	if !ok {
		c.mu.RUnlock()
		return storepkg.PropertyStats{}, nil
	}
	stats, err := c.nodePropertyStats(tok, propertyKey)
	c.mu.RUnlock()
	return stats, err
}

// RelCountByType forwards to Core.Rels.CountByType.
func (s *StatOps) RelCountByType(typeName string) (int, error) { return s.c.Rels.CountByType(typeName) }

// RangeCardinality forwards to Core.Nodes.RangeCardinality — the SAME core op
// g.Nodes().RangeCardinality uses (identical signature and semantics). See
// NodeOps.RangeCardinality for the full contract: an O(distinct values in
// range) bucket-sum count from the property index, with exact=false when the
// fast path declines (no capability, no/poisoned index, or a temporal filter
// in opts).
func (s *StatOps) RangeCardinality(label, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts) (int64, bool, error) {
	return s.c.Nodes.RangeCardinality(label, propKey, min, max, inclMin, inclMax, opts)
}

// RelRangeCardinality forwards to Core.Rels.RangeCardinality — the relationship
// mirror of RangeCardinality (rule 2). See RelOps.RangeCardinality for the contract
// (rel property indexes are RAM-only, so tiered/sharded decline with exact=false).
func (s *StatOps) RelRangeCardinality(typeName, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts) (int64, bool, error) {
	return s.c.Rels.RangeCardinality(typeName, propKey, min, max, inclMin, inclMax, opts)
}

// PropertyKeyCount returns the number of distinct property keys registered
// in the property-key registry. Useful for monitoring cardinality growth
// against the uint16 ceiling (65535). When the registry approaches its
// capacity the wire encoder falls back to writing raw keys instead of
// dictionary-encoded tokens.
func (s *StatOps) PropertyKeyCount() (int, error) {
	c := s.c
	if err := c.checkOpen(); err != nil {
		return 0, err
	}
	if c.propKeys == nil {
		return 0, nil
	}
	return c.propKeys.Len(), nil
}

// AllLabelCounts returns a map of label name to node count for all registered labels.
// Labels with zero nodes are omitted.
func (s *StatOps) AllLabelCounts() (map[string]int, error) {
	c := s.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	var result map[string]int
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.allLabelCountsLocked()
		return err
	})
	return result, err
}

// allLabelCountsLocked is the lock-free body of StatOps.AllLabelCounts.
func (c *Core) allLabelCountsLocked() (map[string]int, error) {
	result := make(map[string]int)
	names := c.labels.ExportNames()
	for i := 1; i < len(names); i++ {
		count, err := c.nodeCountByLabel(uint16(i))
		if err != nil {
			return nil, err
		}
		if count > 0 {
			result[names[i]] = count
		}
	}
	return result, nil
}

// AllRelTypeCounts returns a map of relationship type name to relationship count
// for all registered types. Types with zero relationships are omitted.
func (s *StatOps) AllRelTypeCounts() (map[string]int, error) {
	c := s.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	var result map[string]int
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.allRelTypeCountsLocked()
		return err
	})
	return result, err
}

// allRelTypeCountsLocked is the lock-free body of StatOps.AllRelTypeCounts.
func (c *Core) allRelTypeCountsLocked() (map[string]int, error) {
	result := make(map[string]int)
	names := c.relTypes.ExportNames()
	for i := 1; i < len(names); i++ {
		count, err := c.relCountByType(uint16(i))
		if err != nil {
			return nil, err
		}
		if count > 0 {
			result[names[i]] = count
		}
	}
	return result, nil
}
