package core

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
func (s *StatOps) Get() GraphStats {
	c := s.c
	c.mu.RLock()
	defer c.mu.RUnlock()
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
	if !c.closed.Load() {
		if ss, ok := c.store.(StoreStats); ok {
			out.NodeCacheHits = ss.NodeCacheHits()
			out.NodeCacheMisses = ss.NodeCacheMisses()
			out.RelCacheHits = ss.RelCacheHits()
			out.RelCacheMisses = ss.RelCacheMisses()
		}
	}
	return out
}

// SnapshotCounters returns the same operation counters and cache metrics as
// Get but as discrete int64 return values, so consumers (e.g. the
// pkg/graph/stats sub-API) can satisfy a local Ops interface without
// importing core's GraphStats struct.
func (s *StatOps) SnapshotCounters() (
	nodesAdded, nodesRead, nodesUpdated, nodesDeleted int64,
	relsAdded, relsRead, relsUpdated, relsDeleted int64,
	nodeCacheHits, nodeCacheMisses, relCacheHits, relCacheMisses int64,
) {
	g := s.Get()
	return g.NodesAdded, g.NodesRead, g.NodesUpdated, g.NodesDeleted,
		g.RelsAdded, g.RelsRead, g.RelsUpdated, g.RelsDeleted,
		g.NodeCacheHits, g.NodeCacheMisses, g.RelCacheHits, g.RelCacheMisses
}

// NodeCount forwards to Core.Nodes.Count until NodeOps migration moves it.
func (s *StatOps) NodeCount() (int, error) { return s.c.Nodes.Count() }

// RelCount forwards to Core.Rels.Count until RelOps migration moves it.
func (s *StatOps) RelCount() (int, error) { return s.c.Rels.Count() }

// NodeCountByLabel forwards to Core.Nodes.CountByLabel.
func (s *StatOps) NodeCountByLabel(label string) (int, error) { return s.c.Nodes.CountByLabel(label) }

// RelCountByType forwards to Core.Rels.CountByType.
func (s *StatOps) RelCountByType(typeName string) (int, error) { return s.c.Rels.CountByType(typeName) }

// AllLabelCounts returns a map of label name to node count for all registered labels.
// Labels with zero nodes are omitted.
func (s *StatOps) AllLabelCounts() (map[string]int, error) {
	c := s.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	result := make(map[string]int)
	err := c.readUnderRLock(func() error {
		names := c.labels.ExportNames()
		// Skip index 0 (reserved empty string).
		for i := 1; i < len(names); i++ {
			count, err := c.store.NodeCountByLabel(uint16(i))
			if err != nil {
				return err
			}
			if count > 0 {
				result[names[i]] = count
			}
		}
		return nil
	})
	return result, err
}

// AllRelTypeCounts returns a map of relationship type name to relationship count
// for all registered types. Types with zero relationships are omitted.
func (s *StatOps) AllRelTypeCounts() (map[string]int, error) {
	c := s.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	result := make(map[string]int)
	err := c.readUnderRLock(func() error {
		names := c.relTypes.ExportNames()
		// Skip index 0 (reserved empty string).
		for i := 1; i < len(names); i++ {
			count, err := c.store.RelCountByType(uint16(i))
			if err != nil {
				return err
			}
			if count > 0 {
				result[names[i]] = count
			}
		}
		return nil
	})
	return result, err
}
