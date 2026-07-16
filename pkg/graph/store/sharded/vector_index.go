package sharded

import (
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	"github.com/vmihailenco/msgpack/v5"
)

// Vector indexes (VectorIndexCapability + VectorIndexOptionsCapability +
// FilteredVectorSearchCapability) — ADR-0007 S5.
//
// A k-NN search must consider vectors GLOBALLY, but a label's nodes are spread
// across slots. Rather than replicate tiered's store-level index (which hooks
// every one of ~13 node-write paths for maintenance — the silent-staleness
// risk class), the sharded store keeps ONE vector index PER SHARD (each badger
// shard already maintains its own on every write it owns) and merges the
// per-shard top-k on read: fan the query out to every shard, then globally
// re-rank the union of results by distance to the query and take the top-k.
//
// The merge is EXACT for the brute-force engine (each shard returns its exact
// local top-k; the global top-k is a subset of the union) and a sound
// approximation for HNSW (same "approximate by default" contract as a single
// HNSW index). The store keeps only per-index def metadata (dims + metric) so
// it can re-rank without reimplementing the metric — the distance itself comes
// from indexpkg.VectorDistance, the exact primitive the engines rank by.

var (
	_ storecontract.VectorIndexCapability          = (*Store)(nil)
	_ storecontract.VectorIndexOptionsCapability   = (*Store)(nil)
	_ storecontract.FilteredVectorSearchCapability = (*Store)(nil)
)

const vectorDefsMetaKey = "vector_index_defs"

// vectorDefKey identifies a vector index by (label, property key).
type vectorDefKey struct {
	LabelToken  uint16
	PropertyKey string
}

// vectorDefMeta is the store-level metadata kept for a vector index so the
// merge can re-rank per-shard results. The vectors themselves live per shard.
type vectorDefMeta struct {
	Dims   int
	Metric storecontract.DistanceMetric
}

// vectorDefBlob is the msgpack persistence shape for the def metadata.
type vectorDefBlob struct {
	LabelToken  uint16                       `msgpack:"l"`
	PropertyKey string                       `msgpack:"p"`
	Dims        int                          `msgpack:"d"`
	Metric      storecontract.DistanceMetric `msgpack:"m"`
}

// CreateVectorIndex creates a vector index on every shard with default HNSW
// tuning. Returns ErrVectorIndexExists if it already exists.
func (s *Store) CreateVectorIndex(labelToken uint16, propertyKey string, dims int, metric storecontract.DistanceMetric) error {
	return s.CreateVectorIndexWithOptions(labelToken, propertyKey, dims, metric, storecontract.VectorIndexOptions{})
}

// CreateVectorIndexWithOptions creates a vector index on every shard with the
// given engine/tuning options. Returns ErrVectorIndexExists if it exists.
func (s *Store) CreateVectorIndexWithOptions(labelToken uint16, propertyKey string, dims int, metric storecontract.DistanceMetric, opts storecontract.VectorIndexOptions) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return err
	}
	if err := indexpkg.ValidateVectorIndexConfig(dims, metric); err != nil {
		return err
	}
	if err := s.fanOutUniform(func(shard *badgerShard) error {
		return shard.CreateVectorIndexWithOptions(labelToken, propertyKey, dims, metric, opts)
	}); err != nil {
		return err
	}
	s.vectorDefMu.Lock()
	s.vectorDefs[vectorDefKey{labelToken, propertyKey}] = vectorDefMeta{Dims: dims, Metric: metric}
	err := s.persistVectorDefsLocked()
	s.vectorDefMu.Unlock()
	return err
}

// DropVectorIndex removes the vector index from every shard. Returns
// ErrVectorIndexNotFound if no such index exists.
func (s *Store) DropVectorIndex(labelToken uint16, propertyKey string) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return err
	}
	if err := s.fanOutUniform(func(shard *badgerShard) error {
		return shard.DropVectorIndex(labelToken, propertyKey)
	}); err != nil {
		return err
	}
	s.vectorDefMu.Lock()
	delete(s.vectorDefs, vectorDefKey{labelToken, propertyKey})
	err := s.persistVectorDefsLocked()
	s.vectorDefMu.Unlock()
	return err
}

// SearchNearestNodes returns the k nearest nodes to query across ALL shards.
// Each shard returns its local top-k; the union is globally re-ranked by
// distance and truncated to k. opts (incl. temporal filters) push down per
// shard.
func (s *Store) SearchNearestNodes(labelToken uint16, propertyKey string, query []float32, k int, opts QueryOpts) ([]*types.Node, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	meta, err := s.vectorDefFor(labelToken, propertyKey)
	if err != nil {
		return nil, err
	}
	per := make([][]*types.Node, len(s.shards))
	errs := make([]error, len(s.shards))
	s.parallelShards(func(idx int, shard *badgerShard) {
		per[idx], errs[idx] = shard.SearchNearestNodes(labelToken, propertyKey, query, k, opts)
	})
	if err := coalesceUniform(errs); err != nil {
		return nil, err
	}
	var candidates []*types.Node
	for _, c := range per {
		candidates = append(candidates, c...)
	}
	return mergeVectorNodes(candidates, query, propertyKey, meta.Metric, k), nil
}

// SearchNearestFiltered returns the k nearest node IDs across all shards that
// satisfy filter. Each shard applies filter locally and returns its top-k
// eligible IDs; the union is re-ranked globally by distance (the nodes are
// refetched to recompute the distance) and truncated to k.
func (s *Store) SearchNearestFiltered(labelToken uint16, propertyKey string, query []float32, k int, filter func(snowflake.ID) bool) ([]snowflake.ID, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	meta, err := s.vectorDefFor(labelToken, propertyKey)
	if err != nil {
		return nil, err
	}
	per := make([][]snowflake.ID, len(s.shards))
	errs := make([]error, len(s.shards))
	s.parallelShards(func(idx int, shard *badgerShard) {
		per[idx], errs[idx] = shard.SearchNearestFiltered(labelToken, propertyKey, query, k, filter)
	})
	if err := coalesceUniform(errs); err != nil {
		return nil, err
	}
	var ids []types.NodeID
	for _, c := range per {
		for _, id := range c {
			ids = append(ids, types.NodeID(id))
		}
	}
	if len(ids) == 0 {
		return nil, nil
	}
	nodes, err := s.GetNodesByIDs(ids)
	if err != nil {
		return nil, err
	}
	ranked := mergeVectorNodes(nodes, query, propertyKey, meta.Metric, k)
	out := make([]snowflake.ID, len(ranked))
	for i, n := range ranked {
		out[i] = n.ID().SnowflakeID()
	}
	return out, nil
}

// mergeVectorNodes globally re-ranks candidate nodes by their distance to query
// under metric and returns the k nearest. Ties break by ascending ID for a
// deterministic global order. Nodes lacking the vector property are dropped.
func mergeVectorNodes(candidates []*types.Node, query []float32, propertyKey string, metric storecontract.DistanceMetric, k int) []*types.Node {
	if k <= 0 || len(candidates) == 0 {
		return nil
	}
	type scored struct {
		n *types.Node
		d float64
	}
	scoredNodes := make([]scored, 0, len(candidates))
	seen := make(map[types.NodeID]struct{}, len(candidates))
	for _, n := range candidates {
		if n == nil {
			continue
		}
		if _, dup := seen[n.ID()]; dup {
			continue // a node can only live on one shard, but guard anyway
		}
		vec, ok := n.Float32SlicePropertyCopy(propertyKey)
		if !ok || len(vec) != len(query) {
			continue
		}
		seen[n.ID()] = struct{}{}
		scoredNodes = append(scoredNodes, scored{n: n, d: indexpkg.VectorDistance(metric, query, vec)})
	}
	sort.Slice(scoredNodes, func(i, j int) bool {
		if scoredNodes[i].d != scoredNodes[j].d {
			return scoredNodes[i].d < scoredNodes[j].d
		}
		return scoredNodes[i].n.ID().SnowflakeID() < scoredNodes[j].n.ID().SnowflakeID()
	})
	if len(scoredNodes) > k {
		scoredNodes = scoredNodes[:k]
	}
	out := make([]*types.Node, len(scoredNodes))
	for i, sc := range scoredNodes {
		out[i] = sc.n
	}
	return out
}

// vectorDefFor returns the store-level def metadata for a vector index, or
// ErrVectorIndexNotFound if none is defined.
func (s *Store) vectorDefFor(labelToken uint16, propertyKey string) (vectorDefMeta, error) {
	s.vectorDefMu.RLock()
	meta, ok := s.vectorDefs[vectorDefKey{labelToken, propertyKey}]
	s.vectorDefMu.RUnlock()
	if !ok {
		return vectorDefMeta{}, storecontract.ErrVectorIndexNotFound
	}
	return meta, nil
}

// parallelShards runs fn against every shard in parallel and waits.
func (s *Store) parallelShards(fn func(idx int, shard *badgerShard)) {
	_ = s.forEachShardErr(func(idx int, shard *badgerShard) error {
		fn(idx, shard)
		return nil
	})
}

// persistVectorDefsLocked writes the store-level def metadata to the anchor
// MetaKV. Caller holds s.vectorDefMu. In-memory stores keep RAM-only defs.
func (s *Store) persistVectorDefsLocked() error {
	if s.inMemory {
		return nil
	}
	blobs := make([]vectorDefBlob, 0, len(s.vectorDefs))
	for key, meta := range s.vectorDefs {
		blobs = append(blobs, vectorDefBlob{
			LabelToken:  key.LabelToken,
			PropertyKey: key.PropertyKey,
			Dims:        meta.Dims,
			Metric:      meta.Metric,
		})
	}
	data, err := msgpack.Marshal(blobs)
	if err != nil {
		return err
	}
	return s.MetaSet(vectorDefsMetaKey, data)
}

// loadVectorDefs rehydrates the store-level def metadata at open. Called from
// New after the shards are up; in-memory stores have nothing persisted.
func (s *Store) loadVectorDefs() error {
	if s.inMemory {
		return nil
	}
	data, err := s.anchor().MetaGet(vectorDefsMetaKey)
	if err != nil || len(data) == 0 {
		return err
	}
	var blobs []vectorDefBlob
	if err := storeutil.SafeUnmarshal(data, &blobs); err != nil {
		return err
	}
	s.vectorDefMu.Lock()
	defer s.vectorDefMu.Unlock()
	for _, b := range blobs {
		s.vectorDefs[vectorDefKey{b.LabelToken, b.PropertyKey}] = vectorDefMeta{Dims: b.Dims, Metric: b.Metric}
	}
	return nil
}
