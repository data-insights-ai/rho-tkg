// Package memory provides memory.Store — the thread-safe in-memory
// implementation of the pkg/graph/store.Store interface. Used as the
// default backend by pkg/graph and also as a building block in tests.
package memory

import (
	"fmt"
	"log/slog"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- Property indexes ---

// NodeRangeCardinality returns the COUNT of the label's nodes whose numeric
// propertyKey value lies within [min, max] (per the inclusivity flags), summed
// from the property index's sorted per-value bucket sizes (R1) — O(distinct
// values in range), no node scan. exact=false declines (no usable index /
// poisoned by an integer past 2^53) and the caller scans-and-counts. Mirrors the
// badger store's method so both backends answer `count(p) WHERE p.k > x` identically.
func (ms *Store) NodeRangeCardinality(labelToken uint16, propertyKey string, min, max float64, inclMin, inclMax bool) (int64, bool, error) {
	if ms == nil {
		return 0, false, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return 0, false, err
	}
	idx, ok := ms.propertyIndexes[indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}]
	if !ok {
		return 0, false, nil
	}
	count, exact := idx.RangeCardinality(min, max, inclMin, inclMax)
	return count, exact, nil
}

// CreatePropertyIndex creates a property index for the given label token and property key.
// Three-phase approach (mirrors the badger backend and CLAUDE.md's documented pattern) so
// scanning a large existing label does not hold the store's single mutex — and therefore
// block every concurrent read/write — for the whole scan (BACKLOG 17h):
//
//	Phase 1 (Lock): install an empty live index so concurrent PutNode/ReplaceNode writes are
//	captured immediately via Mutated tracking. Snapshot the label's current node IDs.
//	Phase 2 (brief per-row RLock, never held across the scan): read each snapshotted node's
//	current value. Safe because every stored *types.Node is a frozen, immutable-once-cached
//	copy (freezeNodeCopy) — a concurrent write always REPLACES the map entry rather than
//	mutating a row in place, so a stale pointer read under a since-released lock can never
//	tear.
//	Phase 3 (Lock): merge the backfill into the live index, skipping IDs a concurrent write
//	already handled (Mutated) or that were deleted during Phase 2.
//
// Returns ErrIndexExists if the index already exists.
func (ms *Store) CreatePropertyIndex(labelToken uint16, propertyKey string) error {
	if ms == nil {
		return ErrNilStore
	}

	// Phase 1. checkOpenLocked before argument validation — a closed store
	// must report ErrStoreClosed even for otherwise-invalid arguments (the
	// lifecycle-before-validation contract every index door shares, pinned
	// by TestMemoryStoreIndexAPIsCheckLifecycleBeforeValidation).
	ms.mu.Lock()
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.Unlock()
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		ms.mu.Unlock()
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		ms.mu.Unlock()
		return err
	}
	key := indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	if _, exists := ms.propertyIndexes[key]; exists {
		ms.mu.Unlock()
		return ErrIndexExists
	}
	liveIdx := indexpkg.NewPropertyIndex()
	liveIdx.Mutated = make(map[snowflake.ID]struct{})
	ms.propertyIndexes[key] = liveIdx
	nids := ms.labelNodeIDsSnapshotLocked(labelToken)
	ms.mu.Unlock()

	// Phase 2.
	backfill := indexpkg.NewPropertyIndex()
	for _, nid := range nids {
		ms.mu.RLock()
		n, ok := ms.nodes[nid]
		ms.mu.RUnlock()
		if !ok {
			continue // deleted between snapshot and fetch
		}
		if valueKey, found := n.IndexablePropertyValueKey(propertyKey); found {
			backfill.AddKey(nid.SnowflakeID(), valueKey)
		}
	}

	// Phase 3.
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if err := requirePropertyIndexCurrentForCreate(ms.propertyIndexes, key, liveIdx); err != nil {
		return err
	}
	for vk, idSet := range backfill.Entries {
		for id := range idSet {
			if _, mutated := liveIdx.Mutated[id]; mutated {
				continue // concurrent write handled this ID during Phase 2
			}
			if _, alive := ms.nodes[types.NodeID(id)]; !alive {
				continue // node deleted during Phase 2
			}
			// AddKey, not a direct Entries write — the index's ordered
			// numeric view is maintained inside AddKey.
			liveIdx.AddKey(id, vk)
		}
	}
	liveIdx.Mutated = nil // stop tracking — index creation complete
	return nil
}

// labelNodeIDsSnapshotLocked returns a slice copy of the label's current node
// ID set. The caller MUST hold at least ms.mu's read lock.
func (ms *Store) labelNodeIDsSnapshotLocked(labelToken uint16) []types.NodeID {
	idSet, ok := ms.labelIdx[labelToken]
	if !ok {
		return nil
	}
	nids := make([]types.NodeID, 0, len(idSet))
	for nid := range idSet {
		nids = append(nids, nid)
	}
	return nids
}

// requirePropertyIndexCurrentForCreate mirrors the badger backend's
// identically-named helper (badgerstore_index.go) — the index-creation
// Phase 3 "is my just-installed placeholder still the live index" check,
// shared shape across both backends' 3-phase index-creation doors. Memory has
// no phase-2 fatal-I/O-error path (a map lookup cannot fail), so unlike
// badger there is no companion delete-on-fatal-error helper.
func requirePropertyIndexCurrentForCreate(idxs map[indexpkg.PropertyIndexKey]*indexpkg.PropertyIndex, key indexpkg.PropertyIndexKey, expected *indexpkg.PropertyIndex) error {
	current := idxs[key]
	if current == expected {
		return nil
	}
	if current == nil {
		return fmt.Errorf("graph: create property index: index dropped during creation: %w", ErrIndexNotFound)
	}
	return fmt.Errorf("graph: create property index: index replaced during creation: %w", ErrIndexExists)
}

// DropPropertyIndex removes a property index.
// Returns ErrIndexNotFound if the index does not exist.
func (ms *Store) DropPropertyIndex(labelToken uint16, propertyKey string) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return err
	}

	key := indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	if _, exists := ms.propertyIndexes[key]; !exists {
		return ErrIndexNotFound
	}

	delete(ms.propertyIndexes, key)
	return nil
}

// --- Composite property indexes ---

// CreateCompositePropertyIndex creates a composite property index over the
// declared, ORDER-PRESERVING keys (2..4) for the given label token. Scans all
// existing nodes with that label to populate the index. Returns ErrIndexExists
// if an index for the exact same (labelToken, ordered keys) already exists —
// a different key ORDER for the same key SET is a distinct definition.
func (ms *Store) CreateCompositePropertyIndex(labelToken uint16, keys []string) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateCompositeIndexKeys(keys); err != nil {
		return err
	}

	key := indexpkg.CompositeIndexKey{LabelToken: labelToken, Keys: indexpkg.EncodeCompositeKeyTuple(keys)}
	if _, exists := ms.compositeIndexes[key]; exists {
		return ErrIndexExists
	}

	idx := indexpkg.NewCompositePropertyIndex(keys)

	// Populate from existing nodes with this label.
	if nodeIDs, ok := ms.labelIdx[labelToken]; ok {
		for nodeID := range nodeIDs {
			n := ms.nodes[nodeID]
			if n == nil {
				continue
			}
			if vk, found := indexpkg.NodeCompositeValueKey(idx.Keys, n); found {
				idx.AddKey(nodeID.SnowflakeID(), vk)
			}
		}
	}

	indexpkg.RegisterCompositeIndex(ms.compositeIndexes, ms.compositeIndexesByLabel, key, idx)
	return nil
}

// DropCompositePropertyIndex removes a composite property index declared
// over the exact ordered keys. Returns ErrIndexNotFound if no such
// definition exists.
func (ms *Store) DropCompositePropertyIndex(labelToken uint16, keys []string) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateCompositeIndexKeys(keys); err != nil {
		return err
	}

	key := indexpkg.CompositeIndexKey{LabelToken: labelToken, Keys: indexpkg.EncodeCompositeKeyTuple(keys)}
	if _, exists := ms.compositeIndexes[key]; !exists {
		return ErrIndexNotFound
	}

	indexpkg.UnregisterCompositeIndex(ms.compositeIndexes, ms.compositeIndexesByLabel, key)
	return nil
}

// NodesByLabelAndProperties returns nodes matching labelToken whose current
// row matches EVERY (key, value) pair in values (AND-conjunction, equality
// only — v1 has no partial-prefix or range semantics). Uses a matching
// composite index if one exists whose declared key SET equals values' keys;
// falls back to a label scan + post-filter otherwise.
func (ms *Store) NodesByLabelAndProperties(labelToken uint16, values map[string]any, opts QueryOpts) ([]*types.Node, error) {
	if ms == nil {
		return nil, ErrNilStore
	}

	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return nil, err
	}
	if err := validateCompositeQueryValues(values); err != nil {
		return nil, err
	}

	if idx, found := indexpkg.FindCompositeIndexForQuery(ms.compositeIndexes, ms.compositeIndexesByLabel, labelToken, values); found {
		vk, ok := indexpkg.QueryCompositeValueKey(idx.Keys, values)
		if !ok {
			return nil, nil
		}
		ids := idx.NodeIDs(vk)
		if len(ids) == 0 {
			return nil, nil
		}
		storepkg.SortNodeIDs(ids)
		return ms.nodesByLabelPropertiesFromIDs(labelToken, values, ids, opts), nil
	}

	// Fallback: label scan + post-filter.
	slog.Debug("graph: NodesByLabelAndProperties using full label scan (no matching composite index)",
		"labelToken", labelToken, "keys", len(values))
	labelIDs := ms.labelIdx[labelToken]
	if len(labelIDs) == 0 {
		return nil, nil
	}
	matchIDs := make([]types.NodeID, 0, len(labelIDs))
	for id := range labelIDs {
		matchIDs = append(matchIDs, id)
	}
	storepkg.SortNodeIDs(matchIDs)
	return ms.nodesByLabelPropertiesFromIDs(labelToken, values, matchIDs, opts), nil
}

func (ms *Store) nodesByLabelPropertiesFromIDs(labelToken uint16, values map[string]any, ids []types.NodeID, opts QueryOpts) []*types.Node {
	ids = storepkg.PaginateNodeIDs(ids, opts.After, 0)
	if len(ids) == 0 {
		return nil
	}

	hasTemporal := storepkg.HasTemporalFilter(opts)
	capHint := len(ids)
	if opts.Limit > 0 && opts.Limit < capHint {
		capHint = opts.Limit
	}
	result := make([]*types.Node, 0, capHint)
	for _, id := range ids {
		if n, ok := ms.nodes[id]; ok {
			if !n.HasLabelTokenRaw(labelToken) {
				continue
			}
			if !indexpkg.NodeMatchesAllProperties(n, values) {
				continue
			}
			if hasTemporal && !storepkg.MatchesTemporalFilter(id.SnowflakeID(), n.Temporal(), opts) {
				continue
			}
			result = append(result, n.DeepCopy())
			if opts.Limit > 0 && len(result) >= opts.Limit {
				break
			}
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// addNodeToCompositeIndexesLocked/removeNodeFromCompositeIndexesLocked are
// the composite-index counterparts every node-mutation door calls alongside
// indexpkg.AddNodeToPropertyIndexes/RemoveNodeFromPropertyIndexes (same call
// sites — grep both together).
func (ms *Store) addNodeToCompositeIndexesLocked(n *types.Node, rawID snowflake.ID) {
	indexpkg.AddNodeToCompositeIndexes(ms.compositeIndexes, ms.compositeIndexesByLabel, n, rawID)
}

func (ms *Store) removeNodeFromCompositeIndexesLocked(n *types.Node, rawID snowflake.ID) {
	indexpkg.RemoveNodeFromCompositeIndexes(ms.compositeIndexes, ms.compositeIndexesByLabel, n, rawID)
}

// validateCompositeQueryValues validates the query-side (key,value) map: 2..4
// keys, no shadow keys, and every value indexable per the property allowlist.
func validateCompositeQueryValues(values map[string]any) error {
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	if err := storecontract.ValidateCompositeIndexKeys(keys); err != nil {
		return err
	}
	for _, v := range values {
		if err := types.ValidatePropertyValue(v); err != nil {
			return fmt.Errorf("graph: nodes by label and properties value: %w", err)
		}
	}
	return nil
}

// --- Temporal indexes ---

// CreateTemporalIndex creates a temporal interval index on nodes with the given label token.
// Scans existing nodes with that label to populate the index.
// Returns ErrTemporalIndexExists if an index already exists for this label.
func (ms *Store) CreateTemporalIndex(labelToken uint16) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}

	if _, exists := ms.temporalIndexes[labelToken]; exists {
		return ErrTemporalIndexExists
	}
	if _, exists := ms.hfIndexes[labelToken]; exists {
		return ErrTemporalIndexExists
	}

	ti := indexpkg.NewTemporalIndex()
	if nodeIDs, ok := ms.labelIdx[labelToken]; ok {
		for nodeID := range nodeIDs {
			n := ms.nodes[nodeID]
			if n == nil {
				continue
			}
			rawID := nodeID.SnowflakeID()
			// Fold the current version AND every history version's bounds into the
			// node's ENVELOPE (B4 sound superset): a past version whose valid
			// interval differs from the current one must still be covered, so the
			// core resolver's predicate-anywhere candidate narrowing never misses it.
			from, to := indexpkg.NodeTemporalBounds(rawID, n.Temporal())
			ti.Extend(rawID, from, to)
			for _, hv := range ms.nodeHistory[nodeID] {
				if hv == nil {
					continue
				}
				hf, ht := indexpkg.NodeTemporalBounds(rawID, hv.Temporal())
				ti.Extend(rawID, hf, ht)
			}
		}
	}

	ms.temporalIndexes[labelToken] = ti
	return nil
}

// DropTemporalIndex removes a temporal index for the given label token.
// Returns ErrTemporalIndexNotFound if no index exists.
func (ms *Store) DropTemporalIndex(labelToken uint16) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}

	if _, exists := ms.temporalIndexes[labelToken]; !exists {
		return ErrTemporalIndexNotFound
	}
	delete(ms.temporalIndexes, labelToken)
	return nil
}

// --- High-frequency indexes ---

// CreateHighFrequencyIndex creates a time-bucketed high-frequency index on nodes
// with the given label token. Only one temporal index type can exist per label —
// returns ErrInvalidTemporalIndexConfig if bucketSize is not a positive whole
// millisecond and
// returns ErrTemporalIndexExists if a temporalIndex or highFrequencyIndex already
// exists for this label.
func (ms *Store) CreateHighFrequencyIndex(labelToken uint16, bucketSize time.Duration) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateHighFrequencyBucketSize(bucketSize); err != nil {
		return err
	}

	if _, exists := ms.temporalIndexes[labelToken]; exists {
		return ErrTemporalIndexExists
	}
	if _, exists := ms.hfIndexes[labelToken]; exists {
		return ErrTemporalIndexExists
	}

	hfi := indexpkg.NewHighFrequencyIndex(bucketSize, 0)
	if nodeIDs, ok := ms.labelIdx[labelToken]; ok {
		for nodeID := range nodeIDs {
			n := ms.nodes[nodeID]
			if n == nil {
				continue
			}
			from, _ := indexpkg.NodeTemporalBounds(nodeID.SnowflakeID(), n.Temporal())
			hfi.Add(nodeID, from)
		}
	}

	ms.hfIndexes[labelToken] = hfi
	return nil
}

// DropHighFrequencyIndex removes the high-frequency index for the given label token.
// Returns ErrTemporalIndexNotFound if no high-frequency index exists.
func (ms *Store) DropHighFrequencyIndex(labelToken uint16) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}

	if _, exists := ms.hfIndexes[labelToken]; !exists {
		return ErrTemporalIndexNotFound
	}
	delete(ms.hfIndexes, labelToken)
	return nil
}

// CreateVectorIndex creates a vector similarity index for nodes with the given label token,
// on the given property key, expecting vectors of length dims. The index
// defaults to the approximate HNSW engine; use CreateVectorIndexWithOptions
// for the brute-force escape hatch or HNSW tuning.
// Scans existing nodes with that label to populate the index. Returns ErrVectorIndexExists on duplicate.
func (ms *Store) CreateVectorIndex(labelToken uint16, propertyKey string, dims int, metric DistanceMetric) error {
	return ms.CreateVectorIndexWithOptions(labelToken, propertyKey, dims, metric, storecontract.VectorIndexOptions{})
}

// CreateVectorIndexWithOptions is CreateVectorIndex with additional control
// over the search engine (opts.UseBruteForce) and HNSW tuning (opts.M /
// EfConstruction / EfSearch). A zero-value opts is identical to
// CreateVectorIndex (documented HNSW defaults).
func (ms *Store) CreateVectorIndexWithOptions(labelToken uint16, propertyKey string, dims int, metric DistanceMetric, opts storecontract.VectorIndexOptions) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
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
	if err := indexpkg.ValidateVectorIndexOptions(opts); err != nil {
		return err
	}

	key := indexpkg.VectorIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	if _, exists := ms.vectorIndexes[key]; exists {
		return ErrVectorIndexExists
	}
	vi := &indexpkg.VectorIndex{Dims: dims, Metric: metric}
	indexpkg.ApplyVectorIndexOptions(vi, opts)
	ms.vectorIndexes[key] = vi

	// Populate from nodes carrying this label. Keep this backfill shape aligned
	// with the other index builders and avoid scanning unrelated node rows.
	if nodeIDs, ok := ms.labelIdx[labelToken]; ok {
		for id := range nodeIDs {
			n := ms.nodes[id]
			if n == nil {
				continue
			}
			vec, ok := n.Float32SlicePropertyCopy(propertyKey)
			if !ok {
				continue
			}
			if err := vi.AddOwned(id.SnowflakeID(), vec); err != nil {
				delete(ms.vectorIndexes, key)
				return fmt.Errorf("graph: create vector index: node %d: %w", id.SnowflakeID(), err)
			}
		}
	}
	return nil
}

// DropVectorIndex removes a vector index.
// Returns ErrVectorIndexNotFound if the index does not exist.
func (ms *Store) DropVectorIndex(labelToken uint16, propertyKey string) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return err
	}

	key := indexpkg.VectorIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	if _, exists := ms.vectorIndexes[key]; !exists {
		return ErrVectorIndexNotFound
	}
	delete(ms.vectorIndexes, key)
	return nil
}

// SearchNearestNodes returns the k nodes with vectors closest to query
// under the index defined for labelToken+propertyKey.
// Results are ordered by ascending distance (closest first).
// Returns ErrVectorIndexNotFound if no index exists.
// Returns ErrDimensionMismatch if query length differs from the index's dims.
// Returns ErrInvalidVectorValue if query contains NaN or infinity.
// Returns nil, nil if the index exists but has no entries.
//
// Temporal filters and After/Limit are applied to the distance-ordered result.
// Depth has no meaning for this single-tier backend beyond enum validation.
func (ms *Store) SearchNearestNodes(labelToken uint16, propertyKey string, query []float32, k int, opts QueryOpts) ([]*types.Node, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.RUnlock()
		return nil, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		ms.mu.RUnlock()
		return nil, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		ms.mu.RUnlock()
		return nil, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		ms.mu.RUnlock()
		return nil, err
	}
	key := indexpkg.VectorIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	vi, exists := ms.vectorIndexes[key]
	ms.mu.RUnlock()

	if !exists {
		return nil, ErrVectorIndexNotFound
	}
	if k <= 0 {
		if _, err := vi.SearchNearest(query, k, nil); err != nil {
			return nil, err
		}
		return nil, nil
	}

	dims := vi.Dims
	filter, err := ms.vectorTemporalFilter(vi, key, dims, opts)
	if err != nil {
		return nil, err
	}
	ids, err := vi.SearchNearest(query, k, filter)
	if err != nil {
		return nil, err
	}
	// Fetch nodes in distance order — do NOT sort by ID (would destroy distance ranking).
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	hasTemporal := storepkg.HasTemporalFilter(opts)
	result := make([]*types.Node, 0, len(ids))
	for _, id := range ids {
		if n, ok := ms.nodes[types.NodeID(id)]; ok {
			if !indexpkg.NodeMatchesVectorIndex(n, key, dims) {
				continue
			}
			if hasTemporal && !storepkg.MatchesTemporalFilter(id, n.Temporal(), opts) {
				continue
			}
			result = append(result, n.DeepCopy())
		}
	}
	if len(result) == 0 {
		return nil, nil
	}
	return storepkg.PaginateNodesInOrder(result, opts.After, opts.Limit), nil
}

func (ms *Store) vectorTemporalFilter(vi *indexpkg.VectorIndex, key indexpkg.VectorIndexKey, dims int, opts QueryOpts) (func(snowflake.ID) bool, error) {
	if !storepkg.HasTemporalFilter(opts) {
		return nil, nil
	}
	ids := vi.IDs()
	eligible := make(map[snowflake.ID]struct{})
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	for _, id := range ids {
		n := ms.nodes[types.NodeID(id)]
		if n == nil {
			continue
		}
		if !indexpkg.NodeMatchesVectorIndex(n, key, dims) {
			continue
		}
		if storepkg.MatchesTemporalFilter(id, n.Temporal(), opts) {
			eligible[id] = struct{}{}
		}
	}
	return func(id snowflake.ID) bool {
		_, ok := eligible[id]
		return ok
	}, nil
}

// SearchNearestFiltered is the package-internal entry point used by the
// Graph layer to perform vector search with an eligibility filter applied
// BEFORE the k-cut. The filter is combined with current-row shape validation
// before the vector index heap selection.
//
// Returns raw snowflake.IDs in ascending distance order; the caller is
// responsible for resolving entities (current or historical version).
func (ms *Store) SearchNearestFiltered(labelToken uint16, propertyKey string, query []float32, k int, filter func(snowflake.ID) bool) ([]snowflake.ID, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.RUnlock()
		return nil, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		ms.mu.RUnlock()
		return nil, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		ms.mu.RUnlock()
		return nil, err
	}
	key := indexpkg.VectorIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	vi, exists := ms.vectorIndexes[key]
	ms.mu.RUnlock()

	if !exists {
		return nil, ErrVectorIndexNotFound
	}
	dims := vi.Dims
	var filterErr error
	shapeFilter := func(id snowflake.ID) bool {
		ms.mu.RLock()
		if err := ms.checkOpenLocked(); err != nil {
			ms.mu.RUnlock()
			if filterErr == nil {
				filterErr = err
			}
			return false
		}
		n := ms.nodes[types.NodeID(id)]
		if n == nil {
			ms.mu.RUnlock()
			return false
		}
		matches := indexpkg.NodeMatchesVectorIndex(n, key, dims)
		ms.mu.RUnlock()
		if !matches {
			return false
		}
		return filter == nil || filter(id)
	}
	ids, err := vi.SearchNearest(query, k, shapeFilter)
	if err != nil {
		return nil, err
	}
	if filterErr != nil {
		return nil, filterErr
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	return ids, nil
}

// NodesByLabelAndProperty returns nodes matching the label and property value,
// with optional pagination and temporal filtering. Uses the property index if
// one exists; falls back to label scan + property filter.
// Results are sorted by snowflake.ID for deterministic output.
func (ms *Store) NodesByLabelAndProperty(labelToken uint16, propKey string, value any, opts QueryOpts) ([]*types.Node, error) {
	if ms == nil {
		return nil, ErrNilStore
	}

	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propKey); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return nil, err
	}
	if err := types.ValidatePropertyValue(value); err != nil {
		return nil, fmt.Errorf("graph: nodes by label and property value: %w", err)
	}
	targetKey := indexpkg.PropertyValueKey(value)
	if targetKey == "" {
		return nil, nil
	}

	key := indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propKey}
	if idx, ok := ms.propertyIndexes[key]; ok {
		// Indexed path: collect matching IDs, sort, then verify fetched rows before pagination limit.
		ids := idx.NodeIDs(value)
		if len(ids) == 0 {
			return nil, nil
		}
		storepkg.SortNodeIDs(ids)
		return ms.nodesByLabelPropertyFromIDs(labelToken, propKey, targetKey, ids, opts), nil
	}

	// Fallback: label scan + property filter.
	slog.Debug("graph: NodesByLabelAndProperty using full label scan (no property index)",
		"labelToken", labelToken, "propertyKey", propKey)
	labelIDs := ms.labelIdx[labelToken]
	if len(labelIDs) == 0 {
		return nil, nil
	}

	// Collect candidate IDs from label scan.
	matchIDs := make([]types.NodeID, 0, len(labelIDs))
	for id := range labelIDs {
		matchIDs = append(matchIDs, id)
	}

	if len(matchIDs) == 0 {
		return nil, nil
	}
	storepkg.SortNodeIDs(matchIDs)
	return ms.nodesByLabelPropertyFromIDs(labelToken, propKey, targetKey, matchIDs, opts), nil
}

func (ms *Store) nodesByLabelPropertyFromIDs(labelToken uint16, propKey, targetKey string, ids []types.NodeID, opts QueryOpts) []*types.Node {
	ids = storepkg.PaginateNodeIDs(ids, opts.After, 0)
	if len(ids) == 0 {
		return nil
	}

	hasTemporal := storepkg.HasTemporalFilter(opts)
	capHint := len(ids)
	if opts.Limit > 0 && opts.Limit < capHint {
		capHint = opts.Limit
	}
	result := make([]*types.Node, 0, capHint)
	for _, id := range ids {
		if n, ok := ms.nodes[id]; ok {
			if !n.HasLabelTokenRaw(labelToken) {
				continue
			}
			if valueKey, found := n.IndexablePropertyValueKey(propKey); !found || valueKey != targetKey {
				continue
			}
			if hasTemporal && !storepkg.MatchesTemporalFilter(id.SnowflakeID(), n.Temporal(), opts) {
				continue
			}
			result = append(result, n.DeepCopy())
			if opts.Limit > 0 && len(result) >= opts.Limit {
				break
			}
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}
