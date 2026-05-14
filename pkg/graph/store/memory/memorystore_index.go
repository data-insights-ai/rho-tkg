// Package memory provides memory.Store — the thread-safe in-memory
// implementation of the pkg/graph/store.Store interface. Used as the
// default backend by pkg/graph and also as a building block in tests.
package memory

import (
	"fmt"
	"log/slog"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	storecontract "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Property indexes ---

// CreatePropertyIndex creates a property index for the given label token and property key.
// Scans all existing nodes with that label to populate the index.
// Returns ErrIndexExists if the index already exists.
func (ms *Store) CreatePropertyIndex(labelToken uint16, propertyKey string) error {
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
	if _, exists := ms.propertyIndexes[key]; exists {
		return ErrIndexExists
	}

	idx := indexpkg.NewPropertyIndex()

	// Populate from existing nodes with this label.
	if nodeIDs, ok := ms.labelIdx[labelToken]; ok {
		for nodeID := range nodeIDs {
			n := ms.nodes[nodeID]
			if n == nil {
				continue
			}
			if valueKey, found := n.IndexablePropertyValueKey(propertyKey); found {
				idx.AddKey(nodeID.SnowflakeID(), valueKey)
			}
		}
	}

	ms.propertyIndexes[key] = idx
	return nil
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
			from, to := indexpkg.NodeTemporalBounds(rawID, n.Temporal())
			ti.AddKnownAbsent(rawID, from, to)
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
// on the given property key, expecting vectors of length dims.
// Scans existing nodes with that label to populate the index. Returns ErrVectorIndexExists on duplicate.
func (ms *Store) CreateVectorIndex(labelToken uint16, propertyKey string, dims int, metric DistanceMetric) error {
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

	key := indexpkg.VectorIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	if _, exists := ms.vectorIndexes[key]; exists {
		return ErrVectorIndexExists
	}
	vi := &indexpkg.VectorIndex{Dims: dims, Metric: metric}
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
