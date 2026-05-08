package tiered

import (
	"errors"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Property indexes ---

// ErrEventPropertyIndex is returned when attempting to create a property index
// on an event label in a Store. Only reference entities support indexes.
var ErrEventPropertyIndex = errors.New("graph: property indexes only supported for reference entities in Store")

// ErrPrimaryLabelClassMutation is returned when a label mutation would change a
// node's primary-label ontology class (reference vs event). Such a change would
// move the live entity to a different shard while leaving prior history on the
// original shard, fragmenting the version chain. Callers must instead delete
// and recreate the node under the new class.
var ErrPrimaryLabelClassMutation = errors.New("graph: label mutation would change primary-label class (reference<->event); not supported on Store")

// ensurePrimaryLabelClassUnchanged compares the primary-label classes of the
// pre-mutation and post-mutation nodes and rejects the mutation if they differ.
// old may be nil (entity already gone) — in that case the check is skipped.
func (ts *Store) ensurePrimaryLabelClassUnchanged(old, updated *types.Node) error {
	if old == nil || updated == nil {
		return nil
	}
	oldClass := ts.ontology.ClassifyByToken(old.PrimaryLabelToken().Value())
	newClass := ts.ontology.ClassifyByToken(updated.PrimaryLabelToken().Value())
	if oldClass != newClass {
		return ErrPrimaryLabelClassMutation
	}
	return nil
}

func (ts *Store) CreatePropertyIndex(labelToken uint16, propertyKey string) error {
	if ts.ontology.ClassifyByToken(labelToken) != ClassReference {
		return ErrEventPropertyIndex
	}
	return ts.refShard.CreatePropertyIndex(labelToken, propertyKey)
}

func (ts *Store) DropPropertyIndex(labelToken uint16, propertyKey string) error {
	shard := ts.shardForNode(labelToken)
	return shard.DropPropertyIndex(labelToken, propertyKey)
}

// --- Temporal indexes ---

// CreateTemporalIndex creates a temporal index on nodes with the given label token
// across all shards (reference + all event shards). New hot shards created via
// rotation will also inherit the index.
func (ts *Store) CreateTemporalIndex(labelToken uint16) error {
	stores, release, err := ts.allShardStoresWithLazyOpen()
	if err != nil {
		return err
	}
	defer release()

	for _, ns := range stores {
		if err := ns.store.CreateTemporalIndex(labelToken); err != nil && !errors.Is(err, ErrTemporalIndexExists) {
			return err
		}
	}

	ts.tempIdxMu.Lock()
	// Record label token if not already tracked.
	found := false
	for _, tok := range ts.tempIdxLabels {
		if tok == labelToken {
			found = true
			break
		}
	}
	if !found {
		ts.tempIdxLabels = append(ts.tempIdxLabels, labelToken)
	}
	ts.tempIdxMu.Unlock()
	return nil
}

// DropTemporalIndex removes the temporal index for the given label token
// from all shards.
func (ts *Store) DropTemporalIndex(labelToken uint16) error {
	stores, release, err := ts.allShardStoresWithLazyOpen()
	if err != nil {
		return err
	}
	defer release()

	var lastErr error
	found := false
	for _, ns := range stores {
		shard := ns.store
		if err := shard.DropTemporalIndex(labelToken); err != nil {
			if !errors.Is(err, ErrTemporalIndexNotFound) {
				lastErr = err
			}
		} else {
			found = true
		}
	}
	if lastErr != nil {
		return lastErr
	}

	ts.tempIdxMu.Lock()
	for i, tok := range ts.tempIdxLabels {
		if tok == labelToken {
			ts.tempIdxLabels = append(ts.tempIdxLabels[:i], ts.tempIdxLabels[i+1:]...)
			break
		}
	}
	ts.tempIdxMu.Unlock()

	if !found {
		return ErrTemporalIndexNotFound
	}
	return nil
}

// --- High-frequency indexes ---

// CreateHighFrequencyIndex creates a time-bucketed high-frequency index on nodes
// with the given label token across all shards (reference + all event shards).
// New hot shards created via rotation will NOT automatically inherit HFI — callers
// must re-call CreateHighFrequencyIndex after rotation if needed.
// Returns ErrTemporalIndexExists if any temporal index already exists for this label.
func (ts *Store) CreateHighFrequencyIndex(labelToken uint16, bucketSize time.Duration) error {
	stores, release, err := ts.allShardStoresWithLazyOpen()
	if err != nil {
		return err
	}
	defer release()

	for _, ns := range stores {
		if err := ns.store.CreateHighFrequencyIndex(labelToken, bucketSize); err != nil && !errors.Is(err, ErrTemporalIndexExists) {
			return err
		}
	}
	return nil
}

// DropHighFrequencyIndex removes the high-frequency index for the given label token
// from all shards. Returns ErrTemporalIndexNotFound if no index exists on any shard.
func (ts *Store) DropHighFrequencyIndex(labelToken uint16) error {
	stores, release, err := ts.allShardStoresWithLazyOpen()
	if err != nil {
		return err
	}
	defer release()

	var lastErr error
	found := false
	for _, ns := range stores {
		shard := ns.store
		if err := shard.DropHighFrequencyIndex(labelToken); err != nil {
			if !errors.Is(err, ErrTemporalIndexNotFound) {
				lastErr = err
			}
		} else {
			found = true
		}
	}
	if lastErr != nil {
		return lastErr
	}
	if !found {
		return ErrTemporalIndexNotFound
	}
	return nil
}

// CreateVectorIndex creates a vector similarity index spanning all shards.
// The index is maintained at the Store level (not per-shard).
// Scans existing nodes across all shards to populate the index.
// Returns ErrVectorIndexExists on duplicate.
func (ts *Store) CreateVectorIndex(labelToken uint16, propertyKey string, dims int, metric DistanceMetric) error {
	key := indexpkg.VectorIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}

	ts.vectorIdxMu.Lock()
	if _, exists := ts.vectorIndexes[key]; exists {
		ts.vectorIdxMu.Unlock()
		return ErrVectorIndexExists
	}
	vi := &indexpkg.VectorIndex{Dims: dims, Metric: metric}
	ts.vectorIndexes[key] = vi
	ts.vectorIdxMu.Unlock()

	// Populate from all existing nodes across all shards.
	nodes, err := ts.AllNodes(QueryOpts{})
	if err != nil {
		return err
	}
	for _, n := range nodes {
		if !n.HasLabelTokenRaw(labelToken) {
			continue
		}
		val, ok := n.GetProperty(propertyKey)
		if !ok {
			continue
		}
		vec, ok := indexpkg.ToFloat32Slice(val)
		if !ok {
			continue
		}
		id := n.ID().SnowflakeID()
		_ = vi.Add(id, vec) // dimension mismatch: skip entry, index is still usable
	}
	return nil
}

// DropVectorIndex removes a vector index.
// Returns ErrVectorIndexNotFound if the index does not exist.
func (ts *Store) DropVectorIndex(labelToken uint16, propertyKey string) error {
	ts.vectorIdxMu.Lock()
	defer ts.vectorIdxMu.Unlock()

	key := indexpkg.VectorIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	if _, exists := ts.vectorIndexes[key]; !exists {
		return ErrVectorIndexNotFound
	}
	delete(ts.vectorIndexes, key)
	return nil
}

// SearchNearestNodes returns the k nodes with vectors closest to query
// under the index defined for labelToken+propertyKey.
// Results are ordered by ascending distance (closest first).
// Returns ErrVectorIndexNotFound if no index exists.
// Returns ErrDimensionMismatch if query length differs from the index's dims.
// Returns nil, nil if the index exists but has no entries.
func (ts *Store) SearchNearestNodes(labelToken uint16, propertyKey string, query []float32, k int, opts QueryOpts) ([]*types.Node, error) {
	ts.vectorIdxMu.RLock()
	key := indexpkg.VectorIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	vi, exists := ts.vectorIndexes[key]
	ts.vectorIdxMu.RUnlock()

	if !exists {
		return nil, ErrVectorIndexNotFound
	}

	// Depth gating: archive is the coldest tier of reference data. For
	// DepthHot/DepthWarm, archived nodes must be excluded so callers don't
	// see entities they explicitly asked to skip. Mirrors the gating policy
	// of NodesByLabel/AllNodes/RelationshipsByType.
	//
	// Filtering happens BEFORE the heap selection (passed into searchNearest
	// as the filter callback): otherwise a near-but-archived candidate
	// could push a farther-but-eligible candidate out of the top-k.
	filter := ts.depthFilter(opts.Depth)
	ids, err := vi.SearchNearest(query, k, filter)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	// Fetch nodes in distance order — do NOT sort by ID (would destroy distance ranking).
	result := make([]*types.Node, 0, len(ids))
	for _, id := range ids {
		n, err := ts.GetNode(types.NodeID(id))
		if err != nil {
			continue // node may have been deleted concurrently
		}
		result = append(result, n)
	}
	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

// SearchNearestFiltered is the package-internal entry point used by the
// Graph layer's TEMPORAL path to perform vector search with an
// eligibility filter applied BEFORE the k-cut. The filter is invoked
// under the vector index read lock, so it must NOT call back into the
// store (deadlock).
//
// Depth gating is NOT applied here. Graph.SearchNearestNodes already
// rejects (temporal + Depth != DepthAll) at the entry point with
// ErrDepthTemporalUnsupported, so by the time we reach this path the
// effective Depth is DepthAll and the depthFilter would be a no-op.
// The caller's filter is the only filter that needs composition.
// (For the non-temporal path, depthFilter is built inside
// Store.SearchNearestNodes and passed directly to
// vi.searchNearest — it does NOT route through this function.)
//
// Returns raw snowflake.IDs in ascending distance order; the caller is
// responsible for resolving entities (current or historical version).
func (ts *Store) SearchNearestFiltered(labelToken uint16, propertyKey string, query []float32, k int, filter func(snowflake.ID) bool) ([]snowflake.ID, error) {
	ts.vectorIdxMu.RLock()
	key := indexpkg.VectorIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	vi, exists := ts.vectorIndexes[key]
	ts.vectorIdxMu.RUnlock()

	if !exists {
		return nil, ErrVectorIndexNotFound
	}
	return vi.SearchNearest(query, k, filter)
}

// depthFilter returns an eligibility predicate that excludes archive-resident
// nodes from DepthHot/DepthWarm queries. Returns nil for DepthAll (no filter
// needed — accept everything). Mirrors the archive-exclusion policy in
// NodesByLabel/AllNodes/RelationshipsByType for non-temporal reads.
//
// Note: this filter only checks refArchive residency. Event-shard nodes are
// not excluded here because the vector index is populated for all shards
// (refShard, refArchive, all event shards) and event-shard membership cannot
// be cheaply derived from an ID alone. In practice, vector indexes target
// reference labels (the auto-maintenance code path requires a label match),
// so the archive distinction is the meaningful one for Depth.
func (ts *Store) depthFilter(depth ShardDepth) func(snowflake.ID) bool {
	if depth == DepthAll {
		return nil
	}
	archive := ts.refArchive.Load()
	if archive == nil {
		return nil // archive not open: nothing to exclude
	}
	return func(id snowflake.ID) bool {
		return !archive.HasNodeID(id)
	}
}
