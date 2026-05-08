// Package badgerstore provides Store — the persistent Store
// implementation backed by Badger v4. Used as a backend by pkg/graph
// directly and as a shard implementation inside internal/tieredstore.
package badger

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/vmihailenco/msgpack/v5"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// CreatePropertyIndex creates a property index for the given label token and property key.
// Three-phase approach to prevent blocking concurrent reads/writes during slow I/O:
//
//	Phase 1 (write Lock): Install an empty live index so concurrent PutNode/ReplaceNode
//	writes are captured immediately. Snapshot existing node IDs.
//	Phase 2 (no lock): Fetch node data via public GetNode to build a backfill set.
//	Phase 3 (write Lock): Merge backfill entries into the live index, skipping IDs
//	that were already handled by concurrent writes during Phase 2.
//
// Returns ErrIndexExists if the index already exists.
func (bs *Store) CreatePropertyIndex(labelToken uint16, propertyKey string) error {
	// Phase 1: Install empty live index + snapshot IDs under write Lock.
	// Write lock (not RLock) ensures the index is visible to concurrent mutations.
	bs.idxMu.Lock()
	key := indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	if _, exists := bs.propertyIndexes[key]; exists {
		bs.idxMu.Unlock()
		return ErrIndexExists
	}
	liveIdx := indexpkg.NewPropertyIndex()
	liveIdx.Mutated = make(map[snowflake.ID]struct{})
	bs.propertyIndexes[key] = liveIdx
	var nids []types.NodeID
	if nodeIDs, ok := bs.labelIdx[labelToken]; ok {
		nids = make([]types.NodeID, 0, len(nodeIDs))
		for id := range nodeIDs {
			nids = append(nids, id)
		}
	}
	bs.idxMu.Unlock()

	// Phase 2: Fetch node data OUTSIDE any lock via public GetNode.
	// Builds a backfill index for nodes that existed before Phase 1.
	backfill := indexpkg.NewPropertyIndex()
	for _, nid := range nids {
		n, err := bs.GetNode(nid)
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue // deleted between snapshot and fetch
			}
			// Fatal error — remove the incomplete index.
			bs.idxMu.Lock()
			delete(bs.propertyIndexes, key)
			bs.idxMu.Unlock()
			return fmt.Errorf("graph: create property index: %w", err)
		}
		if val, found := n.GetProperty(propertyKey); found {
			backfill.Add(nid.SnowflakeID(), val)
		}
	}

	// Phase 3: Merge backfill into live index under write Lock.
	// Skip entries for IDs already handled by concurrent writes during Phase 2,
	// and entries for nodes deleted during Phase 2.
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()
	for vk, idSet := range backfill.Entries {
		for id := range idSet {
			if _, mutated := liveIdx.Mutated[id]; mutated {
				continue // concurrent write handled this ID during Phase 2
			}
			if _, alive := bs.nodeIDs[types.NodeID(id)]; !alive {
				continue // node deleted during Phase 2
			}
			if liveIdx.Entries[vk] == nil {
				liveIdx.Entries[vk] = make(map[snowflake.ID]struct{})
			}
			liveIdx.Entries[vk][id] = struct{}{}
		}
	}
	liveIdx.Mutated = nil // stop tracking — index creation complete
	bs.persistPropertyIndexDefs()
	return nil
}

// DropPropertyIndex removes a property index.
// Returns ErrIndexNotFound if the index does not exist.
func (bs *Store) DropPropertyIndex(labelToken uint16, propertyKey string) error {
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()

	key := indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	if _, exists := bs.propertyIndexes[key]; !exists {
		return ErrIndexNotFound
	}

	delete(bs.propertyIndexes, key)
	bs.persistPropertyIndexDefs()
	return nil
}

// propIdxDef is the serialization format for property index definitions.
type propIdxDef struct {
	LabelToken  uint16 `msgpack:"l"`
	PropertyKey string `msgpack:"p"`
}

// CreateTemporalIndex creates a temporal index on nodes with the given label token.
// Three-phase approach (same as CreatePropertyIndex) for safe concurrent operation.
// Returns ErrTemporalIndexExists if the index already exists.
func (bs *Store) CreateTemporalIndex(labelToken uint16) error {
	// Phase 1: Install empty live index + snapshot IDs under write Lock.
	bs.idxMu.Lock()
	if _, exists := bs.temporalIndexes[labelToken]; exists {
		bs.idxMu.Unlock()
		return ErrTemporalIndexExists
	}
	liveTI := indexpkg.NewTemporalIndex()
	bs.temporalIndexes[labelToken] = liveTI
	var nids []types.NodeID
	if nodeIDs, ok := bs.labelIdx[labelToken]; ok {
		nids = make([]types.NodeID, 0, len(nodeIDs))
		for id := range nodeIDs {
			nids = append(nids, id)
		}
	}
	bs.idxMu.Unlock()

	// Phase 2: Fetch node data OUTSIDE any lock via public GetNode.
	type nodeEntry struct {
		id   snowflake.ID
		from types.Instant
		to   types.Instant
	}
	backfill := make([]nodeEntry, 0, len(nids))
	for _, nid := range nids {
		n, err := bs.GetNode(nid)
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue // deleted between snapshot and fetch
			}
			// Fatal error — remove the incomplete index.
			bs.idxMu.Lock()
			delete(bs.temporalIndexes, labelToken)
			bs.idxMu.Unlock()
			return fmt.Errorf("graph: create temporal index: %w", err)
		}
		rawID := nid.SnowflakeID()
		from, to := indexpkg.NodeTemporalBounds(rawID, n.Temporal())
		backfill = append(backfill, nodeEntry{id: rawID, from: from, to: to})
	}

	// Phase 3: Merge backfill into live index under write Lock.
	// Skip IDs that were touched by concurrent mutations during Phase 2.
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()
	for _, entry := range backfill {
		if _, alive := bs.nodeIDs[types.NodeID(entry.id)]; !alive {
			continue // node deleted during Phase 2
		}
		// Only add if not already handled by a concurrent write.
		// The live index starts empty; any entry already present was added
		// by a concurrent PutNode/ReplaceNode that ran during Phase 2.
		found := false
		for _, e := range liveTI.Entries {
			if e.ID == entry.id {
				found = true
				break
			}
		}
		if !found {
			liveTI.Add(entry.id, entry.from, entry.to)
		}
	}
	bs.persistTemporalIndexDefs()
	return nil
}

// DropTemporalIndex removes a temporal index.
// Returns ErrTemporalIndexNotFound if the index does not exist.
func (bs *Store) DropTemporalIndex(labelToken uint16) error {
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()

	if _, exists := bs.temporalIndexes[labelToken]; !exists {
		return ErrTemporalIndexNotFound
	}

	delete(bs.temporalIndexes, labelToken)
	bs.persistTemporalIndexDefs()
	return nil
}

// CreateHighFrequencyIndex creates a time-bucketed high-frequency index on nodes
// with the given label token. Only one temporal index type can exist per label —
// returns ErrTemporalIndexExists if a temporalIndex or highFrequencyIndex already
// exists for this label.
func (bs *Store) CreateHighFrequencyIndex(labelToken uint16, bucketSize time.Duration) error {
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()

	if _, exists := bs.temporalIndexes[labelToken]; exists {
		return ErrTemporalIndexExists
	}
	if _, exists := bs.hfIndexes[labelToken]; exists {
		return ErrTemporalIndexExists
	}

	bs.hfIndexes[labelToken] = indexpkg.NewHighFrequencyIndex(bucketSize, 0)
	return nil
}

// DropHighFrequencyIndex removes the high-frequency index for the given label token.
// Returns ErrTemporalIndexNotFound if no high-frequency index exists.
func (bs *Store) DropHighFrequencyIndex(labelToken uint16) error {
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()

	if _, exists := bs.hfIndexes[labelToken]; !exists {
		return ErrTemporalIndexNotFound
	}
	delete(bs.hfIndexes, labelToken)
	return nil
}

// persistTemporalIndexDefs serializes the current temporal index label tokens to Badger.
// Caller must hold bs.idxMu write lock.
func (bs *Store) persistTemporalIndexDefs() {
	tokens := make([]uint16, 0, len(bs.temporalIndexes))
	for tok := range bs.temporalIndexes {
		tokens = append(tokens, tok)
	}
	if len(tokens) == 0 {
		bs.appendOps(writeOp{opType: writeOpDelete, key: storepkg.TemporalIndexDefsKey})
		return
	}
	data, err := msgpack.Marshal(tokens)
	if err != nil {
		slog.Error("graph: persist temporal index defs: marshal failed", "error", err)
		return // index still works in-memory; will retry on next change
	}
	bs.appendOps(writeOp{opType: writeOpSet, key: storepkg.TemporalIndexDefsKey, value: data})
}

// CreateVectorIndex creates a vector similarity index for nodes with the given label token,
// on the given property key, expecting vectors of length dims.
// Scans existing nodes to populate the index. Returns ErrVectorIndexExists on duplicate.
// Vector indexes are in-memory only and are not persisted.
func (bs *Store) CreateVectorIndex(labelToken uint16, propertyKey string, dims int, metric DistanceMetric) error {
	key := indexpkg.VectorIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}

	// Phase 1: Install empty placeholder under write lock for concurrent-write visibility.
	bs.idxMu.Lock()
	if _, exists := bs.vectorIndexes[key]; exists {
		bs.idxMu.Unlock()
		return ErrVectorIndexExists
	}
	vi := &indexpkg.VectorIndex{Dims: dims, Metric: metric}
	bs.vectorIndexes[key] = vi

	// Snapshot existing node IDs for population scan.
	nids := make([]types.NodeID, 0, len(bs.nodeIDs))
	for id := range bs.nodeIDs {
		nids = append(nids, id)
	}
	bs.idxMu.Unlock()

	// Phase 2: Populate from existing nodes (unlocked I/O).
	for _, nid := range nids {
		n, err := bs.GetNode(nid)
		if err != nil {
			continue // node may have been deleted concurrently
		}
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
		_ = vi.Add(nid.SnowflakeID(), vec) // dimension mismatch: skip entry, index is still usable
	}
	return nil
}

// DropVectorIndex removes a vector index.
// Returns ErrVectorIndexNotFound if the index does not exist.
func (bs *Store) DropVectorIndex(labelToken uint16, propertyKey string) error {
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()

	key := indexpkg.VectorIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	if _, exists := bs.vectorIndexes[key]; !exists {
		return ErrVectorIndexNotFound
	}
	delete(bs.vectorIndexes, key)
	return nil
}

// SearchNearestNodes returns the k nodes with vectors closest to query
// under the index defined for labelToken+propertyKey.
// Results are ordered by ascending distance (closest first).
// Returns ErrVectorIndexNotFound if no index exists.
// Returns ErrDimensionMismatch if query length differs from the index's dims.
// Returns nil, nil if the index exists but has no entries.
//
// The opts parameter is intentionally unused: Store is single-tier,
// so Depth has no meaning, and temporal filtering is applied by the Graph
// layer via searchNearestFiltered before this path is taken. The parameter
// is required by the Store interface contract.
func (bs *Store) SearchNearestNodes(labelToken uint16, propertyKey string, query []float32, k int, _ QueryOpts) ([]*types.Node, error) {
	bs.idxMu.RLock()
	key := indexpkg.VectorIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	vi, exists := bs.vectorIndexes[key]
	bs.idxMu.RUnlock()

	if !exists {
		return nil, ErrVectorIndexNotFound
	}

	rawIDs, err := vi.SearchNearest(query, k, nil)
	if err != nil {
		return nil, err
	}
	if len(rawIDs) == 0 {
		return nil, nil
	}
	// Fetch nodes in distance order — do NOT sort by ID (would destroy distance ranking).
	result := make([]*types.Node, 0, len(rawIDs))
	for _, id := range rawIDs {
		n, err := bs.GetNode(types.NodeID(id))
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
// Graph layer to perform vector search with an eligibility filter applied
// BEFORE the k-cut. The filter is invoked under the vector index read lock,
// so it must NOT call back into the store (deadlock).
//
// Returns raw snowflake.IDs in ascending distance order; the caller is
// responsible for resolving entities (current or historical version).
func (bs *Store) SearchNearestFiltered(labelToken uint16, propertyKey string, query []float32, k int, filter func(snowflake.ID) bool) ([]snowflake.ID, error) {
	bs.idxMu.RLock()
	key := indexpkg.VectorIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	vi, exists := bs.vectorIndexes[key]
	bs.idxMu.RUnlock()

	if !exists {
		return nil, ErrVectorIndexNotFound
	}
	return vi.SearchNearest(query, k, filter)
}

// persistPropertyIndexDefs serializes the current property index definitions to Badger.
// Caller must hold bs.idxMu write lock.
func (bs *Store) persistPropertyIndexDefs() {
	var defs []propIdxDef
	for key := range bs.propertyIndexes {
		defs = append(defs, propIdxDef{LabelToken: key.LabelToken, PropertyKey: key.PropertyKey})
	}
	if len(defs) == 0 {
		bs.appendOps(writeOp{opType: writeOpDelete, key: storepkg.PropIndexDefsKey})
		return
	}
	data, err := msgpack.Marshal(defs)
	if err != nil {
		slog.Error("graph: persist property index defs: marshal failed", "error", err)
		return // index still works in-memory; will retry on next change
	}
	bs.appendOps(writeOp{opType: writeOpSet, key: storepkg.PropIndexDefsKey, value: data})
}
