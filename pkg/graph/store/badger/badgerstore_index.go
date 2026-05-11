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
	storecontract "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
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
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return err
	}
	if err := bs.checkOpen(); err != nil {
		return err
	}

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
			deletePropertyIndexIfCurrent(bs.propertyIndexes, key, liveIdx)
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
	if err := requirePropertyIndexCurrentForCreate(bs.propertyIndexes, key, liveIdx); err != nil {
		bs.idxMu.Unlock()
		return err
	}
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
	bs.idxMu.Unlock()
	return bs.flushIfSyncWrites()
}

// DropPropertyIndex removes a property index.
// Returns ErrIndexNotFound if the index does not exist.
func (bs *Store) DropPropertyIndex(labelToken uint16, propertyKey string) error {
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return err
	}
	if err := bs.checkOpen(); err != nil {
		return err
	}

	bs.idxMu.Lock()

	key := indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	if _, exists := bs.propertyIndexes[key]; !exists {
		bs.idxMu.Unlock()
		return ErrIndexNotFound
	}

	delete(bs.propertyIndexes, key)
	bs.persistPropertyIndexDefs()
	bs.idxMu.Unlock()
	return bs.flushIfSyncWrites()
}

// propIdxDef is the serialization format for property index definitions.
type propIdxDef struct {
	LabelToken  uint16 `msgpack:"l"`
	PropertyKey string `msgpack:"p"`
}

// vectorIdxDef is the serialization format for vector index definitions.
type vectorIdxDef struct {
	LabelToken  uint16         `msgpack:"l"`
	PropertyKey string         `msgpack:"p"`
	Dims        int            `msgpack:"d"`
	Metric      DistanceMetric `msgpack:"m"`
}

// hfIdxDef is the serialization format for high-frequency temporal index definitions.
type hfIdxDef struct {
	LabelToken       uint16 `msgpack:"l"`
	BucketSizeMillis int64  `msgpack:"b"`
}

const maxHighFrequencyBucketMillis = int64(1<<63-1) / int64(time.Millisecond)

func highFrequencyBucketDuration(bucketMillis int64) (time.Duration, error) {
	if bucketMillis <= 0 || bucketMillis > maxHighFrequencyBucketMillis {
		return 0, fmt.Errorf("%w: high-frequency bucket size must be a positive whole millisecond, got %dms",
			ErrInvalidTemporalIndexConfig, bucketMillis)
	}
	bucketSize := time.Duration(bucketMillis) * time.Millisecond
	if err := storecontract.ValidateHighFrequencyBucketSize(bucketSize); err != nil {
		return 0, err
	}
	return bucketSize, nil
}

// CreateTemporalIndex creates a temporal index on nodes with the given label token.
// Three-phase approach (same as CreatePropertyIndex) for safe concurrent operation.
// Returns ErrTemporalIndexExists if the index already exists.
func (bs *Store) CreateTemporalIndex(labelToken uint16) error {
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := bs.checkOpen(); err != nil {
		return err
	}

	// Phase 1: Install empty live index + snapshot IDs under write Lock.
	bs.idxMu.Lock()
	if _, exists := bs.temporalIndexes[labelToken]; exists {
		bs.idxMu.Unlock()
		return ErrTemporalIndexExists
	}
	if _, exists := bs.hfIndexes[labelToken]; exists {
		bs.idxMu.Unlock()
		return ErrTemporalIndexExists
	}
	liveTI := indexpkg.NewTemporalIndex()
	liveTI.Building = true
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
			deleteTemporalIndexIfCurrent(bs.temporalIndexes, labelToken, liveTI)
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
	if err := requireTemporalIndexCurrentForCreate(bs.temporalIndexes, labelToken, liveTI); err != nil {
		bs.idxMu.Unlock()
		return err
	}
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
	liveTI.Building = false
	bs.persistTemporalIndexDefs()
	bs.idxMu.Unlock()
	return bs.flushIfSyncWrites()
}

func deletePropertyIndexIfCurrent(idxs map[indexpkg.PropertyIndexKey]*indexpkg.PropertyIndex, key indexpkg.PropertyIndexKey, expected *indexpkg.PropertyIndex) {
	if idxs[key] == expected {
		delete(idxs, key)
	}
}

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

func deleteTemporalIndexIfCurrent(idxs map[uint16]*indexpkg.TemporalIndex, labelToken uint16, expected *indexpkg.TemporalIndex) {
	if idxs[labelToken] == expected {
		delete(idxs, labelToken)
	}
}

func requireTemporalIndexCurrentForCreate(idxs map[uint16]*indexpkg.TemporalIndex, labelToken uint16, expected *indexpkg.TemporalIndex) error {
	current := idxs[labelToken]
	if current == expected {
		return nil
	}
	if current == nil {
		return fmt.Errorf("graph: create temporal index: index dropped during creation: %w", ErrTemporalIndexNotFound)
	}
	return fmt.Errorf("graph: create temporal index: index replaced during creation: %w", ErrTemporalIndexExists)
}

// DropTemporalIndex removes a temporal index.
// Returns ErrTemporalIndexNotFound if the index does not exist.
func (bs *Store) DropTemporalIndex(labelToken uint16) error {
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := bs.checkOpen(); err != nil {
		return err
	}

	bs.idxMu.Lock()

	if _, exists := bs.temporalIndexes[labelToken]; !exists {
		bs.idxMu.Unlock()
		return ErrTemporalIndexNotFound
	}

	delete(bs.temporalIndexes, labelToken)
	bs.persistTemporalIndexDefs()
	bs.idxMu.Unlock()
	return bs.flushIfSyncWrites()
}

// CreateHighFrequencyIndex creates a time-bucketed high-frequency index on nodes
// with the given label token. Only one temporal index type can exist per label —
// returns ErrInvalidTemporalIndexConfig if bucketSize is not a positive whole
// millisecond and
// returns ErrTemporalIndexExists if a temporalIndex or highFrequencyIndex already
// exists for this label.
func (bs *Store) CreateHighFrequencyIndex(labelToken uint16, bucketSize time.Duration) error {
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateHighFrequencyBucketSize(bucketSize); err != nil {
		return err
	}
	if err := bs.checkOpen(); err != nil {
		return err
	}

	// Phase 1: install an empty live index so concurrent node mutations are
	// captured while existing rows are read outside idxMu.
	bs.idxMu.Lock()
	if _, exists := bs.temporalIndexes[labelToken]; exists {
		bs.idxMu.Unlock()
		return ErrTemporalIndexExists
	}
	if _, exists := bs.hfIndexes[labelToken]; exists {
		bs.idxMu.Unlock()
		return ErrTemporalIndexExists
	}
	liveHFI := indexpkg.NewHighFrequencyIndex(bucketSize, 0)
	liveHFI.Mutated = make(map[snowflake.ID]struct{})
	bs.hfIndexes[labelToken] = liveHFI
	var nids []types.NodeID
	if nodeIDs, ok := bs.labelIdx[labelToken]; ok {
		nids = make([]types.NodeID, 0, len(nodeIDs))
		for id := range nodeIDs {
			nids = append(nids, id)
		}
	}
	bs.idxMu.Unlock()

	// Phase 2: fetch existing rows outside the index lock. Corrupt rows must
	// fail creation, otherwise the new index silently excludes live data.
	type nodeEntry struct {
		id   snowflake.ID
		from types.Instant
	}
	backfill := make([]nodeEntry, 0, len(nids))
	for _, nid := range nids {
		n, err := bs.GetNode(nid)
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue // deleted between snapshot and fetch
			}
			bs.idxMu.Lock()
			deleteHighFrequencyIndexIfCurrent(bs.hfIndexes, labelToken, liveHFI)
			bs.idxMu.Unlock()
			return fmt.Errorf("graph: create high-frequency index: %w", err)
		}
		rawID := nid.SnowflakeID()
		from, _ := indexpkg.NodeTemporalBounds(rawID, n.Temporal())
		backfill = append(backfill, nodeEntry{id: rawID, from: from})
	}

	// Phase 3: merge backfill into the live index, skipping nodes already
	// handled by concurrent writes and nodes deleted during Phase 2.
	bs.idxMu.Lock()
	if err := requireHighFrequencyIndexCurrentForCreate(bs.hfIndexes, labelToken, liveHFI); err != nil {
		bs.idxMu.Unlock()
		return err
	}
	for _, entry := range backfill {
		if _, alive := bs.nodeIDs[types.NodeID(entry.id)]; !alive {
			continue
		}
		if liveHFI.WasMutated(entry.id) {
			continue
		}
		liveHFI.Add(types.NodeID(entry.id), entry.from)
	}
	liveHFI.ClearMutationTracking()
	bs.persistHighFrequencyIndexDefs()
	bs.idxMu.Unlock()
	return bs.flushIfSyncWrites()
}

// DropHighFrequencyIndex removes the high-frequency index for the given label token.
// Returns ErrTemporalIndexNotFound if no high-frequency index exists.
func (bs *Store) DropHighFrequencyIndex(labelToken uint16) error {
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := bs.checkOpen(); err != nil {
		return err
	}

	bs.idxMu.Lock()

	if _, exists := bs.hfIndexes[labelToken]; !exists {
		bs.idxMu.Unlock()
		return ErrTemporalIndexNotFound
	}
	delete(bs.hfIndexes, labelToken)
	bs.persistHighFrequencyIndexDefs()
	bs.idxMu.Unlock()
	return bs.flushIfSyncWrites()
}

func deleteHighFrequencyIndexIfCurrent(idxs map[uint16]*indexpkg.HighFrequencyIndex, labelToken uint16, expected *indexpkg.HighFrequencyIndex) {
	if idxs[labelToken] == expected {
		delete(idxs, labelToken)
	}
}

func requireHighFrequencyIndexCurrentForCreate(idxs map[uint16]*indexpkg.HighFrequencyIndex, labelToken uint16, expected *indexpkg.HighFrequencyIndex) error {
	current := idxs[labelToken]
	if current == expected {
		return nil
	}
	if current == nil {
		return fmt.Errorf("graph: create high-frequency index: index dropped during creation: %w", ErrTemporalIndexNotFound)
	}
	return fmt.Errorf("graph: create high-frequency index: index replaced during creation: %w", ErrTemporalIndexExists)
}

// TemporalIndexState reports which temporal index kind currently exists for
// the label token on this shard.
func (bs *Store) TemporalIndexState(labelToken uint16) (hasTemporal bool, hasHighFrequency bool, highFrequencyBucketSize time.Duration, err error) {
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return false, false, 0, err
	}
	if err := bs.checkOpen(); err != nil {
		return false, false, 0, err
	}
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	_, hasTemporal = bs.temporalIndexes[labelToken]
	hfi, ok := bs.hfIndexes[labelToken]
	if !ok {
		return hasTemporal, false, 0, nil
	}
	return hasTemporal, true, hfi.BucketSize(), nil
}

// persistTemporalIndexDefs serializes the current temporal index label tokens to Badger.
// Caller must hold bs.idxMu write lock.
func (bs *Store) persistTemporalIndexDefs() {
	tokens := make([]uint16, 0, len(bs.temporalIndexes))
	for tok, idx := range bs.temporalIndexes {
		if idx == nil || idx.Building {
			continue
		}
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

// persistHighFrequencyIndexDefs serializes the current HFI definitions to Badger.
// Caller must hold bs.idxMu write lock.
func (bs *Store) persistHighFrequencyIndexDefs() {
	defs := make([]hfIdxDef, 0, len(bs.hfIndexes))
	for tok, idx := range bs.hfIndexes {
		if idx == nil || idx.Mutated != nil {
			continue
		}
		defs = append(defs, hfIdxDef{
			LabelToken:       tok,
			BucketSizeMillis: idx.BucketSize().Milliseconds(),
		})
	}
	if len(defs) == 0 {
		bs.appendOps(writeOp{opType: writeOpDelete, key: storepkg.HighFrequencyIndexDefsKey})
		return
	}
	data, err := msgpack.Marshal(defs)
	if err != nil {
		slog.Error("graph: persist high-frequency index defs: marshal failed", "error", err)
		return // index still works in-memory; will retry on next change
	}
	bs.appendOps(writeOp{opType: writeOpSet, key: storepkg.HighFrequencyIndexDefsKey, value: data})
}

// CreateVectorIndex creates a vector similarity index for nodes with the given label token,
// on the given property key, expecting vectors of length dims.
// Scans existing nodes to populate the index. Returns ErrVectorIndexExists on duplicate.
// Definitions are persisted and entries are rebuilt from node properties on startup.
func (bs *Store) CreateVectorIndex(labelToken uint16, propertyKey string, dims int, metric DistanceMetric) error {
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return err
	}
	if err := indexpkg.ValidateVectorIndexConfig(dims, metric); err != nil {
		return err
	}
	if err := bs.checkOpen(); err != nil {
		return err
	}

	key := indexpkg.VectorIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}

	// Phase 1: Install empty placeholder under write lock for concurrent-write visibility.
	bs.idxMu.Lock()
	if _, exists := bs.vectorIndexes[key]; exists {
		bs.idxMu.Unlock()
		return ErrVectorIndexExists
	}
	vi := &indexpkg.VectorIndex{Dims: dims, Metric: metric, Mutated: make(map[snowflake.ID]struct{})}
	bs.vectorIndexes[key] = vi

	// Snapshot existing node IDs for population scan.
	nids := make([]types.NodeID, 0, len(bs.nodeIDs))
	for id := range bs.nodeIDs {
		nids = append(nids, id)
	}
	bs.idxMu.Unlock()

	type vectorBackfillEntry struct {
		id  snowflake.ID
		vec []float32
	}
	backfill := make([]vectorBackfillEntry, 0, len(nids))

	// Phase 2: Fetch node data outside the store lock.
	for _, nid := range nids {
		n, err := bs.GetNode(nid)
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue // node may have been deleted concurrently
			}
			bs.idxMu.Lock()
			indexpkg.DeleteVectorIndexIfCurrent(bs.vectorIndexes, key, vi)
			bs.idxMu.Unlock()
			return fmt.Errorf("graph: create vector index: %w", err)
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
		cp := make([]float32, len(vec))
		copy(cp, vec)
		backfill = append(backfill, vectorBackfillEntry{id: nid.SnowflakeID(), vec: cp})
	}

	// Phase 3: Merge backfill under the store lock. Concurrent writes that
	// happened during Phase 2 already updated the live placeholder, so do not
	// overwrite those newer vectors with stale backfill rows.
	bs.idxMu.Lock()
	if err := indexpkg.RequireVectorIndexCurrentForCreate(bs.vectorIndexes, key, vi); err != nil {
		bs.idxMu.Unlock()
		return err
	}
	for _, entry := range backfill {
		if _, alive := bs.nodeIDs[types.NodeID(entry.id)]; !alive {
			continue
		}
		if vi.WasMutated(entry.id) {
			continue
		}
		if err := vi.Add(entry.id, entry.vec); err != nil {
			indexpkg.DeleteVectorIndexIfCurrent(bs.vectorIndexes, key, vi)
			bs.idxMu.Unlock()
			return fmt.Errorf("graph: create vector index: node %d: %w", entry.id, err)
		}
	}
	vi.ClearMutationTracking()
	bs.persistVectorIndexDefs()
	bs.idxMu.Unlock()
	return bs.flushIfSyncWrites()
}

// DropVectorIndex removes a vector index.
// Returns ErrVectorIndexNotFound if the index does not exist.
func (bs *Store) DropVectorIndex(labelToken uint16, propertyKey string) error {
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return err
	}
	if err := bs.checkOpen(); err != nil {
		return err
	}

	bs.idxMu.Lock()

	key := indexpkg.VectorIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	if _, exists := bs.vectorIndexes[key]; !exists {
		bs.idxMu.Unlock()
		return ErrVectorIndexNotFound
	}
	delete(bs.vectorIndexes, key)
	bs.persistVectorIndexDefs()
	bs.idxMu.Unlock()
	return bs.flushIfSyncWrites()
}

// SearchNearestNodes returns the k nodes with vectors closest to query
// under the index defined for labelToken+propertyKey.
// Results are ordered by ascending distance (closest first).
// Returns ErrVectorIndexNotFound if no index exists.
// Returns ErrDimensionMismatch if query length differs from the index's dims.
// Returns nil, nil if the index exists but has no entries.
//
// After and Limit are applied to the distance-ordered result. Depth has no
// meaning for this single-tier backend, and temporal filtering is applied by
// the Graph layer via SearchNearestFiltered before this path is taken.
func (bs *Store) SearchNearestNodes(labelToken uint16, propertyKey string, query []float32, k int, opts QueryOpts) ([]*types.Node, error) {
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return nil, err
	}
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}

	bs.idxMu.RLock()
	key := indexpkg.VectorIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	vi, exists := bs.vectorIndexes[key]
	bs.idxMu.RUnlock()

	if !exists {
		return nil, ErrVectorIndexNotFound
	}
	if vi.IsBuilding() {
		return nil, ErrVectorIndexNotFound
	}

	filter, err := bs.vectorTemporalFilter(vi, opts)
	if err != nil {
		return nil, err
	}
	rawIDs, err := vi.SearchNearest(query, k, filter)
	if err != nil {
		return nil, err
	}
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if len(rawIDs) == 0 {
		return nil, nil
	}
	// Fetch nodes in distance order — do NOT sort by ID (would destroy distance ranking).
	hasTemporal := storepkg.HasTemporalFilter(opts)
	result := make([]*types.Node, 0, len(rawIDs))
	for _, id := range rawIDs {
		n, err := bs.GetNode(types.NodeID(id))
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue // node may have been deleted concurrently
			}
			return nil, fmt.Errorf("graph: resolve vector search candidate: %w", err)
		}
		if hasTemporal && !storepkg.MatchesTemporalFilter(id, n.Temporal(), opts) {
			continue
		}
		result = append(result, n)
	}
	if len(result) == 0 {
		return nil, nil
	}
	return storepkg.PaginateNodesInOrder(result, opts.After, opts.Limit), nil
}

func (bs *Store) vectorTemporalFilter(vi *indexpkg.VectorIndex, opts QueryOpts) (func(snowflake.ID) bool, error) {
	if !storepkg.HasTemporalFilter(opts) {
		return nil, nil
	}
	eligible := make(map[snowflake.ID]struct{})
	for _, id := range vi.IDs() {
		n, err := bs.GetNode(types.NodeID(id))
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue
			}
			return nil, err
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
// BEFORE the k-cut. The filter is invoked while the vector index is being
// scanned; it should avoid mutating or re-entering the same vector index.
//
// Returns raw snowflake.IDs in ascending distance order; the caller is
// responsible for resolving entities (current or historical version).
func (bs *Store) SearchNearestFiltered(labelToken uint16, propertyKey string, query []float32, k int, filter func(snowflake.ID) bool) ([]snowflake.ID, error) {
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return nil, err
	}
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}

	bs.idxMu.RLock()
	key := indexpkg.VectorIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	vi, exists := bs.vectorIndexes[key]
	bs.idxMu.RUnlock()

	if !exists {
		return nil, ErrVectorIndexNotFound
	}
	if vi.IsBuilding() {
		return nil, ErrVectorIndexNotFound
	}
	ids, err := vi.SearchNearest(query, k, filter)
	if err != nil {
		return nil, err
	}
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	return ids, nil
}

// persistPropertyIndexDefs serializes the current property index definitions to Badger.
// Caller must hold bs.idxMu write lock.
func (bs *Store) persistPropertyIndexDefs() {
	var defs []propIdxDef
	for key, idx := range bs.propertyIndexes {
		if idx == nil || idx.Mutated != nil {
			continue
		}
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

// persistVectorIndexDefs serializes the current vector index definitions to Badger.
// Caller must hold bs.idxMu write lock.
func (bs *Store) persistVectorIndexDefs() {
	var defs []vectorIdxDef
	for key, idx := range bs.vectorIndexes {
		if idx == nil || idx.Mutated != nil {
			continue
		}
		defs = append(defs, vectorIdxDef{
			LabelToken:  key.LabelToken,
			PropertyKey: key.PropertyKey,
			Dims:        idx.Dims,
			Metric:      idx.Metric,
		})
	}
	if len(defs) == 0 {
		bs.appendOps(writeOp{opType: writeOpDelete, key: storepkg.VectorIndexDefsKey})
		return
	}
	data, err := msgpack.Marshal(defs)
	if err != nil {
		slog.Error("graph: persist vector index defs: marshal failed", "error", err)
		return // index still works in-memory; will retry on next change
	}
	bs.appendOps(writeOp{opType: writeOpSet, key: storepkg.VectorIndexDefsKey, value: data})
}
