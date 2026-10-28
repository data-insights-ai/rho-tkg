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
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	"github.com/vmihailenco/msgpack/v5"
)

// CreatePropertyIndex creates a property index for the label token and property key.
// Writes during the build are captured (live index installed under the write
// lock; backfill prefetched without a lock, then merged skipping IDs already
// written). Returns ErrIndexExists if the index already exists.
func (bs *Store) CreatePropertyIndex(labelToken uint16, propertyKey string) error {
	return bs.CreatePropertyIndexWithOptions(labelToken, propertyKey, storecontract.PropertyIndexOptions{})
}

// CreatePropertyIndexWithOptions is CreatePropertyIndex with
// storecontract.PropertyIndexOptions, persisted with the definition. RangeCounts and
// Elements need the RAM index: with PropertyIndexOnDisk (or the label in
// PropertyIndexOnDiskLabels) they return ErrCapabilityNotSupported (the disk keyspace
// is shared by every label indexing the key).
func (bs *Store) CreatePropertyIndexWithOptions(labelToken uint16, propertyKey string, opts storecontract.PropertyIndexOptions) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return err
	}
	if err := storecontract.ValidatePropertyIndexOptions(opts); err != nil {
		return err
	}
	onDisk := bs.propIdxOnDiskWanted(labelToken)
	if onDisk && opts.RangeCounts {
		return fmt.Errorf("graph: create property index: range counts need the RAM index, not PropertyIndexOnDisk: %w", storecontract.ErrCapabilityNotSupported)
	}
	if onDisk && opts.Elements {
		return fmt.Errorf("graph: create property index: an elements index needs the RAM index, not PropertyIndexOnDisk: %w", storecontract.ErrCapabilityNotSupported)
	}
	if onDisk {
		if _, ok := bs.propKeyTokenFor(propertyKey); !ok {
			return fmt.Errorf("graph: create property index: property-key registry required for PropertyIndexOnDisk: %w", ErrInvalidStoreMutation)
		}
	}

	// Phase 1: Install empty live index + snapshot IDs under write Lock.
	// Write lock (not RLock) ensures the index is visible to concurrent mutations.
	bs.idxMu.Lock()
	key := indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	if _, exists := bs.propertyIndexes[key]; exists {
		bs.idxMu.Unlock()
		return ErrIndexExists
	}
	liveIdx := indexpkg.NewPropertyIndexWith(opts.RangeCounts, opts.Elements)
	if onDisk {
		liveIdx = indexpkg.NewPropertyIndexOnDisk()
	}
	liveIdx.Mutated = make(map[snowflake.ID]struct{})
	bs.propertyIndexes[key] = liveIdx
	nids, idErr := bs.labelNodeIDsSnapshotLocked(labelToken)
	if idErr != nil {
		delete(bs.propertyIndexes, key)
		bs.idxMu.Unlock()
		return idErr
	}
	bs.idxMu.Unlock()
	if propertyCreatePhase2Hook != nil {
		propertyCreatePhase2Hook()
	}

	// Phase 2: fetch node data outside any lock, without the public GetNode defensive
	// copy. Disk mode collects writeOps instead of populating the RAM Entries map.
	backfill := indexpkg.NewPostingsBuild(opts.Elements)
	var diskOps []writeOp
	for _, nid := range nids {
		n, err := bs.prefetchNode(nid)
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
		if onDisk {
			if valueKey, found := n.IndexablePropertyValueKey(propertyKey); found {
				if op, ok := bs.propertyIndexDiskOp(propertyKey, valueKey, nid.SnowflakeID(), writeOpSet); ok {
					diskOps = append(diskOps, op)
				}
			}
			continue
		}
		backfill.AddEntityKeys(nid.SnowflakeID(), propertyKey, n.ForEachPropertyIndexKey)
	}

	// Phase 3: merge backfill under the write lock, skipping IDs written or deleted during Phase 2.
	bs.idxMu.Lock()
	if err := requirePropertyIndexCurrentForCreate(bs.propertyIndexes, key, liveIdx); err != nil {
		bs.idxMu.Unlock()
		return err
	}
	if onDisk {
		var ops []writeOp
		for _, op := range diskOps {
			id := storepkg.PropertyIndexNodeIDFromKey(op.key)
			if _, mutated := liveIdx.Mutated[id]; mutated {
				continue // concurrent write handled this ID during Phase 2
			}
			if !bs.nodeIDs.Has(int64(types.NodeID(id))) {
				continue // node deleted during Phase 2
			}
			ops = append(ops, op)
		}
		liveIdx.Mutated = nil // stop tracking — index creation complete
		bs.persistPropertyIndexDefs()
		if bs.propIdxOnDisk {
			bs.appendOps(ops...)
		} else {
			// Its rows are built: its marker, so the next open trusts them
			// (planPropertyIndexDisk).
			ops = append(ops, writeOp{opType: writeOpSet, key: storepkg.PropertyIndexOnDiskDefBuiltKey(labelToken, propertyKey), value: []byte{1}})
			bs.appendOpsRaising(storepkg.PropDiskLabelsStoreFormatVersion, ops...)
		}
		bs.idxMu.Unlock()
		return bs.flushIfNeeded()
	}
	_ = liveIdx.FinishBuild(backfill, func(id snowflake.ID) (bool, error) {
		alive := bs.nodeIDs.Has(int64(types.NodeID(id))) // a node deleted during Phase 2 is skipped
		return alive, nil
	})
	bs.persistPropertyIndexDefs()
	bs.idxMu.Unlock()
	return bs.flushIfNeeded()
}

// DropPropertyIndex removes a property index.
// Returns ErrIndexNotFound if the index does not exist.
func (bs *Store) DropPropertyIndex(labelToken uint16, propertyKey string) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return err
	}

	bs.idxMu.Lock()

	key := indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	if _, exists := bs.propertyIndexes[key]; !exists {
		bs.idxMu.Unlock()
		return ErrIndexNotFound
	}
	dropped := bs.propertyIndexes[key]
	delete(bs.propertyIndexes, key)
	if _, ok := bs.nodePropTx[key]; ok {
		delete(bs.nodePropTx, key) // its membership sidecar goes with it (backlog 8)
		bs.propTxGen++
	}

	// Disk mode: rows are shared by every on-disk definition indexing the same
	// PropertyKey (see badgerstore_property_disk.go), so purge only when no other
	// one uses it. Its built marker goes (planPropertyIndexDisk).
	var purgeOps []writeOp
	var purgeErr error
	if dropped.OnDisk() {
		purgeOps = append(purgeOps, writeOp{opType: writeOpDelete, key: storepkg.PropertyIndexOnDiskDefBuiltKey(labelToken, propertyKey)})
		stillReferenced := false
		for k, idx := range bs.propertyIndexes {
			if k.PropertyKey == propertyKey && idx.OnDisk() {
				stillReferenced = true
				break
			}
		}
		if !stillReferenced {
			var rows []writeOp
			rows, purgeErr = bs.purgePropertyKeyDiskEntriesLocked(propertyKey)
			purgeOps = append(purgeOps, rows...)
		}
	}
	bs.persistPropertyIndexDefs()
	if len(purgeOps) > 0 {
		bs.appendOps(purgeOps...)
	}
	bs.idxMu.Unlock()
	if purgeErr != nil {
		return purgeErr
	}
	return bs.flushIfNeeded()
}

// propIdxDef is the serialized property index definition. RangeCounts and Elements
// (backlog 63) are written only when set, so plain definitions stay byte-identical; an
// older binary skips them and loads the index plain.
type propIdxDef struct {
	LabelToken  uint16 `msgpack:"l"`
	PropertyKey string `msgpack:"p"`
	RangeCounts bool   `msgpack:"rc,omitempty"`
	Elements    bool   `msgpack:"el,omitempty"`
}

// vectorIdxDef is the serialized vector index definition. The tuning fields are
// additive: absent decodes to zero = default HNSW.
type vectorIdxDef struct {
	LabelToken     uint16         `msgpack:"l"`
	PropertyKey    string         `msgpack:"p"`
	Dims           int            `msgpack:"d"`
	Metric         DistanceMetric `msgpack:"m"`
	UseBruteForce  bool           `msgpack:"bf,omitempty"`
	M              int            `msgpack:"hm,omitempty"`
	EfConstruction int            `msgpack:"efc,omitempty"`
	EfSearch       int            `msgpack:"efs,omitempty"`
}

func (d vectorIdxDef) vectorIndexOptions() storecontract.VectorIndexOptions {
	return storecontract.VectorIndexOptions{
		UseBruteForce:  d.UseBruteForce,
		M:              d.M,
		EfConstruction: d.EfConstruction,
		EfSearch:       d.EfSearch,
	}
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

// temporalCreateBeforeFoldHook, set only by tests, runs in CreateTemporalIndex
// after the current-row backfill is merged and before the history fold.
var temporalCreateBeforeFoldHook func()

// propertyCreatePhase2Hook, set only by tests, runs in CreatePropertyIndex
// after the live index is installed and before its backfill is read: a write
// there is a concurrent write the build must not lose.
var propertyCreatePhase2Hook func()

// temporalCreatePhase2Hook, set only by tests, runs in CreateTemporalIndex
// after the Phase 2 fetch, before the Phase 3 merge takes idxMu.
var temporalCreatePhase2Hook func()

// CreateTemporalIndex creates a temporal index on nodes with the label token
// (three-phase, like CreatePropertyIndex). Returns ErrTemporalIndexExists if present.
func (bs *Store) CreateTemporalIndex(labelToken uint16) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	// Build the history presence set before any lock: every later create of
	// an uncovered ID asks it under idxMu (foldRecreatedNodeHistoryLocked).
	if err := bs.ensureHistoryPresenceBuilt(&bs.histNodePresence, storepkg.KeyHistNode); err != nil {
		return fmt.Errorf("graph: create temporal index: %w", err)
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
	liveTI.Mutated = make(map[snowflake.ID]struct{})
	if bs.temporalIdxOnDisk {
		liveTI.DiskWritten = make(map[snowflake.ID]struct{})
	}
	bs.temporalIndexes[labelToken] = liveTI
	nids, idErr := bs.labelNodeIDsSnapshotLocked(labelToken)
	if idErr != nil {
		delete(bs.temporalIndexes, labelToken)
		bs.idxMu.Unlock()
		return idErr
	}
	bs.idxMu.Unlock()

	// Phase 2: Fetch node data OUTSIDE any lock without the defensive copy
	// required by the public GetNode boundary.
	type nodeEntry struct {
		id   snowflake.ID
		from types.Instant
		to   types.Instant
	}
	backfill := make([]nodeEntry, 0, len(nids))
	for _, nid := range nids {
		n, err := bs.prefetchNode(nid)
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue // deleted between snapshot and fetch
			}
			// Fatal error — remove the incomplete index.
			bs.idxMu.Lock()
			bs.abortTemporalCreateLocked(labelToken, liveTI)
			bs.idxMu.Unlock()
			return fmt.Errorf("graph: create temporal index: %w", err)
		}
		rawID := nid.SnowflakeID()
		from, to := indexpkg.NodeTemporalBounds(bs.validAxis.Get(), rawID, n.Temporal())
		backfill = append(backfill, nodeEntry{id: rawID, from: from, to: to})
	}
	if temporalCreatePhase2Hook != nil {
		temporalCreatePhase2Hook()
	}

	// Phase 3: merge backfill under the write lock. The index stays Building (readers
	// skip it) until the history fold below is in; current rows alone would let the
	// envelope prune a node by a past row (backlog 56). Disk row is written unless a
	// write since Phase 1 added/removed it (DiskWritten). An ID written since Phase 1
	// gets its fetched row by Extend (a union); a write that left disk untouched
	// (PutNodeVersion) left the fetched current row current.
	bs.idxMu.Lock()
	if err := requireTemporalIndexCurrentForCreate(bs.temporalIndexes, labelToken, liveTI); err != nil {
		bs.idxMu.Unlock()
		return err
	}
	var diskOps []writeOp
	for _, entry := range backfill {
		if !bs.nodeIDs.Has(int64(types.NodeID(entry.id))) {
			continue // node deleted during Phase 2
		}
		if liveTI.WasMutated(entry.id) {
			liveTI.Extend(entry.id, entry.from, entry.to)
		} else {
			liveTI.AddKnownAbsent(entry.id, entry.from, entry.to)
		}
		if _, written := liveTI.DiskWritten[entry.id]; bs.temporalIdxOnDisk && !written {
			diskOps = append(diskOps, writeOp{
				opType: writeOpSet,
				key:    storepkg.TemporalIndexEntryKey(labelToken, entry.from, entry.id),
				value:  storepkg.TemporalIndexEntryValue(entry.to),
			})
		}
	}
	if len(diskOps) > 0 {
		bs.appendOps(diskOps...)
	}
	liveTI.DiskWritten = nil
	bs.idxMu.Unlock()
	if temporalCreateBeforeFoldHook != nil {
		temporalCreateBeforeFoldHook()
	}

	// Phase 4: read history, without the lock, of the Phase 1 members (a superset of
	// Phase 3's IDs): a fresh snapshot would miss a member that left the label or was
	// deleted since (backlog 56).
	foldIDs, folds, err := bs.nodeHistoryFolds(labelToken, nids)
	if err != nil {
		bs.idxMu.Lock()
		bs.abortTemporalCreateLocked(labelToken, liveTI)
		bs.idxMu.Unlock()
		return err
	}

	// Phase 5: fold and publish. IDs written since Phase 1 that Phase 4 did not read
	// are folded here under the lock, so no write lands between read and publish.
	bs.idxMu.Lock()
	if err := requireTemporalIndexCurrentForCreate(bs.temporalIndexes, labelToken, liveTI); err != nil {
		bs.idxMu.Unlock()
		return err
	}
	late := liveTI.Mutated
	liveTI.ClearMutationTracking()
	for _, f := range folds {
		liveTI.Extend(f.id, f.from, f.to)
	}
	for id := range late {
		if _, read := foldIDs[id]; read {
			continue
		}
		if err := bs.foldNodeHistoryLocked(liveTI, id); err != nil {
			bs.abortTemporalCreateLocked(labelToken, liveTI)
			bs.idxMu.Unlock()
			return fmt.Errorf("graph: create temporal index: %w", err)
		}
	}
	liveTI.Building = false
	bs.persistTemporalIndexDefs() // skips a Building index
	bs.idxMu.Unlock()
	return bs.flushIfNeeded()
}

// temporalFoldEntry is one history row's valid-time bounds.
type temporalFoldEntry struct {
	id       snowflake.ID
	from, to types.Instant
}

// nodeHistoryFolds reads history of ids without idxMu and returns the IDs read
// and every history row's bounds; IDs with no node and no history are skipped.
// GetNodeHistory never re-enters idxMu; it reads a deleted node's history too.
func (bs *Store) nodeHistoryFolds(tok uint16, ids []types.NodeID) (map[snowflake.ID]struct{}, []temporalFoldEntry, error) {
	read := make(map[snowflake.ID]struct{}, len(ids))
	var folds []temporalFoldEntry
	for _, id := range ids {
		read[id.SnowflakeID()] = struct{}{}
		hist, err := bs.GetNodeHistory(id)
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue
			}
			return nil, nil, fmt.Errorf("graph: temporal-index history envelope fold (label %d): %w", tok, err)
		}
		for _, hv := range hist {
			if hv == nil {
				continue
			}
			f, t := indexpkg.NodeTemporalBounds(bs.validAxis.Get(), id.SnowflakeID(), hv.Temporal())
			folds = append(folds, temporalFoldEntry{id.SnowflakeID(), f, t})
		}
	}
	return read, folds, nil
}

// abortTemporalCreateLocked removes a create's unpublished index and, under
// TemporalIndexOnDisk, its 0x0B rows: with no definition a later create of the
// label would read them again after a crash reopen. A failed purge is left to the
// open-time purge. A replaced index owns the prefix and is left alone. Caller holds idxMu.Lock.
func (bs *Store) abortTemporalCreateLocked(labelToken uint16, ti *indexpkg.TemporalIndex) {
	if bs.temporalIndexes[labelToken] != ti {
		return
	}
	delete(bs.temporalIndexes, labelToken)
	ops, err := bs.purgeTemporalIndexDiskEntriesLocked(labelToken)
	if err != nil {
		slog.Warn("graph: create temporal index: purge disk entries", "label", labelToken, "error", err)
		return
	}
	if len(ops) > 0 {
		bs.appendOps(ops...)
	}
}

// foldTemporalHistoryEnvelopes folds every history version's valid-time bounds
// into the per-node envelope of each temporal index in tokens (sound superset);
// the current-version rebuild covers only the latest row. Tolerant of concurrent deletes.
func (bs *Store) foldTemporalHistoryEnvelopes(tokens []uint16) error {
	for _, tok := range tokens {
		bs.idxMu.RLock()
		ti, ok := bs.temporalIndexes[tok]
		var ids []types.NodeID
		if ok {
			// Via the snapshot helper, not labelIdx: under LabelIndexOnDisk the map is empty (backlog 54).
			var err error
			if ids, err = bs.labelNodeIDsSnapshotLocked(tok); err != nil {
				bs.idxMu.RUnlock()
				return fmt.Errorf("graph: temporal-index history envelope fold (label %d): %w", tok, err)
			}
		}
		bs.idxMu.RUnlock()
		if !ok || len(ids) == 0 {
			continue
		}
		_, folds, err := bs.nodeHistoryFolds(tok, ids)
		if err != nil {
			return err
		}
		if len(folds) == 0 {
			continue
		}
		bs.idxMu.Lock()
		if cur, still := bs.temporalIndexes[tok]; still && cur == ti {
			for _, f := range folds {
				ti.Extend(f.id, f.from, f.to)
			}
		}
		bs.idxMu.Unlock()
	}
	return nil
}

// foldTemporalHistoryEnvelopesOf folds the history of ids (members of label
// tok) into tok's temporal index, as foldTemporalHistoryEnvelopes does for
// every member (a delta open: badgerstore_open_image_delta.go).
func (bs *Store) foldTemporalHistoryEnvelopesOf(tok uint16, ids []types.NodeID) error {
	bs.idxMu.RLock()
	ti, ok := bs.temporalIndexes[tok]
	bs.idxMu.RUnlock()
	if !ok || len(ids) == 0 {
		return nil
	}
	_, folds, err := bs.nodeHistoryFolds(tok, ids)
	if err != nil {
		return err
	}
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()
	if cur, still := bs.temporalIndexes[tok]; still && cur == ti {
		for _, f := range folds {
			ti.Extend(f.id, f.from, f.to)
		}
	}
	return nil
}

// foldNodeHistoryLocked extends ti by every history row of id; the RAM presence
// set answers "has none" without a read. Caller holds idxMu.Lock (order idxMu ->
// presence buildMu -> wbMu); the history read takes only wbMu and a read transaction.
func (bs *Store) foldNodeHistoryLocked(ti *indexpkg.TemporalIndex, id snowflake.ID) error {
	has, err := bs.hasHistory(&bs.histNodePresence, storepkg.KeyHistNode, id, bs.probeHistoryPresence)
	if err != nil || !has {
		return err
	}
	hist, err := bs.getNodeHistoryByPrefix(storepkg.HistNodePrefix(id))
	if err != nil {
		return err
	}
	for _, hv := range hist {
		if hv != nil {
			from, to := indexpkg.NodeTemporalBounds(bs.validAxis.Get(), id, hv.Temporal())
			ti.Extend(id, from, to)
		}
	}
	return nil
}

// nodeHasTemporalIndexLocked reports whether one of n's labels has a node
// temporal index. Caller holds idxMu (R or W).
func (bs *Store) nodeHasTemporalIndexLocked(n *types.Node) bool {
	if len(bs.temporalIndexes) == 0 {
		return false
	}
	for i := 0; i < n.LabelTokenCount(); i++ {
		if _, ok := bs.temporalIndexes[n.LabelTokenRawAt(i)]; ok {
			return true
		}
	}
	return false
}

// extendNodeVersionTemporalLocked folds a PutNodeVersion row into each label's
// index that covers id or is still Building. An uncovered ID stays uncovered
// (never pruned): extending would vouch for this one row only. Caller holds idxMu.Lock.
func (bs *Store) extendNodeVersionTemporalLocked(n *types.Node, id snowflake.ID) {
	from, to := indexpkg.NodeTemporalBounds(bs.validAxis.Get(), id, n.Temporal())
	for i := 0; i < n.LabelTokenCount(); i++ {
		ti := bs.temporalIndexes[n.LabelTokenRawAt(i)]
		if ti == nil {
			continue
		}
		if _, _, covered := ti.EnvelopeOf(id); covered || ti.Building {
			ti.Extend(id, from, to)
		}
	}
}

// foldRecreatedNodeHistoryLocked runs before a node create / label-add door adds
// n (backlog 56): for each label whose published temporal index does not cover
// id, a deleted node's history may still be on disk and the new row alone would
// make the envelope vouch for less than the ID holds, so it is folded first (RAM
// envelope only). A Building index needs nothing. Caller holds idxMu.Lock (order
// idxMu -> presence buildMu -> wbMu); the history read takes only wbMu and a read transaction.
func (bs *Store) foldRecreatedNodeHistoryLocked(n *types.Node, id snowflake.ID) error {
	var hist []*types.Node
	read := false
	for i := 0; i < n.LabelTokenCount(); i++ {
		ti := bs.temporalIndexes[n.LabelTokenRawAt(i)]
		if ti == nil || ti.Building {
			continue
		}
		if _, _, covered := ti.EnvelopeOf(id); covered {
			continue
		}
		if !read {
			read = true
			has, err := bs.hasHistory(&bs.histNodePresence, storepkg.KeyHistNode, id, bs.probeHistoryPresence)
			if err != nil {
				return fmt.Errorf("graph: temporal index history fold: %w", err)
			}
			if !has {
				return nil
			}
			if hist, err = bs.getNodeHistoryByPrefix(storepkg.HistNodePrefix(id)); err != nil {
				return fmt.Errorf("graph: temporal index history fold: %w", err)
			}
		}
		for _, hv := range hist {
			if hv != nil {
				from, to := indexpkg.NodeTemporalBounds(bs.validAxis.Get(), id, hv.Temporal())
				ti.Extend(id, from, to)
			}
		}
	}
	return nil
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
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}

	bs.idxMu.Lock()

	if _, exists := bs.temporalIndexes[labelToken]; !exists {
		bs.idxMu.Unlock()
		return ErrTemporalIndexNotFound
	}

	delete(bs.temporalIndexes, labelToken)
	// A TemporalIndex is keyed exclusively by labelToken, so purging the whole prefix is safe.
	purgeOps, purgeErr := bs.purgeTemporalIndexDiskEntriesLocked(labelToken)
	bs.persistTemporalIndexDefs()
	if len(purgeOps) > 0 {
		bs.appendOps(purgeOps...)
	}
	bs.idxMu.Unlock()
	if purgeErr != nil {
		return purgeErr
	}
	return bs.flushIfNeeded()
}

// CreateHighFrequencyIndex creates a time-bucketed index on nodes with the label
// token. One temporal index type per label: ErrInvalidTemporalIndexConfig if
// bucketSize is not a positive whole millisecond; ErrTemporalIndexExists if a
// temporalIndex or highFrequencyIndex exists for the label.
func (bs *Store) CreateHighFrequencyIndex(labelToken uint16, bucketSize time.Duration) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateHighFrequencyBucketSize(bucketSize); err != nil {
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
	nids, idErr := bs.labelNodeIDsSnapshotLocked(labelToken)
	if idErr != nil {
		delete(bs.hfIndexes, labelToken)
		bs.idxMu.Unlock()
		return idErr
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
		n, err := bs.prefetchNode(nid)
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
		from, _ := indexpkg.NodeTemporalBounds(bs.validAxis.Get(), rawID, n.Temporal())
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
		if !bs.nodeIDs.Has(int64(types.NodeID(entry.id))) {
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
	return bs.flushIfNeeded()
}

// DropHighFrequencyIndex removes the high-frequency index for the given label token.
// Returns ErrTemporalIndexNotFound if no high-frequency index exists.
func (bs *Store) DropHighFrequencyIndex(labelToken uint16) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
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
	return bs.flushIfNeeded()
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
	if err := bs.checkOpen(); err != nil {
		return false, false, 0, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
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

// CreateVectorIndex creates a vector index for the label token and property key
// for vectors of length dims (approximate HNSW by default; see
// CreateVectorIndexWithOptions). Scans existing nodes; definitions persist and
// entries rebuild on startup. Returns ErrVectorIndexExists on duplicate.
func (bs *Store) CreateVectorIndex(labelToken uint16, propertyKey string, dims int, metric DistanceMetric) error {
	return bs.CreateVectorIndexWithOptions(labelToken, propertyKey, dims, metric, storecontract.VectorIndexOptions{})
}

// CreateVectorIndexWithOptions is CreateVectorIndex with control over the engine
// (opts.UseBruteForce) and HNSW tuning; zero opts equals CreateVectorIndex. The
// choice persists (see vectorIdxDef) so a restart rebuilds the same engine.
func (bs *Store) CreateVectorIndexWithOptions(labelToken uint16, propertyKey string, dims int, metric DistanceMetric, opts storecontract.VectorIndexOptions) error {
	if err := bs.checkWritable(); err != nil {
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

	// Phase 1: Install empty placeholder under write lock for concurrent-write visibility.
	bs.idxMu.Lock()
	if _, exists := bs.vectorIndexes[key]; exists {
		bs.idxMu.Unlock()
		return ErrVectorIndexExists
	}
	vi := &indexpkg.VectorIndex{Dims: dims, Metric: metric, Mutated: make(map[snowflake.ID]struct{})}
	indexpkg.ApplyVectorIndexOptions(vi, opts)
	bs.vectorIndexes[key] = vi

	// Snapshot only nodes carrying this label for population scan. Scanning
	// all nodeIDs would force full-row reads for unrelated labels.
	nids, idErr := bs.labelNodeIDsSnapshotLocked(labelToken)
	if idErr != nil {
		delete(bs.vectorIndexes, key)
		bs.idxMu.Unlock()
		return idErr
	}
	bs.idxMu.Unlock()

	type vectorBackfillEntry struct {
		id  snowflake.ID
		vec []float32
	}
	backfill := make([]vectorBackfillEntry, 0, len(nids))

	// Phase 2: Fetch node data outside the store lock without defensive copies.
	for _, nid := range nids {
		n, err := bs.prefetchNode(nid)
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue // node may have been deleted concurrently
			}
			bs.idxMu.Lock()
			indexpkg.DeleteVectorIndexIfCurrent(bs.vectorIndexes, key, vi)
			bs.idxMu.Unlock()
			return fmt.Errorf("graph: create vector index: %w", err)
		}
		vec, ok := n.Float32SlicePropertyCopy(propertyKey)
		if !ok {
			continue
		}
		backfill = append(backfill, vectorBackfillEntry{id: nid.SnowflakeID(), vec: vec})
	}

	// Phase 3: merge backfill under the store lock without overwriting newer vectors from concurrent writes.
	bs.idxMu.Lock()
	if err := indexpkg.RequireVectorIndexCurrentForCreate(bs.vectorIndexes, key, vi); err != nil {
		bs.idxMu.Unlock()
		return err
	}
	for _, entry := range backfill {
		if !bs.nodeIDs.Has(int64(types.NodeID(entry.id))) {
			continue
		}
		if vi.WasMutated(entry.id) {
			continue
		}
		if err := vi.AddOwned(entry.id, entry.vec); err != nil {
			indexpkg.DeleteVectorIndexIfCurrent(bs.vectorIndexes, key, vi)
			bs.idxMu.Unlock()
			return fmt.Errorf("graph: create vector index: node %d: %w", entry.id, err)
		}
	}
	vi.ClearMutationTracking()
	bs.persistVectorIndexDefs()
	bs.idxMu.Unlock()
	return bs.flushIfNeeded()
}

// DropVectorIndex removes a vector index.
// Returns ErrVectorIndexNotFound if the index does not exist.
func (bs *Store) DropVectorIndex(labelToken uint16, propertyKey string) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
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
	return bs.flushIfNeeded()
}

// SearchNearestNodes returns the k nodes closest to query under the index for
// labelToken+propertyKey, ascending by distance. Errors: ErrVectorIndexNotFound,
// ErrDimensionMismatch (query length), ErrInvalidVectorValue (NaN/Inf). Returns
// nil, nil for an empty index. After and Limit apply to the distance order; Depth
// is meaningless here. Temporal filtering precedes heap selection so ineligible
// candidates cannot occupy top-k slots.
func (bs *Store) SearchNearestNodes(labelToken uint16, propertyKey string, query []float32, k int, opts QueryOpts) ([]*types.Node, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
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
	if k <= 0 {
		if _, err := vi.SearchNearest(query, k, nil); err != nil {
			return nil, err
		}
		return nil, nil
	}

	dims := vi.Dims
	filter, filterErr := bs.vectorTemporalFilter(key, dims, opts)
	rawIDs, err := vi.SearchNearest(query, k, filter)
	if err != nil {
		return nil, err
	}
	if err := filterErr(); err != nil {
		return nil, err
	}
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if len(rawIDs) == 0 {
		return nil, nil
	}
	// Fetch nodes in distance order — do NOT sort by ID (would destroy distance ranking).
	hasTemporal := opts.HasValidTimeFilter()
	result := make([]*types.Node, 0, len(rawIDs))
	for _, id := range rawIDs {
		n, err := bs.prefetchNode(types.NodeID(id))
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue // node may have been deleted concurrently
			}
			return nil, fmt.Errorf("graph: resolve vector search candidate: %w", err)
		}
		if !indexpkg.NodeMatchesVectorIndex(n, key, dims) {
			continue
		}
		if hasTemporal && !storepkg.MatchesTemporalFilter(bs.validAxis.Get(), id, n.Temporal(), opts) {
			continue
		}
		result = append(result, n)
	}
	if len(result) == 0 {
		return nil, nil
	}
	return storepkg.PaginateNodesInOrder(result, opts.After, opts.Limit), nil
}

func (bs *Store) vectorTemporalFilter(key indexpkg.VectorIndexKey, dims int, opts QueryOpts) (func(snowflake.ID) bool, func() error) {
	if !opts.HasValidTimeFilter() {
		return nil, func() error { return nil }
	}
	var filterErr error
	return func(id snowflake.ID) bool {
		n, err := bs.prefetchNode(types.NodeID(id))
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				return false
			}
			if filterErr == nil {
				filterErr = err
			}
			return false
		}
		if !indexpkg.NodeMatchesVectorIndex(n, key, dims) {
			return false
		}
		return storepkg.MatchesTemporalFilter(bs.validAxis.Get(), id, n.Temporal(), opts)
	}, func() error { return filterErr }
}

// SearchNearestFiltered is the package-internal entry point for the Graph layer:
// the eligibility filter (combined with current-row shape validation) applies
// before the k-cut. Returns raw snowflake.IDs by ascending distance; the caller
// resolves entities.
func (bs *Store) SearchNearestFiltered(labelToken uint16, propertyKey string, query []float32, k int, filter func(snowflake.ID) bool) ([]snowflake.ID, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
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
	dims := vi.Dims
	var filterErr error
	shapeFilter := func(id snowflake.ID) bool {
		n, err := bs.prefetchNode(types.NodeID(id))
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				return false
			}
			if filterErr == nil {
				filterErr = err
			}
			return false
		}
		if !indexpkg.NodeMatchesVectorIndex(n, key, dims) {
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
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	return ids, nil
}

// persistPropertyIndexDefs serializes the current property index definitions to Badger.
// Caller must hold bs.idxMu write lock.
func (bs *Store) persistPropertyIndexDefs() {
	var defs []propIdxDef
	var marker uint16 // ElementsStoreFormatVersion when an Elements definition is persisted
	for key, idx := range bs.propertyIndexes {
		if idx == nil || idx.Mutated != nil {
			continue
		}
		defs = append(defs, propIdxDef{LabelToken: key.LabelToken, PropertyKey: key.PropertyKey, RangeCounts: idx.RangeCounts(), Elements: idx.Elements()})
		if idx.Elements() {
			marker = storepkg.ElementsStoreFormatVersion
		}
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
	bs.appendOpsRaising(marker, writeOp{opType: writeOpSet, key: storepkg.PropIndexDefsKey, value: data})
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
			LabelToken:     key.LabelToken,
			PropertyKey:    key.PropertyKey,
			Dims:           idx.Dims,
			Metric:         idx.Metric,
			UseBruteForce:  idx.BruteForce,
			M:              idx.HNSWM,
			EfConstruction: idx.HNSWEfConstruction,
			EfSearch:       idx.HNSWEfSearch,
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
