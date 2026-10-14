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

// CreatePropertyIndex creates a property index for the given label token and property key.
// Three-phase approach to prevent blocking concurrent reads/writes during slow I/O:
//
//	Phase 1 (write Lock): Install an empty live index so concurrent PutNode/ReplaceNode
//	writes are captured immediately. Snapshot existing node IDs.
//	Phase 2 (no lock): Prefetch node data to build a backfill set.
//	Phase 3 (write Lock): Merge backfill entries into the live index, skipping IDs
//	that were already handled by concurrent writes during Phase 2.
//
// Returns ErrIndexExists if the index already exists.
func (bs *Store) CreatePropertyIndex(labelToken uint16, propertyKey string) error {
	return bs.CreatePropertyIndexWithOptions(labelToken, propertyKey, storecontract.PropertyIndexOptions{})
}

// CreatePropertyIndexWithOptions is CreatePropertyIndex with
// storecontract.PropertyIndexOptions (round 4 R2). The options persist with
// the definition. RangeCounts needs the RAM index: with PropertyIndexOnDisk it
// is refused (ErrCapabilityNotSupported).
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
	if bs.propIdxOnDisk && opts.RangeCounts {
		return fmt.Errorf("graph: create property index: range counts need the RAM index, not PropertyIndexOnDisk: %w", storecontract.ErrCapabilityNotSupported)
	}
	if bs.propIdxOnDisk {
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
	liveIdx := indexpkg.NewPropertyIndexWith(opts.RangeCounts)
	liveIdx.Mutated = make(map[snowflake.ID]struct{})
	bs.propertyIndexes[key] = liveIdx
	nids, idErr := bs.labelNodeIDsSnapshotLocked(labelToken)
	if idErr != nil {
		delete(bs.propertyIndexes, key)
		bs.idxMu.Unlock()
		return idErr
	}
	bs.idxMu.Unlock()

	// Phase 2: Fetch node data OUTSIDE any lock. Builds a backfill index for
	// nodes that existed before Phase 1 without the defensive copy required by
	// the public GetNode boundary. Disk mode collects writeOps instead of
	// populating a RAM PropertyIndex's Entries map.
	backfill := indexpkg.NewPropertyIndex()
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
		valueKey, found := n.IndexablePropertyValueKey(propertyKey)
		if !found {
			continue
		}
		if bs.propIdxOnDisk {
			if op, ok := bs.propertyIndexDiskOp(propertyKey, valueKey, nid.SnowflakeID(), writeOpSet); ok {
				diskOps = append(diskOps, op)
			}
			continue
		}
		backfill.AddKey(nid.SnowflakeID(), valueKey)
	}

	// Phase 3: Merge backfill into live index under write Lock.
	// Skip entries for IDs already handled by concurrent writes during Phase 2,
	// and entries for nodes deleted during Phase 2.
	bs.idxMu.Lock()
	if err := requirePropertyIndexCurrentForCreate(bs.propertyIndexes, key, liveIdx); err != nil {
		bs.idxMu.Unlock()
		return err
	}
	if bs.propIdxOnDisk {
		var ops []writeOp
		for _, op := range diskOps {
			id := storepkg.PropertyIndexNodeIDFromKey(op.key)
			if _, mutated := liveIdx.Mutated[id]; mutated {
				continue // concurrent write handled this ID during Phase 2
			}
			if _, alive := bs.nodeIDs[types.NodeID(id)]; !alive {
				continue // node deleted during Phase 2
			}
			ops = append(ops, op)
		}
		liveIdx.Mutated = nil // stop tracking — index creation complete
		bs.persistPropertyIndexDefs()
		bs.appendOps(ops...)
		bs.idxMu.Unlock()
		return bs.flushIfNeeded()
	}
	for vk, idSet := range backfill.Entries {
		for id := range idSet {
			if _, mutated := liveIdx.Mutated[id]; mutated {
				continue // concurrent write handled this ID during Phase 2
			}
			if _, alive := bs.nodeIDs[types.NodeID(id)]; !alive {
				continue // node deleted during Phase 2
			}
			// AddKey, not a direct Entries write — the index's ordered
			// numeric view is maintained inside AddKey.
			liveIdx.AddKey(id, vk)
		}
	}
	liveIdx.Mutated = nil // stop tracking — index creation complete
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
	delete(bs.propertyIndexes, key)
	if _, ok := bs.nodePropTx[key]; ok {
		delete(bs.nodePropTx, key) // its membership sidecar goes with it (backlog 8)
		bs.propTxGen++
	}

	// Disk mode: on-disk rows are shared across every definition indexing the
	// SAME PropertyKey (regardless of label — see badgerstore_property_disk.go's
	// file doc comment), so only physically purge them once no OTHER active
	// definition still references propertyKey.
	var purgeOps []writeOp
	var purgeErr error
	if bs.propIdxOnDisk {
		stillReferenced := false
		for k := range bs.propertyIndexes {
			if k.PropertyKey == propertyKey {
				stillReferenced = true
				break
			}
		}
		if !stillReferenced {
			purgeOps, purgeErr = bs.purgePropertyKeyDiskEntriesLocked(propertyKey)
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

// propIdxDef is the serialization format for property index definitions.
// RangeCounts (round 4 R2) is written only when set, so a plain index's
// definition stays byte-identical; a binary without the field skips it and
// loads the index plain (its range counts then walk).
type propIdxDef struct {
	LabelToken  uint16 `msgpack:"l"`
	PropertyKey string `msgpack:"p"`
	RangeCounts bool   `msgpack:"rc,omitempty"`
}

// vectorIdxDef is the serialization format for vector index definitions.
// UseBruteForce/M/EfConstruction/EfSearch are additive: a def
// written before these fields existed decodes them to zero, which is
// exactly "default HNSW with default tuning" — backward compatible.
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

// CreateTemporalIndex creates a temporal index on nodes with the given label token.
// Three-phase approach (same as CreatePropertyIndex) for safe concurrent operation.
// Returns ErrTemporalIndexExists if the index already exists.
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
			deleteTemporalIndexIfCurrent(bs.temporalIndexes, labelToken, liveTI)
			bs.idxMu.Unlock()
			return fmt.Errorf("graph: create temporal index: %w", err)
		}
		rawID := nid.SnowflakeID()
		from, to := indexpkg.NodeTemporalBounds(rawID, n.Temporal())
		backfill = append(backfill, nodeEntry{id: rawID, from: from, to: to})
	}

	// Phase 3: Merge backfill into live index under write Lock. The index stays
	// Building (readers skip it) until the history fold below is in: a
	// published envelope of current rows alone would prune a node by a past
	// row (backlog 56). An ID written since Phase 1 gets its fetched row by
	// Extend (a union: the write folded its own row, and an older row is still
	// a row the ID had).
	bs.idxMu.Lock()
	if err := requireTemporalIndexCurrentForCreate(bs.temporalIndexes, labelToken, liveTI); err != nil {
		bs.idxMu.Unlock()
		return err
	}
	var diskOps []writeOp
	for _, entry := range backfill {
		if _, alive := bs.nodeIDs[types.NodeID(entry.id)]; !alive {
			continue // node deleted during Phase 2
		}
		if liveTI.WasMutated(entry.id) {
			liveTI.Extend(entry.id, entry.from, entry.to) // RAM only: the write maintained disk
			continue
		}
		liveTI.AddKnownAbsent(entry.id, entry.from, entry.to)
		if bs.temporalIdxOnDisk {
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
	bs.idxMu.Unlock()
	if temporalCreateBeforeFoldHook != nil {
		temporalCreateBeforeFoldHook()
	}

	// Phase 4: read the history of the label's members without the lock.
	_, foldIDs, folds, err := bs.temporalHistoryFolds(labelToken)
	if err != nil {
		bs.idxMu.Lock()
		deleteTemporalIndexIfCurrent(bs.temporalIndexes, labelToken, liveTI)
		bs.idxMu.Unlock()
		return err
	}

	// Phase 5: fold and publish. An ID written since Phase 1 that Phase 4 did
	// not read (re-created, given a label or a PutNodeVersion after the member
	// snapshot) has its history folded here, under the lock, so no write lands
	// between that read and the publish.
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
			deleteTemporalIndexIfCurrent(bs.temporalIndexes, labelToken, liveTI)
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

// temporalHistoryFolds snapshots the members of tok under idxMu.RLock and
// reads their history without the lock (GetNodeHistory never re-enters
// idxMu). It returns the index seen, the member IDs read, and every history
// row's bounds; a member deleted concurrently is skipped. ti is nil when tok
// has no temporal index.
func (bs *Store) temporalHistoryFolds(tok uint16) (*indexpkg.TemporalIndex, map[snowflake.ID]struct{}, []temporalFoldEntry, error) {
	bs.idxMu.RLock()
	ti, ok := bs.temporalIndexes[tok]
	var ids []types.NodeID
	if ok {
		// Through the snapshot helper, not labelIdx: under LabelIndexOnDisk
		// the map is empty and the fold silently dropped every history
		// envelope (backlog 54 found it).
		var err error
		if ids, err = bs.labelNodeIDsSnapshotLocked(tok); err != nil {
			bs.idxMu.RUnlock()
			return nil, nil, nil, fmt.Errorf("graph: temporal-index history envelope fold (label %d): %w", tok, err)
		}
	}
	bs.idxMu.RUnlock()
	if !ok {
		return nil, nil, nil, nil
	}
	read := make(map[snowflake.ID]struct{}, len(ids))
	var folds []temporalFoldEntry
	for _, id := range ids {
		read[id.SnowflakeID()] = struct{}{}
		hist, err := bs.GetNodeHistory(id)
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue // deleted concurrently — its current row is already gone
			}
			return nil, nil, nil, fmt.Errorf("graph: temporal-index history envelope fold (label %d): %w", tok, err)
		}
		for _, hv := range hist {
			if hv == nil {
				continue
			}
			f, t := indexpkg.NodeTemporalBounds(id.SnowflakeID(), hv.Temporal())
			folds = append(folds, temporalFoldEntry{id.SnowflakeID(), f, t})
		}
	}
	return ti, read, folds, nil
}

// foldTemporalHistoryEnvelopes folds every history version's valid-time bounds
// into the per-node ENVELOPE of each temporal index in tokens (sound superset).
// The current-version rebuild records only each node's latest version; this
// pass adds the historical versions via Extend, so a past interval that
// differs from the current one stays covered by the envelope. Called after
// loadIndexes (all tokens); tolerant of nodes deleted concurrently.
func (bs *Store) foldTemporalHistoryEnvelopes(tokens []uint16) error {
	for _, tok := range tokens {
		ti, _, folds, err := bs.temporalHistoryFolds(tok)
		if err != nil {
			return err
		}
		if ti == nil || len(folds) == 0 {
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

// foldNodeHistoryLocked extends ti by every history row of id; the RAM history
// presence set answers "has none" without a read. Caller holds idxMu.Lock (lock
// order idxMu -> presence buildMu -> wbMu; the history read takes only wbMu and
// a read transaction).
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
			from, to := indexpkg.NodeTemporalBounds(id, hv.Temporal())
			ti.Extend(id, from, to)
		}
	}
	return nil
}

// extendNodeVersionTemporalLocked folds a PutNodeVersion row into the index of
// each of its labels that covers id (its envelope then holds every row of id)
// or is still Building (the create folds the history of every ID written
// meanwhile). An uncovered ID stays uncovered and is never pruned: extending
// it here would vouch for this one row while its other rows stay outside.
// Caller holds idxMu.Lock.
func (bs *Store) extendNodeVersionTemporalLocked(n *types.Node, id snowflake.ID) {
	from, to := indexpkg.NodeTemporalBounds(id, n.Temporal())
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

// foldRecreatedNodeHistoryLocked is called by a node create door, and by a
// label-add door (backlog 56), before it adds n: for every label of n whose published temporal index does not cover
// id, id may be a deleted node whose history is still on disk (deleted before
// the index was built, or before a reopen rebuilt it), and the new row alone
// would make the envelope vouch for less than the ID holds. Its history is
// folded first (RAM envelope only, as foldTemporalHistoryEnvelopes does). A
// Building index needs nothing: the create's ID is live when the create's
// history fold runs. The presence set answers "no history" without a read.
// Caller holds idxMu.Lock (lock order idxMu -> presence buildMu -> wbMu; the
// history read takes only wbMu and a read transaction).
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
				from, to := indexpkg.NodeTemporalBounds(id, hv.Temporal())
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
	// A TemporalIndex definition is exclusively keyed by labelToken (only one
	// can ever exist per label — see purgeTemporalIndexDiskEntriesLocked's doc
	// comment), so purging the whole labelToken prefix is always safe.
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

// CreateHighFrequencyIndex creates a time-bucketed high-frequency index on nodes
// with the given label token. Only one temporal index type can exist per label —
// returns ErrInvalidTemporalIndexConfig if bucketSize is not a positive whole
// millisecond and
// returns ErrTemporalIndexExists if a temporalIndex or highFrequencyIndex already
// exists for this label.
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

// CreateVectorIndex creates a vector similarity index for nodes with the given label token,
// on the given property key, expecting vectors of length dims. The index
// defaults to the approximate HNSW engine; use CreateVectorIndexWithOptions
// for the brute-force escape hatch or HNSW tuning.
// Scans existing nodes to populate the index. Returns ErrVectorIndexExists on duplicate.
// Definitions are persisted and entries are rebuilt from node properties on startup.
func (bs *Store) CreateVectorIndex(labelToken uint16, propertyKey string, dims int, metric DistanceMetric) error {
	return bs.CreateVectorIndexWithOptions(labelToken, propertyKey, dims, metric, storecontract.VectorIndexOptions{})
}

// CreateVectorIndexWithOptions is CreateVectorIndex with additional control
// over the search engine (opts.UseBruteForce) and HNSW tuning (opts.M /
// EfConstruction / EfSearch). A zero-value opts is identical to
// CreateVectorIndex (documented HNSW defaults). The chosen engine/tuning is
// persisted alongside Dims/Metric so a restart rebuilds the SAME engine
// (see vectorIdxDef) rather than silently reverting to the default.
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

// SearchNearestNodes returns the k nodes with vectors closest to query
// under the index defined for labelToken+propertyKey.
// Results are ordered by ascending distance (closest first).
// Returns ErrVectorIndexNotFound if no index exists.
// Returns ErrDimensionMismatch if query length differs from the index's dims.
// Returns ErrInvalidVectorValue if query contains NaN or infinity.
// Returns nil, nil if the index exists but has no entries.
//
// After and Limit are applied to the distance-ordered result. Depth has no
// meaning for this single-tier backend. Temporal filtering is applied before
// heap selection so ineligible candidates cannot occupy top-k slots.
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
	hasTemporal := storepkg.HasTemporalFilter(opts)
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

func (bs *Store) vectorTemporalFilter(key indexpkg.VectorIndexKey, dims int, opts QueryOpts) (func(snowflake.ID) bool, func() error) {
	if !storepkg.HasTemporalFilter(opts) {
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
		return storepkg.MatchesTemporalFilter(id, n.Temporal(), opts)
	}, func() error { return filterErr }
}

// SearchNearestFiltered is the package-internal entry point used by the
// Graph layer to perform vector search with an eligibility filter applied
// BEFORE the k-cut. The filter is combined with current-row shape validation
// before the vector index heap selection.
//
// Returns raw snowflake.IDs in ascending distance order; the caller is
// responsible for resolving entities (current or historical version).
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
	for key, idx := range bs.propertyIndexes {
		if idx == nil || idx.Mutated != nil {
			continue
		}
		defs = append(defs, propIdxDef{LabelToken: key.LabelToken, PropertyKey: key.PropertyKey, RangeCounts: idx.RangeCounts()})
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
