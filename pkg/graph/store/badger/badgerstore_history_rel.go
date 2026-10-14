package badger

import (
	"fmt"
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// Relationship-history methods.

func (bs *Store) ReplaceRelWithHistory(current *types.Relationship, prevVersion uint32, prevState *types.Relationship) error {
	return bs.replaceRelWithHistoryRouted(current, prevVersion, prevState, 0)
}

// ReplaceRelWithHistoryScoped is ReplaceRelWithHistory with the change-log record
// routed to the ScopedTxChangeLog buffer named by token (0 = unscoped).
func (bs *Store) ReplaceRelWithHistoryScoped(current *types.Relationship, prevVersion uint32, prevState *types.Relationship, token uint64) error {
	if token == 0 {
		return bs.ReplaceRelWithHistory(current, prevVersion, prevState)
	}
	return bs.replaceRelWithHistoryRouted(current, prevVersion, prevState, token)
}

func (bs *Store) replaceRelWithHistoryRouted(current *types.Relationship, prevVersion uint32, prevState *types.Relationship, token uint64) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipWrite(current); err != nil {
		return err
	}
	rid := current.InternalID()
	id := rid.SnowflakeID()
	if err := storecontract.ValidateRelationshipHistoryVersionSnapshot(rid, prevVersion, prevState); err != nil {
		return err
	}
	// A property write, a close or an interval change of a relationship
	// arrives here (UPDATE: poisons the type's columns, records no append).
	defer bs.bumpRelEpochForType(uint16(current.TypeToken()))

	_, prefetchErr := bs.prefetchRel(rid)

	// Serialize current state.
	data, err := bs.marshalRelToBytes(current)
	if err != nil {
		return fmt.Errorf("graph: marshal relationship: %w", err)
	}

	// Serialize history snapshot (anchor or delta — see historyRelValue).
	histData, err := bs.historyRelValue(id, uint64(prevVersion), prevState)
	if err != nil {
		return fmt.Errorf("graph: marshal rel version: %w", err)
	}
	changePayload, err := bs.buildRelPutPayload(current, true)
	if err != nil {
		return err
	}

	bs.idxMu.Lock()

	if _, exists := bs.relIDs[rid]; !exists {
		bs.idxMu.Unlock()
		return ErrRelNotFound
	}
	if prefetchErr != nil {
		bs.idxMu.Unlock()
		return fmt.Errorf("graph: read relationship before replace history: %w", prefetchErr)
	}
	old, err := bs.getRelLocked(rid)
	if err != nil {
		bs.idxMu.Unlock()
		return fmt.Errorf("graph: read relationship before replace history: %w", err)
	}
	if err := storecontract.ValidateRelationshipLiveVersion(old, prevVersion); err != nil {
		bs.idxMu.Unlock()
		return err
	}
	if err := storecontract.ValidateRelationshipReplacement(old, current); err != nil {
		bs.idxMu.Unlock()
		return err
	}
	if err := storecontract.ValidateRelationshipReplacement(old, prevState); err != nil {
		bs.idxMu.Unlock()
		return err
	}

	// Refresh the rel property index (property values may have changed).
	bs.maintainRelPropertyIndexesRemove(old, id)
	bs.maintainRelTypeTemporalIndexesRemove(old, id) // BACKLOG 21c
	bs.removeRelPropertyTypeClassCounts(old)
	bs.removeRelPropertyStatsCounts(old)
	bs.bumpRelRevLocked(rid)                                   // this door always re-reads via getRelLocked above, but must still bump so a concurrent ReplaceRelationship's prefetch detects this write (BACKLOG 18b)
	bs.bumpRelBeliefWatermarkLocked(rid, relTxFrom(current))   // BACKLOG 10c
	bs.bumpRelBeliefWatermarkLocked(rid, relTxFrom(prevState)) // BACKLOG 10c — the demoted history row
	bs.recordRelRowLocked(prevState)                           // backlog 8 — and its property values
	bs.maintainRelPropertyIndexesAdd(current, id)
	bs.maintainRelTypeTemporalIndexesAdd(current, id) // BACKLOG 21c
	bs.addRelPropertyTypeClassCounts(current)
	bs.addRelPropertyStatsCounts(current)
	// Refresh the inline valid-time stamp — a history version-close moves
	// valid_to while leaving endpoints/type (and thus adjacency) unchanged.
	bs.setRelValidStampLocked(rid, current)

	// Single appendOps call — atomic in the pending buffer — then the cache.
	histKey := storepkg.HistRelKey(id, uint64(prevVersion))
	bs.publishMoveLocked([]writeOp{
		{opType: writeOpSet, key: storepkg.RelKey(id), value: data},
		{opType: writeOpSet, key: histKey, value: histData},
	}, func() { bs.putMovedRelLocked(id, current) })
	logErr := bs.logChangeRoutedRaw(storecontract.ChangeRelPut, changePayload, token)
	bs.idxMu.Unlock()
	if logErr != nil {
		return logErr
	}

	return bs.flushIfNeeded()
}

func (bs *Store) DeleteRelWithHistory(rid types.RelID, prevVersion uint32, tombstone *types.Relationship) error {
	return bs.deleteRelWithHistoryRouted(rid, prevVersion, tombstone, 0)
}

// DeleteRelWithHistoryScoped is DeleteRelWithHistory with the change-log record
// routed to the ScopedTxChangeLog buffer named by token (0 = unscoped).
func (bs *Store) DeleteRelWithHistoryScoped(rid types.RelID, prevVersion uint32, tombstone *types.Relationship, token uint64) error {
	if token == 0 {
		return bs.DeleteRelWithHistory(rid, prevVersion, tombstone)
	}
	return bs.deleteRelWithHistoryRouted(rid, prevVersion, tombstone, token)
}

func (bs *Store) deleteRelWithHistoryRouted(rid types.RelID, prevVersion uint32, tombstone *types.Relationship, token uint64) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipHistoryVersionSnapshot(rid, prevVersion, tombstone); err != nil {
		return err
	}
	defer bs.bumpRelEpoch() // the type and adjacency views lose the relationship
	id := rid.SnowflakeID()
	// Serialize tombstone OUTSIDE lock (no I/O under write lock).
	tombData, err := bs.historyRelValue(id, uint64(prevVersion), tombstone)
	if err != nil {
		return fmt.Errorf("graph: marshal rel tombstone: %w", err)
	}
	// Build the with-history rel-delete record before the lock.
	delPayload, err := bs.relDeleteWithHistoryPayload(id, tombstone)
	if err != nil {
		return fmt.Errorf("graph: encode change-log: %w", err)
	}
	histKey := storepkg.HistRelKey(id, uint64(prevVersion))
	if _, err := bs.prefetchRelDeleteInfo(rid); err != nil {
		return err
	}

	bs.idxMu.Lock()
	r, err := bs.getRelLocked(rid)
	if err != nil {
		bs.idxMu.Unlock()
		return err
	}
	if err := storecontract.ValidateRelationshipLiveVersion(r, prevVersion); err != nil {
		bs.idxMu.Unlock()
		return err
	}
	if err := storecontract.ValidateRelationshipReplacement(r, tombstone); err != nil {
		bs.idxMu.Unlock()
		return err
	}
	info := relDeleteInfoFromRelationship(r)
	bs.bumpRelBeliefWatermarkLocked(rid, relTxFrom(tombstone)) // BACKLOG 10c
	bs.recordRelRowLocked(tombstone)                           // backlog 8
	// The tombstone first, then the removal (appends its delete ops to pending
	// under the lock; no record — emitted here).
	bs.publishMoveLocked([]writeOp{{opType: writeOpSet, key: histKey, value: tombData}}, func() { bs.deleteRelByInfo(info, r) })
	logErr := bs.logChangeRoutedRaw(storecontract.ChangeRelDelete, delPayload, token)
	bs.idxMu.Unlock()
	if logErr != nil {
		return logErr
	}

	return bs.flushIfNeeded()
}

func (bs *Store) marshalRelToBytes(r *types.Relationship) ([]byte, error) {
	return bs.marshalRelBytes(r)
}

func (bs *Store) PutRelVersion(rid types.RelID, version uint32, r *types.Relationship) error {
	return bs.putRelVersionRouted(rid, version, r, 0)
}

// PutRelVersionScoped is PutRelVersion with the change-log record routed to the
// ScopedTxChangeLog buffer named by token (0 = unscoped).
func (bs *Store) PutRelVersionScoped(rid types.RelID, version uint32, r *types.Relationship, token uint64) error {
	if token == 0 {
		return bs.PutRelVersion(rid, version, r)
	}
	return bs.putRelVersionRouted(rid, version, r, token)
}

func (bs *Store) putRelVersionRouted(rid types.RelID, version uint32, r *types.Relationship, token uint64) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipHistoryVersionSnapshot(rid, version, r); err != nil {
		return err
	}
	defer bs.bumpRelEpochForType(uint16(r.TypeToken())) // a version-chain write, as PutNodeVersion bumps the node epoch
	id := rid.SnowflakeID()
	data, err := bs.historyRelValue(id, uint64(version), r)
	if err != nil {
		return fmt.Errorf("graph: marshal rel version: %w", err)
	}
	logPayload, err := bs.historyVersionRelPayload(version, r)
	if err != nil {
		return fmt.Errorf("graph: encode change-log: %w", err)
	}
	key := storepkg.HistRelKey(id, uint64(version))
	// Capture a history-only rel in the rel-type sidecar and belief watermark
	// (BACKLOG 10c); op and record are enqueued under one wbMu section
	// (appendOpsLoggedRouted) for snapshot atomicity.
	// A live or covered rel's envelope covers every row since the index existed, so
	// this version joins it. An uncovered rel without a current row stays uncovered
	// (never pruned): extending would vouch for this one version only. While the index
	// is Building the version joins it and records the ID (backlog 22).
	// A row whose endpoints differ from the live row's joins the ex-adjacency in the
	// same batch (backlog 50), decided before the enqueue (a delete in between adds
	// the legs itself).
	bs.idxMu.RLock()
	ops := append([]writeOp{{opType: writeOpSet, key: key, value: data}}, bs.exAdjOpsForHistoryRowLocked(rid, r)...)
	ops = append(ops, bs.voidMarkerOps(r.Temporal())...)
	bs.idxMu.RUnlock()
	err = bs.enqueueVersionAgainstLazyBuilds(
		func() bool {
			return bs.relTypeMembersBuilt.Load() || bs.relBeliefWatermarkBuilt.Load() || len(bs.relTypeTemporalIndexes) > 0 || len(bs.relPropTx) > 0
		},
		func() {
			bs.recordRelTypeMemberLocked(r)
			bs.bumpRelBeliefWatermarkLocked(rid, relTxFrom(r))
			bs.recordRelRowLocked(r) // backlog 8: tracking or built property sidecars
			if _, live := bs.relIDs[rid]; live || indexpkg.RelCoveredInTemporalIndexes(bs.relTypeTemporalIndexes, r, id) || indexpkg.RelTemporalIndexBuilding(bs.relTypeTemporalIndexes, r) {
				bs.maintainRelTypeTemporalIndexesAdd(r, id)
			}
		},
		func() error {
			return bs.appendOpsLoggedRouted(storecontract.ChangeRelHistoryVersion, logPayload, token, ops...)
		},
	)
	if err != nil {
		return err
	}
	return bs.flushIfNeeded()
}

func (bs *Store) GetRelVersion(rid types.RelID, version uint32) (*types.Relationship, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return nil, err
	}
	id := rid.SnowflakeID()
	key := storepkg.HistRelKey(id, uint64(version))

	// Check pending and the snapshot a flush is mid-committing (`flushing`): pending
	// alone would miss a parked row and return ErrVersionNotFound for a just-written version.
	op, found := bs.lookupPending(string(key))

	if found {
		if op.opType == writeOpDelete {
			return nil, ErrVersionNotFound
		}
		r, err := bs.decodeHistoryRelValue(id, uint64(version), op.value)
		if err != nil {
			return nil, err
		}
		return r.DeepCopy(), nil
	}

	// Fall through to Badger. Copy the raw value out before reconstruction (a
	// delta reconstruct point-reads the interval anchor via a separate txn).
	var raw []byte
	err := bs.view(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(key)
		if err == badgerv4.ErrKeyNotFound {
			return ErrVersionNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			raw = make([]byte, len(val))
			copy(raw, val)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	r, err := bs.decodeHistoryRelValue(id, uint64(version), raw)
	if err != nil {
		return nil, err
	}
	return r.DeepCopy(), nil
}

// decodeHistoryRelValue reconstructs (full-or-delta) then decodes one rel history
// value to a *types.Relationship. Point-reads the interval anchor for a delta.
func (bs *Store) decodeHistoryRelValue(id snowflake.ID, version uint64, raw []byte) (*types.Relationship, error) {
	w, err := bs.reconstructRelHistoryWire(id, version, raw, nil)
	if err != nil {
		return nil, fmt.Errorf("graph: unmarshal rel version: %w", err)
	}
	r, err := bs.decodeRelHistoryWireForKey(w, id, version)
	if err != nil {
		return nil, fmt.Errorf("graph: decode rel version: %w", err)
	}
	return r, nil
}

func (bs *Store) GetRelHistory(rid types.RelID) ([]*types.Relationship, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return nil, err
	}
	id := rid.SnowflakeID()
	prefix := storepkg.HistRelPrefix(id)
	return bs.getRelHistoryByPrefix(prefix)
}

func (bs *Store) RelHistoryVersionsFrom(rid types.RelID, startVersion uint32, limit int) ([]*types.Relationship, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateHistoryPageLimit(limit); err != nil {
		return nil, err
	}
	id := rid.SnowflakeID()
	prefix := storepkg.HistRelPrefix(id)
	return bs.relHistoryVersionsFromPrefix(prefix, startVersion, limit)
}

func (bs *Store) getRelHistoryByPrefix(prefix []byte) ([]*types.Relationship, error) {
	expectedID := storepkg.ParseIDFromKey(prefix, 1)

	// Snapshot the overlay BEFORE the Badger scan to close the commit-window drop;
	// mirror of getNodeHistoryByPrefix (lesson 64).
	overlay, overlayDeletes := bs.pendingHistoryVersionOverlay(prefix, 0)

	entries := make(map[string][]byte)
	err := bs.view(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.Prefix = prefix // bound the scan and the value prefetch to this entity (backlog 20)
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			if len(key) != storepkg.SizeHistKey {
				continue
			}
			k := string(key)
			err := item.Value(func(val []byte) error {
				cp := make([]byte, len(val))
				copy(cp, val)
				entries[k] = cp
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	if bs.historyScanTestHook != nil {
		bs.historyScanTestHook()
	}

	// Merge the pre-scan overlay snapshot (strictly newer than committed Badger):
	// a delete masks a scanned row, a set overwrites it.
	for k := range overlayDeletes {
		delete(entries, k)
	}
	for k, v := range overlay {
		entries[k] = v
	}

	if len(entries) == 0 {
		return nil, nil
	}

	keys := make([]string, 0, len(entries))
	for k := range entries {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	localAnchor := func(anchorVer uint64) ([]byte, bool) {
		b, ok := entries[string(storepkg.HistRelKey(expectedID, anchorVer))]
		return b, ok
	}
	result := make([]*types.Relationship, 0, len(keys))
	for _, k := range keys {
		version := historyVersionFromKey([]byte(k))
		w, err := bs.reconstructRelHistoryWire(expectedID, version, entries[k], localAnchor)
		if err != nil {
			return nil, fmt.Errorf("graph: unmarshal rel version: %w", err)
		}
		r, err := bs.decodeRelHistoryWireForKey(w, expectedID, version)
		if err != nil {
			return nil, fmt.Errorf("graph: decode rel version: %w", err)
		}
		result = append(result, r.DeepCopy())
	}
	return result, nil
}

func (bs *Store) relHistoryVersionsFromPrefix(prefix []byte, startVersion uint32, limit int) ([]*types.Relationship, error) {
	expectedID := storepkg.ParseIDFromKey(prefix, 1)
	pending, pendingDeletes := bs.pendingHistoryVersionOverlay(prefix, startVersion)
	pendingKeys := make([]string, 0, len(pending))
	for k := range pending {
		pendingKeys = append(pendingKeys, k)
	}
	sort.Strings(pendingKeys)

	collected := make([]historyRawRow, 0, capForLimit(limit))
	reachedLimit := func() bool {
		return limit > 0 && len(collected) >= limit
	}
	// No error return: appending to a slice cannot fail, and threading a
	// permanently-nil error through four call sites only obscures that.
	emit := func(key string, data []byte) {
		raw := make([]byte, len(data))
		copy(raw, data)
		collected = append(collected, historyRawRow{version: historyVersionFromKey([]byte(key)), raw: raw})
	}

	pendingIdx := 0
	// No error return, for the same reason as emit: with emit errorless this can
	// no longer fail either. Dropping it here is the cascade of that change.
	drainPendingBefore := func(bound string) {
		for pendingIdx < len(pendingKeys) && pendingKeys[pendingIdx] < bound {
			emit(pendingKeys[pendingIdx], pending[pendingKeys[pendingIdx]])
			pendingIdx++
			if reachedLimit() {
				return
			}
		}
	}

	seekKey := historyVersionSeekKey(prefix, startVersion)
	err := bs.view(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.Prefix = prefix // bound the scan and the value prefetch to this entity (backlog 20)
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(seekKey); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			if len(key) != storepkg.SizeHistKey {
				continue
			}
			if historyVersionFromKey(key) < uint64(startVersion) {
				continue
			}
			k := string(key)
			drainPendingBefore(k)
			if reachedLimit() {
				return nil
			}
			if pendingIdx < len(pendingKeys) && pendingKeys[pendingIdx] == k {
				emit(k, pending[k])
				pendingIdx++
				if reachedLimit() {
					return nil
				}
				continue
			}
			if _, deleted := pendingDeletes[k]; deleted {
				continue
			}
			if err := it.Item().Value(func(val []byte) error {
				emit(k, val)
				return nil
			}); err != nil {
				return err
			}
			if reachedLimit() {
				return nil
			}
		}
		for pendingIdx < len(pendingKeys) {
			emit(pendingKeys[pendingIdx], pending[pendingKeys[pendingIdx]])
			pendingIdx++
			if reachedLimit() {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(collected) == 0 {
		return nil, nil
	}
	anchorCache := make(map[uint64][]byte, len(collected))
	for i := range collected {
		anchorCache[collected[i].version] = collected[i].raw
	}
	local := func(anchorVer uint64) ([]byte, bool) { b, ok := anchorCache[anchorVer]; return b, ok }
	result := make([]*types.Relationship, 0, len(collected))
	for i := range collected {
		w, err := bs.reconstructRelHistoryWire(expectedID, collected[i].version, collected[i].raw, local)
		if err != nil {
			return nil, fmt.Errorf("graph: unmarshal rel version: %w", err)
		}
		r, err := bs.decodeRelHistoryWireForKey(w, expectedID, collected[i].version)
		if err != nil {
			return nil, fmt.Errorf("graph: decode rel version: %w", err)
		}
		result = append(result, r.DeepCopy())
	}
	return result, nil
}

func (bs *Store) TruncateRelHistory(rid types.RelID, keepVersions int) error {
	return bs.truncateRelHistoryRouted(rid, keepVersions, 0)
}

// TruncateRelHistoryScoped is TruncateRelHistory with the change-log record routed
// to the ScopedTxChangeLog buffer named by token (0 = unscoped).
func (bs *Store) TruncateRelHistoryScoped(rid types.RelID, keepVersions int, token uint64) error {
	if token == 0 {
		return bs.TruncateRelHistory(rid, keepVersions)
	}
	return bs.truncateRelHistoryRouted(rid, keepVersions, token)
}

func (bs *Store) truncateRelHistoryRouted(rid types.RelID, keepVersions int, token uint64) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return err
	}
	id := rid.SnowflakeID()
	prefix := storepkg.HistRelPrefix(id)
	payload, err := bs.buildChangePayload(storepkg.HistoryTruncateBody{ID: int64(id), Bound: int64(keepVersions)})
	if err != nil {
		return fmt.Errorf("graph: encode change-log: %w", err)
	}
	return bs.truncateHistoryByPrefixRouted(prefix, keepVersions, storecontract.ChangeRelHistoryTruncate, payload, token)
}

// TrimRelHistoryFrom removes relationship history entries at or after minVersion.
func (bs *Store) TrimRelHistoryFrom(rid types.RelID, minVersion uint32) error {
	return bs.trimRelHistoryFromRouted(rid, minVersion, 0)
}

// TrimRelHistoryFromScoped is TrimRelHistoryFrom with the change-log record routed
// to the ScopedTxChangeLog buffer named by token (0 = unscoped).
func (bs *Store) TrimRelHistoryFromScoped(rid types.RelID, minVersion uint32, token uint64) error {
	if token == 0 {
		return bs.TrimRelHistoryFrom(rid, minVersion)
	}
	return bs.trimRelHistoryFromRouted(rid, minVersion, token)
}

func (bs *Store) trimRelHistoryFromRouted(rid types.RelID, minVersion uint32, token uint64) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return err
	}
	id := rid.SnowflakeID()
	prefix := storepkg.HistRelPrefix(id)
	payload, err := bs.buildChangePayload(storepkg.HistoryTruncateBody{ID: int64(id), IsTrim: true, Bound: int64(minVersion)})
	if err != nil {
		return fmt.Errorf("graph: encode change-log: %w", err)
	}
	return bs.trimHistoryFromPrefixRouted(prefix, minVersion, storecontract.ChangeRelHistoryTruncate, payload, token)
}

func (bs *Store) ForEachRelHistoryID(fn func(types.RelID) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	// Once the RAM history-presence set is built, walk it instead of the
	// keyspace (round 5 requests 7 and 8; forEachPresentHistoryID).
	if ok, err := bs.forEachPresentHistoryID(&bs.histRelPresence, storepkg.KeyHistRel, nil, func(id snowflake.ID) bool {
		return fn(types.RelID(id))
	}); ok {
		return err
	}
	maxID, err := bs.maxHistoryID(storepkg.KeyHistRel)
	if err != nil {
		return fmt.Errorf("graph: scan rel history max ID: %w", err)
	}
	if maxID == 0 {
		return nil
	}
	var after types.RelID
	for {
		ids, err := bs.AllRelHistoryIDsFrom(after, 1024)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		for _, id := range ids {
			if id.SnowflakeID() > maxID {
				return nil
			}
			if !fn(id) {
				return nil
			}
		}
		after = ids[len(ids)-1]
		if after.SnowflakeID() >= maxID {
			return nil
		}
	}
}

// ForEachDeletedRelID is the rel counterpart of ForEachDeletedNodeID (history but
// no current row, inline HasRelID). Once the RAM history-presence set is built
// (forEachPresentHistoryID) IDs come from it, not a keyspace scan.
func (bs *Store) ForEachDeletedRelID(fn func(types.RelID) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	if ok, err := bs.forEachPresentHistoryID(&bs.histRelPresence, storepkg.KeyHistRel, bs.HasRelID, func(id snowflake.ID) bool {
		return fn(types.RelID(id))
	}); ok {
		return err
	}
	maxID, err := bs.maxHistoryID(storepkg.KeyHistRel)
	if err != nil {
		return fmt.Errorf("graph: scan rel history max ID: %w", err)
	}
	if maxID == 0 {
		return nil
	}
	var after types.RelID
	for {
		ids, err := bs.AllRelHistoryIDsFrom(after, 1024)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		for _, id := range ids {
			if id.SnowflakeID() > maxID {
				return nil
			}
			if bs.HasRelID(id.SnowflakeID()) {
				continue
			}
			if !fn(id) {
				return nil
			}
		}
		after = ids[len(ids)-1]
		if after.SnowflakeID() >= maxID {
			return nil
		}
	}
}

// MaxRelHistoryID returns the highest relationship ID with version history,
// including pending buffered writes; zero when there is none.
func (bs *Store) MaxRelHistoryID() (types.RelID, error) {
	if err := bs.checkOpen(); err != nil {
		return 0, err
	}
	maxID, err := bs.maxHistoryID(storepkg.KeyHistRel)
	if err != nil {
		return 0, err
	}
	return types.RelID(maxID), nil
}

func (bs *Store) AllRelHistoryIDs() ([]types.RelID, error) {
	return bs.AllRelHistoryIDsFrom(types.RelID(0), 0)
}

func (bs *Store) AllRelHistoryIDsFrom(after types.RelID, limit int) ([]types.RelID, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidatePagination(types.EntityID(after), limit); err != nil {
		return nil, err
	}
	afterRaw := snowflake.ID(after)

	pending, pendingDeletes := bs.pendingHistoryIDOverlay(storepkg.KeyHistRel, afterRaw)

	pendingSorted := make([]snowflake.ID, 0, len(pending))
	for id := range pending {
		pendingSorted = append(pendingSorted, id)
	}
	storepkg.SortSnowflakeIDs(pendingSorted)

	seekKey := historyIDSeekKey(storepkg.KeyHistRel, afterRaw)
	if seekKey == nil {
		// after+1 overflow: after is already the max snowflake ID. The pending
		// set is filtered to id > afterRaw, so nothing can remain.
		return nil, nil
	}
	prefix := []byte{storepkg.KeyHistRel}

	out := make([]types.RelID, 0, capForLimit(limit))
	pendingIdx := 0
	emitted := make(map[snowflake.ID]struct{})

	err := bs.view(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(seekKey); it.ValidForPrefix(prefix); {
			key := it.Item().Key()
			if len(key) != storepkg.SizeHistKey {
				it.Next()
				continue
			}
			if _, deleted := pendingDeletes[string(key)]; deleted {
				// Only this version row is masked; the id may still be emitted via another row, so step to the next row.
				it.Next()
				continue
			}
			id := storepkg.ParseIDFromKey(key, 1)
			if id <= afterRaw {
				it.Next()
				continue
			}

			for pendingIdx < len(pendingSorted) && pendingSorted[pendingIdx] < id {
				pid := pendingSorted[pendingIdx]
				pendingIdx++
				if _, dup := emitted[pid]; dup {
					continue
				}
				emitted[pid] = struct{}{}
				out = append(out, types.RelID(pid))
				if limit > 0 && len(out) >= limit {
					return nil
				}
			}
			if pendingIdx < len(pendingSorted) && pendingSorted[pendingIdx] == id {
				pendingIdx++
			}
			if _, dup := emitted[id]; !dup {
				emitted[id] = struct{}{}
				out = append(out, types.RelID(id))
				if limit > 0 && len(out) >= limit {
					return nil
				}
			}
			// Id decided: Seek past its remaining version rows so the scan is O(distinct IDs),
			// not O(version rows).
			next := historyIDSeekKey(storepkg.KeyHistRel, id)
			if next == nil {
				break // id is the max snowflake ID — no successor exists.
			}
			it.Seek(next)
		}
		for pendingIdx < len(pendingSorted) {
			pid := pendingSorted[pendingIdx]
			pendingIdx++
			if _, dup := emitted[pid]; dup {
				continue
			}
			emitted[pid] = struct{}{}
			out = append(out, types.RelID(pid))
			if limit > 0 && len(out) >= limit {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("graph: scan rel history IDs: %w", err)
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}
