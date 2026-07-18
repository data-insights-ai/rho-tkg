package badger

import (
	"fmt"
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// Relationship-history methods.

func (bs *Store) ReplaceRelWithHistory(current *types.Relationship, prevVersion uint32, prevState *types.Relationship) error {
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
	bs.removeRelPropertyTypeClassCountsByID(id, old.TypeToken().Value())
	bs.relCache.Put(id, freezeRelCopy(current))
	bs.maintainRelPropertyIndexesAdd(current, id)
	bs.addRelPropertyTypeClassCounts(current)
	// Refresh the inline valid-time stamp — a history version-close moves
	// valid_to while leaving endpoints/type (and thus adjacency) unchanged.
	bs.setRelValidStampLocked(rid, current)

	// Single appendOps call — atomic in the pending buffer.
	histKey := storepkg.HistRelKey(id, uint64(prevVersion))
	bs.appendOps(
		writeOp{opType: writeOpSet, key: storepkg.RelKey(id), value: data},
		writeOp{opType: writeOpSet, key: histKey, value: histData},
	)
	logErr := bs.logRelPut(current, true)
	bs.idxMu.Unlock()

	if logErr != nil {
		return logErr
	}
	return bs.flushIfNeeded()
}

func (bs *Store) DeleteRelWithHistory(rid types.RelID, prevVersion uint32, tombstone *types.Relationship) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipHistoryVersionSnapshot(rid, prevVersion, tombstone); err != nil {
		return err
	}
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
	bs.deleteRelByInfo(info) // appends delete ops to pending under lock (no record — emitted here)
	bs.appendOps(writeOp{opType: writeOpSet, key: histKey, value: tombData})
	bs.logChangeRaw(storecontract.ChangeRelDelete, delPayload)
	bs.idxMu.Unlock()

	return bs.flushIfNeeded()
}

func (bs *Store) marshalRelToBytes(r *types.Relationship) ([]byte, error) {
	return bs.marshalRelBytes(r)
}

func (bs *Store) PutRelVersion(rid types.RelID, version uint32, r *types.Relationship) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipHistoryVersionSnapshot(rid, version, r); err != nil {
		return err
	}
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
	// Capture a history-only rel (deleted rel reconstructed via version
	// inserts) once the sidecar is built; the bootstrap common case runs before
	// any pinned scan, so the lazy build catches these rows.
	if bs.relTypeMembersBuilt.Load() {
		bs.idxMu.Lock()
		bs.recordRelTypeMemberLocked(r)
		bs.idxMu.Unlock()
	}
	// PutRelVersion holds no idxMu, so enqueue the op and its record together
	// under one wbMu critical section (appendOpsLogged) for snapshot atomicity.
	bs.appendOpsLogged(storecontract.ChangeRelHistoryVersion, logPayload, writeOp{opType: writeOpSet, key: key, value: data})
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

	// Check the write buffer (pending + the snapshot a concurrent flush is
	// mid-committing). Consulting only `pending` would miss a row parked in
	// `flushing` during the commit window and wrongly return ErrVersionNotFound
	// for a version that was just written.
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
	err := bs.db.View(func(txn *badgerv4.Txn) error {
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

	// Snapshot the overlay BEFORE the Badger scan to close the commit-window drop
	// (a flush that commits a parked row to Badger and clears `flushing` between a
	// scan-first reader's Badger snapshot and its overlay read loses the row).
	// Mirror of getNodeHistoryByPrefix — see lesson 64.
	overlay, overlayDeletes := bs.pendingHistoryVersionOverlay(prefix, 0)

	entries := make(map[string][]byte)
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = true
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
	emit := func(key string, data []byte) error {
		raw := make([]byte, len(data))
		copy(raw, data)
		collected = append(collected, historyRawRow{version: historyVersionFromKey([]byte(key)), raw: raw})
		return nil
	}

	pendingIdx := 0
	drainPendingBefore := func(bound string) error {
		for pendingIdx < len(pendingKeys) && pendingKeys[pendingIdx] < bound {
			if err := emit(pendingKeys[pendingIdx], pending[pendingKeys[pendingIdx]]); err != nil {
				return err
			}
			pendingIdx++
			if reachedLimit() {
				return nil
			}
		}
		return nil
	}

	seekKey := historyVersionSeekKey(prefix, startVersion)
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = true
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
			if err := drainPendingBefore(k); err != nil {
				return err
			}
			if reachedLimit() {
				return nil
			}
			if pendingIdx < len(pendingKeys) && pendingKeys[pendingIdx] == k {
				if err := emit(k, pending[k]); err != nil {
					return err
				}
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
				return emit(k, val)
			}); err != nil {
				return err
			}
			if reachedLimit() {
				return nil
			}
		}
		for pendingIdx < len(pendingKeys) {
			if err := emit(pendingKeys[pendingIdx], pending[pendingKeys[pendingIdx]]); err != nil {
				return err
			}
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
	return bs.truncateHistoryByPrefix(prefix, keepVersions, storecontract.ChangeRelHistoryTruncate, payload)
}

// TrimRelHistoryFrom removes relationship history entries at or after minVersion.
func (bs *Store) TrimRelHistoryFrom(rid types.RelID, minVersion uint32) error {
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
	return bs.trimHistoryFromPrefix(prefix, minVersion, storecontract.ChangeRelHistoryTruncate, payload)
}

func (bs *Store) ForEachRelHistoryID(fn func(types.RelID) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
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

// ForEachDeletedRelID is the relationship counterpart of ForEachDeletedNodeID
// — visits only history-but-no-current IDs via an inline HasRelID check.
func (bs *Store) ForEachDeletedRelID(fn func(types.RelID) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
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

// MaxRelHistoryID returns the highest relationship ID with version history
// visible to this store. It includes pending buffered writes and the persisted
// Badger key space, and returns zero when no relationship history exists.
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

	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		var lastBadger snowflake.ID
		var haveLastBadger bool

		for it.Seek(seekKey); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			if len(key) != storepkg.SizeHistKey {
				continue
			}
			if _, deleted := pendingDeletes[string(key)]; deleted {
				continue
			}
			id := storepkg.ParseIDFromKey(key, 1)
			if id <= afterRaw {
				continue
			}
			if haveLastBadger && id == lastBadger {
				continue
			}
			lastBadger = id
			haveLastBadger = true

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
			if _, dup := emitted[id]; dup {
				continue
			}
			emitted[id] = struct{}{}
			out = append(out, types.RelID(id))
			if limit > 0 && len(out) >= limit {
				return nil
			}
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
