package badger

import (
	"fmt"
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
)

// Relationship-history methods (R5-F9 split out from badgerstore_history.go).

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

	// Serialize history snapshot.
	histData, err := bs.marshalRelToBytes(prevState)
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

	bs.relCache.Put(id, freezeRelCopy(current))

	// Single appendOps call — atomic in the pending buffer.
	histKey := storepkg.HistRelKey(id, uint64(prevVersion))
	bs.appendOps(
		writeOp{opType: writeOpSet, key: storepkg.RelKey(id), value: data},
		writeOp{opType: writeOpSet, key: histKey, value: histData},
	)
	bs.idxMu.Unlock()

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
	// Serialize tombstone OUTSIDE lock (B3: no I/O under write lock).
	tombData, err := bs.marshalRelToBytes(tombstone)
	if err != nil {
		return fmt.Errorf("graph: marshal rel tombstone: %w", err)
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
	bs.deleteRelByInfo(info) // appends delete ops to pending under lock
	bs.appendOps(writeOp{opType: writeOpSet, key: histKey, value: tombData})
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
	data, err := bs.marshalRelToBytes(r)
	if err != nil {
		return fmt.Errorf("graph: marshal rel version: %w", err)
	}
	key := storepkg.HistRelKey(id, uint64(version))
	bs.appendOps(writeOp{opType: writeOpSet, key: key, value: data})
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

	// Check pending buffer.
	bs.wbMu.Lock()
	op, found := bs.pending[string(key)]
	bs.wbMu.Unlock()

	if found {
		if op.opType == writeOpDelete {
			return nil, ErrVersionNotFound
		}
		var w storepkg.RelWire
		if err := msgpack.Unmarshal(op.value, &w); err != nil {
			return nil, fmt.Errorf("graph: unmarshal rel version: %w", err)
		}
		r, err := bs.decodeRelHistoryWireForKey(w, id, uint64(version))
		if err != nil {
			return nil, fmt.Errorf("graph: decode rel version: %w", err)
		}
		return r.DeepCopy(), nil
	}

	// Fall through to Badger.
	var r *types.Relationship
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(key)
		if err == badgerv4.ErrKeyNotFound {
			return ErrVersionNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var w storepkg.RelWire
			if err := msgpack.Unmarshal(val, &w); err != nil {
				return fmt.Errorf("graph: unmarshal rel version: %w", err)
			}
			decoded, err := bs.decodeRelHistoryWireForKey(w, id, uint64(version))
			if err != nil {
				return fmt.Errorf("graph: decode rel version: %w", err)
			}
			r = decoded
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return r.DeepCopy(), nil
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
	prefixStr := string(prefix)

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

	bs.wbMu.Lock()
	for k, op := range bs.pending {
		if len(k) == storepkg.SizeHistKey && k[:len(prefixStr)] == prefixStr {
			if op.opType == writeOpDelete {
				delete(entries, k)
			} else {
				cp := make([]byte, len(op.value))
				copy(cp, op.value)
				entries[k] = cp
			}
		}
	}
	bs.wbMu.Unlock()

	if len(entries) == 0 {
		return nil, nil
	}

	keys := make([]string, 0, len(entries))
	for k := range entries {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	result := make([]*types.Relationship, 0, len(keys))
	for _, k := range keys {
		var w storepkg.RelWire
		if err := msgpack.Unmarshal(entries[k], &w); err != nil {
			return nil, fmt.Errorf("graph: unmarshal rel version: %w", err)
		}
		r, err := bs.decodeRelHistoryWireForKey(w, expectedID, historyVersionFromKey([]byte(k)))
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

	result := make([]*types.Relationship, 0, capForLimit(limit))
	reachedLimit := func() bool {
		return limit > 0 && len(result) >= limit
	}
	emit := func(key string, data []byte) error {
		var w storepkg.RelWire
		if err := msgpack.Unmarshal(data, &w); err != nil {
			return fmt.Errorf("graph: unmarshal rel version: %w", err)
		}
		r, err := bs.decodeRelHistoryWireForKey(w, expectedID, historyVersionFromKey([]byte(key)))
		if err != nil {
			return fmt.Errorf("graph: decode rel version: %w", err)
		}
		result = append(result, r.DeepCopy())
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
	if len(result) == 0 {
		return nil, nil
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
	return bs.truncateHistoryByPrefix(prefix, keepVersions)
}

// TrimRelHistoryFrom removes relationship history entries at or after minVersion.
func (bs *Store) TrimRelHistoryFrom(rid types.RelID, minVersion uint32) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return err
	}
	prefix := storepkg.HistRelPrefix(rid.SnowflakeID())
	return bs.trimHistoryFromPrefix(prefix, minVersion)
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
