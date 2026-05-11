package badger

import (
	"fmt"
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	storecontract "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// Relationship-history methods (R5-F9 split out from badgerstore_history.go).

func (bs *Store) ReplaceRelWithHistory(current *types.Relationship, prevVersion uint32, prevState *types.Relationship) error {
	if err := storecontract.ValidateRelationshipWrite(current); err != nil {
		return err
	}
	rid := current.InternalID()
	id := rid.SnowflakeID()
	if err := storecontract.ValidateRelationshipHistorySnapshot(rid, prevState); err != nil {
		return err
	}
	if err := bs.checkOpen(); err != nil {
		return err
	}

	old, prefetchErr := bs.prefetchRel(rid)

	// Serialize current state.
	data, err := marshalRelToBytes(current)
	if err != nil {
		return fmt.Errorf("graph: marshal relationship: %w", err)
	}

	// Serialize history snapshot.
	histData, err := marshalRelToBytes(prevState)
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
	if err := storecontract.ValidateRelationshipReplacement(old, current); err != nil {
		bs.idxMu.Unlock()
		return err
	}
	if err := storecontract.ValidateRelationshipReplacement(old, prevState); err != nil {
		bs.idxMu.Unlock()
		return err
	}

	bs.relCache.Put(id, current.DeepCopy())

	// Single appendOps call — atomic in the pending buffer.
	histKey := storepkg.HistRelKey(id, uint64(prevVersion))
	bs.appendOps(
		writeOp{opType: writeOpSet, key: storepkg.RelKey(id), value: data},
		writeOp{opType: writeOpSet, key: histKey, value: histData},
	)
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

func (bs *Store) DeleteRelWithHistory(rid types.RelID, prevVersion uint32, tombstone *types.Relationship) error {
	if err := storecontract.ValidateRelationshipHistorySnapshot(rid, tombstone); err != nil {
		return err
	}
	if err := bs.checkOpen(); err != nil {
		return err
	}
	id := rid.SnowflakeID()
	// Serialize tombstone OUTSIDE lock (B3: no I/O under write lock).
	tombData, err := marshalRelToBytes(tombstone)
	if err != nil {
		return fmt.Errorf("graph: marshal rel tombstone: %w", err)
	}
	histKey := storepkg.HistRelKey(id, uint64(prevVersion))

	bs.idxMu.Lock()
	r, err := bs.getRelLocked(rid)
	if err != nil {
		bs.idxMu.Unlock()
		return err
	}
	if err := storecontract.ValidateRelationshipReplacement(r, tombstone); err != nil {
		bs.idxMu.Unlock()
		return err
	}
	info := RelDeleteInfo{
		ID:      id,
		RelType: r.TypeToken().Value(),
		StartID: r.StartNodeID().SnowflakeID(),
		EndID:   r.EndNodeID().SnowflakeID(),
	}
	bs.deleteRelByInfo(info) // appends delete ops to pending under lock
	bs.appendOps(writeOp{opType: writeOpSet, key: histKey, value: tombData})
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

func marshalRelToBytes(r *types.Relationship) ([]byte, error) {
	return storepkg.MarshalRelWire(r)
}

func (bs *Store) PutRelVersion(rid types.RelID, version uint32, r *types.Relationship) error {
	if err := storecontract.ValidateRelationshipHistorySnapshot(rid, r); err != nil {
		return err
	}
	if err := bs.checkOpen(); err != nil {
		return err
	}
	id := rid.SnowflakeID()
	data, err := marshalRelToBytes(r)
	if err != nil {
		return fmt.Errorf("graph: marshal rel version: %w", err)
	}
	key := storepkg.HistRelKey(id, uint64(version))
	bs.appendOps(writeOp{opType: writeOpSet, key: key, value: data})
	if bs.syncWrites {
		return bs.flush()
	}
	return nil
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
		r, err := decodeRelWireForKey(w, id)
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
			decoded, err := decodeRelWireForKey(w, id)
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
		r, err := decodeRelWireForKey(w, expectedID)
		if err != nil {
			return nil, fmt.Errorf("graph: decode rel version: %w", err)
		}
		result = append(result, r.DeepCopy())
	}
	return result, nil
}

func (bs *Store) TruncateRelHistory(rid types.RelID, keepVersions int) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return err
	}
	id := rid.SnowflakeID()
	prefix := storepkg.HistRelPrefix(id)
	return bs.truncateHistoryByPrefix(prefix, keepVersions)
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
	sort.Slice(pendingSorted, func(i, j int) bool { return pendingSorted[i] < pendingSorted[j] })

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
