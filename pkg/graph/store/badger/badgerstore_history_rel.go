package badger

import (
	"fmt"
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// Relationship-history methods (R5-F9 split out from badgerstore_history.go).

func (bs *Store) ReplaceRelWithHistory(current *types.Relationship, prevVersion uint32, prevState *types.Relationship) error {
	rid := current.InternalID()
	id := rid.SnowflakeID()

	// Serialize current state.
	w := storepkg.RelToWire(current)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal relationship: %w", err)
	}

	// Serialize history snapshot.
	hw := storepkg.RelToWire(prevState)
	histData, err := msgpack.Marshal(hw)
	if err != nil {
		return fmt.Errorf("graph: marshal rel version: %w", err)
	}

	bs.idxMu.Lock()

	if _, exists := bs.relIDs[rid]; !exists {
		bs.idxMu.Unlock()
		return ErrRelNotFound
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
	id := rid.SnowflakeID()
	// Serialize tombstone OUTSIDE lock (B3: no I/O under write lock).
	w := storepkg.RelToWire(tombstone)
	tombData, err := msgpack.Marshal(w)
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
	return msgpack.Marshal(storepkg.RelToWire(r))
}

func (bs *Store) PutRelVersion(rid types.RelID, version uint32, r *types.Relationship) error {
	id := rid.SnowflakeID()
	w := storepkg.RelToWire(r)
	data, err := msgpack.Marshal(w)
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
		r := storepkg.WireToRel(w)
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
			r = storepkg.WireToRel(w)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return r.DeepCopy(), nil
}

func (bs *Store) GetRelHistory(rid types.RelID) ([]*types.Relationship, error) {
	id := rid.SnowflakeID()
	prefix := storepkg.HistRelPrefix(id)
	return bs.getRelHistoryByPrefix(prefix)
}

func (bs *Store) getRelHistoryByPrefix(prefix []byte) ([]*types.Relationship, error) {
	prefixStr := string(prefix)

	entries := make(map[string][]byte)
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := string(item.KeyCopy(nil))
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
		if len(k) >= len(prefixStr) && k[:len(prefixStr)] == prefixStr {
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
		r := storepkg.WireToRel(w)
		result = append(result, r.DeepCopy())
	}
	return result, nil
}

func (bs *Store) TruncateRelHistory(rid types.RelID, keepVersions int) error {
	id := rid.SnowflakeID()
	prefix := storepkg.HistRelPrefix(id)
	return bs.truncateHistoryByPrefix(prefix, keepVersions)
}

func (bs *Store) ForEachRelHistoryID(fn func(types.RelID) bool) error {
	seen := make(map[snowflake.ID]struct{})

	// Phase 1: pending buffer.
	bs.wbMu.Lock()
	for k, op := range bs.pending {
		if op.opType == writeOpSet && len(k) >= storepkg.SizeHistKey && k[0] == storepkg.KeyHistRel {
			id := storepkg.ParseIDFromKey([]byte(k), 1)
			seen[id] = struct{}{}
		}
	}
	bs.wbMu.Unlock()

	// Emit pending IDs.
	for id := range seen {
		if !fn(types.RelID(id)) {
			return nil
		}
	}

	// Phase 2: Badger prefix scan.
	return bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		pfx := []byte{storepkg.KeyHistRel}
		for it.Seek(pfx); it.ValidForPrefix(pfx); it.Next() {
			key := it.Item().Key()
			if len(key) >= storepkg.SizeHistKey {
				id := storepkg.ParseIDFromKey(key, 1)
				if _, ok := seen[id]; ok {
					continue // already emitted
				}
				seen[id] = struct{}{}
				if !fn(types.RelID(id)) {
					return nil
				}
			}
		}
		return nil
	})
}

func (bs *Store) AllRelHistoryIDs() ([]types.RelID, error) {
	return bs.AllRelHistoryIDsFrom(types.RelID(0), 0)
}

func (bs *Store) AllRelHistoryIDsFrom(after types.RelID, limit int) ([]types.RelID, error) {
	afterRaw := snowflake.ID(after)

	pending := make(map[snowflake.ID]struct{})
	bs.wbMu.Lock()
	for k, op := range bs.pending {
		if op.opType == writeOpSet && len(k) >= storepkg.SizeHistKey && k[0] == storepkg.KeyHistRel {
			id := storepkg.ParseIDFromKey([]byte(k), 1)
			if id > afterRaw {
				pending[id] = struct{}{}
			}
		}
	}
	bs.wbMu.Unlock()

	pendingSorted := make([]snowflake.ID, 0, len(pending))
	for id := range pending {
		pendingSorted = append(pendingSorted, id)
	}
	sort.Slice(pendingSorted, func(i, j int) bool { return pendingSorted[i] < pendingSorted[j] })

	seekKey := historyIDSeekKey(storepkg.KeyHistRel, afterRaw)
	if seekKey == nil {
		return paginateRawRelIDs(pendingSorted, limit), nil
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
			if len(key) < storepkg.SizeHistKey {
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

func paginateRawRelIDs(ids []snowflake.ID, limit int) []types.RelID {
	if len(ids) == 0 {
		return nil
	}
	if limit > 0 && limit < len(ids) {
		ids = ids[:limit]
	}
	out := make([]types.RelID, len(ids))
	for i, id := range ids {
		out[i] = types.RelID(id)
	}
	return out
}
