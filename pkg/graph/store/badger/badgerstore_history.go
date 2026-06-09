// Package badgerstore provides Store — the persistent Store
// implementation backed by Badger v4. Used as a backend by pkg/graph
// directly and as a shard implementation inside internal/tieredstore.
package badger

import (
	"encoding/binary"
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// History helpers shared between node and rel paths (R5-F9 split).
// Per-entity history methods live in badgerstore_history_node.go and
// badgerstore_history_rel.go.

func (bs *Store) truncateHistoryByPrefix(prefix []byte, keepVersions int) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateHistoryRetention(keepVersions); err != nil {
		return err
	}
	prefixStr := string(prefix)

	// Collect all keys from Badger.
	var allKeys []string
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().KeyCopy(nil)
			if len(key) != storepkg.SizeHistKey {
				continue
			}
			allKeys = append(allKeys, string(key))
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Merge in pending buffer keys.
	keySet := make(map[string]struct{}, len(allKeys))
	for _, k := range allKeys {
		keySet[k] = struct{}{}
	}
	bs.wbMu.Lock()
	for k, op := range bs.pending {
		if len(k) == storepkg.SizeHistKey && k[:len(prefixStr)] == prefixStr {
			if op.opType == writeOpDelete {
				delete(keySet, k)
			} else {
				keySet[k] = struct{}{}
			}
		}
	}
	bs.wbMu.Unlock()

	if len(keySet) == 0 {
		return nil
	}

	sorted := make([]string, 0, len(keySet))
	for k := range keySet {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)

	var toDelete []string
	if keepVersions == 0 {
		toDelete = sorted
	} else if len(sorted) > keepVersions {
		toDelete = sorted[:len(sorted)-keepVersions]
	}

	if len(toDelete) == 0 {
		return nil
	}

	ops := make([]writeOp, len(toDelete))
	for i, k := range toDelete {
		ops[i] = writeOp{opType: writeOpDelete, key: []byte(k)}
	}
	bs.appendOps(ops...)
	return bs.flushIfSyncWrites()
}

func (bs *Store) trimHistoryFromPrefix(prefix []byte, minVersion uint32) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	prefixStr := string(prefix)
	startKey := historyVersionSeekKey(prefix, minVersion)

	var persisted []string
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(startKey); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().KeyCopy(nil)
			if len(key) != storepkg.SizeHistKey {
				continue
			}
			persisted = append(persisted, string(key))
		}
		return nil
	})
	if err != nil {
		return err
	}

	keySet := make(map[string]struct{}, len(persisted))
	for _, k := range persisted {
		keySet[k] = struct{}{}
	}
	bs.wbMu.Lock()
	for k := range bs.pending {
		if len(k) != storepkg.SizeHistKey || k[:len(prefixStr)] != prefixStr {
			continue
		}
		if historyVersionFromKey([]byte(k)) < uint64(minVersion) {
			continue
		}
		keySet[k] = struct{}{}
	}
	bs.wbMu.Unlock()

	if len(keySet) == 0 {
		return nil
	}
	ops := make([]writeOp, 0, len(keySet))
	for k := range keySet {
		ops = append(ops, writeOp{opType: writeOpDelete, key: []byte(k)})
	}
	bs.appendOps(ops...)
	return bs.flushIfSyncWrites()
}

func historyIDSeekKey(prefix byte, after snowflake.ID) []byte {
	// Treat zero (unset cursor) as "from the very beginning".
	if after == 0 {
		return []byte{prefix}
	}
	next := int64(after) + 1
	if next < int64(after) {
		// Overflow guard: after is already MaxInt64, no successor exists.
		return nil
	}
	out := make([]byte, 9)
	out[0] = prefix
	storepkg.PutUint64(out, 1, next)
	return out
}

func historyVersionSeekKey(prefix []byte, startVersion uint32) []byte {
	key := make([]byte, storepkg.SizeHistKey)
	copy(key, prefix)
	binary.BigEndian.PutUint64(key[9:], uint64(startVersion))
	return key
}

func historyVersionFromKey(key []byte) uint64 {
	return binary.BigEndian.Uint64(key[9:])
}

func historyIDMaxSeekKey(prefix byte) []byte {
	out := make([]byte, storepkg.SizeHistKey)
	for i := range out {
		out[i] = 0xff
	}
	out[0] = prefix
	return out
}

func (bs *Store) pendingHistoryVersionOverlay(prefix []byte, startVersion uint32) (map[string][]byte, map[string]struct{}) {
	prefixStr := string(prefix)
	entries := make(map[string][]byte)
	deletes := make(map[string]struct{})

	bs.wbMu.Lock()
	for k, op := range bs.pending {
		if len(k) != storepkg.SizeHistKey || k[:len(prefixStr)] != prefixStr {
			continue
		}
		if historyVersionFromKey([]byte(k)) < uint64(startVersion) {
			continue
		}
		if op.opType == writeOpDelete {
			delete(entries, k)
			deletes[k] = struct{}{}
			continue
		}
		cp := make([]byte, len(op.value))
		copy(cp, op.value)
		entries[k] = cp
		delete(deletes, k)
	}
	bs.wbMu.Unlock()

	return entries, deletes
}

func (bs *Store) maxHistoryID(prefix byte) (snowflake.ID, error) {
	var maxID snowflake.ID
	pendingDeletes := make(map[string]struct{})

	bs.wbMu.Lock()
	for k, op := range bs.pending {
		if len(k) != storepkg.SizeHistKey || k[0] != prefix {
			continue
		}
		if op.opType == writeOpDelete {
			pendingDeletes[k] = struct{}{}
			continue
		}
		if op.opType == writeOpSet {
			id := storepkg.ParseIDFromKey([]byte(k), 1)
			if id > maxID {
				maxID = id
			}
		}
	}
	bs.wbMu.Unlock()

	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()

		seekKey := historyIDMaxSeekKey(prefix)
		prefixBytes := []byte{prefix}
		for it.Seek(seekKey); it.ValidForPrefix(prefixBytes); it.Next() {
			key := it.Item().Key()
			if len(key) != storepkg.SizeHistKey {
				continue
			}
			if _, deleted := pendingDeletes[string(key)]; deleted {
				continue
			}
			id := storepkg.ParseIDFromKey(key, 1)
			if id > maxID {
				maxID = id
			}
			return nil
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return maxID, nil
}

func (bs *Store) pendingHistoryIDOverlay(prefix byte, after snowflake.ID) (map[snowflake.ID]struct{}, map[string]struct{}) {
	pendingSets := make(map[snowflake.ID]struct{})
	pendingDeletes := make(map[string]struct{})

	bs.wbMu.Lock()
	for k, op := range bs.pending {
		if len(k) != storepkg.SizeHistKey || k[0] != prefix {
			continue
		}
		if op.opType == writeOpDelete {
			pendingDeletes[k] = struct{}{}
			continue
		}
		id := storepkg.ParseIDFromKey([]byte(k), 1)
		if id > after {
			pendingSets[id] = struct{}{}
		}
	}
	bs.wbMu.Unlock()

	return pendingSets, pendingDeletes
}

func capForLimit(limit int) int {
	if limit > 0 && limit <= 1<<20 {
		return limit
	}
	return 0
}
