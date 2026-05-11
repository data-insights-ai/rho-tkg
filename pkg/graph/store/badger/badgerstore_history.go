// Package badgerstore provides Store — the persistent Store
// implementation backed by Badger v4. Used as a backend by pkg/graph
// directly and as a shard implementation inside internal/tieredstore.
package badger

import (
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	storecontract "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
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
	return nil
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

func historyIDMaxSeekKey(prefix byte) []byte {
	out := make([]byte, storepkg.SizeHistKey)
	for i := range out {
		out[i] = 0xff
	}
	out[0] = prefix
	return out
}

func (bs *Store) maxHistoryID(prefix byte) (snowflake.ID, error) {
	var maxID snowflake.ID

	bs.wbMu.Lock()
	for k, op := range bs.pending {
		if op.opType == writeOpSet && len(k) == storepkg.SizeHistKey && k[0] == prefix {
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
		it.Seek(seekKey)
		if !it.Valid() {
			return nil
		}
		key := it.Item().Key()
		if len(key) != storepkg.SizeHistKey || key[0] != prefix {
			return nil
		}
		id := storepkg.ParseIDFromKey(key, 1)
		if id > maxID {
			maxID = id
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
