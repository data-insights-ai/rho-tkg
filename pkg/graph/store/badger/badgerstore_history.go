// Package badgerstore provides Store — the persistent Store
// implementation backed by Badger v4. Used as a backend by pkg/graph
// directly and as a shard implementation inside internal/tieredstore.
package badger

import (
	"encoding/binary"
	"fmt"
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// History helpers shared between node and rel paths (R5-F9 split).
// Per-entity history methods live in badgerstore_history_node.go and
// badgerstore_history_rel.go.

func (bs *Store) truncateHistoryByPrefix(prefix []byte, keepVersions int, logTag storecontract.ChangeTag, logPayload []byte) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateHistoryRetention(keepVersions); err != nil {
		return err
	}
	// Snapshot the write-buffer overlay BEFORE the Badger scan. A concurrent
	// flush() commits a parked version to Badger and THEN clears `flushing`, so a
	// scan-first reader that reads Badger at Ts and the overlay at Tr > Ts would
	// miss a version committed in (Ts, Tr) — invisible to both the older Badger
	// snapshot and the cleared overlay — under-counting the retention set (the
	// version escapes deletion / distorts the keepVersions window). Capturing the
	// overlay first closes the window. See lesson 64.
	overlayEntries, overlayDeletes := bs.pendingHistoryVersionOverlay(prefix, 0)

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
	keySet := make(map[string]struct{}, len(allKeys)+len(overlayEntries))
	for _, k := range allKeys {
		keySet[k] = struct{}{}
	}
	// Apply the pre-scan overlay snapshot (strictly newer than committed Badger):
	// a set adds a version to the retention set, a delete removes it.
	for k := range overlayEntries {
		keySet[k] = struct{}{}
	}
	for k := range overlayDeletes {
		delete(keySet, k)
	}

	if len(keySet) == 0 {
		return nil
	}

	sorted := make([]string, 0, len(keySet))
	for k := range keySet {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)

	var toDelete, kept []string
	if keepVersions == 0 {
		toDelete = sorted
	} else if len(sorted) > keepVersions {
		toDelete = sorted[:len(sorted)-keepVersions]
		kept = sorted[len(sorted)-keepVersions:]
	} else {
		kept = sorted
	}

	if len(toDelete) == 0 {
		return nil
	}

	// Anchor-safety: rewrite any KEPT delta whose interval anchor is being
	// deleted into a full snapshot BEFORE the deletes commit, in the same batch.
	rematerOps, err := bs.rematerializeOrphanedDeltas(prefix, kept)
	if err != nil {
		return fmt.Errorf("graph: re-anchor kept history deltas: %w", err)
	}

	ops := make([]writeOp, 0, len(toDelete)+len(rematerOps))
	ops = append(ops, rematerOps...)
	for _, k := range toDelete {
		ops = append(ops, writeOp{opType: writeOpDelete, key: []byte(k)})
	}
	// Emit the truncation record atomically with the delete ops (this helper
	// holds no idxMu). The record is produced only when something is actually
	// deleted — the no-op early returns above emit nothing.
	bs.appendOpsLogged(logTag, logPayload, ops...)
	return bs.flushIfNeeded()
}

// historyTruncateDeleteKeys computes the set of history keys that a
// keepVersions truncation would delete for prefix (keeping the newest
// keepVersions versions), merging the pre-scan write-buffer overlay exactly as
// truncateHistoryByPrefix does (see the lesson-64 note there). It performs no
// writes — the caller decides how to commit the deletes.
func (bs *Store) historyTruncateDeleteKeys(prefix []byte, keepVersions int) ([]string, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateHistoryRetention(keepVersions); err != nil {
		return nil, err
	}
	overlayEntries, overlayDeletes := bs.pendingHistoryVersionOverlay(prefix, 0)

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
		return nil, err
	}

	keySet := make(map[string]struct{}, len(allKeys)+len(overlayEntries))
	for _, k := range allKeys {
		keySet[k] = struct{}{}
	}
	for k := range overlayEntries {
		keySet[k] = struct{}{}
	}
	for k := range overlayDeletes {
		delete(keySet, k)
	}
	if len(keySet) == 0 {
		return nil, nil
	}

	sorted := make([]string, 0, len(keySet))
	for k := range keySet {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)

	if keepVersions == 0 {
		return sorted, nil
	}
	if len(sorted) > keepVersions {
		return sorted[:len(sorted)-keepVersions], nil
	}
	return nil, nil
}

// CompactNodeHistory implements store.HistoryCompactionCapability: it trims the
// oldest node history versions (keeping the newest keepVersions) AND applies the
// compaction meta writes (the per-entity stub) in the SAME WriteBatch, so a crash
// never separates the trim from its stub. The graph watermark is routed
// separately by the graph layer (store-level MetaSet). No change-log record is
// emitted (compaction over a change-log-enabled graph is refused a layer up).
func (bs *Store) CompactNodeHistory(nid types.NodeID, keepVersions int, metaWrites []storecontract.MetaWrite) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return err
	}
	prefix := storepkg.HistNodePrefix(nid.SnowflakeID())
	return bs.compactHistoryByPrefix(prefix, keepVersions, metaWrites)
}

// CompactRelHistory is the relationship mirror of CompactNodeHistory.
func (bs *Store) CompactRelHistory(rid types.RelID, keepVersions int, metaWrites []storecontract.MetaWrite) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return err
	}
	prefix := storepkg.HistRelPrefix(rid.SnowflakeID())
	return bs.compactHistoryByPrefix(prefix, keepVersions, metaWrites)
}

// compactHistoryByPrefix builds the history-delete ops + the compaction meta
// writes and commits them together in one WriteBatch (forced synchronous flush).
// appendOps installs all ops under a single wbMu window, so a concurrent flush
// snapshot sees either all of them or none — the trim and the stub are atomic.
func (bs *Store) compactHistoryByPrefix(prefix []byte, keepVersions int, metaWrites []storecontract.MetaWrite) error {
	deleteKeys, err := bs.historyTruncateDeleteKeys(prefix, keepVersions)
	if err != nil {
		return err
	}
	ops := make([]writeOp, 0, len(deleteKeys)+len(metaWrites))
	for _, k := range deleteKeys {
		ops = append(ops, writeOp{opType: writeOpDelete, key: []byte(k)})
	}
	for _, w := range metaWrites {
		mk := storepkg.MetaKey(w.Key)
		if w.Value == nil {
			ops = append(ops, writeOp{opType: writeOpDelete, key: mk})
			continue
		}
		ops = append(ops, writeOp{opType: writeOpSet, key: mk, value: append([]byte(nil), w.Value...)})
	}
	if len(ops) == 0 {
		return nil
	}
	bs.appendOps(ops...)
	return bs.flush()
}

func (bs *Store) trimHistoryFromPrefix(prefix []byte, minVersion uint32, logTag storecontract.ChangeTag, logPayload []byte) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	startKey := historyVersionSeekKey(prefix, minVersion)

	// Snapshot the overlay (versions >= minVersion) BEFORE the Badger scan to
	// close the commit-window drop — see truncateHistoryByPrefix / lesson 64.
	overlayEntries, overlayDeletes := bs.pendingHistoryVersionOverlay(prefix, minVersion)

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

	keySet := make(map[string]struct{}, len(persisted)+len(overlayEntries)+len(overlayDeletes))
	for _, k := range persisted {
		keySet[k] = struct{}{}
	}
	// Every buffered key at or past minVersion is a trim target (a parked delete
	// key is already being removed — folding it in is idempotent, matching the
	// prior rangePending behavior that added regardless of op type).
	for k := range overlayEntries {
		keySet[k] = struct{}{}
	}
	for k := range overlayDeletes {
		keySet[k] = struct{}{}
	}

	if len(keySet) == 0 {
		return nil
	}
	ops := make([]writeOp, 0, len(keySet))
	for k := range keySet {
		ops = append(ops, writeOp{opType: writeOpDelete, key: []byte(k)})
	}
	bs.appendOpsLogged(logTag, logPayload, ops...)
	return bs.flushIfNeeded()
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

	bs.rangePending(func(k string, op writeOp) {
		if len(k) != storepkg.SizeHistKey || k[:len(prefixStr)] != prefixStr {
			return
		}
		if historyVersionFromKey([]byte(k)) < uint64(startVersion) {
			return
		}
		if op.opType == writeOpDelete {
			delete(entries, k)
			deletes[k] = struct{}{}
			return
		}
		cp := make([]byte, len(op.value))
		copy(cp, op.value)
		entries[k] = cp
		delete(deletes, k)
	})

	return entries, deletes
}

func (bs *Store) maxHistoryID(prefix byte) (snowflake.ID, error) {
	var maxID snowflake.ID

	// Consult BOTH buffers; ignoring `flushing` under-reported the max while
	// AllNodeHistoryIDsFrom (which goes through pendingHistoryIDOverlay) did
	// consult it — an inconsistency between the two doors. Resolve set-vs-delete
	// PER KEY the same way pendingHistoryIDOverlay does (rangePending visits
	// flushing before pending, so a newer op wins): a running max cannot be
	// un-bumped, so track the surviving SET keys and a key DELETE removes its
	// candidate — otherwise a flushing SET masked by a pending DELETE would
	// inflate the max above what AllNodeHistoryIDs reports (the same id).
	pendingSets := make(map[string]struct{})
	pendingDeletes := make(map[string]struct{})
	bs.rangePending(func(k string, op writeOp) {
		if len(k) != storepkg.SizeHistKey || k[0] != prefix {
			return
		}
		if op.opType == writeOpDelete {
			pendingDeletes[k] = struct{}{}
			delete(pendingSets, k)
			return
		}
		if op.opType == writeOpSet {
			pendingSets[k] = struct{}{}
			delete(pendingDeletes, k)
		}
	})
	for k := range pendingSets {
		id := storepkg.ParseIDFromKey([]byte(k), 1)
		if id > maxID {
			maxID = id
		}
	}

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

	// rangePending visits flushing (older, in-flight) then pending (newer), so a
	// newer op for the same key is applied last and wins. Resolve set-vs-delete
	// per key so an id can never end up in BOTH result maps.
	bs.rangePending(func(k string, op writeOp) {
		if len(k) != storepkg.SizeHistKey || k[0] != prefix {
			return
		}
		if op.opType == writeOpDelete {
			pendingDeletes[k] = struct{}{}
			id := storepkg.ParseIDFromKey([]byte(k), 1)
			delete(pendingSets, id)
			return
		}
		id := storepkg.ParseIDFromKey([]byte(k), 1)
		delete(pendingDeletes, k)
		if id > after {
			pendingSets[id] = struct{}{}
		}
	})

	return pendingSets, pendingDeletes
}

// rangePending invokes fn for every buffered write op, OLDEST-first: the
// in-flight `flushing` snapshot (being committed by a concurrent flush) before
// the current `pending` entries. Visiting pending last means a newer op for the
// same key wins under each reader's last-write-wins overlay logic. Held under
// wbMu for the call; fn must not re-enter the write buffer.
func (bs *Store) rangePending(fn func(k string, op writeOp)) {
	bs.wbMu.Lock()
	defer bs.wbMu.Unlock()
	for k, op := range bs.flushing {
		fn(k, op)
	}
	for k, op := range bs.pending {
		fn(k, op)
	}
}

// lookupPending resolves a single buffered key, pending (newer) taking
// precedence over flushing (older, in-flight). Returns (op, true) when buffered.
func (bs *Store) lookupPending(key string) (writeOp, bool) {
	bs.wbMu.Lock()
	defer bs.wbMu.Unlock()
	if op, ok := bs.pending[key]; ok {
		return op, true
	}
	op, ok := bs.flushing[key]
	return op, ok
}

func capForLimit(limit int) int {
	if limit > 0 && limit <= 1<<20 {
		return limit
	}
	return 0
}
