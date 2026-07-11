// Package badgerstore provides Store — the persistent Store
// implementation backed by Badger v4. Used as a backend by pkg/graph
// directly and as a shard implementation inside internal/tieredstore.
package badger

import (
	"encoding/binary"
	"fmt"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// Disk-resident raw interval-entry log for the temporal index (opt-in via
// Config.TemporalIndexOnDisk). See storeutil/temporal_index_key.go for the
// key-layout rationale and the design distinction from the
// Label/Adjacency/PropertyIndexOnDisk family: this keyspace NEVER answers
// live reads (the maxTo-augmented TemporalIndex always stays resident in
// RAM — its stabbing/overlap queries have no on-disk analogue). It exists
// solely to make loadIndexesScan's rebuild-at-open cheap: a compact prefix
// iteration per label instead of a full node fetch+decode per entity.
//
// Maintained alongside — never instead of — the RAM
// indexpkg.AddNodeToTemporalIndexes / RemoveNodeFromTemporalIndexes calls at
// every node write site (badgerstore_node.go, badgerstore_node_batch.go,
// badgerstore_history_node.go): the RAM structure remains the sole
// authority for reads at runtime. A row exists for (labelToken, id) iff
// TemporalIndexOnDisk is enabled AND a TemporalIndex definition currently
// covers labelToken AND the node currently carries labelToken.

// maintainTemporalIndexDiskAdd returns the write ops that record n's
// (from, to) bounds under the on-disk keyspace for every label n carries
// that has a live TemporalIndex definition. No-op (nil) when
// TemporalIndexOnDisk is off or no temporal index is defined. Caller must
// hold idxMu (every call site already does, alongside
// indexpkg.AddNodeToTemporalIndexes) and is responsible for merging the
// returned ops into the SAME appendOps call as the entity row (crash
// consistency — mirrors maintainPropertyIndexesAdd).
func (bs *Store) maintainTemporalIndexDiskAdd(n *types.Node, id snowflake.ID) []writeOp {
	if !bs.temporalIdxOnDisk || len(bs.temporalIndexes) == 0 {
		return nil
	}
	from, to := indexpkg.NodeTemporalBounds(id, n.Temporal())
	var ops []writeOp
	labelCount := n.LabelTokenCount()
	for i := 0; i < labelCount; i++ {
		tok := n.LabelTokenRawAt(i)
		if _, ok := bs.temporalIndexes[tok]; !ok {
			continue
		}
		ops = append(ops, writeOp{
			opType: writeOpSet,
			key:    storepkg.TemporalIndexEntryKey(tok, from, id),
			value:  storepkg.TemporalIndexEntryValue(to),
		})
	}
	return ops
}

// maintainTemporalIndexDiskRemove is the Remove counterpart of
// maintainTemporalIndexDiskAdd — see its doc comment. n must be the node's
// state BEFORE the mutation (the same "old" snapshot passed to
// indexpkg.RemoveNodeFromTemporalIndexes) so the deleted key's FROM
// component matches the row that was actually written.
func (bs *Store) maintainTemporalIndexDiskRemove(n *types.Node, id snowflake.ID) []writeOp {
	if !bs.temporalIdxOnDisk || len(bs.temporalIndexes) == 0 {
		return nil
	}
	from, _ := indexpkg.NodeTemporalBounds(id, n.Temporal())
	var ops []writeOp
	labelCount := n.LabelTokenCount()
	for i := 0; i < labelCount; i++ {
		tok := n.LabelTokenRawAt(i)
		if _, ok := bs.temporalIndexes[tok]; !ok {
			continue
		}
		ops = append(ops, writeOp{opType: writeOpDelete, key: storepkg.TemporalIndexEntryKey(tok, from, id)})
	}
	return ops
}

// maintainTemporalIndexDiskPurge is the corruption-path brute-force fallback
// (node data unavailable, so labels/from can't be computed) — mirrors
// maintainPropertyIndexesPurge. For every label token currently carrying a
// TemporalIndex definition, scans its on-disk sub-keyspace for a row whose
// trailing node ID matches id and deletes it. O(index size) per label —
// corruption-only path, same complexity class as the RAM-mode brute-force
// sweep it mirrors (indexpkg.PurgeNodeFromAllTemporalIndexes). Caller holds idxMu.
func (bs *Store) maintainTemporalIndexDiskPurge(id snowflake.ID) []writeOp {
	if !bs.temporalIdxOnDisk || len(bs.temporalIndexes) == 0 {
		return nil
	}
	var ops []writeOp
	// Purge any unflushed SET for this ID under a tracked label FIRST — before
	// the Badger scan. A concurrent flush() commits a parked entry and THEN
	// clears `flushing`, so a scan-first purge that read Badger before the
	// commit and the overlay after `flushing` was cleared would emit NO delete
	// for that key and ORPHAN the just-committed entry. Capturing the overlay
	// first closes the window; a key seen by both passes yields a duplicate
	// delete op, which coalesces harmlessly. See lesson 64.
	bs.rangePending(func(k string, op writeOp) {
		kb := []byte(k)
		if len(kb) != storepkg.SizeTemporalIndexEntryKey || kb[0] != storepkg.KeyTemporalIndex {
			return
		}
		tok := binary.BigEndian.Uint16(kb[1:3])
		if _, ok := bs.temporalIndexes[tok]; !ok {
			return
		}
		if op.opType != writeOpSet {
			return
		}
		if storepkg.TemporalIndexNodeIDFromKey(kb) == id {
			ops = append(ops, writeOp{opType: writeOpDelete, key: kb})
		}
	})
	_ = bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		for tok := range bs.temporalIndexes {
			prefix := storepkg.TemporalIndexTokenPrefix(tok)
			it := txn.NewIterator(opts)
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				key := it.Item().KeyCopy(nil)
				if storepkg.TemporalIndexNodeIDFromKey(key) == id {
					ops = append(ops, writeOp{opType: writeOpDelete, key: key})
				}
			}
			it.Close()
		}
		return nil
	})
	return ops
}

// purgeTemporalIndexDiskEntriesLocked deletes every on-disk row for
// labelToken's temporal index — both persisted (via a Badger scan) and any
// unflushed SET still sitting in the write buffer. Called from
// DropTemporalIndex only when disk mode is on. Unlike the property-index
// keyspace (where one PropertyKey's rows are physically shared across
// labels), a TemporalIndex definition is exclusively keyed by labelToken —
// only one definition can ever exist per label (ErrTemporalIndexExists
// guards a second CreateTemporalIndex on the same token) — so purging the
// whole labelToken prefix can never corrupt a sibling definition. Caller
// holds idxMu.
func (bs *Store) purgeTemporalIndexDiskEntriesLocked(labelToken uint16) ([]writeOp, error) {
	if !bs.temporalIdxOnDisk {
		return nil, nil
	}
	prefix := storepkg.TemporalIndexTokenPrefix(labelToken)
	prefixStr := string(prefix)
	var ops []writeOp
	// Snapshot parked SETs for this label BEFORE the Badger scan (lesson 64 —
	// see maintainTemporalIndexDiskPurge's doc comment for the race it closes).
	bs.rangePending(func(k string, op writeOp) {
		if len(k) < len(prefixStr) || k[:len(prefixStr)] != prefixStr {
			return
		}
		if op.opType == writeOpSet {
			keyCopy := make([]byte, len(k))
			copy(keyCopy, k)
			ops = append(ops, writeOp{opType: writeOpDelete, key: keyCopy})
		}
	})
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			ops = append(ops, writeOp{opType: writeOpDelete, key: it.Item().KeyCopy(nil)})
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("graph: drop temporal index: purge disk entries: %w", err)
	}
	return ops, nil
}

// commitTemporalIndexOnDiskBackfill writes the one-time rebuild-on-enable
// backfill plus the built marker in a single WriteBatch, so a crash
// mid-backfill leaves either NO rows and no marker (retried on the next
// open) or ALL rows and the marker (never a half-built keyspace a later
// open trusts as complete). Mirrors commitPropertyIndexOnDiskBackfill.
func (bs *Store) commitTemporalIndexOnDiskBackfill(ops []writeOp) error {
	wb := bs.db.NewWriteBatch()
	defer wb.Cancel()
	for _, op := range ops {
		if err := wb.SetEntry(badgerv4.NewEntry(op.key, op.value)); err != nil {
			return fmt.Errorf("graph: temporal-index-on-disk backfill: %w", err)
		}
	}
	if err := wb.SetEntry(badgerv4.NewEntry(storepkg.TemporalIndexOnDiskBuiltKey, []byte{1})); err != nil {
		return fmt.Errorf("graph: temporal-index-on-disk backfill: mark built: %w", err)
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("graph: temporal-index-on-disk backfill: %w", err)
	}
	return nil
}
