package badger

import (
	"errors"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// A complete history/identity purge appends this AFTER its history deletes:
// their ordinary mutation hooks invalidate the proof, then the purge removes
// it. Aliases let each deleted version remove its seek key without a scan.
func asOfProofDelete(kind byte, id snowflake.ID) writeOp {
	return writeOp{opType: writeOpDelete, key: storepkg.HistoryAsOfProofKey(kind, id)}
}

// A full identity/history purge also removes orphan seek/alias entries from
// a torn apply whose history row did not survive. Walk only this entity's two
// index prefixes, outside wbMu; capture its listed buffered keys before the
// transaction so a concurrent flush cannot drop them from both views.
func (bs *Store) asOfPurgeOps(prefix []byte) ([]writeOp, error) {
	kind, id := prefix[0], storepkg.ParseIDFromKey(prefix, 1)
	proof := asOfProofDelete(kind, id)
	prefixes := [][]byte{storepkg.HistoryAsOfSeekKey(kind, id, 0, 0)[:10], storepkg.HistoryAsOfAliasKey(kind, id, 0)[:10]}
	var ops []writeOp
	proofSeen, proofExists := false, false
	bs.wbMu.Lock()
	for _, source := range []map[string]writeOp{bs.pending, bs.flushing} {
		if op, ok := source[string(proof.key)]; ok && !proofSeen {
			proofSeen, proofExists = true, op.opType != writeOpDelete
		}
	}
	for _, p := range prefixes {
		for _, listed := range []map[string][]string{bs.pendingByPrefix, bs.flushingByPrefix} {
			for _, key := range listed[string(p)] {
				ops = append(ops, writeOp{opType: writeOpDelete, key: []byte(key)})
			}
		}
	}
	bs.wbMu.Unlock()
	err := bs.view(func(txn *badgerv4.Txn) error {
		if !proofSeen {
			_, err := txn.Get(proof.key)
			if err != nil && !errors.Is(err, badgerv4.ErrKeyNotFound) {
				return err
			}
			proofExists = err == nil
		}
		for _, p := range prefixes {
			func() {
				it := txn.NewIterator(badgerv4.IteratorOptions{Prefix: p})
				defer it.Close()
				for it.Rewind(); it.ValidForPrefix(p); it.Next() {
					ops = append(ops, writeOp{opType: writeOpDelete, key: it.Item().KeyCopy(nil)})
				}
			}()
		}
		return nil
	})
	if proofExists || len(ops) > 0 {
		ops = append(ops, proof)
	}
	return ops, err
}

// An already empty history can retain invalid/orphan index state. A clean
// empty prefix remains a no-op; a full purge removes that residue too.
func (bs *Store) dropEmptyAsOfIndex(prefix []byte) error {
	ops, err := bs.asOfPurgeOps(prefix)
	if err != nil || len(ops) == 0 {
		return err
	}
	bs.appendOps(ops...)
	return bs.flushIfNeeded()
}

// Full relationship purges use the same preflight whether the current row
// is live or already absent. Adjacency can outlive a torn current-row delete;
// its descriptor still identifies the history and index that must disappear.
// Caller holds idxMu.
func (bs *Store) fullHistoryPurgeOps(prefix []byte) ([]writeOp, error) {
	keys, _, err := bs.historyTruncateDeleteKeys(prefix, 0)
	if err != nil {
		return nil, err
	}
	indexOps, err := bs.asOfPurgeOps(prefix)
	if err != nil {
		return nil, err
	}
	ops := make([]writeOp, 0, len(keys)+len(indexOps))
	for _, key := range keys {
		ops = append(ops, writeOp{opType: writeOpDelete, key: []byte(key)})
	}
	return append(ops, indexOps...), nil
}
