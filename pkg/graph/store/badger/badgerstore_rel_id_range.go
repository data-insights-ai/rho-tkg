package badger

import (
	"bytes"
	"fmt"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// RelIDRange returns the lowest and highest relationship ID the store holds a
// current row or a history row of, pending writes included; ok false when it
// holds neither. Every read of a relationship by ID reads one of those two
// keyspaces, so an ID outside [lo, hi] has nothing here. Four seeks, no scan:
// a tiered store seals it with a closed shard's counts (Sigma RH-S7) and skips
// the shard for such an ID. A pending delete's key still counts: the range is a
// superset, never narrower than what a read could find.
func (bs *Store) RelIDRange() (lo, hi snowflake.ID, ok bool, err error) {
	if err := bs.checkOpen(); err != nil {
		return 0, 0, false, err
	}
	widen := func(id snowflake.ID) {
		if !ok || id < lo {
			lo = id
		}
		if !ok || id > hi {
			hi = id
		}
		ok = true
	}
	bs.rangePending(func(k string, _ writeOp) {
		if len(k) >= 9 && (k[0] == storepkg.KeyRel || k[0] == storepkg.KeyHistRel) {
			widen(storepkg.ParseIDFromKey([]byte(k), 1))
		}
	})
	err = bs.view(func(txn *badgerv4.Txn) error {
		for _, prefix := range []byte{storepkg.KeyRel, storepkg.KeyHistRel} {
			p := []byte{prefix}
			for _, reverse := range []bool{false, true} {
				opts := badgerv4.DefaultIteratorOptions
				opts.PrefetchValues = false
				opts.Reverse = reverse
				opts.Prefix = p
				it := txn.NewIterator(opts)
				if reverse {
					it.Seek(append(bytes.Clone(p), bytes.Repeat([]byte{0xFF}, 17)...))
				} else {
					it.Seek(p)
				}
				if it.ValidForPrefix(p) && len(it.Item().Key()) >= 9 {
					widen(storepkg.ParseIDFromKey(it.Item().Key(), 1))
				}
				it.Close()
			}
		}
		// A staged flush not yet applied (FLUSHPART) holds the entry's own key
		// after its 8B staging sequence; normally there is none.
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = []byte{storepkg.KeyFlushStage}
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			k := it.Item().Key()
			if len(k) >= 18 && (k[9] == storepkg.KeyRel || k[9] == storepkg.KeyHistRel) {
				widen(storepkg.ParseIDFromKey(k, 10))
			}
		}
		return nil
	})
	if err != nil {
		return 0, 0, false, fmt.Errorf("graph: relationship ID range: %w", err)
	}
	return lo, hi, ok, nil
}
