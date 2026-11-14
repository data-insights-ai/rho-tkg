package badger

import (
	"errors"
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// prepareAsOfIndexOpen runs before staged recovery or other open writes.
// A complete close header at the newest database version is the existing
// constant-size witness that rows and their proofs were fully applied. An
// unclean open invalidates all old proofs in O(1), including a torn apply
// whose marker and staging both disappeared. Replayed proof values still
// carry the old generation. Read-only or budgeted opens decline before this
// recovery write; the ordinary writable recovery route performs it.
func prepareAsOfIndexOpen(db *badgerv4.DB, format uint16, readOnly, hadWAL bool, t *storecontract.ReadTally) (uint16, error) {
	if format < storepkg.HistoryAsOfStoreFormatVersion {
		// An unsynced partial apply may lose its new format marker while an
		// index key survives. Probe one key, never infer absence from marker.
		present := false
		err := db.View(func(txn *badgerv4.Txn) error {
			it := txn.NewIterator(badgerv4.IteratorOptions{Prefix: []byte{storepkg.KeyHistoryAsOf}})
			defer it.Close()
			if err := t.Candidates(1); err != nil {
				return err
			}
			it.Rewind()
			present = it.Valid()
			if present {
				noteIndexEntryVisited()
			}
			return nil
		})
		if err != nil || !present {
			return format, err
		}
	}
	clean := false
	newest := db.MaxVersion() // open owns the database: no concurrent writer
	err := db.View(func(txn *badgerv4.Txn) error {
		if err := t.Candidates(1); err != nil {
			return err
		}
		noteAsOfIndexProbe()
		item, err := txn.Get(openImageHeaderKey)
		if errors.Is(err, badgerv4.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := t.Decoded(int(item.ValueSize())); err != nil {
			return err
		}
		return item.Value(func(raw []byte) error {
			h, ok := parseOpenImageHeader(raw)
			noteAsOfIndexDecode()
			clean = !hadWAL && ok && h.kind == openImageKindClose && item.Version() == newest
			return nil
		})
	})
	if err != nil || (clean && format >= storepkg.HistoryAsOfStoreFormatVersion) {
		return format, err
	}
	if err := t.RefuseUnsupported(); err != nil {
		return format, err
	}
	if readOnly {
		return format, fmt.Errorf("graph: history seek proofs need writable recovery after an unclean close: %w", badgerv4.ErrTruncateNeeded)
	}
	if err := db.Update(func(txn *badgerv4.Txn) error {
		if err := invalidateAsOfGeneration(txn); err != nil {
			return err
		}
		if format < storepkg.HistoryAsOfStoreFormatVersion {
			return txn.Set(storepkg.WireFormatVersionKey, formatMarkerValue(storepkg.HistoryAsOfStoreFormatVersion))
		}
		return nil
	}); err != nil {
		return format, fmt.Errorf("graph: invalidate history seek proofs at recovery: %w", err)
	}
	if err := syncAsOfRecovery(db); err != nil {
		return format, err
	}
	return max(format, storepkg.HistoryAsOfStoreFormatVersion), nil
}

var asOfRecoverySyncTestHook func() error

// Open and staged discard exclusively own the database. Badger rotates the
// active memtable before a write request, never after that request writes
// its entries. With no later request, Sync therefore covers the just-written
// generation. It must be durable before any new proof can use that value:
// losing it and incrementing again could otherwise reuse an old generation.
func syncAsOfRecovery(db *badgerv4.DB) error {
	if asOfRecoverySyncTestHook != nil {
		if err := asOfRecoverySyncTestHook(); err != nil {
			return err
		}
	}
	if err := db.Sync(); err != nil {
		return fmt.Errorf("graph: sync history seek recovery generation: %w", err)
	}
	return nil
}
