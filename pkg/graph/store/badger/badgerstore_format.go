package badger

import (
	"encoding/binary"
	"errors"
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// verifyAndStampWireFormatVersion enforces the store-level on-disk format
// contract at open, before any row is decoded:
//
//   - marker > CurrentWireFormatVersion: the directory was written by a newer
//     release — fail closed with ErrWireFormatVersionUnsupported rather than
//     misdecoding rows whose layout this binary does not know.
//   - marker == CurrentWireFormatVersion: open normally.
//   - marker < CurrentWireFormatVersion: open normally and (read-write only)
//     raise the marker to the current version, since rows written from now on
//     may carry it — an older binary must then refuse at open instead of
//     failing row-by-row mid-operation.
//   - marker absent: pre-versioning directory, equivalent to version 1 —
//     stamp the current version (read-write only).
//
// A marker that exists but cannot be parsed is corruption and fails closed
// (lesson 10: counters and registry metadata must fail closed when corrupt).
func verifyAndStampWireFormatVersion(db *badgerv4.DB, readOnly bool) error {
	var stored uint16
	present := false
	err := db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storepkg.WireFormatVersionKey)
		if errors.Is(err, badgerv4.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			if len(val) != 2 {
				return fmt.Errorf("marker is %d bytes, want 2", len(val))
			}
			stored = binary.BigEndian.Uint16(val)
			if stored == 0 {
				return fmt.Errorf("marker holds version 0")
			}
			present = true
			return nil
		})
	})
	if err != nil {
		return fmt.Errorf("graph: wire format marker: %w", err)
	}
	if present && stored > storepkg.CurrentWireFormatVersion {
		return fmt.Errorf("graph: store written by a newer release (on-disk wire format v%d, this binary supports up to v%d): %w",
			stored, storepkg.CurrentWireFormatVersion, storecontract.ErrWireFormatVersionUnsupported)
	}
	if readOnly || (present && stored == storepkg.CurrentWireFormatVersion) {
		return nil
	}
	val := make([]byte, 2)
	binary.BigEndian.PutUint16(val, uint16(storepkg.CurrentWireFormatVersion))
	if err := db.Update(func(txn *badgerv4.Txn) error {
		return txn.Set(storepkg.WireFormatVersionKey, val)
	}); err != nil {
		return fmt.Errorf("graph: stamp wire format marker: %w", err)
	}
	return nil
}
