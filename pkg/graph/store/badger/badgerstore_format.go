package badger

import (
	"encoding/binary"
	"errors"
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// maxReadableStoreFormatVersion is the highest store-level marker this binary
// opens. A variable only so a test can play an older reader.
var maxReadableStoreFormatVersion uint16 = storepkg.MaxReadableStoreFormatVersion

// verifyAndStampWireFormatVersion enforces the store-level on-disk format
// contract at open, before any row is decoded:
//
//   - marker > maxReadableStoreFormatVersion: the directory was written by a
//     newer release — fail closed with ErrWireFormatVersionUnsupported rather
//     than misdecoding rows whose layout this binary does not know.
//   - StoreFormatVersion <= marker <= maxReadableStoreFormatVersion: open
//     normally and keep the marker (a store that persisted an Elements
//     definition stays at ElementsStoreFormatVersion).
//   - marker < StoreFormatVersion: open normally and (read-write only)
//     raise the marker to the current version, since rows written from now on
//     may carry it — an older binary must then refuse at open instead of
//     failing row-by-row mid-operation.
//   - marker absent: pre-versioning directory, equivalent to version 1 —
//     stamp the current version (read-write only).
//
// It returns the marker the directory carries after the call (0 when a
// read-only open left it absent). A marker that exists but cannot be parsed
// is corruption and fails closed (lesson 10: counters and registry metadata
// must fail closed when corrupt).
func verifyAndStampWireFormatVersion(db *badgerv4.DB, readOnly bool) (uint16, error) {
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
		return 0, fmt.Errorf("graph: wire format marker: %w", err)
	}
	if present && stored > maxReadableStoreFormatVersion {
		return 0, fmt.Errorf("graph: store written by a newer release (on-disk wire format v%d, this binary supports up to v%d): %w",
			stored, maxReadableStoreFormatVersion, storecontract.ErrWireFormatVersionUnsupported)
	}
	if readOnly || (present && stored >= storepkg.StoreFormatVersion) {
		return stored, nil
	}
	if err := db.Update(func(txn *badgerv4.Txn) error {
		return txn.Set(storepkg.WireFormatVersionKey, formatMarkerValue(storepkg.StoreFormatVersion))
	}); err != nil {
		return 0, fmt.Errorf("graph: stamp wire format marker: %w", err)
	}
	return storepkg.StoreFormatVersion, nil
}

func formatMarkerValue(v uint16) []byte {
	val := make([]byte, 2)
	binary.BigEndian.PutUint16(val, v)
	return val
}

// raiseFormatMarkerLocked raises the store-level marker to v in the pending
// write buffer (the same flush as the definitions that need it) when it is
// lower. Caller holds idxMu (write).
func (bs *Store) raiseFormatMarkerLocked(v uint16) {
	if bs.formatMarker >= v {
		return
	}
	bs.formatMarker = v
	bs.appendOps(writeOp{opType: writeOpSet, key: storepkg.WireFormatVersionKey, value: formatMarkerValue(v)})
}

// validateHistoryAnchorInterval rejects an out-of-range configured interval at New
// (0 = default 16, else must be in [2, 4096]). A value of 1 would make every version
// an anchor (deltas never used); absurdly large values bloat reconstruction reads.
func validateHistoryAnchorInterval(configured int) error {
	if configured == 0 {
		return nil
	}
	if configured < 2 || configured > 4096 {
		return fmt.Errorf("graph: HistoryAnchorInterval %d out of range [2, 4096]", configured)
	}
	return nil
}

// verifyAndStampHistoryAnchorInterval enforces that a store's delta history is always
// read at the interval it was written at (the interval is baked into the on-disk delta
// layout — a mismatch is a silent misread). Called at open after the wire-format
// marker:
//
//   - marker present and != interval: fail closed (ErrHistoryAnchorIntervalMismatch),
//     regardless of the current HistoryDeltaEncoding flag — existing deltas still need
//     the original interval to reconstruct.
//   - marker present and == interval: open normally.
//   - marker absent: stamp `interval` ONLY when delta encoding is enabled and the
//     store is writable (a store that never wrote deltas has no interval-dependent
//     layout to pin; a read-only open cannot stamp). Pre-marker delta stores were
//     always written at the default 16, so a first writable open at the default
//     stamps 16 and pins it thereafter.
//
// A marker that exists but cannot be parsed is corruption and fails closed.
func verifyAndStampHistoryAnchorInterval(db *badgerv4.DB, interval uint64, deltaEnabled, readOnly bool) error {
	var stored uint64
	present := false
	err := db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storepkg.HistoryAnchorIntervalKey)
		if errors.Is(err, badgerv4.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			if len(val) != 8 {
				return fmt.Errorf("marker is %d bytes, want 8", len(val))
			}
			stored = binary.BigEndian.Uint64(val)
			if stored == 0 {
				return fmt.Errorf("marker holds interval 0")
			}
			present = true
			return nil
		})
	})
	if err != nil {
		return fmt.Errorf("graph: history anchor interval marker: %w", err)
	}
	if present {
		if stored != interval {
			return fmt.Errorf("graph: store's delta history was written at anchor interval %d, configured %d: %w",
				stored, interval, storecontract.ErrHistoryAnchorIntervalMismatch)
		}
		return nil
	}
	if readOnly || !deltaEnabled {
		return nil
	}
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, interval)
	if err := db.Update(func(txn *badgerv4.Txn) error {
		return txn.Set(storepkg.HistoryAnchorIntervalKey, val)
	}); err != nil {
		return fmt.Errorf("graph: stamp history anchor interval marker: %w", err)
	}
	return nil
}
