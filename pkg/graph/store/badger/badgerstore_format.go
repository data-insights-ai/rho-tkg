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
