package badger

import (
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
)

// readRawMarker reads the wire-format marker straight from a closed Badger
// directory, bypassing the Store layer entirely.
func readRawMarker(t *testing.T, dir string) (uint16, bool) {
	t.Helper()
	db, err := badgerv4.Open(badgerv4.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		t.Fatalf("raw badger open: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("raw badger close: %v", err)
		}
	}()
	var v uint16
	found := false
	err = db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storepkg.WireFormatVersionKey)
		if errors.Is(err, badgerv4.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			if len(val) != 2 {
				t.Fatalf("marker on disk is %d bytes, want 2", len(val))
			}
			v = binary.BigEndian.Uint16(val)
			found = true
			return nil
		})
	})
	if err != nil {
		t.Fatalf("raw badger view: %v", err)
	}
	return v, found
}

// newDiskStoreWithNode creates an on-disk store containing one node and
// closes it, returning the directory and the node ID.
func newDiskStoreWithNode(t *testing.T, dir string, id int64) {
	t.Helper()
	bs, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	n := types.NewNode(types.NodeID(snowflake.ID(id)), 1, nil)
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// Open stamps the marker; deleting it (a pre-versioning directory) must keep
// the data readable and re-stamp on the next read-write open.
func TestWireFormatMarkerStampAndLegacyDirReopen(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	newDiskStoreWithNode(t, dir, 100)

	v, found := readRawMarker(t, dir)
	if !found || v != storepkg.CurrentWireFormatVersion {
		t.Fatalf("marker after first open = (%d, %v), want (%d, true)", v, found, storepkg.CurrentWireFormatVersion)
	}

	// Simulate a pre-versioning directory: strip the marker.
	updateRawBadgerDir(t, dir, func(txn *badgerv4.Txn) error {
		return txn.Delete(storepkg.WireFormatVersionKey)
	})

	bs, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen legacy dir: %v", err)
	}
	got, err := bs.GetNode(types.NodeID(snowflake.ID(100)))
	if err != nil {
		t.Fatalf("GetNode on legacy dir: %v", err)
	}
	if int64(got.ID().SnowflakeID()) != 100 || got.PrimaryLabelToken().Value() != 1 {
		t.Fatalf("legacy reopen mutated node: id=%v label=%v", got.ID(), got.PrimaryLabelToken())
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	v, found = readRawMarker(t, dir)
	if !found || v != storepkg.CurrentWireFormatVersion {
		t.Fatalf("marker after legacy reopen = (%d, %v), want re-stamped (%d, true)", v, found, storepkg.CurrentWireFormatVersion)
	}
}

// A directory stamped by a NEWER release must refuse to open with the
// sentinel — and restoring the marker must fully recover the store, proving
// the refusal was the marker check and not collateral damage.
func TestWireFormatMarkerFromNewerReleaseFailsOpen(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	newDiskStoreWithNode(t, dir, 200)

	futureVal := make([]byte, 2)
	binary.BigEndian.PutUint16(futureVal, storepkg.CurrentWireFormatVersion+8)
	updateRawBadgerDir(t, dir, func(txn *badgerv4.Txn) error {
		return txn.Set(storepkg.WireFormatVersionKey, futureVal)
	})

	bs, err := New(Config{Dir: dir})
	if err == nil {
		_ = bs.Close()
		t.Fatalf("New on future-stamped dir succeeded, want fail-closed")
	}
	if !errors.Is(err, storecontract.ErrWireFormatVersionUnsupported) {
		t.Fatalf("New error = %v, want errors.Is ErrWireFormatVersionUnsupported", err)
	}

	currentVal := make([]byte, 2)
	binary.BigEndian.PutUint16(currentVal, storepkg.CurrentWireFormatVersion)
	updateRawBadgerDir(t, dir, func(txn *badgerv4.Txn) error {
		return txn.Set(storepkg.WireFormatVersionKey, currentVal)
	})

	bs, err = New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen after marker restore: %v", err)
	}
	defer bs.Close()
	if _, err := bs.GetNode(types.NodeID(snowflake.ID(200))); err != nil {
		t.Fatalf("node unreadable after marker restore: %v", err)
	}
}

// A marker that exists but cannot be parsed is corruption: fail closed, do
// not guess.
func TestWireFormatMarkerCorruptFailsOpen(t *testing.T) {
	t.Parallel()

	for name, val := range map[string][]byte{
		"wrong-length": {1, 2, 3},
		"version-zero": {0, 0},
	} {
		dir := t.TempDir()
		newDiskStoreWithNode(t, dir, 300)
		updateRawBadgerDir(t, dir, func(txn *badgerv4.Txn) error {
			return txn.Set(storepkg.WireFormatVersionKey, val)
		})
		bs, err := New(Config{Dir: dir})
		if err == nil {
			_ = bs.Close()
			t.Fatalf("%s: New succeeded on corrupt marker, want fail-closed", name)
		}
		if !strings.Contains(err.Error(), "wire format marker") {
			t.Fatalf("%s: error %v does not identify the marker as the cause", name, err)
		}
	}
}

// A single row claiming a future per-row version must make open fail closed —
// silently dropping the row and reporting one node fewer would be data loss
// masquerading as success. (The store-level marker normally prevents this
// scenario; the per-row check is defense in depth for hand-planted or
// partially-upgraded data.) Node and Relationship scans are structural
// mirrors and must both enforce it.
func TestWireFormatFutureRowFailsOpenNotSilentDrop(t *testing.T) {
	t.Parallel()

	t.Run("node", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		bs, err := New(Config{Dir: dir})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		for _, id := range []int64{401, 402} {
			if err := bs.PutNode(types.NewNode(types.NodeID(snowflake.ID(id)), 1, nil)); err != nil {
				t.Fatalf("PutNode(%d): %v", id, err)
			}
		}
		if err := bs.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		// Overwrite one row with a future-format encoding (custom encoder
		// emits whatever FormatVersion the wire struct carries).
		hostile, err := msgpack.Marshal(storepkg.NodeWire{FormatVersion: 99, ID: 401, PrimaryLabel: 1})
		if err != nil {
			t.Fatalf("marshal hostile row: %v", err)
		}
		updateRawBadgerDir(t, dir, func(txn *badgerv4.Txn) error {
			return txn.Set(storepkg.NodeKey(snowflake.ID(401)), hostile)
		})

		reopened, err := New(Config{Dir: dir})
		if err == nil {
			count, countErr := reopened.NodeCount()
			_ = reopened.Close()
			t.Fatalf("New succeeded over a future-format node row (NodeCount=%d, err=%v); want open to fail closed", count, countErr)
		}
		if !errors.Is(err, storecontract.ErrWireFormatVersionUnsupported) {
			t.Fatalf("New error = %v, want errors.Is ErrWireFormatVersionUnsupported", err)
		}
	})

	t.Run("relationship", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		bs, err := New(Config{Dir: dir})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		for _, id := range []int64{411, 412} {
			if err := bs.PutNode(types.NewNode(types.NodeID(snowflake.ID(id)), 1, nil)); err != nil {
				t.Fatalf("PutNode(%d): %v", id, err)
			}
		}
		rel := types.NewRelationship(types.RelID(snowflake.ID(950)), 3, types.NodeID(snowflake.ID(411)), types.NodeID(snowflake.ID(412)))
		if err := bs.PutRelationship(rel); err != nil {
			t.Fatalf("PutRelationship: %v", err)
		}
		if err := bs.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		hostile, err := msgpack.Marshal(storepkg.RelWire{FormatVersion: 99, ID: 950, RelType: 3, StartID: 411, EndID: 412})
		if err != nil {
			t.Fatalf("marshal hostile rel row: %v", err)
		}
		updateRawBadgerDir(t, dir, func(txn *badgerv4.Txn) error {
			return txn.Set(storepkg.RelKey(snowflake.ID(950)), hostile)
		})

		reopened, err := New(Config{Dir: dir})
		if err == nil {
			count, countErr := reopened.RelationshipCount()
			_ = reopened.Close()
			t.Fatalf("New succeeded over a future-format rel row (RelCount=%d, err=%v); want open to fail closed", count, countErr)
		}
		if !errors.Is(err, storecontract.ErrWireFormatVersionUnsupported) {
			t.Fatalf("New error = %v, want errors.Is ErrWireFormatVersionUnsupported", err)
		}
	})
}
