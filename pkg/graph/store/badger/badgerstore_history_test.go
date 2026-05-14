package badger

import (
	"errors"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/storeutil"
	storecontract "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

// ─── Store: Node version history ──────────────────────────────────────

func newSlowFlushBadgerStore(t *testing.T) *Store {
	t.Helper()
	bs, err := New(Config{InMemory: true, FlushInterval: time.Hour})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	return bs
}

func TestBadgerStorePutGetNodeVersion(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	_ = n.SetProperty("name", "Alice")

	if err := bs.PutNodeVersion(types.NodeID(1), 0, n); err != nil {
		t.Fatalf("PutNodeVersion: %v", err)
	}

	got, err := bs.GetNodeVersion(types.NodeID(1), 0)
	if err != nil {
		t.Fatalf("GetNodeVersion: %v", err)
	}
	if int64(got.ID()) != 1 {
		t.Fatal("version snapshot has wrong ID")
	}
	v, ok := got.GetProperty("name")
	if !ok || v != "Alice" {
		t.Fatalf("property mismatch: got %v", v)
	}

	// Cache isolation: mutate returned copy.
	_ = got.SetProperty("name", "mutated")
	got2, _ := bs.GetNodeVersion(types.NodeID(1), 0)
	v2, _ := got2.GetProperty("name")
	if v2 != "Alice" {
		t.Fatalf("GetNodeVersion returned shared pointer: got %v, want Alice", v2)
	}
}

func TestBadgerStoreGetNodeVersionNotFound(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	_, err := bs.GetNodeVersion(types.NodeID(1), 0)
	if !errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("expected ErrVersionNotFound, got %v", err)
	}
}

func TestBadgerStoreGetNodeVersionRejectsSemanticWireCorruption(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	if err := bs.PutNodeVersion(types.NodeID(1), 0, n); err != nil {
		t.Fatalf("PutNodeVersion: %v", err)
	}
	corruptNodeHistoryWireAfterFlush(t, bs, 0, storepkg.NodeWire{ID: 1, PrimaryLabel: 0})

	defer func() {
		if rec := recover(); rec != nil {
			t.Fatalf("GetNodeVersion panicked on semantically corrupt node history wire: %v", rec)
		}
	}()
	_, err := bs.GetNodeVersion(types.NodeID(1), 0)
	if err == nil {
		t.Fatal("GetNodeVersion should return error for semantically corrupt node history wire")
	}
	if errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("GetNodeVersion returned ErrVersionNotFound for corrupt node history wire: %v", err)
	}
}

func TestBadgerStoreGetNodeHistoryRejectsSemanticWireCorruption(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	if err := bs.PutNodeVersion(types.NodeID(1), 0, n); err != nil {
		t.Fatalf("PutNodeVersion: %v", err)
	}
	corruptNodeHistoryWireAfterFlush(t, bs, 0, storepkg.NodeWire{ID: 1, PrimaryLabel: 0})

	defer func() {
		if rec := recover(); rec != nil {
			t.Fatalf("GetNodeHistory panicked on semantically corrupt node history wire: %v", rec)
		}
	}()
	_, err := bs.GetNodeHistory(types.NodeID(1))
	if err == nil {
		t.Fatal("GetNodeHistory should return error for semantically corrupt node history wire")
	}
	if errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("GetNodeHistory returned ErrVersionNotFound for corrupt node history wire: %v", err)
	}
}

func TestBadgerStoreNodeHistoryVersionsFromRejectsSemanticWireCorruption(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	if err := bs.PutNodeVersion(types.NodeID(1), 0, n); err != nil {
		t.Fatalf("PutNodeVersion: %v", err)
	}
	corruptNodeHistoryWireAfterFlush(t, bs, 0, storepkg.NodeWire{ID: 1, PrimaryLabel: 0})

	defer func() {
		if rec := recover(); rec != nil {
			t.Fatalf("NodeHistoryVersionsFrom panicked on semantically corrupt node history wire: %v", rec)
		}
	}()
	_, err := bs.NodeHistoryVersionsFrom(types.NodeID(1), 0, 1)
	if err == nil {
		t.Fatal("NodeHistoryVersionsFrom should return error for semantically corrupt node history wire")
	}
	if errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("NodeHistoryVersionsFrom returned ErrVersionNotFound for corrupt node history wire: %v", err)
	}
}

func TestBadgerStoreNodeHistoryRejectsKeyPayloadVersionMismatch(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	if err := bs.PutNodeVersion(types.NodeID(1), 0, n); err != nil {
		t.Fatalf("PutNodeVersion: %v", err)
	}
	corruptNodeHistoryWireAfterFlush(t, bs, 0, storepkg.NodeWire{ID: 1, PrimaryLabel: 10, Version: 7})

	readers := []struct {
		name string
		run  func() error
	}{
		{
			name: "GetNodeVersion",
			run: func() error {
				_, err := bs.GetNodeVersion(types.NodeID(1), 0)
				return err
			},
		},
		{
			name: "GetNodeHistory",
			run: func() error {
				_, err := bs.GetNodeHistory(types.NodeID(1))
				return err
			},
		},
		{
			name: "NodeHistoryVersionsFrom",
			run: func() error {
				_, err := bs.NodeHistoryVersionsFrom(types.NodeID(1), 0, 1)
				return err
			},
		},
	}
	for _, reader := range readers {
		if err := reader.run(); !errors.Is(err, ErrInvalidStoreMutation) {
			t.Fatalf("%s mismatched history payload version = %v, want ErrInvalidStoreMutation", reader.name, err)
		}
	}
}

func TestBadgerStoreGetNodeHistory(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	id := snowflake.ID(1)
	for ver := uint32(0); ver < 3; ver++ {
		n := types.NewNode(types.NodeID(id), 10, nil)
		n.SetVersion(ver)
		if err := bs.PutNodeVersion(types.NodeID(id), ver, n); err != nil {
			t.Fatalf("PutNodeVersion(%d): %v", ver, err)
		}
	}

	history, err := bs.GetNodeHistory(types.NodeID(id))
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(history) != 3 {
		t.Fatalf("len(history) = %d, want 3", len(history))
	}
	for i, h := range history {
		if h.Version() != uint32(i) {
			t.Errorf("history[%d].Version() = %d, want %d", i, h.Version(), i)
		}
	}
}

func TestBadgerStoreGetNodeHistoryEmpty(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	history, err := bs.GetNodeHistory(types.NodeID(999))
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(history) != 0 {
		t.Fatalf("expected empty history, got %d entries", len(history))
	}
}

func TestBadgerStoreGetNodeHistoryAscending(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	id := snowflake.ID(1)
	for _, ver := range []uint32{2, 0, 1} {
		n := types.NewNode(types.NodeID(id), 10, nil)
		n.SetVersion(ver)
		bs.PutNodeVersion(types.NodeID(id), ver, n)
	}

	history, _ := bs.GetNodeHistory(types.NodeID(id))
	for i := 0; i < len(history)-1; i++ {
		if history[i].Version() >= history[i+1].Version() {
			t.Fatalf("not ascending: v[%d]=%d >= v[%d]=%d",
				i, history[i].Version(), i+1, history[i+1].Version())
		}
	}
}

func TestBadgerStoreNodeHistoryVersionsFrom(t *testing.T) {
	t.Parallel()
	bs := newSlowFlushBadgerStore(t)

	id := types.NodeID(snowflake.ID(1))
	for _, ver := range []uint32{4, 0, 2, 1, 3} {
		n := types.NewNode(id, 10, nil)
		n.SetVersion(ver)
		if err := n.SetProperty("version", int64(ver)); err != nil {
			t.Fatalf("SetProperty(%d): %v", ver, err)
		}
		if err := bs.PutNodeVersion(id, ver, n); err != nil {
			t.Fatalf("PutNodeVersion(%d): %v", ver, err)
		}
	}

	page, err := bs.NodeHistoryVersionsFrom(id, 2, 2)
	if err != nil {
		t.Fatalf("NodeHistoryVersionsFrom pending: %v", err)
	}
	if len(page) != 2 || page[0].Version() != 2 || page[1].Version() != 3 {
		t.Fatalf("NodeHistoryVersionsFrom(2,2) pending versions = %v, want [2 3]", badgerNodeHistoryVersions(page))
	}
	if err := page[0].SetProperty("version", int64(99)); err != nil {
		t.Fatalf("mutate returned page: %v", err)
	}
	again, err := bs.NodeHistoryVersionsFrom(id, 2, 1)
	if err != nil {
		t.Fatalf("NodeHistoryVersionsFrom again: %v", err)
	}
	v, _ := again[0].GetProperty("version")
	if v != int64(2) {
		t.Fatalf("NodeHistoryVersionsFrom returned shared node, version property = %v", v)
	}
	if err := bs.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	all, err := bs.NodeHistoryVersionsFrom(id, 3, 0)
	if err != nil {
		t.Fatalf("NodeHistoryVersionsFrom flushed limit 0: %v", err)
	}
	if len(all) != 2 || all[0].Version() != 3 || all[1].Version() != 4 {
		t.Fatalf("NodeHistoryVersionsFrom(3,0) flushed versions = %v, want [3 4]", badgerNodeHistoryVersions(all))
	}
	if err := bs.TruncateNodeHistory(id, 1); err != nil {
		t.Fatalf("TruncateNodeHistory: %v", err)
	}
	kept, err := bs.NodeHistoryVersionsFrom(id, 0, 0)
	if err != nil {
		t.Fatalf("NodeHistoryVersionsFrom after pending truncate: %v", err)
	}
	if len(kept) != 1 || kept[0].Version() != 4 {
		t.Fatalf("NodeHistoryVersionsFrom after pending truncate versions = %v, want [4]", badgerNodeHistoryVersions(kept))
	}
	if _, err := bs.NodeHistoryVersionsFrom(id, 0, -1); !errors.Is(err, storecontract.ErrInvalidQueryLimit) {
		t.Fatalf("NodeHistoryVersionsFrom negative limit = %v, want ErrInvalidQueryLimit", err)
	}
	if _, err := bs.NodeHistoryVersionsFrom(0, 0, 1); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("NodeHistoryVersionsFrom zero ID = %v, want ErrInvalidStoreMutation", err)
	}

	closed := newTestBadgerStore(t)
	if err := closed.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := closed.NodeHistoryVersionsFrom(id, 0, 1); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Fatalf("NodeHistoryVersionsFrom closed store = %v, want ErrStoreClosed", err)
	}
}

func TestBadgerStoreTruncateNodeHistory(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	id := snowflake.ID(1)
	for ver := uint32(0); ver < 5; ver++ {
		n := types.NewNode(types.NodeID(id), 10, nil)
		n.SetVersion(ver)
		bs.PutNodeVersion(types.NodeID(id), ver, n)
	}

	if err := bs.TruncateNodeHistory(types.NodeID(id), 2); err != nil {
		t.Fatalf("TruncateNodeHistory: %v", err)
	}

	history, _ := bs.GetNodeHistory(types.NodeID(id))
	if len(history) != 2 {
		t.Fatalf("len(history) = %d, want 2", len(history))
	}
	if history[0].Version() != 3 {
		t.Errorf("history[0].Version() = %d, want 3", history[0].Version())
	}
	if history[1].Version() != 4 {
		t.Errorf("history[1].Version() = %d, want 4", history[1].Version())
	}
}

func TestBadgerStoreTruncateNodeHistoryAll(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	id := snowflake.ID(1)
	for ver := uint32(0); ver < 3; ver++ {
		n := types.NewNode(types.NodeID(id), 10, nil)
		n.SetVersion(ver)
		bs.PutNodeVersion(types.NodeID(id), ver, n)
	}

	if err := bs.TruncateNodeHistory(types.NodeID(id), 0); err != nil {
		t.Fatalf("TruncateNodeHistory(0): %v", err)
	}

	history, _ := bs.GetNodeHistory(types.NodeID(id))
	if len(history) != 0 {
		t.Fatalf("expected empty after truncate all, got %d", len(history))
	}
}

func TestBadgerStoreTruncateNodeHistoryRejectsNegativeKeep(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	id := types.NodeID(snowflake.ID(1))
	n := types.NewNode(id, 10, nil)
	if err := bs.PutNodeVersion(id, 0, n); err != nil {
		t.Fatalf("PutNodeVersion: %v", err)
	}

	if err := bs.TruncateNodeHistory(id, -1); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("TruncateNodeHistory(-1) = %v, want ErrInvalidStoreMutation", err)
	}
	history, _ := bs.GetNodeHistory(id)
	if len(history) != 1 {
		t.Fatalf("negative truncate mutated history: len = %d, want 1", len(history))
	}
}

func TestBadgerStoreTrimNodeHistoryFrom(t *testing.T) {
	t.Parallel()
	bs := newSlowFlushBadgerStore(t)

	id := types.NodeID(snowflake.ID(1))
	for ver := uint32(0); ver < 2; ver++ {
		n := types.NewNode(id, 10, nil)
		n.SetVersion(ver)
		if err := bs.PutNodeVersion(id, ver, n); err != nil {
			t.Fatalf("PutNodeVersion(%d): %v", ver, err)
		}
	}
	if err := bs.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	for ver := uint32(2); ver < 5; ver++ {
		n := types.NewNode(id, 10, nil)
		n.SetVersion(ver)
		if err := bs.PutNodeVersion(id, ver, n); err != nil {
			t.Fatalf("PutNodeVersion(%d): %v", ver, err)
		}
	}

	if err := bs.TrimNodeHistoryFrom(id, 2); err != nil {
		t.Fatalf("TrimNodeHistoryFrom: %v", err)
	}
	history, err := bs.NodeHistoryVersionsFrom(id, 0, 0)
	if err != nil {
		t.Fatalf("NodeHistoryVersionsFrom after trim: %v", err)
	}
	if got := badgerNodeHistoryVersions(history); len(got) != 2 || got[0] != 0 || got[1] != 1 {
		t.Fatalf("versions after trim = %v, want [0 1]", got)
	}
	if err := bs.flush(); err != nil {
		t.Fatalf("flush trimmed deletes: %v", err)
	}
	history, err = bs.NodeHistoryVersionsFrom(id, 0, 0)
	if err != nil {
		t.Fatalf("NodeHistoryVersionsFrom after flush: %v", err)
	}
	if got := badgerNodeHistoryVersions(history); len(got) != 2 || got[0] != 0 || got[1] != 1 {
		t.Fatalf("versions after flushed trim = %v, want [0 1]", got)
	}
	if err := bs.TrimNodeHistoryFrom(0, 0); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("TrimNodeHistoryFrom zero ID = %v, want ErrInvalidStoreMutation", err)
	}
}

func TestBadgerStoreDeleteNodePreservesHistory(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 1, 10, nil)
	for ver := uint32(0); ver < 3; ver++ {
		n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
		n.SetVersion(ver)
		bs.PutNodeVersion(types.NodeID(1), ver, n)
	}

	if err := bs.DeleteNodeCascade(types.NodeID(1)); err != nil {
		t.Fatalf("DeleteNodeCascade: %v", err)
	}

	// History is preserved after cascade delete — temporal queries need it.
	history, _ := bs.GetNodeHistory(types.NodeID(1))
	if len(history) != 3 {
		t.Fatalf("expected 3 preserved history entries after cascade, got %d", len(history))
	}
}

func TestBadgerStoreNodeHistorySurvivesRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Phase 1: store history, flush, close.
	bs1, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	for ver := uint32(0); ver < 3; ver++ {
		n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
		n.SetVersion(ver)
		if err := bs1.PutNodeVersion(types.NodeID(1), ver, n); err != nil {
			t.Fatalf("PutNodeVersion: %v", err)
		}
	}
	if err := bs1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	// Phase 2: reopen and verify.
	bs2, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer bs2.Close()

	history, err := bs2.GetNodeHistory(types.NodeID(1))
	if err != nil {
		t.Fatalf("GetNodeHistory after restart: %v", err)
	}
	if len(history) != 3 {
		t.Fatalf("len(history) = %d, want 3", len(history))
	}
	for i, h := range history {
		if h.Version() != uint32(i) {
			t.Errorf("history[%d].Version() = %d, want %d", i, h.Version(), i)
		}
	}
}

// ─── Store: Relationship version history ──────────────────────────────

func TestBadgerStorePutGetRelVersion(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	_ = r.SetProperty("weight", 1.5)

	if err := bs.PutRelVersion(types.RelID(100), 0, r); err != nil {
		t.Fatalf("PutRelVersion: %v", err)
	}

	got, err := bs.GetRelVersion(types.RelID(100), 0)
	if err != nil {
		t.Fatalf("GetRelVersion: %v", err)
	}
	v, ok := got.GetProperty("weight")
	if !ok || v != 1.5 {
		t.Fatalf("property mismatch: got %v", v)
	}

	// Cache isolation.
	_ = got.SetProperty("weight", 999.0)
	got2, _ := bs.GetRelVersion(types.RelID(100), 0)
	v2, _ := got2.GetProperty("weight")
	if v2 != 1.5 {
		t.Fatalf("GetRelVersion returned shared pointer: got %v, want 1.5", v2)
	}
}

func TestBadgerStoreGetRelVersionNotFound(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	_, err := bs.GetRelVersion(types.RelID(100), 0)
	if !errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("expected ErrVersionNotFound, got %v", err)
	}
}

func TestBadgerStoreGetRelVersionRejectsSemanticWireCorruption(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 20, types.NodeID(1), types.NodeID(2))
	if err := bs.PutRelVersion(types.RelID(100), 0, r); err != nil {
		t.Fatalf("PutRelVersion: %v", err)
	}
	corruptRelHistoryWireAfterFlush(t, bs, 0, storepkg.RelWire{ID: 100, RelType: 0, StartID: 1, EndID: 2})

	defer func() {
		if rec := recover(); rec != nil {
			t.Fatalf("GetRelVersion panicked on semantically corrupt rel history wire: %v", rec)
		}
	}()
	_, err := bs.GetRelVersion(types.RelID(100), 0)
	if err == nil {
		t.Fatal("GetRelVersion should return error for semantically corrupt rel history wire")
	}
	if errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("GetRelVersion returned ErrVersionNotFound for corrupt rel history wire: %v", err)
	}
}

func TestBadgerStoreGetRelHistoryRejectsSemanticWireCorruption(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 20, types.NodeID(1), types.NodeID(2))
	if err := bs.PutRelVersion(types.RelID(100), 0, r); err != nil {
		t.Fatalf("PutRelVersion: %v", err)
	}
	corruptRelHistoryWireAfterFlush(t, bs, 0, storepkg.RelWire{ID: 100, RelType: 0, StartID: 1, EndID: 2})

	defer func() {
		if rec := recover(); rec != nil {
			t.Fatalf("GetRelHistory panicked on semantically corrupt rel history wire: %v", rec)
		}
	}()
	_, err := bs.GetRelHistory(types.RelID(100))
	if err == nil {
		t.Fatal("GetRelHistory should return error for semantically corrupt rel history wire")
	}
	if errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("GetRelHistory returned ErrVersionNotFound for corrupt rel history wire: %v", err)
	}
}

func TestBadgerStoreRelHistoryVersionsFromRejectsSemanticWireCorruption(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 20, types.NodeID(1), types.NodeID(2))
	if err := bs.PutRelVersion(types.RelID(100), 0, r); err != nil {
		t.Fatalf("PutRelVersion: %v", err)
	}
	corruptRelHistoryWireAfterFlush(t, bs, 0, storepkg.RelWire{ID: 100, RelType: 0, StartID: 1, EndID: 2})

	defer func() {
		if rec := recover(); rec != nil {
			t.Fatalf("RelHistoryVersionsFrom panicked on semantically corrupt rel history wire: %v", rec)
		}
	}()
	_, err := bs.RelHistoryVersionsFrom(types.RelID(100), 0, 1)
	if err == nil {
		t.Fatal("RelHistoryVersionsFrom should return error for semantically corrupt rel history wire")
	}
	if errors.Is(err, ErrVersionNotFound) {
		t.Fatalf("RelHistoryVersionsFrom returned ErrVersionNotFound for corrupt rel history wire: %v", err)
	}
}

func TestBadgerStoreRelHistoryRejectsKeyPayloadVersionMismatch(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 20, types.NodeID(1), types.NodeID(2))
	if err := bs.PutRelVersion(types.RelID(100), 0, r); err != nil {
		t.Fatalf("PutRelVersion: %v", err)
	}
	corruptRelHistoryWireAfterFlush(t, bs, 0, storepkg.RelWire{ID: 100, RelType: 20, StartID: 1, EndID: 2, Version: 8})

	readers := []struct {
		name string
		run  func() error
	}{
		{
			name: "GetRelVersion",
			run: func() error {
				_, err := bs.GetRelVersion(types.RelID(100), 0)
				return err
			},
		},
		{
			name: "GetRelHistory",
			run: func() error {
				_, err := bs.GetRelHistory(types.RelID(100))
				return err
			},
		},
		{
			name: "RelHistoryVersionsFrom",
			run: func() error {
				_, err := bs.RelHistoryVersionsFrom(types.RelID(100), 0, 1)
				return err
			},
		},
	}
	for _, reader := range readers {
		if err := reader.run(); !errors.Is(err, ErrInvalidStoreMutation) {
			t.Fatalf("%s mismatched history payload version = %v, want ErrInvalidStoreMutation", reader.name, err)
		}
	}
}

func TestBadgerStoreGetRelHistory(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	id := snowflake.ID(100)
	for ver := uint32(0); ver < 3; ver++ {
		r := types.NewRelationship(types.RelID(id), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
		r.SetVersion(ver)
		bs.PutRelVersion(types.RelID(id), ver, r)
	}

	history, err := bs.GetRelHistory(types.RelID(id))
	if err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	}
	if len(history) != 3 {
		t.Fatalf("len(history) = %d, want 3", len(history))
	}
	for i, h := range history {
		if h.Version() != uint32(i) {
			t.Errorf("history[%d].Version() = %d, want %d", i, h.Version(), i)
		}
	}
}

func TestBadgerStoreGetRelHistoryEmpty(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	history, err := bs.GetRelHistory(types.RelID(999))
	if err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	}
	if len(history) != 0 {
		t.Fatalf("expected empty, got %d", len(history))
	}
}

func TestBadgerStoreGetRelHistoryAscending(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	id := snowflake.ID(100)
	for _, ver := range []uint32{2, 0, 1} {
		r := types.NewRelationship(types.RelID(id), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
		r.SetVersion(ver)
		bs.PutRelVersion(types.RelID(id), ver, r)
	}

	history, _ := bs.GetRelHistory(types.RelID(id))
	for i := 0; i < len(history)-1; i++ {
		if history[i].Version() >= history[i+1].Version() {
			t.Fatalf("not ascending: v[%d]=%d >= v[%d]=%d",
				i, history[i].Version(), i+1, history[i+1].Version())
		}
	}
}

func TestBadgerStoreRelHistoryVersionsFrom(t *testing.T) {
	t.Parallel()
	bs := newSlowFlushBadgerStore(t)

	id := types.RelID(snowflake.ID(100))
	for _, ver := range []uint32{4, 0, 2, 1, 3} {
		r := types.NewRelationship(id, 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
		r.SetVersion(ver)
		if err := r.SetProperty("version", int64(ver)); err != nil {
			t.Fatalf("SetProperty(%d): %v", ver, err)
		}
		if err := bs.PutRelVersion(id, ver, r); err != nil {
			t.Fatalf("PutRelVersion(%d): %v", ver, err)
		}
	}

	page, err := bs.RelHistoryVersionsFrom(id, 2, 2)
	if err != nil {
		t.Fatalf("RelHistoryVersionsFrom pending: %v", err)
	}
	if len(page) != 2 || page[0].Version() != 2 || page[1].Version() != 3 {
		t.Fatalf("RelHistoryVersionsFrom(2,2) pending versions = %v, want [2 3]", badgerRelHistoryVersions(page))
	}
	if err := page[0].SetProperty("version", int64(99)); err != nil {
		t.Fatalf("mutate returned page: %v", err)
	}
	again, err := bs.RelHistoryVersionsFrom(id, 2, 1)
	if err != nil {
		t.Fatalf("RelHistoryVersionsFrom again: %v", err)
	}
	v, _ := again[0].GetProperty("version")
	if v != int64(2) {
		t.Fatalf("RelHistoryVersionsFrom returned shared relationship, version property = %v", v)
	}
	if err := bs.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	all, err := bs.RelHistoryVersionsFrom(id, 3, 0)
	if err != nil {
		t.Fatalf("RelHistoryVersionsFrom flushed limit 0: %v", err)
	}
	if len(all) != 2 || all[0].Version() != 3 || all[1].Version() != 4 {
		t.Fatalf("RelHistoryVersionsFrom(3,0) flushed versions = %v, want [3 4]", badgerRelHistoryVersions(all))
	}
	if err := bs.TruncateRelHistory(id, 1); err != nil {
		t.Fatalf("TruncateRelHistory: %v", err)
	}
	kept, err := bs.RelHistoryVersionsFrom(id, 0, 0)
	if err != nil {
		t.Fatalf("RelHistoryVersionsFrom after pending truncate: %v", err)
	}
	if len(kept) != 1 || kept[0].Version() != 4 {
		t.Fatalf("RelHistoryVersionsFrom after pending truncate versions = %v, want [4]", badgerRelHistoryVersions(kept))
	}
	if _, err := bs.RelHistoryVersionsFrom(id, 0, -1); !errors.Is(err, storecontract.ErrInvalidQueryLimit) {
		t.Fatalf("RelHistoryVersionsFrom negative limit = %v, want ErrInvalidQueryLimit", err)
	}
	if _, err := bs.RelHistoryVersionsFrom(0, 0, 1); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("RelHistoryVersionsFrom zero ID = %v, want ErrInvalidStoreMutation", err)
	}

	closed := newTestBadgerStore(t)
	if err := closed.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := closed.RelHistoryVersionsFrom(id, 0, 1); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Fatalf("RelHistoryVersionsFrom closed store = %v, want ErrStoreClosed", err)
	}
}

func TestBadgerStoreTruncateRelHistory(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	id := snowflake.ID(100)
	for ver := uint32(0); ver < 5; ver++ {
		r := types.NewRelationship(types.RelID(id), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
		r.SetVersion(ver)
		bs.PutRelVersion(types.RelID(id), ver, r)
	}

	if err := bs.TruncateRelHistory(types.RelID(id), 2); err != nil {
		t.Fatalf("TruncateRelHistory: %v", err)
	}

	history, _ := bs.GetRelHistory(types.RelID(id))
	if len(history) != 2 {
		t.Fatalf("len(history) = %d, want 2", len(history))
	}
	if history[0].Version() != 3 {
		t.Errorf("history[0].Version() = %d, want 3", history[0].Version())
	}
	if history[1].Version() != 4 {
		t.Errorf("history[1].Version() = %d, want 4", history[1].Version())
	}
}

func TestBadgerStoreTruncateRelHistoryAll(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	id := snowflake.ID(100)
	for ver := uint32(0); ver < 3; ver++ {
		r := types.NewRelationship(types.RelID(id), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
		r.SetVersion(ver)
		bs.PutRelVersion(types.RelID(id), ver, r)
	}

	if err := bs.TruncateRelHistory(types.RelID(id), 0); err != nil {
		t.Fatalf("TruncateRelHistory(0): %v", err)
	}

	history, _ := bs.GetRelHistory(types.RelID(id))
	if len(history) != 0 {
		t.Fatalf("expected empty after truncate all, got %d", len(history))
	}
}

func TestBadgerStoreTruncateRelHistoryRejectsNegativeKeep(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	id := types.RelID(snowflake.ID(100))
	r := types.NewRelationship(id, 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
	if err := bs.PutRelVersion(id, 0, r); err != nil {
		t.Fatalf("PutRelVersion: %v", err)
	}

	if err := bs.TruncateRelHistory(id, -1); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("TruncateRelHistory(-1) = %v, want ErrInvalidStoreMutation", err)
	}
	history, _ := bs.GetRelHistory(id)
	if len(history) != 1 {
		t.Fatalf("negative truncate mutated rel history: len = %d, want 1", len(history))
	}
}

func TestBadgerStoreTrimRelHistoryFrom(t *testing.T) {
	t.Parallel()
	bs := newSlowFlushBadgerStore(t)

	id := types.RelID(snowflake.ID(100))
	for ver := uint32(0); ver < 2; ver++ {
		r := types.NewRelationship(id, 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
		r.SetVersion(ver)
		if err := bs.PutRelVersion(id, ver, r); err != nil {
			t.Fatalf("PutRelVersion(%d): %v", ver, err)
		}
	}
	if err := bs.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	for ver := uint32(2); ver < 5; ver++ {
		r := types.NewRelationship(id, 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
		r.SetVersion(ver)
		if err := bs.PutRelVersion(id, ver, r); err != nil {
			t.Fatalf("PutRelVersion(%d): %v", ver, err)
		}
	}

	if err := bs.TrimRelHistoryFrom(id, 2); err != nil {
		t.Fatalf("TrimRelHistoryFrom: %v", err)
	}
	history, err := bs.RelHistoryVersionsFrom(id, 0, 0)
	if err != nil {
		t.Fatalf("RelHistoryVersionsFrom after trim: %v", err)
	}
	if got := badgerRelHistoryVersions(history); len(got) != 2 || got[0] != 0 || got[1] != 1 {
		t.Fatalf("versions after trim = %v, want [0 1]", got)
	}
	if err := bs.flush(); err != nil {
		t.Fatalf("flush trimmed deletes: %v", err)
	}
	history, err = bs.RelHistoryVersionsFrom(id, 0, 0)
	if err != nil {
		t.Fatalf("RelHistoryVersionsFrom after flush: %v", err)
	}
	if got := badgerRelHistoryVersions(history); len(got) != 2 || got[0] != 0 || got[1] != 1 {
		t.Fatalf("versions after flushed trim = %v, want [0 1]", got)
	}
	if err := bs.TrimRelHistoryFrom(0, 0); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("TrimRelHistoryFrom zero ID = %v, want ErrInvalidStoreMutation", err)
	}
}

func TestBadgerStoreDeleteRelPreservesHistory(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestRel(t, bs, 100, 5, 10, 20)

	for ver := uint32(0); ver < 3; ver++ {
		r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
		r.SetVersion(ver)
		bs.PutRelVersion(types.RelID(100), ver, r)
	}

	if err := bs.DeleteRelationship(types.RelID(100)); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}

	// History is preserved after delete — temporal queries need it.
	history, _ := bs.GetRelHistory(types.RelID(100))
	if len(history) != 3 {
		t.Fatalf("expected 3 preserved history entries after delete, got %d", len(history))
	}
}

func TestBadgerStoreDeleteNodeCascadePreservesHistory(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 1, nil)
	putTestRel(t, bs, 100, 5, 10, 20)

	// Store rel and node history.
	for ver := uint32(0); ver < 3; ver++ {
		r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
		r.SetVersion(ver)
		bs.PutRelVersion(types.RelID(100), ver, r)

		n := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
		n.SetVersion(ver)
		bs.PutNodeVersion(types.NodeID(10), ver, n)
	}

	if err := bs.DeleteNodeCascade(types.NodeID(10)); err != nil {
		t.Fatalf("DeleteNodeCascade: %v", err)
	}

	// History is preserved after cascade delete — temporal queries need it.
	relHistory, _ := bs.GetRelHistory(types.RelID(100))
	if len(relHistory) != 3 {
		t.Fatalf("expected 3 preserved rel history after cascade, got %d", len(relHistory))
	}

	nodeHistory, _ := bs.GetNodeHistory(types.NodeID(10))
	if len(nodeHistory) != 3 {
		t.Fatalf("expected 3 preserved node history after cascade, got %d", len(nodeHistory))
	}
}

func TestBadgerStoreRelHistorySurvivesRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	bs1, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	for ver := uint32(0); ver < 3; ver++ {
		r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(20)))
		r.SetVersion(ver)
		bs1.PutRelVersion(types.RelID(100), ver, r)
	}
	if err := bs1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	bs2, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer bs2.Close()

	history, err := bs2.GetRelHistory(types.RelID(100))
	if err != nil {
		t.Fatalf("GetRelHistory after restart: %v", err)
	}
	if len(history) != 3 {
		t.Fatalf("len(history) = %d, want 3", len(history))
	}
	for i, h := range history {
		if h.Version() != uint32(i) {
			t.Errorf("history[%d].Version() = %d, want %d", i, h.Version(), i)
		}
	}
}

// ─── ReplaceNodeWithHistory ─────────────────────────────────────────────────

func TestBadgerStoreReplaceNodeWithHistory(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := putTestNode(t, bs, 1, 10, nil)
	_ = n.SetProperty("name", "Alice")
	n.SetVersion(0)
	// We need to replace with the property (putTestNode already stored without it).
	// Simpler: use ReplaceNode to set up initial state with property.
	_ = n.SetProperty("name", "Alice")
	if err := bs.ReplaceNode(n); err != nil {
		t.Fatal(err)
	}

	// Get current state.
	current, _ := bs.GetNode(types.NodeID(1))
	prevState := current.DeepCopy()
	prevVersion := current.Version()

	// Mutate.
	_ = current.SetProperty("name", "Bob")
	current.SetVersion(1)

	if err := bs.ReplaceNodeWithHistory(current, prevVersion, prevState); err != nil {
		t.Fatalf("ReplaceNodeWithHistory: %v", err)
	}

	// Verify current state.
	got, _ := bs.GetNode(types.NodeID(1))
	if got.PropertiesMap()["name"] != "Bob" {
		t.Fatalf("got name=%v, want Bob", got.PropertiesMap()["name"])
	}

	// Verify history.
	hist, err := bs.GetNodeVersion(types.NodeID(1), prevVersion)
	if err != nil {
		t.Fatalf("GetNodeVersion: %v", err)
	}
	if hist.PropertiesMap()["name"] != "Alice" {
		t.Fatalf("history name=%v, want Alice", hist.PropertiesMap()["name"])
	}
}

func TestBadgerStoreReplaceNodeWithHistoryNotFound(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(999)), 10, nil)
	err := bs.ReplaceNodeWithHistory(n, 0, n)
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("want ErrNodeNotFound, got %v", err)
	}
}

func TestBadgerStoreReplaceNodeRejectsLabelMutation(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := putTestNode(t, bs, 1, 10, nil)

	replacement := types.NewNode(n.ID(), 20, nil)
	if err := bs.ReplaceNode(replacement); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ReplaceNode label mutation = %v, want ErrInvalidStoreMutation", err)
	}
	if nodes, err := bs.NodesByLabel(20, QueryOpts{}); err != nil || len(nodes) != 0 {
		t.Fatalf("NodesByLabel(20) = %d, %v; want 0, nil", len(nodes), err)
	}
	if nodes, err := bs.NodesByLabel(10, QueryOpts{}); err != nil || len(nodes) != 1 {
		t.Fatalf("NodesByLabel(10) = %d, %v; want 1, nil", len(nodes), err)
	}

	current, err := bs.GetNode(n.ID())
	if err != nil {
		t.Fatal(err)
	}
	withHistory := types.NewNode(n.ID(), 20, nil)
	withHistory.SetVersion(1)
	if err := bs.ReplaceNodeWithHistory(withHistory, current.Version(), current); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ReplaceNodeWithHistory label mutation = %v, want ErrInvalidStoreMutation", err)
	}
	history, err := bs.GetNodeHistory(n.ID())
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 0 {
		t.Fatalf("history entries after rejected label mutation = %d, want 0", len(history))
	}
}

func TestBadgerStoreNodeLabelTokenHelpersRejectInvalidDeltas(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := putTestNode(t, bs, 1, 10, []uint16{20})

	stillHasRemoved := n.DeepCopy()
	if err := bs.RemoveNodeLabelToken(n.ID(), 20, stillHasRemoved); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("RemoveNodeLabelToken unchanged payload = %v, want ErrInvalidStoreMutation", err)
	}
	if nodes, err := bs.NodesByLabel(20, QueryOpts{}); err != nil || len(nodes) != 1 {
		t.Fatalf("NodesByLabel(20) after rejected remove = %d, %v; want 1, nil", len(nodes), err)
	}

	missingAdded := n.DeepCopy()
	if err := bs.AddNodeLabelToken(n.ID(), 30, missingAdded); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("AddNodeLabelToken unchanged payload = %v, want ErrInvalidStoreMutation", err)
	}
	if nodes, err := bs.NodesByLabel(30, QueryOpts{}); err != nil || len(nodes) != 0 {
		t.Fatalf("NodesByLabel(30) after rejected add = %d, %v; want 0, nil", len(nodes), err)
	}

	prev := n.DeepCopy()
	invalidRemoveWithHistory := n.DeepCopy()
	invalidRemoveWithHistory.SetVersion(1)
	if err := bs.RemoveNodeLabelTokenWithHistory(n.ID(), 20, invalidRemoveWithHistory, prev.Version(), prev); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("RemoveNodeLabelTokenWithHistory unchanged payload = %v, want ErrInvalidStoreMutation", err)
	}

	invalidAddWithHistory := n.DeepCopy()
	invalidAddWithHistory.SetVersion(1)
	if err := bs.AddNodeLabelTokenWithHistory(n.ID(), 30, invalidAddWithHistory, prev.Version(), prev); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("AddNodeLabelTokenWithHistory unchanged payload = %v, want ErrInvalidStoreMutation", err)
	}

	history, err := bs.GetNodeHistory(n.ID())
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 0 {
		t.Fatalf("history entries after rejected label-token helpers = %d, want 0", len(history))
	}
}

func TestBadgerStoreReplaceNodeWithHistoryUsesStoredStateForIndexCleanup(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := putTestNode(t, bs, 1, 10, nil)
	_ = n.SetProperty("name", "Alice")
	if err := bs.ReplaceNode(n); err != nil {
		t.Fatal(err)
	}
	if err := bs.CreatePropertyIndex(10, "name"); err != nil {
		t.Fatal(err)
	}

	current, err := bs.GetNode(n.ID())
	if err != nil {
		t.Fatal(err)
	}
	stalePrevState := types.NewNode(n.ID(), 10, nil)
	updated := current.DeepCopy()
	_ = updated.SetProperty("name", "Bob")
	updated.SetVersion(1)

	if err := bs.ReplaceNodeWithHistory(updated, current.Version(), stalePrevState); err != nil {
		t.Fatalf("ReplaceNodeWithHistory: %v", err)
	}
	alice, err := bs.NodesByLabelAndProperty(10, "name", "Alice", QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(alice) != 0 {
		t.Fatalf("Alice index results after replace = %d, want 0", len(alice))
	}
	bob, err := bs.NodesByLabelAndProperty(10, "name", "Bob", QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(bob) != 1 || bob[0].ID() != n.ID() {
		t.Fatalf("Bob index results after replace = %d, want node %d", len(bob), n.ID())
	}
}

func TestBadgerStoreReplaceWithHistoryRejectsNilPayloads(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	if err := bs.ReplaceNodeWithHistory(nil, 0, n); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ReplaceNodeWithHistory(nil current) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := bs.ReplaceNodeWithHistory(n, 0, nil); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ReplaceNodeWithHistory(nil history) = %v, want ErrInvalidStoreMutation", err)
	}

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 1, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	if err := bs.ReplaceRelWithHistory(nil, 0, r); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ReplaceRelWithHistory(nil current) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := bs.ReplaceRelWithHistory(r, 0, nil); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ReplaceRelWithHistory(nil history) = %v, want ErrInvalidStoreMutation", err)
	}
}

func TestBadgerStoreReplaceNodeWithHistoryPersistence(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 1, 10, nil)
	current, _ := bs.GetNode(types.NodeID(1))
	prevState := current.DeepCopy()
	_ = current.SetProperty("x", int64(42))
	current.SetVersion(1)

	if err := bs.ReplaceNodeWithHistory(current, 0, prevState); err != nil {
		t.Fatal(err)
	}

	// Flush to Badger.
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}

	// Both entity and history must be in Badger now.
	got, _ := bs.GetNode(types.NodeID(1))
	if got.Version() != 1 {
		t.Fatalf("version = %d, want 1", got.Version())
	}
	hist, _ := bs.GetNodeVersion(types.NodeID(1), 0)
	if hist.Version() != 0 {
		t.Fatalf("history version = %d, want 0", hist.Version())
	}
}

// ─── ReplaceRelWithHistory ──────────────────────────────────────────────────

func TestBadgerStoreReplaceRelWithHistory(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)

	r := putTestRel(t, bs, 100, 5, 1, 2)
	_ = r.SetProperty("weight", int64(5))
	if err := bs.ReplaceRelationship(r); err != nil {
		t.Fatal(err)
	}

	// Get current state.
	current, _ := bs.GetRelationship(types.RelID(100))
	prevState := current.DeepCopy()
	prevVersion := current.Version()

	// Mutate.
	_ = current.SetProperty("weight", int64(10))
	current.SetVersion(1)

	if err := bs.ReplaceRelWithHistory(current, prevVersion, prevState); err != nil {
		t.Fatalf("ReplaceRelWithHistory: %v", err)
	}

	// Verify current state.
	got, _ := bs.GetRelationship(types.RelID(100))
	if got.PropertiesMap()["weight"] != int64(10) {
		t.Fatalf("got weight=%v, want 10", got.PropertiesMap()["weight"])
	}

	// Verify history.
	hist, err := bs.GetRelVersion(types.RelID(100), prevVersion)
	if err != nil {
		t.Fatalf("GetRelVersion: %v", err)
	}
	if hist.PropertiesMap()["weight"] != int64(5) {
		t.Fatalf("history weight=%v, want 5", hist.PropertiesMap()["weight"])
	}
}

func TestBadgerStoreReplaceRelWithHistoryNotFound(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	r := types.NewRelationship(types.RelID(snowflake.ID(999)), 1, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	err := bs.ReplaceRelWithHistory(r, 0, r)
	if !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("want ErrRelNotFound, got %v", err)
	}
}

func TestBadgerStoreReplaceRelWithHistoryRejectsIndexedFieldMutation(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	putTestNode(t, bs, 3, 10, nil)

	original := putTestRel(t, bs, 100, 5, 1, 2)
	updated := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(3)))
	updated.SetVersion(1)

	err := bs.ReplaceRelWithHistory(updated, original.Version(), original.DeepCopy())
	if !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ReplaceRelWithHistory indexed-field mutation = %v, want ErrInvalidStoreMutation", err)
	}
	hist, histErr := bs.GetRelHistory(types.RelID(snowflake.ID(100)))
	if histErr != nil {
		t.Fatal(histErr)
	}
	if len(hist) != 0 {
		t.Fatalf("history written for rejected relationship replacement: %d entries", len(hist))
	}
	current, err := bs.GetRelationship(types.RelID(snowflake.ID(100)))
	if err != nil {
		t.Fatal(err)
	}
	if current.EndNodeID() != types.NodeID(snowflake.ID(2)) || current.Version() != 0 {
		t.Fatalf("relationship changed after rejected replacement: end=%d version=%d", current.EndNodeID(), current.Version())
	}
}

// ─── DeleteRelWithHistory ───────────────────────────────────────────────────

func TestBadgerStoreDeleteRelWithHistory(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	rel := putTestRel(t, bs, 100, 5, 1, 2)
	if err := rel.SetProperty("deleted", true); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	if err := bs.ReplaceRelationship(rel); err != nil {
		t.Fatalf("ReplaceRelationship: %v", err)
	}

	tombstone := rel.DeepCopy()
	if err := bs.DeleteRelWithHistory(rel.ID(), rel.Version(), tombstone); err != nil {
		t.Fatalf("DeleteRelWithHistory: %v", err)
	}

	if _, err := bs.GetRelationship(rel.ID()); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("GetRelationship after DeleteRelWithHistory = %v, want ErrRelNotFound", err)
	}
	if got := bs.OutgoingRelIDs(rel.StartNodeID().SnowflakeID()); len(got) != 0 {
		t.Fatalf("OutgoingRelIDs after DeleteRelWithHistory = %v, want empty", got)
	}
	if got := bs.IncomingRelIDs(rel.EndNodeID().SnowflakeID(), 0); len(got) != 0 {
		t.Fatalf("IncomingRelIDs after DeleteRelWithHistory = %v, want empty", got)
	}
	if got, err := bs.RelationshipCount(); err != nil || got != 0 {
		t.Fatalf("RelationshipCount after DeleteRelWithHistory = %d err %v, want 0 nil", got, err)
	}
	hist, err := bs.GetRelVersion(rel.ID(), rel.Version())
	if err != nil {
		t.Fatalf("GetRelVersion: %v", err)
	}
	if hist.ID() != rel.ID() || hist.PropertiesMap()["deleted"] != true {
		t.Fatalf("history tombstone = %#v, want deleted relationship snapshot", hist)
	}
}

func TestBadgerStoreDeleteRelWithHistoryNotFound(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	tombstone := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	err := bs.DeleteRelWithHistory(tombstone.ID(), tombstone.Version(), tombstone)
	if !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("DeleteRelWithHistory missing rel = %v, want ErrRelNotFound", err)
	}
	history, histErr := bs.GetRelHistory(tombstone.ID())
	if histErr != nil {
		t.Fatalf("GetRelHistory after missing delete: %v", histErr)
	}
	if len(history) != 0 {
		t.Fatalf("history after missing DeleteRelWithHistory = %d entries, want 0", len(history))
	}
}

func TestBadgerStoreDeleteRelWithHistoryRejectsIndexedFieldMutation(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	putTestNode(t, bs, 3, 10, nil)
	rel := putTestRel(t, bs, 100, 5, 1, 2)
	badTombstone := types.NewRelationship(rel.ID(), rel.TypeToken().Value(), rel.StartNodeID(), types.NodeID(snowflake.ID(3)))

	err := bs.DeleteRelWithHistory(rel.ID(), rel.Version(), badTombstone)
	if !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteRelWithHistory indexed-field mutation = %v, want ErrInvalidStoreMutation", err)
	}
	got, err := bs.GetRelationship(rel.ID())
	if err != nil {
		t.Fatalf("GetRelationship after rejected DeleteRelWithHistory: %v", err)
	}
	if got.EndNodeID() != types.NodeID(snowflake.ID(2)) {
		t.Fatalf("relationship endpoint changed after rejected DeleteRelWithHistory: got %d, want 2", got.EndNodeID())
	}
	history, histErr := bs.GetRelHistory(rel.ID())
	if histErr != nil {
		t.Fatalf("GetRelHistory after rejected DeleteRelWithHistory: %v", histErr)
	}
	if len(history) != 0 {
		t.Fatalf("history after rejected DeleteRelWithHistory = %d entries, want 0", len(history))
	}
}

type badgerUnmarshalableCustomProperty struct {
	Ch chan int
}

func (b badgerUnmarshalableCustomProperty) HashBytes() []byte { return []byte("badger-unmarshalable") }
func (b badgerUnmarshalableCustomProperty) DeepCopyValue() any {
	return badgerUnmarshalableCustomProperty{Ch: b.Ch}
}

func TestBadgerStoreDeleteRelWithHistoryRejectsUnmarshalableTombstone(t *testing.T) {
	if err := types.RegisterPropertyStructType(badgerUnmarshalableCustomProperty{}); err != nil {
		t.Fatalf("RegisterPropertyStructType: %v", err)
	}
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 1, 10, nil)
	putTestNode(t, bs, 2, 10, nil)
	rel := putTestRel(t, bs, 100, 5, 1, 2)
	tombstone := rel.DeepCopy()
	if err := tombstone.SetProperties(types.PropertySlice{{
		Key:   "bad",
		Value: badgerUnmarshalableCustomProperty{Ch: make(chan int)},
	}}); err != nil {
		t.Fatalf("SetProperties: %v", err)
	}

	err := bs.DeleteRelWithHistory(rel.ID(), rel.Version(), tombstone)
	if err == nil {
		t.Fatal("DeleteRelWithHistory returned nil for unmarshalable tombstone")
	}
	if _, getErr := bs.GetRelationship(rel.ID()); getErr != nil {
		t.Fatalf("relationship changed after unmarshalable tombstone rejection: %v", getErr)
	}
	history, histErr := bs.GetRelHistory(rel.ID())
	if histErr != nil {
		t.Fatalf("GetRelHistory after unmarshalable tombstone rejection: %v", histErr)
	}
	if len(history) != 0 {
		t.Fatalf("history after unmarshalable tombstone rejection = %d entries, want 0", len(history))
	}
}

func TestBadgerStoreDeleteNodeWithHistoryRejectsRelTombstoneIndexedFieldMutation(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 10, 1, nil)
	putTestNode(t, bs, 20, 2, nil)
	putTestNode(t, bs, 30, 3, nil)
	rel := putTestRel(t, bs, 100, 1, 10, 20)

	nodeTombstone := types.NewNode(types.NodeID(snowflake.ID(10)), 1, nil)
	badRelTombstone := types.NewRelationship(types.RelID(snowflake.ID(100)), 1, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(30)))
	err := bs.DeleteNodeWithHistory(types.NodeID(snowflake.ID(10)), 0, nodeTombstone, []RelTombstone{{
		ID:          rel.ID(),
		PrevVersion: rel.Version(),
		Tombstone:   badRelTombstone,
	}})
	if !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteNodeWithHistory bad rel tombstone = %v, want ErrInvalidStoreMutation", err)
	}
	if _, err := bs.GetNode(types.NodeID(snowflake.ID(10))); err != nil {
		t.Fatalf("node deleted after rejected tombstone: %v", err)
	}
	gotRel, err := bs.GetRelationship(types.RelID(snowflake.ID(100)))
	if err != nil {
		t.Fatalf("relationship deleted after rejected tombstone: %v", err)
	}
	if gotRel.EndNodeID() != types.NodeID(snowflake.ID(20)) {
		t.Fatalf("relationship tombstone changed live endpoint: got %d, want 20", gotRel.EndNodeID())
	}
	if hist, err := bs.GetRelHistory(types.RelID(snowflake.ID(100))); err != nil || len(hist) != 0 {
		t.Fatalf("relationship history after rejected tombstone = len %d err %v, want empty nil", len(hist), err)
	}
}

// --- AllNodeHistoryIDs / AllRelHistoryIDs ---

func TestBadgerStoreAllNodeHistoryIDs(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// No history yet.
	ids, err := bs.AllNodeHistoryIDs()
	if err != nil {
		t.Fatalf("AllNodeHistoryIDs: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected 0 history IDs, got %d", len(ids))
	}

	// Add nodes and create history versions.
	putTestNode(t, bs, 100, 1, nil)
	putTestNode(t, bs, 200, 1, nil)
	putTestNode(t, bs, 300, 1, nil)

	n100 := types.NewNode(types.NodeID(snowflake.ID(100)), 1, nil)
	_ = n100.SetProperty("v", int64(1))
	if err := bs.PutNodeVersion(types.NodeID(100), 0, n100); err != nil {
		t.Fatalf("PutNodeVersion(100): %v", err)
	}

	n300 := types.NewNode(types.NodeID(snowflake.ID(300)), 1, nil)
	_ = n300.SetProperty("v", int64(1))
	if err := bs.PutNodeVersion(types.NodeID(300), 0, n300); err != nil {
		t.Fatalf("PutNodeVersion(300): %v", err)
	}

	ids, err = bs.AllNodeHistoryIDs()
	if err != nil {
		t.Fatalf("AllNodeHistoryIDs: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 history IDs, got %d", len(ids))
	}

	// IDs should be sorted.
	if ids[0] >= ids[1] {
		t.Fatalf("IDs not sorted: %d >= %d", ids[0], ids[1])
	}
}

func TestBadgerStoreAllNodeHistoryIDs_PendingBuffer(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// Add node and version — don't flush, so it's in the pending buffer.
	putTestNode(t, bs, 100, 1, nil)
	n := types.NewNode(types.NodeID(snowflake.ID(100)), 1, nil)
	if err := bs.PutNodeVersion(types.NodeID(100), 0, n); err != nil {
		t.Fatalf("PutNodeVersion: %v", err)
	}

	// Should find it in pending buffer.
	ids, err := bs.AllNodeHistoryIDs()
	if err != nil {
		t.Fatalf("AllNodeHistoryIDs: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 history ID from pending buffer, got %d", len(ids))
	}

	// Flush and verify still found.
	bs.Flush()
	ids, err = bs.AllNodeHistoryIDs()
	if err != nil {
		t.Fatalf("AllNodeHistoryIDs after flush: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 history ID after flush, got %d", len(ids))
	}
}

func TestBadgerStoreHistoryIDPendingOverlayIgnoresOverlongFixedWidthKeys(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	nodeKey := append(append([]byte(nil), storepkg.HistNodeKey(100, 1)...), 0x99)
	relKey := append(append([]byte(nil), storepkg.HistRelKey(500, 1)...), 0x99)

	bs.wbMu.Lock()
	bs.pending[string(nodeKey)] = writeOp{opType: writeOpSet, key: nodeKey}
	bs.pending[string(relKey)] = writeOp{opType: writeOpSet, key: relKey}
	bs.wbMu.Unlock()

	nodeIDs, err := bs.AllNodeHistoryIDs()
	if err != nil {
		t.Fatalf("AllNodeHistoryIDs: %v", err)
	}
	if len(nodeIDs) != 0 {
		t.Fatalf("AllNodeHistoryIDs with overlong pending key = %v, want empty", nodeIDs)
	}
	var iteratedNodes []types.NodeID
	if err := bs.ForEachNodeHistoryID(func(id types.NodeID) bool {
		iteratedNodes = append(iteratedNodes, id)
		return true
	}); err != nil {
		t.Fatalf("ForEachNodeHistoryID: %v", err)
	}
	if len(iteratedNodes) != 0 {
		t.Fatalf("ForEachNodeHistoryID with overlong pending key = %v, want empty", iteratedNodes)
	}

	relIDs, err := bs.AllRelHistoryIDs()
	if err != nil {
		t.Fatalf("AllRelHistoryIDs: %v", err)
	}
	if len(relIDs) != 0 {
		t.Fatalf("AllRelHistoryIDs with overlong pending key = %v, want empty", relIDs)
	}
	var iteratedRels []types.RelID
	if err := bs.ForEachRelHistoryID(func(id types.RelID) bool {
		iteratedRels = append(iteratedRels, id)
		return true
	}); err != nil {
		t.Fatalf("ForEachRelHistoryID: %v", err)
	}
	if len(iteratedRels) != 0 {
		t.Fatalf("ForEachRelHistoryID with overlong pending key = %v, want empty", iteratedRels)
	}
}

func TestBadgerStoreGetHistoryIgnoresOverlongFixedWidthKeys(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	node := types.NewNode(types.NodeID(snowflake.ID(100)), 1, nil)
	nodeData, err := msgpack.Marshal(storepkg.NodeToWire(node))
	if err != nil {
		t.Fatalf("marshal node wire: %v", err)
	}
	nodeExactKey := storepkg.HistNodeKey(100, 0)
	nodePersistedOverlongKey := append(append([]byte(nil), storepkg.HistNodeKey(100, 1)...), 0x99)
	nodePendingOverlongKey := append(append([]byte(nil), storepkg.HistNodeKey(100, 2)...), 0x99)

	rel := types.NewRelationship(types.RelID(snowflake.ID(500)), 1, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	relData, err := msgpack.Marshal(storepkg.RelToWire(rel))
	if err != nil {
		t.Fatalf("marshal rel wire: %v", err)
	}
	relExactKey := storepkg.HistRelKey(500, 0)
	relPersistedOverlongKey := append(append([]byte(nil), storepkg.HistRelKey(500, 1)...), 0x99)
	relPendingOverlongKey := append(append([]byte(nil), storepkg.HistRelKey(500, 2)...), 0x99)

	if err := bs.db.Update(func(txn *badgerv4.Txn) error {
		if err := txn.Set(nodeExactKey, nodeData); err != nil {
			return err
		}
		if err := txn.Set(nodePersistedOverlongKey, nodeData); err != nil {
			return err
		}
		if err := txn.Set(relExactKey, relData); err != nil {
			return err
		}
		return txn.Set(relPersistedOverlongKey, relData)
	}); err != nil {
		t.Fatalf("seed raw history keys: %v", err)
	}

	bs.wbMu.Lock()
	bs.pending[string(nodePendingOverlongKey)] = writeOp{opType: writeOpSet, key: nodePendingOverlongKey, value: nodeData}
	bs.pending[string(relPendingOverlongKey)] = writeOp{opType: writeOpSet, key: relPendingOverlongKey, value: relData}
	bs.wbMu.Unlock()

	nodeHistory, err := bs.GetNodeHistory(types.NodeID(100))
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(nodeHistory) != 1 {
		t.Fatalf("GetNodeHistory with overlong keys returned %d entries, want 1", len(nodeHistory))
	}

	relHistory, err := bs.GetRelHistory(types.RelID(500))
	if err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	}
	if len(relHistory) != 1 {
		t.Fatalf("GetRelHistory with overlong keys returned %d entries, want 1", len(relHistory))
	}
}

func TestBadgerStoreAllNodeHistoryIDsHonorsPendingDeletes(t *testing.T) {
	t.Parallel()
	bs := newSlowFlushBadgerStore(t)

	n100 := types.NewNode(types.NodeID(snowflake.ID(100)), 1, nil)
	if err := bs.PutNodeVersion(types.NodeID(100), 0, n100); err != nil {
		t.Fatalf("PutNodeVersion(100): %v", err)
	}
	n300v0 := types.NewNode(types.NodeID(snowflake.ID(300)), 1, nil)
	n300v0.SetVersion(0)
	if err := bs.PutNodeVersion(types.NodeID(300), 0, n300v0); err != nil {
		t.Fatalf("PutNodeVersion(300,0): %v", err)
	}
	n300v1 := types.NewNode(types.NodeID(snowflake.ID(300)), 1, nil)
	n300v1.SetVersion(1)
	if err := bs.PutNodeVersion(types.NodeID(300), 1, n300v1); err != nil {
		t.Fatalf("PutNodeVersion(300,1): %v", err)
	}
	n500 := types.NewNode(types.NodeID(snowflake.ID(500)), 1, nil)
	if err := bs.PutNodeVersion(types.NodeID(500), 0, n500); err != nil {
		t.Fatalf("PutNodeVersion(500): %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if err := bs.TruncateNodeHistory(types.NodeID(100), 0); err != nil {
		t.Fatalf("TruncateNodeHistory(100,0): %v", err)
	}
	if err := bs.TruncateNodeHistory(types.NodeID(300), 1); err != nil {
		t.Fatalf("TruncateNodeHistory(300,1): %v", err)
	}
	if err := bs.TruncateNodeHistory(types.NodeID(500), 0); err != nil {
		t.Fatalf("TruncateNodeHistory(500,0): %v", err)
	}

	ids, err := bs.AllNodeHistoryIDs()
	if err != nil {
		t.Fatalf("AllNodeHistoryIDs: %v", err)
	}
	if len(ids) != 1 || ids[0] != types.NodeID(300) {
		t.Fatalf("AllNodeHistoryIDs after pending truncate = %v, want [300]", ids)
	}

	var iterated []types.NodeID
	if err := bs.ForEachNodeHistoryID(func(id types.NodeID) bool {
		iterated = append(iterated, id)
		return true
	}); err != nil {
		t.Fatalf("ForEachNodeHistoryID: %v", err)
	}
	if len(iterated) != 1 || iterated[0] != types.NodeID(300) {
		t.Fatalf("ForEachNodeHistoryID after pending truncate = %v, want [300]", iterated)
	}

	iterated = nil
	if err := bs.ForEachNodeHistoryID(func(id types.NodeID) bool {
		iterated = append(iterated, id)
		if id == types.NodeID(300) {
			created := types.NewNode(types.NodeID(snowflake.ID(400)), 1, nil)
			if err := bs.PutNodeVersion(created.ID(), 0, created); err != nil {
				t.Errorf("PutNodeVersion callback-created history: %v", err)
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatalf("ForEachNodeHistoryID with callback-created history: %v", err)
	}
	if len(iterated) != 1 || iterated[0] != types.NodeID(300) {
		t.Fatalf("ForEachNodeHistoryID visited callback-created history after stale pending delete max = %v, want [300]", iterated)
	}
}

func TestBadgerStoreAllRelHistoryIDs(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	// No history yet.
	ids, err := bs.AllRelHistoryIDs()
	if err != nil {
		t.Fatalf("AllRelHistoryIDs: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected 0 history IDs, got %d", len(ids))
	}

	// Add rels and create history versions.
	putTestNode(t, bs, 1, 1, nil)
	putTestNode(t, bs, 2, 1, nil)
	putTestRel(t, bs, 100, 1, 1, 2)
	putTestRel(t, bs, 200, 1, 1, 2)

	r100 := types.NewRelationship(types.RelID(snowflake.ID(100)), 1, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	if err := bs.PutRelVersion(types.RelID(100), 0, r100); err != nil {
		t.Fatalf("PutRelVersion(100): %v", err)
	}

	ids, err = bs.AllRelHistoryIDs()
	if err != nil {
		t.Fatalf("AllRelHistoryIDs: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 history ID, got %d", len(ids))
	}
}

func TestBadgerStoreAllRelHistoryIDs_PendingBuffer(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	putTestNode(t, bs, 1, 1, nil)
	putTestNode(t, bs, 2, 1, nil)
	putTestRel(t, bs, 100, 1, 1, 2)

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 1, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	if err := bs.PutRelVersion(types.RelID(100), 0, r); err != nil {
		t.Fatalf("PutRelVersion: %v", err)
	}

	// Should find in pending buffer before flush.
	ids, err := bs.AllRelHistoryIDs()
	if err != nil {
		t.Fatalf("AllRelHistoryIDs: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 history ID from pending, got %d", len(ids))
	}

	bs.Flush()
	ids, err = bs.AllRelHistoryIDs()
	if err != nil {
		t.Fatalf("AllRelHistoryIDs after flush: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 history ID after flush, got %d", len(ids))
	}
}

func TestBadgerStoreAllRelHistoryIDsHonorsPendingDeletes(t *testing.T) {
	t.Parallel()
	bs := newSlowFlushBadgerStore(t)

	r100 := types.NewRelationship(types.RelID(snowflake.ID(100)), 1, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	if err := bs.PutRelVersion(types.RelID(100), 0, r100); err != nil {
		t.Fatalf("PutRelVersion(100): %v", err)
	}
	r300v0 := types.NewRelationship(types.RelID(snowflake.ID(300)), 1, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	r300v0.SetVersion(0)
	if err := bs.PutRelVersion(types.RelID(300), 0, r300v0); err != nil {
		t.Fatalf("PutRelVersion(300,0): %v", err)
	}
	r300v1 := types.NewRelationship(types.RelID(snowflake.ID(300)), 1, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	r300v1.SetVersion(1)
	if err := bs.PutRelVersion(types.RelID(300), 1, r300v1); err != nil {
		t.Fatalf("PutRelVersion(300,1): %v", err)
	}
	r500 := types.NewRelationship(types.RelID(snowflake.ID(500)), 1, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	if err := bs.PutRelVersion(types.RelID(500), 0, r500); err != nil {
		t.Fatalf("PutRelVersion(500): %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if err := bs.TruncateRelHistory(types.RelID(100), 0); err != nil {
		t.Fatalf("TruncateRelHistory(100,0): %v", err)
	}
	if err := bs.TruncateRelHistory(types.RelID(300), 1); err != nil {
		t.Fatalf("TruncateRelHistory(300,1): %v", err)
	}
	if err := bs.TruncateRelHistory(types.RelID(500), 0); err != nil {
		t.Fatalf("TruncateRelHistory(500,0): %v", err)
	}

	ids, err := bs.AllRelHistoryIDs()
	if err != nil {
		t.Fatalf("AllRelHistoryIDs: %v", err)
	}
	if len(ids) != 1 || ids[0] != types.RelID(300) {
		t.Fatalf("AllRelHistoryIDs after pending truncate = %v, want [300]", ids)
	}

	var iterated []types.RelID
	if err := bs.ForEachRelHistoryID(func(id types.RelID) bool {
		iterated = append(iterated, id)
		return true
	}); err != nil {
		t.Fatalf("ForEachRelHistoryID: %v", err)
	}
	if len(iterated) != 1 || iterated[0] != types.RelID(300) {
		t.Fatalf("ForEachRelHistoryID after pending truncate = %v, want [300]", iterated)
	}

	iterated = nil
	if err := bs.ForEachRelHistoryID(func(id types.RelID) bool {
		iterated = append(iterated, id)
		if id == types.RelID(300) {
			created := types.NewRelationship(types.RelID(snowflake.ID(400)), 1, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
			if err := bs.PutRelVersion(created.ID(), 0, created); err != nil {
				t.Errorf("PutRelVersion callback-created history: %v", err)
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatalf("ForEachRelHistoryID with callback-created history: %v", err)
	}
	if len(iterated) != 1 || iterated[0] != types.RelID(300) {
		t.Fatalf("ForEachRelHistoryID visited callback-created history after stale pending delete max = %v, want [300]", iterated)
	}
}

func corruptNodeHistoryWireAfterFlush(t *testing.T, bs *Store, version uint32, w storepkg.NodeWire) {
	t.Helper()
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	data, err := msgpack.Marshal(w)
	if err != nil {
		t.Fatalf("marshal corrupt node history wire: %v", err)
	}
	key := storepkg.HistNodeKey(snowflake.ID(w.ID), uint64(version))
	if err := bs.db.Update(func(txn *badgerv4.Txn) error {
		return txn.Set(key, data)
	}); err != nil {
		t.Fatalf("corrupt node history wire: %v", err)
	}
}

func corruptRelHistoryWireAfterFlush(t *testing.T, bs *Store, version uint32, w storepkg.RelWire) {
	t.Helper()
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	data, err := msgpack.Marshal(w)
	if err != nil {
		t.Fatalf("marshal corrupt rel history wire: %v", err)
	}
	key := storepkg.HistRelKey(snowflake.ID(w.ID), uint64(version))
	if err := bs.db.Update(func(txn *badgerv4.Txn) error {
		return txn.Set(key, data)
	}); err != nil {
		t.Fatalf("corrupt rel history wire: %v", err)
	}
}

func badgerNodeHistoryVersions(history []*types.Node) []uint32 {
	versions := make([]uint32, len(history))
	for i, n := range history {
		versions[i] = n.Version()
	}
	return versions
}

func badgerRelHistoryVersions(history []*types.Relationship) []uint32 {
	versions := make([]uint32, len(history))
	for i, r := range history {
		versions[i] = r.Version()
	}
	return versions
}
