package tiered

import (
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// coldROFixture is closedColdFixtureWith a retained cold handle
// (MaxOpenColdShards 1), so the handle a read opens stays to be inspected; it
// also returns an ID minted in the cold shard's window, not yet written.
func coldROFixture(t *testing.T) (*Store, *types.Node, types.NodeID, string, uint16) {
	t.Helper()
	cfg := volumeCfg(t.TempDir(), t.TempDir(), "")
	cfg.MaxOpenColdShards = 1
	ts, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	tok, _ := reg.GetOrCreate("Signal")
	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), tok, nil)
	spare := types.NodeID(gen.Generate())
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	old := ts.HotShardForTest().Name()
	forceRotation(t, ts)
	demoteToCold(ts, old)
	es := ts.EventShardsForTest()[old]
	es.LockShardMuForTest()
	if err := es.Store().Close(); err != nil {
		es.UnlockShardMuForTest()
		t.Fatalf("close cold store: %v", err)
	}
	es.SetStoreForTest(nil)
	es.UnlockShardMuForTest()
	return ts, n, spare, old, tok
}

// storeIsRO reports whether the shard's open store is a read-only handle.
func storeIsRO(es *EventShard) (open, ro bool) {
	es.shardMu.Lock()
	defer es.shardMu.Unlock()
	return es.store != nil, es.storeRO
}

// TestColdReadOpensReadOnlyAndWriteUpgrades pins COLDRO: a read of a closed
// cold shard opens it read-only; a checkout by a caller that may write
// (checkoutStoreForRead) and a write each replace the handle with a writable
// one, and the write lands; a later read sees the write. Catches a read that
// still opens writable (28 ms and 94 MB instead of 1 ms and 3 MB) and a write
// refused by, or lost on, the read-only handle.
func TestColdReadOpensReadOnlyAndWriteUpgrades(t *testing.T) {
	t.Parallel()
	ts, n, _, old, tok := coldROFixture(t)
	es := ts.EventShardsForTest()[old]
	if _, err := ts.GetNode(n.ID()); err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if open, ro := storeIsRO(es); !open || !ro {
		t.Fatalf("after a read the cold shard is open %v, read-only %v; want an open read-only handle", open, ro)
	}
	// A checkout by a caller that may write gets a writable store.
	_, release, err := es.checkoutStoreForRead(ts)
	if err != nil {
		t.Fatalf("checkoutStoreForRead: %v", err)
	}
	release()
	if open, ro := storeIsRO(es); !open || ro {
		t.Fatalf("after checkoutStoreForRead the cold shard is open %v, read-only %v; want writable", open, ro)
	}
	if err := es.Store().Close(); err != nil {
		t.Fatal(err)
	}
	es.LockShardMuForTest()
	es.clearStoreLocked(ts)
	es.UnlockShardMuForTest()
	if _, err := ts.GetNode(n.ID()); err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if open, ro := storeIsRO(es); !open || !ro {
		t.Fatalf("after a second read the cold shard is open %v, read-only %v; want read-only", open, ro)
	}
	updated := types.NewNode(n.ID(), tok, nil)
	if err := updated.SetProperty("seen", int64(1)); err != nil {
		t.Fatal(err)
	}
	if err := ts.ReplaceNode(updated); err != nil {
		t.Fatalf("ReplaceNode on the read-only cold shard: %v", err)
	}
	if open, ro := storeIsRO(es); !open || ro {
		t.Fatalf("after a write the cold shard is open %v, read-only %v; want writable", open, ro)
	}
	got, err := ts.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode after the write: %v", err)
	}
	if v, ok := got.GetProperty("seen"); !ok || v != int64(1) {
		t.Fatalf("the write is not visible: seen = %v (%v)", v, ok)
	}
}

// TestColdCreateUpgradesReadOnlyHandle: a create whose ID falls in the window
// of a cold shard a read opened read-only (a backfill) gets a writable handle
// (checkoutStore) and lands there.
func TestColdCreateUpgradesReadOnlyHandle(t *testing.T) {
	t.Parallel()
	ts, n, spare, old, tok := coldROFixture(t)
	es := ts.EventShardsForTest()[old]
	if _, err := ts.GetNode(n.ID()); err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if open, ro := storeIsRO(es); !open || !ro {
		t.Fatalf("after a read the cold shard is open %v, read-only %v; want read-only", open, ro)
	}
	if err := ts.PutNode(types.NewNode(spare, tok, nil)); err != nil {
		t.Fatalf("PutNode into the read-only cold shard's window: %v", err)
	}
	if open, ro := storeIsRO(es); !open || ro {
		t.Fatalf("after the create the cold shard is open %v, read-only %v; want writable", open, ro)
	}
	if !es.Store().HasNodeID(spare.SnowflakeID()) {
		t.Fatal("the created node is not on the cold shard")
	}
}

// TestColdUpgradeWaitsForReaders pins that the upgrade closes the read-only
// handle only after its readers finish: a reader holding it keeps reading
// while a write waits, and the write lands once the reader is done.
func TestColdUpgradeWaitsForReaders(t *testing.T) {
	t.Parallel()
	ts, n, _, old, tok := coldROFixture(t)
	es := ts.EventShardsForTest()[old]
	store, unpin, err := es.checkoutStoreReadOnly(ts)
	if err != nil {
		t.Fatalf("checkout: %v", err)
	}
	wrote := make(chan error, 1)
	go func() { wrote <- ts.ReplaceNode(types.NewNode(n.ID(), tok, nil)) }()
	select {
	case err := <-wrote:
		unpin()
		t.Fatalf("the write did not wait for the reader of the read-only handle (err %v)", err)
	case <-time.After(200 * time.Millisecond):
	}
	if _, err := store.GetNode(n.ID()); err != nil {
		unpin()
		t.Fatalf("read from the held handle during the upgrade: %v", err)
	}
	unpin()
	if err := <-wrote; err != nil {
		t.Fatalf("ReplaceNode after the reader finished: %v", err)
	}
}

// TestColdReadOpensWritableWhenIndexesDiffer pins COLDRO's fallback: a cold
// shard whose index definitions differ from the anchor's (an index left on
// the shard alone, or on the anchor alone, as an interrupted DDL leaves one) is
// opened writable by a read, because only a writable open syncs them.
func TestColdReadOpensWritableWhenIndexesDiffer(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name   string
		create func(*BadgerStore, uint16) error
		stray  func(*BadgerStore, uint16) (bool, error)
	}{
		{"property", func(s *BadgerStore, tok uint16) error { return s.CreatePropertyIndex(tok, "stray") },
			func(s *BadgerStore, tok uint16) (bool, error) {
				defs, err := s.PropertyIndexDefs()
				_, ok := defs[tok]["stray"]
				return ok, err
			}},
		{"composite", func(s *BadgerStore, tok uint16) error { return s.CreateCompositePropertyIndex(tok, []string{"a", "b"}) },
			func(s *BadgerStore, tok uint16) (bool, error) {
				defs, err := s.CompositePropertyIndexDefs()
				return len(defs[tok]) > 0, err
			}},
		{"anchor composite", nil, func(s *BadgerStore, tok uint16) (bool, error) {
			defs, err := s.CompositePropertyIndexDefs()
			return len(defs[tok]) == 0, err // the shard lacks the anchor's
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			ts, n, _, old, tok := coldROFixture(t)
			es := ts.EventShardsForTest()[old]
			if c.create == nil {
				// The anchor alone gets an index the closed shard lacks.
				if err := ts.refShard.CreateCompositePropertyIndex(tok, []string{"a", "b"}); err != nil {
					t.Fatalf("index on the anchor alone: %v", err)
				}
			} else {
				store, release, err := es.checkoutStoreForRead(ts)
				if err != nil {
					t.Fatalf("checkout: %v", err)
				}
				if err := c.create(store, tok); err != nil {
					release()
					t.Fatalf("index on the shard alone: %v", err)
				}
				release()
				es.LockShardMuForTest()
				if err := es.store.Close(); err != nil {
					es.UnlockShardMuForTest()
					t.Fatal(err)
				}
				es.clearStoreLocked(ts)
				es.UnlockShardMuForTest()
			}
			if _, err := ts.GetNode(n.ID()); err != nil {
				t.Fatalf("GetNode: %v", err)
			}
			if open, ro := storeIsRO(es); !open || ro {
				t.Fatalf("cold shard open %v, read-only %v; want writable (its indexes needed a sync)", open, ro)
			}
			if stray, err := c.stray(es.Store(), tok); err != nil || stray {
				t.Fatalf("the shard's stray index survived the open (%v)", err)
			}
		})
	}
}
