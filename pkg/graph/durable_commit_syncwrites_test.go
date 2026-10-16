package graph_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
)

// Backlog 52 (ai-soc): Config.DurableCommit + SyncWrites paid one Badger sync
// per mutation (~822 per 530-op group, ~1 s). A durable group must cost a
// bounded number of syncs, independent of its size, and keep strict power-loss
// durability (every sync is a Badger SyncWrites commit). Counted through
// badger.Store.SyncCountForTest, not wall time.

const (
	scSolo      = 40   // standalone writes racing an open durable group
	scBigNodes  = 400  // x 4 KiB: above Badger's transaction size limit at an 8 MiB memtable
	scBigValue  = 4096 // bytes per big node
	scMaxGroup  = 2    // syncs one durable group may cost
	scBoundRows = 300  // group size for the held-buffer bound (MaxPendingWrites 16)
)

func init() {
	// A group of big nodes: the commit's WriteBatch exceeds Badger's
	// transaction size limit, so Badger splits it.
	dcExtraDoors["tx-big"] = func(g *graphpkg.Graph, _ string) error {
		return g.Tx().Run(func(tx *graphpkg.GraphTx) error {
			return scAddNodes(tx, "Big", scBigNodes, strings.Repeat("x", scBigValue))
		})
	}
	// A group larger than the held-buffer bound, rolled back: part of it was
	// flushed mid-group, so the rollback must make its restores durable.
	dcExtraDoors["tx-rollback"] = func(g *graphpkg.Graph, _ string) error {
		tx, err := g.Tx().Begin()
		if err != nil {
			return err
		}
		if err := scAddNodes(tx, "Signal", scBoundRows, "v"); err != nil {
			return err
		}
		return tx.Rollback()
	}
	// Standalone writes race an open durable group; the child crashes with the
	// group still open. Every standalone write returned, so each must be on disk.
	dcExtraDoors["tx-concurrent"] = func(g *graphpkg.Graph, _ string) error {
		tx, err := g.Tx().Begin()
		if err != nil {
			return err
		}
		done := make(chan error, 1)
		go func() {
			for i := 0; i < scSolo; i++ {
				if _, err := g.Nodes().Add(context.Background(), []string{"Solo"}, map[string]any{"i": int64(i)}); err != nil {
					done <- err
					return
				}
			}
			done <- nil
		}()
		for {
			select {
			case err := <-done:
				return err // crash with the group open
			default:
				if _, err := tx.AddNode([]string{"Signal"}, map[string]any{"v": "v"}); err != nil {
					return err
				}
			}
		}
	}
}

// scAddNodes adds n nodes with one string property to tx.
func scAddNodes(tx *graphpkg.GraphTx, label string, n int, v string) error {
	for i := 0; i < n; i++ {
		if _, err := tx.AddNode([]string{label}, map[string]any{"i": int64(i), "v": v}); err != nil {
			return err
		}
	}
	return nil
}

// scGroup is the ai-soc group shape: per record two keyed endpoint upserts and
// one relationship, ceil(ops/3) records (seq keeps each group's keys fresh).
func scGroup(tx *graphpkg.GraphTx, seq, ops int) error {
	for r := 0; r < (ops+2)/3; r++ {
		k := fmt.Sprintf("%d-%d", seq, r)
		dev, _, err := tx.GetOrCreateByKey("Device", "key", "dev-"+k, map[string]any{"estate": "BA"})
		if err != nil {
			return err
		}
		acct, _, err := tx.GetOrCreateByKey("Account", "key", "acct-"+k, map[string]any{"estate": "BA"})
		if err != nil {
			return err
		}
		if _, err := tx.AddRelationshipByID("LOGON", acct.ID(), dev.ID(), map[string]any{"record": k, "tkg_valid_from": int64(1_790_700_000_000 + r)}); err != nil {
			return err
		}
	}
	return nil
}

// newSyncWritesGraph opens a SyncWrites badger store and the graph over it, and
// commits one warm-up group so the registries hold every token (a registry
// checkpoint is not a row sync, but keeps the counts comparable).
func newSyncWritesGraph(t *testing.T, durable bool, bc badger.Config) (*graphpkg.Graph, *badger.Store) {
	t.Helper()
	bc.Dir, bc.SyncWrites = t.TempDir(), true
	bs, err := badger.New(bc)
	if err != nil {
		t.Fatalf("badger.New: %v", err)
	}
	g, err := graphpkg.New(graphpkg.Config{Store: bs, DurableCommit: durable})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	for _, l := range []string{"Device", "Account"} {
		if err := g.Constraints().CreateUnique(context.Background(), l, "key"); err != nil {
			t.Fatalf("CreateUnique: %v", err)
		}
	}
	if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
		if err := scGroup(tx, -1, 3); err != nil {
			return err
		}
		return scAddNodes(tx, "Signal", 1, "v")
	}); err != nil {
		t.Fatalf("warm-up group: %v", err)
	}
	return g, bs
}

// syncsOf returns the syncs fn cost on bs.
func syncsOf(bs *badger.Store, fn func() error) (int64, error) {
	before := bs.SyncCountForTest()
	err := fn()
	return bs.SyncCountForTest() - before, err
}

func TestDurableCommitSyncWritesSyncsPerGroup(t *testing.T) {
	if testing.Short() && !isRaceEnabled() {
		t.Skip("long tier: release gate")
	}
	type door func(g *graphpkg.Graph, seq, ops int) error
	commit := func(g *graphpkg.Graph, seq, ops int) error {
		return g.Tx().Run(func(tx *graphpkg.GraphTx) error { return scGroup(tx, seq, ops) })
	}
	batch := func(g *graphpkg.Graph, seq, ops int) error {
		_, err := g.Batch().Run(func(bb *graphpkg.BatchBuilder) error {
			for i := 0; i < (ops+1)/2; i++ { // an add and an update per node
				n, err := bb.AddNode([]string{"Signal"}, map[string]any{"i": int64(i)})
				if err != nil {
					return err
				}
				if err := bb.UpdateNode(n.ID(), map[string]any{"u": int64(seq)}); err != nil {
					return err
				}
			}
			return nil
		})
		return err
	}
	rollback := func(g *graphpkg.Graph, seq, ops int) error {
		tx, err := g.Tx().Begin()
		if err != nil {
			return err
		}
		if err := scGroup(tx, seq, ops); err != nil {
			return err
		}
		return tx.Rollback()
	}

	// Syncs per group at 10 and at 500 operations: [lo, hi] each.
	cases := []struct {
		name    string
		durable bool
		bc      badger.Config
		run     door
		lo, hi  func(ops int) int64
	}{
		// Catches: one sync per mutation inside a durable group (the bug).
		{"tx", true, badger.Config{}, commit, fixed(1), fixed(scMaxGroup)},
		{"batch", true, badger.Config{}, batch, fixed(1), fixed(scMaxGroup)},
		// Catches: a rollback that syncs every restore, or none at all.
		{"rollback", true, badger.Config{}, rollback, fixed(1), fixed(scMaxGroup)},
		// Catches: a held buffer that grows without bound under SyncWrites (it
		// must flush in steps at MaxPendingWrites), or that flushes per mutation.
		{"held-buffer-bound", true, badger.Config{MaxPendingWrites: 16}, commit, func(ops int) int64 { return max(1, int64(ops/100)) }, func(ops int) int64 { return int64(ops) - 1 }},
		// Catches: a change to SyncWrites without DurableCommit (every mutation
		// still syncs on return).
		{"sync-writes-only", false, badger.Config{}, commit, func(ops int) int64 { return int64(ops) }, fixed(1 << 30)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			g, bs := newSyncWritesGraph(t, c.durable, c.bc)
			for i, ops := range []int{10, 500} {
				n, err := syncsOf(bs, func() error { return c.run(g, i, ops) })
				if err != nil {
					t.Fatalf("%d ops: %v", ops, err)
				}
				t.Logf("%d ops: %d syncs", ops, n)
				if n < c.lo(ops) || n > c.hi(ops) {
					t.Errorf("%d ops: %d syncs, want [%d, %d]", ops, n, c.lo(ops), c.hi(ops))
				}
				if c.durable && bs.PendingWriteCount() != 0 {
					t.Errorf("%d ops: %d writes pending after the durable door returned", ops, bs.PendingWriteCount())
				}
			}
		})
	}

	// Catches: a hold release that outlives its holder's lock — Batch.Execute's
	// deferred release ran after its unlock and durable flush, so it could end
	// the hold of the GraphTx group that took the lock next (whose mutations
	// then each sync) — and a double release. Every release must end a live
	// hold; a batch racing tx groups costs at most one sync per group.
	t.Run("hold-release-pairs", func(t *testing.T) {
		bs, err := badger.New(badger.Config{Dir: t.TempDir(), SyncWrites: true})
		if err != nil {
			t.Fatalf("badger.New: %v", err)
		}
		spy := &holdSpyStore{Store: bs}
		g, err := graphpkg.New(graphpkg.Config{Store: spy, DurableCommit: true})
		if err != nil {
			t.Fatalf("graph.New: %v", err)
		}
		t.Cleanup(func() { _ = g.Close() })
		const groups = 40
		before := bs.SyncCountForTest()
		var wg sync.WaitGroup
		errs := make(chan error, 3)
		for _, d := range []door{batch, commit, rollback} {
			wg.Add(1)
			go func(d door) {
				defer wg.Done()
				for i := 0; i < groups; i++ {
					if err := d(g, i, 30); err != nil {
						errs <- err
						return
					}
				}
			}(d)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
		if n := spy.stray.Load(); n != 0 {
			t.Errorf("%d hold releases without a live hold (after the holder's unlock, or twice)", n)
		}
		if n := bs.SyncCountForTest() - before; n > 3*groups {
			t.Errorf("%d durable groups racing: %d syncs, want <= %d (one per group)", 3*groups, n, 3*groups)
		}
	})

	// Catches: a group above Badger's transaction size limit failing
	// (ErrTxnTooBig) or costing a sync per mutation.
	t.Run("above-txn-size-limit", func(t *testing.T) {
		g, bs := newSyncWritesGraph(t, true, badger.Config{MemTableSize: 8 << 20})
		n, err := syncsOf(bs, func() error { return dcExtraDoors["tx-big"](g, "") })
		if err != nil || n < 1 || n > scMaxGroup {
			t.Fatalf("group above the txn size limit: %d syncs, err %v; want [1, %d], nil", n, err, scMaxGroup)
		}
	})

	// Catches: a failed commit flush that drops the held group, reports
	// success, or syncs per mutation; the next durable group must persist it.
	t.Run("not-durable-retry", func(t *testing.T) {
		g, bs := newSyncWritesGraph(t, true, badger.Config{})
		bs.FailNextFlushForTest(errInjectedFlush)
		n, err := syncsOf(bs, func() error { return commit(g, 1, 500) })
		if !errors.Is(err, graphpkg.ErrCommitNotDurable) || n != 0 {
			t.Fatalf("failing flush: err %v, %d syncs; want ErrCommitNotDurable, 0", err, n)
		}
		if bs.PendingWriteCount() == 0 {
			t.Fatal("failing flush: the group was dropped from the pending buffer")
		}
		n, err = syncsOf(bs, func() error { return g.Tx().Run(func(*graphpkg.GraphTx) error { return nil }) })
		if err != nil || n < 1 || n > scMaxGroup || bs.PendingWriteCount() != 0 {
			t.Fatalf("retry: err %v, %d syncs, %d pending; want nil, [1, %d], 0", err, n, bs.PendingWriteCount(), scMaxGroup)
		}
	})

	// Crash children: the child exits without Close right after the door
	// returns; the parent reopens and counts.
	if testing.Short() {
		return
	}
	crashes := []struct {
		name, backend, door string
		label               string
		want                int
	}{
		// Catches: a commit returning before the held group is synced.
		{"tx", "badger-sync", "tx", "Signal", dcSignals},
		{"batch", "badger-sync", "batch", "Signal", dcSignals},
		{"above-txn-size-limit", "badger-sync-small", "tx-big", "Big", scBigNodes},
		// Catches: a rollback whose restores stay pending while part of the
		// group already reached disk (the rolled-back rows come back). All
		// nodes: the rollback also drops the "Signal" label it interned.
		{"rollback", "badger-sync-bound", "tx-rollback", "", 0},
		{"not-durable-no-retry", "badger-sync", "tx-fail", "Signal", 0},
		{"not-durable-retry", "badger-sync", "tx-fail-retry", "Signal", dcSignals},
		// Catches: a hold that also skips the flush of a concurrent standalone
		// write (it returned, so SyncWrites promised it on disk).
		{"concurrent-standalone", "badger-sync", "tx-concurrent", "Solo", scSolo},
	}
	for _, c := range crashes {
		t.Run("crash/"+c.name, func(t *testing.T) {
			g := dcCrashReopen(t, c.backend, c.door, true, "")
			n, err := g.Nodes().Count()
			if c.label != "" {
				n, err = g.Nodes().CountByLabel(c.label)
			}
			if err != nil || n != c.want {
				t.Fatalf("%s after the crash: %d %s nodes, err %v; want %d", c.door, n, c.label, err, c.want)
			}
		})
	}
}

// fixed returns a bound that ignores the operation count.
func fixed(n int64) func(int) int64 { return func(int) int64 { return n } }

// holdSpyStore counts flush-hold releases that end no live hold: the graph
// holds under its exclusive lock, so every release must pair with one hold.
type holdSpyStore struct {
	*badger.Store
	mu    sync.Mutex
	held  bool
	stray atomic.Int64
}

func (s *holdSpyStore) HoldMutationFlushes() {
	s.mu.Lock()
	s.held = true
	s.mu.Unlock()
	s.Store.HoldMutationFlushes()
}

func (s *holdSpyStore) ReleaseMutationFlushes() {
	s.mu.Lock()
	if !s.held {
		s.stray.Add(1)
	}
	s.held = false
	s.mu.Unlock()
	s.Store.ReleaseMutationFlushes()
}
