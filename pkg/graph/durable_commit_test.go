package graph_test

import (
	"context"
	"errors"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
)

// Config.DurableCommit (backlog item 11, ai-soc request 9). The crash harness
// lives in durable_commit_crash_test.go.

func init() {
	durableCommitHook = func(c *graphpkg.Config) { c.DurableCommit = true }
}

var errInjectedFlush = errors.New("injected durable-flush failure")

// DurableFlush counts the graph's durable flushes and fails while failDurable
// is positive — WITHOUT touching the inner store, so the group stays pending
// exactly as a failed badger flush leaves it (requeued).
func (s *flushSpyStore) DurableFlush() error {
	s.durableFlushes.Add(1)
	if s.failDurable.Load() > 0 {
		s.failDurable.Add(-1)
		return errInjectedFlush
	}
	return s.Store.DurableFlush()
}

var _ storepkg.DurableFlushCapability = (*flushSpyStore)(nil)

// newDurableSpyGraph opens a badger dir store whose background flush is an hour
// away, wrapped in the flush spy.
func newDurableSpyGraph(t *testing.T, durable bool) (*graphpkg.Graph, *flushSpyStore) {
	t.Helper()
	bs, err := badger.New(badger.Config{Dir: t.TempDir(), FlushInterval: time.Hour})
	if err != nil {
		t.Fatalf("badger.New: %v", err)
	}
	spy := &flushSpyStore{Store: bs}
	g, err := graphpkg.New(graphpkg.Config{Store: spy, DurableCommit: durable})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g, spy
}

// Catches: GraphTx.Commit / Tx().Run / Batch.Execute returning success while the
// group is still only in the pending buffer — a crash right after the return
// loses it. The ingest subtest is a regression guard: that door already flushed
// before the feature (its fsync is proven by TestDurableFlush_Badger_*).
func TestDurableCommit_CrashAfterReturnKeepsWholeGroup_Badger(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns crash children")
	}
	for _, door := range []string{"tx", "run", "batch", "ingest"} {
		t.Run(door, func(t *testing.T) {
			nodes, rels := dcCrashCounts(t, "badger", door, true)
			if nodes != dcSignals+2 || rels != 1 {
				t.Fatalf("DurableCommit: %d nodes, %d rels on disk after a crash after %s returned, want %d/1", nodes, rels, door, dcSignals+2)
			}
		})
	}
}

// Catches: a tiered durable flush that reaches only one shard (the reference
// shard, or only the hot event shard). The group writes a Ref node (reference
// shard) and Signal/Cut nodes (hot event shard).
func TestDurableCommit_CrashAfterReturnKeepsWholeGroup_TieredTwoShards(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns crash children")
	}
	for _, door := range []string{"tx", "batch"} {
		t.Run(door, func(t *testing.T) {
			nodes, rels := dcCrashCounts(t, "tiered", door, true)
			if nodes != dcSignals+2 || rels != 1 {
				t.Fatalf("tiered DurableCommit: %d nodes, %d rels on disk after the crash, want %d/1 (both shards durable)", nodes, rels, dcSignals+2)
			}
		})
	}
}

// Catches: a sharded durable flush that reaches only the anchor slot. Nodes are
// minted in slot 0, the relationship in slot 1.
func TestDurableCommit_CrashAfterReturnKeepsWholeGroup_ShardedTwoSlots(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns crash children")
	}
	for _, door := range []string{"tx", "batch"} {
		t.Run(door, func(t *testing.T) {
			nodes, rels := dcCrashCounts(t, "sharded", door, true)
			if nodes != dcSignals+2 || rels != 1 {
				t.Fatalf("sharded DurableCommit: %d nodes, %d rels on disk after the crash, want %d/1 (both slots durable)", nodes, rels, dcSignals+2)
			}
		})
	}
}

// Catches: a flush per mutation (N+3 calls instead of one), no flush at all, and
// a door that skips the flush (RunContext, RunWithLSN, ingest Submit).
func TestDurableCommit_OneDurableFlushPerGroup(t *testing.T) {
	g, spy := newDurableSpyGraph(t, true)
	ctx := context.Background()
	doors := []struct {
		name string
		run  func() error
	}{
		{"tx", func() error { return dcWriteGroup(g, "tx") }},
		{"run", func() error { return dcWriteGroup(g, "run") }},
		{"batch", func() error { return dcWriteGroup(g, "batch") }},
		{"ingest", func() error { return dcWriteGroup(g, "ingest") }},
		{"run-context", func() error {
			return g.Tx().RunContext(ctx, func(tx *graphpkg.GraphTx) error {
				_, err := tx.AddNode([]string{"Signal"}, nil)
				return err
			})
		}},
		{"run-with-lsn", func() error {
			_, err := g.Tx().RunWithLSN(func(tx *graphpkg.GraphTx) error {
				_, err := tx.AddNode([]string{"Signal"}, nil)
				return err
			})
			return err
		}},
	}
	for _, d := range doors {
		before := spy.durableFlushes.Load()
		if err := d.run(); err != nil {
			t.Fatalf("%s: %v", d.name, err)
		}
		if got := spy.durableFlushes.Load() - before; got != 1 {
			t.Errorf("%s: %d durable flushes for one group, want exactly 1", d.name, got)
		}
		if n := spy.Store.PendingWriteCount(); n != 0 {
			t.Errorf("%s: %d writes still pending after the door returned under DurableCommit", d.name, n)
		}
	}
}

// Catches: a durable flush issued although the flag is off (the default must not
// add a flush or an fsync to any door).
func TestDurableCommit_OffNeverDurableFlushes(t *testing.T) {
	g, spy := newDurableSpyGraph(t, false)
	for _, door := range []string{"tx", "run", "batch", "ingest"} {
		if err := dcWriteGroup(g, door); err != nil {
			t.Fatalf("%s: %v", door, err)
		}
	}
	if n := spy.durableFlushes.Load(); n != 0 {
		t.Fatalf("flag off: %d durable flushes, want 0", n)
	}
}

// Catches: a Rollback (explicit, or Run's on a callback error) that flushes —
// it would write the rolled-back churn to disk on the caller's latency.
func TestDurableCommit_RollbackDoesNotFlush(t *testing.T) {
	g, spy := newDurableSpyGraph(t, true)
	tx, err := g.Tx().Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := tx.AddNode([]string{"Signal"}, map[string]any{"i": int64(1)}); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	errFn := errors.New("callback refuses")
	err = g.Tx().Run(func(tx *graphpkg.GraphTx) error {
		if _, err := tx.AddNode([]string{"Signal"}, nil); err != nil {
			return err
		}
		return errFn
	})
	if !errors.Is(err, errFn) {
		t.Fatalf("Run: got %v, want the callback error", err)
	}
	if n := spy.durableFlushes.Load(); n != 0 {
		t.Fatalf("%d durable flushes from Rollback / failed Run, want 0", n)
	}
	if spy.Store.PendingWriteCount() == 0 {
		t.Fatal("pending buffer empty after rollbacks — something flushed")
	}
}

// Catches: a swallowed flush error (Commit reports success for a group that is
// not on disk), a panic or leaked txMu on the error path, a Commit that leaves
// the tx open after its change-log records were minted, and a failure path that
// drops the buffered group instead of keeping it for the next flush.
func TestDurableCommit_FlushErrorSurfacesAndGroupStaysPending(t *testing.T) {
	g, spy := newDurableSpyGraph(t, true)

	t.Run("tx", func(t *testing.T) {
		tx, err := g.Tx().Begin()
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		if _, err := tx.AddNode([]string{"Signal"}, map[string]any{"i": int64(1)}); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		spy.failDurable.Store(1)
		err = tx.Commit()
		if !errors.Is(err, graphpkg.ErrCommitNotDurable) || !errors.Is(err, errInjectedFlush) {
			t.Fatalf("Commit: got %v, want ErrCommitNotDurable wrapping the store error", err)
		}
		if err := tx.Rollback(); !errors.Is(err, graphpkg.ErrTxDone) {
			t.Fatalf("Rollback after a not-durable Commit: got %v, want ErrTxDone (the group is committed)", err)
		}
		assertPendingThenRetried(t, g, spy)
	})

	t.Run("run", func(t *testing.T) {
		spy.failDurable.Store(1)
		err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
			_, err := tx.AddNode([]string{"Signal"}, nil)
			return err
		})
		if !errors.Is(err, graphpkg.ErrCommitNotDurable) || !errors.Is(err, errInjectedFlush) {
			t.Fatalf("Run: got %v, want ErrCommitNotDurable wrapping the store error", err)
		}
		assertPendingThenRetried(t, g, spy)
	})

	t.Run("batch", func(t *testing.T) {
		spy.failDurable.Store(1)
		_, err := g.Batch().Run(func(bb *graphpkg.BatchBuilder) error {
			_, err := bb.AddNode([]string{"Signal"}, nil)
			return err
		})
		if !errors.Is(err, graphpkg.ErrCommitNotDurable) || !errors.Is(err, errInjectedFlush) {
			t.Fatalf("Batch.Execute: got %v, want ErrCommitNotDurable wrapping the store error", err)
		}
		assertPendingThenRetried(t, g, spy)
	})

	t.Run("ingest", func(t *testing.T) {
		s, err := g.Ingest().NewSession(ingest.IngestOptions{Sync: true, DeclareLabels: []string{"Signal"}})
		if err != nil {
			t.Fatalf("NewSession: %v", err)
		}
		defer func() { _ = s.Close() }()
		if _, err := s.AddNode([]string{"Signal"}, nil); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		spy.failDurable.Store(1)
		_, err = s.Submit()
		if !errors.Is(err, graphpkg.ErrCommitNotDurable) || !errors.Is(err, errInjectedFlush) {
			t.Fatalf("Submit: got %v, want ErrCommitNotDurable wrapping the store error", err)
		}
		// The ingest group already went through EndGroupCommit's WriteBatch; the
		// failed step is the fsync, so only the retry is asserted.
		if err := runWithTimeout(func() error { return g.Tx().Run(func(*graphpkg.GraphTx) error { return nil }) }); err != nil {
			t.Fatalf("retry after a not-durable Submit: %v", err)
		}
	})
}

// assertPendingThenRetried checks the failed group is visible in memory and
// still buffered, then that the next (empty) commit drains it.
func assertPendingThenRetried(t *testing.T, g *graphpkg.Graph, spy *flushSpyStore) {
	t.Helper()
	if spy.Store.PendingWriteCount() == 0 {
		t.Fatal("the not-durable group left the pending buffer — it was dropped, not kept for the next flush")
	}
	if n, err := g.Nodes().Count(); err != nil || n == 0 {
		t.Fatalf("Nodes().Count after a not-durable commit: %d, %v — the committed group must stay visible", n, err)
	}
	if err := runWithTimeout(func() error { return g.Tx().Run(func(*graphpkg.GraphTx) error { return nil }) }); err != nil {
		t.Fatalf("retry commit: %v", err)
	}
	if n := spy.Store.PendingWriteCount(); n != 0 {
		t.Fatalf("%d writes still pending after the retry commit, want 0", n)
	}
}

// runWithTimeout fails fast instead of hanging when a lock leaked.
func runWithTimeout(fn func() error) error {
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		return errors.New("timed out: a graph lock leaked on the not-durable path")
	}
}

// Catches: accepting DurableCommit on a store with no stable storage (a silent
// no-op would break the flag's promise), and the inverse — declining a disk store.
func TestDurableCommit_NewDeclinesStoreWithoutStableStorage(t *testing.T) {
	inMemBadger, err := badger.New(badger.Config{InMemory: true})
	if err != nil {
		t.Fatalf("badger in-memory: %v", err)
	}
	t.Cleanup(func() { _ = inMemBadger.Close() })
	inMemSharded, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
	if err != nil {
		t.Fatalf("sharded in-memory: %v", err)
	}
	t.Cleanup(func() { _ = inMemSharded.Close() })
	inMemTiered, err := tiered.New(tiered.Config{InMemory: true, RefLabels: []string{"Ref"}})
	if err != nil {
		t.Fatalf("tiered in-memory: %v", err)
	}
	t.Cleanup(func() { _ = inMemTiered.Close() })

	declined := []struct {
		name string
		cfg  graphpkg.Config
	}{
		{"default-memory", graphpkg.Config{DurableCommit: true}},
		{"memory-store", graphpkg.Config{Store: memory.New(), DurableCommit: true}},
		{"badger-in-memory-flag", graphpkg.Config{BadgerInMemory: true, DurableCommit: true}},
		{"badger-in-memory-store", graphpkg.Config{Store: inMemBadger, DurableCommit: true}},
		{"sharded-in-memory", graphpkg.Config{Store: inMemSharded, DurableCommit: true}},
		{"tiered-in-memory", graphpkg.Config{Store: inMemTiered, DurableCommit: true}},
	}
	for _, c := range declined {
		t.Run(c.name, func(t *testing.T) {
			g, err := graphpkg.New(c.cfg)
			if !errors.Is(err, graphpkg.ErrCapabilityNotSupported) {
				if g != nil {
					_ = g.Close()
				}
				t.Fatalf("New: got %v, want ErrCapabilityNotSupported", err)
			}
			if g != nil {
				t.Fatal("New returned a graph together with the decline")
			}
		})
	}
	t.Run("badger-dir-accepted", func(t *testing.T) {
		g, err := graphpkg.New(graphpkg.Config{BadgerDir: t.TempDir(), DurableCommit: true})
		if err != nil {
			t.Fatalf("New with a disk badger store: %v", err)
		}
		_ = g.Close()
	})
}
