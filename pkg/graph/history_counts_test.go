package graph_test

import (
	"context"
	"errors"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	adminpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/admin"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// historyIterator is the store's own walk of the IDs with history rows, the
// set HistoryCounts counts.
type historyIterator interface {
	ForEachNodeHistoryID(func(types.NodeID) bool) error
	ForEachRelHistoryID(func(types.RelID) bool) error
}

func walkedHistory(t *testing.T, it historyIterator) storepkg.HistoryCounts {
	t.Helper()
	var c storepkg.HistoryCounts
	if err := it.ForEachNodeHistoryID(func(types.NodeID) bool { c.Nodes++; return true }); err != nil {
		t.Fatal(err)
	}
	if err := it.ForEachRelHistoryID(func(types.RelID) bool { c.Rels++; return true }); err != nil {
		t.Fatal(err)
	}
	return c
}

type historyCountBackend struct {
	name string
	open func(t *testing.T) (graphpkg.Config, historyIterator)
}

func historyCountBackends() []historyCountBackend {
	gates := func(cfg graphpkg.Config) graphpkg.Config {
		cfg.AllowRetentionPurge = true
		cfg.AllowReset = true
		cfg.Validation = graphpkg.ValidationLimits{AllowSelfLoops: true}
		return cfg
	}
	return []historyCountBackend{
		{"memory", func(t *testing.T) (graphpkg.Config, historyIterator) {
			st := memory.New()
			return gates(graphpkg.Config{Store: st}), st
		}},
		{"badger", func(t *testing.T) (graphpkg.Config, historyIterator) {
			st, err := badger.New(badger.Config{InMemory: true, CacheCapacity: 8})
			if err != nil {
				t.Fatal(err)
			}
			return gates(graphpkg.Config{Store: st}), st
		}},
		{"sharded", func(t *testing.T) (graphpkg.Config, historyIterator) {
			st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
			if err != nil {
				t.Fatal(err)
			}
			return gates(graphpkg.Config{Store: st}), st
		}},
	}
}

// HistoryCounts equals the store's own walk of the IDs with history rows
// after every kind of history change: creates (none), updates, label changes,
// closes, deletes (tombstones), relationship updates and deletes, a rolled-back
// transaction's writes, a compaction, a retention purge and a reset. Two-phase:
// a count taken before an update is the old one, the count after it moved.
func TestHistoryCountsEqualTheStoresWalk(t *testing.T) {
	for _, b := range historyCountBackends() {
		t.Run(b.name, func(t *testing.T) {
			cfg, it := b.open(t)
			g, err := graphpkg.New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = g.Close() })
			ctx := context.Background()
			check := func(step string) storepkg.HistoryCounts {
				t.Helper()
				got, ok, err := g.Stats().HistoryCounts()
				if err != nil || !ok {
					t.Fatalf("%s: HistoryCounts ok=%v err=%v", step, ok, err)
				}
				if want := walkedHistory(t, it); got != want {
					t.Fatalf("%s: HistoryCounts = %+v, the store walks %+v", step, got, want)
				}
				return got
			}
			var nodes []types.NodeID
			var rels []types.RelID
			for i := range 12 {
				n, err := g.Nodes().Add(ctx, []string{"Event"}, map[string]any{"i": int64(i)})
				if err != nil {
					t.Fatal(err)
				}
				nodes = append(nodes, n.ID())
				if i > 0 {
					r, err := g.Rels().AddByID(ctx, "NEXT", nodes[i-1], n.ID(), map[string]any{"w": int64(i)})
					if err != nil {
						t.Fatal(err)
					}
					rels = append(rels, r.ID())
				}
			}
			if c := check("created"); c.Nodes != 0 || c.Rels != 0 {
				t.Fatalf("fresh entities have history: %+v", c)
			}
			before := check("before update")
			if _, err := g.Nodes().Update(ctx, nodes[0], map[string]any{"i": int64(-1)}); err != nil {
				t.Fatal(err)
			}
			after := check("updated")
			if after.Nodes != before.Nodes+1 || before.Nodes != 0 {
				t.Fatalf("update: %+v -> %+v", before, after)
			}
			if _, err := g.Nodes().Update(ctx, nodes[0], map[string]any{"i": int64(-2)}); err != nil {
				t.Fatal(err)
			}
			if c := check("updated twice"); c.Nodes != 1 {
				t.Fatalf("a second update of the same node: %+v", c)
			}
			if err := g.Nodes().AddLabel(ctx, nodes[1], "Seen"); err != nil {
				t.Fatal(err)
			}
			check("label added")
			if err := g.Nodes().CloseVersion(ctx, nodes[2], types.InstantFromTime(time.Now().Add(time.Hour))); err != nil {
				t.Fatal(err)
			}
			check("closed")
			if _, err := g.Rels().Update(ctx, rels[3], map[string]any{"w": int64(-3)}); err != nil {
				t.Fatal(err)
			}
			check("relationship updated")
			if err := g.Rels().Delete(ctx, rels[5]); err != nil {
				t.Fatal(err)
			}
			check("relationship deleted")
			if err := g.Rels().Delete(ctx, rels[6]); err != nil {
				t.Fatal(err)
			}
			// The node delete cascades to its relationship (rels[10]).
			if err := g.Nodes().Delete(ctx, nodes[11]); err != nil {
				t.Fatal(err)
			}
			check("node deleted")
			tx, err := g.Tx().Begin()
			if err != nil {
				t.Fatal(err)
			}
			n, err := tx.AddNode([]string{"Event"}, map[string]any{"i": int64(100)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.UpdateNode(n.ID(), map[string]any{"i": int64(101)}); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.UpdateNode(nodes[4], map[string]any{"i": int64(102)}); err != nil {
				t.Fatal(err)
			}
			check("inside a transaction")
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			check("rolled back")
			// Sharded has no compaction (ErrCapabilityNotSupported).
			if _, err := g.Admin().CompactHistoryNodes(ctx, adminpkg.RetentionPolicy{KeepVersions: 1}); err != nil && !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
				t.Fatal(err)
			}
			check("compacted")
			farFuture := types.InstantFromTime(time.Now().Add(24 * time.Hour))
			if _, err := g.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{Label: "Event", Mode: adminpkg.PurgeByAge, Before: farFuture}); err != nil {
				t.Fatal(err)
			}
			check("purged")
			if err := g.Admin().Reset(); err != nil {
				t.Fatal(err)
			}
			if c := check("reset"); c.Nodes != 0 || c.Rels != 0 {
				t.Fatalf("history after reset: %+v", c)
			}
		})
	}
}

// Badger's count survives a reopen: the first call after it counts the
// persisted history keys.
func TestHistoryCountsAfterReopen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{BadgerDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	var ids []types.NodeID
	for i := range 5 {
		n, err := g.Nodes().Add(ctx, []string{"Event"}, map[string]any{"i": int64(i)})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, n.ID())
	}
	for _, id := range ids[:3] {
		if _, err := g.Nodes().Update(ctx, id, map[string]any{"i": int64(-1)}); err != nil {
			t.Fatal(err)
		}
	}
	if c, ok, err := g.Stats().HistoryCounts(); err != nil || !ok || c.Nodes != 3 {
		t.Fatalf("before reopen: %+v %v %v", c, ok, err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g, err = graphpkg.New(graphpkg.Config{BadgerDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if c, ok, err := g.Stats().HistoryCounts(); err != nil || !ok || c.Nodes != 3 || c.Rels != 0 {
		t.Fatalf("after reopen: %+v %v %v", c, ok, err)
	}
}

// Tiered states none; a closed graph fails.
func TestHistoryCountsUnstated(t *testing.T) {
	for _, b := range allStoreBackends() {
		if b.name != "tiered" {
			continue
		}
		g := b.open(t)
		if _, ok, err := g.Stats().HistoryCounts(); ok || err != nil {
			t.Fatalf("tiered HistoryCounts ok=%v err=%v", ok, err)
		}
	}
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := g.Stats().HistoryCounts(); !errors.Is(err, graphpkg.ErrGraphClosed) {
		t.Fatalf("closed HistoryCounts = %v", err)
	}
}

// Readers of HistoryCounts beside writers of history: no race, and once the
// writers stop the count equals the store's walk (no stale cache survives).
func TestHistoryCountsConcurrentWriters(t *testing.T) {
	for _, b := range historyCountBackends() {
		t.Run(b.name, func(t *testing.T) {
			cfg, it := b.open(t)
			g, err := graphpkg.New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = g.Close() })
			ctx := context.Background()
			var ids []types.NodeID
			for i := range 64 {
				n, err := g.Nodes().Add(ctx, []string{"Event"}, map[string]any{"i": int64(i)})
				if err != nil {
					t.Fatal(err)
				}
				ids = append(ids, n.ID())
			}
			done := make(chan struct{})
			errs := make(chan error, 4)
			for w := range 2 {
				go func() {
					for i := range 32 {
						if _, err := g.Nodes().Update(ctx, ids[w*32+i], map[string]any{"i": int64(-1 - i)}); err != nil {
							errs <- err
							return
						}
					}
					errs <- nil
				}()
			}
			go func() {
				for {
					select {
					case <-done:
						errs <- nil
						return
					default:
						if _, _, err := g.Stats().HistoryCounts(); err != nil {
							errs <- err
							return
						}
					}
				}
			}()
			for range 2 {
				if err := <-errs; err != nil {
					t.Fatal(err)
				}
			}
			close(done)
			if err := <-errs; err != nil {
				t.Fatal(err)
			}
			got, ok, err := g.Stats().HistoryCounts()
			if err != nil || !ok || got != walkedHistory(t, it) || got.Nodes != 64 {
				t.Fatalf("after the writers: %+v %v %v, walk %+v", got, ok, err, walkedHistory(t, it))
			}
		})
	}
}
