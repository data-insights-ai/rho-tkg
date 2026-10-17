package graph_test

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Row 66 (v4.60.1): single-ID composite keys held inline and lazily cached
// node hashes answer exactly as before on every backend.

// A composite key held by 0, 1, 2 nodes, by 1 after a delete and after a
// value move, and by 2 again after a move back: the index door (unpaged and
// in keyset pages) equals the label scan filtered.
func TestCompositeSingleIDKeysAllBackends(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		if err := g.Index().CreateComposite("Ref", []string{"a", "b"}); err != nil {
			t.Fatal(err)
		}
		add := func(a int64, bv string) types.NodeID {
			n, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"a": a, "b": bv})
			if err != nil {
				t.Fatal(err)
			}
			return n.ID()
		}
		check := func(step string) {
			t.Helper()
			all, err := g.Nodes().ByLabel("Ref", storepkg.QueryOpts{})
			if err != nil {
				t.Fatal(err)
			}
			for a := int64(0); a <= 4; a++ {
				values := map[string]any{"a": a, "b": "x"}
				var want []types.NodeID
				for _, n := range all {
					av, _ := n.GetProperty("a")
					bv, _ := n.GetProperty("b")
					if av == a && bv == "x" {
						want = append(want, n.ID())
					}
				}
				slices.Sort(want)
				got, err := g.Nodes().ByLabelAndProperties("Ref", values, storepkg.QueryOpts{})
				if err != nil {
					t.Fatal(err)
				}
				if fmt.Sprint(nodeIDsOf(got)) != fmt.Sprint(want) {
					t.Fatalf("%s %s a=%d: index %v, scan %v", b.name, step, a, nodeIDsOf(got), want)
				}
				var walked []types.NodeID
				o := storepkg.QueryOpts{Limit: 1}
				for {
					page, err := g.Nodes().ByLabelAndProperties("Ref", values, o)
					if err != nil {
						t.Fatal(err)
					}
					if len(page) == 0 {
						break
					}
					walked = append(walked, page[0].ID())
					o.After = types.EntityID(page[0].ID())
				}
				if !slices.Equal(walked, want) {
					t.Fatalf("%s %s a=%d: pages %v, scan %v", b.name, step, a, walked, want)
				}
			}
		}
		// a=0: none; a=1: one; a=2: two; a=3: two, one deleted; a=4: two, one moved away.
		add(1, "x")
		add(2, "x")
		add(2, "x")
		add(3, "x")
		del := add(3, "x")
		add(4, "x")
		moved := add(4, "x")
		add(1, "y") // same a, other b: a different key
		check("written")
		if err := g.Nodes().Delete(ctx, del); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Nodes().Update(ctx, moved, map[string]any{"a": int64(9)}); err != nil {
			t.Fatal(err)
		}
		check("after delete and move")
		if _, err := g.Nodes().Update(ctx, moved, map[string]any{"a": int64(4)}); err != nil {
			t.Fatal(err)
		}
		add(3, "x")
		check("after move back")
	})
}

// A relationship created against nodes whose hashes no read has cached yet
// (fresh writes, an update, a reopened badger store) carries the endpoints'
// current hashes, and both chains verify.
func TestRelEndpointHashesFromUncachedNodes(t *testing.T) {
	ctx := context.Background()
	check := func(t *testing.T, g *graphpkg.Graph, from, to types.NodeID) {
		t.Helper()
		r, err := g.Rels().AddByID(ctx, "E", from, to, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, end := range []struct {
			id   types.NodeID
			hash string
		}{{from, r.Integrity().FromNodeHash}, {to, r.Integrity().ToNodeHash}} {
			n, err := g.Nodes().Get(ctx, end.id)
			if err != nil {
				t.Fatal(err)
			}
			if n.Integrity() == nil || n.Integrity().Hash == "" || end.hash != n.Integrity().Hash {
				t.Fatalf("rel endpoint hash %q, node %d holds %+v", end.hash, end.id, n.Integrity())
			}
			if ok, err := g.Hash().VerifyNodeChain(end.id); err != nil || !ok {
				t.Fatalf("VerifyNodeChain(%d) = %v, %v", end.id, ok, err)
			}
		}
		if ok, err := g.Hash().VerifyRelChain(r.ID()); err != nil || !ok {
			t.Fatalf("VerifyRelChain = %v, %v", ok, err)
		}
	}
	write := func(t *testing.T, g *graphpkg.Graph) (types.NodeID, types.NodeID) {
		t.Helper()
		a, err := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"k": "a"})
		if err != nil {
			t.Fatal(err)
		}
		b, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"k": "b"}) // tiered: cross-shard
		if err != nil {
			t.Fatal(err)
		}
		return a.ID(), b.ID()
	}
	forAllStoreBackends(t, func(t *testing.T, _ storeBackend, g *graphpkg.Graph) {
		a, b := write(t, g)
		check(t, g, a, b)
		if _, err := g.Nodes().Update(ctx, a, map[string]any{"k": "a2"}); err != nil {
			t.Fatal(err)
		}
		check(t, g, a, b) // the update dropped a's cached hash
		check(t, g, b, b) // self-loop
	})
	t.Run("badger-reopen", func(t *testing.T) {
		dir := t.TempDir()
		open := func() *graphpkg.Graph {
			bs, err := badger.New(badger.Config{Dir: dir})
			if err != nil {
				t.Fatal(err)
			}
			g, err := graphpkg.New(graphpkg.Config{Store: bs})
			if err != nil {
				t.Fatal(err)
			}
			return g
		}
		g := open()
		a, b := write(t, g)
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		g = open()
		defer func() { _ = g.Close() }()
		check(t, g, a, b)
	})
}

// settledHeap is the live heap once it held still for 4 s (badger flushes
// its immutable memtables in the background, ~29 MB a step), at most 90 s.
func settledHeap() uint64 {
	var m runtime.MemStats
	var last uint64
	still := 0
	for i := 0; i < 180 && still < 8; i++ {
		runtime.GC()
		runtime.GC()
		runtime.ReadMemStats(&m)
		if last > 0 && max(m.HeapAlloc, last)-min(m.HeapAlloc, last) < last/200 {
			still++
		} else {
			still = 0
		}
		last = m.HeapAlloc
		time.Sleep(500 * time.Millisecond)
	}
	return m.HeapAlloc
}

// The live heap a badger graph holds per node, as the slope between 40,000
// and 200,000 sigma-overview nodes (6 properties, a 3-key composite index of
// single-ID keys when indexed). v4.60.0: 185 B/node plain, 388 indexed;
// v4.60.1 (row 66, no cached hash string, single-ID keys inline): 88 and 200.
func TestBadgerHeapPerNodeSlope(t *testing.T) {
	if testing.Short() || isRaceEnabled() {
		t.Skip("long tier: release gate (heap measurement, no race detector)")
	}
	rels := []string{"s4_seen", "seat_seen", "own_seen", "rf_seen", "rf_pair", "rf_port", "rf_aseen", "rf_apair", "rf_aport"}
	props := func(i int) map[string]any {
		key := fmt.Sprintf("[%d,%d,\"%d\"]", 7340000000000000000+i*7919, 7350000000000000000+i*104729, i%65536)
		return map[string]any{"ov_namespace": "ba", "ov_rel": rels[i%9], "ov_key": key,
			"ov_c3": int64(1717372800000 + i), "ov_c4": int64(1717372805000 + i), "ov_c5": int64(i % 100)}
	}
	for _, tc := range []struct {
		indexed bool
		bound   float64
	}{{false, 120}, {true, 260}} {
		t.Run(fmt.Sprintf("indexed=%v", tc.indexed), func(t *testing.T) {
			bs, err := badger.New(badger.Config{Dir: t.TempDir(), CacheCapacity: 1000, BlockCacheSize: 4 << 20, IndexCacheSize: 4 << 20, MemTableSize: 8 << 20})
			if err != nil {
				t.Fatal(err)
			}
			g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 1, Store: bs, AllowTxBackfill: true})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = g.Close() }()
			if tc.indexed {
				if err := g.Index().CreateComposite("Derived", []string{"ov_namespace", "ov_rel", "ov_key"}); err != nil {
					t.Fatal(err)
				}
			}
			written := 0
			writeTo := func(n int) {
				for written < n {
					if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
						for j := written; j < min(n, written+10_000); j++ {
							if _, err := tx.AddNode([]string{"Derived"}, props(j)); err != nil {
								return err
							}
						}
						return nil
					}); err != nil {
						t.Fatal(err)
					}
					written = min(n, written+10_000)
				}
			}
			const n1, n2 = 40_000, 200_000
			settle := func() uint64 {
				if err := bs.Flush(); err != nil { // the store's write buffer
					t.Fatal(err)
				}
				return settledHeap()
			}
			writeTo(n1)
			h1 := settle()
			writeTo(n2)
			h2 := settle()
			slope := (float64(h2) - float64(h1)) / float64(n2-n1)
			t.Logf("indexed=%v: %.0f B/node (heap %d -> %d)", tc.indexed, slope, h1, h2)
			if slope > tc.bound {
				t.Fatalf("indexed=%v: %.0f B/node, want <= %.0f", tc.indexed, slope, tc.bound)
			}
		})
	}
}
