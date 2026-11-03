package graph_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// UNIQUELOOK (ai-soc, 3 Nov): a unique key's GetOrCreateByKey and create-time
// check on a plain badger store, and on a tiered reference label, read the
// owner index (one claim, one row) instead of seeking the on-disk property
// index, 2-3 seeks per edge in ai-soc's admission profile.

type uniqLookStore struct {
	name string
	open func(t *testing.T, dir string) *graphpkg.Graph
}

func uniqLookStores() []uniqLookStore {
	return []uniqLookStore{
		{"badger", func(t *testing.T, dir string) *graphpkg.Graph {
			t.Helper()
			bs, err := badger.New(badger.Config{Dir: dir, PropertyIndexOnDisk: true})
			if err != nil {
				t.Fatal(err)
			}
			g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 12, Store: bs})
			if err != nil {
				t.Fatal(err)
			}
			return g
		}},
		{"tiered-reference", func(t *testing.T, dir string) *graphpkg.Graph {
			t.Helper()
			ts, err := tiered.New(tiered.Config{DataDir: dir, RefLabels: []string{"Host"}, ShardWindow: 7 * 24 * time.Hour,
				PropertyIndexOnDisk: true})
			if err != nil {
				t.Fatal(err)
			}
			g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 12, Store: ts})
			if err != nil {
				t.Fatal(err)
			}
			return g
		}},
	}
}

// TestUniqueLookNoPropertyIndexSeeks: 1,000 GetOrCreateByKey calls on a
// unique key, half hits and half misses, at the default depth and at
// DepthHot, seek the on-disk property index zero times, and each answers
// right (a hit returns the holder, a miss creates once). Not parallel: the
// seek counter is the process's.
func TestUniqueLookNoPropertyIndexSeeks(t *testing.T) {
	ctx := context.Background()
	for _, st := range uniqLookStores() {
		t.Run(st.name, func(t *testing.T) {
			g := st.open(t, t.TempDir())
			defer g.Close()
			if err := g.Constraints().CreateUnique(ctx, "Host", "key"); err != nil {
				t.Fatal(err)
			}
			ids := map[string]types.NodeID{}
			for i := 0; i < 250; i++ {
				k := fmt.Sprintf("pre-%d", i)
				n, err := g.Nodes().Add(ctx, []string{"Host"}, map[string]any{"key": k})
				if err != nil {
					t.Fatal(err)
				}
				ids[k] = n.ID()
			}
			before := badger.PropertyDiskLookupsForTest()
			for i := 0; i < 1000; i++ {
				opts := storepkg.QueryOpts{}
				if i%2 == 1 {
					opts.Depth = storepkg.DepthHot
				}
				hit := i%4 < 2
				k := fmt.Sprintf("new-%d", i)
				if hit {
					k = fmt.Sprintf("pre-%d", i%250)
				}
				n, created, err := g.Nodes().GetOrCreateByKeyOpts(ctx, "Host", "key", k, nil, opts)
				if err != nil {
					t.Fatal(err)
				}
				if hit && (created || n.ID() != ids[k]) {
					t.Fatalf("hit %s: created %v id %d, want holder %d", k, created, n.ID(), ids[k])
				}
				if !hit && !created {
					t.Fatalf("miss %s: not created", k)
				}
			}
			if got := badger.PropertyDiskLookupsForTest() - before; got != 0 {
				t.Errorf("on-disk property index lookups for 1,000 GetOrCreateByKey = %d, want 0 (owner index)", got)
			}
		})
	}
}

// TestUniqueLookConcurrentOneValue: 16 goroutines GetOrCreateByKey one value
// on a ready owner index: exactly one creates, all return its node.
func TestUniqueLookConcurrentOneValue(t *testing.T) {
	ctx := context.Background()
	for _, st := range uniqLookStores() {
		t.Run(st.name, func(t *testing.T) {
			g := st.open(t, t.TempDir())
			defer g.Close()
			if err := g.Constraints().CreateUnique(ctx, "Host", "key"); err != nil {
				t.Fatal(err)
			}
			for round := 0; round < 20; round++ {
				k := fmt.Sprintf("v%d", round)
				var wg sync.WaitGroup
				var mu sync.Mutex
				created := 0
				got := map[types.NodeID]bool{}
				for w := 0; w < 16; w++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						n, c, err := g.Nodes().GetOrCreateByKey(ctx, "Host", "key", k, nil)
						if err != nil {
							t.Error(err)
							return
						}
						mu.Lock()
						defer mu.Unlock()
						if c {
							created++
						}
						got[n.ID()] = true
					}()
				}
				wg.Wait()
				if created != 1 || len(got) != 1 {
					t.Fatalf("%s: created %d nodes, returned %d IDs; want 1 and 1", k, created, len(got))
				}
				rows, err := g.Nodes().ByLabelAndProperty("Host", "key", k, storepkg.QueryOpts{})
				if err != nil || len(rows) != 1 {
					t.Fatalf("%s: %d holders (%v), want 1", k, len(rows), err)
				}
			}
		})
	}
}

// TestUniqueLookValueChangeAndDelete: a claim whose row moved to another
// value, or was deleted, is ignored, and the old value is claimable again; a
// second holder is refused. Then the same over a reopen.
func TestUniqueLookValueChangeAndDelete(t *testing.T) {
	ctx := context.Background()
	for _, st := range uniqLookStores() {
		t.Run(st.name, func(t *testing.T) {
			dir := t.TempDir()
			g := st.open(t, dir)
			if err := g.Constraints().CreateUnique(ctx, "Host", "key"); err != nil {
				t.Fatal(err)
			}
			a, err := g.Nodes().Add(ctx, []string{"Host"}, map[string]any{"key": "a"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes().Update(ctx, a.ID(), map[string]any{"key": "b"}); err != nil {
				t.Fatal(err)
			}
			n, created, err := g.Nodes().GetOrCreateByKey(ctx, "Host", "key", "a", nil)
			if err != nil || !created || n.ID() == a.ID() {
				t.Fatalf("GetOrCreate(a) after a->b: id %d created %v err %v; want a new node", n.ID(), created, err)
			}
			na := n.ID()
			n, created, err = g.Nodes().GetOrCreateByKey(ctx, "Host", "key", "b", nil)
			if err != nil || created || n.ID() != a.ID() {
				t.Fatalf("GetOrCreate(b): id %d created %v err %v; want %d", n.ID(), created, err, a.ID())
			}
			if _, err := g.Nodes().Add(ctx, []string{"Host"}, map[string]any{"key": "b"}); err == nil {
				t.Fatal("a second holder of b was admitted")
			}
			if err := g.Nodes().Delete(ctx, a.ID()); err != nil {
				t.Fatal(err)
			}
			n, created, err = g.Nodes().GetOrCreateByKey(ctx, "Host", "key", "b", nil)
			if err != nil || !created || n.ID() == a.ID() {
				t.Fatalf("GetOrCreate(b) after delete: id %d created %v err %v; want a new node", n.ID(), created, err)
			}
			nb := n.ID()
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			g = st.open(t, dir)
			defer g.Close()
			before := badger.PropertyDiskLookupsForTest()
			for k, want := range map[string]types.NodeID{"a": na, "b": nb} {
				n, created, err := g.Nodes().GetOrCreateByKey(ctx, "Host", "key", k, nil)
				if err != nil || created || n.ID() != want {
					t.Fatalf("reopen GetOrCreate(%s): id %d created %v err %v; want %d", k, n.ID(), created, err, want)
				}
			}
			if got := badger.PropertyDiskLookupsForTest() - before; got != 0 {
				t.Errorf("property index lookups after reopen = %d, want 0", got)
			}
		})
	}
}

// TestUniqueLookSeedsExistingRows: CreateUnique over rows that exist seeds
// the owner index, so GetOrCreateByKey finds every one of them, over a
// reopen too, and a batch create of a held value is refused while a batch
// create's own value is found afterwards.
func TestUniqueLookSeedsExistingRows(t *testing.T) {
	ctx := context.Background()
	for _, st := range uniqLookStores() {
		t.Run(st.name, func(t *testing.T) {
			dir := t.TempDir()
			g := st.open(t, dir)
			ids := map[string]types.NodeID{}
			for i := 0; i < 50; i++ {
				k := fmt.Sprintf("s%d", i)
				n, err := g.Nodes().Add(ctx, []string{"Host"}, map[string]any{"key": k})
				if err != nil {
					t.Fatal(err)
				}
				ids[k] = n.ID()
			}
			if err := g.Constraints().CreateUnique(ctx, "Host", "key"); err != nil {
				t.Fatal(err)
			}
			b, err := g.Batch().New()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := b.AddNode([]string{"Host"}, map[string]any{"key": "batch-1"}); err != nil {
				t.Fatal(err)
			}
			if _, err := b.Execute(); err != nil {
				t.Fatal(err)
			}
			check := func(g *graphpkg.Graph) {
				t.Helper()
				for k, want := range ids {
					n, created, err := g.Nodes().GetOrCreateByKey(ctx, "Host", "key", k, nil)
					if err != nil || created || n.ID() != want {
						t.Fatalf("GetOrCreate(%s): id %d created %v err %v; want %d", k, n.ID(), created, err, want)
					}
				}
				_, created, err := g.Nodes().GetOrCreateByKey(ctx, "Host", "key", "batch-1", nil)
				if err != nil || created {
					t.Fatalf("GetOrCreate(batch-1): created %v err %v; want the batch's node", created, err)
				}
				rows, err := g.Nodes().ByLabelAndProperty("Host", "key", "batch-1", storepkg.QueryOpts{})
				if err != nil || len(rows) != 1 {
					t.Fatalf("batch-1 holders %d (%v), want 1", len(rows), err)
				}
			}
			check(g)
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			g = st.open(t, dir)
			defer g.Close()
			check(g)
		})
	}
}
