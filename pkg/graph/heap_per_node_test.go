package graph_test

import (
	"context"
	"fmt"
	"slices"
	"testing"

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
