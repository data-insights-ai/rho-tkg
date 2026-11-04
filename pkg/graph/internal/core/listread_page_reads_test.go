package core

import (
	"context"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badgerpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestListReadPagesReadRowsInOneScan (LISTREAD) pins that a cold
// ByLabelAndProperties or ByLabelAndProperty page reads its rows through the
// store's forward scan, not a point read and decode per row (sigma's restore:
// 75 % of a cold page was per-row Gets): the store's per-row point-read count
// stays 0 while every row of the label comes back, in ID order, once.
// Mutation: the per-row prefetch loop reads 300 rows by point read.
func TestListReadPagesReadRowsInOneScan(t *testing.T) {
	dir := t.TempDir()
	open := func() (*Core, *badgerpkg.Store) {
		bs, err := badgerpkg.New(badgerpkg.Config{Dir: dir, CacheCapacity: 1})
		if err != nil {
			t.Fatal(err)
		}
		g, err := New(Config{Store: bs})
		if err != nil {
			t.Fatal(err)
		}
		return g, bs
	}
	g, _ := open()
	if err := g.Index.CreateComposite("Page", []string{"kind", "tenant"}); err != nil {
		t.Fatal(err)
	}
	if err := g.Index.CreateProperty("Page", "kind"); err != nil {
		t.Fatal(err)
	}
	const n = 300
	var want []types.NodeID
	for i := 0; i < n; i++ {
		x, err := g.Nodes.Add(context.Background(), []string{"Page"}, map[string]any{"kind": "blob", "tenant": "t1", "seq": i})
		if err != nil {
			t.Fatal(err)
		}
		want = append(want, x.ID())
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g, bs := open()
	defer g.Close()
	doors := map[string]func(after types.EntityID) ([]*types.Node, error){
		"ByLabelAndProperties": func(after types.EntityID) ([]*types.Node, error) {
			return g.Nodes.ByLabelAndProperties("Page", map[string]any{"kind": "blob", "tenant": "t1"}, storepkg.QueryOpts{Limit: 64, After: after})
		},
		"ByLabelAndProperty": func(after types.EntityID) ([]*types.Node, error) {
			return g.Nodes.ByLabelAndProperty("Page", "kind", "blob", storepkg.QueryOpts{Limit: 64, After: after})
		},
	}
	for name, page := range doors {
		before := bs.NodeRowDecodesForTest()
		var got []types.NodeID
		var after types.EntityID
		for {
			rows, err := page(after)
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) == 0 {
				break
			}
			for _, r := range rows {
				got = append(got, r.ID())
			}
			after = types.EntityID(rows[len(rows)-1].ID())
		}
		if len(got) != n {
			t.Fatalf("%s: listed %d rows, want %d", name, len(got), n)
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("%s: row %d is %d, want %d (ID order, each once)", name, i, got[i], want[i])
			}
		}
		if reads := bs.NodeRowDecodesForTest() - before; reads != 0 {
			t.Errorf("%s: %d rows read by point read, want 0 (one forward scan per page)", name, reads)
		}
	}
}
