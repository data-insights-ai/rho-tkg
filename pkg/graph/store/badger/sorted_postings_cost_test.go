package badger

import (
	"runtime"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 72 on badger: an equality page copies a batch of its posting list
// under idxMu, not the whole key, so the bytes one page allocates do not grow
// with the key. Before, every call copied and sorted every ID of the key.
func TestPostingsPageCostIndependentOfKeySizeBadger(t *testing.T) {
	bs := newTestBadgerStoreInMemory(t)
	if err := bs.CreatePropertyIndex(1, "a"); err != nil {
		t.Fatal(err)
	}
	if err := bs.CreateCompositePropertyIndex(1, []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	if err := bs.CreateRelPropertyIndex(7, "k"); err != nil {
		t.Fatal(err)
	}
	n := 60_000
	if testing.Short() {
		n = 20_000
	}
	nodes := make([]*types.Node, 0, n)
	rels := make([]*types.Relationship, 0, n)
	for i := 0; i < n; i++ {
		node := types.NewNode(types.NodeID(snowflake.ID(int64(i+1)<<15)), 1, nil)
		_ = node.SetProperty("a", int64(1))
		_ = node.SetProperty("b", "x")
		nodes = append(nodes, node)
	}
	if err := bs.PutNodesBatch(nodes); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		r := types.NewRelationship(types.RelID(snowflake.ID(int64(i+1)<<15|1)), 7, nodes[i].ID(), nodes[(i+1)%n].ID())
		_ = r.SetProperty("k", int64(1))
		rels = append(rels, r)
	}
	if err := bs.PutRelationshipsBatch(rels); err != nil {
		t.Fatal(err)
	}
	const limit = 16
	after := types.EntityID(nodes[n/2].ID())
	afterRel := types.EntityID(rels[n/2].ID())
	doors := []struct {
		name string
		call func() int
	}{
		{"NodesByLabelAndProperties", func() int {
			out, err := bs.NodesByLabelAndProperties(1, map[string]any{"a": int64(1), "b": "x"}, QueryOpts{After: after, Limit: limit})
			if err != nil {
				t.Fatal(err)
			}
			return len(out)
		}},
		{"NodesByLabelAndProperty", func() int {
			out, err := bs.NodesByLabelAndProperty(1, "a", int64(1), QueryOpts{After: after, Limit: limit})
			if err != nil {
				t.Fatal(err)
			}
			return len(out)
		}},
		{"RelationshipsByTypeAndProperty", func() int {
			out, err := bs.RelationshipsByTypeAndProperty(7, "k", int64(1), QueryOpts{After: afterRel, Limit: limit})
			if err != nil {
				t.Fatal(err)
			}
			return len(out)
		}},
	}
	// Rows come from the cache or are decoded per page; the bound is a page's
	// rows and batch, a quarter of one 8-byte copy of the key.
	bound := uint64(n * 2)
	for _, d := range doors {
		if got := d.call(); got != limit {
			t.Fatalf("%s: page holds %d rows, want %d", d.name, got, limit)
		}
		const calls = 20
		runtime.GC()
		var before, afterStats runtime.MemStats
		runtime.ReadMemStats(&before)
		for i := 0; i < calls; i++ {
			d.call()
		}
		runtime.ReadMemStats(&afterStats)
		b := (afterStats.TotalAlloc - before.TotalAlloc) / calls
		t.Logf("%s: %d B per page of %d over a key of %d", d.name, b, limit, n)
		if b > bound {
			t.Errorf("%s: one page of %d allocates %d B over a key of %d members (bound %d B): the page copies or sorts the key", d.name, limit, b, n, bound)
		}
	}
}
