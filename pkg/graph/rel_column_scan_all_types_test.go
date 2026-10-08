package graph_test

import (
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// scanRelColumnsByType returns, per relationship, the RelType its batch named
// and how often it was seen.
func scanRelColumnsByType(t *testing.T, g *graphpkg.Graph, relType string) (map[types.RelID]string, map[types.RelID]int, bool) {
	t.Helper()
	names := map[types.RelID]string{}
	seen := map[types.RelID]int{}
	ok, err := g.ScanRelColumns(relType, nil, graphpkg.QueryOpts{}, func(b *graphpkg.RelColumnBatch) bool {
		if b.RelType == "" {
			t.Fatalf("scan %q: a batch of %d rows names no type", relType, len(b.IDs))
		}
		for _, id := range b.IDs {
			names[id] = b.RelType
			seen[id]++
		}
		return true
	})
	if err != nil {
		t.Fatalf("ScanRelColumns(%q): %v", relType, err)
	}
	return names, seen, ok
}

// ScanRelColumns("") reads the relationships of every type, each exactly
// once, in batches naming their type: the union of the typed scans. A type
// whose relationships were all deleted contributes nothing; a deleted
// relationship is gone; typed scans name their type too. Memory with a
// declared segment type (sealed and memtable rows) and badger; tiered and
// sharded have no column scan (ok=false), for "" as for a type.
func TestScanRelColumnsEveryType(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  graphpkg.Config
	}{
		{"memory", graphpkg.Config{SnowflakeNodeID: 4, Store: memory.New(), RelSegments: []graphpkg.RelSegmentSpec{{
			Type: "HOP", Columns: []graphpkg.SegmentColumn{{Name: "w", Kind: graphpkg.SegmentInt64}},
		}}}},
		{"badger", graphpkg.Config{SnowflakeNodeID: 4, BadgerInMemory: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, err := graphpkg.New(tc.cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer g.Close()
			ctx := context.Background()
			var nodes []*types.Node
			for range 5 {
				n, err := g.Nodes().Add(ctx, []string{"P"}, nil)
				if err != nil {
					t.Fatal(err)
				}
				nodes = append(nodes, n)
			}
			want := map[types.RelID]string{}
			add := func(typ string, i int) types.RelID {
				r, err := g.Rels().Add(ctx, typ, nodes[i%5], nodes[(i+2)%5], map[string]any{"w": int64(i)})
				if err != nil {
					t.Fatal(err)
				}
				want[r.ID()] = typ
				return r.ID()
			}
			for i := range 6 {
				add("KNOWS", i)
			}
			for i := range 3 {
				add("LIKES", i)
			}
			gone := add("GONE", 0) // a type whose only relationship is deleted
			for i := range 4 {
				add("HOP", i)
			}
			if tc.name == "memory" {
				if err := g.Admin().SealRelSegments("HOP"); err != nil {
					t.Fatal(err)
				}
				add("HOP", 9) // a memtable row beside the sealed ones
			}
			dropped := add("KNOWS", 7)
			for _, id := range []types.RelID{gone, dropped} {
				if err := g.Rels().Delete(ctx, id); err != nil {
					t.Fatal(err)
				}
				delete(want, id)
			}

			names, seen, ok := scanRelColumnsByType(t, g, "")
			if !ok {
				t.Fatal("ScanRelColumns(\"\") ok=false on a backend with a column scan")
			}
			if len(names) != len(want) {
				t.Fatalf("every type: %d relationships, want %d", len(names), len(want))
			}
			for id, typ := range want {
				if names[id] != typ || seen[id] != 1 {
					t.Fatalf("rel %d: named %q seen %d times, want %q once", id, names[id], seen[id], typ)
				}
			}
			union := map[types.RelID]string{}
			for _, typ := range []string{"KNOWS", "LIKES", "GONE", "HOP"} {
				typed, _, ok := scanRelColumnsByType(t, g, typ)
				if !ok {
					t.Fatalf("ScanRelColumns(%q) ok=false", typ)
				}
				for id, name := range typed {
					if name != typ {
						t.Fatalf("typed scan %q names %q", typ, name)
					}
					union[id] = name
				}
			}
			if len(union) != len(names) {
				t.Fatalf("union of the typed scans %d, every-type scan %d", len(union), len(names))
			}
			stopped := 0
			if _, err := g.ScanRelColumns("", nil, graphpkg.QueryOpts{}, func(*graphpkg.RelColumnBatch) bool {
				stopped++
				return false
			}); err != nil || stopped != 1 {
				t.Fatalf("returning false must stop the every-type scan: %d batches, %v", stopped, err)
			}
		})
	}
	for _, b := range allStoreBackends() {
		if b.name != "tiered" && b.name != "sharded" {
			continue
		}
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			a, _ := g.Nodes().Add(ctx, []string{"P"}, nil)
			c, _ := g.Nodes().Add(ctx, []string{"P"}, nil)
			if _, err := g.Rels().Add(ctx, "KNOWS", a, c, nil); err != nil {
				t.Fatal(err)
			}
			if _, _, ok := scanRelColumnsByType(t, g, ""); ok {
				t.Fatal("a backend without a column scan said ok")
			}
		})
	}
}
