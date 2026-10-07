package graph_test

import (
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// columnScanOrdinals reads every node and relationship column batch of the
// fixture and returns, per ID, the ordinal the batch carried.
func columnScanOrdinals(t *testing.T, g *graphpkg.Graph) (map[types.NodeID]uint32, map[types.RelID]uint32) {
	t.Helper()
	nodes := map[types.NodeID]uint32{}
	ok, err := g.ScanNodeColumns("P", []string{"w"}, graphpkg.QueryOpts{}, func(b *graphpkg.ColumnBatch) bool {
		if len(b.Ordinals) != len(b.IDs) {
			t.Fatalf("node batch: %d ordinals for %d IDs", len(b.Ordinals), len(b.IDs))
		}
		for i, id := range b.IDs {
			nodes[id] = b.Ordinals[i]
		}
		return true
	})
	if err != nil || !ok {
		t.Fatalf("ScanNodeColumns: %v %v", ok, err)
	}
	rels := map[types.RelID]uint32{}
	ok, err = g.ScanRelColumns("KNOWS", []string{"w"}, graphpkg.QueryOpts{}, func(b *graphpkg.RelColumnBatch) bool {
		if len(b.Ordinals) != len(b.IDs) {
			t.Fatalf("rel batch: %d ordinals for %d IDs", len(b.Ordinals), len(b.IDs))
		}
		for i, id := range b.IDs {
			rels[id] = b.Ordinals[i]
		}
		return true
	})
	if err != nil || !ok {
		t.Fatalf("ScanRelColumns: %v %v", ok, err)
	}
	return nodes, rels
}

// The column scans carry each row's dense ordinal, the one Lend reports, on
// memory (the row path) and badger (the columnar snapshot), across batches
// (more rows than one batch holds). Two-phase: after a delete and a new
// relationship, the survivors keep their ordinals, the deleted one is gone and
// the new one carries its own. Badger with DisableOrdinals carries zeros.
func TestColumnScansCarryOrdinals(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  func(t *testing.T) graphpkg.Config
		none bool
	}{
		{"memory", func(*testing.T) graphpkg.Config { return graphpkg.Config{SnowflakeNodeID: 0} }, false},
		{"badger", func(*testing.T) graphpkg.Config { return graphpkg.Config{SnowflakeNodeID: 0, BadgerInMemory: true} }, false},
		{"badger-no-ordinals", func(t *testing.T) graphpkg.Config {
			st, err := badger.New(badger.Config{InMemory: true, DisableOrdinals: true})
			if err != nil {
				t.Fatal(err)
			}
			return graphpkg.Config{SnowflakeNodeID: 0, Store: st}
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, err := graphpkg.New(tc.cfg(t))
			if err != nil {
				t.Fatal(err)
			}
			defer g.Close()
			ctx := context.Background()
			const n = 5000 // more than one 4,096-row batch
			created := make([]*types.Node, n)
			for i := range created {
				if created[i], err = g.Nodes().Add(ctx, []string{"P"}, map[string]any{"w": int64(i)}); err != nil {
					t.Fatal(err)
				}
			}
			var relIDs []types.RelID
			for i := range n {
				r, err := g.Rels().Add(ctx, "KNOWS", created[i], created[(i+1)%n], map[string]any{"w": int64(i)})
				if err != nil {
					t.Fatal(err)
				}
				relIDs = append(relIDs, r.ID())
			}
			check := func(step string, nodes map[types.NodeID]uint32, rels map[types.RelID]uint32) {
				t.Helper()
				seen := map[uint32]bool{}
				for id, ord := range nodes {
					lent, err := g.Nodes().Lend(ctx, id)
					if err != nil {
						t.Fatal(err)
					}
					if ord != lent.Ordinal() || (tc.none != (ord == 0)) {
						t.Fatalf("%s: node %d carries %d, Lend says %d", step, id, ord, lent.Ordinal())
					}
				}
				for id, ord := range rels {
					lent, err := g.Rels().Lend(ctx, id)
					if err != nil {
						t.Fatal(err)
					}
					if ord != lent.Ordinal() || (tc.none != (ord == 0)) {
						t.Fatalf("%s: rel %d carries %d, Lend says %d", step, id, ord, lent.Ordinal())
					}
					if ord != 0 && seen[ord] {
						t.Fatalf("%s: ordinal %d twice", step, ord)
					}
					seen[ord] = true
				}
			}
			nodes, rels := columnScanOrdinals(t, g)
			if len(nodes) != n || len(rels) != n {
				t.Fatalf("%d nodes, %d rels scanned", len(nodes), len(rels))
			}
			check("first scan", nodes, rels)

			if err := g.Rels().Delete(ctx, relIDs[10]); err != nil {
				t.Fatal(err)
			}
			fresh, err := g.Rels().Add(ctx, "KNOWS", created[3], created[9], map[string]any{"w": int64(-1)})
			if err != nil {
				t.Fatal(err)
			}
			_, after := columnScanOrdinals(t, g)
			if _, ok := after[relIDs[10]]; ok {
				t.Fatal("the deleted relationship is still scanned")
			}
			if ord := after[relIDs[20]]; ord != rels[relIDs[20]] {
				t.Fatalf("a survivor's ordinal moved: %d -> %d", rels[relIDs[20]], ord)
			}
			if !tc.none && (after[fresh.ID()] == 0 || after[fresh.ID()] == rels[relIDs[10]]) {
				t.Fatalf("the new relationship carries %d", after[fresh.ID()])
			}
			check("after the writes", nil, after)
		})
	}
}

// A declared segment type's rows carry ordinal 0 in the column scan, as they
// do everywhere.
func TestRelColumnScanSegmentTypeOrdinals(t *testing.T) {
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 4, Store: memory.New(), RelSegments: []graphpkg.RelSegmentSpec{{
		Type: "HOP", Columns: []graphpkg.SegmentColumn{{Name: "w", Kind: graphpkg.SegmentInt64}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	ctx := context.Background()
	a, _ := g.Nodes().Add(ctx, []string{"P"}, nil)
	b, _ := g.Nodes().Add(ctx, []string{"P"}, nil)
	for i := range 3 {
		if _, err := g.Rels().Add(ctx, "HOP", a, b, map[string]any{"w": int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Admin().SealRelSegments("HOP"); err != nil {
		t.Fatal(err)
	}
	rows := 0
	ok, err := g.ScanRelColumns("HOP", []string{"w"}, graphpkg.QueryOpts{}, func(rb *graphpkg.RelColumnBatch) bool {
		if len(rb.Ordinals) != len(rb.IDs) {
			t.Fatalf("%d ordinals for %d IDs", len(rb.Ordinals), len(rb.IDs))
		}
		for _, o := range rb.Ordinals {
			if o != 0 {
				t.Fatalf("a segment row carries ordinal %d", o)
			}
		}
		rows += len(rb.IDs)
		return true
	})
	if err != nil || !ok || rows != 3 {
		t.Fatalf("%d rows, %v %v", rows, ok, err)
	}
}
