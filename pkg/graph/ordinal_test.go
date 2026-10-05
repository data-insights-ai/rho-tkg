package graph_test

import (
	"context"
	"sync"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Dense ordinals (store.OrdinalCapability): every read door hands out the
// same ordinal for an entity, it survives writes to the entity, deleted
// entities' ordinals are never handed out again, and MaxOrdinal bounds them.
func TestOrdinalsAcrossDoorsAndWrites(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		_, nodesOK, err := g.Nodes().MaxOrdinal()
		if err != nil {
			t.Fatal(err)
		}
		_, relsOK, err := g.Rels().MaxOrdinal()
		if err != nil {
			t.Fatal(err)
		}
		if nodesOK != !b.tiered || relsOK != !b.tiered {
			t.Fatalf("MaxOrdinal ok = %v/%v on %s", nodesOK, relsOK, b.name)
		}
		label := "P"
		var nodes []*types.Node
		for i := 0; i < 12; i++ {
			n, err := g.Nodes().Add(ctx, []string{label}, map[string]any{"i": int64(i)})
			if err != nil {
				t.Fatal(err)
			}
			nodes = append(nodes, n)
		}
		var rels []*types.Relationship
		for i := 0; i+1 < len(nodes); i++ {
			r, err := g.Rels().Add(ctx, "T", nodes[i], nodes[i+1], nil)
			if err != nil {
				t.Fatal(err)
			}
			rels = append(rels, r)
		}
		nodeOrd := map[types.NodeID]uint32{}
		seen := map[uint32]types.NodeID{}
		for _, n := range nodes {
			got, err := g.Nodes().Get(ctx, n.ID())
			if err != nil {
				t.Fatal(err)
			}
			o := got.Ordinal()
			if b.tiered {
				if o != 0 {
					t.Fatalf("tiered row carries ordinal %d", o)
				}
				continue
			}
			if o == 0 {
				t.Fatalf("node %d has no ordinal", n.ID())
			}
			if other, dup := seen[o]; dup {
				t.Fatalf("ordinal %d on %d and %d", o, n.ID(), other)
			}
			seen[o], nodeOrd[n.ID()] = n.ID(), o
		}
		if b.tiered {
			return
		}
		maxN, _, _ := g.Nodes().MaxOrdinal()
		for o := range seen {
			if o > maxN {
				t.Fatalf("ordinal %d above MaxOrdinal %d", o, maxN)
			}
		}
		check := func(step string) {
			t.Helper()
			for _, n := range nodes {
				want, live := nodeOrd[n.ID()]
				if !live {
					continue
				}
				lent, err := g.Nodes().Lend(ctx, n.ID())
				if err != nil {
					t.Fatal(err)
				}
				if lent.Ordinal() != want {
					t.Fatalf("%s: Lend(%d).Ordinal = %d, want %d", step, n.ID(), lent.Ordinal(), want)
				}
			}
			byLabel, err := g.Nodes().ByLabel(label, graphpkg.QueryOpts{})
			if err != nil {
				t.Fatal(err)
			}
			for _, n := range byLabel {
				if n.Ordinal() != nodeOrd[n.ID()] {
					t.Fatalf("%s: ByLabel row %d ordinal %d, want %d", step, n.ID(), n.Ordinal(), nodeOrd[n.ID()])
				}
			}
			_ = g.Nodes().ForEachByLabel(label, graphpkg.QueryOpts{}, func(n *types.Node) bool {
				if n.Ordinal() != nodeOrd[n.ID()] {
					t.Fatalf("%s: ForEachByLabel row %d ordinal %d, want %d", step, n.ID(), n.Ordinal(), nodeOrd[n.ID()])
				}
				return true
			})
			ids := make([]types.NodeID, 0, len(nodeOrd))
			for id := range nodeOrd {
				ids = append(ids, id)
			}
			got, err := g.Nodes().GetByIDs(ids)
			if err != nil {
				t.Fatal(err)
			}
			for _, n := range got {
				if n.Ordinal() != nodeOrd[n.ID()] {
					t.Fatalf("%s: GetByIDs row %d ordinal %d", step, n.ID(), n.Ordinal())
				}
			}
		}
		check("created")

		// Relationships: unique, stable through the adjacency and type doors.
		relOrd := map[types.RelID]uint32{}
		relSeen := map[uint32]bool{}
		for _, r := range rels {
			got, err := g.Rels().Lend(ctx, r.ID())
			if err != nil {
				t.Fatal(err)
			}
			if got.Ordinal() == 0 || relSeen[got.Ordinal()] {
				t.Fatalf("relationship %d ordinal %d (zero or repeated)", r.ID(), got.Ordinal())
			}
			relSeen[got.Ordinal()], relOrd[r.ID()] = true, got.Ordinal()
		}
		out, err := g.Rels().Outgoing(nodes[0].ID(), "T")
		if err != nil || len(out) != 1 || out[0].Ordinal() != relOrd[rels[0].ID()] {
			t.Fatalf("Outgoing ordinal = %v, %v", out, err)
		}
		_ = g.Rels().ForEachByType("T", graphpkg.QueryOpts{}, func(r *types.Relationship) bool {
			if r.Ordinal() != relOrd[r.ID()] {
				t.Fatalf("ForEachByType row %d ordinal %d, want %d", r.ID(), r.Ordinal(), relOrd[r.ID()])
			}
			return true
		})

		// Writes keep the ordinal (two-phase: read, write, read again).
		if err := g.Nodes().SetProperty(ctx, nodes[3].ID(), "i", int64(99)); err != nil {
			t.Fatal(err)
		}
		if err := g.Nodes().AddLabel(ctx, nodes[4].ID(), "Q"); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Nodes().Update(ctx, nodes[5].ID(), map[string]any{"j": int64(1)}); err != nil {
			t.Fatal(err)
		}
		if err := g.Rels().SetProperty(ctx, rels[2].ID(), "w", int64(1)); err != nil {
			t.Fatal(err)
		}
		check("after writes")
		if got, _ := g.Rels().Lend(ctx, rels[2].ID()); got.Ordinal() != relOrd[rels[2].ID()] {
			t.Fatal("a relationship property write changed its ordinal")
		}
		hist, err := g.Nodes().History(nodes[5].ID())
		if err != nil {
			t.Fatal(err)
		}
		for _, h := range hist {
			if h.Ordinal() != nodeOrd[nodes[5].ID()] {
				t.Fatalf("history row of a live node carries %d, want %d", h.Ordinal(), nodeOrd[nodes[5].ID()])
			}
		}

		// A deleted entity's ordinal is never handed out again.
		gone := nodes[11]
		goneOrd := nodeOrd[gone.ID()]
		if err := g.Nodes().Delete(ctx, gone.ID()); err != nil {
			t.Fatal(err)
		}
		delete(nodeOrd, gone.ID())
		for i := 0; i < 5; i++ {
			n, err := g.Nodes().Add(ctx, []string{label}, nil)
			if err != nil {
				t.Fatal(err)
			}
			lent, _ := g.Nodes().Lend(ctx, n.ID())
			if lent.Ordinal() == goneOrd || seen[lent.Ordinal()] != 0 || lent.Ordinal() == 0 {
				t.Fatalf("new node got ordinal %d (deleted %d, seen %v)", lent.Ordinal(), goneOrd, seen[lent.Ordinal()])
			}
			seen[lent.Ordinal()], nodeOrd[n.ID()] = n.ID(), lent.Ordinal()
		}
		check("after delete and creates")
		if maxN, _, _ := g.Nodes().MaxOrdinal(); maxN < uint32(len(seen)) {
			t.Fatalf("MaxOrdinal %d below the %d ordinals handed out", maxN, len(seen))
		}
	})
}

func TestOrdinalsConcurrentCreates(t *testing.T) {
	for _, b := range allStoreBackends() {
		if b.tiered {
			continue
		}
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			const workers, each = 4, 50
			var mu sync.Mutex
			var ids []types.NodeID
			var wg sync.WaitGroup
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < each; i++ {
						n, err := g.Nodes().Add(ctx, []string{"C"}, nil)
						if err != nil {
							t.Error(err)
							return
						}
						got, err := g.Nodes().Lend(ctx, n.ID())
						if err != nil || got.Ordinal() == 0 {
							t.Errorf("lend: %v ordinal %d", err, got.Ordinal())
							return
						}
						mu.Lock()
						ids = append(ids, n.ID())
						mu.Unlock()
					}
				}()
			}
			wg.Wait()
			seen := map[uint32]bool{}
			for _, id := range ids {
				n, err := g.Nodes().Lend(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				if seen[n.Ordinal()] {
					t.Fatalf("ordinal %d handed out twice", n.Ordinal())
				}
				seen[n.Ordinal()] = true
			}
			if maxN, _, _ := g.Nodes().MaxOrdinal(); maxN != uint32(workers*each) {
				t.Fatalf("MaxOrdinal = %d, want %d (dense: no gaps without deletes)", maxN, workers*each)
			}
		})
	}
}

// A memory store numbers the relationships of a declared segment type 0: a
// row that moves between the memtable and a segment would change ordinal.
func TestOrdinalsDeclaredSegmentTypeHasNone(t *testing.T) {
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 4, Store: memory.New(), RelSegments: []graphpkg.RelSegmentSpec{{
		Type: "HOP", Columns: []graphpkg.SegmentColumn{{Name: "w", Kind: graphpkg.SegmentInt64}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	ctx := context.Background()
	a, _ := g.Nodes().Add(ctx, []string{"A"}, nil)
	b, _ := g.Nodes().Add(ctx, []string{"A"}, nil)
	hop, err := g.Rels().Add(ctx, "HOP", a, b, map[string]any{"w": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	other, err := g.Rels().Add(ctx, "OTHER", a, b, nil)
	if err != nil {
		t.Fatal(err)
	}
	if r, _ := g.Rels().Lend(ctx, hop.ID()); r.Ordinal() != 0 {
		t.Fatalf("declared-type relationship ordinal %d, want 0", r.Ordinal())
	}
	if r, _ := g.Rels().Lend(ctx, other.ID()); r.Ordinal() == 0 {
		t.Fatal("an undeclared type's relationship must carry an ordinal")
	}
	if err := g.Admin().SealRelSegments("HOP"); err != nil {
		t.Fatal(err)
	}
	if r, _ := g.Rels().Lend(ctx, hop.ID()); r.Ordinal() != 0 {
		t.Fatalf("sealed relationship ordinal %d, want 0", r.Ordinal())
	}
}

// Badger numbers the stored entities 1..N in ID order when it opens; the
// numbering is not persisted, so it is checked for its own guarantees only.
func TestOrdinalsBadgerReopen(t *testing.T) {
	dir := t.TempDir()
	open := func() *graphpkg.Graph {
		g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 2, BadgerDir: dir})
		if err != nil {
			t.Fatal(err)
		}
		return g
	}
	g := open()
	ctx := context.Background()
	var ids []types.NodeID
	for i := 0; i < 10; i++ {
		n, err := g.Nodes().Add(ctx, []string{"P"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, n.ID())
	}
	if err := g.Nodes().Delete(ctx, ids[3]); err != nil {
		t.Fatal(err)
	}
	ids = append(ids[:3], ids[4:]...)
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g = open()
	defer g.Close()
	for i, id := range ids {
		n, err := g.Nodes().Lend(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if n.Ordinal() != uint32(i+1) {
			t.Fatalf("after reopen node %d (the %d-th by ID) has ordinal %d, want %d", id, i+1, n.Ordinal(), i+1)
		}
	}
	if maxN, ok, _ := g.Nodes().MaxOrdinal(); !ok || maxN != uint32(len(ids)) {
		t.Fatalf("MaxOrdinal after reopen = %d (%v), want %d", maxN, ok, len(ids))
	}
}

// The sharded store's slots draw from one allocator: nodes on two slots never
// share an ordinal.
func TestOrdinalsShardedUniqueAcrossSlots(t *testing.T) {
	for _, b := range allStoreBackends() {
		if b.name != "sharded" {
			continue
		}
		g := b.open(t)
		ctx := context.Background()
		seen := map[uint32]types.NodeID{}
		for i := 0; i < 6; i++ {
			a, err := g.Nodes().Add(ctx, []string{"P"}, nil) // slot 0
			if err != nil {
				t.Fatal(err)
			}
			other, err := g.Nodes().Import(ctx, types.NodeID(g.Rels().NextID()), []string{"P"}, nil) // slot 1
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range []types.NodeID{a.ID(), other.ID()} {
				n, err := g.Nodes().Lend(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				if prev, dup := seen[n.Ordinal()]; dup || n.Ordinal() == 0 {
					t.Fatalf("ordinal %d on %d and %d", n.Ordinal(), id, prev)
				}
				seen[n.Ordinal()] = id
			}
		}
	}
}
