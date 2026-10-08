package graph_test

import (
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// checkEndpointOrdinals asserts that every row carries its endpoints'
// ordinals as Lend reports them, nonzero unless none is true.
func checkEndpointOrdinals(t *testing.T, g *graphpkg.Graph, door string, rows []*types.Relationship, none bool) {
	t.Helper()
	ctx := context.Background()
	for _, r := range rows {
		s, err := g.Nodes().Lend(ctx, r.StartNodeID())
		if err != nil {
			t.Fatal(err)
		}
		e, err := g.Nodes().Lend(ctx, r.EndNodeID())
		if err != nil {
			t.Fatal(err)
		}
		if r.StartOrdinal() != s.Ordinal() || r.EndOrdinal() != e.Ordinal() {
			t.Fatalf("%s: rel %d carries (%d, %d), Lend says (%d, %d)", door, r.ID(), r.StartOrdinal(), r.EndOrdinal(), s.Ordinal(), e.Ordinal())
		}
		if none != (r.StartOrdinal() == 0) || none != (r.EndOrdinal() == 0) {
			t.Fatalf("%s: rel %d carries (%d, %d), none=%v", door, r.ID(), r.StartOrdinal(), r.EndOrdinal(), none)
		}
	}
}

// endpointOrdinalRows reads every relationship of KNOWS through each read
// door and checks the rows' endpoint ordinals, and the column batches' too.
func endpointOrdinalRows(t *testing.T, g *graphpkg.Graph, step string, nodes []*types.Node, none bool) map[types.RelID][2]uint32 {
	t.Helper()
	ctx := context.Background()
	byType, err := g.Rels().ByType("KNOWS", graphpkg.QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	checkEndpointOrdinals(t, g, step+" ByType", byType, none)
	var streamed []*types.Relationship
	if err := g.Rels().ForEachByType("KNOWS", graphpkg.QueryOpts{}, func(r *types.Relationship) bool {
		streamed = append(streamed, r)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	checkEndpointOrdinals(t, g, step+" ForEachByType", streamed, none)
	all, err := g.Rels().All(graphpkg.QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	checkEndpointOrdinals(t, g, step+" All", all, none)
	var point []*types.Relationship
	for _, r := range byType {
		got, err := g.Rels().Get(ctx, r.ID())
		if err != nil {
			t.Fatal(err)
		}
		lent, err := g.Rels().Lend(ctx, r.ID())
		if err != nil {
			t.Fatal(err)
		}
		point = append(point, got, lent)
	}
	checkEndpointOrdinals(t, g, step+" Get/Lend", point, none)
	for _, n := range nodes {
		out, err := g.Rels().Outgoing(n.ID(), "")
		if err != nil {
			t.Fatal(err)
		}
		checkEndpointOrdinals(t, g, step+" Outgoing", out, none)
		in, err := g.Rels().Incoming(n.ID(), "KNOWS")
		if err != nil {
			t.Fatal(err)
		}
		checkEndpointOrdinals(t, g, step+" Incoming", in, none)
	}
	got := map[types.RelID][2]uint32{}
	for _, r := range byType {
		got[r.ID()] = [2]uint32{r.StartOrdinal(), r.EndOrdinal()}
	}
	scanned := 0
	ok, err := g.ScanRelColumns("KNOWS", []string{"w"}, graphpkg.QueryOpts{}, func(b *graphpkg.RelColumnBatch) bool {
		if len(b.StartOrdinals) != len(b.IDs) || len(b.EndOrdinals) != len(b.IDs) {
			t.Fatalf("%s: batch of %d IDs with %d/%d endpoint ordinals", step, len(b.IDs), len(b.StartOrdinals), len(b.EndOrdinals))
		}
		for i, id := range b.IDs {
			if want := got[id]; b.StartOrdinals[i] != want[0] || b.EndOrdinals[i] != want[1] {
				t.Fatalf("%s: batch carries (%d, %d) for rel %d, the row (%d, %d)", step, b.StartOrdinals[i], b.EndOrdinals[i], id, want[0], want[1])
			}
			scanned++
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if ok && scanned != len(byType) {
		t.Fatalf("%s: scanned %d of %d", step, scanned, len(byType))
	}
	return got
}

// Every current relationship row carries the dense ordinals of its endpoints,
// equal to what Lend reports for them, through every read door and in the
// column batches; 0 on tiered, which numbers nothing. Two-phase: after a
// relationship is deleted, a relationship updated and new nodes and
// relationships written, the survivors keep theirs; history rows carry 0.
// The sharded fixture puts nodes and relationships on different slots
// (node field 0, relationship field 1), so every endpoint is on another slot.
func TestRelEndpointOrdinals(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		none := b.tiered
		var nodes []*types.Node
		for i := range 12 {
			label := "P"
			if i%3 == 0 {
				label = "Ref" // tiered: the reference shard, so some relationships are cross-shard
			}
			n, err := g.Nodes().Add(ctx, []string{label}, map[string]any{"i": int64(i)})
			if err != nil {
				t.Fatal(err)
			}
			nodes = append(nodes, n)
		}
		var rels []*types.Relationship
		for i := range nodes {
			r, err := g.Rels().Add(ctx, "KNOWS", nodes[i], nodes[(i*5+1)%len(nodes)], map[string]any{"w": int64(i)})
			if err != nil {
				t.Fatal(err)
			}
			rels = append(rels, r)
		}
		self, err := g.Rels().Add(ctx, "KNOWS", nodes[4], nodes[4], map[string]any{"w": int64(99)})
		if err != nil {
			t.Fatal(err)
		}
		before := endpointOrdinalRows(t, g, "first", nodes, none)
		if before[self.ID()][0] != before[self.ID()][1] {
			t.Fatalf("a self-loop carries two ordinals %v", before[self.ID()])
		}

		if err := g.Rels().Delete(ctx, rels[2].ID()); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Rels().Update(ctx, rels[5].ID(), map[string]any{"w": int64(-5)}); err != nil {
			t.Fatal(err)
		}
		fresh, err := g.Nodes().Add(ctx, []string{"P"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		nodes = append(nodes, fresh)
		late, err := g.Rels().Add(ctx, "KNOWS", fresh, nodes[1], map[string]any{"w": int64(7)})
		if err != nil {
			t.Fatal(err)
		}
		after := endpointOrdinalRows(t, g, "after the writes", nodes, none)
		if _, ok := after[rels[2].ID()]; ok {
			t.Fatal("the deleted relationship is still read")
		}
		for id, ords := range before {
			if id == rels[2].ID() {
				continue
			}
			if after[id] != ords {
				t.Fatalf("rel %d's endpoint ordinals moved: %v -> %v", id, ords, after[id])
			}
		}
		if _, ok := after[late.ID()]; !ok {
			t.Fatal("the new relationship is not read")
		}
		hist, err := g.Rels().History(rels[5].ID())
		if err != nil {
			t.Fatal(err)
		}
		cur, err := g.Rels().Lend(ctx, rels[5].ID())
		if err != nil {
			t.Fatal(err)
		}
		past := 0
		for _, h := range hist {
			if h.Version() == cur.Version() {
				continue
			}
			past++
			if h.StartOrdinal() != 0 || h.EndOrdinal() != 0 {
				t.Fatalf("history version %d carries (%d, %d)", h.Version(), h.StartOrdinal(), h.EndOrdinal())
			}
		}
		if past == 0 {
			t.Fatal("no history row to check")
		}
	})
}

// Badger with DisableOrdinals numbers nothing: every row carries 0, the
// column batches too.
func TestRelEndpointOrdinalsDisabled(t *testing.T) {
	st, err := badger.New(badger.Config{InMemory: true, DisableOrdinals: true})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 0, Store: st})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	ctx := context.Background()
	a, _ := g.Nodes().Add(ctx, []string{"P"}, nil)
	c, _ := g.Nodes().Add(ctx, []string{"P"}, nil)
	if _, err := g.Rels().Add(ctx, "KNOWS", a, c, map[string]any{"w": int64(1)}); err != nil {
		t.Fatal(err)
	}
	endpointOrdinalRows(t, g, "disabled", []*types.Node{a, c}, true)
}

// After a reopen badger renumbers the stored entities; the rows' endpoint
// ordinals follow the new numbering (they are read from the RAM maps, never
// persisted). The sharded store's slots resolve each other's endpoints after
// their reopen too.
func TestRelEndpointOrdinalsAfterReopen(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  func(t *testing.T, dir string) graphpkg.Config
	}{
		{"badger", func(_ *testing.T, dir string) graphpkg.Config {
			return graphpkg.Config{SnowflakeNodeID: 0, BadgerDir: dir}
		}},
		{"sharded", func(t *testing.T, dir string) graphpkg.Config {
			st, err := sharded.New(sharded.Config{Dir: dir, BaseSlot: 0, SlotCount: 2})
			if err != nil {
				t.Fatal(err)
			}
			return graphpkg.Config{SnowflakeNodeID: 0, Store: st}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			ctx := context.Background()
			open := func() *graphpkg.Graph {
				g, err := graphpkg.New(tc.cfg(t, dir))
				if err != nil {
					t.Fatal(err)
				}
				return g
			}
			g := open()
			var nodes []*types.Node
			for range 6 {
				n, err := g.Nodes().Add(ctx, []string{"P"}, nil)
				if err != nil {
					t.Fatal(err)
				}
				nodes = append(nodes, n)
			}
			// Delete the first nodes so the reopen's 1..N numbering differs.
			for _, n := range nodes[:2] {
				if err := g.Nodes().Delete(ctx, n.ID()); err != nil {
					t.Fatal(err)
				}
			}
			nodes = nodes[2:]
			for i := range nodes {
				if _, err := g.Rels().Add(ctx, "KNOWS", nodes[i], nodes[(i+1)%len(nodes)], map[string]any{"w": int64(i)}); err != nil {
					t.Fatal(err)
				}
			}
			before := endpointOrdinalRows(t, g, "before the reopen", nodes, false)
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			g = open()
			defer g.Close()
			after := endpointOrdinalRows(t, g, "after the reopen", nodes, false)
			if len(after) != len(before) {
				t.Fatalf("%d relationships after the reopen, %d before", len(after), len(before))
			}
		})
	}
}

// A declared segment type's rows carry 0 for the endpoints, as for their own
// ordinal, in the memtable and once sealed, rows and column batches alike.
func TestRelEndpointOrdinalsSegmentType(t *testing.T) {
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 4, Store: memory.New(), RelSegments: []graphpkg.RelSegmentSpec{{
		Type: "HOP", Columns: []graphpkg.SegmentColumn{{Name: "w", Kind: graphpkg.SegmentInt64}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	ctx := context.Background()
	a, _ := g.Nodes().Add(ctx, []string{"P"}, nil)
	c, _ := g.Nodes().Add(ctx, []string{"P"}, nil)
	for i := range 3 {
		if _, err := g.Rels().Add(ctx, "HOP", a, c, map[string]any{"w": int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	check := func(step string) {
		rows, err := g.Rels().ByType("HOP", graphpkg.QueryOpts{})
		if err != nil || len(rows) != 3 {
			t.Fatalf("%s: %d rows, %v", step, len(rows), err)
		}
		for _, r := range rows {
			if r.StartOrdinal() != 0 || r.EndOrdinal() != 0 {
				t.Fatalf("%s: a segment type's row carries (%d, %d)", step, r.StartOrdinal(), r.EndOrdinal())
			}
		}
		ok, err := g.ScanRelColumns("HOP", []string{"w"}, graphpkg.QueryOpts{}, func(b *graphpkg.RelColumnBatch) bool {
			if len(b.StartOrdinals) != len(b.IDs) || len(b.EndOrdinals) != len(b.IDs) {
				t.Fatalf("%s: batch of %d IDs with %d/%d endpoint ordinals", step, len(b.IDs), len(b.StartOrdinals), len(b.EndOrdinals))
			}
			for i := range b.IDs {
				if b.StartOrdinals[i] != 0 || b.EndOrdinals[i] != 0 {
					t.Fatalf("%s: a segment batch row carries endpoint ordinals", step)
				}
			}
			return true
		})
		if err != nil || !ok {
			t.Fatalf("%s: %v %v", step, ok, err)
		}
	}
	check("memtable")
	if err := g.Admin().SealRelSegments("HOP"); err != nil {
		t.Fatal(err)
	}
	check("sealed")
}
