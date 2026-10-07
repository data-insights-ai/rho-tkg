package graph_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// nativeRowStore reports whether the graph trusts the backend's rows and hands
// them out as the store holds them (memory, badger); tiered and sharded rows go
// through the graph layer's validating copy.
func nativeRowStore(b storeBackend) bool { return b.name == "memory" || b.name == "badger" }

// The node property doors (an indexed equality lookup, a composite lookup, a
// vector search) are plural reads: memory hands out the store's frozen rows as
// badger and the label and type scans do (it deep-copied every match). Two-phase:
// rows read before a write keep the old value, a read after it has the new one;
// mutating a returned row fails with ErrFrozenNode; DeepCopy thaws. Tiered and
// sharded keep the graph layer's copy; their answers are checked the same way.
func TestNodePropertyDoorsReturnFrozenRows(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		label := "P"
		if b.tiered {
			label = "Ref" // tiered indexes reference entities only
		}
		if err := g.Index().CreateProperty(label, "city"); err != nil {
			t.Fatal(err)
		}
		if err := g.Index().CreateComposite(label, []string{"city", "zip"}); err != nil && !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
			t.Fatal(err)
		}
		a, err := g.Nodes().Add(ctx, []string{label}, map[string]any{"city": "Linz", "zip": int64(4020), "w": int64(1)})
		if err != nil {
			t.Fatal(err)
		}
		other, err := g.Nodes().Add(ctx, []string{label}, map[string]any{"city": "Wels", "zip": int64(4600), "w": int64(1)})
		if err != nil {
			t.Fatal(err)
		}

		before, err := g.Nodes().ByLabelAndProperty(label, "city", "Linz", storepkg.QueryOpts{})
		if err != nil {
			t.Fatal(err)
		}
		composite, err := g.Nodes().ByLabelAndProperties(label, map[string]any{"city": "Linz", "zip": int64(4020)}, storepkg.QueryOpts{})
		if err != nil {
			t.Fatal(err)
		}
		for name, rows := range map[string][]*types.Node{"property": before, "composite": composite} {
			if len(rows) != 1 || rows[0].ID() != a.ID() {
				t.Fatalf("%s: got %d rows, want exactly the Linz node", name, len(rows))
			}
			if rows[0].ID() == other.ID() {
				t.Fatalf("%s: must not return the Wels node", name)
			}
			if !nativeRowStore(b) {
				continue
			}
			if !rows[0].IsFrozen() {
				t.Fatalf("%s: rows must be frozen", name)
			}
			if err := rows[0].SetProperty("w", int64(9)); !errors.Is(err, types.ErrFrozenNode) {
				t.Fatalf("%s: SetProperty on a returned row: %v, want ErrFrozenNode", name, err)
			}
			thawed := rows[0].DeepCopy()
			if thawed.IsFrozen() {
				t.Fatalf("%s: DeepCopy must thaw", name)
			}
			if err := thawed.SetProperty("w", int64(9)); err != nil {
				t.Fatalf("%s: SetProperty on the thawed copy: %v", name, err)
			}
		}

		if _, err := g.Nodes().Update(ctx, a.ID(), map[string]any{"w": int64(2)}); err != nil {
			t.Fatal(err)
		}
		if w, _ := before[0].GetProperty("w"); w != int64(1) {
			t.Fatalf("a row read before the write: w = %v, want 1", w)
		}
		after, err := g.Nodes().ByLabelAndProperty(label, "city", "Linz", storepkg.QueryOpts{})
		if err != nil {
			t.Fatal(err)
		}
		if len(after) != 1 {
			t.Fatalf("after the write: %d rows, want 1", len(after))
		}
		if w, _ := after[0].GetProperty("w"); w != int64(2) {
			t.Fatalf("a row read after the write: w = %v, want 2", w)
		}
		if missing, err := g.Nodes().ByLabelAndProperty(label, "city", "Graz", storepkg.QueryOpts{}); err != nil || len(missing) != 0 {
			t.Fatalf("a phantom value: %d rows, %v; want none", len(missing), err)
		}
	})
}

// A vector search hands out frozen rows too, in distance order.
func TestVectorSearchReturnsFrozenRows(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		if err := g.Index().CreateVector("V", "emb", 2, storepkg.DistanceCosine); err != nil {
			if errors.Is(err, storepkg.ErrCapabilityNotSupported) {
				t.Skip("no vector index on this backend")
			}
			t.Fatal(err)
		}
		near, err := g.Nodes().Add(ctx, []string{"V"}, map[string]any{"emb": []float32{1, 0}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := g.Nodes().Add(ctx, []string{"V"}, map[string]any{"emb": []float32{0, 1}}); err != nil {
			t.Fatal(err)
		}
		rows, err := g.Index().SearchNearest("V", "emb", []float32{1, 0.1}, 2, storepkg.QueryOpts{})
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 2 || rows[0].ID() != near.ID() {
			t.Fatalf("got %d rows, the nearest first expected", len(rows))
		}
		for _, n := range rows {
			if nativeRowStore(b) && !n.IsFrozen() {
				t.Fatal("vector search rows must be frozen")
			}
		}
	})
}

// BenchmarkNodesByLabelAndProperty: an indexed equality lookup over 20,000
// nodes, one match ("name") and 200 matches ("city"), per backend. It is the
// door a query engine's index seek calls per binding (sigma-tkgd C5b store
// request 2: the memory store deep-copied every match).
func BenchmarkNodesByLabelAndProperty(b *testing.B) {
	for _, backend := range []struct {
		name string
		cfg  graphpkg.Config
	}{
		{"memory", graphpkg.Config{SnowflakeNodeID: 0}},
		{"badger", graphpkg.Config{SnowflakeNodeID: 0, BadgerInMemory: true}},
	} {
		b.Run(backend.name, func(b *testing.B) {
			g, err := graphpkg.New(backend.cfg)
			if err != nil {
				b.Fatal(err)
			}
			defer g.Close()
			ctx := context.Background()
			for _, key := range []string{"name", "city"} {
				if err := g.Index().CreateProperty("Person", key); err != nil {
					b.Fatal(err)
				}
			}
			for i := range 20000 {
				props := map[string]any{"name": fmt.Sprintf("p%d", i), "city": fmt.Sprintf("c%d", i%100), "age": int64(i % 90), "score": float64(i)}
				if _, err := g.Nodes().Add(ctx, []string{"Person"}, props); err != nil {
					b.Fatal(err)
				}
			}
			for _, q := range []struct {
				name, key string
				value     any
				want      int
			}{{"one", "name", "p777", 1}, {"200", "city", "c7", 200}} {
				b.Run(q.name, func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						rows, err := g.Nodes().ByLabelAndProperty("Person", q.key, q.value, storepkg.QueryOpts{})
						if err != nil || len(rows) != q.want {
							b.Fatalf("%d rows, %v", len(rows), err)
						}
					}
				})
			}
		})
	}
}
