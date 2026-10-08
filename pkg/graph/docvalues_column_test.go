package graph_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// builtColumn is what ForEachDocValues actually built for key: none when it
// declines, else numeric or string by the values it streams (a column with
// no present value counts as numeric, the build rule's choice).
func builtColumn(t *testing.T, g *graphpkg.Graph, label, key string) storepkg.DocValuesColumn {
	t.Helper()
	kind := storepkg.DocValuesNumeric
	_, ok, err := g.Nodes().ForEachDocValues(label, []string{key}, func(_ types.NodeID, vals []any, present []bool) bool {
		if present[0] {
			if _, isString := vals[0].(string); isString {
				kind = storepkg.DocValuesString
			}
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		return storepkg.DocValuesNone
	}
	return kind
}

// DocValuesColumn states what the build does, without building: over random
// mixes of value kinds per key (integers, floats, NaN, ±Inf, strings, bools,
// lists, absent), after creates, updates, removals and deletes, on every
// backend, the statement equals the column ForEachDocValues builds.
// Two-phase: a key that was numeric turns none when a string arrives and
// numeric again when it leaves.
func TestDocValuesColumnAgreesWithTheBuild(t *testing.T) {
	values := []func(r *rand.Rand) any{
		func(r *rand.Rand) any { return r.Int64N(100) },
		func(r *rand.Rand) any { return r.Float64() },
		func(*rand.Rand) any { return math.NaN() },
		func(*rand.Rand) any { return math.Inf(-1) },
		func(r *rand.Rand) any { return fmt.Sprint(r.IntN(10)) },
		func(*rand.Rand) any { return true },
		func(*rand.Rand) any { return []int64{1, 2} },
	}
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		label := "C"
		if b.tiered {
			label = "Ref"
		}
		r := rand.New(rand.NewPCG(3, 5))
		keys := []string{"k0", "k1", "k2", "k3"}
		// Each key draws from a small subset of kinds, so pure columns occur.
		kinds := [][]int{{0}, {0, 1, 2, 3}, {4}, {0, 4}}
		var ids []types.NodeID
		check := func(step string) {
			t.Helper()
			for _, key := range append(keys, "never") {
				stated, ok, err := g.Nodes().DocValuesColumn(label, key)
				if err != nil || !ok {
					t.Fatalf("%s %s: DocValuesColumn %v %v", step, key, ok, err)
				}
				if built := builtColumn(t, g, label, key); stated != built {
					t.Fatalf("%s %s: stated %v, built %v", step, key, stated, built)
				}
			}
		}
		for round := range 6 {
			for range 20 {
				props := map[string]any{}
				for k, key := range keys {
					if r.IntN(4) == 0 {
						continue // absent
					}
					pick := kinds[k][r.IntN(len(kinds[k]))]
					if round >= 3 && k == 0 && r.IntN(30) == 0 {
						pick = 5 + r.IntN(2) // a bool or a list in the integer column
					}
					props[key] = values[pick](r)
				}
				n, err := g.Nodes().Add(ctx, []string{label}, props)
				if err != nil {
					t.Fatal(err)
				}
				ids = append(ids, n.ID())
			}
			check(fmt.Sprintf("round %d", round))
			id := ids[r.IntN(len(ids))]
			if _, err := g.Nodes().Update(ctx, id, map[string]any{"k1": "now a string"}); err != nil {
				t.Fatal(err)
			}
			check(fmt.Sprintf("round %d, a string in k1", round))
			if err := g.Nodes().DeleteProperty(ctx, id, "k1"); err != nil {
				t.Fatal(err)
			}
			check(fmt.Sprintf("round %d, the string removed", round))
			if err := g.Nodes().Delete(ctx, ids[0]); err == nil {
				ids = ids[1:]
			}
			check(fmt.Sprintf("round %d, a delete", round))
		}
		if kind, ok, err := g.Nodes().DocValuesColumn("Missing", "k0"); kind != storepkg.DocValuesNone || !ok || err != nil {
			t.Fatalf("unknown label: %v %v %v", kind, ok, err)
		}
		if _, _, err := g.Nodes().DocValuesColumn("", "k0"); err == nil {
			t.Fatal("an empty label passed")
		}
		if _, _, err := g.Nodes().DocValuesColumn(label, "tkg_x"); !errors.Is(err, types.ErrReservedPrefix) {
			t.Fatalf("a reserved key: %v", err)
		}
	})
}

// The rule itself, branch by branch (store.DocValuesColumnOf).
func TestDocValuesColumnOf(t *testing.T) {
	type counts = storepkg.PropertyTypeClassCounts
	for _, tc := range []struct {
		name string
		c    counts
		n    int
		want storepkg.DocValuesColumn
	}{
		{"empty label", counts{Numeric: 0}, 0, storepkg.DocValuesNone},
		{"over the cap", counts{Numeric: 1}, 11, storepkg.DocValuesNone},
		{"bool", counts{Numeric: 2, Bool: 1}, 3, storepkg.DocValuesNone},
		{"other", counts{String: 2, Other: 1}, 3, storepkg.DocValuesNone},
		{"mixed", counts{Numeric: 1, String: 1}, 2, storepkg.DocValuesNone},
		{"NaN and string", counts{NaN: 1, String: 1}, 2, storepkg.DocValuesNone},
		{"strings", counts{String: 2}, 5, storepkg.DocValuesString},
		{"numbers and NaN", counts{Numeric: 2, NaN: 1}, 5, storepkg.DocValuesNumeric},
		{"all absent", counts{}, 5, storepkg.DocValuesNumeric},
	} {
		if got := storepkg.DocValuesColumnOf(tc.c, tc.n, 10); got != tc.want {
			t.Errorf("%s: %v, want %v", tc.name, got, tc.want)
		}
	}
}

// Badger without planner statistics keeps no class counts: ok=false. A closed
// graph is an error.
func TestDocValuesColumnWithoutCounters(t *testing.T) {
	st, err := badger.New(badger.Config{InMemory: true, DisablePlannerStats: true})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 0, Store: st})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Nodes().Add(context.Background(), []string{"C"}, map[string]any{"k": int64(1)}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := g.Nodes().DocValuesColumn("C", "k"); ok || err != nil {
		t.Fatalf("without counters: ok=%v err=%v, want ok=false", ok, err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := g.Nodes().DocValuesColumn("C", "k"); !errors.Is(err, graphpkg.ErrGraphClosed) {
		t.Fatalf("closed: %v", err)
	}
}

// BenchmarkDocValuesColumn: knowing a column's kind for a label of 20,000
// nodes after a write, by the statement against by building the column
// (DocValuesSnapshot rebuilds after every write) — sigma-tkgd C4c store
// request 3.
func BenchmarkDocValuesColumn(b *testing.B) {
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
			var last *types.Node
			for i := range 20000 {
				if last, err = g.Nodes().Add(ctx, []string{"P"}, map[string]any{"age": int64(i % 90)}); err != nil {
					b.Fatal(err)
				}
			}
			write := func(i int) {
				if _, err := g.Nodes().Update(ctx, last.ID(), map[string]any{"age": int64(i)}); err != nil {
					b.Fatal(err)
				}
			}
			b.Run("statement", func(b *testing.B) {
				i := 0
				for b.Loop() {
					i++
					write(i)
					if kind, ok, err := g.Nodes().DocValuesColumn("P", "age"); kind != storepkg.DocValuesNumeric || !ok || err != nil {
						b.Fatal(kind, ok, err)
					}
				}
			})
			b.Run("build", func(b *testing.B) {
				i := 0
				for b.Loop() {
					i++
					write(i)
					if _, _, ok, err := g.Nodes().DocValuesSnapshot("P", []string{"age"}); !ok || err != nil {
						b.Fatal(ok, err)
					}
				}
			})
		})
	}
}
