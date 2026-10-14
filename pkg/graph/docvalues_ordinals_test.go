package graph_test

import (
	"context"
	"slices"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// docRow is one row a DocValues door handed out.
type docRow struct {
	id      types.NodeID
	ord     uint32
	vals    []any
	present []bool
}

// TestForEachDocValuesOrdinal (sigma-tkgd round 6 request 11): the ordinal
// doors hand out the rows ForEachDocValues / ForEachDocValuesMulti hand out,
// in the same order, each with the node's ordinal — the one a point read of
// the node carries. Two-phase: after a property write, a delete and a new
// member, the rows and ordinals follow (a kept node keeps its ordinal, the
// deleted one is gone, the new one has its own). An early stop stops. A store
// without ordinals (tiered) declines with ok=false and no row; an unknown
// label declines as ForEachDocValues does.
func TestForEachDocValuesOrdinal(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		keys := []string{"v", "s"}
		var all []*types.Node
		add := func(v int64, labels ...string) *types.Node {
			n, err := g.Nodes().Add(ctx, labels, map[string]any{"v": v, "s": "x"})
			if err != nil {
				t.Fatal(err)
			}
			all = append(all, n)
			return n
		}
		for i := range 12 {
			if i%3 == 0 {
				add(int64(i), "A", "B")
			} else {
				add(int64(i), "A")
			}
		}
		_, hasOrdinals, err := g.Nodes().MaxOrdinal()
		if err != nil {
			t.Fatal(err)
		}

		plain := func(multi bool) ([]docRow, uint64) {
			t.Helper()
			var rows []docRow
			collect := func(id types.NodeID, vals []any, present []bool) bool {
				rows = append(rows, docRow{id: id, vals: slices.Clone(vals), present: slices.Clone(present)})
				return true
			}
			var gen uint64
			var ok bool
			var err error
			if multi {
				gen, ok, err = g.Nodes().ForEachDocValuesMulti([]string{"A", "B"}, keys, collect)
			} else {
				gen, ok, err = g.Nodes().ForEachDocValues("A", keys, collect)
			}
			if err != nil || !ok {
				t.Fatalf("plain door multi=%v: ok=%v err=%v", multi, ok, err)
			}
			return rows, gen
		}
		withOrd := func(multi bool, stopAfter int) ([]docRow, uint64, bool) {
			t.Helper()
			var rows []docRow
			collect := func(id types.NodeID, ord uint32, vals []any, present []bool) bool {
				rows = append(rows, docRow{id: id, ord: ord, vals: slices.Clone(vals), present: slices.Clone(present)})
				return stopAfter == 0 || len(rows) < stopAfter
			}
			var gen uint64
			var ok bool
			var err error
			if multi {
				gen, ok, err = g.Nodes().ForEachDocValuesMultiOrdinal([]string{"A", "B"}, keys, collect)
			} else {
				gen, ok, err = g.Nodes().ForEachDocValuesOrdinal("A", keys, collect)
			}
			if err != nil {
				t.Fatalf("ordinal door multi=%v: %v", multi, err)
			}
			return rows, gen, ok
		}
		check := func(step string) {
			t.Helper()
			for _, multi := range []bool{false, true} {
				got, gen, ok := withOrd(multi, 0)
				if !hasOrdinals {
					if ok || len(got) != 0 {
						t.Fatalf("%s multi=%v: a store without ordinals served %d rows (ok=%v); it declines", step, multi, len(got), ok)
					}
					continue
				}
				if !ok {
					t.Fatalf("%s multi=%v: declined on a store with ordinals", step, multi)
				}
				want, wantGen := plain(multi)
				if gen != wantGen {
					t.Fatalf("%s multi=%v: gen %d, the plain door's %d", step, multi, gen, wantGen)
				}
				if len(got) != len(want) {
					t.Fatalf("%s multi=%v: %d rows, the plain door %d", step, multi, len(got), len(want))
				}
				seen := map[uint32]types.NodeID{}
				for i := range got {
					if got[i].id != want[i].id || !slices.Equal(got[i].vals, want[i].vals) || !slices.Equal(got[i].present, want[i].present) {
						t.Fatalf("%s multi=%v row %d: %+v, the plain door %+v", step, multi, i, got[i], want[i])
					}
					n, err := g.Nodes().Get(ctx, got[i].id)
					if err != nil {
						t.Fatal(err)
					}
					if got[i].ord == 0 || got[i].ord != n.Ordinal() {
						t.Fatalf("%s multi=%v: node %d ordinal %d, its row carries %d", step, multi, got[i].id, got[i].ord, n.Ordinal())
					}
					if other, dup := seen[got[i].ord]; dup {
						t.Fatalf("%s: ordinal %d on %d and %d", step, got[i].ord, other, got[i].id)
					}
					seen[got[i].ord] = got[i].id
				}
				if stopped, _, _ := withOrd(multi, 2); len(stopped) != 2 {
					t.Fatalf("%s multi=%v: an early stop after 2 rows read %d", step, multi, len(stopped))
				}
			}
		}
		check("first")
		before := map[types.NodeID]uint32{}
		if hasOrdinals {
			rows, _, _ := withOrd(false, 0)
			for _, r := range rows {
				before[r.id] = r.ord
			}
		}

		// Phase two: a property write, a delete, a new member of both labels.
		if _, err := g.Nodes().Update(ctx, all[1].ID(), map[string]any{"v": int64(100)}); err != nil {
			t.Fatal(err)
		}
		if err := g.Nodes().Delete(ctx, all[3].ID()); err != nil {
			t.Fatal(err)
		}
		fresh := add(77, "A", "B")
		check("after writes")
		if hasOrdinals {
			rows, _, _ := withOrd(false, 0)
			for _, r := range rows {
				if r.id == all[3].ID() {
					t.Fatal("the deleted node is still a row")
				}
				if old, kept := before[r.id]; kept && old != r.ord {
					t.Fatalf("node %d changed ordinal %d -> %d", r.id, old, r.ord)
				}
				if r.id == all[1].ID() && r.vals[0] != int64(100) {
					t.Fatalf("updated node reads %v", r.vals[0])
				}
			}
			if !slices.ContainsFunc(rows, func(r docRow) bool { return r.id == fresh.ID() }) {
				t.Fatal("the new member is missing")
			}
		}

		if _, ok, err := g.Nodes().ForEachDocValuesOrdinal("NoSuchLabel", keys, func(types.NodeID, uint32, []any, []bool) bool { return true }); err != nil || ok {
			t.Fatalf("unknown label: ok=%v err=%v, want a decline", ok, err)
		}
		if _, ok, err := g.Nodes().ForEachDocValuesMultiOrdinal([]string{"A", "NoSuchLabel"}, keys, func(types.NodeID, uint32, []any, []bool) bool { return true }); err != nil || ok {
			t.Fatalf("unknown label in an intersection: ok=%v err=%v, want a decline", ok, err)
		}
	})
}

// TestForEachDocValuesOrdinalDeclinesWithoutOrdinals: a badger store opened
// with DisableOrdinals numbers no node, so the ordinal doors decline (ok=false,
// no row) rather than hand out zeros; the plain doors still serve.
func TestForEachDocValuesOrdinalDeclinesWithoutOrdinals(t *testing.T) {
	st, err := badger.New(badger.Config{InMemory: true, DisableOrdinals: true})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graphpkg.New(graphpkg.Config{Store: st})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if _, err := g.Nodes().Add(context.Background(), []string{"A", "B"}, map[string]any{"v": int64(1)}); err != nil {
		t.Fatal(err)
	}
	rows := 0
	count := func(types.NodeID, uint32, []any, []bool) bool { rows++; return true }
	if _, ok, err := g.Nodes().ForEachDocValuesOrdinal("A", []string{"v"}, count); err != nil || ok || rows != 0 {
		t.Fatalf("single: ok=%v rows=%d err=%v", ok, rows, err)
	}
	if _, ok, err := g.Nodes().ForEachDocValuesMultiOrdinal([]string{"A", "B"}, []string{"v"}, count); err != nil || ok || rows != 0 {
		t.Fatalf("multi: ok=%v rows=%d err=%v", ok, rows, err)
	}
	if _, ok, err := g.Nodes().ForEachDocValues("A", []string{"v"}, func(types.NodeID, []any, []bool) bool { return true }); err != nil || !ok {
		t.Fatalf("plain door: ok=%v err=%v", ok, err)
	}
}
