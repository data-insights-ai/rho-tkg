package graph_test

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// idValue is one (ID, value) an ordered ID walk handed out.
type idValue struct {
	id int64
	v  any
}

func renderIDValues(ps []idValue) string {
	var b strings.Builder
	for _, p := range ps {
		fmt.Fprintf(&b, "%d=%T(%v) ", p.id, p.v, p.v)
	}
	return b.String()
}

// orderedOracle sorts the current (ID, value) pairs that keep is true for
// by value (numbers as float64, strings bytewise), ties by ID ascending.
func orderedOracle(rows []idValue, keep func(any) bool, desc bool) []idValue {
	var out []idValue
	for _, r := range rows {
		if keep(r.v) {
			out = append(out, r)
		}
	}
	key := func(v any) (float64, string) {
		switch x := v.(type) {
		case int64:
			return float64(x), ""
		case int:
			return float64(x), ""
		case float64:
			return x, ""
		case string:
			return 0, x
		}
		return 0, ""
	}
	slices.SortFunc(out, func(a, b idValue) int {
		fa, sa := key(a.v)
		fb, sb := key(b.v)
		c := cmp.Compare(fa, fb)
		if c == 0 {
			c = strings.Compare(sa, sb)
		}
		if desc {
			c = -c
		}
		if c != 0 {
			return c
		}
		return cmp.Compare(a.id, b.id)
	})
	return out
}

// TestForEachIDOrdered (sigma-tkgd round 7 request 5): on memory and badger
// the ordered ID walks hand out exactly the (ID, stored value) pairs a
// brute-force walk over the current rows keeps — exact bounds, value order
// both ways, ties by ID, int64 3 and float64 3.0 in one position with their
// types — after updates and deletes; early stop stops; a canceled context
// stops within 4,096 entries; unsupported cases report ErrIndexNotFound
// (temporal opts, no index, tiered, sharded); an unknown label walks
// nothing. Nodes and relationships.
func TestForEachIDOrdered(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		if err := g.Index().CreateProperty("Ref", "v"); err != nil {
			t.Fatal(err)
		}
		if err := g.Index().CreateRelProperty("R", "v"); err != nil && !b.tiered {
			t.Fatal(err)
		}
		values := []any{int64(3), 3.0, int64(-2), 2.5, int64(10), "apple", "apricot", "b", int64(7), 7.5, true, "ap"}
		var nodes []*types.Node
		for i := range 8000 {
			n, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"v": values[i%len(values)]})
			if err != nil {
				t.Fatal(err)
			}
			nodes = append(nodes, n)
		}
		for i := range 40 {
			if _, err := g.Rels().Add(ctx, "R", nodes[i], nodes[i+1], map[string]any{"v": values[(i*5)%len(values)]}); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := g.Nodes().Update(ctx, nodes[0].ID(), map[string]any{"v": int64(8)}); err != nil {
			t.Fatal(err)
		}
		if err := g.Nodes().Delete(ctx, nodes[7999].ID()); err != nil {
			t.Fatal(err)
		}

		var nodeRows, relRows []idValue
		if err := g.Nodes().ForEachByLabel("Ref", storepkg.QueryOpts{}, func(n *types.Node) bool {
			if v, ok := n.GetProperty("v"); ok {
				nodeRows = append(nodeRows, idValue{int64(n.ID()), v})
			}
			return true
		}); err != nil {
			t.Fatal(err)
		}
		if err := g.Rels().ForEachByType("R", storepkg.QueryOpts{}, func(r *types.Relationship) bool {
			if v, ok := r.GetProperty("v"); ok {
				relRows = append(relRows, idValue{int64(r.ID()), v})
			}
			return true
		}); err != nil {
			t.Fatal(err)
		}
		inRange := func(lo, hi float64, inclLo, inclHi bool) func(any) bool {
			return func(v any) bool {
				var f float64
				switch x := v.(type) {
				case int64:
					f = float64(x)
				case float64:
					f = x
				default:
					return false
				}
				return (f > lo || inclLo && f == lo) && (f < hi || inclHi && f == hi)
			}
		}
		hasPrefix := func(p string) func(any) bool {
			return func(v any) bool { s, ok := v.(string); return ok && strings.HasPrefix(s, p) }
		}
		supported := b.name == "memory" || b.name == "badger"
		for _, desc := range []bool{false, true} {
			for _, bounds := range []struct {
				lo, hi         float64
				inclLo, inclHi bool
			}{{3, 7.5, true, true}, {3, 7.5, false, false}, {-100, 100, true, true}} {
				var gotN, gotR []idValue
				errN := g.Nodes().ForEachIDByLabelPropertyRangeOrdered(ctx, "Ref", "v", bounds.lo, bounds.hi, bounds.inclLo, bounds.inclHi, desc, storepkg.QueryOpts{}, func(id types.NodeID, v any) bool {
					gotN = append(gotN, idValue{int64(id), v})
					return true
				})
				errR := g.Rels().ForEachIDByTypePropertyRangeOrdered(ctx, "R", "v", bounds.lo, bounds.hi, bounds.inclLo, bounds.inclHi, desc, storepkg.QueryOpts{}, func(id types.RelID, v any) bool {
					gotR = append(gotR, idValue{int64(id), v})
					return true
				})
				if !supported {
					if !errors.Is(errN, storepkg.ErrIndexNotFound) || (!b.tiered && !errors.Is(errR, storepkg.ErrIndexNotFound)) {
						t.Fatalf("unsupported store: errs %v / %v, want ErrIndexNotFound", errN, errR)
					}
					return
				}
				if errN != nil || errR != nil {
					t.Fatal(errN, errR)
				}
				if want := orderedOracle(nodeRows, inRange(bounds.lo, bounds.hi, bounds.inclLo, bounds.inclHi), desc); renderIDValues(gotN) != renderIDValues(want) {
					t.Fatalf("nodes %+v desc=%v:\n got  %s\n want %s", bounds, desc, renderIDValues(gotN)[:min(400, len(renderIDValues(gotN)))], renderIDValues(want)[:min(400, len(renderIDValues(want)))])
				}
				if want := orderedOracle(relRows, inRange(bounds.lo, bounds.hi, bounds.inclLo, bounds.inclHi), desc); renderIDValues(gotR) != renderIDValues(want) {
					t.Fatalf("rels %+v desc=%v:\n got  %s\n want %s", bounds, desc, renderIDValues(gotR), renderIDValues(want))
				}
			}
			for _, prefix := range []string{"ap", "", "z"} {
				var gotN, gotR []idValue
				if err := g.Nodes().ForEachIDByLabelPropertyPrefix(ctx, "Ref", "v", prefix, desc, storepkg.QueryOpts{}, func(id types.NodeID, v any) bool {
					gotN = append(gotN, idValue{int64(id), v})
					return true
				}); err != nil {
					t.Fatal(err)
				}
				if err := g.Rels().ForEachIDByTypePropertyPrefix(ctx, "R", "v", prefix, desc, storepkg.QueryOpts{}, func(id types.RelID, v any) bool {
					gotR = append(gotR, idValue{int64(id), v})
					return true
				}); err != nil {
					t.Fatal(err)
				}
				if want := orderedOracle(nodeRows, hasPrefix(prefix), desc); renderIDValues(gotN) != renderIDValues(want) {
					t.Fatalf("node prefix %q desc=%v: %d entries, want %d", prefix, desc, len(gotN), len(want))
				}
				if want := orderedOracle(relRows, hasPrefix(prefix), desc); renderIDValues(gotR) != renderIDValues(want) {
					t.Fatalf("rel prefix %q desc=%v:\n got  %s\n want %s", prefix, desc, renderIDValues(gotR), renderIDValues(want))
				}
			}
		}

		calls := 0
		if err := g.Nodes().ForEachIDByLabelPropertyRangeOrdered(ctx, "Ref", "v", -100, 100, true, true, false, storepkg.QueryOpts{}, func(types.NodeID, any) bool {
			calls++
			return calls < 3
		}); err != nil || calls != 3 {
			t.Fatalf("early stop: %d calls, err %v", calls, err)
		}
		canceled, cancel := context.WithCancel(ctx)
		calls = 0
		err := g.Nodes().ForEachIDByLabelPropertyRangeOrdered(canceled, "Ref", "v", -100, 100, true, true, false, storepkg.QueryOpts{}, func(types.NodeID, any) bool {
			calls++
			cancel()
			return true
		})
		if !errors.Is(err, context.Canceled) || calls >= 4096 {
			t.Fatalf("canceled mid-walk: %d calls, err %v; want context.Canceled within 4,096", calls, err)
		}
		if err := g.Rels().ForEachIDByTypePropertyPrefix(canceled, "R", "v", "", false, storepkg.QueryOpts{}, func(types.RelID, any) bool { return true }); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled before: err %v", err)
		}
		never := func(types.NodeID, any) bool { t.Fatal("fn called"); return false }
		for name, err := range map[string]error{
			"ValidAt":  g.Nodes().ForEachIDByLabelPropertyRangeOrdered(ctx, "Ref", "v", 0, 1, true, true, false, storepkg.QueryOpts{ValidAt: 5}, never),
			"TxPin":    g.Nodes().ForEachIDByLabelPropertyPrefix(ctx, "Ref", "v", "a", false, storepkg.QueryOpts{TxPin: 5}, never),
			"no index": g.Nodes().ForEachIDByLabelPropertyPrefix(ctx, "Ref", "w", "a", false, storepkg.QueryOpts{}, never),
			"rel TxAt": g.Rels().ForEachIDByTypePropertyRangeOrdered(ctx, "R", "v", 0, 1, true, true, false, storepkg.QueryOpts{TxAt: 5, ValidAt: 5}, func(types.RelID, any) bool { return true }),
		} {
			if !errors.Is(err, storepkg.ErrIndexNotFound) {
				t.Errorf("%s: err %v, want ErrIndexNotFound", name, err)
			}
		}
		if err := g.Nodes().ForEachIDByLabelPropertyPrefix(ctx, "Unknown", "v", "a", false, storepkg.QueryOpts{}, never); err != nil {
			t.Errorf("unknown label: %v", err)
		}
		if err := g.Nodes().ForEachIDByLabelPropertyPrefix(ctx, "Ref", "v", "a", false, storepkg.QueryOpts{}, nil); err == nil {
			t.Error("nil fn: no error")
		}
		if _, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"v": int64(1<<53 + 1)}); err != nil {
			t.Fatal(err)
		}
		if err := g.Nodes().ForEachIDByLabelPropertyRangeOrdered(ctx, "Ref", "v", 0, 1, true, true, false, storepkg.QueryOpts{}, never); !errors.Is(err, storepkg.ErrIndexNotFound) {
			t.Errorf("integer past 2^53: err %v, want ErrIndexNotFound", err)
		}
	})
}

// TestForEachIDOrderedDeclinesOnDiskIndex: badger with PropertyIndexOnDisk
// keeps no RAM node index, so the node walks report ErrIndexNotFound.
func TestForEachIDOrderedDeclinesOnDiskIndex(t *testing.T) {
	g, err := graphpkg.New(graphpkg.Config{BadgerInMemory: true, PropertyIndexOnDisk: true})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if err := g.Index().CreateProperty("P", "v"); err != nil {
		t.Fatal(err)
	}
	never := func(types.NodeID, any) bool { t.Fatal("fn called"); return false }
	if err := g.Nodes().ForEachIDByLabelPropertyRangeOrdered(context.Background(), "P", "v", 0, 1, true, true, false, storepkg.QueryOpts{}, never); !errors.Is(err, storepkg.ErrIndexNotFound) {
		t.Fatalf("disk index: err %v, want ErrIndexNotFound", err)
	}
}
