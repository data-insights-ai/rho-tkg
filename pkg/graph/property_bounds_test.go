package graph_test

import (
	"context"
	"errors"
	"math"
	"strconv"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// boundsOracle computes PropertyBounds by brute force over the current values
// of one label or type: Min/Max over the orderable numbers (a float32 by its
// shortest decimal rendering, NaN left out), every stored value at Min and
// Max, and the class partition with Missing.
type boundsOracle struct {
	want         storepkg.PropertyBounds
	atMin, atMax []any
}

func numericKey(v any) (float64, bool) {
	switch x := v.(type) {
	case int:
		return float64(x), true
	case int8:
		return float64(x), true
	case int16:
		return float64(x), true
	case int32:
		return float64(x), true
	case int64:
		return float64(x), true
	case uint:
		return float64(x), true
	case uint8:
		return float64(x), true
	case uint16:
		return float64(x), true
	case uint32:
		return float64(x), true
	case uint64:
		return float64(x), true
	case float32:
		f, _ := strconv.ParseFloat(strconv.FormatFloat(float64(x), 'g', -1, 32), 64)
		return f, !math.IsNaN(float64(x))
	case float64:
		return x, !math.IsNaN(x)
	}
	return 0, false
}

func oracleBounds(values []any, present []bool) boundsOracle {
	var o boundsOracle
	c := &o.want.Classes
	first := true
	for i, v := range values {
		if !present[i] {
			c.Missing++
			continue
		}
		k, num := numericKey(v)
		switch {
		case num:
			c.Numeric++
		case isNaN(v):
			c.NaN++
		default:
			switch v.(type) {
			case string:
				c.String++
			case bool:
				c.Bool++
			default:
				c.Other++
			}
		}
		if !num {
			continue
		}
		if first || k < o.want.Min {
			o.want.Min, o.atMin = k, nil
		}
		if first || k > o.want.Max {
			o.want.Max, o.atMax = k, nil
		}
		first = false
		if k == o.want.Min {
			o.atMin = append(o.atMin, v)
		}
		if k == o.want.Max {
			o.atMax = append(o.atMax, v)
		}
	}
	return o
}

func isNaN(v any) bool {
	switch x := v.(type) {
	case float32:
		return math.IsNaN(float64(x))
	case float64:
		return math.IsNaN(x)
	}
	return false
}

// typeRank is the stored-type preference of a value at Min or Max.
func typeRank(v any) int {
	switch v.(type) {
	case float64:
		return 0
	case float32:
		return 1
	case int64:
		return 2
	case int:
		return 3
	case int32:
		return 4
	case int16:
		return 5
	case int8:
		return 6
	case uint64:
		return 7
	case uint:
		return 8
	case uint32:
		return 9
	case uint16:
		return 10
	}
	return 11
}

// check compares a door's answer with the oracle: exact classes, Min/Max,
// and a stored value at each of the preferred type.
func (o boundsOracle) check(t *testing.T, name string, got storepkg.PropertyBounds) {
	t.Helper()
	if got.Classes != o.want.Classes {
		t.Fatalf("%s classes = %+v, want %+v", name, got.Classes, o.want.Classes)
	}
	if !o.want.HasNumeric() {
		if got.Min != 0 || got.Max != 0 || got.MinValue != nil || got.MaxValue != nil {
			t.Fatalf("%s no numeric value but bounds %+v", name, got)
		}
		return
	}
	if got.Min != o.want.Min || got.Max != o.want.Max {
		t.Fatalf("%s bounds = [%v, %v], want [%v, %v]", name, got.Min, got.Max, o.want.Min, o.want.Max)
	}
	pick := func(at []any) any {
		best := at[0]
		for _, v := range at[1:] {
			if typeRank(v) < typeRank(best) {
				best = v
			}
		}
		return best
	}
	if wantV := pick(o.atMin); got.MinValue != wantV {
		t.Fatalf("%s MinValue = %T %v, want %T %v", name, got.MinValue, got.MinValue, wantV, wantV)
	}
	if wantV := pick(o.atMax); got.MaxValue != wantV {
		t.Fatalf("%s MaxValue = %T %v, want %T %v", name, got.MaxValue, got.MaxValue, wantV, wantV)
	}
}

func nodeBoundsOracle(t *testing.T, g *graphpkg.Graph, label, key string) boundsOracle {
	t.Helper()
	var values []any
	var present []bool
	if err := g.Nodes().ForEachByLabel(label, storepkg.QueryOpts{}, func(n *types.Node) bool {
		v, ok := n.GetProperty(key)
		values, present = append(values, v), append(present, ok)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	return oracleBounds(values, present)
}

func relBoundsOracle(t *testing.T, g *graphpkg.Graph, typeName, key string) boundsOracle {
	t.Helper()
	var values []any
	var present []bool
	if err := g.Rels().ForEachByType(typeName, storepkg.QueryOpts{}, func(r *types.Relationship) bool {
		v, ok := r.GetProperty(key)
		values, present = append(values, v), append(present, ok)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	return oracleBounds(values, present)
}

// TestPropertyBounds (sigma-tkgd round 8 request 1): on every backend
// Stats().PropertyBounds / RelPropertyBounds equal a brute-force pass over the
// current rows (bounds, the stored value at each, the class partition with
// Missing) through creates, updates that move the minimum and maximum away,
// deletes of the extreme rows, label changes, and an index built over
// existing rows (tiered and sharded fold their shards' bounds since sigma round 10;
// tiered's nodes here span the reference and an event shard).
func TestPropertyBounds(t *testing.T) {
	t.Parallel()
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		native := true // every in-tree backend states bounds (sharded and tiered fold their parts, sigma round 10)
		if err := g.Index().CreateProperty("Ref", "v"); err != nil {
			t.Fatal(err)
		}
		if err := g.Index().CreateRelProperty("R", "v"); err != nil && !b.tiered {
			t.Fatal(err)
		}
		values := []any{int64(3), 3.0, float32(0.1), uint8(7), -2.5, "s", true, math.NaN(), []any{int64(1)},
			int32(-2), int64(9), 9.0, int16(9), math.Inf(1), float32(-7.25), nil}
		var nodes []*types.Node
		for i := range 40 {
			props := map[string]any{"v": values[i%len(values)]}
			if values[i%len(values)] == nil {
				props = map[string]any{"w": int64(i)}
			}
			labels := []string{"Ref"}
			if i%7 == 0 {
				labels = []string{"Other"}
			}
			n, err := g.Nodes().Add(ctx, labels, props)
			if err != nil {
				t.Fatal(err)
			}
			nodes = append(nodes, n)
		}
		var rels []*types.Relationship
		for i := range 30 {
			props := map[string]any{"v": values[(i*3)%len(values)]}
			if values[(i*3)%len(values)] == nil {
				props = nil
			}
			r, err := g.Rels().Add(ctx, "R", nodes[i], nodes[i+1], props)
			if err != nil {
				t.Fatal(err)
			}
			rels = append(rels, r)
		}

		verify := func(phase string) {
			t.Helper()
			nb, ok, err := g.Stats().PropertyBounds("Ref", "v", storepkg.QueryOpts{})
			if err != nil {
				t.Fatalf("%s: %v", phase, err)
			}
			rb, rok, err := g.Stats().RelPropertyBounds("R", "v", storepkg.QueryOpts{})
			if err != nil {
				t.Fatalf("%s rel: %v", phase, err)
			}
			if !native {
				if ok || rok {
					t.Fatalf("%s %s: ok %v / %v, want declined", phase, b.name, ok, rok)
				}
				return
			}
			if !ok || !rok {
				t.Fatalf("%s: ok %v / %v, want answered", phase, ok, rok)
			}
			nodeBoundsOracle(t, g, "Ref", "v").check(t, phase+" nodes", nb)
			relBoundsOracle(t, g, "R", "v").check(t, phase+" rels", rb)
		}
		verify("created")

		// Move the extremes away: +Inf and the 9s down, the minimum up.
		for _, n := range nodes {
			v, _ := n.GetProperty("v")
			if k, num := numericKey(v); num && (k == math.Inf(1) || k == 9) {
				if _, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"v": int64(5)}); err != nil {
					t.Fatal(err)
				}
			}
		}
		verify("max moved")
		for _, r := range rels {
			v, _ := r.GetProperty("v")
			if k, num := numericKey(v); num && (k < 0 || k > 8) {
				if err := g.Rels().Delete(ctx, r.ID()); err != nil {
					t.Fatal(err)
				}
			}
		}
		for _, n := range nodes {
			v, _ := n.GetProperty("v")
			if k, num := numericKey(v); num && k < 0 {
				if err := g.Nodes().Delete(ctx, n.ID()); err != nil {
					t.Fatal(err)
				}
			}
		}
		verify("extremes deleted")
		// A node entering the label with a new maximum, one leaving it with
		// the minimum, a string turned into a number.
		if _, err := g.Nodes().Update(ctx, nodes[0].ID(), map[string]any{"v": int64(1000)}); err != nil {
			t.Fatal(err)
		}
		if !b.tiered { // tiered refuses label changes across its shard classes
			if err := g.Nodes().AddLabel(ctx, nodes[0].ID(), "Ref"); err != nil {
				t.Fatal(err)
			}
			verify("label added")
			for i, n := range nodes {
				v, _ := n.GetProperty("v")
				if k, num := numericKey(v); num && k == 0.1 && i%7 != 0 {
					if err := g.Nodes().AddLabel(ctx, n.ID(), "Extra"); err != nil {
						t.Fatal(err)
					}
					if err := g.Nodes().RemoveLabel(ctx, n.ID(), "Ref"); err != nil {
						t.Fatal(err)
					}
				}
				if s, isStr := v.(string); isStr && s == "s" {
					if _, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"v": float32(-1.5)}); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
		verify("label removed")

		// An index built over existing rows.
		if err := g.Index().CreateProperty("Ref", "w"); err != nil {
			t.Fatal(err)
		}
		wb, ok, err := g.Stats().PropertyBounds("Ref", "w", storepkg.QueryOpts{})
		if err != nil {
			t.Fatal(err)
		}
		if native {
			if !ok {
				t.Fatal("backfilled index declined")
			}
			nodeBoundsOracle(t, g, "Ref", "w").check(t, "backfilled", wb)
			if !wb.NumericOnly() || wb.NullFree() {
				t.Fatalf("w: NumericOnly %v NullFree %v, want true, false", wb.NumericOnly(), wb.NullFree())
			}
		}

		// Declines: no index, unknown label or type, temporal opts.
		declined := func(name string, got storepkg.PropertyBounds, ok bool, err error) {
			t.Helper()
			if err != nil || ok || got != (storepkg.PropertyBounds{}) {
				t.Errorf("%s: %+v ok=%v err=%v, want declined", name, got, ok, err)
			}
		}
		got, ok, err := g.Stats().PropertyBounds("Ref", "nope", storepkg.QueryOpts{})
		declined("no index", got, ok, err)
		got, ok, err = g.Stats().RelPropertyBounds("R", "nope", storepkg.QueryOpts{})
		declined("no rel index", got, ok, err)
		got, ok, err = g.Stats().PropertyBounds("Nope", "v", storepkg.QueryOpts{})
		declined("unknown label", got, ok, err)
		got, ok, err = g.Stats().RelPropertyBounds("NOPE", "v", storepkg.QueryOpts{})
		declined("unknown type", got, ok, err)
		got, ok, err = g.Stats().PropertyBounds("Ref", "v", storepkg.QueryOpts{ValidAt: types.Instant(1)})
		declined("ValidAt", got, ok, err)
		got, ok, err = g.Stats().RelPropertyBounds("R", "v", storepkg.QueryOpts{TxPin: types.Instant(1)})
		declined("TxPin", got, ok, err)

		// Malformed targets are errors.
		if _, _, err := g.Stats().PropertyBounds("", "v", storepkg.QueryOpts{}); err == nil {
			t.Error("empty label: no error")
		}
		if _, _, err := g.Stats().PropertyBounds("Ref", "tkg_valid_from", storepkg.QueryOpts{}); !errors.Is(err, types.ErrReservedPrefix) {
			t.Errorf("reserved key: %v", err)
		}
		if _, _, err := g.Stats().RelPropertyBounds("R", "tkg_valid_to", storepkg.QueryOpts{}); !errors.Is(err, types.ErrReservedPrefix) {
			t.Errorf("reserved rel key: %v", err)
		}

		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := g.Stats().PropertyBounds("Ref", "v", storepkg.QueryOpts{}); !errors.Is(err, graphpkg.ErrGraphClosed) {
			t.Errorf("closed graph: %v", err)
		}
		if _, _, err := g.Stats().RelPropertyBounds("R", "v", storepkg.QueryOpts{}); !errors.Is(err, graphpkg.ErrGraphClosed) {
			t.Errorf("closed graph rel: %v", err)
		}
	})
}

// TestPropertyBoundsFlags: NumericOnly and NullFree on exact class counts —
// NaN counts as a number, a string or a list does not, a node or relationship
// without the key breaks NullFree; an empty answer is HasNumeric false.
func TestPropertyBoundsFlags(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"memory", "badger"} {
		t.Run(name, func(t *testing.T) {
			g := openBoundsGraph(t, name == "badger", nil)
			ctx := context.Background()
			for _, key := range []string{"a", "b", "c"} {
				if err := g.Index().CreateProperty("L", key); err != nil {
					t.Fatal(err)
				}
			}
			for _, props := range []map[string]any{
				{"a": int64(1), "b": math.NaN(), "c": "x"},
				{"a": 2.5, "b": int64(4), "c": int64(1)},
				{"a": int64(-1), "c": []any{"l"}},
			} {
				if _, err := g.Nodes().Add(ctx, []string{"L"}, props); err != nil {
					t.Fatal(err)
				}
			}
			want := map[string][3]bool{ // NumericOnly, NullFree, HasNumeric
				"a": {true, true, true},
				"b": {true, false, true},
				"c": {false, true, true},
			}
			for key, w := range want {
				b, ok, err := g.Stats().PropertyBounds("L", key, storepkg.QueryOpts{})
				if err != nil || !ok {
					t.Fatalf("%s: ok=%v err=%v", key, ok, err)
				}
				if got := [3]bool{b.NumericOnly(), b.NullFree(), b.HasNumeric()}; got != w {
					t.Errorf("%s: NumericOnly, NullFree, HasNumeric = %v, want %v (%+v)", key, got, w, b)
				}
			}
			if b, _, _ := g.Stats().PropertyBounds("L", "b", storepkg.QueryOpts{}); b.Min != 4 || b.Max != 4 || b.Classes.NaN != 1 {
				t.Errorf("b: NaN not left out of the bounds: %+v", b)
			}
			// Every value of c removed: no number left.
			if err := g.Index().CreateProperty("L", "d"); err != nil {
				t.Fatal(err)
			}
			b, ok, err := g.Stats().PropertyBounds("L", "d", storepkg.QueryOpts{})
			if err != nil || !ok || b.HasNumeric() || !b.NumericOnly() || b.NullFree() || b.Classes.Missing != 3 {
				t.Errorf("d: %+v ok=%v err=%v, want no number, 3 missing", b, ok, err)
			}
		})
	}
}

// openBoundsGraph opens a memory or badger graph closed at cleanup.
func openBoundsGraph(t *testing.T, badger bool, mut func(*graphpkg.Config)) *graphpkg.Graph {
	t.Helper()
	cfg := graphpkg.Config{Validation: graphpkg.ValidationLimits{AllowSelfLoops: true}}
	if badger {
		cfg.BadgerInMemory = true
	}
	if mut != nil {
		mut(&cfg)
	}
	g, err := graphpkg.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

// TestPropertyBoundsDeclines: the cases the bounds cannot be stated exactly
// are ok=false with no error: an Elements index, an index holding an integer
// past 2^53 (until it leaves), planner statistics disabled, badger
// PropertyIndexOnDisk for nodes (its relationship index stays in RAM).
func TestPropertyBoundsDeclines(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, name := range []string{"memory", "badger"} {
		t.Run(name, func(t *testing.T) {
			isBadger := name == "badger"
			g := openBoundsGraph(t, isBadger, nil)
			if err := g.Index().CreatePropertyWithOptions("L", "e", storepkg.PropertyIndexOptions{Elements: true}); err != nil {
				t.Fatal(err)
			}
			if err := g.Index().CreateProperty("L", "big"); err != nil {
				t.Fatal(err)
			}
			if err := g.Index().CreateRelProperty("R", "big"); err != nil {
				t.Fatal(err)
			}
			a, err := g.Nodes().Add(ctx, []string{"L"}, map[string]any{"e": int64(1), "big": int64(1<<53 + 1)})
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels().Add(ctx, "R", a, a, map[string]any{"big": uint64(1<<60 + 3)})
			if err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"e", "big"} {
				if b, ok, err := g.Stats().PropertyBounds("L", key, storepkg.QueryOpts{}); err != nil || ok {
					t.Errorf("%s: %+v ok=%v err=%v, want declined", key, b, ok, err)
				}
			}
			if b, ok, err := g.Stats().RelPropertyBounds("R", "big", storepkg.QueryOpts{}); err != nil || ok {
				t.Errorf("rel big: %+v ok=%v err=%v, want declined", b, ok, err)
			}
			if _, err := g.Nodes().Update(ctx, a.ID(), map[string]any{"big": int64(7)}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Rels().Update(ctx, r.ID(), map[string]any{"big": int64(8)}); err != nil {
				t.Fatal(err)
			}
			if b, ok, err := g.Stats().PropertyBounds("L", "big", storepkg.QueryOpts{}); err != nil || !ok || b.Min != 7 || b.Max != 7 {
				t.Errorf("big after update: %+v ok=%v err=%v, want [7, 7]", b, ok, err)
			}
			if b, ok, err := g.Stats().RelPropertyBounds("R", "big", storepkg.QueryOpts{}); err != nil || !ok || b.Min != 8 || b.MaxValue != int64(8) {
				t.Errorf("rel big after update: %+v ok=%v err=%v, want [8, 8]", b, ok, err)
			}

			off := openBoundsGraph(t, isBadger, func(c *graphpkg.Config) { c.DisablePlannerStats = true })
			if err := off.Index().CreateProperty("L", "v"); err != nil {
				t.Fatal(err)
			}
			if err := off.Index().CreateRelProperty("R", "v"); err != nil {
				t.Fatal(err)
			}
			n, err := off.Nodes().Add(ctx, []string{"L"}, map[string]any{"v": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := off.Rels().Add(ctx, "R", n, n, map[string]any{"v": int64(1)}); err != nil {
				t.Fatal(err)
			}
			if b, ok, err := off.Stats().PropertyBounds("L", "v", storepkg.QueryOpts{}); err != nil || ok {
				t.Errorf("stats disabled: %+v ok=%v err=%v, want declined", b, ok, err)
			}
			if b, ok, err := off.Stats().RelPropertyBounds("R", "v", storepkg.QueryOpts{}); err != nil || ok {
				t.Errorf("stats disabled rel: %+v ok=%v err=%v, want declined", b, ok, err)
			}
		})
	}

	disk := openBoundsGraph(t, true, func(c *graphpkg.Config) { c.PropertyIndexOnDisk = true })
	if err := disk.Index().CreateProperty("L", "v"); err != nil {
		t.Fatal(err)
	}
	if err := disk.Index().CreateRelProperty("R", "v"); err != nil {
		t.Fatal(err)
	}
	n, err := disk.Nodes().Add(ctx, []string{"L"}, map[string]any{"v": int64(2)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := disk.Rels().Add(ctx, "R", n, n, map[string]any{"v": int64(3)}); err != nil {
		t.Fatal(err)
	}
	if b, ok, err := disk.Stats().PropertyBounds("L", "v", storepkg.QueryOpts{}); err != nil || ok {
		t.Errorf("disk index: %+v ok=%v err=%v, want declined", b, ok, err)
	}
	if b, ok, err := disk.Stats().RelPropertyBounds("R", "v", storepkg.QueryOpts{}); err != nil || !ok || b.Min != 3 || !b.NullFree() {
		t.Errorf("disk mode rel index: %+v ok=%v err=%v, want [3, 3]", b, ok, err)
	}
}

// TestPropertyBoundsAfterReopen: badger rebuilds the index and (deferred)
// planner statistics at open; the bounds after a reopen equal those before.
func TestPropertyBoundsAfterReopen(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	open := func() *graphpkg.Graph {
		g, err := graphpkg.New(graphpkg.Config{BadgerDir: dir})
		if err != nil {
			t.Fatal(err)
		}
		return g
	}
	g := open()
	if err := g.Index().CreateProperty("L", "v"); err != nil {
		t.Fatal(err)
	}
	if err := g.Index().CreateRelProperty("R", "v"); err != nil {
		t.Fatal(err)
	}
	var prev *types.Node
	for i, v := range []any{int64(4), -3.5, "x", float32(2.5), int64(11)} {
		n, err := g.Nodes().Add(ctx, []string{"L"}, map[string]any{"v": v})
		if err != nil {
			t.Fatal(err)
		}
		if prev != nil {
			if _, err := g.Rels().Add(ctx, "R", prev, n, map[string]any{"v": int64(i * 10)}); err != nil {
				t.Fatal(err)
			}
		}
		prev = n
	}
	if _, err := g.Nodes().Add(ctx, []string{"L"}, nil); err != nil {
		t.Fatal(err)
	}
	before, ok, err := g.Stats().PropertyBounds("L", "v", storepkg.QueryOpts{})
	if err != nil || !ok {
		t.Fatalf("before: ok=%v err=%v", ok, err)
	}
	relBefore, ok, err := g.Stats().RelPropertyBounds("R", "v", storepkg.QueryOpts{})
	if err != nil || !ok {
		t.Fatalf("rel before: ok=%v err=%v", ok, err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g = open()
	defer g.Close()
	after, ok, err := g.Stats().PropertyBounds("L", "v", storepkg.QueryOpts{})
	if err != nil || !ok || after != before {
		t.Fatalf("after reopen: %+v ok=%v err=%v, want %+v", after, ok, err, before)
	}
	relAfter, ok, err := g.Stats().RelPropertyBounds("R", "v", storepkg.QueryOpts{})
	if err != nil || !ok || relAfter != relBefore {
		t.Fatalf("rel after reopen: %+v ok=%v err=%v, want %+v", relAfter, ok, err, relBefore)
	}
	nodeBoundsOracle(t, g, "L", "v").check(t, "reopened", after)
}

// TestPropertyBoundsUnderWriters: the index, the class counts and the label
// count are read at one moment. A writer flips nodes between a number and a
// string and moves them in and out of the label; every concurrent read
// answers, and its bounds agree with its class counts (one node: a read that
// took the index and the counts at different moments sees a number in one and
// none in the other, and declines).
func TestPropertyBoundsUnderWriters(t *testing.T) {
	for _, name := range []string{"memory", "badger"} {
		t.Run(name, func(t *testing.T) {
			g := openBoundsGraph(t, name == "badger", nil)
			ctx := context.Background()
			if err := g.Index().CreateProperty("L", "v"); err != nil {
				t.Fatal(err)
			}
			var ids []types.NodeID
			for i := range 1 {
				n, err := g.Nodes().Add(ctx, []string{"L", "K"}, map[string]any{"v": int64(i + 1)})
				if err != nil {
					t.Fatal(err)
				}
				ids = append(ids, n.ID())
			}
			done := make(chan struct{})
			errs := make(chan error, 1)
			go func() {
				defer close(done)
				for i := range 3000 {
					id := ids[i%len(ids)]
					var err error
					switch i % 3 {
					case 0:
						_, err = g.Nodes().Update(ctx, id, map[string]any{"v": "s"})
					case 1:
						_, err = g.Nodes().Update(ctx, id, map[string]any{"v": int64(10 + i)})
					default:
						if err = g.Nodes().RemoveLabel(ctx, id, "L"); err == nil {
							err = g.Nodes().AddLabel(ctx, id, "L")
						}
					}
					if err != nil {
						errs <- err
						return
					}
				}
			}()
			reads := 0
			for running := true; running || reads == 0; reads++ {
				select {
				case <-done:
					running = false
				default:
				}
				b, ok, err := g.Stats().PropertyBounds("L", "v", storepkg.QueryOpts{})
				if err != nil || !ok {
					t.Fatalf("read %d: ok=%v err=%v", reads, ok, err)
				}
				c := b.Classes
				if c.Present()+c.Missing > int64(len(ids)) || b.HasNumeric() != (b.MinValue != nil) || (b.HasNumeric() && b.Min > b.Max) {
					t.Fatalf("read %d: torn answer %+v", reads, b)
				}
			}
			select {
			case err := <-errs:
				t.Fatal(err)
			default:
			}
		})
	}
}
