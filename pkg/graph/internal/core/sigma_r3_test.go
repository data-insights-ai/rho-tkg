package core

import (
	"context"
	"errors"
	"math"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
)

func sigmaBackends(t *testing.T) map[string]func(t *testing.T) *Core {
	t.Helper()
	return map[string]func(t *testing.T) *Core{
		"memory": func(t *testing.T) *Core {
			g, err := New(Config{Store: memory.New()})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { g.Close() })
			return g
		},
		"badger": func(t *testing.T) *Core {
			g, err := New(Config{BadgerInMemory: true})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { g.Close() })
			return g
		},
	}
}

func assertClassCounts(t *testing.T, g *Core, label, key string, want storepkg.PropertyTypeClassCounts, msg string) {
	t.Helper()
	got, err := g.Stats.PropertyTypeClassCounts(label, key)
	if err != nil {
		t.Fatalf("%s: PropertyTypeClassCounts: %v", msg, err)
	}
	if got != want {
		t.Fatalf("%s: counts = %+v, want %+v", msg, got, want)
	}
}

// The exact probe from the consumer report: 4 nodes carrying label P — one
// int value, one string, one []int64, one WITHOUT the key. The legacy
// indexable-presence counter answers 2 and conflates the slice with the
// missing node (slices sort BEFORE numbers, nulls sort last — opposite ends);
// the type-class partition separates every case exactly.
func TestPropertyTypeClassCounts_SigmaProbe(t *testing.T) {
	for name, mk := range sigmaBackends(t) {
		t.Run(name, func(t *testing.T) {
			g := mk(t)
			ctx := context.Background()
			mustAddP := func(props map[string]any) {
				t.Helper()
				if _, err := g.Nodes.Add(ctx, []string{"P"}, props); err != nil {
					t.Fatalf("Add: %v", err)
				}
			}
			mustAddP(map[string]any{"v": int64(7)})
			mustAddP(map[string]any{"v": "seven"})
			mustAddP(map[string]any{"v": []int64{7}})
			mustAddP(map[string]any{"other": true})

			legacy, err := g.Stats.NodeCountByLabelAndPropertyKey("P", "v")
			if err != nil {
				t.Fatalf("legacy count: %v", err)
			}
			if legacy != 2 {
				t.Fatalf("legacy indexable count = %d, want 2 (the documented conflation)", legacy)
			}
			assertClassCounts(t, g, "P", "v",
				storepkg.PropertyTypeClassCounts{Numeric: 1, String: 1, Other: 1, Missing: 1},
				"sigma probe")

			// The two soundness gates the partition enables:
			// "nulls-only gap" holds for a key where every present value is numeric.
			mustAddP(map[string]any{"score": 1.5})
			mustAddP(map[string]any{"score": int64(2)})
			c, err := g.Stats.PropertyTypeClassCounts("P", "score")
			if err != nil {
				t.Fatalf("score counts: %v", err)
			}
			if c.Numeric != 2 || c.Present() != 2 || c.Missing != 4 {
				t.Fatalf("score counts = %+v, want Numeric 2, Missing 4 (nulls-only gap provable)", c)
			}
		})
	}
}

// Exactness across the mutation lifecycle: class-changing update, delete,
// label add/remove, NaN and ±Inf placement.
func TestPropertyTypeClassCounts_MutationLifecycle(t *testing.T) {
	for name, mk := range sigmaBackends(t) {
		t.Run(name, func(t *testing.T) {
			g := mk(t)
			ctx := context.Background()

			n1, err := g.Nodes.Add(ctx, []string{"L"}, map[string]any{"v": int64(1)})
			if err != nil {
				t.Fatalf("Add: %v", err)
			}
			n2, err := g.Nodes.Add(ctx, []string{"L"}, map[string]any{"v": math.NaN()})
			if err != nil {
				t.Fatalf("Add NaN: %v", err)
			}
			n3, err := g.Nodes.Add(ctx, []string{"M"}, map[string]any{"v": math.Inf(1)})
			if err != nil {
				t.Fatalf("Add Inf: %v", err)
			}
			assertClassCounts(t, g, "L", "v", storepkg.PropertyTypeClassCounts{Numeric: 1, NaN: 1}, "after adds")
			assertClassCounts(t, g, "M", "v", storepkg.PropertyTypeClassCounts{Numeric: 1}, "+Inf is Numeric")

			// Class-changing update: int -> string (with-history door).
			if _, err := g.Nodes.Update(ctx, n1.ID(), map[string]any{"v": "now-a-string"}); err != nil {
				t.Fatalf("Update: %v", err)
			}
			assertClassCounts(t, g, "L", "v", storepkg.PropertyTypeClassCounts{String: 1, NaN: 1}, "after class change")

			// Label add: n3 (M, +Inf) also becomes L.
			if err := g.Nodes.AddLabel(ctx, n3.ID(), "L"); err != nil {
				t.Fatalf("AddLabel: %v", err)
			}
			assertClassCounts(t, g, "L", "v", storepkg.PropertyTypeClassCounts{Numeric: 1, String: 1, NaN: 1}, "after label add")

			// Label remove undoes it.
			if err := g.Nodes.RemoveLabel(ctx, n3.ID(), "L"); err != nil {
				t.Fatalf("RemoveLabel: %v", err)
			}
			assertClassCounts(t, g, "L", "v", storepkg.PropertyTypeClassCounts{String: 1, NaN: 1}, "after label remove")

			// Delete removes n2's NaN contribution.
			if err := g.Nodes.Delete(ctx, n2.ID()); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			assertClassCounts(t, g, "L", "v", storepkg.PropertyTypeClassCounts{String: 1}, "after delete")

			// Unregistered label and absent key answer zero, not an error.
			assertClassCounts(t, g, "NoSuchLabel", "v", storepkg.PropertyTypeClassCounts{}, "unregistered label")
			assertClassCounts(t, g, "L", "nokey", storepkg.PropertyTypeClassCounts{Missing: 1}, "absent key = all missing")
		})
	}
}

// Badger rebuild-at-open: the counters are RAM-only, rebuilt by the same
// loadIndexes pass that rebuilds the presence counters — a reopened directory
// must answer identically.
func TestPropertyTypeClassCounts_BadgerReopenRebuild(t *testing.T) {
	dir := t.TempDir()
	g, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	if _, err := g.Nodes.Add(ctx, []string{"R"}, map[string]any{"v": int64(1)}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, err := g.Nodes.Add(ctx, []string{"R"}, map[string]any{"v": []any{"x"}}); err != nil {
		t.Fatalf("Add slice: %v", err)
	}
	if _, err := g.Nodes.Add(ctx, []string{"R"}, map[string]any{"w": false}); err != nil {
		t.Fatalf("Add bool: %v", err)
	}
	want := storepkg.PropertyTypeClassCounts{Numeric: 1, Other: 1, Missing: 1}
	assertClassCounts(t, g, "R", "v", want, "before close")
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	g2, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("reopen New: %v", err)
	}
	defer g2.Close()
	assertClassCounts(t, g2, "R", "v", want, "after reopen")
	assertClassCounts(t, g2, "R", "w", storepkg.PropertyTypeClassCounts{Bool: 1, Missing: 2}, "after reopen w")
}

// Composite-index introspection: ListComposites returns declared orders
// (distinct orderings are distinct definitions, both listed); HasComposite is
// order-insensitive set matching — exactly the query door's routing rule;
// definitions survive a badger reopen; drop removes them.
func TestCompositeIntrospection(t *testing.T) {
	for name, mk := range sigmaBackends(t) {
		t.Run(name, func(t *testing.T) {
			g := mk(t)
			if err := g.Index.CreateComposite("A", []string{"x", "y"}); err != nil {
				t.Fatalf("CreateComposite xy: %v", err)
			}
			if err := g.Index.CreateComposite("A", []string{"y", "x"}); err != nil {
				t.Fatalf("CreateComposite yx (distinct order): %v", err)
			}
			if err := g.Index.CreateComposite("A", []string{"x", "y", "z"}); err != nil {
				t.Fatalf("CreateComposite xyz: %v", err)
			}

			defs, err := g.Index.ListComposites("A")
			if err != nil {
				t.Fatalf("ListComposites: %v", err)
			}
			if len(defs) != 3 {
				t.Fatalf("ListComposites returned %d definitions, want 3: %v", len(defs), defs)
			}
			seen := map[string]bool{}
			for _, d := range defs {
				k := ""
				for _, s := range d {
					k += s + ","
				}
				seen[k] = true
			}
			for _, want := range []string{"x,y,", "y,x,", "x,y,z,"} {
				if !seen[want] {
					t.Fatalf("declared order %q missing from %v", want, defs)
				}
			}

			cases := []struct {
				keys []string
				want bool
			}{
				{[]string{"x", "y"}, true},
				{[]string{"y", "x"}, true},            // order-insensitive
				{[]string{"z", "x", "y"}, true},       // any order of the 3-key set
				{[]string{"x"}, false},                // no 1-key composite
				{[]string{"x", "y", "z", "w"}, false}, // superset
				{[]string{"x", "q"}, false},
			}
			for _, tc := range cases {
				got, err := g.Index.HasComposite("A", tc.keys)
				if err != nil {
					t.Fatalf("HasComposite(%v): %v", tc.keys, err)
				}
				if got != tc.want {
					t.Fatalf("HasComposite(%v) = %v, want %v", tc.keys, got, tc.want)
				}
			}

			// Unregistered label: empty list, false, no error.
			empty, err := g.Index.ListComposites("NoSuchLabel")
			if err != nil || len(empty) != 0 {
				t.Fatalf("unregistered label: %v, %v", empty, err)
			}
			has, err := g.Index.HasComposite("NoSuchLabel", []string{"x", "y"})
			if err != nil || has {
				t.Fatalf("unregistered label HasComposite = %v, %v", has, err)
			}

			// Empty keys probe is an input error.
			if _, err := g.Index.HasComposite("A", nil); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
				t.Fatalf("empty keys: err = %v, want ErrInvalidStoreMutation", err)
			}

			// Drop removes exactly the dropped ordering.
			if err := g.Index.DeleteComposite("A", []string{"x", "y"}); err != nil {
				t.Fatalf("DeleteComposite: %v", err)
			}
			has, err = g.Index.HasComposite("A", []string{"x", "y"})
			if err != nil {
				t.Fatalf("HasComposite after drop: %v", err)
			}
			if !has {
				t.Fatal("set still matched by the surviving y,x definition — expected true")
			}
			defs, err = g.Index.ListComposites("A")
			if err != nil || len(defs) != 2 {
				t.Fatalf("after drop: %d definitions (%v), err=%v, want 2", len(defs), defs, err)
			}
		})
	}
}

// Composite definitions are persisted; a reopened badger dir must answer the
// introspection doors identically (the query-door acceleration also depends
// on this reload).
func TestCompositeIntrospection_BadgerReopen(t *testing.T) {
	dir := t.TempDir()
	g, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := g.Index.CreateComposite("A", []string{"x", "y"}); err != nil {
		t.Fatalf("CreateComposite: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	g2, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer g2.Close()
	has, err := g2.Index.HasComposite("A", []string{"y", "x"})
	if err != nil {
		t.Fatalf("HasComposite after reopen: %v", err)
	}
	if !has {
		t.Fatal("composite definition lost across reopen")
	}
}

// The introspection capability is optional: tiered declines composite indexes
// entirely, and the type-class counters FOLD across shards there instead.
func TestSigmaR3_TieredBehavior(t *testing.T) {
	g := newTieredTestCore(t)
	if _, err := g.Index.ListComposites("A"); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatalf("tiered ListComposites err = %v, want ErrCapabilityNotSupported", err)
	}
	ctx := context.Background()
	if _, err := g.Nodes.Add(ctx, []string{"Ref"}, map[string]any{"v": int64(1)}); err != nil {
		t.Fatalf("Add ref: %v", err)
	}
	if _, err := g.Nodes.Add(ctx, []string{"Ev"}, map[string]any{"v": "s"}); err != nil {
		t.Fatalf("Add event: %v", err)
	}
	if _, err := g.Nodes.Add(ctx, []string{"Ev"}, map[string]any{"w": int64(2)}); err != nil {
		t.Fatalf("Add event 2: %v", err)
	}
	assertClassCounts(t, g, "Ref", "v", storepkg.PropertyTypeClassCounts{Numeric: 1}, "tiered ref shard")
	assertClassCounts(t, g, "Ev", "v", storepkg.PropertyTypeClassCounts{String: 1, Missing: 1}, "tiered event fold")
}
