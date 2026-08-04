package index

import (
	"math"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The typed column layout (ints/flts/isFloat + lazily-boxed views) replaced an
// eagerly-boxed []any. These probes pin the properties that layout must hold and
// that a shape-only test would not catch: the three storage states must be
// EXACTLY the cheapest one that can represent the data, both doors must agree
// VALUE-for-value, and the typed door must not allocate.

// buildOne is a one-property column over ids with the given values.
func buildOne(t *testing.T, ids []types.NodeID, vals map[types.NodeID]any) *LabelDocValues {
	t.Helper()
	props := make(map[types.NodeID]map[string]any, len(vals))
	for id, v := range vals {
		props[id] = map[string]any{"v": v}
	}
	return BuildLabelDocValues(1, ids, []string{"v"}, getter(props), nil)
}

// TestTypedColumn_StorageStateIsTheCheapestExactOne asserts the STRUCTURE, not an
// outcome: a uniform column must not carry the array it does not need. Reading a
// value cannot detect the waste — a mixed-representation column returns identical
// values while costing double the memory the typed layout exists to save
// (lessons.md 46: assert the structural property).
func TestTypedColumn_StorageStateIsTheCheapestExactOne(t *testing.T) {
	ids := []types.NodeID{1, 2, 3}

	t.Run("uniform_int64_has_no_float_half_and_no_selector", func(t *testing.T) {
		l := buildOne(t, ids, map[types.NodeID]any{1: int64(1), 2: int64(2), 3: int64(3)})
		v, ok := l.View("v")
		if !ok {
			t.Fatal("View declined a buildable numeric column")
		}
		if v.Ints == nil {
			t.Error("uniform int64 column has no int half")
		}
		if v.Flts != nil {
			t.Errorf("uniform int64 column allocated a float half of len %d", len(v.Flts))
		}
		if v.isFloat != nil {
			t.Error("uniform int64 column allocated a selector bitset it can never need")
		}
	})

	t.Run("uniform_float64_has_no_int_half_and_no_selector", func(t *testing.T) {
		l := buildOne(t, ids, map[types.NodeID]any{1: 1.5, 2: 2.5, 3: 3.5})
		v, _ := l.View("v")
		if v.Flts == nil {
			t.Error("uniform float column has no float half")
		}
		if v.Ints != nil {
			t.Errorf("uniform float column allocated an int half of len %d", len(v.Ints))
		}
		if v.isFloat != nil {
			t.Error("uniform float column allocated a selector bitset it can never need")
		}
		for ord := range ids {
			if !v.IsFloat(ord) {
				t.Errorf("ord %d: uniform float column reports IsFloat=false", ord)
			}
		}
	})

	t.Run("mixed_carries_both_halves_and_a_selector", func(t *testing.T) {
		l := buildOne(t, ids, map[types.NodeID]any{1: int64(1), 2: 2.5, 3: int64(3)})
		v, ok := l.View("v")
		if !ok {
			t.Fatal("a mixed int/float column must still BUILD — refusing it would " +
				"regress every consumer that relies on it today")
		}
		if v.Ints == nil || v.Flts == nil || v.isFloat == nil {
			t.Fatalf("mixed column missing a half: ints=%v flts=%v sel=%v",
				v.Ints != nil, v.Flts != nil, v.isFloat != nil)
		}
		// ids sort ascending, so ord 1 is node 2 — the only float.
		if v.IsFloat(0) || !v.IsFloat(1) || v.IsFloat(2) {
			t.Errorf("selector wrong: got [%v %v %v], want [false true false]",
				v.IsFloat(0), v.IsFloat(1), v.IsFloat(2))
		}
	})
}

// TestTypedColumn_BothDoorsAgreeValueForValue is the equivalence oracle between the
// typed door (View) and the published boxed door (ForEachRow). A shape match is not
// a payload match — the two doors read DIFFERENT storage (typed arrays vs the lazily
// materialised []any), so only a value-level comparison can catch a selector or
// dictionary-index bug that returns a plausible wrong number.
func TestTypedColumn_BothDoorsAgreeValueForValue(t *testing.T) {
	const big = int64(9_000_000_000_000_001) // > 2^53
	cases := map[string]map[types.NodeID]any{
		"uniform_int":     {1: int64(-5), 2: int64(0), 3: big},
		"uniform_float":   {1: -1.5, 2: 0.0, 3: math.MaxFloat64},
		"mixed":           {1: int64(7), 2: 2.25, 3: big},
		"with_absent":     {1: int64(7), 3: int64(9)}, // node 2 has no value
		"uint64_overflow": {1: uint64(math.MaxUint64), 2: int64(1), 3: uint64(3)},
		"strings":         {1: "b", 2: "a", 3: "b"}, // repeated term exercises the dictionary
	}
	ids := []types.NodeID{1, 2, 3}

	for name, vals := range cases {
		t.Run(name, func(t *testing.T) {
			l := buildOne(t, ids, vals)
			view, ok := l.View("v")
			if !ok {
				t.Fatalf("View declined a column ForEachRow accepts")
			}
			_, boxed, present := collectRows(l, []string{"v"})

			for ord := range l.IDs() {
				gotPresent := view.Present(ord)
				if gotPresent != present[ord][0] {
					t.Fatalf("ord %d: presence disagrees: typed=%v boxed=%v",
						ord, gotPresent, present[ord][0])
				}
				if !gotPresent {
					continue
				}
				var typed any
				if view.Type == ColString {
					typed = view.StringAt(ord)
				} else if view.IsFloat(ord) {
					typed = view.Flts[ord]
				} else {
					typed = view.Ints[ord]
				}
				if typed != boxed[ord][0] {
					t.Errorf("ord %d: typed door returned %v (%T), boxed door %v (%T)",
						ord, typed, typed, boxed[ord][0], boxed[ord][0])
				}
			}
		})
	}
}

// TestTypedColumn_BoxedViewIsBuiltOnceAndIsStable pins the lazy cache: repeated
// boxed reads must return the SAME backing slice, not rebuild per call. A per-call
// rebuild would be the regression the lazy design exists to avoid, and it is
// invisible to a value assertion.
func TestTypedColumn_BoxedViewIsBuiltOnceAndIsStable(t *testing.T) {
	ids := []types.NodeID{1, 2, 3}
	l := buildOne(t, ids, map[types.NodeID]any{1: int64(1), 2: int64(2), 3: int64(3)})
	c := l.cols["v"]

	first := c.boxedNumeric()
	second := c.boxedNumeric()
	if &first[0] != &second[0] {
		t.Error("boxedNumeric rebuilt the view instead of returning the cached one")
	}

	ls := buildOne(t, ids, map[types.NodeID]any{1: "a", 2: "a", 3: "b"})
	cs := ls.cols["v"]
	if d1, d2 := cs.boxedDict(), cs.boxedDict(); &d1[0] != &d2[0] {
		t.Error("boxedDict rebuilt the dictionary view instead of returning the cached one")
	}
	// Dictionary is sized by DISTINCT terms, not rows — the property that makes the
	// string column cheaper than typed-per-row storage would be.
	if got := len(cs.dict); got != 2 {
		t.Errorf("dictionary holds %d terms, want 2 distinct", got)
	}
}

// TestTypedColumn_TypedReadDoesNotAllocate is the point of the whole layout. If
// this reports >0 allocs the typed door is boxing somewhere and the change bought
// nothing.
func TestTypedColumn_TypedReadDoesNotAllocate(t *testing.T) {
	ids := make([]types.NodeID, 1000)
	vals := make(map[types.NodeID]any, len(ids))
	for i := range ids {
		ids[i] = types.NodeID(i + 1)
		vals[types.NodeID(i+1)] = int64(i)
	}
	l := buildOne(t, ids, vals)
	view, _ := l.View("v")

	var sum int64
	got := testing.AllocsPerRun(100, func() {
		sum = 0
		for ord := 0; ord < view.N; ord++ {
			if view.Present(ord) && !view.IsFloat(ord) {
				sum += view.Ints[ord]
			}
		}
	})
	if got != 0 {
		t.Errorf("typed column scan allocated %.1f times per run, want 0", got)
	}
	if want := int64(999 * 1000 / 2); sum != want {
		t.Errorf("scan summed %d, want %d — the loop did not read the column", sum, want)
	}
}

// TestTypedColumn_UnbuildableStillDeclines pins that widening the storage did NOT
// widen what the column accepts. A bool/list/map or a numeric+string mix must still
// be refused so the consumer falls back, rather than being coerced into a numeric
// column that answers a different question.
func TestTypedColumn_UnbuildableStillDeclines(t *testing.T) {
	ids := []types.NodeID{1, 2}
	for name, vals := range map[string]map[types.NodeID]any{
		"bool":           {1: true, 2: false},
		"numeric_string": {1: int64(1), 2: "x"},
		"list":           {1: []int64{1}, 2: []int64{2}},
	} {
		t.Run(name, func(t *testing.T) {
			l := buildOne(t, ids, vals)
			if l.Has("v") {
				t.Error("column built for an unbuildable value class")
			}
			if _, ok := l.View("v"); ok {
				t.Error("View accepted a column Has() refuses — the two must mirror " +
					"exactly, or a typed reader sees spurious nulls where the boxed " +
					"reader correctly falls back")
			}
		})
	}
}
