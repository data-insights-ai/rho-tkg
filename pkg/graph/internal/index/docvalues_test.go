package index

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// getter builds a getProp callback from a per-node property map.
func getter(props map[types.NodeID]map[string]any) func(types.NodeID, string) (any, bool) {
	return func(id types.NodeID, key string) (any, bool) {
		m, ok := props[id]
		if !ok {
			return nil, false
		}
		v, ok := m[key]
		return v, ok
	}
}

// collectRows drains ForEachRow into aligned slices keyed by node ID.
func collectRows(l *LabelDocValues, keys []string) (ids []types.NodeID, vals [][]any, present [][]bool) {
	l.ForEachRow(keys, func(id types.NodeID, vs []any, ps []bool) bool {
		ids = append(ids, id)
		cv := make([]any, len(vs))
		copy(cv, vs)
		cp := make([]bool, len(ps))
		copy(cp, ps)
		vals = append(vals, cv)
		present = append(present, cp)
		return true
	})
	return
}

// TestDocValues_Int64PrecisionPreserved pins that a value above 2^53 round-trips
// as an EXACT int64, not a lossy float64 — the column must not silently truncate
// the way a float64-only store would (CLAUDE.md int64 precision rule).
func TestDocValues_Int64PrecisionPreserved(t *testing.T) {
	const big = int64(9_000_000_000_000_001) // > 2^53; not representable exactly as float64
	ids := []types.NodeID{3, 1, 2}
	props := map[types.NodeID]map[string]any{
		1: {"v": big}, 2: {"v": int64(5)}, 3: {"v": 7.5},
	}
	l := BuildLabelDocValues(1, ids, []string{"v"}, getter(props))
	got := map[types.NodeID]any{}
	l.ForEachRow([]string{"v"}, func(id types.NodeID, vs []any, ps []bool) bool {
		got[id] = vs[0]
		return true
	})
	if v, ok := got[1].(int64); !ok || v != big {
		t.Fatalf("node 1: got %T %v, want exact int64 %d", got[1], got[1], big)
	}
	if v, ok := got[2].(int64); !ok || v != 5 {
		t.Fatalf("node 2: got %T %v, want int64 5", got[2], got[2])
	}
	if v, ok := got[3].(float64); !ok || v != 7.5 {
		t.Fatalf("node 3: got %T %v, want float64 7.5 (mixed int/float column)", got[3], got[3])
	}
}

// TestDocValues_FullMembershipAndAbsent pins that EVERY label member is a row even
// when it lacks the property (present=false, value nil) — so count(*) over the
// column counts the same rows the unfiltered scan would (critique M2).
func TestDocValues_FullMembershipAndAbsent(t *testing.T) {
	ids := []types.NodeID{1, 2, 3, 4}
	props := map[types.NodeID]map[string]any{
		1: {"age": int64(30)},
		2: {"other": int64(1)}, // no "age"
		3: {"age": int64(40)},
		// node 4: no properties at all
	}
	l := BuildLabelDocValues(1, ids, []string{"age"}, getter(props))
	gotIDs, vals, present := collectRows(l, []string{"age"})
	if len(gotIDs) != 4 {
		t.Fatalf("rows = %d, want 4 (full membership)", len(gotIDs))
	}
	absent := 0
	for i := range gotIDs {
		if !present[i][0] {
			absent++
			if vals[i][0] != nil {
				t.Fatalf("absent row %v: value %v, want nil", gotIDs[i], vals[i][0])
			}
		}
	}
	if absent != 2 {
		t.Fatalf("absent rows = %d, want 2 (nodes 2 and 4)", absent)
	}
}

// TestDocValues_StringDictEncoding pins string columns reconstruct the exact value
// for low-cardinality group keys (the city/status case).
func TestDocValues_StringDictEncoding(t *testing.T) {
	ids := []types.NodeID{1, 2, 3, 4}
	props := map[types.NodeID]map[string]any{
		1: {"city": "berlin"}, 2: {"city": "munich"}, 3: {"city": "berlin"}, 4: {"city": "berlin"},
	}
	l := BuildLabelDocValues(1, ids, []string{"city"}, getter(props))
	if !l.Has("city") {
		t.Fatal("city column not buildable")
	}
	count := map[string]int{}
	l.ForEachRow([]string{"city"}, func(_ types.NodeID, vs []any, ps []bool) bool {
		if ps[0] {
			count[vs[0].(string)]++
		}
		return true
	})
	if count["berlin"] != 3 || count["munich"] != 1 {
		t.Fatalf("dict counts = %v, want berlin:3 munich:1", count)
	}
}

// TestDocValues_HeterogeneousUnbuildable pins that a property with mixed
// numeric+string values, or an unsupported type (bool), is NOT buildable — the
// consumer must fall back rather than guess (correctness over reach).
func TestDocValues_HeterogeneousUnbuildable(t *testing.T) {
	ids := []types.NodeID{1, 2}
	cases := map[string]map[types.NodeID]map[string]any{
		"numeric+string": {1: {"x": int64(5)}, 2: {"x": "five"}},
		"bool":           {1: {"x": true}, 2: {"x": false}},
		"list":           {1: {"x": []any{1}}, 2: {"x": []any{2}}},
	}
	for name, props := range cases {
		t.Run(name, func(t *testing.T) {
			l := BuildLabelDocValues(1, ids, []string{"x"}, getter(props))
			if l.Has("x") {
				t.Fatalf("%s: column reported buildable, want unbuildable (fallback)", name)
			}
			if l.HasAll([]string{"x"}) {
				t.Fatalf("%s: HasAll true, want false", name)
			}
		})
	}
}

// TestDocValues_ZippedAlignment pins that two columns iterate aligned by ordinal —
// the (groupKey, aggArg) pair the sink zips must refer to the same node per row.
func TestDocValues_ZippedAlignment(t *testing.T) {
	ids := []types.NodeID{5, 1, 3} // unsorted on purpose; builder sorts
	props := map[types.NodeID]map[string]any{
		1: {"city": "a", "age": int64(10)},
		3: {"city": "b", "age": int64(20)},
		5: {"city": "a", "age": int64(30)},
	}
	l := BuildLabelDocValues(1, ids, []string{"city", "age"}, getter(props))
	gotIDs, vals, _ := collectRows(l, []string{"city", "age"})
	// Sorted ordinal order: 1, 3, 5.
	want := []struct {
		id   types.NodeID
		city string
		age  int64
	}{{1, "a", 10}, {3, "b", 20}, {5, "a", 30}}
	for i, w := range want {
		if gotIDs[i] != w.id || vals[i][0].(string) != w.city || vals[i][1].(int64) != w.age {
			t.Fatalf("row %d: got (%v, %v, %v), want (%v, %q, %d)",
				i, gotIDs[i], vals[i][0], vals[i][1], w.id, w.city, w.age)
		}
	}
}

// TestDocValues_Immutable pins that a rebuild produces an independent snapshot — a
// reader holding the old LabelDocValues sees the old values (Pattern 36: published
// columns are never mutated in place).
func TestDocValues_Immutable(t *testing.T) {
	ids := []types.NodeID{1}
	v1 := map[types.NodeID]map[string]any{1: {"v": int64(1)}}
	v2 := map[types.NodeID]map[string]any{1: {"v": int64(2)}}
	old := BuildLabelDocValues(1, ids, []string{"v"}, getter(v1))
	_ = BuildLabelDocValues(2, ids, []string{"v"}, getter(v2)) // a "rebuild" at a new epoch
	var got any
	old.ForEachRow([]string{"v"}, func(_ types.NodeID, vs []any, _ []bool) bool { got = vs[0]; return true })
	if got.(int64) != 1 {
		t.Fatalf("old snapshot mutated: got %v, want 1", got)
	}
	if old.Epoch() != 1 {
		t.Fatalf("old epoch = %d, want 1", old.Epoch())
	}
}
