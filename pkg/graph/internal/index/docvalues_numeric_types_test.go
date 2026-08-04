package index

import (
	"math"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 16c: buildColumn's classification switch only recognized
// int64/int/int32/float64/float32 as numeric. Any OTHER value from the
// canonical scalar-numeric allowlist (pkg/types/propertyslice.go's
// isScalarPropertyValue: int8/int16/uint/uint8/uint16/uint32/uint64) landed
// in the "default: sawOther = true" arm, which poisons the WHOLE column —
// every other node's value for the same property on the same label loses
// the columnar accelerator too, silently, with no error or log. A property
// that happens to be stored as e.g. uint8 on just one node degrades an
// entire label/property pair back to the per-node fallback path.
//
// This proves each of the seven previously-unsupported types, mixed in
// alongside int64/float64 values (mirroring real heterogeneous-but-numeric
// data), now builds as ColNumeric instead of colUnbuildable.
func TestDocValues_WidenedNumericTypesBuildable(t *testing.T) {
	cases := []struct {
		name string
		val  any
		want int64 // expected boxed int64 value (all cases here fit)
	}{
		{"int8", int8(-5), -5},
		{"int16", int16(-500), -500},
		{"uint", uint(7), 7},
		{"uint8", uint8(200), 200},
		{"uint16", uint16(50000), 50000},
		{"uint32", uint32(3_000_000_000), 3_000_000_000},
		{"uint64_small", uint64(42), 42},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ids := []types.NodeID{1, 2}
			props := map[types.NodeID]map[string]any{
				1: {"x": tc.val},
				2: {"x": int64(99)},
			}
			l := BuildLabelDocValues(1, ids, []string{"x"}, getter(props), nil)
			if !l.Has("x") {
				t.Fatalf("%s: column reported unbuildable, want buildable (numeric) — BACKLOG 16c regression", tc.name)
			}
			got := map[types.NodeID]any{}
			l.ForEachRow([]string{"x"}, func(id types.NodeID, vs []any, ps []bool) bool {
				got[id] = vs[0]
				return true
			})
			v, ok := got[1].(int64)
			if !ok {
				t.Fatalf("%s: node 1 boxed as %T, want int64", tc.name, got[1])
			}
			if v != tc.want {
				t.Fatalf("%s: node 1 = %d, want %d", tc.name, v, tc.want)
			}
			if v2, ok := got[2].(int64); !ok || v2 != 99 {
				t.Fatalf("%s: node 2 = %v (%T), want int64 99", tc.name, got[2], got[2])
			}
		})
	}
}

// TestDocValues_Uint64OverflowFallsBackToFloat64 pins the one genuine edge
// case widening introduces: a uint64 value beyond math.MaxInt64 cannot be
// boxed as int64 without wrapping negative, so it must box as float64
// instead — the same magnitude-only precision trade-off
// property_stats_accumulator.go's numericValue already documents and
// accepts for int64/uint64 beyond 2^53, never a silent negative wrap.
func TestDocValues_Uint64OverflowFallsBackToFloat64(t *testing.T) {
	huge := uint64(math.MaxInt64) + 1000
	ids := []types.NodeID{1}
	props := map[types.NodeID]map[string]any{1: {"x": huge}}
	l := BuildLabelDocValues(1, ids, []string{"x"}, getter(props), nil)
	if !l.Has("x") {
		t.Fatalf("column reported unbuildable, want buildable (numeric)")
	}
	var got any
	l.ForEachRow([]string{"x"}, func(id types.NodeID, vs []any, ps []bool) bool {
		got = vs[0]
		return true
	})
	f, ok := got.(float64)
	if !ok {
		t.Fatalf("got %T %v, want float64 (int64 would wrap negative)", got, got)
	}
	if f <= 0 {
		t.Fatalf("got %v, want a positive float64 approximation of %d, not a wrapped negative", f, huge)
	}
}

// TestDocValues_MixedWidenedTypesInOneColumn pins that several of the
// newly-recognized types coexisting in the SAME column (the realistic case
// — different nodes' properties may have been written with different Go
// integer widths over time) all build as one numeric column, not just each
// type in isolation.
func TestDocValues_MixedWidenedTypesInOneColumn(t *testing.T) {
	ids := []types.NodeID{1, 2, 3, 4, 5}
	props := map[types.NodeID]map[string]any{
		1: {"x": int8(1)},
		2: {"x": uint16(2)},
		3: {"x": uint32(3)},
		4: {"x": int64(4)},
		5: {"x": float64(5.5)},
	}
	l := BuildLabelDocValues(1, ids, []string{"x"}, getter(props), nil)
	if !l.Has("x") {
		t.Fatalf("mixed-width numeric column reported unbuildable — BACKLOG 16c regression")
	}
	gotIDs, vals, present := collectRows(l, []string{"x"})
	if len(gotIDs) != 5 {
		t.Fatalf("got %d rows, want 5", len(gotIDs))
	}
	for i, id := range gotIDs {
		if !present[i][0] {
			t.Fatalf("node %v: present=false, want true", id)
		}
	}
	sum := 0.0
	for _, row := range vals {
		switch x := row[0].(type) {
		case int64:
			sum += float64(x)
		case float64:
			sum += x
		default:
			t.Fatalf("unexpected boxed type %T", row[0])
		}
	}
	if sum != 15.5 {
		t.Fatalf("sum = %v, want 15.5", sum)
	}
}
