package index

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Extend must be INDISTINGUISHABLE from a full rebuild, or it trades a correctness
// bug for a speed-up. The oracle below is that equality; the probes after it are the
// inputs where "just append" is wrong and Extend must refuse instead.

func extProps(vals map[types.NodeID]any) func(types.NodeID, string) (any, bool) {
	return func(id types.NodeID, _ string) (any, bool) {
		v, ok := vals[id]
		return v, ok
	}
}

func extTemporal(id types.NodeID) (int64, int64, bool) {
	return int64(id) * 10, int64(id)*10 + 5, true
}

// dump flattens a snapshot to a comparable form: ids, presence, typed values,
// validity, and the zone-map decisions.
func dump(t *testing.T, l *LabelDocValues) []string {
	t.Helper()
	if l == nil {
		return nil
	}
	v, ok := l.View("v")
	if !ok {
		return []string{"unbuildable"}
	}
	out := make([]string, 0, l.Len()+8)
	for ord, id := range l.IDs() {
		s := ""
		switch {
		case !v.Present(ord):
			s = "absent"
		case v.Type == ColString:
			s = "s:" + v.StringAt(ord)
		case v.IsFloat(ord):
			s = fmtF(v.Flts[ord])
		default:
			s = fmtI(v.Ints[ord])
		}
		out = append(out, fmtRow(int64(id), s, l.ValidFrom()[ord], l.ValidTo()[ord]))
	}
	// Zone-map decisions are part of observable behaviour: a wrong block bound
	// silently drops rows, and comparing values alone would not see it.
	for _, q := range [][2]int64{{0, 25}, {40, 60}, {0, 0}, {1000, 2000}} {
		for s := 0; s < l.Len(); s += zoneBlockSize {
			out = append(out, fmtZone(s, q[0], q[1], l.BlockCanMatch(s, q[0], q[1])))
		}
	}
	return out
}

func fmtRow(id int64, val string, f, to int64) string {
	return "id=" + itoa(id) + " v=" + val + " [" + itoa(f) + "," + itoa(to) + ")"
}
func fmtZone(s int, a, b int64, ok bool) string {
	return "zone" + itoa(int64(s)) + ":" + itoa(a) + "-" + itoa(b) + "=" + btoa(ok)
}
func fmtI(v int64) string   { return "i" + itoa(v) }
func fmtF(v float64) string { return "f" + fmtFloat(v) }
func btoa(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var b []byte
	for v > 0 {
		b = append([]byte{byte('0' + v%10)}, b...)
		v /= 10
	}
	if neg {
		return "-" + string(b)
	}
	return string(b)
}

func fmtFloat(v float64) string {
	i := int64(v * 1000)
	return itoa(i)
}

func sameDump(t *testing.T, what string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: length %d, rebuild produced %d", what, len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("%s: entry %d: extend=%q rebuild=%q", what, i, got[i], want[i])
		}
	}
}

// TestExtend_MatchesFullRebuild is the oracle. Extend copies existing rows and
// re-reads only the appended ones; a rebuild re-reads everything. They must agree on
// values, presence, validity AND zone-map decisions.
func TestExtend_MatchesFullRebuild(t *testing.T) {
	cases := map[string]struct {
		base, add map[types.NodeID]any
	}{
		"int_then_int":    {map[types.NodeID]any{1: int64(1), 2: int64(2)}, map[types.NodeID]any{3: int64(3), 4: int64(4)}},
		"int_then_float":  {map[types.NodeID]any{1: int64(1), 2: int64(2)}, map[types.NodeID]any{3: 3.5}},
		"float_then_int":  {map[types.NodeID]any{1: 1.5, 2: 2.5}, map[types.NodeID]any{3: int64(3)}},
		"mixed_then_int":  {map[types.NodeID]any{1: int64(1), 2: 2.5}, map[types.NodeID]any{3: int64(3)}},
		"string_new_term": {map[types.NodeID]any{1: "a", 2: "b"}, map[types.NodeID]any{3: "c"}},
		"string_dup_term": {map[types.NodeID]any{1: "a", 2: "b"}, map[types.NodeID]any{3: "a"}},
		"absent_in_base":  {map[types.NodeID]any{1: int64(1)}, map[types.NodeID]any{3: int64(3)}},
		"absent_in_added": {map[types.NodeID]any{1: int64(1), 2: int64(2)}, map[types.NodeID]any{}},
		"big_int64":       {map[types.NodeID]any{1: int64(9_000_000_000_000_001)}, map[types.NodeID]any{3: int64(9_000_000_000_000_002)}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			all := map[types.NodeID]any{}
			baseIDs := []types.NodeID{}
			for id, v := range tc.base {
				all[id], baseIDs = v, append(baseIDs, id)
			}
			addIDs := []types.NodeID{}
			for id, v := range tc.add {
				all[id], addIDs = v, append(addIDs, id)
			}
			// "absent_in_added" appends an id with no value at all.
			if len(addIDs) == 0 {
				addIDs = []types.NodeID{9}
			}

			base := BuildLabelDocValues(1, baseIDs, []string{"v"}, extProps(all), extTemporal)
			ext := base.Extend(2, addIDs, extProps(all), extTemporal)
			if ext == nil {
				t.Fatal("Extend refused a legitimate append")
			}
			full := BuildLabelDocValues(2, append(baseIDs, addIDs...), []string{"v"},
				extProps(all), extTemporal)
			sameDump(t, name, dump(t, ext), dump(t, full))
		})
	}
}

// TestExtend_RefusesNonAppends pins every input where "just append" would be wrong.
// Each must return nil so the caller rebuilds; returning a snapshot would ship a
// silently stale or mistyped column.
func TestExtend_RefusesNonAppends(t *testing.T) {
	base := BuildLabelDocValues(1, []types.NodeID{10, 20}, []string{"v"},
		extProps(map[types.NodeID]any{10: int64(1), 20: int64(2)}), extTemporal)
	vals := map[types.NodeID]any{10: int64(1), 20: int64(2), 30: int64(3)}

	t.Run("empty", func(t *testing.T) {
		if base.Extend(2, nil, extProps(vals), extTemporal) != nil {
			t.Error("extended by nothing instead of refusing")
		}
	})
	t.Run("id_already_present", func(t *testing.T) {
		// An existing ID means an UPDATE, which can change a value already captured.
		if base.Extend(2, []types.NodeID{20}, extProps(vals), extTemporal) != nil {
			t.Error("accepted an ID it already holds — that is an update, not an append")
		}
	})
	t.Run("id_before_max", func(t *testing.T) {
		// Out-of-order breaks the sorted-ordinal invariant lookup's binary search needs.
		if base.Extend(2, []types.NodeID{15}, extProps(vals), extTemporal) != nil {
			t.Error("accepted an out-of-order ID")
		}
	})
	t.Run("duplicate_within_batch", func(t *testing.T) {
		if base.Extend(2, []types.NodeID{30, 30}, extProps(vals), extTemporal) != nil {
			t.Error("accepted a batch containing a duplicate")
		}
	})
	t.Run("temporal_shape_mismatch", func(t *testing.T) {
		// Base HAS temporal columns; extending without an accessor would leave the new
		// rows at (0,0), which reads as valid for all time.
		if base.Extend(2, []types.NodeID{30}, extProps(vals), nil) != nil {
			t.Error("extended a temporal snapshot without a temporal accessor")
		}
		noTemp := BuildLabelDocValues(1, []types.NodeID{10}, []string{"v"}, extProps(vals), nil)
		if noTemp.Extend(2, []types.NodeID{30}, extProps(vals), extTemporal) != nil {
			t.Error("added temporal columns to a snapshot that had none")
		}
	})
	t.Run("type_conflict_string_into_numeric", func(t *testing.T) {
		bad := map[types.NodeID]any{10: int64(1), 20: int64(2), 30: "oops"}
		if base.Extend(2, []types.NodeID{30}, extProps(bad), extTemporal) != nil {
			t.Error("appended a string into a numeric column instead of refusing")
		}
	})
	t.Run("type_conflict_bool_into_numeric", func(t *testing.T) {
		bad := map[types.NodeID]any{10: int64(1), 20: int64(2), 30: true}
		if base.Extend(2, []types.NodeID{30}, extProps(bad), extTemporal) != nil {
			t.Error("appended a bool into a numeric column instead of refusing")
		}
	})
	t.Run("numeric_into_string_column", func(t *testing.T) {
		sbase := BuildLabelDocValues(1, []types.NodeID{10, 20}, []string{"v"},
			extProps(map[types.NodeID]any{10: "a", 20: "b"}), extTemporal)
		bad := map[types.NodeID]any{30: int64(7)}
		if sbase.Extend(2, []types.NodeID{30}, extProps(bad), extTemporal) != nil {
			t.Error("appended a number into a string column instead of refusing")
		}
	})
}

// TestExtend_DoesNotMutateTheReceiver pins the immutability the lock-free read
// discipline rests on: a reader holding the old snapshot must keep seeing exactly
// what it saw.
func TestExtend_DoesNotMutateTheReceiver(t *testing.T) {
	vals := map[types.NodeID]any{1: int64(1), 2: int64(2), 3: int64(3)}
	base := BuildLabelDocValues(1, []types.NodeID{1, 2}, []string{"v"}, extProps(vals), extTemporal)
	before := dump(t, base)

	ext := base.Extend(2, []types.NodeID{3}, extProps(vals), extTemporal)
	if ext == nil {
		t.Fatal("Extend refused a legitimate append")
	}
	if got := len(base.IDs()); got != 2 {
		t.Errorf("receiver grew to %d rows — Extend mutated it", got)
	}
	if base.Epoch() != 1 {
		t.Errorf("receiver epoch changed to %d", base.Epoch())
	}
	sameDump(t, "receiver-after-extend", dump(t, base), before)
}

// TestExtend_UniformColumnStaysUniform pins that appending does not quietly bloat a
// column into carrying both halves. A mixed-representation column returns identical
// values while costing double, so only a structural assertion catches it.
func TestExtend_UniformColumnStaysUniform(t *testing.T) {
	vals := map[types.NodeID]any{1: int64(1), 2: int64(2), 3: int64(3)}
	base := BuildLabelDocValues(1, []types.NodeID{1, 2}, []string{"v"}, extProps(vals), extTemporal)
	ext := base.Extend(2, []types.NodeID{3}, extProps(vals), extTemporal)
	v, _ := ext.View("v")
	if v.Flts != nil || v.Mixed() {
		t.Error("appending an int to a uniform int column allocated the float half")
	}
}
