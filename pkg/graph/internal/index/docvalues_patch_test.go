package index

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Patch must be INDISTINGUISHABLE from a full rebuild over the patched
// values (round 5 request 4), the same oracle Extend answers to: values,
// presence, representation (uniform int, uniform float, mixed, dictionary
// order), validity and zone-map decisions.

func patchTemporal(shift map[types.NodeID]int64) func(types.NodeID) (int64, int64, int64, bool) {
	return func(id types.NodeID) (int64, int64, int64, bool) {
		if id%7 == 0 {
			return 0, 0, 0, false // no metadata
		}
		return int64(id)*10 + shift[id], int64(id)*10 + 5 + shift[id], int64(id)*10 + shift[id], true
	}
}

// reprOf is the column's representation, which dump does not show.
func reprOf(l *LabelDocValues) string {
	v, ok := l.View("v")
	if !ok {
		return "unbuildable"
	}
	if v.Type == ColString {
		return fmt.Sprintf("string dict=%v", v.Dict)
	}
	return fmt.Sprintf("numeric ints=%v flts=%v mixed=%v", v.Ints != nil, v.Flts != nil, v.Mixed())
}

// TestPatch_MatchesFullRebuild: random columns (numeric with ints, floats and
// absent rows; strings with repeated terms and absent rows; spanning several
// zone blocks), random in-place changes within the column's class (values,
// int<->float, present<->absent, validity), Patch against a full rebuild.
func TestPatch_MatchesFullRebuild(t *testing.T) {
	r := rand.New(rand.NewSource(5)) // #nosec G404 -- deterministic test sequence
	for round := range 300 {
		n := 1 + r.Intn(3*zoneBlockSize)
		str := r.Intn(2) == 0
		val := func() any {
			switch k := r.Intn(10); {
			case k == 0:
				return nil
			case str:
				return string(rune('a' + r.Intn(6)))
			case k < 4:
				return float64(r.Intn(50)) + 0.5
			default:
				return int64(r.Intn(50))
			}
		}
		vals := map[types.NodeID]any{}
		shift := map[types.NodeID]int64{}
		ids := make([]types.NodeID, n)
		for i := range ids {
			ids[i] = types.NodeID(i*3 + 1)
			if v := val(); v != nil {
				vals[ids[i]] = v
			}
		}
		base := BuildLabelDocValues(1, ids, []string{"v"}, extProps(vals), patchTemporal(shift))
		before := dump(t, base)
		beforeRepr := reprOf(base)

		var changed []types.NodeID
		for range 1 + r.Intn(12) {
			id := ids[r.Intn(n)]
			changed = append(changed, id)
			if v := val(); v != nil {
				vals[id] = v
			} else {
				delete(vals, id)
			}
			shift[id] = int64(r.Intn(4))
		}
		patched := base.Patch(2, changed, extProps(vals), patchTemporal(shift))
		full := BuildLabelDocValues(2, ids, []string{"v"}, extProps(vals), patchTemporal(shift))
		if len(vals) == 0 {
			if patched != nil {
				t.Fatalf("round %d: patched an all-absent column", round)
			}
			continue
		}
		if patched == nil {
			t.Fatalf("round %d: Patch refused an in-place update within the column's class", round)
		}
		sameDump(t, fmt.Sprintf("round %d", round), dump(t, patched), dump(t, full))
		if got, want := reprOf(patched), reprOf(full); got != want {
			t.Fatalf("round %d: representation %s, rebuild %s", round, got, want)
		}
		if patched.Epoch() != 2 {
			t.Fatalf("round %d: epoch %d", round, patched.Epoch())
		}
		// The receiver is untouched.
		sameDump(t, fmt.Sprintf("round %d receiver", round), dump(t, base), before)
		if reprOf(base) != beforeRepr {
			t.Fatalf("round %d: receiver representation changed", round)
		}
	}
}

// TestPatch_Refuses pins the inputs where an in-place patch is not what a
// rebuild would give; each must return nil.
func TestPatch_Refuses(t *testing.T) {
	ids := []types.NodeID{10, 20, 30}
	nums := map[types.NodeID]any{10: int64(1), 20: int64(2), 30: 3.5}
	strs := map[types.NodeID]any{10: "a", 20: "b", 30: "a"}
	numBase := BuildLabelDocValues(1, ids, []string{"v"}, extProps(nums), extTemporal)
	strBase := BuildLabelDocValues(1, ids, []string{"v"}, extProps(strs), extTemporal)
	mixed := map[types.NodeID]any{10: int64(1), 20: "x", 30: int64(3)}
	unbuildable := BuildLabelDocValues(1, ids, []string{"v"}, extProps(mixed), extTemporal)
	with := func(base map[types.NodeID]any, id types.NodeID, v any) map[types.NodeID]any {
		out := map[types.NodeID]any{}
		for k, x := range base {
			out[k] = x
		}
		if v == nil {
			delete(out, id)
		} else {
			out[id] = v
		}
		return out
	}
	cases := []struct {
		name string
		got  *LabelDocValues
	}{
		{"no changed rows", numBase.Patch(2, nil, extProps(nums), extTemporal)},
		{"unknown row (an insert)", numBase.Patch(2, []types.NodeID{40}, extProps(with(nums, 40, int64(4))), extTemporal)},
		{"string into numeric", numBase.Patch(2, []types.NodeID{20}, extProps(with(nums, 20, "s")), extTemporal)},
		{"number into string", strBase.Patch(2, []types.NodeID{20}, extProps(with(strs, 20, int64(1))), extTemporal)},
		{"bool into numeric", numBase.Patch(2, []types.NodeID{20}, extProps(with(nums, 20, true)), extTemporal)},
		{"unbuildable column", unbuildable.Patch(2, []types.NodeID{20}, extProps(with(mixed, 20, int64(2))), extTemporal)},
		{"temporal accessor missing", numBase.Patch(2, []types.NodeID{20}, extProps(nums), nil)},
		{"all rows absent", strBase.Patch(2, ids, extProps(map[types.NodeID]any{}), extTemporal)},
		{"nil receiver", (*LabelDocValues)(nil).Patch(2, ids, extProps(nums), extTemporal)},
	}
	for _, tc := range cases {
		if tc.got != nil {
			t.Errorf("%s: Patch returned a snapshot, want nil (rebuild)", tc.name)
		}
	}
	if !numBase.Contains(20) || numBase.Contains(40) || (*LabelDocValues)(nil).Contains(20) {
		t.Fatal("Contains")
	}
}

// TestPatch_StringDictionaryDropsUnusedTerms: a term no row uses after the
// patch leaves the dictionary, and new terms are numbered in first-appearance
// order, as a rebuild numbers them.
func TestPatch_StringDictionaryDropsUnusedTerms(t *testing.T) {
	ids := []types.NodeID{1, 2, 3, 4}
	vals := map[types.NodeID]any{1: "a", 2: "b", 3: "c", 4: "a"}
	base := BuildLabelDocValues(1, ids, []string{"v"}, extProps(vals), nil)
	vals[2], vals[3] = "z", "a"
	p := base.Patch(2, []types.NodeID{3, 2}, extProps(vals), nil)
	if p == nil {
		t.Fatal("refused")
	}
	v, _ := p.View("v")
	if !slices.Equal(v.Dict, []string{"a", "z"}) {
		t.Fatalf("dict %v, want [a z]", v.Dict)
	}
	rebuilt := BuildLabelDocValues(2, ids, []string{"v"}, extProps(vals), nil)
	if !SameIDs(base, p) || SameIDs(base, rebuilt) || SameIDs[types.NodeID](nil, p) {
		t.Fatal("SameIDs: a patch shares its receiver's ID vector, a rebuild does not")
	}
}
