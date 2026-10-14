package index

import (
	"math/rand/v2"
	"reflect"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestSubsetDocValuesEqualsBuild (round 7 request 6): over random rows and
// random subsets (empty, one row, all rows, every third, random), the
// snapshot SubsetDocValues takes from a label's columns is deep-equal to the
// one BuildDocValues builds from the same rows' values: IDs, validity and
// zone map, and per key the subset's own classification (an all-int subset of
// a mixed column is int64, a string column absent on the subset is an empty
// numeric column, dictionaries renumbered). A key the source cannot build
// (strings and numbers) or has not built declines, as do rows without
// validity columns.
func TestSubsetDocValuesEqualsBuild(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 11))
	const n = 9000
	ids := make([]types.NodeID, n)
	props := make([]map[string]any, n)
	valid := make([][2]int64, n)
	for i := range ids {
		ids[i] = types.NodeID(snowflake.ID(1000 + 3*i))
		p := map[string]any{}
		if rng.IntN(10) > 0 {
			p["i"] = int64(rng.IntN(50))
		}
		if rng.IntN(2) == 0 {
			p["mixed"] = int64(rng.IntN(9))
		} else if rng.IntN(3) > 0 {
			p["mixed"] = rng.Float64()
		}
		if rng.IntN(4) > 0 {
			p["s"] = []string{"a", "b", "c", "d"}[rng.IntN(4)]
		}
		if rng.IntN(100) == 0 {
			p["rare"] = "x"
		}
		if rng.IntN(2) == 0 {
			p["strnum"] = "v"
		} else {
			p["strnum"] = int64(1)
		}
		p["u"] = uint64(1) << 63 // past MaxInt64: a float
		props[i] = p
		valid[i] = [2]int64{int64(rng.IntN(1000)), int64(rng.IntN(3)) * 2000}
	}
	pos := map[types.NodeID]int{}
	for i, id := range ids {
		pos[id] = i
	}
	getProp := func(id types.NodeID, key string) (any, bool) { v, ok := props[pos[id]][key]; return v, ok }
	getTemporal := func(id types.NodeID) (int64, int64, bool) { v := valid[pos[id]]; return v[0], v[1], true }
	keys := []string{"i", "mixed", "s", "rare", "u", "i"}
	src := BuildLabelDocValues(5, ids, append(keys, "strnum"), getProp, getTemporal)

	subsets := map[string][]int{"empty": nil, "one": {4321}, "all": nil, "third": nil, "random": nil}
	for o := range n {
		subsets["all"] = append(subsets["all"], o)
		if o%3 == 0 {
			subsets["third"] = append(subsets["third"], o)
		}
		if rng.IntN(7) == 0 {
			subsets["random"] = append(subsets["random"], o)
		}
	}
	// A subset where "mixed" holds only ints and "s" is absent.
	for o := range n {
		if _, isInt := props[o]["mixed"].(int64); isInt {
			if _, hasS := props[o]["s"]; !hasS {
				subsets["intsNoS"] = append(subsets["intsNoS"], o)
			}
		}
	}
	for name, ords := range subsets {
		got, ok := SubsetDocValues(9, src, ords, keys)
		if !ok {
			t.Fatalf("%s: declined", name)
		}
		subIDs := make([]types.NodeID, len(ords))
		for j, o := range ords {
			subIDs[j] = src.ids[o]
		}
		want := BuildLabelDocValues(9, subIDs, keys, getProp, getTemporal)
		if !reflect.DeepEqual(got, want) {
			for _, k := range keys {
				if !reflect.DeepEqual(got.cols[k], want.cols[k]) {
					t.Errorf("%s: column %q differs:\n got  %+v\n want %+v", name, k, got.cols[k], want.cols[k])
				}
			}
			t.Fatalf("%s: snapshot differs from the build (%d rows)", name, len(ords))
		}
	}
	if _, ok := SubsetDocValues(9, src, subsets["one"], []string{"strnum"}); ok {
		t.Error("an unbuildable source column: not declined")
	}
	if _, ok := SubsetDocValues(9, src, subsets["one"], []string{"never"}); ok {
		t.Error("an unbuilt key: not declined")
	}
	plain := BuildLabelDocValues(5, ids, keys, getProp, nil)
	if _, ok := SubsetDocValues(9, plain, subsets["one"], keys); ok {
		t.Error("no validity columns: not declined")
	}
}
