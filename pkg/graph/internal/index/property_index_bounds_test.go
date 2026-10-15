package index

import (
	"math"
	"math/rand/v2"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// TestNumericBoundsMatchesScan: through random adds and removes in set and
// counted mode, NumericBounds equals the smallest and largest sort key among
// the live numeric entries (NaN never), the stored value at each is of the
// preferred type among those present at the key, and an empty view is
// has=false.
func TestNumericBoundsMatchesScan(t *testing.T) {
	pool := []any{int64(3), 3.0, int8(3), uint16(3), float32(0.5), 0.5, math.Inf(-1), math.Inf(1), math.NaN(),
		int64(-7), -7.0, int32(-7), uint8(200), 200.0, float32(-7), "s", true}
	rank := func(v any) int {
		switch v.(type) {
		case float64:
			return 0
		case float32:
			return 1
		case int64:
			return 2
		case int32:
			return 4
		case int8:
			return 6
		case uint16:
			return 10
		case uint8:
			return 11
		}
		return 99
	}
	for _, counted := range []bool{false, true} {
		rng := rand.New(rand.NewPCG(7, uint64(len(pool))))
		pi := NewPropertyIndexWith(counted, false)
		live := map[snowflake.ID]any{}
		for step := range 3000 {
			id := snowflake.ID(rng.IntN(40) + 1)
			if old, ok := live[id]; ok {
				pi.Remove(id, old)
				delete(live, id)
			}
			if rng.IntN(3) > 0 {
				v := pool[rng.IntN(len(pool))]
				pi.Add(id, v)
				live[id] = v
			}
			var lo, hi float64
			var atLo, atHi any
			found := false
			for _, v := range live {
				k, ok := numericSortKey(PropertyValueKey(v))
				if !ok {
					continue
				}
				if !found || k < lo || (k == lo && rank(v) < rank(atLo)) {
					lo, atLo = k, v
				}
				if !found || k > hi || (k == hi && rank(v) < rank(atHi)) {
					hi, atHi = k, v
				}
				found = true
			}
			gotLo, gotHi, vLo, vHi, has, supported := pi.NumericBounds()
			if !supported || has != found {
				t.Fatalf("counted=%v step %d: has=%v supported=%v, want has=%v", counted, step, has, supported, found)
			}
			if found && (gotLo != lo || gotHi != hi || vLo != atLo || vHi != atHi) {
				t.Fatalf("counted=%v step %d: [%v %T(%v), %v %T(%v)], want [%v %T(%v), %v %T(%v)]", counted, step,
					gotLo, vLo, vLo, gotHi, vHi, vHi, lo, atLo, atLo, hi, atHi, atHi)
			}
		}
	}
}

// TestNumericBoundsUnsupported: a nil index, an Elements index, an index being
// built and one holding an integer past 2^53 state nothing.
func TestNumericBoundsUnsupported(t *testing.T) {
	var nilIdx *PropertyIndex
	if _, _, _, _, _, ok := nilIdx.NumericBounds(); ok {
		t.Error("nil index supported")
	}
	if _, _, _, _, _, ok := NewPropertyIndexWith(false, true).NumericBounds(); ok {
		t.Error("Elements index supported")
	}
	pi := NewPropertyIndex()
	pi.Add(1, int64(4))
	pi.Mutated = map[snowflake.ID]struct{}{}
	if _, _, _, _, _, ok := pi.NumericBounds(); ok {
		t.Error("index being built supported")
	}
	pi.Mutated = nil
	pi.Add(2, uint64(1<<60))
	if _, _, _, _, _, ok := pi.NumericBounds(); ok {
		t.Error("integer past 2^53 supported")
	}
	pi.Remove(2, uint64(1<<60))
	if lo, hi, _, _, has, ok := pi.NumericBounds(); !ok || !has || lo != 4 || hi != 4 {
		t.Errorf("after the large integer left: [%v, %v] has=%v ok=%v", lo, hi, has, ok)
	}
}

// TestPropertyBoundsOf: the assembly computes Missing from the entity count,
// declines where NumericBounds does, and declines where the index and the
// class counts disagree on whether a number exists.
func TestPropertyBoundsOf(t *testing.T) {
	pi := NewPropertyIndex()
	pi.Add(1, int64(2))
	pi.Add(2, 5.5)
	pi.Add(3, "x")
	classes := storepkg.PropertyTypeClassCounts{Numeric: 2, String: 1, Missing: 9}
	b, ok := PropertyBoundsOf(pi, classes, 5)
	want := storepkg.PropertyBounds{Min: 2, Max: 5.5, MinValue: int64(2), MaxValue: 5.5,
		Classes: storepkg.PropertyTypeClassCounts{Numeric: 2, String: 1, Missing: 2}}
	if !ok || b != want {
		t.Fatalf("PropertyBoundsOf = %+v ok=%v, want %+v", b, ok, want)
	}
	if _, ok := PropertyBoundsOf(pi, storepkg.PropertyTypeClassCounts{String: 1}, 5); ok {
		t.Error("index holds numbers, counts none: answered")
	}
	empty := NewPropertyIndex()
	empty.Add(1, "x")
	if _, ok := PropertyBoundsOf(empty, storepkg.PropertyTypeClassCounts{Numeric: 1, String: 1}, 2); ok {
		t.Error("counts hold a number, index none: answered")
	}
	if b, ok := PropertyBoundsOf(empty, storepkg.PropertyTypeClassCounts{String: 1}, 1); !ok || b.HasNumeric() || !b.NullFree() || b.NumericOnly() {
		t.Errorf("strings only: %+v ok=%v", b, ok)
	}
	if _, ok := PropertyBoundsOf(nil, classes, 5); ok {
		t.Error("nil index answered")
	}
}
