package index

import (
	"math"
	"math/big"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestEqualCountMatchesCypherEquality (sigma-tkgd round 7 request 4): over
// stored values of every width, EqualCount equals a brute-force count of the
// stored values the property equality keeps (numbers by exact value with
// float32 widened, also past 2^53; no cross-kind match, NaN and nil never
// equal), and declines (exact=false) where the index cannot say.
func TestEqualCountMatchesCypherEquality(t *testing.T) {
	stored := []any{
		int(3), int8(3), int16(3), int32(3), int64(3), uint(3), uint8(3), uint16(3), uint32(3), uint64(3),
		float32(3), float64(3), "3", true, false, math.Copysign(0, -1), int64(0), float32(0.1), float64(0.1),
		math.NaN(), math.Inf(1), float32(math.Inf(-1)), int64(-200), int16(-200), uint8(200), float64(2.5),
		int64(1 << 53), int64(1<<53 + 1), float64(1 << 53), uint64(1 << 60), "x", types.TemporalValue{Kind: 1, Value: "2026-01-01"},
	}
	pi := NewPropertyIndex()
	for i, v := range stored {
		pi.Add(snowflake.ID(i+1), v)
	}
	exact := func(v any) (*big.Float, bool) {
		switch x := v.(type) {
		case int, int8, int16, int32, int64:
			return new(big.Float).SetInt64(reflectInt(x)), true
		case uint, uint8, uint16, uint32, uint64:
			return new(big.Float).SetUint64(reflectUint(x)), true
		case float32:
			if math.IsNaN(float64(x)) {
				return nil, true
			}
			return new(big.Float).SetFloat64(float64(x)), true
		case float64:
			if math.IsNaN(x) {
				return nil, true
			}
			return new(big.Float).SetFloat64(x), true
		}
		return nil, false
	}
	equal := func(a, b any) bool { // the property equality on the stored kinds
		fa, na := exact(a)
		fb, nb := exact(b)
		if na || nb {
			return na && nb && fa != nil && fb != nil && fa.Cmp(fb) == 0 // NaN != NaN
		}
		return a == b
	}
	for _, q := range []any{
		int64(3), 3.0, float32(3), uint8(3), "3", true, false, 0.0, math.Copysign(0, -1), int(0),
		float32(0.1), 0.1, math.NaN(), math.Inf(1), math.Inf(-1), int64(-200), 200, 2.5, int64(7), "x", "y", nil,
		float64(1 << 53), int64(1<<53 + 1), uint64(1<<53 + 1), uint64(1 << 60), float64(1 << 60), -1e300,
	} {
		var want int64
		for _, s := range stored {
			if q != nil && equal(q, s) {
				want++
			}
		}
		got, exact := pi.EqualCount(q)
		if !exact || got != want {
			t.Errorf("EqualCount(%T %v) = %d, %v; want %d, true", q, q, got, exact, want)
		}
	}
	for _, q := range []any{types.TemporalValue{Kind: 1, Value: "2026-01-01"}, []any{int64(3)}, map[string]any{}} {
		if _, exact := pi.EqualCount(q); exact {
			t.Errorf("EqualCount(%T %v): exact, want declined", q, q)
		}
	}
	if _, exact := (*PropertyIndex)(nil).EqualCount(int64(3)); exact {
		t.Error("nil index: exact")
	}
	elements := NewPropertyIndexWith(false, true)
	elements.Add(1, int64(3))
	if _, exact := elements.EqualCount(int64(3)); exact {
		t.Error("Elements index: exact")
	}
	pi.Mutated = map[snowflake.ID]struct{}{}
	if _, exact := pi.EqualCount(int64(3)); exact {
		t.Error("index being built: exact")
	}
	pi.Mutated = nil
	pi.Remove(snowflake.ID(5), int64(3))
	if got, _ := pi.EqualCount(3); got != 11 {
		t.Errorf("after a remove: EqualCount(3) = %d, want 11", got)
	}
}

// reflectInt / reflectUint widen a signed / unsigned integer of any width.
func reflectInt(v any) int64 {
	switch x := v.(type) {
	case int:
		return int64(x)
	case int8:
		return int64(x)
	case int16:
		return int64(x)
	case int32:
		return int64(x)
	}
	return v.(int64)
}

func reflectUint(v any) uint64 {
	switch x := v.(type) {
	case uint:
		return uint64(x)
	case uint8:
		return uint64(x)
	case uint16:
		return uint64(x)
	case uint32:
		return uint64(x)
	}
	return v.(uint64)
}
