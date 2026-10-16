package importmap

import (
	"math"
	"strconv"
	"testing"

	fixtures "github.com/data-insights-ai/rho-tkg/tools/v5-import-fixtures"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/graph"
)

func ty(t, v string) fixtures.Typed { return fixtures.Typed{T: t, V: v} }

// wantInt / wantFloat / wantKind describe one expected mapping.
type want struct {
	kind  string // "" = mapped
	i     int64
	f     float64
	isInt bool
	isF   bool
	other graph.ValueKind // KindString/KindBool/KindNull when mapped to those
}

func TestMapWidthsToScalar(t *testing.T) {
	maxI := strconv.FormatInt(math.MaxInt64, 10)
	over := strconv.FormatUint(math.MaxInt64+1, 10)
	cases := []struct {
		name string
		in   fixtures.Typed
		w    want
	}{
		{"int", ty("int", "-7"), want{isInt: true, i: -7}},
		{"int8", ty("int8", "-128"), want{isInt: true, i: -128}},
		{"int16", ty("int16", "32767"), want{isInt: true, i: 32767}},
		{"int32", ty("int32", "-2147483648"), want{isInt: true, i: math.MinInt32}},
		{"int64 min", ty("int64", strconv.FormatInt(math.MinInt64, 10)), want{isInt: true, i: math.MinInt64}},
		{"int64 max", ty("int64", maxI), want{isInt: true, i: math.MaxInt64}},
		{"uint", ty("uint", "5"), want{isInt: true, i: 5}},
		{"uint8", ty("uint8", "255"), want{isInt: true, i: 255}},
		{"uint16", ty("uint16", "65535"), want{isInt: true, i: 65535}},
		{"uint32", ty("uint32", "4294967295"), want{isInt: true, i: math.MaxUint32}},
		{"uint64 = MaxInt64", ty("uint64", maxI), want{isInt: true, i: math.MaxInt64}},
		{"uint64 = MaxInt64+1", ty("uint64", over), want{kind: "uint_over_maxint64"}},
		{"uint64 max", ty("uint64", "18446744073709551615"), want{kind: "uint_over_maxint64"}},
		{"uint over", ty("uint", over), want{kind: "uint_over_maxint64"}},
		{"float64", ty("float64", "3.25"), want{isF: true, f: 3.25}},
		{"float32 widens exactly", ty("float32", "0.1"), want{isF: true, f: float64(float32(0.1))}},
		{"float64 -0 is +0", ty("float64", "-0"), want{isF: true, f: 0}},
		{"float64 NaN", ty("float64", "NaN"), want{kind: "nan"}},
		{"float32 NaN", ty("float32", "NaN"), want{kind: "nan"}},
		{"float64 +Inf", ty("float64", "+Inf"), want{kind: "inf"}},
		{"float64 -Inf", ty("float64", "-Inf"), want{kind: "inf"}},
		{"float32 +Inf", ty("float32", "+Inf"), want{kind: "inf"}},
		{"float32 overflow text", ty("float32", "1e39"), want{kind: "malformed"}},
		{"empty string", ty("string", ""), want{other: graph.KindString}},
		{"bool", ty("bool", "true"), want{other: graph.KindBool}},
		{"nil", fixtures.Typed{T: "nil"}, want{other: graph.KindNull}},
		{"bytes", ty("bytes", "00ff"), want{kind: "bytes"}},
		{"time", ty("time", "2024-03-14T09:26:53Z"), want{kind: "time"}},
		{"temporal", ty("temporal4", "2024-01-02T03:04:05Z"), want{kind: "temporal"}},
		{"list", fixtures.Typed{T: "[]int64", V: []fixtures.Typed{ty("int64", "1")}}, want{kind: "list"}},
		{"empty list", fixtures.Typed{T: "[]string", V: []fixtures.Typed{}}, want{kind: "list"}},
		{"map", fixtures.Typed{T: "map", V: map[string]fixtures.Typed{"a": ty("bool", "true")}}, want{kind: "map"}},
		{"unsupported", ty("unsupported:chan int", "x"), want{kind: "unsupported"}},
		{"unknown tag", ty("complex128", "1"), want{kind: "unsupported"}},
		{"int garbage", ty("int64", "12x"), want{kind: "malformed"}},
		{"int out of range for width", ty("int8", "200"), want{kind: "malformed"}},
		{"int empty", ty("int64", ""), want{kind: "malformed"}},
		{"negative uint", ty("uint32", "-1"), want{kind: "malformed"}},
		{"float garbage", ty("float64", "abc"), want{kind: "malformed"}},
		{"bool garbage", ty("bool", "yes"), want{kind: "malformed"}},
	}
	for _, c := range cases {
		v, kind := MapValue(c.in)
		if kind != c.w.kind {
			t.Errorf("%s: ambiguity kind %q, want %q", c.name, kind, c.w.kind)
			continue
		}
		if kind != "" {
			if v.Kind() != graph.KindInvalid {
				t.Errorf("%s: ambiguous value must be the zero Value", c.name)
			}
			continue
		}
		switch {
		case c.w.isInt:
			if got, ok := v.AsInt(); !ok || got != c.w.i {
				t.Errorf("%s: AsInt = %d,%v want %d", c.name, got, ok, c.w.i)
			}
		case c.w.isF:
			if got, ok := v.AsFloat(); !ok || got != c.w.f || math.Signbit(got) {
				t.Errorf("%s: AsFloat = %v,%v want %v", c.name, got, ok, c.w.f)
			}
		default:
			if v.Kind() != c.w.other {
				t.Errorf("%s: kind %v want %v", c.name, v.Kind(), c.w.other)
			}
		}
	}
	// Mapped integers stay integers and floats stay floats: 3 is not 3.0 by kind.
	i, _ := MapValue(ty("int64", "3"))
	f, _ := MapValue(ty("float64", "3"))
	if _, ok := f.AsInt(); ok || i.Kind() == f.Kind() {
		t.Error("float64 3 must stay a float")
	}
}
