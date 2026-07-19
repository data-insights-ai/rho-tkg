package index

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 16d: tasks/lessons.md #65 documented the FNV-1a short-sequential-
// integer undercount as a TEST-AUTHORING footgun ("feed well-distributed
// values, never short sequential integers") and left production code
// unchanged. But the sketch's actual hash input in production is
// types.IndexablePropertyValueKey(value) — e.g. "i64:1990", "i64:1991",
// "i64:1992" for a "birthYear" property, or "i64:0".."i64:120" for an "age"
// property — which is EXACTLY the adversarial shape lesson 65 identified:
// consecutive keys differ only in their low-order ASCII digit(s). Real
// property data (years, ages, small counters, sequential external IDs) has
// this shape routinely, so NodePropertyStats/NodePropertyStatsSketch NDV
// accuracy was compromised for realistic data, not just a contrived test.
//
// These tests feed the SAME key format the store backends actually hash
// (via types.IndexablePropertyValueKey) for a realistic sequential-integer
// property and assert the NDV estimate is close to the true distinct count
// — not just "positive" or "not absurdly high" (the loose bound style
// lesson 65 noted was the reason nobody caught this).

// sequentialIntKeys returns the IndexablePropertyValueKey strings for
// int64(from)..int64(from+n-1) — the same encoding a real "age" or "year"
// property's values would produce.
func sequentialIntKeys(from int64, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = types.IndexablePropertyValueKey(from + int64(i))
	}
	return out
}

func TestHyperLogLogSequentialIntegerKeysAccuracy(t *testing.T) {
	t.Parallel()
	h, err := NewHyperLogLog(DefaultHLLPrecision)
	if err != nil {
		t.Fatalf("NewHyperLogLog: %v", err)
	}
	// Mirrors a realistic "age" property: 0..1999, the same shape as the
	// lesson-65 repro (fmt.Sprintf("%d", i) for i:=0..n), but through the
	// actual production key encoding ("i64:<n>").
	const n = 2000
	for _, s := range sequentialIntKeys(0, n) {
		h.AddString(s)
	}
	est := h.Estimate()
	if re := relativeError(est, n); re >= 0.10 {
		t.Fatalf("sequential int64 keys 0..%d: estimate=%d actual=%d relative error=%.4f, want <0.10 — BACKLOG 16d regression (FNV-1a low-avalanche undercount on production-shaped keys)", n, est, n, re)
	}
}

func TestHyperLogLogSequentialIntegerKeysAccuracy10k(t *testing.T) {
	t.Parallel()
	h, err := NewHyperLogLog(DefaultHLLPrecision)
	if err != nil {
		t.Fatalf("NewHyperLogLog: %v", err)
	}
	const n = 10000
	for _, s := range sequentialIntKeys(1_000_000, n) {
		h.AddString(s)
	}
	est := h.Estimate()
	if re := relativeError(est, n); re >= 0.05 {
		t.Fatalf("sequential int64 keys at 10k: estimate=%d actual=%d relative error=%.4f, want <0.05 — BACKLOG 16d regression", est, n, re)
	}
}

// TestPropertyStatsAccumulator_SequentialYearsAccuracy exercises the bug at
// the actual production call site: PropertyStatsAccumulator.Observe fed a
// realistic "birthYear" distribution (1950..2020, 71 distinct years) via the
// same valueKey encoding NodePropertyStats/NodePropertyStatsSketch use.
func TestPropertyStatsAccumulator_SequentialYearsAccuracy(t *testing.T) {
	t.Parallel()
	a := NewPropertyStatsAccumulator()
	const firstYear, lastYear = 1950, 2020
	const n = lastYear - firstYear + 1
	for y := firstYear; y <= lastYear; y++ {
		v := int64(y)
		a.Observe(types.IndexablePropertyValueKey(v), v)
	}
	got := a.Sketch().Estimate()
	if re := relativeError(got, n); re >= 0.15 {
		t.Fatalf("years %d..%d (%d distinct): NDV estimate=%d relative error=%.4f, want <0.15 — BACKLOG 16d regression", firstYear, lastYear, n, got, re)
	}
}

// TestHyperLogLogSequentialIntegerKeysManyDistinctRegisters is a coarser,
// implementation-independent sanity check: with m=2^14=16384 registers and
// 2000 well-mixed distinct sequential-int keys, the sketch must actually
// touch a large fraction of distinct registers (proving the mix genuinely
// scatters the inputs) rather than concentrating them into a handful — the
// direct mechanism of the bug (register collapse), checked independent of
// the Estimate() formula.
func TestHyperLogLogSequentialIntegerKeysManyDistinctRegisters(t *testing.T) {
	t.Parallel()
	h, err := NewHyperLogLog(DefaultHLLPrecision)
	if err != nil {
		t.Fatalf("NewHyperLogLog: %v", err)
	}
	for _, s := range sequentialIntKeys(0, 2000) {
		h.AddString(s)
	}
	if h.dense == nil {
		h.convertToDense()
	}
	nonzero := 0
	for _, rho := range h.dense {
		if rho != 0 {
			nonzero++
		}
	}
	// A well-scattered 2000-item load over 16384 registers should touch
	// somewhere around 2000*(1-1/e) ≈ 1264 distinct registers (birthday-
	// paradox fill fraction); a collapsed load (the bug) touches only a
	// couple dozen. Use a generous floor well below the expected value but
	// far above what collapse produces.
	const wantMin = 800
	if nonzero < wantMin {
		t.Fatalf("nonzero registers = %d, want >= %d (sequential keys collapsing into too few registers — BACKLOG 16d regression)", nonzero, wantMin)
	}
}
