package index

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestPropertyStatsAccumulatorObserveMinMax(t *testing.T) {
	t.Parallel()
	a := NewPropertyStatsAccumulator()

	a.Observe(types.IndexablePropertyValueKey(int64(5)), int64(5))
	a.Observe(types.IndexablePropertyValueKey(int64(1)), int64(1))
	a.Observe(types.IndexablePropertyValueKey(int64(9)), int64(9))
	a.Observe(types.IndexablePropertyValueKey(int64(1)), int64(1)) // duplicate

	ndv, min, max := a.Snapshot()
	if min != int64(1) {
		t.Fatalf("min = %v, want int64(1)", min)
	}
	if max != int64(9) {
		t.Fatalf("max = %v, want int64(9)", max)
	}
	// HyperLogLog has large RELATIVE variance at tiny N by design (the
	// accuracy contract this package is held to — <5%/<3% at 10k/100k — is
	// pinned in hyperloglog_test.go, not here); just sanity-check NDV is
	// positive and not wildly larger than the true 3 distinct values.
	if ndv < 1 || ndv > 20 {
		t.Fatalf("ndv = %d, want a small positive estimate (true distinct count is 3)", ndv)
	}
}

func TestPropertyStatsAccumulatorStringFamily(t *testing.T) {
	t.Parallel()
	a := NewPropertyStatsAccumulator()
	a.Observe(types.IndexablePropertyValueKey("banana"), "banana")
	a.Observe(types.IndexablePropertyValueKey("apple"), "apple")
	a.Observe(types.IndexablePropertyValueKey("cherry"), "cherry")

	_, min, max := a.Snapshot()
	if min != "apple" {
		t.Fatalf("min = %v, want \"apple\"", min)
	}
	if max != "cherry" {
		t.Fatalf("max = %v, want \"cherry\"", max)
	}
}

// TestPropertyStatsAccumulatorUnorderedFamilyExcludedFromMinMax asserts that
// bool/TemporalValue-shaped observations still count toward NDV but never
// set Min/Max — the documented "scalar-ordered families only" limitation.
func TestPropertyStatsAccumulatorUnorderedFamilyExcludedFromMinMax(t *testing.T) {
	t.Parallel()
	a := NewPropertyStatsAccumulator()
	a.Observe(types.IndexablePropertyValueKey(true), true)
	a.Observe(types.IndexablePropertyValueKey(false), false)

	ndv, min, max := a.Snapshot()
	if ndv != 2 {
		t.Fatalf("ndv = %d, want 2 (bool values still count toward NDV)", ndv)
	}
	if min != nil || max != nil {
		t.Fatalf("min=%v max=%v, want both nil (bool is not a scalar-ordered family)", min, max)
	}
}

// TestPropertyStatsAccumulatorMixedFamilyFirstWins asserts the documented
// tie-break: the FIRST family observed wins; a later value from a different
// family is excluded from Min/Max but still folded into NDV.
func TestPropertyStatsAccumulatorMixedFamilyFirstWins(t *testing.T) {
	t.Parallel()
	a := NewPropertyStatsAccumulator()
	a.Observe(types.IndexablePropertyValueKey(int64(42)), int64(42))
	a.Observe(types.IndexablePropertyValueKey("a-string"), "a-string") // different family, ignored for min/max

	ndv, min, max := a.Snapshot()
	if ndv != 2 {
		t.Fatalf("ndv = %d, want 2", ndv)
	}
	if min != int64(42) || max != int64(42) {
		t.Fatalf("min=%v max=%v, want both int64(42) (numeric family won first)", min, max)
	}
}

// TestPropertyStatsAccumulatorForgetDirtyRescan is the delete-the-extremum
// case: Forget-ing the current max marks the accumulator dirty; Rescan then
// recomputes an exact min/max from the caller-supplied surviving values.
func TestPropertyStatsAccumulatorForgetDirtyRescan(t *testing.T) {
	t.Parallel()
	a := NewPropertyStatsAccumulator()
	a.Observe(types.IndexablePropertyValueKey(int64(1)), int64(1))
	a.Observe(types.IndexablePropertyValueKey(int64(5)), int64(5))
	a.Observe(types.IndexablePropertyValueKey(int64(9)), int64(9))

	if a.Dirty() {
		t.Fatalf("accumulator should not be dirty before any Forget")
	}

	// Forget-ing a NON-extremal value must not mark dirty.
	a.Forget(int64(5))
	if a.Dirty() {
		t.Fatalf("forgetting a non-extremal value should not mark dirty")
	}

	// Forget-ing the current max (9) marks dirty.
	a.Forget(int64(9))
	if !a.Dirty() {
		t.Fatalf("forgetting the current max should mark dirty")
	}
	// Snapshot before Rescan still reports the STALE max (9) — Dirty() is the
	// caller's signal to Rescan before trusting Min/Max.
	_, _, staleMax := a.Snapshot()
	if staleMax != int64(9) {
		t.Fatalf("stale max before Rescan = %v, want int64(9) (unchanged until Rescan)", staleMax)
	}

	// Rescan with the surviving values (1 was never forgotten from the live
	// set in this scenario — only 9 was actually removed).
	a.Rescan([]any{int64(1)})
	if a.Dirty() {
		t.Fatalf("Rescan should clear the dirty flag")
	}
	ndv, min, max := a.Snapshot()
	if min != int64(1) || max != int64(1) {
		t.Fatalf("after rescan min=%v max=%v, want both int64(1)", min, max)
	}
	// NDV is untouched by Rescan — it stays at the sketch's own estimate over
	// every value EVER Observed (3 distinct values, HyperLogLog never
	// forgets); see TestHyperLogLogAccuracy10k/100k for the tight accuracy
	// bound — at this tiny N the estimate only needs to stay small/positive.
	if ndv < 1 || ndv > 20 {
		t.Fatalf("ndv after rescan = %d, want a small positive estimate (Rescan/Forget do not affect NDV)", ndv)
	}
}

// TestPropertyStatsAccumulatorRescanEmptyClearsMinMax covers the case where
// every value has left the population: Rescan([]) clears Min/Max to nil.
func TestPropertyStatsAccumulatorRescanEmptyClearsMinMax(t *testing.T) {
	t.Parallel()
	a := NewPropertyStatsAccumulator()
	a.Observe(types.IndexablePropertyValueKey(int64(1)), int64(1))
	a.Forget(int64(1))
	if !a.Dirty() {
		t.Fatalf("forgetting the only value (also the min/max) should mark dirty")
	}
	a.Rescan(nil)
	_, min, max := a.Snapshot()
	if min != nil || max != nil {
		t.Fatalf("min=%v max=%v after emptying, want both nil", min, max)
	}
}

// TestPropertyStatsAccumulatorRescanSkipsUnorderedAndMixedFamily asserts that
// Rescan applies the SAME family rules as Observe when rebuilding from
// scratch.
func TestPropertyStatsAccumulatorRescanSkipsUnorderedAndMixedFamily(t *testing.T) {
	t.Parallel()
	a := NewPropertyStatsAccumulator()
	a.Rescan([]any{true, int64(3), "a-string", int64(7)})
	_, min, max := a.Snapshot()
	if min != int64(3) || max != int64(7) {
		t.Fatalf("min=%v max=%v, want int64(3)/int64(7) (bool and the later string family are excluded)", min, max)
	}
}

func TestPropertyStatsAccumulatorNilSafety(t *testing.T) {
	t.Parallel()
	var a *PropertyStatsAccumulator
	a.Observe("k", int64(1)) // must not panic
	a.Forget(int64(1))       // must not panic
	a.Rescan([]any{int64(1)})
	if a.Dirty() {
		t.Fatalf("nil accumulator Dirty() = true, want false")
	}
	ndv, min, max := a.Snapshot()
	if ndv != 0 || min != nil || max != nil {
		t.Fatalf("nil accumulator Snapshot() = (%d,%v,%v), want (0,nil,nil)", ndv, min, max)
	}
	if g := a.WriteGen(); g != 0 {
		t.Fatalf("nil accumulator WriteGen() = %d, want 0", g)
	}
}

// TestPropertyStatsAccumulatorWriteGen pins the optimistic-concurrency guard:
// WriteGen starts at zero, bumps on every Observe AND every Forget (regardless
// of value family — a conservative guard over-signals rather than misses a
// population change), and is NOT bumped by the read-only Rescan/Snapshot/Dirty
// methods.
func TestPropertyStatsAccumulatorWriteGen(t *testing.T) {
	t.Parallel()
	a := NewPropertyStatsAccumulator()
	if g := a.WriteGen(); g != 0 {
		t.Fatalf("fresh WriteGen() = %d, want 0", g)
	}

	a.Observe("10", int64(10)) // numeric — enters min/max
	if g := a.WriteGen(); g != 1 {
		t.Fatalf("WriteGen after 1 Observe = %d, want 1", g)
	}
	a.Observe("t", true) // unordered family — still a population change
	if g := a.WriteGen(); g != 2 {
		t.Fatalf("WriteGen after 2 Observe = %d, want 2", g)
	}

	a.Forget(int64(10)) // forgets the current extremum
	if g := a.WriteGen(); g != 3 {
		t.Fatalf("WriteGen after Forget = %d, want 3", g)
	}
	a.Forget("nomatch") // different family — no dirty-mark, but still a write
	if g := a.WriteGen(); g != 4 {
		t.Fatalf("WriteGen after 2nd Forget = %d, want 4", g)
	}

	// Read-only methods must not move the generation.
	_ = a.Dirty()
	a.Snapshot()
	a.Rescan([]any{int64(5)})
	if g := a.WriteGen(); g != 4 {
		t.Fatalf("WriteGen after read-only calls = %d, want 4 (unchanged)", g)
	}
}

func TestScalarOrderFamily(t *testing.T) {
	t.Parallel()
	numeric := []any{int(1), int8(1), int16(1), int32(1), int64(1), uint(1), uint8(1), uint16(1), uint32(1), uint64(1), float32(1), float64(1)}
	for _, v := range numeric {
		if fam := scalarOrderFamily(v); fam != "n" {
			t.Errorf("scalarOrderFamily(%T) = %q, want \"n\"", v, fam)
		}
	}
	if fam := scalarOrderFamily("s"); fam != "s" {
		t.Errorf("scalarOrderFamily(string) = %q, want \"s\"", fam)
	}
	unordered := []any{true, nil, []any{1}, map[string]any{}}
	for _, v := range unordered {
		if fam := scalarOrderFamily(v); fam != "" {
			t.Errorf("scalarOrderFamily(%T) = %q, want \"\"", v, fam)
		}
	}
}

// TestPropertyStatsAccumulatorForgetDifferentFamilyIgnored exercises the
// "fam != a.family" branch of Forget (as opposed to the "fam == unordered"
// branch already covered elsewhere): forgetting a STRING value while the
// accumulator's tracked family is numeric must be a no-op, not a dirty-mark.
func TestPropertyStatsAccumulatorForgetDifferentFamilyIgnored(t *testing.T) {
	t.Parallel()
	a := NewPropertyStatsAccumulator()
	a.Observe(types.IndexablePropertyValueKey(int64(5)), int64(5))
	a.Forget("not-the-tracked-family")
	if a.Dirty() {
		t.Fatalf("forgetting a value from a DIFFERENT family than the tracked one should not mark dirty")
	}
}

// TestPropertyStatsAccumulatorRescanDescendingUpdatesMin exercises Rescan's
// min-update branch (as opposed to its max-update branch, already covered by
// TestPropertyStatsAccumulatorRescanSkipsUnorderedAndMixedFamily): a value
// SMALLER than the running min, arriving after the first element.
func TestPropertyStatsAccumulatorRescanDescendingUpdatesMin(t *testing.T) {
	t.Parallel()
	a := NewPropertyStatsAccumulator()
	a.Rescan([]any{int64(9), int64(5), int64(1)})
	_, min, max := a.Snapshot()
	if min != int64(1) || max != int64(9) {
		t.Fatalf("min=%v max=%v, want int64(1)/int64(9)", min, max)
	}
}

// TestScalarLessAllNumericTypes exercises numericValue's full type switch —
// every property-allowlist numeric type, compared against its own type (a
// same-family comparison, matching scalarLess's contract).
func TestScalarLessAllNumericTypes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		lo   any
		hi   any
	}{
		{"int", int(1), int(2)},
		{"int8", int8(1), int8(2)},
		{"int16", int16(1), int16(2)},
		{"int32", int32(1), int32(2)},
		{"int64", int64(1), int64(2)},
		{"uint", uint(1), uint(2)},
		{"uint8", uint8(1), uint8(2)},
		{"uint16", uint16(1), uint16(2)},
		{"uint32", uint32(1), uint32(2)},
		{"uint64", uint64(1), uint64(2)},
		{"float32", float32(1), float32(2)},
		{"float64", float64(1), float64(2)},
	}
	for _, tc := range cases {
		if !scalarLess(tc.lo, tc.hi) {
			t.Errorf("%s: scalarLess(lo, hi) = false, want true", tc.name)
		}
		if scalarLess(tc.hi, tc.lo) {
			t.Errorf("%s: scalarLess(hi, lo) = true, want false", tc.name)
		}
		if !scalarEqual(tc.lo, tc.lo) {
			t.Errorf("%s: scalarEqual(lo, lo) = false, want true", tc.name)
		}
		if scalarEqual(tc.lo, tc.hi) {
			t.Errorf("%s: scalarEqual(lo, hi) = true, want false", tc.name)
		}
	}
	// An unrecognized type (numericValue's default branch) projects to 0.
	if numericValue(struct{}{}) != 0 {
		t.Errorf("numericValue(unrecognized type) != 0")
	}
}

// TestScalarLessAndEqualStrings exercises scalarLess/scalarEqual's string
// branch directly — Forget's own tests only ever reach scalarEqual with
// numeric values, since a string Forget on a numeric-family accumulator
// short-circuits at the family-mismatch check before comparing.
func TestScalarLessAndEqualStrings(t *testing.T) {
	t.Parallel()
	if !scalarLess("apple", "banana") {
		t.Errorf("scalarLess(\"apple\",\"banana\") = false, want true")
	}
	if scalarLess("banana", "apple") {
		t.Errorf("scalarLess(\"banana\",\"apple\") = true, want false")
	}
	if !scalarEqual("apple", "apple") {
		t.Errorf("scalarEqual(\"apple\",\"apple\") = false, want true")
	}
	if scalarEqual("apple", "banana") {
		t.Errorf("scalarEqual(\"apple\",\"banana\") = true, want false")
	}
}

// TestPropertyStatsAccumulatorSketch exercises Sketch() directly: a
// populated accumulator returns a CLONE whose Estimate() matches the
// accumulator's own NDV, mutating the clone must not perturb the
// accumulator, and a nil receiver returns nil (the tiered fold's "no
// accumulator on this shard" case).
func TestPropertyStatsAccumulatorSketch(t *testing.T) {
	t.Parallel()
	a := NewPropertyStatsAccumulator()
	a.Observe(types.IndexablePropertyValueKey(int64(1)), int64(1))
	a.Observe(types.IndexablePropertyValueKey(int64(2)), int64(2))

	wantNDV, _, _ := a.Snapshot()
	sketch := a.Sketch()
	if sketch == nil {
		t.Fatal("Sketch() = nil, want a populated sketch")
	}
	if sketch.Estimate() != wantNDV {
		t.Fatalf("sketch.Estimate() = %d, want %d (must match Snapshot's NDV)", sketch.Estimate(), wantNDV)
	}

	// The returned sketch is an independent CLONE: mutating it must not
	// perturb the accumulator's own NDV.
	sketch.AddString("a-value-never-observed-by-the-accumulator")
	afterNDV, _, _ := a.Snapshot()
	if afterNDV != wantNDV {
		t.Fatalf("accumulator NDV changed after mutating the returned clone: before=%d after=%d", wantNDV, afterNDV)
	}

	var nilAcc *PropertyStatsAccumulator
	if got := nilAcc.Sketch(); got != nil {
		t.Fatalf("nil-receiver Sketch() = %v, want nil", got)
	}
}

// TestCombineExtrema exercises every branch of the cross-shard fold's
// min/max tie-break helper directly (the tiered backend's
// NodePropertyStats calls this per shard — see
// docs/adr/0005-tiered-parity.md §3.1): a nil incoming pair is a no-op, a nil
// running pair adopts the incoming one, same-family pairs fold to the
// min-of-mins/max-of-maxes, and a MIXED-family incoming pair (the "unusual
// in a well-typed graph" case documented on PropertyStatsAccumulator) is
// ignored — first-family-wins, mirroring Observe/Rescan's own rule.
func TestCombineExtrema(t *testing.T) {
	t.Parallel()

	// Nil incoming is a no-op.
	if min, max := CombineExtrema(int64(5), int64(10), nil, nil); min != int64(5) || max != int64(10) {
		t.Fatalf("nil incoming: got (%v,%v), want (5,10)", min, max)
	}
	// Nil running adopts the incoming pair.
	if min, max := CombineExtrema(nil, nil, int64(3), int64(7)); min != int64(3) || max != int64(7) {
		t.Fatalf("nil running: got (%v,%v), want (3,7)", min, max)
	}
	// Same family: incoming pushes both the min DOWN and the max UP.
	if min, max := CombineExtrema(int64(10), int64(20), int64(1), int64(30)); min != int64(1) || max != int64(30) {
		t.Fatalf("widen both: got (%v,%v), want (1,30)", min, max)
	}
	// Same family: incoming is strictly inside the running range — neither
	// bound moves.
	if min, max := CombineExtrema(int64(1), int64(30), int64(10), int64(20)); min != int64(1) || max != int64(30) {
		t.Fatalf("interior incoming: got (%v,%v), want (1,30)", min, max)
	}
	// Mixed family: a string incoming pair against a numeric running pair is
	// IGNORED (first-family-wins) — still exercised via the numeric branch
	// above; here the reverse direction.
	if min, max := CombineExtrema(int64(1), int64(30), "z", "z"); min != int64(1) || max != int64(30) {
		t.Fatalf("mixed family incoming ignored: got (%v,%v), want (1,30)", min, max)
	}
}
