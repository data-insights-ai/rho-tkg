package index

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
)

// seededDistinctStrings returns n DISTINCT deterministic strings — seeded via
// a fixed PRNG (math/rand/v2 with a literal seed) so the accuracy assertions
// below are reproducible across runs and machines, in place of an externally
// published HLL test-vector file (this package has no test fixture
// infrastructure to load one from).
func seededDistinctStrings(seed uint64, n int) []string {
	rng := rand.New(rand.NewPCG(seed, seed^0xa5a5a5a5))
	out := make([]string, n)
	for i := range out {
		// 128 bits of randomness per value — collision probability across
		// even 10^6 draws is negligible, so "n distinct inputs" holds.
		out[i] = fmt.Sprintf("%016x%016x", rng.Uint64(), rng.Uint64())
	}
	return out
}

func relativeError(estimate, actual int64) float64 {
	if actual == 0 {
		return 0
	}
	diff := estimate - actual
	if diff < 0 {
		diff = -diff
	}
	return float64(diff) / float64(actual)
}

// TestHyperLogLogAccuracy10k pins the WP accuracy bar: error < 5% at 10k
// distinct values, at the default precision (14) the store backends use.
func TestHyperLogLogAccuracy10k(t *testing.T) {
	t.Parallel()
	h, err := NewHyperLogLog(DefaultHLLPrecision)
	if err != nil {
		t.Fatalf("NewHyperLogLog: %v", err)
	}
	const n = 10000
	for _, s := range seededDistinctStrings(1, n) {
		h.AddString(s)
	}
	est := h.Estimate()
	if re := relativeError(est, n); re >= 0.05 {
		t.Fatalf("10k distinct: estimate=%d actual=%d relative error=%.4f, want <0.05", est, n, re)
	}
}

// TestHyperLogLogAccuracy100k pins the WP accuracy bar: error < 3% at 100k
// distinct values.
func TestHyperLogLogAccuracy100k(t *testing.T) {
	t.Parallel()
	h, err := NewHyperLogLog(DefaultHLLPrecision)
	if err != nil {
		t.Fatalf("NewHyperLogLog: %v", err)
	}
	const n = 100000
	for _, s := range seededDistinctStrings(2, n) {
		h.AddString(s)
	}
	est := h.Estimate()
	if re := relativeError(est, n); re >= 0.03 {
		t.Fatalf("100k distinct: estimate=%d actual=%d relative error=%.4f, want <0.03", est, n, re)
	}
}

// TestHyperLogLogSmallRangeLinearCounting exercises the small-range
// linear-counting correction branch (raw <= 2.5m) with a low but nonzero
// cardinality, and asserts the estimate is still reasonably close.
func TestHyperLogLogSmallRangeLinearCounting(t *testing.T) {
	t.Parallel()
	h, err := NewHyperLogLog(DefaultHLLPrecision)
	if err != nil {
		t.Fatalf("NewHyperLogLog: %v", err)
	}
	const n = 50
	for _, s := range seededDistinctStrings(3, n) {
		h.AddString(s)
	}
	est := h.Estimate()
	if re := relativeError(est, n); re >= 0.5 {
		t.Fatalf("50 distinct (small range): estimate=%d actual=%d relative error=%.4f, want <0.5", est, n, re)
	}
}

// TestHyperLogLogDuplicatesDoNotInflateEstimate re-adding the SAME values
// must not move the estimate — the defining idempotency property of a
// cardinality sketch (as opposed to a plain counter).
func TestHyperLogLogDuplicatesDoNotInflateEstimate(t *testing.T) {
	t.Parallel()
	h, err := NewHyperLogLog(DefaultHLLPrecision)
	if err != nil {
		t.Fatalf("NewHyperLogLog: %v", err)
	}
	values := seededDistinctStrings(4, 500)
	for _, s := range values {
		h.AddString(s)
	}
	first := h.Estimate()
	for i := 0; i < 3; i++ {
		for _, s := range values {
			h.AddString(s)
		}
	}
	second := h.Estimate()
	if first != second {
		t.Fatalf("re-adding identical values changed the estimate: %d -> %d", first, second)
	}
}

func TestHyperLogLogEmptyEstimatesZero(t *testing.T) {
	t.Parallel()
	h, err := NewHyperLogLog(DefaultHLLPrecision)
	if err != nil {
		t.Fatalf("NewHyperLogLog: %v", err)
	}
	if got := h.Estimate(); got != 0 {
		t.Fatalf("empty sketch estimate = %d, want 0", got)
	}
}

func TestHyperLogLogNilSafety(t *testing.T) {
	t.Parallel()
	var h *HyperLogLog
	h.AddString("x") // must not panic
	if got := h.Estimate(); got != 0 {
		t.Fatalf("nil Estimate = %d, want 0", got)
	}
	if got := h.Precision(); got != 0 {
		t.Fatalf("nil Precision = %d, want 0", got)
	}
	if got := h.Clone(); got != nil {
		t.Fatalf("nil Clone = %v, want nil", got)
	}
	if err := h.Merge(nil); err != nil {
		t.Fatalf("nil.Merge(nil) = %v, want nil", err)
	}
	other, _ := NewHyperLogLog(DefaultHLLPrecision)
	if err := h.Merge(other); err != nil {
		t.Fatalf("nil.Merge(other) = %v, want nil", err)
	}
}

func TestHyperLogLogInvalidPrecision(t *testing.T) {
	t.Parallel()
	for _, p := range []uint8{0, 1, 3, 19, 255} {
		if _, err := NewHyperLogLog(p); !errors.Is(err, ErrInvalidHLLPrecision) {
			t.Fatalf("precision %d: err=%v, want ErrInvalidHLLPrecision", p, err)
		}
	}
	// Boundary values are accepted.
	for _, p := range []uint8{minHLLPrecision, maxHLLPrecision} {
		if _, err := NewHyperLogLog(p); err != nil {
			t.Fatalf("precision %d should be accepted: %v", p, err)
		}
	}
}

// TestHyperLogLogSparseToDenseConversion asserts the sketch starts sparse
// (a handful of adds) and converts to dense once the sparse map would cost
// more than the full register array (sparseToDenseDivisor), and never
// reverts to sparse afterward.
func TestHyperLogLogSparseToDenseConversion(t *testing.T) {
	t.Parallel()
	h, err := NewHyperLogLog(DefaultHLLPrecision)
	if err != nil {
		t.Fatalf("NewHyperLogLog: %v", err)
	}
	h.AddString("only-one-value")
	if h.dense != nil {
		t.Fatalf("sketch should still be sparse after one add")
	}
	if len(h.sparse) == 0 {
		t.Fatalf("sparse map should hold at least one register after one add")
	}

	// m/sparseToDenseDivisor + 1 distinct values guarantees the conversion
	// threshold is crossed (every register index is likely distinct given
	// m=16384 buckets and far fewer draws).
	threshold := int(h.m)/sparseToDenseDivisor + 1
	for _, s := range seededDistinctStrings(5, threshold*2) {
		h.AddString(s)
		if h.dense != nil {
			break
		}
	}
	if h.dense == nil {
		t.Fatalf("sketch should have converted to dense by now")
	}
	if h.sparse != nil {
		t.Fatalf("sparse map should be nil once dense")
	}

	// Adding more values after conversion must not revert to sparse.
	h.AddString("after-dense-conversion")
	if h.dense == nil || h.sparse != nil {
		t.Fatalf("sketch reverted to sparse after conversion")
	}
}

func TestHyperLogLogMergeMismatchedPrecision(t *testing.T) {
	t.Parallel()
	a, _ := NewHyperLogLog(12)
	b, _ := NewHyperLogLog(14)
	if err := a.Merge(b); !errors.Is(err, ErrHLLPrecisionMismatch) {
		t.Fatalf("Merge mismatched precision = %v, want ErrHLLPrecisionMismatch", err)
	}
}

// TestHyperLogLogMergeEquivalence asserts that merging two sketches fed
// disjoint halves of a value set produces (approximately) the same estimate
// as one sketch fed the whole set — the defining correctness property of a
// register-max merge. Exercises both the sparse+sparse and dense+dense merge
// paths.
func TestHyperLogLogMergeEquivalence(t *testing.T) {
	t.Parallel()
	values := seededDistinctStrings(6, 20000)
	half := len(values) / 2

	whole, _ := NewHyperLogLog(DefaultHLLPrecision)
	for _, s := range values {
		whole.AddString(s)
	}

	partA, _ := NewHyperLogLog(DefaultHLLPrecision)
	for _, s := range values[:half] {
		partA.AddString(s)
	}
	partB, _ := NewHyperLogLog(DefaultHLLPrecision)
	for _, s := range values[half:] {
		partB.AddString(s)
	}
	if err := partA.Merge(partB); err != nil {
		t.Fatalf("Merge: %v", err)
	}

	wantEst := whole.Estimate()
	gotEst := partA.Estimate()
	if wantEst != gotEst {
		t.Fatalf("merged estimate = %d, want EXACTLY the whole-sketch estimate %d (register-max merge must reproduce the same registers)", gotEst, wantEst)
	}
}

// TestHyperLogLogMergeSparsePlusDense exercises the mixed sparse-merges-into
// path (other is dense, receiver is sparse) explicitly, since the
// dense/sparse branch inside Merge is otherwise only reached incidentally by
// TestHyperLogLogMergeEquivalence.
func TestHyperLogLogMergeSparsePlusDense(t *testing.T) {
	t.Parallel()
	sparse, _ := NewHyperLogLog(DefaultHLLPrecision)
	sparse.AddString("a-single-sparse-value")

	dense, _ := NewHyperLogLog(DefaultHLLPrecision)
	threshold := int(dense.m)/sparseToDenseDivisor + 1
	for _, s := range seededDistinctStrings(7, threshold*2) {
		dense.AddString(s)
		if dense.dense != nil {
			break
		}
	}
	if dense.dense == nil {
		t.Fatalf("setup: dense sketch failed to convert")
	}

	if err := sparse.Merge(dense); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if sparse.Estimate() < dense.Estimate() {
		t.Fatalf("merged estimate %d should be >= the dense-only estimate %d", sparse.Estimate(), dense.Estimate())
	}
}

func TestHyperLogLogClone(t *testing.T) {
	t.Parallel()
	h, _ := NewHyperLogLog(DefaultHLLPrecision)
	h.AddString("seed-value")
	clone := h.Clone()

	h.AddString("only-on-original")
	if len(clone.sparse) != 1 {
		t.Fatalf("clone should have exactly the 1 register from before divergence, got %d", len(clone.sparse))
	}
	if len(h.sparse) < len(clone.sparse) {
		t.Fatalf("original should have at least as many registers as the clone after adding more values")
	}

	// Clone a DENSE sketch too.
	threshold := int(h.m)/sparseToDenseDivisor + 1
	for _, s := range seededDistinctStrings(8, threshold*2) {
		h.AddString(s)
		if h.dense != nil {
			break
		}
	}
	if h.dense == nil {
		t.Fatalf("setup: failed to convert to dense")
	}
	denseClone := h.Clone()
	if denseClone.dense == nil {
		t.Fatalf("dense clone should also be dense")
	}
	// Mutating the clone's dense slice must not affect the original (deep copy).
	original := h.dense[0]
	denseClone.dense[0] = original + 1 // any value different from `original`
	if h.dense[0] != original {
		t.Fatalf("mutating the clone's register array changed the original: got %d, want unchanged %d", h.dense[0], original)
	}
}

func TestHyperLogLogPrecision(t *testing.T) {
	t.Parallel()
	h, _ := NewHyperLogLog(10)
	if got := h.Precision(); got != 10 {
		t.Fatalf("Precision() = %d, want 10", got)
	}
}

// TestHyperLogLogMergeSparsePlusSparse exercises the OTHER-IS-SPARSE branch
// of Merge (both TestHyperLogLogMergeEquivalence and
// TestHyperLogLogMergeSparsePlusDense only exercise the dense-other path at
// realistic scale).
func TestHyperLogLogMergeSparsePlusSparse(t *testing.T) {
	t.Parallel()
	a, _ := NewHyperLogLog(DefaultHLLPrecision)
	a.AddString("a-value")
	b, _ := NewHyperLogLog(DefaultHLLPrecision)
	b.AddString("b-value")

	if b.dense != nil {
		t.Fatalf("setup: b should still be sparse")
	}
	if err := a.Merge(b); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if len(a.sparse) != 2 {
		t.Fatalf("merged sparse register count = %d, want 2", len(a.sparse))
	}
}

// TestRankAllZeroRemainderCaps exercises rank's capping branch directly: a
// hash whose (64-precision) low bits are all zero would naturally compute
// bits.LeadingZeros64 == 64 without the cap; this hash is constructed so
// every bit below the top `precision` index bits is zero.
func TestRankAllZeroRemainderCaps(t *testing.T) {
	t.Parallel()
	const precision = 14
	// hash = all 1s in the top `precision` bits (so idx is nonzero/irrelevant
	// here), all 0s below — i.e. rem = hash<<precision == 0.
	hash := ^uint64(0) &^ (uint64(1)<<(64-precision) - 1)
	got := rank(hash, precision)
	want := uint8(64 - precision + 1)
	if got != want {
		t.Fatalf("rank(all-zero remainder) = %d, want %d (capped)", got, want)
	}
}

// TestAlphaForMSmallMValues exercises the three named special-case constants
// in alphaForM (m=16,32,64) — Estimate() at the library's DefaultHLLPrecision
// (14, m=16384) only ever reaches the asymptotic default branch.
func TestAlphaForMSmallMValues(t *testing.T) {
	t.Parallel()
	cases := []struct {
		precision uint8
		wantAlpha float64
	}{
		{4, 0.673}, // m=16
		{5, 0.697}, // m=32
		{6, 0.709}, // m=64
	}
	for _, tc := range cases {
		m := uint32(1) << tc.precision
		if got := alphaForM(m); got != tc.wantAlpha {
			t.Errorf("alphaForM(m=%d) = %v, want %v", m, got, tc.wantAlpha)
		}
		// Also exercise it end-to-end through Estimate() at this precision.
		h, err := NewHyperLogLog(tc.precision)
		if err != nil {
			t.Fatalf("NewHyperLogLog(%d): %v", tc.precision, err)
		}
		for _, s := range seededDistinctStrings(uint64(100+tc.precision), int(m)*2) {
			h.AddString(s)
		}
		if est := h.Estimate(); est <= 0 {
			t.Errorf("precision=%d Estimate() = %d, want > 0", tc.precision, est)
		}
	}
}
