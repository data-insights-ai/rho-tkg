package index

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// TestRangeCardinality_VsBruteForce is the range-count soundness gate (R1):
// across random datasets (integers AND fractional floats, with removals) and
// random bounds (including fractional and open-ended, all inclusivity flags),
// RangeCardinality must equal the brute-force count — or decline (ok=false),
// in which case the caller scans. A single mismatch makes `count(p) WHERE
// p.k > x` return the wrong number.
func TestRangeCardinality_VsBruteForce(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(0xB51))

	for trial := 0; trial < 200; trial++ {
		t.Run(fmt.Sprintf("trial%d", trial), func(t *testing.T) {
			pi := NewPropertyIndex()
			n := 1 + rng.Intn(300)
			vals := make(map[snowflake.ID]float64, n)
			frac := rng.Intn(2) == 0 // half the trials mix in fractional values
			for i := 0; i < n; i++ {
				id := snowflake.ID(int64(1000 + i))
				var vk string
				var v float64
				if frac && rng.Intn(3) == 0 {
					v = float64(rng.Intn(200)-50) + 0.5
					vk = fmt.Sprintf("f64:%v", v)
				} else {
					iv := int64(rng.Intn(200) - 50)
					v = float64(iv)
					vk = fmt.Sprintf("i64:%d", iv)
				}
				pi.AddKey(id, vk)
				vals[id] = v
			}

			// Random removals to exercise bucket deletion.
			for id := range vals {
				if rng.Intn(4) == 0 {
					v := vals[id]
					var vk string
					if v == math.Trunc(v) {
						vk = fmt.Sprintf("i64:%d", int64(v))
					} else {
						vk = fmt.Sprintf("f64:%v", v)
					}
					pi.removeKey(id, vk)
					delete(vals, id)
				}
			}

			check := func(min, max float64, inclMin, inclMax bool) {
				got, ok := pi.RangeCardinality(min, max, inclMin, inclMax)
				if !ok {
					return
				}
				var want int64
				for _, v := range vals {
					geMin := (inclMin && v >= min) || (!inclMin && v > min)
					leMax := (inclMax && v <= max) || (!inclMax && v < max)
					if geMin && leMax {
						want++
					}
				}
				if got != want {
					t.Fatalf("range (%.1f,%.1f] incl(%v,%v): got %d want %d", min, max, inclMin, inclMax, got, want)
				}
			}

			bounds := []float64{math.Inf(-1), -60, -50.5, -10, 0, 37, 37.5, 96, 100, 150, math.Inf(1)}
			for _, lo := range bounds {
				for _, hi := range bounds {
					for _, im := range []bool{true, false} {
						for _, ix := range []bool{true, false} {
							check(lo, hi, im, ix)
						}
					}
				}
			}
		})
	}
}

// TestRangeCardinality_DeclinesOnLargeInt verifies an integer past 2^53 — whose
// float64 sort key can collide with a neighbour — poisons the count so it
// declines and the caller falls back to the exact-recheck scan.
func TestRangeCardinality_DeclinesOnLargeInt(t *testing.T) {
	t.Parallel()
	pi := NewPropertyIndex()
	pi.AddKey(snowflake.ID(1), "i64:10")
	if _, ok := pi.RangeCardinality(0, 100, true, true); !ok {
		t.Fatal("expected count usable for small ints")
	}
	pi.AddKey(snowflake.ID(2), fmt.Sprintf("i64:%d", (int64(1)<<53)+1)) // > 2^53
	if _, ok := pi.RangeCardinality(0, math.Inf(1), true, true); ok {
		t.Fatal("expected decline after a >2^53 integer (float64 key collision risk)")
	}
}

// TestRangeCardinality_RecoversAfterPoisoningValueRemoved is the BACKLOG 16j
// regression: numImpreciseCount is a COUNT, not a one-way sticky flag — once
// every >2^53 value is removed (or replaced via remove-then-add, the update
// pattern), RangeCardinality must re-enable rather than staying permanently
// disabled for the index's whole remaining lifetime.
func TestRangeCardinality_RecoversAfterPoisoningValueRemoved(t *testing.T) {
	t.Parallel()
	pi := NewPropertyIndex()
	pi.AddKey(snowflake.ID(1), "i64:10")
	poison := fmt.Sprintf("i64:%d", (int64(1)<<53)+1) // > 2^53
	pi.AddKey(snowflake.ID(2), poison)
	if _, ok := pi.RangeCardinality(0, math.Inf(1), true, true); ok {
		t.Fatal("expected decline while the poisoning value is indexed")
	}

	pi.removeKey(snowflake.ID(2), poison)

	count, ok := pi.RangeCardinality(0, math.Inf(1), true, true)
	if !ok {
		t.Fatal("RangeCardinality still declines after the poisoning value was removed — BACKLOG 16j regression")
	}
	if count != 1 {
		t.Fatalf("RangeCardinality = %d, want 1 (only the remaining non-poisoning value)", count)
	}
}

// TestRangeCardinality_StaysDeclinedWhileAnyPoisoningValueRemains proves the
// count tracks MULTIPLE poisoning values correctly — removing one of two must
// NOT re-enable RangeCardinality while the other is still indexed.
func TestRangeCardinality_StaysDeclinedWhileAnyPoisoningValueRemains(t *testing.T) {
	t.Parallel()
	pi := NewPropertyIndex()
	poisonA := fmt.Sprintf("i64:%d", (int64(1)<<53)+1)
	poisonB := fmt.Sprintf("i64:%d", (int64(1)<<53)+2)
	pi.AddKey(snowflake.ID(1), poisonA)
	pi.AddKey(snowflake.ID(2), poisonB)
	if _, ok := pi.RangeCardinality(0, math.Inf(1), true, true); ok {
		t.Fatal("expected decline with two poisoning values indexed")
	}

	pi.removeKey(snowflake.ID(1), poisonA)
	if _, ok := pi.RangeCardinality(0, math.Inf(1), true, true); ok {
		t.Fatal("RangeCardinality re-enabled after removing only ONE of two poisoning values — BACKLOG 16j regression")
	}

	pi.removeKey(snowflake.ID(2), poisonB)
	if _, ok := pi.RangeCardinality(0, math.Inf(1), true, true); !ok {
		t.Fatal("RangeCardinality still declines after removing BOTH poisoning values")
	}
}

// TestRangeCardinality_FractionalValuesAndBounds pins that fractional values and
// fractional bounds count exactly (the sorted-bucket sum needs no integer gate).
func TestRangeCardinality_FractionalValuesAndBounds(t *testing.T) {
	t.Parallel()
	pi := NewPropertyIndex()
	for i, v := range []string{"f64:1.5", "f64:2.5", "f64:2.5", "i64:3", "f64:3.5"} {
		pi.AddKey(snowflake.ID(int64(i+1)), v)
	}
	// values: {1.5, 2.5, 2.5, 3, 3.5}
	if c, ok := pi.RangeCardinality(2.5, math.Inf(1), true, true); !ok || c != 4 {
		t.Fatalf("v>=2.5: got %d ok=%v, want 4", c, ok)
	}
	if c, ok := pi.RangeCardinality(2.5, math.Inf(1), false, true); !ok || c != 2 {
		t.Fatalf("v>2.5: got %d ok=%v, want 2", c, ok)
	}
	if c, ok := pi.RangeCardinality(2.0, 3.4, true, true); !ok || c != 3 {
		t.Fatalf("2.0<=v<=3.4: got %d ok=%v, want 3", c, ok)
	}
}
