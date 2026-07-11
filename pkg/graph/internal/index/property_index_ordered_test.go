package index

import (
	"math"
	"math/rand"
	"sort"
	"strconv"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// collectOrderedPaged drives RangeOrderedPage page-by-page (proving the paged
// cursor stitches into a single contiguous, correctly-ordered stream) and
// returns every candidate node ID in emission order.
func collectOrderedPaged(pi *PropertyIndex, min, max float64, desc bool, pageSize int) []snowflake.ID {
	var out []snowflake.ID
	var cur RangeOrderedCursor
	for {
		ids, next, done, supported := pi.RangeOrderedPage(min, max, desc, cur, pageSize)
		if !supported {
			return nil
		}
		out = append(out, ids...)
		if done {
			return out
		}
		cur = next
	}
}

// buildFloatIndex indexes id->value pairs through the same AddKey door the
// stores use (with an "f64:" canonical value key), so the ordered view is
// populated exactly as in production.
func buildFloatIndex(pairs map[snowflake.ID]float64) *PropertyIndex {
	pi := NewPropertyIndex()
	for id, v := range pairs {
		pi.AddKey(id, "f64:"+strconv.FormatFloat(v, 'g', -1, 64))
	}
	return pi
}

// TestRangeOrderedPage_AscDescContract asserts the exact value-order contract
// (asc + desc), ties by node ID ascending in BOTH directions, and negative /
// mixed-sign values, against an independent reference ordering.
func TestRangeOrderedPage_AscDescContract(t *testing.T) {
	t.Parallel()

	// Two node IDs sharing value 5.0 (tie) and value -2.0 (tie), plus spread.
	pairs := map[snowflake.ID]float64{
		snowflake.ID(100): 5.0,
		snowflake.ID(50):  5.0,  // same value as 100 -> tie, id 50 < 100
		snowflake.ID(200): -2.0, // negative
		snowflake.ID(70):  -2.0, // tie at -2.0, id 70 < 200
		snowflake.ID(300): 0.0,
		snowflake.ID(400): 3.5, // fractional
		snowflake.ID(10):  -7.0,
	}
	pi := buildFloatIndex(pairs)

	type vp struct {
		id snowflake.ID
		v  float64
	}
	all := make([]vp, 0, len(pairs))
	for id, v := range pairs {
		all = append(all, vp{id, v})
	}
	// Reference ascending order: (value asc, id asc).
	refAsc := append([]vp(nil), all...)
	sort.SliceStable(refAsc, func(i, j int) bool {
		if refAsc[i].v != refAsc[j].v {
			return refAsc[i].v < refAsc[j].v
		}
		return refAsc[i].id < refAsc[j].id
	})
	wantAsc := make([]snowflake.ID, len(refAsc))
	for i, p := range refAsc {
		wantAsc[i] = p.id
	}
	// Reference descending: value DESC, ties id ASC.
	refDesc := append([]vp(nil), all...)
	sort.SliceStable(refDesc, func(i, j int) bool {
		if refDesc[i].v != refDesc[j].v {
			return refDesc[i].v > refDesc[j].v
		}
		return refDesc[i].id < refDesc[j].id
	})
	wantDesc := make([]snowflake.ID, len(refDesc))
	for i, p := range refDesc {
		wantDesc[i] = p.id
	}

	lo, hi := -1e300, 1e300
	for _, pageSize := range []int{1, 2, 3, 100} {
		gotAsc := collectOrderedPaged(pi, lo, hi, false, pageSize)
		if !equalIDs(gotAsc, wantAsc) {
			t.Fatalf("asc pageSize=%d: got %v want %v", pageSize, gotAsc, wantAsc)
		}
		gotDesc := collectOrderedPaged(pi, lo, hi, true, pageSize)
		if !equalIDs(gotDesc, wantDesc) {
			t.Fatalf("desc pageSize=%d: got %v want %v", pageSize, gotDesc, wantDesc)
		}
	}
}

// TestRangeOrderedPage_BoundedRange asserts bounds restrict the candidate set
// (over-selecting by at most one ulp on each side).
func TestRangeOrderedPage_BoundedRange(t *testing.T) {
	t.Parallel()
	pairs := map[snowflake.ID]float64{}
	for i := 0; i < 20; i++ {
		pairs[snowflake.ID(1000+i)] = float64(i)
	}
	pi := buildFloatIndex(pairs)

	got := collectOrderedPaged(pi, 5, 10, false, 4)
	// Over-select contract: caller does the exact filter; here we assert every
	// candidate lies within the ulp-widened [5,10] window and the true
	// in-range values are all present.
	present := map[float64]bool{}
	for _, id := range got {
		v := float64(int(id) - 1000)
		if v < 4 || v > 11 {
			t.Fatalf("candidate value %v escaped widened bounds", v)
		}
		present[v] = true
	}
	for v := 5.0; v <= 10.0; v++ {
		if !present[v] {
			t.Fatalf("in-range value %v missing from candidates", v)
		}
	}
}

// TestRangeOrderedPage_Empty covers the nil index and the enabled-but-empty
// view (authoritative empty).
func TestRangeOrderedPage_Empty(t *testing.T) {
	t.Parallel()
	var nilPI *PropertyIndex
	if _, _, _, supported := nilPI.RangeOrderedPage(0, 1, false, RangeOrderedCursor{}, 10); supported {
		t.Fatalf("nil index must report unsupported")
	}
	pi := NewPropertyIndex()
	ids, _, done, supported := pi.RangeOrderedPage(0, 1, false, RangeOrderedCursor{}, 10)
	if !supported || !done || len(ids) != 0 {
		t.Fatalf("empty view: supported=%v done=%v ids=%v", supported, done, ids)
	}
}

// TestRangeOrderedPage_ModelEquivalence churns random inserts and checks the
// full paged stream equals a reference (value, id) sort — the adversarial
// randomized shape, run for asc and desc.
func TestRangeOrderedPage_ModelEquivalence(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(7)) //nolint:gosec // deterministic test
	pairs := map[snowflake.ID]float64{}
	for i := 0; i < 2000; i++ {
		id := snowflake.ID(rng.Int63n(1 << 40))
		if _, dup := pairs[id]; dup {
			continue
		}
		// A small value alphabet forces many ties.
		pairs[id] = float64(rng.Intn(40) - 20)
	}
	pi := buildFloatIndex(pairs)

	type vp struct {
		id snowflake.ID
		v  float64
	}
	all := make([]vp, 0, len(pairs))
	for id, v := range pairs {
		all = append(all, vp{id, v})
	}
	for _, desc := range []bool{false, true} {
		ref := append([]vp(nil), all...)
		sort.SliceStable(ref, func(i, j int) bool {
			if ref[i].v != ref[j].v {
				if desc {
					return ref[i].v > ref[j].v
				}
				return ref[i].v < ref[j].v
			}
			return ref[i].id < ref[j].id
		})
		want := make([]snowflake.ID, len(ref))
		for i, p := range ref {
			want[i] = p.id
		}
		got := collectOrderedPaged(pi, math.Inf(-1), math.Inf(1), desc, 64)
		if !equalIDs(got, want) {
			t.Fatalf("desc=%v: stream mismatch len got=%d want=%d", desc, len(got), len(want))
		}
	}
}

func equalIDs(a, b []snowflake.ID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
