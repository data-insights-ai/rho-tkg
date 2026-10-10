package index

import (
	"fmt"
	"math"
	"math/rand"
	"slices"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// Round 4 R2: RangeCardinality answers from the ordered view's per-key counts
// and their per-chunk prefix sums instead of visiting each distinct value in
// the range. These tests compare it against iteration over every indexed
// entry on indexes large enough to span many chunks, under churn that splits,
// merges and drains chunks, with re-adds of a present entry and the
// corruption-path purge.

// rangeCountByIteration is the oracle: every live (id, value) visited.
func rangeCountByIteration(vals map[snowflake.ID]float64, min, max float64, inclMin, inclMax bool) int64 {
	var n int64
	for _, v := range vals {
		geMin := (inclMin && v >= min) || (!inclMin && v > min)
		leMax := (inclMax && v <= max) || (!inclMax && v < max)
		if geMin && leMax {
			n++
		}
	}
	return n
}

func numericVK(v float64) string {
	if v == math.Trunc(v) && math.Abs(v) < 1<<53 {
		return fmt.Sprintf("i64:%d", int64(v))
	}
	return fmt.Sprintf("f64:%v", v)
}

// assertCountsConsistent checks the counted view's invariants: each key's
// count is its bucket's size, each chunk total is the sum of its counts, and
// the prefix sums agree with the totals.
func assertCountsConsistent(t *testing.T, pi *PropertyIndex) {
	t.Helper()
	o := &pi.numKeys
	if len(o.chunks) != len(o.cnt) || len(o.chunks) != len(o.tot) {
		t.Fatalf("%d chunks, %d count rows, %d totals", len(o.chunks), len(o.cnt), len(o.tot))
	}
	var running int64
	for ci, c := range o.chunks {
		if len(c) != len(o.cnt[ci]) {
			t.Fatalf("chunk %d: %d keys, %d counts", ci, len(c), len(o.cnt[ci]))
		}
		var sum int64
		for j, k := range c {
			if got, want := o.cnt[ci][j], int64(len(pi.numBuckets[k])); got != want || got <= 0 {
				t.Fatalf("key %v: count %d, bucket %d", k, got, want)
			}
			sum += o.cnt[ci][j]
		}
		if o.tot[ci] != sum {
			t.Fatalf("chunk %d: total %d, counts sum to %d", ci, o.tot[ci], sum)
		}
		if got := o.prefixTotal(ci); got != running {
			t.Fatalf("chunk %d: prefix sum %d, want %d", ci, got, running)
		}
		running += sum
	}
	if got := o.prefixTotal(len(o.chunks)); got != running {
		t.Fatalf("whole prefix sum %d, want %d", got, running)
	}
	if len(pi.numBuckets) != o.n {
		t.Fatalf("%d buckets, %d keys", len(pi.numBuckets), o.n)
	}
}

// Many chunks (up to ~6,000 distinct values, chunk cap 1,024), duplicates per
// value, removals that drain values and chunks, re-adds of present entries
// (must not double count), value moves, the purge path, and bounds that fall
// on keys, between keys, inside one chunk, across chunk boundaries, outside
// the data, reversed, equal, infinite and NaN.
func TestRangeCardinality_PrefixSumsVsIteration(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(0x5EED4)) //nolint:gosec // deterministic test
	maxChunks := 0
	defer func() {
		if maxChunks < 4 {
			t.Fatalf("the largest index spanned %d chunks; the test must cross chunk boundaries", maxChunks)
		}
	}()
	for trial := range 12 {
		t.Run(fmt.Sprintf("trial%d", trial), func(t *testing.T) {
			pi := NewPropertyIndexWith(true)
			vals := map[snowflake.ID]float64{}
			universe := 50 + rng.Intn(6000)
			next := snowflake.ID(1)
			value := func() float64 {
				v := float64(rng.Intn(universe) - universe/4)
				if rng.Intn(5) == 0 {
					v += 0.25
				}
				return v
			}
			check := func(step string) {
				t.Helper()
				assertCountsConsistent(t, pi)
				bounds := []float64{math.Inf(-1), math.Inf(1), math.NaN(), -float64(universe), float64(universe) * 2}
				for range 40 {
					bounds = append(bounds, value(), value()+0.5)
				}
				for i := range 400 {
					lo := bounds[rng.Intn(len(bounds))]
					hi := bounds[rng.Intn(len(bounds))]
					if i%7 == 0 {
						hi = lo
					}
					im, ix := rng.Intn(2) == 0, rng.Intn(2) == 0
					got, ok := pi.RangeCardinality(lo, hi, im, ix)
					if !ok {
						t.Fatalf("%s: declined without an imprecise value", step)
					}
					if want := rangeCountByIteration(vals, lo, hi, im, ix); got != want {
						t.Fatalf("%s: [%v,%v] incl(%v,%v): got %d want %d", step, lo, hi, im, ix, got, want)
					}
				}
			}
			for range 3 * universe {
				v := value()
				pi.AddKey(next, numericVK(v))
				vals[next] = v
				next++
			}
			maxChunks = max(maxChunks, len(pi.numKeys.chunks))
			check("loaded")
			for id, v := range vals { // re-adding a present entry changes nothing
				if rng.Intn(10) == 0 {
					pi.AddKey(id, numericVK(v))
				}
			}
			check("re-added")
			for id, v := range vals {
				switch rng.Intn(4) {
				case 0: // delete
					pi.removeKey(id, numericVK(v))
					delete(vals, id)
				case 1: // move to another value
					nv := value()
					pi.removeKey(id, numericVK(v))
					pi.AddKey(id, numericVK(nv))
					vals[id] = nv
				}
			}
			check("churned")
			pi.removeKey(next+1, numericVK(1)) // removing an absent entry changes nothing
			check("absent removal")
			indexes := map[PropertyIndexKey]*PropertyIndex{{PropertyKey: "p"}: pi}
			n := 0
			for id := range vals {
				if n++; n > 50 {
					break
				}
				PurgeNodeFromAllPropertyIndexes(indexes, id)
				delete(vals, id)
			}
			check("purged")
			for id, v := range vals { // drain everything above the median of the universe
				if v > float64(universe)/4 {
					pi.removeKey(id, numericVK(v))
					delete(vals, id)
				}
			}
			check("drained")
		})
	}
}

// A NaN bound counts 0 on a counted index too.
func TestRangeCardinality_CountedNaNBoundCountsNothing(t *testing.T) {
	t.Parallel()
	pi := NewPropertyIndexWith(true)
	for i := range 10 {
		pi.AddKey(snowflake.ID(i+1), numericVK(float64(i)))
	}
	nan := math.NaN()
	for _, c := range []struct{ lo, hi float64 }{{nan, 5}, {0, nan}, {nan, nan}, {math.Inf(-1), nan}} {
		for _, im := range []bool{true, false} {
			for _, ix := range []bool{true, false} {
				got, ok := pi.RangeCardinality(c.lo, c.hi, im, ix)
				if !ok || got != 0 {
					t.Fatalf("[%v,%v] incl(%v,%v) = %d, %v; want 0, true", c.lo, c.hi, im, ix, got, ok)
				}
			}
		}
	}
}

// A chunk drained to empty while both neighbours are too full to merge with
// it leaves the directory by slot deletion, not by a merge: the prefix sums of
// every chunk after it must move down with it.
func TestCountedChunks_DrainedChunkBetweenFullNeighbours(t *testing.T) {
	t.Parallel()
	var o sortedChunks[float64]
	model := map[float64]int64{}
	add := func(k float64, d int64) {
		o.addCount(k, d)
		model[k] += d
		if model[k] <= 0 {
			delete(model, k)
		}
	}
	check := func(step string) {
		t.Helper()
		for _, b := range [][2]float64{{-1, 1e9}, {0, 600}, {300, 1200}, {700, 2000}, {1100, 1e9}, {900, 1030}, {512.5, 512.5}} {
			var want int64
			for k, c := range model {
				if k >= b[0] && k <= b[1] {
					want += c
				}
			}
			if got := o.countRange(b[0], b[1], true, true); got != want {
				t.Fatalf("%s: [%v,%v] = %d, want %d (chunks %d)", step, b[0], b[1], got, want, len(o.chunks))
			}
		}
	}
	for k := range 2048 { // sequential appends: chunks of 512, 512 and 1,024
		add(float64(k), int64(k%3+1))
	}
	if len(o.chunks) != 3 {
		t.Fatalf("setup: %d chunks, want 3", len(o.chunks))
	}
	// Fill the outer chunks to the split cap (no split below it) so the middle one cannot
	// merge into either of them.
	for _, ci := range []int{0, 2} {
		base := o.chunks[ci][0]
		for j := 0; len(o.chunks[ci]) < 2*orderedKeyChunk; j++ {
			add(base+float64(j+1)/2048, 2) // between the chunk's first two keys
		}
	}
	if len(o.chunks) != 3 {
		t.Fatalf("setup after filling: %d chunks, want 3", len(o.chunks))
	}
	check("filled")
	middle := slices.Clone(o.chunks[1])
	for i, k := range middle {
		add(k, -model[k])
		if i%64 == 0 {
			check(fmt.Sprintf("drain %d", i))
		}
	}
	if len(o.chunks) != 2 {
		t.Fatalf("after the drain: %d chunks, want 2 (the middle one deleted, no merge)", len(o.chunks))
	}
	check("drained")
	add(-5, 7) // and a later count change still lands on the right chunk
	add(1e6, 3)
	check("after")
}

// An index created without range counts keeps the set-mode ordered view: no
// counts, totals or prefix sums after loads, re-adds, moves, removals and the
// purge, and the walk gives the same answers as a counted twin fed the same
// operations.
func TestRangeCardinality_PlainIndexKeepsNoCounts(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(0xC0)) //nolint:gosec // deterministic test
	plain, counted := NewPropertyIndex(), NewPropertyIndexWith(true)
	if plain.RangeCounts() || !counted.RangeCounts() || (*PropertyIndex)(nil).RangeCounts() {
		t.Fatal("RangeCounts does not report the creation option")
	}
	vals := map[snowflake.ID]float64{}
	both := func(f func(*PropertyIndex)) { f(plain); f(counted) }
	for i := range 5000 {
		id, v := snowflake.ID(i+1), float64(rng.Intn(3000))
		both(func(pi *PropertyIndex) { pi.AddKey(id, numericVK(v)) })
		vals[id] = v
	}
	for id, v := range vals {
		switch rng.Intn(5) {
		case 0:
			both(func(pi *PropertyIndex) { pi.removeKey(id, numericVK(v)) })
			delete(vals, id)
		case 1:
			both(func(pi *PropertyIndex) { pi.AddKey(id, numericVK(v)) })
		case 2:
			both(func(pi *PropertyIndex) {
				PurgeNodeFromAllPropertyIndexes(map[PropertyIndexKey]*PropertyIndex{{PropertyKey: "p"}: pi}, id)
			})
			delete(vals, id)
		}
	}
	o := &plain.numKeys
	if o.counted || o.cnt != nil || o.tot != nil || o.fen != nil {
		t.Fatalf("a plain index grew counts: counted=%v cnt=%d tot=%d fen=%d", o.counted, len(o.cnt), len(o.tot), len(o.fen))
	}
	assertCountsConsistent(t, counted)
	for range 500 {
		lo, hi := float64(rng.Intn(3200)-100), float64(rng.Intn(3200)-100)
		im, ix := rng.Intn(2) == 0, rng.Intn(2) == 0
		p, pok := plain.RangeCardinality(lo, hi, im, ix)
		c, cok := counted.RangeCardinality(lo, hi, im, ix)
		want := rangeCountByIteration(vals, lo, hi, im, ix)
		if !pok || !cok || p != want || c != want {
			t.Fatalf("[%v,%v] incl(%v,%v): plain %d %v, counted %d %v, iteration %d", lo, hi, im, ix, p, pok, c, cok, want)
		}
	}
}
