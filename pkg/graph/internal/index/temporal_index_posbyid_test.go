package index

import (
	"math/rand"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 16h: Extend's O(n) linear scan (to find whether id already has an
// entry) was replaced with an O(1) posByID lookup. Unlike byID (bounds,
// sort-invariant), posByID tracks slice POSITION, which every sort and
// Remove's filter-copy invalidates — so this correctness-critical bookkeeping
// needed its own randomized oracle test interleaving ALL FOUR mutators
// (Add, Extend, AddKnownAbsent, Remove) against a brute-force reference,
// mirroring TestTemporalIndex_AugmentEquivalence_WithRemovals but adding
// Extend to the mix (the existing oracle tests never called Extend at all).

// TestTemporalIndex_PosByIDOracle_ExtendAddRemoveInterleaved drives many
// randomized sequences of Add / Extend / AddKnownAbsent / Remove against a
// brute-force reference model, verifying QueryAt/QueryOverlap results match
// after every operation — not just at the end. A stale posByID entry would
// manifest as Extend silently corrupting the WRONG entry (in-place mutation
// at a stale index) rather than the intended one, which only shows up as a
// query returning wrong bounds/membership — exactly what this oracle checks.
func TestTemporalIndex_PosByIDOracle_ExtendAddRemoveInterleaved(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(0xB105F00D))

	for trial := 0; trial < 300; trial++ {
		ti := NewTemporalIndex()
		live := make(map[snowflake.ID]IntervalEntry)
		nextID := snowflake.ID(1)

		ops := rng.Intn(100) + 1
		for op := 0; op < ops; op++ {
			roll := rng.Intn(10)
			switch {
			case roll < 3 && len(live) > 0:
				// Extend a random EXISTING id — the posByID lookup path.
				victim := pickRandomLive(rng, live)
				from := types.Instant(rng.Intn(50))
				var to types.Instant
				if rng.Intn(3) != 0 {
					to = from + types.Instant(rng.Intn(30))
				}
				ti.Extend(victim, from, to)
				prev := live[victim]
				newFrom := prev.From
				if from < newFrom {
					newFrom = from
				}
				live[victim] = IntervalEntry{From: newFrom, To: unionTo(prev.To, to), ID: victim}
			case roll < 5:
				// Extend a FRESH id — the posByID "not found, append" path.
				id := nextID
				nextID++
				from := types.Instant(rng.Intn(50))
				var to types.Instant
				if rng.Intn(3) != 0 {
					to = from + types.Instant(rng.Intn(30))
				}
				ti.Extend(id, from, to)
				live[id] = IntervalEntry{From: from, To: to, ID: id}
			case roll < 7 && len(live) > 0:
				// Remove a random live id.
				victim := pickRandomLive(rng, live)
				ti.Remove(victim)
				delete(live, victim)
			default:
				// Add (replace semantics) — fresh or existing id.
				var id snowflake.ID
				if len(live) > 0 && rng.Intn(2) == 0 {
					id = pickRandomLive(rng, live)
				} else {
					id = nextID
					nextID++
				}
				from := types.Instant(rng.Intn(50))
				var to types.Instant
				if rng.Intn(3) != 0 {
					to = from + types.Instant(rng.Intn(30))
				}
				ti.Add(id, from, to)
				live[id] = IntervalEntry{From: from, To: to, ID: id}
			}

			ref := make([]IntervalEntry, 0, len(live))
			for _, e := range live {
				ref = append(ref, e)
			}

			// EnvelopeOf reads byID directly (never sortIfDirty), so it is
			// unaffected by whether a query has run since the last mutation
			// — safe to check unconditionally, every op. It reads byID, not
			// posByID, so this also catches any accidental cross-
			// contamination between the two maps.
			for id, want := range live {
				gotFrom, gotTo, ok := ti.EnvelopeOf(id)
				if !ok || gotFrom != want.From || gotTo != want.To {
					t.Fatalf("trial %d op %d: EnvelopeOf(%d) = (%d,%d,%v), want (%d,%d,true)",
						trial, op, id, gotFrom, gotTo, ok, want.From, want.To)
				}
			}

			// Query only ~40% of the time, deliberately — QueryAt/QueryOverlap
			// call sortIfDirty, which fully REBUILDS posByID after every sort
			// and would mask a bug where Extend's posByID lookup used a STALE
			// position left over from an earlier Remove/sort that a mutator
			// forgot to account for. Querying after every single op (as an
			// earlier version of this test did) never exercises that window:
			// bursts of 2+ mutations with no intervening query are what let a
			// stale posByID entry actually get READ by Extend before the next
			// sortIfDirty has a chance to repair it.
			if rng.Intn(10) >= 4 {
				continue
			}
			for probe := 0; probe < 5; probe++ {
				tp := types.Instant(rng.Intn(60) - 5)
				if got, want := ti.QueryAt(tp), refQueryAt(ref, tp); !eqIDs(got, want) {
					t.Fatalf("trial %d op %d: QueryAt(%d) = %v, want %v", trial, op, tp, got, want)
				}
			}
			a := types.Instant(rng.Intn(60) - 5)
			b := types.Instant(rng.Intn(60) - 5)
			if got, want := ti.QueryOverlap(a, b), refQueryOverlap(ref, a, b); !eqIDs(got, want) {
				t.Fatalf("trial %d op %d: QueryOverlap(%d,%d) = %v, want %v", trial, op, a, b, got, want)
			}
		}
		// Always verify the final state, regardless of the last op's query roll.
		ref := make([]IntervalEntry, 0, len(live))
		for _, e := range live {
			ref = append(ref, e)
		}
		for probe := 0; probe < 10; probe++ {
			tp := types.Instant(rng.Intn(60) - 5)
			if got, want := ti.QueryAt(tp), refQueryAt(ref, tp); !eqIDs(got, want) {
				t.Fatalf("trial %d final: QueryAt(%d) = %v, want %v", trial, tp, got, want)
			}
		}
	}
}

func pickRandomLive(rng *rand.Rand, live map[snowflake.ID]IntervalEntry) snowflake.ID {
	pick := rng.Intn(len(live))
	i := 0
	for id := range live {
		if i == pick {
			return id
		}
		i++
	}
	panic("unreachable")
}
