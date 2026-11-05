package index

import (
	"math/rand"
	"slices"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestTemporalIndexUnqueriedKeepsOnlyEnvelopes (RELTEMPID): an index read only
// by EnvelopeOf holds byID alone, whatever the mutators; the first stabbing
// query builds the sorted entries from it and answers as a reference does.
func TestTemporalIndexUnqueriedKeepsOnlyEnvelopes(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(7))
	ti := NewTemporalIndex()
	ref := map[snowflake.ID]IntervalEntry{}
	for i := 0; i < 2000; i++ {
		id := snowflake.ID(1 + rng.Intn(400))
		from := types.Instant(rng.Intn(1000))
		to := types.Instant(0)
		if rng.Intn(3) > 0 {
			to = from + types.Instant(1+rng.Intn(300))
		}
		switch rng.Intn(4) {
		case 0:
			ti.Add(id, from, to)
			ref[id] = IntervalEntry{From: from, To: to, ID: id}
		case 1:
			ti.Remove(id)
			delete(ref, id)
		default:
			ti.Extend(id, from, to)
			if e, ok := ref[id]; ok {
				from, to = min(from, e.From), unionTo(e.To, to)
			}
			ref[id] = IntervalEntry{From: from, To: to, ID: id}
		}
		if f, to2, ok := ti.EnvelopeOf(id); ok != (ref[id] != IntervalEntry{}) || f != ref[id].From || to2 != ref[id].To {
			t.Fatalf("op %d: EnvelopeOf(%d) = (%d,%d,%v), want %+v", i, id, f, to2, ok, ref[id])
		}
	}
	if ti.Entries != nil || ti.subMax != nil || ti.posByID != nil {
		t.Fatalf("unqueried index built its stabbing structure: %d entries, %d subMax, %d positions",
			len(ti.Entries), len(ti.subMax), len(ti.posByID))
	}
	if ti.Len() != len(ref) {
		t.Fatalf("Len = %d, want %d", ti.Len(), len(ref))
	}
	var seen int
	ti.ForEachEntry(func(e IntervalEntry) {
		seen++
		if ref[e.ID] != e {
			t.Fatalf("ForEachEntry %+v, want %+v", e, ref[e.ID])
		}
	})
	if seen != len(ref) {
		t.Fatalf("ForEachEntry saw %d, want %d", seen, len(ref))
	}
	entries := make([]IntervalEntry, 0, len(ref))
	for _, e := range ref {
		entries = append(entries, e)
	}
	for _, at := range []types.Instant{0, 1, 250, 999, 1400} {
		want := refQueryAt(entries, at)
		slices.Sort(want)
		if got := ti.QueryAt(at); !eqIDs(got, want) {
			t.Fatalf("QueryAt(%d) = %v, want %v", at, got, want)
		}
	}
	if got, want := ti.QueryOverlap(100, 400), refQueryOverlap(entries, 100, 400); !eqIDs(got, sorted(want)) {
		t.Fatalf("QueryOverlap = %v, want %v", got, want)
	}
	// Queried, the mutators keep the stabbing structure as before.
	ti.Extend(snowflake.ID(9999), 5, 6)
	if got := ti.QueryAt(5); !slices.Contains(got, snowflake.ID(9999)) {
		t.Fatalf("Extend after the first query is not stabbed: %v", got)
	}
}

func sorted(ids []snowflake.ID) []snowflake.ID {
	slices.Sort(ids)
	return ids
}
