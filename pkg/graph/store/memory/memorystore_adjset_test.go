package memory

import (
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func adjSetIDs(s *adjSet) []types.RelID {
	var out []types.RelID
	for id := range s.all() {
		out = append(out, id)
	}
	slices.Sort(out)
	return out
}

// TestAdjSetMatchesAMapSet: a random sequence of adds (duplicates included)
// and removes (absent IDs included) leaves the adjacency set answering
// exactly like the map set it replaces, across the small-to-map switch.
func TestAdjSetMatchesAMapSet(t *testing.T) {
	for _, span := range []int{4, adjSetSmallMax, adjSetSmallMax + 1, 4 * adjSetSmallMax} {
		rng := rand.New(rand.NewPCG(uint64(span), 7)) // #nosec G404 -- deterministic test input
		var s *adjSet
		ref := map[types.RelID]struct{}{}
		for op := 0; op < 20*span; op++ {
			id := types.RelID(1 + rng.IntN(span+span/2))
			if rng.IntN(3) == 0 {
				_, want := ref[id]
				delete(ref, id)
				if got := s.remove(id); got != want {
					t.Fatalf("span %d op %d: remove(%d) = %t, want %t", span, op, id, got, want)
				}
			} else {
				if s == nil {
					s = &adjSet{}
				}
				ref[id] = struct{}{}
				s.add(id)
			}
			if s.len() != len(ref) {
				t.Fatalf("span %d op %d: len %d, want %d", span, op, s.len(), len(ref))
			}
			_, want := ref[id]
			if s.has(id) != want {
				t.Fatalf("span %d op %d: has(%d) = %t, want %t", span, op, id, s.has(id), want)
			}
		}
		want := make([]types.RelID, 0, len(ref))
		for id := range ref {
			want = append(want, id)
		}
		slices.Sort(want)
		if got := adjSetIDs(s); !slices.Equal(got, want) {
			t.Fatalf("span %d: ids %v, want %v", span, got, want)
		}
	}
}

func TestAdjSetNilAndEarlyStop(t *testing.T) {
	var s *adjSet
	if s.len() != 0 || s.has(1) || s.remove(1) || len(adjSetIDs(s)) != 0 {
		t.Fatal("nil adjacency set is not empty")
	}
	for _, n := range []int{3, adjSetSmallMax + 5} {
		s = newAdjSetOf()
		for i := 1; i <= n; i++ {
			s.add(types.RelID(i))
		}
		seen := 0
		for range s.all() {
			seen++
			if seen == 2 {
				break
			}
		}
		if seen != 2 {
			t.Fatalf("n=%d: iteration did not stop early", n)
		}
	}
	if got := adjSetIDs(newAdjSetOf(5, 3, 5)); !slices.Equal(got, []types.RelID{3, 5}) {
		t.Fatalf("newAdjSetOf dedupe: %v", got)
	}
}
