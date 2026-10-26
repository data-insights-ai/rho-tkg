package index

import (
	"math/rand/v2"
	"slices"
	"testing"
)

// TestIDSetAgreesWithMap drives an IDSet and a map through the same random
// adds and removes (ascending snowflake-like IDs mixed with older ones, so
// both the append path and the insert path run, and chunks split and merge)
// and checks every answer and the ascending order. Catches a lost or extra ID,
// a wrong Add/Remove result, a stale count and an unordered Ascend.
func TestIDSetAgreesWithMap(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewPCG(1, 2))
	var s IDSet
	m := map[int64]struct{}{}
	next := int64(1) << 42
	for i := range 20_000 {
		var id int64
		switch r := rng.IntN(10); {
		case r < 6: // a fresh ID above every other
			next += 1 + rng.Int64N(3)
			id = next
		case r < 8: // an older ID, present or not
			id = next - rng.Int64N(5000)
		default: // remove one
			id = next - rng.Int64N(5000)
			_, had := m[id]
			if got := s.Remove(id); got != had {
				t.Fatalf("step %d: Remove(%d) = %v, map had it %v", i, id, got, had)
			}
			delete(m, id)
			continue
		}
		_, had := m[id]
		if got := s.Add(id); got == had {
			t.Fatalf("step %d: Add(%d) = %v, map had it %v", i, id, got, had)
		}
		m[id] = struct{}{}
	}
	if s.Len() != len(m) {
		t.Fatalf("Len = %d, map %d", s.Len(), len(m))
	}
	want := make([]int64, 0, len(m))
	for id := range m {
		want = append(want, id)
	}
	slices.Sort(want)
	var got []int64
	s.Ascend(func(id int64) bool { got = append(got, id); return true })
	if !slices.Equal(got, want) {
		t.Fatalf("Ascend: %d IDs, want %d (or out of order)", len(got), len(want))
	}
	for _, id := range want[:100] {
		if !s.Has(id) || s.Has(id+1<<41) {
			t.Fatalf("Has(%d) wrong", id)
		}
	}
	built := NewIDSetFrom(want)
	if built.Len() != len(want) || !built.Has(want[len(want)/2]) {
		t.Fatal("NewIDSetFrom lost IDs")
	}
	n := 0
	built.Ascend(func(int64) bool { n++; return n < 10 })
	if n != 10 {
		t.Fatalf("Ascend did not stop when fn returned false: %d calls", n)
	}
}
