package store

import "testing"

// TestOrdinalSetHas: bit k of word k/64; ordinal 0 never; past the set never.
func TestOrdinalSetHas(t *testing.T) {
	t.Parallel()
	set := []uint64{1<<0 | 1<<5, 1 << 1}
	for ord, want := range map[uint32]bool{0: false, 5: true, 6: false, 65: true, 64: false, 200: false} {
		if got := OrdinalSetHas(set, ord); got != want {
			t.Errorf("OrdinalSetHas(%d) = %v, want %v", ord, got, want)
		}
	}
	if OrdinalSetHas(nil, 1) {
		t.Error("nil set")
	}
}
