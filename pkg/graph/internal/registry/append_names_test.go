package registry

import (
	"fmt"
	"slices"
	"testing"
)

// appendable is the common surface AppendNames is tested through, so the three
// registries (Label / RelType / PropertyKey) get strict parity coverage.
type appendable interface {
	AppendNames(prefix, suffix []string) (bool, error)
	ExportNames() []string
	Len() int
	GetOrCreate(name string) (uint16, error)
	Lookup(name string) (uint16, bool)
}

func appendables() map[string]func() appendable {
	return map[string]func() appendable{
		"label":   func() appendable { return NewLabelRegistry() },
		"reltype": func() appendable { return NewRelTypeRegistry() },
		"propkey": func() appendable { return NewPropertyKeyRegistry() },
	}
}

// seed registers names in order, returning the resulting ExportNames snapshot.
func seed(t *testing.T, r appendable, names ...string) []string {
	t.Helper()
	for _, n := range names {
		if _, err := r.GetOrCreate(n); err != nil {
			t.Fatalf("seed GetOrCreate(%q): %v", n, err)
		}
	}
	return r.ExportNames()
}

func TestAppendNames_GrowsAtContiguousIndices(t *testing.T) {
	for name, mk := range appendables() {
		t.Run(name, func(t *testing.T) {
			r := mk()
			prefix := seed(t, r, "A", "B") // tokens 1,2; prefix = ["","A","B"]
			ok, err := r.AppendNames(prefix, []string{"C", "D"})
			if err != nil || !ok {
				t.Fatalf("AppendNames = (%v, %v), want (true, nil)", ok, err)
			}
			if r.Len() != 4 {
				t.Fatalf("Len = %d, want 4", r.Len())
			}
			// C, D landed at the next contiguous tokens (3, 4): Lookup finds
			// them there, and GetOrCreate returns the appended token rather than
			// allocating a new one.
			for name, want := range map[string]uint16{"C": 3, "D": 4} {
				if tok, ok := r.Lookup(name); !ok || tok != want {
					t.Fatalf("Lookup(%s) = (%d, %v), want (%d, true)", name, tok, ok, want)
				}
				if tok, _ := r.GetOrCreate(name); tok != want {
					t.Fatalf("GetOrCreate(%s) = %d, want %d (contiguous append)", name, tok, want)
				}
			}
			if got, want := r.ExportNames(), []string{"", "A", "B", "C", "D"}; !slices.Equal(got, want) {
				t.Fatalf("ExportNames = %v, want %v", got, want)
			}
		})
	}
}

// TestAppendNames_PrefixMismatchIsNoOp: a prefix that no longer matches the
// registry, by length (a local allocation since the snapshot) or by content
// (a stale name at the same length), returns (false, nil) and leaves the
// registry unchanged.
func TestAppendNames_PrefixMismatchIsNoOp(t *testing.T) {
	for name, mk := range appendables() {
		t.Run(name, func(t *testing.T) {
			for _, tc := range []struct {
				name   string
				prefix []string
			}{
				// The registry is ["","A","X"]; the caller still believes ["","A"].
				{"longer registry", []string{"", "A"}},
				// Same length, different content.
				{"stale content", []string{"", "A", "different"}},
			} {
				r := mk()
				seed(t, r, "A", "X")
				before := r.ExportNames()
				ok, err := r.AppendNames(tc.prefix, []string{"C"})
				if err != nil {
					t.Fatalf("%s: unexpected error: %v", tc.name, err)
				}
				if ok {
					t.Fatalf("%s: AppendNames returned true on a diverged registry, want false", tc.name)
				}
				if got := r.ExportNames(); !slices.Equal(got, before) {
					t.Fatalf("%s: registry mutated on prefix mismatch: %v -> %v", tc.name, before, got)
				}
				if _, ok := r.Lookup("C"); ok {
					t.Fatalf("%s: C registered despite the prefix mismatch", tc.name)
				}
			}
		})
	}
}

func TestAppendNames_EmptySuffixIsNoOp(t *testing.T) {
	for name, mk := range appendables() {
		t.Run(name, func(t *testing.T) {
			r := mk()
			prefix := seed(t, r, "A")
			ok, err := r.AppendNames(prefix, nil)
			if err != nil || !ok {
				t.Fatalf("AppendNames(empty) = (%v, %v), want (true, nil)", ok, err)
			}
			if r.Len() != 1 {
				t.Fatalf("Len = %d, want 1 (unchanged)", r.Len())
			}
		})
	}
}

func TestAppendNames_RejectsMalformedSuffix(t *testing.T) {
	for name, mk := range appendables() {
		t.Run(name, func(t *testing.T) {
			prefix := []string{"", "A"}

			// (d) blank name (empty or all-whitespace) in the suffix, after a
			// valid one: nothing of the suffix is registered. GetOrCreate rejects
			// the same names, so both doors agree on what is admissible.
			for _, blank := range []string{"", " ", "\t", "  \n "} {
				r := mk()
				seed(t, r, "A")
				if ok, err := r.AppendNames(prefix, []string{"C", blank}); err == nil || ok {
					t.Fatalf("blank suffix %q: got (%v,%v), want (false, err)", blank, ok, err)
				}
				if _, ok := r.Lookup("C"); ok || r.Len() != 1 {
					t.Fatalf("blank suffix %q: C registered or Len %d != 1; the suffix must apply all or nothing", blank, r.Len())
				}
			}
			// (e) duplicate within suffix
			r := mk()
			seed(t, r, "A")
			if ok, err := r.AppendNames(prefix, []string{"C", "C"}); err == nil || ok {
				t.Fatalf("dup suffix: got (%v,%v), want (false, err)", ok, err)
			}
			// (f) suffix name collides with an existing prefix name
			r = mk()
			seed(t, r, "A")
			if ok, err := r.AppendNames(prefix, []string{"A"}); err == nil || ok {
				t.Fatalf("prefix-colliding suffix: got (%v,%v), want (false, err)", ok, err)
			}
			// malformed prefix (no reserved index 0)
			r = mk()
			if ok, err := r.AppendNames([]string{"A"}, []string{"C"}); err == nil || ok {
				t.Fatalf("bad prefix[0]: got (%v,%v), want (false, err)", ok, err)
			}
		})
	}
}

// TestAppendNames_CapacityOverflow: a suffix longer than TokenCapacityMax is
// rejected by the length arithmetic (which runs before content validation);
// a suffix that fills the registry to exactly TokenCapacityMax is admitted, and
// one more name after that is rejected without changing the registry.
func TestAppendNames_CapacityOverflow(t *testing.T) {
	full := make([]string, TokenCapacityMax)
	for i := range full {
		full[i] = fmt.Sprintf("k%d", i+1)
	}
	for name, mk := range appendables() {
		t.Run(name, func(t *testing.T) {
			r := mk()
			suffix := make([]string, TokenCapacityMax+1)
			for i := range suffix {
				suffix[i] = "x"
			}
			if ok, err := r.AppendNames([]string{""}, suffix); err == nil || ok {
				t.Fatalf("oversized suffix: got (%v,%v), want (false, err)", ok, err)
			}

			if ok, err := r.AppendNames([]string{""}, full); err != nil || !ok {
				t.Fatalf("fill to capacity: got (%v,%v), want (true, nil)", ok, err)
			}
			if r.Len() != int(TokenCapacityMax) {
				t.Fatalf("Len = %d, want %d", r.Len(), TokenCapacityMax)
			}
			if ok, err := r.AppendNames(r.ExportNames(), []string{"overflow"}); err == nil || ok {
				t.Fatalf("one past capacity: got (%v,%v), want (false, err)", ok, err)
			}
			if _, ok := r.Lookup("overflow"); ok || r.Len() != int(TokenCapacityMax) {
				t.Fatalf("one past capacity: overflow registered or Len %d changed", r.Len())
			}
		})
	}
}
