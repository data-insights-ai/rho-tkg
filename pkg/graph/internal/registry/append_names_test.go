package registry

import (
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
			// C, D landed at the next contiguous tokens (3, 4): GetOrCreate must
			// return the appended token, not allocate a new one.
			if tok, _ := r.GetOrCreate("C"); tok != 3 {
				t.Fatalf("token(C) = %d, want 3 (contiguous append)", tok)
			}
			if tok, _ := r.GetOrCreate("D"); tok != 4 {
				t.Fatalf("token(D) = %d, want 4", tok)
			}
			if got, want := r.ExportNames(), []string{"", "A", "B", "C", "D"}; !slices.Equal(got, want) {
				t.Fatalf("ExportNames = %v, want %v", got, want)
			}
		})
	}
}

func TestAppendNames_PrefixMismatchIsNoOp(t *testing.T) {
	for name, mk := range appendables() {
		t.Run(name, func(t *testing.T) {
			r := mk()
			prefix := seed(t, r, "A")
			// A local allocation happened since the snapshot: the registry now
			// has ["","A","X"] but the caller still believes the prefix is ["","A"].
			if _, err := r.GetOrCreate("X"); err != nil {
				t.Fatal(err)
			}
			before := r.ExportNames()
			ok, err := r.AppendNames(prefix, []string{"C"})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ok {
				t.Fatal("AppendNames returned true on a diverged registry, want false")
			}
			if got := r.ExportNames(); !slices.Equal(got, before) {
				t.Fatalf("registry mutated on prefix mismatch: %v -> %v", before, got)
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

			// (d) blank name in suffix
			r := mk()
			seed(t, r, "A")
			if ok, err := r.AppendNames(prefix, []string{"  "}); err == nil || ok {
				t.Fatalf("blank suffix: got (%v,%v), want (false, err)", ok, err)
			}
			// (e) duplicate within suffix
			r = mk()
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

func TestAppendNames_CapacityOverflow(t *testing.T) {
	for name, mk := range appendables() {
		t.Run(name, func(t *testing.T) {
			r := mk()
			prefix := []string{""} // empty registry
			// A suffix longer than TokenCapacityMax must be rejected by the
			// length arithmetic (which runs before content validation).
			suffix := make([]string, TokenCapacityMax+1)
			for i := range suffix {
				suffix[i] = "x"
			}
			if ok, err := r.AppendNames(prefix, suffix); err == nil || ok {
				t.Fatalf("capacity overflow: got (%v,%v), want (false, err)", ok, err)
			}
		})
	}
}
