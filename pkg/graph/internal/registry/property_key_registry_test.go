package registry

import (
	"errors"
	"testing"
)

// The PropertyKeyRegistry CREATE door (GetOrCreate) and GROW door (AppendNames)
// must agree on which names are admissible, or a key one door mints is one the
// other refuses — the exact split that let a blank key enter the token table via
// GetOrCreate yet be rejected by AppendNames (the replica grow primitive). Both
// now reject a blank (empty OR all-whitespace) name, matching the Label/RelType
// registries. A blank key is not a hard write failure: the wire encoder ignores
// GetOrCreate's error and falls back to the raw key (token 0), so a degenerate
// blank key still round-trips — it simply never enters the token table.
func TestPropertyKeyRegistry_BlankNameRejectedByCreateAndGrow(t *testing.T) {
	for _, blank := range []string{"", " ", "\t", "  \n "} {
		// CREATE door rejects blank.
		if tok, err := NewPropertyKeyRegistry().GetOrCreate(blank); !errors.Is(err, ErrEmptyName) || tok != 0 {
			t.Fatalf("GetOrCreate(%q) = (%d, %v), want (0, ErrEmptyName)", blank, tok, err)
		}
		// GROW door rejects blank in the suffix.
		r := NewPropertyKeyRegistry()
		if _, err := r.GetOrCreate("k"); err != nil {
			t.Fatalf("seed GetOrCreate(\"k\"): %v", err)
		}
		if ok, err := r.AppendNames([]string{"", "k"}, []string{blank}); ok || err == nil {
			t.Fatalf("AppendNames suffix %q = (%v, %v), want (false, err)", blank, ok, err)
		}
	}

	// A real (non-blank) key is still admitted by both doors.
	r := NewPropertyKeyRegistry()
	if _, err := r.GetOrCreate("name"); err != nil {
		t.Fatalf("GetOrCreate(\"name\"): %v", err)
	}
	if ok, err := r.AppendNames([]string{"", "name"}, []string{"age"}); !ok || err != nil {
		t.Fatalf("AppendNames real suffix = (%v, %v), want (true, nil)", ok, err)
	}
}

// ImportNames stays LENIENT (empty-only rejection) on purpose: a registry
// persisted before the GetOrCreate guard could have tokenized a blank key, and
// that graph must still load. Going forward GetOrCreate refuses to mint new blank
// tokens, so the registry never accumulates one from normal operation — this pins
// the deliberate load-vs-create asymmetry so a future change does not "tidy" it
// into a hard load failure for legacy data.
func TestPropertyKeyRegistry_ImportTolerantOfLegacyBlankKey(t *testing.T) {
	r := NewPropertyKeyRegistry()
	if err := r.ImportNames([]string{"", " ", "k"}); err != nil {
		t.Fatalf("ImportNames with a legacy blank key = %v, want nil (load must tolerate legacy data)", err)
	}
	if _, ok := r.Lookup(" "); !ok {
		t.Fatal("legacy blank key not loaded by ImportNames")
	}
	// But the empty string is never a valid token name, even on load.
	if err := NewPropertyKeyRegistry().ImportNames([]string{"", ""}); err == nil {
		t.Fatal("ImportNames with an empty-string entry = nil, want error")
	}
}
