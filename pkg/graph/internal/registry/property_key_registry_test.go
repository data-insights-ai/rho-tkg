package registry

import (
	"testing"
)

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
