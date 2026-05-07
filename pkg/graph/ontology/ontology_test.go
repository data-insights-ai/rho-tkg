package ontology

import (
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
)

func TestOntologyMapping_ClassifyByName(t *testing.T) {
	om := NewOntologyMapping([]string{"Case", "Organization", "User"})

	tests := []struct {
		name string
		want EntityClass
	}{
		{"Case", ClassReference},
		{"Organization", ClassReference},
		{"User", ClassReference},
		{"Signal", ClassEvent},
		{"Alert", ClassEvent},
		{"", ClassEvent},
	}
	for _, tt := range tests {
		got := om.ClassifyByName(tt.name)
		if got != tt.want {
			t.Errorf("ClassifyByName(%q) = %d, want %d", tt.name, got, tt.want)
		}
	}
}

func TestOntologyMapping_ClassifyByToken_NoRegistry(t *testing.T) {
	om := NewOntologyMapping([]string{"Case"})

	// Without a registry, all tokens return ClassEvent.
	if got := om.ClassifyByToken(1); got != ClassEvent {
		t.Errorf("ClassifyByToken(1) without registry = %d, want ClassEvent", got)
	}

	// Token 0 is always ClassEvent.
	if got := om.ClassifyByToken(0); got != ClassEvent {
		t.Errorf("ClassifyByToken(0) = %d, want ClassEvent", got)
	}
}

func TestOntologyMapping_ClassifyByToken_WithRegistry(t *testing.T) {
	om := NewOntologyMapping([]string{"Case", "User"})
	reg := registry.NewLabelRegistry()
	om.SetLabelRegistry(reg)

	// Register labels.
	caseTok, err := reg.GetOrCreate("Case")
	if err != nil {
		t.Fatal(err)
	}
	signalTok, err := reg.GetOrCreate("Signal")
	if err != nil {
		t.Fatal(err)
	}
	userTok, err := reg.GetOrCreate("User")
	if err != nil {
		t.Fatal(err)
	}

	// Reference labels.
	if got := om.ClassifyByToken(caseTok); got != ClassReference {
		t.Errorf("ClassifyByToken(%d/Case) = %d, want ClassReference", caseTok, got)
	}
	if got := om.ClassifyByToken(userTok); got != ClassReference {
		t.Errorf("ClassifyByToken(%d/User) = %d, want ClassReference", userTok, got)
	}

	// Event label.
	if got := om.ClassifyByToken(signalTok); got != ClassEvent {
		t.Errorf("ClassifyByToken(%d/Signal) = %d, want ClassEvent", signalTok, got)
	}

	// Second call should use cache.
	if got := om.ClassifyByToken(caseTok); got != ClassReference {
		t.Errorf("cached ClassifyByToken(%d/Case) = %d, want ClassReference", caseTok, got)
	}
}

func TestOntologyMapping_ClassifyByToken_UnknownToken(t *testing.T) {
	om := NewOntologyMapping([]string{"Case"})
	reg := registry.NewLabelRegistry()
	om.SetLabelRegistry(reg)

	// Token 999 doesn't exist in the registry.
	if got := om.ClassifyByToken(999); got != ClassEvent {
		t.Errorf("ClassifyByToken(999) = %d, want ClassEvent", got)
	}
}

func TestOntologyMapping_RefLabels(t *testing.T) {
	om := NewOntologyMapping([]string{"Case", "User"})
	refs := om.RefLabels()
	if len(refs) != 2 {
		t.Fatalf("RefLabels() returned %d, want 2", len(refs))
	}
	has := make(map[string]bool)
	for _, r := range refs {
		has[r] = true
	}
	if !has["Case"] || !has["User"] {
		t.Errorf("RefLabels() = %v, want Case and User", refs)
	}
}

func TestOntologyMapping_EmptyRefLabels(t *testing.T) {
	om := NewOntologyMapping(nil)
	if got := om.ClassifyByName("Anything"); got != ClassEvent {
		t.Errorf("empty mapping ClassifyByName = %d, want ClassEvent", got)
	}
	refs := om.RefLabels()
	if len(refs) != 0 {
		t.Errorf("empty mapping RefLabels() = %v, want empty", refs)
	}
}
