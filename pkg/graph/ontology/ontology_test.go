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

func TestOntologyMapping_SetLabelRegistryClearsTokenCache(t *testing.T) {
	om := NewOntologyMapping([]string{"Case"})

	reg1 := registry.NewLabelRegistry()
	caseTok, err := reg1.GetOrCreate("Case")
	if err != nil {
		t.Fatalf("GetOrCreate Case: %v", err)
	}
	if prev := om.SetLabelRegistry(reg1); prev != nil {
		t.Fatalf("first SetLabelRegistry previous = %v, want nil", prev)
	}
	if got := om.ClassifyByToken(caseTok); got != ClassReference {
		t.Fatalf("ClassifyByToken(Case) = %d, want ClassReference", got)
	}

	reg2 := registry.NewLabelRegistry()
	signalTok, err := reg2.GetOrCreate("Signal")
	if err != nil {
		t.Fatalf("GetOrCreate Signal: %v", err)
	}
	if signalTok != caseTok {
		t.Fatalf("test setup token mismatch: Signal=%d Case=%d", signalTok, caseTok)
	}
	if prev := om.SetLabelRegistry(reg2); prev != reg1 {
		t.Fatalf("SetLabelRegistry previous = %p, want %p", prev, reg1)
	}
	if got := om.ClassifyByToken(signalTok); got != ClassEvent {
		t.Fatalf("ClassifyByToken(Signal after registry swap) = %d, want ClassEvent", got)
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

func TestOntologyMapping_IgnoresEmptyRefLabelNames(t *testing.T) {
	om := NewOntologyMapping([]string{"", " \t", "Case"})

	if got := om.ClassifyByName(""); got != ClassEvent {
		t.Fatalf("ClassifyByName(empty) = %d, want ClassEvent", got)
	}
	if got := om.ClassifyByName(" \t"); got != ClassEvent {
		t.Fatalf("ClassifyByName(whitespace) = %d, want ClassEvent", got)
	}
	if got := om.ClassifyByName("Case"); got != ClassReference {
		t.Fatalf("ClassifyByName(Case) = %d, want ClassReference", got)
	}
	refs := om.RefLabels()
	if len(refs) != 1 || refs[0] != "Case" {
		t.Fatalf("RefLabels() = %v, want [Case]", refs)
	}
}

func TestOntologyMappingNilReceiverMethodsFailClosed(t *testing.T) {
	var om *OntologyMapping
	reg := registry.NewLabelRegistry()

	if got := om.ClassifyByName("Case"); got != ClassEvent {
		t.Fatalf("ClassifyByName on nil receiver = %d, want ClassEvent", got)
	}
	if got := om.ClassifyByToken(1); got != ClassEvent {
		t.Fatalf("ClassifyByToken on nil receiver = %d, want ClassEvent", got)
	}
	if prev := om.SetLabelRegistry(reg); prev != nil {
		t.Fatalf("SetLabelRegistry on nil receiver = %v, want nil", prev)
	}
	if refs := om.RefLabels(); refs != nil {
		t.Fatalf("RefLabels on nil receiver = %v, want nil", refs)
	}
}
