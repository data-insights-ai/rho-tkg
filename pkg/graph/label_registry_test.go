package graph

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestLabelRegistryGetOrCreate(t *testing.T) {
	t.Parallel()

	reg := newLabelRegistry()

	tok1, err := reg.GetOrCreate("Person")
	if err != nil {
		t.Fatal(err)
	}
	if tok1 == 0 {
		t.Fatal("GetOrCreate should never return token 0")
	}

	tok2, err := reg.GetOrCreate("Person")
	if err != nil {
		t.Fatal(err)
	}
	if tok1 != tok2 {
		t.Errorf("second GetOrCreate(\"Person\") = %d, want %d (same token)", tok2, tok1)
	}
}

func TestLabelRegistryResolveRoundTrip(t *testing.T) {
	t.Parallel()

	reg := newLabelRegistry()
	tok, _ := reg.GetOrCreate("Movie")
	got := reg.Resolve(tok)
	if got != "Movie" {
		t.Errorf("Resolve(%d) = %q, want \"Movie\"", tok, got)
	}
}

func TestLabelRegistryLookupMiss(t *testing.T) {
	t.Parallel()

	reg := newLabelRegistry()
	_, ok := reg.Lookup("Nonexistent")
	if ok {
		t.Fatal("Lookup(\"Nonexistent\") should return false")
	}
}

func TestLabelRegistryTokenZeroReserved(t *testing.T) {
	t.Parallel()

	reg := newLabelRegistry()
	got := reg.Resolve(0)
	if got != "" {
		t.Errorf("Resolve(0) = %q, want empty string (reserved)", got)
	}
}

func TestLabelRegistryCapacityError(t *testing.T) {
	t.Parallel()

	reg := newLabelRegistry()
	// Fill the registry to full capacity (tokens 1..65535).
	reg.mu.Lock()
	for i := 1; i <= 65535; i++ {
		name := fmt.Sprintf("L%d", i)
		reg.toToken[name] = uint16(i)
		reg.toLabel = append(reg.toLabel, name)
	}
	reg.nextToken = 0 // wraps — doesn't matter, len check catches it
	reg.mu.Unlock()

	// Registry is full (65535 tokens assigned). Next allocation should error.
	_, err := reg.GetOrCreate("Overflow")
	if err == nil {
		t.Fatal("GetOrCreate should return error when registry is full")
	}
}

func TestLabelRegistryConcurrentGetOrCreate(t *testing.T) {
	t.Parallel()

	reg := newLabelRegistry()
	const goroutines = 50
	results := make([]uint16, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := range goroutines {
		go func(idx int) {
			defer wg.Done()
			tok, err := reg.GetOrCreate("SharedLabel")
			if err != nil {
				t.Errorf("goroutine %d: GetOrCreate failed: %v", idx, err)
				return
			}
			results[idx] = tok
		}(i)
	}
	wg.Wait()

	// All goroutines must get the same token.
	for i := 1; i < goroutines; i++ {
		if results[i] != results[0] {
			t.Errorf("goroutine %d got token %d, goroutine 0 got %d", i, results[i], results[0])
		}
	}
}

func TestLabelRegistryResolveAll(t *testing.T) {
	t.Parallel()

	reg := newLabelRegistry()
	tok1, _ := reg.GetOrCreate("Person")
	tok2, _ := reg.GetOrCreate("Actor")

	labels := reg.ResolveAll([]uint16{tok1, tok2})
	if len(labels) != 2 {
		t.Fatalf("ResolveAll len = %d, want 2", len(labels))
	}
	if labels[0] != "Person" || labels[1] != "Actor" {
		t.Errorf("ResolveAll = %v, want [Person Actor]", labels)
	}
}

func TestLabelRegistryToken65535IsAssignable(t *testing.T) {
	t.Parallel()

	reg := newLabelRegistry()
	// Fill tokens 1..65534 by manipulating internal state.
	reg.mu.Lock()
	for i := uint16(1); i <= 65534; i++ {
		name := fmt.Sprintf("L%d", i)
		reg.toToken[name] = i
		reg.toLabel = append(reg.toLabel, name)
	}
	reg.nextToken = 65535
	reg.mu.Unlock()

	// Token 65535 should be assignable.
	tok, err := reg.GetOrCreate("Final")
	if err != nil {
		t.Fatalf("GetOrCreate(\"Final\") should succeed for token 65535, got: %v", err)
	}
	if tok != 65535 {
		t.Errorf("expected token 65535, got %d", tok)
	}

	// Now it should be truly full.
	_, err = reg.GetOrCreate("Overflow")
	if err == nil {
		t.Fatal("GetOrCreate should return error after all 65535 tokens assigned")
	}
}

func TestLabelRegistryRejectsEmptyName(t *testing.T) {
	t.Parallel()

	reg := newLabelRegistry()
	_, err := reg.GetOrCreate("")
	if err == nil {
		t.Fatal("GetOrCreate(\"\") should return error for empty name")
	}
	if !errors.Is(err, ErrEmptyName) {
		t.Errorf("errors.Is(err, ErrEmptyName) = false; err = %v", err)
	}
	if reg.Len() != 0 {
		t.Errorf("registry should be empty after rejected empty name, got Len()=%d", reg.Len())
	}
}

func TestLabelRegistryResolveOutOfRange(t *testing.T) {
	t.Parallel()

	reg := newLabelRegistry()
	reg.GetOrCreate("Person") // token 1

	got := reg.Resolve(9999)
	if got != "" {
		t.Errorf("Resolve(9999) = %q, want empty string (out of range)", got)
	}
}

func TestLabelRegistryLen(t *testing.T) {
	t.Parallel()

	reg := newLabelRegistry()
	if reg.Len() != 0 {
		t.Errorf("empty Len() = %d, want 0", reg.Len())
	}

	reg.GetOrCreate("A")
	reg.GetOrCreate("B")
	reg.GetOrCreate("A") // duplicate

	if reg.Len() != 2 {
		t.Errorf("after 2 unique labels Len() = %d, want 2", reg.Len())
	}
}

// ─── Whitespace rejection edge cases ─────────────────────────────────────────

func TestLabelRegistryRejectsWhitespaceOnlyName(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		input string
	}{
		{"space", " "},
		{"tab", "\t"},
		{"newline", "\n"},
		{"mixed whitespace", "  \t\n  "},
	}

	for _, tc := range cases {
		reg := newLabelRegistry()
		_, err := reg.GetOrCreate(tc.input)
		if err == nil {
			t.Errorf("GetOrCreate(%q) [%s] should return error", tc.input, tc.name)
			continue
		}
		if !errors.Is(err, ErrEmptyName) {
			t.Errorf("GetOrCreate(%q) [%s]: errors.Is(err, ErrEmptyName) = false; err = %v", tc.input, tc.name, err)
		}
	}
}

func TestLabelRegistryLookupEmptyReturnsFalse(t *testing.T) {
	t.Parallel()

	reg := newLabelRegistry()
	reg.GetOrCreate("Person")

	_, ok := reg.Lookup("")
	if ok {
		t.Fatal("Lookup(\"\") should return false")
	}
}

func TestLabelRegistryRecoveryAfterEmptyRejection(t *testing.T) {
	t.Parallel()

	reg := newLabelRegistry()
	_, err := reg.GetOrCreate("")
	if err == nil {
		t.Fatal("GetOrCreate(\"\") should return error")
	}

	tok, err := reg.GetOrCreate("Valid")
	if err != nil {
		t.Fatalf("GetOrCreate(\"Valid\") failed after empty rejection: %v", err)
	}
	if tok == 0 {
		t.Fatal("GetOrCreate(\"Valid\") returned reserved token 0")
	}
	if reg.Len() != 1 {
		t.Errorf("Len() = %d after 1 valid registration, want 1", reg.Len())
	}
}

func TestLabelRegistryConcurrentEmptyRejection(t *testing.T) {
	t.Parallel()

	reg := newLabelRegistry()
	const goroutines = 50
	errs := make([]error, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := range goroutines {
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = reg.GetOrCreate("")
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if !errors.Is(err, ErrEmptyName) {
			t.Errorf("goroutine %d: errors.Is(err, ErrEmptyName) = false; err = %v", i, err)
		}
	}
	if reg.Len() != 0 {
		t.Errorf("Len() = %d after all-empty rejections, want 0", reg.Len())
	}
}

// ─── Export/Import tests ──────────────────────────────────────────────────────

func TestLabelRegistryExportImportRoundTrip(t *testing.T) {
	t.Parallel()

	reg1 := newLabelRegistry()
	reg1.GetOrCreate("Person")
	reg1.GetOrCreate("Movie")
	reg1.GetOrCreate("Actor")

	names := reg1.ExportNames()

	reg2 := newLabelRegistry()
	if err := reg2.ImportNames(names); err != nil {
		t.Fatalf("ImportNames: %v", err)
	}

	// Verify lookups work.
	tok, ok := reg2.Lookup("Person")
	if !ok || tok != 1 {
		t.Fatalf("Lookup(Person): got (%d, %v), want (1, true)", tok, ok)
	}
	tok, ok = reg2.Lookup("Movie")
	if !ok || tok != 2 {
		t.Fatalf("Lookup(Movie): got (%d, %v), want (2, true)", tok, ok)
	}
	tok, ok = reg2.Lookup("Actor")
	if !ok || tok != 3 {
		t.Fatalf("Lookup(Actor): got (%d, %v), want (3, true)", tok, ok)
	}

	// Verify resolution works.
	if reg2.Resolve(1) != "Person" {
		t.Fatal("Resolve(1) should be Person")
	}
	if reg2.Resolve(2) != "Movie" {
		t.Fatal("Resolve(2) should be Movie")
	}

	// Verify new tokens continue from the right place.
	tok, err := reg2.GetOrCreate("Director")
	if err != nil {
		t.Fatalf("GetOrCreate(Director): %v", err)
	}
	if tok != 4 {
		t.Fatalf("expected token 4, got %d", tok)
	}
}

func TestLabelRegistryImportOnNonEmpty(t *testing.T) {
	t.Parallel()

	reg := newLabelRegistry()
	reg.GetOrCreate("Person")

	err := reg.ImportNames([]string{"", "Person"})
	if err == nil {
		t.Fatal("ImportNames on non-empty registry should error")
	}
}

func TestLabelRegistryImportInvalidFirst(t *testing.T) {
	t.Parallel()

	reg := newLabelRegistry()
	err := reg.ImportNames([]string{"NotEmpty", "Person"})
	if err == nil {
		t.Fatal("ImportNames with names[0] != \"\" should error")
	}
}

func TestLabelRegistryImportEmpty(t *testing.T) {
	t.Parallel()

	reg := newLabelRegistry()
	err := reg.ImportNames([]string{})
	if err == nil {
		t.Fatal("ImportNames with empty slice should error")
	}
}

func TestLabelRegistryImportAtCapacityAccepted(t *testing.T) {
	t.Parallel()

	// Exactly tokenCapacityMax tokens (slot 0 + 65535 entries) — must succeed.
	names := make([]string, tokenCapacityMax+1) // len = 65536
	names[0] = ""
	for i := 1; i < len(names); i++ {
		names[i] = fmt.Sprintf("L%d", i)
	}

	reg := newLabelRegistry()
	if err := reg.ImportNames(names); err != nil {
		t.Fatalf("ImportNames at exact capacity should succeed, got: %v", err)
	}
	if reg.Len() != int(tokenCapacityMax) {
		t.Errorf("Len() = %d, want %d", reg.Len(), tokenCapacityMax)
	}
}

func TestLabelRegistryImportOverflowRejected(t *testing.T) {
	t.Parallel()

	// tokenCapacityMax+1 tokens (slot 0 + 65536 entries) — 1 beyond capacity.
	names := make([]string, tokenCapacityMax+2) // len = 65537
	names[0] = ""
	for i := 1; i < len(names); i++ {
		names[i] = fmt.Sprintf("L%d", i)
	}

	reg := newLabelRegistry()
	err := reg.ImportNames(names)
	if err == nil {
		t.Fatal("ImportNames should reject slice exceeding registry capacity")
	}
	if reg.Len() != 0 {
		t.Errorf("registry should remain empty after overflow rejection, got Len()=%d", reg.Len())
	}
}

func TestLabelRegistryImportPreservesTokenOrder(t *testing.T) {
	t.Parallel()

	names := []string{"", "Alpha", "Beta", "Gamma"}
	reg := newLabelRegistry()
	if err := reg.ImportNames(names); err != nil {
		t.Fatalf("ImportNames: %v", err)
	}

	// Token 1 = names[1], etc.
	if reg.Resolve(1) != "Alpha" {
		t.Fatal("token 1 should be Alpha")
	}
	if reg.Resolve(2) != "Beta" {
		t.Fatal("token 2 should be Beta")
	}
	if reg.Resolve(3) != "Gamma" {
		t.Fatal("token 3 should be Gamma")
	}
	if reg.Len() != 3 {
		t.Fatalf("Len() = %d, want 3", reg.Len())
	}
}
