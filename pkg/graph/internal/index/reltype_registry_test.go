package index

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestRelTypeRegistryGetOrCreate(t *testing.T) {
	t.Parallel()

	reg := NewRelTypeRegistry()

	tok1, err := reg.GetOrCreate("KNOWS")
	if err != nil {
		t.Fatal(err)
	}
	if tok1 == 0 {
		t.Fatal("GetOrCreate should never return token 0")
	}

	tok2, err := reg.GetOrCreate("KNOWS")
	if err != nil {
		t.Fatal(err)
	}
	if tok1 != tok2 {
		t.Errorf("second GetOrCreate(\"KNOWS\") = %d, want %d (same token)", tok2, tok1)
	}
}

func TestRelTypeRegistryResolveRoundTrip(t *testing.T) {
	t.Parallel()

	reg := NewRelTypeRegistry()
	tok, _ := reg.GetOrCreate("ACTED_IN")
	got := reg.Resolve(tok)
	if got != "ACTED_IN" {
		t.Errorf("Resolve(%d) = %q, want \"ACTED_IN\"", tok, got)
	}
}

func TestRelTypeRegistryLookupMiss(t *testing.T) {
	t.Parallel()

	reg := NewRelTypeRegistry()
	_, ok := reg.Lookup("Nonexistent")
	if ok {
		t.Fatal("Lookup(\"Nonexistent\") should return false")
	}
}

func TestRelTypeRegistryTokenZeroReserved(t *testing.T) {
	t.Parallel()

	reg := NewRelTypeRegistry()
	got := reg.Resolve(0)
	if got != "" {
		t.Errorf("Resolve(0) = %q, want empty string (reserved)", got)
	}
}

func TestRelTypeRegistryCapacityError(t *testing.T) {
	t.Parallel()

	reg := NewRelTypeRegistry()
	// Fill the registry to full capacity (tokens 1..65535).
	reg.mu.Lock()
	for i := 1; i <= 65535; i++ {
		name := fmt.Sprintf("RT%d", i)
		reg.toToken[name] = uint16(i)
		reg.toName = append(reg.toName, name)
	}
	reg.nextToken = 0 // wraps — doesn't matter, len check catches it
	reg.mu.Unlock()

	// Registry is full (65535 tokens assigned). Next allocation should error.
	_, err := reg.GetOrCreate("Overflow")
	if err == nil {
		t.Fatal("GetOrCreate should return error when registry is full")
	}
}

func TestRelTypeRegistryConcurrentGetOrCreate(t *testing.T) {
	t.Parallel()

	reg := NewRelTypeRegistry()
	const goroutines = 50
	results := make([]uint16, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := range goroutines {
		go func(idx int) {
			defer wg.Done()
			tok, err := reg.GetOrCreate("SharedType")
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

func TestRelTypeRegistryResolveAll(t *testing.T) {
	t.Parallel()

	reg := NewRelTypeRegistry()
	tok1, _ := reg.GetOrCreate("KNOWS")
	tok2, _ := reg.GetOrCreate("ACTED_IN")

	names := reg.ResolveAll([]uint16{tok1, tok2})
	if len(names) != 2 {
		t.Fatalf("ResolveAll len = %d, want 2", len(names))
	}
	if names[0] != "KNOWS" || names[1] != "ACTED_IN" {
		t.Errorf("ResolveAll = %v, want [KNOWS ACTED_IN]", names)
	}
}

func TestRelTypeRegistryToken65535IsAssignable(t *testing.T) {
	t.Parallel()

	reg := NewRelTypeRegistry()
	// Fill tokens 1..65534 by manipulating internal state.
	reg.mu.Lock()
	for i := uint16(1); i <= 65534; i++ {
		name := fmt.Sprintf("RT%d", i)
		reg.toToken[name] = i
		reg.toName = append(reg.toName, name)
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

func TestRelTypeRegistryRejectsEmptyName(t *testing.T) {
	t.Parallel()

	reg := NewRelTypeRegistry()
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

func TestRelTypeRegistryResolveOutOfRange(t *testing.T) {
	t.Parallel()

	reg := NewRelTypeRegistry()
	reg.GetOrCreate("KNOWS") // token 1

	got := reg.Resolve(9999)
	if got != "" {
		t.Errorf("Resolve(9999) = %q, want empty string (out of range)", got)
	}
}

func TestRelTypeRegistryLen(t *testing.T) {
	t.Parallel()

	reg := NewRelTypeRegistry()
	if reg.Len() != 0 {
		t.Errorf("empty Len() = %d, want 0", reg.Len())
	}

	reg.GetOrCreate("A")
	reg.GetOrCreate("B")
	reg.GetOrCreate("A") // duplicate

	if reg.Len() != 2 {
		t.Errorf("after 2 unique types Len() = %d, want 2", reg.Len())
	}
}

// ─── Whitespace rejection edge cases ─────────────────────────────────────────

func TestRelTypeRegistryRejectsWhitespaceOnlyName(t *testing.T) {
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
		reg := NewRelTypeRegistry()
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

func TestRelTypeRegistryLookupEmptyReturnsFalse(t *testing.T) {
	t.Parallel()

	reg := NewRelTypeRegistry()
	reg.GetOrCreate("KNOWS")

	_, ok := reg.Lookup("")
	if ok {
		t.Fatal("Lookup(\"\") should return false")
	}
}

func TestRelTypeRegistryRecoveryAfterEmptyRejection(t *testing.T) {
	t.Parallel()

	reg := NewRelTypeRegistry()
	_, err := reg.GetOrCreate("")
	if err == nil {
		t.Fatal("GetOrCreate(\"\") should return error")
	}

	tok, err := reg.GetOrCreate("VALID")
	if err != nil {
		t.Fatalf("GetOrCreate(\"VALID\") failed after empty rejection: %v", err)
	}
	if tok == 0 {
		t.Fatal("GetOrCreate(\"VALID\") returned reserved token 0")
	}
	if reg.Len() != 1 {
		t.Errorf("Len() = %d after 1 valid registration, want 1", reg.Len())
	}
}

func TestRelTypeRegistryConcurrentEmptyRejection(t *testing.T) {
	t.Parallel()

	reg := NewRelTypeRegistry()
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

func TestRelTypeRegistryExportImportRoundTrip(t *testing.T) {
	t.Parallel()

	reg1 := NewRelTypeRegistry()
	reg1.GetOrCreate("KNOWS")
	reg1.GetOrCreate("ACTED_IN")
	reg1.GetOrCreate("DIRECTED")

	names := reg1.ExportNames()

	reg2 := NewRelTypeRegistry()
	if err := reg2.ImportNames(names); err != nil {
		t.Fatalf("ImportNames: %v", err)
	}

	tok, ok := reg2.Lookup("KNOWS")
	if !ok || tok != 1 {
		t.Fatalf("Lookup(KNOWS): got (%d, %v), want (1, true)", tok, ok)
	}
	tok, ok = reg2.Lookup("ACTED_IN")
	if !ok || tok != 2 {
		t.Fatalf("Lookup(ACTED_IN): got (%d, %v), want (2, true)", tok, ok)
	}

	if reg2.Resolve(1) != "KNOWS" {
		t.Fatal("Resolve(1) should be KNOWS")
	}

	tok, err := reg2.GetOrCreate("PRODUCED")
	if err != nil {
		t.Fatalf("GetOrCreate(PRODUCED): %v", err)
	}
	if tok != 4 {
		t.Fatalf("expected token 4, got %d", tok)
	}
}

func TestRelTypeRegistryImportOnNonEmpty(t *testing.T) {
	t.Parallel()

	reg := NewRelTypeRegistry()
	reg.GetOrCreate("KNOWS")

	err := reg.ImportNames([]string{"", "KNOWS"})
	if err == nil {
		t.Fatal("ImportNames on non-empty registry should error")
	}
}

func TestRelTypeRegistryImportInvalidFirst(t *testing.T) {
	t.Parallel()

	reg := NewRelTypeRegistry()
	err := reg.ImportNames([]string{"NotEmpty", "KNOWS"})
	if err == nil {
		t.Fatal("ImportNames with names[0] != \"\" should error")
	}
}

func TestRelTypeRegistryImportEmpty(t *testing.T) {
	t.Parallel()

	reg := NewRelTypeRegistry()
	err := reg.ImportNames([]string{})
	if err == nil {
		t.Fatal("ImportNames with empty slice should error")
	}
}

func TestRelTypeRegistryImportAtCapacityAccepted(t *testing.T) {
	t.Parallel()

	// Exactly TokenCapacityMax tokens (slot 0 + 65535 entries) — must succeed.
	names := make([]string, TokenCapacityMax+1) // len = 65536
	names[0] = ""
	for i := 1; i < len(names); i++ {
		names[i] = fmt.Sprintf("RT%d", i)
	}

	reg := NewRelTypeRegistry()
	if err := reg.ImportNames(names); err != nil {
		t.Fatalf("ImportNames at exact capacity should succeed, got: %v", err)
	}
	if reg.Len() != int(TokenCapacityMax) {
		t.Errorf("Len() = %d, want %d", reg.Len(), TokenCapacityMax)
	}
}

func TestRelTypeRegistryImportOverflowRejected(t *testing.T) {
	t.Parallel()

	// TokenCapacityMax+1 tokens (slot 0 + 65536 entries) — 1 beyond capacity.
	names := make([]string, TokenCapacityMax+2) // len = 65537
	names[0] = ""
	for i := 1; i < len(names); i++ {
		names[i] = fmt.Sprintf("RT%d", i)
	}

	reg := NewRelTypeRegistry()
	err := reg.ImportNames(names)
	if err == nil {
		t.Fatal("ImportNames should reject slice exceeding registry capacity")
	}
	if reg.Len() != 0 {
		t.Errorf("registry should remain empty after overflow rejection, got Len()=%d", reg.Len())
	}
}

func TestRelTypeRegistryImportPreservesTokenOrder(t *testing.T) {
	t.Parallel()

	names := []string{"", "KNOWS", "ACTED_IN", "DIRECTED"}
	reg := NewRelTypeRegistry()
	if err := reg.ImportNames(names); err != nil {
		t.Fatalf("ImportNames: %v", err)
	}

	if reg.Resolve(1) != "KNOWS" {
		t.Fatal("token 1 should be KNOWS")
	}
	if reg.Resolve(2) != "ACTED_IN" {
		t.Fatal("token 2 should be ACTED_IN")
	}
	if reg.Resolve(3) != "DIRECTED" {
		t.Fatal("token 3 should be DIRECTED")
	}
	if reg.Len() != 3 {
		t.Fatalf("Len() = %d, want 3", reg.Len())
	}
}

func TestRelTypeRegistryImportRejectsEmptyEntry(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		names []string
	}{
		{"literal empty", []string{"", "KNOWS", "", "ACTED_IN"}},
		{"whitespace only", []string{"", "KNOWS", "  ", "ACTED_IN"}},
		{"tab only", []string{"", "\t", "ACTED_IN"}},
	}

	for _, tc := range cases {
		reg := NewRelTypeRegistry()
		err := reg.ImportNames(tc.names)
		if err == nil {
			t.Errorf("[%s] ImportNames should reject slice with empty/whitespace entry at index > 0", tc.name)
			continue
		}
		if reg.Len() != 0 {
			t.Errorf("[%s] registry should remain empty after rejection, got Len()=%d", tc.name, reg.Len())
		}
	}
}

func TestRelTypeRegistryImportRejectsDuplicateEntry(t *testing.T) {
	t.Parallel()

	// "KNOWS" appears at both index 1 and index 3 — duplicate must be rejected.
	names := []string{"", "KNOWS", "ACTED_IN", "KNOWS"}
	reg := NewRelTypeRegistry()
	err := reg.ImportNames(names)
	if err == nil {
		t.Fatal("ImportNames should reject slice with duplicate names")
	}
	if reg.Len() != 0 {
		t.Errorf("registry should remain empty after rejection, got Len()=%d", reg.Len())
	}
}
