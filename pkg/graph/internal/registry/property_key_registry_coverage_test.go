package registry

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

// BACKLOG 15b: PropertyKeyRegistry had only 2 direct tests for 8 public
// methods (GetOrCreate, Resolve, Lookup, Len, ExportNames, ImportNames,
// AppendNames, plus the constructor), vs. ~25 each for the sibling
// label/reltype registries — a Testing Rule 1 violation on the hot
// property-encode path. This file mirrors label_registry_test.go's coverage
// categories, adapted for PropertyKeyRegistry's two real differences from its
// siblings: (1) GetOrCreate is CAPACITY-SOFT — it returns (0, nil) at
// capacity, not an error (lesson 37: a property-key overflow must degrade to
// "write the raw key on wire", never fail the entity write); (2) it has no
// ResolveAll or RollbackNames (those are label/reltype-only doors).

func TestPropertyKeyRegistryGetOrCreate(t *testing.T) {
	t.Parallel()

	reg := NewPropertyKeyRegistry()

	tok1, err := reg.GetOrCreate("name")
	if err != nil {
		t.Fatal(err)
	}
	if tok1 == 0 {
		t.Fatal("GetOrCreate should never return token 0 for a real key below capacity")
	}

	tok2, err := reg.GetOrCreate("name")
	if err != nil {
		t.Fatal(err)
	}
	if tok1 != tok2 {
		t.Errorf("second GetOrCreate(\"name\") = %d, want %d (same token)", tok2, tok1)
	}
}

func TestPropertyKeyRegistryResolveRoundTrip(t *testing.T) {
	t.Parallel()

	reg := NewPropertyKeyRegistry()
	tok, _ := reg.GetOrCreate("email")
	got := reg.Resolve(tok)
	if got != "email" {
		t.Errorf("Resolve(%d) = %q, want \"email\"", tok, got)
	}
}

func TestPropertyKeyRegistryLookupMiss(t *testing.T) {
	t.Parallel()

	reg := NewPropertyKeyRegistry()
	_, ok := reg.Lookup("nonexistent")
	if ok {
		t.Fatal("Lookup(\"nonexistent\") should return false")
	}
}

func TestPropertyKeyRegistryTokenZeroReserved(t *testing.T) {
	t.Parallel()

	reg := NewPropertyKeyRegistry()
	got := reg.Resolve(0)
	if got != "" {
		t.Errorf("Resolve(0) = %q, want empty string (reserved)", got)
	}
}

func TestPropertyKeyRegistryZeroValueBehavesLikeEmptyRegistry(t *testing.T) {
	t.Parallel()

	var reg PropertyKeyRegistry
	if got := reg.Len(); got != 0 {
		t.Fatalf("zero-value Len = %d, want 0", got)
	}
	if names := reg.ExportNames(); len(names) != 1 || names[0] != "" {
		t.Fatalf("zero-value ExportNames = %v, want reserved token entry only", names)
	}
	if got := reg.Resolve(0); got != "" {
		t.Fatalf("zero-value Resolve(0) = %q, want empty string", got)
	}
	if tok, ok := reg.Lookup("name"); ok || tok != 0 {
		t.Fatalf("zero-value Lookup before allocation = (%d, %v), want (0, false)", tok, ok)
	}

	tok, err := reg.GetOrCreate("name")
	if err != nil {
		t.Fatalf("zero-value GetOrCreate: %v", err)
	}
	if tok != 1 {
		t.Fatalf("zero-value first token = %d, want 1", tok)
	}
	if got := reg.Resolve(tok); got != "name" {
		t.Fatalf("zero-value Resolve(%d) = %q, want name", tok, got)
	}
	if got := reg.Len(); got != 1 {
		t.Fatalf("zero-value Len after allocation = %d, want 1", got)
	}
}

func TestPropertyKeyRegistryZeroValueImportNames(t *testing.T) {
	t.Parallel()

	var reg PropertyKeyRegistry
	if err := reg.ImportNames([]string{"", "name", "email"}); err != nil {
		t.Fatalf("zero-value ImportNames: %v", err)
	}
	if got := reg.Len(); got != 2 {
		t.Fatalf("Len after zero-value import = %d, want 2", got)
	}
	if got := reg.Resolve(2); got != "email" {
		t.Fatalf("Resolve(2) after zero-value import = %q, want email", got)
	}
	tok, err := reg.GetOrCreate("age")
	if err != nil {
		t.Fatalf("GetOrCreate after zero-value import: %v", err)
	}
	if tok != 3 {
		t.Fatalf("token after zero-value import = %d, want 3", tok)
	}
}

// TestPropertyKeyRegistryCapacityIsSoft is the direct pin for lesson 37: at
// capacity, GetOrCreate returns (0, nil) — NOT an error — so the wire encoder
// can fall back to the raw key string instead of failing the entity write.
func TestPropertyKeyRegistryCapacityIsSoft(t *testing.T) {
	t.Parallel()

	reg := NewPropertyKeyRegistry()
	// Fill the registry to full capacity (tokens 1..65535).
	reg.mu.Lock()
	for i := 1; i <= 65535; i++ {
		name := fmt.Sprintf("k%d", i)
		reg.toToken[name] = uint16(i)
		reg.toKey = append(reg.toKey, name)
	}
	reg.mu.Unlock()

	tok, err := reg.GetOrCreate("overflow")
	if err != nil {
		t.Fatalf("GetOrCreate at capacity returned an error (%v), want (0, nil) — capacity-soft per lesson 37", err)
	}
	if tok != 0 {
		t.Fatalf("GetOrCreate at capacity returned token %d, want 0 (fall back to raw key on wire)", tok)
	}
	// The overflow key must NOT have been admitted to the registry.
	if _, ok := reg.Lookup("overflow"); ok {
		t.Fatal("overflow key was registered despite being at capacity")
	}
}

func TestPropertyKeyRegistryConcurrentGetOrCreate(t *testing.T) {
	t.Parallel()

	reg := NewPropertyKeyRegistry()
	const goroutines = 50
	results := make([]uint16, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := range goroutines {
		go func(idx int) {
			defer wg.Done()
			tok, err := reg.GetOrCreate("shared_key")
			if err != nil {
				t.Errorf("goroutine %d: GetOrCreate failed: %v", idx, err)
				return
			}
			results[idx] = tok
		}(i)
	}
	wg.Wait()

	for i := 1; i < goroutines; i++ {
		if results[i] != results[0] {
			t.Errorf("goroutine %d got token %d, goroutine 0 got %d", i, results[i], results[0])
		}
	}
}

func TestPropertyKeyRegistryToken65535IsAssignable(t *testing.T) {
	t.Parallel()

	reg := NewPropertyKeyRegistry()
	reg.mu.Lock()
	for i := uint16(1); i <= 65534; i++ {
		name := fmt.Sprintf("k%d", i)
		reg.toToken[name] = i
		reg.toKey = append(reg.toKey, name)
	}
	reg.nextToken = 65535
	reg.mu.Unlock()

	tok, err := reg.GetOrCreate("final")
	if err != nil {
		t.Fatalf("GetOrCreate(\"final\") should succeed for token 65535, got: %v", err)
	}
	if tok != 65535 {
		t.Errorf("expected token 65535, got %d", tok)
	}

	// Now truly full — capacity-soft, so this is (0, nil), not an error.
	tok2, err := reg.GetOrCreate("overflow")
	if err != nil {
		t.Fatalf("GetOrCreate after all 65535 tokens assigned returned an error, want (0, nil): %v", err)
	}
	if tok2 != 0 {
		t.Fatalf("GetOrCreate after full capacity = %d, want 0", tok2)
	}
}

func TestPropertyKeyRegistryRejectsEmptyName(t *testing.T) {
	t.Parallel()

	reg := NewPropertyKeyRegistry()
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

func TestPropertyKeyRegistryResolveOutOfRange(t *testing.T) {
	t.Parallel()

	reg := NewPropertyKeyRegistry()
	if _, err := reg.GetOrCreate("name"); err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}

	got := reg.Resolve(9999)
	if got != "" {
		t.Errorf("Resolve(9999) = %q, want empty string (out of range)", got)
	}
}

func TestPropertyKeyRegistryLen(t *testing.T) {
	t.Parallel()

	reg := NewPropertyKeyRegistry()
	if reg.Len() != 0 {
		t.Errorf("empty Len() = %d, want 0", reg.Len())
	}

	if _, err := reg.GetOrCreate("a"); err != nil {
		t.Fatalf("GetOrCreate a: %v", err)
	}
	if _, err := reg.GetOrCreate("b"); err != nil {
		t.Fatalf("GetOrCreate b: %v", err)
	}
	if _, err := reg.GetOrCreate("a"); err != nil { // duplicate
		t.Fatalf("GetOrCreate a (dup): %v", err)
	}

	if reg.Len() != 2 {
		t.Errorf("after 2 unique keys Len() = %d, want 2", reg.Len())
	}
}

func TestPropertyKeyRegistryRejectsWhitespaceOnlyName(t *testing.T) {
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
		reg := NewPropertyKeyRegistry()
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

func TestPropertyKeyRegistryLookupEmptyReturnsFalse(t *testing.T) {
	t.Parallel()

	reg := NewPropertyKeyRegistry()
	if _, err := reg.GetOrCreate("name"); err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}

	_, ok := reg.Lookup("")
	if ok {
		t.Fatal("Lookup(\"\") should return false")
	}
}

func TestPropertyKeyRegistryRecoveryAfterEmptyRejection(t *testing.T) {
	t.Parallel()

	reg := NewPropertyKeyRegistry()
	_, err := reg.GetOrCreate("")
	if err == nil {
		t.Fatal("GetOrCreate(\"\") should return error")
	}

	tok, err := reg.GetOrCreate("valid")
	if err != nil {
		t.Fatalf("GetOrCreate(\"valid\") failed after empty rejection: %v", err)
	}
	if tok == 0 {
		t.Fatal("GetOrCreate(\"valid\") returned reserved token 0")
	}
	if reg.Len() != 1 {
		t.Errorf("Len() = %d after 1 valid registration, want 1", reg.Len())
	}
}

func TestPropertyKeyRegistryConcurrentEmptyRejection(t *testing.T) {
	t.Parallel()

	reg := NewPropertyKeyRegistry()
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

func TestPropertyKeyRegistryExportImportRoundTrip(t *testing.T) {
	t.Parallel()

	reg1 := NewPropertyKeyRegistry()
	if _, err := reg1.GetOrCreate("name"); err != nil {
		t.Fatalf("GetOrCreate name: %v", err)
	}
	if _, err := reg1.GetOrCreate("email"); err != nil {
		t.Fatalf("GetOrCreate email: %v", err)
	}
	if _, err := reg1.GetOrCreate("age"); err != nil {
		t.Fatalf("GetOrCreate age: %v", err)
	}

	names := reg1.ExportNames()

	reg2 := NewPropertyKeyRegistry()
	if err := reg2.ImportNames(names); err != nil {
		t.Fatalf("ImportNames: %v", err)
	}

	tok, ok := reg2.Lookup("name")
	if !ok || tok != 1 {
		t.Fatalf("Lookup(name): got (%d, %v), want (1, true)", tok, ok)
	}
	tok, ok = reg2.Lookup("email")
	if !ok || tok != 2 {
		t.Fatalf("Lookup(email): got (%d, %v), want (2, true)", tok, ok)
	}
	tok, ok = reg2.Lookup("age")
	if !ok || tok != 3 {
		t.Fatalf("Lookup(age): got (%d, %v), want (3, true)", tok, ok)
	}

	if reg2.Resolve(1) != "name" {
		t.Fatal("Resolve(1) should be name")
	}
	if reg2.Resolve(2) != "email" {
		t.Fatal("Resolve(2) should be email")
	}

	tok, err := reg2.GetOrCreate("city")
	if err != nil {
		t.Fatalf("GetOrCreate(city): %v", err)
	}
	if tok != 4 {
		t.Fatalf("expected token 4, got %d", tok)
	}
}

func TestPropertyKeyRegistryImportOnNonEmpty(t *testing.T) {
	t.Parallel()

	reg := NewPropertyKeyRegistry()
	if _, err := reg.GetOrCreate("name"); err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}

	err := reg.ImportNames([]string{"", "name"})
	if err == nil {
		t.Fatal("ImportNames on non-empty registry should error")
	}
}

func TestPropertyKeyRegistryImportInvalidFirst(t *testing.T) {
	t.Parallel()

	reg := NewPropertyKeyRegistry()
	err := reg.ImportNames([]string{"NotEmpty", "name"})
	if err == nil {
		t.Fatal("ImportNames with names[0] != \"\" should error")
	}
}

func TestPropertyKeyRegistryImportEmpty(t *testing.T) {
	t.Parallel()

	reg := NewPropertyKeyRegistry()
	err := reg.ImportNames([]string{})
	if err == nil {
		t.Fatal("ImportNames with empty slice should error")
	}
}

func TestPropertyKeyRegistryImportAtCapacityAccepted(t *testing.T) {
	t.Parallel()

	names := make([]string, TokenCapacityMax+1) // len = 65536
	names[0] = ""
	for i := 1; i < len(names); i++ {
		names[i] = fmt.Sprintf("k%d", i)
	}

	reg := NewPropertyKeyRegistry()
	if err := reg.ImportNames(names); err != nil {
		t.Fatalf("ImportNames at exact capacity should succeed, got: %v", err)
	}
	if reg.Len() != int(TokenCapacityMax) {
		t.Errorf("Len() = %d, want %d", reg.Len(), TokenCapacityMax)
	}
}

func TestPropertyKeyRegistryImportOverflowRejected(t *testing.T) {
	t.Parallel()

	names := make([]string, TokenCapacityMax+2) // len = 65537, 1 beyond capacity
	names[0] = ""
	for i := 1; i < len(names); i++ {
		names[i] = fmt.Sprintf("k%d", i)
	}

	reg := NewPropertyKeyRegistry()
	err := reg.ImportNames(names)
	if err == nil {
		t.Fatal("ImportNames should reject slice exceeding registry capacity")
	}
	if reg.Len() != 0 {
		t.Errorf("registry should remain empty after overflow rejection, got Len()=%d", reg.Len())
	}
}

func TestPropertyKeyRegistryImportPreservesTokenOrder(t *testing.T) {
	t.Parallel()

	names := []string{"", "alpha", "beta", "gamma"}
	reg := NewPropertyKeyRegistry()
	if err := reg.ImportNames(names); err != nil {
		t.Fatalf("ImportNames: %v", err)
	}

	if reg.Resolve(1) != "alpha" {
		t.Fatal("token 1 should be alpha")
	}
	if reg.Resolve(2) != "beta" {
		t.Fatal("token 2 should be beta")
	}
	if reg.Resolve(3) != "gamma" {
		t.Fatal("token 3 should be gamma")
	}
	if reg.Len() != 3 {
		t.Fatalf("Len() = %d, want 3", reg.Len())
	}
}

func TestPropertyKeyRegistryImportRejectsDuplicateEntry(t *testing.T) {
	t.Parallel()

	names := []string{"", "name", "email", "name"}
	reg := NewPropertyKeyRegistry()
	err := reg.ImportNames(names)
	if err == nil {
		t.Fatal("ImportNames should reject slice with duplicate names")
	}
	if reg.Len() != 0 {
		t.Errorf("registry should remain empty after rejection, got Len()=%d", reg.Len())
	}
}

// ─── AppendNames tests ────────────────────────────────────────────────────────

func TestPropertyKeyRegistryAppendNamesSuccess(t *testing.T) {
	t.Parallel()

	reg := NewPropertyKeyRegistry()
	if _, err := reg.GetOrCreate("name"); err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}
	prefix := reg.ExportNames()

	ok, err := reg.AppendNames(prefix, []string{"email", "age"})
	if err != nil {
		t.Fatalf("AppendNames: %v", err)
	}
	if !ok {
		t.Fatal("AppendNames returned false for exact prefix match")
	}
	if tok, exists := reg.Lookup("email"); !exists || tok != 2 {
		t.Fatalf("Lookup(email) after append = (%d, %v), want (2, true)", tok, exists)
	}
	if tok, exists := reg.Lookup("age"); !exists || tok != 3 {
		t.Fatalf("Lookup(age) after append = (%d, %v), want (3, true)", tok, exists)
	}
}

func TestPropertyKeyRegistryAppendNamesPrefixMismatch(t *testing.T) {
	t.Parallel()

	reg := NewPropertyKeyRegistry()
	if _, err := reg.GetOrCreate("name"); err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}
	stalePrefix := []string{"", "different"}

	ok, err := reg.AppendNames(stalePrefix, []string{"email"})
	if err != nil {
		t.Fatalf("AppendNames: %v", err)
	}
	if ok {
		t.Fatal("AppendNames returned true despite a prefix mismatch")
	}
	if _, exists := reg.Lookup("email"); exists {
		t.Fatal("email was registered despite the prefix mismatch")
	}
}

func TestPropertyKeyRegistryAppendNamesEmptyPrefixRejected(t *testing.T) {
	t.Parallel()

	reg := NewPropertyKeyRegistry()
	ok, err := reg.AppendNames(nil, []string{"email"})
	if err == nil || ok {
		t.Fatalf("AppendNames(nil prefix) = (%v, %v), want (false, error)", ok, err)
	}
}

func TestPropertyKeyRegistryAppendNamesInvalidPrefixFirst(t *testing.T) {
	t.Parallel()

	reg := NewPropertyKeyRegistry()
	ok, err := reg.AppendNames([]string{"not-reserved"}, []string{"email"})
	if err == nil || ok {
		t.Fatalf("AppendNames(non-reserved prefix[0]) = (%v, %v), want (false, error)", ok, err)
	}
}

func TestPropertyKeyRegistryAppendNamesEmptySuffixIsNoOp(t *testing.T) {
	t.Parallel()

	reg := NewPropertyKeyRegistry()
	if _, err := reg.GetOrCreate("name"); err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}
	prefix := reg.ExportNames()

	ok, err := reg.AppendNames(prefix, nil)
	if err != nil || !ok {
		t.Fatalf("AppendNames(empty suffix) = (%v, %v), want (true, nil)", ok, err)
	}
	if reg.Len() != 1 {
		t.Fatalf("Len() after empty-suffix append = %d, want 1", reg.Len())
	}
}

func TestPropertyKeyRegistryAppendNamesRejectsBlankSuffixEntry(t *testing.T) {
	t.Parallel()

	reg := NewPropertyKeyRegistry()
	if _, err := reg.GetOrCreate("name"); err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}
	prefix := reg.ExportNames()

	ok, err := reg.AppendNames(prefix, []string{"email", " "})
	if err == nil || ok {
		t.Fatalf("AppendNames with a blank suffix entry = (%v, %v), want (false, error)", ok, err)
	}
	if _, exists := reg.Lookup("email"); exists {
		t.Fatal("email was registered despite a later blank entry in the same suffix")
	}
}

func TestPropertyKeyRegistryAppendNamesRejectsDuplicateInSuffix(t *testing.T) {
	t.Parallel()

	reg := NewPropertyKeyRegistry()
	prefix := reg.ExportNames()

	ok, err := reg.AppendNames(prefix, []string{"email", "email"})
	if err == nil || ok {
		t.Fatalf("AppendNames with a duplicate suffix entry = (%v, %v), want (false, error)", ok, err)
	}
}

func TestPropertyKeyRegistryAppendNamesRejectsSuffixClashingWithPrefix(t *testing.T) {
	t.Parallel()

	reg := NewPropertyKeyRegistry()
	if _, err := reg.GetOrCreate("name"); err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}
	prefix := reg.ExportNames()

	ok, err := reg.AppendNames(prefix, []string{"name"})
	if err == nil || ok {
		t.Fatalf("AppendNames with a suffix entry already in prefix = (%v, %v), want (false, error)", ok, err)
	}
}

func TestPropertyKeyRegistryAppendNamesCapacityOverflowRejected(t *testing.T) {
	t.Parallel()

	reg := NewPropertyKeyRegistry()
	reg.mu.Lock()
	for i := 1; i <= 65535; i++ {
		name := fmt.Sprintf("k%d", i)
		reg.toToken[name] = uint16(i)
		reg.toKey = append(reg.toKey, name)
	}
	reg.mu.Unlock()
	prefix := reg.ExportNames()

	ok, err := reg.AppendNames(prefix, []string{"overflow"})
	if err == nil || ok {
		t.Fatalf("AppendNames beyond capacity = (%v, %v), want (false, error)", ok, err)
	}
}
