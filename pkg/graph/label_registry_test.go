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
