package graph

import (
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
	// Fill the registry to capacity (tokens 1..65534).
	reg.mu.Lock()
	for i := uint16(1); i < 65535; i++ {
		name := string(rune('A' + (i % 26)))
		reg.toToken[name+string(rune(i))] = i
		reg.toLabel = append(reg.toLabel, name+string(rune(i)))
	}
	reg.nextToken = 65535
	reg.mu.Unlock()

	// Next allocation should return error (65535 is max uint16).
	_, err := reg.GetOrCreate("OneMore")
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
