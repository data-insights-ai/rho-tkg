package graph

import (
	"sync"
	"testing"
)

func TestRelTypeRegistryGetOrCreate(t *testing.T) {
	t.Parallel()

	reg := newRelTypeRegistry()

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

	reg := newRelTypeRegistry()
	tok, _ := reg.GetOrCreate("ACTED_IN")
	got := reg.Resolve(tok)
	if got != "ACTED_IN" {
		t.Errorf("Resolve(%d) = %q, want \"ACTED_IN\"", tok, got)
	}
}

func TestRelTypeRegistryLookupMiss(t *testing.T) {
	t.Parallel()

	reg := newRelTypeRegistry()
	_, ok := reg.Lookup("Nonexistent")
	if ok {
		t.Fatal("Lookup(\"Nonexistent\") should return false")
	}
}

func TestRelTypeRegistryTokenZeroReserved(t *testing.T) {
	t.Parallel()

	reg := newRelTypeRegistry()
	got := reg.Resolve(0)
	if got != "" {
		t.Errorf("Resolve(0) = %q, want empty string (reserved)", got)
	}
}

func TestRelTypeRegistryCapacityError(t *testing.T) {
	t.Parallel()

	reg := newRelTypeRegistry()
	// Fill the registry to capacity (tokens 1..65534).
	reg.mu.Lock()
	for i := uint16(1); i < 65535; i++ {
		name := string(rune('A' + (i % 26)))
		reg.toToken[name+string(rune(i))] = i
		reg.toName = append(reg.toName, name+string(rune(i)))
	}
	reg.nextToken = 65535
	reg.mu.Unlock()

	// Next allocation should return error (65535 is max uint16).
	_, err := reg.GetOrCreate("OneMore")
	if err == nil {
		t.Fatal("GetOrCreate should return error when registry is full")
	}
}

func TestRelTypeRegistryConcurrentGetOrCreate(t *testing.T) {
	t.Parallel()

	reg := newRelTypeRegistry()
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

	reg := newRelTypeRegistry()
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

func TestRelTypeRegistryLen(t *testing.T) {
	t.Parallel()

	reg := newRelTypeRegistry()
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
