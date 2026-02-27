package graph

import (
	"fmt"
	"log/slog"
	"sync"
)

// relTypeRegistry maps relationship type strings to uint16 tokens and back.
// Token 0 is reserved as the zero/invalid value and is never assigned.
// Thread-safe via RWMutex with double-check on write miss.
type relTypeRegistry struct {
	mu        sync.RWMutex
	toToken   map[string]uint16
	toName    []string // index 0 = "" (reserved)
	nextToken uint16   // starts at 1
	warnOnce  sync.Once
}

// newRelTypeRegistry creates a relationship type registry with token 0 reserved.
func newRelTypeRegistry() *relTypeRegistry {
	return &relTypeRegistry{
		toToken:   make(map[string]uint16),
		toName:    []string{""}, // index 0 reserved
		nextToken: 1,
	}
}

// GetOrCreate returns the token for name, creating it if it doesn't exist.
// Returns an error if the registry is full (65535 tokens).
func (r *relTypeRegistry) GetOrCreate(name string) (uint16, error) {
	// Fast path: read lock.
	r.mu.RLock()
	tok, ok := r.toToken[name]
	r.mu.RUnlock()
	if ok {
		return tok, nil
	}

	// Slow path: write lock with double-check.
	r.mu.Lock()
	defer r.mu.Unlock()

	if tok, ok := r.toToken[name]; ok {
		return tok, nil
	}

	if r.nextToken >= tokenCapacityMax {
		return 0, fmt.Errorf("graph: reltype registry full (%d tokens)", tokenCapacityMax)
	}

	tok = r.nextToken
	r.toToken[name] = tok
	r.toName = append(r.toName, name)
	r.nextToken++

	if int(tok) >= tokenCapacityWarning {
		r.warnOnce.Do(func() {
			slog.Warn("reltype registry approaching capacity",
				"count", tok,
				"max", tokenCapacityMax,
			)
		})
	}

	return tok, nil
}

// Resolve returns the relationship type string for the given token.
// Returns "" for token 0 or out-of-range tokens.
func (r *relTypeRegistry) Resolve(token uint16) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if int(token) >= len(r.toName) {
		return ""
	}
	return r.toName[token]
}

// ResolveAll resolves a batch of tokens to relationship type strings.
func (r *relTypeRegistry) ResolveAll(tokens []uint16) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]string, len(tokens))
	for i, tok := range tokens {
		if int(tok) < len(r.toName) {
			out[i] = r.toName[tok]
		}
	}
	return out
}

// Lookup returns the token for name without creating it.
// Returns false if the name is not registered.
func (r *relTypeRegistry) Lookup(name string) (uint16, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	tok, ok := r.toToken[name]
	return tok, ok
}

// Len returns the number of registered relationship types (excluding reserved token 0).
func (r *relTypeRegistry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.toName) - 1
}
