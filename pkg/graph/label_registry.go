package graph

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
)

// ErrEmptyName is returned when GetOrCreate is called with an empty string.
var ErrEmptyName = errors.New("graph: name must not be empty")

const (
	tokenCapacityWarning = 60000
	tokenCapacityMax     = 65535
)

// labelRegistry maps label strings to uint16 tokens and back.
// Token 0 is reserved as the zero/invalid value and is never assigned.
// Thread-safe via RWMutex with double-check on write miss.
type labelRegistry struct {
	mu        sync.RWMutex
	toToken   map[string]uint16
	toLabel   []string // index 0 = "" (reserved)
	nextToken uint16   // starts at 1
	warnOnce  sync.Once
}

// newLabelRegistry creates a label registry with token 0 reserved.
func newLabelRegistry() *labelRegistry {
	return &labelRegistry{
		toToken:   make(map[string]uint16),
		toLabel:   []string{""}, // index 0 reserved
		nextToken: 1,
	}
}

// GetOrCreate returns the token for name, creating it if it doesn't exist.
// Returns ErrEmptyName if name is empty. Returns an error if the registry is full (65535 tokens).
func (r *labelRegistry) GetOrCreate(name string) (uint16, error) {
	if name == "" {
		return 0, ErrEmptyName
	}

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

	if len(r.toLabel) > int(tokenCapacityMax) {
		return 0, fmt.Errorf("graph: label registry full (%d tokens)", tokenCapacityMax)
	}

	tok = r.nextToken
	r.toToken[name] = tok
	r.toLabel = append(r.toLabel, name)
	r.nextToken++ // may wrap to 0 after assigning 65535; the len check above
	// prevents any further assignments, so the wrapped value is never used.

	if int(tok) >= tokenCapacityWarning {
		r.warnOnce.Do(func() {
			slog.Warn("label registry approaching capacity",
				"count", tok,
				"max", tokenCapacityMax,
			)
		})
	}

	return tok, nil
}

// Resolve returns the label string for the given token.
// Returns "" for token 0 or out-of-range tokens.
func (r *labelRegistry) Resolve(token uint16) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if int(token) >= len(r.toLabel) {
		return ""
	}
	return r.toLabel[token]
}

// ResolveAll resolves a batch of tokens to label strings.
func (r *labelRegistry) ResolveAll(tokens []uint16) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]string, len(tokens))
	for i, tok := range tokens {
		if int(tok) < len(r.toLabel) {
			out[i] = r.toLabel[tok]
		}
	}
	return out
}

// Lookup returns the token for name without creating it.
// Returns false if the name is not registered.
func (r *labelRegistry) Lookup(name string) (uint16, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	tok, ok := r.toToken[name]
	return tok, ok
}

// Len returns the number of registered labels (excluding reserved token 0).
func (r *labelRegistry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.toLabel) - 1
}
