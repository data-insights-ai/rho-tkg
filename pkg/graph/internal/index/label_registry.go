package index

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
)

// ErrRegistryNotEmpty is returned when importing into a non-empty registry.
var ErrRegistryNotEmpty = errors.New("graph: registry is not empty")

// ErrEmptyName is returned when GetOrCreate is called with an empty or whitespace-only string.
var ErrEmptyName = errors.New("graph: name must not be empty")

const (
	tokenCapacityWarning = 60000
	// TokenCapacityMax is the maximum number of tokens a registry can hold
	// before GetOrCreate returns an error. Exposed for tests that fill the
	// registry to its capacity edge.
	TokenCapacityMax = 65535
)

// LabelRegistry maps label strings to uint16 tokens and back.
// Token 0 is reserved as the zero/invalid value and is never assigned.
// Thread-safe via RWMutex with double-check on write miss.
type LabelRegistry struct {
	mu        sync.RWMutex
	toToken   map[string]uint16
	toLabel   []string // index 0 = "" (reserved)
	nextToken uint16   // starts at 1
	warnOnce  sync.Once
}

// NewLabelRegistry creates a label registry with token 0 reserved.
func NewLabelRegistry() *LabelRegistry {
	return &LabelRegistry{
		toToken:   make(map[string]uint16),
		toLabel:   []string{""}, // index 0 reserved
		nextToken: 1,
	}
}

// GetOrCreate returns the token for name, creating it if it doesn't exist.
// Returns ErrEmptyName if name is empty or whitespace-only.
// Returns an error if the registry is full (65535 tokens).
func (r *LabelRegistry) GetOrCreate(name string) (uint16, error) {
	if strings.TrimSpace(name) == "" {
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

	if len(r.toLabel) > int(TokenCapacityMax) {
		return 0, fmt.Errorf("graph: label registry full (%d tokens)", TokenCapacityMax)
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
				"max", TokenCapacityMax,
			)
		})
	}

	return tok, nil
}

// Resolve returns the label string for the given token.
// Returns "" for token 0 or out-of-range tokens.
func (r *LabelRegistry) Resolve(token uint16) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if int(token) >= len(r.toLabel) {
		return ""
	}
	return r.toLabel[token]
}

// ResolveAll resolves a batch of tokens to label strings.
func (r *LabelRegistry) ResolveAll(tokens []uint16) []string {
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
func (r *LabelRegistry) Lookup(name string) (uint16, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	tok, ok := r.toToken[name]
	return tok, ok
}

// Len returns the number of registered labels (excluding reserved token 0).
func (r *LabelRegistry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.toLabel) - 1
}

// ExportNames returns a copy of the registered names slice for persistence.
// Index 0 is always "" (reserved). Read-locked.
func (r *LabelRegistry) ExportNames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	cp := make([]string, len(r.toLabel))
	copy(cp, r.toLabel)
	return cp
}

// ImportNames restores the registry from persisted data. Write-locked.
// The registry must be empty (freshly constructed). names[0] must be "".
func (r *LabelRegistry) ImportNames(names []string) error {
	if len(names) == 0 {
		return errors.New("graph: import: names slice must not be empty")
	}
	if names[0] != "" {
		return errors.New("graph: import: names[0] must be empty (reserved)")
	}
	if len(names)-1 > int(TokenCapacityMax) {
		return fmt.Errorf("graph: label import: %d entries exceeds registry capacity (%d)",
			len(names)-1, TokenCapacityMax)
	}

	// Validate entries before acquiring lock: reject empty or duplicate names.
	seen := make(map[string]struct{}, len(names)-1)
	for i := 1; i < len(names); i++ {
		if strings.TrimSpace(names[i]) == "" {
			return fmt.Errorf("graph: label import: empty name at index %d", i)
		}
		if _, dup := seen[names[i]]; dup {
			return fmt.Errorf("graph: label import: duplicate name %q", names[i])
		}
		seen[names[i]] = struct{}{}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.toLabel) > 1 {
		return ErrRegistryNotEmpty
	}

	r.toLabel = make([]string, len(names))
	copy(r.toLabel, names)

	r.toToken = make(map[string]uint16, len(names)-1)
	for i := 1; i < len(names); i++ {
		r.toToken[names[i]] = uint16(i)
	}

	r.nextToken = uint16(len(names)) // #nosec G115 — bounds checked at function entry against TokenCapacityMax
	return nil
}
