package registry

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
)

// RelTypeRegistry maps relationship type strings to uint16 tokens and back.
// Token 0 is reserved as the zero/invalid value and is never assigned.
// Thread-safe via RWMutex with double-check on write miss.
type RelTypeRegistry struct {
	initOnce    sync.Once
	initialized atomic.Bool
	mu          sync.RWMutex
	toToken     map[string]uint16
	toName      []string // index 0 = "" (reserved)
	nextToken   uint16   // starts at 1
	warnOnce    sync.Once
}

// NewRelTypeRegistry creates a relationship type registry with token 0 reserved.
func NewRelTypeRegistry() *RelTypeRegistry {
	r := &RelTypeRegistry{
		toToken:   make(map[string]uint16),
		toName:    []string{""}, // index 0 reserved
		nextToken: 1,
	}
	r.initialized.Store(true)
	return r
}

func (r *RelTypeRegistry) ensureInitialized() {
	if r.initialized.Load() {
		return
	}
	r.initOnce.Do(func() {
		if len(r.toName) == 0 {
			r.toName = []string{""}
		}
		if r.toToken == nil {
			r.toToken = make(map[string]uint16, len(r.toName)-1)
			for i := 1; i < len(r.toName); i++ {
				r.toToken[r.toName[i]] = uint16(i) // #nosec G115 — registry state is capacity-bounded by import/allocation paths.
			}
		}
		if r.nextToken == 0 && len(r.toName) > 0 {
			r.nextToken = uint16(len(r.toName)) // #nosec G115 — registry state is capacity-bounded by import/allocation paths.
		}
		r.initialized.Store(true)
	})
}

// GetOrCreate returns the token for name, creating it if it doesn't exist.
// Returns ErrEmptyName if name is empty or whitespace-only.
// Returns an error if the registry is full (65535 tokens).
func (r *RelTypeRegistry) GetOrCreate(name string) (uint16, error) {
	if isBlankName(name) {
		return 0, ErrEmptyName
	}
	r.ensureInitialized()

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

	if len(r.toName) > int(TokenCapacityMax) {
		return 0, fmt.Errorf("graph: reltype registry full (%d tokens)", TokenCapacityMax)
	}

	tok = r.nextToken
	r.toToken[name] = tok
	r.toName = append(r.toName, name)
	r.nextToken++ // may wrap to 0 after assigning 65535; the len check above
	// prevents any further assignments, so the wrapped value is never used.

	if int(tok) >= tokenCapacityWarning {
		r.warnOnce.Do(func() {
			slog.Warn("reltype registry approaching capacity",
				"count", tok,
				"max", TokenCapacityMax,
			)
		})
	}

	return tok, nil
}

// Resolve returns the relationship type string for the given token.
// Returns "" for token 0 or out-of-range tokens.
func (r *RelTypeRegistry) Resolve(token uint16) string {
	r.ensureInitialized()

	r.mu.RLock()
	defer r.mu.RUnlock()

	if int(token) >= len(r.toName) {
		return ""
	}
	return r.toName[token]
}

// ResolveAll resolves a batch of tokens to relationship type strings.
func (r *RelTypeRegistry) ResolveAll(tokens []uint16) []string {
	r.ensureInitialized()

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
func (r *RelTypeRegistry) Lookup(name string) (uint16, bool) {
	r.ensureInitialized()

	r.mu.RLock()
	defer r.mu.RUnlock()

	tok, ok := r.toToken[name]
	return tok, ok
}

// Len returns the number of registered relationship types (excluding reserved token 0).
func (r *RelTypeRegistry) Len() int {
	r.ensureInitialized()

	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.toName) - 1
}

// ExportNames returns a copy of the registered names slice for persistence.
// Index 0 is always "" (reserved). Read-locked.
func (r *RelTypeRegistry) ExportNames() []string {
	r.ensureInitialized()

	r.mu.RLock()
	defer r.mu.RUnlock()

	cp := make([]string, len(r.toName))
	copy(cp, r.toName)
	return cp
}

// ImportNames restores the registry from persisted data. Write-locked.
// The registry must be empty (freshly constructed). names[0] must be "".
func (r *RelTypeRegistry) ImportNames(names []string) error {
	r.ensureInitialized()

	if len(names) == 0 {
		return errors.New("graph: import: names slice must not be empty")
	}
	if names[0] != "" {
		return errors.New("graph: import: names[0] must be empty (reserved)")
	}
	if len(names)-1 > int(TokenCapacityMax) {
		return fmt.Errorf("graph: reltype import: %d entries exceeds registry capacity (%d)",
			len(names)-1, TokenCapacityMax)
	}

	// Validate entries before acquiring lock: reject empty or duplicate names.
	seen := make(map[string]struct{}, len(names)-1)
	for i := 1; i < len(names); i++ {
		if isBlankName(names[i]) {
			return fmt.Errorf("graph: reltype import: empty name at index %d", i)
		}
		if _, dup := seen[names[i]]; dup {
			return fmt.Errorf("graph: reltype import: duplicate name %q", names[i])
		}
		seen[names[i]] = struct{}{}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.toName) > 1 {
		return ErrRegistryNotEmpty
	}

	r.toName = make([]string, len(names))
	copy(r.toName, names)

	r.toToken = make(map[string]uint16, len(names)-1)
	for i := 1; i < len(names); i++ {
		r.toToken[names[i]] = uint16(i)
	}

	r.nextToken = uint16(len(names)) // #nosec G115 — bounds checked at function entry against TokenCapacityMax
	return nil
}

// RollbackNames restores a previous registry snapshot when the current
// registry contains exactly that snapshot plus the supplied newly allocated
// suffix names. It returns false without mutating if the registry has changed
// in any other way.
func (r *RelTypeRegistry) RollbackNames(snapshot []string, allocated ...string) (bool, error) {
	r.ensureInitialized()

	if len(snapshot) == 0 {
		return false, errors.New("graph: rollback: snapshot must not be empty")
	}
	if snapshot[0] != "" {
		return false, errors.New("graph: rollback: snapshot[0] must be empty (reserved)")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.toName) != len(snapshot)+len(allocated) {
		return false, nil
	}
	for i := range snapshot {
		if r.toName[i] != snapshot[i] {
			return false, nil
		}
	}
	for i, name := range allocated {
		if r.toName[len(snapshot)+i] != name {
			return false, nil
		}
	}

	r.toName = make([]string, len(snapshot))
	copy(r.toName, snapshot)
	r.toToken = make(map[string]uint16, len(snapshot)-1)
	for i := 1; i < len(snapshot); i++ {
		r.toToken[snapshot[i]] = uint16(i)
	}
	r.nextToken = uint16(len(snapshot)) // #nosec G115 — snapshot came from a capacity-bounded registry
	return true, nil
}
