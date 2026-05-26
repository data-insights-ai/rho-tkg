package registry

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
)

// PropertyKeyRegistry maps property key strings to uint16 tokens and back.
// Used by the wire encoder to dictionary-encode property keys on disk,
// reducing per-entity storage for property-heavy workloads.
//
// Property-key cardinality is typically higher than label cardinality (some
// callers put UUIDs or timestamps in keys). When the registry approaches its
// uint16 ceiling, the encoder falls back to writing the raw key string
// instead of a token — slightly larger on disk but does not fail the write.
//
// Token 0 is reserved as the zero/invalid value and is never assigned.
// Thread-safe via RWMutex with double-check on write miss.
type PropertyKeyRegistry struct {
	initOnce    sync.Once
	initialized atomic.Bool
	mu          sync.RWMutex
	toToken     map[string]uint16
	toKey       []string // index 0 = "" (reserved)
	nextToken   uint16   // starts at 1
	warnOnce    sync.Once
	capacityHit atomic.Bool // sticky once the registry has refused an allocation
}

// NewPropertyKeyRegistry creates an empty registry with token 0 reserved.
func NewPropertyKeyRegistry() *PropertyKeyRegistry {
	r := &PropertyKeyRegistry{
		toToken:   make(map[string]uint16),
		toKey:     []string{""}, // index 0 reserved
		nextToken: 1,
	}
	r.initialized.Store(true)
	return r
}

func (r *PropertyKeyRegistry) ensureInitialized() {
	if r.initialized.Load() {
		return
	}
	r.initOnce.Do(func() {
		if len(r.toKey) == 0 {
			r.toKey = []string{""}
		}
		if r.toToken == nil {
			r.toToken = make(map[string]uint16, len(r.toKey)-1)
			for i := 1; i < len(r.toKey); i++ {
				r.toToken[r.toKey[i]] = uint16(i) // #nosec G115
			}
		}
		if r.nextToken == 0 && len(r.toKey) > 0 {
			r.nextToken = uint16(len(r.toKey)) // #nosec G115
		}
		r.initialized.Store(true)
	})
}

// GetOrCreate returns the token for key, allocating one on first sight.
// Returns (0, nil) — NOT an error — when the registry is full. Callers
// (the wire encoder) treat token 0 as "fall back to writing the raw key
// string in the wire format" so high-cardinality workloads still complete.
// Returns ErrEmptyName if key is empty.
func (r *PropertyKeyRegistry) GetOrCreate(key string) (uint16, error) {
	if key == "" {
		return 0, ErrEmptyName
	}
	r.ensureInitialized()

	r.mu.RLock()
	tok, ok := r.toToken[key]
	r.mu.RUnlock()
	if ok {
		return tok, nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if tok, ok := r.toToken[key]; ok {
		return tok, nil
	}

	if len(r.toKey) > int(TokenCapacityMax) {
		if !r.capacityHit.Load() {
			r.capacityHit.Store(true)
			slog.Warn("property-key registry full — falling back to raw key on wire",
				"max", TokenCapacityMax,
			)
		}
		return 0, nil
	}

	tok = r.nextToken
	r.toToken[key] = tok
	r.toKey = append(r.toKey, key)
	r.nextToken++

	if int(tok) >= tokenCapacityWarning {
		r.warnOnce.Do(func() {
			slog.Warn("property-key registry approaching capacity",
				"count", tok,
				"max", TokenCapacityMax,
			)
		})
	}

	return tok, nil
}

// Resolve returns the key string for the given token. Returns "" for token 0
// or out-of-range tokens.
func (r *PropertyKeyRegistry) Resolve(token uint16) string {
	r.ensureInitialized()

	r.mu.RLock()
	defer r.mu.RUnlock()

	if int(token) >= len(r.toKey) {
		return ""
	}
	return r.toKey[token]
}

// Lookup returns the token for key without creating it.
func (r *PropertyKeyRegistry) Lookup(key string) (uint16, bool) {
	r.ensureInitialized()

	r.mu.RLock()
	defer r.mu.RUnlock()

	tok, ok := r.toToken[key]
	return tok, ok
}

// Len returns the number of registered keys (excluding reserved token 0).
func (r *PropertyKeyRegistry) Len() int {
	r.ensureInitialized()

	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.toKey) - 1
}

// ExportNames returns a copy of the registered names for persistence.
// Index 0 is always "" (reserved).
func (r *PropertyKeyRegistry) ExportNames() []string {
	r.ensureInitialized()

	r.mu.RLock()
	defer r.mu.RUnlock()

	cp := make([]string, len(r.toKey))
	copy(cp, r.toKey)
	return cp
}

// ImportNames restores the registry from persisted data. The registry must be
// empty (freshly constructed). names[0] must be "".
func (r *PropertyKeyRegistry) ImportNames(names []string) error {
	r.ensureInitialized()

	if len(names) == 0 {
		return errors.New("graph: property-key import: names slice must not be empty")
	}
	if names[0] != "" {
		return errors.New("graph: property-key import: names[0] must be empty (reserved)")
	}
	if len(names)-1 > int(TokenCapacityMax) {
		return fmt.Errorf("graph: property-key import: %d entries exceeds registry capacity (%d)",
			len(names)-1, TokenCapacityMax)
	}

	seen := make(map[string]struct{}, len(names)-1)
	for i := 1; i < len(names); i++ {
		if names[i] == "" {
			return fmt.Errorf("graph: property-key import: empty name at index %d", i)
		}
		if _, dup := seen[names[i]]; dup {
			return fmt.Errorf("graph: property-key import: duplicate name %q", names[i])
		}
		seen[names[i]] = struct{}{}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.toKey) > 1 {
		return ErrRegistryNotEmpty
	}

	r.toKey = make([]string, len(names))
	copy(r.toKey, names)
	r.toToken = make(map[string]uint16, len(names)-1)
	for i := 1; i < len(names); i++ {
		r.toToken[names[i]] = uint16(i)
	}
	r.nextToken = uint16(len(names))

	if len(r.toKey) > tokenCapacityWarning {
		r.warnOnce.Do(func() {
			slog.Warn("property-key registry approaching capacity",
				"count", len(r.toKey)-1,
				"max", TokenCapacityMax,
			)
		})
	}
	return nil
}
