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
//
// Unlike LabelRegistry/RelTypeRegistry, this registry has no RollbackNames —
// intentional, not an oversight (BACKLOG 15o). RollbackNames exists on the
// other two because a failed tx/import/replica-apply can leave them having
// grown past a snapshot that must be undone. PropertyKeyRegistry never faces
// that: (1) lesson 37 — growth here is capacity-soft and never fails a write
// (overflow returns token 0, encoders fall back to the raw key string), so
// there is no failed-operation-that-grew-the-registry case to roll back; and
// (2) property keys are never synced from a replication primary (records
// carry UNTOKENIZED string keys — see CLAUDE.md's replication notes), so a
// replica tokenizes them purely locally and has no primary-driven growth to
// undo either.
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
// Returns ErrEmptyName if key is blank (empty or all-whitespace).
//
// Blank rejection matches the sibling registries' GetOrCreate and this
// registry's own AppendNames (the replica grow door), so a blank key can never
// be tokenized — the wire encoder already ignores this error and falls back to
// the raw key (token 0), so a degenerate blank key still round-trips, it just
// never enters the token table to later diverge the grow door. (ImportNames
// stays lenient so a legacy registry that tokenized a blank key before this
// guard still loads.)
func (r *PropertyKeyRegistry) GetOrCreate(key string) (uint16, error) {
	if isBlankName(key) {
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

// AppendNames extends the registry by the suffix keys when its current contents
// exactly equal prefix. The property-key counterpart of
// LabelRegistry.AppendNames, added for three-way registry parity. (The replica
// apply path tokenizes property keys LOCALLY from untokenized record wires, so
// it does not strictly need to sync property-key tokens — but RegistrySnapshot
// ships them and this keeps the grow primitive uniform across all registries.)
// Returns (false, nil) without mutating on prefix divergence; an error on a
// malformed suffix / capacity overflow. Write-locked.
func (r *PropertyKeyRegistry) AppendNames(prefix []string, suffix []string) (bool, error) {
	r.ensureInitialized()

	if len(prefix) == 0 {
		return false, errors.New("graph: append: prefix must not be empty")
	}
	if prefix[0] != "" {
		return false, errors.New("graph: append: prefix[0] must be empty (reserved)")
	}
	if len(suffix) == 0 {
		return true, nil
	}
	if (len(prefix)-1)+len(suffix) > int(TokenCapacityMax) {
		return false, fmt.Errorf("graph: property-key append: %d entries exceeds registry capacity (%d)",
			(len(prefix)-1)+len(suffix), TokenCapacityMax)
	}

	pset := make(map[string]struct{}, len(prefix)-1)
	for i := 1; i < len(prefix); i++ {
		pset[prefix[i]] = struct{}{}
	}
	sset := make(map[string]struct{}, len(suffix))
	for i, name := range suffix {
		if isBlankName(name) {
			return false, fmt.Errorf("graph: property-key append: blank name at suffix index %d", i)
		}
		if _, dup := sset[name]; dup {
			return false, fmt.Errorf("graph: property-key append: duplicate name %q in suffix", name)
		}
		if _, clash := pset[name]; clash {
			return false, fmt.Errorf("graph: property-key append: suffix name %q already in prefix", name)
		}
		sset[name] = struct{}{}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.toKey) != len(prefix) {
		return false, nil
	}
	for i := range prefix {
		if r.toKey[i] != prefix[i] {
			return false, nil
		}
	}

	base := len(r.toKey)
	r.toKey = append(r.toKey, suffix...)
	for i, name := range suffix {
		r.toToken[name] = uint16(base + i) // #nosec G115 — bounded by capacity check above
	}
	r.nextToken = uint16(len(r.toKey)) // #nosec G115 — bounded by capacity check above

	if len(r.toKey) > tokenCapacityWarning {
		r.warnOnce.Do(func() {
			slog.Warn("property-key registry approaching capacity",
				"count", len(r.toKey)-1,
				"max", TokenCapacityMax,
			)
		})
	}
	return true, nil
}
