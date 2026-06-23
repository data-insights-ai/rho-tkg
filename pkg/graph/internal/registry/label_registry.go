// Package registry provides string-to-uint16 token registries shared by the
// label and relationship-type indexes. Hosting these registries in a
// dedicated package (instead of in internal/index) avoids forcing
// store-level packages — which need only the token mapping — to depend on
// the broader index package.
package registry

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"unicode"
	"unicode/utf8"
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
	initOnce    sync.Once
	initialized atomic.Bool
	mu          sync.RWMutex
	toToken     map[string]uint16
	toLabel     []string // index 0 = "" (reserved)
	nextToken   uint16   // starts at 1
	warnOnce    sync.Once
}

// NewLabelRegistry creates a label registry with token 0 reserved.
func NewLabelRegistry() *LabelRegistry {
	r := &LabelRegistry{
		toToken:   make(map[string]uint16),
		toLabel:   []string{""}, // index 0 reserved
		nextToken: 1,
	}
	r.initialized.Store(true)
	return r
}

func (r *LabelRegistry) ensureInitialized() {
	if r.initialized.Load() {
		return
	}
	r.initOnce.Do(func() {
		if len(r.toLabel) == 0 {
			r.toLabel = []string{""}
		}
		if r.toToken == nil {
			r.toToken = make(map[string]uint16, len(r.toLabel)-1)
			for i := 1; i < len(r.toLabel); i++ {
				r.toToken[r.toLabel[i]] = uint16(i) // #nosec G115 — registry state is capacity-bounded by import/allocation paths.
			}
		}
		if r.nextToken == 0 && len(r.toLabel) > 0 {
			r.nextToken = uint16(len(r.toLabel)) // #nosec G115 — registry state is capacity-bounded by import/allocation paths.
		}
		r.initialized.Store(true)
	})
}

func isBlankName(name string) bool {
	if name == "" {
		return true
	}
	for i := 0; i < len(name); {
		c := name[i]
		if c < utf8.RuneSelf {
			if c != ' ' && (c < '\t' || c > '\r') {
				return false
			}
			i++
			continue
		}
		r, size := utf8.DecodeRuneInString(name[i:])
		if !unicode.IsSpace(r) {
			return false
		}
		i += size
	}
	return true
}

// GetOrCreate returns the token for name, creating it if it doesn't exist.
// Returns ErrEmptyName if name is empty or whitespace-only.
// Returns an error if the registry is full (65535 tokens).
func (r *LabelRegistry) GetOrCreate(name string) (uint16, error) {
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
	r.ensureInitialized()

	r.mu.RLock()
	defer r.mu.RUnlock()

	if int(token) >= len(r.toLabel) {
		return ""
	}
	return r.toLabel[token]
}

// ResolveAll resolves a batch of tokens to label strings.
func (r *LabelRegistry) ResolveAll(tokens []uint16) []string {
	r.ensureInitialized()

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
	r.ensureInitialized()

	r.mu.RLock()
	defer r.mu.RUnlock()

	tok, ok := r.toToken[name]
	return tok, ok
}

// Len returns the number of registered labels (excluding reserved token 0).
func (r *LabelRegistry) Len() int {
	r.ensureInitialized()

	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.toLabel) - 1
}

// ExportNames returns a copy of the registered names slice for persistence.
// Index 0 is always "" (reserved). Read-locked.
func (r *LabelRegistry) ExportNames() []string {
	r.ensureInitialized()

	r.mu.RLock()
	defer r.mu.RUnlock()

	cp := make([]string, len(r.toLabel))
	copy(cp, r.toLabel)
	return cp
}

// ImportNames restores the registry from persisted data. Write-locked.
// The registry must be empty (freshly constructed). names[0] must be "".
func (r *LabelRegistry) ImportNames(names []string) error {
	r.ensureInitialized()

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
		if isBlankName(names[i]) {
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

	// Fire the capacity warning if the imported set already exceeds the
	// threshold; otherwise a large restore could push the registry past
	// tokenCapacityWarning without any operator-visible signal until the
	// next GetOrCreate happens to allocate a new token.
	if len(r.toLabel) > tokenCapacityWarning {
		r.warnOnce.Do(func() {
			slog.Warn("label registry approaching capacity",
				"count", len(r.toLabel)-1,
				"max", TokenCapacityMax,
			)
		})
	}

	return nil
}

// RollbackNames restores a previous registry snapshot when the current
// registry contains exactly that snapshot plus the supplied newly allocated
// suffix names. It returns false without mutating if the registry has changed
// in any other way.
func (r *LabelRegistry) RollbackNames(snapshot []string, allocated ...string) (bool, error) {
	r.ensureInitialized()

	if len(snapshot) == 0 {
		return false, errors.New("graph: rollback: snapshot must not be empty")
	}
	if snapshot[0] != "" {
		return false, errors.New("graph: rollback: snapshot[0] must be empty (reserved)")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.toLabel) != len(snapshot)+len(allocated) {
		return false, nil
	}
	for i := range snapshot {
		if r.toLabel[i] != snapshot[i] {
			return false, nil
		}
	}
	for i, name := range allocated {
		if r.toLabel[len(snapshot)+i] != name {
			return false, nil
		}
	}

	r.toLabel = make([]string, len(snapshot))
	copy(r.toLabel, snapshot)
	r.toToken = make(map[string]uint16, len(snapshot)-1)
	for i := 1; i < len(snapshot); i++ {
		r.toToken[snapshot[i]] = uint16(i)
	}
	r.nextToken = uint16(len(snapshot)) // #nosec G115 — snapshot came from a capacity-bounded registry
	return true, nil
}

// AppendNames extends the registry by the suffix names, provided the registry's
// current contents exactly equal prefix (the snapshot the caller observed). It
// is the append-only GROW operation a log-shipped replica uses to extend its
// token registry with a primary's newly-allocated names — ImportNames is
// load-only (it rejects a non-empty registry), and the registry is gap-free and
// monotone, so a replica that has seen token T needs only the name-suffix up to
// T. Returns (false, nil) WITHOUT mutating when the registry has diverged from
// prefix (a local allocation happened since the snapshot); returns an error on a
// malformed suffix or capacity overflow. Write-locked. The inverse of
// RollbackNames (which shrinks back to a snapshot); this grows past one.
func (r *LabelRegistry) AppendNames(prefix []string, suffix []string) (bool, error) {
	r.ensureInitialized()

	if len(prefix) == 0 {
		return false, errors.New("graph: append: prefix must not be empty")
	}
	if prefix[0] != "" {
		return false, errors.New("graph: append: prefix[0] must be empty (reserved)")
	}
	if len(suffix) == 0 {
		return true, nil // nothing to append (prefix-equality still asserted below)
	}
	if (len(prefix)-1)+len(suffix) > int(TokenCapacityMax) {
		return false, fmt.Errorf("graph: label append: %d entries exceeds registry capacity (%d)",
			(len(prefix)-1)+len(suffix), TokenCapacityMax)
	}

	// Validate the suffix before locking: non-blank, no dups within the suffix,
	// no collision with a prefix name.
	pset := make(map[string]struct{}, len(prefix)-1)
	for i := 1; i < len(prefix); i++ {
		pset[prefix[i]] = struct{}{}
	}
	sset := make(map[string]struct{}, len(suffix))
	for i, name := range suffix {
		if isBlankName(name) {
			return false, fmt.Errorf("graph: label append: empty name at suffix index %d", i)
		}
		if _, dup := sset[name]; dup {
			return false, fmt.Errorf("graph: label append: duplicate name %q in suffix", name)
		}
		if _, clash := pset[name]; clash {
			return false, fmt.Errorf("graph: label append: suffix name %q already in prefix", name)
		}
		sset[name] = struct{}{}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// The registry's current contents must exactly equal the caller's observed
	// prefix; otherwise it diverged locally and we must not append.
	if len(r.toLabel) != len(prefix) {
		return false, nil
	}
	for i := range prefix {
		if r.toLabel[i] != prefix[i] {
			return false, nil
		}
	}

	base := len(r.toLabel)
	r.toLabel = append(r.toLabel, suffix...)
	for i, name := range suffix {
		r.toToken[name] = uint16(base + i) // #nosec G115 — bounded by capacity check above
	}
	r.nextToken = uint16(len(r.toLabel)) // #nosec G115 — bounded by capacity check above

	if len(r.toLabel) > tokenCapacityWarning {
		r.warnOnce.Do(func() {
			slog.Warn("label registry approaching capacity",
				"count", len(r.toLabel)-1,
				"max", TokenCapacityMax,
			)
		})
	}
	return true, nil
}
