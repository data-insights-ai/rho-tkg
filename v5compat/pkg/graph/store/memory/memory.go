// Package memory mirrors v4 store/memory: New selects the in-memory graph when
// passed as graph.Config.Store. It is the only v4 backend v5compat accepts.
package memory

// Store is the in-memory backend marker.
type Store struct{}

// New returns the in-memory backend marker.
func New() *Store { return &Store{} }

// IsV5CompatMemory implements store.Store.
func (*Store) IsV5CompatMemory() bool { return true }
