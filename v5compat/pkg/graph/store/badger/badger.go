// Package badger mirrors the v4 badger store's constructor. The backend is
// declined: a directory graph is graph.Config.BadgerDir (a v5 partition).
package badger

import (
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/store"
)

// Config carries the v4 knobs consumers pass.
type Config struct{ Dir string }

// Store is the declined backend.
type Store struct{}

// IsV5CompatMemory implements store.Store (never memory).
func (*Store) IsV5CompatMemory() bool { return false }

// New is declined.
func New(Config) (*Store, error) {
	return nil, fmt.Errorf("%w: store/badger backend (use graph.Config.BadgerDir)", store.ErrNotSupportedInV5)
}
