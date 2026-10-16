// Package sharded mirrors the v4 sharded store's constructor and Config fields.
// The backend is declined: New returns store.ErrNotSupportedInV5.
package sharded

import (
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/store"
)

// Config carries the v4 knobs consumers pass.
type Config struct {
	InMemory         bool
	BaseSlot         int
	SlotCount        int
	MemTableSize     int64
	ValueLogFileSize int64
}

// Store is the declined backend.
type Store struct{}

// IsV5CompatMemory implements store.Store (never memory).
func (*Store) IsV5CompatMemory() bool { return false }

// New is declined.
func New(Config) (*Store, error) {
	return nil, fmt.Errorf("%w: store/sharded backend", store.ErrNotSupportedInV5)
}
