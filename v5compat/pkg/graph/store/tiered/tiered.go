// Package tiered mirrors the v4 tiered store's constructor and Config fields
// consumers set. The backend is declined: New returns store.ErrNotSupportedInV5
// (one v5 partition has no shard windows or tiers; tier admin is declined too).
package tiered

import (
	"fmt"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/store"
)

// Config carries the v4 knobs consumers pass.
type Config struct {
	DataDir       string
	InMemory      bool
	ShardWindow   time.Duration
	RefLabels     []string
	FlushInterval time.Duration
}

// Store is the declined backend.
type Store struct{}

// IsV5CompatMemory implements store.Store (never memory).
func (*Store) IsV5CompatMemory() bool { return false }

// New is declined.
func New(Config) (*Store, error) {
	return nil, fmt.Errorf("%w: store/tiered backend", store.ErrNotSupportedInV5)
}
