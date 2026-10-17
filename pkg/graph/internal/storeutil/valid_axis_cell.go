package storeutil

import (
	"sync/atomic"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ValidAxisCell holds a store's valid axis (store.ValidAxisCapability) for
// lock-free reads: set once at open (and on a mode change of an empty
// store), read on every valid-time door. The zero cell reads the legacy axis.
type ValidAxisCell struct {
	p atomic.Pointer[types.ValidAxis]
}

var (
	legacyAxis  = types.LegacyValidAxis
	literalAxis = types.LiteralValidAxis
)

// Get returns the store's axis.
func (c *ValidAxisCell) Get() types.ValidAxis {
	if a := c.p.Load(); a != nil {
		return *a
	}
	return types.LegacyValidAxis
}

// Set fixes the store's axis.
func (c *ValidAxisCell) Set(a types.ValidAxis) {
	if a.Literal() {
		c.p.Store(&literalAxis)
		return
	}
	c.p.Store(&legacyAxis)
}
