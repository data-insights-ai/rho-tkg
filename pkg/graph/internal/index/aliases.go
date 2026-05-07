package index

import (
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
)

// Backward-compat re-exports for the registry types and helpers, which now
// live in pkg/graph/internal/registry. Internal callers (badgerstore,
// tieredstore, graph layer, tests) reference these names today; aliasing
// them here lets the move land without churning every caller in one go.

// LabelRegistry maps label strings to uint16 tokens.
type LabelRegistry = registry.LabelRegistry

// RelTypeRegistry maps relationship-type strings to uint16 tokens.
type RelTypeRegistry = registry.RelTypeRegistry

// NewLabelRegistry constructs an empty LabelRegistry.
func NewLabelRegistry() *LabelRegistry { return registry.NewLabelRegistry() }

// NewRelTypeRegistry constructs an empty RelTypeRegistry.
func NewRelTypeRegistry() *RelTypeRegistry { return registry.NewRelTypeRegistry() }

// Sentinel errors are declared in package registry; re-exported here for
// callers that still resolve them through pkg/graph/internal/index.
var (
	ErrEmptyName        = registry.ErrEmptyName
	ErrRegistryNotEmpty = registry.ErrRegistryNotEmpty
)

// TokenCapacityMax mirrors registry.TokenCapacityMax for tests that fill the
// registry to its capacity edge through the index re-export.
const TokenCapacityMax = registry.TokenCapacityMax
