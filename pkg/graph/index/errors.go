package index

import (
	"errors"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store"
)

// ErrIndexProviderExists is returned by g.Index.RegisterProvider /
// RegisterLegacyProvider when a provider with the same Name is
// already registered.
var ErrIndexProviderExists = errors.New("graph: index provider already registered")

// ErrIndexProviderNotFound is returned by g.Index.UnregisterProvider
// when no provider with the given name is registered.
var ErrIndexProviderNotFound = errors.New("graph: index provider not found")

// ErrIndexProviderEmptyName is returned by g.Index.RegisterProvider /
// RegisterLegacyProvider when the provider's Name() is empty or
// whitespace-only. Names are the registry key.
var ErrIndexProviderEmptyName = errors.New("graph: index provider Name must be non-empty")

// Index sentinel errors returned by g.Index operations. Canonical declarations
// live in pkg/graph/store because the optional Store capability interfaces
// live there. These aliases keep the g.Index package a complete error-checking
// surface for callers already importing it.
var (
	ErrVectorIndexExists          = storepkg.ErrVectorIndexExists
	ErrVectorIndexNotFound        = storepkg.ErrVectorIndexNotFound
	ErrDimensionMismatch          = storepkg.ErrDimensionMismatch
	ErrInvalidTemporalIndexConfig = storepkg.ErrInvalidTemporalIndexConfig
	ErrInvalidVectorIndexConfig   = storepkg.ErrInvalidVectorIndexConfig
	ErrInvalidVectorValue         = storepkg.ErrInvalidVectorValue
)
