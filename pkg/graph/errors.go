package graph

import (
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/core"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
)

// Vector-index sentinel errors. Returned from the index sub-API.
var (
	ErrVectorIndexExists   = indexpkg.ErrVectorIndexExists
	ErrVectorIndexNotFound = indexpkg.ErrVectorIndexNotFound
	ErrDimensionMismatch   = indexpkg.ErrDimensionMismatch
)

// Registry sentinel errors. Surface through node/rel mutations.
var (
	ErrEmptyName        = registrypkg.ErrEmptyName
	ErrRegistryNotEmpty = registrypkg.ErrRegistryNotEmpty
)

// Sentinel errors re-exported from pkg/graph/internal/core for the public API.
var (
	ErrNoLabels                 = core.ErrNoLabels
	ErrNilNode                  = core.ErrNilNode
	ErrZeroID                   = core.ErrZeroID
	ErrNotTieredStore           = core.ErrNotTieredStore
	ErrAlreadyClosed            = core.ErrAlreadyClosed
	ErrInvalidTimeRange         = core.ErrInvalidTimeRange
	ErrLabelNotFound            = core.ErrLabelNotFound
	ErrLastLabel                = core.ErrLastLabel
	ErrDepthTemporalUnsupported = core.ErrDepthTemporalUnsupported
	ErrTooManyLabels            = core.ErrTooManyLabels
	ErrTooManyProperties        = core.ErrTooManyProperties
	ErrKeyTooLong               = core.ErrKeyTooLong
	ErrValueTooLarge            = core.ErrValueTooLarge
	ErrNameTooLong              = core.ErrNameTooLong
	ErrSelfLoop                 = core.ErrSelfLoop
)
