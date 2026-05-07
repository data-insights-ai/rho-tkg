package graph

import (
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
)

// Vector-index sentinel errors. Returned from `Graph.CreateVectorIndex`,
// `Graph.DropVectorIndex`, and `Graph.SearchNearestNodes`.
var (
	ErrVectorIndexExists   = indexpkg.ErrVectorIndexExists
	ErrVectorIndexNotFound = indexpkg.ErrVectorIndexNotFound
	ErrDimensionMismatch   = indexpkg.ErrDimensionMismatch
)

// Registry sentinel errors. The label and reltype registries (in
// `pkg/graph/internal/registry`) are not part of the public API surface,
// but the sentinel errors they return surface through `AddNode`/
// `AddRelationship` and friends.
var (
	ErrEmptyName        = registrypkg.ErrEmptyName
	ErrRegistryNotEmpty = registrypkg.ErrRegistryNotEmpty
)
