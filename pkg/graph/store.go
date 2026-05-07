package graph

import (
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/store"
)

// Store is the persistence contract for the graph layer. Production
// implementations live in `pkg/graph/internal/{memorystore,badgerstore,tieredstore}`.
//
// The canonical declaration of the interface and its supporting types
// (`QueryOpts`, `ShardDepth`, `RelTombstone`, `DistanceMetric`) lives in
// `pkg/graph/internal/store` so that internal helpers and the concrete
// backends can refer to it without an import cycle through `pkg/graph`.
// These type aliases ARE the public API: callers depend on `graph.Store`,
// `graph.QueryOpts`, `graph.ErrNodeNotFound`, etc., and `errors.Is` matches
// against the canonical sentinel values exported below.
type (
	// Store is the persistence contract for the graph layer.
	Store = store.Store
	// QueryOpts controls pagination and temporal filtering.
	QueryOpts = store.QueryOpts
	// ShardDepth controls which shard tiers are included in merge queries.
	ShardDepth = store.ShardDepth
	// RelTombstone packages a relationship's tombstone data.
	RelTombstone = store.RelTombstone
	// DistanceMetric determines how similarity is measured between two vectors.
	DistanceMetric = store.DistanceMetric
)

// ShardDepth constants.
const (
	DepthAll  = store.DepthAll  // All tiers (default).
	DepthHot  = store.DepthHot  // Hot shard only.
	DepthWarm = store.DepthWarm // Hot + warm shards.
)

// DistanceMetric constants.
const (
	DistanceCosine    = store.DistanceCosine
	DistanceEuclidean = store.DistanceEuclidean
)

// Store-layer sentinel errors. Callers compare with errors.Is.
var (
	ErrNodeNotFound          = store.ErrNodeNotFound
	ErrRelNotFound           = store.ErrRelNotFound
	ErrNodeExists            = store.ErrNodeExists
	ErrRelExists             = store.ErrRelExists
	ErrVersionNotFound       = store.ErrVersionNotFound
	ErrNoVersionValidAt      = store.ErrNoVersionValidAt
	ErrIndexExists           = store.ErrIndexExists
	ErrIndexNotFound         = store.ErrIndexNotFound
	ErrTemporalIndexExists   = store.ErrTemporalIndexExists
	ErrTemporalIndexNotFound = store.ErrTemporalIndexNotFound
	ErrTxDone                = store.ErrTxDone
	ErrStoreClosed           = store.ErrStoreClosed
)
