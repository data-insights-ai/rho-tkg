package graph

import (
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/store"
)

// This file aliases the Store-contract types and helpers back into the
// pkg/graph namespace. The implementations live in
// pkg/graph/internal/store after the v3.1.17 structural restructure;
// these aliases preserve the public API surface (`graph.Store`,
// `graph.QueryOpts`, the sentinel errors callers compare against, etc.)
// and let internal pkg/graph code keep using the unqualified names that
// existed before the move.
//
// No new symbols are added here. Anything not aliased was already
// either internal to pkg/graph (and stays so) or already exported via a
// different mechanism.

// --- Persistence-contract types ---

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
	// IDComponents holds the decomposed fields of a snowflake ID.
	IDComponents = store.IDComponents
)

// --- ShardDepth constants ---

const (
	DepthAll  = store.DepthAll  // All tiers (default).
	DepthHot  = store.DepthHot  // Hot shard only.
	DepthWarm = store.DepthWarm // Hot + warm shards.
)

// --- DistanceMetric constants ---

const (
	DistanceCosine    = store.DistanceCosine
	DistanceEuclidean = store.DistanceEuclidean
)

// --- Store sentinel errors ---

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

// --- Snowflake helpers ---

// DecomposeID extracts the creation time, node ID, and sequence number
// from a snowflake ID. Re-exported from pkg/graph/internal/store.
func DecomposeID(id snowflake.ID) IDComponents {
	return store.DecomposeID(id)
}

// snowflakeEpoch and snowflakeLayout remain available to legacy
// pkg/graph code that referenced them as package-level identifiers.
// Both now forward to the canonical definition in internal/store.
var (
	snowflakeEpoch  time.Time        = store.SnowflakeEpoch
	snowflakeLayout snowflake.Layout = store.SnowflakeLayout
)
