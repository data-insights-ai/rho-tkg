package store

import "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"

// DistanceMetric determines how similarity is measured between two vectors
// stored in a Store-backed vector index.
type DistanceMetric uint8

// Distance metrics for vector indexes.
const (
	DistanceCosine DistanceMetric = iota + 1
	DistanceEuclidean
)

// ShardDepth controls which shard tiers are included in merge queries.
// Zero (DepthAll) includes all tiers — backward-compatible default.
type ShardDepth byte

// Shard-depth selectors for Store queries. Single-shard stores accept the
// defined selectors but treat them equivalently because they have no cold tiers.
const (
	DepthAll  ShardDepth = 0 // All tiers (default, backward-compatible).
	DepthHot  ShardDepth = 1 // Hot shard only.
	DepthWarm ShardDepth = 2 // Hot + warm shards.
)

// QueryOpts controls pagination and temporal filtering for unbounded query methods.
// Zero values mean "return all" — backward-compatible with existing callers.
type QueryOpts struct {
	Limit int            // Max results. 0 = no limit.
	After types.EntityID // Return entities with ID > After. 0 = from start.

	// Temporal filters — zero values = no filter (backward-compatible).
	// ValidAt takes precedence if both ValidAt and ValidStart/ValidEnd are set.
	ValidAt    types.Instant // Point-in-time filter (valid time). 0 = disabled.
	ValidStart types.Instant // Interval filter start. Both must be > 0 for interval filter.
	ValidEnd   types.Instant // Interval filter end. 0 = disabled.

	// TxAt restricts the chain to versions visible at the given transaction
	// time: TxFrom <= TxAt AND (TxTo == 0 OR TxTo > TxAt). 0 = no TX filter
	// (current behaviour: any version regardless of when it was written).
	// Combine with ValidAt / ValidStart / ValidEnd for bitemporal queries.
	TxAt types.Instant

	// IncludeEclipsed includes history rows that were superseded by a cascade
	// edit (Phase 3+). Default false: eclipsed rows are skipped for valid-time
	// queries. Reserved field; pre-cascade builds treat all history rows as
	// non-eclipsed.
	IncludeEclipsed bool

	// Depth controls which shard tiers to query. 0 (DepthAll) = all tiers.
	// Single-shard stores accept only the defined enum values, but all valid
	// depth values see the full single shard.
	Depth ShardDepth
}
