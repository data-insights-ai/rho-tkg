// Package tieredstore provides TieredStore — the multi-shard Store
// implementation that routes entities across a reference shard, time-windowed
// event shards, and an optional reference archive.
package tieredstore

import (
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/badgerstore"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/store"
)

// Aliases for unqualified use of types/values that the moved file contents
// previously referenced as bare identifiers in package graph.

// BadgerStore is the underlying single-shard backend. TieredStore composes
// many of these into ref / event / archive shards.
type BadgerStore = badgerstore.BadgerStore

// BadgerStoreConfig configures a single shard.
type BadgerStoreConfig = badgerstore.BadgerStoreConfig

// RelDeleteInfo holds pre-read relationship metadata for cross-shard
// cascade-delete coordination.
type RelDeleteInfo = badgerstore.RelDeleteInfo

// QueryOpts / ShardDepth / DistanceMetric / RelTombstone are the Store
// contract opaques used throughout TieredStore.
type (
	QueryOpts      = storepkg.QueryOpts
	ShardDepth     = storepkg.ShardDepth
	DistanceMetric = storepkg.DistanceMetric
	RelTombstone   = storepkg.RelTombstone
)

// EntityClass and OntologyMapping classify labels for shard routing.
type (
	EntityClass     = indexpkg.EntityClass
	OntologyMapping = indexpkg.OntologyMapping
)

const (
	ClassEvent     = indexpkg.ClassEvent
	ClassReference = indexpkg.ClassReference
)

const (
	DepthAll  = storepkg.DepthAll
	DepthHot  = storepkg.DepthHot
	DepthWarm = storepkg.DepthWarm
)

const (
	DistanceCosine    = storepkg.DistanceCosine
	DistanceEuclidean = storepkg.DistanceEuclidean
)

// NewOntologyMapping constructs a mapping that classifies the given labels
// as reference and everything else as event.
func NewOntologyMapping(refLabels []string) *OntologyMapping {
	return indexpkg.NewOntologyMapping(refLabels)
}

// NewBadgerStore constructs a BadgerStore. Re-exposed so that existing
// pkg/graph call sites that opened a BadgerStore via the local symbol can
// keep doing so after the move.
func NewBadgerStore(cfg BadgerStoreConfig) (*BadgerStore, error) {
	return badgerstore.NewBadgerStore(cfg)
}

// Sentinel error aliases.
var (
	ErrNodeExists            = storepkg.ErrNodeExists
	ErrNodeNotFound          = storepkg.ErrNodeNotFound
	ErrRelExists             = storepkg.ErrRelExists
	ErrRelNotFound           = storepkg.ErrRelNotFound
	ErrVersionNotFound       = storepkg.ErrVersionNotFound
	ErrIndexExists           = storepkg.ErrIndexExists
	ErrIndexNotFound         = storepkg.ErrIndexNotFound
	ErrTemporalIndexExists   = storepkg.ErrTemporalIndexExists
	ErrTemporalIndexNotFound = storepkg.ErrTemporalIndexNotFound
	ErrStoreClosed           = storepkg.ErrStoreClosed
	ErrVectorIndexExists     = indexpkg.ErrVectorIndexExists
	ErrVectorIndexNotFound   = indexpkg.ErrVectorIndexNotFound
	ErrDimensionMismatch     = indexpkg.ErrDimensionMismatch
)
