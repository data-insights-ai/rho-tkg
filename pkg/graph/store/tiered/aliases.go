// Package tiered provides tiered.Store — the multi-shard Store
// implementation that routes entities across a reference shard, time-windowed
// event shards, and an optional reference archive.
package tiered

import (
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/ontology"
	storecontract "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	badger "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/badger"
)

// Aliases for unqualified use of types/values that the moved file contents
// previously referenced as bare identifiers in package graph.

// BadgerStore is the underlying single-shard backend. Store composes
// many of these into ref / event / archive shards.
type BadgerStore = badger.Store

// BadgerStoreConfig configures a single shard.
type BadgerStoreConfig = badger.Config

// RelDeleteInfo holds pre-read relationship metadata for cross-shard
// cascade-delete coordination.
type RelDeleteInfo = badger.RelDeleteInfo

// QueryOpts / ShardDepth / DistanceMetric / RelTombstone are the Store
// contract opaques used throughout Store. Canonical declarations
// live in pkg/graph/store (the public contract).
type (
	QueryOpts      = storecontract.QueryOpts
	ShardDepth     = storecontract.ShardDepth
	DistanceMetric = storecontract.DistanceMetric
	RelTombstone   = storecontract.RelTombstone
)

// EntityClass and OntologyMapping classify labels for shard routing.
// Public type lives in pkg/graph/ontology; aliased here so tieredstore
// internals keep their unqualified identifiers.
type (
	EntityClass     = ontology.EntityClass
	OntologyMapping = ontology.OntologyMapping
)

const (
	ClassEvent     = ontology.ClassEvent
	ClassReference = ontology.ClassReference
)

const (
	DepthAll  = storecontract.DepthAll
	DepthHot  = storecontract.DepthHot
	DepthWarm = storecontract.DepthWarm
)

const (
	DistanceCosine    = storecontract.DistanceCosine
	DistanceEuclidean = storecontract.DistanceEuclidean
)

// NewOntologyMapping constructs a mapping that classifies the given labels
// as reference and everything else as event.
func NewOntologyMapping(refLabels []string) *OntologyMapping {
	return ontology.NewOntologyMapping(refLabels)
}

// NewBadgerStore constructs a BadgerStore. Re-exposed so that existing
// pkg/graph call sites that opened a BadgerStore via the local symbol can
// keep doing so after the move.
func NewBadgerStore(cfg BadgerStoreConfig) (*BadgerStore, error) {
	return badger.New(cfg)
}

// Sentinel error aliases.
var (
	ErrNodeExists            = storecontract.ErrNodeExists
	ErrNodeNotFound          = storecontract.ErrNodeNotFound
	ErrRelExists             = storecontract.ErrRelExists
	ErrRelNotFound           = storecontract.ErrRelNotFound
	ErrVersionNotFound       = storecontract.ErrVersionNotFound
	ErrIndexExists           = storecontract.ErrIndexExists
	ErrIndexNotFound         = storecontract.ErrIndexNotFound
	ErrTemporalIndexExists   = storecontract.ErrTemporalIndexExists
	ErrTemporalIndexNotFound = storecontract.ErrTemporalIndexNotFound
	ErrStoreClosed           = storecontract.ErrStoreClosed
	ErrVectorIndexExists     = indexpkg.ErrVectorIndexExists
	ErrVectorIndexNotFound   = indexpkg.ErrVectorIndexNotFound
	ErrDimensionMismatch     = indexpkg.ErrDimensionMismatch
)
