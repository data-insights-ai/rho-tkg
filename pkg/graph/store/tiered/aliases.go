// Package tiered provides tiered.Store — the multi-shard Store
// implementation that routes entities across a reference shard, time-windowed
// event shards, and an optional reference archive.
package tiered

import (
	"fmt"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/ontology"
	storecontract "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store"
	badger "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store/badger"
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

// QueryOpts is a Store-contract alias; canonical declaration lives in
// pkg/graph/store.
type QueryOpts = storecontract.QueryOpts

// ShardDepth is a Store-contract alias; canonical declaration lives in
// pkg/graph/store.
type ShardDepth = storecontract.ShardDepth

// DistanceMetric is a Store-contract alias; canonical declaration lives in
// pkg/graph/store.
type DistanceMetric = storecontract.DistanceMetric

// RelTombstone is a Store-contract alias; canonical declaration lives in
// pkg/graph/store.
type RelTombstone = storecontract.RelTombstone

// EntityClass classifies a label for shard routing. Alias of
// pkg/graph/ontology.EntityClass.
type EntityClass = ontology.EntityClass

// OntologyMapping classifies labels for shard routing. Alias of
// pkg/graph/ontology.OntologyMapping.
type OntologyMapping = ontology.OntologyMapping

// EntityClass values — aliases of the canonical declarations in
// pkg/graph/ontology.
const (
	ClassEvent     = ontology.ClassEvent
	ClassReference = ontology.ClassReference
)

// ShardDepth values — aliases of the canonical declarations in
// pkg/graph/store.
const (
	DepthAll  = storecontract.DepthAll
	DepthHot  = storecontract.DepthHot
	DepthWarm = storecontract.DepthWarm
)

// DistanceMetric values — aliases of the canonical declarations in
// pkg/graph/store.
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
	ErrNodeExists                 = storecontract.ErrNodeExists
	ErrNodeNotFound               = storecontract.ErrNodeNotFound
	ErrRelExists                  = storecontract.ErrRelExists
	ErrRelNotFound                = storecontract.ErrRelNotFound
	ErrVersionNotFound            = storecontract.ErrVersionNotFound
	ErrIndexExists                = storecontract.ErrIndexExists
	ErrIndexNotFound              = storecontract.ErrIndexNotFound
	ErrTemporalIndexExists        = storecontract.ErrTemporalIndexExists
	ErrTemporalIndexNotFound      = storecontract.ErrTemporalIndexNotFound
	ErrInvalidTemporalIndexConfig = storecontract.ErrInvalidTemporalIndexConfig
	ErrStoreClosed                = storecontract.ErrStoreClosed
	ErrVectorIndexExists          = storecontract.ErrVectorIndexExists
	ErrVectorIndexNotFound        = storecontract.ErrVectorIndexNotFound
	ErrDimensionMismatch          = storecontract.ErrDimensionMismatch
	ErrInvalidVectorIndexConfig   = storecontract.ErrInvalidVectorIndexConfig
	ErrInvalidVectorValue         = storecontract.ErrInvalidVectorValue
	ErrInvalidShardDepth          = storecontract.ErrInvalidShardDepth
	ErrInvalidStoreMutation       = storecontract.ErrInvalidStoreMutation
	ErrNilStore                   = storecontract.ErrNilStore
)

func errNilIterationCallback() error {
	return fmt.Errorf("%w: nil iteration callback", ErrInvalidStoreMutation)
}

func errNilLabelRegistry() error {
	return fmt.Errorf("%w: nil label registry", ErrInvalidStoreMutation)
}

func errNilRelTypeRegistry() error {
	return fmt.Errorf("%w: nil relationship type registry", ErrInvalidStoreMutation)
}
