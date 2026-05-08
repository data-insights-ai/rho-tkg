// Package memory provides memory.Store — the thread-safe in-memory
// implementation of the pkg/graph/store.Store interface. Used as the
// default backend by pkg/graph and also as a building block in tests.
package memory

import (
	"sync"

	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	storecontract "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// Store-contract sentinel error aliases. Re-exporting them as package-local
// names keeps the moved file readable. The canonical sentinel-error
// declarations live in pkg/graph/store (public contract).
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
	ErrVectorIndexExists     = indexpkg.ErrVectorIndexExists
	ErrVectorIndexNotFound   = indexpkg.ErrVectorIndexNotFound
	ErrDimensionMismatch     = indexpkg.ErrDimensionMismatch
)

// QueryOpts is a Store-contract alias; canonical declaration lives in
// pkg/graph/store (the public contract).
type QueryOpts = storecontract.QueryOpts

// DistanceMetric is a Store-contract alias; canonical declaration lives in
// pkg/graph/store.
type DistanceMetric = storecontract.DistanceMetric

// RelTombstone is a Store-contract alias; canonical declaration lives in
// pkg/graph/store.
type RelTombstone = storecontract.RelTombstone

// Store is a thread-safe in-memory Store implementation.
// Uses maps for O(1) entity lookup and nested hash-sets for O(1) index maintenance.
type Store struct {
	mu    sync.RWMutex
	nodes map[types.NodeID]*types.Node
	rels  map[types.RelID]*types.Relationship

	// Label index: labelToken → set of node IDs.
	labelIdx map[uint16]map[types.NodeID]struct{}

	// RelType index: relTypeToken → set of rel IDs.
	typeIdx map[uint16]map[types.RelID]struct{}

	// Adjacency indexes — nested hash sets for O(1) insert/delete.
	outIdx map[types.NodeID]map[types.RelID]struct{} // startNodeID → set(relID)
	inIdx  map[types.NodeID]map[types.RelID]struct{} // endNodeID → set(relID)

	// Version history — pre-mutation snapshots keyed by entity ID and version.
	nodeHistory map[types.NodeID]map[uint32]*types.Node
	relHistory  map[types.RelID]map[uint32]*types.Relationship

	// Property indexes — label+property → value → set of node IDs.
	propertyIndexes map[indexpkg.PropertyIndexKey]*indexpkg.PropertyIndex

	// Temporal indexes — labelToken → interval index for temporal push-down.
	temporalIndexes map[uint16]*indexpkg.TemporalIndex

	// High-frequency indexes — labelToken → time-bucketed index for O(1) insertion.
	// Separate map from temporalIndexes; only one type can exist per label at a time.
	hfIndexes map[uint16]*indexpkg.HighFrequencyIndex

	// Vector indexes — in-memory brute-force k-NN index on node properties.
	vectorIndexes map[indexpkg.VectorIndexKey]*indexpkg.VectorIndex
}

// New creates an empty Store with all indexes initialized.
func New() *Store {
	return &Store{
		nodes:           make(map[types.NodeID]*types.Node),
		rels:            make(map[types.RelID]*types.Relationship),
		labelIdx:        make(map[uint16]map[types.NodeID]struct{}),
		typeIdx:         make(map[uint16]map[types.RelID]struct{}),
		outIdx:          make(map[types.NodeID]map[types.RelID]struct{}),
		inIdx:           make(map[types.NodeID]map[types.RelID]struct{}),
		nodeHistory:     make(map[types.NodeID]map[uint32]*types.Node),
		relHistory:      make(map[types.RelID]map[uint32]*types.Relationship),
		propertyIndexes: make(map[indexpkg.PropertyIndexKey]*indexpkg.PropertyIndex),
		temporalIndexes: make(map[uint16]*indexpkg.TemporalIndex),
		hfIndexes:       make(map[uint16]*indexpkg.HighFrequencyIndex),
		vectorIndexes:   make(map[indexpkg.VectorIndexKey]*indexpkg.VectorIndex),
	}
}

// Clear removes all entities, indexes, history, and property indexes.
// After Clear(), the Store is empty (same state as New()).
func (ms *Store) Clear() error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.nodes = make(map[types.NodeID]*types.Node)
	ms.rels = make(map[types.RelID]*types.Relationship)
	ms.labelIdx = make(map[uint16]map[types.NodeID]struct{})
	ms.typeIdx = make(map[uint16]map[types.RelID]struct{})
	ms.outIdx = make(map[types.NodeID]map[types.RelID]struct{})
	ms.inIdx = make(map[types.NodeID]map[types.RelID]struct{})
	ms.nodeHistory = make(map[types.NodeID]map[uint32]*types.Node)
	ms.relHistory = make(map[types.RelID]map[uint32]*types.Relationship)
	ms.propertyIndexes = make(map[indexpkg.PropertyIndexKey]*indexpkg.PropertyIndex)
	ms.temporalIndexes = make(map[uint16]*indexpkg.TemporalIndex)
	ms.hfIndexes = make(map[uint16]*indexpkg.HighFrequencyIndex)
	ms.vectorIndexes = make(map[indexpkg.VectorIndexKey]*indexpkg.VectorIndex)
	return nil
}

// Close is a no-op for Store. Satisfies the Store interface.
func (ms *Store) Close() error { return nil }
