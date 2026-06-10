// Package store defines the persistence contract for the graph layer.
// External callers (notably tkgd-v3) implement this interface to plug
// in custom backends; production implementations live in
// pkg/graph/store/{memory,badger,tiered}.
package store

import (
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Store is the persistence contract for the graph layer. It is the
// composition of the capability interfaces declared in capabilities.go —
// the original ~65-method aggregate written out by hand. Today every
// in-tree implementation (memory.Store, badger.Store, tiered.Store)
// satisfies the full composition; out-of-tree backends that need only a
// subset can implement the narrower capability interfaces and have call
// sites type-assert on the specific capability.
//
// See capabilities.go for the per-cluster boundaries and which clusters
// are mandatory vs. optional. PropertyIndex / TemporalIndex / VectorIndex /
// HighFrequencyIndex are embedded here for the current full Store contract;
// newer optional helper capabilities such as FilteredVectorSearchCapability
// and DepthHistoryIterationCapability are type-asserted directly by callers.
//
// Sharded-backend invariant: any implementation that routes entities to
// physical shards by primary-label ontology class (the in-tree example is
// tiered.Store) MUST reject label mutations that would change the primary
// label's class (reference <-> event) — otherwise the live row stays on its
// original shard while later history snapshots route elsewhere, fragmenting
// the version chain. Single-shard backends (memory, badger) need no guard.
// See tiered.ErrPrimaryLabelClassMutation and lessons.md B33.
type Store interface {
	Lifecycle
	NodeCRUDCapability
	RelationshipCRUDCapability
	AdjacencyCapability
	BulkReadCapability
	BatchCapability
	HistoryCapability
	StatsCapability
	IterationCapability

	// Optional capabilities — listed here so existing implementations
	// continue to satisfy Store unchanged. Future backends may omit
	// these and consumers must type-assert before calling.
	PropertyIndexCapability
	TemporalIndexCapability
	VectorIndexCapability
	HighFrequencyIndexCapability
}

// RelTombstone packages a relationship's tombstone data for use in atomic delete-with-history.
// PrevVersion is the history slot to write the tombstone to (matches r.Version() before deletion).
// Tombstone is a pre-built deep copy with DeletedAt, ValidTo, TxFrom, TxTo populated by the caller.
// DeleteNodeWithHistory requires one RelTombstone per live connected relationship,
// with no duplicates and no relationships unrelated to the deleted node.
type RelTombstone struct {
	ID          types.RelID
	PrevVersion uint32
	Tombstone   *types.Relationship
}
