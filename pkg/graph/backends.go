package graph

import (
	"fmt"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/badgerstore"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/memorystore"
	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/tieredstore"
)

// MemoryStore is the thread-safe in-memory Store implementation. The
// canonical type lives in `pkg/graph/internal/memorystore`.
type MemoryStore = memorystore.MemoryStore

// NewMemoryStore constructs an empty MemoryStore.
func NewMemoryStore() *MemoryStore { return memorystore.NewMemoryStore() }

// BadgerStore is the persistent Store implementation backed by Badger v4.
// The canonical type lives in `pkg/graph/internal/badgerstore`.
type BadgerStore = badgerstore.BadgerStore

// BadgerStoreConfig configures a BadgerStore.
type BadgerStoreConfig = badgerstore.BadgerStoreConfig

// NewBadgerStore opens a Badger database with the given configuration and
// rebuilds in-memory indexes from persisted data.
func NewBadgerStore(cfg BadgerStoreConfig) (*BadgerStore, error) {
	return badgerstore.NewBadgerStore(cfg)
}

// TieredStore is the multi-shard Store implementation that routes entities
// across a reference shard, time-windowed event shards, and an optional
// reference archive. The canonical type lives in
// `pkg/graph/internal/tieredstore`.
type TieredStore = tieredstore.TieredStore

// TieredStoreConfig configures a TieredStore.
type TieredStoreConfig = tieredstore.TieredStoreConfig

// ShardInfo describes a shard in a TieredStore. Returned from
// `Graph.ListShards` / `TieredStore.ListShards`.
type ShardInfo = tieredstore.ShardInfo

// VerifyResult is the result of `Graph.VerifyShard` /
// `TieredStore.VerifyShard`.
type VerifyResult = tieredstore.VerifyResult

// RepairResult is the result of `Graph.RunRepair` / `TieredStore.RunRepair`.
type RepairResult = tieredstore.RepairResult

// NewTieredStore constructs a TieredStore from cfg.
func NewTieredStore(cfg TieredStoreConfig) (*TieredStore, error) {
	return tieredstore.NewTieredStore(cfg)
}

// TieredStore-specific sentinel errors.
var (
	ErrEventPropertyIndex        = tieredstore.ErrEventPropertyIndex
	ErrPrimaryLabelClassMutation = tieredstore.ErrPrimaryLabelClassMutation
	ErrNotReferenceEntity        = tieredstore.ErrNotReferenceEntity
	ErrCrossShardArchiveRel      = tieredstore.ErrCrossShardArchiveRel
)

// MigrateFromBadger copies all current entities from a single BadgerStore
// into a TieredStore. Routing is determined by the TieredStore's ontology:
// reference labels go to the reference shard, event labels go to the hot
// event shard.
//
// The label registry is loaded from the source BadgerStore directly — callers
// no longer need to thread one through. If the source has no persisted
// registry data, the migration starts with an empty registry (and the
// destination TieredStore will populate one as labels are encountered).
//
// No history migration: hash chains would need re-creation. This handles
// the typical case of migrating a single-BadgerStore deployment to the
// tiered layout.
func MigrateFromBadger(src *BadgerStore, dst *TieredStore) error {
	labels := registrypkg.NewLabelRegistry()
	if _, err := src.LoadLabelRegistry(labels); err != nil {
		return fmt.Errorf("graph: migrate: load label registry: %w", err)
	}
	return tieredstore.MigrateFromBadger(src, dst, labels)
}
