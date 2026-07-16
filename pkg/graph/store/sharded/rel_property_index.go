package sharded

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Relationship property indexes (RelPropertyIndexCapability) — ADR-0007 S5.
//
// Rels are distributed by their start node's slot. UNLIKE tiered — whose event
// rels route by timestamp across a DYNAMIC (cold/archive) shard set, so it
// declines rel-index acceleration and returns ErrRelPropertyIndexUnsupported on
// Create/Drop — the sharded store's shards are STATIC and all open, so it CAN
// accelerate: fan the index DDL out to every shard (each maintains a rel-value
// index over its own local rels) and fold the per-shard accelerated matches on
// read, mirroring RelationshipsByType. The fold order and pagination match the
// node property-index path.

var _ storecontract.RelPropertyIndexCapability = (*Store)(nil)

// CreateRelPropertyIndex builds a rel-property index over (relTypeToken,
// propertyKey) on every shard. Returns ErrIndexExists if it already exists.
func (s *Store) CreateRelPropertyIndex(relTypeToken uint16, propertyKey string) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelTypeToken(relTypeToken); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return err
	}
	return s.fanOutUniform(func(shard *badgerShard) error {
		return shard.CreateRelPropertyIndex(relTypeToken, propertyKey)
	})
}

// DropRelPropertyIndex removes the (relTypeToken, propertyKey) rel index from
// every shard. Returns ErrIndexNotFound if no such index exists.
func (s *Store) DropRelPropertyIndex(relTypeToken uint16, propertyKey string) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelTypeToken(relTypeToken); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return err
	}
	return s.fanOutUniform(func(shard *badgerShard) error {
		return shard.DropRelPropertyIndex(relTypeToken, propertyKey)
	})
}

// RelationshipsByTypeAndProperty folds each shard's accelerated equality
// matches into one globally ID-sorted, paginated result.
func (s *Store) RelationshipsByTypeAndProperty(relTypeToken uint16, key string, value any, opts QueryOpts) ([]*types.Relationship, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelTypeToken(relTypeToken); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return nil, err
	}
	per := make([][]*types.Relationship, len(s.shards))
	err := s.forEachShardErr(func(idx int, shard *badgerShard) error {
		rels, e := shard.RelationshipsByTypeAndProperty(relTypeToken, key, value, stripPagination(opts))
		per[idx] = rels
		return e
	})
	if err != nil {
		return nil, err
	}
	return paginateRels(mergeSortRels(per), opts), nil
}
