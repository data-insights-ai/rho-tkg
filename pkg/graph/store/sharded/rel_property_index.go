package sharded

import (
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Relationship property indexes (RelPropertyIndexCapability) — ADR-0007.
//
// Rels are distributed by their start node's slot. The shards are STATIC and
// all open: fan the index DDL out to every shard (each maintains a rel-value
// index over its own local rels) and fold the per-shard accelerated matches on
// read, mirroring RelationshipsByType (tiered does the same over its dynamic
// shard set with an anchored fan-out, tiered/shard_index_fanout.go). The fold order and pagination match the
// node property-index path.

var _ storecontract.RelPropertyIndexCapability = (*Store)(nil)

// CreateRelPropertyIndex builds a rel-property index over (relTypeToken,
// propertyKey) on every shard. Returns ErrIndexExists if it already exists.
func (s *Store) CreateRelPropertyIndex(relTypeToken uint16, propertyKey string) error {
	return s.CreateRelPropertyIndexWithOptions(relTypeToken, propertyKey, storecontract.PropertyIndexOptions{})
}

var _ storecontract.RelPropertyIndexOptionsCapability = (*Store)(nil)

// CreateRelPropertyIndexWithOptions builds the rel index with the same options
// on every shard.
func (s *Store) CreateRelPropertyIndexWithOptions(relTypeToken uint16, propertyKey string, opts storecontract.PropertyIndexOptions) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelTypeToken(relTypeToken); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return err
	}
	return s.fanOutUniformCreate(
		func(shard *badgerShard) error {
			return shard.CreateRelPropertyIndexWithOptions(relTypeToken, propertyKey, opts)
		},
		func(shard *badgerShard) error { return shard.DropRelPropertyIndex(relTypeToken, propertyKey) },
	)
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
// matches into one globally ID-sorted, paginated result; each shard answers
// the same page, as in NodesByLabelAndProperty (backlog 72).
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
	keys, err := storeutil.ValueIndexKeys(value, "relationships by type and property")
	if err != nil {
		return nil, err
	}
	return s.RelationshipsByTypeAndPropertyKeys(relTypeToken, key, keys, opts)
}

var _ storecontract.RelPropertyKeysLookupCapability = (*Store)(nil)

// RelationshipsByTypeAndPropertyKeys implements
// storecontract.RelPropertyKeysLookupCapability: each shard asked once with
// every key, folded as RelationshipsByTypeAndProperty (round 11).
func (s *Store) RelationshipsByTypeAndPropertyKeys(relTypeToken uint16, key string, valueKeys []string, opts QueryOpts) ([]*types.Relationship, error) {
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
		rels, e := shard.RelationshipsByTypeAndPropertyKeys(relTypeToken, key, valueKeys, opts)
		per[idx] = rels
		return e
	})
	if err != nil {
		return nil, err
	}
	return paginateRels(mergeSortRels(per), opts), nil
}
