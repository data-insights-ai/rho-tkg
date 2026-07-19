package sharded

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Composite property indexes (CompositePropertyIndexCapability) +
// introspection (CompositeIndexIntrospectionCapability) — ADR-0007.
//
// Same cross-shard model as the single-key node property index: fan the DDL out
// to every shard and fold the per-shard equality matches on read. Because the
// shards hold IDENTICAL composite definitions (every create/drop hits all of
// them, each rebuilds its own defs at open), introspection reads the declared
// key tuples from the anchor shard — the badger door already returns
// caller-owned copies.

var (
	_ storecontract.CompositePropertyIndexCapability      = (*Store)(nil)
	_ storecontract.CompositeIndexIntrospectionCapability = (*Store)(nil)
)

// CreateCompositePropertyIndex builds a composite index over the ordered keys
// under labelToken on every shard. Returns ErrIndexExists for a duplicate
// (labelToken, ordered keys) definition.
func (s *Store) CreateCompositePropertyIndex(labelToken uint16, keys []string) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateCompositeIndexKeys(keys); err != nil {
		return err
	}
	return s.fanOutUniformCreate(
		func(shard *badgerShard) error { return shard.CreateCompositePropertyIndex(labelToken, keys) },
		func(shard *badgerShard) error { return shard.DropCompositePropertyIndex(labelToken, keys) },
	)
}

// DropCompositePropertyIndex removes the composite index declared over the
// exact ordered keys from every shard. Returns ErrIndexNotFound if absent.
func (s *Store) DropCompositePropertyIndex(labelToken uint16, keys []string) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateCompositeIndexKeys(keys); err != nil {
		return err
	}
	return s.fanOutUniform(func(shard *badgerShard) error {
		return shard.DropCompositePropertyIndex(labelToken, keys)
	})
}

// NodesByLabelAndProperties folds each shard's AND-conjunction equality matches
// into one globally ID-sorted, paginated result.
func (s *Store) NodesByLabelAndProperties(labelToken uint16, values map[string]any, opts QueryOpts) ([]*types.Node, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return nil, err
	}
	per := make([][]*types.Node, len(s.shards))
	err := s.forEachShardErr(func(idx int, shard *badgerShard) error {
		nodes, e := shard.NodesByLabelAndProperties(labelToken, values, stripPagination(opts))
		per[idx] = nodes
		return e
	})
	if err != nil {
		return nil, err
	}
	return paginateNodes(mergeSortNodes(per), opts), nil
}

// ListCompositePropertyIndexes returns the declared key tuples registered under
// labelToken. Every shard holds identical definitions, so this reads from the
// anchor shard; the badger door returns caller-owned copies.
func (s *Store) ListCompositePropertyIndexes(labelToken uint16) ([][]string, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return nil, err
	}
	return s.anchor().ListCompositePropertyIndexes(labelToken)
}
