package sharded

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// Query-planner index-existence/config introspection (BACKLOG 21b) —
// property/temporal/vector/rel-property index counterparts to
// CompositeIndexIntrospectionCapability (composite_index.go). Every shard
// holds IDENTICAL definitions (DDL fans out uniformly via fanOutUniform, per
// CLAUDE.md "Full index/stats capability parity (S5)"), so each door reads
// from the anchor shard exactly like ListCompositePropertyIndexes does.

var (
	_ storecontract.PropertyIndexIntrospectionCapability    = (*Store)(nil)
	_ storecontract.TemporalIndexIntrospectionCapability    = (*Store)(nil)
	_ storecontract.VectorIndexIntrospectionCapability      = (*Store)(nil)
	_ storecontract.RelPropertyIndexIntrospectionCapability = (*Store)(nil)
)

// HasPropertyIndex reports whether a property index exists on (labelToken,
// propertyKey), reading from the anchor shard.
func (s *Store) HasPropertyIndex(labelToken uint16, propertyKey string) (bool, error) {
	if err := s.checkOpen(); err != nil {
		return false, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return false, err
	}
	return s.anchor().HasPropertyIndex(labelToken, propertyKey)
}

// HasTemporalIndex reports whether a temporal interval index exists on
// labelToken, reading from the anchor shard.
func (s *Store) HasTemporalIndex(labelToken uint16) (bool, error) {
	if err := s.checkOpen(); err != nil {
		return false, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return false, err
	}
	return s.anchor().HasTemporalIndex(labelToken)
}

// VectorIndexInfo returns the declared configuration of the vector index on
// (labelToken, propertyKey), reading from the anchor shard — every shard's
// vector index for the same key shares an identical definition, only the
// indexed ENTRIES differ per shard (see CLAUDE.md "Vector Indexes: Store-level
// scope in TieredStore" analog for sharded).
func (s *Store) VectorIndexInfo(labelToken uint16, propertyKey string) (storecontract.VectorIndexInfo, bool, error) {
	if err := s.checkOpen(); err != nil {
		return storecontract.VectorIndexInfo{}, false, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return storecontract.VectorIndexInfo{}, false, err
	}
	return s.anchor().VectorIndexInfo(labelToken, propertyKey)
}

// HasRelPropertyIndex reports whether a relationship property index exists on
// (relTypeToken, propertyKey), reading from the anchor shard.
func (s *Store) HasRelPropertyIndex(relTypeToken uint16, propertyKey string) (bool, error) {
	if err := s.checkOpen(); err != nil {
		return false, err
	}
	if err := storecontract.ValidateRelTypeToken(relTypeToken); err != nil {
		return false, err
	}
	return s.anchor().HasRelPropertyIndex(relTypeToken, propertyKey)
}
