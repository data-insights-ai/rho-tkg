package sharded

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// Temporal indexes (TemporalIndexCapability) — ADR-0007 S5.
//
// A temporal index accelerates a shard's own label-scoped temporal scans over
// its LOCAL nodes; a label's nodes are distributed across slots, so the index
// is fanned out to every shard and each accelerates its local portion. The DDL
// moves in lockstep (as with property indexes), so a uniform per-shard sentinel
// (ErrTemporalIndexExists / ErrTemporalIndexNotFound) is the single outcome.

var _ storecontract.TemporalIndexCapability = (*Store)(nil)

// CreateTemporalIndex builds a temporal interval index over labelToken on every
// shard. Returns ErrTemporalIndexExists if it already exists.
func (s *Store) CreateTemporalIndex(labelToken uint16) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	return s.fanOutUniform(func(shard *badgerShard) error {
		return shard.CreateTemporalIndex(labelToken)
	})
}

// DropTemporalIndex removes the temporal index for labelToken from every shard.
// Returns ErrTemporalIndexNotFound if no such index exists.
func (s *Store) DropTemporalIndex(labelToken uint16) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	return s.fanOutUniform(func(shard *badgerShard) error {
		return shard.DropTemporalIndex(labelToken)
	})
}
