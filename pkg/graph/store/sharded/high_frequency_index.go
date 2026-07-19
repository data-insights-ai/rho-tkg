package sharded

import (
	"time"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// High-frequency temporal indexes (HighFrequencyIndexCapability) — ADR-0007.
//
// A high-frequency index is the time-bucketed amortised-insertion variant of
// the temporal index; like the temporal index, a label's nodes are spread
// across slots, so the DDL is fanned out to every shard and each accelerates
// its local portion. It shares the temporal-index namespace (a label may carry
// EITHER a temporal or a high-frequency index, not both), so the uniform
// per-shard sentinels are ErrTemporalIndexExists / ErrTemporalIndexNotFound.

var _ storecontract.HighFrequencyIndexCapability = (*Store)(nil)

// CreateHighFrequencyIndex builds a high-frequency temporal index over
// labelToken with the given bucket size on every shard. Returns
// ErrTemporalIndexExists if a temporal or high-frequency index already exists
// for the label, or ErrInvalidTemporalIndexConfig for an invalid bucket size.
func (s *Store) CreateHighFrequencyIndex(labelToken uint16, bucketSize time.Duration) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	if err := storecontract.ValidateHighFrequencyBucketSize(bucketSize); err != nil {
		return err
	}
	return s.fanOutUniformCreate(
		func(shard *badgerShard) error { return shard.CreateHighFrequencyIndex(labelToken, bucketSize) },
		func(shard *badgerShard) error { return shard.DropHighFrequencyIndex(labelToken) },
	)
}

// DropHighFrequencyIndex removes the high-frequency index for labelToken from
// every shard. Returns ErrTemporalIndexNotFound if no such index exists.
func (s *Store) DropHighFrequencyIndex(labelToken uint16) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return err
	}
	return s.fanOutUniform(func(shard *badgerShard) error {
		return shard.DropHighFrequencyIndex(labelToken)
	})
}
