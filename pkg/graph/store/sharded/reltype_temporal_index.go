package sharded

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Relationship-type temporal indexes (RelTypeTemporalIndexCapability,
// BACKLOG 21c) — the rel-type-keyed mirror of temporal_index.go, following the
// same ADR-0007 fan-out shape: a rel type's relationships are distributed
// across slots by the RELATIONSHIP'S OWN ID (never an endpoint's — see
// CLAUDE.md TieredStore/sharded routing rule), so the index is fanned out to
// every shard and each accelerates its local portion via the badger
// implementation added alongside this file.

var _ storecontract.RelTypeTemporalIndexCapability = (*Store)(nil)

// CreateRelTemporalIndex builds a temporal interval index over relType on
// every shard. Returns ErrTemporalIndexExists if it already exists.
func (s *Store) CreateRelTemporalIndex(relType uint16) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelTypeToken(relType); err != nil {
		return err
	}
	return s.fanOutUniform(func(shard *badgerShard) error {
		return shard.CreateRelTemporalIndex(relType)
	})
}

// DropRelTemporalIndex removes the rel-type temporal index from every shard.
// Returns ErrTemporalIndexNotFound if no such index exists.
func (s *Store) DropRelTemporalIndex(relType uint16) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelTypeToken(relType); err != nil {
		return err
	}
	return s.fanOutUniform(func(shard *badgerShard) error {
		return shard.DropRelTemporalIndex(relType)
	})
}

var _ storecontract.RelTypeTemporalCandidateCapability = (*Store)(nil)

// PruneRelTypeTemporalCandidates implements
// store.RelTypeTemporalCandidateCapability (BACKLOG 21c) by routing each
// candidate id to its OWNING shard's envelope index — mirrors
// PruneTemporalCandidates exactly, see that method's doc comment for the
// per-id routing/soundness rationale. A relationship's shard is determined by
// its OWN id (slotOf), matching the fact that rel creation itself routes and
// locks by the rel's own id, not either endpoint's.
func (s *Store) PruneRelTypeTemporalCandidates(relType uint16, ids []types.RelID, opts storecontract.QueryOpts) ([]types.RelID, bool) {
	if s == nil {
		return ids, false
	}
	if opts.ValidAt == 0 && !(opts.ValidStart > 0 && opts.ValidEnd > 0) {
		return ids, false
	}
	if err := s.checkOpen(); err != nil {
		return ids, false
	}

	byShard := make(map[int][]types.RelID)
	kept := make([]types.RelID, 0, len(ids))
	for _, id := range ids {
		idx, ok := s.catalog.shardIndexForSlot(slotOf(id.SnowflakeID()))
		if !ok {
			kept = append(kept, id)
			continue
		}
		byShard[idx] = append(byShard[idx], id)
	}

	prunedAny := false
	for idx, shardIDs := range byShard {
		shardKept, ok := s.shards[idx].PruneRelTypeTemporalCandidates(relType, shardIDs, opts)
		if !ok {
			kept = append(kept, shardIDs...)
			continue
		}
		prunedAny = true
		kept = append(kept, shardKept...)
	}
	if !prunedAny {
		return ids, false
	}
	return kept, true
}
