package sharded

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Temporal indexes (TemporalIndexCapability) — ADR-0007.
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
	return s.fanOutUniformCreate(
		func(shard *badgerShard) error { return shard.CreateTemporalIndex(labelToken) },
		func(shard *badgerShard) error { return shard.DropTemporalIndex(labelToken) },
	)
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

var _ storecontract.TemporalCandidateCapability = (*Store)(nil)

// PruneTemporalCandidates implements store.TemporalCandidateCapability (B4) by
// ROUTING each candidate id to its OWNING shard's envelope index — no cross-shard
// envelope merge is needed, since the prune only asks "can THIS id's envelope
// overlap the filter?" and every id lives on exactly one shard (slotOf). Each
// shard's local badger.PruneTemporalCandidates decides its own ids; the kept
// subsets are concatenated (core sorts the result, so shard-iteration order is
// irrelevant). ok is true iff at least one shard could prune (has the index for
// the label); a shard without the index keeps all its ids (sound). Returns
// (ids, false) when opts carries no valid-time filter, the store is closed, or no
// shard holds a temporal index — the caller then keeps every candidate.
func (s *Store) PruneTemporalCandidates(labelToken uint16, ids []types.NodeID, opts storecontract.QueryOpts) ([]types.NodeID, bool) {
	if s == nil {
		return ids, false
	}
	if opts.ValidAt == 0 && (opts.ValidStart <= 0 || opts.ValidEnd <= 0) {
		return ids, false
	}
	if err := s.checkOpen(); err != nil {
		return ids, false
	}

	// Partition ids by owning shard; an unroutable id (unclaimed slot) is kept —
	// declining to prune is always sound.
	byShard := make(map[int][]types.NodeID)
	kept := make([]types.NodeID, 0, len(ids))
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
		shardKept, ok := s.shards[idx].PruneTemporalCandidates(labelToken, shardIDs, opts)
		if !ok {
			kept = append(kept, shardIDs...) // this shard has no index → keep all
			continue
		}
		prunedAny = true
		kept = append(kept, shardKept...)
	}
	if !prunedAny {
		return ids, false // no shard could prune → signal "unchanged"
	}
	return kept, true
}
