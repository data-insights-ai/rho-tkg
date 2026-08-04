package badger

import (
	"errors"
	"fmt"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Relationship-type temporal indexes (BACKLOG 21c) — the rel-side mirror of
// badgerstore_index.go's CreateTemporalIndex/DropTemporalIndex, keyed by
// rel-type token instead of label token. See the relTypeTemporalIndexes field
// doc comment (badgerstore.go) for the RAM-only / not-persisted-across-reopen
// scope decision.
//
// Simpler single-phase build than CreateTemporalIndex's 3-phase
// lock-free-backfill dance: the whole scan runs under one bs.idxMu.Lock, so
// CreateRelTemporalIndex blocks concurrent relationship writes on this store
// for the duration of the backfill. This is a deliberate, documented
// throughput trade-off for what is an infrequent administrative DDL call, not
// a per-request hot path — see CHANGELOG BACKLOG 21c.

// CreateRelTemporalIndex creates a temporal interval index on relationships
// with the given rel-type token. Scans existing relationships of that type
// (current + history, folding both into the per-rel valid-time ENVELOPE — the
// same sound-superset construction CreateTemporalIndex uses) to populate the
// index. Returns ErrTemporalIndexExists if an index already exists for this
// rel type.
func (bs *Store) CreateRelTemporalIndex(relType uint16) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelTypeToken(relType); err != nil {
		return err
	}

	bs.idxMu.Lock()
	if _, exists := bs.relTypeTemporalIndexes[relType]; exists {
		bs.idxMu.Unlock()
		return ErrTemporalIndexExists
	}
	rids := bs.relTypeRelIDsSnapshotLocked(relType)
	bs.idxMu.Unlock()

	ti := indexpkg.NewTemporalIndex()
	for _, rid := range rids {
		r, err := bs.prefetchRelScan(rid)
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue // deleted between snapshot and fetch
			}
			return fmt.Errorf("graph: create relationship temporal index: %w", err)
		}
		rawID := rid.SnowflakeID()
		from, to := indexpkg.RelTemporalBounds(rawID, r.Temporal())
		ti.Extend(rawID, from, to)

		hist, err := bs.GetRelHistory(rid)
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue // deleted concurrently — its current row is already gone
			}
			return fmt.Errorf("graph: create relationship temporal index: history fold: %w", err)
		}
		for _, hv := range hist {
			if hv == nil {
				continue
			}
			hf, ht := indexpkg.RelTemporalBounds(rawID, hv.Temporal())
			ti.Extend(rawID, hf, ht)
		}
	}

	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()
	if _, exists := bs.relTypeTemporalIndexes[relType]; exists {
		return ErrTemporalIndexExists
	}
	bs.relTypeTemporalIndexes[relType] = ti
	return nil
}

// DropRelTemporalIndex removes a rel-type temporal index.
// Returns ErrTemporalIndexNotFound if no index exists.
func (bs *Store) DropRelTemporalIndex(relType uint16) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelTypeToken(relType); err != nil {
		return err
	}

	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()
	if _, exists := bs.relTypeTemporalIndexes[relType]; !exists {
		return ErrTemporalIndexNotFound
	}
	delete(bs.relTypeTemporalIndexes, relType)
	return nil
}

// maintainRelTypeTemporalIndexesAdd / Remove are the write-path maintenance
// entry points every rel-mutation door calls, mirroring
// maintainRelPropertyIndexesAdd/Remove. RAM-only, no disk ops. Caller must
// already hold bs.idxMu (every existing maintainRelPropertyIndexes* call site
// does).
func (bs *Store) maintainRelTypeTemporalIndexesAdd(r *types.Relationship, id snowflake.ID) {
	indexpkg.AddRelToTemporalIndexes(bs.relTypeTemporalIndexes, r, id)
}

func (bs *Store) maintainRelTypeTemporalIndexesRemove(r *types.Relationship, id snowflake.ID) {
	indexpkg.RemoveRelFromTemporalIndexes(bs.relTypeTemporalIndexes, r, id)
}

// maintainRelTypeTemporalIndexesPurge is the shared-seam brute-force removal
// (deleteRelByInfo carries no temporal metadata), mirroring
// maintainRelPropertyIndexesPurge.
func (bs *Store) maintainRelTypeTemporalIndexesPurge(id snowflake.ID) {
	indexpkg.PurgeRelFromAllTemporalIndexes(bs.relTypeTemporalIndexes, id)
}

// PruneRelTypeTemporalCandidates implements
// store.RelTypeTemporalCandidateCapability (BACKLOG 21c, the rel-side mirror
// of PruneTemporalCandidates). See that method's doc comment for the
// sound-superset contract.
func (bs *Store) PruneRelTypeTemporalCandidates(relType uint16, ids []types.RelID, opts QueryOpts) ([]types.RelID, bool) {
	if bs == nil {
		return ids, false
	}
	if opts.ValidAt == 0 && (opts.ValidStart <= 0 || opts.ValidEnd <= 0) {
		return ids, false
	}
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	ti := bs.relTypeTemporalIndexes[relType]
	if ti == nil {
		return ids, false
	}
	kept := make([]types.RelID, 0, len(ids))
	for _, id := range ids {
		from, to, ok := ti.EnvelopeOf(id.SnowflakeID())
		if ok && !storepkg.EnvelopeOverlaps(from, to, opts) {
			continue
		}
		kept = append(kept, id)
	}
	return kept, true
}
