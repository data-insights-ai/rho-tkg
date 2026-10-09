package badger

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync/atomic"

	badgerv4 "github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"

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
// Simpler build than CreateTemporalIndex's 3-phase dance: the relationship IDs
// are snapshotted under idxMu, the index is built from them WITHOUT the lock,
// and then installed under idxMu. A relationship written or updated between
// the snapshot and the install is not folded in (the write path's
// maintenance finds no index yet) — a known race, backlog item 21.

// relTemporalBuildsTotal counts relationship temporal index builds across
// every store in the process, including short-lived ones such as a recovery
// probe (RelTemporalIndexBuildsTotalForTest).
var relTemporalBuildsTotal atomic.Int64

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

	ti, err := bs.buildRelTypeTemporalIndex(rids)
	if err != nil {
		return fmt.Errorf("graph: create relationship temporal index: %w", err)
	}

	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()
	if _, exists := bs.relTypeTemporalIndexes[relType]; exists {
		return ErrTemporalIndexExists
	}
	bs.relTypeTemporalIndexes[relType] = ti
	bs.persistRelTypeTemporalIndexDefs()
	return nil
}

// buildRelTypeTemporalIndex folds the current row and every history version
// of rids into a fresh envelope index. A relationship deleted meanwhile is
// skipped.
func (bs *Store) buildRelTypeTemporalIndex(rids []types.RelID) (*indexpkg.TemporalIndex, error) {
	bs.relTemporalBuilds.Add(1)
	relTemporalBuildsTotal.Add(1)
	ti := indexpkg.NewTemporalIndex()
	for _, rid := range rids {
		r, err := bs.prefetchRelScan(rid)
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue // deleted between snapshot and fetch
			}
			return nil, err
		}
		rawID := rid.SnowflakeID()
		from, to := indexpkg.RelTemporalBounds(rawID, r.Temporal())
		ti.Extend(rawID, from, to)

		hist, err := bs.GetRelHistory(rid)
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue // deleted concurrently — its current row is already gone
			}
			return nil, fmt.Errorf("history fold: %w", err)
		}
		for _, hv := range hist {
			if hv == nil {
				continue
			}
			hf, ht := indexpkg.RelTemporalBounds(rawID, hv.Temporal())
			ti.Extend(rawID, hf, ht)
		}
	}
	return ti, nil
}

// persistRelTypeTemporalIndexDefs writes the indexed rel-type tokens
// (ascending) to RelTypeTemporalIndexDefsKey, or deletes the key when none is
// left. Caller holds the idxMu write lock. Mirror of persistTemporalIndexDefs.
func (bs *Store) persistRelTypeTemporalIndexDefs() {
	tokens := slices.Sorted(maps.Keys(bs.relTypeTemporalIndexes))
	if len(tokens) == 0 {
		bs.appendOps(writeOp{opType: writeOpDelete, key: storepkg.RelTypeTemporalIndexDefsKey})
		return
	}
	data, err := msgpack.Marshal(tokens)
	if err != nil {
		slog.Error("graph: persist relationship temporal index defs: marshal failed", "error", err)
		return // index still works in-memory; will retry on next change
	}
	bs.appendOps(writeOp{opType: writeOpSet, key: storepkg.RelTypeTemporalIndexDefsKey, value: data})
}

// loadRelTypeTemporalIndexes rebuilds the persisted rel-type temporal indexes
// at open, after loadIndexes built the type index: each from the type's
// current rows and their history, as CreateRelTemporalIndex builds it.
func (bs *Store) loadRelTypeTemporalIndexes() error {
	var tokens []uint16
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storepkg.RelTypeTemporalIndexDefsKey)
		if errors.Is(err, badgerv4.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error { return storepkg.SafeUnmarshal(val, &tokens) })
	})
	if err != nil {
		return fmt.Errorf("graph: load relationship temporal index definitions: %w", err)
	}
	for _, tok := range tokens {
		if err := storecontract.ValidateRelTypeToken(tok); err != nil {
			return fmt.Errorf("graph: load relationship temporal index definition type %d: %w", tok, err)
		}
		bs.idxMu.RLock()
		_, done := bs.relTypeTemporalIndexes[tok]
		rids := bs.relTypeRelIDsSnapshotLocked(tok)
		bs.idxMu.RUnlock()
		if done {
			continue
		}
		ti, err := bs.buildRelTypeTemporalIndex(rids)
		if err != nil {
			return fmt.Errorf("graph: rebuild relationship temporal index type %d: %w", tok, err)
		}
		bs.idxMu.Lock()
		bs.relTypeTemporalIndexes[tok] = ti
		bs.idxMu.Unlock()
	}
	return nil
}

// discardRelTypeTemporalIndexDefs deletes the persisted rel-type temporal
// index definitions without building them (Config.DropRelTemporalIndexesAtOpen).
// A read-only open leaves the key alone; it only skips the rebuild.
func (bs *Store) discardRelTypeTemporalIndexDefs() error {
	if bs.readOnly {
		return nil
	}
	if err := bs.db.Update(func(txn *badgerv4.Txn) error {
		return txn.Delete(storepkg.RelTypeTemporalIndexDefsKey)
	}); err != nil {
		return fmt.Errorf("graph: discard relationship temporal index definitions: %w", err)
	}
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
	bs.persistRelTypeTemporalIndexDefs()
	return nil
}

// maintainRelTypeTemporalIndexesAdd / Remove are the write-path maintenance
// entry points every rel-mutation door calls, mirroring
// maintainRelPropertyIndexesAdd/Remove. RAM-only, no disk ops (the data is
// rebuilt on open; only the definitions are persisted). Caller must
// already hold bs.idxMu (every existing maintainRelPropertyIndexes* call site
// does).
func (bs *Store) maintainRelTypeTemporalIndexesAdd(r *types.Relationship, id snowflake.ID) {
	indexpkg.AddRelToTemporalIndexes(bs.relTypeTemporalIndexes, r, id)
}

func (bs *Store) maintainRelTypeTemporalIndexesRemove(r *types.Relationship, id snowflake.ID) {
	indexpkg.RemoveRelFromTemporalIndexes(bs.relTypeTemporalIndexes, r, id)
}

// maintainRelTypeTemporalIndexesPurge removes a relationship from every
// rel-type temporal envelope. Only exact erasure calls it — there every row of
// the relationship, history included, is gone. A plain delete keeps the
// envelope (append-only): the history stays and the envelope stays a sound
// superset of it.
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
