package badger

import (
	"bytes"
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
// maintenance finds no index yet) — a known race, backlog item 22.

// relTemporalBuildsTotal counts relationship temporal index builds across
// every store in the process, including short-lived ones such as a recovery
// probe (RelTemporalIndexBuildsTotalForTest).
var relTemporalBuildsTotal atomic.Int64

// relTemporalBuildSeeks counts the iterator seeks the build does, across every
// store of the process: the seam of the no-per-relationship-read test.
var relTemporalBuildSeeks atomic.Int64

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
// skipped, its history too.
//
// Backlog 10(a): one read transaction and two forward walks — one over the
// relationship rows (KeyRel), one over the relationship history (KeyHistRel)
// — visiting rids in key order, instead of a point read plus a history prefix
// scan (two transactions) per relationship. A walk steps with Next and seeks
// only past a gap of more than relWalkMaxSteps foreign keys, so a dense type
// costs one seek per range and a sparse one at most one seek per relationship.
// Buffered history writes are snapshotted before the scan and win over it
// (lesson 64), as getRelHistoryByPrefix does; the cache answers before the
// row walk, as prefetchRelScan does.
func (bs *Store) buildRelTypeTemporalIndex(rids []types.RelID) (*indexpkg.TemporalIndex, error) {
	bs.relTemporalBuilds.Add(1)
	relTemporalBuildsTotal.Add(1)
	ti := indexpkg.NewTemporalIndex()
	if len(rids) == 0 {
		return ti, nil
	}
	ids := make([]snowflake.ID, len(rids))
	for i, rid := range rids {
		ids[i] = rid.SnowflakeID()
	}
	slices.Sort(ids) // validated IDs are positive: int64 order is key order
	ids = slices.Compact(ids)
	pending := bs.pendingRelHistoryRows(ids)

	err := bs.view(func(txn *badgerv4.Txn) error {
		rows := newRelBuildWalk(txn, storepkg.KeyRel)
		defer rows.it.Close()
		hist := newRelBuildWalk(txn, storepkg.KeyHistRel)
		defer hist.it.Close()
		for _, id := range ids {
			r, ok, err := bs.relBuildCurrent(rows, id)
			if err != nil {
				return err
			}
			if !ok {
				continue // deleted between snapshot and fetch
			}
			from, to := indexpkg.RelTemporalBounds(id, r.Temporal())
			ti.Extend(id, from, to)
			if err := bs.foldRelBuildHistory(ti, hist, id, pending[id]); err != nil {
				return fmt.Errorf("history fold: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ti, nil
}

// relWalkMaxSteps bounds the foreign keys a build walk steps over with Next
// before it seeks instead.
const relWalkMaxSteps = 32

// relBuildWalk is a forward key walk over one key-tag range of a read
// transaction, values not prefetched.
type relBuildWalk struct {
	it      *badgerv4.Iterator
	prefix  []byte
	started bool
}

func newRelBuildWalk(txn *badgerv4.Txn, tag byte) *relBuildWalk {
	opts := badgerv4.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Prefix = []byte{tag}
	return &relBuildWalk{it: txn.NewIterator(opts), prefix: opts.Prefix}
}

// seekTo positions the walk at the first key >= target (targets ascend).
func (w *relBuildWalk) seekTo(target []byte) {
	if w.started {
		for steps := 0; w.it.ValidForPrefix(w.prefix); steps++ {
			if bytes.Compare(w.it.Item().Key(), target) >= 0 {
				return
			}
			if steps == relWalkMaxSteps {
				break
			}
			w.it.Next()
		}
		if !w.it.ValidForPrefix(w.prefix) {
			return // exhausted: every later target is absent too
		}
	}
	w.started = true
	relTemporalBuildSeeks.Add(1)
	w.it.Seek(target)
}

// relBuildCurrent resolves id's current row as prefetchRelScan does — the
// cache first, then the type's membership, then the row — with the row read
// from the walk. ok=false: the relationship is gone.
func (bs *Store) relBuildCurrent(w *relBuildWalk, id snowflake.ID) (*types.Relationship, bool, error) {
	switch v, status := bs.relCache.GetNoPromote(id); status {
	case indexpkg.CacheHit:
		return v, true, nil
	case indexpkg.CacheDeleted:
		return nil, false, nil
	}
	bs.idxMu.RLock()
	_, exists := bs.relIDs[types.RelID(id)]
	bs.idxMu.RUnlock()
	if !exists {
		return nil, false, nil
	}
	key := storepkg.RelKey(id)
	w.seekTo(key)
	if !w.it.ValidForPrefix(w.prefix) || !bytes.Equal(w.it.Item().Key(), key) {
		return nil, false, nil
	}
	var r *types.Relationship
	err := w.it.Item().Value(func(val []byte) error {
		var wire storepkg.RelWire
		if err := storepkg.SafeUnmarshal(val, &wire); err != nil {
			return fmt.Errorf("graph: unmarshal relationship: %w", err)
		}
		decoded, err := bs.decodeRelWireForKey(wire, id)
		if err != nil {
			return fmt.Errorf("graph: decode relationship: %w", err)
		}
		r = decoded
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return r, true, nil
}

// relBuildPendingRow is one buffered history write: a row or a delete.
type relBuildPendingRow struct {
	raw []byte
	del bool
}

// pendingRelHistoryRows snapshots the buffered history writes of ids (sorted)
// in one pass over the write buffers, newer winning per key as in
// pendingHistoryVersionOverlay.
func (bs *Store) pendingRelHistoryRows(ids []snowflake.ID) map[snowflake.ID]map[uint64]relBuildPendingRow {
	out := make(map[snowflake.ID]map[uint64]relBuildPendingRow)
	bs.rangePending(func(k string, op writeOp) {
		if len(k) != storepkg.SizeHistKey || k[0] != storepkg.KeyHistRel {
			return
		}
		id := storepkg.ParseIDFromKey([]byte(k), 1)
		if _, found := slices.BinarySearch(ids, id); !found {
			return
		}
		rows := out[id]
		if rows == nil {
			rows = make(map[uint64]relBuildPendingRow)
			out[id] = rows
		}
		version := historyVersionFromKey([]byte(k))
		if op.opType == writeOpDelete {
			rows[version] = relBuildPendingRow{del: true}
			return
		}
		rows[version] = relBuildPendingRow{raw: slices.Clone(op.value)}
	})
	return out
}

// foldRelBuildHistory extends id's envelope by every history row: the walk's
// committed rows not shadowed by a buffered write, then the buffered rows.
// Only the temporal block is decoded (a delta carries it verbatim).
func (bs *Store) foldRelBuildHistory(ti *indexpkg.TemporalIndex, w *relBuildWalk, id snowflake.ID, pending map[uint64]relBuildPendingRow) error {
	extend := func(version uint64, raw []byte) error {
		hv, err := bs.historyRelTemporal(id, version, raw)
		if err != nil {
			return err
		}
		from, to := indexpkg.RelTemporalBounds(id, hv.Temporal())
		ti.Extend(id, from, to)
		return nil
	}
	prefix := storepkg.HistRelPrefix(id)
	for w.seekTo(prefix); w.it.ValidForPrefix(prefix); w.it.Next() {
		item := w.it.Item()
		if len(item.Key()) != storepkg.SizeHistKey {
			continue
		}
		version := historyVersionFromKey(item.Key())
		if _, shadowed := pending[version]; shadowed {
			continue
		}
		if err := item.Value(func(val []byte) error { return extend(version, val) }); err != nil {
			return err
		}
	}
	for version, row := range pending {
		if row.del {
			continue
		}
		if err := extend(version, row.raw); err != nil {
			return err
		}
	}
	return nil
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
// current rows and their history, as CreateRelTemporalIndex builds it — or,
// after a clean close, from the open image without reading a row.
func (bs *Store) loadRelTypeTemporalIndexes(img *openImage) error {
	var tokens []uint16
	err := bs.view(func(txn *badgerv4.Txn) error {
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
		if entries, ok := img.takeRelTemporal(tok); ok {
			// The runtime envelope keeps deleted relationships (append-only,
			// see deleteRelByInfo); the open keeps the type's current members,
			// as buildRelTypeTemporalIndex would.
			members := make(map[types.RelID]struct{}, len(rids))
			for _, rid := range rids {
				members[rid] = struct{}{}
			}
			ti := indexpkg.NewTemporalIndex()
			for _, e := range entries {
				if _, ok := members[types.RelID(e.ID)]; ok {
					ti.AddKnownAbsent(e.ID, e.From, e.To)
				}
			}
			bs.idxMu.Lock()
			bs.relTypeTemporalIndexes[tok] = ti
			bs.idxMu.Unlock()
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
// rel-type temporal envelope. Exact erasure and the retention purge doors
// (PurgeRelationshipByInfo, PurgeAdjacentRelsForNode) call it — there every row
// of the relationship, history included, is gone. A plain delete keeps the
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
