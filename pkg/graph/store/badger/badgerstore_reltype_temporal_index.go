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
// Create is CreateTemporalIndex's 3-phase mutated-set install (backlog 22): a
// Building index is installed with the ID snapshot, so every write during the
// build already folds its row into it and records the ID; the build runs
// without the lock; the recorded IDs are re-read in full; then the build is
// merged in and the index published.

// relTemporalBuildsTotal counts relationship temporal index builds across
// every store in the process, including short-lived ones such as a recovery
// probe (RelTemporalIndexBuildsTotalForTest).
var relTemporalBuildsTotal atomic.Int64

// relTemporalBuildSeeks counts the iterator seeks the build does, across every
// store of the process: the seam of the no-per-relationship-read test.
var relTemporalBuildSeeks atomic.Int64

// relTemporalBuildFoldHook, set only by tests, receives every row the build
// folds: (id, history version), -1 for the current row.
var relTemporalBuildFoldHook func(id snowflake.ID, version int64)

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
	// Build the history presence set before any lock: the install and every
	// later create of an uncovered ID ask it under idxMu.
	if err := bs.ensureHistoryPresenceBuilt(&bs.histRelPresence, storepkg.KeyHistRel); err != nil {
		return fmt.Errorf("graph: create relationship temporal index: %w", err)
	}

	// Phase 1: install a Building index and snapshot the type's IDs. From here
	// every write of the type extends the live index and records its ID
	// (Extend marks Mutated); readers skip a Building index.
	bs.idxMu.Lock()
	if _, exists := bs.relTypeTemporalIndexes[relType]; exists {
		bs.idxMu.Unlock()
		return ErrTemporalIndexExists
	}
	live := indexpkg.NewTemporalIndex()
	live.Building = true
	live.Mutated = make(map[snowflake.ID]struct{})
	bs.relTypeTemporalIndexes[relType] = live
	rids := bs.relTypeRelIDsSnapshotLocked(relType)
	bs.idxMu.Unlock()

	fail := func(err error) error {
		bs.idxMu.Lock()
		deleteTemporalIndexIfCurrent(bs.relTypeTemporalIndexes, relType, live)
		bs.idxMu.Unlock()
		return fmt.Errorf("graph: create relationship temporal index: %w", err)
	}

	// Phase 2: build from the snapshot without the lock.
	built, err := bs.buildRelTypeTemporalIndex(rids)
	if err != nil {
		return fail(err)
	}

	// Phase 3a: take the IDs written so far and re-read every row of each
	// (current row and whole history, deleted ones included) without the lock.
	// A write before the re-read is in what it reads; a write after it folds
	// its new row into the live index itself, and its previous row is one the
	// re-read or an earlier tracked write already folded.
	bs.idxMu.Lock()
	written := slices.Collect(maps.Keys(live.Mutated))
	live.Mutated = make(map[snowflake.ID]struct{})
	bs.idxMu.Unlock()
	refold, err := bs.relTemporalRowsOf(written)
	if err != nil {
		return fail(err)
	}

	// Phase 3b: merge (a union, so the order against the live writes does
	// not matter) and publish. An ID first written after the Phase 3a take
	// whose earlier rows Phase 2 did not read (it had no current row when the
	// walk reached it: deleted, history kept) has only that write's row in the
	// live index: its history is folded here, under the lock, so no write can
	// land between the read and the publish. A current row such an ID has was
	// made during the build and folded by its write.
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()
	if err := requireTemporalIndexCurrentForCreate(bs.relTypeTemporalIndexes, relType, live); err != nil {
		return err
	}
	late := live.Mutated
	live.ClearMutationTracking() // the merge below records nothing
	built.ForEachEntry(func(e indexpkg.IntervalEntry) { live.Extend(e.ID, e.From, e.To) })
	refold.ForEachEntry(func(e indexpkg.IntervalEntry) { live.Extend(e.ID, e.From, e.To) })
	for id := range late {
		if _, _, ok := built.EnvelopeOf(id); ok {
			continue
		}
		if _, _, ok := refold.EnvelopeOf(id); ok {
			continue
		}
		if err := bs.foldRelHistoryLocked(live, id); err != nil {
			deleteTemporalIndexIfCurrent(bs.relTypeTemporalIndexes, relType, live)
			return fmt.Errorf("graph: create relationship temporal index: %w", err)
		}
	}
	live.Building = false
	bs.persistRelTypeTemporalIndexDefs()
	return nil
}

// foldRelHistoryLocked extends ti by every history row of id; the RAM history
// presence set answers "has none" without a read. Caller holds idxMu.Lock (lock
// order idxMu -> presence buildMu -> wbMu; the history read takes only wbMu and
// a read transaction).
func (bs *Store) foldRelHistoryLocked(ti *indexpkg.TemporalIndex, id snowflake.ID) error {
	has, err := bs.hasHistory(&bs.histRelPresence, storepkg.KeyHistRel, id, bs.probeHistoryPresence)
	if err != nil || !has {
		return err
	}
	hist, err := bs.getRelHistoryByPrefix(storepkg.HistRelPrefix(id))
	if err != nil {
		return err
	}
	for _, hv := range hist {
		if hv != nil {
			from, to := indexpkg.RelTemporalBounds(id, hv.Temporal())
			ti.Extend(id, from, to)
		}
	}
	return nil
}

// foldRecreatedRelHistoryLocked is called by a create door before it adds r:
// when the published index of r's type does not cover id, id may be a deleted
// relationship whose history is still on disk (deleted before the index was
// built, or before a reopen rebuilt it), and the new row alone would make the
// envelope vouch for less than the ID holds. Its history is folded first — the
// rule PutRelVersion keeps by leaving such an ID uncovered. A Building index
// needs nothing: the create records the ID and the build reads its rows.
// Caller holds idxMu.Lock.
func (bs *Store) foldRecreatedRelHistoryLocked(r *types.Relationship, id snowflake.ID) error {
	ti := bs.relTypeTemporalIndexes[r.TypeToken().Value()]
	if ti == nil || ti.Building {
		return nil
	}
	if _, _, covered := ti.EnvelopeOf(id); covered {
		return nil
	}
	if err := bs.foldRelHistoryLocked(ti, id); err != nil {
		return fmt.Errorf("graph: relationship temporal index history fold: %w", err)
	}
	return nil
}

// relTemporalRowsOf folds every row of ids — the current one if any and the
// whole history, also of a deleted relationship — into a fresh envelope index.
// Point reads: ids are the relationships written during a create.
func (bs *Store) relTemporalRowsOf(ids []snowflake.ID) (*indexpkg.TemporalIndex, error) {
	ti := indexpkg.NewTemporalIndex()
	for _, id := range ids {
		rid := types.RelID(id)
		r, err := bs.prefetchRelScan(rid)
		switch {
		case err == nil:
			relBuildFold(ti, id, -1, r)
		case !errors.Is(err, ErrRelNotFound):
			return nil, err
		}
		hist, err := bs.GetRelHistory(rid)
		if err != nil {
			return nil, err
		}
		for _, hv := range hist {
			if hv != nil {
				relBuildFold(ti, id, int64(hv.Version()), hv)
			}
		}
	}
	return ti, nil
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
//
// Snapshot rules (both reproduced as unsound prunes when broken):
//   - The cache answers before the row walk, as prefetchRelScan does, so the
//     walk follows scanSnapshot's rule: a flush that commits and evicts a row
//     after the transaction opened leaves the snapshot without its latest
//     version. The flush epoch is read before the transaction opens; a cache
//     miss or a history fold under a changed epoch re-pins (new transaction,
//     new walks). One transaction unless a flush lands mid-build.
//   - Buffered history writes are snapshotted before the first transaction
//     (lesson 64) and win over committed rows; a second snapshot after the
//     walk is folded too, so a version an update moved into history during
//     the build is not lost (widening is sound). Other writes during the
//     build are backlog 22.
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

	snap := &relBuildSnap{db: bs.db, epoch: bs.relCache.FlushEpoch}
	defer snap.close()
	kept := make([]bool, len(ids))
	for i, id := range ids {
		r, ok, err := bs.relBuildCurrent(snap, id)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue // deleted between snapshot and fetch
		}
		kept[i] = true
		relBuildFold(ti, id, -1, r)
		if err := bs.foldRelBuildHistory(ti, snap, id, pending[id]); err != nil {
			return nil, fmt.Errorf("history fold: %w", err)
		}
	}
	after := bs.pendingRelHistoryRows(ids)
	for i, id := range ids {
		if !kept[i] {
			continue
		}
		for version, row := range after[id] {
			if row.del {
				continue
			}
			hv, err := bs.historyRelTemporal(id, version, row.raw)
			if err != nil {
				return nil, fmt.Errorf("history fold: %w", err)
			}
			relBuildFold(ti, id, int64(version), hv) // #nosec G115 -- history versions are uint32
		}
	}
	return ti, nil
}

// relBuildFold extends id's envelope by one row.
func relBuildFold(ti *indexpkg.TemporalIndex, id snowflake.ID, version int64, r *types.Relationship) {
	from, to := indexpkg.RelTemporalBounds(id, r.Temporal())
	ti.Extend(id, from, to)
	if relTemporalBuildFoldHook != nil {
		relTemporalBuildFoldHook(id, version)
	}
}

// relWalkMaxSteps bounds the foreign keys a build walk steps over with Next
// before it seeks instead.
const relWalkMaxSteps = 32

// relBuildSnap is the build's read snapshot: one transaction and its two
// walks, re-pinned when the relationship cache's flush epoch moved (see
// scanSnapshot for the rule). One goroutine only.
type relBuildSnap struct {
	db         *badgerv4.DB
	epoch      func() uint64
	pinned     uint64
	txn        *badgerv4.Txn
	rows, hist *relBuildWalk
}

// current returns the snapshot, re-pinned if a flush landed since it opened.
// Targets keep ascending across a re-pin: the new walks seek to the next one.
func (s *relBuildSnap) current() *relBuildSnap {
	if e := s.epoch(); s.txn == nil || e != s.pinned {
		s.close()
		s.pinned = e
		s.txn = newReadTxn(s.db)
		s.rows = newRelBuildWalk(s.txn, storepkg.KeyRel)
		s.hist = newRelBuildWalk(s.txn, storepkg.KeyHistRel)
	}
	return s
}

func (s *relBuildSnap) close() {
	if s.txn == nil {
		return
	}
	s.rows.it.Close()
	s.hist.it.Close()
	endReadTxn(s.txn, true)
	s.txn, s.rows, s.hist = nil, nil, nil
}

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
// from the walk after the miss. ok=false: the relationship is gone.
func (bs *Store) relBuildCurrent(snap *relBuildSnap, id snowflake.ID) (*types.Relationship, bool, error) {
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
	w := snap.current().rows // the epoch is read after the miss
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
func (bs *Store) foldRelBuildHistory(ti *indexpkg.TemporalIndex, snap *relBuildSnap, id snowflake.ID, pending map[uint64]relBuildPendingRow) error {
	extend := func(version uint64, raw []byte) error {
		hv, err := bs.historyRelTemporal(id, version, raw)
		if err != nil {
			return err
		}
		relBuildFold(ti, id, int64(version), hv) // #nosec G115 -- history versions are uint32
		return nil
	}
	w := snap.current().hist
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
	tokens := make([]uint16, 0, len(bs.relTypeTemporalIndexes))
	for tok, ti := range bs.relTypeTemporalIndexes {
		if ti != nil && !ti.Building {
			tokens = append(tokens, tok)
		}
	}
	slices.Sort(tokens)
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
	if ti == nil || ti.Building {
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
