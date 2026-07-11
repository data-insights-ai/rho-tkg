package tiered

import (
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// This file implements the tiered store's cross-shard change-log (ADR-0005 §2):
// a store-global LSN allocator slaved into every shard (changeLogAllocator), a
// flush-before-read durability barrier (Flush), a W-bounded k-way merge over the
// per-shard logs (ForEachChange/ChangeFeed), the watermark fold (LastCommittedLSN),
// and a store-level TxChangeLogScope. Each shard co-commits its own records in its
// own WriteBatch exactly as standalone badger does; this layer only allocates the
// global LSNs, persists the reseed watermark, and merges the per-shard feeds.

// changeLogWatermarkKey is the refShard MetaKV key holding the store-global
// allocator high-water. Reseed at open reads ONLY this key (never opens a cold
// shard); it is written after every log-bearing flush, monotonically.
const changeLogWatermarkKey = "changelog_lsn_watermark"

// ErrChangeLogWatermarkUnreadable fences the change-log capability when the
// reseed watermark on the reference shard cannot be read/decoded at open. The
// store still serves its primary reads/writes; only the feed doors fail closed
// (ADR-0005 §2.1-reseed Problem 1). Recover via RecoverChangeLog once the
// underlying condition is fixed.
var ErrChangeLogWatermarkUnreadable = errors.New("graph: change-log reseed watermark unreadable — change-log fenced")

// changeLogAllocator is the tiered store's store-global change-log LSN allocator
// (ADR-0005 §2.1). It is injected into every shard as
// badger.Config.ChangeLogSeqSource, so every shard's record draws a store-global
// LSN under that shard's wbMu — a total commit order across shards. It also
// persists its high-water to the refShard catalog watermark so reseed at open
// needs no cold-shard opens, and it fails the change-log closed (poisoned) when
// that watermark is unreadable.
type changeLogAllocator struct {
	ts  *Store
	seq atomic.Uint64

	// wmMu serializes watermark persistence so two shards flushing concurrently
	// cannot write the watermark out of order (it must be monotonic on disk).
	wmMu       sync.Mutex
	wmDurable  uint64 // highest watermark written to refShard so far
	poisoned   atomic.Bool
	poisonErr  error
	poisonOnce sync.Once
}

func newChangeLogAllocator(ts *Store) *changeLogAllocator {
	return &changeLogAllocator{ts: ts}
}

// Next mints the next store-global LSN. Called under a shard's wbMu, so within
// one shard buffer order == LSN order; across shards the atomic add gives a
// total order. When poisoned the shards' record production has already been
// disabled (see poison), so Next is not reached; the defensive 0 keeps a stray
// caller from committing a reused LSN.
func (a *changeLogAllocator) Next() uint64 {
	if a.poisoned.Load() {
		return 0
	}
	return a.seq.Add(1)
}

// Observe folds a shard's durable watermark (its LastLSNKey, read at open) into
// the allocator seed so the store-global sequence resumes strictly above every
// shard's persisted max. Monotonic — a lower watermark never lowers the seq.
func (a *changeLogAllocator) Observe(watermark uint64) {
	for {
		cur := a.seq.Load()
		if watermark <= cur {
			return
		}
		if a.seq.CompareAndSwap(cur, watermark) {
			return
		}
	}
}

func (a *changeLogAllocator) isPoisoned() bool { return a.poisoned.Load() }

func (a *changeLogAllocator) poisonError() error {
	if !a.poisoned.Load() {
		return nil
	}
	a.wmMu.Lock()
	defer a.wmMu.Unlock()
	return a.poisonErr
}

// poison fences the change-log: it disables record production on every open
// shard (so no mutation commits a reused LSN) and records the sticky error the
// feed doors return until RecoverChangeLog clears it.
func (a *changeLogAllocator) poison(err error) {
	a.poisonOnce.Do(func() {
		a.wmMu.Lock()
		a.poisonErr = err
		a.wmMu.Unlock()
		a.poisoned.Store(true)
		// Disable record production on the reference shard (already open with the
		// log enabled). Event/hot shards opened after this see poisoned==true in
		// badgerCfg and open without the change-log at all.
		if a.ts.refShard != nil {
			a.ts.refShard.DisableChangeLog()
		}
	})
}

// reseedFromRefShard reads the refShard catalog watermark and folds it into the
// allocator seed (covering cold shards not opened at startup). An unreadable /
// wrong-length watermark poisons the change-log (fail closed) rather than
// reseeding below a durable cold-shard LSN. An absent watermark is fine (fresh
// store, or a store whose shards' own LastLSNKeys were already Observed).
func (a *changeLogAllocator) reseedFromRefShard() {
	if a.ts.refShard == nil {
		return
	}
	raw, err := a.ts.refShard.MetaGet(changeLogWatermarkKey)
	if err != nil {
		a.poison(fmt.Errorf("%w: read: %v", ErrChangeLogWatermarkUnreadable, err))
		return
	}
	if raw == nil {
		return // absent — nothing persisted yet
	}
	if len(raw) != 8 {
		a.poison(fmt.Errorf("%w: value size %d, want 8", ErrChangeLogWatermarkUnreadable, len(raw)))
		return
	}
	wm := binary.BigEndian.Uint64(raw)
	a.Observe(wm)
	a.wmMu.Lock()
	if wm > a.wmDurable {
		a.wmDurable = wm
	}
	a.wmMu.Unlock()
}

// persistWatermark writes the current allocator high-water to the refShard
// catalog watermark, monotonically. Invoked from a shard's flush() AFTER its
// change-log records commit (badger.Config.OnChangeLogFlush), so the durable
// watermark is always >= that shard's just-committed max LSN. The refShard's own
// flush calls this too; a MetaSet on refShard is an async queued write. Monotonic
// under wmMu so concurrent shard flushes never regress it.
func (a *changeLogAllocator) persistWatermark() error {
	if a.poisoned.Load() {
		return nil
	}
	hw := a.seq.Load()
	a.wmMu.Lock()
	defer a.wmMu.Unlock()
	if hw <= a.wmDurable {
		return nil // already persisted a >= value
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, hw)
	if a.ts.refShard == nil {
		return nil
	}
	if err := a.ts.refShard.MetaSet(changeLogWatermarkKey, buf); err != nil {
		return err
	}
	a.wmDurable = hw
	return nil
}

// --- store.ChangeLogStatusCapability ---

// ChangeLogEnabled reports whether this store records mutations to its
// change-log. False when opened without the log OR when the change-log has been
// fenced (poisoned) by an unreadable reseed watermark.
func (ts *Store) ChangeLogEnabled() bool {
	if ts == nil {
		return false
	}
	return ts.logEnabled && ts.changeLogAlloc != nil && !ts.changeLogAlloc.isPoisoned()
}

// changeLogFenceErr returns the sticky poison error when the change-log is
// fenced, else nil. Every feed door consults it first and fails closed.
func (ts *Store) changeLogFenceErr() error {
	if ts.changeLogAlloc == nil {
		return nil
	}
	return ts.changeLogAlloc.poisonError()
}

// RecoverChangeLog re-reads the refShard reseed watermark and, if it is now
// readable, clears the change-log fence in place (no close/reopen) and
// re-enables record production on the currently-open shards — mirroring
// RecoverBackgroundError. Returns the still-sticky error if the watermark
// remains unreadable, or nil when the log was never fenced.
func (ts *Store) RecoverChangeLog() error {
	if ts.changeLogAlloc == nil || !ts.changeLogAlloc.isPoisoned() {
		return nil
	}
	a := ts.changeLogAlloc
	if a.ts.refShard == nil {
		return a.poisonError()
	}
	raw, err := a.ts.refShard.MetaGet(changeLogWatermarkKey)
	if err != nil {
		return a.poisonError()
	}
	if raw != nil && len(raw) != 8 {
		return a.poisonError()
	}
	if len(raw) == 8 {
		a.Observe(binary.BigEndian.Uint64(raw))
	}
	// Clear the fence, then re-enable + (re)wire record production on EVERY
	// currently-open shard (reference, archive if open, and every open event
	// shard) — not just the reference shard. A shard opened WHILE poisoned
	// never received Config.ChangeLogSeqSource (badgerCfg gates it on the
	// allocator's poisoned state at open time), so a plain EnableChangeLog on
	// it would stay forever inert ("a shard opened without it stays off" —
	// see badger.Store.EnableChangeLog). EnableChangeLogWithSource re-wires
	// the allocator (+ the watermark-flush hook for non-reference shards, mirroring
	// badgerCfg's own wiring) so an already-open shard resumes proper
	// change-log production without a close/reopen — closing the silent-feed-
	// loss gap the fail-closed poison gate exists to prevent. Shards opened
	// AFTER this point see isPoisoned()==false in badgerCfg and are wired
	// normally at open, so no fix is needed there.
	a.wmMu.Lock()
	a.poisonErr = nil
	a.wmMu.Unlock()
	a.poisoned.Store(false)
	a.poisonOnce = sync.Once{}
	return ts.forEachOpenShard(func(bs *BadgerStore) error {
		if bs == a.ts.refShard {
			bs.EnableChangeLogWithSource(a, nil)
		} else {
			bs.EnableChangeLogWithSource(a, a.persistWatermark)
		}
		return nil
	})
}

// --- store.ChangeFeedCapability ---

// LastCommittedLSN returns the highest durably-committed change-log LSN across
// all shards. It runs the flush-before-read barrier FIRST so every allocated LSN
// is durable, then folds each open shard's watermark (ADR-0005 §2.2) — after the
// barrier this equals the allocator high-water. A consumer resumes from this
// value, so it must never straddle an un-durable record.
func (ts *Store) LastCommittedLSN() (uint64, error) {
	if err := ts.checkOpen(); err != nil {
		return 0, err
	}
	if err := ts.changeLogFenceErr(); err != nil {
		return 0, err
	}
	if !ts.logEnabled {
		return 0, nil
	}
	if err := ts.Flush(); err != nil {
		return 0, err
	}
	var maxLSN uint64
	err := ts.forEachOpenShard(func(bs *BadgerStore) error {
		lsn, err := bs.LastCommittedLSN()
		if err != nil {
			return err
		}
		if lsn > maxLSN {
			maxLSN = lsn
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return maxLSN, nil
}

// ChangeFeed materializes up to limit committed records with LSN > afterLSN, in
// ascending LSN order (the store-global order). limit <= 0 returns all available.
func (ts *Store) ChangeFeed(afterLSN uint64, limit int) ([]storecontract.ChangeRecord, error) {
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	var out []storecontract.ChangeRecord
	err := ts.ForEachChange(afterLSN, func(rec storecontract.ChangeRecord) bool {
		payload := make([]byte, len(rec.Payload))
		copy(payload, rec.Payload)
		out = append(out, storecontract.ChangeRecord{LSN: rec.LSN, Tag: rec.Tag, Payload: payload})
		return limit <= 0 || len(out) < limit
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ForEachChange streams committed records with LSN > afterLSN in ascending
// store-global LSN order (ADR-0005 §2.2). It runs the flush-before-read barrier
// FIRST (so every allocated LSN is durable), captures W = LastCommittedLSN
// immediately after the barrier, and bounds the k-way merge to emit ONLY records
// with LSN <= W — records allocated DURING the drain (heads > W) are deferred to
// the next poll (whose own barrier makes them durable). The W-bound is
// load-bearing: the barrier alone is insufficient under a concurrent writer
// (ADR-0005 Finding-1). fn runs OUTSIDE every shard checkout, so it may re-enter
// store methods; the payload it receives is valid only for the call.
func (ts *Store) ForEachChange(afterLSN uint64, fn func(storecontract.ChangeRecord) bool) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	if err := ts.changeLogFenceErr(); err != nil {
		return err
	}
	if !ts.logEnabled {
		return nil
	}
	// Barrier: flush every open shard's async buffer so no allocated LSN is
	// buffered-but-invisible, then capture the emission bound W.
	if err := ts.Flush(); err != nil {
		return err
	}
	w, err := ts.lastCommittedLSNNoFlush()
	if err != nil {
		return err
	}
	if w <= afterLSN {
		return nil
	}
	return ts.mergeChangeFeed(afterLSN, w, fn)
}

// lastCommittedLSNNoFlush folds each open shard's watermark WITHOUT the barrier
// (the caller already ran it). It is the emission bound W for the merge.
func (ts *Store) lastCommittedLSNNoFlush() (uint64, error) {
	var maxLSN uint64
	err := ts.forEachOpenShard(func(bs *BadgerStore) error {
		lsn, err := bs.LastCommittedLSN()
		if err != nil {
			return err
		}
		if lsn > maxLSN {
			maxLSN = lsn
		}
		return nil
	})
	return maxLSN, err
}

// mergeChangeFeed is the W-bounded paged k-way merge (ADR-0005 §2.2 option 1). It
// pages every catalog-listed shard's log (ref + archive + all event shards,
// regardless of tier), buffering a bounded page per shard, and emits the
// globally-smallest LSN <= W until all shards are exhausted. It holds at most ONE
// shard checked out at a time (checkout → page → checkin); only the small
// per-shard buffers coexist in RAM (CLAUDE.md "Sequential ForEach").
func (ts *Store) mergeChangeFeed(afterLSN, w uint64, fn func(storecontract.ChangeRecord) bool) error {
	shards, err := ts.collectLogShards()
	if err != nil {
		return err
	}
	// Per-shard cursor state for the paged merge.
	type shardCursor struct {
		src    *logShardSource
		cursor uint64
		buf    []storecontract.ChangeRecord
		pos    int
		done   bool
	}
	cursors := make([]*shardCursor, len(shards))
	for i := range shards {
		cursors[i] = &shardCursor{src: shards[i], cursor: afterLSN}
	}

	// page refills a cursor's buffer from its shard, bounded to LSN <= w.
	page := func(sc *shardCursor) error {
		if sc.done {
			return nil
		}
		recs, err := sc.src.page(ts, sc.cursor, w)
		if err != nil {
			return err
		}
		if len(recs) == 0 {
			sc.done = true
			return nil
		}
		sc.buf = recs
		sc.pos = 0
		sc.cursor = recs[len(recs)-1].LSN
		return nil
	}

	// Prime each shard's buffer.
	for _, sc := range cursors {
		if err := page(sc); err != nil {
			return err
		}
	}

	// Min-heap over the current head of each non-exhausted shard.
	h := &feedHeap{}
	heap.Init(h)
	for i, sc := range cursors {
		if !sc.done && sc.pos < len(sc.buf) {
			heap.Push(h, feedHeapItem{lsn: sc.buf[sc.pos].LSN, shard: i})
		}
	}
	for h.Len() > 0 {
		top := heap.Pop(h).(feedHeapItem)
		sc := cursors[top.shard]
		rec := sc.buf[sc.pos]
		if rec.LSN > w {
			// Should not happen (pages are w-bounded), but guard the invariant.
			continue
		}
		if !fn(rec) {
			return nil
		}
		sc.pos++
		if sc.pos >= len(sc.buf) {
			if err := page(sc); err != nil {
				return err
			}
		}
		if !sc.done && sc.pos < len(sc.buf) {
			heap.Push(h, feedHeapItem{lsn: sc.buf[sc.pos].LSN, shard: top.shard})
		}
	}
	return nil
}

// feedHeapItem/feedHeap implement a min-heap over shard head LSNs.
type feedHeapItem struct {
	lsn   uint64
	shard int
}

type feedHeap []feedHeapItem

func (h feedHeap) Len() int           { return len(h) }
func (h feedHeap) Less(i, j int) bool { return h[i].lsn < h[j].lsn }
func (h feedHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *feedHeap) Push(x any)        { *h = append(*h, x.(feedHeapItem)) }
func (h *feedHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}

// logShardSource identifies one catalog-listed shard for the feed merge. It
// carries just enough to checkout the right store and page its log; the actual
// checkout/checkin happens inside page (one shard open at a time).
type logShardSource struct {
	kind logShardKind
	es   *EventShard // for kind == logShardEvent
}

type logShardKind int

const (
	logShardRef logShardKind = iota
	logShardArchive
	logShardEvent
)

// page checks out the shard, pages up to changeFeedPageSize records with
// afterLSN < LSN <= w, then checks in — so fn (called by the merge) always runs
// with no shard held. A closed cold shard is opened read-only for the page (its
// log is immutable) and closed by checkin; no flush on a closed shard.
func (s *logShardSource) page(ts *Store, afterLSN, w uint64) ([]storecontract.ChangeRecord, error) {
	switch s.kind {
	case logShardRef:
		ref, checkin, err := ts.checkoutRefShard()
		if err != nil {
			return nil, err
		}
		defer checkin()
		return pageBoundedFeed(ref, afterLSN, w)
	case logShardArchive:
		archive, checkin, err := ts.checkoutArchive()
		if err != nil {
			return nil, err
		}
		defer checkin()
		if archive == nil {
			return nil, nil
		}
		return pageBoundedFeed(archive, afterLSN, w)
	case logShardEvent:
		store, release, err := s.es.checkoutStoreForRead(ts)
		if err != nil {
			return nil, err
		}
		defer release()
		return pageBoundedFeed(store, afterLSN, w)
	}
	return nil, nil
}

// pageBoundedFeed reads one page from a shard's log and drops any head > w (the
// emission bound). Because a shard's log is ascending by LSN, once a record
// exceeds w every later record does too, so the returned slice is a prefix.
func pageBoundedFeed(bs *BadgerStore, afterLSN, w uint64) ([]storecontract.ChangeRecord, error) {
	recs, err := bs.ChangeFeed(afterLSN, changeFeedPageSize)
	if err != nil {
		return nil, err
	}
	for i := range recs {
		if recs[i].LSN > w {
			return recs[:i], nil
		}
	}
	return recs, nil
}

// changeFeedPageSize bounds per-shard page size in the merge (mirrors badger).
const changeFeedPageSize = 256

// collectLogShards snapshots every catalog-listed shard as a feed source under
// ts.mu.RLock (tolerating a rotation mid-feed — the snapshot is stable). The
// merge pages each source with its own checkout/checkin, one at a time.
func (ts *Store) collectLogShards() ([]*logShardSource, error) {
	sources := []*logShardSource{{kind: logShardRef}}
	// Archive is optional (nil until first archive/restore); include it as a
	// source and let page() return nil when absent.
	sources = append(sources, &logShardSource{kind: logShardArchive})
	ts.mu.RLock()
	shards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()
	for _, es := range shards {
		sources = append(sources, &logShardSource{kind: logShardEvent, es: es})
	}
	return sources, nil
}

// forEachOpenShard folds fn over refShard + archive (if open) + every event
// shard, one at a time under checkout/checkin. Used by the watermark folds.
func (ts *Store) forEachOpenShard(fn func(*BadgerStore) error) error {
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return err
	}
	err = fn(ref)
	refCheckin()
	if err != nil {
		return err
	}
	archive, archiveCheckin, archiveErr := ts.checkoutArchive()
	if archiveErr != nil {
		return archiveErr
	}
	if archive != nil {
		err = fn(archive)
		archiveCheckin()
		if err != nil {
			return err
		}
	} else {
		archiveCheckin()
	}
	ts.mu.RLock()
	shards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()
	for _, es := range shards {
		store, release, err := es.checkoutStoreForRead(ts)
		if err != nil {
			return err
		}
		ferr := fn(store)
		release()
		if ferr != nil {
			return ferr
		}
	}
	return nil
}

// --- store.TxChangeLogScope: per-transaction change-log buffer (ADR-0005 §2.5) ---
//
// The core brackets a tx/batch's mutations with BeginLogScope … (SetLogDivert
// true/mutation/false)* … CommitLogScope|DiscardLogScope, all UNDER c.mu.Lock, so
// no concurrent standalone can misroute. A tiered tx touches multiple shards; the
// scope is reused PER SHARD (each shard already buffers records without an LSN
// while diverted and mints store-global LSNs at commit — the store allocator is
// shared, so a rolled-back tx burns no LSN on ANY shard). The shards scoped are
// snapshotted at Begin so Commit/Discard target exactly them.
//
// Cross-shard record order within one tx follows shard-commit order (reference →
// archive → event), matching the common create-reference-then-reference-it
// pattern; each record is self-describing, so a replica applies them correctly in
// LSN order (ADR-0005 §2.4). A shard opened mid-tx (rotation) is a documented
// edge not covered by the snapshot — tiered cascades are already not
// cross-shard-atomic (CLAUDE.md).

// BeginLogScope opens a per-shard record buffer on every currently-open shard and
// remembers that set for Commit/Discard. No-op when the change-log is off/fenced.
func (ts *Store) BeginLogScope() error {
	if !ts.ChangeLogEnabled() {
		return nil
	}
	return ts.forEachScopeShard(func(bs *BadgerStore) error { return bs.BeginLogScope() })
}

// SetLogDivert is the ONE divert seam on tiered. It toggles record diversion into
// the open scope buffer on every scoped shard. The coming redesign (measurements
// 2026-07-11: the badger per-mutation exclusive-lock divert costs ~36% and
// serializes) replaces this global flag with scope-tagged routing — when it
// lands, it lands HERE, in this single helper, not in two dozen call sites.
func (ts *Store) SetLogDivert(on bool) {
	if !ts.ChangeLogEnabled() {
		return
	}
	_ = ts.forEachScopeShard(func(bs *BadgerStore) error { bs.SetLogDivert(on); return nil })
}

// CommitLogScope mints store-global LSNs for each scoped shard's buffered records
// (reference → archive → event order) and flushes them so they co-commit with the
// tx's data.
func (ts *Store) CommitLogScope() error {
	if !ts.ChangeLogEnabled() {
		return nil
	}
	return ts.forEachScopeShard(func(bs *BadgerStore) error { return bs.CommitLogScope() })
}

// DiscardLogScope drops every scoped shard's buffer — a rolled-back tx emits
// nothing and burns no LSN on any shard.
func (ts *Store) DiscardLogScope() error {
	if !ts.ChangeLogEnabled() {
		return nil
	}
	return ts.forEachScopeShard(func(bs *BadgerStore) error { return bs.DiscardLogScope() })
}

// forEachScopeShard folds fn over every currently-open writable shard (refShard +
// archive-if-open + open event shards) WITHOUT lazy-opening a closed cold shard —
// a tx never writes a closed shard. The core serializes tx/batch under c.mu.Lock,
// so the open-shard set is stable across a single scope lifecycle in practice.
func (ts *Store) forEachScopeShard(fn func(*BadgerStore) error) error {
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return err
	}
	err = fn(ref)
	refCheckin()
	if err != nil {
		return err
	}
	if archive := ts.refArchive.Load(); archive != nil {
		ts.archiveActiveReqs.Add(1)
		aerr := fn(archive)
		ts.archiveActiveReqs.Add(-1)
		if aerr != nil {
			return aerr
		}
	}
	ts.mu.RLock()
	shards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()
	for _, es := range shards {
		store, checkin, ok, cerr := es.checkoutOpenStoreForRead(ts)
		if cerr != nil {
			return cerr
		}
		if !ok || store == nil {
			checkin()
			continue
		}
		ferr := fn(store)
		checkin()
		if ferr != nil {
			return ferr
		}
	}
	return nil
}

// Flush is the durability barrier (ADR-0005 §2.1-barrier): it folds
// shard.Flush() over every OPEN shard under checkout/checkin, so every LSN the
// allocator has handed out is either durable or lost-with-its-row — never
// "allocated, buffered, invisible". The feed doors call it before the merge; it
// also makes tiered satisfy storeFlusher, so Export/ExportSince flush before
// snapshotting SnapshotLSN for free. A flush error fails closed (errors.Join) —
// the caller must not serve a possibly-reordered feed.
func (ts *Store) Flush() error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	var joined error
	if err := ts.forEachOpenShard(func(bs *BadgerStore) error {
		return bs.Flush()
	}); err != nil {
		joined = errors.Join(joined, err)
	}
	return joined
}
