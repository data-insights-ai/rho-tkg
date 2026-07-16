package sharded

import (
	"container/heap"
	"sync/atomic"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badger "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
)

// Change-log (ADR-0007 S3). The sharded store's change-log is the tiered pattern
// (ADR-0005 §2) with the cold-shard machinery stripped out: because EVERY shard is
// an open local badger (never checked out / never cold), reseed is automatic (each
// shard's badger.New folds its own durable LastLSNKey into the shared allocator via
// ChangeLogSeqSource.Observe at open) and there is no persisted refShard watermark
// and no poison gate — a shard that cannot open fails New outright, so the feed
// never has to fence around an unreadable cold watermark.
//
// One store-global allocator (changeLogAllocator) is injected into every shard as
// badger.Config.ChangeLogSeqSource, so every shard's record draws a store-global
// LSN under that shard's wbMu — a total commit order across shards. Each shard
// co-commits its own records + LastLSNKey in its own WriteBatch exactly as
// standalone badger does; this layer only allocates the global LSNs and merges the
// per-shard feeds (barrier-first + W-bounded k-way merge).

// changeLogAllocator is the store-global LSN allocator. Next mints the next LSN
// (called under a shard's wbMu, so within a shard buffer order == LSN order; across
// shards the atomic add gives a total order). Observe folds a shard's durable
// LastLSNKey (read by badger at open) into the seed so the sequence resumes strictly
// above every shard's persisted max — monotonic, so a lower watermark never lowers it.
type changeLogAllocator struct {
	seq atomic.Uint64
}

func (a *changeLogAllocator) Next() uint64 { return a.seq.Add(1) }

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

// Compile-time assertions: the allocator satisfies the badger seam, and the store
// satisfies the three change-log capabilities core type-asserts for.
var (
	_ badger.ChangeLogSeqSource               = (*changeLogAllocator)(nil)
	_ storecontract.ChangeFeedCapability      = (*Store)(nil)
	_ storecontract.ChangeLogStatusCapability = (*Store)(nil)
	_ storecontract.TxChangeLogScope          = (*Store)(nil)
)

// --- store.ChangeLogStatusCapability ---

// ChangeLogEnabled reports whether this store records mutations to its change-log.
func (s *Store) ChangeLogEnabled() bool {
	return s != nil && s.logEnabled
}

// --- store.ChangeFeedCapability ---

// LastCommittedLSN returns the highest durably-committed LSN across all shards. It
// runs the flush-before-read barrier FIRST so every allocated LSN is durable, then
// folds each shard's watermark. A consumer resumes from this value, so it must
// never straddle an un-durable record.
func (s *Store) LastCommittedLSN() (uint64, error) {
	if err := s.checkOpen(); err != nil {
		return 0, err
	}
	if !s.logEnabled {
		return 0, nil
	}
	if err := s.Flush(); err != nil {
		return 0, err
	}
	return s.lastCommittedLSNNoFlush()
}

// lastCommittedLSNNoFlush folds each shard's watermark WITHOUT the barrier (the
// caller already ran it). It is the emission bound W for the merge.
func (s *Store) lastCommittedLSNNoFlush() (uint64, error) {
	var maxLSN uint64
	for _, shard := range s.shards {
		lsn, err := shard.LastCommittedLSN()
		if err != nil {
			return 0, err
		}
		if lsn > maxLSN {
			maxLSN = lsn
		}
	}
	return maxLSN, nil
}

// ChangeFeed materializes up to limit committed records with LSN > afterLSN, in
// ascending store-global LSN order. limit <= 0 returns all available.
func (s *Store) ChangeFeed(afterLSN uint64, limit int) ([]storecontract.ChangeRecord, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	var out []storecontract.ChangeRecord
	err := s.ForEachChange(afterLSN, func(rec storecontract.ChangeRecord) bool {
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
// store-global LSN order. It runs the flush-before-read barrier FIRST (so every
// allocated LSN is durable), captures W = LastCommittedLSN immediately after the
// barrier, and bounds the k-way merge to emit ONLY records with LSN <= W — records
// allocated DURING the drain (heads > W) are deferred to the next poll. The W-bound
// is load-bearing: the barrier alone is insufficient under a concurrent writer
// (ADR-0005 Finding-1). fn runs OUTSIDE the merge's shard reads.
func (s *Store) ForEachChange(afterLSN uint64, fn func(storecontract.ChangeRecord) bool) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return storecontract.ErrInvalidStoreMutation
	}
	if !s.logEnabled {
		return nil
	}
	if err := s.Flush(); err != nil {
		return err
	}
	w, err := s.lastCommittedLSNNoFlush()
	if err != nil {
		return err
	}
	if w <= afterLSN {
		return nil
	}
	return s.mergeChangeFeed(afterLSN, w, fn)
}

// mergeChangeFeed is the W-bounded paged k-way merge over every shard's log. Unlike
// tiered it holds no checkout — all shards are open — so it pages each shard's log
// (bounded to LSN <= w) into a small per-shard buffer and emits the globally-smallest
// head until every shard is exhausted.
func (s *Store) mergeChangeFeed(afterLSN, w uint64, fn func(storecontract.ChangeRecord) bool) error {
	type shardCursor struct {
		shard  *badger.Store
		cursor uint64
		buf    []storecontract.ChangeRecord
		pos    int
		done   bool
	}
	cursors := make([]*shardCursor, len(s.shards))
	for i, shard := range s.shards {
		cursors[i] = &shardCursor{shard: shard, cursor: afterLSN}
	}
	page := func(sc *shardCursor) error {
		if sc.done {
			return nil
		}
		recs, err := pageBoundedFeed(sc.shard, sc.cursor, w)
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
	for _, sc := range cursors {
		if err := page(sc); err != nil {
			return err
		}
	}
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

// pageBoundedFeed reads one page from a shard's log and drops any head > w (the
// emission bound). A shard's log is ascending by LSN, so the returned slice is a prefix.
func pageBoundedFeed(shard *badger.Store, afterLSN, w uint64) ([]storecontract.ChangeRecord, error) {
	recs, err := shard.ChangeFeed(afterLSN, changeFeedPageSize)
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

const changeFeedPageSize = 256

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

// --- store.TxChangeLogScope: per-transaction change-log buffer ---
//
// The core brackets a tx/batch's mutations with BeginLogScope … (SetLogDivert
// true/mutation/false)* … CommitLogScope|DiscardLogScope, all under c.mu.Lock. A
// sharded tx may touch multiple shards; the scope is reused PER SHARD (each shard
// buffers records without an LSN while diverted and mints store-global LSNs at
// commit — the allocator is shared, so a rolled-back tx burns no LSN on ANY shard).

func (s *Store) BeginLogScope() error {
	if !s.ChangeLogEnabled() {
		return nil
	}
	for _, shard := range s.shards {
		if err := shard.BeginLogScope(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) SetLogDivert(on bool) {
	if !s.ChangeLogEnabled() {
		return
	}
	for _, shard := range s.shards {
		shard.SetLogDivert(on)
	}
}

func (s *Store) CommitLogScope() error {
	if !s.ChangeLogEnabled() {
		return nil
	}
	// Commit in ascending shard order so cross-shard records within one tx take
	// contiguous LSNs in a deterministic order (self-describing records apply
	// correctly on the replica in LSN order regardless).
	for _, shard := range s.shards {
		if err := shard.CommitLogScope(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) DiscardLogScope() error {
	if !s.ChangeLogEnabled() {
		return nil
	}
	for _, shard := range s.shards {
		if err := shard.DiscardLogScope(); err != nil {
			return err
		}
	}
	return nil
}
