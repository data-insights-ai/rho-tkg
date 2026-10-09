package core

import (
	"sync"
	"sync/atomic"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// asOfColumnCache caches the columnar snapshot of a label's members AS BELIEVED at a
// fixed past transaction time, keyed by (label token, txAt). It is the fix for the
// X5-temporal aggregation gap: the first build materializes the as-of members (the
// unavoidable version-chain resolution), but the compact primitive columns are then
// CACHED, so repeated same-txAt aggregations (a dashboard "AS OF SYSTEM TIME $t
// RETURN count/sum/…") scan the compact column instead of re-materializing.
//
// The key property — and why this beats the current-state column cache for a TKG —
// is that a past belief is IMMUTABLE under forward ingest. A new version
// (TxFrom = now > txAt), a fresh node, a soft delete (DeletedAt = now > txAt): none
// change the belief at a past txAt, because the as-of resolver selects the version
// with TxFrom <= txAt. ONLY a history rewrite that touches versions with
// TxFrom <= txAt can — compaction, retention purge, truncate/rollback-trim, or a
// past-dated write: a backfilled create, or a DeleteWithTx / UpdateWithTx at a
// caller instant, on the primary, or a replica apply whose stamp (a put's TxFrom, a tombstone's
// TxTo/DeletedAt) is at or below a cached pin. So this cache SURVIVES
// write-active ingest, where the current-state cache is perpetually cold.
//
// `epoch` is bumped on every such history rewrite; a cached column stamps the epoch
// it was built under (via LabelDocValues.Epoch) and is discarded once the epoch
// advances. Correctness rests on completeness of the bump sites (see the callers of
// bump / notePastDatedWrite / noteAppliedTx / noteAppliedNodeStamp), never on a
// per-entry validity check — and on their ORDER: a bump must follow the store
// write it reports. A bump before the write lets a build that starts in between
// read the new epoch, miss the row, and be cached as current (R14).
//
// The cache holds NODE label columns only (asOfCacheKey is a node label token):
// a relationship write can never make a cached column stale. Relationship
// stamps still feed the replica's high-water detector (noteAppliedTx), which is
// conservative and entity-agnostic by design.
type asOfColumnCache struct {
	mu    sync.Mutex
	epoch atomic.Uint64
	cols  map[asOfCacheKey]*indexpkg.LabelDocValues
	// order tracks LRU eviction order (BACKLOG 14f — was FIFO/insertion-order,
	// which undercuts the cache's own stated goal once a workload exceeds
	// asOfCacheCap distinct hot pins: a genuinely hot key inserted early would
	// be evicted ahead of a key touched once and never again. Least-recently-
	// used at index 0, most-recently-used at the tail; get/put move a touched
	// key to the tail via touchLocked. A linear O(cap) scan per touch is
	// deliberately simple — cap is a small fixed constant (64), so this is
	// negligible; a doubly-linked-list O(1) LRU would be premature
	// optimization at this scale.
	order []asOfCacheKey
	// maxAppliedTx is the max entity stamp a replica has applied. A forward apply
	// (stamp >= max) leaves it to the pin check below; an out-of-order apply
	// (stamp < max) is a past-dated write and bumps the epoch.
	maxAppliedTx atomic.Int64
	// maxPin is the highest txAt any build has registered (notePin), evictions
	// included — an upper bound on every cached pin. A node write stamped t
	// changes the belief at every pin >= t, so it must bump iff t <= maxPin. The
	// high-water mark alone misses a replica pin taken above it (the replica's
	// clock runs ahead of the primary's in-flight stamps): a lagging put, or a
	// tombstone, stamped between the two is "forward" to the mark yet inside the
	// cached pin.
	maxPin atomic.Int64
}

type asOfCacheKey struct {
	label uint16
	txAt  int64
}

// asOfCacheCap bounds the number of distinct (label, txAt) column sets held. A
// realistic dashboard queries a handful of named snapshots; the LRU cap keeps memory
// bounded against a flood of distinct pins. Each column is itself size-capped at
// indexpkg.MaxDocValuesNodes (large labels are built one-shot, never cached).
const asOfCacheCap = 64

func newAsOfColumnCache() *asOfColumnCache {
	return &asOfColumnCache{cols: make(map[asOfCacheKey]*indexpkg.LabelDocValues)}
}

// notePastDatedWrite reports a primary write stamped with a caller transaction
// instant t (a backfilled TxFrom; a backdated TxTo once DeleteWithTx /
// UpdateWithTx exist) to the as-of cache. It MUST be called AFTER the door's
// store write — never before: the bump is what discards a column built while
// the write was in flight, and a build that starts after a pre-write bump reads
// the new epoch, misses the row and is cached as current. Every door defers it
// right after resolveBackfillTxFrom (or calls it after its last store write),
// so it also runs after a failed or rolled-back write (an extra bump is only a
// cold cache). t <= 0 (no caller instant: the system clock stamped the write,
// which no pin can precede) is a no-op.
func (c *Core) notePastDatedWrite(t types.Instant) {
	if t <= 0 {
		return
	}
	c.asOfColumns.bump()
}

// minPastDated folds a caller instant into the lowest one seen so far (0 = none),
// for doors that write several rows and report once after the last write.
func minPastDated(cur, t types.Instant) types.Instant {
	if t > 0 && (cur == 0 || t < cur) {
		return t
	}
	return cur
}

// noteAppliedNodeStamp feeds a replica-applied NODE stamp — a put's TxFrom or a
// tombstone's end (tombstoneEnd) — to both detectors: the high-water mark and
// the cached-pin bound. Callers run it after verification (BACKLOG 12f) and
// hold c.mu.Lock, so no build interleaves the apply's store write.
func (c *Core) noteAppliedNodeStamp(t types.Instant) {
	c.asOfColumns.noteAppliedTx(t)
	c.asOfColumns.noteWriteAt(t)
}

// tombstoneEnd returns the instant a delete tombstone ends the entity's belief:
// min(TxTo, DeletedAt) over the positive ones (both equal the delete instant on
// every door today; the min keeps a skewed record conservative). A tombstone's
// TxFrom is its version START, not the delete — feeding TxFrom would let a
// tombstone backdated below a cached pin pass as a forward write. 0 when
// neither is set.
func tombstoneEnd(tm *types.TemporalMetadata) types.Instant {
	if tm == nil {
		return 0
	}
	return minPastDated(minPastDated(0, tm.TxTo), tm.DeletedAt)
}

// noteAppliedTxFrom feeds a replica-applied entity's transaction time to the as-of
// cache's past-dated detector. A nil metadata or non-positive TxFrom is ignored.
// Relationship applies use it as is (high-water mark only — a rel cannot stale a
// node column); node applies use noteAppliedNodeTxFrom.
//
// BACKLOG 12f: every apply_record.go call site invokes this AFTER property/hash
// verification succeeds, never before. A record that will ultimately be
// REJECTED (oversized properties, a hash mismatch) must not be counted as
// "applied" for the out-of-order detector — doing so would be an unforced
// over-invalidation (a rejected record's TxFrom could still trip the
// past-dated check and discard every cached as-of column for an entity that
// never actually landed).
func (c *Core) noteAppliedTxFrom(tm *types.TemporalMetadata) {
	if tm == nil {
		return
	}
	c.asOfColumns.noteAppliedTx(tm.TxFrom)
}

// noteAppliedNodeTxFrom is noteAppliedTxFrom for a node row: the TxFrom also
// bumps when it lies at or below a cached pin (noteAppliedNodeStamp).
func (c *Core) noteAppliedNodeTxFrom(tm *types.TemporalMetadata) {
	if tm == nil {
		return
	}
	c.noteAppliedNodeStamp(tm.TxFrom)
}

// currentEpoch returns the epoch to stamp a build with (read BEFORE building so a
// concurrent history rewrite during the build is caught by put's re-check).
func (a *asOfColumnCache) currentEpoch() uint64 { return a.epoch.Load() }

// bump invalidates every cached as-of column — called at each history-rewrite choke
// point (compaction / retention purge / truncate / past-dated write, after it lands).
func (a *asOfColumnCache) bump() { a.epoch.Add(1) }

// notePin registers txAt as a pin a build is about to cache. buildAsOfColumns
// calls it BEFORE reading the epoch, which makes noteWriteAt exact: a writer
// that stores its row and then loads maxPin either sees this pin (and bumps,
// discarding the build if it raced) or loaded maxPin before the pin was
// registered — then the store write preceded the build's epoch read and the
// build reads the row.
func (a *asOfColumnCache) notePin(txAt types.Instant) {
	p := int64(txAt)
	for {
		cur := a.maxPin.Load()
		if p <= cur || a.maxPin.CompareAndSwap(cur, p) {
			return
		}
	}
}

// noteWriteAt bumps the epoch iff a write stamped t can change a cached belief:
// t <= the highest pin ever cached. t <= 0 is ignored.
func (a *asOfColumnCache) noteWriteAt(t types.Instant) {
	if t > 0 && int64(t) <= a.maxPin.Load() {
		a.bump()
	}
}

// clear invalidates and physically drops cached columns. Exact legal erasure
// cannot leave an unreachable stale snapshot resident because it may still hold
// erased property values; an epoch bump alone only prevents future reads.
func (a *asOfColumnCache) clear() {
	a.mu.Lock()
	a.epoch.Add(1)
	a.cols = make(map[asOfCacheKey]*indexpkg.LabelDocValues)
	a.order = nil
	a.mu.Unlock()
}

// noteAppliedTx records a replica apply's entity stamp: a forward apply advances the
// high-water mark; an out-of-order (past-dated) apply bumps the epoch. A forward
// stamp can still lie inside a cached pin — node applies add that check via
// noteAppliedNodeStamp. txFrom <= 0 (no transaction time) is ignored.
func (a *asOfColumnCache) noteAppliedTx(txFrom types.Instant) {
	tf := int64(txFrom)
	if tf <= 0 {
		return
	}
	for {
		max := a.maxAppliedTx.Load()
		if tf < max {
			a.bump() // past-dated write — a cached past belief may now be stale
			return
		}
		if a.maxAppliedTx.CompareAndSwap(max, tf) {
			return
		}
	}
}

// touchLocked moves key to the most-recently-used end of order (a no-op if
// key is not tracked, e.g. called for a key not yet inserted). Caller holds
// a.mu.
func (a *asOfColumnCache) touchLocked(key asOfCacheKey) {
	for i, k := range a.order {
		if k == key {
			a.order = append(a.order[:i], a.order[i+1:]...)
			a.order = append(a.order, key)
			return
		}
	}
}

// get returns the cached column for key iff it was built under the current epoch and
// already holds every requiredKey; otherwise (nil, false) — the caller rebuilds. A
// hit marks key most-recently-used (see touchLocked / the LRU note on order).
func (a *asOfColumnCache) get(key asOfCacheKey, requiredKeys []string, epoch uint64) (*indexpkg.LabelDocValues, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	col := a.cols[key]
	if col == nil || col.Epoch() != epoch || !col.HasAll(requiredKeys) {
		return nil, false
	}
	a.touchLocked(key)
	return col, true
}

// memberCount returns the member count of the column set cached for key under
// epoch, whatever keys it holds: the label's node count at the pin
// (CountByLabelAt). A hit marks key most-recently-used.
func (a *asOfColumnCache) memberCount(key asOfCacheKey, epoch uint64) (int, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	col := a.cols[key]
	if col == nil || col.Epoch() != epoch {
		return 0, false
	}
	a.touchLocked(key)
	return len(col.IDs()), true
}

// unionKeysFor returns requested unioned with any keys an existing (possibly
// stale-epoch) entry for key already built, so a rebuild is a superset — a column
// once built for {a} then queried for {b} rebuilds for {a,b}, mirroring the
// current-state cache. Safe to consult a stale-epoch entry: it only widens the key
// set, never serves stale values.
func (a *asOfColumnCache) unionKeysFor(key asOfCacheKey, requested []string) []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if col := a.cols[key]; col != nil {
		return indexpkg.UnionKeys(col.Keys(), requested)
	}
	return requested
}

// put installs col for key, but ONLY if the epoch has not advanced since the build
// started (a history rewrite mid-build → discard, the build saw a torn belief).
// Evicts the LEAST-recently-used entry when over the cap; overwriting an
// existing key also marks it most-recently-used.
func (a *asOfColumnCache) put(key asOfCacheKey, col *indexpkg.LabelDocValues, buildEpoch uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.epoch.Load() != buildEpoch {
		return // a history rewrite raced the build — do not cache a possibly-torn column
	}
	if _, exists := a.cols[key]; !exists {
		if len(a.order) >= asOfCacheCap {
			oldest := a.order[0]
			a.order = a.order[1:]
			delete(a.cols, oldest)
		}
		a.order = append(a.order, key)
	} else {
		a.touchLocked(key)
	}
	a.cols[key] = col
}
