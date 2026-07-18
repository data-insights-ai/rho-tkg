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
// past-dated backfill / replica apply. So this cache SURVIVES write-active ingest,
// where the current-state cache is perpetually cold.
//
// `epoch` is bumped on every such history rewrite; a cached column stamps the epoch
// it was built under (via LabelDocValues.Epoch) and is discarded once the epoch
// advances. Correctness rests on completeness of the bump sites (see the callers of
// bump / noteAppliedTx), never on a per-entry validity check.
type asOfColumnCache struct {
	mu    sync.Mutex
	epoch atomic.Uint64
	cols  map[asOfCacheKey]*indexpkg.LabelDocValues
	order []asOfCacheKey // FIFO eviction order, bounded by asOfCacheCap
	// maxAppliedTx is the max entity TxFrom a replica has applied. A forward apply
	// (TxFrom >= max) cannot change any past belief and leaves the cache warm; an
	// out-of-order apply (TxFrom < max) is a past-dated write and bumps the epoch.
	maxAppliedTx atomic.Int64
}

type asOfCacheKey struct {
	label uint16
	txAt  int64
}

// asOfCacheCap bounds the number of distinct (label, txAt) column sets held. A
// realistic dashboard queries a handful of named snapshots; the FIFO cap keeps memory
// bounded against a flood of distinct pins. Each column is itself size-capped at
// indexpkg.MaxDocValuesNodes (large labels are built one-shot, never cached).
const asOfCacheCap = 64

func newAsOfColumnCache() *asOfColumnCache {
	return &asOfColumnCache{cols: make(map[asOfCacheKey]*indexpkg.LabelDocValues)}
}

// noteAppliedTxFrom feeds a replica-applied entity's transaction time to the as-of
// cache's past-dated detector. A nil metadata or non-positive TxFrom is ignored.
func (c *Core) noteAppliedTxFrom(tm *types.TemporalMetadata) {
	if tm == nil {
		return
	}
	c.asOfColumns.noteAppliedTx(tm.TxFrom)
}

// currentEpoch returns the epoch to stamp a build with (read BEFORE building so a
// concurrent history rewrite during the build is caught by put's re-check).
func (a *asOfColumnCache) currentEpoch() uint64 { return a.epoch.Load() }

// bump invalidates every cached as-of column — called at each history-rewrite choke
// point (compaction / retention purge / truncate / backfill honored).
func (a *asOfColumnCache) bump() { a.epoch.Add(1) }

// noteAppliedTx records a replica apply's entity TxFrom: a forward apply advances the
// high-water mark and leaves the cache warm; an out-of-order (past-dated) apply bumps
// the epoch — the only apply class that can change a past belief. txFrom <= 0 (no
// transaction time) is ignored.
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

// get returns the cached column for key iff it was built under the current epoch and
// already holds every requiredKey; otherwise (nil, false) — the caller rebuilds.
func (a *asOfColumnCache) get(key asOfCacheKey, requiredKeys []string, epoch uint64) (*indexpkg.LabelDocValues, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	col := a.cols[key]
	if col == nil || col.Epoch() != epoch || !col.HasAll(requiredKeys) {
		return nil, false
	}
	return col, true
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
// Evicts the oldest entry (FIFO) when over the cap.
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
	}
	a.cols[key] = col
}
