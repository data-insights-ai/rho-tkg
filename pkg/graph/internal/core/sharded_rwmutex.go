package core

import "sync"

// shardedRWMutex is a striped reader/writer lock with the EXACT semantics of a
// single sync.RWMutex — a writer fully excludes every reader and every other
// writer — but with the reader fast path spread across N independent stripes so
// that concurrent readers pinned to different stripes never contend on one
// reader-count atomic.
//
// Motivation (ADR-0007 lever #1). The concurrent-ingest write door
// (ingest §14) self-applies on the caller thread under c.mu.RLock, and the
// batch prepare step (batch_queue.go) takes c.mu.RLock per queued node. With a
// plain sync.RWMutex those RLock/RUnlock pairs serialize on the shared
// reader-count cache line, capping N-session throughput at ~2x single-thread
// regardless of core count. Each ingest lane already carries a stable identity
// (BatchBuilder.genLane / Session.lane), so a lane-keyed stripe lets N lanes
// take N distinct locks — near-linear reader scaling — while a writer still
// acquires ALL stripes and therefore excludes everyone.
//
// Semantics preserved exactly:
//   - Readers never conflict with readers (any stripes).
//   - A writer (Lock) acquires every stripe, so it excludes ALL readers and all
//     other writers.
//   - Non-reentrant, exactly like sync.RWMutex: a reader holding a stripe that
//     re-locks the SAME stripe while a writer waits deadlocks — the existing
//     "inner methods must be lock-free" discipline (see CLAUDE.md Concurrency)
//     is unchanged and still required.
//
// Deadlock freedom: a reader holds AT MOST ONE stripe at a time (RLockShard
// returns the single stripe it took; callers never nest two different stripes),
// and every writer acquires stripes in ascending index order, so no lock-order
// cycle exists between two writers or between a reader and a writer.
//
// Drop-in vs. spread: RLock()/RUnlock() use stripe 0 and keep the sync.RWMutex
// signature, so the ~24 non-hot reader sites compile unchanged (they simply
// share stripe 0, exactly as they shared the single mutex before — no
// regression). Only the hot ingest paths call RLockShard(lane)/RUnlockShard to
// fan out. Lock()/Unlock() keep the sync.RWMutex signature, so all writer sites
// compile unchanged.
type shardedRWMutex struct {
	stripes [shardedRWMutexStripes]paddedRWMutex
}

// shardedRWMutexStripes is the stripe count. It is >= the maximum snowflake
// slot count (32, maxSnowflakeSlots) so every valid ingest lane maps 1:1 to its
// own stripe with no collision, and it is a power of two so the hint reduction
// is a mask.
const shardedRWMutexStripes = 32

// paddedRWMutex pads a sync.RWMutex out to a full 64-byte cache line so
// adjacent stripes never share a line — false sharing between stripes would
// defeat the whole point of striping. sync.RWMutex is 24 bytes on 64-bit; the
// pad rounds the struct up to 64.
type paddedRWMutex struct {
	mu sync.RWMutex
	_  [64 - 24]byte
}

// RLock takes stripe 0 for read. Drop-in replacement for sync.RWMutex.RLock —
// used by reader sites that are not on the concurrent-ingest hot path. Pair
// with RUnlock.
func (m *shardedRWMutex) RLock() {
	m.stripes[0].mu.RLock()
}

// RUnlock releases stripe 0. Pair with RLock.
func (m *shardedRWMutex) RUnlock() {
	m.stripes[0].mu.RUnlock()
}

// RLockShard takes the stripe selected by hint for read and returns the stripe
// index actually locked. The caller MUST pass that exact index back to
// RUnlockShard — returning the resolved index (rather than recomputing it)
// keeps the pair correct even if the stripe count changes. Used by the
// concurrent-ingest prepare and apply paths, keyed by ingest lane, so distinct
// lanes fan out to distinct cache lines.
func (m *shardedRWMutex) RLockShard(hint uint) int {
	idx := int(hint & (shardedRWMutexStripes - 1))
	m.stripes[idx].mu.RLock()
	return idx
}

// RUnlockShard releases the stripe returned by RLockShard.
func (m *shardedRWMutex) RUnlockShard(idx int) {
	m.stripes[idx].mu.RUnlock()
}

// Lock acquires every stripe for write in ascending index order, fully
// excluding all readers and all other writers. Drop-in replacement for
// sync.RWMutex.Lock — every writer site compiles unchanged. The ascending
// acquisition order is the same across all writers, so two concurrent writers
// cannot deadlock.
func (m *shardedRWMutex) Lock() {
	for i := range m.stripes {
		m.stripes[i].mu.Lock()
	}
}

// Unlock releases every stripe held by Lock, in reverse acquisition order.
func (m *shardedRWMutex) Unlock() {
	for i := len(m.stripes) - 1; i >= 0; i-- {
		m.stripes[i].mu.Unlock()
	}
}
