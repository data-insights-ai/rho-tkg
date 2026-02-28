// Package graph implements the graph layer for the Temporal Knowledge Graph v3.
//
// It owns the label and relationship type registries, dual snowflake ID
// generators (one for nodes, one for relationships), and provides string
// resolution for token-based entities.
//
// # Storage
//
// The Store interface defines pure persistence operations. Two implementations
// are provided:
//
//   - MemoryStore: thread-safe in-memory store with hash-set adjacency indexes
//     for O(1) insert/delete. Atomic cascade-delete under single write lock.
//
//   - BadgerStore: persistent store using Badger v4. In-memory state is the
//     source of truth while running — LRU entity caches with version-aware
//     dirty tracking, and in-memory indexes (nodeIDs, relIDs, labelIdx,
//     typeIdx, outIdx, inIdx) rebuilt from Badger on startup via loadIndexes().
//
// # BadgerStore Architecture
//
// Read path: LRU cache hit → return; cache tombstone → ErrNotFound;
// nodeIDs/relIDs O(1) check → short-circuit non-existent entities without
// touching Badger; cache miss → db.View() → populate cache as clean.
//
// Write path: update LRU cache (dirty, monotonic dirtyVer), update in-memory
// indexes, queue writeOps into map[string]writeOp (last-write-wins dedup).
//
// Background flush loop (configurable FlushInterval, default 100ms): swaps
// pending ops under lock, snapshots counters under idxMu.RLock(), writes all
// ops + counters atomically via Badger WriteBatch (blind writes — zero OCC
// conflicts). Counter keys are in the same WriteBatch as entity data —
// no TOCTOU drift on crash recovery. Failed ops re-queued via requeueOps()
// (preserves newer writes over stale retries). CollectDirty() is read-only;
// MarkFlushed() only clears entries matching the collected dirtyVer,
// preventing data loss on concurrent writes.
//
// Background GC loop (default 5min) runs RunValueLogGC periodically.
// Skipped in in-memory mode.
//
// Close() stops goroutines, calls flush() unconditionally (handles InMemory
// mode where flushLoop was never spawned), then closes Badger. Idempotent
// via sync.Once.
//
// # Concurrency
//
// An entity lock manager (256-shard mutex array) serializes operations on
// overlapping entities. AddRelationship locks both endpoints via LockTwo;
// DeleteNode locks the target before cascade. LockTwo acquires shards in
// ascending order to prevent deadlocks. Lock ordering: entity locks → idxMu.
//
// Node and Relationship remain pure-data structs in pkg/types.
// This package is the sole owner of string resolution — entities never
// resolve tokens to strings themselves.
package graph
