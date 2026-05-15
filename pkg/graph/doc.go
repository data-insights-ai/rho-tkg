// Package graph implements the graph layer for the Temporal Knowledge Graph v3.
//
// It owns the label and relationship type registries, dual snowflake ID
// generators (one for nodes, one for relationships), and provides string
// resolution for token-based entities.
//
// # Storage
//
// The Store interface defines pure persistence operations. Three implementations
// are provided:
//
//   - memory.Store: thread-safe in-memory store with hash-set adjacency indexes
//     for O(1) insert/delete. Atomic cascade-delete under single write lock.
//
//   - badger.Store: persistent store using Badger v4. In-memory state is the
//     source of truth while running — LRU entity caches with version-aware
//     dirty tracking, and in-memory indexes (nodeIDs, relIDs, labelIdx,
//     typeIdx, outIdx, inIdx) rebuilt from Badger on startup via loadIndexes().
//
//   - tiered.Store: multi-shard store routing entities across ref shard +
//     time-windowed event shards by ontology classification (RefLabels config).
//     Hot→warm→cold shard rotation. Warm/cold recovery on restart. Cross-shard
//     relationship split-writes. Depth-aware reads via storepkg.ShardDepth.
//
// # badger.Store Architecture
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
// # Temporal Push-Down
//
// storepkg.QueryOpts supports ValidAt (point-in-time) and ValidStart/ValidEnd (interval)
// temporal filters. Both memory.Store and badger.Store evaluate these filters
// before deep-copying entities, avoiding O(N) materialization waste. badger.Store
// uses a two-stage approach: Peek pre-filter for zero-allocation cache hits,
// then post-filter for cache misses.
//
// # tiered.Store
//
// tiered.Store routes entities across multiple badger.Store instances by ontology
// classification. Reference entities (configured via RefLabels) go to a single
// reference shard; event entities go to time-windowed event shards. The hot
// shard receives all new event writes. On window expiry, RotateHotShard demotes
// hot→warm (read-only) and creates a new hot shard. Warm shards recover from
// catalog on restart. Cold shards are lazy-opened on first access and auto-closed
// after idle timeout. Cross-shard relationships use split writes: entity+out/ in
// start shard, in/ in end shard. Merge queries use bounded shard fan-out.
// storepkg.ShardDepth (storepkg.DepthAll/storepkg.DepthHot/storepkg.DepthWarm) controls tier inclusion in queries.
//
// # Transactions
//
// GraphTx is a mutation transaction that holds the graph write lock for its
// duration. It supports full CRUD: AddNode/AddRelationship track created IDs;
// UpdateNode/UpdateRelationship snapshot pre-mutation state; DeleteNode/
// DeleteRelationship snapshot before deletion. Commit releases the lock.
// Rollback restores all mutations in reverse order: re-creates deleted entities,
// reverts updates to snapshots, deletes created entities.
// g.Admin().Reset atomically clears all entities via Store.Clear while preserving
// registries.
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
