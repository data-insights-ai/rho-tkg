### 🛑 BLOCKER: Phase 2 Temporal Queries Cause O(N) Memory and N+1 I/O Devastation
**Location:** `pkg/graph/temporal.go:94`, calls `allKnownNodeIDs()`
**Explanation:** 
To implement history-aware `GetNodesValidAt`, you've created `g.allKnownNodeIDs()`, which loads `store.AllNodes()` AND `store.AllNodeHistoryIDs()`. This materializes *every single Snowflake ID across the entire database's history* into an enormous in-memory map. It then loops over this map, calling `GetNodeAt(id, t)`—a discrete Badger DB point-read transaction—for every single ID. 
If your graph has 10 million entities, this single snapshot query consumes hundreds of megabytes of RAM strictly for IDs, then triggers 10 million individual Badger snapshot reads. This will literally freeze or OOM kill the process. 
**Required Fix:** 
Temporal queries and snapshots over the whole graph MUST NOT be implemented via pointwise ID iteration. You must extend the `Store` interface with a scanner/iterator (e.g. `ScanNodesAndHistory`) that yields pages of bytes dynamically so the temporal filter can be applied lazily without materializing all IDs.

### 🛑 BLOCKER: `DeleteNodeWithContext` Race Condition Corrupts Relationship History
**Location:** `pkg/graph/context.go:187-214`
**Explanation:** 
`DeleteNodeWithContext` correctly acquires `g.entityLocks.LockEntity(id)` for the Node itself. However, it then loops over `outRels` and `inRels`, creating tombstones and calling `g.store.PutRelVersion(rid, ...)` for all connected relationships **WITHOUT holding locks on those relationships.**
If a background worker concurrently calls `UpdateRelationshipWithContext` on one of those relationships, it will read the state, bump the version, and write both current and history keys concurrently with your tombstone logic. This creates a data race resulting in write skews and permanently corrupted relationship version history.
**Required Fix:** 
`DeleteNodeWithContext` must acquire entity locks for *all connected relationships* (using your existing deadlock-free sorted ID order locking mechanism) before generating tombstones for them.

### 🛑 BLOCKER: `DeleteRelationshipWithContext` Missing Locks Entirely
**Location:** `pkg/graph/context.go:235-260`
**Explanation:** 
In `UpdateRelationshipWithContext`, you correctly acquire `g.entityLocks.LockEntity(id)`. But in `DeleteRelationshipWithContext`, there is literally zero locking logic. It reads `GetRelationship`, creates a tombstone, calls `PutRelVersion`, and calls `DeleteRelationship` with no entity locking.
A concurrent update will interleave across the read-modify-delete boundary, easily causing the relationship to be recreated (resurrected) in Badger after deletion, while leaving an orphaned tombstone behind in history.
**Required Fix:** 
Wrap the entire implementation of `DeleteRelationshipWithContext` in `g.entityLocks.LockEntity(id)` / `defer g.entityLocks.UnlockEntity(id)`.

### 🧨 MAJOR: `cascadeDeleteLocked` Skips Property Index Cleanup on Corruption Fallback
**Location:** `pkg/graph/badgerstore.go:1162`
**Explanation:** 
In `cascadeDeleteLocked`, if `bs.getNodeLocked(id)` fails due to cache corruption, it falls back to an O(L) scrub of the `labelIdx`. However, it skips the `removeNodeFromPropertyIndexes` call completely because it doesn't have the node data. This means the phantom deleted node ID is permanently leaked into the in-memory property indexes. Subsequent indexing queries will panic or silently choke when trying to load this zombie block.
**Required Fix:** 
If the node properties cannot be retrieved, you must either iterate all property indexes to purge the ID, or explicitly mark the index as tainted and trigger a background rebuild. 

### 🧨 MAJOR: `Snapshot` Read Consistency is Destroyed by `BatchBuilder`
**Location:** `pkg/graph/batch.go` vs `pkg/graph/temporal.go:555`
**Explanation:** 
You claim `BatchBuilder` acts atomically, but it's just a loop running discrete insertions. Now that Phase 2 has added `Snapshot(t)` temporal queries, this flaw is catastrophic. If `Snapshot(now)` is evaluated while a large batch is being processed halfway across the graph, the snapshot will read a torn state reflecting 50% of the batch. 
**Required Fix:** 
Temporal snapshots demand true read-consistency. You either need a multi-version logical clock (transactions get one timestamp assigned to all entries), or you need a global read/write lock for the graph to stall `Snapshot` while a Batch commits. 

***
### VERDICT: 🚫 NOT READY FOR PRODUCTION

You successfully fixed the index persistence and the cascade delete loops, which is fantastic. But the core concurrency of the temporal tombstoning is completely porous, missing entity locks that will inevitably mangle your deterministic HashChains in production. And the naive `AllNodes` mapping approach to Temporal Queries literally violates Big-O worst-case complexity and will crash your Badger backend on any decent graph scale. 

Lock the relationships, rewrite the Temporal scanner to use iterators, and you'll be there.
