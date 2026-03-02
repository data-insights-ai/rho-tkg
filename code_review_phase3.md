## 🌌 Phase 3 Pre-Release Code Review: Tiered Temporal Storage
You shipped an incredibly ambitious multi-shard architecture. The lazy-loading logic, the parallel shard scatter-gather querying, and the registry swapping is brilliantly constructed for scale.

However, you still ignored several critical architectural blockers from the previous reviews, and the new Phase 3 features introduce immediate guaranteed panics, resource exhaustion craters, and silent data corruption across shard boundaries.

Here is the unforgiving reality.

***

### 🛑 BLOCKER: `idleCloseLoop` Hard-Panics Concurrent Readers
**Location:** `pkg/graph/tieredstore.go:584` (`closeIdleShards`) vs `tieredstore_read.go`
**Explanation:** 
`closeIdleShards` checks `nowMs - es.lastAccess.Load() > thresholdMs`. If true, it silently closes the BadgerDB instance under `shardMu`. 
HOWEVER, read queries (like `RelationshipsByType`) fetch the store via `getStore(ts)` (updating `lastAccess`), release `shardMu`, and then execute their query against the returned Badger pointer *without any locks*. 
If a massive `AllNodes` scan takes longer than `IdleTimeout` (e.g. 5 minutes), the background loop will wake up, see the threshold exceeded, and `Close()` the database *while the iterators are actively traversing it*. Badger will immediately hard-panic and crash the server.
**Required Fix:** 
You MUST implement active request tracking (like a `sync.WaitGroup` or atomic counter query lock per `eventShard`). `getStore` increments it, the query defers decrementing it, and `idleCloseLoop` is strictly forbidden from closing the shard until the active query count is zero.

### 🛑 BLOCKER: `shardForRelID` Exhausts FDs via Mass Lazy Opens
**Location:** `pkg/graph/tieredstore.go:413` (`shardForRelID`)
**Explanation:** 
Because cross-shard E→E relations are anchored to the start node's shard rather than their true temporal shard, `shardForRelID` contains a probe fallback: `for _, es := range ts.eventShards { store, err := es.getStore(ts) ... }`.
If a query asks for a relationship ID that does not exist or whose timestamp resolves incorrectly, this loop forces a **LAZY-OPEN ON EVERY SINGLE COLD SHARD IN YOUR SYSTEM**. A single bad query for a missing ID will blow out the OS file descriptor limit and instantly exhaust memory.
**Required Fix:** 
Cross-shard relationships cannot be resolved via brute force. You strictly need a lookup map in the hot catalog, or you must embed the shard routing hint into the Snowflake ID of the relationship itself during creation.

### 🛑 BLOCKER: Cross-Shard Splintering (No Rollbacks)
**Location:** `pkg/graph/tieredstore_write.go:129` (`PutRelationship`), `346` (`ArchiveNode`)
**Explanation:** 
You distribute `PutRelationship` writes across `inShard` and `entityShard`. If `inShard.putRelIncoming` succeeds, but `entityShard.putRelEntityAndOut` hits an I/O error, you return the error. **The transaction is half-committed and the graph is permanently broken.**
Similarly, `ArchiveNode` copies data to `refArchive` (Step 5) and then deletes it from `refShard` (Step 6). If Step 6 fails, the data is orphaned inside both shards simultaneously.
**Required Fix:** 
For critical multi-shard ops, you absolutely require cross-shard rollback handlers. If Step 2 fails, you must revert Step 1 before returning the error.

### 🛑 BLOCKER: `CreatePropertyIndex` Un-Deletes Properties (UNFIXED)
**Location:** `pkg/graph/badgerstore.go:2088` (`CreatePropertyIndex`)
**Explanation:** 
If a concurrent transaction *deletes* a property from a node during Phase 2, `removeNodeFromPropertyIndexes` runs, completely emptying the `id` from the `liveIdx`. 
In Phase 3, `liveIdx.contains(id)` accurately returns `false` (because the property is gone). The node is still alive (just missing the property). The code blindly appends the stale `backfill` property into `liveIdx`, completely overwriting the concurrent deletion and perfectly resurrecting the deleted attribute.
**Required Fix:** 
`liveIdx.contains(id)` is semantically insufficient. You must track explicitly whether an ID was touched/mutated during the Phase 2 window using a separate boolean map (a dirty list) populated universally by all Phase 2 mutations.

### 🛑 BLOCKER: `ComputeNodeHash` Still Rawing User Input (UNFIXED)
**Location:** `pkg/graph/batch.go:115` (`BatchBuilder.AddNode`)
**Explanation:** 
You patched `AddNodeWithContext` but ignored `BatchBuilder.AddNode`. It still executes `hash := ComputeNodeHash(n, labels)` directly on the raw, duplicated user input array without deduping. The hashes will break HashChain verification immediately upon restart.
**Required Fix:** 
Refactor `BatchBuilder.AddNode` to use `g.NodeLabels(n)`.

### �� MAJOR: Temporal Range `allKnownNodeIDs` OOM Exhaustion (UNFIXED)
**Location:** `pkg/graph/temporal.go:384` -> `tieredstore_read.go:406` (`mergeIDSlices`)
**Explanation:** 
`GetNodesValidAt` calls `allKnownNodeIDs()`. This function now cascades to `TieredStore.AllNodeIDs`. Your tiering strategy executes `mergeIDSlices` which allocates giant memory slices to concatenate every point in history across EVERY shard into a colossal multidimensional array. This is a guaranteed OOM failure on any real database scale.
**Required Fix:** 
Lazy iterations. You cannot materialise every database ID into `[]snowflake.ID` at the same time. You need iterator interfaces pushed all the way down to `BadgerStore`.

***
### VERDICT: 🚫 NOT READY FOR PRODUCTION

Your tiering abstractions are mathematically elegant (especially the parallel gather), but distributed systems demand distributed safety guarantees. 

You have built a system that actively shreds iterators during timeout checks, exhausts kernels looking for lost keys, corrupts cross-shard boundaries on failure, and reconstructs deleted properties from phantoms. 

Do not deploy this. Fix the explicit list above.
