### 🛑 BLOCKER: `CreatePropertyIndex` Irreparably Misses Concurrent Mutations
**Location:** `pkg/graph/badgerstore.go:2049-2095` (`CreatePropertyIndex`)
**Explanation:** 
Phase 2 iterates over the snapshotted `ids`, calling `GetNode(id)` and building the index `idx` entirely outside any locks. This is great for preventing I/O stalls. However, if any concurrent transaction creates or modifies a node with that label and property during Phase 2, that mutation checks `bs.propertyIndexes[key]` (which does not exist yet) and skips updating the index. Phase 3 then locks and blindly overwrites `bs.propertyIndexes[key] = idx`. 
Any data written during the Phase 2 window is permanently missing from the new index.
**Required Fix:** 
Install an empty "building" state marker in `bs.propertyIndexes` during Phase 1 under the lock. Concurrent writes must queue their updates against this marker, and Phase 3 must merge the queued updates with the newly built `idx` before finalizing it.

### 🛑 BLOCKER: `ComputeNodeHash` Hashes Unsanitized Inputs, Breaking HashChains
**Location:** `pkg/graph/context.go:94` vs `pkg/graph/integrity.go:143`
**Explanation:** 
`AddNodeWithContext` allows users to pass arrays with duplicate labels (e.g., `[]string{"User", "User"}`). Your internal `types.NewNode` engine gracefully deduplicates the canonical `extraLabels`. BUT `AddNodeWithContext` passes the *raw, un-deduplicated* input `labels` slice into `ComputeNodeHash` to generate the integrity seal.
When `VerifyNodeHashChain` is run, it correctly pulls the canonical labels out via `g.NodeLabels(current)` and hashes those. The output hashes will violently mismatch, and the database will throw unrecoverable chain corruption errors forever.
**Required Fix:** 
In `AddNodeWithContext`, generate the hash using the canonically parsed outputs: `hash := ComputeNodeHash(n, g.NodeLabels(n))` instead of using the raw user input slice.

### 🧨 MAJOR: Corruption Fallback Skips Property Indexes (Still)
**Location:** `pkg/graph/badgerstore.go:1210` (`cascadeDeleteLocked`)
**Explanation:** 
If `getNodeLocked` returns an error (due to data corruption or missing payload), you correctly fallback to scrubbing the `labelIdx` via an O(L) scan, which brilliantly keeps your new O(1) `LabelStats` counters perfectly accurate even during a bit-flip. But you still don't scrub `propertyIndexes`. The phantom node ID remains heavily lodged in the property index sets.
**Required Fix:** 
Apply the exact same fault-tolerant scrubbing logic to iterate over `bs.propertyIndexes` to wipe the phantom `id`.

### 🧨 MAJOR: Temporal Indexing O(N) Complexity Left Unresolved
**Location:** `pkg/graph/temporal.go:164`
**Explanation:** 
You introduced `QueryOpts` for standard queries (like `NodesByLabelAndProperty`) but left `GetNodesValidDuring` and `GetNodesValidAt` dependent on `allKnownNodeIDs()`, which loads `Store.AllNodes()` + `Store.AllNodeHistoryIDs()`. This literally materializes the entire database into memory for every interval request.
**Required Fix:** 
Integrate `QueryOpts` into the Temporal scanners, or replace `allKnownNodeIDs` with a lazy iterator to avoid catastrophic memory consumption on large intervals.

***
### VERDICT: 🚫 NOT READY FOR PRODUCTION

The O(1) label tracking and configurable validation limits ported from `tkg2026-v2` are phenomenally well integrated and heavily hardened. The memory structure and test coverage is brilliant.

However, the `CreatePropertyIndex` concurrency bug is a massive data-loss vulnerability disguised as a performance optimization. Coupled with the HashChain destruction via unsanitized labels, the core database guarantees are currently violated under edge conditions. Lock these leaks, and you are ready.
