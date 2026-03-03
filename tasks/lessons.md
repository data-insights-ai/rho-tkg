# Lessons — tkg-v3

Patterns that caused real bugs. Code examples are the actionable part.
Design rules are in CLAUDE.md — this file has the BAD/GOOD diffs.

**Tiers:** A = audit (grep all call sites), B = structural, C = reference.

---

# Tier A — Audit Rules

## A1. Hash Canonical State, Not Raw Input
*See CLAUDE.md > Integrity & Indexes*

```
BAD:  labels := []string{"User", "User"}
      hash := ComputeNodeHash(n, labels)  // hashes dupes

GOOD: canonicalLabels := g.NodeLabels(n)  // deduplicated
      hash := ComputeNodeHash(n, canonicalLabels)
```

Audit: `grep -rn 'ComputeNodeHash\|ComputeRelHash' pkg/`

**History:** Found in code_review_topics_v2 as BLOCKER. Reported again as UNFIXED in code_review_topics_v2_round3. Also found in BatchBuilder in code_review_phase3. Finally fixed in v3.0.26 (context.go) and v3.0.30 (batch.go). **Took 3 review rounds to fully fix** — the initial fix missed a second call site.

## A2. Every Mutation Path Needs Entity Locks
*See CLAUDE.md > Concurrency*

```
BAD:  store.DeleteRelationship(id)  // unlocked write

GOOD: g.entityLocks.LockEntity(id)
      defer g.entityLocks.UnlockEntity(id)
      store.DeleteRelationship(id)
```

Audit: `grep -rn 'store\.Put\|store\.Delete\|store\.Replace' pkg/graph/`

**History:** Found in code_review_phase2 as BLOCKER — `DeleteNodeWithContext` had no locks on connected relationships, `DeleteRelationshipWithContext` had zero locking. Fixed in v3.0.25.

## A3. Corruption Paths Must Clean All Indexes
*See CLAUDE.md > Integrity & Indexes*

```
BAD:  cleanLabelIndexes(id)      // property indexes? skipped

GOOD: cleanLabelIndexes(id)
      purgeNodeFromAllPropertyIndexes(indexes, id)  // brute-force O(V)
```

**History:** Found in code_review_phase2 as MAJOR, reported again as still unfixed in code_review_topics_v2. Fixed in v3.0.33.

## A4. Index Creation: Visibility + Dirty-Map Tracking
*See CLAUDE.md > Integrity & Indexes*

```
BAD:  Phase 1 (RLock): snapshot IDs, index not installed
      Phase 3: if liveIdx.contains(id) { continue }

GOOD: Phase 1 (Lock): install empty index, snapshot IDs
      Phase 3: if _, mutated := liveIdx.mutated[id]; mutated { continue }
```

**History:** Found in code_review_topics_v2 as BLOCKER. Reported again as UNFIXED in code_review_topics_v2_round3 and code_review_phase3. Fixed in v3.0.26 (Phase 1 installed under Lock) and v3.0.30 (dirty-map tracking). **3 review rounds** — the conceptual fix was right in round 1, but `contains(id)` vs `mutated[id]` needed a second iteration.

---

# Tier B — Structural Rules

## B1. Challenge the Plan
*See CLAUDE.md > Session Protocol*

Ask: "Does this algorithm deliver what the method name promises?"
Example: `Snapshot(t)` calling `AllNodes()` only returns current tip versions.

**History:** The `Snapshot(t)` design flaw was a theme across ALL reviews. code_review_phase2 found temporal queries ignoring deleted entities. code_review_topics_v2_round3 found `Snapshot` read consistency torn by single mutations. Both required fundamental rethinking, not patches.

## B2. In-Memory State Must Survive Restart
*See CLAUDE.md > Persistence*

```
BAD:  CreatePropertyIndex populates in memory. On restart, empty.
GOOD: Serialize definitions to Badger. loadIndexes() rebuilds.
```

**History:** Found in code_review_phase2 context (index persistence gap was in v3.0.23 fixes). Temporal indexes had the same pattern — fixed in v3.0.36. Pattern: if you create anything in-memory, ask "what happens on restart?"

## B3. Lock Scope: Fast Mutations vs Slow I/O
*See CLAUDE.md > Concurrency*

```
BAD:  bs.idxMu.Lock()
      for nodeID := range nodeIDs { bs.getNodeLocked(nodeID) }  // I/O under lock

GOOD: bs.idxMu.RLock(); ids := collectIDs(nodeIDs); bs.idxMu.RUnlock()
      for _, id := range ids { node := bs.GetNode(id) }         // I/O outside lock
```

**History:** Found in v3.0.23 review. `CreatePropertyIndex` held `idxMu.Lock` during Badger I/O.

## B4. Temporal Data Is Append-Only
*See CLAUDE.md > Version History*

```
BAD:  DeleteNode -> remove -> deleteHistoryByPrefix
GOOD: DeleteNode -> append tombstone with DeletedAt -> set ValidTo
```

**History:** `deleteHistoryByPrefix` was removed in v3.0.23 after code review found it silently destroyed temporal queryability.

## B5. Don't Materialize Data You Won't Use

```
BAD:  nodes, _ := store.NodesByLabel(tok); return len(nodes)  // deep-copy 5M nodes
GOOD: return len(ms.labelIdx[tok])                             // O(1)
```

**History:** O(1) counters were added in v3.0.20 (scan-based), upgraded to store-level atomics in v3.0.22.

## B6. Two-Phase: Preflight Then Apply

```
BAD:  for _, relID := range rels { deleteRelLocked(relID) }  // partial mutations on error

GOOD: // Phase 1: read all, mutate nothing
      infos := preflight(rels)
      // Phase 2: apply all, no error exits
      for _, info := range infos { deleteRelByInfo(info) }
```

**History:** `DeleteNodeCascade` originally had partial mutation on mid-loop error. Fixed in v3.0.14.

## B7. Multi-Shard Move Must Rollback
*See CLAUDE.md > TieredStore*

```
BAD:  archive.PutNode(n); archive.PutRel(r); refShard.Delete(id)  // step 3 fails -> duped

GOOD: archive.PutNode(n); archive.PutRel(r)
      if err := refShard.Delete(id); err != nil { archive.Delete(id); return err }
```

**History:** Found in code_review_phase3 as BLOCKER ("Cross-Shard Splintering — No Rollbacks"). Mitigated in v3.0.29 with `RunRepair` tool, then direct rollback added in v3.0.30.

## B8. Store Boundary = Trust Boundary
*See CLAUDE.md > Defensive Copying*

```
BAD:  ms.nodes[id] = n           // caller and cache share pointer
GOOD: ms.nodes[id] = n.DeepCopy()
```

**History:** Found in v3.0.14 review. Both `PutNode`/`GetNode` in MemoryStore and BadgerStore shared pointers, allowing silent cache corruption.

## B9. Sentinel Discrimination

```
BAD:  if err != nil { continue }                       // swallows real errors
GOOD: if errors.Is(err, ErrNodeNotFound) { continue }  // skip orphan only
```

**History:** Found in v3.0.13 review. Query methods used bare `continue` on ALL errors, silently eating I/O and corruption errors.

## B10. Testing Discipline
*See CLAUDE.md > Testing Rules*

Coverage gates, node/rel parity, feature interactions, every branch tested.

## B11. Concurrency Patterns
*See CLAUDE.md > Concurrency*

sync.Once for Close(). Ascending shard order. TOCTOU retry. Atomic counters outside Badger txns.
Version-aware dirty tracking. Last-write-wins buffers. Tombstones in cache-first.

## B12. Async Persistence
*See CLAUDE.md > Persistence*

Close() must flush unconditionally. Log background errors. Flush before reopen in tests.

## B13. Array Position Is Not Identity
*See CLAUDE.md > Version History*

```
BAD:  if i == 0 { /* genesis */ }           // breaks after TruncateHistory
GOOD: if entry.Version() == 0 { /* genesis */ }
```

**History:** Found in v3.0.23 review. Hash chain verification used `i == 0` as genesis marker, permanently broke after `TruncateNodeHistory`.

## B14. History-Aware Queries Need ID Merging
*See CLAUDE.md > Temporal Queries*

```
BAD:  all := store.AllNodes()                                    // deleted invisible
GOOD: allIDs := merge(store.AllNodeIDs(), store.AllNodeHistoryIDs())
```

**History:** Found in v3.0.23 review. Temporal queries only scanned current tips, making deleted nodes invisible to past-time queries.

## B15. sync.RWMutex Is Not Reentrant
*See CLAUDE.md > Concurrency*

```
BAD:  func Snapshot(t) { g.mu.RLock(); GetNodesValidAt(t) }        // inner RLock -> deadlock
GOOD: func Snapshot(t) { g.mu.RLock(); getNodesValidAtLocked(t) }  // no inner lock
```

## B16. Cross-Shard Split Writes Need Defined Ordering
*See CLAUDE.md > TieredStore*

E->R: ref shard in/ first. R->E: entity shard first. Verify both endpoints exist first.

## B17. Cold Shard Checkout/Checkin
*See CLAUDE.md > TieredStore*

```
BAD:  store, _ := es.getStore(ts); store.AllNodes(opts)       // race with idle-close

ALSO BAD:  // v3.0.30 fix was incomplete
      func checkoutStore(ts) { store := getStore(ts); activeReqs.Add(1) }
      // getStore releases shardMu, then activeReqs increments AFTER — TOCTOU gap

GOOD: func checkoutStore(ts) {
        shardMu.Lock()
        // open store if nil, increment activeReqs, snapshot pointer — all under lock
        shardMu.Unlock()
      }
```

**History:** Found in code_review_phase3 as BLOCKER ("idleCloseLoop Hard-Panics Concurrent Readers"). First fix (v3.0.30) had a TOCTOU gap between `getStore` and `activeReqs.Add(1)`. Fully fixed in v3.0.33 by moving increment inside `shardMu`.

## B18. Shard Rotation: Boundary Alignment + Catalog Sync
*See CLAUDE.md > TieredStore*

```
BAD:  boundary := time.Now()                                              // nanosecond
GOOD: boundary := time.Now().Truncate(time.Millisecond).Add(time.Millisecond)
```

Update both `eventShard.timeEnd` AND `ShardEntry.TimeEnd` on rotation.

## B19. Timestamp Routing, Not Class Routing
*See CLAUDE.md > TieredStore*

```
BAD:  shardForClass(ClassEvent).PutRelationship(r)  // always hot shard
GOOD: shardForNodeID(startID)                        // actual shard via timestamp
```

## B20. Atomic File Persistence: Sync Before Rename

```
BAD:  tmp.Write(data); tmp.Close(); os.Rename(tmp, final)
      // crash before OS writeback = corrupt final file

GOOD: tmp.Write(data); tmp.Sync(); tmp.Close(); os.Rename(tmp, final)
      // Sync forces data to stable storage before rename
```

Audit: `grep -rn 'os.Rename' pkg/` — every write-tmp+rename must have Sync() between Write and Close.

**History:** Found in v3.0.33 review. `shard_catalog.go` and `registry_file.go` both lacked fsync.

## B21. Registry Save Must Be Atomic Across Both Halves

```
BAD:  // SaveLabelRegistry: load file, replace labels, save
      // SaveRelTypeRegistry: load file, replace relTypes, save
      // concurrent call → last writer wins, other half is stale

GOOD: // Single SaveRegistries(labels, relTypes) call writes both atomically
```

**History:** Found in v3.0.33 review. Read-modify-write race between two independent save calls.

## B22. Badger WriteBatch.Flush() Blocks Forever on Closed DB

```
BAD:  bs.db.Close()
      bs.flush()   // hangs: WaitForMark blocks (oracle goroutines stopped)

GOOD: bs.dbClosed.Store(true)  // set BEFORE db.Close()
      bs.db.Close()
      // flush() checks dbClosed and returns ErrDBClosed immediately
```

Badger v4's `WriteBatch.Flush()` → `commit()` → `oracle.readTs()` →
`WaitForMark(context.Background(), ...)` uses a Background context, so it
blocks forever once DB goroutines stop. Fix: add `dbClosed atomic.Bool` to
`BadgerStore`, check it in `flush()` before calling `wb.Flush()`, set it in
`Close()` and in any test that directly closes `bs.db`.

Tests that close `bs.db` directly MUST set `bs.dbClosed.Store(true)` first.

**History:** Found in v3.0.36 as B22 fix.

---

# Tier C — Reference

## C1. Verification Must Handle Deleted Entities
*See CLAUDE.md > Temporal Queries*

```
BAD:  if err != nil { return false, err }                              // can't verify deleted
GOOD: if err != nil && !errors.Is(err, ErrNodeNotFound) { return false, err }
```

**History:** Found in code_review_topics_v2_round3 as BLOCKER. `VerifyNodeHashChain` refused to verify any deleted entity's history. Fixed in v3.0.26.

## C2. Dirty-Map Tracking
*See A4 — implementation detail.*

## C3. Validation: Allowlists, Recursion, Depth Limits
*See CLAUDE.md > Properties*

Allowlist > denylist. Recursive. Depth-limited (`maxDepth`).

## C4. ForEach Pattern for OOM-Safe Iteration
*See CLAUDE.md > Temporal Queries*

```
BAD:  ids := store.AllNodeIDs()            // ~176 MB for 12 shards x 10M
      merged := mergeIDSlices(shardSlices) // ~80 MB merge output
      // peak: ~416 MB

GOOD: seen := map[id]struct{}{}
      store.ForEachNodeID(func(id) bool { seen[id] = struct{}{}; return true })
      // peak: ~160 MB (just the dedup map)
```

Two-phase: callback collects IDs only (lock held). Process after ForEach returns (lock released).

**History:** This was the **most persistent BLOCKER across all 5 code reviews**. Found as BLOCKER in code_review_phase2 (O(N) memory devastation). Reported again in code_review_topics_v2 (MAJOR), code_review_topics_v2_round3 (MAJOR), and code_review_phase3 (MAJOR — now cascading to `mergeIDSlices` across shards). code_review_phase3e_update called it the **sole remaining BLOCKER**. Finally fixed in v3.0.31 with lazy `ForEach*ID` iterators — ~83% memory reduction. **Took 5 review rounds to fix.**

---

# Review Effectiveness Summary

## What Worked Well

1. **Iterative severity tracking**: Marking issues as UNFIXED with "(STILL UNFIXED)" across rounds created clear accountability. Issues couldn't be silently dropped.
2. **Specific location + explanation + required fix**: Every issue included file:line, root cause, and concrete fix action — no ambiguity.
3. **Severity tiers (BLOCKER/MAJOR/MINOR)**: Clear prioritization prevented minor style issues from drowning out data-loss bugs.
4. **Concrete BAD/GOOD diffs**: Showing the exact code pattern to avoid vs. the correct pattern made fixes unambiguous.
5. **Cross-cutting audits**: `grep` commands for hash calls, store writes, and file renames caught issues across all call sites, not just the one that was reported.

## What Did Not Work

1. **Fixes that missed second call sites**: A1 (hash canonical state) was fixed in `AddNodeWithContext` but the same bug in `BatchBuilder.AddNode` was only caught in a later review. **Lesson: every fix needs a grep audit for the same pattern elsewhere.**
2. **"Required Fix" descriptions that were too vague**: Telling the developer to "use lazy iterators" (C4) without specifying the interface shape led to 4 rounds of partial fixes before the `ForEach` callback pattern was finally adopted. **Lesson: the fix description should include the exact interface or function signature.**
3. **Not tracking carry-forward explicitly**: Issues marked UNFIXED relied on the reviewer remembering them across sessions. A formal carry-forward tracker (like the todo.md table) would have prevented issues from being re-discovered as if new. **Lesson: maintain a living issue list separate from review documents.**
4. **Single-file review scope**: Reviews that focused on one file at a time missed cross-file interactions (e.g., `batch.go` hash bug only caught when reviewing `batch.go` specifically, not when reviewing `integrity.go` where the hash function lives). **Lesson: review by feature, not by file.**
5. **Rollback patterns deferred as "mitigated"**: The cross-shard rollback issue (B7) was accepted as "mitigated" by a repair tool in v3.0.29 when it should have been fixed inline. It needed a second fix in v3.0.30. **Lesson: repair tools are complements, not substitutes for correctness.**
