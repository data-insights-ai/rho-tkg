# Lessons — tkg-v3

Patterns that caused real bugs, organized by effectiveness tier.
Read before implementing. Re-read before marking done.

**How tiers work:**
- **Tier A — Audit Rules**: Cross-cutting patterns that require grepping ALL call sites. These FAIL when written as principles — they need checklists.
- **Tier B — Structural Rules**: Patterns baked into architecture (lock ordering, two-phase, split-write). These WORK well as lessons.
- **Tier C — Reference**: Project-specific context. Less likely to cause repeat bugs, but needed for understanding.

---

# Tier A — Audit Rules

These lessons describe patterns that recur because the fix is correct but incomplete.
After applying a fix in this tier, **grep for ALL call sites** — the bug is never in just one place.

---

## A1. Hash Inputs Must Come From Canonical Internal State

Never hash raw user input when the internal representation differs. Token deduplication,
normalization, and ordering happen during construction — the hash must reflect the canonical
state, not the user-provided input.

```
BAD:  labels := []string{"User", "User"}
      n := types.NewNode(id, primaryToken, extraTokens)  // deduplicates to ["User"]
      hash := ComputeNodeHash(n, labels)                  // hashes ["User", "User"]
      // VerifyNodeHashChain later resolves canonical labels → ["User"] → hash mismatch

GOOD: n := types.NewNode(id, primaryToken, extraTokens)
      canonicalLabels := g.NodeLabels(n)                  // ["User"] — deduplicated
      hash := ComputeNodeHash(n, canonicalLabels)         // hashes canonical state
```

**Audit checklist** (this lesson existed when v3.0.30 bug #5 was found — `BatchBuilder.AddNode`
repeated the exact same pattern as v3.0.26's `AddNodeWithContext`. The lesson described the fix
but didn't enforce the audit):

1. `grep -rn 'ComputeNodeHash\|ComputeRelHash' pkg/` — every call site must pass canonical labels/type
2. Check `AddNode`, `AddNodeWithContext`, `BatchBuilder.AddNode`, and any future node-creation path
3. If a new hash-computation path is added, verify it resolves labels via registry, not raw input

---

## A2. Every Mutation Path Needs Entity Locks

If a method mutates entity state (delete, update, create with shared endpoints), it MUST
lock the entity. This was missed for `DeleteRelationshipWithContext` — the only mutation
method without an entity lock.

**Audit checklist:**
1. `grep -rn 'store\.Put\|store\.Delete\|store\.Replace' pkg/graph/` — every Store write call
2. Each must have `LockEntity`/`LockTwo`/`LockMany` before it
3. No lock = data corruption under concurrency

```
BAD:  func DeleteRelationshipWithContext(ctx, id) {
          current, _ := store.GetRelationship(id)  // unlocked read
          store.DeleteRelationship(id)               // unlocked write
      }

GOOD: func DeleteRelationshipWithContext(ctx, id) {
          g.entityLocks.LockEntity(id)
          defer g.entityLocks.UnlockEntity(id)
          current, _ := store.GetRelationship(id)
          store.DeleteRelationship(id)
      }
```

---

## A3. Corruption Paths Must Clean All Indexes

When a corruption fallback skips entity data (because it's unavailable), it must still
clean ALL indexes — not just the ones it can clean cheaply. Leaving stale entries in any
index (label, property, adjacency) causes phantom results in queries.

**Audit checklist:**
1. Every delete path that handles missing entities must purge label, property, AND adjacency indexes
2. Brute-force purge is acceptable for corruption paths

```
BAD:  cleanLabelIndexes(id)      // ✓
      // property indexes? skipped — "we don't have the node data"

GOOD: cleanLabelIndexes(id)                          // ✓
      purgeNodeFromAllPropertyIndexes(indexes, id)   // ✓ brute-force O(V)
```

---

## A4. Index Creation: Visibility + Dirty-Map Tracking

Combines two related bugs from `CreatePropertyIndex`:

**Phase 1 bug (v3.0.26):** The index must be installed as an empty placeholder BEFORE the I/O phase.
Otherwise concurrent writes that check `if idx, ok := indexes[key]; ok` see nothing and skip maintenance.

**Phase 3 bug (v3.0.30):** `contains(id)` during Phase 3 can't distinguish "never touched" from
"property deleted during Phase 2". A concurrent delete removes the ID, `contains()` returns false,
Phase 3 re-adds the stale value.

Solution: Track all IDs mutated during Phase 2 in `propertyIndex.mutated`. Phase 3 checks
`mutated[id]` instead of `contains(id)`.

```
BAD:  Phase 1 (RLock): snapshot IDs, index not installed
      Phase 3: if liveIdx.contains(id) { continue }  // false after concurrent delete

GOOD: Phase 1 (Lock): install empty index, snapshot IDs
      Phase 3: if _, mutated := liveIdx.mutated[id]; mutated { continue }
```

---

# Tier B — Structural Rules

These lessons describe architectural patterns. They work well because they're baked into
the code structure — following the pattern once protects all future code in that area.

---

## B1. Challenge the Plan

A detailed plan is a hypothesis, not a specification. Before implementing any method, ask:
**"Does this algorithm actually deliver what the method name promises?"** and
**"Does this interact with an existing feature that could break it?"** and
**"Does the constraint make sense?"**

Example: `Snapshot(t)` calling `AllNodes()` only returns current tip versions — a "snapshot from
3 days ago" includes today's property values and omits deleted nodes. The name says time-travel;
the algorithm does current-state filtering. Fix: walk the 0x07 history tape.

---

## B2. In-Memory State Must Survive Restart

If it's in memory and it matters, it needs a persistence path and a rebuild path.
Violated twice: once for in-memory indexes (v3.0.10), once for property indexes (v3.0.23).

The pattern: if `loadIndexes()` doesn't rebuild it, it doesn't survive restart.

```
BAD:  CreatePropertyIndex populates bs.propertyIndexes[key] in memory.
      On restart, map is empty. NodesByLabelAndProperty silently degrades to O(N) scan.

GOOD: Serialize index definitions to Badger during flush(). In loadIndexes(),
      read definitions back, scan matching nodes, rebuild in-memory entries.
```

---

## B3. Lock Scope: Fast Mutations vs Slow I/O

Never hold idxMu.Lock during disk I/O. Collect IDs fast, release lock, process outside.

```
BAD:  bs.idxMu.Lock()
      for nodeID := range nodeIDs {
          bs.getNodeLocked(nodeID)  // msgpack deserialization, Badger reads
      }
      bs.idxMu.Unlock()

GOOD: bs.idxMu.RLock()
      ids := collectIDs(nodeIDs)  // snapshot IDs quickly
      bs.idxMu.RUnlock()
      for _, id := range ids {
          node := bs.GetNode(id)  // I/O outside lock
      }
```

---

## B4. Temporal Data Is Append-Only

In a temporal database, you never physically delete history. You append a tombstone.
Phase 1's `DeleteNodeCascade` called `deleteHistoryByPrefix`, erasing the 0x07 tape.

```
BAD:  DeleteNode → remove from store → deleteHistoryByPrefix
GOOD: DeleteNode → append final version with DeletedAt timestamp → set ValidTo
```

---

## B5. Don't Materialize Data You Won't Use

When an in-memory index gives you the answer, use the index.

```
BAD:  nodes, _ := store.NodesByLabel(tok)  // alloc + DeepCopy + sort 5M nodes
      return len(nodes)

GOOD: return len(ms.labelIdx[tok])  // O(1), zero allocations
```

---

## B6. Two-Phase Operations: Preflight Then Apply

Multi-step mutations must be all-or-nothing. Phase 1: read everything, fail fast.
Phase 2: apply everything, no error exits.

```
BAD:  for _, relID := range rels {
          deleteRelLocked(relID)  // mutates indexes on each iteration
      }
      // If iteration 5 fails, iterations 1-4 already mutated indexes.

GOOD: // Phase 1: preflight — read all, mutate nothing
      infos := make([]relDeleteInfo, len(rels))
      for i, relID := range rels {
          info, err := getRelLocked(relID)
          if err != nil { return err }
      }
      // Phase 2: apply — all mutations, no error exits
      for _, info := range infos {
          deleteRelByInfo(info)
      }
```

---

## B7. Multi-Shard Move Must Rollback on Partial Failure

Cross-shard operations that move data (ArchiveNode/RestoreNode) must undo completed
steps when a later step fails. Otherwise partial failure leaves data duplicated or orphaned.

Found in v3.0.30: `ArchiveNode` wrote nodes+rels to archive, then failed deleting from
refShard. Result: entity existed in both shards.

```
BAD:  archiveStore.PutNode(node)        // step 1: succeeds
      archiveStore.PutRelationship(r)   // step 2: succeeds
      refShard.DeleteNode(id)           // step 3: FAILS
      // Node now in both refShard AND archive

GOOD: archiveStore.PutNode(node)
      archiveStore.PutRelationship(r)
      if err := refShard.DeleteNode(id); err != nil {
          archiveStore.DeleteNode(id)   // rollback step 1
          // rollback rels too
          return err
      }
```

---

## B8. Store Boundary = Trust Boundary

Entities must be deep-copied at the store boundary. Cache and caller must never share pointers.

```
BAD:  func PutNode(n *types.Node) {
          ms.nodes[id] = n  // caller and cache share same pointer
      }

GOOD: func PutNode(n *types.Node) {
          ms.nodes[id] = n.DeepCopy()
      }
```

---

## B9. Error Handling: Sentinel Discrimination

Never bare `continue` on error. Check the specific sentinel. Propagate everything else.

```
BAD:  if err != nil { continue }  // swallows corruption, I/O errors

GOOD: if errors.Is(err, ErrNodeNotFound) { continue }  // orphan: skip
      if err != nil { return nil, err }                  // real error: propagate
```

---

## B10. Testing Discipline

- **Coverage gates.** Run `make cover` before marking done. 0% on any public method is a blocker.
- **Node/Rel parity.** They are structural mirrors. Every node test needs a relationship equivalent.
- **Feature interactions.** After happy-path tests: "What existing features could produce different inputs?"
- **Every branch.** Every `case` in a type switch. Every `if/else` path. The test IS the proof.

---

## B11. Concurrency Patterns

- **sync.Once** for idempotent `Close()`. Never nil-guard a function pointer across goroutines.
- **Ascending shard order** for multi-lock acquisition. `LockTwo` normalizes. `LockMany` deduplicates+sorts.
- **TOCTOU retry for dynamic lock sets.** Re-verify after acquiring locks. Adjacency can change between discovery and locking.
- **Atomic counters** outside Badger transactions. OCC conflicts kill concurrent writes.
- **Counters in the same WriteBatch** as data. Separate transactions = crash inconsistency.
- **Version-aware dirty tracking.** `CollectDirty()` is read-only. `MarkFlushed()` is CAS.
- **Last-write-wins buffers.** `map[string]writeOp`, not `[]writeOp`. Retries must not replay stale writes.
- **Tombstones** in cache-first architectures. A cache miss must not fall through to stale Badger data.

---

## B12. Async Persistence

- `Close()` must call `flush()` unconditionally — even when `flushLoop` was never started.
- Background loop errors must be logged (`slog.Error`), never `_ = fn()`.
- Tests verifying durability must `Flush()` or `Close()` before reopening the DB.
- `FlushInterval: 0` means "use default", not "disabled". Use large values to disable.

---

## B13. Array Position Is Not Identity

Never use array position (`i == 0`) as a proxy for semantic identity (`version == 0`).
Array position changes when elements are removed; semantic identity does not.

```
BAD:  if i == 0 { // "genesis version" — breaks after TruncateHistory }

GOOD: if entry.Version() == 0 { // actually genesis — works regardless of truncation }
```

---

## B14. History-Aware Queries Need ID Merging

Temporal queries that should include deleted entities must merge IDs from two sources:
current entities (from `AllNodes()`) and historical entities (from `AllNodeHistoryIDs()`).

```
BAD:  all := store.AllNodes()    // only current — deleted invisible
GOOD: allIDs := merge(store.AllNodes(), store.AllNodeHistoryIDs())
```

---

## B15. sync.RWMutex Is Not Reentrant

If method A holds `RLock` and calls method B which also calls `RLock`, and a writer is waiting
between them, deadlock occurs. Solution: only the outermost method acquires the lock; inner
methods must be lock-free.

```
BAD:  func Snapshot(t) { g.mu.RLock(); GetNodesValidAt(t) /* also RLocks */ }
GOOD: func Snapshot(t) { g.mu.RLock(); getNodesValidAtLocked(t) /* no lock */ }
```

---

## B16. Cross-Shard Split Writes Must Have Defined Ordering

When a relationship spans two shards, the 7-key write is split. Without defined ordering,
partial failures leave the system inconsistent.

- **E→R**: ref shard in/ first (critical path for `Case ← Signal` queries)
- **R→E**: entity shard first (entity is the critical path)

Always verify both endpoints exist before any partial writes begin.

---

## B17. Cold Shard Checkout/Checkin for Reference Counting

Returning a `*BadgerStore` pointer from `getStore()` and releasing the lock creates a race:
`closeIdleShards()` can close the store while a caller is still using it.

```
BAD:  store, _ := es.getStore(ts)   // returns pointer, releases lock
      store.AllNodes(opts)            // idle-close can close store here

GOOD: store, _ := es.checkoutStore(ts)  // increments activeReqs
      defer es.checkinStore()             // decrements when done
```

---

## B18. Shard Rotation: Boundary Alignment + Catalog Update

Two related bugs from v3.0.28:

**Boundary alignment:** Snowflake IDs encode time at millisecond resolution. Shard boundaries
with nanosecond precision create gaps. Fix: truncate to millisecond, add one unit.

```
BAD:  boundary := time.Now()                           // nanosecond precision
GOOD: boundary := time.Now().Truncate(time.Millisecond).Add(time.Millisecond)
```

**Catalog sync:** When rotating hot→warm, the catalog entry's `TimeEnd` must be updated to the
actual rotation time — not left at the original window end. Both in-memory `eventShard.timeEnd`
AND the catalog's `ShardEntry.TimeEnd` must be updated. The catalog is persisted — warm shard
recovery on restart depends on it.

---

## B19. Class-Based Routing Breaks with Multi-Tier Event Shards

With a single hot event shard, routing by `EntityClass` works. Once warm shards exist,
two event entities may live in different shards — both classify as `ClassEvent`, but
`shardForClass(ClassEvent)` returns only the hot shard.

```
BAD:  shardForClass(ClassEvent).PutRelationship(r)  // always hot shard
GOOD: shardForNodeID(startID)                        // actual shard via timestamp
```

Rule: always resolve the **actual shard** (via snowflake ID timestamp or ref probe), never
the **class**. Class tells you where new entities go; shard tells you where existing entities live.

---

# Tier C — Reference

Project-specific patterns documented for context.

---

## C1. Verification Must Handle Deleted Entities

Any verification function that reads entity state must tolerate the entity being deleted.
If the entity has history but no current state, proceed using history alone.

```
BAD:  if err != nil { return false, err }  // ErrNodeNotFound → can't verify deleted
GOOD: if err != nil && !errors.Is(err, ErrNodeNotFound) { return false, err }
```

---

## C2. Dirty-Map Tracking for Concurrent Index Creation

`propertyIndex.contains(id)` during Phase 3 of `CreatePropertyIndex` can't distinguish
"never touched" from "property deleted during Phase 2". Track all IDs mutated during
Phase 2 in `propertyIndex.mutated`. Phase 3 checks `mutated[id]` instead of `contains(id)`.

(See A4 for the full pattern — this is the implementation detail.)

---

## C3. Validation: Allowlists, Recursion, Depth Limits

- **Allowlist > denylist.** Enumerate what's safe. Reject everything else.
- **Recursive.** `[]any{&myStruct{}}` bypasses top-level-only checks.
- **Depth-limited.** Every recursive function on untrusted input needs `maxDepth`.

(Covered in CLAUDE.md design invariants — kept here as the bug origin story.)

---

## C4. ForEach Pattern for OOM-Safe Iteration

When a query needs the union of entity IDs across multiple shards, **never** materialize all
per-shard slices + merge them. Use `ForEach*ID` iterators that call a callback for each ID.

Two constraints make this safe:
1. **Lock reentrancy (B15):** Go's `sync.RWMutex` is NOT reentrant. ForEach holds the store lock,
   so the callback must NOT call store methods (GetNode, etc.) — deadlock. Solution: two-phase.
   Phase 1 collects IDs into a `seen` map (callback is trivial). Phase 2 processes IDs after
   ForEach returns (lock released).
2. **Sequential shard iteration:** TieredStore iterates shards one at a time (not parallel).
   Only one shard is open via checkout/checkin at a time. This trades parallelism for
   ~83% memory reduction.

```
BAD:  ids := store.AllNodeIDs()            // ~176 MB for 12 shards × 10M
      merged := mergeIDSlices(shardSlices) // ~80 MB merge output
      dedup := map[id]struct{}{...}        // ~160 MB dedup map
      // peak: ~416 MB just for node IDs

GOOD: seen := map[id]struct{}{}
      store.ForEachNodeID(func(id) bool { seen[id] = struct{}{}; return true })
      store.ForEachNodeHistoryID(func(id) bool { seen[id] = struct{}{}; return true })
      // peak: ~160 MB (just the dedup map)
```
