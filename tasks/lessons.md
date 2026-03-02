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

## A2. Every Mutation Path Needs Entity Locks
*See CLAUDE.md > Concurrency*

```
BAD:  store.DeleteRelationship(id)  // unlocked write

GOOD: g.entityLocks.LockEntity(id)
      defer g.entityLocks.UnlockEntity(id)
      store.DeleteRelationship(id)
```

Audit: `grep -rn 'store\.Put\|store\.Delete\|store\.Replace' pkg/graph/`

## A3. Corruption Paths Must Clean All Indexes
*See CLAUDE.md > Integrity & Indexes*

```
BAD:  cleanLabelIndexes(id)      // property indexes? skipped

GOOD: cleanLabelIndexes(id)
      purgeNodeFromAllPropertyIndexes(indexes, id)  // brute-force O(V)
```

## A4. Index Creation: Visibility + Dirty-Map Tracking
*See CLAUDE.md > Integrity & Indexes*

```
BAD:  Phase 1 (RLock): snapshot IDs, index not installed
      Phase 3: if liveIdx.contains(id) { continue }

GOOD: Phase 1 (Lock): install empty index, snapshot IDs
      Phase 3: if _, mutated := liveIdx.mutated[id]; mutated { continue }
```

---

# Tier B — Structural Rules

## B1. Challenge the Plan
*See CLAUDE.md > Session Protocol*

Ask: "Does this algorithm deliver what the method name promises?"
Example: `Snapshot(t)` calling `AllNodes()` only returns current tip versions.

## B2. In-Memory State Must Survive Restart
*See CLAUDE.md > Persistence*

```
BAD:  CreatePropertyIndex populates in memory. On restart, empty.
GOOD: Serialize definitions to Badger. loadIndexes() rebuilds.
```

## B3. Lock Scope: Fast Mutations vs Slow I/O
*See CLAUDE.md > Concurrency*

```
BAD:  bs.idxMu.Lock()
      for nodeID := range nodeIDs { bs.getNodeLocked(nodeID) }  // I/O under lock

GOOD: bs.idxMu.RLock(); ids := collectIDs(nodeIDs); bs.idxMu.RUnlock()
      for _, id := range ids { node := bs.GetNode(id) }         // I/O outside lock
```

## B4. Temporal Data Is Append-Only
*See CLAUDE.md > Version History*

```
BAD:  DeleteNode -> remove -> deleteHistoryByPrefix
GOOD: DeleteNode -> append tombstone with DeletedAt -> set ValidTo
```

## B5. Don't Materialize Data You Won't Use

```
BAD:  nodes, _ := store.NodesByLabel(tok); return len(nodes)  // deep-copy 5M nodes
GOOD: return len(ms.labelIdx[tok])                             // O(1)
```

## B6. Two-Phase: Preflight Then Apply

```
BAD:  for _, relID := range rels { deleteRelLocked(relID) }  // partial mutations on error

GOOD: // Phase 1: read all, mutate nothing
      infos := preflight(rels)
      // Phase 2: apply all, no error exits
      for _, info := range infos { deleteRelByInfo(info) }
```

## B7. Multi-Shard Move Must Rollback
*See CLAUDE.md > TieredStore*

```
BAD:  archive.PutNode(n); archive.PutRel(r); refShard.Delete(id)  // step 3 fails -> duped

GOOD: archive.PutNode(n); archive.PutRel(r)
      if err := refShard.Delete(id); err != nil { archive.Delete(id); return err }
```

## B8. Store Boundary = Trust Boundary
*See CLAUDE.md > Defensive Copying*

```
BAD:  ms.nodes[id] = n           // caller and cache share pointer
GOOD: ms.nodes[id] = n.DeepCopy()
```

## B9. Sentinel Discrimination

```
BAD:  if err != nil { continue }                       // swallows real errors
GOOD: if errors.Is(err, ErrNodeNotFound) { continue }  // skip orphan only
```

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

## B14. History-Aware Queries Need ID Merging
*See CLAUDE.md > Temporal Queries*

```
BAD:  all := store.AllNodes()                                    // deleted invisible
GOOD: allIDs := merge(store.AllNodeIDs(), store.AllNodeHistoryIDs())
```

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

## B21. Registry Save Must Be Atomic Across Both Halves

```
BAD:  // SaveLabelRegistry: load file, replace labels, save
      // SaveRelTypeRegistry: load file, replace relTypes, save
      // concurrent call → last writer wins, other half is stale

GOOD: // Single SaveRegistries(labels, relTypes) call writes both atomically
```

---

# Tier C — Reference

## C1. Verification Must Handle Deleted Entities
*See CLAUDE.md > Temporal Queries*

```
BAD:  if err != nil { return false, err }                              // can't verify deleted
GOOD: if err != nil && !errors.Is(err, ErrNodeNotFound) { return false, err }
```

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
