# Lessons — tkg/v3

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

**History:** Found in code_review_phase3 as BLOCKER ("idleCloseLoop Hard-Panics Concurrent Readers"). First fix (v3.0.30) had a TOCTOU gap between `getStore` and `activeReqs.Add(1)`. Fully fixed in v3.0.33 by moving increment inside `shardMu`. Later TieredStore relationship-routing fixes needed the same lifecycle assertion on fallback cold relationship-owner lookup: finding the cold owner is not enough; the returned store must stay pinned until checkin.

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

Audit: `grep -rn 'os.Rename' pkg/` — every write-tmp+rename must have file Sync() before Close AND directory Sync() after Rename.

**History:** Found in v3.0.33 review. `shard_catalog.go` and `registry_file.go` both lacked fsync. v3.0.59 added directory fsync after rename for crash durability.

## B21. Registry Save Must Be Atomic Across Both Halves

```
BAD:  // SaveLabelRegistry: load file, replace labels, save
      // SaveRelTypeRegistry: load file, replace relTypes, save
      // concurrent call → last writer wins, other half is stale

GOOD: // Single SaveRegistries(labels, relTypes) call writes both atomically
```

**History:** Found in v3.0.33 review. Read-modify-write race between two independent save calls.

## B23. Never Mutate the Caller's Input Props Map

When extracting reserved keys (e.g. `tkg_*` shadow props) from a caller-supplied `map[string]any`, always produce a filtered copy — never `delete()` from the original map. The caller may reuse or introspect the map after the call.

```
BAD:  func addNode(props map[string]any) {
          authorID := props["tkg_author_id"].(string)
          delete(props, "tkg_author_id")  // mutates caller's map — surprising
          buildPropertySlice(props)
      }

GOOD: func extractProvenance(props map[string]any) (authorID string, filtered map[string]any) {
          if _, has := props["tkg_author_id"]; !has {
              return "", props  // fast path: no allocation, original map is clean
          }
          authorID, _ = props["tkg_author_id"].(string)
          filtered = make(map[string]any, len(props))
          for k, v := range props {
              if k != "tkg_author_id" { filtered[k] = v }
          }
          return authorID, filtered
      }
```

Use the fast path (return original map) when no reserved key is present — zero allocation. Only allocate the filtered copy when a reserved key actually exists.

**History:** Applied in v3.0.45 (`extractProvenance` in context.go) for `tkg_author_id` / `tkg_signature` extraction before PropertySlice construction.

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

## B24. Transaction Isolation: Internal/External Method Split

```
BAD:  func (g *Graph) AddNodeWithContext(...) { /* no g.mu */ ... }
      // BeginTx holds g.mu.Lock, but standalone AddNode races with tx

GOOD: func (g *Graph) AddNodeWithContext(...) {
          g.mu.RLock(); defer g.mu.RUnlock()
          return g.addNodeInternal(ctx, labels, props)
      }
      // Tx calls g.addNodeInternal directly (already holds g.mu.Lock)
```

All exported mutation methods must acquire `g.mu.RLock()`. Tx/batch call unexported `*Internal` variants under `g.mu.Lock()`. Lock ordering: `g.mu` → entity locks.

Audit: `grep -rn 'func (g \*Graph).*Internal' pkg/graph/` — every internal must have a corresponding exported wrapper with `g.mu.RLock`.

**History:** Found in v3.0.59 external audit. Standalone mutations could bypass tx isolation.

## B29. Transaction Rollback Must Reverse Every Side Effect, Including Indexes

```
BAD:  tx.RemoveNodeLabel(id, "B")
      tx.Rollback()
      // ReplaceNode(prev) restores the node's own label set, but the
      // store-level label index still has the old entry → NodesByLabel
      // returns a node that no longer has the label.

GOOD: track label deltas per tx
      tx.labelDeltas = append(tx.labelDeltas, labelDelta{id, tok, added})
      // in Rollback, after ReplaceNode has restored node state:
      if d.added {
          store.RemoveNodeLabelToken(id, tok, restoredNode)
      } else {
          store.AddNodeLabelToken(id, tok, restoredNode)
      }
```

When a tx path mutates BOTH entity state AND a separate index, the rollback path must reverse BOTH. `ReplaceNode` deliberately skips the label index (labels were "immutable"), so any label-mutating tx needs a dedicated tracker. Audit: any tx operation that touches an auxiliary index (label, type, adjacency, property, temporal) needs a delta tracker whose reverse runs during `Rollback`.

**History:** Found in v3.1.6 while adding `GraphTx.AddNodeLabel`/`RemoveNodeLabel`. The naive path compiled and passed the obvious "is the label gone?" test because `NodeHasLabel` checks the node state, not the index. Only a second test querying `NodesByLabel` after rollback exposed the corruption. Two regression tests now cover both directions.

## B26. Lock Acquisition Without Defer Leaks on Panic

```
BAD:  b.g.mu.Lock()
      // ... 100 lines of operations ...
      b.g.mu.Unlock()
      // if anything panics between Lock and Unlock, the lock is leaked forever

GOOD: b.g.mu.Lock()
      unlocked := false
      defer func() {
          if !unlocked {
              b.g.txEventBuffer = nil
              b.g.mu.Unlock()
          }
      }()
      // ... operations ...
      b.g.mu.Unlock()
      unlocked = true
```

Pattern: when `defer mu.Unlock()` can't be used (because you need to do work after unlock), use an `unlocked` flag with deferred cleanup. This protects against panics in Store implementations or other injected code.

**History:** Found in v3.0.62 review. `BatchBuilder.Execute()` had 110+ lines between Lock and Unlock with no panic protection.

## B27. Input Validation at Startup: Reject Nonsensical Configs

```
BAD:  v.MaxLabelsPerNode == 0 → default
      v.MaxLabelsPerNode == -1 → accepted (breaks comparisons)

GOOD: if v.MaxLabelsPerNode < 0 { return error }
```

After resolving zero-to-default, check for negatives. A negative validation limit passes through and silently disables the guard (since `len(labels) > -1` is always true).

**History:** Found in v3.0.62 review. `Graph.New()` and `NewBadgerStore()` both accepted negative config values.

## B28. Fractional Float-to-Integer Truncation Is Silent Data Loss

```
BAD:  authLevel = uint8(v)  // v=5.9 → 5 (truncated silently)

GOOD: if v != math.Trunc(v) { return error }
      authLevel = uint8(v)
```

JSON `number` values arrive as `float64` in Go. `5.0` → `uint8(5)` is safe. `5.9` → `uint8(5)` is silent data loss. Always check `math.Trunc` before integer conversion.

**History:** Found in v3.0.62 review. `extractProvenance` silently truncated fractional auth levels.

## B25. Tx Event Buffering: Publish After Unlock

```
BAD:  // Events published during tx mutations — on Rollback, subscribers have stale state

GOOD: // publishEvent buffers to txEventBuffer during tx
      // Commit: clear buffer, unlock g.mu, THEN publish events
      // Rollback: discard buffer (subscribers never see rolled-back mutations)
```

Event handlers may call Graph read methods — publishing must happen after `g.mu.Unlock()`.

**History:** Found in v3.0.59 external audit. Rollback left EventBus subscribers inconsistent.

## B30. Versioned Metadata Must Be Verified Per Version

```
BAD:  labels := g.NodeLabels(current)
      for _, entry := range chain {
          computed := ComputeNodeHash(entry, labels) // wrong after label mutation
      }

GOOD: for _, entry := range chain {
          labels := g.NodeLabels(entry)
          computed := ComputeNodeHash(entry, labels)
      }
```

Hash verification, temporal filters, and transaction-time bounds must use the
metadata that belonged to the specific version being evaluated. Never reuse the
current tip's labels, properties, adjacency, or TxFrom/TxTo when answering a
historical question.

**Rule:** Any mutation that changes labels, properties, or adjacency needs a
regression test that queries/verifies both the old version and the new current
version.

## B31. History-Aware Code Needs Two-Phase Tests

```
BAD:  // Single-mutation happy path — passes for the wrong reason
      n, _ := g.AddNode([]string{"A"}, nil)
      ok, _ := g.VerifyNodeHashChain(n.InternalID().SnowflakeID())
      assert.True(t, ok)  // there's only one version, no past to get wrong

GOOD: // Two-phase: mutate, then ask about the past
      t0 := nowInstant()
      n, _ := g.AddNode([]string{"A"}, nil)
      _ = g.AddNodeLabel(n.InternalID().SnowflakeID(), "B")
      ok, _ := g.VerifyNodeHashChain(n.InternalID().SnowflakeID())
      assert.True(t, ok)  // exposes per-version hash bug
      hits, _ := g.GetNodesByLabelValidAt("A", t0)
      assert.Len(t, hits, 1)  // exposes "current-only label index" bug
      hits, _ = g.GetNodesByLabelValidAt("B", t0)
      assert.Len(t, hits, 0)  // same bug from the other side
```

Code that answers questions about a different point in time, a different
version, or a comparative state ("verify chain", "valid at t", "as of",
"snapshot", "during interval") cannot be validated by a single-mutation test.
Single-mutation tests verify the API exists; only mutation-then-query tests
verify it remembers.

**Rule:** any method whose name contains `ValidAt`, `ValidDuring`, `AsOf`,
`Verify*Chain`, `Snapshot`, `Diff`, `*At` — and any generic query method that
accepts a `QueryOpts` carrying temporal filters — needs at least one test that
(1) creates an entity in state X at t0, (2) mutates it to state Y after t0,
(3) queries with t = t0 and asserts the result reflects state X, not Y.

**Audit when adding a new mutation operation:** every existing history-aware
method must be re-tested with the new mutation as the phase-1 step. When
`AddNodeLabel` was added in v3.1.6, `VerifyNodeHashChain`,
`GetNodesByLabelValidAt`, `NodesByLabelPropertyAndTime`,
`NodesByLabelPropertyDuring`, and `GetNeighborsValidAt` should all have been
re-tested through it. None were, and all five were broken.

**Audit for "two doors, same shape":** a fix that touches a named historical
method (`Get*ValidAt`) must grep for the generic equivalent
(`*(opts QueryOpts)`) and apply the same fix and the same two-phase test there.
The first round of this fix landed `Get*ValidAt` corrections but left
`NodesByLabel(opts)`, `NodesByLabelAndProperty(opts)`, and
`RelationshipsByType(opts)` with the same bug; only a follow-up commit caught
them.

**History:** Found while reviewing MR !2 (2026-05-05). Five history-aware
methods broke because every test in Phases 1c, 2, 2g, and v3.1.6 was
single-mutation. The MR author (Markus Nissl) added the missing two-phase
tests; the original buggy code shipped across multiple Claude-co-authored
commits, all with high test counts but only single-mutation coverage.

## B34. Performance Tests Need Production Shape

```
BAD:  BenchmarkGetNode on 2K nodes and call it a performance gate

GOOD: Benchmark public Graph APIs on large graphs, high-degree nodes, dense
      edge sets, deep version chains, export/import, events, and TieredStore
      multi-shard reads
```

Microbenchmarks are useful for localizing costs, but they do not prove production
performance. A regression gate must include realistic fixture shape: many nodes,
many relationships, fan-out hotspots, indexed reads, temporal/history queries,
batch/transaction writes, persistence backends, and event delivery.

Use profile sizes intentionally. Small production benchmarks are for routine
branch-to-main comparison. Large stress benchmarks must include deep history
chains, such as 3,000 daily node and relationship updates, and larger fixtures
for the same public interfaces so they expose algorithmic regressions that a
30-version smoke shape can hide.

**Rule:** Before claiming "no performance regression", compare the feature branch
against `main` with both a routine API baseline and small/large production-shaped
benchmark suites using `benchstat`.

## B32. Deleted Entity History Needs History-Aware Routing

```
BAD:  func GetRelHistory(id) {
          shard := shardForRelID(id)  // probes current rel indexes
          return shard.GetRelHistory(id)
      }
      // after delete, the current rel is gone; cross-shard tombstone history
      // may live on a different shard than timestamp fallback chooses

GOOD: func GetRelHistory(id) {
          shard := shardForRelID(id)
          history := shard.GetRelHistory(id)
          if len(history) > 0 { return history }
          return probeHistoryShards(id) // current-index fallback failed
      }
```

History and tombstone lookups must not depend solely on current-entity indexes.
Contract tests should include deleted reference nodes and deleted cross-shard
relationships, because the failure only appears after the current entity has
been removed.

For history writes, route from the immutable entity shape: reference node
snapshots belong to the reference shard; cross-shard relationship snapshots
belong to the relationship start-node shard, because that is where the
relationship entity and outgoing index are stored.

**History:** Found while adding the store contract suite for the first contract-test MR.
Fixed by probing history-owning shards for deleted entity reads and routing
history writes by snapshot ownership.

---

## B33. Primary-Label Class Must Be Immutable Across Versions

```
BAD:  RemoveNodeLabel(id, primaryLabel) auto-promotes the next extra label
      to primary. If the new primary has a different ontology class, the
      live entity stays on its original shard while subsequent history
      snapshots route to a different shard, fragmenting the version chain
      and breaking forEachHistoryShard's first-match semantics.

GOOD: TieredStore.{Add,Remove}NodeLabelToken{,WithHistory} compare the
      pre/post primary-label classes (ClassReference vs ClassEvent) and
      reject the mutation with ErrPrimaryLabelClassMutation when they
      differ. An entity's full history then provably lives on a single
      shard, and read-fallback can stop at the first match.
```

When routing decisions depend on a property that is part of mutable state
(primary label is a snapshot property; ontology class is derived from it),
the routing component must either (a) treat that property as immutable
across versions, or (b) merge across all candidate shards on read. Option
(a) is cheaper and easier to reason about; enforce it at the write
boundary that creates the inconsistency.

**Audit:** When a write decides "where does this go?" based on a value
that can change, grep for all paths that mutate that value and add a
guard at each one — not just at the read site that exposes the bug.

**History:** Surfaced during review of the deleted-entity history routing
fix (B32). The fix routed history writes by snapshot label class, which
made the first-match read fallback correct only as long as primary class
never changes. v3.1.6's `AddNodeLabel`/`RemoveNodeLabel` plus
`Node.RemoveLabelTokenRaw`'s auto-promotion made class change reachable.
Closed at the TieredStore boundary because the graph layer is
backend-agnostic.

---

## B35. Same-Pattern Audit After Merging a "Consistency MR"

```
BAD:  MR adds checkoutArchive() pin to GetNode/Update/Delete and
      forEachHistoryShard. Merge it. Tag.
      ...later: a different code path still uses raw refArchive.Load()
      without the pin and races Close in production.

GOOD: After merging, grep for every callsite of the pre-fix pattern
      (refArchive.Load(), or whatever the bug-class is). For each
      remaining site, classify as either:
        - already pinned (caller wraps in checkoutArchive),
        - pointer-comparison only (load result never dereferenced),
        - or actual Close-race bug — fix before tagging.
```

When a merged MR fixes a bug class (e.g. "missing pin against Close
race"), the same pattern likely exists elsewhere in the codebase that
the MR author didn't audit — especially in adjacent admin/repair paths
the MR touches indirectly.

**Audit pattern after a consistency MR:**

1. Identify the pre-fix pattern (e.g. `refArchive.Load()` without
   `checkoutArchive`).
2. `grep -n` for every callsite of the pattern across the package.
3. For each, walk one of three paths:
   - Already correct (pinned upstream or pointer-comparison only).
   - Pre-fix bug missed by the MR — fix inline before tagging.
   - Justified raw use — document in-line with a comment.
4. Tag only when the audit is clean.

**History:** v3.1.11 audit caught two refArchive Close-race sites
(`findRelInAnyShardStore` and `ArchiveNode`/`RestoreNode`) that MR !6
fixed elsewhere but missed in admin and write paths. The function's
own doc comment in `findRelInAnyShardStore` literally said *"Missing
the archive probe causes ... silent data loss"* — yet the implementation
used the unsafe raw `Load()` pattern MR !6 fixed elsewhere. Fixed
inline as part of v3.1.11.

**Tooling:**

```sh
# After a consistency MR, audit all sites of the pre-fix pattern:
grep -rn '<pre-fix pattern>' pkg/<pkg>/*.go | grep -v '_test\.go'
# For each, decide: pinned / pointer-comparison-only / actual bug.
```

---

## B36. Admin Paths Need the Same Pinning Discipline as Read Paths

```
BAD:  func (ts *TieredStore) ListShards() {
          for _, es := range ts.eventShards {
              si.Nodes, _ = es.store.NodeCount()   // no pin
          }
      }
      // concurrent Close frees es.store mid-call → use-after-free

GOOD: func (ts *TieredStore) ListShards() {
          // snapshot under RLock, then release
          for _, sn := range snaps {
              store, err := sn.es.checkoutStore(ts)
              if err != nil { continue }           // Close started — skip
              si.Nodes, _ = store.NodeCount()
              sn.es.checkinStore()
          }
      }
```

The `checkoutStore` / `checkoutArchive` / `checkinStore` pin discipline that guards read paths is equally required for admin paths (ListShards, RebuildCatalog, Clear, Create/Drop index). Admin methods are written less frequently under concurrent Close, so the discipline is easier to miss. When adding any admin method that touches a BadgerStore through an `eventShard`, reach for `checkoutStore` first.

**Why:** `Close` doesn't take `ts.mu` — it only spin-waits on `activeReqs`. An admin call that takes `ts.mu.RLock` or `ts.mu.Lock` is still racing `Close` unless every individual DB handle is pinned.

**How to apply:** After writing an admin method, grep for `es.store.<method>` or bare `archive.<method>` calls without a preceding checkout. Any such call is a Close race.

**History:** Found during MR !7 review. `ListShards`, `RebuildCatalog`, `Clear`, and four index methods all had unpinned direct access. Fixed by adding `checkoutStore` with `ErrStoreClosed` skip paths and migrating index callers to `allShardStoresWithLazyOpen`.

---

## B37. Custom Property Types Must Enforce Both Contracts at Registration

```
BAD:  func RegisterPropertyStructType(v any) {
          // accepts anything — no interface check
      }
      // later: AddNode stores Polygon{Rings: [][]Ring{...}}
      //        ComputeNodeHash calls appendPropertyValue → panic (no HashBytes)
      //        PutNode deep-copies → shallow copy (no DeepCopyValue)
      //        Caller mutates Polygon after Put, corrupts cached graph state

GOOD: func RegisterPropertyStructType(v any) error {
          if !t.Implements(hashableValueType) && !elemT.Implements(hashableValueType) {
              return ErrTypeNotHashable
          }
          if !t.Implements(deepCopierType) && !elemT.Implements(deepCopierType) {
              return ErrTypeNotDeepCopyable
          }
          // only reach the registry if both contracts hold
      }
```

Check the form actually passed (`t`), not `reflect.PointerTo(elemT)`. Accepting the value form when methods are on the pointer receiver only stores non-addressable values; the runtime type-assert to `HashableValue` / `DeepCopier` returns `ok=false` and the code falls back to panic / shallow-copy — the same bugs registration was supposed to prevent. Callers with pointer-receiver methods must register `(*T)(nil)`.

**Why:** Registration is the last gate before a type reaches the data plane (hash computation, store boundary). Both interfaces are required: `HashableValue` prevents panic in `ComputeNodeHash`; `DeepCopier` prevents the store boundary from silently sharing mutable state between caller and cache.

**How to apply:** Any `RegisterPropertyStructType` call site outside this module must be updated to check the returned error. Test the rejection cases — value form with pointer-receiver methods is the easy-to-miss adversarial scenario.

**History:** Found during MR review of `codex/property-type-safety`. The original registration silently accepted any struct, deferring both failures to runtime (panic in hash, corruption at store boundary).

---

## B38. Trust Boundaries Require Explicit Validation Before Construction

```
BAD:  func (g *Graph) ImportGraph(r io.Reader) error {
          ...
          n := wireToNode(wn)   // wireToNode panics if primaryLabel == 0
          ...
      }
      // a corrupt or hostile io.Reader stream crashes the process

GOOD: func (g *Graph) ImportGraph(r io.Reader) error {
          ...
          if err := validateNodeWire(&wn); err != nil {
              return fmt.Errorf("import: node %d: %w", wn.ID, err)
          }
          n := wireToNode(wn)   // safe — token invariants verified
          ...
      }
```

Any function that reads from a caller-supplied `io.Reader`, network socket, or external file is an untrusted boundary. Construction functions that enforce invariants via panics (token 0 reserved, non-zero type required) must be defended at the boundary — not inside the constructor where the panic has already crossed the trust line.

**Why:** Internal reads from Badger (trusted — written by this library, validated at write time) don't need this guard. `io.Reader` paths do: the reader may be corrupted, truncated, or hostile.

**How to apply:** Before any construction call (`wireToNode`, `wireToRel`, `types.NewNode`, `types.NewRelationship`) in an import/deserialize path, add an explicit pre-validation step that returns a typed sentinel error on invalid input. Name the sentinel `Err*` and test it with `errors.Is`.

**History:** Found during `codex/defensive-boundaries` MR review. `ImportGraph` called `wireToNode`/`wireToRel` directly on untrusted bytes — token-0 input panicked the process. Fixed with `validateNodeWire`/`validateRelWire` + `ErrCorruptExport`.

---

## B39. Scan Loops Must Distinguish Legitimate Skips from Operational Failures

```
BAD:  r, err := ns.store.GetRelationship(relID)
      if err != nil {
          continue // swallows I/O failures, routing errors, closed shards
      }
      // repair returns "success" while genuinely broken entries are missed

GOOD: r, err := ns.store.GetRelationship(relID)
      if err != nil {
          if errors.Is(err, ErrRelNotFound) {
              continue  // TOCTOU: rel deleted between AllRelIDs and GetRelationship
          }
          return nil, fmt.Errorf("repair: shard %q: read rel %d: %w", ns.name, relID, err)
      }
```

Repair, migration, and export loops that read entities by ID must distinguish the "entity deleted between snapshot and fetch" TOCTOU case (always legitimate to skip) from operational errors (I/O failure, closed shard, routing failure). Swallowing all errors returns a false "success" result while leaving the work undone.

**Why:** The TOCTOU case is inherently benign — the caller snapshotted IDs, something deleted the entity concurrently, the entity is gone and no repair is needed. Operational errors are different: the repair couldn't determine whether action was needed. Conflating them hides real failures.

**How to apply:** In any scan-loop `if err != nil` block: use `errors.Is(err, ErrXxxNotFound)` for the legitimate-skip class; propagate everything else. Write two tests — one that triggers the legitimate skip and one that injects an operational error — to verify the `errors.Is` gate is exercised independently.

**History:** Found during `codex/defensive-boundaries` MR review. `RunRepair` Phase 2's `GetRelationship` and `shardForNodeID` errors were all silently continued. Repair returned a clean result while needing-repair relationships were skipped.

---

## B40. Pre-Filter Before k-Cut in Top-k Searches

```
BAD:  ids, _ := vi.searchNearest(query, k)   // top-k by raw distance
      // then filter ineligible: result may have < k even if eligible
      // candidates exist farther out

GOOD: ids, _ := vi.searchNearest(query, k, filter)  // filter INSIDE heap loop
      // ineligible candidates skipped before heap insertion → top-k is
      // taken from the eligible-only set
```

Any "return k nearest" API that also accepts a temporal or access-control filter MUST apply the filter before the heap selection. Post-filtering degrades silently: a near-but-ineligible candidate occupies a heap slot and crowds a farther-but-eligible candidate out of the top-k. The result count drops below k without any error.

**Why:** The heap tracks the best-k-so-far. Every ineligible entry that enters the heap is a wasted slot. Once the heap is full, eligible candidates with distance > the ineligible's distance are discarded. No post-processing step can recover them.

**How to apply:** Thread the eligibility predicate into the innermost loop of the search (before heap insertion). Accept the predicate as a `func(id) bool` parameter on the internal search method so the top-level API can compose depth gating, temporal checks, and access controls without re-implementing the heap.

**History:** Found in `codex/vector-index-correctness` MR review. `SearchNearestNodes` accepted `QueryOpts` but ignored temporal filters and Depth; k ≤ 0 also panicked. Fixed via `filteredVectorSearchStore` hook + `depthFilter` in TieredStore.

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
