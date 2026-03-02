# Lessons — tkg-v3

These are patterns that caused real bugs. Organized by principle, not by date.
Read before implementing. Re-read before marking done.

---

## 1. Challenge the Plan

A detailed plan is a hypothesis, not a specification. Before implementing any method,
ask these three questions:

**"Does this algorithm actually deliver what the method name promises?"**

```
BAD:  Snapshot(t) calls AllNodes() → filter. AllNodes() only returns current tip versions.
      A "snapshot from 3 days ago" includes today's property values and omits deleted nodes.
      The name says time-travel; the algorithm does current-state filtering.

GOOD: Snapshot(t) walks the 0x07 history tape and reconstructs each entity's state at time t.
      Deleted nodes appear (via their last historical version). Properties match their values at t.
```

**"Does this interact with an existing feature that could break it?"**

```
BAD:  VerifyNodeHashChain checks if chain[0].PrevHash == "". But TruncateNodeHistory
      removes old versions. After truncation, chain[0] is no longer genesis — it has a
      valid PrevHash pointing to truncated history. Verification permanently returns false.

GOOD: Check entry.Version() == 0 for genesis, not array position. If the oldest version
      in the chain isn't genesis, verify only the links that exist.
```

**"Does the constraint make sense?"**

```
BAD:  Plan says "No Store interface change." So NodeCountByLabel calls
      NodesByLabel(tok) — allocates, deep-copies, sorts 5M nodes — then returns len().
      Both stores already have labelIdx maps. len(labelIdx[tok]) is O(1).

GOOD: Add NodeCountByLabel to the Store interface. MemoryStore: len(labelIdx[tok]).
      BadgerStore: atomic counter. Question constraints that force bad solutions.
```

---

## 2. In-Memory State Must Survive Restart

If it's in memory and it matters, it needs a persistence path and a rebuild path.
This has been violated twice: once for in-memory indexes (fixed in v3.0.10), once
for property indexes (fixed in v3.0.23 — definitions persisted to `0x0F/prop_indexes`).

```
BAD:  CreatePropertyIndex populates bs.propertyIndexes[key] in memory.
      On restart, map is empty. NodesByLabelAndProperty silently degrades to O(N) scan.
      Plan said "persist via 0x0F/prop_indexes meta key" — never implemented.

GOOD: Serialize index definitions to Badger during flush(). In loadIndexes(),
      read definitions back, scan matching nodes, rebuild in-memory entries.
      Same pattern as labelIdx/typeIdx/outIdx/inIdx rebuild.
```

The pattern: if `loadIndexes()` doesn't rebuild it, it doesn't survive restart.

---

## 3. Lock Scope: Fast Mutations vs Slow I/O

Never hold idxMu.Lock during disk I/O. Collect IDs fast, release lock, process outside.
This was learned in v3.0.18 (cascade history cleanup) and immediately re-violated
in Phase 2c (CreatePropertyIndex).

```
BAD:  bs.idxMu.Lock()
      for nodeID := range nodeIDs {
          bs.getNodeLocked(nodeID)  // msgpack deserialization, Badger reads
      }
      bs.idxMu.Unlock()
      // All concurrent reads/writes blocked for entire scan duration

GOOD: bs.idxMu.RLock()
      ids := collectIDs(nodeIDs)  // snapshot IDs quickly
      bs.idxMu.RUnlock()
      for _, id := range ids {
          node := bs.GetNode(id)  // I/O outside lock
          idx.add(id, node)       // index-specific lock, not global
      }
```

---

## 4. Temporal Data Is Append-Only

In a temporal database, you never physically delete history. You append a tombstone.
Phase 1's `DeleteNodeCascade` calls `deleteHistoryByPrefix`, erasing the 0x07 tape.
This makes it impossible to reconstruct past states.

```
BAD:  DeleteNode → remove from store → deleteHistoryByPrefix
      The node and all its versions are gone. Snapshot(t) for any past t cannot find it.

GOOD: DeleteNode → append final version with DeletedAt timestamp → set ValidTo
      The node is "deleted" but its history survives. Snapshot(t) before deletion sees it.
```

---

## 5. Don't Materialize Data You Won't Use

When an in-memory index gives you the answer, use the index. Don't round-trip through
entity deserialization, deep copy, and sort just to count or check existence.

```
BAD:  func NodeCountByLabel(label) int {
          nodes, _ := store.NodesByLabel(tok)  // alloc + DeepCopy + sort 5M nodes
          return len(nodes)                     // throw them all away
      }

GOOD: func NodeCountByLabel(tok uint16) int {
          return len(ms.labelIdx[tok])  // O(1), zero allocations
      }
```

Same principle applies to existence checks: `nodeIDs[id]` is O(1). Don't call
`GetNode(id)` just to check if it exists.

---

## 6. Two-Phase Operations: Preflight Then Apply

Multi-step mutations must be all-or-nothing. Phase 1: read everything, fail fast.
Phase 2: apply everything, no error exits.

```
BAD:  for _, relID := range rels {
          deleteRelLocked(relID)  // mutates indexes on each iteration
      }
      // If iteration 5 fails, iterations 1-4 already mutated indexes.
      // Graph is permanently split.

GOOD: // Phase 1: preflight — read all, mutate nothing
      infos := make([]relDeleteInfo, len(rels))
      for i, relID := range rels {
          info, err := getRelLocked(relID)
          if err != nil { return err }  // zero mutations happened
          infos[i] = info
      }
      // Phase 2: apply — all mutations, no error exits
      for _, info := range infos {
          deleteRelByInfo(info)  // pure mutations, cannot fail
      }
```

---

## 7. Store Boundary = Trust Boundary

Entities must be deep-copied at the store boundary. Cache and caller must never
share pointers. Pick one side: copy on Put, or copy on Get.

```
BAD:  func PutNode(n *types.Node) {
          ms.nodes[id] = n  // caller and cache share same pointer
      }
      // Caller mutates n.SetProperty("x", "y") → cache is silently corrupted

GOOD: func PutNode(n *types.Node) {
          ms.nodes[id] = n.DeepCopy()  // cache owns independent copy
      }
```

---

## 8. Error Handling: Sentinel Discrimination

Never bare `continue` on error. Check the specific sentinel. Propagate everything else.

```
BAD:  for _, id := range index {
          node, err := GetNode(id)
          if err != nil { continue }  // swallows corruption, I/O errors
      }

GOOD: for _, id := range index {
          node, err := GetNode(id)
          if errors.Is(err, ErrNodeNotFound) { continue }  // orphan: skip
          if err != nil { return nil, err }                  // real error: propagate
      }
```

Every sentinel error test must use `errors.Is`, not just `err != nil`.

---

## 9. Validation: Allowlists, Recursion, Depth Limits

- **Allowlist > denylist.** Enumerate what's safe. Reject everything else.
- **Recursive.** `[]any{&myStruct{}}` bypasses top-level-only checks.
- **Depth-limited.** Every recursive function on untrusted input needs `maxDepth`.
- **At boundaries.** Validate before data enters internal state. Registries reject `""`.

```
BAD:  if kind == reflect.Ptr || kind == reflect.Struct { reject }
      // Arrays, channels, functions, unsafe.Pointer all pass through

GOOD: switch kind {
      case reflect.Bool, reflect.String, reflect.Int, ...: // safe
      case reflect.Slice, reflect.Map: recurse(depth+1)
      default: reject  // unknown = unsafe
      }
```

---

## 10. Testing Discipline

**Coverage gates.** Run `make cover` before marking done. 0% on any public method is a blocker.

**Node/Rel parity.** They are structural mirrors. Every node test needs a relationship equivalent.

**Feature interactions.** After writing happy-path tests, ask: "What existing features
could produce different inputs?" TruncateHistory + VerifyHashChain. Delete + Snapshot.
Concurrent AddRelationship + DeleteNode.

**Every branch.** Every `case` in a type switch. Every `if/else` path. The test IS the proof.

---

## 11. Concurrency Patterns

- **sync.Once** for idempotent `Close()`. Never nil-guard a function pointer across goroutines.
- **Ascending shard order** for multi-lock acquisition. `LockTwo` normalizes before acquiring.
- **Atomic counters** outside Badger transactions. OCC conflicts kill concurrent writes.
- **Counters in the same WriteBatch** as data. Separate transactions = crash inconsistency.
- **Version-aware dirty tracking.** `CollectDirty()` is read-only. `MarkFlushed()` is CAS.
- **Last-write-wins buffers.** `map[string]writeOp`, not `[]writeOp`. Retries must not replay stale writes.
- **Tombstones** in cache-first architectures. A cache miss must not fall through to stale Badger data.

---

## 12. Async Persistence

- `Close()` must call `flush()` unconditionally — even when `flushLoop` was never started.
- Background loop errors must be logged (`slog.Error`), never `_ = fn()`.
- Tests verifying durability must `Flush()` or `Close()` before reopening the DB.
- `FlushInterval: 0` means "use default", not "disabled". Use large values to disable.

---

## 13. Determinism

- Sort by `snowflake.ID` before returning slices from map iteration.
- Never `fmt.Sprintf("%v")` for hash inputs. Maps have random iteration order.
- Use typed binary serialization with sorted keys for hash computation.

---

## 14. API Design

- **Config fields must be used or removed.** Never accept input that does nothing.
- **Opaque wrappers must wrap the real type.** `type nodeID snowflake.ID`, not `int64`.
- **Graph is the sole external API.** Add passthroughs rather than exposing Store.
- **Doc comments must match behavior.** After changing logic, grep for stale descriptions.
- **Validate before generating IDs.** `NewPropertySlice(props)` before `NextNodeID()`.

---

## 15. Array Position Is Not Identity

Never use array position (`i == 0`) as a proxy for semantic identity (`version == 0`).
Array position changes when elements are removed; semantic identity does not.

```
BAD:  if i == 0 { // "genesis version"
          // This breaks after TruncateHistory removes earlier versions.
          // chain[0] is now version 3 with a valid PrevHash.
      }

GOOD: if entry.Version() == 0 { // actually genesis
          // Works regardless of truncation — genesis is always version 0.
      }
```

This applies to any check that assumes the first element in a collection has special
meaning. After truncation, filtering, or pagination, `[0]` is just the first *remaining*
element, not the first *created* element.

---

## 16. Refactor Shared Logic When Adding Parallel Methods

When adding a method that mirrors an existing one (e.g., `GetRelAt` mirrors `GetNodeAt`),
factor out the shared algorithm into a helper instead of copy-pasting.

```
BAD:  GetNodeAt has 30 lines of version resolution logic.
      GetRelAt copy-pastes the same 30 lines with s/Node/Rel/.
      Bug fix must be applied in two places. They will drift.

GOOD: nodeVersionBounds(chain, i) / relVersionBounds(chain, i) — type-specific bounds.
      resolveNodeVersionAt(chain, t) / resolveRelVersionAt(chain, t) — shared algorithm.
      GetNodeAt and GetRelAt call the helpers. Fix once, correct everywhere.
```

---

## 17. History-Aware Queries Need ID Merging

Temporal queries that should include deleted entities must merge IDs from two sources:
current entities (from `AllNodes()`) and historical entities (from `AllNodeHistoryIDs()`).
Querying only current entities makes deleted entities invisible to time-travel queries.

```
BAD:  func GetNodesValidAt(t) {
          all := store.AllNodes()    // only current tip versions
          return filter(all, t)      // deleted nodes are invisible
      }

GOOD: func GetNodesValidAt(t) {
          currentIDs := store.AllNodes()
          histIDs := store.AllNodeHistoryIDs()  // includes deleted entities
          allIDs := merge(currentIDs, histIDs)
          for id := range allIDs {
              n := GetNodeAt(id, t)  // handles nil current via history chain
          }
      }
```
