# Lessons - tkg/v3

Actionable rules distilled from real bugs. Keep this file short. Add a new
entry only when a fresh issue exposes a reusable pattern that is not already
covered here.

## Audit Commands

Use these after touching the named subsystem:

```bash
rg -n 'ComputeNodeHash|ComputeRelHash' pkg/
rg -n 'store\.(Put|Delete|Replace)' pkg/graph/
rg -n '== 0|< 0|<= 0' pkg/graph pkg/types
rg -n 'ForEach.*ID|All.*IDs|mergeIDSlices' pkg/graph/store pkg/graph/internal/core
```

## 1. Validate At Every Boundary

```
BAD:  if id == 0 { return ErrInvalidStoreMutation }
      return lookupOrNoop(id) // negative IDs fall through

GOOD: if err := store.ValidateNodeID(id); err != nil { return err }
      return lookupOrNoop(id)
```

Zero and negative entity IDs are malformed except where an API explicitly
defines `0` as a cursor, unset value, or "all types" sentinel. Store mutations,
direct reads, history reads, graph mutation targets, Badger split helpers, and
repair helpers must all use the shared validators before lookup, scan, or
no-op handling.

## 2. Hash Canonical State

```
BAD:  hash := ComputeNodeHash(n, rawLabels)
GOOD: hash := ComputeNodeHash(n, resolvedCanonicalLabels)
```

Hash the canonical entity state after token normalization and registry
resolution. When fixing one hash call site, grep all other call sites.

## 3. Lock Before Store Mutation

```
BAD:  store.DeleteRelationship(id)

GOOD: g.entityLocks.LockEntity(id)
      defer g.entityLocks.UnlockEntity(id)
      store.DeleteRelationship(id)
```

Graph mutations must take the relevant entity locks before Store writes.
Transactions and batches call internal lock-free helpers only while holding the
transaction/batch graph lock.

## 4. Preflight Then Apply

```
BAD:  for _, id := range ids { delete(id) } // partial mutation on mid-loop error

GOOD: infos := preflight(ids)               // reads and validates, no writes
      for _, info := range infos { apply(info) }
```

Multi-row Store operations must separate validation/read gathering from the
mutation phase. If an apply step can still fail, add rollback or return
per-operation errors explicitly.

## 5. Multi-Shard Moves Need Rollback

```
BAD:  dst.PutNode(n); dst.PutRel(r); src.DeleteNode(id)

GOOD: dst.PutNode(n); dst.PutRel(r)
      if err := src.DeleteNode(id); err != nil {
          rollbackDestination()
          return err
      }
```

Tiered archive/restore, migration, and cross-shard relationship moves must not
leave duplicates or orphaned split indexes when a later step fails. Repair tools
are not a substitute for transactional cleanup.

## 6. Store Is The Trust Boundary

```
BAD:  cache[id] = callerPointer
GOOD: cache[id] = callerPointer.DeepCopy()
```

Store put/get paths must deep-copy entities and validate payload invariants.
Badger wire decode must treat MsgPack as untrusted: reject key/value ID
mismatches, invalid tokens, invalid temporal ranges, malformed properties, and
counter/index metadata corruption before constructing live entities.

## 7. Deleted Entities Still Matter

```
BAD:  ids := store.AllNodeIDs() // deleted historical nodes vanish
GOOD: ids := mergeCurrentAndHistoryIDs()
```

Temporal, tx-time, diff, snapshot, and integrity queries must merge current and
history rows. Verification paths must tolerate current-row deletion when valid
history exists.

## 8. Iterate Without Materializing Everything

```
BAD:  ids := store.AllNodeIDs()
      merged := mergeIDSlices(shards)

GOOD: store.ForEachNodeID(func(id types.NodeID) bool {
          seen[id] = struct{}{}
          return true
      })
```

Large scans should use callback iterators or paged scans. Callbacks must run
outside backend locks and Tiered shard checkouts so callers can safely use
Store methods inside callbacks.

## 9. sync.RWMutex Is Not Reentrant

```
BAD:  g.mu.RLock(); g.Nodes.ValidAt(t) // inner RLock can deadlock
GOOD: g.mu.RLock(); getNodesValidAtLocked(t)
```

Do not call exported graph methods from already-locked graph internals. Use
unexported lock-free helpers for nested operations.

## 10. Persistence Must Rebuild In-Memory State

```
BAD:  CreateIndex only updates maps in memory
GOOD: persist definition; loadIndexes rebuilds it on open
```

Anything that affects query correctness and survives a process lifetime needs a
durable definition and a startup rebuild path. Counters and registry metadata
must fail closed when corrupt or impossible.

## 11. Index Creation Is A Three-Phase Operation

```
BAD:  build index while invisible to concurrent writes
GOOD: install placeholder; build from snapshot; install if not mutated
```

Concurrent writes must see an empty placeholder before index build I/O starts.
Final installation must use dirty/mutated tracking, not "does the built index
already contain this ID?"

## 12. Sentinel Errors Must Be Precise

```
BAD:  if err != nil { continue }
GOOD: if errors.Is(err, ErrRelNotFound) { continue }
      return err
```

Only skip the sentinel that the operation explicitly tolerates. I/O errors,
corruption, lifecycle errors, and invalid input must surface.

## 13. Version Identity Is Not Slice Position

```
BAD:  if i == 0 { checkGenesis() }
GOOD: if entry.Version() == 0 { checkGenesis() }
```

History can be truncated. Use version numbers and entity IDs as identity, never
array positions.

## 14. Public API Coverage Is Direct

Every public method needs a direct test. Delegation coverage is not enough.
Node and relationship mirror types should have parity tests, including nil,
sentinel, and malformed-input cases.

## 15. Performance Tests Need Production Shape

Tiny fixtures hide O(N) behavior. Keep both microbenchmarks and production
shapes: baseline graph operations, high-fanout relationships, history-heavy
temporal queries, export/import, batch writes, and Tiered hot/warm routing.

## 16. Lazy Init Flags Must Be Synchronized

```
BAD:  if !initialized { once.Do(init) } // initialized written during init under RLock
GOOD: if !initialized.Load() { once.Do(init) } // or guard every access with Lock
```

If zero-value lazy initialization can run under a shared read lock, any separate
initialized marker must be atomic or protected by an exclusive lock. `sync.Once`
serializes the init function, not unrelated reads of a plain flag.

## 17. Rollback Logs Track Pre-Transaction Existence

```
BAD:  deleted(id); importedSameID(id); createdIDs = append(createdIDs, id)
GOOD: createdIDs includes only IDs absent at BeginTx
```

Caller-specified imports can reuse an ID that was deleted earlier in the same
transaction. Rollback must restore the pre-transaction row and must not later
delete it as a created row. Keep create/delete rollback logs keyed by
pre-transaction existence, not only by the operation name that produced the
current row.

## 18. Map-Backed Query Results Need Explicit Ordering

```
BAD:  for id := range seen { append(result, load(id)) }
GOOD: ids := sortedKeys(seen); for _, id := range ids { append(result, load(id)) }
```

Any public query assembled from maps or dedup sets must sort by entity ID before
returning. Store and graph query surfaces should not expose Go map iteration
order, even when the logical result set is correct.

## 19. Native Fast Paths Must Respect Store Wrappers

```
BAD:  if cap, ok := store.(fastPath); ok { cap.FastPath(...) }
GOOD: enable fast paths only for exact in-tree stores, or prove wrappers cannot
      override the operation being optimized
```

Tests and consumers embed in-tree stores to inject failures or stale reads. A
native optimization must not bypass those overrides. Keep the generic Store
contract path as the default and opt into exact-store shortcuts only when the
in-tree implementation itself provides the invariant.
