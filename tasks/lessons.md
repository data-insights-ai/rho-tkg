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
When a relationship mutation recomputes integrity, refresh `FromNodeHash` and
`ToNodeHash` from locked current endpoints before persisting the row.

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
No-error defensive-copy fallbacks must not panic on legacy/bypassed shapes:
use typed zero values for nil reflect elements and avoid unassignable writes.

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

Tiered scans that dereference shard stores must pin every handle that `Close`
can close. Event shards use `checkoutStore`, the reference archive uses
`checkoutArchive`, and the reference shard must use the same checked lifecycle
discipline even though idle-close never touches it. Wide reads over many
shards must also bound handle lifetime: group cheaply, then checkout/read/release
one shard batch at a time instead of pinning every cold owner before work starts.
Fanout helpers should use bounded worker pools, not one goroutine per shard
guarded by a semaphore, so historical shard counts do not translate into
scheduler or memory pressure.

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
must fail closed when corrupt or impossible. Persistent backends should mark
close-in-progress before stopping flush workers or taking the final flush
snapshot so public operations fail closed once shutdown begins.

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

## 20. Mutation Transaction Time Must Be Monotonic

```
BAD:  txNow := time.Now().UnixMilli()
GOOD: txNow := c.now() // per-Core monotonic millisecond instant
```

Temporal mutation paths that stamp `TxFrom`, `TxTo`, `UpdatedAt`, `DeletedAt`,
or mutation events must use the Core clock helper. Wall-clock milliseconds can
repeat or move backwards, and repeated instants collapse version intervals for
transaction-time queries.

## 21. Batch Visibility Must Match Consumer Scheduling

```
BAD:  enqueue normal; enqueue critical // worker can scan after normal only
GOOD: enqueue critical first, or block consumers until the full batch is visible
```

A producer-side batch mutex does not make the batch atomic if consumers can
observe the underlying queues directly. Publish batched work in the order that
preserves the consumer contract under partial observation, or gate consumers on
the same boundary.

## 22. Peek-Then-Lock Must Revalidate Identity

```
BAD:  peek endpoints; lock peeked IDs; mutate whatever row is current
GOOD: peek endpoints; lock peeked IDs; re-read; retry if the identity changed
```

Caller-specified imports can reuse deleted IDs with a different entity shape.
Any mutation that performs an unlocked peek only to discover the lock set must
revalidate the row identity under those locks before trusting the peeked shape.

## 23. Property Index Keys Are Equality Semantics

```
BAD:  return "f64:" + strconv.FormatFloat(v, 'g', -1, 64) // NaN payloads collapse
GOOD: use exact bit keys for values whose printable form loses equality detail
```

Property-index value keys are not display strings. They must preserve the
library's property equality contract, including special float values, so
fallback scans and indexed lookups return the same exact result set.

## 24. Bound Every Retry Loop

```
BAD:  for { peek; lockAll; reread; if changed { continue } } // unbounded
GOOD: for range maxRetries { ... }; return "changed after N retries" error
```

Peek-then-lock patterns retry when a concurrent writer changes the lock set
between peek and acquire. Every retry loop must have a max-iteration ceiling
so a hostile concurrent workload cannot deadlock the caller. Mirror the
ceiling across symmetric methods — `deleteNodeInternal` uses
`const maxRetries = 10`; `lockRelationshipCurrentEndpoints` must match.

## 25. Equality and Hash Have Different Contracts (Float Bit-Pattern Case)

```
BAD:  assume "equal CAS short-circuit" implies "equal hash"
GOOD: document the divergence — IEEE-754 ±0 and NaN payloads are equal
      by CAS short-circuit but distinct by integrity hash
```

`PropertyValueEqual` is the CAS short-circuit contract (NaN == NaN, +0 == -0).
The integrity hash is the durable-identity contract — it preserves IEEE-754
bit patterns. The asymmetry is intentional and tested. Callers exchanging
data with systems that canonicalize NaN bits must canonicalize at the
boundary if cross-system hash chains are expected to verify.

## 26. Public Sub-API Forwarding Must Match Internal Contract Width

```
BAD:  external Ops interface lists obsolete methods after API change
GOOD: every sub-API package declares its own minimal Ops; *core.Core
      satisfies each by implementing the union
```

When collapsing or splitting sub-APIs, update each `Ops` interface to the
minimal new shape and let `*core.Core` satisfy it implicitly. Old methods
on the internal core type can stay if they're useful internally — what
matters is that the public Ops interface advertises only the new contract.

## 27. Don't Ship "Documented Known Limitations" In Place Of Real Fixes

```
BAD:  flagging a real perf issue with a v4.1 carry-forward note and
      shipping v4 with the regression in place
GOOD: implement the actual fix now; only carry forward if there is a
      concrete reason that can be defended in review
```

B2 (history-aware adjacency O(total history) fold) was originally shipped
as a "known limitation, v4.1 will introduce a deleted-adjacency index" note
in CHANGELOG.md and three API doc comments. That is delay-by-documentation.
The honest fix turned out to be small: a new optional
`DeletedIterationCapability` (`ForEachDeletedNodeID` / `ForEachDeletedRelID`,
plus depth variants) implemented by every in-tree backend, plus an
adjacency-specific candidate fold (`forEachRelAdjacencyCandidateID`) in
the graph layer. Cost drops from O(total history) to O(deleted count).

Rule: every "known limitation / will fix in v4.1" note is a code smell.
Before writing one, ask "if this had bitten me 6 months ago, would I be
asking why we shipped with the regression?" If yes, fix it now.

## 28. Adjacency Endpoint Immutability Permits Deleted-Only Folds

```
BAD:  unified "fold all history" helper used both for label/property
      temporal queries and adjacency temporal queries
GOOD: split into forEach*CandidateID (all-history, for queries where
      current state can differ from at-t state) and
      forEachRelAdjacencyCandidateID (deleted-only, for adjacency where
      endpoints are immutable)
```

The first attempt at B2 collapsed all candidate folds to "deleted only".
That broke history-aware label/property queries because an entity whose
CURRENT label is Y but historically was X at t is NOT deleted — it has a
current row — yet still must be visited so the predicate can match its
at-t version.

Adjacency is the special case: rel endpoints are immutable, so a rel that
ever pointed at node N still does if alive. Therefore the only rels
missing from the live adjacency index for N are DELETED rels.

When optimising a generic helper used in multiple semantic contexts, split
the helper before changing one of its callers — never let one call site's
optimization break another's correctness.

## 29. PublishBatch "Atomic" Ordering Survives Saturation Only With A Floor

```
BAD:  docstring promises "no lower-priority event before all higher-
      priority batch events" but BackpressureBlock fires an in-batch
      wake-up that lets the dispatcher do exactly that
GOOD: per-batch priority ceiling enforced by the dispatcher
```

`AsyncEventBus.PublishBatch` documented strict priority ordering across the
whole batch. Implementation reality: when a priority queue saturates
mid-batch under BackpressureBlock, `enqueueLocked` signals the dispatcher
to drain space (avoids deadlock). The dispatcher wakes, drains the
saturated priority, then — before the batch's later same-or-higher
priority events arrive — can pick up a pre-existing lower-priority event.

Fix: `batchPriorityCeiling` atomic Int32. PublishBatch raises it to the
current priority index+1 at the top of each pass, clears it at end of
batch. Dispatcher's priority scan skips `priorityOrder[i]` for
`i >= ceiling`, so the in-batch wake-up serves only same-or-higher
priorities. Liveness preserved (existing
`TestAsyncEventBusPublishBatchBlockWakesBeforeFullQueueWait` still passes).

Test pattern: stage a pre-existing lower-priority event, force a batch
saturation with QueueSize smaller than the per-priority slice, then assert
the dispatched order has all batch events before the lower-priority one.

## 30. File Size Is A Signal, Not A Rule

```
BAD:  one growing file becomes a grab-bag because every helper has the
      same receiver type
GOOD: split when the file claims to be one thing in its top comment but
      has accumulated multiple unrelated responsibilities
```

`store_capabilities.go` started as the four capability getters (~50 LOC)
and grew to 917 LOC of validators, row copiers, entity fetchers, and ID
iterators — all glued together because they share `c *Core` as receiver.
Split into 4 files (`store_capabilities.go`, `store_validation.go`,
`store_copy.go`, `store_fetch.go`) along "what does this return" lines.

The signal isn't line count alone. It's the gap between the file's
self-described purpose (per its top comment) and what's actually inside.
A 900-line file is fine if it does one thing; a 200-line grab-bag is not.

## 31. Tx Holds The Write Lock — Mirror Every Read Accessor

```
BAD:  tx := g.Tx.Begin(); defer tx.Rollback()
      n, _ := tx.GetNode(id)
      labels := g.Nodes.Labels(n)   // deadlocks: RLock waits on tx's Lock
GOOD: labels := tx.Labels(n)        // calls *Unlocked helper, no c.mu re-entry
```

`BeginTx` holds `c.mu.Lock` for the entire transaction. Any public read
accessor that opens with `c.mu.RLock` (resolution, shadow properties,
snapshot, export, verification) deadlocks the calling goroutine because
`sync.RWMutex` is not reentrant (lesson 9). Whenever you add a public
read accessor that does `c.mu.RLock(); … c.fooUnlocked(…)`, you owe a
matching `(tx *GraphTx) Foo(...)` in `tx_consistent_reads.go` that calls
`tx.g.fooUnlocked` directly under `tx.mu`. Audit by grepping
`c\.mu\.RLock\(\)` and listing every entry point — each must have a tx
mirror or a written reason it never gets called inside a tx.

