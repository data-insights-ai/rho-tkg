# Lessons - tkg/v4

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
BAD:  g.mu.RLock(); g.Nodes().Get(ctx, id) // inner RLock can deadlock
GOOD: g.mu.RLock(); getNodeLocked(id)      // *Locked helper, no c.mu re-entry
```

Do not call exported graph methods from already-locked graph internals. Use
unexported lock-free helpers (`*Locked` / `*Unlocked`) for nested operations.
This rule was load-bearing in v3.4 / v4.0.x when `BeginTx` held `c.mu.Lock`
for the whole tx lifetime — see SUPERSEDED lesson 31 for that bug class.
Path B (v4.1.0) removed the lifetime-Lock, but the principle still applies
inside any internal code path that legitimately needs an enclosing lock
(rollback, batch execute, snapshot, export).

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

## 31. Tx Holds The Write Lock — Mirror Every Read Accessor (SUPERSEDED in v4.1.0)

**Resolved in v4.1.0 by Path B (`BeginTx` no longer holds `c.mu.Lock`).**
Both call shapes below now work; the tx-side mirrors remain for clarity
but are no longer required for deadlock avoidance. The historical rule
is preserved for context — if anyone ever reintroduces
`c.mu.Lock`-for-tx-lifetime, every read accessor that takes
`c.mu.RLock` deadlocks again.

```
v4.0.x (deadlock):   tx := g.Tx.Begin(); defer tx.Rollback()
                     labels := g.Nodes.Labels(n)            // hangs
                     rows, _ := g.Nodes.ByLabel(l, opts)    // hangs
v4.1.0 (both work):  labels := g.Nodes.Labels(n)            // ok
                     labels := tx.Labels(n)                  // ok
```

`BeginTx` holds `c.mu.Lock` for the entire transaction. Two doors lead
into deadlock:

1. **Direct** `c.mu.RLock` in the method body (resolution, shadow
   properties, hot-path Get, Count*, SearchNearest, ListShards,
   GetSync/GetAsync, ConstraintOps.Get, StatOps.Get).
2. **Indirect** via `c.readUnderRLock(func(){…})` — the helper at
   `locks.go:40` that wraps a closure under RLock. This was the
   v4.0.1 miss: ~28 query methods route through it (all of
   `queries.go`, `graph_property_query.go`, `temporal_queries.go`,
   `txtime.go`, `stats.go`).

`sync.RWMutex` is not reentrant (lesson 9), so either door deadlocks
the same goroutine that owns `c.mu.Lock` through the tx.

**Audit recipe** when adding a new public read accessor:

```
grep -nE "c\.mu\.RLock|c\.readUnderRLock" pkg/graph/internal/core/*.go
```

Every hit in that grep that's reachable from a public sub-API method
owes either (a) a `(tx *GraphTx) Foo(...)` mirror in
`tx_consistent_reads.go` that calls a lock-free `c.fooLocked` helper
directly under `tx.mu`, or (b) a written reason it can never be called
inside a tx. The mirror pattern is:

```go
// Public method (still acquires lock):
func (n *NodeOps) Foo(...) (T, error) {
    c := n.c
    // validation that doesn't need the lock
    var result T
    err := c.readUnderRLock(func() error {
        var err error
        result, err = c.fooLocked(...)
        return err
    })
    return result, err
}

// Lock-free helper (callers must hold c.mu R or W):
func (c *Core) fooLocked(...) (T, error) { /* … */ }

// Tx mirror (inherits c.mu.Lock from BeginTx):
func (tx *GraphTx) Foo(...) (T, error) {
    if err := tx.lockActive(); err != nil { return zero, err }
    defer tx.mu.Unlock()
    // same validation
    return tx.g.fooLocked(...)
}
```

v4.0.1 mirrored only the direct-RLock metadata-resolution set;
v4.0.2 added ~27 mirrors for the bulk-read methods (queries, temporal,
AsOf, stats, search). Path B (v4.1.0) will drop `c.mu.Lock` from tx
lifetime entirely and eliminate the whole bug class — but until then,
this rule remains load-bearing.


## 32. Don't Conflate Transaction Time With Valid Time In The Resolver

```
BAD:  vEnd = next.UpdatedAt  // TX time of the supersede
GOOD: vEnd = next.ValidFrom  // VT of the next state — falls back to UpdatedAt when unset
```

Bitemporal storage keeps TxFrom/TxTo independent of ValidFrom/ValidTo,
but the resolver historically used the next version's UpdatedAt
(transaction time) as the *valid-time* end of the prior version. Two
consequences:

- Caller-supplied `tkg_valid_from` on the next version did not close the
  prior version's tile — the prior interval ran up to the supersede TX
  time, leaving a gap between TX time and the new VT.
- `NodesAtTx(validAt, txAt)` queries that should reconstruct the
  historical "as known then" view fell back to the current VT
  derivation.

Fix in `nodeVersionBounds` / `relVersionBounds`: when computing `vEnd`,
prefer `next.ValidFrom` over `next.UpdatedAt`. Keep `UpdatedAt` as the
fallback so callers who haven't adopted explicit valid-time still see
TX-as-VT semantics.

## 33. Update Must Not Inherit Valid-Time From The Previous Version

The pre-bitemporal Update path deep-copied the previous version then
left its TemporalMetadata.ValidFrom intact. The resolver had to detect
this with `nodeVersionInheritedValidFrom` ("is this row's ValidFrom
suspiciously equal to prev's?") and ignore it. The detector was a
heuristic that breaks the moment a Phase 2 caller explicitly sets
ValidFrom equal to prev's value.

Rule: after `prevState := current.DeepCopy()`, clear
`current.Temporal().ValidFrom` and `current.Temporal().ValidTo` before
re-applying caller-supplied values. `ValidFrom != 0` on a non-genesis
version then literally means "caller-supplied" — no heuristic needed.

The legacy detector stays as a back-compat shim for pre-Phase-1 data
on disk; it is harmless on new data because the predicate fails on
`tm.ValidFrom == 0`.

## 34. AsOf TX Queries Default To Now — Pass TxAt For Bitemporal Reads

`QueryOpts.TxAt == 0` means "no TX filter" (any version matches by VT
regardless of when it was written). This preserves backward-compat for
every existing query, but it is NOT bitemporally correct "as of now":
it returns whatever VT-matches across the union of all history rows,
including writes that haven't yet happened in the caller's logical
timeline.

For genuine bitemporal point queries set `TxAt` explicitly. The
no-filter default exists to keep pre-bitemporal call sites working
unchanged — new bitemporal call sites should make their TX intent
explicit.



## 35. Cascade Edit Rewrites History Rows In Place

```
BAD:  cascade leaves overlapping history rows alone, relies on resolver
      precedence to pick the "right" version → ambiguous, surprising
GOOD: cascade classifies each overlapping version (close/open/eclipse/split)
      and rewrites the affected rows directly via PutNodeVersion;
      eclipsed rows use a zero-width sentinel (ValidTo = ValidFrom + 1)
```

The MVP cascade just added a new version and relied on resolver
latest-wins precedence to mask earlier versions — that surfaced
overlapping history rows with the same `ValidFrom`, which the
inheritance heuristic mistook for inherited values and dropped.

The full cascade rewrites every affected history row in place. Temporal
metadata is not part of the content hash (per
`integrity.computeNodeHashWithBuffer`), so mutating `ValidFrom` /
`ValidTo` on a frozen history row preserves the hash chain.

Split fragments need fresh version numbers (allocate as `maxVersion+1+i`).
Eclipsed rows must be invisible to VT queries — use `ValidTo ==
ValidFrom + 1` (the store rejects `ValidFrom == ValidTo`) and add an
explicit skip in `resolveNodeVersionAt` / `resolveRelVersionAt` so the
1-instant width does not cause spurious matches.

The new-current decision: the cascade row becomes current iff
`newVT == 0 AND no surviving post-cascade row has a later open-ended
ValidFrom`. Otherwise it lands in history.

## 36. Migration-Gated Resolver Heuristics Need A Runtime Flag

When you migrate persistent data to eliminate a resolver heuristic, the
heuristic must remain in code for stores that cannot run the migration:
backends without the new capability, mock stores in tests, and any
caller passing an injected store the migration cannot detect or write to.

Pattern: a `c.bitemporalMigrated bool` flag set by the post-`New`
migration runner. The resolver consults `if c.flag || !heuristic(...)`
to bypass the heuristic only when the migration has actually run on this
particular core. Removing the heuristic outright breaks tests that
construct cores with mock stores whose iteration deliberately errors.



## 37. Capacity-Soft Token Registries Don't Fail Writes

```
BAD:  registry.GetOrCreate(key) returns ErrRegistryFull at 65536 entries
      → wire encode fails → entity write fails → caller cannot persist
GOOD: registry.GetOrCreate(key) returns (0, nil) at capacity, sticky
      warn-once; encoder treats 0 as "fall back to raw key on wire"
```

Property-key cardinality is harder to bound than label cardinality
(UUID-keyed properties, dynamic schema, etc.). Refusing the write at
capacity turns a soft-degradation case into a hard outage. The
PropertyKeyRegistry returns token 0 on overflow; encoders MUST treat 0
as "no token assigned, write the raw key string." Storage savings
degrade gracefully without breaking persistence.

## 38. Validation Hooks Are The Right Place For Side Indexes

Adding a side index (e.g. property-key registry, cardinality stats)
requires hooking into a path that sees every entity mutation. The
attractive candidates — `types.PropertySlice.Set`, the wire encoder —
have downsides: `Set` lives in `pkg/types` which is fundamental
(circular dep risk), and the wire encoder is called at the store level
without Core context.

The validation layer on Core (`validateOwnedPropertyEntryForCreate`,
`validatePropertyUpdates`) is the natural seam: it sees every property
key on Add and Update, and runs with Core in scope. Hooking the
registry update there is one line per validator and stays out of the
type layer entirely.



## 39. Custom EncodeMsgpack Bypasses Struct Tags

```
BAD:  added a new msgpack-tagged field to PropertyWire; assumed msgpack.Marshal
      would serialize it. EncodeMsgpack on the same type hand-wrote fields
      and silently dropped the new one.
GOOD: when adding a tagged field to a struct that implements EncodeMsgpack,
      update the custom encoder to emit the new field too. Round-trip tests
      via msgpack.Marshal alone will not catch the gap when the project uses
      the custom encoder in production paths.
```

The custom `(PropertyWire) EncodeMsgpack` in `wire_encode.go` exists
"to avoid per-property reflective omitempty checks." Any new field added
to PropertyWire must also be appended to that custom encoder, with its
omitempty semantics replicated by hand. The struct tag alone is not
sufficient.

Audit recipe when adding a wire field:

```bash
grep -nE "EncodeMsgpack|EncodeMapLen" pkg/graph/internal/storeutil/
```

Every type listed there has a custom encoder you must update.

## 40. Load Registry Before Index Rebuild

When the store backend rebuilds an in-memory index from persisted rows
on `New()`, it must already have any registry needed to decode those
rows. For property-key tokenization specifically: load the property-key
registry from meta KV BEFORE `loadIndexes` calls
`bs.decodeNodeWireForKey`. Without this, tokenized rows fail decoding
during the rebuild — the node ID is dropped from the live-node map and
later `GetNode(id)` returns ErrNodeNotFound for a row that physically
exists on disk.

The rule generalises: any backend-internal decode that runs during
init must have its dependencies resolved before that init runs.

## 41. A Version Bump Is Not Done Until Every Doc-Consistency Target Matches

`TestDocsMetadataMatchesSourceOfTruth` derives the "current version" from
the first numeric `## [x.y.z]` heading in `CHANGELOG.md` and then asserts
`AGENTS.md` (`Status: vX.Y.Z`) and `docs/architecture.md` (title `(vX.Y.Z)`)
contain it, plus the `go.mod` Go version in README/AGENTS/architecture. The
4.4.0 release wrote a CHANGELOG entry claiming those docs were updated but
left both at `v4.3.2`, so the test failed on a clean checkout.

Rule: any commit that adds a numbered CHANGELOG heading MUST, in the same
commit, bump the `Status:` line in `AGENTS.md` AND the `(vX.Y.Z)` title in
`docs/architecture.md` (and the `Status:` lines in `CLAUDE.md`/`README.md`
for human consistency). Run `go test -run TestDocsMetadataMatchesSourceOfTruth
./pkg/graph/internal/core/` before committing a release. "The CHANGELOG says I
updated the docs" is not evidence the docs were updated — the test is.

## 42. Lesson 33 Applies To EVERY Deep-Copy Mutation Door, Not Just Update

```
BAD:  fix "clear inherited ValidFrom/ValidTo after DeepCopy" in
      updateNodeInternal only
GOOD: grep every mutation that DeepCopies current and writes *WithHistory;
      each one must clear the inherited world-time claim (unless its
      semantics are "close the interval": delete tombstones, CloseVersion)
```

The 4.3.0 fix for inherited valid-time landed on the Update door. The same
deep-copy-then-stamp pattern existed in `addNodeLabelInternal`,
`removeNodeLabelInternal`, and both property-CAS sites — so a label or
property mutation after an explicit-`tkg_valid_from` Add produced a new
current version that silently claimed the previous version's interval, and
every historical query (named, generic, per-ID — all three doors agreed,
all three wrong) resolved to the post-mutation state.

Audit recipe when fixing any version-boundary temporal bug:

```bash
rg -n 'WithHistory\(' pkg/graph/internal/core/*.go
```

Every hit that creates a NEW version from a deep copy of current owes the
lesson-33 clear. Delete paths and CloseVersion set ValidTo deliberately —
their semantics close the current interval rather than open a new state.

The detector for this whole class is the cross-door equivalence test
(`TestTemporalTwoDoorsAgreeOnLabelQueries`): exact-set agreement between
NodesByLabelAt, ByLabel+QueryOpts, and per-ID NodeAt on a dataset where the
label held historically but not on the current version. Single-door tests
cannot catch it because all doors share the broken resolver input.

## 43. Superseded Is Not Retracted — TxTo Must Not Bound Valid-Time Answerability

```
BAD:  visibleAtTx := TxFrom <= txAt && (TxTo == 0 || txAt < TxTo)
      // every update hides the prior version from ALL later txAt:
      // NodeAtTx(oldVT, now) returns nothing after one tiled update
GOOD: visibleAtTx := TxFrom <= txAt   // recorded-by-then
      // the resolver's vEnd derivation over the filtered chain
      // reconstructs the belief state as of txAt
```

TxTo marks when a version stopped being the CURRENT record. For bitemporal
point queries that is supersession, not retraction: the row remains the
authority for its valid-time slot in every later belief state. Bounding
visibility by TxTo conflated the two (lesson 32's bug class, in the
visibility rule itself) and silently broke the flagship 4.3.0 scenario —
no test had ever asked the (historical VT, current txAt) question; the only
pins were unit-level boundary checks on the predicate.

Detector pattern: after an explicit-VT update (v0 VT=[1000,∞) → v1
VT=[2000,∞)), assert NodeAtTx(1500, now) == v0 content, NodeAtTx(2500, now)
== v1, NodeAtTx(2500, txBetween) == v0 (as believed then), and
NodeAtTx(*, txBeforeFirstRecord) == nothing. Also the frozen-row poisoning
pattern from the same round: every pointer an accessor returns from a frozen
scan row must be independent (Temporal()/Integrity() copy-on-frozen) —
flag-guarded methods do not guard pointer escapes.

## 44. A Trust Boundary That Stores Hashes Must Verify Them On Entry

```
BAD:  import validates structure (tokens, temporal ranges, property shapes)
      but stores the stream's rows verbatim — a transport bit flip in a
      property value or a PrevHash string imports cleanly and the graph
      fails its own Verify*Chain afterwards
GOOD: recompute each imported row's content hash against the hash the
      stream claims, AND run the existing chain verification over every
      imported entity after replay (content checks cannot see LINK
      corruption); any mismatch → ErrCorruptExport + rollback
```

Structural validation proves the bytes decode into a well-formed entity;
it proves nothing about whether the entity is the one that was exported.
When the format carries its own integrity state, "untrusted input" (lesson
6) includes verifying that state — otherwise the trust boundary launders
corruption into a store whose verification feature then reports it as if
the GRAPH were corrupt. Classification matters too: every structural
failure mode (including truncation mid-record) must wrap the documented
sentinel, or consumers cannot distinguish corrupt input from I/O errors.

Detector pattern: export a graph with history + integrity, then (a)
truncate the stream at many offsets, (b) flip single bytes at spread
positions. Every attempt must either fail with errors.Is(ErrCorruptExport)
and leave ZERO partial state, or import a graph whose every entity passes
Verify*Chain. There is no third outcome.

## 45. Shrinking A Resource Bound Must Account For State Written Under The Old Bound — And A Dependency's Assertion Failure May Be os.Exit, Not An Error

```
BAD:  gate := MemTableSize > 0 && !InMemory && !ReadOnly   // migrate only here
      // a read-only open over an oversized WAL still replays it -> os.Exit
GOOD: writable path migrates; read-only / above-cap paths detect the unsafe
      condition BEFORE the dependency open and fail closed (ErrOversizedWAL)
```

Two reusable rules from the Badger memtable-shrink work (lesson source:
[4.8.0]):

1. **State written under the old bound may be unopenable under the new one.**
   Badger creates each WAL at 2x the MemTableSize that wrote it and replays it
   into an arena sized by the CURRENT MemTableSize. Shrink the memtable on a
   dir that still holds live WALs (copied from a running server, or left by a
   crash — a clean Close deletes WALs, so unit tests never see it) and the
   open fails. Any "make the bound smaller" change owes a migration path for
   data persisted under the larger bound. `MigrateOversizedWAL` flushes such
   WALs at their recovered original size first.

2. **A dependency's invariant violation may terminate the process, not return.**
   Badger raises "Arena too small" via `y.AssertTruef` → `log.Fatal` →
   `os.Exit` — it is NOT a recoverable error and NOT a panic you can
   `recover()`. So a "probe" open (e.g. the tiered read-only recovery probe)
   is useless against it: a read-only open replays WALs into the same bounded
   arena and os.Exits identically. The fix is to detect the unsafe condition
   with cheap, non-destructive code (scan `.mem` file sizes — Badger decides
   on apparent size) BEFORE handing the dir to the dependency, then either
   migrate (writable path) or fail closed with a returned sentinel
   (`ErrOversizedWAL`) on paths that cannot migrate (read-only, or a WAL
   recovered-size above the 1GB cap from a foreign/older writer).

Test the os.Exit faithfully with a subprocess (`os.Args[0]` re-exec + an env
flag) that performs the RAW dependency open and assert the child does not
exit 0 — an in-process test cannot observe `os.Exit`. Mutation-verify every
"the knob is applied" test by dropping the `With…` call and confirming the
test fails (a clean Close truncates files, so on-disk size checks must run
while the store is OPEN; `db.Opts()` witnesses BlockCacheSize/NumCompactors
that leave no file footprint).
## 46. A Correction Is A New Belief — Never Mutate A Stored Row To Express It

The cascade (`SetNodeVersionInterval`) expressed a valid-time correction by
rewriting and splitting existing version rows IN PLACE, changing their stored
`ValidFrom`/`ValidTo` while preserving their original `TxFrom`. That is a
contradiction in a bitemporal store: transaction time is an append-only,
monotonic ledger of *when the DB learned each fact*. A row whose asserted
world-interval was decided *now* but whose `TxFrom` still reads the original
write time claims the DB believed a boundary it had not yet decided — so
`NodeAtTx(_, oldTxAt)` reconstructs a belief that never existed: holes where the
mutated row no longer covers, leaks where a later decision appears early. It also
created two TX-open rows with overlapping transaction-time intervals, which broke
the native `NodeAsOf` early-stop (it assumes version order == TxFrom order) and
silently diverged it from the memory/tiered backends.

Rule: a correction recorded at `now` must be expressed ONLY by appending fresh
rows stamped `TxFrom = UpdatedAt = now`; existing rows are immutable. This is the
same discipline a normal `Update` already follows (it appends a new version and
never edits a prior row's stored fact). The resolver then reconstructs any belief
state by filtering the chain to `TxFrom <= txAt` and tiling what remains — so the
untouched rows ARE the older belief and the appended rows the newer one. For the
resolver to tile a non-monotonic chain (a correction can land at an earlier
valid-from than a later version), it must order by effective valid-from (not
array/version order) and break valid-time overlaps by the newer belief (higher
`TxFrom`, then version). This pairs with lesson 43: `TxTo` does not bound
answerability (a superseded row still answers its valid slot), and `TxFrom` must
never be back-dated on a row the DB is writing now.

Detector pattern (two-phase, all backends): create state, record a correction at
a later tx, then query `*AtTx` at a `txAt` BEFORE the correction — every
world-time the entity covered then must still resolve to its pre-correction
value, with no holes and no leaked corrected value. A single-backend or
single-txAt test passes the buggy in-place cascade.

## 47. The Decode Step Is Part Of The Trust Boundary — And `recover()` Cannot Catch A Fatal Stack Overflow

```
BAD:  msgpack.Unmarshal(diskBytes, &NodeWire{})  // raw, at every store/import read
GOOD: storeutil.SafeUnmarshal(diskBytes, &w)     // depth-guard THEN recover
```

Lesson 6/44 said "the store is the trust boundary; verify untrusted bytes." The
miss: the *very first* step — `msgpack.Unmarshal` itself — was assumed to only
ever return an error on bad input. It does not. The vmihailenco/msgpack/v5
decoder turns hostile/corrupt bytes into a **process crash** two ways, both
upstream of `WireToNodeChecked`/`ValidateNodeWire` so every careful validator
ran too late:

1. **Reflect panic.** A msgpack map that repeats a key bound to an
   interface-typed struct field (`PropertyWire.Value any`) makes the second
   decode target an unaddressable `reflect.Value` → `panic: reflect:
   reflect.Value.SetString/SetInt using unaddressable value`. ~17 bytes.
2. **Fatal stack overflow.** `Value any` decodes deeply-nested arrays/maps by
   recursing once per level; a hundreds-of-thousands-deep blob aborts with
   `fatal error: stack overflow`. This is NOT a panic — `recover()` cannot
   catch it. It must be prevented BEFORE the decoder runs.

So the fix is two-layered and the ORDER matters: `SafeUnmarshal` first runs
`guardMsgpackDepth` (a non-recursive scan of the msgpack token stream — explicit
pending-count stack, never recursion — that rejects nesting beyond
`maxWireDecodeDepth`=64, far above the 32-level property allowlist, far below the
overflow point), THEN `defer recover()` around the decode for the panic class.
Both return the new `store.ErrCorruptWire` sentinel. The guard is deliberately
NOT a full validator — over-permissive is fine (msgpack + recover backstop it);
it only must never UNDER-count depth and must keep its cursor aligned by skipping
scalar payloads exactly.

Why depth 64 and not lower: legitimate wire is map(1)→"p" array(2)→PropertyWire
map(3)→value, and a value nested to the allowlist's 32-level limit sits at ~35;
64 leaves headroom so the guard never rejects data that would pass validation.
Pin it with a "guard accepts a depth-30 property + every seed entity" test —
lowering the cap below ~36 must fail that test.

Audit recipe (every fix needs the grep): `grep -rn 'msgpack.Unmarshal(' pkg/ |
grep -v _test`. Every hit decoding persisted or imported bytes owes
`SafeUnmarshal`. `msgpack.Marshal` is panic-free and needs no wrapper. Decodes
into flat typed structs with no interface or deeply-nestable field (tiered
catalog/registry/index metadata) are outside this class but routing them is
harmless (SafeUnmarshal returns the raw error on the normal path, preserving
`errors.Is`).

Detector: a fuzz target that feeds arbitrary bytes to the decode boundary and
asserts "never panics, returns entity-or-error" (`FuzzWireToNodeChecked`). It
found the panic on its own first run; the saved crasher stays as a corpus seed.
A fatal-overflow regression can't be a normal failing assertion (the process
aborts) — assert the POSITIVE contract instead (SafeUnmarshal returns
ErrCorruptWire on a 200000-deep blob); removing the guard fatal-crashes the test
binary, which is the signal.

## 48. Allocate Proportional To Bytes DELIVERED, Not To An Untrusted Size/Count Field

```
BAD:  data = make([]byte, length)   // length is an attacker-controlled header field
      if _, err := io.ReadFull(r, data); err != nil { ... }
GOOD: var b bytes.Buffer; b.Grow(min(length, cap))
      io.CopyN(&b, r, int64(length))  // grows with what actually arrives
```

A length/count read from an untrusted stream is a CLAIM, not a fact, until the
bytes behind it arrive. Pre-allocating the claimed size lets a tiny input
amplify into a huge allocation — a memory-exhaustion DoS at the trust boundary.
The import framing had TWO such sites, both found by fuzzing `Import`
end-to-end (`FuzzImport`) — the fuzzer didn't crash but STALLED at 0 execs/sec,
the signature of repeated giant allocations / GC thrash, not a hang:

1. `readImportStageRecord` did `make([]byte, length)` for the per-record
   declared length (<= maxExportRecordSize = 128 MiB). A 5-byte record header
   claiming 128 MiB on an empty body forced a 128 MiB allocation before the
   truncation was even detected (~25-million-x amplification). Fixed with
   `io.CopyN` into a `bytes.Buffer` (pre-reserving at most a 64 KiB cap), so the
   buffer grows only with bytes actually read; a short stream returns
   ErrCorruptExport having allocated ~nothing.
2. `reserve()` pre-sized SIX replay/rollback maps and slices from the export
   HEADER's node/rel COUNTS, capped only at `importPreallocLimit` = 1<<20. A
   ~20-byte header claiming 1M+1M counts forced ~312 MiB of map allocation
   before a single entity record was read. The maps are created empty in their
   constructors and grow naturally, so the count is a pure pre-sizing hint —
   lowered the cap to 4096 (common-case optimization kept; hostile amplification
   bounded to ~hundreds of KiB).

Rule: any `make(T, n)` / `Grow(n)` / map-capacity hint where `n` derives from an
untrusted decoded length or count must be bounded by either (a) the bytes that
have actually been delivered, or (b) a small constant cap — never the raw
declared value, even when a "max" cap exists (128 MiB / 1M are themselves DoS
sizes from a tiny input). This is the allocation-time twin of lesson 6.

Detector: `runtime.ReadMemStats(&m).TotalAlloc` is cumulative (GC never lowers
it), so a regression test can feed a tiny lying header and assert the alloc
delta stays far below the declared size — a deterministic mutation pin
(reverting the fix makes the delta jump to the declared 128 MiB / 312 MiB).
Fuzz a streaming/replay boundary END-TO-END, not just the leaf decoder, and read
a frozen exec rate as a DoS signal even when nothing crashes.

## 49. A Change-Log Must Be Emitted In-Backend And Co-Committed — A Decorator Cannot Be Crash-Safe

The horizontal-scaling Phase-0 design first proposed a `ChangeLogStore`
*decorator* wrapping `MandatoryStore`. Two code-grounded facts killed it:

1. **Atomicity.** Crash-safety requires the change-log record to commit in the
   SAME Badger `WriteBatch` as the entity rows + counters (the existing
   "counters in same batch" rule). A decorator sits ABOVE the `Store` interface
   and cannot reach the inner backend's `WriteBatch`; it could only do a second,
   separate durable write — reintroducing exactly the committed-but-unlogged
   window the feature exists to avoid.
2. **Trust.** `core.New` discovers native backends by reflection
   (`isExactNativeStore`); a decorator is none of `*memory`/`*badger`/`*tiered`,
   so it is treated as UNTRUSTED and the frozen-pointer zero-copy scan path is
   silently disabled (every scan row deep-copied).

Rule: a write-observer / op-log / CDC seam that must be crash-consistent with the
data belongs INSIDE each backend (co-committed in the same atomic write), exposed
as an OPTIONAL capability the core type-asserts — never as a Store wrapper.

Corollaries that bit during implementation:
- **The LSN + record must enqueue under one lock window with the entity ops.**
  Doors that hold `idxMu.Lock` across `appendOps` can log right after; doors that
  do NOT (`PutNodeVersion`/`PutRelVersion`, history truncate) need a combined
  `appendOpsLogged` so a background flush cannot snapshot the op without its
  record. Audit EVERY early-return-after-snapshot in `flush()` — each must
  requeue the log records too, and the empty-batch guard must check
  `len(ops)==0 && len(logs)==0` or a log-only flush silently drops records.
- **Shared, multi-`appendOps` doors must not each emit.** `deleteRelByInfo` /
  `cascadeDeleteInner` are reused by several doors; they stay record-free and the
  PUBLIC door emits exactly one logical record (one `NodeDelete` per cascade,
  not one `RelDelete` per edge). Build the payload from the door's own data.
- **Parity by construction, not by duplication.** Two backends emitting the same
  feed must share ONE set of record-body builders (`storeutil`), then a
  cross-backend test asserts byte-identical feeds. (Caveat: badger tokenizes
  property KEYS on the wire while memory keeps strings, so property-bearing
  payloads are semantically equal but not byte-identical — assert byte-parity on
  property-free entities, decode-equivalence otherwise.)
- **The op-log alone does not converge a replica.** Token allocation lives in the
  CORE layer (the backend only ever sees tokenized rows), so a per-token
  registry record is impossible in-backend — ship the registry in a full
  snapshot and have the replica bootstrap from it, then tail the feed.
- **A synchronous side path (`MetaSet` via its own txn) cannot share the async
  buffer's LSN/watermark without a non-monotonic-watermark hazard** — defer such
  records rather than bolt them on.

## 50. A Replica Must REPRODUCE The Primary's Rows, Not Re-Derive Them — And One Bit Of Provenance Decides History Depth

Phase-1 read replicas apply a primary's change-feed. The defining constraint:
the replica must be a BYTE-EXACT copy — same integrity hash, version, `TxFrom`,
temporal metadata — because the hash chains those fields together and any
divergence cascades to every later version. That forces several rules.

- **Apply writes the wire VERBATIM through foreign-ID store doors; it NEVER calls
  the normal create/update doors.** `NodeOps.Add`/`Update` re-mint IDs, re-stamp
  `TxFrom`, and recompute the hash — correct for an origin write, fatal for a
  replica. The apply path (`apply_record.go`) reuses the *import* trust pipeline
  (`WireTo*Checked` reconstructs the entity verbatim → token-validate →
  prop-limits → hash recompute-AND-COMPARE) and then calls `PutNode` /
  `ReplaceNode` / `ReplaceNodeWithHistory` / `Delete*` / `PutNodeVersion` /
  `Truncate*`/`Trim*` / label-token doors, all of which marshal the supplied
  entity as-is. The hash is VERIFIED, never STAMPED. Use the integrity hash as
  the convergence oracle in tests — equal hash proves equal canonical content +
  prev-hash, i.e. byte-exact reproduction including the chain.

- **A bare new-current row is ambiguous about provenance; carry the one missing
  bit at the door.** Phase-0's `ChangeNodePut` was emitted identically by create,
  in-place `ReplaceNode` (no history row), `ReplaceNodeWithHistory` (history row),
  AND label mutations. A replica reconstructing history from its own current row
  manufactures phantom rows for in-place updates and loses real ones for
  with-history updates — either way history depth diverges. The fix is exactly
  ONE bit (`NodePutBody.WithHistory`): the replica regenerates the EXACT prior
  history row from its in-sync local current (which, by LSN total-ordering,
  equals the primary's pre-mutation state), so it never needs the prior bytes
  shipped. Create-vs-update is inferred from local existence; a label change is a
  one-token diff of the local label set vs the wire. Lesson: when a log record
  must drive a state machine on the consumer, audit whether the record carries
  enough to disambiguate which TRANSITION produced it — not just the resulting
  state. The cheapest fix is usually a provenance bit at the emitting door, set
  identically across backends, not re-deriving on the consumer.

- **Reshaping the record to be UNTOKENIZED removed a whole sync problem.**
  Building the put-record wire via `NodeToWireChecked` (property keys as strings)
  instead of the backend's tokenized storage bytes made badger and memory feeds
  byte-identical even for property-bearing entities (closing the Phase-0 caveat)
  AND dropped the property-key registry from the replica's dependency set (only
  label/rel-type tokens remain, since those are part of the logical row). Prefer
  the backend-independent representation for a cross-node protocol; storage-local
  encodings (token dictionaries) leak coupling.

- **The read-only gate is a CORE-layer concern, not the store's `ReadOnly`
  mode.** Badger `ReadOnly` disables the change-log and rejects the apply path's
  own writes — useless for a replica that must keep applying. Add a core
  `checkWritable()` (= `checkOpen` + replica flag → `ErrReadOnlyReplica`) called
  by every USER mutation door; reads, the bootstrap importer, and `ApplyChange`
  call only `checkOpen`. Converting `checkOpen`→`checkWritable` per-door is
  error-prone: file-scope the rename ONLY in files that are purely mutation
  (`node_add`/`node_update`/`relationship_*`/`node_label`/`graph_indexes` —
  index management is all-mutation), and edit door-by-door where reads share the
  file (`node_delete`/`relationship_delete` have `Get`; `crud`'s `History` is a
  read; `temporal_queries` is mostly reads). `SetProperty`/`DeleteProperty`
  delegate to `Update`, so they inherit the gate for free — don't double-gate.

- **A watermark advanced AFTER the door commits is safe ONLY if apply is
  idempotent.** The applied-LSN is a separate `MetaSet` after each record's door
  commits (no co-commit — that would need a new batch-apply store capability), so
  a crash in between replays the last record. Every handler must absorb that:
  identical row → no-op (`nodeWireMatches`/`relWireMatches`, which must work
  against the untokenized wire), delete → tolerate not-found. Prove it with a
  double-apply test asserting history depth is unchanged — re-applying a
  with-history `NodePut` must NOT append a second history row.

Two bugs an adversarial review caught (both now fixed + tested):

- **A separate-path watermark can OUTRUN buffered data → permanent record loss.**
  The data door writes through the async write buffer (`flushIfNeeded` is a NO-OP
  without `SyncWrites` — the row sits in RAM), but the watermark `MetaSet` writes
  straight to Badger. A crash in between recovers the ADVANCED watermark while the
  entity write is lost, and the replica resumes past the gap — permanent
  divergence. The direction matters: a watermark BEHIND the data is harmless
  (idempotent re-apply); AHEAD is fatal. Rule: when a progress marker lives on a
  different durability path than the work it records, FLUSH the work to that same
  path BEFORE advancing the marker. Fix here: `flushStoreLocked()` (drains the
  buffer to the backend WAL) before `setAppliedLSNLocked`; batch apply flushes
  ONCE then advances to the last LSN. Also add a monotonicity guard (`rec.LSN <=
  applied` → skip) so a stale/out-of-order redelivery can't regress the row OR
  the watermark. InMemory-only tests hide this entirely — add a disk-backed,
  non-`SyncWrites` restart-resume test.

- **Gate-completeness is enumerated, not pattern-matched.** The read-only gate
  conversion missed `CompareAndSetProperty` and `CloseVersion` (both genuine
  writers — `ReplaceNode`/`ReplaceNodeWithHistory` under the hood) because they
  live in files (`property_cas.go`, `version_chain.go`) whose OTHER methods are
  reads, so the file-scope `perl` sweep skipped them and the door-by-door pass
  overlooked them. A write door is not always in an obviously-named file. Audit
  by what the method DOES (does its body reach a `store.Put/Replace/Delete`
  door?), grepping mutation verbs across EVERY `*Ops` method, and back it with a
  gate test that exercises each door by name — a gate with a hole is invisible
  until the missing door is tested.


## 51. Syncing An Append-Only Registry Across Nodes: Capture-Order, Prefix-Guard, And Persist-Before-Use

Phase-1 replicas resolve a label/rel-type the primary registered AFTER their
bootstrap by refetching the primary's registry and growing their own. Four
things make it correct; each is a trap if skipped.

- **Capture the anchor LSN BEFORE the names, never after.** `RegistrySnapshot`
  returns the registries plus a `CapturedAtLSN` the replica checks against the
  record that triggered the refetch (`CapturedAtLSN >= rec.LSN`). If you read the
  names first and the LSN second, a token allocated+committed in between makes
  `CapturedAtLSN` claim coverage the names don't have → the replica appends an
  incomplete registry and the missing token never resolves. Reading the LSN
  first guarantees `CapturedAtLSN <= names coverage` (a mutation committed at/below
  that LSN allocated its token before committing, hence before the later
  names read under `registryMu`); extra tokens beyond the LSN are harmless.

- **The grow primitive must be prefix-guarded — `ImportNames` is load-only.** A
  registry that bootstrapped non-empty cannot be re-imported (`ImportNames`
  rejects a non-empty registry). The new `AppendNames(prefix, suffix)` asserts
  the registry's CURRENT contents DeepEqual the caller's observed `prefix`, then
  appends `suffix` at contiguous indices; on any divergence it returns
  `(false, nil)` WITHOUT mutating. That guard is the linchpin: the registry is
  gap-free/monotone, so a wrong guard would silently place suffix tokens at the
  wrong indices and corrupt every later token resolution. Add it to all three
  registries (parity), even the one (`PropertyKeyRegistry`) the hook doesn't use.

- **Persist the grow BEFORE the row that needs it; roll back on persist failure.**
  The refetch grows the in-memory registry, then `persistRegistries()`, all under
  `registryMu`, BEFORE the entity store door runs. If the persist fails, the
  in-memory grow MUST be rolled back (`RollbackNames`) so in-memory == disk —
  otherwise a later retry sees the token already resolvable in RAM, skips the
  refetch+persist, writes the entity, and a crash then leaves a row referencing a
  token that was never persisted (a dangling token on restart). Same
  flush-before-watermark discipline as the apply path, one level down.

- **Don't sync what's tokenized locally.** Records carry UNTOKENIZED property
  keys (string keys), so a replica tokenizes them in its OWN independent
  property-key registry — its propkey tokens need not match the primary's. The
  refetch therefore syncs ONLY labels + rel-types (which appear as tokens in the
  wire and MUST match). Appending the primary's propkeys would fail the
  prefix-guard against the replica's divergent propkey registry. `RegistrySnapshot`
  still ships `PropKeys` (a full snapshot for any consumer), but the hook ignores
  them. Know which tokens are part of the logical row vs a storage-local detail.

Lock ordering: the refetch runs UNDER the apply's `c.mu.Lock` and takes ONLY
`c.registryMu` (order `c.mu → registryMu`, matching the existing
`getOrCreate*Persisted` path); it calls the source (a DIFFERENT/primary Core,
taking only ITS locks) and never re-takes `c.mu` — so even a self-pointing
source can't deadlock (distinct mutexes), and the `CapturedAtLSN` guard catches
the self-misconfiguration.

Failover plumbing belongs in the library; the coordination does not. The ID-slot
lease is persisted/read by rho-tkg (`SafeUnmarshal` on read — it's an
untrusted-bytes site; slot range-validated at write) but is last-writer-wins
(`DetectConflicts=false`), NOT a consensus primitive. The external orchestrator
serializes writes, assigns slots, and triggers promotion = `Close()`+`New()`
under the leased slot (snowflake generators are immutable post-`New`, so
promotion is always a reopen). A promoted node and the node it replaces must
hold DIFFERENT slots — that slot difference, not the lease's durability, is what
prevents minted-ID collisions in a split-brain window. Ship the durable hint and
the reopen path; leave "who holds the lease, and when" to the layer that can
actually coordinate.

## 52. Delta Backups Reuse The Op-Log, Not A Diff — And A Hash Oracle Hides Non-Hashed Divergence

`ExportSince` / `ImportMerge` (node-level incremental backups) frame the
change-log feed into the export stream and replay it through the replica-apply
doors. Five reusable lessons fell out of building it.

- **Key a delta off the change-log (TX-ordered op-log), never a temporal DIFF.**
  Two independent reasons. (1) A *valid-time* diff (`Temporal().Diff`) silently
  DROPS backdated/backfilled writes — a fact recorded now with an old
  `valid_from` is "unchanged" by a VT diff, so a backup misses it. The cursor
  must be transaction-monotonic (the LSN), not application time. (2) A
  state-endpoint diff (state-at-since vs state-at-now) cannot reconstruct the
  PRIMITIVE mutations between the endpoints: a label-set change must be replayed
  as per-token `Add/RemoveNodeLabelToken` (a `ReplaceNode` does NOT reindex
  labels), and a cascade delete needs the exact connected-rel tombstones
  (`DeleteNodeWithHistory` requires one per live edge). The op-log already
  records those primitives in order and the apply path already replays them
  correctly — reuse it; don't re-derive.

- **A hash-equality convergence oracle cannot see a non-hashed-field divergence.**
  The replica byte-exactness claim (lesson 50) was verified only by integrity
  hash, which excludes `TxFrom`/`TxTo` (lesson 43). A WithHistory put record
  carries only the NEW version and regenerates the superseded prior row from the
  consumer's local current — which has `TxTo = 0`, while the origin stamped
  `prev.TxTo = now` (= the new version's `TxFrom`). So replicas/deltas were
  hash-exact but NOT byte-exact in that one field; the hash oracle was blind to
  it. A byte-level export comparison caught it. Fix: reproduce
  `prev.TxTo = next.TxFrom` at the apply door (`reproduceSuperseded*TxTo`) — every
  WithHistory emitter uses one monotonic `now` for both, so the equality holds
  universally. Rule: when reproducing rows across a boundary, assert the
  reproduction with a check that includes fields the hash omits, or the byte/
  field gap ships invisibly.

- **A change-feed shows only DURABLY-COMMITTED records — flush before reading it.**
  Badger's async write buffer holds committed-but-unflushed rows; `ChangeFeed` /
  `LastCommittedLSN` (and thus the export-header `SnapshotLSN`) surface only
  flushed records. Reading the feed without flushing yields a delta missing the
  most recent mutations, and a full export stamps a stale (often 0) snapshot LSN
  so a replica / first delta resumes from 0 and re-ships the whole graph. Both
  `Export` and `ExportSince` now `flushStoreLocked()` before reading. (Memory has
  no buffer, so memory-only tests never expose this — add a badger/disk path.)
  Same family as lesson 50's "flush the work to its durability path before
  advancing the marker."

- **A "present but disabled" optional capability needs a status probe.** The
  in-tree backends implement `ChangeFeedCapability` UNCONDITIONALLY (the methods
  exist, returning an empty feed when the log is off), so a nil-interface check
  cannot tell "recording" from "off." A delta from a disabled log would silently
  record nothing — a data-loss footgun. Added `store.ChangeLogStatusCapability`
  (`ChangeLogEnabled() bool`); `ExportSince`/`Watermark` fail closed when off.
  Rule: if a capability can be present-but-inert, expose an explicit status, don't
  infer activity from the interface being satisfied.

- **The integrity hash chain is a DAG, not a line — verification must match.**
  `VerifyNodeChain`/`VerifyRelChain` checked `v_k.PrevHash == v_{k-1}.Hash` in
  VERSION order. But a bitemporal correction (`SetNodeVersionInterval`) appends a
  version whose `PrevHash` links to the version it supersedes ON THE VALID-TIME
  axis (a fresh-numbered row pointing back to a possibly-lower version) — a DAG,
  as `temporal_cascade.go` explicitly documents. The linear check rejected every
  cascade-corrected graph, so it failed its OWN verification and a full
  `Export`+`Import` of it died with `ErrCorruptExport` — such graphs could not be
  backed up at all (found via the delta round-trip test; the bug long predated it).
  Fix: verify linkage as "every non-genesis `PrevHash` ∈ {hashes of all versions}"
  (genesis empty; lowest retained version may dangle for truncation), keeping the
  per-version content-hash recompute as the tamper evidence. Lesson: when a
  feature documents a non-linear structure (the cascade's VT-axis PrevHash), the
  VALIDATOR for that structure must be written to the same model — a validator
  silently assuming linearity is a latent rejector of valid data, invisible until
  something round-trips the non-linear case end to end.

- **A purge-and-recreate rollback must clear version history too.** The merge
  rollback purges each touched entity and re-creates it from a pre-merge snapshot
  via the CREATE doors (which rebuild every index correctly, sidestepping the
  label-immutability of `ReplaceNode`). But `DeleteNodeCascade` clears the current
  row + adjacency + indexes and NOT the `0x07` history keyspace, so a rolled-back
  update left a stale history row → partial state. Truncate history (`Truncate*History(id,0)`)
  in the purge step, mirroring `restoreImportNodeHistory`. Also: a delta's
  registry merge is a BIDIRECTIONAL prefix check (an OLDER delta re-applied after
  a newer one already grew the base is a no-op, not a divergence) — distinct from
  replication's append-only `appendRegistrySuffix`, where the primary is always
  ahead of the replica.

## 53. A Reset That Wipes A Durable Watermark Must Keep The Watermark Continuously Present — Order The Wipe So No Crash Point Is Inconsistent

`Clear()` with the change-log enabled used `db.DropAll()` (atomic, wipes the
WHOLE keyspace incl. `LastLSNKey`) then wrote a fresh `ChangeClear` marker at a
new LSN. The gap: a crash AFTER the DropAll commit but BEFORE the marker flush
reopens the store with neither change-log records NOR a watermark, so the LSN
allocator reseeds from 0 — and post-`Clear` LSNs then collide with a tailing
consumer's pre-`Clear` watermark. The consumer is checkpointed ABOVE those reused
LSNs, so it never sees the new records: silent, permanent strand.

The fix (`clearAndReanchorChangeLog`) never lets `LastLSNKey` be absent: it
`DropPrefix`es every data / index / history / change-log-record keyspace and
deliberately leaves `KeyMeta` (which holds `LastLSNKey`) intact, then overwrites
`LastLSNKey` with the new watermark alongside the marker in one atomic batch. At
every crash point the watermark holds its old OR new value, never nothing, so the
allocator is monotone.

Two ordering invariants make it crash-correct, and both are easy to get wrong:

- **Delete the stale meta keys (entity counters, registries, index defs) BEFORE
  dropping the data, not after.** `loadIndexes` treats a persisted counter that
  EXCEEDS the live row count as fatal data loss (`reconcilePersistedCounter`),
  while an ABSENT counter safely trusts the live rows. Drop the data first and a
  crash before the counter delete reopens with a counter claiming rows that no
  longer exist — the store refuses to open. Counters-first means a crash there
  leaves data present + counters absent = consistent.
- **Scan for the meta keys to delete rather than hard-coding a list**, so
  dynamically-set meta (the replica watermark, the id-slot lease, future keys)
  is reaped too — but skip `LastLSNKey` explicitly. A wipe that forgets a new
  meta key leaks pre-`Clear` state into a logically-empty store.

General rule: when a "reset" must preserve ONE durable invariant (here, watermark
monotonicity), you cannot express it as wipe-everything-then-rewrite — the
rewrite is a second transaction and the crash window between them violates the
invariant. Keep the invariant's key continuously present and mutate it in place.

**Fix EVERY door to the invariant, not just the one in front of you.** The first
fix only guarded the change-log-ENABLED `Clear()` arm. But the durable watermark
outlives the config flag: a store run with the log ON leaves `LastLSNKey` on disk,
and the log-DISABLED `Clear()` arm was a bare `DropAll` that wiped it — so
`ChangeLog`-on → reopen `ChangeLog`-off + `Clear` → reopen `ChangeLog`-on reseeded
the allocator to 0 and reused LSNs a consumer had already passed, the identical
silent-divergence bug through a sibling door (a break-test audit found it; the
failing test showed pre-`Clear` watermark 2, post-reopen write LSN 1). The
invariant is "the watermark is never absent if it was ever present" — that is a
property of the DATA on disk, not of the current session's config, so every reset
path must honor it regardless of whether THIS session produces records. Both arms
now share one `clearDataPreservingLastLSN` helper that preserves `LastLSNKey`
whenever present; only a never-logged store (no watermark to protect) still
`DropAll`s. When you fix a durability invariant, grep for every code path that
deletes/overwrites its key and confirm each preserves it — a config-gated branch
is exactly where the second door hides.

## 54. A Swap-Out Commit-Window Buffer Must Be Cleared On Success AND Consulted By EVERY Reader — Or It Both Drops And Resurrects Rows

The badger async flush snapshots the `pending` write buffer and swaps in a fresh
empty map under `wbMu`, then releases `idxMu` and commits the WriteBatch with NO
lock held. For the whole commit window (registry persist + batch build +
`wb.Flush()`, i.e. milliseconds) a just-written row is GONE from `pending` and
NOT YET in a Badger `View`. Any overlay reader that reads only `pending` and
falls through to a `View` momentarily DROPS that row — a latent read-your-writes
hole. The intended fix parks the swapped-out snapshot in a second map
(`flushing`) and unions `flushing ++ pending` in a shared helper
(`rangePending` / `lookupPending`). It was both incomplete and self-harming:

- **Incomplete: only ONE of ~13 overlay readers was routed through the helper.**
  The single-key version lookups, the history scans, `Max*HistoryID`, the
  truncate/trim retention scans, the AsOf version-chain overlay, and the on-disk
  label/adjacency overlays all still read `pending` directly — so they kept
  dropping in-flight rows. A partial fix of a latent bug reads like a fix while
  the bug still fires behind every door you didn't touch. **Audit EVERY reader of
  the buffer, not just the one that motivated the change** (`grep 'range
  bs.pending\|bs.pending\['`), and route them through ONE helper so the
  union logic has a single definition — divergence between "readers that consult
  the snapshot" and "readers that don't" is itself the bug class (`Max*HistoryID`
  disagreed with `All*HistoryIDsFrom`).

- **Self-harming: the snapshot was never cleared on a successful commit** (only
  on the failed-flush requeue), so after a normal flush `flushing` retained the
  just-committed batch until the next flush overwrote it — and under `SyncWrites`
  there is no periodic flush, so it persisted indefinitely. Combined with a
  `Clear()` that reset `pending` but not `flushing`, the wiped history keys
  RESURRECTED as phantom IDs through the one reader that DID consult `flushing`.
  A commit-window buffer has a precise lifetime: populated between the swap and
  the commit, EMPTY otherwise. Clear it on success, on the failure requeue, AND
  in any `Clear()`/reset that wipes the underlying keyspace — anything that holds
  it past the commit turns "make in-flight rows visible" into "make committed-or-
  deleted rows visible."

Two more traps the completion surfaced:

- **The delete-set-computing MUTATORS need the snapshot too, not just the
  read-only readers.** Cascade delete and incoming-repair compute "which index
  keys exist" from a Badger `View` + `pending` to decide what to delete; a key
  parked in `flushing` is in neither, so they queue no delete and ORPHAN a
  persisted index key once the in-flight commit lands it. They can't rewrite a
  `flushing` entry in place (it's committing) — queue an explicit delete into
  `pending` so a later flush removes it after the commit.
- **Why only SOME readers were exposed:** in RAM mode the in-memory index maps
  (`labelIdx` / `inIdx` / `outIdx`) are synchronous and still hold a parked op's
  key, so RAM-mode readers were always correct — only the history readers (no RAM
  mirror) and the on-disk label/adjacency overlays read the buffer directly and
  were exposed. When a buffer has a synchronous shadow for most consumers, the
  few that read it raw are exactly where the window bites; enumerate them
  deliberately. When the helper unions two buffers, make set/delete SYMMETRIC
  (each op removes the key from the other set) so a key present in both during the
  requeue window resolves to the newer (visited-last) op.

Test the window deterministically without real concurrency: a white-box helper
that performs the exact `pending`→`flushing` swap WITHOUT committing reproduces
the mid-commit state, so every reader can be asserted to still see the row, and
`Clear()`-then-read can be asserted to NOT (failing-first verified by reverting
each fix).

## 55. The Change-Log Is A PHYSICAL Redo Log, Not A LOGICAL Transaction Log — A Rolled-Back Tx Ships Its Churn To The Feed

The op-log is emitted IN-BACKEND on every store mutation (`logChangeRaw` inside
`PutNode`/`DeleteNodeCascade`/…). A `GraphTx` applies its mutations to the store
IMMEDIATELY during the tx body (it is not a staged write set — `Rollback` undoes
them by reverse store calls, which is only possible because they were applied),
so each tx-body write emits a record AND each rollback reverse-write emits a
record. A break-test audit confirmed it concretely: a tx that creates a node then
`Rollback()`s leaves TWO records past the pre-tx watermark — `ChangeNodePut`
(LSN N) then a hard-cascade `ChangeNodeDelete` (LSN N+1) — even though the final
local state is empty.

Consequences, none of which is a convergence bug but all of which are contract:
- **Replicas CONVERGE but transiently materialize uncommitted state.** A replica
  tailing create-then-hard-delete ends in the correct final state (phantom
  absent), but between applying the two records it holds a node that no committed
  primary state ever contained. "Eventually byte-exact," not transactionally
  isolated.
- **Asymmetric with events.** The EventBus BUFFERS tx events and DISCARDS them on
  rollback (subscribers never see rolled-back mutations — `txEventBuffer`). The
  change-log does the opposite. So the change-feed is NOT a logical committed-tx
  CDC source; a consumer that treats it as one will see aborted-tx churn.
- **"Records every committed mutation" means BACKEND-committed, not
  tx-committed.** Both the tx-body put and the rollback delete are committed to
  the backend `WriteBatch`; the log faithfully records both. The word "committed"
  in the feature's prose is about the store, not the transaction.
- **This is the ONLY public primary door that emits a hard-cascade
  `ChangeNodeDelete` (WithHistory=false).** Every standalone/transactional delete
  door routes to `DeleteNodeWithHistory`; the hard path is reached only by
  rollback (tx/add/import) and the replica apply itself. So the apply
  `!WithHistory` node-delete branch is live precisely BECAUSE of rollback churn —
  test it with a real rolled-back-tx record, not only a synthetic body.

**The SERIOUS bug this surfaced (BUG, not contract): a rolled-back tx that
allocated a new token POISONED the feed and permanently stalled replicas.** I
first wrote this lesson off as "converges, no behavior change, just document it."
That was wrong, and a deeper analysis (forced by the user — "this is production
code, analyse") proved it. A tx that allocates a NEW label / rel-type token emits
a durable `ChangeNodePut`/`ChangeRelPut` referencing it, then `Rollback()` →
`restoreRegistries()` rebuilt the registry from the pre-tx snapshot, DE-allocating
the token. The feed now permanently referenced a token the primary no longer held.
A replica — EVEN WITH a `ReplicationSource` — could never resolve it (the refetch
finds it absent, because the primary rolled it back too) → **stuck forever at that
LSN**; and the next committed allocation REUSED the number for a different name →
silent divergence. Confirmed by a failing test: pre-rollback the replica stalls
with "rel type token 1 not in registry (size 0)". The same poison exists in the
whole de-allocation FAMILY — standalone `restoreNewLabelsOnError` /
`restoreNewRelTypeOnError`, the batch partial-failure path (it deletes the nodes it
already created — whose puts referenced the token — then de-allocates), and index
creation (funnels through the same leaf).

Fix: **registries are APPEND-ONLY across rollback when the change-log is ENABLED.**
The de-allocation chokepoints (`restoreRegistries` for tx, `restoreNewLabelsOnError`
/ `restoreNewRelTypeOnError` for standalone/batch/index) keep tx-allocated tokens
when `c.changeLogEnabled` — an allocated-but-unused token is harmless (it was
already persisted; `GetOrCreate` returns it again later), and keeping it makes
every emitted feed record resolvable, so replicas converge. When the log is OFF the
old exact-rollback behavior is preserved. The gate signal is NOT `changeFeed != nil`
(a badger/memory store ALWAYS implements the feed methods — that flag is true even
when the log is off); it is the `store.ChangeLogStatusCapability.ChangeLogEnabled()`
optional probe, captured into `c.changeLogEnabled` at `New`. The `getOrCreate*`
persist-failure rollbacks are deliberately NOT gated — they fire BEFORE any record
is emitted (a failed persist), and must de-allocate to keep in-memory == disk.

Residual (genuinely just contract, not a bug): the rolled-back ENTITY churn still
ships (create+delete records), so replicas converge but transiently materialize the
uncommitted entity, and the feed is not a logical committed-tx CDC source (asymmetric
with events, which discard on rollback). Eliminating that too would need a tx-level
change-log buffer mirroring `txEventBuffer` — a deliberate future design change.
Lesson: when a rolled-back/aborted operation feeds a replicated log, audit BOTH what
it emits AND what it RETRACTS from shared monotone state (the registry) — a retraction
of something already in the durable feed is the subtle, fatal half.

Completeness check (do this for every "gate the de-allocation" fix): there were SIX
`RollbackNames` sites. Only the TWO that can fire AFTER a record was emitted
(`restoreNewLabelsOnError` / `restoreNewRelTypeOnError`, the entity-failure
rollbacks) are gated. The other FOUR (`getOrCreate*` allocation/persist-failure
`fail` closures) fire DURING allocation, BEFORE any entity write — there is no
emitted record to orphan, and they MUST still de-allocate to keep in-memory ==
disk. Gating a pre-record site would be a bug (a token in RAM but not on disk).
Classify each de-allocation by "could a durable record already reference this
token?" — gate iff yes.

Accepted tradeoff (minor — do not overstate it): keeping tokens append-only leaves
an unused token after a rolled-back NEW-name allocation. But a token is per DISTINCT
NAME, not per request — once a name leaks a token, every later rolled-back-or-
committed use of that name REUSES it. So the accumulation is bounded by the number
of distinct schema names ever ATTEMPTED (dozens-to-thousands for a real schema,
against a 65535 space), not by traffic; exhausting the space needs 65535 distinct
names, which exhausts it with or without rollback. It is also symmetric and
consistent (primary and replica both carry the unused token — not divergence) and
fails CLOSED (registry warns at 60K, errors at 65535). For any stable schema it is
negligible.

The genuinely better solution (NOW IMPLEMENTED for the TX path): the leak is a
symptom of the change-log being a PHYSICAL redo log while a transaction is LOGICALLY
atomic. Make the change-log logical for txs — a per-tx log buffer that mirrors
`txEventBuffer`: buffer the tx's records, DISCARD on Rollback, EMIT on Commit. A
rolled-back tx then emits nothing, so the token can be safely de-allocated (no leak)
AND the other two residuals vanish (no transient phantom on replicas, feed becomes a
true committed-tx CDC source). Shipped as the optional `store.TxChangeLogScope`
capability (badger + memory) wired into `GraphTx` Begin/Commit/Rollback; LSNs are
minted AT commit (a rolled-back tx burns none → no feed gap); the tx
`restoreRegistries` reverted to EXACT de-allocation (the stopgap is gone for the tx
path). Two implementation lessons worth keeping:

- **Records are emitted IN-BACKEND, so the store cannot distinguish a tx call from a
  concurrent standalone call** (both hold the SHARED `c.mu.RLock` under Path B — and
  events dodge this only because they dispatch in the WRAPPER, not in-backend). A
  store-global "divert" flag would misroute a concurrent standalone's record into
  the tx's buffer. The fix is NOT threading a scope handle through ~18 mutation
  methods: it is to have the change-log-enabled tx take `c.mu.Lock` (EXCLUSIVE)
  PER-MUTATION (not per-lifetime — that re-creates the lesson-31 in-tx-read
  deadlock) and toggle the divert flag under it. The exclusive Lock excludes any
  concurrent standalone RLock, so the flag is true only when no standalone can emit.
  Reads stay on RLock (concurrent). Verified by a `-race` test that FAILS (records
  misrouted, feed under-counts) when the lock is reverted to RLock.
- **Co-commit (lesson 49) is preserved only at COMMIT, not during the body.** The
  accepted variant buffers RECORDS but lets the tx's DATA flow to pending and flush
  normally (so in-tx reads + concurrent standalone durability are untouched). A
  crash between a mid-tx data flush and CommitLogScope leaves committed-but-unlogged
  data — but it is invisible to the feed (the watermark never advanced for it) and
  no worse than a tx's pre-existing SyncWrites non-atomicity, so accept+document it.
  Fully closing it would need a staged-write-set tx (defer DATA too) — a far larger
  change rejected here.

Still on the stopgap (their token-poison is PREVENTED by the append-only skip, so
these are improvement-not-bug follow-ups, `tasks/backlog.md`): `Batch.Execute` and
`IO().Import` emit records eagerly (not through a scope), so their token-deallocation
chokepoints KEEP the append-only skip — reverting it there would re-poison the feed.
Import's `restoreRegistries` is now gated on `changeLogEnabled` (same as the others);
a read-only replica's bootstrap import has the log OFF so emits nothing anyway. The
batch is intricate to scope because it KEEPS successful ops on a partial failure
(not all-or-nothing), so it must commit its scope, and a failed op that emitted a
record must have its data cleaned up or the feed orphans a record → divergence; do
that wiring only with full batch-partial-failure understanding.

## 56. A Purge-And-Recreate Rollback Must Capture EVERY Door's Full Side-Effect Footprint — A Capture Branch That Forgets Adjacency Turns Rollback Into Data Loss

`ImportMerge`'s rollback does NOT invert each applied door surgically (label-index
rebuilds and cascades make that error-prone). Instead it PURGES every touched
entity (`DeleteRelationship` + `DeleteNodeCascade`, clearing all indexes) and
RE-CREATES it from a pre-merge snapshot through the CREATE doors. `DeleteNodeCascade`
deletes the node AND every edge attached to it — so the rollback can only be lossless
if `captureMergeRecord` snapshotted EVERY edge the cascade will drop, including edges
the delta itself never touched. The module even states this invariant in a comment:
"capture records every touched node's full adjacency — even edges the delta did not
change." A break-rounds campaign found the invariant was VIOLATED for two record
kinds: the `ChangeNodeHistoryVersion` and `ChangeNodeHistoryTruncate` capture
branches called only `captureNode(id)`, never `captureNodeAdjacency(id)` — unlike
`ChangeNodePut` / `ChangeNodeDelete`. Reachability is real and was already exercised
elsewhere: a `SetNodeVersionInterval` cascade on a bounded PAST interval leaves the
node's open-ended current row in place, so it emits ONLY bare
`ChangeNodeHistoryVersion` records — no `ChangeNodePut`, no rel record. A node whose
only window record is that bare history-version had its UNCHANGED edge cascade-deleted
during rollback's purge and never restored (it was not in `relOrder`): silent edge
data loss on a rolled-back merge.

Fix: both history branches now `captureNodeAdjacency(id)` too. Rules:
- **When rollback is "purge the entity + recreate from snapshot," the capture pass
  must record the TRANSITIVE closure the purge will destroy, not just the entity the
  record names.** `DeleteNodeCascade` destroys adjacency → capture adjacency, for
  EVERY record kind that can be the sole record touching a node.
- **Audit by symmetry across the type switch (rule 3).** The defect was
  one `switch` arm missing a call its sibling arms all make. Grep the capture switch:
  every node-touching arm must reach the same adjacency-capture as `ChangeNodePut`.
- **The corruption test must use a node whose ONLY record is the under-tested kind,
  and must ASSERT that precondition** (decode the change feed: a `ChangeNodePut` for
  the node would capture adjacency and mask the bug). A precondition assertion is what
  separates a real break-test from one that silently passes for the wrong reason.

## 57. A Multi-Buffer Overlay Must Resolve Set-vs-Delete PER KEY At EVERY Reader Door — A "Running Max" Reader Can't Be Un-Bumped By A Later Delete

The badger commit-window overlay has TWO buffers: `flushing` (rows mid-commit,
swapped out of `pending`) and `pending` (newer). `rangePending` visits flushing then
pending so a newer op wins — but only if each reader RESOLVES set-vs-delete per key.
`pendingHistoryIDOverlay` (behind `AllNodeHistoryIDs`) does: it tracks a `pendingSets`
id-set and, on a DELETE, `delete(pendingSets, id)` — so a flushing SET masked by a
pending DELETE drops out. `maxHistoryID` (behind `MaxNodeHistoryID`/`MaxRelHistoryID`)
did NOT: it computed a RUNNING MAX over SET keys and recorded DELETEs into a
`pendingDeletes` map that was applied ONLY to the persisted Badger reverse-scan. With
Badger empty, a flushing SET for id 500 bumped the running max to 500 and the pending
DELETE for that same key never un-bumped it — so `MaxNodeHistoryID` returned 500 while
`AllNodeHistoryIDs` returned none. Two doors onto the SAME overlay, contradicting each
other. (Ironic: the comment on `maxHistoryID` claimed it had closed exactly this
cross-door inconsistency — it added flushing VISIBILITY but not flushing
DELETE-MASKING.)

Root cause is the shape of the accumulator: a scalar running max is monotonic, so it
structurally CANNOT represent "this candidate was later deleted." The fix is to track
the surviving SET keys (a set), apply DELETEs to that set per key exactly like the
sibling overlay, THEN take the max. Rules:
- **When two reader doors share one overlay, they must share one resolution.** Prefer
  literally reusing the resolver (`pendingHistoryIDOverlay`) over a parallel
  hand-rolled scan; a parallel scan is where the two doors drift.
- **A running min/max/count over a multi-op buffer is a red flag.** A later DELETE/
  un-set for an already-counted key cannot retract a scalar accumulator. Collect the
  surviving keys, resolve set-vs-delete, then reduce.
- **Test cross-door AGREEMENT, not each door alone (rule 17 generalized).** The bug
  hid because every existing test asserted on `GetNodeHistory`/`GetNodeVersion` or on
  `All*` — never on `Max*` with a flushing-SET-masked-by-pending-DELETE and Badger
  empty. The pinning test asserts `Max*` and `All*` return consistent answers for the
  same overlay state.

## 58. A Contract That Spans A FAMILY Of Sibling Doors Must Land In EVERY Door — Audit The Family, Don't Patch One

A second break-rounds campaign found two defects that are the same shape: a
contract that should hold uniformly across a family of doors had drifted in one.

- **Trust-boundary error CLASSIFICATION.** The untrusted-stream contract is
  "every decode/validation rejection wraps `ErrCorruptExport`." The bootstrap
  import wraps it (`import.go` `WireToNodeChecked` → `ErrCorruptExport`), and the
  sibling `verifyImportedNodeHash` wraps it — but the SHARED replica-apply /
  delta-merge path (`applyChangeRecordLocked`'s doors) returned the
  `WireTo{Node,Rel}Checked` error BARE. So `errors.Is(err, ErrCorruptExport)`
  worked for a hash mismatch but not for a malformed wire (`id == 0`), through the
  same logical boundary. Fix: wrap at ALL SEVEN `WireTo*Checked` apply sites.
- **Name-admission POLICY.** A registry has three name doors — `GetOrCreate`
  (create), `ImportNames` (load), `AppendNames` (replica grow). The Label/RelType
  registries reject blank (all-whitespace) names in all three via `isBlankName`;
  the PropertyKeyRegistry rejected blank only in `AppendNames`, while `GetOrCreate`
  guarded merely `== ""`. So a blank key the create door minted was one the grow
  door refused. Fix: tighten `GetOrCreate` to `isBlankName` to match the grow door
  and the siblings.

Rules this yields:
- **Find the family, then grep it.** A door fix is suspect until you have listed
  its siblings (the other `WireTo*Checked` sites; the other name doors; the other
  registries) and confirmed the contract holds in each. The same `errors.Is`
  classification, the same validation predicate, the same ordering — applied
  everywhere or nowhere.
- **The fix DIRECTION is set by the existing shared test, not by the loosest
  door.** I first "fixed" the blank-name split by LOOSENING `AppendNames` — and
  the shared `TestAppendNames_RejectsMalformedSuffix` (which runs over all three
  registries) went red, revealing the codebase's encoded intent is REJECT-blank.
  A pre-existing parametric test across the family is the oracle for which way a
  divergence should be reconciled; let it veto the wrong direction.
- **Reconcile toward intent, but don't break load back-compat.** Tightening the
  CREATE door is safe (new data only) when the consuming layer degrades
  gracefully — the wire encoder ignores `GetOrCreate`'s error and falls back to
  the raw key on token 0, so rejecting a blank key costs nothing. Tightening the
  LOAD door (`ImportNames`) is NOT safe: a registry persisted before the guard may
  hold a blank token, so load stays lenient (strict-on-create, lenient-on-load is
  a deliberate, documented asymmetry — pin it so a later "tidy-up" can't turn it
  into a hard load failure for legacy data).
- **Make the un-expressible invariant expressible.** The flush-before-watermark
  ordering (the durable applied-LSN must never lead the durable data) was asserted
  only by code-reading ("not expressible at the graph façade"). A
  `Config.Store` decorator embedding a real backend and overriding ONE method
  (`Flush()` injects a one-shot failure; `MetaGet`/`MetaSet` forwarded so the
  watermark store is independent of the failed data flush) turns a
  previously-structural-only guarantee into an executable break-test.

## 59. A Privileged Override Of A System-Controlled Field Lands At The Shared Seam, Scoped To Where The Invariant It Relaxes Actually Binds

Two additive features (§4.1 transaction-time backfill, §4.2 named as-of tags) that
each turned a design constraint into a small, safe control surface. The reusable
patterns:

- **Relax a system invariant only where it binds — not everywhere the value is
  written.** `TxFrom` is system-stamped on EVERY mutation door (create, update,
  delete, cascade, label, CAS, version). The monotonicity invariant (lesson 20/46:
  "a correction recorded now is stamped now") binds the SUPERSESSION doors — an
  update/correction must carry a real, current TX time or the belief-state
  reconstruction goes wrong. It does NOT bind the CREATE doors: a first write of a
  never-before-seen entity can legitimately assert "the DB learned this at T" for a
  backdated T (that is exactly what the binary import path already did). So backfill
  is CREATE-only by design, and the audit found the split cleanly: the 6 create
  doors are exactly the 6 that call the shared `extractTemporal` helper; every other
  `TxFrom = now` site is a supersession write that must keep the clock. Before adding
  a caller override for a system-controlled field, enumerate its write sites and ask
  per-site "does the invariant I'm relaxing actually bind HERE?" — relax only those.

- **Land the feature at the ONE shared seam so the whole door family inherits it —
  the constructive form of lesson 58.** Lesson 58 said "a contract that spans a
  family must land in every door; audit the family." The cheapest way to satisfy
  that for a NEW capability is to add it at the seam the family already funnels
  through, not to touch each door. Extracting `tkg_tx_from` inside `extractTemporal`
  (shared by node Add/Import, the rel create kernel, and both batch-queue create
  paths) covered standalone + import + batch + tx in one edit; each door then only
  needs a 3-line gate call (`resolveBackfillTxFrom`) and to honor the override where
  it already stamps `TxFrom`. `AddWithTx` is a 3-line wrapper that injects the
  property and reuses `Add` — no second code path. When the family shares a kernel
  or an extraction helper, put the new bit THERE and the family-completeness
  obligation is discharged by construction.

- **Gate a privileged write with a Config flag; make the malformed-input error take
  precedence over the disabled-gate error.** `Config.AllowTxBackfill` (off by
  default) is the "audit flag, import scope" boundary — a deliberate, auditable
  opt-in on graph open, not a per-call permission. `resolveBackfillTxFrom` orders its
  checks `value==0 → none`, `value invalid → ErrInvalidTxFrom`, `!gate → ErrTxBackfillDisabled`:
  a malformed value is malformed regardless of privilege, so it wins. Test both
  orderings (invalid-value with gate ON *and* OFF both return the invalid-value
  sentinel).

- **A privileged override of a system-controlled value needs BOTH bounds — and the
  upper bound is the one adversarial review finds, because happy-path tests only
  exercise the "obvious" direction.** The first cut of the backfill guard checked
  only `txFrom > 0` (the lower bound — reject negative/unset). An adversarial pass
  (two independent lenses) caught the missing UPPER bound: a *future* `TxFrom` is
  incoherent (a knowledge/transaction time is "when the DB learned a fact" and
  cannot exceed wall-clock at write — the feature is *back*fill), and it silently
  corrupts bitemporal state — once the version is superseded, `Update` stamps the
  successor's `TxFrom = now < the future value` and the predecessor's `TxTo = now`,
  producing an INVERTED TX interval (`TxFrom > TxTo`) that is invisible to every
  AS-OF query yet passes `Verify*Chain` (TxFrom is not hashed) and replicates. The
  realistic trigger is a unit footgun (`Instant` is Unix-**milli**; the snowflake
  layer is **micro**second — `time.Now().UnixMicro()` is ~1000× too large → year
  58000). Every original test used a PAST instant, so the gap was invisible. Rule:
  when a door accepts a value that feeds a monotonic/ordered axis, bound it on BOTH
  sides against the system's own clock, and write at least one test per bound;
  "positive" is not "valid" for a time that must be ≤ now.

- **A non-hashed field is invisible to the hash/VerifyChain oracle — pin its
  reproduction by reading it directly (lesson 52, applied to tests).** `TxFrom` is
  not part of the integrity hash, so `VerifyNodeChain` passing proves nothing about
  whether the backfilled `TxFrom` round-tripped. The verbatim test re-fetches the
  stored row and asserts `Temporal().TxFrom == backfilled` exactly; the flagship test
  asserts the WHOLE point (`NodeAsOf(id, knowledgeTime)` sees the backfilled row while
  a no-backfill control returns `ErrNoVersionAsOf`). Mutation-verify by neutering the
  stamp override → both fail.

- **A durable-metadata registry's lifecycle must be made backend-consistent at the
  CORE layer when store `Clear` semantics diverge.** The `asof_tags` MetaKV entry
  surfaced a pre-existing split: badger's `Clear` scan-reaps all meta except the LSN
  watermark (lesson 53), but memory's `Clear` preserves the whole `metaKV`. Rather
  than redesign either store's `Clear`, `Admin().Reset()` reaps the tag registry
  explicitly (`MetaSet(asof_tags, nil)` under `asofMu`) so the contract ("a reset
  graph has no tags") holds on every backend. When a new durable meta key needs a
  specific lifecycle and the backends' reset paths disagree, enforce it once at the
  layer above the store, don't depend on each backend's `Clear` internals. Persisted
  meta is untrusted bytes: decode through `SafeUnmarshal` (fail closed on a corrupt
  blob), and serialize the read-modify-write with a dedicated mutex while keeping
  reads lock-free (MetaGet is an atomic snapshot).

- **Fail-closed on load means re-validating decoded VALUES against the write-door
  invariants, not just structural decode — especially when a value can alias a
  sentinel.** `SafeUnmarshal` proves the bytes decode into a well-formed
  `map[string]int64`; it proves nothing about whether the values satisfy what the
  write door (`tagAsOf`) enforces (`at > 0`, non-blank name). A corrupt disk row or
  foreign writer carrying `{"tag": 0}` decoded cleanly and `resolveAsOf` returned
  `(Instant(0), true, nil)` — and `Instant(0)` ALIASES the "no TX filter" sentinel,
  so a consumer's `AS OF SYSTEM TIME $tag` silently returned CURRENT belief instead
  of the named knowledge time (a silent wrong answer, not a crash — the most
  dangerous kind). Because the key is NEW (no legacy data can hold an invalid
  entry, unlike lesson 58's propkey blank-name where load stays lenient), the load
  door mirrors the write door EXACTLY and fails closed (`ErrCorruptWire`). Rule:
  when a decoded value can equal a control sentinel downstream, the load path must
  re-assert the write-door invariant on it; "it decoded" is not "it is valid," and
  the structural-corruption test (wrong msgpack shape) does not cover the
  value-level case.


## 60. A Retroactive In-Place Stamp Must Be Un-Applied At Every Belief-Reconstruction Door — Delete Is A Transaction-Time Tombstone

The hard-Delete door stamps `DeletedAt`/`ValidTo`/`TxTo` IN PLACE on the entity's
final version before moving it to history. That is a retroactive edit of a stored
row: a reader reconstructing the belief state at an earlier `txAt` sees stamps
that did not exist at that pin. Lesson 43 solved this for `TxTo` by weakening the
VISIBILITY predicate (`TxFrom <= txAt` only, "superseded ≠ retracted") — but the
delete-stamped `ValidTo` slips past visibility and kills the row in VALID-TIME
COVERAGE instead (`nodeVersionBounds` overrides `vEnd` with `ValidTo`). The result:
every generic `QueryOpts.TxAt` read (`ByLabel`/`ByType`/`All`, `NodesAtTx`,
`NodeAtTx`) silently dropped deleted entities at every pin, while the named as-of
door (`NodeAsOf`/`NodesAsOf`) — which normalizes post-pin stamps away via
`normalizeTemporalVisibleAtTxTime` — resurrected them correctly. Two doors, same
shape, divergent for months (rule 17); found from the CONSUMER side when
sigma-tkgd's Tyla `AS OF` pinning cross-probed the two doors (2026-07-03).

Rules:

- **When a mutation retroactively stamps a stored row, every belief-reconstruction
  path needs the matching un-apply.** Enumerate the fields the mutation writes
  (`DeletedAt`, `ValidTo`, `TxTo`), then enumerate every reader that reconstructs
  "as known at T" and check EACH stamp is disregarded when recorded after T.
  Visibility-predicate fixes (lesson 43) don't cover stamps consumed by a
  DIFFERENT axis (valid-time coverage vs tx-time visibility).
- **Land the un-apply at the seam the family funnels through** (constructive
  lesson 58/59 form): all four chain-based TxAt resolutions (`nodeAtLockedTx`,
  `relAtLockedTx`, `find{Node,Rel}VersionMatchingDuringTx`) share
  `filter{Node,Rel}ChainByTxAt` — normalizing there fixed every generic door in
  one edit, reusing the named door's own `normalizeTemporalVisibleAtTxTime` so
  the two doors cannot drift again.
- **Never normalize in place on chain rows — deep-copy first.** Chain rows can be
  shared frozen store rows, and `Temporal()` is the documented mutation-access
  exception that frozen entities do NOT protect (the v4.6.1 poisoning family).
- **Test-clock discipline for TxAt tests:** `Core.now()` has a monotonic ≥1ms
  floor, so a mutation burst outruns the wall clock — (a) a slept wall-clock pin
  can land BEFORE the last write's logical stamp (derive pins from the entities'
  own `TxFrom` instead), and (b) the TxAt-only door probes valid time at WALL now
  (`resolveOpenEndInstant`), so assertions flip while stamps are still "in the
  future" (wait until the wall clock passes every minted stamp before asserting).
  Both produced flakes in the first cut of `bitemporal_tombstone_test.go`.

## 61. A "Current Transaction Time" Reader Must Consult The Commit Clock (Wall-Dominated), Not The Session High-Water Mark — The Latter Resets To Zero On Reopen

> **Corrected by lesson 71.** The premise below — "`lastInstant` is deliberately
> not seeded on open, because `Core.now()`'s `observed = wall.now()` already
> dominates every historical stamp" — holds ONLY at throughput ≤ 1 write/ms. A
> burst whose monotonic floor outran the wall leaves persisted `TxFrom` stamps
> ABOVE the reopened wall clock, so `NowTx()=c.now()=wall` under-covers them.
> Lesson 71 seeds the floor on open (durable watermark) and advances it on every
> foreign-stamp ingress (apply / import).

`Temporal().NowTx()` must return an instant that covers every committed write and
precedes every future one, so a caller can pin an AS-OF read at it. The tempting
implementation — return the in-memory monotonic high-water mark
(`c.lastInstant.Load()`) — is a PURE read but WRONG across a Close/reopen: the
high-water mark is **session-local** (cross-session monotonicity rides the wall
clock, not a persisted watermark — `lastInstant` is deliberately not seeded on
open, because `Core.now()`'s `observed = wall.now()` already dominates every
historical stamp). So after reopening a graph that already holds data it returns 0.

And 0 is not an innocuous stale value: `QueryOpts.TxAt == 0` means "no TX filter",
so a pin of 0 resolves to CURRENT belief and silently INCLUDES writes made after
the pin was taken — the precise anachronism the pin exists to prevent.

The fix is to return `c.now()` (advancing the clock by one tick). It consults the
same monotonic-floor-over-wall-clock the mutation stamps use, so: (a) the wall
clock dominates every historical stamp, making the value correct on a fresh
session/reopen; (b) advancing RESERVES the instant, guaranteeing the next mutation
is stamped STRICTLY greater — a non-advancing peek can collide with a
same-millisecond future write, which would then leak past the pin. The "cost" — a
read that advances a monotonic counter — is harmless (no entity is stamped with
it) and is exactly what buys the strict, reopen-safe separation.

- **Rule:** a primitive that returns "the current logical time of a monotonic,
  wall-dominated clock" must READ THROUGH the clock (advancing when strict
  ordering against future writes is required), never a session-local cached
  high-water mark that resets on reopen — especially when the zero value of the
  returned type is an overloaded "no filter" sentinel downstream.
- **Test:** the reopen regression (`TestNowTx_ReopenSafe`, on-disk badger) is what
  pins the choice — it fails the instant someone "optimizes" `NowTx` back to a pure
  `lastInstant.Load()`.

## 62. An As-Of Resolver Must Not Fall Through Past A Retracted Newest Belief To An Older Open-TxTo Row — And "Newest" Is By Version, Not TxFrom

The named as-of door (`NodeAsOf`/`NodesAsOf` and mirrors) answers "the record
recorded-current at txTime". The chain-scanning resolvers (memory-native
`nodeAsOfLocked`, the core fallback used by tiered) selected the newest version
whose TX interval COVERED the pin (`TxFrom <= txAt && (TxTo == 0 || TxTo > txAt)`).
That coverage filter is the bug: the v4.9.0 append-only cascade
(`SetNodeVersionInterval`) demotes the prior current to history WITHOUT stamping
its `TxTo`, so a superseded genesis keeps `TxTo == 0`; a later hard Delete
tombstones only the FINAL version. At a pin AT/AFTER the delete the filter
EXCLUDED the retracted newest belief and fell through to the still-open genesis,
reporting the entity PRESENT — while badger's native reverse-scan stops at the
first (newest) recorded-by-then version and, finding it retracted, reports ABSENT.
Repro: `Add(valid_from=1000)` → `SetNodeVersionInterval(id, 2000, 0)` →
`Delete(id)` → `NodesAsOf(far-future)` was present on memory/tiered, absent on
badger. Badger is correct.

The deeper trap, surfaced by the generative oracle once the doors were being
cross-checked: "newest belief" is the highest **version** with `TxFrom <= txAt`,
NOT the highest `TxFrom`. An `Update` derives its stamp via
`validInstantAfter(now, versionStart)`, which can bump `TxFrom` ABOVE a LATER
cascade row's plain `c.now()` stamp — so a higher-version row can carry a lower
`TxFrom`. Selecting by `(TxFrom, version)` then disagrees with badger's
version-ordered reverse-scan on exactly that inversion. Version = allocation
order = authoritative recency.

Rules:

- **Select the decisive belief, then judge its retraction — never filter it out
  during selection.** The newest recorded-by-txAt version is decisive: if it is
  superseded or deleted by the pin (`TxTo != 0 && TxTo <= txAt`, or
  `DeletedAt != 0 && DeletedAt <= txAt`) the entity is ABSENT. A coverage filter
  that skips it and keeps scanning resurrects a stale open-`TxTo` row.
- **"Newest" in an append-only bitemporal chain is by version, not `TxFrom`.** A
  monotonic-looking `TxFrom` is not: `validInstantAfter` and the append-only
  cascade both break `TxFrom`↔version co-monotonicity. Mirror the store's own
  recency order (badger scans by descending version); a `(TxFrom, version)` proxy
  is wrong on the inversion.
- **Fix in the READ path only.** The append-only cascade discipline (never mutate
  stored rows; leave the demoted row's `TxTo == 0`) stays — align the resolver
  with the native reverse-scan, do not re-stamp on write.
- **Test:** `asof_cascade_delete_test.go` (all three backends — memory/badger/
  tiered — node+rel mirrors: absent at/after the delete pin, corrected shape
  between cascade and delete, original belief before it), plus an as-of clause in
  the bitemporal oracle harness cross-checking `NodesAsOf`/`RelsAsOf` against the
  version-ordered rule on both backends now that they agree.

## 63. An Unlocked Collect Window Before A Rebuild-Commit Needs A Write-Generation Guard; And A Rescan That Fetches Cache-Cold Rows Must Not Run Under The Caller-Held Write Lock

Two coupled defects on badger's `NodePropertyStats` deferred Min/Max rescan, both
about the SAME `sync.RWMutex` non-reentrancy fact seen from opposite sides.

**(a) The self-deadlock (found during development).** The obvious design — hold
`idxMu.Lock()` for the whole call, including the rescan — deadlocks: the rescan
fetches the label's current node bodies, and a cache-cold fetch
(`prefetchNodeScan` → `prefetchNodeNoFill`) takes `idxMu.RLock()` itself. A
`sync.RWMutex` is not reentrant, so a `Lock`-held goroutine re-`RLock`ing (with a
writer possibly queued) hangs. So the node-fetch loop MUST run with `idxMu`
released — the memory backend can hold one lock only because its lookups are
direct in-process map reads.

**(b) The stale-rescan-overwrite (found by an adversarial verifier).** Releasing
the lock for the collect opened a lost-update window: a concurrent `PutNode`
landing a NEW live extremum between the unlocked collect and the `Rescan` commit
was OVERWRITTEN by the stale snapshot, and because `Rescan` clears `dirty`, the
wrong EXACT value persisted forever (until an unrelated delete happened to
re-arm dirty). It is NOT a data race — every access is lock-ordered — so
`go test -race` never flags it; it is a logic/ordering bug that only a
deterministic interleaving through the real door reproduces (a test hook that
pauses between collect and commit, lands the extremum, resumes → returned Max=2
vs true 999999).

The fix is an optimistic-retry keyed on a per-accumulator **write generation**
bumped under the lock on every `Observe`/`Forget`
(`PropertyStatsAccumulator.WriteGen`): read the generation under the lock BEFORE
the collect, re-read it under the lock BEFORE committing; if it moved, discard
the stale values and redo the collect (bounded — lesson 24). On exhaustion,
return the LIVE snapshot WITHOUT committing a stale rescan and leave the pair
`dirty` so a later quiescent read reconciles (`Observe` keeps Min/Max
monotonically correct for additions, so the fallback never under-reports a live
extremum). Never "solve" exhaustion by holding the write lock across the collect
— that reintroduces defect (a).

- **Rule:** whenever a read releases a lock to gather inputs and then re-takes it
  to COMMIT a rebuilt cache/aggregate, guard the window with a write-generation
  counter bumped under the lock by every mutator — re-check it before committing
  and redo the gather if it moved. A committed rebuild from a stale gather is a
  silent lost update, invisible to the race detector. And never widen a lock to
  cover a sub-call that re-takes the same non-reentrant lock (RWMutex is not
  reentrant); the coarse-lock "fix" for a race can be a deadlock.
- **Test:** a deterministic collect→commit interleaving through the real public
  door (`TestBadgerStoreNodePropertyStatsStaleRescanOverwrite`, the red test),
  the bounded-retry exhaustion fallback
  (`TestBadgerStoreNodePropertyStatsRescanGenerationExhaustion`), and the
  concurrent-storm test upgraded to assert VALUE correctness against a
  sequentially-computed ground truth (not just no-error). The pre-existing
  concurrent test only checked "no deadlock / no error" and sailed straight past
  the overwrite.

## 64. A Scan-Then-Overlay Reader Drops A Row Across The Commit Window — Snapshot The Overlay BEFORE The Badger View, Not After

Lesson 54 said "every overlay reader must consult `flushing`." That is necessary
but NOT sufficient: a reader that consults `flushing` at the WRONG TIME still
drops rows. The badger full-history readers (`getNodeHistoryByPrefix` /
`getRelHistoryByPrefix`) did it in the wrong order — Badger `db.View` scan FIRST,
then `rangePending` (flushing ++ pending) merge SECOND — and lost an in-flight
version across `flush()`'s commit:

1. Reader opens `db.View` at snapshot Ts. A version parked in `flushing` (swapped
   out of `pending`, not yet committed) is NOT in that Badger snapshot.
2. A concurrent `flush()` commits the parked row to Badger and THEN clears
   `flushing` (flush order is: swap → `wb.Flush()` → `flushing = nil`, so a row is
   in `flushing` until strictly AFTER it lands in Badger).
3. Reader's `rangePending` runs at Tr > Ts, sees `flushing` already cleared and
   `pending` empty → no merge.
4. The row is in NEITHER view — the reader's older Badger snapshot (before the
   commit) nor the now-cleared overlay. **Dropped.**

It is load-dependent EXACTLY because the drop needs the flush's commit+clear to
land in the (Ts, Tr) gap: on an idle machine that gap is sub-microsecond and the
flush rarely lands in it (a prior session saw it 2/30); under heavy load the
flush goroutine is descheduled mid-window and the gap widens, so it fires. It is
NOT a data race — every buffer access is `wbMu`-locked — so `-race` does not flag
it; only a deterministic interleaving reproduces it.

The symptom shape at the public doors: two temporal doors that resolve through
the SAME per-ID function (`nodeAtLockedTx` / `relAtLockedTx`, which reads
`GetNode*History`) disagree (`Nodes.All(point)` vs `NodesAtTx`, `Rels.All` vs
`RelsAtTx`) because they call the reader at slightly different instants and one
catches the drop; and `NodeAtTx` returns `ErrNodeNotFound` for an entity whose
`History()` returned rows moments earlier — the resolver's internal history read
hit the window, the standalone `History()` did not.

**The fix is ORDERING, not just "consult flushing": snapshot the overlay (into a
local map) BEFORE opening the Badger `View`, then merge the local overlay
snapshot (which is strictly newer than any committed Badger state) over the scan
results.** Capturing the overlay at Ta and opening the View at Tb ≥ Ta closes the
window: a row committed after Ta was still in `flushing` at Ta (in the snapshot);
a row committed before Ta is durable and in the View. The already-correct
`*HistoryVersionsFromPrefix` readers (and `maxHistoryID`, the
`pendingHistory*Overlay`-backed ID scans) were ALREADY overlay-first — the bug was
the divergence between two members of the same reader family (lesson 54's
"divergence between readers is itself the bug class," now sharpened from WHICH
buffer to WHEN it is read).

Rules:
- **A cache-side overlay + a snapshot-isolated store are two clocks.** When a read
  combines a point-in-time store snapshot (Badger `View`, MVCC) with a mutable
  side buffer, read the MUTABLE buffer FIRST and the immutable snapshot SECOND.
  The reverse order lets a row slip out of the buffer and into a store state the
  snapshot predates.
- **Audit BOTH the readers and the delete-set-computing mutators.** The same
  scan-first order in retention/purge/repair mutators
  (`truncateHistoryByPrefix` / `trimHistoryFromPrefix`, on-disk
  `maintainPropertyIndexesPurge` / `purgePropertyKeyDiskEntriesLocked` /
  `incomingIndexEntriesFromKeyspaceLocked`) drops a key from the computed
  delete/retention set → an ORPHANED index entry or a distorted keepVersions
  window, not a wrong read but the same window. All were reordered overlay-first.
  For a mutator whose merge is "set-vs-delete wins" (the incoming-index scrub),
  overlay-first means the scan must only FILL keys the overlay did not resolve
  (else the scan resurrects a parked delete); for one that only appends delete
  ops (the property purges), a plain block-swap suffices (duplicate delete ops
  coalesce).
- **Entity POINT reads (`GetNode`/`GetRelationship`) are exempt for a real
  reason, and it is worth stating why they don't need this:** every entity write
  does `cache.Put` (dirty) BEFORE `appendOps`, and `markCacheFlushed` runs AFTER
  the commit, so a row is dirty-in-cache for the whole window it is in
  `flushing` — a cache HIT covers it, and a cache MISS means the row is clean =
  already durable in Badger. The invariant "entity in `flushing` ⇒ dirty in
  cache" is what makes the point-read cache-only path sound; the history/index
  keyspaces have no such synchronous cache shadow, which is why only they were
  exposed (the same "synchronous shadow for most consumers" observation as
  lesson 54).
- **Test the WINDOW, not just the presence.** The existing flushing tests parked
  rows into `flushing` and never committed — they proved "consult flushing" but
  could never catch the scan→commit→clear drop. The deterministic reproduction
  needs a hook that fires INSIDE the reader's scan→merge gap and lands a full
  flush commit there (`historyScanTestHook` + `commitFlushingToBadger`): red on
  the old order, green on the new. A full-stack oracle-harness run under a tiny
  flush interval is a useful -race guard but CANNOT deterministically hit a
  sub-microsecond window (it passed even with the bug present) — the in-reader
  hook is the real reproduction.

## 65. HyperLogLog Accuracy Tests (And Any Test Asserting An NDV Bound) Must Feed Well-Distributed Values, Never Short Sequential Integers

Writing the ADR-0005 §3.1 tiered cross-shard NDV fold test (two event shards,
50 distinct `int64` values each, 25-value overlap, true union 75), the first
draft used plain sequential values `0..74` and got `NDV == 7` — an order of
magnitude under the true 75, even though the register-max MERGE itself
(`HyperLogLog.Merge`) was correct (Min/Max/Count all folded exactly right;
only NDV was wrong). Isolating it (`AddString(fmt.Sprintf("%d", i))` for
`i := 0..n`) reproduced the undercount on a SINGLE unmerged sketch too:
n=1000 sequential decimal strings estimate ~90, n=10000 estimate ~862 — both
roughly 11x under. `AddString` hashes with FNV-1a (`hash/fnv`), and FNV-1a is
documented to avalanche poorly on short inputs that differ only in their
low-order byte — which is exactly what `"0"`, `"1"`, `"2"`, … `"9973"` are:
almost every consecutive pair differs by one ASCII digit, so their 64-bit FNV
digests correlate heavily in the TOP bits `HyperLogLog.addHash` uses for the
register index (`idx := hash >> (64-precision)`), collapsing thousands of
logically-distinct inputs into a handful of registers. Feeding the SAME
count of RANDOM strings (`rand`-suffixed) or sequential integers spaced by a
moderate PRIME step (`i*97`) estimates correctly (n=75 distinct → ~74-75
either way) — the sketch itself is fine; the input distribution was
adversarial for this specific hash choice.

This is NOT a new defect this WP introduced — `hyperloglog_test.go`'s own
accuracy regression (the "<5% at 10k, <3% at 100k" claim) already only feeds
RANDOM strings (`rand.NewPCG`-seeded), so it never exercised this input
shape; and the existing badger/memory `NodePropertyStats` concurrent tests
already use small sequential ints (worker-local `0..opsPerWorker-1`) but only
assert an UPPER bound on NDV (`got.NDV > everDistinct+8` fails, `got.NDV < 1`
fails — nothing catches an under-estimate), so they silently tolerate
whatever the true estimate happens to be. Nobody had asserted a LOWER bound
against sequential-int inputs before, so nobody had noticed.

Rule: **any test that asserts NDV is CLOSE TO a known true distinct count
(not just "positive" or "not absurdly high") must feed well-distributed
values** — real random strings, hashed/salted values, or integers spaced by a
prime step large enough to scatter their decimal encodings — never a tight
run of consecutive small integers. This is a test-authoring rule (documented
in `crossShardPropStatsStep`'s doc comment,
`pkg/graph/store/tiered/tieredstore_property_stats_test.go`), not a
production-code fix: the hash choice, its documented accuracy contract (only
claimed against random inputs), and the existing loose concurrent-test bounds
are all out of scope for the ADR-0005 §3.1 brief and are left as they are.
A future session tightening those concurrent tests' NDV bounds should feed
scattered values too, or the tightened bound will flake red for a reason that
has nothing to do with the thing under test.

## 66. A Pointer Swap Guarded By One Lock Class Races Every Reader Outside That Class — And "Targeted Race Tests" Before A Release Miss What The Full-Suite Storm Catches

`GraphTx.restoreRegistries` swaps the registry POINTERS (`c.labels`/`c.relTypes`)
under `c.mu.Lock` — which excludes every reader under `c.mu.RLock` (all normal
doors) but NOT the two readers that run outside `c.mu`: `Close`'s final
`persistRegistries` (after Close has already released `c.mu`) and the ingest
sessions' declare-on-prepare path (`registryMu` only). The v4.15.1 release CI
caught `Close`-vs-`Rollback` as a data race via `TestLifecycleStormCloseMidFlight`
under `-race` on a slow runner; the fast dev machine never reproduced it in 36
runs.

Fix pattern: a shared POINTER may only be swapped while holding EVERY lock class
its readers use — the registry swap sites (tx rollback, import rollback,
import-merge rollback) now hold `registryMu` in addition to `c.mu.Lock`, and
`Close`'s persist takes `registryMu`. Rules:

1. When adding a reader of shared state on a new lock class (or no lock), grep
   every WRITER of that state and prove each holds your class too. `grep -rn
   'c\.labels = \|c\.relTypes = ' pkg/` is the swap-site audit for registries.
2. The pre-release race gate is the FULL `make test-race`, never a targeted
   `-run` subset — the storm/lifecycle tests only run in the full suite, and CI's
   slower runners hit windows a fast dev box never opens. A release commit whose
   full race suite did not run locally is a release gambling on CI.

## 67. A Raw-Byte Storage-Saving Estimate Is Not A Disk-Saving Estimate — Validate Every Wire-Compaction Proposal Against BLOCK-Snappy, Not Per-Row And Not Uncompressed

The v3-redesign plan carried two on-disk-shrink levers, both justified by RAW
(uncompressed) byte estimates: B3 (delta-encode the 5 mid-map timestamps against
the snowflake-derived base, est. 7–15%) and B6 (anchor+delta history, est.
40–94%). Badger does NOT store raw wire bytes — it Snappy-compresses SSTable
*blocks* (default 4KB, `s2.EncodeSnappy`, `badger/table/builder.go`). A measured
gate (`wire_b3_ondisk_gate_test.go` / `wire_b6_history_gate_test.go`, block-Snappy
at 4KB in keyspace order) showed the two estimates diverge OPPOSITELY under real
compression:

- **B3: 2.99% raw → 1.13% post-Snappy.** Block-Snappy already dedups the
  low-entropy repeated timestamp bytes (many rows share `ca`/`ua`/base), so
  delta-encoding at the source reclaims almost nothing the compressor didn't. The
  high-entropy 64-hex hashes (~128 B/row, incompressible) dominate the compressed
  size and dilute the rest. **B3 dropped** — building it would have added the
  first custom decoder in the wire layer + an `fv` bump for a 1.13% disk win.
- **B6: 57.65% raw → 39.10% post-Snappy.** It elides WHOLE property structures
  (key + type tag + value) for every unchanged property on every non-anchor
  version. Even though the smaller delta rows compress *worse* individually
  (2.27× vs the full-snapshot 3.26×, less internal redundancy), their absolute
  size is far smaller. **B6 kept.**

Rules:
1. Any "shrink the wire" proposal states its saving as **post-block-Snappy over a
   realistic corpus in keyspace order**, never uncompressed and never per-row
   (per-row Snappy overstates cross-row wins the block compressor already gets;
   uncompressed overstates everything). History rows are keyed
   `0x07/<id>/<version>` — contiguous — so a repeated blob lands in one block and
   the compressor sees it; model that contiguity.
2. Removing low-entropy redundancy (timestamps, repeated small ints) is usually a
   mirage post-Snappy. Removing high-entropy or whole-structure bulk (large
   distinct values, entire unchanged property sub-trees) is what survives.
3. Keep the measurement as an executable decision record — a `_test.go` gate that
   re-runs the comparison — so a future revisit re-measures instead of re-guessing.

## 68. Reconstructed Wire Properties Must Be Re-Sorted By KEY STRING, Not Token — And A Cross-Backend Differential Oracle Is What Catches It

B6 anchor+delta reconstruction (`ApplyNodeHistory`) merges the anchor's and
delta's properties into a map and emits them sorted by the property-key IDENTITY
(the uint16 token, since a tokenized wire has `Key==""`). But the entity decoder
(`WireToNodeChecked`) VALIDATES that wire properties are in strict key-STRING
ascending order and REJECTS an unsorted row. Token order equals key-string order
ONLY when tokens were assigned alphabetically — which `SetProperty` happens to
produce (it pre-sorts by key), so a store-level test that built nodes via
`SetProperty` passed, hiding the bug. The core path tokenizes in
property-validation order (not alphabetical), so `Update`-built chains reconstruct
into `[region, blob, ...]` and the decoder rejects them: `property "blob" is not
in strict sorted order after "region"`.

Fix: reconstruction resolves `KeyToken→Key` (registry available in the badger
layer) and re-sorts by `Key` (`storeutil.SortWirePropertiesByKey`) before decode;
the truncate re-materialization path then re-tokenizes (`ApplyPropertyKeyTokens`)
before re-marshaling so a rewritten full row stays canonical. Rules:
1. Any code that REBUILDS a wire's property slice from parts (delta apply, merge,
   splice) must emit key-string order, not token/insertion order — the decoder
   validates order, it does not re-sort.
2. Token order is NOT key order. Never assume registry tokens are assigned
   alphabetically; that coincidence is an artifact of one construction path.
3. The bug was invisible to a same-backend test and only surfaced under a
   memory-vs-badger DIFFERENTIAL ORACLE running an identical workload through both
   backends. For any storage-representation change (delta, compaction, re-encode),
   the differential-oracle test — not just a round-trip on one backend — is the
   test that catches representation-specific corruption.

## 69. An Overlap-Based Candidate Prune Must NOT Be Applied To A Query Door Whose Predicate Set Includes NON-Overlapping Relations

The B4 valid-time envelope prune (`PruneTemporalCandidates`) drops a candidate
when its `[minFrom, maxTo]` envelope provably cannot OVERLAP the query interval.
That is sound for the `During` / point-in-time doors, whose match predicate IS
overlap. OPT10 added `NodesRelating(from, to, rels)` — an Allen-relation door
whose `rels` set may include `Before` / `After` / `Meets` / `MetBy`, which are
precisely the relations that hold when the two intervals do NOT overlap. Wiring
the same overlap-prune into the Relating door "for consistency" would silently
drop every `{Before}`/`{After}` match — a query for "entities that ended before
the window" would return empty. The prune is a candidate NARROWER keyed to one
predicate (overlap); it is not a general temporal accelerator.

Rules:
1. Before reusing a pruning/short-circuit built for one predicate on a new door,
   check whether the new door's predicate is a SUBSET of what the prune assumes.
   Overlap-prune is valid only where the match is overlap. An Allen door with any
   non-overlap relation in its set must take the full fold (or a prune keyed to
   the actual relation set).
2. Scope the wiring explicitly and say WHY at the call site: the Step-1 prune is
   wired into `nodesByLabelPropertyDuringLocked` with a comment that it is NOT
   applied to the Relating doors, so the next person does not "complete" the
   symmetry and reintroduce the bug.
3. The adversarial test must include the non-overlap relations (`{Before, After,
   Meets, MetBy}` as an exact-set assertion) — a happy-path `{Overlaps}`/`{During}`
   test would pass even with the incorrect prune, because those relations DO
   overlap. Rule 16's "must return the relations envelope-pruning would drop" is
   the specific assertion that fails closed here.
4. Open interval ends are +∞, not a wall-clock "now+": the Relating classifier
   (`types.RelateOpen`) passes the query end RAW (0 = open) so Before/After/Meets
   classify against the true open edge. Pre-resolving an open end to a concrete
   bound (as the overlap `During` door does) would move the Before/After boundary
   and misclassify — another reason the two doors cannot share the open-end
   handling.

## 70. `reflect`-Based Sorting/Comparison On A Hot Path Is A Perf Bug — But Not All Reflection Is

BACKLOG 16f found `hnsw.go`'s `connect()` calling `sort.Slice` (Go's
reflection-boxed sort, invoked via a `reflect.Value` swap under the hood) on
the hottest HNSW construction path — every neighbor-cap overflow during
`insert()`. `lru.go` already carried an explicit comment warning against this
exact pattern ("reflection sorting showed up in ingestion profiles"), so the
convention existed but wasn't consistently applied. Fixed by swapping to
`slices.SortFunc` (generic, no reflection) with a comparator that mirrors the
original tie-break exactly.

Rule: **no `reflect`-mediated operation (`sort.Slice`, `reflect.DeepEqual` used
as a substitute for a typed `Equal`, `reflect.Value` field walks, etc.) on a
per-write or per-query hot path** — anywhere a call happens once per node/rel
write, once per property, or once per search/insert step. Prefer the generic
(`slices`/`cmp`/`maps`) or hand-written typed equivalent; see `ValueStripe`'s
inlined FNV-1a (BACKLOG 15g) and `decodeMapKeyLen`'s hand-written msgpack
decoders for the same discipline applied to hashing and decoding.

This is NOT a blanket "never use `reflect` anywhere" rule. Reflection remains
the CORRECT tool, used deliberately, in several places that are either
(a) inherently reflection-shaped problems — `pkg/types` property
validation/deep-copy/equality over an arbitrary caller-supplied `any` value,
where the concrete type is unknowable at compile time and reflection is what
lets one recursive function validate every allowed shape instead of a
combinatorial type-switch per call site — or (b) COLD paths: custom
property-type registration (`RegisterPropertyStructType`, once per type, not
per value), and store-capability wrapper-detection at `Core` construction
time (`embedsNativeCapability` and friends in `core.go` — once per `New()`
call, not per query). The dividing line is call FREQUENCY relative to the
write/query hot path, not "does this identifier contain `reflect`".

CORRECTION (same session, user follow-up): the first audit pass only grepped
for OUR OWN direct `reflect.*` calls (`import "reflect"`) and missed the
broader version of this same bug class — a THIRD-PARTY library call that
internally falls back to reflection because the target Go type doesn't
implement that library's fast-path interface. `msgpack.Marshal`/`Unmarshal`
is reflection-based UNLESS the target type implements `msgpack.CustomEncoder`/
`CustomDecoder` — `NodeWire`/`RelWire`/`PropertyWire` do (hand-written, see
`wire_encode.go`/`wire_decode.go`), but the 10 change-log body WRAPPER types
that embed them (`NodePutBody`, `RelPutBody`, etc., BACKLOG 15s) and the
`HistoryDeltaEncoding` delta wrapper types (`NodeHistoryDelta`/
`RelHistoryDelta`, BACKLOG 15t) do NOT — so `msgpack.Marshal(body)` on any of
them reflects over the OUTER wrapper's fields even though the embedded
`Wire NodeWire` field, once reflection reaches it, still dispatches to the
fast custom encoder. Rule, extended: auditing "no reflection on a hot path"
means checking not just `grep '"reflect"'` in your own imports, but also
"does every type that crosses a reflection-capable third-party
Marshal/Unmarshal/Encode/Decode call on a hot path implement that library's
opt-out interface" — a struct embedding an already-optimized field is NOT
automatically itself optimized.

## 71. A Wall-Dominated Monotonic Floor Is Not Reopen-Safe Once A Burst Outran The Wall — Seed It On Open And Advance It On Every Foreign-Stamp Ingress

Lesson 61 made `NowTx()` return `c.now()` on the premise that the wall clock
"dominates every historical stamp", so the session-local floor (`lastInstant`)
need not be persisted or reseeded on open. That premise is false whenever
throughput exceeds **1 write/ms**. `Core.now()`'s floor is `next = max(wall,
last+1)`, so a burst (a bulk import, a tight write loop) stamps `TxFrom` at
`wall, wall+1, …, wall+k` — up to `(writes − elapsed_ms)` **above** the wall. The
inflated stamps are real, committed, persisted. Two ingress paths then let a
persisted/foreign `TxFrom` exceed *this* session's wall clock:

1. **Reopen.** `lastInstant` resets to 0 and is not reseeded. If a pre-close
   burst left persisted `TxFrom` above the reopened wall, `NowTx()=c.now()=wall`
   under-covers them — and worse, a **new** write is stamped `c.now()=wall`, which
   is BELOW an already-committed `TxFrom`: transaction time goes *backwards* across
   the reopen. Silent (`TxFrom` is not in the integrity hash, so `Verify*Chain`
   passes; the AS-OF door just quietly drops the burst at a `NowTx` pin).
2. **Replica apply / bootstrap import.** The row's `TxFrom` is the **primary's**
   stamp, reproduced verbatim (never re-minted). Under NTP skew (replica trailing
   the primary) or the primary's own burst floor, it exceeds the replica's wall, so
   `NowTx()` on the replica under-covers already-applied/imported data.

Root cause: `lastInstant` is advanced by **exactly one writer** — `c.now()`. Every
`TxFrom` that enters the store by another door (reload on open, apply, import)
escapes the floor. The fix closes all three doors: **(a)** persist the floor as a
durable MetaKV watermark on `Close` and reseed it on open (`seedInstantFloor` /
`persistInstantFloor`); **(b)** advance the floor to cover every applied record's
stamp at the ONE central apply seam (`recordCommitStamp` in the
`applyChangeRecordLocked` wrapper — so replica tail AND delta-merge are covered);
**(c)** advance it per replayed wire on `Import`. Only the **system-minted** TX
stamps pull the floor (`maxCommitStamp` = max of `TxFrom/TxTo/UpdatedAt/DeletedAt`)
— NEVER `ValidFrom/ValidTo`, which are caller-asserted world time and may
legitimately lie in the future (a future valid-to must not poison the commit clock).

- **Rule:** a monotonic logical clock whose value must be correct **across
  sessions / across nodes** cannot lean on "the wall dominates every stamp" — that
  fails the moment the in-session floor legitimately outran the wall. Persist a
  high-water watermark and reseed it on open, AND advance the floor at every door
  that admits a foreign or reloaded stamp, not just the local mutation path.
  Corollary: only the fields the clock itself mints may raise the floor.
- **Caveat (documented, not a regression):** persist-on-close restores the floor
  EXACTLY on a clean shutdown; after an unclean crash the watermark is stale/absent
  and the floor falls back to the wall clock, self-healing as the wall advances past
  the drift — the pre-fix behavior.
- **Corollary — "every door" is a MOVING set, so re-audit it on every rebase.**
  This lesson was written against v4.11.2 and landed on v4.24.0, and the base had
  grown two more doors in between, each of which the fix silently missed until
  re-audited:
  1. **`ChangeForeignIncoming`** (change tag 11, ADR-0010 Model A — did not exist
     at v4.11.2, which stopped at tag 10). It is the cross-machine incoming
     half-edge stub, and its body IS a `ChangeRelPut` body carrying the
     ORIGINATING partition's `TxFrom` verbatim — a textbook foreign stamp. The
     tag-driven `recordCommitStamp` switch returned 0 for it, skipping the
     advance. The remaining unhandled tags are now enumerated with a reason each,
     so the next added tag is a deliberate decision rather than a silent 0.
  2. **`Admin.Reset()` / `ChangeClear`**, which wipe the whole MetaKV keyspace and
     would take the durable watermark with them. Classified `reapPolicyPreserve`
     (BACKLOG 13l): the commit clock is this NODE'S transaction-time position, not
     a description of the erased entities — the same rationale as `idSlotLeaseMeta`
     and the opposite of every Reap key. Uniquely among Preserve keys it needs no
     capture-before-Clear, because the authoritative value is the in-memory
     `lastInstant` (which `Clear` never lowers), not the persisted blob.
- **Tests:** `TestNowTx_ReopenAfterBurst_MonotonicFloorAnachronism` (frozen-clock
  burst → reopen), `TestNowTx_ReplicaCoversAppliedFutureTxFrom` (clock-skewed
  primary → apply), `TestNowTx_BootstrapImportCoversFutureTxFrom` (future-stamped
  snapshot → import), `TestRecordCommitStamp_CoversForeignIncomingStub` (a real
  encoded rel-put record RE-TAGGED, which also proves the "same body" premise), and
  `TestInstantFloor_PreservedAcrossReset` (badger only — the memory store's `Clear`
  leaves its meta map intact, so it cannot exhibit the defect). Each fails RED
  without its door's fix.

## 72. An Externally-Filed Perf Diagnosis Names A Plausible Mechanism — Reproduce And Profile Before Touching The Named Component

The sigma-tkgd rel head-pin ask (2026-07-29) reported a real ~8× depth
degradation and attributed it to `RelBeliefWatermarkCapability` "not consulted
on the `OutgoingForNodesAtPin` path" — a plausible hypothesis from outside the
codebase, since the 10c watermark IS the known head-pin accelerator. Code
reading alone already contradicted it (the as-of doors have their own
current-row fast path, and the watermark was never part of the as-of seam), and
a local repro benchmark + CPU profile located the actual gate somewhere the ask
never mentioned: `ForEachDeletedRelID` → `AllRelHistoryIDsFrom` stepping the
badger iterator through every `0x08` history row to enumerate distinct IDs —
the whole-store deleted-rel candidate fold, O(total version rows) per query.

- **Rule:** when a consumer files a perf ask with a numbered symptom AND a
  named culprit, treat only the SYMPTOM as data. Reproduce it in-repo (mirror
  the reporter's fixture shape), profile, and let the profile name the
  component. Fixing the named-but-innocent component would have added a
  watermark gate that changed nothing and left the real O(rows) scan behind.
- **Corollary (profiling under async write buffers):** a benchmark that
  measures reads immediately after seeding writes measures the PENDING-BUFFER
  overlay (`rangePending`) as much as the read path — drain the flush tick
  (sleep past `FlushInterval`) before `b.ResetTimer()` or the profile blames
  the wrong frames. Both costs were real here, but only one was the reported
  steady-state defect.
- **Fix pattern (distinct-ID scans over versioned keyspaces):** enumerating
  distinct entity IDs from a `prefix/<id>/<version>` keyspace by `it.Next()`
  + last-ID dedup is O(total rows); once an id is decided, `Seek` to
  `prefix/<id+1>` — O(distinct ids). Keep row-wise stepping for a
  pending-delete-masked row (the same id may still be emitted via a surviving
  row — regression test
  `TestBadgerStore_AllNodeHistoryIDsFrom_PendingDeleteMasksSomeVersions`).

## 73. The Chain Resolver's Input Order Is A Semantic Input — A Sorted Chain Fed Back In Flips The Monotonic-vs-Cascade Branch

Discovered building the selection-skeleton fast path (TemporalMetaHistoryCapability):
`resolveNodeChain`/`resolveRelChain` take the chain in ASCENDING-VERSION order and
`sortNodeChainForResolve` both DETECTS non-monotonicity relative to that order AND
sorts the slice IN PLACE. Resolving twice on the same slice (select on skeletons,
hydrate the winner, re-resolve) made the second run see the first run's
sorted-by-valid-from output — which looks MONOTONIC — so it took the positional-
tiling arm instead of the cascade own-bounds arm and silently lost a
cascade-reopened row (fast path returned "no version" where the full path returned
the reopened row). The per-row selection inputs were byte-identical; only the ORDER
differed.

- **Rule:** when a resolver both classifies its input's shape AND mutates the input
  in place, the input's order is part of the API contract. A caller that invokes it
  more than once must hand EACH invocation its own pristine copy — and the contract
  belongs in the resolver's doc comment (now in chain_resolver.go's funnel comment),
  not in the caller's head.
- **Debugging pattern that found it:** an env-gated divergence cross-check inside
  the door (run fast AND full, panic with both chains + selection keys on
  divergence) under the existing randomized oracle harness. The harness found the
  divergent lifecycle in seconds; targeted-fixture guessing had failed to reproduce
  it because the trigger was the RE-USE of a sorted slice, not any particular row
  layout.
- **Corollary:** an "accelerator must never change answers" claim needs an
  equivalence oracle wired into CI, not just targeted fixtures — the divergent
  input class here (created-closed + cascade-reopen + post-cascade-update +
  delete) was nothing anyone would have hand-written.

## 74. "Back-To-Back, So The Window Is Negligible" Is A Probability Argument, Not A Correctness Argument — Every Overlay Reader Must Capture Before The Snapshot, Structurally

One full `make check` run (under heavy parallel load AND a nearly-full disk from
a 65GB go-build cache) failed `TestBitemporalOracle_BadgerCommitWindow` with a
genuine MISMATCH — a rel vanishing from `ByType`/`*AtTx` doors — that then
refused to reproduce in 60 targeted stress runs. Re-auditing EVERY
overlay-merging reader's capture ordering (instead of chasing the
reproduction) found that the single-entity as-of reverse walk
(`reverseScanHistoryVersion`) captured the pending/flushing overlay INSIDE
`db.View` — i.e. AFTER badger assigned the transaction's snapshot instant —
while its own comment argued this was safe "because both happen back-to-back
under the SAME call". That is the exact lesson-64 dropped-row window: a
concurrent flush committing a parked row and clearing `flushing` between the
snapshot instant and the overlay read leaves the row in NEITHER view; the gap
is nanoseconds on an idle machine and arbitrarily wide when the goroutine is
descheduled under load. The bulk variant (`...InTxnSnapshot`) already
pre-captured for precisely this reason — the single-entity path had argued
itself an exemption.

- **Rule:** an ordering invariant ("overlay before snapshot") admits no
  fast-path exemptions justified by expected timing. If the safety of a read
  path depends on two operations happening "immediately" after one another,
  it is not safe — make the ordering structural and delete the argument.
- **Rule (audit recipe):** when a rare, load-dependent oracle failure will not
  reproduce, stop rerunning and instead enumerate every reader that merges
  the write-buffer overlay with a store snapshot and CHECK THE CAPTURE ORDER
  of each: `grep -rn 'pendingHistoryVersionOverlay\|pendingHistoryIDOverlay\|snapshotHistoryOverlay' pkg/graph/store/badger/ | grep -v _test`
  — each hit must capture BEFORE its `db.View`/`NewTransaction`, or receive a
  pre-captured snapshot.
- **Honesty note:** the observed mismatch was on point/set doors while the
  flawed ordering was in the as-of walk, so the attribution is PLAUSIBLE, not
  proven (the artifact is preserved in the failure log recorded in this
  repo's CHANGELOG entry). The fix is justified by inspection regardless; the
  observation stays flagged rather than silently explained away.
