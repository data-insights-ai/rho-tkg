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
