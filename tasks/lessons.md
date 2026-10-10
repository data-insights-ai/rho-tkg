# Lessons - tkg/v4

Actionable rules distilled from real bugs. Keep this file short. Add a new
entry only when a fresh issue exposes a reusable pattern that is not already
covered here. Entry shape: Trigger, Rule, Detector (grep/test), Evidence.
Numbers and the `## N. Title` headings are cited from code, docs, tests and the
backlog ("lesson N"): never renumber, never reuse a number.

## Audit Commands

Use these after touching the named subsystem:

```bash
rg -n 'ComputeNodeHash|ComputeRelHash' pkg/
rg -n 'store\.(Put|Delete|Replace)' pkg/graph/
rg -n '== 0|< 0|<= 0' pkg/graph pkg/types
rg -n 'ForEach.*ID|All.*IDs|mergeIDSlices' pkg/graph/store pkg/graph/internal/core
```

## Index

Areas: temporal, persistence, concurrency, testing, process, docs, consumers.

| # | Rule | Area | Status |
|---|------|------|--------|
| 1 | Zero/negative IDs are malformed: shared validators before every lookup, scan, no-op | persistence | active |
| 2 | Hash canonical (resolved) state; refresh endpoint hashes from locked endpoints | persistence | active |
| 3 | Take entity locks before any Store write | concurrency | active |
| 4 | Multi-row ops: preflight (no writes), then apply | persistence | active |
| 5 | Cross-shard moves roll back the destination on later failure | persistence | active |
| 6 | Store is the trust boundary: deep-copy, validate, treat MsgPack as untrusted | persistence | active |
| 7 | Temporal/diff/integrity queries merge current and history rows | temporal | active |
| 8 | Callback/paged scans outside locks; pin every Tiered handle; bounded fanout | persistence | active |
| 9 | RWMutex is not reentrant: use `*Locked` helpers inside locked code | concurrency | active |
| 10 | Query-relevant state needs a durable definition and a startup rebuild; fail closed | persistence | active |
| 11 | Index build: placeholder, snapshot build, install only if not mutated | concurrency | active |
| 12 | Skip only the sentinel the operation tolerates | persistence | active |
| 13 | Version identity is the version number, not slice position | temporal | active |
| 14 | Every public method gets a direct test | testing | active |
| 15 | Perf tests need production shapes | testing | active |
| 16 | Lazy-init flags are atomic or under an exclusive lock | concurrency | active |
| 17 | Rollback logs key on pre-transaction existence | persistence | active |
| 18 | Map-backed query results are sorted by ID | persistence | active |
| 19 | Native fast paths only for exact in-tree stores | persistence | active |
| 20 | Stamp transaction time with `c.now()`, never wall clock | temporal | active |
| 21 | Batch publish order must hold under partial observation | concurrency | active |
| 22 | Peek-then-lock revalidates identity under the locks | concurrency | active |
| 23 | Property-index keys follow equality semantics (exact float bits) | persistence | active |
| 24 | Every retry loop has a ceiling, mirrored across symmetric methods | concurrency | active |
| 25 | CAS equality and integrity hash are different contracts (NaN, -0) | persistence | active |
| 26 | Each sub-API declares its own minimal `Ops` | consumers | active |
| 27 | Fix now, do not ship "known limitation" notes | process | active |
| 28 | Split a shared helper before optimizing one caller (deleted-only adjacency fold) | temporal | active |
| 29 | PublishBatch priority order needs a per-batch ceiling | concurrency | active |
| 30 | Split files by responsibility, not by line count | process | active |
| 31 | Tx mirrors of read accessors (tx held `c.mu.Lock`) | concurrency | superseded (Path B, v4.1.0) |
| 32 | Valid-time end comes from `next.ValidFrom`, not `UpdatedAt` | temporal | active |
| 33 | Update clears inherited ValidFrom/ValidTo | temporal | active |
| 34 | `TxAt == 0` is "no filter"; bitemporal reads set `TxAt` | temporal | active |
| 35 | Cascade edit rewrote history in place | temporal | superseded by 46 |
| 36 | Keep migration-gated heuristics behind a per-core flag | temporal | active |
| 37 | Full token registry returns token 0, never fails writes | persistence | active |
| 38 | Side indexes hook into Core validation | persistence | active |
| 39 | Custom `EncodeMsgpack` must emit every new tagged field | persistence | active |
| 40 | Load the registry before the index rebuild | persistence | active |
| 41 | A version bump updates every doc-consistency target in the same commit | docs | active |
| 42 | Every deep-copy mutation door clears inherited valid time (cross-door test) | temporal | active |
| 43 | `TxTo` never bounds valid-time answerability (visible iff `TxFrom <= txAt`) | temporal | active |
| 44 | Verify imported hashes and chains on entry; classify as `ErrCorruptExport` | persistence | active |
| 45 | Shrinking a bound needs migration; dependency `os.Exit` is detected before open | persistence | active |
| 46 | A correction appends new rows; stored rows are immutable | temporal | active |
| 47 | Decode only via `SafeUnmarshal` (depth guard, then recover) | persistence | active |
| 48 | Allocate by bytes delivered, never by an untrusted size/count | persistence | active |
| 49 | Change-log lives in-backend, co-committed; never a Store decorator | persistence | active |
| 50 | Replica apply reproduces rows verbatim; provenance bit; flush before watermark | persistence | active |
| 51 | Registry sync: LSN before names, prefix-guarded append, persist before use | persistence | active |
| 52 | Deltas reuse the op-log; hash oracle misses non-hashed fields; chain is a DAG | persistence | active |
| 53 | A reset keeps the durable watermark continuously present, in every door | persistence | active |
| 54 | Commit-window buffer: clear on success, consult in every reader | concurrency | active |
| 55 | Change-log is a physical redo log; registries append-only where eager | persistence | superseded for GraphTx (scoped log); active for Batch/Import |
| 56 | Purge-and-recreate rollback captures adjacency for every record kind | persistence | active |
| 57 | Overlay readers resolve set-vs-delete per key; no running max | concurrency | active |
| 58 | A contract over a door family lands in every door | process | active |
| 59 | Privileged overrides land at the shared seam, scoped where the invariant binds | temporal | active |
| 60 | Retroactive in-place stamps are un-applied at every belief-reconstruction door | temporal | active |
| 61 | `NowTx` reads through `c.now()`, not the session high-water mark | temporal | stub, corrected by 71 |
| 62 | As-of resolvers judge the newest-by-version belief; never fall through | temporal | active |
| 63 | Write-generation guard for unlocked collect then commit; no rescan under held lock | concurrency | active |
| 64 | Snapshot the overlay BEFORE the Badger View; publish in reverse of reader order | concurrency | active |
| 65 | HLL/NDV bound tests need well-distributed values | testing | active |
| 66 | Swap shared pointers under every lock class; full `make test-race` before release | concurrency | active |
| 67 | Judge wire-compaction by block-Snappy, not raw bytes | persistence | active |
| 68 | Rebuilt wire properties sort by key string; use a cross-backend oracle | persistence | active |
| 69 | Overlap prune only on overlap predicates, never Allen doors | temporal | active |
| 70 | No reflection on hot paths (incl. third-party fallbacks) | persistence | active |
| 71 | Seed the commit-clock floor on open; advance it at every foreign-stamp door | temporal | active |
| 72 | Reproduce and profile an external perf diagnosis before touching the named part | consumers | active |
| 73 | Resolver input order is semantic; pass a pristine copy per call | temporal | active |
| 74 | Overlay capture before snapshot is structural, never timing-justified | concurrency | active |
| 75 | A green workflow means its gates passed | process | active |
| 76 | Codec tests cover every kind at depth through the real bytes | testing | active |
| 77 | Handover test lists are break-the-code only | testing | stub, owner 78 |
| 78 | Clean root-cause fix and a red break-the-code test first | process | active |
| 79 | Batch patch releases, author-run gates, one review round, short briefs | process | active |

## 1. Validate At Every Boundary

Trigger: a store/graph entry point looks up, scans or no-ops on an entity ID.
Rule: zero and negative IDs are malformed (except where an API defines `0` as cursor / unset / "all
types"). Every store mutation, direct read, history read, graph mutation target, Badger split helper
and repair helper calls the shared validator (`store.ValidateNodeID`) before lookup, scan or no-op.
BAD: `if id == 0 {...}; lookup(id)` (negatives fall through).

## 2. Hash Canonical State

Trigger: any `ComputeNodeHash` / `ComputeRelHash` call site.
Rule: hash the canonical state after token normalization and registry resolution
(`resolvedCanonicalLabels`, never raw labels). Fixing one call site means grepping all of them. A
relationship mutation that recomputes integrity refreshes `FromNodeHash` / `ToNodeHash` from locked
current endpoints before persisting.
Detector: `rg -n 'ComputeNodeHash|ComputeRelHash' pkg/`.

## 3. Lock Before Store Mutation

Trigger: a graph mutation that writes to the Store.
Rule: take the entity locks (`g.entityLocks.LockEntity(id)` + deferred unlock) before the Store
write. Transactions and batches call internal lock-free helpers only while holding the tx/batch
graph lock.

## 4. Preflight Then Apply

Trigger: a multi-row Store operation.
Rule: separate validation/read gathering (`infos := preflight(ids)`, no writes) from the apply
phase. If an apply step can still fail, add rollback or return per-operation errors explicitly. BAD:
delete-in-loop (partial mutation on mid-loop error).

## 5. Multi-Shard Moves Need Rollback

Trigger: Tiered archive/restore, migration, cross-shard relationship moves.
Rule: a later failing step (e.g. `src.DeleteNode`) must roll back the destination writes; no
duplicates or orphaned split indexes. Repair tools are not a substitute for transactional cleanup.

## 6. Store Is The Trust Boundary

Trigger: Store put/get paths; Badger wire decode.
Rule: deep-copy entities on put/get (`cache[id] = p.DeepCopy()`) and validate payload invariants.
Treat MsgPack as untrusted: reject key/value ID mismatches, invalid tokens, invalid temporal ranges,
malformed properties and counter/index metadata corruption before constructing live entities.
No-error defensive-copy fallbacks must not panic on legacy/bypassed shapes: typed zero values for
nil reflect elements, no unassignable writes.

## 7. Deleted Entities Still Matter

Trigger: temporal, tx-time, diff, snapshot, integrity queries.
Rule: merge current and history rows (`mergeCurrentAndHistoryIDs()`, not `AllNodeIDs()` alone, or
deleted historical nodes vanish). Verification tolerates current-row deletion when valid history
exists.

## 8. Iterate Without Materializing Everything

Trigger: large scans; Tiered shard scans.
Rule:
- Use callback iterators (`ForEachNodeID`) or paged scans, not `AllNodeIDs` + `mergeIDSlices`.
  Callbacks run outside backend locks and Tiered shard checkouts so callers may use Store methods
  inside them.
- Tiered scans that dereference shard stores pin every handle `Close` can close: event shards
  `checkoutStore`, the reference archive `checkoutArchive`, and the reference shard the same checked
  lifecycle even though idle-close never touches it.
- Wide reads bound handle lifetime: group cheaply, then checkout/read/release one shard batch at a
  time, never pin every cold owner up front.
- Fanout helpers use bounded worker pools, not one goroutine per shard behind a semaphore.

## 9. sync.RWMutex Is Not Reentrant

Trigger: graph internals already holding `g.mu` / `c.mu`.
Rule: never call exported graph methods from locked internals (BAD
`g.mu.RLock(); g.Nodes().Get(...)`); use unexported `*Locked` / `*Unlocked` helpers. Load-bearing in
v3.4/v4.0.x when `BeginTx` held `c.mu.Lock` for the tx lifetime (see superseded 31); Path B (v4.1.0)
removed that, but the rule still applies in rollback, batch execute, snapshot, export.

## 10. Persistence Must Rebuild In-Memory State

Trigger: anything affecting query correctness that outlives a process.
Rule: persist a durable definition and rebuild at startup (`CreateIndex` persists, `loadIndexes`
rebuilds). Counters and registry metadata fail closed when corrupt or impossible. Persistent
backends mark close-in-progress before stopping flush workers or taking the final flush snapshot, so
public ops fail closed once shutdown begins.

## 11. Index Creation Is A Three-Phase Operation

Trigger: building an index while writes run.
Rule: install an empty placeholder first (concurrent writes see it before build I/O), build from a
snapshot, install only if not mutated. Final install uses dirty/mutated tracking, never "does the
built index already contain this ID?".

## 12. Sentinel Errors Must Be Precise

Trigger: `if err != nil { continue }`.
Rule: skip only the sentinel the operation tolerates (`errors.Is(err, ErrRelNotFound)`); I/O,
corruption, lifecycle and invalid-input errors surface.

## 13. Version Identity Is Not Slice Position

Trigger: `if i == 0` over history.
Rule: history can be truncated; use `entry.Version()` and entity IDs as identity, never array
positions.

## 14. Public API Coverage Is Direct

Rule: every public method has a direct test (delegation coverage is not enough). Node/relationship
mirror types get parity tests incl. nil, sentinel and malformed input.

## 15. Performance Tests Need Production Shape

Rule: tiny fixtures hide O(N). Keep microbenchmarks plus production shapes: baseline graph ops,
high-fanout relationships, history-heavy temporal queries, export/import, batch writes, Tiered
hot/warm routing.

## 16. Lazy Init Flags Must Be Synchronized

Trigger: zero-value lazy init that can run under a shared read lock.
Rule: the separate initialized marker is atomic (`initialized.Load()`) or guarded by an exclusive
lock; `sync.Once` serializes the init function, not unrelated reads of a plain flag.

## 17. Rollback Logs Track Pre-Transaction Existence

Trigger: caller-specified imports reuse an ID deleted earlier in the same tx.
Rule: `createdIDs` holds only IDs absent at `BeginTx`; rollback restores the pre-tx row and must not
later delete it as "created". Key create/delete rollback logs by pre-tx existence, not by the
operation name that produced the current row.

## 18. Map-Backed Query Results Need Explicit Ordering

Rule: any public query built from maps/dedup sets sorts by entity ID before returning
(`sortedKeys(seen)`); never expose Go map iteration order.

## 19. Native Fast Paths Must Respect Store Wrappers

Trigger: `store.(fastPath)` type assertions.
Rule: tests/consumers embed in-tree stores to inject failures or stale reads; a native shortcut must
not bypass those overrides. Keep the generic Store contract path the default; enable exact-store
shortcuts only for exact in-tree stores, or prove wrappers cannot override the optimized operation.

## 20. Mutation Transaction Time Must Be Monotonic

Trigger: stamping `TxFrom`, `TxTo`, `UpdatedAt`, `DeletedAt` or mutation events.
Rule: use `c.now()` (per-Core monotonic ms), never `time.Now().UnixMilli()`; wall-clock ms repeat or
go backwards and collapse version intervals for tx-time queries.

## 21. Batch Visibility Must Match Consumer Scheduling

Trigger: producer-side batch mutex over queues that consumers can observe directly.
Rule: the mutex does not make the batch atomic. Publish in the order that keeps the consumer
contract under partial observation (critical first), or gate consumers on the same boundary.

## 22. Peek-Then-Lock Must Revalidate Identity

Trigger: unlocked peek to discover the lock set (imports can reuse deleted IDs with a different
entity shape).
Rule: after locking the peeked IDs, re-read and revalidate the row identity; retry if it changed.

## 23. Property Index Keys Are Equality Semantics

Rule: property-index value keys are not display strings; they preserve the property equality
contract incl. special floats (exact bit keys where `strconv.FormatFloat(v,'g',-1,64)` collapses NaN
payloads), so fallback scans and indexed lookups return the same set.

## 24. Bound Every Retry Loop

Trigger: peek-then-lock retries.
Rule: every retry loop has a max-iteration ceiling (`for range maxRetries`, error "changed after N
retries") so a hostile workload cannot deadlock the caller. Mirror the ceiling across symmetric
methods: `deleteNodeInternal` uses `const maxRetries = 10`; `lockRelationshipCurrentEndpoints` must
match.

## 25. Equality and Hash Have Different Contracts (Float Bit-Pattern Case)

Rule: `PropertyValueEqual` is the CAS short-circuit contract (NaN == NaN, +0 == -0); the integrity
hash is the durable-identity contract and preserves IEEE-754 bit patterns. The asymmetry is
intentional and tested; do not assume "equal CAS" implies "equal hash". Callers exchanging data with
systems that canonicalize NaN bits canonicalize at the boundary if cross-system hash chains must
verify.

## 26. Public Sub-API Forwarding Must Match Internal Contract Width

Rule: each sub-API package declares its own minimal `Ops` interface and `*core.Core` satisfies each
implicitly by implementing the union. When collapsing/splitting sub-APIs, shrink each `Ops` to the
new shape; old methods on the internal type may stay, but the public Ops advertises only the new
contract.

## 27. Don't Ship "Documented Known Limitations" In Place Of Real Fixes

Trigger: writing a "known limitation / will fix in v4.1" note.
Rule: such a note is a code smell. Ask "if this had bitten me 6 months ago, would I ask why we
shipped it?"; if yes, fix now; carry forward only with a reviewable concrete reason.
Evidence: B2 (history-aware adjacency O(total history) fold) was first shipped as a v4.1 note; the
real fix was small: optional `DeletedIterationCapability` (`ForEachDeletedNodeID` /
`ForEachDeletedRelID`, plus depth variants) in every in-tree backend +
`forEachRelAdjacencyCandidateID` in the graph layer, O(total history) -> O(deleted count).

## 28. Adjacency Endpoint Immutability Permits Deleted-Only Folds

Trigger: optimizing a generic helper used in several semantic contexts.
Rule: split the helper before optimizing one caller. `forEach*CandidateID` stays all-history
(label/property temporal queries: an entity whose CURRENT label is Y but was X at t is not deleted
yet must be visited); `forEachRelAdjacencyCandidateID` is deleted-only, valid because rel endpoints
are immutable, so the only rels missing from node N's live adjacency index are DELETED rels. The
first B2 attempt collapsed all folds to deleted-only and broke label/property queries.

## 29. PublishBatch "Atomic" Ordering Survives Saturation Only With A Floor

Trigger: `AsyncEventBus.PublishBatch` strict-priority promise vs `BackpressureBlock` in-batch (`enqueueLocked` signals the dispatcher)
wake-up (dispatcher could pick a pre-existing lower-priority event mid-batch).
Rule: enforce a per-batch priority ceiling in the dispatcher: `batchPriorityCeiling` atomic Int32,
raised to the current priority index+1 at the top of each pass and cleared at batch end; the
dispatcher skips `priorityOrder[i]` for `i >= ceiling`. Liveness preserved
(`TestAsyncEventBusPublishBatchBlockWakesBeforeFullQueueWait`).
Detector: stage a lower-priority event, saturate with QueueSize smaller than the per-priority slice,
assert all batch events dispatch before it.

## 30. File Size Is A Signal, Not A Rule

Trigger: a file whose top comment describes one thing but holds several responsibilities (shared
receiver `c *Core` is not cohesion).
Rule: split by "what does this return"; line count alone is not the signal (900 lines doing one
thing is fine, a 200-line grab-bag is not).
Evidence: `store_capabilities.go` (~50 -> 917 LOC) split into `store_capabilities.go`,
`store_validation.go`, `store_copy.go`, `store_fetch.go`.

## 31. Tx Holds The Write Lock — Mirror Every Read Accessor (SUPERSEDED in v4.1.0)

Superseded: Path B (v4.1.0) means `BeginTx` no longer holds `c.mu.Lock`; both `g.Nodes.Labels(n)`
and `tx.Labels(n)` work in a tx and the tx-side mirrors in `tx_consistent_reads.go` are for clarity
only. Still needed: if lifetime-`c.mu.Lock` is ever reintroduced, every `c.mu.RLock` accessor
(direct, or via `c.readUnderRLock` (`locks.go:40`) in `queries.go`, `graph_property_query.go`,
`temporal_queries.go`, `txtime.go`, `stats.go`) deadlocks (RWMutex not reentrant, lesson 9), so each
needs a `(tx *GraphTx) Foo` mirror calling a lock-free `c.fooLocked` under `tx.mu`.
Detector: `grep -nE "c\.mu\.RLock|c\.readUnderRLock" pkg/graph/internal/core/*.go`.

## 32. Don't Conflate Transaction Time With Valid Time In The Resolver

Trigger: resolver deriving the valid-time end (`vEnd`) of a version.
Rule: `vEnd = next.ValidFrom` (valid time of the next state), falling back to `next.UpdatedAt` only
when unset; never `UpdatedAt` (the TX time of the supersede) alone. Fixed in `nodeVersionBounds` /
`relVersionBounds`. Failure shape: a caller-supplied `tkg_valid_from` on the next version did not
close the prior tile, leaving a gap, and `NodesAtTx(validAt, txAt)` fell back to the current VT
derivation.

## 33. Update Must Not Inherit Valid-Time From The Previous Version

Trigger: `prevState := current.DeepCopy()` then stamping a new version.
Rule: clear `current.Temporal().ValidFrom` and `ValidTo` before re-applying caller values, so
`ValidFrom != 0` on a non-genesis version literally means caller-supplied (no heuristic). The legacy
detector `nodeVersionInheritedValidFrom` stays only as a back-compat shim for pre-Phase-1 on-disk
data (harmless on new data: predicate fails on `ValidFrom == 0`); a heuristic breaks when a caller
sets ValidFrom equal to prev's.

## 34. AsOf TX Queries Default To Now — Pass TxAt For Bitemporal Reads

Rule: `QueryOpts.TxAt == 0` means "no TX filter" (VT match across the union of all history rows,
incl. writes not yet happened in the caller's timeline); it is kept for pre-bitemporal call sites
and is NOT bitemporally "as of now". New bitemporal point queries set `TxAt` explicitly.

## 35. Cascade Edit Rewrites History Rows In Place (SUPERSEDED by 46)

Superseded by 46: the cascade is append-only and never rewrites a stored row. Still needed: eclipsed
rows use the zero-width sentinel `ValidTo = ValidFrom + 1` (the store rejects
`ValidFrom == ValidTo`); the explicit skip in `resolveNodeVersionAt` / `resolveRelVersionAt` was
removed 2026-10-09, one-tick rows are ordinary spans. Split fragments get fresh versions
(`maxVersion+1+i`). The cascade row becomes current iff `newVT == 0` and no surviving post-cascade
row has a later open-ended `ValidFrom`. Temporal metadata is not in the content hash
(`integrity.computeNodeHashWithBuffer`).

## 36. Migration-Gated Resolver Heuristics Need A Runtime Flag

Trigger: a data migration that removes the need for a resolver heuristic.
Rule: keep the heuristic in code for stores that cannot run the migration (no capability, mock
stores, injected stores). Gate it on a per-core flag set by the post-`New` migration runner
(`if c.bitemporalMigrated || !heuristic(...)`). Removing it outright breaks tests with mock stores
whose iteration deliberately errors.

## 37. Capacity-Soft Token Registries Don't Fail Writes

Trigger: token registry at capacity (65536 entries).
Rule: `GetOrCreate` returns `(0, nil)` with a sticky warn-once, never `ErrRegistryFull`
(property-key cardinality is hard to bound: UUID keys, dynamic schema; failing turns soft
degradation into an outage). Encoders MUST treat token 0 as "no token, write the raw key string".

## 38. Validation Hooks Are The Right Place For Side Indexes

Rule: hook side indexes (property-key registry, cardinality stats) into the Core validation layer
(`validateOwnedPropertyEntryForCreate`, `validatePropertyUpdates`): it sees every key on Add and
Update with Core in scope, one line per validator. Not `types.PropertySlice.Set` (`pkg/types` is
fundamental, circular-dep risk) and not the wire encoder (store level, no Core context).

## 39. Custom EncodeMsgpack Bypasses Struct Tags

Trigger: adding a msgpack-tagged field to a struct that implements `EncodeMsgpack`
(`(PropertyWire) EncodeMsgpack` in `wire_encode.go`, hand-written to avoid reflective omitempty).
Rule: append the field to the custom encoder too, replicating omitempty by hand; the tag alone is
not enough and round-trips via plain `msgpack.Marshal` do not catch the gap.
Detector: `grep -nE "EncodeMsgpack|EncodeMapLen" pkg/graph/internal/storeutil/` lists every custom
encoder to update.

## 40. Load Registry Before Index Rebuild

Rule: any backend-internal decode that runs during init has its dependencies resolved first. Load
the property-key registry from meta KV BEFORE `loadIndexes` calls `bs.decodeNodeWireForKey`, else
tokenized rows fail decoding, the ID is dropped from the live-node map and `GetNode(id)` returns
`ErrNodeNotFound` for a row that exists on disk.

## 41. A Version Bump Is Not Done Until Every Doc-Consistency Target Matches

Trigger: a commit adds a numbered `## [x.y.z]` CHANGELOG heading.
Rule: in the same commit bump `Status: vX.Y.Z` in `AGENTS.md` and the `(vX.Y.Z)` title in
`docs/architecture.md` (and `Status:` in `CLAUDE.md`/`README.md`); the go.mod Go version must match
README/AGENTS/architecture. "The CHANGELOG says docs were updated" is not evidence; the test is.
Detector: `go test -run TestDocsMetadataMatchesSourceOfTruth ./pkg/graph/internal/core/` before
committing a release (it derives the version from the first numeric CHANGELOG heading). Evidence:
4.4.0 left both docs at v4.3.2.

## 42. Lesson 33 Applies To EVERY Deep-Copy Mutation Door, Not Just Update

Trigger: any mutation that DeepCopies current and writes a `*WithHistory` row (`updateNodeInternal`,
`addNodeLabelInternal`, `removeNodeLabelInternal`, both property-CAS sites).
Rule: each one clears the inherited world-time claim (lesson 33), except doors whose semantics close
the interval (delete tombstones, `CloseVersion`, which set ValidTo deliberately). Failure shape:
after an explicit-`tkg_valid_from` Add, a label/property mutation produced a current version
claiming the previous interval, and all three query doors agreed on the wrong post-mutation state.
Detector: `rg -n 'WithHistory\(' pkg/graph/internal/core/*.go`; plus the cross-door equivalence test
`TestTemporalTwoDoorsAgreeOnLabelQueries` (exact-set agreement of `NodesByLabelAt`,
`ByLabel`+QueryOpts and per-ID `NodeAt` on data where the label held historically but not
currently). Single-door tests cannot catch it because all doors share the broken resolver input.

## 43. Superseded Is Not Retracted — TxTo Must Not Bound Valid-Time Answerability

Trigger: the tx-visibility predicate for bitemporal point queries.
Rule: `visibleAtTx := TxFrom <= txAt` (recorded-by-then), never `&& (TxTo == 0 || txAt < TxTo)`.
`TxTo` marks supersession, not retraction: the row stays the authority for its valid-time slot in
later belief states; the resolver's `vEnd` derivation over the filtered chain reconstructs the
belief as of txAt. (Lesson 32's class, in the visibility rule.) Also: every pointer an accessor
returns from a frozen scan row must be independent (`Temporal()` / `Integrity()` copy-on-frozen);
flag-guarded methods do not guard pointer escapes.
Detector: after an explicit-VT update (v0 VT=[1000,inf) then v1 VT=[2000,inf)) assert
`NodeAtTx(1500, now)==v0`, `NodeAtTx(2500, now)==v1`, `NodeAtTx(2500, txBetween)==v0`,
`NodeAtTx(*, txBeforeFirstRecord)==nothing`. The (historical VT, current txAt) question had no test;
the only pins were predicate unit checks.

## 44. A Trust Boundary That Stores Hashes Must Verify Them On Entry

Trigger: import of rows that carry their own integrity state.
Rule: besides structural validation (lesson 6), recompute each imported row's content hash against
the claimed hash AND run chain verification over every imported entity after replay (content checks
cannot see LINK corruption, e.g. a flipped `PrevHash`); any mismatch gives `ErrCorruptExport` +
rollback. Every structural failure mode, incl. truncation mid-record, wraps the documented sentinel
so consumers can tell corrupt input from I/O errors.
Detector: export a graph with history + integrity; (a) truncate at many offsets, (b) flip single
bytes at spread positions. Each attempt either fails `errors.Is(ErrCorruptExport)` leaving ZERO
partial state, or imports a graph whose every entity passes `Verify*Chain`; no third outcome.

## 45. Shrinking A Resource Bound Must Account For State Written Under The Old Bound — And A Dependency's Assertion Failure May Be os.Exit, Not An Error

Trigger: Badger memtable-shrink ([4.8.0]); any "make the bound smaller" change.
Rule:
1. State written under the old bound may be unopenable under the new one: Badger creates each WAL at
   2x the MemTableSize that wrote it and replays it into an arena sized by the CURRENT MemTableSize
   (a clean Close deletes WALs, so unit tests never see it). Owe a migration path
   (`MigrateOversizedWAL` flushes such WALs at recovered original size first).
2. A dependency's invariant violation may terminate the process: "Arena too small" is
   `y.AssertTruef` -> `log.Fatal` -> `os.Exit`, not recoverable, not a `recover()`able panic; a
   probe open (tiered read-only recovery probe) os.Exits identically. Detect the unsafe condition
   with cheap non-destructive code BEFORE handing the dir to the dependency (scan `.mem` file sizes,
   Badger decides on apparent size), then migrate (writable path) or fail closed with a returned
   sentinel (`ErrOversizedWAL`) on paths that cannot (read-only, or recovered WAL size above the 1
   GB cap from a foreign/older writer). BAD gate: `MemTableSize > 0 && !InMemory && !ReadOnly`
   migrates only there, a read-only open over an oversized WAL still replays it.
Detector: test the os.Exit with a subprocess (`os.Args[0]` re-exec + env flag) doing the RAW
dependency open, assert the child does not exit 0 (in-process tests cannot see `os.Exit`).
Mutation-verify every "knob is applied" test by dropping the `With...` call; on-disk size checks run
while the store is OPEN (clean Close truncates files); `db.Opts()` witnesses
BlockCacheSize/NumCompactors, which leave no file footprint.

A successful self-SIGKILL request may return before process termination. Crash
callbacks must never return to the driver or run cleanup after signal success:
use the established timer-sleep loop until death (return only on signal error).
The parent must require actual SIGKILL and assert its deadline did not expire;
a timeout kill cannot witness the intended crash seam.

## 46. A Correction Is A New Belief — Never Mutate A Stored Row To Express It

Trigger: a valid-time correction (`SetNodeVersionInterval` cascade).
Rule: express a correction recorded at `now` ONLY by appending fresh rows stamped
`TxFrom = UpdatedAt = now`; existing rows are immutable (as a normal `Update` already does). The old
in-place rewrite/split kept the original `TxFrom` while asserting an interval decided now, so
`NodeAtTx(_, oldTxAt)` reconstructed a belief that never existed (holes, early leaks) and created
two TX-open rows with overlapping tx intervals, breaking the native `NodeAsOf` early-stop (assumes
version order == TxFrom order) and diverging it from memory/tiered. The resolver reconstructs a
belief state by filtering the chain to `TxFrom <= txAt` and tiling the rest; to tile a non-monotonic
chain (a correction can land at an earlier valid-from than a later version) it orders by effective
valid-from (not array/version order) and breaks valid-time overlaps by the newer belief (higher
`TxFrom`, then version). `TxFrom` is never back-dated on a row written now. Pairs with 43.
Detector: two-phase, all backends: create state, record a correction at a later tx, query `*AtTx` at
a `txAt` BEFORE the correction; every world-time covered then still resolves to its pre-correction
value, no holes, no leaked corrected value. A single-backend or single-txAt test passes the buggy
in-place cascade.

## 47. The Decode Step Is Part Of The Trust Boundary — And `recover()` Cannot Catch A Fatal Stack Overflow

Trigger: any decode of persisted/imported bytes (`msgpack.Unmarshal` assumed to only return errors
on bad input; lessons 6/44 ran too late).
Rule: decode through `storeutil.SafeUnmarshal` (BAD: raw
`msgpack.Unmarshal(diskBytes, &NodeWire{})`). vmihailenco/msgpack/v5 crashes the process two ways:
1. Reflect panic: a map repeating a key bound to an interface-typed field (`PropertyWire.Value any`)
   hits an unaddressable `reflect.Value` (`SetString/SetInt`), ~17 bytes.
2. Fatal stack overflow: `Value any` recurses per nesting level; a hundreds-of-thousands-deep blob
   aborts with `fatal error: stack overflow`, NOT recoverable.
So ORDER matters: `SafeUnmarshal` first runs `guardMsgpackDepth` (non-recursive token-stream scan
with an explicit pending-count stack; rejects nesting beyond `maxWireDecodeDepth`=64), THEN
`defer recover()` for the panic class; both return `store.ErrCorruptWire`. The guard is not a full
validator: over-permissive is fine, it must never UNDER-count depth and must keep its cursor aligned
by skipping scalar payloads exactly. Depth 64 not lower: legitimate wire is map(1)->"p"
array(2)->PropertyWire map(3)->value, a value nested to the 32-level property allowlist sits at ~35.
`msgpack.Marshal` is panic-free (no wrapper). Flat typed structs with no interface/deep field
(tiered catalog/registry/index metadata) are outside the class but routing them is harmless. The
single sanctioned non-SafeUnmarshal reader is `scanWireTemporalMeta` (see AGENTS.md).
Detector: `grep -rn 'msgpack.Unmarshal(' pkg/ | grep -v _test` (every hit decoding
persisted/imported bytes owes `SafeUnmarshal`); fuzz `FuzzWireToNodeChecked` (arbitrary bytes, never
panics, entity-or-error; found the panic on its first run, crasher kept as corpus seed). Fatal
overflow cannot be a failing assertion: assert the POSITIVE contract (`SafeUnmarshal` returns
`ErrCorruptWire` on a 200000-deep blob; removing the guard crashes the test binary) and a "guard
accepts a depth-30 property + every seed entity" test (lowering the cap below ~36 must fail it).

## 48. Allocate Proportional To Bytes DELIVERED, Not To An Untrusted Size/Count Field

Trigger: `make(T, n)` / `Grow(n)` / map-capacity hint where `n` comes from an untrusted decoded
length or count. Allocation-time twin of lesson 6.
Rule: bound by (a) bytes actually delivered or (b) a small constant cap, never the raw declared
value, even when a "max" cap exists (128 MiB / 1M are DoS sizes from a tiny input). BAD:
`data = make([]byte, length); io.ReadFull`. GOOD:
`var b bytes.Buffer; b.Grow(min(length, cap)); io.CopyN(&b, r, int64(length))`.
Evidence (both found by `FuzzImport` end-to-end, which STALLED at 0 execs/sec: repeated giant
allocations / GC thrash, not a hang): (1) `readImportStageRecord` did `make([]byte, length)` for the
per-record length (<= `maxExportRecordSize` = 128 MiB): 5-byte header, ~25-million-x amplification;
fixed with `io.CopyN` into a `bytes.Buffer` pre-reserving at most 64 KiB, so a short stream returns
`ErrCorruptExport` having allocated ~nothing. (2) `reserve()` pre-sized SIX replay/rollback
maps/slices from the export HEADER counts (cap `importPreallocLimit` = 1<<20): ~20 bytes forced ~312
MiB; the maps grow naturally so the count is a pure hint, cap lowered to 4096.
Detector: `runtime.ReadMemStats(&m).TotalAlloc` is cumulative, so feed a tiny lying header and
assert the alloc delta stays far below the declared size (reverting jumps to 128 MiB / 312 MiB).
Fuzz a streaming/replay boundary END-TO-END, not just the leaf decoder; read a frozen exec rate as a
DoS signal.

## 49. A Change-Log Must Be Emitted In-Backend And Co-Committed — A Decorator Cannot Be Crash-Safe

Trigger: a write-observer / op-log / CDC seam that must be crash-consistent with the data.
Rule: it lives INSIDE each backend, co-committed in the same atomic write, exposed as an OPTIONAL
capability the core type-asserts; never a Store wrapper. A `ChangeLogStore` decorator (a) sits above
`Store`, cannot reach the inner Badger `WriteBatch`, so a second durable write reintroduces the
committed-but-unlogged window; (b) is none of `*memory`/`*badger`/`*tiered` for
`isExactNativeStore`, so it is UNTRUSTED and the frozen-pointer zero-copy scan is silently disabled.
Design body: AGENTS.md "Change-log (op-log) is IN-BACKEND".
Corollaries:
- LSN + record enqueue under ONE lock window with the entity ops: doors holding `idxMu.Lock` across
  `appendOps` log right after; doors that do NOT (`PutNodeVersion`/`PutRelVersion`, history
  truncate) need the combined `appendOpsLogged` so a background flush cannot snapshot the op without
  its record. Audit EVERY early-return-after-snapshot in `flush()` (each requeues the log records)
  and the empty-batch guard `len(ops)==0 && len(logs)==0` (else a log-only flush drops records).
- Shared multi-`appendOps` helpers (`deleteRelByInfo`, `cascadeDeleteInner`) stay record-free; the
  PUBLIC door emits exactly one logical record (one `NodeDelete` per cascade, not one `RelDelete`
  per edge), built from the door's own data.
- Parity by construction: both backends share ONE set of record-body builders (`storeutil`), then a
  cross-backend test asserts byte-identical feeds (badger tokenizes property KEYS on the wire,
  memory keeps strings: byte-parity on property-free entities, decode-equivalence otherwise;
  superseded for put records by 50's untokenized wire).
- The op-log alone does not converge a replica: token allocation is CORE-layer (backends only see
  tokenized rows), so a per-token registry record is impossible in-backend; ship the registry in a
  full snapshot, bootstrap from it, then tail.
- A synchronous side path (`MetaSet` via its own txn) cannot share the async buffer's LSN/watermark
  without a non-monotonic-watermark hazard: defer such records.

## 50. A Replica Must REPRODUCE The Primary's Rows, Not Re-Derive Them — And One Bit Of Provenance Decides History Depth

Trigger: Phase-1 read replica applying a primary's change-feed; the replica must be BYTE-EXACT
(hash, version, `TxFrom`, temporal metadata), since hashes chain those fields. Apply design:
AGENTS.md "Replica apply reproduces the primary's rows VERBATIM".
Rules:
- Apply writes the wire VERBATIM through foreign-ID store doors; it NEVER calls
  `NodeOps.Add`/`Update` (they re-mint IDs, re-stamp `TxFrom`, recompute hashes). `apply_record.go`
  reuses the import trust pipeline (`WireTo*Checked` -> token-validate -> prop-limits -> hash
  recompute-AND-COMPARE), then `PutNode` / `ReplaceNode` / `ReplaceNodeWithHistory` / `Delete*` /
  `PutNodeVersion` / `Truncate*`/`Trim*` / label-token doors. The hash is VERIFIED, never STAMPED.
  Use the integrity hash as the convergence oracle in tests.
- A bare new-current row is ambiguous about provenance: Phase-0 `ChangeNodePut` was emitted
  identically by create, in-place `ReplaceNode`, `ReplaceNodeWithHistory` and label mutations, so a
  replica deriving history from its own current row invents phantom rows or loses real ones. Carry
  exactly ONE bit (`NodePutBody.WithHistory`), set identically across backends; the replica
  regenerates the exact prior history row from its in-sync local current (equal to the primary's
  pre-mutation state by LSN total order). Create-vs-update is inferred from local existence; a label
  change is a one-token diff. General: when a log record drives a consumer state machine, check it
  disambiguates the TRANSITION, not only the resulting state; prefer a provenance bit at the
  emitting door over re-deriving.
- Put-record wire is UNTOKENIZED (`NodeToWireChecked`, property keys as strings): byte-identical
  feeds even for property-bearing entities and no property-key registry dependency (only
  label/rel-type tokens). Prefer backend-independent representations for cross-node protocols.
- The read-only gate is CORE-layer: Badger `ReadOnly` disables the change-log and rejects apply's
  own writes. `checkWritable()` (= `checkOpen` + replica flag -> `ErrReadOnlyReplica`) on every USER
  mutation door; reads, the bootstrap importer and `ApplyChange` call only `checkOpen`. File-scope
  the `checkOpen`->`checkWritable` rename only in purely-mutation files
  (`node_add`/`node_update`/`relationship_*`/`node_label`/`graph_indexes`); door-by-door where reads
  share the file (`node_delete`/`relationship_delete` have `Get`, `crud` `History` is a read,
  `temporal_queries` mostly reads). `SetProperty`/`DeleteProperty` delegate to `Update` and inherit
  the gate; do not double-gate.
- A watermark advanced AFTER the door commits is safe ONLY if apply is idempotent (applied-LSN is a
  separate `MetaSet`; a crash in between replays the last record): identical row -> no-op
  (`nodeWireMatches`/`relWireMatches`, working against the untokenized wire), delete tolerates
  not-found. Re-applying a with-history `NodePut` must NOT append a second history row.
- A separate-path watermark can OUTRUN buffered data (permanent loss): the data door writes through
  the async buffer (`flushIfNeeded` is a NO-OP without `SyncWrites`), the watermark `MetaSet` goes
  straight to Badger; a crash recovers the ADVANCED watermark with the entity write lost. Behind is
  harmless (idempotent re-apply), AHEAD is fatal. Rule: when a progress marker lives on a different
  durability path than the work it records, FLUSH the work to that path BEFORE advancing the marker
  (`flushStoreLocked()` before `setAppliedLSNLocked`; batch apply flushes ONCE then advances to the
  last LSN). Add a monotonicity guard (`rec.LSN <= applied` -> skip) so stale redelivery cannot
  regress the row or watermark.
- Gate-completeness is enumerated, not pattern-matched: the first gate conversion missed
  `CompareAndSetProperty` and `CloseVersion` (genuine writers via `ReplaceNode*`, living in
  `property_cas.go`/`version_chain.go` whose other methods are reads). Audit by what the method DOES
  (does its body reach a `store.Put/Replace/Delete` door?), grep mutation verbs across EVERY `*Ops`
  method.
Detector: InMemory-only tests hide the watermark bug: add a disk-backed, non-`SyncWrites`
restart-resume test; a double-apply test asserting history depth unchanged; a gate test exercising
each door by name.

## 51. Syncing An Append-Only Registry Across Nodes: Capture-Order, Prefix-Guard, And Persist-Before-Use

Trigger: a Phase-1 replica resolving a label/rel-type the primary registered AFTER its bootstrap
(refetch `RegistrySnapshot` + `AppendNames`). Design: AGENTS.md "Token-registry refetch" and
"Bootstrap LSN ... failover lease".
Rules:
- Capture the anchor LSN BEFORE the names, never after. `RegistrySnapshot` returns the registries
  plus `CapturedAtLSN`, checked against the triggering record (`CapturedAtLSN >= rec.LSN`). Names
  first then LSN lets a token allocated+committed in between make the LSN claim coverage the names
  lack (incomplete registry appended, token never resolves). LSN first guarantees
  `CapturedAtLSN <= names coverage` (a mutation at/below that LSN allocated its token before
  committing); extra tokens beyond it are harmless.
- The grow primitive is prefix-guarded; `ImportNames` is load-only (rejects a non-empty registry).
  `AppendNames(prefix, suffix)` asserts the registry's CURRENT contents DeepEqual the observed
  `prefix`, then appends `suffix` at contiguous indices; on divergence returns `(false, nil)`
  WITHOUT mutating. The registry is gap-free/monotone, so a wrong guard silently misplaces tokens.
  Provide it on all three registries (parity), even `PropertyKeyRegistry`, which the hook does not
  use.
- Persist the grow BEFORE the row that needs it; roll back on persist failure. Grow in memory,
  `persistRegistries()`, all under `registryMu`, BEFORE the entity door runs. On persist failure
  `RollbackNames` the in-memory grow so memory == disk; otherwise a retry sees the token resolvable
  in RAM, skips refetch+persist, writes the entity, and a crash leaves a dangling token on restart.
  Same flush-before-watermark discipline as 50, one level down.
- Do not sync what is tokenized locally: records carry UNTOKENIZED property keys, so a replica
  tokenizes them in its OWN registry (tokens need not match the primary's). The refetch syncs ONLY
  labels + rel-types; appending the primary's propkeys would fail the prefix-guard.
  `RegistrySnapshot` still ships `PropKeys`; the hook ignores them. Know which tokens are part of
  the logical row vs storage-local.
- Lock order: refetch runs UNDER apply's `c.mu.Lock`, takes ONLY `c.registryMu` (order
  `c.mu -> registryMu`, as `getOrCreate*Persisted`); calls the source (a DIFFERENT/primary Core with
  its own locks), never re-takes `c.mu`; a self-pointing source cannot deadlock (distinct mutexes)
  and the `CapturedAtLSN` guard catches the misconfiguration.
- Failover: the library ships the durable hint and reopen path; coordination belongs to the external
  orchestrator. The ID-slot lease is persisted/read by rho-tkg (`SafeUnmarshal` on read, slot
  range-validated at write), last-writer-wins (`DetectConflicts=false`), NOT consensus. Promotion =
  `Close()`+`New()` under the leased slot (snowflake generators are immutable post-`New`). A
  promoted node and the node it replaces must hold DIFFERENT slots; that, not lease durability,
  prevents minted-ID collisions in a split-brain window.

## 52. Delta Backups Reuse The Op-Log, Not A Diff — And A Hash Oracle Hides Non-Hashed Divergence

Trigger: `ExportSince` / `ImportMerge` (node-level incremental backups; the change-log feed is
framed into the export stream and replayed through replica-apply doors).
Rules:
- Key a delta off the change-log (TX-ordered), never a temporal DIFF: (1) a valid-time diff
  (`Temporal().Diff`) DROPS backdated writes (old `valid_from` reads as unchanged); the cursor must
  be transaction-monotonic (LSN). (2) A state-endpoint diff cannot reconstruct the primitives: a
  label-set change replays as per-token `Add/RemoveNodeLabelToken` (`ReplaceNode` does NOT reindex
  labels), a cascade delete needs the exact connected-rel tombstones (`DeleteNodeWithHistory`
  requires one per live edge). The op-log already records them in order.
- A hash-equality convergence oracle (lesson 50) cannot see non-hashed fields (`TxFrom`/`TxTo`,
  lesson 43). A WithHistory put regenerates the superseded prior row from the consumer's local
  current (`TxTo = 0`) while the origin stamped `prev.TxTo = now` (= new `TxFrom`): hash-exact but
  not byte-exact; a byte-level export comparison caught it. Fix: `reproduceSuperseded*TxTo` at the
  apply door (every WithHistory emitter uses one monotonic `now` for both). Assert reproduction with
  a check that includes fields the hash omits.
- A change-feed shows only DURABLY-COMMITTED records: Badger's async buffer holds
  committed-but-unflushed rows, so `ChangeFeed` / `LastCommittedLSN` (hence export-header
  `SnapshotLSN`) lag. `Export` and `ExportSince` `flushStoreLocked()` before reading, else deltas
  miss recent mutations and a full export stamps a stale (often 0) snapshot LSN, so the replica
  resumes from 0 and re-ships the whole graph. Memory has no buffer: add a badger/disk path test.
  Same family as 50's flush-before-marker.
- A "present but disabled" optional capability needs a status probe: in-tree backends implement
  `ChangeFeedCapability` UNCONDITIONALLY (empty feed when off), so a nil-interface check cannot tell
  recording from off. `store.ChangeLogStatusCapability` (`ChangeLogEnabled() bool`);
  `ExportSince`/`Watermark` fail closed when off. If a capability can be present-but-inert, expose
  explicit status.
- The integrity hash chain is a DAG, not a line: `SetNodeVersionInterval` appends a version whose
  `PrevHash` links to the version it supersedes on the VALID-TIME axis (documented in
  `temporal_cascade.go`). The linear check `v_k.PrevHash == v_{k-1}.Hash` rejected every
  cascade-corrected graph, so even full `Export`+`Import` died with `ErrCorruptExport`.
  `VerifyNodeChain`/`VerifyRelChain` verify "every non-genesis `PrevHash` is in the set of all
  version hashes" (genesis empty; lowest retained version may dangle for truncation) plus
  per-version content-hash recompute. A validator for a documented non-linear structure must use the
  same model.
- A purge-and-recreate rollback must clear version history: `DeleteNodeCascade` clears current row +
  adjacency + indexes, NOT the `0x07` history keyspace, so a rolled-back update left a stale history
  row. `Truncate*History(id,0)` in the purge step (mirrors `restoreImportNodeHistory`). A delta's
  registry merge is a BIDIRECTIONAL prefix check (an OLDER delta re-applied after a newer one is a
  no-op), distinct from replication's append-only `appendRegistrySuffix`.

## 53. A Reset That Wipes A Durable Watermark Must Keep The Watermark Continuously Present — Order The Wipe So No Crash Point Is Inconsistent

Trigger: `Clear()` with change-log enabled used `db.DropAll()` (wipes `LastLSNKey` too) then wrote a
`ChangeClear` marker; a crash in between reopens with no records and no watermark, the allocator
reseeds from 0 and post-`Clear` LSNs collide with a tailing consumer's checkpoint (silent permanent
strand).
Rule: when a reset must preserve ONE durable invariant (watermark monotonicity), do not
wipe-then-rewrite (the rewrite is a second transaction); keep the key continuously present and
mutate in place. `clearAndReanchorChangeLog` `DropPrefix`es every
data/index/history/change-log-record keyspace, leaves `KeyMeta` (holding `LastLSNKey`) intact, then
overwrites `LastLSNKey` alongside the marker in one atomic batch. Ordering invariants:
- Delete stale meta keys (entity counters, registries, index defs) BEFORE dropping data:
  `loadIndexes` treats a persisted counter EXCEEDING the live row count as fatal data loss
  (`reconcilePersistedCounter`), an ABSENT counter safely trusts live rows. Counters-first leaves
  data present + counters absent = consistent.
- Scan for the meta keys to delete rather than hard-coding (reaps the replica watermark, id-slot
  lease, future keys) but skip `LastLSNKey` explicitly.
- Fix EVERY door to the invariant: the durable watermark outlives the config flag. The first fix
  guarded only the log-ENABLED arm; the log-DISABLED `Clear()` arm was a bare `DropAll`, so
  ChangeLog-on -> reopen off + `Clear` -> reopen on reseeded to 0 and reused passed LSNs (failing
  test: pre-`Clear` watermark 2, post-reopen write LSN 1). Both arms share
  `clearDataPreservingLastLSN`, which preserves `LastLSNKey` whenever present; only a never-logged
  store still `DropAll`s. When fixing a durability invariant, grep every path that
  deletes/overwrites its key; a config-gated branch is where the second door hides.

## 54. A Swap-Out Commit-Window Buffer Must Be Cleared On Success AND Consulted By EVERY Reader — Or It Both Drops And Resurrects Rows

Trigger: the badger async flush swaps the `pending` buffer for a fresh map under `wbMu`, releases
`idxMu` and commits with NO lock held. During that window (registry persist + batch build +
`wb.Flush()`) a just-written row is gone from `pending` and not yet in a Badger `View`; a reader of
`pending` + `View` drops it. The fix parks the snapshot in a second map `flushing` and unions
`flushing ++ pending` in ONE helper (`rangePending` / `lookupPending`).
Rules:
- Audit EVERY reader of the buffer (`grep 'range bs.pending\|bs.pending\['`), not the one that
  motivated the change: only 1 of ~13 overlay readers was routed (single-key version lookups,
  history scans, `Max*HistoryID`, truncate/trim retention scans, AsOf version-chain overlay, on-disk
  label/adjacency overlays still dropped rows; `Max*HistoryID` disagreed with `All*HistoryIDsFrom`).
  One helper gives the union logic one definition.
- Lifetime: populated between swap and commit, EMPTY otherwise. Clear it on success, on the failure
  requeue, AND in any `Clear()`/reset that wipes the keyspace. The first fix never cleared on
  success (only on failed-flush requeue; under `SyncWrites` there is no periodic flush, so it
  persisted), and `Clear()` reset `pending` not `flushing`, so wiped history keys RESURRECTED as
  phantom IDs through the one reader that consulted `flushing`.
- Delete-set-computing MUTATORS need the snapshot too: cascade delete and incoming-repair compute
  "which index keys exist" from `View` + `pending`; a key parked in `flushing` is in neither, so
  they queue no delete and ORPHAN the key when the commit lands. Cannot rewrite a committing
  `flushing` entry: queue an explicit delete into `pending`.
- RAM mode was always correct (synchronous `labelIdx`/`inIdx`/`outIdx` still hold the key); only
  history readers (no RAM mirror) and on-disk label/adjacency overlays were exposed. When a buffer
  has a synchronous shadow for most consumers, enumerate the raw readers deliberately. When the
  helper unions two buffers make set/delete SYMMETRIC (each op removes the key from the other set)
  so a key in both during the requeue window resolves to the newer op.
Detector: deterministic white-box helper performing the exact `pending`->`flushing` swap WITHOUT
committing; assert every reader still sees the row and `Clear()`-then-read does NOT (failing-first
by reverting each fix).

## 55. The Change-Log Is A PHYSICAL Redo Log, Not A LOGICAL Transaction Log — A Rolled-Back Tx Ships Its Churn To The Feed

Superseded for `GraphTx` by the scoped log (`store.TxChangeLogScope` on
memory/badger/tiered/sharded; `DiscardScopedLog` in `GraphTx.Rollback`): a rolled-back tx emits no
records, LSNs are minted AT commit (no gap), tx `restoreRegistries` is exact de-allocation again.
Failure shape it replaced: records are emitted in-backend (`logChangeRaw`) on every store mutation and a `GraphTx`
applies writes immediately (rollback = reverse store calls), so create+`Rollback()` left
`ChangeNodePut` (LSN N) + hard-cascade `ChangeNodeDelete` (N+1): replicas converge but transiently
materialize uncommitted state, and the feed is not a logical CDC source (EventBus buffers/discards
tx events via `txEventBuffer`; the log did not). The `!WithHistory` node-delete apply branch is live
only because of rollback churn: test it with a real rolled-back-tx record.
Still live for `Batch.Execute` and `IO().Import` (they emit eagerly, not through a scope; wiring is
a `tasks/backlog.md` follow-up):
- Registries are APPEND-ONLY across rollback when the change-log is ENABLED. A rolled-back tx that
  allocated a NEW label/rel-type token had emitted a durable put referencing it, then
  `restoreRegistries` de-allocated it: replicas stuck forever at that LSN ("rel type token 1 not in
  registry (size 0)") and the next allocation REUSED the number for another name (silent
  divergence). Same poison in `restoreNewLabelsOnError` / `restoreNewRelTypeOnError`
  (standalone/batch/index) and the batch partial-failure path; they keep tx-allocated tokens when
  `c.changeLogEnabled`. With the log OFF, exact rollback is preserved.
- Gate signal is `store.ChangeLogStatusCapability.ChangeLogEnabled()` captured into
  `c.changeLogEnabled` at `New`, NOT `changeFeed != nil` (badger/memory always implement the feed
  methods). Of SIX `RollbackNames` sites only the TWO entity-failure ones (`restoreNewLabelsOnError`
  / `restoreNewRelTypeOnError`, firing AFTER a record was emitted) are gated; the FOUR
  `getOrCreate*` allocation/persist-failure `fail` closures fire BEFORE any record and MUST still
  de-allocate (in-memory == disk). Rule: gate iff a durable record could already reference the
  token. Import's `restoreRegistries` is gated too (a replica bootstrap import has the log OFF).
- Accepted tradeoff: a token is per DISTINCT NAME, so leakage is bounded by distinct schema names
  ever attempted (against 65535; registry warns at 60K, errors at 65535), symmetric on primary and
  replica, fails closed.
- Batch keeps successful ops on partial failure (not all-or-nothing): scoping it requires committing
  its scope, and a failed op that emitted a record must have its data cleaned up or the feed orphans
  a record.
- Scoped-log implementation lessons: (a) records are emitted IN-BACKEND so the store cannot tell a
  tx call from a concurrent standalone call (both hold shared `c.mu.RLock`, Path B); a store-global
  "divert" flag would misroute. The change-log-enabled tx takes `c.mu.Lock` (EXCLUSIVE) PER-MUTATION
  (not per-lifetime: that re-creates the lesson-31 deadlock) and toggles the divert flag under it;
  reads stay on RLock. Pinned by a `-race` test that FAILS when reverted to RLock. (b) Co-commit
  (lesson 49) holds only at COMMIT: records are buffered but DATA flows to pending and flushes
  normally; a crash between a mid-tx flush and `CommitLogScope` leaves committed-but-unlogged data
  invisible to the feed (watermark never advanced), accepted and documented; closing it needs a
  staged-write-set tx.
General: when an aborted operation feeds a replicated log, audit both what it EMITS and what it
RETRACTS from shared monotone state (the registry); retracting something already in the durable feed
is the fatal half.

## 56. A Purge-And-Recreate Rollback Must Capture EVERY Door's Full Side-Effect Footprint — A Capture Branch That Forgets Adjacency Turns Rollback Into Data Loss

Trigger: `ImportMerge` rollback PURGES every touched entity (`DeleteRelationship` +
`DeleteNodeCascade`, which also drops every attached edge) and RE-CREATES it from a pre-merge
snapshot via the CREATE doors. The `ChangeNodeHistoryVersion` and `ChangeNodeHistoryTruncate` arms
of `captureMergeRecord` called only `captureNode(id)`, not `captureNodeAdjacency(id)` like
`ChangeNodePut` / `ChangeNodeDelete`; a `SetNodeVersionInterval` cascade on a bounded PAST interval
emits ONLY bare `ChangeNodeHistoryVersion` records, so an unchanged edge was cascade-deleted by the
purge and never restored (silent edge loss on a rolled-back merge). Both arms now capture adjacency.
Rules:
- Purge+recreate rollback: the capture pass records the TRANSITIVE closure the purge destroys
  (adjacency), for EVERY record kind that can be the sole record touching a node.
- Audit by symmetry across the `switch`: every node-touching arm reaches the same adjacency capture
  as `ChangeNodePut`.
Detector: the corruption test uses a node whose ONLY record is the under-tested kind and ASSERTS
that precondition by decoding the change feed (a `ChangeNodePut` would capture adjacency and mask
the bug).

## 57. A Multi-Buffer Overlay Must Resolve Set-vs-Delete PER KEY At EVERY Reader Door — A "Running Max" Reader Can't Be Un-Bumped By A Later Delete

Trigger: the badger commit-window overlay has two buffers (`flushing` older, `pending` newer;
`rangePending` visits flushing then pending). `pendingHistoryIDOverlay` (`AllNodeHistoryIDs`) tracks
`pendingSets` and deletes on DELETE; `maxHistoryID` (`MaxNodeHistoryID`/`MaxRelHistoryID`) kept a
scalar RUNNING MAX over SET keys with DELETEs applied only to the persisted reverse-scan. Badger
empty + flushing SET id 500 + pending DELETE id 500: `Max*` returned 500, `All*` none.
Rules:
- Two reader doors sharing one overlay share one resolution: reuse the resolver
  (`pendingHistoryIDOverlay`), not a parallel hand-rolled scan.
- A running min/max/count over a multi-op buffer is a red flag (a scalar cannot retract a deleted
  key): collect surviving keys, resolve set-vs-delete, then reduce.
Detector: test cross-door AGREEMENT (`Max*` vs `All*` for the same overlay state, flushing-SET
masked by pending-DELETE, Badger empty), not each door alone (rule 17 generalized).

## 58. A Contract That Spans A FAMILY Of Sibling Doors Must Land In EVERY Door — Audit The Family, Don't Patch One

Trigger: a contract that should hold across a family of doors drifted in one.
Evidence (two defects): (1) untrusted-stream contract "every decode/validation rejection wraps
`ErrCorruptExport`": bootstrap import (`import.go`) and `verifyImportedNodeHash` wrapped, but the SHARED
replica-apply / delta-merge path (`applyChangeRecordLocked`) returned `WireTo{Node,Rel}Checked`
errors BARE (`errors.Is` worked for hash mismatch, not malformed wire `id == 0`); fixed at ALL SEVEN
`WireTo*Checked` apply sites. (2) registry name doors (`GetOrCreate`, `ImportNames`, `AppendNames`):
Label/RelType reject blank via `isBlankName` in all three; `PropertyKeyRegistry` rejected blank only
in `AppendNames` while `GetOrCreate` guarded `== ""`; `GetOrCreate` tightened to `isBlankName`.
Rules:
- Find the family, then grep it: list the siblings (other `WireTo*Checked` sites, name doors,
  registries) and confirm the same `errors.Is` classification / validation predicate / ordering in
  each.
- The fix DIRECTION is set by the existing shared test, not the loosest door: loosening
  `AppendNames` turned the parametric `TestAppendNames_RejectsMalformedSuffix` (all three
  registries) red, revealing intent = REJECT blank.
- Tighten CREATE doors (new data only; the wire encoder ignores `GetOrCreate`'s error and falls back
  to raw key on token 0), NOT the LOAD door: `ImportNames` stays lenient because a registry
  persisted before the guard may hold a blank token (strict-on-create, lenient-on-load is
  deliberate; pin it so a tidy-up cannot hard-fail legacy loads).
- Make un-expressible invariants executable: flush-before-watermark (durable applied-LSN never leads
  durable data) is tested via a `Config.Store` decorator embedding a real backend and overriding ONE
  method (`Flush()` injects a one-shot failure; `MetaGet`/`MetaSet` forwarded so the watermark store
  is independent).

## 59. A Privileged Override Of A System-Controlled Field Lands At The Shared Seam, Scoped To Where The Invariant It Relaxes Actually Binds

Trigger: letting a caller override a system-stamped field (§4.1 tx-time backfill `tkg_tx_from`, §4.2
named as-of tags).
Rules:
- Relax an invariant only where it binds. Enumerate the field's write sites and ask per site "does
  the invariant I relax bind HERE?". `TxFrom` is stamped on every door; monotonicity (lessons 20/46)
  binds the SUPERSESSION doors (update/correction must carry a real current TX time) but not the
  CREATE doors (a never-seen entity may assert "the DB learned this at T", as binary import already
  did). Backfill is therefore CREATE-only: the 6 create doors are exactly the 6 calling
  `extractTemporal`; every other `TxFrom = now` site is supersession and keeps the clock. Amended
  2026-10-09 (`DeleteWithTx` / `UpdateWithTx`): "binds here" is not "never relax here": a
  supersession door CAN take a caller instant once the invariant is re-established as a check, `t`
  after every TxFrom/TxTo on the chain and after the version start, under the entity lock
  (`checkTxOrder`, `ErrTxOrder`). The instant travels as an argument, never as the reserved
  property, so the create-only rule for `tkg_tx_from` holds.
- Land the feature at the ONE shared seam so the family inherits it (constructive form of lesson
  58): `tkg_tx_from` is extracted inside `extractTemporal` (node Add/Import, rel create kernel, both
  batch-queue create paths) covering standalone + import + batch + tx; each door needs a 3-line gate
  call (`resolveBackfillTxFrom`) and honors the override where it stamps `TxFrom`; `AddWithTx` is a
  3-line wrapper injecting the property. Amended 2026-10-09: the end/supersede doors use one `at`
  seam on `deleteRelationshipInternal` / `deleteNodeLocked` and one `updateTemporal.txAt` (ten
  doors: standalone, GraphTx, Batch, ingest strong and concurrent; nodes and rels). A DEFERRED door
  (Batch / ingest apply: creates, updates, deletes, continue after a failed op) is not covered by
  the seam alone, since a per-op refusal leaves a partial past no later earlier-`t` write can
  repair: run a whole-unit pre-flight using the seam's OWN refusal functions
  (`checkRelCallerDelete`, `checkNodeCallerUpdate`, ... extracted so they cannot diverge) over every
  caller-instant op before any write, plus "a caller-instant op is the only op on its entity in the
  unit" (apply order is not queue order; node deletes count the relationships they cascade).
- Gate with a Config flag (`Config.AllowTxBackfill`, off by default; deliberate auditable opt-in on
  open, not per-call); malformed input beats the disabled-gate error: `resolveBackfillTxFrom` orders
  `value==0 -> none`, `invalid -> ErrInvalidTxFrom`, `!gate -> ErrTxBackfillDisabled`. Test invalid
  value with gate ON and OFF.
- Bound a caller value feeding a monotonic axis on BOTH sides against the system clock, one test per
  bound ("positive" is not "valid" for a time that must be <= now). The first guard checked only
  `txFrom > 0`; a FUTURE `TxFrom` later yields an INVERTED TX interval (`TxFrom > TxTo`) after
  supersede (successor `TxFrom = now`), invisible to AS-OF queries, passing `Verify*Chain` (TxFrom
  not hashed) and replicating. Realistic trigger: `Instant` is Unix-milli, snowflake is micro
  (`time.Now().UnixMicro()` is ~1000x too large, year 58000). All original tests used a PAST
  instant.
- A non-hashed field is invisible to the hash/`VerifyChain` oracle (lesson 52 applied to tests):
  re-fetch the stored row and assert `Temporal().TxFrom == backfilled` exactly; flagship test:
  `NodeAsOf(id, knowledgeTime)` sees the backfilled row while a no-backfill control returns
  `ErrNoVersionAsOf`. Mutation-verify by neutering the stamp override.
- Durable-metadata registry lifecycle is enforced at the CORE layer when store `Clear` semantics
  diverge: badger `Clear` scan-reaps all meta except the LSN watermark (lesson 53), memory `Clear`
  preserves `metaKV`, so `Admin().Reset()` reaps `asof_tags` explicitly (`MetaSet(asof_tags, nil)`
  under `asofMu`). Persisted meta is untrusted: decode via `SafeUnmarshal` (fail closed), serialize
  read-modify-write with a dedicated mutex, reads lock-free (MetaGet is an atomic snapshot).
- Fail-closed on load re-validates decoded VALUES against the write-door invariants, especially when
  a value can alias a sentinel: a row `{"tag": 0}` decoded cleanly and `resolveAsOf` returned
  `(Instant(0), true, nil)`; `Instant(0)` ALIASES "no TX filter", so `AS OF SYSTEM TIME $tag`
  silently returned CURRENT belief. The key is NEW (no legacy data, unlike 58's propkey blank-name
  where load stays lenient), so the load door mirrors the write door `tagAsOf` (`at > 0`, non-blank name) and
  fails `ErrCorruptWire`. A structural-corruption test does not cover the value-level case.

## 60. A Retroactive In-Place Stamp Must Be Un-Applied At Every Belief-Reconstruction Door — Delete Is A Transaction-Time Tombstone

Trigger: the hard-Delete door stamps `DeletedAt`/`ValidTo`/`TxTo` IN PLACE on the final version
before moving it to history, a retroactive edit. Lesson 43 fixed `TxTo` via the visibility
predicate, but the delete-stamped `ValidTo` kills the row in VALID-TIME COVERAGE
(`nodeVersionBounds` overrides `vEnd` with `ValidTo`): every generic `QueryOpts.TxAt` read
(`ByLabel`/`ByType`/`All`, `NodesAtTx`, `NodeAtTx`) dropped deleted entities at every pin, while the
named as-of door (`NodeAsOf`/`NodesAsOf`, via `normalizeTemporalVisibleAtTxTime`) resurrected them
correctly. Found 2026-07-03 from the consumer side (tx-time `AS OF` pinning).
Rules:
- When a mutation retroactively stamps a stored row, enumerate the fields written (`DeletedAt`,
  `ValidTo`, `TxTo`) and every "as known at T" reader; each stamp must be disregarded when recorded
  after T. Visibility fixes (43) do not cover stamps consumed by a DIFFERENT axis.
- Land the un-apply at the shared seam: the four chain-based TxAt resolutions (`nodeAtLockedTx`,
  `relAtLockedTx`, `find{Node,Rel}VersionMatchingDuringTx`) share `filter{Node,Rel}ChainByTxAt`;
  normalize there, reusing the named door's `normalizeTemporalVisibleAtTxTime`.
- Never normalize in place on chain rows: deep-copy first (shared frozen store rows; `Temporal()` is
  the documented mutation exception frozen entities do not protect, v4.6.1 poisoning family).
- Test-clock discipline (`bitemporal_tombstone_test.go`): `Core.now()` has a monotonic >=1ms floor, so a mutation burst outruns the
  wall clock: derive pins from the entities' own `TxFrom`, not a slept wall-clock pin. The TxAt-only
  door probing valid time at WALL now (`resolveOpenEndInstant`) was a product bug fixed 2026-09-24
  (reads use `c.readNow()`, lesson 71 corollary). A flake that tests "fix" by waiting for the wall
  clock is a finding.

## 61. A "Current Transaction Time" Reader Must Consult The Commit Clock (Wall-Dominated), Not The Session High-Water Mark — The Latter Resets To Zero On Reopen

Stub -> owner 71 (corrects this lesson's premise "`lastInstant` need not be seeded on open because
the wall dominates every stamp": true only at <= 1 write/ms; 71 seeds the floor on open and advances
it on every foreign-stamp ingress).
Still needed: `Temporal().NowTx()` returns `c.now()` (advancing one tick, which RESERVES the instant
so the next mutation is stamped strictly greater), never the session-local `c.lastInstant.Load()` (0
after reopening a graph with data). 0 is the overloaded `QueryOpts.TxAt == 0` "no TX filter"
sentinel, so a pin of 0 resolves to CURRENT belief and includes writes made after the pin. Detector:
`TestNowTx_ReopenSafe` (on-disk badger).

## 62. An As-Of Resolver Must Not Fall Through Past A Retracted Newest Belief To An Older Open-TxTo Row — And "Newest" Is By Version, Not TxFrom

Trigger: the named as-of door (`NodeAsOf`/`NodesAsOf` and mirrors) on chain-scanning resolvers
(memory-native `nodeAsOfLocked`, the core fallback used by tiered) selected the newest version whose
TX interval COVERED the pin (`TxFrom <= txAt && (TxTo == 0 || TxTo > txAt)`). The v4.9.0 append-only
cascade (`SetNodeVersionInterval`) demotes the prior current WITHOUT stamping its `TxTo`, and a
later hard Delete tombstones only the FINAL version, so at a pin AT/AFTER the delete the filter
excluded the retracted newest belief and fell through to the open genesis (PRESENT) while badger's
reverse-scan correctly reports ABSENT. Repro: `Add(valid_from=1000)` ->
`SetNodeVersionInterval(id,2000,0)` -> `Delete(id)` -> `NodesAsOf(far-future)`. Second trap:
"newest" is the highest **version** with `TxFrom <= txAt`, not the highest `TxFrom`: `Update` stamps
via `validInstantAfter(now, versionStart)`, which can bump `TxFrom` above a LATER cascade row's
plain `c.now()`; version = allocation order = authoritative recency.
Rules:
- Select the decisive belief, then judge its retraction, never filter it out during selection: the
  newest recorded-by-txAt version, if superseded or deleted by the pin (`TxTo != 0 && TxTo <= txAt`,
  or `DeletedAt != 0 && DeletedAt <= txAt`), means ABSENT.
- "Newest" in an append-only bitemporal chain is by version, not `TxFrom` (`validInstantAfter` and
  the cascade break `TxFrom`/version co-monotonicity); mirror the store's descending-version scan,
  not a `(TxFrom, version)` proxy.
- Fix in the READ path only: keep append-only cascade discipline (demoted row's `TxTo == 0`), do not
  re-stamp on write.
Detector: `asof_cascade_delete_test.go` (memory/badger/tiered, node+rel mirrors: absent at/after the
delete pin, corrected shape between cascade and delete, original belief before) plus an as-of clause
in the bitemporal oracle harness cross-checking `NodesAsOf`/`RelsAsOf` against the version-ordered
rule.

## 63. An Unlocked Collect Window Before A Rebuild-Commit Needs A Write-Generation Guard; And A Rescan That Fetches Cache-Cold Rows Must Not Run Under The Caller-Held Write Lock

Trigger: badger `NodePropertyStats` deferred Min/Max rescan; two defects from the same RWMutex
non-reentrancy fact.
(a) Self-deadlock: holding `idxMu.Lock()` across the rescan deadlocks because the rescan's
cache-cold fetch (`prefetchNodeScan` -> `prefetchNodeNoFill`) takes `idxMu.RLock()`; the node-fetch
loop MUST run with `idxMu` released (memory can hold one lock only because its lookups are direct
map reads).
(b) Stale-rescan overwrite: releasing the lock opens a lost-update window; a concurrent `PutNode`
landing a NEW extremum between the unlocked collect and the `Rescan` commit is overwritten by the
stale snapshot, and since `Rescan` clears `dirty` the wrong EXACT value persists. Not a data race
(all access lock-ordered), so `-race` never flags it (repro: Max=2 vs true 999999).
Rule: whenever a read releases a lock to gather inputs and re-takes it to COMMIT a rebuilt
cache/aggregate, guard the window with a write-generation counter bumped under the lock by every
mutator (`PropertyStatsAccumulator.WriteGen`, bumped on `Observe`/`Forget`): read it under the lock
BEFORE the collect, re-read BEFORE committing, redo the collect if it moved (bounded, lesson 24). On
exhaustion return the LIVE snapshot WITHOUT committing a stale rescan and leave the pair `dirty` for
a later quiescent read (`Observe` keeps Min/Max monotonically correct for additions, so the fallback
never under-reports). Never widen a lock across a sub-call that re-takes it (coarse-lock "fix" =
deadlock (a)).
Detector: deterministic collect->commit interleaving via the real public door
(`TestBadgerStoreNodePropertyStatsStaleRescanOverwrite`, the red test);
`TestBadgerStoreNodePropertyStatsRescanGenerationExhaustion`; the concurrent-storm test asserts
VALUE correctness against a sequentially-computed ground truth (the old one checked only "no
deadlock / no error").

## 64. A Scan-Then-Overlay Reader Drops A Row Across The Commit Window — Snapshot The Overlay BEFORE The Badger View, Not After

Trigger: lesson 54 ("every overlay reader consults `flushing`") is necessary but not sufficient. The
full-history readers (`getNodeHistoryByPrefix` / `getRelHistoryByPrefix`) opened the Badger
`db.View` FIRST, then merged `rangePending` (flushing ++ pending): a row parked in `flushing` is not
in that View snapshot, a concurrent `flush()` commits it then clears `flushing` (order: swap ->
`wb.Flush()` -> `flushing = nil`), and the reader's later `rangePending` sees nothing, so the row is
in NEITHER view. Load-dependent (gap sub-microsecond idle, seen 2/30; widens under load), NOT a data
race, so `-race` does not flag it. Symptom: two doors resolving via the same per-ID function
(`nodeAtLockedTx` / `relAtLockedTx`) disagree (`Nodes.All(point)` vs `NodesAtTx`, `Rels.All` vs
`RelsAtTx`), or `NodeAtTx` returns `ErrNodeNotFound` for an entity whose `History()` returned rows
moments earlier.
Rules:
- Cache-side overlay + snapshot-isolated store are two clocks: read the MUTABLE buffer FIRST
  (snapshot the overlay into a local map before opening the `View`, then merge it over the scan
  results; Ta <= Tb closes the window), the immutable snapshot SECOND. `*HistoryVersionsFromPrefix`,
  `maxHistoryID` and the `pendingHistory*Overlay` ID scans were already overlay-first; the
  divergence between readers of one family is the bug class (54 sharpened from WHICH buffer to
  WHEN).
- Audit BOTH readers and delete-set-computing mutators: scan-first order in
  `truncateHistoryByPrefix` / `trimHistoryFromPrefix`, on-disk `maintainPropertyIndexesPurge` /
  `purgePropertyKeyDiskEntriesLocked` / `incomingIndexEntriesFromKeyspaceLocked` drops a key from
  the delete/retention set (ORPHANED index entry or distorted keepVersions window). All reordered
  overlay-first. For "set-vs-delete wins" mutators (incoming-index scrub) the scan only FILLS keys
  the overlay did not resolve (else it resurrects a parked delete); for append-only-delete mutators
  (property purges) a plain block swap suffices (duplicate deletes coalesce).
- Entity POINT reads (`GetNode`/`GetRelationship`) are exempt: every entity write does `cache.Put`
  (dirty) and `appendOps` in ONE `idxMu.Lock` section (flush snapshots under `idxMu.RLock`) and
  `markCacheFlushed` runs AFTER the commit, so "entity in `flushing` implies dirty in cache": a
  cache HIT covers it, a MISS means clean = durable. History/index keyspaces have no synchronous
  cache shadow, hence only they were exposed (as in 54).
- Amended 2026-10-09 (backlog 32): inside that section the ORDER still matters to lock-free readers
  (`GetNode` on a cache hit takes `idxMu.RLock` only on a miss). A with-history door changed the
  cache (new current) before appending the moved row to the history overlay; core resolvers read
  current then history in two lock-free calls, so a reader between saw the new current and a history
  without the moved row (NodeAtTx / NodeAsOf "absent" at a pin before the write). Rule: when a write
  changes two views a lock-free reader reads one after the other, publish them in the REVERSE of the
  reader's order (reader current-then-history, writer history-then-current; `publishMoveLocked`),
  keep the order in one helper, and test with a hook between the writer's two halves
  (`TestWithHistoryDoorsPublishHistoryBeforeCurrent`) and one between the reader's two reads
  (`TestPointDoorRace_MoveBetweenChainReads`).
Detector: test the WINDOW, not presence: park-only tests never catch scan->commit->clear. Hook
INSIDE the reader's scan->merge gap landing a full flush commit (`historyScanTestHook` +
`commitFlushingToBadger`): red on old order, green on new. A full-stack oracle run under a tiny
flush interval cannot hit a sub-microsecond window (passed with the bug present).

## 65. HyperLogLog Accuracy Tests (And Any Test Asserting An NDV Bound) Must Feed Well-Distributed Values, Never Short Sequential Integers

Trigger: ADR-0005 §3.1 tiered cross-shard NDV fold test (two shards, 50 distinct int64 each, 25
overlap, true union 75) with sequential `0..74` returned `NDV == 7`; the `HyperLogLog.Merge` was
correct (Min/Max/Count exact). Isolated: `AddString(fmt.Sprintf("%d", i))` undercounts on a SINGLE
sketch too (n=1000 -> ~90, n=10000 -> ~862, ~11x under). `AddString` uses FNV-1a, which avalanches
poorly on short inputs differing in the low-order byte; consecutive decimal strings correlate in the
TOP bits `addHash` uses for the register index (`idx := hash >> (64-precision)`), collapsing inputs
into few registers. Random strings or integers spaced by a prime step (`i*97`) estimate correctly
(75 -> ~74-75).
Rule: any test asserting NDV is CLOSE TO a known true count (not just "positive" / "not absurdly
high") feeds well-distributed values (random strings, hashed/salted values, prime-stepped integers),
never a tight run of small consecutive integers. Test-authoring rule only (documented in
`crossShardPropStatsStep`, `pkg/graph/store/tiered/tieredstore_property_stats_test.go`); not a
production defect: the hash choice and its accuracy contract (random inputs only) are out of scope.
`hyperloglog_test.go`'s accuracy regression (<5% at 10k, <3% at 100k) feeds only random strings; the
badger/memory `NodePropertyStats` concurrent tests use small sequential ints but assert only an
UPPER bound (`NDV > everDistinct+8`, `NDV < 1`), so no lower bound had been asserted. Whoever
tightens those bounds must feed scattered values or it flakes for a reason unrelated to the thing
under test. Evidence: `hyperloglog_sequential_test.go` (BACKLOG 16d).

## 66. A Pointer Swap Guarded By One Lock Class Races Every Reader Outside That Class — And "Targeted Race Tests" Before A Release Miss What The Full-Suite Storm Catches

Trigger: `GraphTx.restoreRegistries` swaps registry POINTERS (`c.labels`/`c.relTypes`) under
`c.mu.Lock`, which excludes `c.mu.RLock` readers but NOT the two readers outside `c.mu`: `Close`'s
final `persistRegistries` (after Close released `c.mu`) and ingest sessions' declare-on-prepare path
(`registryMu` only). v4.15.1 release CI caught Close-vs-Rollback via
`TestLifecycleStormCloseMidFlight` under `-race` on a slow runner; the fast dev machine never
reproduced it in 36 runs.
Rules:
1. A shared POINTER is swapped only while holding EVERY lock class its readers use: registry swap
   sites (tx rollback, import rollback, import-merge rollback) hold `registryMu` in addition to
   `c.mu.Lock`; `Close`'s persist takes `registryMu`. When adding a reader on a new lock class (or
   no lock), grep every WRITER and prove each holds your class.
2. The pre-release race gate is the FULL `make test-race`, never a targeted `-run` subset
   (storm/lifecycle tests run only in the full suite; CI's slower runners open windows a fast box
   never does). A release whose full race suite did not run locally gambles on CI.
Detector: `grep -rn 'c\.labels = \|c\.relTypes = ' pkg/` (registry swap-site audit).

## 67. A Raw-Byte Storage-Saving Estimate Is Not A Disk-Saving Estimate — Validate Every Wire-Compaction Proposal Against BLOCK-Snappy, Not Per-Row And Not Uncompressed

Trigger: v3-redesign lever estimates on RAW bytes, but Badger Snappy-compresses SSTable *blocks*
(default 4KB, `s2.EncodeSnappy`, `badger/table/builder.go`). Measured (`wire_b3_ondisk_gate_test.go`
/ `wire_b6_history_gate_test.go`, block-Snappy at 4KB in keyspace order): B3 (delta-encode 5 mid-map
timestamps) 2.99% raw -> 1.13% post-Snappy (the compressor already dedups low-entropy timestamps;
incompressible 64-hex hashes ~128 B/row dominate) -> DROPPED (it would have added the first custom
wire decoder + an `fv` bump). B6 (anchor+delta history, elides whole unchanged property structures)
57.65% raw -> 39.10% post-Snappy (delta rows compress worse individually, 2.27x vs 3.26x, but are
far smaller) -> KEPT.
Rules:
1. State any "shrink the wire" saving as post-block-Snappy over a realistic corpus in keyspace
   order, never uncompressed and never per-row (per-row overstates cross-row wins). History rows are
   keyed `0x07/<id>/<version>`, contiguous, so a repeated blob lands in one block: model that
   contiguity.
2. Removing low-entropy redundancy (timestamps, repeated small ints) is usually a mirage
   post-Snappy; removing high-entropy or whole-structure bulk (large distinct values, unchanged
   property sub-trees) survives.
3. Keep the measurement as an executable decision record (`_test.go` gate that re-runs the
   comparison) so a revisit re-measures.

## 68. Reconstructed Wire Properties Must Be Re-Sorted By KEY STRING, Not Token — And A Cross-Backend Differential Oracle Is What Catches It

Trigger: B6 `ApplyNodeHistory` merges anchor+delta properties into a map and emitted them sorted by
token (uint16; tokenized wire has `Key==""`), but `WireToNodeChecked` REJECTS non-strictly-ascending
key-STRING order (`property "blob" is not in strict sorted order after "region"`). Token order
equals key order only when tokens were assigned alphabetically, which `SetProperty` (pre-sorts)
happens to produce, hiding the bug in store-level tests; the core path tokenizes in validation
order, so `Update`-built chains fail.
Rules:
1. Code that REBUILDS a wire's property slice from parts (delta apply, merge, splice) emits
   key-string order (`storeutil.SortWirePropertiesByKey` after resolving `KeyToken->Key`); the
   decoder validates order, it does not re-sort. The truncate re-materialization path re-tokenizes
   (`ApplyPropertyKeyTokens`) before re-marshaling so a rewritten full row stays canonical.
2. Token order is NOT key order; never assume alphabetical assignment.
3. Detector: a memory-vs-badger DIFFERENTIAL ORACLE running an identical workload through both
   backends; for any storage-representation change (delta, compaction, re-encode) a same-backend
   round trip is not enough. Evidence: `history_delta_test.go` (BACKLOG 18s).

## 69. An Overlap-Based Candidate Prune Must NOT Be Applied To A Query Door Whose Predicate Set Includes NON-Overlapping Relations

Trigger: the B4 valid-time envelope prune (`PruneTemporalCandidates`) drops a candidate whose
`[minFrom, maxTo]` envelope cannot OVERLAP the query interval: sound for `During` / point doors
(predicate IS overlap), wrong for OPT10 `NodesRelating(from, to, rels)`, whose Allen set may include
`Before` / `After` / `Meets` / `MetBy` (precisely the non-overlapping relations); wiring it "for
consistency" would silently return empty for "ended before the window". The prune is a narrower
keyed to one predicate, not a general temporal accelerator.
Rules:
1. Before reusing a prune/short-circuit on a new door, check the new door's predicate is a SUBSET of
   what the prune assumes. An Allen door with any non-overlap relation takes the full fold (or a
   prune keyed to the actual relation set).
2. Scope the wiring explicitly and say why at the call site: the Step-1 prune is wired into
   `nodesByLabelPropertyDuringLocked` with a comment that it is NOT applied to the Relating doors.
3. Adversarial test includes the non-overlap relations: exact-set assertion over
   `{Before, After, Meets, MetBy}` (rule 16: "must return the relations envelope-pruning would
   drop"); a happy-path `{Overlaps}`/`{During}` test passes with the wrong prune.
4. Open interval ends are +inf, not a wall-clock "now+": the Relating classifier
   (`types.RelateOpen`) takes the query end RAW (0 = open) so Before/After/Meets classify against
   the true open edge; pre-resolving an open end (as the overlap `During` door does) misclassifies,
   so the two doors cannot share open-end handling.

## 70. `reflect`-Based Sorting/Comparison On A Hot Path Is A Perf Bug — But Not All Reflection Is

Trigger: BACKLOG 16f: `hnsw.go` `connect()` called `sort.Slice` (reflection-boxed swap) on every
neighbor-cap overflow in `insert()`; `lru.go` already warned against it. Fixed with
`slices.SortFunc` and a comparator mirroring the original tie-break.
Rule: no `reflect`-mediated operation (`sort.Slice`, `reflect.DeepEqual` as a typed-`Equal`
substitute, `reflect.Value` field walks) on a per-write or per-query hot path (once per node/rel
write, per property, per search/insert step). Prefer `slices`/`cmp`/`maps` or typed hand-written
code (`ValueStripe` inlined FNV-1a, BACKLOG 15g; `decodeMapKeyLen` msgpack decoders). Not blanket:
reflection stays correct where (a) the problem is reflection-shaped (`pkg/types` property
validation/deep-copy/equality over an arbitrary caller `any`) or (b) the path is COLD
(`RegisterPropertyStructType`, store-capability wrapper detection `embedsNativeCapability` in
`core.go` once per `New()`). The line is call frequency, not whether the identifier contains
`reflect`.
Extension (same session): also audit THIRD-PARTY calls that fall back to reflection.
`msgpack.Marshal`/`Unmarshal` is reflective unless the target implements
`CustomEncoder`/`CustomDecoder`: `NodeWire`/`RelWire`/`PropertyWire` do
(`wire_encode.go`/`wire_decode.go`), but the 10 change-log body wrappers (`NodePutBody`,
`RelPutBody`, ...; BACKLOG 15s) and the `HistoryDeltaEncoding` wrappers
(`NodeHistoryDelta`/`RelHistoryDelta`; BACKLOG 15t) do not, so `msgpack.Marshal(body)` reflects over
the outer wrapper even though the embedded `Wire` field dispatches to the fast encoder. Check not
just `grep '"reflect"'` but that every type crossing a reflection-capable
Marshal/Unmarshal/Encode/Decode on a hot path implements the library's opt-out interface; a struct
embedding an optimized field is not itself optimized.

## 71. A Wall-Dominated Monotonic Floor Is Not Reopen-Safe Once A Burst Outran The Wall — Seed It On Open And Advance It On Every Foreign-Stamp Ingress

Trigger: lesson 61 assumed the wall clock "dominates every historical stamp", so `lastInstant` need
not be persisted. False above **1 write/ms**: `Core.now()` floor is `next = max(wall, last+1)`, so a
burst stamps `TxFrom` up to `(writes - elapsed_ms)` ABOVE the wall, and the stamps are persisted.
Two ingress paths then admit a `TxFrom` above this session's wall: (1) reopen: `lastInstant` resets
to 0, `NowTx()=c.now()=wall` under-covers the burst and a NEW write is stamped below an
already-committed `TxFrom` (tx time goes backwards; silent, `TxFrom` is not hashed so `Verify*Chain`
passes); (2) replica apply / bootstrap import: the row carries the **primary's** stamp verbatim (NTP
skew or the primary's own burst floor).
Rule: a monotonic logical clock that must be correct across sessions/nodes cannot lean on "the wall
dominates"; persist a high-water watermark, reseed on open, AND advance the floor at every door
admitting a foreign or reloaded stamp. Fix, three doors: (a) persist the floor as a durable MetaKV
watermark on `Close`, reseed on open (`persistInstantFloor` / `seedInstantFloor`); (b) advance it
for every applied record at the ONE central apply seam (`recordCommitStamp` in the
`applyChangeRecordLocked` wrapper: replica tail AND delta-merge); (c) advance it per replayed wire
on `Import`. Only system-minted TX stamps pull the floor (`maxCommitStamp` = max of
`TxFrom/TxTo/UpdatedAt/DeletedAt`), NEVER `ValidFrom/ValidTo` (caller-asserted world time, may lie
in the future). Caveat: persist-on-close restores the floor exactly on a clean shutdown; after a
crash the floor falls back to the wall and self-heals (pre-fix behavior).
Corollaries:
- "Every door" is a MOVING set: re-audit on every rebase (written on v4.11.2, landed on v4.24.0, two
  doors added meanwhile). (1) `ChangeForeignIncoming` (tag 11, ADR-0010 Model A) is the
  cross-machine incoming half-edge stub whose body IS a `ChangeRelPut` body carrying the ORIGINATING
  partition's `TxFrom`; the tag-driven `recordCommitStamp` switch returned 0 for it. Unhandled tags
  are now enumerated with a reason each, so the next added tag is a deliberate decision. (2)
  `Admin.Reset()` / `ChangeClear` wipe the whole MetaKV keyspace incl. the watermark: classified
  `reapPolicyPreserve` (BACKLOG 13l; the commit clock is this NODE'S tx-time position, like
  `idSlotLeaseMeta`); needs no capture-before-Clear because the authority is the in-memory
  `lastInstant`, which `Clear` never lowers.
- The READ side too (2026-09-24): any implicit "now" a read compares versions against must dominate
  every stamp: TxAt-only doors and open-ended interval reads probed valid time at the bare wall
  clock, below `UpdatedAt` of floor-stamped versions, and returned an older superseded version
  (different per call and per primary/replica; found by the ADR-0011 S2 oracle as "nondeterminism").
  Reads use `c.readNow()` = max(wall, floor).
Detector (each fails RED without its door's fix):
`TestNowTx_ReopenAfterBurst_MonotonicFloorAnachronism` (frozen-clock burst -> reopen),
`TestNowTx_ReplicaCoversAppliedFutureTxFrom`, `TestNowTx_BootstrapImportCoversFutureTxFrom`,
`TestRecordCommitStamp_CoversForeignIncomingStub` (a real encoded rel-put RE-TAGGED),
`TestInstantFloor_PreservedAcrossReset` (badger only; memory `Clear` leaves its meta intact).

## 72. An Externally-Filed Perf Diagnosis Names A Plausible Mechanism — Reproduce And Profile Before Touching The Named Component

Trigger: a consumer rel head-pin ask (2026-07-29) reported a real ~8x depth degradation and blamed
`RelBeliefWatermarkCapability` "not consulted on `OutgoingForNodesAtPin`". Code reading contradicted
it (as-of doors have their own current-row fast path); a local repro benchmark + CPU profile found
the gate elsewhere: `ForEachDeletedRelID` -> `AllRelHistoryIDsFrom` stepping the badger iterator
through every `0x08` history row to enumerate distinct IDs, O(total version rows) per query.
Rules:
- Treat only the SYMPTOM as data: reproduce in-repo (mirror the reporter's fixture shape), profile,
  let the profile name the component. Fixing the named-but-innocent component would have added a
  no-op watermark gate and left the real O(rows) scan.
- Profiling under async write buffers: a benchmark reading right after seeding measures the
  PENDING-BUFFER overlay (`rangePending`) as much as the read path; drain the flush tick (sleep past
  `FlushInterval`) before `b.ResetTimer()`.
- Distinct-ID scans over `prefix/<id>/<version>` keyspaces: `it.Next()` + last-ID dedup is O(total
  rows); once an id is decided `Seek` to `prefix/<id+1>` (O(distinct ids)). Keep row-wise stepping
  for a pending-delete-masked row (the id may still be emitted via a surviving row).
Detector: `TestBadgerStore_AllNodeHistoryIDsFrom_PendingDeleteMasksSomeVersions`.

## 73. The Chain Resolver's Input Order Is A Semantic Input — A Sorted Chain Fed Back In Flips The Monotonic-vs-Cascade Branch

Trigger: `resolveNodeChain`/`resolveRelChain` take the chain in ASCENDING-VERSION order;
`sortNodeChainForResolve` both DETECTS non-monotonicity relative to that order AND sorts IN PLACE.
The selection-skeleton fast path (TemporalMetaHistoryCapability: select on skeletons, hydrate the
winner, re-resolve) fed the already-sorted slice back in; it looked MONOTONIC, took the
positional-tiling arm instead of the cascade own-bounds arm and lost a cascade-reopened row (fast
path "no version" vs full path the reopened row; per-row inputs byte-identical, only ORDER
differed).
Rules:
- A resolver that both classifies its input's shape AND mutates it in place makes input order part
  of the API contract: each invocation gets its own pristine copy, and the contract is documented in
  the resolver (funnel comment in `chain_resolver.go`).
- "An accelerator must never change answers" needs an equivalence oracle wired into CI, not targeted
  fixtures (the divergent class here: created-closed + cascade-reopen + post-cascade-update +
  delete, which nobody hand-writes).
Detector: an env-gated divergence cross-check inside the door (run fast AND full, panic with both
chains + selection keys on divergence) under the randomized oracle harness; found it in seconds
where fixture guessing failed.

## 74. "Back-To-Back, So The Window Is Negligible" Is A Probability Argument, Not A Correctness Argument — Every Overlay Reader Must Capture Before The Snapshot, Structurally

Trigger: one `make check` (heavy parallel load, nearly-full disk) failed
`TestBitemporalOracle_BadgerCommitWindow` with a genuine MISMATCH (rel vanishing from
`ByType`/`*AtTx`) that did not reproduce in 60 stress runs. Auditing every overlay-merging reader's
capture order instead found `reverseScanHistoryVersion` capturing the overlay INSIDE `db.View`
(after badger assigned the snapshot instant), its comment arguing safety "because both happen
back-to-back": the lesson-64 dropped-row window. The bulk variant (`...InTxnSnapshot`) already
pre-captured; the single-entity path argued itself an exemption.
Rules:
- An ordering invariant ("overlay before snapshot") admits no fast-path exemptions justified by
  expected timing; make the ordering structural and delete the argument.
- Audit recipe when a rare load-dependent oracle failure will not reproduce: stop rerunning,
  enumerate every reader merging the write-buffer overlay with a store snapshot and check the
  capture order:
  `grep -rn 'pendingHistoryVersionOverlay\|pendingHistoryIDOverlay\|snapshotHistoryOverlay\|rangePending(\|lookupPending(' pkg/graph/store/badger/ | grep -v _test`;
  each hit captures BEFORE its `db.View`/`NewTransaction` or receives a pre-captured snapshot. (The
  first grep omitted `rangePending` and five Badger-first readers survived: the K1
  membership-sidecar builds (cause of the 2026-09-24 flake), the belief-watermark builds,
  `relationshipIndexKeysForRel`.)
- Lazy builds: a structure built ONCE then maintained incrementally turns a transient drop into a
  permanent one (a row the build misses is never revisited). Holding `idxMu.Lock` during the build
  excludes writers but not the commit of a flush parked before it. A door that enqueues without
  `idxMu` and decides from an atomic "built" flag is check-then-act against the build: read the flag
  and enqueue under `idxMu.RLock` (`enqueueVersionAgainstLazyBuilds`).
Caveat (honesty): the observed mismatch was on point/set doors while the flawed ordering was in the
as-of walk, so the attribution is PLAUSIBLE, not proven; the fix is justified by inspection
regardless. 2026-09-24 follow-up: the symptom (entity missing from `ByType`/`ByLabel`) reproduced in
~1% of loaded runs; logging in the sidecar build tied all 4 dropped nodes to rows a flush committed
during that build (CHANGELOG, Unreleased), so the earlier observation was most likely the sidecar
build.
Evidence: failure log in this repo's CHANGELOG entry.

## 75. A Green Workflow Must Mean Its Gates Passed

Trigger: `continue-on-error` plus a prose-only finding baseline turns a security scan into an
unaudited log stream.
Rule: pin analyzer versions, document audited suppressions at the call site, make the scan exit
status blocking.

## 76. A Type Tag Covers One Level — Test Every Kind At Depth, Through The Bytes

Trigger: the property wire tags only the top-level value; values nested in `[]any` /
`map[string]any` crossed msgpack untagged, and eight integer widths, six typed slices,
`map[string]string`, typed nil and registered structs came back as other kinds for years (hash chain
failed after reopen). The 4.36.0 temporal fix enveloped one kind only (backlog item 3).
Rules:
- A codec round-trip test enumerates EVERY kind the allowlist accepts, at depth 0, 1 and 2 in both
  container kinds, through the real bytes (marshal, checked decode), comparing value AND Go kind
  (hash bytes for NaN / -0).
- An envelope adds wire depth: apply the property depth limit to the reconstructed value, never the
  raw wire value, and bound the raw walk by the decode limit.

## 77. A Handover's Test List Is Break-The-Code Only, Written Red First

Stub -> owner 78 (red break-the-code test first). Still needed, for handovers: the tx-backfill
handover (2026-10-09) listed happy-path cases ("t+1 succeeds") as tests; every planned test instead
names the faulty implementation it catches ("ignores t and stamps now", "moves t silently") and the
breaking input (zero, negative, future, equal, reversed, duplicate, boundary t-1/t/t+1, concurrent,
rollback, replica, per backend); the accepted case is only the counterpart assertion inside such a
test. A handover's §tests is written before its §file changes and run red before the first line of
code.

## 78. A New Item Gets The Clean Root-Cause Solution And A Red Break-The-Code Test First — No Shortcut Is Delivered Quietly

Trigger: René (2026-10-09, v4.44-v4.46 waves): "Always clean solutions to new items.. no hacks or
shortcuts .. always write a no-happy-path test before the code." Repeats 77 and the CLAUDE.md TDD
rule for every NEW item, delegated or not.
Rules:
- The test naming the faulty implementation and its breaking input is written and run RED (output
  kept under `tasks/evidence/<item>/`) before the first line of the fix. A test that already passes
  before the fix is a guard, labelled as such, never a replacement for the red one. Mutants (one
  real edit each) prove the tests catch the fault.
- The fix lands at the root and at the shared seam the door family funnels through (lessons 58-60),
  not behind a flag, skip, per-caller special case, consumer heuristic, or a documented limit
  standing in for possible work. A documented limit is allowed only after the sound solution was
  tried and shown impossible (state why: lock order, format change), and goes into the backlog with
  its red test.
- When delegating, the prompt carries both rules verbatim and the reviewer checks them: red evidence
  exists, guards labelled, no test skip hides a known failure. A change I make myself (even lint or
  release edits) goes through the same gates; a pure refactor is covered by existing tests and says
  so in the commit.
Detector: `grep -rn 't.Skip' pkg | grep -v 'sharded does not support'` against the change; evidence
files whose red run is missing; "unaffected" claims without a test.

## 79. Process Cost: Batch Releases, Author-Run Gates, One Review Round, Short Briefs

Trigger: René 2026-10-10: "i think there is a lot of inefficient work" after a session of six
releases, two to three review rounds per branch, lint/security failures found only after merging,
30-file reading lists in every brief, eight agents for a documentation pass whose findings were
mostly stale wording, two stopped agents, many unrequested backlog items.
Rules: (1) release in batches as PATCH versions (one gate, one consumer build, one message), not per
merged branch; (2) the author runs `make lint-docker` and `make security-docker` before reporting
(in the brief: reject a report without them); (3) one reviewer round per feature branch, the fix
round goes to the same agent, a second review only for a blocker; doc-only and comment-only changes
get the docs tests and a diff read, no reviewer; (4) a brief names the 5-8 files that matter and
points to HANDOVER/AGENTS for the rest; (5) file a backlog item only for a consumer request or a
failing test; (6) never stop a running agent unless the user asks, let it finish and use the result.
Not cut: the failing test first, break-the-code cases and the release gate (77/78): they found the
real bugs.
Detector: more than one review round on a branch, a lint finding after a merge, a brief with more
than ten file names, a backlog item with no named requester or test.


