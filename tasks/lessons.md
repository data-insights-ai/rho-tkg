# Lessons - tkg/v4

Rules distilled from real bugs: Rule, Trigger, Detector; long evidence lives in git history and `CHANGELOG.md`. Add an entry only
for a new recurring pattern, else edit the owner. Numbers and `## N.` headings are cited from code, docs and the backlog ("lesson
N"): never renumber or reuse. "merged into N" / "superseded by N" stubs keep a number alive; the newest rule wins.

## Rules digest (standing rules; the number owns the detail)

- Trace the cause before asking or adding; consolidate duplicate paths, notes and rules instead of layering, docs
  rewritten in place (84).
- Keep todo.md and HANDOVER current: `scripts/check-ledger.sh` after every merge and before every release (80).
- Act, do not offer: open work with a clear next step (consumer request, review finding, patch batch) starts now; ask only for the
  irreversible or materially different (81). Tick the ledger and doc targets at every release (80, 41).
- Red first: a failing break-the-code test before the fix, a clean root-cause fix at the shared seam, no "known limitation" in
  place of work (78, 27, 77).
- Releases: MINOR for a really new feature, PATCH otherwise (René 2026-10-14; was patch-only since 10-10), batched, one gate; behaviour changes via the deprecation ritual, no new named exceptions (79); full
  `make test-race` before a tag (66); a green workflow means its gates ran (75).
- Reviews: one round per branch, authors run lint + security first; models: builds Opus, code reviews Opus high, final Fable
  review before tagging, doc work Sonnet (79).
- A contract over a family of doors lands in every door; test cross-door agreement (58, 59, 42, 57, 69).
- Trust boundary: validate IDs and decoded bytes at every entry, `SafeUnmarshal`, allocate by bytes delivered, verify imported
  hashes (1, 6, 44, 47, 48).
- Time: `c.now()` never wall clock, reads `readNow()`, floor seeded on open (20, 71); corrections append (46); `TxTo` never bounds
  valid time (43); newest belief by version (62); resolver input order is semantic (73).
- Locks: entity locks before Store writes, no reentrant RWMutex, bounded retries, swap pointers under every lock class (3, 9, 22,
  24, 66); overlays: capture before the snapshot, set-vs-delete per key (54, 57, 64).
- Persistence: rebuild at open and fail closed; change-log in-backend; flush work before the watermark (10, 40, 49-53).
- Tests: every public method, Node/Rel parity, production-shape perf, every codec kind at depth, scattered NDV inputs, oracles for
  accelerators (14, 15, 65, 68, 73, 76).

## 1. Validate At Every Boundary

Rule: zero and negative IDs are malformed (except where an API defines `0` as cursor / unset / "all types"); every
store mutation, direct and history read, graph mutation target and repair helper calls `store.ValidateNodeID` before
lookup, scan or no-op. BAD: `if id == 0 {...}; lookup(id)` (negatives fall through).
Trigger: a store/graph entry point looks up, scans or no-ops on an entity ID.

## 2. Hash Canonical State

Rule: hash the canonical state after token normalization and registry resolution (`resolvedCanonicalLabels`, never raw
labels); fixing one call site means grepping all. A relationship mutation that recomputes integrity refreshes
`FromNodeHash` / `ToNodeHash` from locked current endpoints first.
Trigger: any `ComputeNodeHash` / `ComputeRelHash` call site. Detector: `rg -n 'ComputeNodeHash|ComputeRelHash' pkg/`.

## 3. Lock Before Store Mutation

Rule: take the entity locks (`g.entityLocks.LockEntity(id)` + deferred unlock) before the Store write; transactions and
batches call internal lock-free helpers only while holding the tx/batch graph lock.
Trigger: a graph mutation that writes to the Store. Detector: `rg -n 'store\.(Put|Delete|Replace)' pkg/graph/`.

## 4. Preflight Then Apply

Rule: separate validation/read gathering (no writes) from the apply phase; if an apply step can still fail, add
rollback or return per-operation errors. BAD: delete-in-loop (partial mutation on a mid-loop error).
Trigger: a multi-row Store operation.

## 5. Multi-Shard Moves Need Rollback

Rule: a later failing step (e.g. `src.DeleteNode`) rolls back the destination writes: no duplicates, no orphaned split
indexes; repair tools are not a substitute for transactional cleanup.
Trigger: Tiered archive/restore, migration, cross-shard relationship moves.

## 6. Store Is The Trust Boundary

Rule: deep-copy entities on put/get and validate payload invariants; treat MsgPack as untrusted (reject key/value ID
mismatches, invalid tokens, temporal ranges, malformed properties, corrupt counters/metadata before building live
entities). Defensive-copy fallbacks must not panic on legacy shapes (typed zero values for nil reflect elements).
Trigger: Store put/get paths; Badger wire decode.

## 7. Deleted Entities Still Matter

Rule: temporal, tx-time, diff, snapshot and integrity queries merge current and history rows
(`mergeCurrentAndHistoryIDs()`, not `AllNodeIDs()` alone); verification tolerates current-row deletion when history exists.
Trigger: any query that must see entities deleted before the pin.

## 8. Iterate Without Materializing Everything

Rule: callback iterators (`ForEachNodeID`) or paged scans, not `AllNodeIDs` + `mergeIDSlices`; callbacks run outside backend locks and
Tiered shard checkouts. Tiered scans that dereference shard stores pin every handle `Close` can close (`checkoutStore`,
`checkoutArchive`), check out one shard batch at a time, and fan out with bounded pools.

## 9. sync.RWMutex Is Not Reentrant

Rule: never call exported graph methods from code already holding `g.mu` / `c.mu` (BAD `g.mu.RLock(); g.Nodes().Get(...)`);
use unexported `*Locked` / `*Unlocked` helpers. Still binding in rollback, batch execute, snapshot, export (see 31, 63).
Trigger: graph internals holding a lock.

## 10. Persistence Must Rebuild In-Memory State

Rule: anything affecting query correctness that outlives a process has a durable definition and is rebuilt at startup
(`CreateIndex` persists, `loadIndexes` rebuilds). Counters and registry metadata fail closed when corrupt; persistent
backends mark close-in-progress before stopping flush workers so public ops fail closed once shutdown begins.
Trigger: a new index, counter or registry.

## 11. Index Creation Is A Three-Phase Operation

Rule: install an empty placeholder first (concurrent writes see it before build I/O), build from a snapshot, install only
if not mutated, using dirty/mutated tracking, never "does the built index already contain this ID?".
Trigger: building an index while writes run.

## 12. Sentinel Errors Must Be Precise

Rule: skip only the sentinel the operation tolerates (`errors.Is(err, ErrRelNotFound)`); I/O, corruption, lifecycle and
invalid-input errors surface. Trigger: `if err != nil { continue }`.

## 13. Version Identity Is Not Slice Position

Rule: history can be truncated; use `entry.Version()` and entity IDs as identity, never array positions.
Trigger: `if i == 0` over history.

## 14. Public API Coverage Is Direct

Rule: every public method has a direct test (delegation coverage is not enough); Node/Relationship mirror types get
parity tests including nil, sentinel and malformed input (AGENTS.md Testing Rules 1-2).

## 15. Performance Tests Need Production Shape

Rule: tiny fixtures hide O(N); keep microbenchmarks plus production shapes (baseline graph ops, high-fanout
relationships, history-heavy temporal queries, export/import, batch writes, Tiered hot/warm routing).

## 16. Lazy Init Flags Must Be Synchronized

Rule: the separate initialized marker is atomic (`initialized.Load()`) or guarded by an exclusive lock; `sync.Once`
serializes the init function, not unrelated reads of a plain flag.
Trigger: zero-value lazy init that can run under a shared read lock.

## 17. Rollback Logs Track Pre-Transaction Existence

Rule: `createdIDs` holds only IDs absent at `BeginTx`; rollback restores the pre-tx row and never deletes it as
"created". Key create/delete rollback logs by pre-tx existence, not by the operation that produced the current row.
Trigger: caller-specified imports reuse an ID deleted earlier in the same tx.

## 18. Map-Backed Query Results Need Explicit Ordering

Rule: any public query built from maps/dedup sets sorts by entity ID before returning (`sortedKeys(seen)`); never expose
Go map iteration order.

## 19. Native Fast Paths Must Respect Store Wrappers

Rule: tests/consumers embed in-tree stores to inject failures or stale reads; a native shortcut must not bypass those
overrides. The generic Store contract path is the default; exact-store shortcuts only for exact in-tree stores
(`isExactNativeStore`), or prove wrappers cannot override the optimized operation. Trigger: `store.(fastPath)` assertions.

## 20. Mutation Transaction Time Must Be Monotonic

Rule: stamp `TxFrom`, `TxTo`, `UpdatedAt`, `DeletedAt` and mutation events with `c.now()` (per-Core monotonic ms), never
`time.Now().UnixMilli()`; wall-clock ms repeat or go backwards and collapse tx-time version intervals (see 71).

## 21. Batch Visibility Must Match Consumer Scheduling

Rule: a producer-side batch mutex does not make the batch atomic for consumers that observe queues directly; publish in
the order that keeps the consumer contract under partial observation (critical first), or gate consumers on the same boundary.

## 22. Peek-Then-Lock Must Revalidate Identity

Rule: after locking the IDs found by an unlocked peek, re-read and revalidate the row identity (imports can reuse
deleted IDs with a different shape); retry if it changed, within the bound of 24.

## 23. Property Index Keys Are Equality Semantics

Rule: property-index value keys are not display strings; they preserve the property equality contract incl. special
floats (exact bit keys where `strconv.FormatFloat(v,'g',-1,64)` collapses NaN payloads), so fallback scans and indexed
lookups return the same set.

## 24. Bound Every Retry Loop

Rule: every retry loop has a max-iteration ceiling (`for range maxRetries`, error "changed after N retries") so a hostile
workload cannot deadlock the caller; mirror the ceiling across symmetric methods (`deleteNodeInternal` and
`lockRelationshipCurrentEndpoints` both use 10). Trigger: peek-then-lock retries.

## 25. Equality and Hash Have Different Contracts (Float Bit-Pattern Case)

Rule: `PropertyValueEqual` is the CAS short-circuit (NaN == NaN, +0 == -0); the integrity hash is the durable identity and
preserves IEEE-754 bit patterns. The asymmetry is intentional and tested: "equal CAS" does not imply "equal hash";
callers exchanging data with NaN-canonicalizing systems canonicalize at the boundary.

## 26. Public Sub-API Forwarding Must Match Internal Contract Width

Rule: each sub-API package declares its own minimal `Ops` interface and `*core.Core` satisfies each implicitly; when
collapsing or splitting sub-APIs, shrink each `Ops` to the new shape (old methods may stay on the internal type).

## 27. Don't Ship "Documented Known Limitations" In Place Of Real Fixes

→ merged into 78 (a limit is documented only after the sound fix was tried and shown impossible). Test: "if this had
bitten me 6 months ago, would I ask why we shipped it?". Evidence: the B2 O(total history) adjacency fold shipped as a
v4.1 note; the real fix was small (`DeletedIterationCapability`, see 28).

## 28. Adjacency Folds Need Only The Seed's Ex-Adjacency

Rule: split a shared helper before optimizing one caller. `forEach*CandidateID` stays all-history (an entity whose CURRENT
label is Y but was X at t must be visited); `forEachRelAdjacencyCandidateID` folds only each seed's ex-adjacency (rels that
named the seed in a row its live adjacency no longer holds: deleted rels, earlier lives re-imported with other endpoints;
backlog 50), kept by the store at every delete and history-row write. Collapsing all folds to deleted-only broke label queries.

## 29. PublishBatch "Atomic" Ordering Survives Saturation Only With A Floor

Rule: `AsyncEventBus.PublishBatch` enforces a per-batch priority ceiling in the dispatcher (`batchPriorityCeiling` atomic
Int32, raised per pass, cleared at batch end; skip `priorityOrder[i]` for `i >= ceiling`) so a `BackpressureBlock`
wake-up cannot dispatch a pre-existing lower-priority event mid-batch; liveness kept.
Detector: stage a lower-priority event, saturate with QueueSize below the per-priority slice, assert all batch events
dispatch first (`TestAsyncEventBusPublishBatchBlockWakesBeforeFullQueueWait`).

## 30. File Size Is A Signal, Not A Rule

Rule: split by "what does this return", not by line count (900 lines doing one thing is fine, a 200-line grab-bag is not;
a shared receiver `c *Core` is not cohesion). Evidence: `store_capabilities.go` split into capabilities, validation,
copy and fetch files.

## 31. Tx Holds The Write Lock — Mirror Every Read Accessor

Superseded by Path B (v4.1.0): `BeginTx` no longer holds `c.mu.Lock`, the tx-side mirrors in `tx_consistent_reads.go` are
for clarity only. Revives only if lifetime-`c.mu.Lock` returns: every `c.mu.RLock` accessor would deadlock (9) and need a
`(tx *GraphTx) Foo` mirror. Detector: `grep -nE "c\.mu\.RLock|c\.readUnderRLock" pkg/graph/internal/core/*.go`.

## 32. Don't Conflate Transaction Time With Valid Time In The Resolver

Rule: the valid-time end of a version is `next.ValidFrom` (valid time of the next state), falling back to
`next.UpdatedAt` only when unset, never `UpdatedAt` (the TX time of the supersede) alone (`nodeVersionBounds` /
`relVersionBounds`). Failure: a caller-supplied `tkg_valid_from` did not close the prior tile and `NodesAtTx` fell back.
Trigger: resolver code deriving `vEnd`.

## 33. Update Must Not Inherit Valid-Time From The Previous Version

→ merged into 42 (Update is one of the deep-copy mutation doors; the rule, the legacy shim and the detector are there).

## 34. AsOf TX Queries Default To Now — Pass TxAt For Bitemporal Reads

Rule: `QueryOpts.TxAt == 0` means "no TX filter" (VT match across the union of all history rows, including writes not yet
happened in the caller's timeline); kept for pre-bitemporal call sites, NOT "as of now". New bitemporal point queries set
`TxAt` explicitly.

## 35. Cascade Edit Rewrites History Rows In Place

Superseded by 46: the cascade is append-only. Still true: eclipsed rows use the zero-width sentinel
`ValidTo = ValidFrom + 1` (the store rejects `ValidFrom == ValidTo`; one-tick rows are ordinary spans since 2026-10-09);
split fragments get fresh versions (`maxVersion+1+i`); the cascade row becomes current iff `newVT == 0` and no surviving
row has a later open-ended `ValidFrom`; temporal metadata is not in the content hash.

## 36. Migration-Gated Resolver Heuristics Need A Runtime Flag

Rule: when a data migration removes the need for a resolver heuristic, keep the heuristic for stores that cannot run the
migration (no capability, mock or injected stores), gated on a per-core flag set by the post-`New` migration runner
(`if c.bitemporalMigrated || !heuristic(...)`); removing it outright breaks mock stores whose iteration errors.

## 37. Capacity-Soft Token Registries Don't Fail Writes

Rule: at 65536 entries `GetOrCreate` returns `(0, nil)` with a sticky warn-once, never `ErrRegistryFull` (property-key
cardinality is hard to bound; failing turns degradation into an outage). Encoders MUST treat token 0 as "no token, write
the raw key string".

## 38. Validation Hooks Are The Right Place For Side Indexes

Rule: hook side indexes (property-key registry, cardinality stats) into the Core validation layer
(`validateOwnedPropertyEntryForCreate`, `validatePropertyUpdates`): it sees every key on Add and Update with Core in scope.
Not `types.PropertySlice.Set` (`pkg/types` is fundamental, circular-dependency risk), not the wire encoder (no Core context).

## 39. Custom EncodeMsgpack Bypasses Struct Tags

Rule: a new msgpack-tagged field on a struct with a hand-written `EncodeMsgpack` (`PropertyWire` in `wire_encode.go`) is
also appended to the custom encoder, replicating omitempty by hand; plain `msgpack.Marshal` round trips do not catch it.
Detector: `grep -nE "EncodeMsgpack|EncodeMapLen" pkg/graph/internal/storeutil/` lists every encoder to update.

## 40. Load Registry Before Index Rebuild

Rule: backend-internal decodes that run during init have their dependencies resolved first: load the property-key
registry from meta KV BEFORE `loadIndexes` calls `bs.decodeNodeWireForKey`, else tokenized rows fail decoding and
`GetNode` returns `ErrNodeNotFound` for a row that exists on disk.

## 41. A Version Bump Is Not Done Until Every Doc-Consistency Target Matches

Rule: a commit that adds a numbered `## [x.y.z]` CHANGELOG heading also bumps `Status: vX.Y.Z` in `AGENTS.md`, the title
in `docs/architecture.md` (and `README.md`), and keeps the go.mod Go version equal to README/AGENTS/architecture; "the
CHANGELOG says docs were updated" is not evidence, the test is.
Detector: `go test -run TestDocsMetadataMatchesSourceOfTruth ./pkg/graph/internal/core/` before committing a release.

## 42. A Mutation Door That Deep-Copies Current Must Clear Inherited Valid Time — EVERY Door, Not Just Update

Rule (absorbs 33): clear `current.Temporal().ValidFrom` / `ValidTo` before re-applying caller values, so `ValidFrom != 0` on a
non-genesis version means caller-supplied. It holds for every door that DeepCopies current and writes a `*WithHistory` row
(`updateNodeInternal`, `add/removeNodeLabelInternal`, both property-CAS sites) except doors that close the interval on purpose
(delete tombstones, `CloseVersion`); the `nodeVersionInheritedValidFrom` heuristic is only a shim for pre-Phase-1 data.
Detector: `rg -n 'WithHistory\(' pkg/graph/internal/core/*.go` and the cross-door test `TestTemporalTwoDoorsAgreeOnLabelQueries`
(single-door tests cannot see it: all doors share the broken resolver input).

## 43. Superseded Is Not Retracted — TxTo Must Not Bound Valid-Time Answerability

Rule: `visibleAtTx := TxFrom <= txAt` (recorded by then), never `&& (TxTo == 0 || txAt < TxTo)`: `TxTo` marks supersession,
not retraction, and the row stays the authority for its valid-time slot in later belief states (32's class, in the
visibility rule). Pointers returned from frozen scan rows must be independent (`Temporal()` / `Integrity()` copy-on-frozen).
Detector: v0 VT=[1000,inf), v1 VT=[2000,inf): `NodeAtTx(1500, now)==v0`, `(2500, now)==v1`, `(2500, txBetween)==v0`,
`(*, txBeforeFirstRecord)==nothing`.

## 44. A Trust Boundary That Stores Hashes Must Verify Them On Entry

Rule: besides structural validation (6), import recomputes each row's content hash against the claimed one AND verifies the chain of
every imported entity after replay (content checks cannot see a flipped `PrevHash`); any mismatch gives `ErrCorruptExport` + rollback,
and every structural failure (truncation mid-record included) wraps that sentinel.
Detector: truncate an export at many offsets and flip single bytes: each attempt fails `errors.Is(ErrCorruptExport)` with ZERO
partial state, or yields a graph whose entities all pass `Verify*Chain`.

## 45. Shrinking A Resource Bound Must Account For State Written Under The Old Bound — And A Dependency's Assertion May Be os.Exit

Rule: (1) state written under the old bound may be unopenable under the new one (Badger replays each WAL, created at 2x the
MemTableSize that wrote it, into an arena sized by the CURRENT one): owe a migration (`MigrateOversizedWAL`). (2) A dependency
invariant violation may be `os.Exit` ("Arena too small" via `y.AssertTruef`), not a `recover()`able panic, probe opens included:
detect the unsafe condition before handing the directory over (scan `.mem` sizes), then migrate or fail closed (`ErrOversizedWAL`).
Detector: test the `os.Exit` with a subprocess re-exec doing the RAW dependency open; mutation-verify "knob is applied" tests.

A successful self-SIGKILL request may return before process termination. Crash
callbacks must never return to the driver or run cleanup after signal success:
use the established timer-sleep loop until death (return only on signal error).
The parent must require actual SIGKILL and assert its deadline did not expire;
a timeout kill cannot witness the intended crash seam.

## 46. A Correction Is A New Belief — Never Mutate A Stored Row To Express It

Rule: a valid-time correction recorded at `now` appends fresh rows stamped `TxFrom = UpdatedAt = now`; stored rows are immutable and
`TxFrom` is never back-dated (the old in-place rewrite made `NodeAtTx(_, oldTxAt)` reconstruct a belief that never existed and left
two TX-open rows that broke the native `NodeAsOf` early-stop). The resolver filters the chain to `TxFrom <= txAt` and tiles the rest
in effective valid-from order, overlaps broken by the newer belief (higher `TxFrom`, then version). Pairs with 43.
Detector: two-phase on all backends: create, correct at a later tx, query `*AtTx` BEFORE it: pre-correction values, no holes, no leak.

## 47. The Decode Step Is Part Of The Trust Boundary — And `recover()` Cannot Catch A Fatal Stack Overflow

Rule: decode persisted/imported bytes only through `storeutil.SafeUnmarshal`, never raw `msgpack.Unmarshal`. msgpack crashes the
process two ways: a reflect panic (repeated map key on an interface field) and a fatal stack overflow on deep `Value any`
nesting. ORDER matters: `guardMsgpackDepth` (non-recursive scan, rejects depth above 64, never under-counts) first, THEN
`defer recover()`; both return `store.ErrCorruptWire`. 64 not lower: legitimate wire with a 32-level property sits at ~35.
Sanctioned exception: `scanWireTemporalMeta` (AGENTS.md).
Detector: `grep -rn 'msgpack.Unmarshal(' pkg/ | grep -v _test`; `FuzzWireToNodeChecked`; assert `ErrCorruptWire` on a 200000-deep blob.

## 48. Allocate Proportional To Bytes DELIVERED, Not To An Untrusted Size/Count Field

Rule: `make(T, n)` / `Grow(n)` / map hints from a decoded length are bounded by bytes actually delivered or a small constant, never the
declared value even under a "max" cap. GOOD: `b.Grow(min(length, cap)); io.CopyN(&b, r, int64(length))`. Found by `FuzzImport` stalling
at 0 execs/sec: `readImportStageRecord` (5-byte header, ~25M x amplification) and `reserve()` (~20 bytes forced ~312 MiB; cap now 4096).
Detector: feed a tiny lying header and assert the `TotalAlloc` delta stays far below the declared size; fuzz streaming boundaries END-TO-END.

## 49. A Change-Log Must Be Emitted In-Backend And Co-Committed — A Decorator Cannot Be Crash-Safe

Rule: a write-observer / op-log / CDC seam lives INSIDE each backend, co-committed in the same atomic write, as an OPTIONAL
capability the core type-asserts; never a Store wrapper (it cannot reach the inner `WriteBatch` and is not an exact native store,
which silently disables the zero-copy scan). Design: AGENTS.md "Change-log (op-log) is IN-BACKEND".
- LSN + record enqueue under ONE lock window with the entity ops (`appendOpsLogged` for doors without `idxMu`); audit every early
  return after the snapshot in `flush()` and the empty-batch guard `len(ops)==0 && len(logs)==0`.
- Shared helpers stay record-free; the public door emits one logical record. Backends share one set of body builders
  (`storeutil`) plus a byte-parity test. The op-log alone does not converge a replica (snapshot the registry, then tail).

## 50. A Replica Must REPRODUCE The Primary's Rows, Not Re-Derive Them — And One Bit Of Provenance Decides History Depth

Rule: apply writes the wire VERBATIM through foreign-ID store doors, never `NodeOps.Add`/`Update` (they re-mint IDs, re-stamp
`TxFrom`, recompute hashes); the import trust pipeline VERIFIES, never STAMPS, the hash (the convergence oracle). Design: AGENTS.md
"Replica apply reproduces the primary's rows VERBATIM".
- A bare new-current row is ambiguous: carry ONE provenance bit (`NodePutBody.WithHistory`); a record must disambiguate the
  TRANSITION. Put wire is untokenized. The read-only gate is core-layer (`checkWritable()` on every user mutation door, found by what
  a method DOES, not by file).
- Watermark after work needs idempotent apply (identical row -> no-op, `rec.LSN <= applied` -> skip) AND the work flushed first
  (`flushStoreLocked()` before `setAppliedLSNLocked`): ahead of the data is fatal, behind is harmless.
Detector: a disk-backed non-`SyncWrites` restart-resume test, a double-apply history-depth test.

## 51. Syncing An Append-Only Registry Across Nodes: Capture-Order, Prefix-Guard, And Persist-Before-Use

Rule: (1) capture the anchor LSN BEFORE the names (`CapturedAtLSN >= rec.LSN`). (2) Grow via prefix-guarded
`AppendNames(prefix, suffix)`: equal contents or `(false, nil)` without mutating; `ImportNames` is load-only; all three
registries. (3) Persist the grow BEFORE the row that needs it under `registryMu`, `RollbackNames` on persist failure. (4) Sync only
labels and rel types (property keys tokenize locally). (5) Lock order `c.mu -> registryMu`. (6) Failover: the ID-slot lease is a
durable hint, not consensus; a promoted node and the one it replaces hold different slots.
Design: AGENTS.md "Token-registry refetch" and "Bootstrap LSN ... failover lease".

## 52. Delta Backups Reuse The Op-Log, Not A Diff — And A Hash Oracle Hides Non-Hashed Divergence

Rule: `ExportSince` / `ImportMerge` key a delta off the change-log (TX-ordered), never a temporal diff (it drops backdated writes
and cannot rebuild label changes or cascade tombstones). Hash equality cannot see `TxFrom`/`TxTo`: apply reproduces superseded
`TxTo` and tests compare bytes. A feed shows only durably committed records, so `Export`/`ExportSince` `flushStoreLocked()` first
(50). A "present but disabled" capability needs a status probe (`ChangeLogEnabled()`). The hash chain is a DAG: `Verify*Chain`
checks "every non-genesis PrevHash is among the version hashes". A purge-and-recreate rollback truncates history too.

## 53. A Reset That Wipes A Durable Watermark Must Keep The Watermark Continuously Present

Rule: when a reset must preserve one durable invariant, do not wipe-then-rewrite; keep the key present and mutate in place
(`clearAndReanchorChangeLog` drops data/index/history/log keyspaces, leaves `KeyMeta` with `LastLSNKey`, overwrites it with the
marker in one atomic batch). `DropAll` then a marker write let a crash reseed LSNs from 0. Delete stale counters/registries BEFORE
data (a counter above live rows is fatal, an absent one trusts rows). Fix EVERY door: the log-DISABLED `Clear()` arm also keeps
`LastLSNKey` (`clearDataPreservingLastLSN`); grep every path that deletes the key.

## 54. A Swap-Out Commit-Window Buffer Must Be Cleared On Success AND Consulted By EVERY Reader

Rule: the badger flush swaps `pending` for a fresh map and commits lock-free, so a row is in neither `pending` nor a `View`.
Park the snapshot in `flushing` and union `flushing ++ pending` in ONE helper (`rangePending` / `lookupPending`).
- Audit EVERY reader (`grep 'range bs.pending\|bs.pending\['`); at first only 1 of ~13 was routed.
- Populated between swap and commit, EMPTY otherwise: clear on success, on requeue and in `Clear()`/reset.
- Delete-set-computing mutators (cascade delete, incoming repair) need the snapshot too, or they orphan keys; set/delete
  symmetric across both buffers.
Detector: a white-box helper doing the `pending`->`flushing` swap WITHOUT committing; every reader still sees the row.

## 55. The Change-Log Is A PHYSICAL Redo Log, Not A LOGICAL Transaction Log

Superseded for `GraphTx` by the scoped log (`store.TxChangeLogScope`, `DiscardScopedLog` in `Rollback`): a rolled-back tx emits
nothing, LSNs mint at commit; the scoped tx takes `c.mu.Lock` per MUTATION (not lifetime, see 31). Still live for `Batch.Execute`
and `IO().Import` (eager emission): with the log ENABLED registries are APPEND-ONLY across rollback, because a durable put may
reference a new token (replicas stick, the number gets reused). Gate on `c.changeLogEnabled` (not `changeFeed != nil`) and only
the two entity-failure `RollbackNames` sites; allocation/persist-failure closures fire before any record and still de-allocate.
General: audit what an aborted operation EMITS and what it RETRACTS from shared monotone state.

## 56. A Purge-And-Recreate Rollback Must Capture EVERY Door's Full Side-Effect Footprint

Rule: `ImportMerge` rollback purges each touched entity (cascade drops attached edges) and re-creates it from a snapshot; the
capture pass must record the transitive closure the purge destroys (adjacency) for EVERY record kind that can be the sole
record touching a node. `captureMergeRecord` arms for `ChangeNodeHistoryVersion` / `ChangeNodeHistoryTruncate` skipped
`captureNodeAdjacency`, so a bounded-past `SetNodeVersionInterval` lost an unchanged edge. Audit by symmetry across the `switch`.
Detector: a node whose ONLY record is the under-tested kind, asserting that precondition by decoding the feed.

## 57. A Multi-Buffer Overlay Must Resolve Set-vs-Delete PER KEY At EVERY Reader Door

Rule: two readers sharing one overlay (`flushing` older, `pending` newer) share one resolver (`pendingHistoryIDOverlay`); a
running min/max/count over a multi-op buffer is a red flag (a scalar cannot retract a deleted key): collect surviving keys,
resolve set-vs-delete, then reduce. Failure: `MaxNodeHistoryID` returned 500 for a flushing SET masked by a pending DELETE
while `AllNodeHistoryIDs` returned none.
Detector: cross-door AGREEMENT (`Max*` vs `All*` on the same overlay state), not each door alone.

## 58. A Contract That Spans A FAMILY Of Sibling Doors Must Land In EVERY Door — Audit The Family

Rule: list the siblings, grep them, confirm the same `errors.Is` classification, validation predicate and ordering. Failures:
"decode rejections wrap `ErrCorruptExport`" held for bootstrap import but the shared replica-apply path returned
`WireTo*Checked` errors bare (seven sites); `PropertyKeyRegistry.GetOrCreate` guarded `== ""` while the others used `isBlankName`.
The fix DIRECTION comes from the existing shared parametric test, not the loosest door. Tighten CREATE doors, keep the LOAD door
lenient for legacy data (pin it). Make un-expressible invariants executable (a `Config.Store` decorator injecting one `Flush()` failure).

## 59. A Privileged Override Of A System-Controlled Field Lands At The Shared Seam, Scoped Where The Invariant Binds

Rule: enumerate the field's write sites and relax the invariant only where it binds. `tkg_tx_from` backfill is CREATE-only (the six
`extractTemporal` doors); supersession doors keep the clock (20/46) except `DeleteWithTx` / `UpdateWithTx` / `Set*VersionIntervalWithTx`,
which take a caller instant as an ARGUMENT once the invariant is a check under the entity lock (`checkTxOrder`, `ErrTxOrder`).
- Land it at the ONE shared seam (`extractTemporal`; one `at` seam on the delete doors; `updateTemporal.txAt`); deferred doors
  (Batch / ingest) add a whole-unit pre-flight reusing the seam's refusal functions.
- Gate with `Config.AllowTxBackfill`; malformed input beats the disabled-gate error; bound a caller value on a monotonic axis on BOTH
  sides against the clock (`UnixMicro` passed as ms is year 58000).
- Non-hashed fields are invisible to `VerifyChain` (52): assert `TxFrom` on the re-fetched row. Reap durable metadata (`asof_tags`) in
  `Admin().Reset()`; load re-validates VALUES against the write door (`{"tag": 0}` aliased "no TX filter").

## 60. A Retroactive In-Place Stamp Must Be Un-Applied At Every Belief-Reconstruction Door — Delete Is A Transaction-Time Tombstone

Rule: hard Delete stamps `DeletedAt`/`ValidTo`/`TxTo` IN PLACE on the final version (43 fixed `TxTo` only); the stamped `ValidTo`
killed the row in valid-time coverage, so generic `TxAt` reads dropped deleted entities at every pin while the named as-of door
resurrected them. For any retroactive stamp, enumerate the fields and every "as known at T" reader; each stamp is disregarded
when recorded after T. Land it at the shared seam (`filter{Node,Rel}ChainByTxAt`, `normalizeTemporalVisibleAtTxTime`), deep-copy
before normalizing. Tests derive pins from the entities' own `TxFrom`, never a slept wall-clock pin (`Core.now()` has a 1ms floor).

## 61. A "Current Transaction Time" Reader Must Consult The Commit Clock, Not The Session High-Water Mark

→ merged into 71 (its premise "the wall dominates every stamp" holds only at <= 1 write/ms). Still true: `Temporal().NowTx()`
returns `c.now()` (reserving the instant), never the session-local `c.lastInstant.Load()`, which is 0 after reopening a graph
with data; 0 is the `QueryOpts.TxAt == 0` "no TX filter" sentinel. Detector: `TestNowTx_ReopenSafe` (on-disk badger).

## 62. An As-Of Resolver Must Not Fall Through Past A Retracted Newest Belief — And "Newest" Is By Version, Not TxFrom

Rule: select the decisive belief, then judge its retraction, never filter it out during selection: the newest version recorded by
`txAt`, if superseded or deleted by the pin (`TxTo != 0 && TxTo <= txAt` or `DeletedAt != 0 && DeletedAt <= txAt`), means ABSENT.
"Newest" is the highest **version** with `TxFrom <= txAt`, not the highest `TxFrom` (`validInstantAfter` and the cascade break their
co-monotonicity). Fix in the READ path, keep the cascade append-only. Failure: Add(vf=1000) -> `SetNodeVersionInterval(2000)` ->
Delete -> `NodesAsOf(far-future)` returned the open genesis.
Detector: `asof_cascade_delete_test.go` (memory/badger/tiered, node+rel) plus an as-of clause in the bitemporal oracle harness.

## 63. An Unlocked Collect Window Before A Rebuild-Commit Needs A Write-Generation Guard — And No Rescan Under The Caller-Held Write Lock

Rule: (a) a rescan fetching cache-cold rows takes `idxMu.RLock`, so it must not run under the caller's `idxMu.Lock` (self-deadlock,
see 9); (b) releasing the lock to gather and re-taking it to COMMIT opens a lost-update window (not a data race, `-race` is silent).
Guard with a write-generation counter bumped by every mutator (`PropertyStatsAccumulator.WriteGen`): read before the collect,
re-read before the commit, redo if it moved (bounded, 24); on exhaustion return the live snapshot WITHOUT committing, leave `dirty`.
Detector: `TestBadgerStoreNodePropertyStatsStaleRescanOverwrite`; storm tests assert VALUE correctness against a sequential truth.

## 64. A Scan-Then-Overlay Reader Drops A Row Across The Commit Window — Snapshot The Overlay BEFORE The Badger View

Rule: overlay cache + snapshot-isolated store are two clocks: snapshot the MUTABLE overlay into a local map before `db.View`, merge it
over the scan, the immutable snapshot second. Structural, never timing-justified ("back-to-back so the window is negligible" is a
probability argument; absorbs 74). Audit readers AND delete-set mutators (history truncate/trim, index purges, incoming scrub); point
reads are exempt. Lazy builds make a missed row permanent: read the "built" flag and enqueue under `idxMu.RLock`. Publish order: when a
write changes two views a lock-free reader reads in sequence, publish in the REVERSE of the reader's order (`publishMoveLocked`).
Detector: grep every overlay reader (`pendingHistory*Overlay|snapshotHistoryOverlay|rangePending(|lookupPending(` under
`pkg/graph/store/badger/`) for capture before `db.View`; test the WINDOW (`historyScanTestHook`), not presence.

## 65. HyperLogLog Accuracy Tests (And Any NDV-Bound Test) Must Feed Well-Distributed Values

Rule: a test asserting NDV close to a known count feeds random strings, hashed values or prime-stepped integers (`i*97`), never
a tight run of small consecutive integers: `AddString` (FNV-1a) avalanches poorly on short low-byte-varying inputs and
undercounts ~11x. Test-authoring rule only, not a production defect. Anyone tightening the upper-bound-only concurrent tests
must scatter values. Evidence: `hyperloglog_sequential_test.go` (BACKLOG 16d).

## 66. A Pointer Swap Guarded By One Lock Class Races Every Reader Outside That Class

Rule: (1) a shared POINTER is swapped only while holding EVERY lock class its readers use (`restoreRegistries` swaps
`c.labels`/`c.relTypes` under `c.mu.Lock` AND `registryMu`; `Close`'s persist takes `registryMu`); adding a reader on a new lock
class means grepping every writer. (2) The pre-release race gate is the FULL `make test-race`, never a targeted `-run` subset
(storm tests only run in the full suite; slower CI runners open windows a fast box never does).
Detector: `grep -rn 'c\.labels = \|c\.relTypes = ' pkg/`.

## 67. A Raw-Byte Storage-Saving Estimate Is Not A Disk-Saving Estimate

Rule: state any "shrink the wire" saving as post-block-Snappy (4 KB blocks, keyspace order, history rows contiguous) over a realistic
corpus, never uncompressed or per-row. Low-entropy redundancy (timestamps) is a mirage post-Snappy (B3: 2.99% raw -> 1.13%, dropped);
whole-structure bulk survives (B6: 57.65% -> 39.10%, kept). Keep the measurement as an executable gate (`wire_b3_ondisk_gate_test.go`).

## 68. Reconstructed Wire Properties Must Be Re-Sorted By KEY STRING, Not Token

Rule: code that REBUILDS a wire property slice from parts (delta apply, merge, splice) emits key-string order
(`storeutil.SortWirePropertiesByKey` after resolving `KeyToken->Key`), because `WireToNodeChecked` rejects non-ascending key
strings and token order is not key order (it only looked alphabetical when `SetProperty` pre-sorted). Truncate re-tokenizes
before re-marshaling. Detector: a memory-vs-badger DIFFERENTIAL ORACLE for any storage-representation change (a same-backend
round trip is not enough); `history_delta_test.go` (BACKLOG 18s).

## 69. An Overlap-Based Candidate Prune Must NOT Be Applied To A Query Door Whose Predicate Set Includes NON-Overlapping Relations

Rule: before reusing a prune/short-circuit on a new door, check the door's predicate is a SUBSET of what the prune assumes.
`PruneTemporalCandidates` (valid-time envelope) is sound for `During`/point doors but wrong for `NodesRelating`, whose Allen set
includes `Before/After/Meets/MetBy`; scope the wiring and say why at the call site. Open interval ends are +inf: the Relating
classifier (`types.RelateOpen`) takes the query end RAW (0 = open). Detector: an exact-set test over `{Before, After, Meets,
MetBy}`; a happy-path `{Overlaps}` test passes with the wrong prune.

## 70. `reflect`-Based Sorting/Comparison On A Hot Path Is A Perf Bug — But Not All Reflection Is

Rule: no `reflect`-mediated operation (`sort.Slice`, `reflect.DeepEqual` as a typed-`Equal` substitute) on a per-write or
per-query path; use `slices`/`cmp`/`maps` or typed code. Reflection stays where the problem is reflection-shaped (`pkg/types`
property validation over arbitrary `any`) or the path is cold; the line is call frequency. Also audit THIRD-PARTY fallbacks:
`msgpack.Marshal` reflects unless the type implements `CustomEncoder`/`CustomDecoder` (the change-log body wrappers and
`HistoryDeltaEncoding` wrappers did not: a struct embedding an optimized field is not itself optimized).

## 71. A Wall-Dominated Monotonic Floor Is Not Reopen-Safe — Seed It On Open And Advance It On Every Foreign-Stamp Ingress

Rule (absorbs 61): `Core.now()` is `max(wall, last+1)`, so above 1 write/ms stamps run ahead of the wall and persist; after a reopen
(`lastInstant` = 0) or a foreign stamp (replica apply, bootstrap import) a new write can get a `TxFrom` below a committed one
(silent; `TxFrom` is not hashed). Persist the floor as a MetaKV watermark on `Close`, reseed on open (`seedInstantFloor`); advance
it at the central apply seam (`recordCommitStamp`) and per replayed wire on `Import`; only system TX stamps pull it
(`maxCommitStamp`), never `ValidFrom/ValidTo`. A crash loses the Close watermark: the store persists a commit floor with the rows (the newest stamp of every row it flushes, decoded from the batch, so no door can miss it) and `New` raises the clock to it (`seedCommitFloor`, backlog 44). "Every door" is a MOVING set:
re-audit on rebase, enumerate unhandled change tags with a reason. Reads use `c.readNow()` = max(wall, floor).
Detector: `TestNowTx_ReopenAfterBurst_MonotonicFloorAnachronism`, `TestCrashReopenNowTxAboveStoredStamps`, `TestNowTx_ReplicaCoversAppliedFutureTxFrom`, `TestInstantFloor_PreservedAcrossReset`.

## 72. An Externally-Filed Perf Diagnosis Names A Plausible Mechanism — Reproduce And Profile Before Touching The Named Component

Rule: treat only the SYMPTOM as data: reproduce in-repo with the reporter's fixture shape, profile, let the profile name the component
(a head-pin report blamed `RelBeliefWatermarkCapability`; the profile found `ForEachDeletedRelID` stepping every `0x08` row). Drain the
flush tick before `b.ResetTimer()`. Distinct-ID scans over `prefix/<id>/<version>` `Seek` to `prefix/<id+1>` once an id is decided.
Detector: `TestBadgerStore_AllNodeHistoryIDsFrom_PendingDeleteMasksSomeVersions`.

## 73. The Chain Resolver's Input Order Is A Semantic Input

Rule: `resolveNodeChain`/`resolveRelChain` take the chain in ASCENDING-VERSION order and `sortNodeChainForResolve` both detects
non-monotonicity and sorts IN PLACE; feeding a sorted slice back (selection-skeleton fast path) took the positional-tiling arm and lost
a cascade-reopened row. Give every invocation its own pristine copy; "an accelerator must never change answers" needs an equivalence
oracle in CI, not hand-written fixtures. Detector: an env-gated divergence cross-check (run fast AND full, panic with both chains).

## 74. "Back-To-Back, So The Window Is Negligible" Is A Probability Argument, Not A Correctness Argument

→ merged into 64 (capture the overlay before the snapshot structurally; the grep audit recipe and the lazy-build rule moved
there). Origin: a rare `TestBitemporalOracle_BadgerCommitWindow` mismatch under load led to `reverseScanHistoryVersion` capturing
the overlay inside `db.View`; the attribution to the observed failure stayed plausible, not proven (the sidecar build was the likelier cause).

## 75. A Green Workflow Must Mean Its Gates Passed

Rule: `continue-on-error` plus a prose-only finding baseline turns a security scan into an unaudited log stream; pin analyzer
versions, document audited suppressions at the call site, make the scan exit status blocking.

## 76. A Type Tag Covers One Level — Test Every Kind At Depth, Through The Bytes

Rule: the property wire tags only the top-level value, so nested kinds (eight integer widths, typed slices,
`map[string]string`, typed nil, registered structs) came back as other kinds. A codec round-trip test enumerates EVERY allowlisted
kind at depth 0, 1 and 2 in both container kinds through the real bytes, comparing value AND Go kind (hash bytes for NaN / -0).
An envelope adds wire depth: apply the property depth limit to the reconstructed value, bound the raw walk by the decode limit.

## 77. A Handover's Test List Is Break-The-Code Only, Written Red First

→ merged into 78. A handover's test section names the faulty implementation each test catches and the breaking input (zero,
negative, future, equal, reversed, duplicate, boundary t-1/t/t+1, concurrent, rollback, replica, per backend), is written before
its file-changes section and run red before the first line of code.

## 78. A New Item Gets The Clean Root-Cause Solution And A Red Break-The-Code Test First — No Shortcut Is Delivered Quietly

Rule (René 2026-10-09: "Always clean solutions to new items.. no hacks or shortcuts .. always write a no-happy-path test before the
code"; absorbs 27 and 77): the test naming the faulty implementation and its breaking input is written and run RED (output kept under
`tasks/evidence/<item>/`) before the fix; a test that already passes is a labelled guard; mutants prove the tests catch it. The fix
lands at the root and the shared seam (58-60), not behind a flag, skip or consumer heuristic; a documented limit only after the sound
solution was tried and shown impossible (say why; backlog it with its red test). Delegated prompts carry these rules; my own edits too.
Detector: `grep -rn 't.Skip' pkg | grep -v 'sharded does not support'`; evidence without a red run; "unaffected" without a test.

## 79. Release And Review Process: Minor For Features Else Patch, Author-Run Gates, One Review Round, Models

Rule (René 2026-10-10, 2026-10-12): (1) MINOR for a really new feature, PATCH otherwise (René 2026-10-14; patch-only from 2026-10-10 to 10-14), batched: one `make ci-docker`, one consumer
build, one message. (2) Behaviour changes go through the deprecation ritual (`docs/stability.md`); no new named exceptions. (3) Authors
run `make lint-docker` and `make security-docker` before reporting. (4) One reviewer round per branch, fix round to the same agent, a
second only for a blocker; doc/comment-only changes get the docs tests and a diff read. (5) Briefs name 5-8 files. (6) A backlog item
needs a consumer request or a failing test. (7) Never stop a running agent unless asked. (8) Models: builds Opus, code reviews Opus
effort high, final Fable review of the batch before tagging, doc work Sonnet (Opus after a second Sonnet fix round).
Detector: a second review round, a lint finding after a merge, a minor tag for a release without a new feature, a brief with over ten file names.

## 80. Tick The User-Request Ledger At Every Release, Not Only The Work Queue

Rule: a release commit updates, in the same commit, the `tasks/todo.md` user-request rows it closes, the releases table and the
HANDOVER open-request list; every new user or consumer request gets its row before the first action on it (René 2026-10-12:
"Are you really working towards our originally defined goal?" after request 1 stayed "not built" two releases past its ship).
Detector: `scripts/check-ledger.sh` (latest tag in the releases table, the todo header and HANDOVER; no request row "open/building/in progress" whose backlog item is Closed). It runs after every merge and before every release commit; a failure blocks the commit.
Why it broke again (2026-10-14/15, René: "Why not always keep todo.md update? This is important to not lose focus"): the ledger was updated by `sed` one-liners whose patterns silently matched nothing (HANDOVER kept "v4.52.0" for six releases), rows set "building" were never revisited when the work merged, and Markus's releases were never recorded. Edits to todo/HANDOVER are checked by the script, not trusted.

## 81. Open Work With A Clear Next Step Is Started, Not Offered

Rule (René 2026-10-12, on "build 52-55?": "yes, why is this a question?", then "Remove or rewrite this stupid rule"): consumer
requests, review findings and the next patch batch are started right away (ledger lines first, red tests, one review, final Fable,
tag). Ask only for what CLAUDE.md section 1 allows (irreversible or destructive, options that differ materially, not decided by
chat or memory). The old "never start agents he did not ask for" rule is gone.
Detector: a "Gewählt: not started ... Anders: sag fix N" line; four ai-soc blockers sat for hours as "waits for René's go".

## 82. A Release Is Not Green Until GitHub CI Is Green

Rule: the release gate is `make ci-docker` AND the GitHub CI run of the pushed commit; check `gh run list --branch main --workflow CI` before reporting a release done and fix a red CI before the next tag.
Trigger: René 2026-10-13 "why is the ci pipeline always failing?" — CI `test` (race on 4-vCPU runners) timed out on every main push since 2026-10-10 (v4.50.1–v4.51.4) while the local 32-core gate passed; nobody looked.
Detector: a tag whose CI run is not `success`.

## 83. Test Enough To Prove The Item, Then Build

Rule: one compact red test per item (the key break cases), then the code. No mutation runs, probe tests, oversized tables or benchmarks unless the item is about speed or RAM; reviewers read and reason instead of writing tests. Supersedes the mutant/probe parts of 78 and 79.
Trigger: René 2026-10-13 "I think you're testing too much. Focus on more development work."
Detector: evidence files named *mutant* or *probe*, or a test file longer than the code it tests.

## 84. Trace The Cause, Then Consolidate: No Unreflected Questions, No Layers On Top

René 2026-10-14 (standing rules, also in ~/CLAUDE.md). Before a design question to René: trace where it really comes from in the
current code (file:line, not an old note), look for a strategy that removes it (fix the cause upstream, a standard, an existing
mechanism) and solve it if one works; ask only the remainder, naming the cause and each rejected strategy in one line.
Consolidate, don't add: when two code paths, notes or rules do the same job, merge them into one and delete the rest instead of
a fix, layer or note on top; check for duplication first, it is often the real bug. Docs too (lessons, backlog, HANDOVER):
rewrite in place, delete outdated entries, never only append.

## 85. Comment Trimming Is Editing, Not A Script

2026-10-14: a second comment pass cut 15,982 → 10,371 lines in 266 files with sentence-boundary scripts run by six Sonnet agents. The Opus check found ~260 passages to restore: sentences cut mid-word with a period added, and dropped `Concurrency:` / `Caller holds` / `Errors:` / `lesson N` / format-marker paragraphs, so exported godoc lost its contract and surviving comments pointed at deleted text. Rule: trim comments by hand, file by file. Never cut inside a sentence. Never drop lock-order, caller-holds, crash-ordering, re-entrancy, error/nil/ownership/concurrency contract or format-compatibility lines. Every comment pass gets an Opus check of the diff for lost invariants before merge. The pass was discarded, not patched.
