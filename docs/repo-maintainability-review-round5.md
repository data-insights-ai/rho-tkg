# Repository maintainability review - round 5

Date: 2026-05-09  
Branch: `main`  
Reviewed commit: `82e21ee refactor(core): split large files by behavior (R4-F19)`

This review was run after pulling `main`. It re-checked the prior round's findings against the new code and then looked for residual correctness, maintainability, and scalability risks. Fixed prior issues are not repeated as findings unless a residual problem remains.

## Summary

The pull fixed several high-risk round 4 issues: injected-store registry loading, transaction method races, endpoint-state refresh for relationship create/update, batch temporal constraint checks, IO sentinel reachability, index fallback behavior, vector over-fetch, collision probe error handling, async priority wakeups, and a meaningful part of the large-core-file split.

The main remaining risk is uneven application of new lifecycle and import-safety rules. Some fixes were applied to the obvious path but not to all public sub-API entry points or all record classes. Verification also exposed a live async event bus priority failure in the current `main` test suite.

## Status (2026-05-09)

| Finding | Severity | Status |
|---------|----------|--------|
| R5-F1   | High     | Fixed — `c.checkOpen()` primitive at every public entry point; regression tests in `r5_close_completeness_test.go` |
| R5-F2   | High     | Fixed — tx-scoped `Export`/`Snapshot`/`VerifyShard` methods; lock-free internal variants; tests in `r5_strict_snapshot_test.go` |
| R5-F3   | Medium   | Fixed — token-membership validation in `validateNodeTokensInRegistry`/`validateRelTokensInRegistry`; tests in `r5_import_token_test.go` |
| R5-F4   | Medium   | Fixed — history records now byte-compared on duplicate ID+version; tests in `r5_import_token_test.go` |
| R5-F5   | Medium   | Fixed — every batch queue method gates on `checkOpen`; `Delete*` returns error; tests in `r5_close_completeness_test.go` |
| R5-F6   | Medium   | Fixed — token allocation deferred past endpoint-fetch failure paths; `AddByIDIfAbsent` uses `Lookup` short-circuit; tests in `r5_rel_type_alloc_test.go` |
| R5-F7   | Medium   | Fixed — ByID variants enforce constraints when configured; tests in `r5_byid_constraint_test.go` |
| R5-F8   | Low      | Fixed — `architecture.md` lock table updated; `design.md` SnowflakeID bridge entry corrected; `temporal_snapshot.go` comment fixed in R5-F2 work |
| R5-F9   | Low      | Fixed (production files) — `batch.go` 727→114+277+355, `export.go` 707→275 + `import.go` 449, `wire.go` 657→283 + `wire_value.go` 382, `tieredstore_read_history.go` 568→341 + 240 (rel split), `tieredstore_read_bulk.go` 603→317 + 297 (rel split), `badgerstore_rel.go` 761→299 + 322 + 161 (CRUD/query/batch), `badgerstore_node.go` 964→448 + 262 + 279 (CRUD/query/batch), `badgerstore_history.go` 1109→105 + 562 + 393 (shared/node/rel). Test files remain — review-blocked refactor; addressed when adjacent test churn happens. |
| R5-F10  | Low      | Mostly fixed — 64 of 121 wall-clock sleeps eliminated. Files with 0 sleeps remaining: temporal_test (22→0), temporal_queries_rel_parity (8→0), findings_extra_regression (6→0), txtime (3→0), vector_correctness (14→0), diff (3→0), graph_temporal (2→0), foreach (2→0), diff_callback (2→0), v3061_fixes (1→0), bench_production (1→0). Migration patterns: `useTestClock` + `clk.PeekInstant()` for "after-mutation" anchors, `clk.Advance(d)` to widen UpdatedAt gaps, explicit `tkg_valid_from` to pin temporal ordering for snowflake-anchored vector tests. Remaining ~57 sleeps are genuinely scheduler-bound: tiered-store hot/warm/cold rotation (29) depends on wall-clock shard windows; async event bus dispatch (8) tests timing of the worker goroutine; badger flush behavior (5) tests wall-clock-scheduled background work; production code (2 in `tieredstore.go`) waits on `activeReqs` to drain. |
| R5-F11  | Low      | Fixed — `resolveTemporalVectorMatches` moved into `vector_correctness_test.go` |
| R5-F12  | High     | Fixed — `AsyncEventBus.PublishBatch` atomic-enqueue API; strict-ordering test passes 100/100 |

## Findings

### R5-F1 - High - Post-close protection is still incomplete across public sub-APIs

`Core.Close` now sets `c.closed` before draining `c.mu`, and `runUnderRLock` returns `ErrGraphClosed` before invoking mutation bodies (`pkg/graph/internal/core/core.go:355-387`, `pkg/graph/internal/core/locks.go:31-38`). That only protects callers routed through `runUnderRLock`.

Many public APIs still bypass the closed-state gate and can touch a closed store or mutate in-memory graph state after `Close`:

- Reads: `NodeOps.GetWithContext` and `RelOps.GetWithContext` call the store directly (`pkg/graph/internal/core/node_delete.go:20-30`, `pkg/graph/internal/core/relationship_delete.go:15-25`).
- Indexed queries read store and registries without a close check (`pkg/graph/internal/core/queries.go:35-76`).
- Hash verification reads current state and history directly (`pkg/graph/internal/core/integrity.go:20-31`, `pkg/graph/internal/core/integrity.go:86-96`).
- Stats/count helpers call stores or registries directly (`pkg/graph/internal/core/stats.go:40-58`, `pkg/graph/internal/core/stats.go:76-86`, `pkg/graph/internal/core/stats.go:90-123`).
- Admin operations take locks but do not check `c.closed`, including `Archive`, `Restore`, `ForceRotate`, `ListShards`, `RebuildCatalog`, `Repair`, `VerifyShard`, and `Reset` (`pkg/graph/internal/core/admin.go:21-29`, `pkg/graph/internal/core/admin.go:58-65`, `pkg/graph/internal/core/admin.go:124-141`).
- Index creation/drop and constraints can still mutate graph-side state after close (`pkg/graph/internal/core/graph_indexes.go:14-24`, `pkg/graph/internal/core/graph_indexes.go:121-147`, `pkg/graph/internal/core/validation.go:13-35`).
- `BeginTx` and `Batch.New` do not check the closed flag (`pkg/graph/internal/core/tx.go:72-77`, `pkg/graph/subapi.go:18-19`, `pkg/graph/subapi.go:90-94`). A transaction or batch created after close can reach internal mutation paths that do not check `c.closed`.
- `IO.Export` and `ImportWithOptions` take locks but do not reject a closed graph (`pkg/graph/internal/core/export.go:89-92`, `pkg/graph/internal/core/export.go:320-321`).

The new tests cover only a small subset: node add/update, relationship add/delete, and provider registration (`pkg/graph/internal/core/r4_close_state_test.go:15-98`). They do not cover read, admin, index create/drop, tx, batch, IO, stats, hash, or constraints entry points.

Impact: post-close behavior remains backend-dependent. Memory-backed graphs can continue returning or mutating state after close; Badger-backed graphs can surface low-level closed-DB errors instead of the public sentinel; tx/batch paths can cross into internal mutation code after lifecycle shutdown.

Recommendation: add a single open-check primitive and route every public sub-API entry point through it. Split helpers by lock mode, for example `withReadOpen`, `withWriteOpen`, and `checkOpen`, so read-only APIs, write APIs, tx/batch begin, admin, IO, stats, hash, constraints, and index management all share the same sentinel behavior. Add table-driven post-close tests for every sub-API.

### R5-F2 - High - Strict-snapshot guidance tells callers to create self-deadlocks

Several comments/docs correctly say export and verification are best-effort under `c.mu.RLock`, but then recommend driving those APIs from inside a tx/batch for strict consistency:

- `IOOps.Export` takes `c.mu.RLock` for the whole export (`pkg/graph/internal/core/export.go:79-92`) and says callers should drive Export from inside a tx/batch for a strongly consistent snapshot (`pkg/graph/internal/core/export.go:85-88`).
- `docs/api.md` repeats the same guidance: "Drive Export from inside a tx for a strict snapshot" (`docs/api.md:128`).
- `AdminOps.VerifyShard` takes `c.mu.RLock` and recommends driving it from inside a tx (`pkg/graph/internal/core/admin.go:119-129`).
- `TempOps.Snapshot` takes `c.mu.RLock`, while the lower helper says callers needing strong consistency should hold `c.mu.RLock` (`pkg/graph/internal/core/temporal_snapshot.go:18-30`).
- Transactions and batches hold `c.mu.Lock` (`pkg/graph/internal/core/tx.go:72-77`, `pkg/graph/internal/core/batch.go:355-357`).

`sync.RWMutex` is not reentrant. A caller following the documented advice, for example `g.Tx.Run(func(tx) { return g.IO.Export(w) })`, blocks forever because the exported method tries to take `RLock` while the same goroutine already holds the write lock through the transaction.

Impact: the documentation points users at a deadlock-prone consistency pattern, and there is no no-lock internal export/verify method exposed through tx/batch to make the advice possible.

Recommendation: remove the "inside tx/batch" advice from public docs and comments unless a lock-free internal variant is added. If strict export/verify is a supported goal, expose explicit tx/batch methods that call `exportLocked` or `verifyShardLocked` under the already-held write lock. Add timeout tests that prove the documented strict path does not deadlock.

### R5-F3 - Medium - Import requires a registry record but does not validate token membership

The import replay now requires header and registry records before entity records (`pkg/graph/internal/core/export.go:323-390`). That closes the "entity before registry" case, but entity validation still only checks that tokens are nonzero and fit in `uint16`:

- Node validation accepts any primary/extra label token in `[1, 65535]` (`pkg/graph/internal/core/export.go:528-562`).
- Relationship validation accepts any rel type token in `[1, 65535]` (`pkg/graph/internal/core/export.go:565-575`).
- Registry resolution returns an empty string for out-of-range tokens (`pkg/graph/internal/registry/label_registry.go:96-105`), and graph resolution forwards that value (`pkg/graph/internal/core/resolution.go:42-46`).

A corrupt stream can include a registry containing only token 1 and an entity record with label token 42. Import accepts the record because token 42 is in range, but the imported entity has an unresolved label/type. That breaks string resolution, index behavior, exported API semantics, and any hash checks that use resolved canonical names.

Recommendation: validate node and relationship wire tokens against the imported or existing registry lengths after the registry record has been accepted. Reject primary labels, extra labels, and relationship type tokens that do not have a registered name.

### R5-F4 - Medium - Import duplicate checks protect current entities but not history versions

The duplicate-current fix is in place: duplicate current nodes/rels are accepted only if the stored entity serializes identically to the incoming record (`pkg/graph/internal/core/export.go:407-435`, `pkg/graph/internal/core/export.go:451-472`). History records are still written unconditionally:

- Node history import calls `PutNodeVersion` directly (`pkg/graph/internal/core/export.go:437-449`).
- Relationship history import calls `PutRelVersion` directly (`pkg/graph/internal/core/export.go:474-486`).
- Memory history writes overwrite the same `(id, version)` map slot (`pkg/graph/store/memory/memorystore_history.go:177-189`, `pkg/graph/store/memory/memorystore_history.go:266-278`).
- Badger history writes set the same exact history key (`pkg/graph/store/badger/badgerstore_history.go:351-365`, `pkg/graph/store/badger/badgerstore_history.go:502-516`).
- Tiered history delegates to those same store implementations (`pkg/graph/store/tiered/tieredstore_write_history.go:49-62`, `pkg/graph/store/tiered/tieredstore_write_history.go:108-122`).

Impact: re-importing into a non-fresh graph can leave the current entity untouched while silently replacing existing history versions. The resulting graph can combine current state from one source with history from another, which undermines rollback, auditing, temporal queries, and hash-chain verification.

Recommendation: apply the same idempotency rule to history records. If a history `(entityID, version)` exists, load it and compare canonical wire bytes before accepting it. Reject mismatches with a distinct import conflict error. Add tests for identical history re-import and conflicting history re-import.

### R5-F5 - Medium - Batch queue methods still have observable side effects before `Execute`

`BatchBuilder` is documented as queuing work and deferring persistence (`pkg/graph/internal/core/batch.go:113-115`, `pkg/graph/internal/core/batch.go:199-201`, `docs/api.md:75`). In practice, queue-time methods mutate graph-owned registries and allocate IDs before `Execute` takes the graph write lock:

- `AddNode` calls `labels.GetOrCreate` for primary and extra labels at queue time (`pkg/graph/internal/core/batch.go:151-163`) and allocates a node ID at queue time (`pkg/graph/internal/core/batch.go:165`).
- `AddRelationship` calls `relTypes.GetOrCreate` and allocates a relationship ID at queue time (`pkg/graph/internal/core/batch.go:243-250`).
- `Execute` is the first point that takes `b.g.mu.Lock` (`pkg/graph/internal/core/batch.go:355-357`).
- `BatchAPI.New` can create a builder after close because it does not check graph lifecycle state (`pkg/graph/subapi.go:90-94`).

Impact: abandoned or failed batches can permanently register labels/types and burn IDs, even though no data is committed. This also creates a side-effecting public path that bypasses the lifecycle lock and the new close gate. ID gaps may be acceptable for snowflakes, but registry pollution changes user-visible API behavior (`Lookup`, stats, export registry contents) and makes dry-run semantics misleading.

Recommendation: either move registry creation and ID allocation into `Execute` under the write lock, or document `BatchBuilder` as side-effecting at queue time and guard queue methods with the same open-state/lifecycle checks as mutations. If queue-time IDs are required so queued relationships can reference queued nodes, separate "provisional" IDs from registry mutation, or provide an explicit builder `Close/Discard` cleanup model.

### R5-F6 - Medium - Relationship type tokens are still allocated before live endpoint failure paths

Round 4 moved relationship type allocation after cheap validation and collision probes, but several remaining failure paths still happen after `relTypes.GetOrCreate`:

- `RelOps.Add` allocates the type token before acquiring endpoint locks and fetching live endpoint nodes (`pkg/graph/internal/core/relationship_add.go:85-112`).
- `RelOps.Import` probes ID collision first, but then allocates the type token before fetching live endpoint nodes (`pkg/graph/internal/core/relationship_import.go:98-125`).
- `RelOps.AddByID` allocates the type token before the eventual store write verifies endpoints (`pkg/graph/internal/core/relationship_add.go:246-307`).
- `RelOps.AddByIDIfAbsent` allocates the type token before `OutgoingRelationships` and the later store write can fail (`pkg/graph/internal/core/relationship_add.go:386-453`).
- `BatchBuilder.AddRelationship` allocates the type token at queue time, before `Execute` verifies endpoints (`pkg/graph/internal/core/batch.go:243-250`, `pkg/graph/internal/core/batch.go:477-545`).

Impact: failed relationship creation can still pollute the relationship-type registry. Missing endpoints, closed stores, corrupted stores, or operational read errors can make `RelTypes.Lookup` start succeeding for types that have no committed relationship.

Recommendation: allocate the rel type token only after all endpoint and store-read failure points that can be checked before constructing the relationship. For ByID paths, either fetch endpoints before allocation or explicitly document the registry side effect as part of the high-throughput trade-off. Add tests for missing endpoint and operational store-error cases.

### R5-F7 - Medium - Relationship constraint docs remain too broad for public ByID APIs

The constraints docs say `ConstraintRelWithinEndpoints` is "checked during relationship creation and import" (`docs/api.md:122-124`). That is not true for all public relationship creation APIs:

- `AddByIDWithContext` documents internally that endpoint hashes and temporal constraints are skipped (`pkg/graph/internal/core/relationship_add.go:176-185`, `pkg/graph/internal/core/relationship_add.go:298-300`).
- `AddByIDIfAbsentWithContext` has the same trade-off (`pkg/graph/internal/core/relationship_add.go:314-323`).
- The public rels facade comments only say "creates a relationship by node IDs"; they do not warn about skipped endpoint hash capture or skipped temporal constraints (`pkg/graph/rels/api.go:72-89`).

`docs/api.md` has one earlier note about high-throughput rel creation (`docs/api.md:49`), but the constraints section still overstates the invariant. A reader looking up temporal constraints can reasonably conclude every creation/import path enforces them.

Impact: downstream code can choose `AddByID` for throughput and accidentally bypass a graph-level constraint it believes is universally enforced.

Recommendation: make the constraints section explicit: checked by `Rels.Add`, relationship import, and batch relationship execution when live endpoints are available; skipped by `Rels.AddByID*`. Add the same warning to public facade comments in `pkg/graph/rels/api.go`.

### R5-F8 - Low - Documentation drift remains after the thin-facade and locking refactors

Several docs are still out of sync with current code:

- `docs/architecture.md` says `UpdateRelationship` uses only `LockEntity(id)` (`docs/architecture.md:121`), but the current implementation locks the relationship and both endpoint IDs via `LockMany` (`pkg/graph/internal/core/relationship_update.go:82-99`).
- `docs/design.md` still describes `nodeID`, `relID`, and `entityID` as unexported wrapper types with exported bridge methods (`docs/design.md:24`), while the public docs now correctly use exported `NodeID`, `RelID`, and `EntityID` (`docs/api.md:17`).
- `TempOps.snapshotAt` says callers needing strong consistency should hold `c.mu.RLock` (`pkg/graph/internal/core/temporal_snapshot.go:28-30`), but holding an `RLock` does not exclude standalone mutations that also hold `RLock`. Holding the write lock and then calling the exported snapshot method would deadlock.

Impact: these are not immediate runtime defects, but they make future fixes riskier because reviewers and maintainers will reason from stale lock and API contracts.

Recommendation: keep the architecture lock table and design notes in the same commit as behavior changes. Prefer doc comments that describe the exact supported call pattern instead of internal lock names when the lock layering is subtle.

### R5-F9 - Low - Production file split improved core maintainability, but large store and test files remain

The core split reduced the previous biggest production hot spots, but several production files are still large enough to make review-by-feature difficult:

- `pkg/graph/store/badger/badgerstore_history.go`: 1109 LOC
- `pkg/graph/store/badger/badgerstore_node.go`: 964 LOC
- `pkg/graph/store/badger/badgerstore_rel.go`: 761 LOC
- `pkg/graph/internal/core/batch.go`: 675 LOC
- `pkg/graph/internal/storeutil/wire.go`: 657 LOC
- `pkg/graph/internal/core/export.go`: 618 LOC
- `pkg/graph/store/tiered/tieredstore_read_bulk.go`: 603 LOC
- `pkg/graph/store/tiered/tieredstore_read_history.go`: 568 LOC

The test suite has even larger monoliths:

- `pkg/graph/store/tiered/tieredstore_test.go`: 3307 LOC
- `pkg/graph/store/memory/store_test.go`: 2571 LOC
- `pkg/graph/store/badger/badgerstore_test.go`: 1550 LOC
- `pkg/graph/internal/core/temporal_test.go`: 1387 LOC
- `pkg/graph/store/badger/badgerstore_rel_test.go`: 1224 LOC
- `pkg/graph/internal/core/graph_rel_test.go`: 1132 LOC
- `pkg/graph/internal/core/findings_regression_test.go`: 1124 LOC
- `pkg/graph/internal/core/graph_node_test.go`: 1112 LOC

Impact: the remaining large files concentrate unrelated behavior and make it easy to miss cross-feature invariants, especially in Badger history/current writes, import/export, and batch behavior.

Recommendation: continue the same behavior-based split for store files and large tests. Prioritize files where one logical fix needs a grep audit across many methods: Badger history/current writes, batch create/update/delete phases, import/export wire handling, and tiered read history/bulk iteration.

### R5-F10 - Low - Test flakiness risk remains from wall-clock sleeps

Round 4 added a per-Core mock clock, but the migration is incomplete. `rg --count-matches --stats "time\\.Sleep" pkg/graph` still reports 121 `time.Sleep` matches across 29 files. High-density examples include:

- `pkg/graph/internal/core/temporal_test.go`: 22 matches
- `pkg/graph/store/tiered/tieredstore_test.go`: 16 matches
- `pkg/graph/internal/core/vector_correctness_test.go`: 14 matches
- `pkg/graph/internal/core/tieredstore_history_routing_rel_test.go`: 9 matches
- `pkg/graph/internal/core/temporal_queries_rel_parity_test.go`: 8 matches

Impact: millisecond sleeps are still a source of nondeterminism on loaded CI, and they slow the suite as the number of temporal/vector tests grows.

Recommendation: keep migrating temporal/vector tests to `useTestClock(t, g)` and `clk.Advance(...)`. Reserve real sleeps for async event bus and Badger flush behavior where the test is actually about wall-clock scheduling.

### R5-F11 - Low - A test-only vector helper still lives in production code

`resolveTemporalVectorMatches` is documented as test-only and no longer used by the production search path (`pkg/graph/internal/core/vector_search.go:212-218`). Its only call sites are tests in `pkg/graph/internal/core/vector_correctness_test.go:565-589`.

Impact: this is small, but it keeps legacy behavior compiled into production and invites future callers to reuse a helper that the comment says not to use.

Recommendation: move the helper into a `_test.go` file or convert the tests to exercise only production behavior. If the helper encodes an invariant worth preserving, express it as a test fixture rather than production code.

### R5-F12 - High - AsyncEventBus still does not guarantee strict priority order under rapid publishes

Verification failed on the current `main` branch:

```text
--- FAIL: TestR4_AsyncEventBus_StrictPriorityOrder_AfterIdleWake (0.06s)
    r4_priority_order_test.go:97: iteration 24: dispatch order = [2 1 3 0], want [2 1 0 3]
```

A targeted rerun also failed:

```text
go test -short -count=10 ./pkg/graph/events -run TestR4_AsyncEventBus_StrictPriorityOrder_AfterIdleWake
--- FAIL: TestR4_AsyncEventBus_StrictPriorityOrder_AfterIdleWake (0.03s)
    r4_priority_order_test.go:97: iteration 10: dispatch order = [3 2 1 0], want [2 1 0 3]
```

The worker no longer blocks directly on all priority queues, but the current wakeup design still dispatches as soon as a wakeup arrives (`pkg/graph/events/events.go:286-350`). If a low-priority event wakes the worker and higher-priority events arrive slightly later, the worker can observe different subsets on each scan. That allows `Low` to dispatch before `Normal`, or even before `Critical`, despite the test and comments promising strict global priority ordering (`pkg/graph/events/r4_priority_order_test.go:1-15`, `pkg/graph/events/r4_priority_order_test.go:87-99`).

Impact: the public async event bus contract remains nondeterministic under bursty publication. The repo's default `make test` failed during this review, so this is both a correctness issue and a CI stability issue.

Recommendation: decide the actual contract. If strict priority only applies to events already queued at scan time, weaken the docs and tests. If strict priority across rapid bursts is required, the worker needs a deterministic batching/drain strategy, such as coalescing wakeups until the publisher burst quiesces, sequencing events through one mutex-protected heap, or accepting a small configured drain window before selecting the next event. Add repeated tests that cover low-before-critical, low-before-normal, and mixed burst cases.

## Verification

Commands run after report creation:

- `git diff --check` - passed.
- `make test` - failed in `pkg/graph/events` at `TestR4_AsyncEventBus_StrictPriorityOrder_AfterIdleWake`.
- `go test -short -count=10 ./pkg/graph/events -run TestR4_AsyncEventBus_StrictPriorityOrder_AfterIdleWake` - failed, confirming the event bus failure is reproducible.
- `make cover` - passed on a later run; this reinforces that the event bus issue is scheduler-sensitive rather than deterministically failing every invocation.
- `make cover-gate` - passed: total coverage `86.5% >= 80%`.
