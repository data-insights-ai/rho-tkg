# rho-tkg backlog

**STATUS: hardening-pass findings closed (started 2026-07-18, last item closed
2026-07-22, released in `[4.24.0]`).** Every previously-tracked design item
(BACKLOG 1-5) is still shipped and unaffected. A full-library hardening review (16
parallel subsystem audits covering every package under `pkg/`, ~100K LOC)
originally found ~196 items across BACKLOG 6-21; they are now resolved
(fixed-and-verified or investigated-and-confirmed-safe) and removed from this file
— see CHANGELOG.md for what shipped and why. What remains below is the closing
investigation record for the last of them, plus the two genuinely open things: the
carried-over import-under-a-scope follow-up, and the flagged-but-not-actioned CI
bench-gating gap at the end of this file. Nothing here is sigma-tkgd's — these are
all rho-tkg-owned.

**Severity legend:** CRITICAL = crash / data loss / replica divergence / silent
corruption. HIGH = silent wrong answer or a real, reachable correctness bug. MEDIUM =
concurrency edge case, perf cliff, or API/contract inconsistency. LOW = code smell,
doc drift, or narrow-impact issue. TEST-GAP = a real behavior is unverified (may be
hiding a bug). FEATURE = a capability the library plausibly should have but doesn't.

**Remaining open work:** no CRITICAL and no HIGH items remain. BACKLOG 10's 10b
(bitemporal cascade/resumption-row ambiguity), the last HIGH item, was fixed in
`0b5d662` and released in `[4.24.0]` — see CHANGELOG.md's `FIX — BACKLOG 10b`
entry for the full investigation and why the prior fix attempts were reverted; the
perf regression that fix caused was closed the next day as 10c (below). What is
still open is not severity-triaged hardening work at all: the import-under-a-scope
follow-up carried over from the retired `tasks/todo.md`, and the CI bench-gating
gap flagged at the end of this file.

## Shipped (was the backlog)

- **BACKLOG 1 — Retention purge (ex-ADR-0008 R2–R5).** Single-store age purge
  (R2) + `UniqueForever` owner reaping, `ChangeRangePurge` record + replica
  re-execution (R3), sharded + tiered cross-shard sweep (R4) incl. the tiered
  O(1) cold-shard-drop drain-protocol optimization, and `ByValidTo` (R5).
- **BACKLOG 2 — Cross-machine incoming half-edge "Model A" (ADR-0010 §3.3).**
  Store write, replica apply, graph door + byte-exact convergence, cascade, and
  the tx-rollback stub restore. All increments shipped and byte-exact verified.
- **BACKLOG 3 — Columnar / streaming whole-node fetch (sigma X5-wholenode).**
  The one-iterator bulk substrate (`forEachNodeBulk`, `PrefetchValues=false`
  load-bearing), parallel decode across cores (`collectNodesBulkParallel` with
  batched staging), the same substrate under `AllNodes` and the DocValues
  cold-build (`bulkNodePropGetter`), and the streaming door
  `g.Nodes().ForEachByLabel` + `IterByLabel` (O(1) peak memory). The as-of
  columnar sibling (`DocValuesSnapshotAsOf`) shipped + corrected with the
  (label, txAt) cache.
- **BACKLOG 4 — Review-driven adaptations.** 4b per-label DocValues epoch,
  4c configurable `HistoryAnchorInterval` (persisted compat marker), 4d ingest
  `IntentRecord` cleanup, 4e `PeekTx`. (4a per-version temporal-envelope prune
  was owner-decided **DO NOT BUILD** — net-negative for the confirmed workload.)
- **BACKLOG 5 — Rel-side ordering-soundness primitives (sigma).**
  `g.Stats().RelRangeCardinality` (5A) and `g.Stats().RelPropertyTypeClassCounts`
  (5B) — rule-2 mirrors of the node doors.

## Not tracked here (cross-team)

Sigma-coordinated RPCs are sigma's to build; rho-tkg already exposes the local
primitives they call, so they are intentionally **not** rho-tkg backlog items:
the START→END foreign-stub-delete fan-out (BACKLOG 2 Inc 4c) and the
consumer-gated constraint dry-run (HP2.5). When sigma pins a shape that needs a
new rho-tkg primitive, it re-enters here as a fresh, concrete item.

---

## Open — carried over from the retired `tasks/todo.md`

`tasks/todo.md` was deleted in `4f71fc3`; this is the one follow-up on it that is
still real, kept here because this file is the single todo file.

- **Import-under-a-scope** (the proper fix beyond the import stopgap): wrap
  `IO().Import` in a `TxChangeLogScope` so a FAILED import emits NOTHING, not just
  "no poison". Import emits its change-log records IN-BACKEND eagerly, outside any
  scope, so `importRollback.restoreRegistries` (`import.go`) is still gated on
  `changeLogEnabled` — the append-only stopgap — rather than de-allocating tokens
  exactly. Needs import to also take `c.txMu` (it takes only `c.mu.Lock` today, so
  a between-mutations open tx scope would collide with import's `BeginLogScope`);
  a locking change, deferred. The current stopgap is correct, so this is an
  improvement-not-bug item — see `tasks/lessons.md` 55.

## Closed — Hardening Pass (2026-07-18)

### BACKLOG 11 — Batch / ingest / tx concurrency hardening

- **11f. [DONE — closed 2026-07-22] Change-log-enabled tx mutations take the FULL exclusive
  graph lock (`c.mu.Lock()`) per mutation call, not just per commit — a real throughput cliff (LOW-
  MEDIUM). NOT a fix; the underlying bottleneck is still fully present.** `tx.go:387-416`
  (`lockActiveCoreWrite`). Fully defeats ADR-0007's per-shard `RLockShard` striping and blocks every
  concurrent standalone writer AND concurrent-mode (Lanes:N) ingest session for the duration of EACH
  mutation call a change-log-enabled interactive tx makes — not once per tx, once per call. Investigated
  the mechanism (`store.TxChangeLogScope`/`SetLogDivert`, `changefeed.go:191-239`): the interface's own
  doc explicitly frames the current exclusive-lock behavior as "CONCURRENCY POSITION (deliberate
  design, not a gap)" — there is exactly one implicit divert scope, so `SetLogDivert(true)` must run
  under a lock that provably excludes every other writer, or a concurrent standalone mutation's record
  could be silently misrouted into the tx's buffer (a correctness bug, not just a perf one, if broken).
  A cheaper fix needs the divert mechanism to stop being a single global on/off flag and become
  SCOPE-TAGGED instead (each in-flight writer's change-log record carries/derives its own routing key,
  so multiple scopes can be open concurrently without one global exclusive lock) — CLAUDE.md's own
  design notes on the tiered store's change-log ALREADY flag `SetLogDivert` as "the ONE divert seam,
  marked for the scope-tagged-routing redesign" from prior (2026-07-11) measurements, confirming this
  is known, real, cross-cutting architecture work (touches the standalone core AND every store
  backend's change-log wiring), not a narrow one-file patch — exactly the class of change that needs a
  dedicated design pass, not a blind attempt inside a backlog sweep (same caution class as 10b).
  Documentation of the current mechanism and its practical implication (route bulk writes through
  `g.Ingest()` instead of `g.Tx()` when change-log is enabled and Lanes:N throughput matters) was added
  to `docs/api.md`'s "Ingest pipeline" section — that part is genuinely done, since the finding was ALSO
  a real doc gap — but the doc addition must not be mistaken for resolving the underlying bottleneck;
  the scope-tagged-routing redesign itself remains open, real engineering work.
  **Batch A landed (foundation only, zero behavior change):** the ctx-value-token design from the
  dedicated design pass — `context.WithValue`-carried scope token read only at store-door call sites,
  never threaded through shared internal helpers the standalone path also uses (provably zero blast
  radius there) — is now BUILT and independently tested for doors 1-3 (`PutNode`/`PutRelationship`/
  `PutRelationshipGeneratedIDWithEndpointHashes`) plus the `store.ScopedTxChangeLog` capability skeleton
  (`BeginScopedLog`/`CommitScopedLog`/`DiscardScopedLog`, memory + badger). `GraphTx` does NOT construct
  a token-carrying ctx yet — the legacy single-scope `TxChangeLogScope`/`SetLogDivert` mechanism is
  UNCHANGED and still what every real caller goes through, so this batch has zero effect on the
  exclusive-lock bottleneck described above. Remaining: wire the other ~19 store-door call sites (full
  enumeration in the design-pass notes), then flip `GraphTx` itself to open a `ScopedTxChangeLog` scope
  and call `withScopeToken` so a tx mutation can take the same shared read-lock a standalone mutation
  takes instead of `lockActiveCoreWrite`'s full exclusive lock — THAT flip is what actually resolves
  this item, and it is deliberately deferred to a later batch once every door has a Scoped sibling.
  **Batch B landed (foundation only, zero behavior change):** doors 4-5 — `ReplaceNodeWithHistory`/
  `ReplaceRelWithHistory` — now have `Scoped` siblings (new `store.ScopedReplaceCapability`, memory +
  badger), wired ONLY through the two tx-exclusive update helpers (`updateNodePreparedInternal`/
  `updateRelationshipPreparedInternal`), never through the standalone `NodeOps.Update`/`RelOps.Update`
  path, the CAS path (`property_cas.go`), the generic version-chain path (`version_chain.go`), or the
  replica-apply path (`apply_record.go`) — all four confirmed untouched by grep. Same "no lock-behavior
  change yet" status as Batch A: `GraphTx` still constructs no token-carrying ctx anywhere, so this is
  dormant in production. Remaining doors after A+B: ~17 more call sites (label-token doors, batch
  doors, delete-with-history doors, import doors, …) before the `GraphTx` lock-relaxation flip itself.
  **Batch C landed (foundation only, zero behavior change):** doors 6-7 — `DeleteNodeWithHistory`/
  `DeleteRelWithHistory` — now have `Scoped` siblings (new `store.ScopedDeleteCapability`, memory +
  badger), wired through the SHARED `deleteNodeInternal`/`deleteRelationshipInternal` helpers (used by
  both the `GraphTx` and standalone delete paths — safe by the same "nothing constructs a nonzero token
  yet" argument Batch A already used for its shared `putGeneratedNode`/`putGeneratedRelationship`).
  Confirmed by grep that `apply_record.go`'s two replica-apply calls to the same store doors are
  untouched (same exclusion class as Batch A/B's replication-apply carve-out), and that the
  relTombstones-cover-every-connected-relationship invariant is unchanged — the scoped route only
  changes WHERE the change-log record for the delete lands, nothing about tombstone/cascade
  construction. Remaining doors after A+B+C: ~15 more call sites (label-token doors, batch doors,
  import doors, …).
  **Batch D landed (foundation only, zero behavior change):** two independent pieces. (1) Import doors —
  `importNodeWithIDInternal`/the `relPersistImport` branch of `createRelWithTypeRollback` now route
  through the EXISTING Batch A `store.ScopedPutCapability` (`PutNodeScoped`/`PutRelationshipScoped`) via
  new thin wrappers `putImportedNode`/`putImportedRelationship` — no new store capability needed, since
  import's caller-specified-ID create ultimately writes through the same plain `PutNode`/
  `PutRelationship` doors Batch A already scoped. (2) Label-token doors — `AddNodeLabelTokenWithHistory`/
  `RemoveNodeLabelTokenWithHistory` now have `Scoped` siblings (new `store.ScopedLabelCapability`, memory
  + badger), wired through `addNodeLabelInternal`/`removeNodeLabelInternal` in `node_label.go`, which
  needed a genuine signature change (adding `ctx context.Context`, since — unlike every prior batch's
  doors — these two functions took no ctx at all before this batch); all 4 call sites (the standalone
  `NodeOps.AddLabel`/`RemoveLabel` doors, threading their real ctx, and `GraphTx.AddNodeLabel`/
  `RemoveNodeLabel`, passing `context.Background()` per the established no-natural-ctx precedent) updated
  together. `GraphTx` still constructs no token-carrying ctx anywhere — same "dormant foundation" status
  as every prior batch. Remaining doors after A+B+C+D: batch doors and the bitemporal cascade doors
  (`SetNodeVersionInterval`/`SetRelVersionInterval`) — roughly ~11 more call sites — before the
  `GraphTx` lock-relaxation flip itself.
  **Batch E landed (foundation only, zero behavior change):** the LAST remaining doors — investigation
  found "batch doors" was an overestimate from the Batch C/D notes; `SetNodeProperty`/
  `DeleteNodeProperty`/`SetRelationshipProperty`/`DeleteRelationshipProperty` all delegate to
  `UpdateNode`/`UpdateRelationship` (Batch B, already wired), and `GraphTx` has no separate batch-mutation
  surface at all (`g.Batch()`/`BatchBuilder` is a wholly separate write door, never reached from
  `GraphTx`). The ONLY remaining GraphTx-reachable unscoped doors were the four the bitemporal cascade
  (`cascadeNodeVersionInterval`/`cascadeRelVersionInterval`, BACKLOG 10b's carefully-proven append-only
  algorithm) calls: `PutNodeVersion`/`ReplaceNode`/`PutRelVersion`/`ReplaceRelationship`. New optional
  `store.ScopedCascadeCapability` (memory + badger). Badger needed a new `appendOpsLoggedRouted` helper
  (`PutNodeVersion`/`PutRelVersion` hold no `idxMu` across their enqueue, so they can't reuse
  `logChangeRoutedRaw` — mirrors the existing `appendOpsLogged`'s one-critical-section-for-both shape,
  token-aware); `ReplaceNode`/`ReplaceRelationship` reuse `logChangeRoutedRaw` directly, same as prior
  batches' replace doors. The cascade's own append-only algorithm in `temporal_cascade.go` is
  UNCHANGED — only the 4 store-door call sites gained a dormant `if token != 0` routing branch via a new
  `cascade_scoped.go` (mirroring `putGeneratedNode`'s exact pattern), never touching the delicate
  belief-selection/resumption-boundary logic itself. Confirmed by grep that the many OTHER callers of
  these same 4 store doors — replica apply (`apply_record.go`), import/rollback (`import.go`,
  `import_merge.go`), the one-time bitemporal migration (`migration_bitemporal.go`), and the
  `UpdateInPlace`-style direct-write doors (`node_update.go`/`relationship_update.go`, a DIFFERENT path
  than Batch B's `updateNodePreparedInternal`) — are all correctly untouched. Full end-to-end test
  proves the wiring routes through the REAL `cascadeNodeVersionInterval`/`cascadeRelVersionInterval`
  entry points (via `TempOps.SetNodeVersionInterval`/`SetRelVersionInterval`, which forward ctx
  unchanged — the exact function `GraphTx`'s doors call), not just the wrapper helpers in isolation.
  **This closes the door-wiring phase of 11f entirely** — all `GraphTx`-reachable store doors now have
  Scoped siblings. The remaining and final piece is the `GraphTx` lock-relaxation flip itself.
  **Batch F landed (foundation only, zero behavior change):** discovered — via a careful audit of
  `GraphTx.Rollback`'s reverse-mutation path specifically, not assumed — 8 more store doors Batches A-E's
  door-by-door sweep had missed, because they are reachable ONLY from `Rollback`'s undo logic, never from
  a tx's forward-mutation path: `DeleteRelationship`, `DeleteNodeCascade` (the plain hard-delete, distinct
  from Batch C's `DeleteNodeWithHistory`), `TruncateNodeHistory`/`TruncateRelHistory`, the plain
  non-history `AddNodeLabelToken`/`RemoveNodeLabelToken` (distinct from Batch D's `*WithHistory` pair),
  and the optional `TrimNodeHistoryFrom`/`TrimRelHistoryFrom`. This mattered: a rolled-back tx's reverse
  mutations MUST land in the same discardable per-tx buffer as its forward mutations, or the flip below
  would have left a durable leak-on-rollback bug — records from a rolled-back tx silently reaching the
  eager change-log feed, exactly the defect class the whole scoped-buffer design exists to prevent. New
  `store.ScopedRollbackCapability` (8 methods) and the union `store.ScopedTxCapability` (embeds all 7
  Scoped interfaces from Batches A-F) — `GraphTx` checks for this EXACT combined interface before using
  the fast path, so a store implementing only SOME pieces (tiered, sharded — neither implements the full
  set) correctly and safely falls back to the legacy mechanism rather than being silently granted a fast
  path it can't fully support.
  **11f CLOSED.** The `GraphTx` lock-relaxation flip itself landed immediately after Batch F:
  `lockActiveCoreWrite`/`lockActiveCoreWriteContext`/`unlockActiveCoreWrite` now branch on
  `GraphTx.usesSharedLock()` (`tx.g.scopedChangeLog != nil || tx.g.txLogScope == nil`) — a store
  implementing the full `ScopedTxCapability` (memory, badger) takes a shared `c.mu.RLock()` per mutation
  call instead of the full exclusive `c.mu.Lock()`, so a concurrent standalone write no longer blocks
  behind an open change-log-enabled tx purely because the log is on. `GraphTx.doorCtx()`/`doorCtxFrom(ctx)`
  decide whether to carry the tx's scope token into each store-door call; `Rollback`'s entire
  reverse-mutation path (7 steps + 10 helper functions) now routes through new token-aware helpers in
  `rollback_scoped.go` so undo mutations land in the same discardable buffer as the tx's forward ones.
  7 new tests in `tx_scoped_lock_test.go` prove: mechanism selection is correct across full-support /
  no-changelog / partial-support (a real tiered store) stores; the concurrency claim holds via a
  deterministic test bracketing the lock calls directly; the legacy mechanism still correctly blocks for
  partial-capability stores (regression guard); Rollback still discards everything under the new
  mechanism (adversarial multi-door scenario); Commit still emits everything. Full `go test ./...`,
  `go vet`, and repeated `go test -race` across `internal/core`+`store/badger`+`store/memory`+
  `store/tiered` all pass (see CHANGELOG for the flake investigation this surfaced, unrelated to this
  change — `TestBitemporalOracleHarness/seed=47645253227` occasionally fails under combined
  multi-package `-race` load on BOTH this change and unmodified `main` at a similar rate, confirming it's
  a pre-existing, load-triggered flake, not a regression).
- **11h. [DONE — closed 2026-07-22, root-caused via static code reading rather than further reproduction
  attempts]
  `TestOutgoingIncomingForNodesAtTx_RandomizedDivergenceProbe/badger`
  is intermittently flaky under full-suite load (MEDIUM, discovered during BACKLOG 12c's verification
  run, one failure ever observed, not yet reproduced).** `internal/core/adjacency_at_tx_test.go:402-554`.
  One failure observed in a full `go test ./pkg/graph/internal/core/...` run (`got 6 node entries ...
  want 5 entries` — an EXTRA relationship visible in the actual result the reference model says should
  not be, at one of the test's bitemporal pins). The original theory (documented in the test's own
  comment): `OutgoingForNodesAtTx`'s TX-only-door valid-time probe re-reads WALL-now
  (`resolveOpenEndInstant`/`nowInstant()`) fresh on every one of 36 queries in the loop, while the
  EXISTING `waitWallPast(pinD)` mitigation only waits ONCE before the loop starts, so a scheduling
  delay under heavy CI load between individual queries could theoretically let a later query's wall-now
  probe race against "something time-dependent."
  **Ran exactly the repro the finding itself recommended** — inserted a controlled 3ms sleep between
  EVERY one of the 36 per-iteration queries (far larger than any plausible single-goroutine scheduling
  jitter) and ran 20 full repetitions (40 subtest runs): ZERO failures. Also ran the bare test 200+
  times with no injected delay, and the whole `internal/core` package 3x end-to-end (mirroring how the
  original flake was found, inside a full-package run): zero failures in every attempt. Code-level
  analysis also argues AGAINST the theory as stated: wall-clock time is monotonically non-decreasing
  (barring an NTP step), so once `waitWallPast(pinD)` returns (wall-now > pinD), EVERY subsequent
  `nowInstant()` read in the query loop is unconditionally also > pinD — there is no window in which a
  LATER query's wall-now probe could read a SMALLER value than an EARLIER one, so "a later query races
  against a delay" does not by itself explain an extra/wrong relationship appearing.
  **Conclusion of the reproduction-based investigation: the specific mechanism the test's own comment
  theorized (a delay racing `waitWallPast`) is not reproducible by direct, aggressive experiment, and does
  not hold up under wall-clock-monotonicity analysis.** 25 further sequential full-package runs (matching
  the historical failure's exact load shape) also came back clean, exhausting the reproduction-only
  avenue.
  **2026-07-22: ROOT-CAUSED via static code reading instead of further reproduction attempts, per explicit
  session direction ("check the code instead of tests").** `resolveOpenEndInstant(0)` (`temporal.go`)
  resolves an open-ended "now" bound via a bare `nowInstant()` wall-clock read — its own doc comment warns
  this must be resolved ONCE at a scan's entry point, "eliminates time drift on long iterations, where
  each per-ID call would otherwise observe a different `nowInstant()`". That discipline was followed for
  the `*MatchingDuring*` family but was VIOLATED by the TxAt-only branches of `findNodeVersionForOpts` /
  `findRelVersionForOpts` (`temporal.go`): each is called ONCE PER CANDIDATE from inside a multi-entity
  `ForEach` scan (`OutgoingForNodesAtTx`/`IncomingForNodesAtTx`, the generic `ByType`/`ByLabel` TxAt-only
  door, `NodeOps.All`/`RelOps.All`/`ForEach`, `ByLabelAndProperty(Properties)`, `ByTypeAndProperty`,
  `SearchNearest`'s temporal path — 12 call sites total), and each call independently re-resolves
  `resolveOpenEndInstant(0)` — a FRESH wall-clock read per candidate, not once per query. A relationship
  or node whose `ValidTo`/`DeletedAt` boundary falls between two of those per-candidate reads is included
  by one candidate's probe and excluded by another's within the SAME logical query — or diverges from an
  independent second scan over the same nominal opts run moments later (exactly what
  `referenceAdjacencyAtTx`, the test's oracle, does: it calls the generic `ByType(opts)` door as a SEPARATE
  scan AFTER the door under test already ran its own scan).
  **Fix**: new `normalizeTxAtOnlyOpts(opts)` helper (`temporal.go`) resolves the "now" fallback into
  `opts.ValidAt` ONCE, before a scan starts — this makes the (already-correct) `opts.ValidAt != 0` branch
  fire instead of the per-candidate TxAt-only branch, so every candidate in a scan is evaluated against
  the exact same valid-time pin. Applied at all 9 top-level call sites (some functions call
  `findNodeVersionForOpts`/`findRelVersionForOpts` more than once, hence 12 invocations from 9 sites) in
  `queries.go`, `graph_property_query.go`, `graph_rel_property_query.go`, `vector_search.go` — a no-op
  for every other `QueryOpts` shape (`TxPin`, explicit `ValidAt`, an interval query, or no temporal
  filter). `opts` itself is left untouched at every site except where it is used for nothing else
  (`directionalRelsForNodesAtTxLocked`); elsewhere a separate `resolveOpts` local is introduced right
  before the loop so earlier consumers of the raw `opts` (K1 sidecar candidate gathering, the B4 envelope
  prune) are unaffected.
  **Deterministic proof** (`TestFindRelVersionForOpts_TxAtOnly_PerCallNowDriftsAcrossWallClockTick`,
  `adjacency_at_tx_test.go`) reproduces the hazard WITHOUT depending on a random race: creates a
  relationship with an explicit `tkg_valid_to` boundary 30ms in the future, calls the RAW (unnormalized)
  `findRelVersionForOpts` before the boundary (finds it), sleeps 60ms past the boundary, calls it again
  with the identical opts value (now reports `ErrNoVersionValidAt` — the per-call drift, proven) — then
  shows the fix: re-using an `opts` snapshot resolved via `normalizeTxAtOnlyOpts` BEFORE the boundary
  still finds the relationship even after the same 60ms sleep, because its probe is frozen at resolution
  time. Passed 10/10 deterministic runs. Verified via full `go test ./...`, `go vet`, `gofmt`, and
  `go test -race ./pkg/graph/internal/core/...` (clean).

### Perf trajectory investigation (2026-07-22) — two findings, one fixed

Ran the `bench/` suite at 5 checkpoints spanning the last several days of work
(`v4.16.0` → `v4.19.0` → `v4.23.0` → `7919ad0` "BACKLOG 18k, the last `perf:`-labeled
commit before the sustained feature stretch" → `HEAD`) after the user asked whether
recent perf work vs. subsequent feature work had regressed anything. Everything in
the ~100-scenario suite was flat/noise-level across the whole range except two real
deltas, both traced via `git bisect` to `7919ad0` and `0b5d662` respectively:

- **DONE — `BenchmarkAsOfPin/badger` correctness bug, fixed.** `NodesAsOf`/`RelsAsOf`
  silently undercounted results (876-988 of 1500 expected) once `7919ad0` landed —
  see the "fix: BACKLOG 18k" commit for the full root cause (a background-flush
  race the batching refactor's own tests didn't cover) and fix (a
  captured-once-before-the-shared-transaction overlay snapshot). Closed.
- **DONE — BACKLOG 10c — `BenchmarkPinnedScanScaling/.../d_bylabel_txat` ~15x slowdown
  (450µs/6133 allocs vs. 29µs/433 allocs), fixed.** `git bisect` traced this to
  `0b5d662` ("BACKLOG 10b — durable override-vs-resumption resolution in bitemporal
  cascades"), a genuine correctness fix that removed `nodeCurrentAnswersAt`/
  `relCurrentAnswersAt` (a fast path in `nodeAtLockedTx`/`relAtLockedTx` answering a
  point-in-time query from the current row alone) because a bounded cascade
  correction can insert a HISTORY row with a NEWER belief than an untouched,
  still-open current row without replacing it — the old shortcut had no way to
  detect that and would silently return the stale current row. Confirmed real
  downstream impact before attempting a fix: `sigma-tkgd`'s `TemporalSweep`/
  `IncrementalTriangleSweep` hit this exact path via `NodesAtTx`/`RelsAtTx`, up to
  10,000 points per sweep — not just a synthetic benchmark number. Restored the
  shortcut SAFELY via a new per-entity `store.NodeBeliefWatermarkCapability`/
  `RelBeliefWatermarkCapability` sidecar (memory + badger, lazily built, same shape
  as `labelTxMembers`/`relValidIdx`) tracking the maximum TxFrom EVER recorded
  across an entity's whole version chain, however it got there (cascade, plain
  update, import, replica-apply, rollback) — the shortcut now additionally requires
  `current.TxFrom == watermark(entity)`, i.e. current is PROVABLY the newest belief
  in the chain; unknown or non-matching declines to the full scan (never a wrong
  answer, only a slower one). See the "perf: BACKLOG 10c" commit for the full
  design, the exhaustively-grepped write-door coverage, the adversarial invariant
  test (load-bearing confirmed by temporarily disabling the gate), the full
  200-seed non-short bitemporal oracle, and the confirmed perf recovery back to the
  pre-10b baseline (~44µs/433 allocs).

**OPEN — structural gap flagged, not yet addressed**: nothing in `go test ./...`
or CI would have caught either finding on its own. `bench/` is deliberately excluded
from `go test ./...` (fixture-build cost); the regression-detecting tool
(`make bench-check`) needs a manually-captured, gitignored, per-machine baseline;
and the one CI benchmark workflow that exists is manual-dispatch-only and its
regression-check step runs with `continue-on-error: true` by explicit design
(shared-runner noise). A real regression can therefore land and stay invisible
indefinitely unless a human manually brackets a change with
`make bench-baseline`/`make bench-check`. `bench/README.md` already documents a
"flip-to-blocking plan" for wiring `bench-check` into CI once GitHub-hosted-runner
noise is characterized — flagged here as worth pursuing but NOT actioned; it is a
CI/policy decision needing explicit owner sign-off, not a code fix.


