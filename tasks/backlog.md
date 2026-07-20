# rho-tkg backlog

**STATUS: hardening-pass findings in progress (started 2026-07-18).** Every
previously-tracked design item (BACKLOG 1-5) is still shipped and unaffected. A
full-library hardening review (16 parallel subsystem audits covering every package
under `pkg/`, ~100K LOC) originally found ~196 items across BACKLOG 6-21; all
CRITICAL items and most HIGH items are now resolved (fixed-and-verified or
investigated-and-confirmed-safe) and removed from this file — see CHANGELOG.md for
what shipped and why. What remains below is open work only. Nothing here is
sigma-tkgd's — these are all rho-tkg-owned.

**Severity legend:** CRITICAL = crash / data loss / replica divergence / silent
corruption. HIGH = silent wrong answer or a real, reachable correctness bug. MEDIUM =
concurrency edge case, perf cliff, or API/contract inconsistency. LOW = code smell,
doc drift, or narrow-impact issue. TEST-GAP = a real behavior is unverified (may be
hiding a bug). FEATURE = a capability the library plausibly should have but doesn't.

**Remaining open work:** no CRITICAL items remain. BACKLOG 10's 10b (bitemporal
cascade/resumption-row ambiguity) is the one remaining HIGH item — it needs a
dedicated design session, not a quick patch (see its entry for the full
investigation and why the first fix attempt was reverted). Everything else left is
MEDIUM/LOW/TEST-GAP/FEATURE.

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

## Open — Hardening Pass (2026-07-18)

### BACKLOG 6 — `pkg/types` data-model hardening

- **6f. No `unsafe.Sizeof` cross-check test for `heapsize.go` constants (TEST-GAP).** Root cause of 6b
  shipping undetected twice.

### BACKLOG 7 — Public façade & thin sub-API wrapper hardening

### BACKLOG 8 — Dry-run / constraints / replication convenience-API correctness

### BACKLOG 9 — Write-path kernel hardening (CRUD, unique constraints, version chains)

### BACKLOG 10 — Bitemporal resolution engine hardening

- **10b. [CONFIRMED REAL via reproducible test; fix ATTEMPTED and REVERTED — needs a dedicated
  design session, do not attempt a quick patch] An open-ended cascade correction starting *before* an
  untouched open "current" row is silently capped and never wins, even though it's the newer belief
  (HIGH).** `internal/core/temporal_cascade.go:218-233` + `temporal.go:358-406,167-184,420-442`.
  `nodeVersionBounds` derives a version's end **positionally** (next sorted entry's `ValidFrom`), not
  by belief recency. Repro (now in `TestCascade_OpenCorrectionBeforeUntouchedOpenCurrent` history —
  reverted alongside the fix, see below): current v0 `ValidFrom=2000,open,TxFrom=T0`;
  `SetNodeVersionInterval(id,1000,0,corrected)` at `T1>T0` creates v1 `ValidFrom=1000,open,TxFrom=T1`;
  v1's effective end is computed as v0's `ValidFrom`(2000) since v0 sorts after it, so v1 loses the
  "current" slot to the untouched v0 and is demoted to history. `NodeAtTx(id,2500,txAt>=T1)` then
  returns v0's **pre-correction** content. Empirically confirmed with a live test (RED without a fix,
  100% reproducible, no flakiness).
  **Attempted fix (reverted):** (1) `nodeVersionBounds`/`relVersionBounds` skip a later-sorted `next`
  entry whose `TxFrom` is older than the entry being bounded, so `next` can't wrongly truncate a
  newer, wider-reaching correction; (2) the cascade's `newCurrent` selection picks the open-ended
  candidate with the *newest belief* (`nodeBeliefNewerThan`/`relBeliefNewerThan`) instead of "last in
  valid-from order"; (3) discovered that fix (1) alone breaks the **resumption row** the cascade
  constructs for a *bounded* correction (`newVT != 0`) — a resumption row re-asserting old content
  from `newVT` onward is STRUCTURALLY INDISTINGUISHABLE, via its own stored ValidFrom/ValidTo/TxFrom,
  from a genuine override like the one in the repro (both are "open, later TxFrom than some
  pre-existing older-belief row that should — or shouldn't — bound them" — the two cases require
  OPPOSITE treatment and cannot be told apart from per-row temporal metadata alone). Refined fix (3b):
  compute the resumption row's `ValidTo` EXPLICITLY at construction time from its source row's own
  effective end in the **pre-correction chain** (via `nodeVersionBounds` over `preChain` alone, never
  touched by fix (1)'s skip logic) instead of leaving it `0` and relying on positional tiling at read
  time. This fixed both the original repro AND the previously-passing `TestCascade_MidHistoryInsertion`
  — **but** running the full suite surfaced `TestBitemporalOracleHarness` /
  `TestBitemporalOracle_BadgerCommitWindow` (a property-based fuzz harness comparing an independent
  oracle model against live `NodesAtTx` results over long randomized operation sequences, including
  repeated/interleaved cascades) failing on roughly HALF of all random seeds — extra/missing/wrong-
  version nodes in the door's answer vs. the oracle. This means fix (1)'s blanket "skip older-belief
  next" rule, even with refinement (3b) patching the single-cascade resumption case, still produces
  wrong answers once MULTIPLE cascades (each contributing their own newVer/resumption pairs with
  distinct TxFrom values) accumulate on the same chain — the pairwise TxFrom comparison in
  `nodeVersionBounds` doesn't know "which cascade batch" a candidate pair belongs to, and applying it
  indiscriminately across an arbitrarily-merged multi-cascade chain has failure modes beyond the
  single-resumption case this session found and fixed. **All three changes were reverted** (`git
  checkout -- temporal.go temporal_cascade.go cascade_test.go`) — full suite is back to green.
  **For the next attempt:** the oracle harness (`bitemporaloracle_test.go`,
  `bitemporaloracle_commitwindow_test.go`) is the load-bearing regression gate — any fix MUST pass it
  at full iteration count (not just the two hand-written repro cases) before being considered done.
  The core open design question: how to durably distinguish, at read time, "a row asserting a genuine
  override of an older belief within its claimed domain" from "a row merely continuing/resuming an
  older belief that legitimately still bounds it" — per-row temporal metadata alone is provably
  insufficient (both shapes are structurally identical); the fix likely needs either a persisted
  marker distinguishing the two row roles, or a fundamentally different (non-positional,
  non-pairwise-TxFrom) algorithm for interval-bounds derivation in a chain with cascade-inserted rows.
### BACKLOG 11 — Batch / ingest / tx concurrency hardening

- **11f. [PARTIALLY ADDRESSED — see below] Change-log-enabled tx mutations take the FULL exclusive
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
- **11h. [STILL OPEN — NOT resolved; original "clock re-probing" theory RULED OUT by direct experiment,
  but the real root cause remains unknown and no fix has been applied]
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
  **Conclusion: the specific mechanism the test's own comment theorized is not reproducible by direct,
  aggressive experiment, and does not hold up under wall-clock-monotonicity analysis — but the ONE
  observed failure is real and unexplained.** Candidate directions for whoever picks this up: (1) a
  THIRD clock source — entities without explicit `tkg_valid_from` derive effective valid-from from
  their snowflake ID's own embedded MICROSECOND timestamp (a different clock reading than both `c.now()`
  and `nowInstant()`'s millisecond `time.Now()` — see CLAUDE.md "Snowflake Configuration"), not
  investigated here; (2) true cross-test interference under genuine full-suite CPU/memory contention
  (GC pause, scheduler starvation) that a targeted single-goroutine sleep injection cannot replicate,
  since this test's `t.Parallel()` only affects scheduling relative to OTHER package tests, not
  anything reproducible by delaying this test's own goroutine in isolation. A real repro likely needs
  either genuine concurrent load from sibling tests (not a synthetic sleep) or many more full-suite runs
  than were feasible here. NOT fixed — no code or test change applied; left explicitly open rather than
  closed by an unconfirmed theory or a speculative patch.

### BACKLOG 12 — Replication / import / export hardening

- **12i. `strictCheckMergeRecord` (Strict mode) only covers 4 of 8+ mergeable tag kinds — missing
  history-version/truncate tags (LOW, not a correctness bug, incomplete contract).**
  `import_merge.go:409-449`.
- **12j. `applyNodeLabelChangeLocked`'s fail-closed guard rests on an undocumented wire-format
  assumption — single-token label diffs only (informational, correct today).** `apply_record.go:335-362`.
- **12k. `readExportRecord` allocates directly from a length header — the lesson-48 anti-pattern, but
  not currently reachable from untrusted input (informational, landmine for a future refactor).**
  `export.go:464-478`.
- **12l. `ChangeClear` doc ("replica applying it must clear its own state") vs implementation mismatch
  — ties directly to 12a (informational).**

### BACKLOG 13 — Retention / compaction / admin hardening

- **13c. `CompactHistoryNodes`/`Rels` hold the exclusive graph lock across a full O(n) whole-graph
  scan+write, contradicting retention purge's own documented chunked-lock discipline (MEDIUM).**
  `internal/core/compaction.go:518-599,601-681`. No chunking, no periodic lock release, no `Label`
  scoping — always the whole population.
- **13f. `verifyChainLinkage`'s no-stub "legacy leniency" leaves a real tamper-evidence gap — any
  `PrevHash` accepted when the lowest retained version has no compaction stub (MEDIUM, intentional
  backward-compat tradeoff).** `internal/core/integrity.go:126-167`. Any out-of-band row removal *not*
  via `CompactHistoryNodes`/`Rels` is undetectable by `Verify*Chain`.
- **13j. Transient in-memory activation on the persist-failure path in `createUnique` — narrow
  self-healing race window (LOW).** `unique_constraints.go:339-353`.
- **13l. `Admin.Reset`'s correctness depends on a hand-maintained, unenforced checklist of `reap*`
  calls — no test asserting every MetaKV prefix is reaped-or-documented-safe (LOW, latent cross-backend
  divergence risk for future MetaKV features).** `admin.go:250-268`.
- **13m. `CollectShardDropResidue` (badger-layer primitive backing tiered's fast-drop) has zero direct
  badger-package tests (TEST-GAP, Rule 1 — only indirectly exercised via tiered).**
  `pkg/graph/store/badger/badgerstore_shard_drop.go`.

### BACKLOG 14 — Index / docvalues / stats / vector / events hardening (graph layer)

- **14e. Two sibling stats doors disagree on capability-check-vs-label-lookup ordering — inconsistent
  `DisablePlannerStats` fail-closed behavior for an unregistered label (LOW-MEDIUM, lessons 17/58
  drift pattern).** `internal/core/stats.go:111-132,226-247` vs `:144-178,186-217`.
- **14f. FIFO (not LRU) eviction on the as-of DocValues column cache (cap=64) — undercuts the cache's
  own stated goal once a workload exceeds 64 distinct hot pins (LOW, deliberate simplicity tradeoff,
  worth revisiting).** `internal/core/docvalues_asof_cache.go:31-51,121-139`.
- **14g. `graph_epoch.go`'s corrupt-lineage and zero-avoidance branches have no direct test (TEST-GAP).**
  `graph_epoch.go:36-38,43-46`.
- **14h. Composite-index introspection (`ListComposites`/`HasComposite`) has no counterpart for
  property/temporal/vector/rel-property indexes — see BACKLOG 21 for the feature-level entry.**

### BACKLOG 15 — Internal primitives hardening (storeutil / locks / registry / wire codec)

- **15f. `propertyToWire`'s `ptCustom` branch does a full marshal+reflect-unmarshal+2×hash+compare
  round-trip on *every write*, not just type-registration time (MEDIUM, perf, may be intentional
  defense-in-depth).** `storeutil/wire_value.go:429-464`.
- **15g. `ValueStripe` calls `fnv.New64a()` (heap-allocating interface) on every invocation — once per
  constrained property per node write (MEDIUM, perf).** `internal/locks/value_locks.go:44-53`. Fix:
  inline FNV-1a over a local `uint64` accumulator.
- **15h. `TestKeyPrefixesNonOverlapping` omits the 3 newest production key-prefix tags
  (`KeyChangeLog`/`KeyPropertyIndex`/`KeyTemporalIndex`); dead test scaffold aliases real production
  byte values (MEDIUM, test-gap + code smell, zero prod risk).** `storeutil/keys_test.go:297-309`,
  `keys_helpers_test.go:11-14`.
- **15i. `integrity_test.go`/CLAUDE.md attribute the per-type-tag property-value switch to
  `internal/integrity`, but the real switch lives in `pkg/types` — Rule 3 gets enforced against the
  wrong location (MEDIUM, doc/attribution).**
- **15j. `EnvelopeOverlaps` (backs the B4 candidate-prune optimization on every history-aware temporal
  scan) has no direct unit test anywhere — only indirect (LOW-MEDIUM, Rule 1).**
  `storeutil/temporal_filter.go:37`.
- **15k. `WireToNode`/`WireToRel` (unchecked, panic-on-invalid variants) have zero production callers
  but dangerously similar naming to the trust-boundary-safe `*Checked` versions (LOW, foot-gun risk).**
  `storeutil/wire.go:204,346`. Fix: delete or rename unmistakably as test-fixture-only.
- **15m. `decodeMapKeyLen`'s over-long-key path allocates a fresh up-to-65535-byte slice per key
  instead of pooled scratch, on an otherwise zero-alloc path (LOW, perf).**
- **15n. `generatedcreate.FreshGraphID` exported as a mutable package-level `var`, not a
  const/accessor — accidental reassignment zeros a process-wide global (LOW, module-internal blast
  radius).** `internal/generatedcreate/capability.go:16`.
- **15o. `PropertyKeyRegistry` has no `RollbackNames` unlike `Label`/`RelTypeRegistry` — intentional
  (lesson 37) but undocumented at the type level (LOW).**
- **15p. No `PreEncodeRelPutPayloadV2` counterpart to `PreEncodeNodePutPayloadV2` for §4.5 pre-encode —
  see BACKLOG 21 (LOW, likely intentional node-first scope).**
- **15q. `PaginateNodesInOrder` tests the cursor via `after != 0` directly instead of
  `after.SnowflakeID() > 0` like every sibling `Paginate*` function — inconsistent idiom (LOW).**
  `storeutil/pagination.go:95-122`.
- **15r. `wireEncBufPool` (`sync.Pool`, shared on the hot ingest write path across goroutines) has no
  concurrent/parallel-goroutine test pinning its safety guarantee (LOW, `sync.Pool` is safe by design;
  test would pin it rather than rely on the general contract).**

### BACKLOG 16 — In-memory index-engine hardening (HNSW / property & temporal index / HyperLogLog)

- **16e. `hnsw.go`'s `reassignEntryPoint` doesn't pick the max-level survivor — silently collapses
  `maxLevel`, degrading search convergence/quality after entry-point deletion (MEDIUM, no crash, no
  test targets this).** `internal/index/hnsw.go:425-435`.
- **16f. `hnsw.go`'s `connect()` uses reflection-based `sort.Slice` on the hottest construction path,
  contradicting the project's own established `slices.SortFunc` idiom (MEDIUM, perf).**
  `internal/index/hnsw.go:299` — `lru.go:314-316`'s comment explicitly warns against this pattern.
- **16g. `hnsw.go`'s `searchLayer` allocates a fresh `[]bool` visited slice per layer, not reused
  across a query — O(maxLevel) O(n)-byte allocations per query (MEDIUM, perf).**
  `internal/index/hnsw.go:337-345`. Fix: per-graph visited-generation array + epoch counter.
- **16h. `temporal_index.go`'s `Extend`/`Remove` is O(n) linear scan on every node mutation to a
  temporally-indexed label — O(n²) bulk-update under the write lock (MEDIUM).**
  `internal/index/temporal_index.go:130-158,251-270`. Fix: secondary id→index map for O(1) amortized.
- **16i. `hnsw.go`/`hnsw_test.go` has no direct BFS-reachability graph-connectivity regression test —
  only an indirect, `-short`-skipped recall@10 proxy (MEDIUM, TEST-GAP).** CLAUDE.md documents that
  exact BFS test was used during development to catch the naive-closest fragmentation bug; it doesn't
  exist today as a fast always-run test.
- **16k. `RangeNodeIDs`'s `inclMin`/`inclMax` parameters are declared but never read — contract drift
  (LOW).** `internal/index/property_index_range.go:230`.
- **16l. `sorted_chunks.go`'s `remove()` has no merge-on-shrink for adjacent undersized chunks (LOW-
  MEDIUM, missing-feature/perf note for long-lived high-churn indexes).**
- **16m. `lru.go`'s `MarkDeleted` on an already-cached key leaves the stale payload and its accounted
  byte size in place until flush — un-evictable, holds full memory against the byte budget (LOW-
  MEDIUM, untested intermediate state).** `internal/index/lru.go:247-257`.

### BACKLOG 17 — Store interface & MemoryStore hardening

- **17g. `snapshotChangesLocked` is O(total log size) per call instead of O(returned records) — O(n²)
  to fully drain via small-limit polling (LOW-MEDIUM, severity capped since the memory changelog is
  explicitly a non-durable test/parity facility).** `store/memory/memorystore_changelog.go:262-278`.
- **17h. Index-creation doors hold the exclusive write lock for the entire scan-and-build, unlike the
  documented 3-phase pattern (LOW-MEDIUM, possibly acceptable for pure-RAM but deviates from a stated
  rule without a carved-out exception).** `store/memory/memorystore_index.go`, `memorystore_rel_index.go`.
- **17i. `defer bumpNodeEpoch()`/`bumpRelEpoch()` fire even on validation-failure/no-op error paths
  (LOW, already documented as a deliberate/safe tradeoff — flagged only for future profiling).**
### BACKLOG 18 — Badger backend hardening

- **18f. Meta/registry persistence (`MetaSet`, `Save*Registry`) lacks the immediate pre-call
  `dbClosed` guard that `flush()` uses (MEDIUM, the same forever-block class CLAUDE.md calls
  "hard-won").** `badgerstore_meta.go:169,190,239,317,335`.
- **18k. `NodesAsOf`/`RelsAsOf` open one Badger read transaction PER ENTITY instead of sharing one
  across the scan — O(N) independent transactions for a graph-wide as-of query (MEDIUM, perf).**
  `badgerstore_txtime.go:285-333,336-384`.
- **18l. `PutRelEntityAndOut`/`DeleteRelEntityAndOut` skip rel property-index and type-class-count
  maintenance — currently harmless (TieredStore declines both) but these are exported `Store` methods
  any direct caller could invoke (LOW-MEDIUM).** `badgerstore_partial.go`.
- **18m. Oversized-WAL migration guard doesn't cover the "explicit `MemTableSize` reverted to 0
  (stock)" transition — could reproduce the lesson-45 crash via a different input path than the one
  that's tested (LOW-MEDIUM).** `badgerstore.go:720-762`.
- **18n. `ForEachNodeByLabel`'s callback runs inside an open Badger read transaction, contradicting
  its own doc comment ("fn runs WITHOUT any store lock held") — not a deadlock risk but pins Badger's
  min read timestamp for the whole scan, inhibiting value-log GC (LOW-MEDIUM, undocumented
  operational tradeoff).** `badgerstore_node_scan.go:56-77`.
- **18o. Property/temporal-index-on-disk backfill commits lack the `dbClosed` guard used everywhere
  else — currently unreachable but a landmine for a future refactor (LOW).**
  `badgerstore_property_disk.go:680-693`, `badgerstore_temporal_disk.go:189-203`.
- **18p. `CollectShardDropResidue` requires `checkWritable()` despite being documented as read-only
  ("mutates nothing") — possibly deliberate but undocumented (LOW).** `badgerstore_shard_drop.go:23-26`.
- **18q. Code smells: duplicated ad-hoc "closed" checks instead of `checkOpen()` (`badgerstore_meta.go`,
  `badgerstore.go`); orphaned doc comment for a function living in a different file
  (`badgerstore_rel_batch.go:266-267`); ~20 sites compare `err == badgerv4.ErrKeyNotFound` directly
  instead of `errors.Is`, vs. ~4 using `errors.Is` (lesson 12 convention, may be accepted house style)
  (all LOW).**
- **18t. No direct self-loop round-trip test at the raw Store layer; no test pins change-log-marshal-
  failure-mid-write behavior (relevant to 18a); no adversarial test proves the frozen-row guard
  actually rejects mutation on an owned/ingest-transferred cache entry (TEST-GAP, 3 gaps).**
- **18u. Missing-feature (likely intentional): no zero-copy ownership-transfer cache path
  (`freezeRelForCache`) for bulk relationship writes, unlike nodes' `freezeNodeForCache` — see
  BACKLOG 21.**
- **18v. Vector-index apply-order (18e) and every other item in this section were cross-verified by
  two independent review passes on the same code (part A + part B); items not listed here (e.g.
  `dbClosed` guard on the primary `flush()` path, wire-format-version enforcement, encryption
  validation, `NodeAsOf`/`RelAsOf` pending-overlay-before-view ordering) were explicitly verified
  correct.**

### BACKLOG 19 — TieredStore hardening

- **19h. `TxChangeLogScope`'s per-tx shard snapshot has a documented, untested gap under rotation
  (MEDIUM).** `tieredstore_changelog.go:584-699`.
- **19i. `RunRepair` pins EVERY shard (including all cold shards) open simultaneously for the whole
  repair run, contrary to lesson 8 — `RebuildCatalog` in the same file does it correctly (MEDIUM).**
  `tieredstore_repair.go:34-42`.
- **19j. Cross-shard result accumulation in `AllNodes`/`AllNodeIDs` fanout is unbounded — materializes
  every shard's full result into RAM concurrently, undocumented as a caveat despite the streaming
  alternative (`ForEachByLabel`/`IterByLabel`) existing precisely to avoid this (MEDIUM).**
  `tieredstore_read_bulk.go:62-70,323-333`.
- **19m. Duplicated cross-shard residue-sweep logic between `sweepDroppedShardResidue` and
  `purgeNodesFanOut`'s phase 2 — a future fix to one is likely to miss the other (LOW).**
  `retention_purge_drop.go:167-200` vs `retention_purge.go:98-125`.
- **19n. Unbounded spin-wait in `Close()`'s shard drains, inconsistent with the bounded/timed-out
  drain used by the purge protocol (LOW, requires a pre-existing checkin leak to trigger).**
  `tieredstore.go:679-683,706-708,717-719`.
- **19p. Dead defensive-only bound check would silently swallow the exact invariant violation 19c
  would produce, instead of surfacing it (LOW).** `tieredstore_changelog.go:415-419`.
- **19q. Global `nodeCreateMu`/`relCreateMu` serialize ALL creates store-wide — a hard throughput
  ceiling for the stated TB/day workload, likely unavoidable given the correctness requirement (LOW,
  scaling constraint, not obviously cheap to fix).** `tieredstore.go:257-258`.
- **19r. Under-commented TOCTOU defense in fanout cold-shard reclassification — real but narrow-
  window, reads as removable dead code to a future maintainer (LOW).**
  `tieredstore_read_fanout.go:26-59`.
### BACKLOG 20 — Sharded backend hardening (WIP status)

- **20e. §4.5 pre-encoded-put fast path never routed for sharded despite the capability being
  satisfied — the ADR-0006 throughput benefit doesn't materialize for this backend today (MEDIUM,
  documented gap in the code's own comment).** `store/sharded/batch.go:328-336`.
- **20g. Adjacency reads always fan out to every claimed shard regardless of endpoint locality — an
  under-documented architectural cost that scales with `SlotCount` regardless of traversal locality
  (MEDIUM, perf).** `store/sharded/rel.go:241-288`, `node.go:183-248`.
- **20h. No upfront cross-validation between `Config.IngestLanes` and the sharded store's claimed
  slot range at construction time — misconfiguration only caught reactively at first write
  (LOW-MEDIUM).**
- **20i. `PutRelationshipsBatch` per-rel apply loop is not atomic per shard group and non-deterministic
  order — same class as 20c (LOW).** `store/sharded/batch.go:220-229`.
- **20j. `PruneTemporalCandidates` has no cross-backend equivalence test vs. a single badger store
  (LOW, TEST-GAP).**
- **20k. `forEachShardErr`/`fanOutUniform`/`parallelShards` spawn one goroutine per shard
  unconditionally, violating lesson 8's bounded-worker-pool rule — low practical impact given the
  32-shard hard cap (LOW).**
- **20l. `GetNodesByIDs`/`GetRelationshipsByIDs` shard-bucket application is sequential, not
  parallel, unlike every other multi-shard read in the file (LOW, perf).** `store/sharded/bulk.go:94-158`.
- **20m. Catalog is fixed identity-only — no re-sharding/rebalancing path exists at all; see BACKLOG
  21 for the feature-level entry (MEDIUM, missing feature, likely intentional "not yet" for a WIP
  backend).**
### BACKLOG 21 — Missing library-level features (cross-cutting)

Collected here from "missing feature" notes scattered across the subsystem audits — all rho-tkg-owned,
none sigma's:

- **21a. No `RelPropertyStats` (NDV/min/max HyperLogLog estimator) mirror on the relationship side.**
  `RangeCardinality`↔`RelRangeCardinality` and `PropertyTypeClassCounts`↔`RelPropertyTypeClassCounts`
  are deliberately mirrored pairs (the latter shipped as BACKLOG 5B specifically as a rel ordering-
  soundness primitive); `PropertyStats` has no rel-side equivalent, so a planner costing a
  relationship-property predicate has count/type-class tools but no selectivity estimate. Noted
  independently by 3 separate subsystem audits (façade, memory backend, stats layer).
- **21b. No index-introspection doors for property/temporal/vector/rel-property indexes, unlike
  `HasComposite`/`ListComposites` for composite indexes.** A query planner has no way to ask "does
  label X have a property/temporal/vector index on key Y, with what config?" without issuing the
  query and inferring from latency. `HasComposite`'s own doc comment frames the need generally
  ("so a planner can prove the accelerated path exists before routing"). Fix: add
  `HasProperty(label,key)`/`HasTemporal(label)`/`VectorIndexInfo(label,key)`/`HasRelProperty(type,key)`.
- **21c. No `RelTypeTemporalCandidateCapability` mirror of the node-side B4 prune capability** — the
  store contract's `PruneTemporalCandidates` is typed to `types.NodeID` only, so `relsByTypeLocked`
  can never get B4 acceleration unlike `nodesByLabelLocked`. Independently found from the query-door
  wiring angle too: `ByType`-with-property doors (`temporal_queries.go`) structurally cannot prune at
  all for exactly this reason, unlike their `ByLabel` node-side siblings.
- **21d. Sharded backend: `DeletedIterationCapability` entirely absent (see BACKLOG 20f)** — worth
  tracking here too since it's a genuine capability gap, not just a bug.
- **21e. Sharded backend: no re-sharding/rebalancing path** — growing/shrinking `SlotCount` on an
  existing deployment is unsupported; a mismatch is a fail-closed `ErrCatalogConflict` with no
  migration tool (see BACKLOG 20m). Likely intentional for the current WIP stage but worth an explicit
  decision/roadmap note before this backend leaves WIP status.
- **21f. No `PreEncodeRelPutPayloadV2` counterpart to the node-side §4.5 pre-encode fast path** — a
  relationship-heavy concurrent-ingest workload always pays the second msgpack pass that a node-heavy
  workload avoids (see BACKLOG 15p).
- **21g. Badger backend: no zero-copy ownership-transfer cache path (`freezeRelForCache`) for bulk
  relationship writes, unlike nodes' `freezeNodeForCache`/`PutNodesBatchOwnedPreEncoded`** — the
  public `PreEncodedPutCapability` contract is explicitly node-scoped by design (ADR-0006 §4.5), so
  this is very likely deliberate, but flagged as a future-extension opportunity if bulk relationship
  ingest throughput ever becomes a target (see BACKLOG 18u).
