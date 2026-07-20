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

- **12i. [STILL OPEN — NOT resolved; investigated, a safe fix needs more design work than a backlog-
  sweep patch].** `strictCheckMergeRecord` (Strict mode's "delta applied onto the wrong base" detector,
  `import_merge.go:463-503`) explicitly checks `ChangeNodePut`/`ChangeRelPut` (only when
  `body.WithHistory` — the signal a PRIOR version must already exist) and `ChangeNodeDelete`/
  `ChangeRelDelete`; every other tag (`ChangeNodeHistoryVersion`, `ChangeRelHistoryVersion`,
  `ChangeNodeHistoryTruncate`, `ChangeRelHistoryTruncate`, `ChangeForeignIncoming`,
  `ChangeForeignIncomingDelete`) falls through the switch with no default case and returns nil
  unchecked. Investigated whether `ChangeNodeHistoryVersion`/`ChangeRelHistoryVersion` (the two most
  structurally similar to the already-checked WithHistory puts) have an equally safe "entity absent
  from base ⇒ wrong base" signal: they do NOT, at least not as simply. `applyNodeHistoryVersionLocked`
  itself has no existence precondition (`PutNodeVersion` writes unconditionally), and
  `import.go`'s final trust-boundary pass documents that a node can legitimately be "history-only"
  (deleted at the source) — so a base with zero prior knowledge of an ID receiving a bare
  history-version record is not obviously "wrong base" the way a WithHistory PUT for an absent entity
  is (that one is unambiguous: a WithHistory put's OWN semantics guarantee a prior version existed on
  the primary). A candidate refinement (only flag `Version > 0` history-version records for an entity
  with NO current row, NO existing history, and NO compaction stub) requires reasoning through
  compaction-stub interaction and delta-boundary edge cases (does `ExportSince` ever emit a bare
  history-version record with no accompanying WithHistory put for the same entity in the same delta?)
  that a backlog-sweep patch should not attempt without the bitemporal oracle's usual scrutiny —
  Strict mode exists specifically to catch dangerous wrong-base merges, so a subtly wrong check here
  (false negative OR false positive) is worse than the current honest gap. Left open rather than
  force a fix with unverified edge-case coverage.** `import_merge.go:461-503`.
### BACKLOG 13 — Retention / compaction / admin hardening

- **13c. [STILL OPEN — NOT resolved; investigated in depth, a safe fix needs a new store-level chunked
  capability across 3 backends, not a backlog-sweep patch].** `CompactHistoryNodes`/`Rels`
  (`internal/core/compaction.go:518-599,601-681`) hold `c.mu.Lock()` (the FULL exclusive graph lock)
  across a whole-graph plan-then-write pass: `c.allNodeChainIDs()`, a planning loop over EVERY entity
  building an in-memory `plans` map, then a write loop calling `compactor.CompactNodeHistory` per
  entity — all under one held lock, with no chunking, no periodic release, no `Label` scoping (always
  the whole population). This genuinely contradicts retention purge's own documented "chunked-lock
  discipline" (`retention_purge.go` `PurgeExpiredNodes`, invariant 5: "the graph lock is NOT held
  across the range"). Investigated WHY the two differ structurally, not just that they do: retention
  purge's chunking is possible because `store.RetentionPurgeCapability.PurgeNodesByLabelBefore(token,
  before, chunkSize)` is a STORE-LEVEL RANGE primitive — the store internally chunks and self-locks
  per chunk, so the Core layer never holds `c.mu` across the range at all (it just loops calling the
  store's own chunked primitive). `store.HistoryCompactionCapability`, by contrast, is PURELY
  per-entity (`CompactNodeHistory(id, keepVersions, metaWrites) error` — one entity, one atomic
  write) — there is no store-level range/chunk primitive to loop over at all; the Core layer is doing
  ALL of the range iteration and planning itself, in-process, which is why it ended up holding one
  lock across everything. A safe fix therefore needs a NEW store capability (a chunked
  `HistoryCompactionCapability` variant mirroring `RetentionPurgeCapability`'s shape) implemented
  across memory/badger/tiered (tiered is the hard case: ADR-0001's own doc notes the stub and the trim
  already land on DIFFERENT shards there and must be ordered stub-before-trim to fail closed on a
  crash — a chunked version must preserve that same crash-safety property per chunk, not just per
  entity). Rejected a smaller "just release c.mu every K entities inside the existing loop" patch as
  UNSAFE, not just incomplete: the current code plans ALL entities up front (reads each entity's chain
  once, computes `plan.stub`/`plan.keepVersions` from that snapshot), then applies every plan in a
  second loop; releasing the lock between planning and a later entity's apply step opens a window
  where a concurrent write to that not-yet-applied entity (another update, or even a second compaction
  run) invalidates its plan — applying a stale plan atop changed history corrupts the compaction stub
  (wrong `LastTrimmedTxTo`/`TrimmedThroughVersion`), a genuine data-corruption bug in exactly the
  history/temporal caution class CLAUDE.md already flags for BACKLOG 10b-adjacent code. Any real fix
  must re-plan (or re-validate) each entity immediately before applying it, which is what a proper
  chunked store primitive does but a naive lock-release patch does not. Left open rather than force
  either a correctness-risking naive patch or an unreviewed multi-backend capability redesign inside a
  backlog sweep.**
- **13l. [PARTIALLY OPEN — the currently-known checklist IS well tested; the systematic future-proofing
  gap is NOT closed and needs a design decision, not a backlog-sweep patch].** `Admin.Reset`'s
  correctness (`admin.go:250-268`, `reapCoreStateForClear`) depends on a hand-maintained checklist of
  `reap*` calls. Investigated actual test coverage before treating this as a pure gap: EVERY currently-
  known reaped category already has a direct assertion —
  `TestApplyChangeRecord_ChangeClearReapsCoreStateLikeReset` (op counters, unique constraints,
  UniqueForever owners, named as-of tags, compaction watermark, retention watermark, entity count, all
  via the SAME `reapCoreStateForClear` Reset()/ChangeClear share), `TestAdminOpsResetClearsOperationCounters`,
  `TestAdminOpsResetPersistsRegistrySnapshotAfterClear` (registries are deliberately PRESERVED not
  reaped), and `TestAsOfColumnCache_RealChokesBumpEpoch` (as-of DocValues cache epoch bump). So the
  practical risk described by the original finding — "a currently-known category silently stops being
  reaped" — already has a regression net. What remains genuinely open is the SYSTEMATIC guarantee: a
  test that enumerates every MetaKV key constant used anywhere in the codebase and asserts each is
  either covered by `reapCoreStateForClear` or on an explicit preserved-by-design allowlist, so a BRAND
  NEW future MetaKV-backed feature cannot land without being forced to make that choice. A hand-written
  allowlist in a test file would just be a second hand-maintained checklist alongside the first,
  duplicating the risk rather than closing it — a real fix needs either static analysis (a linter rule
  grepping `MetaSet`/`MetaKey` call sites) or a `go generate`-driven registry, which is design work
  outside a backlog-sweep patch. Left open rather than fake-close it with a checklist mirroring a
  checklist.**

### BACKLOG 14 — Index / docvalues / stats / vector / events hardening (graph layer)

- **14h. Composite-index introspection (`ListComposites`/`HasComposite`) has no counterpart for
  property/temporal/vector/rel-property indexes — see BACKLOG 21 for the feature-level entry.**

### BACKLOG 15 — Internal primitives hardening (storeutil / locks / registry / wire codec)

- **15p. No `PreEncodeRelPutPayloadV2` counterpart to `PreEncodeNodePutPayloadV2` for §4.5 pre-encode —
  see BACKLOG 21 (LOW, likely intentional node-first scope).**
- **15s. Change-log body wrapper types have no custom msgpack encoders — reflection on every
  change-log-enabled mutation (MEDIUM-HIGH, perf, user-requested audit following BACKLOG 16f).**
  `internal/storeutil/changelog.go` (`marshalChangeBody`/`SafeUnmarshal` call sites at
  `changelog.go:163` and every `Decode*` helper in the same file). `NodeWire`/`RelWire`/`PropertyWire`
  already have hand-written `EncodeMsgpack`/`DecodeMsgpack` (no reflection — see CLAUDE.md's wire-codec
  notes and the BACKLOG 15h/15m test-gap work in this same area), but the 10 change-log BODY WRAPPER
  types that embed them do NOT: `NodePutBody`, `RelPutBody`, `NodeDeleteBody`, `RelDeleteBody`,
  `ForeignIncomingDeleteBody`, `RangePurgeBody`, `HistoryVersionNodeBody`, `HistoryVersionRelBody`,
  `HistoryTruncateBody`, `MetaBody`. `msgpack.Marshal(body)`/`SafeUnmarshal(payload, &body)` on any of
  these therefore falls back to msgpack's generic reflection-based struct encoder for the OUTER
  wrapper layer (the inner `Wire NodeWire`/`Wire RelWire` field still dispatches correctly to the fast
  path once reflection reaches it — only the wrapper's own 1-4 fields pay the reflection cost, not the
  full entity). `NodePutBody`/`RelPutBody` are BY FAR the hottest — emitted (encode) on every node/rel
  write when a change-log is enabled, and decoded on every record a replica applies
  (`applyChangeRecordLocked`) — so this is a real per-mutation cost on any change-log-enabled
  deployment (replication, tiered's store-global change-log), not a rare admin path. The other 8 body
  types are lower frequency (deletes, history-version/truncate, meta, range-purge) but share the same
  gap. Fix: hand-write `EncodeMsgpack`/`DecodeMsgpack` for all 10 body types following the exact
  `NodeWire`/`RelWire` pattern (`wire_encode.go`/`wire_decode.go`), verified byte-identical to the
  current reflection-based encoding via golden vectors (the same discipline BACKLOG 15g/15m/15h used)
  before any behavior-preserving swap — a change-log/replica-apply wire-format regression would break
  cross-version replica compatibility, so this needs the same rigor as the entity wire codecs, not a
  quick pass.
- **15t. `HistoryDeltaEncoding`'s delta wrapper types have no custom msgpack encoders — reflection on
  every delta-encoded history write when the opt-in feature is enabled (MEDIUM, perf, same audit as
  15s).** `internal/storeutil/wire_history_delta.go` (`EncodeNodeHistoryDelta`/`EncodeRelHistoryDelta`
  call `msgpack.Marshal(d)` on `NodeHistoryDelta`/`RelHistoryDelta` directly). Same shape as 15s: the
  embedded `Meta NodeWire`/`Meta RelWire` field and the `PS []PropertyWire` elements dispatch to their
  fast custom encoders once reflection reaches them, but the outer 3-field wrapper struct itself is
  walked via reflection. Lower priority than 15s: `HistoryDeltaEncoding` is opt-in (default OFF) and
  only engages on history-writes past the anchor interval (not every write), vs 15s's change-log body
  types which cover the CURRENT-row put path too.
- **15u. `propertyToWire`'s `ptCustom` branch (`storeutil/wire_value.go:450`, kept per BACKLOG 15f's
  investigation as necessary defense-in-depth) cannot avoid reflection inside THIS library at all — an
  inherent limitation, not a bug to fix here (INFORMATIONAL).** `msgpack.Marshal(p.Value)` marshals an
  arbitrary USER-REGISTERED custom property type whose concrete shape is unknown to this library at
  compile time; msgpack's reflection-based encoder is the only option unless the caller's OWN type
  implements `msgpack.CustomEncoder`/`CustomDecoder` itself (a caller-side opt-in the library cannot
  force). Action, if any: document this recommendation for custom-property-type authors (implement
  `msgpack.CustomEncoder` on your own registered type for max perf) in the `RegisterPropertyStructType`
  doc comment or docs/api.md — not a library-internal fix.
- **15v. Registry/index-definition/catalog persistence across badger/tiered/sharded (label/reltype
  registry names, property/composite/vector index definitions, tiered registry/temporal-index/vector-
  index files, sharded catalog) marshal plain slices/structs via reflection-based `msgpack.Marshal`
  with no custom encoders (LOW, perf, same audit as 15s — lower priority since these are ADMIN/GROWTH
  paths, not per-entity-write).** `store/badger/badgerstore_meta.go` (5 sites),
  `badgerstore_index.go` (4 sites), `badgerstore_rel_index.go`, `badgerstore_composite_index.go`,
  `store/tiered/{registry_file,temporal_index_file,vector_index_file}.go`,
  `store/sharded/{catalog,vector_index}.go`. All of these persist on registry GROWTH (a new label/
  rel-type token minted — bounded by distinct-name cardinality, cold after warm-up) or explicit ADMIN
  operations (index creation, shard rotation/close), never per-entity-write — genuinely low priority,
  listed for completeness of the reflection audit rather than because it is a live hot-path concern.

### BACKLOG 16 — In-memory index-engine hardening (HNSW / property & temporal index / HyperLogLog)

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
