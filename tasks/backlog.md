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


