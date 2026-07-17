# rho-tkg backlog — designed, ready to build in a focused session

**STATUS: BACKLOG 1 (retention purge R2–R5) and BACKLOG 2 (Model A) are SHIPPED**
(see CHANGELOG). Their design entries below are retained for reference / recovery.
The live remaining entries are BACKLOG 3 (consumer-gated columnar whole-node fetch)
and BACKLOG 4 (review-driven adaptations), plus a later tiered O(1) cold-shard-drop
purge optimization (perf only — functionality complete via the per-shard row scan).

Any new large subsystem must clear the house quality bar: rules 1–17 (esp. 15/16
two-phase + adversarial), cross-backend parity, **byte-exact replica convergence**,
`-race`, and `make cover` (no new public method at 0%, no new code < 80%). Full
prior design also lives in git history:
`git log --all -- docs/adr/0008-event-retention.md docs/adr/0010-cross-machine-edges.md`.

---

## BACKLOG 1 — Retention purge (hard-purge aged-out entities) — ex-ADR-0008 R2–R5  [SHIPPED]

### Why
Cybersecurity/observability workloads ingest TB/day of events (event→machine,
event→user edges) and must continuously REMOVE aged-out events at range scale.
Today's only removal doors are wrong for this:
- **Point delete** writes TOMBSTONE history rows for the node AND every connected
  rel — at retention scale this DOUBLES write volume to remove data.
- **History compaction (ADR-0001)** only trims OLDEST versions of *live* entities;
  events rarely update, so there's nothing to trim.

Neither hard-purges whole aged-out entities + their indexes + their history. That
is the gap.

### What's already shipped (R1 — the fail-closed guard, in the tree)
`ErrRetentionExpired` sentinel (canonical `internal/core/core.go`, re-exported
`pkg/graph`); per-label `retention_watermark/<labelToken>` MetaKV + a graph-max
fast-gate key (`retention_max_watermark`), rehydrated at open — all in
`pkg/graph/internal/core/retention.go`. Point temporal reads consult
`checkNodePointRetention`/`checkRelPointRetention`; scan reads fail the whole scan
via `checkScanRetention` (wired into `validateTemporalQueryOptsScan` + the
`nodesAsOfLocked`/`relsAsOfLocked` seams). **`advanceRetentionWatermark(labelToken, w)`
is the seam the purge calls after a range is fully clean.** `Admin.Reset` reaps it.
The guard shipped BEFORE the purge so a half-built purge can never read as complete.

### Decision recap (the contract to build against)
A first-class **retention purge** that hard-removes whole entities below a policy
boundary WITHOUT tombstones. Constrained on three axes (mirrors ADR-0001):
1. **Policy-gated** — `RetentionPolicy{Label, Mode {ByAge|ByValidTo}, Before}` to an
   admin door (`g.Admin().PurgeExpired…`); a store-global `Config.AllowRetentionPurge`
   must be on. No background sweeper in v1 — the operator/cron drives it.
2. **Watermarked** — advance the per-label watermark ONLY after its range is clean
   (crash mid-purge is closed by re-run; the guard already turns a below-watermark
   read into `ErrRetentionExpired`).
3. **Logged** — ONE logical `ChangeRangePurge{Label, Mode, Before, PolicyHash}`
   record, NOT N per-entity deletes.

**v1 = `ByAge`** (`snowflakeTime(id) < Before`): snowflake IDs are time-ordered so
`0x01/<8B id>` is time-CLUSTERED — "events of label L older than T" is a CONTIGUOUS
key range, a sequential scan-and-delete over a prefix, not N point lookups. The
`RetentionPolicy.Mode` field exists from day one so v2 (`ByValidTo`, via the `0x0B`
temporal index) needs no signature/record change.

### Staged build
- **R2 — single-store age purge (badger + memory). ✅ SHIPPED.** Store capability
  `store.RetentionPurgeCapability.PurgeNodesByLabelBefore(labelToken, before, chunk)`
  (badger composes recordless `cascadeDeleteInner` + `historyTruncateDeleteKeys(prefix, 0)`
  under one lock span → one flush; memory mirrors `DeleteNodeCascade` + history removal).
  Removes entity rows + version-history (`0x07`/`0x08`) + ALL index entries
  (label/adjacency/property/temporal/vector) + survivor inIdx cleanup (inherited from the
  cascade — both adjacency legs). Graph door `g.Admin().PurgeExpiredNodes(ctx,
  PurgePolicy{Label, Mode: PurgeByAge, Before})` gated by `Config.AllowRetentionPurge`,
  chunked (bounded `c.mu` span), advances the per-label watermark FIRST (over-state =
  fail-closed; a crash mid-range → `ErrRetentionExpired`, re-run finishes). Idempotent +
  resumable. Predicate on `storeutil.SnowflakeInstant` (mint-time), not ValidFrom/TxFrom.
  Refuses while change-log enabled (`ErrRetentionPurgeChangeLogEnabled`, lifts in R3).
  Tests: store-level both backends + graph e2e (fires `ErrRetentionExpired`) + gates.
  - **R2 sub-item — `UniqueForever` owner-registry reaping: ✅ SHIPPED.** The purge door
    snapshots the forever-owner set once (`foreverOwnerSnapshot` — nil when the registry is
    empty, so event-heavy graphs pay nothing), accumulates only purged IDs that are actually
    owners (bounded even at range scale via `RetentionPurgeResult.PurgedNodeIDs`, now surfaced
    by all three purge backends), and after the range is drained calls
    `reapForeverOwnersForPurged` (removes each purged owner's claim + re-persists the durable
    blob). Tests: purging a forever-owner frees its value for reuse; an unrelated purge leaves a
    surviving owner's claim intact.
- **R3 — `ChangeRangePurge` record + replica re-execution. ✅ SHIPPED.** New change tag
  13 + `storeutil.RangePurgeBody{LabelToken, Before, Mode}` + `store.RangePurgeLogCapability`
  (`LogRangePurge`, native memory + badger — appends one log-only record). The door emits
  the predicate record (watermark FIRST, then record, then physical purge), lifting the R2
  change-log refusal (now defensive-only: a change-log store lacking the log capability
  still refuses to avoid silent divergence). `core.applyRangePurgeLocked` re-executes the
  predicate on a replica (advances its OWN watermark — never a replicated MetaSet — then
  the same chunked purge; idempotent). Proven by `TestRetentionPurge_ReplicaConvergence`:
  exactly ONE `ChangeRangePurge` record, ZERO per-entity delete records, replica reaches the
  same purged state + advanced watermark, idempotent re-apply. Different shard count is the
  R4 extension (the record already names a PREDICATE, not physical rows).
- **R4 — SHARDED + TIERED: ✅ SHIPPED.** Sharded (ADR-0007): phase 1 fans out the
  per-shard label purge (parallel) — each shard removes its below-boundary nodes + co-located
  edges + history; phase 2 sweeps cross-shard edges (`PurgeAdjacentRelsForNode` per purged ID,
  driven by `RetentionPurgeResult.PurgedNodeIDs`) — the event-as-END cross-shard edge a
  per-shard purge misses. Record emitted once on the anchor shard. Proven by a store-level
  cross-shard-edge test + `TestRetentionPurge_ShardedReplicaConvergence` (sharded→sharded).
  **Tiered — SHIPPED (NOT a sharded mirror).** Tiered uses a SPLIT-WRITE cross-shard adjacency
  layout (`tieredstore_write_rel.go putRelationshipLocked`): a cross-shard rel's ENTITY (0x02) +
  OUT leg (0x05/start) live on the START node's shard, its IN leg (0x06/end) on the END node's
  shard — so the sharded `PurgeAdjacentRelsForNode(purgedNode)` sweep (which finds the edge via the
  purged node's adjacency because sharded co-locates both legs) MISSES tiered's residue: a
  `survivor→purged` rel leaves its entity+out-leg on the SURVIVOR's shard, keyed by the survivor.
  The tiered solution: phase 1 fans out the per-shard badger purge (`forEachOpenShard`) — but that
  purge now ALSO returns `RetentionPurgeResult.PurgedRels`, decoded from the purged node's
  adjacency KEYS (`purgedRelsForNodeLocked` — the key encodes BOTH endpoints, so a cross-shard rel
  whose entity is elsewhere and thus invisible to a local entity read is still captured). Phase 2
  routes each touched rel to its SURVIVING endpoint's shard and calls the new recordless badger
  `PurgeRelationshipByInfo`, which dispatches: entity present here → full delete + history; only a
  dangling in-leg → orphan-index purge. `LogRangePurge` → `ts.refShard.LogRangePurge` reaches the
  global merged feed. Proven by `TestTieredPurge_CrossShardEdgeSweep` (both residue shapes) +
  `TestRetentionPurge_TieredReplicaConvergence` (tiered primary → tiered replica re-executes the
  ONE predicate record, cross-shard sweep on the replica too, dangle-free, watermark advanced).
  Optimization (later, not built — DESIGN SCOPED, deliberately deferred): a ByAge range
  covering a whole aged-out event shard could physically DROP the shard (close + os.RemoveAll +
  catalog remove) instead of row-scan-cascading every entity, skipping the per-row delete writes
  + their flush (the write-amplification cost). Investigation found this is NOT a quick tweak —
  it is a delete-critical CONCURRENT subsystem with three hard constraints and one sharp edge:
  - **Not O(1):** still needs an O(nodes) READ scan of the shard for the purged count, the
    `UniqueForever` owner reap, and — critically — to enumerate cross-shard rels for the
    survivor-side residue sweep (a dropped shard's split-write edges leave residue on survivor
    shards, exactly as the row-scan path handles via `purgedRelsForNodeLocked`). The win is
    "skip the per-row cascade DELETES + flush", not constant time.
  - **Off under ChangeLog:** the row-scan purge leaves each shard's `0x09` change-log records
    intact (feed stays gapless); physically dropping a shard destroys its log segment → a
    tailing replica sees an LSN gap. So the drop MUST fall back to row-scan whenever ChangeLog
    is enabled (the replication config). Replication itself is unaffected (the ONE
    `ChangeRangePurge` predicate re-executes on the replica, which drops its own shard).
  - **Narrow eligibility:** ByAge only (`ValidTo` is orthogonal to the mint-time window),
    single-label shards only (`Labels ⊆ {purged}` — conservative, check the shard's label index
    for any foreign token), whole-window-below-boundary only (`shard.timeEnd <= before`).
  - **THE sharp edge — concurrent-write TOCTOU (the reason it is its own session):** the
    cross-shard rel WRITE path (`putRelationshipLocked`) does NOT serialize against `ts.mu.Lock`
    — it coordinates only through per-shard checkout refcounts (`shardForNodeIDChecked` →
    `checkoutStoreForRead`/`activeReqs`). So a concurrent writer can add a cross-shard in-leg to
    the dropping shard AFTER residue collection and BEFORE removal, leaving un-swept survivor
    residue = a dangling phantom (silent wrong result). A correct drop needs a DRAIN PROTOCOL:
    (1) under `ts.mu.Lock` unlink the shard from `ts.eventShards` so no NEW checkout routes to it
    (and handle the now-possible routing-miss for a concurrent edge-to-a-purged-node — it should
    fail cleanly, the target node is being purged anyway); (2) drain in-flight `activeReqs` to 0;
    (3) only THEN collect residue → sweep survivors → close → `os.RemoveAll` → `catalog.RemoveShard`
    → `catalog.Save` (add `RemoveShard`, mirror rotation's snapshot/restore rollback). Needs
    adversarial concurrent-write race tests (rules: shared-state → `test-race`) proving no phantom
    survives an edge-add racing the drop. A reusable read primitive is straightforward to
    (re)build: a badger `CollectShardDropResidue(labelToken)` returning (onlyLabel, nodeIDs,
    touchingRels) — the single-label check via `hasForeignLabelTokensLocked` + per-node
    `purgedRelsForNodeLocked` deduped by rel ID, read-only. Build it as its own focused increment.
- **R5 — `ByValidTo`: ✅ SHIPPED.** New optional `store.RetentionPurgeByValidToCapability.PurgeNodesByLabelValidToBefore`
  (native memory + badger; sharded/tiered fan out through the SAME cross-shard mechanism as
  ByAge — both refactored to a `purgeNodesFanOut` closure so only the per-shard predicate
  differs). Predicate = current-version `ValidTo != 0 && ValidTo < Before` (open interval never
  purged). Implementation reads the current `ValidTo` directly during the label-node scan
  (badger `getNodeLocked`, memory under-lock map read) — NOT the `0x0B` temporal index; that
  index would only be a later selection-perf optimization, exactly as ByAge does not use a
  special index either. **Key simplification vs the original sketch:** no under-lock re-confirm
  is needed despite selecting on a temporal field — a qualifying node is CLOSED, and a closed
  entity is frozen against every interactive mutation door (`rejectClosedNodeMutation`), so the
  predicate is immutable-once-true (dead re-confirm removed per Testing Rule 5). Record format
  already carried `Mode` (msgpack omitempty; ByAge=0/ByValidTo=1), so replication needed no
  wire change — `applyRangePurgeLocked` re-executes with the record's mode. Tests: exact-set on
  both backends incl. a `closedViaUpdate` two-phase case; tiered cross-shard sweep; ByValidTo
  replica convergence (selective — open survivor kept).

### Invariants (each needs a test)
1. **No silent absence** — a read pinned before a label's watermark → `ErrRetentionExpired`
   (two-door: point per-label incl. precision; scan whole-scan). Already guarded by R1;
   R2 must keep it true as the watermark advances.
2. **Idempotent + resumable** — purge below a watermark is a no-op; a crash mid-range
   re-runs to the same end state (watermark advances only after a clean range).
3. **No orphaned adjacency** — after purge, `VerifyConsistency` (sharded) / the
   tiered/badger equivalents report ZERO `AdjacencyOrphans`/`RelEndpointOrphans` for
   survivors. Adversarial: an event with an edge into a surviving machine node — the
   survivor's inIdx must be clean.
4. **Replica byte-exact convergence** incl. onto a different shard count (R3/R4 crown).
5. **Bounded locks** — never hold `c.mu.Lock` for the whole range; chunk it.
6. **History goes too** — no stale index entry or history row survives below the watermark.

### Risks / gotchas
- **Survivor inIdx cleanup is the expensive, non-range part** (an event→machine edge:
  the machine's `0x06/<machineID>/…` incoming entry must be removed or it's a phantom).
  v1 is correct-synchronous (clean it in the same chunk batch); v2's escape hatch is a
  lazy background repair sweep — but v1 must NOT leave orphans.
- **Purge vs a backfilled `tkg_tx_from`** (`AllowTxBackfill`): the purge predicate is on
  the SNOWFLAKE-ID time (immutable, mint-ordered), NOT the backfilled TxFrom. Backfilled
  facts below the watermark are rejected at WRITE (`ErrRetentionExpired` on create), never
  silently purged later.
- Interacts with ADR-0001 compaction (two coexisting watermarks: `CompactedThroughTx`
  per-entity vs `retention_watermark/<label>` per-label) and ADR-0002 `UniqueForever`
  (reap the owner entry in the purge chunk, else a value is barred forever by a ghost).

---

## BACKLOG 2 — Cross-machine incoming half-edge (Model A) + cascade — ADR-0010 §3.3  [SHIPPED]

### STATUS (2026-07-17): increments 1–4 SHIPPED & byte-exact verified. One narrow fail-closed follow-up remains (tx-rollback stub restore, below).
- **Inc 1 (store write):** `ChangeForeignIncoming` tag + badger `PutRelationshipForeignIncoming`
  (co-located stub, emits the new tag) + sharded `RecordForeignIncoming` (routes to the END
  shard). Store test green.
- **Inc 2 (replica apply):** `applyForeignIncomingLocked` routes the record by END-node slot
  (not the foreign rel slot), idempotently. `ForeignIncomingRelCapability`.
- **Inc 3 (graph door + convergence):** `g.Rels().RecordForeignIncoming(store.ForeignIncomingEdge)`
  — re-tokenizes the type, RECOMPUTES the content hash (byte-identical), via the kernel's
  `relPersistForeignIncoming` mode. `TestModelA_ForeignIncomingConvergence` proves: local
  visibility + slot-routed Get fails closed + BYTE-EXACT replica convergence + idempotent re-apply.
- **Inc 4 (cascade) — SHIPPED.** New `ChangeForeignIncomingDelete` tag (12) + body
  `storeutil.ForeignIncomingDeleteBody{RelID, EndID}`; badger `DeleteRelationshipForeignIncoming`
  (physically removes the co-located stub, emits the dedicated tag carrying the END-node ID);
  sharded `DeleteForeignIncoming(relID, endID)` capability (routes by END slot, idempotent).
  The LIVE connected-node delete path (`sharded.DeleteNodeWithHistory` — standalone AND in-tx via
  `deleteNodeInternal`) partitions foreign-slot stubs OUT of the rel-tombstone set (they have no
  version chain to tombstone) and removes each via the dedicated door BEFORE the node's own
  with-history delete, so badger's tombstone-validation sees a stub-free adjacency. The hard
  `DeleteNodeCascade` (import/rollback) got the symmetric guard. `applyForeignIncomingDeleteLocked`
  reproduces the removal on a replica routed by END slot. Byte-exact delete convergence proven
  (`TestModelA_ForeignIncomingDeleteConvergence`).
  - **Design note — why NOT a "skip + let cascade sweep" no-record approach:** badger's
    `validateDeleteNodeRelTombstonesLocked` requires a matching tombstone for EVERY physically-
    connected rel; the stub IS in the end node's physical adjacency, so it cannot simply be
    skipped. The dedicated record (routed by END slot, emitted BEFORE the node delete so the
    adjacency is stub-free when validation runs) is the clean resolution.
- **Inc 4 follow-up — tx-rollback stub restore: ✅ SHIPPED.** Promoted `ErrSlotNotLocal` to a
  store-level sentinel (`store.ErrSlotNotLocal`, re-exported as `sharded.ErrSlotNotLocal`, same
  value) so the partition-agnostic core can `errors.Is` against it. The tx forward-delete
  history snapshot (`deletedRelHistorySnapshot` / `relHistoryVersionExists`) now treats a
  foreign-slot `ErrSlotNotLocal` as "no local history" (a stub is adjacency-only, its history
  authority is the start machine), and `tx.restoreDeletedRelRow` restores a foreign stub via
  `c.foreignIncomingRel.RecordForeignIncoming` (routed by END slot, idempotent). Proven by
  `TestModelA_TxRollbackRestoresStub` (delete E-with-stub in a tx → force rollback → both E and
  the stub restored byte-identical).
- **Inc 4c — REMAINING (sigma-coordinated):** a door to delete a stub when the AUTHORITATIVE rel is
  deleted on the start machine. rho-tkg already exposes the local primitives (`g.Rels().Record
  ForeignIncoming` for create, store `DeleteForeignIncoming` for delete); the START→END fan-out
  RPC is sigma's.

### Original design (retained):


### Why
The cross-machine edge CREATE door is shipped (`g.Rels().AddByIDForeignEnd`), but
the incoming leg of a cross-machine edge lives on the START node's machine. So an
`IncomingRelationships(E)` query on E's OWN machine misses the edge — end-side
adjacency is partition-local. Model A forwards a half-edge to E's machine so
incoming adjacency is locally complete (required for cross-partition traversal:
each hop reads adjacency locally).

### Design findings (grounded in the code, 2026-07-17)
1. **Read+write are clean — the stub is a co-located put, no new keyspace.**
   `sharded.foldAdjacency` (`pkg/graph/store/sharded/rel.go`) resolves a shard's
   adjacency rel-IDs against the SAME shard (`shard.GetRelationshipsByIDs`), NOT via
   global slot-routing. So writing the whole stub rel onto E's shard via
   `PutRelationshipCoLocated` (routed by `shardForNodeID(endID)`, local on E's machine)
   makes `IncomingRelationships(E)` return it with ZERO fold change. `GetRelationship(relID)`
   on E still correctly fails `ErrSlotNotLocal` (E is not the rel's authority — a point
   read routes to the owner). The stub is adjacency-only.
2. **Registry alignment is required.** The stub's rel-TYPE token is the start-machine's
   token, meaningless in E's independent rel-type registry. The door must take the type
   NAME and re-tokenize locally (`GetOrCreate` in E's registry), then build the stub with
   E's token. The content hash keys on the type STRING (see the rel create kernel), so
   re-tokenizing preserves `tkg_hash` byte-identically — the stub matches the authoritative
   rel's identity. Mirrors the replication token-refetch discipline (lesson 51). ⇒ the door
   takes a descriptor `ForeignIncomingEdge{RelID, TypeName, StartID(foreign), EndID(local),
   FromHash, ToHash, Version, Temporal}`, NOT a `*types.Relationship` with a foreign token.
3. **THE BLOCKER — replica reproduction of the stub.** The stub is a rel whose ID sits
   in a FOREIGN slot but is physically on E's shard. The replica-apply path reproduces a
   rel put via `apply_record.go:~361`: it first probes `c.store.GetRelationship(id)` (on
   sharded → routes by rel-slot → foreign → `ErrSlotNotLocal`, NOT `ErrRelNotFound`, so
   apply errors out instead of creating), and even if it reached the create branch,
   `store.PutRelationship(r)` → `shardForRelID` → foreign → fail. So **E's replica cannot
   reproduce the stub through the existing apply path — a stub written on the primary
   vanishes on the replica = silent cross-machine adjacency loss on failover.** This is
   why Model A was NOT built inline.

### Approach to resolve the blocker (the actual build)
Add a dedicated **foreign-incoming change record** whose apply routes by END-node slot:
- New `ChangeForeignIncoming` tag in `pkg/graph/store/changefeed.go` (currently tags
  1–10; add 11, update `String()` + the `Valid()` range). Its body carries the stub's
  `RelWire` (which already holds the end-node ID → apply can derive the end-slot).
- The sharded store emits this tag (instead of `ChangeRelPut`) when `RecordForeignIncoming`
  writes the stub, co-committed on E's shard.
- Core apply (`apply_record.go`) recognizes the tag and calls a new store capability
  `ApplyForeignIncoming(wire)` that the sharded store routes to E's shard (by end-slot),
  **idempotently** (check the end's shard for the stub; no-op if present — do NOT let
  `PutRelationshipCoLocated`'s `ErrRelExists` fail a re-apply).
- Rejected alternative: making `PutRelationship`/`GetRelationship` end-slot-aware —
  fragile, because `GetRelationship(relID)` can't derive the end-slot from the relID alone,
  so idempotent re-apply breaks. The dedicated record is the clean design.

### Graph door + capability
- Extend `generatedcreate.ForeignEndpointRelCapability` (or a sibling) with
  `RecordForeignIncoming(r *types.Relationship, proof) error` — writes the stub on E's
  shard; require E live (`shardForNodeID(endID)` local); require the START foreign;
  reject a local "foreign" end (`ErrForeignEndpointLocal`, already exists). Only sharded
  implements it.
- Graph door `g.Rels().RecordForeignIncoming(edge ForeignIncomingEdge)` — re-tokenize the
  type, build the stub verbatim from the descriptor (supplied hashes/temporal, NOT
  recomputed — like replication apply), lock E, persist via the capability.

### Cascade (was task #21 — must land WITH Model A, not after)
- Deleting E must remove the foreign incoming stub on E's machine.
- Deleting the authoritative rel on the START machine must propagate stub removal to E's
  machine — sigma-coordinated fan-out; rho-tkg exposes the local primitives
  ("delete this foreign incoming stub" / "delete rels with this foreign endpoint").
- A half-designed cross-machine cascade is the B7 cross-shard-rollback bug class — design
  the delete paths alongside the write path.

### Tests (the crux is replication)
- `IncomingRelationships(E)` on E's machine returns the cross-machine edge after
  `RecordForeignIncoming`; `GetRelationship(relID)` on E still fails `ErrSlotNotLocal`.
- **Byte-exact replica convergence of the stub** — a replica of E's machine reproduces the
  stub via the new `ChangeForeignIncoming` apply (extend
  `pkg/graph/sharded_replica_convergence_test.go`). This is the gate that was missing.
- Cascade removes the stub on rel-delete AND on E-node-delete; the replica reproduces the
  removal. Idempotent re-apply is a no-op. Negatives (E foreign, start local, etc.).

### In-process test harness (no second machine needed)
Simulate "foreign" with a narrow slot range: a `sharded.Store` with `SlotCount` claiming
only some slots, an end-node ID whose slot is outside the range (see the existing
`pkg/graph/store/sharded/foreign_endpoint_test.go` — `foreignNodeID()` = slot 11 vs a
2-slot store). The create door's tests already use this pattern.

---

## BACKLOG 3 — Columnar / batch whole-node fetch — sigma ask X5-wholenode (consumer-gated, shape pinned)

Handed over by sigma-tkgd 2026-07-17. **Priority, not blocker** (sigma's framing):
filed upstream as `X5-wholenode`, re-open trigger = "rho-tkg ships the door OR a
`RETURN n`-over-wide-scan workload profiles hot." Captured now to pin the shape.

### The ask (one primitive)
A batch/columnar **whole-node** materialization door — the all-properties analogue of
the existing `g.Nodes().DocValuesSnapshot(label, propKeys)` (which serves only NAMED
property columns). Give every node of a label with ALL its properties, decoded in bulk
via ONE key-range scan, instead of N per-node point-gets.

### Why it's rho-tkg's, not sigma's
The cost is entirely inside the storage layer's per-node fetch/decode: `NodesByLabel` →
`fetchNodesByLabelIDs` loops `prefetchNodeScan(nid)` = one `Txn.Get`/`SafeCopy`/
`Slice.Resize` PER NODE (`pkg/graph/store/badger/badgerstore_node_query.go:105-113`).
Sigma reaches nodes only through `g.Nodes().ByLabel(...)` (already-materialized
`*types.Node`s) — no sigma-side lever on the decode. Storage-local, deterministic,
LLM-free.

### The caller (pins that a caller exists; NOT yet the return shape — see fork)
Cypher `MATCH (n:L) RETURN n` (+ whole-node projection / whole-node WHERE). Scan site:
sigma `pkg/cypher/exec_match.go findNodeCandidates`.

### Measured evidence (M4 Max, `BenchmarkProjectionScanDominant`, 100k `:Ev`)
| query | ns/op | B/op | allocs/op |
|---|---|---|---|
| `RETURN n` (whole node) | 307 M | 341 MB | 4.06 M |
| `RETURN n.a` (scalar → columnar doc-values path) | 16.5 M | 38 MB | 200 k |

`RETURN n` is ~18× slower / ~9× more allocs, linear in node count (also at 500k). pprof
of `RETURN n`: ~67% in the badger full-node fetch/decode (`prefetchNodeNoFill`), ~28% in
sigma's output-map build. The scalar path is fast because it rides the columnar doc-values
snapshot; whole-node has no such door.

### Design — OWNED by the core library (sigma adapts; NOT a fork for sigma to resolve)
The ask proposed a columnar snapshot "mirroring `DocValuesSnapshot`". As the library, we
decline that framing — it targets the wrong win mechanism. Two grounded corrections:
- **`LabelDocValues` holds numeric/string columns ONLY** (`docvalues.go:277`,
  `buildColumn` → `colUnbuildable` for bool/list/map/typed-temporal/mixed). The `any`-valued
  `Row(id, vals, present)` serves only those buildable columns. So a WHOLE-node snapshot is
  NOT a trivial DocValues generalization — it must hold ALL property types.
- **DocValues wins by CACHING** a compact numeric/string column across REPEATED aggregations
  over the same label; its build is itself N `GetNode` point-gets
  (`badgerstore_docvalues.go:262`). But `MATCH (n:L) RETURN n` is a **one-shot full-label
  scan** — a cache returns nothing on the only pass (and the benchmark's 18× is partly a
  warm-cached scalar vs uncached whole-node artifact: a cold one-shot `RETURN n.a` also pays
  the build). ⇒ a columnar cache does NOT help a one-shot `RETURN n`.

**Decision: the primitive is a single-range-scan BULK whole-node materialization**, streaming,
no cache:
`NodesByLabelBulk(label string, opts QueryOpts) iter.Seq2[*types.Node, error]` (name TBD) —
ONE badger `Iterator` pass (`PrefetchValues`) over the label's node keys, amortized decode,
instead of N `Txn.Get`s. Returns `*types.Node` (the natural currency; sigma adapts
`exec_match.go findNodeCandidates` to consume the stream). This is the lever for a one-shot
scan; it MUST take `QueryOpts` so it honors the B4 valid-time envelope prune ([4.17.0]).
- Bonus: the SAME bulk-scan substrate replaces the N `GetNode` point-gets in
  `buildLabelColumns`/`buildMultiColumns`, speeding DocValues COLD builds too — build that
  substrate first, then both consumers ride it.
- Explicitly OUT: a columnar all-property cache. Only revisit IF a real profile shows a
  workload RE-SCANS the same label repeatedly (then an epoch-keyed whole-node column cache
  on top is warranted — but note the memory cost the `MaxDocValuesNodes` cap already guards).

### Build notes
- **DONE (increment 1, [Unreleased]):** `forEachNodeBulk` — one `db.View` + one forward-
  seeking iterator inside `fetchNodesByLabelIDs`, transparently speeding `ByLabel` / `RETURN n`.
  MEASURED ~1.4× faster, ~5% fewer allocs single-threaded (more under concurrent scans).
  **Hard-won config lesson: `PrefetchValues=false` is load-bearing** — a Seek-per-ID iterator
  WITH prefetch measured ~10× SLOWER (re-fills a discarded value window per seek). The naive
  "one range scan beats N Txn.Gets" intuition was WRONG until the prefetch knob was fixed by
  measurement. Single-threaded the DECODE (msgpack→Node), not the fetch txn, is the floor.
- **✅ SHIPPED (increment 2) — PARALLEL decode.** `collectNodesBulkParallel` (badger): the
  serial iterator pass SEEKS + `ValueCopy`s each cache-miss's raw bytes (txn not concurrent-safe),
  then fans the CPU-bound decode across `GOMAXPROCS` contiguous chunks into per-node result slots.
  Wired into `fetchNodesByLabelIDs` for unbounded (`Limit==0`) scans of `>= parallelDecodeMinIDs`
  (2048) candidates; Limit'd scans keep the serial early-stopping door. Decode verified
  data-parallel-safe (reads the RLock-protected property-key registry, mutates only a per-decode
  local wire, per-node Freeze, own result slot). **MEASURED ~3× (82→27 ms/50k), +17% transient
  memory, +2% allocs.** Correctness: parallel == serial on present/tombstoned/absent mix + wired
  `NodesByLabel` equivalence, `-race` clean. Remaining follow-up: batched raw-byte staging to bound
  memory for extreme scans; cache-hit-heavy scans skip the fan-out (served inline). Tiered/sharded
  inherit it (fold per-shard through badger).
- Remaining spread: apply the bulk substrate to `AllNodes` + the `getProp`/`GetNode` loop in
  `buildLabelColumns`/`buildMultiColumns` (speeds DocValues cold builds). Memory store already
  trivial (live objects); tiered/sharded fold per shard.
- Gate every step with the A/B benchmark (`badgerstore_node_bulk_bench_test.go`) — evidence-
  driven; the prefetch footgun proves "measure, don't guess." Cross-backend parity + `QueryOpts`
  (valid-time) correctness: the scan must return the SAME node set as `ByLabel(label, opts)`.

### Sibling (same family, standing / lower priority) — as-of columnar
`DocValuesSnapshotAsOf(label, keys, txAt)` — time-travel columnar aggregation
(sigma `X5-temporal`). Verified vs v4.17.0: `DocValuesSnapshot` is current-state only;
`g.Temporal()` returns as-of node STRUCTS, not columns. If a whole-node snapshot (above)
is built, an as-of variant of BOTH closes `X5-temporal` too. Lower priority.

### Deferred sibling (no shape yet) — dry-run constraint validation
Already on the consumer-gated list (HP2.5). Sigma can't pin the shape until it builds the
HP2.5 enforcement kernel; the exact caller comes then.

---

## BACKLOG 4 — Review-driven adaptations with silent-wrong-answer sharp edges (fresh session each)

From the since-v4 critical design review (2026-07-17). These are REAL, worth doing, and
adaptable (all additive/opt-in, no released consumer depends on the acceleration). They are
here — not shipped inline — because each has a SILENT-CORRUPTION failure mode that must not be
rushed at the tail of a long session. Already shipped from that review: the bulk label scan
(BACKLOG 3), `TemporalValue.AsTime`, the sharded stale-doc fix, and the B6 SIEM/per-node
CHANGELOG correction.

### 4a. B4 temporal-envelope prune → PER-VERSION interval entries — DECIDED: DO NOT BUILD
**Decision (2026-07-17, owner-confirmed workload):** the workload is mostly current-tx /
current-valid-time, only SOMETIMES previous-valid-time. For current-valid queries the
per-version prune is MOOT (the open current version overlaps "now"; per-node already prunes
the expired/deleted members, which is the real win there). Per-version would help only the
"sometimes previous-valid" case while paying a memory-∝-version-count tax always → net
negative for this workload. Kept per-node; the CHANGELOG [4.17.0] B4 entry now states the
honest scope (accelerates expired/deleted/closed-interval members; inert for updated entities
on past-window queries). The analysis below is retained for the record only.

**Problem (verified):** the prune drops a candidate only when its valid-time envelope can't
overlap the query, but `index/temporal_index.go` stores ONE entry per node (`Add` = replace
semantics; `Extend` = union), and by the domain rule an updated entity's current version is
open-ended, so `effTo(0)=+∞` (`temporal_index.go:16,18`) makes its envelope `[minFrom, ∞)`.
⇒ the moment an entity is updated once, the upper-bound test NEVER prunes it — only `minFrom`
prunes, and only for past-pinned / future-window queries. The shipped "~2.0×" was benched on
90%-cold SINGLE-VERSION CLOSED nodes (best case); for a supersede-heavy TKG (the defining
workload) it does little. NOTE: `EnvelopeOf` AND `QueryOverlap` both read the same per-node
envelope, so switching the prune to `QueryOverlap` does NOT fix it.
**Fix:** make the index hold one interval entry PER VERSION (a closed past version `[a,b)` can
then prune independently of the open current version). `QueryOverlap` (already maxTo-augmented,
per-interval) then returns nodes with ANY version overlapping — correct for updated entities.
**Sharp edge (why fresh session):** this is a redesign of a CORRECTNESS-critical temporal
structure. Per-version means an id appears multiple times in `Entries` (memory ↑, and `byID`/
`EnvelopeOf` semantics change or retire); maintenance must add a version entry on EVERY history
write across memory+badger+sharded AND the `0x0B` persistence; a bug under-prunes (recall loss,
safe) OR over-prunes (drops a real match = SILENT WRONG ANSWER). Gate: the existing
`TestTemporalCandidatePruneEquivalence` extended with MULTI-VERSION updated nodes (the case the
current bench omits), index-present == index-absent == pinned expected, on all backends.
**Cheaper alternative if per-version is too big:** keep per-node but STOP implying general
temporal-scan speedup — document it as an accelerator for closed-interval/archival labels only,
inert for actively-updated labels, and re-bench with updated nodes to size the real win.

### 4b. DocValues per-LABEL epoch (MEDIUM value)
**Problem (verified):** `badgerstore_node.go:267` bumps a GLOBAL `nodeEpoch` on every node
write, invalidating EVERY label's cached column snapshot; each rebuild is N `GetNode`
point-gets (`buildLabelColumns:263`). Under write-active ingest the cache is perpetually cold —
the same caching-moot mismatch as X5-wholenode, in the shipped scalar path.
**Fix:** a per-label epoch (`map[uint16]atomic.Uint64`) bumped only for the labels a written
node carries; DocValues stamps + re-checks the label's epoch, so an unrelated-label write no
longer invalidates.
**Sharp edge (why fresh session):** the global epoch is SAFE by over-invalidating. Per-label
trades that safety for perf and demands covering EVERY membership-changing path (PutNode,
batch, history replace, delete, AddLabel/RemoveLabel, and the multi-label intersection cache's
epoch choice). A single missed bump site = a stale column served = SILENT WRONG AGGREGATION.
Also weigh vs BACKLOG 3's bulk-decode substrate (make cold builds cheap, no cache) — that may
be the better lever than patching the cache.

### 4c. B6 `HistoryAnchorInterval` → config, WITH a persisted compat marker (LOW value)
**Problem:** the anchor spacing is a hardcoded `const = 16` (`storeutil/wire_history_delta.go:41`),
never swept (8/32/64) against the 39% figure.
**Sharp edge (why not a trivial config field):** the interval is baked into the ON-DISK delta
layout — anchors sit at `V - V%interval`. A store written at 16 then reopened at 32 would
reconstruct a delta against the WRONG anchor = SILENT delta MISREAD. So a config field REQUIRES
persisting the interval as a store marker (like `wire_format_version`) and failing closed on a
mismatched reopen. Low value (a tuning knob on an opt-in, still-soaking feature) — do only if a
sweep shows a materially better interval.

### 4d. Ingest strong-mode / IntentRecord cleanup (MEDIUM value, LOW risk) — ✅ DONE
**Shipped:** the serializable `IntentRecord` codec (`EncodeIntent`/`DecodeIntent`/`wireBody`) was
removed 2026-07-17 (verified test-only, no non-test caller — CLAUDE.md ingest section + git
history). The load-bearing §4.5 producer pre-encode was KEPT (concurrent mode reuses it). The doc
steering is in place (CLAUDE.md marks the single-applier strong-mode pipeline as the durable-
`SyncWrites` fsync-amortization niche and steers throughput to concurrent mode). No code remains.
Original analysis retained below for the record.


**Problem (verified):** strong mode (single applier) "p8 gains nothing over p1" (CHANGELOG
[4.14.0]); concurrent mode (self-apply, no applier/handoff/intent records) is the measured
throughput winner. The serializable `IntentRecord` codec (`EncodeIntent`/`DecodeIntent`,
half-edge decomposition, `wireBody`) is verified TEST-ONLY (no non-test caller) — speculative
infra for a Lanes:N future that never shipped. The §4.5 producer pre-encode IS load-bearing
(concurrent mode reuses it) — KEEP it.
**Action:** (1) doc: mark the single-applier strong-mode pipeline as the durable-`SyncWrites`
fsync-amortization NICHE, steer throughput to concurrent mode (CLAUDE.md ingest section +
docs/adr note); (2) either wire `IntentRecord` to a real distributed producer or move the codec
+ its tests to a design note/branch rather than carrying inert code as if shipped. Low risk
(removing verified-unused code) but doc-heavy — hence a deliberate pass, not a tail-of-session edit.

### 4e. `NowTx()` observability footgun (LOW value) — ✅ DONE
Shipped `g.Temporal().PeekTx()` — a non-reserving read of the transaction clock
(`Core.peekNow` = `max(wall, lastInstant)` with NO CompareAndSwap), the observability
sibling of `NowTx` for metrics/polling loops that must not burn instants. Documented
LOUDLY (core + sub-API godoc) as NOT a sound as-of pin — it can coincide with a
concurrent write's stamp, so pinning at it includes/excludes that write
nondeterministically; a sound pin is `NowTx()`, a value returned BY a write, or a
named `TagAsOf`. The `NowTx` godoc is hardened to steer polling to `PeekTx`.
Deterministic test (floor pushed above wall via `AdvanceClock`): 1000 `PeekTx` calls
return the floor exactly and burn ZERO instants (the next `NowTx` is floor+1), both
backends.
