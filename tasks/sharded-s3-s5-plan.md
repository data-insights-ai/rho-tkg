# Sharded store S3–S5 execution plan (ADR-0007)

**NEVER commit this file to `main`** — it lives on the integration branch only and
is dropped at squash (merge protocol). Working notes for the next session.

State at time of writing: **S1 + S2 DONE** (`54fca8c`, `644125b`), both gated
(full non-short suite green; sharded pkg 77.1% cover, no 0% funcs; race-clean on
sharded + core + graph). ADR-0008 (retention) drafted for owner review (`cbe8f68`).

The badger seam for the change-log is ALREADY complete and is the template:
`badger.Store.EnableChangeLogWithSource(seq ChangeLogSeqSource, onFlush func() error)`,
`Config.ChangeLogSeqSource`, `Config.OnChangeLogFlush`, per-shard co-commit of
records + `LastLSNKey`. Tiered already does exactly this
(`pkg/graph/store/tiered/tieredstore_changelog.go`, ~23KB) — S3 is "replicate the
tiered pattern for slot routing instead of time routing."

---

## S3 — change-log + topology-independent replica convergence

**Why this is the highest-risk stage:** replication correctness is the
silent-wrong-answer class the CLAUDE.md persistence rules are most emphatic
about. Do NOT commit any part of S3 without the byte-exact convergence crown
GREEN, including onto a DIFFERENT shard count. Owner-in-loop for this stage.

### S3.1 — config + allocator wiring
1. Add `Config.ChangeLog bool` to `sharded.Config` (mirror `tiered.Config.ChangeLog`).
2. Add a store-global allocator `changeLogAllocator` (an `atomic.Uint64` that
   satisfies `badger.ChangeLogSeqSource`: `Next()` = `Add(1)`; `Observe(w)` =
   monotonic max-in). ONE per `sharded.Store`, injected into EVERY shard's
   `badger.Config.ChangeLogSeqSource` in `shardConfig` when `cfg.ChangeLog`.
   → LSNs form ONE total order across shards (the merged-feed invariant).
3. Inject `OnChangeLogFlush` on the ANCHOR shard only → it persists the
   monotonic `changelog_lsn_watermark` MetaKV key after every log-bearing flush
   (tiered's refShard pattern). Reseed at open reads ONLY that key + a
   belt-and-braces fold of each open shard's `LastLSNKey`; an unreadable
   watermark FAILS CLOSED (poison the allocator, sticky error on every feed door).

### S3.2 — the merged feed (barrier-first + W-bounded)
Implement `ForEachChange` / `ChangeFeed` / `LastCommittedLSN` on `sharded.Store`:
1. `Flush()` folds `shard.Flush()` over every shard (barrier — already exists).
2. Capture `W = LastCommittedLSN` (max persisted LSN across shards).
3. Paged k-way min-heap merge over every shard's `0x09/<LSN>` keyspace, emitting
   ONLY `LSN <= W` (heads > W deferred to next poll). All shards are open local
   badgers (unlike tiered's cold-checkout), so this is simpler than tiered — no
   checkout/checkin, just N open prefix iterators into one heap.
   *Reuse the tiered heap-merge code as the template; the paging/W-bound logic is
   identical, only the shard-enumeration differs (all-open vs catalog-checkout).*

### S3.3 — TxChangeLogScope
`sharded.Store` satisfies `store.TxChangeLogScope` (per-tx record buffer): a
store-level `SetLogDivert` seam buffering per shard, LSNs minted at commit so a
rolled-back tx/batch burns none. Mirror tiered's `SetLogDivert`. NOTE the sharded
batch doors (S2) are per-shard-group; a scope spanning shards buffers per shard
and mints contiguous LSNs at commit.

### S3.4 — admit sharded in core + replica
1. Add `*sharded.Store` to the `changeFeedCapability` switch in
   `internal/core/core.go` (~line 1062) and any sibling capability switches
   (`ChangeLogStatusCapability`, `TxChangeLogScope`) — grep `case *tiered.Store`.
2. Cascade-delete records: a cascade whose neighborhood is LOCAL to one shard
   delegates to `shard.DeleteNodeCascade` (ONE cascade record, byte-identical to
   single-store). A CROSS-shard neighborhood emits one `ChangeRelDelete` per edge
   (ADR-0005 §2.4 pattern) — the S2 cascade already deletes per-shard, so wire the
   per-edge record emission there under the change-log path.

### S3.5 — gates (ALL must be green before commit)
- Cross-shard LSN total-order + gaplessness (ADR-0005 battery re-pointed).
- Three-way cross-backend feed parity (single badger vs tiered vs sharded on the
  SAME op sequence → byte-identical record bodies). Extend
  `changefeed_parity_test.go`.
- **CROWN: byte-exact replica convergence from an N-shard sharded primary onto a
  replica with a DIFFERENT shard count** (e.g. 4-slot primary → 2-slot replica →
  single badger replica). Bootstrap via export snapshot + tail the feed; assert
  final current state byte-identical. This is invariant #4 and the whole point.
- Reseed after reopen; rolled-back-scope emits nothing; race-clean.

---

## S4 — lanes + per-lane generators (HAS AN OWNER HARDWARE GATE)

> **STATUS 2026-07-16: CORRECTNESS CORE DONE & COMMITTED.** `Config.IngestLanes`
> (0=off default), per-lane UNIFIED generators built in `core.New`
> (`ingest_lanes.go`), `BatchBuilder.genLane` routes the `batch_queue.go` mints,
> concurrent ingest session pins lane→slot. The silent-ID-collision class is
> CLOSED by the committed collision battery (3.5M node+rel IDs, global
> uniqueness) + concurrent-mint race gate + slot-pinning + slot-exhaustion
> fail-closed + end-to-end concurrent-lane pinning + sharded integration
> (`TestGraphLevelIngestLanesRouteAcrossShards`) — all race-clean. **The ONE
> remaining item is the throughput acceptance bar (≥2.75M/s on the owner's M4
> Max) — hardware-gated, owner runs the bench; a dev-box number is not claimable
> and is NOT claimed.** CHANGELOG + CLAUDE.md updated.
>
> **Execution-ready integration map (verified 2026-07-16):**
> - Generators built ONLY in `core.New`, `core.go:1208-1222`: `nodeIDGen` =
>   `snowflake.NewNode(SnowflakeNodeID*2, …)` (EVEN node-field), `relIDGen` =
>   `NewNode(SnowflakeNodeID*2+1, …)` (ODD). The even/odd value-uniqueness invariant
>   lives ONLY here. Struct fields `core.go:47-48`.
> - All minting funnels through `c.nextNodeID`/`c.nextRelID` (`validation.go:504-510`);
>   5 call sites, all prepare-time, none lane-aware: `batch_queue.go:99` (node),
>   `batch_queue.go:259` (rel), `node_add.go:171`, `relationship_add.go:171` & `:285`.
> - Lane exists end-to-end but is inert as a routing key: `SubmitToken.Lane`
>   (`ingest.go:94`), `Session.lane` (`ingest.go:652`, set by `nextIngestLane`
>   `ingest.go:581`, only when `opts.Concurrent`), `IntentRecord.Lane`/`PeerLane`
>   (`ingest_intent.go:71-78`). Strong mode always Lane 0; concurrent mode tags a
>   nonzero per-session lane but never routes it to a generator.
> - Concurrent apply mints NOTHING at apply time — IDs are pre-minted on the caller
>   thread in `batch_queue.go`; `ingest_concurrent.go` consumes `pn.node.ID()` etc.
>   So lane→slot pinning must happen at the PREPARE mint, keyed by the session lane.
> - Sharded routing: `slotOf(id)=DecomposeID(id).NodeID` (`sharded.go:306`),
>   `shardForID` (`sharded.go:311`), catalog identity slot→shard map
>   (`catalog.go:43-62`), `maxSlots=32`, discipline marker `disciplineUnified=1`
>   "single generator per slot (nodes+rels)" (`catalog.go:16-19`) — the contract S4
>   must honor.
>
> **Safe design (preserve value-uniqueness):** ONE generator per claimed slot
> (node-field = slot value), used for BOTH node and rel mints in that slot, so every
> mint draws the next (time, seq) → no node/rel value collision within a slot;
> cross-slot differs by node-field. Interactive lane (standalone/tx/batch) mints from
> the BASE slot generator. A concurrent ingest session pins lane→slot→shard, mints
> its whole group in one slot → one shard → one batched door. Non-sharded backends
> KEEP the even/odd model unchanged (dual model — gate on the store's slot discipline,
> not unconditionally). **PRIMARY GATE (verifiable here, unlike throughput): mint
> millions of node+rel IDs across all lanes and assert GLOBAL uniqueness of the
> node∪rel ID set — the silent-collision class. Do NOT commit S4 without this green.**

1. `Config.SlotCount` already exists; add per-lane generator pairs so a concurrent
   ingest session pins lane→slot→shard and a whole commit group mints in one slot
   → lands on ONE shard as one batched door call (group-commit economics survive).
   Core currently mints legacy dual-generator IDs; S4 wires per-lane generators
   built in `New` (generators are only built there — lesson 51).
2. Non-ingest writes (standalone/tx/batch) mint from the base slot ("interactive
   lane").
3. **MEASURED BAR (owner's M4 Max, same-run baselines):** badger-backed sharded,
   8 lanes / 8 slots, ≥2.5× the single-store concurrent p8 (~1.1M/s → ≥2.75M/s),
   change-log ON within 1.2× of off. Honest pprof if the disk/flush wall lands
   earlier. **This bar only means something on the owner's hardware — do not
   claim it from a dev-box number.** Owner runs / confirms the bench.

## S5 — capability parity sweep + docs + release v4.16.0

1. Enumerate remaining declined optionals (vector/temporal/high-freq/property
   index) with reasons; close toward parity as tiered was (staged, not skipped —
   standing owner requirement).
2. Update CLAUDE.md (Status line + the sharded store section), docs/architecture.md,
   docs/api.md, bench tables.
3. Version sync → release v4.16.0 (squash to main; scrub tasks/ + any sigma refs;
   `make ci-docker`; tag; GitHub release). Retention (ADR-0008) is a SEPARATE
   later release per the owner.

---

## Notes / gotchas surfaced during S2
- `sharded.Config` per-shard passthroughs are applied uniformly via `shardConfig`
  — add `ChangeLog`/`ChangeLogSeqSource`/`OnChangeLogFlush` there.
- The anchor shard (`shards[0]`, slot BaseSlot) is the store-global metadata home
  (catalog, registries, watermark) — the tiered refShard analogue.
- All shards are OPEN local badgers (no cold checkout), so the S3 merge feed is
  strictly simpler than tiered's — do not copy tiered's checkout/checkin.
- `PurgeOrphanRelationshipIndexes`, `PutRelIncoming`, `PutRelEntityAndOut`,
  `IncomingIndexEntries`, `AllRelIDs`, `AllNodeIDs` are the badger partial doors
  S2 leaned on — all exist and are tested.
