# ADR-0007 — Slot-sharded store: lane-owned write pipelines routed by snowflake ID

Status: DRAFT — owner review requested
Date: 2026-07-13
Depends on: ADR-0005 (tiered change-log machinery, reused), ADR-0006 (ingest
architecture; this is the in-process form of its stage-3 partitioning)

## 1. Problem

After the v4.14–v4.15.1 ingest program, the write path is parallel up to the
store and the change-log penalty is ~1.12x — but every lane still funnels into
ONE badger store. The mutex profile at 8 concurrent sessions shows ~94% of all
lock wait on the single store's index mutex, whose remaining critical section
is irreducible per-node bookkeeping (~0.7µs/node of map/index writes). Measured
consequence: 8 lanes yield ~1.1M inserts/s where linear scaling would predict
~4M. The wall is architectural: N writers, one store.

## 2. Decision (owner-directed design, refined)

**Partition the snowflake node-ID space statically between machines and lanes,
and give each lane its own physical store** — the ID itself is the router.

Three numbers, deliberately decoupled:

- **SLOTS** — ID-space partitions. A deployment claims a contiguous range of
  the existing 5-bit snowflake node field: `Config.SnowflakeNodeID` (base) +
  `Config.SlotCount` (n). Slots are the IMMUTABLE routing key: an entity minted
  in slot s carries s in its ID forever (`decompose(id).Node`). The budget is
  CLUSTER-WIDE and split purely between machines' lanes — machine A's x lanes
  use slots [y, y+x), machine B's lanes use their own range (the owner's
  model); a failover spare is just an unclaimed slot value, not a separate
  class.

  **The even/odd rel pairing is DROPPED in sharded mode** (owner question,
  resolved): today nodes mint with node-field `id*2` and rels `id*2+1`, purely
  to guarantee node IDs and rel IDs never collide as values — which halves the
  budget to 16 pair-slots. A single per-slot generator gives the same
  uniqueness guarantee trivially (one sequence cannot emit a value twice), so
  sharded deployments mint nodes AND rels from ONE generator per slot: the
  budget becomes the FULL 32 slots (a 128-CPU box wanting 32 local lanes now
  fits, as does 4 machines × 6 lanes + 8 free). Nothing in the codebase
  discriminates node-vs-rel by ID parity (stores key them in separate
  keyspaces; type always comes from context) — the pairing is a legacy layout
  detail that single-store mode keeps unchanged. The sharded catalog records
  the ID discipline so a directory fails closed if opened under the wrong one.
- **LANES** — runtime writers. Each lane owns one claimed slot's single
  generator and mints independently: ZERO shared mint state.
  A concurrent ingest session is pinned to a lane, so a whole commit group
  mints in one slot and lands on ONE shard as ONE batched door call — the
  group-commit economics survive sharding (this is what hash routing would
  destroy: it scatters every group across all shards).
- **SHARDS** — physical stores. A tiny persisted catalog maps `slot → shard`
  (32-entry array; the anchor shard owns it in MetaKV). Default: one shard per
  claimed slot. The indirection is the evolution door: a replica may map all
  slots to 2 shards; two cold lanes' shards may be merged; a restart may run
  fewer lanes than slots ever used. The ID gives the slot (pure function,
  immutable); the catalog gives the shard (movable). Unknown slot → fail
  closed (`ErrSlotNotLocal` — at the horizontal stage this becomes "route to
  the owning machine").

### 2.1 Sizing guidance (the owner's 128-CPU question)

Lanes buy WRITE parallelism only; reads, prepare, and queries use all
remaining cores regardless of lane count. Measured per-lane apply ceiling is
~0.5–1M inserts/s through the full stack, so 4–12 lanes saturate multi-M/s,
at which point shared disk/flush is the wall. Size by
`target inserts/s ÷ per-shard ceiling`, not by CPUs-per-lane. The slot budget
(32 slots once the rel pairing is dropped — §2) therefore does not pinch in
practice; if it ever does, the escape hatch is a LAYOUT-VERSIONED v2 (borrow
2–3 bits from the 10-bit sequence field → 128–256 slots at 128–256 IDs/µs/lane
≈ still >100M/s/lane), opt-in for new deployments — documented here, NOT
built now.

## 3. Architecture

A new backend `pkg/graph/store/sharded` implementing the Store contract over N
`badger.Store` instances (the tiered pattern, with slot routing instead of
time routing). Core stays semantically untouched: entity locks, value stripes,
tx isolation, unique kernels, chain resolvers all operate above the Store
seam. The only core additions are config plumbing and per-lane generator
wiring for ingest sessions.

- **Point ops** (Get/Put/Replace/Delete/history): route by slot. O(1).
- **Scans** (label/type/All/temporal): PARALLEL fold across local shards
  (all shards are open local badgers — unlike tiered's one-at-a-time cold
  checkout). Rebuild-at-open parallelizes the same way. Reads get faster,
  not just writes.
- **Relationships — minted in the START node's slot, stored whole on that
  shard (v1 decision)**: a rel's ID is minted from its start node's slot
  generator, and the rel row PLUS BOTH adjacency index entries (out + in)
  live on that one shard. What this buys: (1) routing stays a pure function
  of the ID for BOTH entity kinds — `GetRelationship(relID)` decomposes and
  routes O(1); (2) `OutgoingRels(node)` remains a POINT LOOKUP on the node's
  own shard (the hot traversal direction — query engines depend on it); (3)
  every rel write is a SINGLE-SHARD single-WriteBatch atomic operation — no
  cross-shard split-write ordering, no torn-edge crash states, no
  generalization of tiered's section-12 protocol; (4) in the dominant ingest
  pattern (a session creates an event node and immediately its edges FROM
  that event) start = a just-minted lane-local node, so the whole group —
  nodes and rels — still lands on ONE shard as batched door calls. Cost:
  `IncomingRels(node)` becomes a parallel fold over ≤N shards' in-indexes
  (N ≤ 32, small parallel map probes; hub fan-in reads amplified but
  bounded); a rel whose start lives on ANOTHER lane's slot mints on that
  slot's generator (a mutex hop — rare outside cross-partition edges) and its
  door call targets that shard. HORIZONTAL NOTE: at the cross-machine stage,
  "mint in the start's slot" implies the machine OWNING the start slot mints
  the rel — edge ownership follows the start node; recorded here as the
  stage-3 routing rule, not built now.
- **Change-log**: per-shard co-committed logs + ONE store-global LSN
  allocator injected via the EXISTING `badger.Config.ChangeLogSeqSource`,
  flush-barrier + W-bounded k-way merge feed, anchor-shard watermark reseed —
  ADR-0005's tiered machinery reused nearly verbatim. The feed stays one
  total order; replica apply is unchanged and a REPLICA'S TOPOLOGY IS
  INDEPENDENT (records carry entities verbatim; the replica routes by its own
  catalog — a 16-lane primary can feed a 2-shard replica).
- **Anchor shard** (slot base): owns MetaKV, registries, markers, the slot
  catalog — the refShard pattern from tiered.
- **Non-ingest writes** (standalone Add / tx / g.Batch): mint from the base
  slot's generators (the "interactive lane") and route normally. Tx semantics
  are core-level and unchanged.
- **Unique constraints**: value stripes are core-level (unchanged); the
  property-index consultation folds across shards; the UniqueForever registry
  lives on the anchor shard.
- **Capabilities**: mandatory contract fully implemented; optionals declared
  per the tiered precedent (initial declines enumerated in stage S5 and
  closed toward parity like tiered was — feature parity is the standing
  owner requirement, staged not skipped).

## 4. Invariants (each gets a test or a stated reason)

1. ID→slot routing is total and stable: every ID ever minted by a claimed
   slot routes to exactly one shard via the catalog; unknown slots fail
   closed.
2. Per-shard writes keep single-WriteBatch co-commit (rows + indexes +
   counters + log records + watermark) — inherited from badger unchanged.
3. LSNs form one gapless total order across shards (ADR-0005 battery,
   re-pointed).
4. Replica byte-exact convergence from an N-lane sharded primary, INCLUDING
   onto a replica with a different shard count (the topology-independence
   crown test).
5. Cross-door agreement: every scan door folds ALL shards (label/type/
   temporal/stats parity vs a single-store oracle over the same op sequence —
   the bitemporal oracle harness gains a sharded arm).
6. Per-entity invariants (chains, bitemporal metadata, frozen rows) are
   untouched — an entity lives wholly on one shard.
7. Catalog changes are fail-closed: a directory opened with a catalog that
   does not cover an on-disk slot refuses to open (no silent partial view).

## 5. Staged plan

- **S0 — ADR acceptance** (this document).
- **S1 — skeleton**: `sharded.Store`, slot catalog + anchor shard, point
  CRUD + history routing, parallel label/type/All folds, counts, open/close/
  parallel rebuild. Gate: full mandatory-contract conformance suite (the
  existing store test batteries parameterized over the new backend) green.
- **S2 — rels**: rel-local adjacency, batched doors (incl.
  `PutNodesBatchPreEncodedLog` per shard), delete/cascade (cascade =
  fold-collect then per-shard deletes; node + its rels may span shards —
  two-phase with the core's existing entity locks). Gate: rel batteries +
  oracle arm.
- **S3 — change-log + replica**: ChangeLogSeqSource wiring, merge feed,
  reseed, `TxChangeLogScope` (exclusive paths), the topology-independence
  replica crown. Gate: ADR-0005 battery re-pointed + convergence crowns.
- **S4 — lanes**: `Config.SlotCount`, per-lane generator pairs, concurrent
  ingest sessions pinned lane→slot→shard (group = one shard door call).
  MEASURED BAR: badger-backed sharded store, 8 lanes / 8 slots, ≥2.5x the
  single-store concurrent p8 baseline (~1.1M/s → ≥2.75M/s) with the change-log
  ON within 1.2x of off; honest pprof if the disk/flush wall lands earlier.
- **S5 — capability parity sweep + docs + release** (v4.16.0): enumerate
  remaining declines with reasons, CLAUDE.md/api docs, bench tables.

## 6. Risks

1. **Cross-shard cascade delete** (S2) — a node's rels live on many shards;
   the cascade must be crash-recoverable without cross-shard WriteBatch
   atomicity. Mitigation: core entity locks serialize the neighborhood;
   per-shard deletes are individually atomic; a verify/repair door (tiered
   precedent) covers the crash window; the change-log records per-edge
   deletes (ADR-0005 §2.4 pattern) so replicas converge regardless.
2. **Fold amplification on adjacency-heavy read workloads** — bounded by
   N ≤ 16 and parallel probes; measured in S2, with the home-shard mirror as
   the designed escape hatch.
3. **Slot-budget misconfiguration** — two deployments claiming overlapping
   slots mint colliding IDs (same failure class as today's duplicated
   SnowflakeNodeID, now with a wider blast radius). Mitigation: the catalog
   persists the claimed range and fails closed on overlap at open; docs carry
   the cluster-budget table.
4. **Scope** — this is a full store backend. The stages are sized so each is
   independently green and shippable behind config (`SlotCount: 0/1` = today's
   single store, the default, byte-identical behavior).
