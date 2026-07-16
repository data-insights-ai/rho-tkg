# Sigma-tkgd performance ports (the "OPTs") — backlog & decisions (2026-07-16)

Consumer request from sigma-tkgd. Group A (API doors) is DONE: A1/A2/A3 shipped in
v4.16.0; A4 op half (`replication.ChangeOp` + `ChangeOpOf`) shipped in `[Unreleased]`.
A4 `labelOrType` — DECLINED by consumer (mirror re-reads on put, deletes by-ID; op +
identity is enough).

## Agreed order

1. **B1 (OPT3) — sharded entity cache + BP-Wrapper.** ✅ CORE ALREADY DONE IN-TREE (v4.16.0).
   - (a) Sharded LRU: DONE — `indexpkg.ShardedCache[V]` (`internal/index/sharded.go`),
     RocksDB ShardedLRUCache model, splitmix64 avalanche routing, per-shard mutex +
     dirtySet, aggregate accounting, GOMAXPROCS-derived shard count (`ShardHint`, ≥16,
     pow2), `TKG_CACHE_SHARDS` A/B kill switch. Wired in `newNodeCache`/`newRelCache`.
   - (b) BP-Wrapper GOAL met by a simpler mechanism: the label-scan path
     (`fetchNodesByLabelIDs` → `prefetchNodeScan` → `prefetchNodeNoFill(promote=false)`)
     uses `GetNoPromote` = per-shard RLock, no MoveToFront. Scan hits take no write lock
     and spread across shards. The literal "batch + replay under one try-lock" was NOT
     built — unnecessary, the RLock+shard path already delivers the win.
   - (c) W-TinyLFU admission: NOT built (classic LRU). A hit-RATE optimization, separate
     from lock contention; scans already don't pollute the cache (no-fill scan path).
   - **MEASURED** (`BenchmarkConcurrentLabelScan`, M4 Max, 16 workers, 20k-node label,
     all cache hits): sharded ~37M nodes/s vs single-mutex (`TKG_CACHE_SHARDS=1`)
     ~5.8M nodes/s = **~6.4×**. The contention sigma flagged is gone. Benchmark kept as a
     permanent regression guard (`badgerstore_scan_contention_test.go`).
   - REMAINING (speculative, needs a measured gap to justify — NOT pursued): full
     BP-Wrapper batch-replay (would only amortize the per-node RLock atomic; marginal
     over 37M/s), W-TinyLFU admission (hit-rate, own overhead). Report says sigma can pull
     v4.16.0 and the win is already there.

2. **B4 (OPT10) — wire the temporal interval index into the query path.** ANALYSIS DONE.
   Verified findings (2026-07-16):
   - The maxTo-augmented interval index EXISTS (`internal/index/temporal_index.go`,
     `QueryAt`/`QueryOverlap`, output-sensitive O(log n + k)) — the interval-tree B4 asks
     for is already built, maintained on every history write, and PERSISTED (badger 0x0B,
     tiered per-shard). Real ongoing cost.
   - memory + badger `NodesByLabel` DO query it — but only for a CURRENT-VERSION valid-time
     filter (proven sound vs their own full scan: BOTH are current-version-only; the store
     door does NOT do predicate-anywhere).
   - **The core/graph layer NEVER routes a temporal query to that store path.**
     `nodesByLabelLocked` (queries.go:320) STRIPS the temporal opts before calling
     `store.NodesByLabel` and resolves versions itself via `findNodeVersionForOpts`
     (predicate-anywhere, bitemporal), narrowed by the K1 label-tx-membership sidecar. So
     the store's index-accelerated temporal query is UNREACHABLE from the graph API —
     effectively dead code from the consumer's view.
   - The core AS-OF/BETWEEN/Allen/point doors (`NodesAt`/`NodesDuring`/`NodesAsOf`) fold
     candidate node IDs (K1 narrows LABEL scans to ever-members; the UNLABELED doors fold
     ALL node IDs) and resolve chains. NONE use the interval index → the O(n) scan B4
     targets is real, in the CORE, not the store.
   - sharded + tiered don't even wire the store-level index query.
   - **True scope:** make the interval index a SOUND CANDIDATE-SUPERSET (must hold ALL
     versions' intervals, currently current-only) feeding the core resolver — the
     valid-time analogue of the K1 sidecar — with the resolver staying authoritative;
     integrate alongside K1 in `findNodeVersionForOpts`; fan out to sharded/tiered;
     two-door equivalence (rule 17) + two-phase adversarial tests (rules 15/16). Weeks.
   - Bonus finding: the index is currently maintain+persist+rebuild cost for ~zero
     graph-level benefit. B4 makes it earn its keep; else it's a cleanup candidate.
   - Recommended start: increment (C) — make the index all-versions + wire ONE door
     (NodesDuring interval, highest value) as a candidate narrower, MEASURE, then extend.

   ### DECISION (2026-07-16): user chose FULL B4 + measure. Staged execution plan:

   **Design:** the per-label temporal index becomes a SOUND VALID-TIME CANDIDATE
   NARROWER for the core resolver — the valid-time analogue of the K1 label-membership
   sidecar. Combined candidate set = (label-ever-members ∩ valid-time-overlappers),
   resolver stays authoritative. Soundness for predicate-anywhere (rule 16) requires the
   index to hold the per-node VALID-TIME ENVELOPE `[min(validFrom over all versions),
   maxTo(validTo over all versions)]` (envelope, not current-version — current is unsound,
   proven by probe: a node whose PAST version overlapped [start,end) but whose current
   doesn't is missed). Envelope over-includes gap queries; the resolver filters precisely.
   Envelope chosen over per-version entries: 1 entry/node (same cardinality), extend-on-
   write maintenance, no per-version removal. Upgrade to per-version (tighter) is a later
   option if measurement shows envelope over-inclusion hurts.

   - **Stage 1 — index-level sound superset (FOUNDATION, self-contained).**
     `TemporalIndex.Extend(id, from, to)` = union with existing entry (insert if absent);
     `ExtendNodeInTemporalIndexes` maintenance helper. Index-level tests proving the
     sound-superset property (QueryOverlap returns a node whose PAST version overlapped).
     Additive — does not yet change any maintenance call site, so zero blast radius.
   - **Stage 2 — switch maintenance to envelope + update the store path.** Route every
     node history write (memory + badger create/update/delete/replace) through `Extend`
     (envelope grows; delete RETAINS envelope so deleted-node history stays queryable, B32).
     The existing store `NodesByLabel(temporal)` path now returns a candidate SUPERSET;
     update its tests + doc (it was unreachable from graph anyway). Badger 0x0B persistence
     stores the envelope.
   - **Stage 3 — wire into the core resolver (the actual acceleration).** New store
     capability `TemporalCandidateCapability` (`NodeTemporalCandidatesDuring/At(labelToken,
     ...) ([]NodeID, bool)`), memory+badger implement. Core: in `forEachLabelTxCandidate`
     (and the unlabeled interval path), when the capability + a per-label index exist,
     INTERSECT the valid-time candidates with the K1/label candidate set before resolving.
     Resolver (`findNodeVersionForOpts`) stays authoritative. Two-door equivalence (rule 17)
     via the shared resolver; both named + generic doors benefit.
   - **Stage 4 — sharded/tiered fan-out.** sharded: per-shard index query, fold candidate
     IDs. tiered: per-shard (reference + event shards). Capability-gated; a store without
     it falls back to the full fold (never wrong).
   - **Stage 5 — two-phase adversarial tests (rules 15/16/17) + MEASURE.** Two-phase
     (create@X, mutate, query@t0 reflects X) across backends; predicate-anywhere interval
     case (past-version overlap); exact-set assertions; index-present-vs-absent equivalence.
     Benchmark: large graph, temporal interval query, index vs no-index throughput.

3. **B5 (OPT8) — deferred/batched index build during bulk load** + offline-shard bypass.
   - Refs: neo4j-admin import; RocksDB bulk ingest. Fits the ingest pipeline.

4. **B2 (OPT6) — lock-free skip-list ordered (label,property) index** for range/prefix/
   sorted scans. Large new index class (persistence + auto-maintenance across every
   mutation path). Alt: ART for single-thread + prefix. Refs: Memgraph; Leis ART.

## Deferred / conditional

- **B3 (OPT4) — adaptive int64 timestamp codec.** ON HOLD — consumer unsure of the
  benefit. STOP/REPORT: conflicts with the shipped v2 fixed-width temporal tail
  (ADR-0006 §4.5 — `PatchWireTemporalTail` needs `tf`/`tt` at a fixed 24-byte offset;
  compressing them breaks the crown property `Patch(PreEncode)==Encode` and the
  byte-exact hash/replica contract). Only sensible as a COLUMNAR temporal-segment tier
  (compress many entities' stamps together), which the row-oriented store lacks. Revisit
  only if a concrete storage-size/throughput benefit is shown.

- **B6 (OPT20) — AeonG-style anchor+delta history layout.** LATER (TODO). Periodic full
  anchors + deltas; reconstruct a version by replaying from the nearest anchor. Real
  throughput/storage win (no full property set stored at every change) but LOTS of work —
  touches the hash chain, compaction (ADR-0001), and replication byte-exactness. Refs:
  AeonG (PVLDB 17(7) 2024 / VLDB Journal 2025).
