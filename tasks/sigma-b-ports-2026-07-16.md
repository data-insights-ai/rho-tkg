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

2. **B4 (OPT10) — wire the temporal interval index into the query path.**
   - `internal/index/temporal_index.go` (maxTo-augmented interval index) ALREADY EXISTS,
     but `temporal_queries.go` does NOT consume it — AS-OF/BETWEEN/Allen queries scan
     version chains. Gap = wire the existing index into the query path + zone maps over
     temporal segments. Smaller than a from-scratch build.

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
