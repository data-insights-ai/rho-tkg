# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [4.23.0] - 2026-07-18

- PERF — the streaming whole-node label door (`g.Nodes().ForEachByLabel` + new `g.Nodes().IterByLabel(ctx, label, opts) iter.Seq2[*types.Node, error]`) now rides the bulk-scan substrate (BACKLOG 3, final increment — the `NodesByLabelBulk` ask). badger's `ForEachNodeByLabel` was still doing N per-node `Txn.Get`s (`prefetchNodeScan` loop); it now streams through `forEachNodeBulk` — one read transaction + one forward-seeking iterator, cache hits served inline — so a one-shot `MATCH (n:L) RETURN n` gets the ~1.3× single-iterator fetch win **while keeping peak memory O(1) nodes** (no result-slice materialization), the whole point of the streaming door vs the materializing `ByLabel`. The label IDs are snapshotted under `idxMu` then released, so `fn` runs holding no `idxMu` (only a badger snapshot txn) — the relaxed-isolation "fn may call back into the graph" contract is preserved. New `IterByLabel` is the ergonomic iter.Seq2 form (parity with `Iter`). Cross-backend unchanged: memory streams live objects; tiered/sharded and any temporal `QueryOpts` fall back to the materialized history-aware `ByLabel` then stream (correct, no streaming-memory win there). **MEASURED (50k, cache-cold): ~120 → ~93 ms** vs the old per-node path. Tests: `IterByLabel`/`ForEachByLabel` == `ByLabel` set+order over a mixed-label 3k scan (both backends) + iter.Seq2 early-stop, `-race` clean; A/B benchmark. This closes the last open `tasks/backlog.md` item.
- CHORE — **`tasks/backlog.md` reduced to an ALL-DONE marker** (kept as the single roadmap/todo file for the repo; every entry shipped). Every rho-tkg-buildable item is complete: retention purge R2–R5 + the tiered cold-shard-drop optimization (BACKLOG 1), cross-machine Model A + cascade (BACKLOG 2), the columnar/streaming whole-node fetch — bulk + parallel decode + `AllNodes`/DocValues-cold-build substrate + the streaming `ForEachByLabel`/`IterByLabel` door (BACKLOG 3), the review-driven adaptations (per-label DocValues epoch 4b, configurable `HistoryAnchorInterval` 4c, ingest `IntentRecord` cleanup 4d, `PeekTx` 4e; 4a was owner-decided DO-NOT-BUILD), and the rel-side ordering-soundness primitives `RelRangeCardinality` + `RelPropertyTypeClassCounts` (BACKLOG 5). The only untracked leftovers are cross-team **sigma-coordinated** RPCs (the START→END stub-delete fan-out, the consumer-gated constraint dry-run) — sigma's to build; rho-tkg already exposes the local primitives, so they are NOT rho-tkg backlog items. Prior long-form design provenance survives in this CHANGELOG + git history; code comments keep their `BACKLOG N`/`Bn` increment tags as stable archaeology keys. CLAUDE.md's "Designed-not-built" section is now a "single roadmap file, all done" note.

## [4.22.0] - 2026-07-18

- PERF — tiered cold-shard fast-drop for ByAge retention purge (ADR-0008 R4 optimization, ex-BACKLOG 1 deferred item). When a `PurgeExpiredNodes` (ByAge) removes a rotated, wholly-aged-out, single-label event shard, the tiered store now physically DROPS the shard (close + `os.RemoveAll` + catalog remove) instead of row-scan-cascading every entity — replacing the per-row delete writes + flush with one directory removal. Correctness under concurrency rests on a **drain protocol** (`dropOneShard`): the shard is UNLINKED from routing (so a new cross-shard edge to a purged-window node fails endpoint validation on the hot-shard fallback rather than adding un-swept residue), its in-flight requests are DRAINED (`activeReqs`→0; new checkouts can't arrive once unlinked; a drain timeout re-links + falls back to the row scan), and only THEN is its cross-shard residue collected (from the quiescent shard's adjacency keys) and swept on the surviving endpoints' shards (the same phase-2 sweep as the row-scan path) before the directory is removed. Eligibility is narrow + safe: ByAge only (`ValidTo` is orthogonal to the mint-time window), single-label shards only (`CollectShardDropResidue` conservatively declines any foreign-label token), whole-window-below-boundary only (`shard.timeEnd <= before`), never the hot shard, and **disabled under ChangeLog** (dropping a shard would destroy its `0x09` log segment → replica LSN gap; falls back to the row scan, which leaves log records intact — replica convergence via the ONE `ChangeRangePurge` predicate is unaffected). Ineligible shards + the ByValidTo path keep the row scan. Tests: a rotated single-label shard dropped from the catalog + directory with both cross-shard residue shapes swept (no dangling phantom); change-log declines (shard row-scanned, not dropped); concurrent-reader stress over the drain, `-race` clean; full tiered + convergence suites green.

- PERF — per-label DocValues cache epochs (BACKLOG 4b): a badger cached column for label X now SURVIVES write-active ingest of UNRELATED labels. The single-label `docColumns` cache was keyed on the GLOBAL `nodeEpoch` (bumped on every node write), so any write to any label invalidated every label's cached column. Now `labelEpoch(token) = nodeLabelEpochs[token%256] + nodeEpochSalt`: a node write bumps only the epochs of the labels it carries (via `bumpNodeLabelEpochs` in the ungated `add`/`removeNodePropertyKeyCounts` wrappers — the seam every node-content write funnels through *with the node*, so the remove-old+add-new pattern covers label changes; two edits, no wide audit), and the label-less events (Clear, retention purge) bump the salt to invalidate every label. The sharded array (256 stripes) is lock-free O(1); a hash collision over-invalidates two labels together (SAFE — never stale). The multi-label intersection cache uses the monotonic SUM of member epochs. `ForEachDocValues`/`DocValuesSnapshot` return the per-label epoch as `gen`, and a new `g.Nodes().NodeLabelMutationEpoch(label)` is the matching per-label Gate-2 reader so a consumer aggregating over one label is not forced to discard a still-valid result after an unrelated-label write (`NodeMutationEpoch()` stays the global signal, unchanged). Badger only (tiered/sharded decline the column scanner). Test: a label-X column survives 20 unrelated label-Y writes (gen unchanged, sum unchanged) while an X write invalidates it; full badger + aggregation suites green, `-race` clean.

- ADD — `Config.HistoryAnchorInterval` — configurable anchor spacing for `HistoryDeltaEncoding` (BACKLOG 4c), previously a hardcoded `const 16`. A larger interval stores more deltas per anchor (less storage, more reconstruction reads); smaller, the reverse. Exposed on both `graph.Config` and `badger.Config` (0 = the default 16; validated at `New` to 0 or `[2, 4096]`). **The interval is baked into the on-disk delta layout** (anchors at `V - V%interval`), so changing it on an existing delta store would silently reconstruct deltas against the wrong anchor — a persisted marker (`history_anchor_interval` MetaKV) records the interval the store's deltas were written at, verified at open: a mismatch FAILS CLOSED with `ErrHistoryAnchorIntervalMismatch` (re-exported at the graph level) regardless of the current delta flag (existing deltas still need their original interval). The marker is stamped only on a writable, delta-enabled open (a store that never wrote deltas is not pinned — reopens at any interval). `storeutil.AnchorVersionFor`/`IsAnchorVersion` now take the interval; badger threads `bs.historyAnchorInterval` through the 7 delta read/write/compaction sites. The DEFAULT stays 16 — a sweep to pick a materially better default is a separate follow-up; this ships the safe, marker-guarded mechanism. Tests: configured interval (4) round-trips across reopen; mismatch (write at 4, reopen at 8 or default 16) fails closed; validation rejects 1/-1/4097; a non-delta store is not pinned; existing delta suite green under the const→param change, `-race` clean.

- ADD — `g.Stats().RelPropertyTypeClassCounts(typeName, propKey)` — the relationship mirror of node `PropertyTypeClassCounts` (rule 2, BACKLOG 5B), the second rel ordering-soundness primitive: the EXACT `{Numeric, NaN, String, Bool, Other, Missing}` partition of a rel type's current rels by the key's value class. The correctness gate for the rel `ORDER BY r.prop LIMIT k` push-down — ordering is sound only when the ordered class is unambiguous (a mix of numeric + string under one type/key makes it undefined). O(1) exact counters maintained incrementally, mirroring the node choke. New rel-side infrastructure (there was none):
  - New optional `store.RelPropertyTypeClassCountsCapability` (native memory + badger). Tiered/sharded DECLINE (`ErrCapabilityNotSupported`) — rel property indexes, the whole rel-ordering path, are RAM-only per-shard (consistent with 5A / `ForEachByTypePropertyRangeOrdered`). `DisablePlannerStats` declines too (parity with the node stat opt-out).
  - **The delete-without-properties sharp edge, solved.** Badger's `deleteRelByInfo` is read-free — `RelDeleteInfo` carries no property values — so a delete cannot classify to decrement. A per-rel MEMOIZED CONTRIBUTION sidecar (`relTypeClassContrib`, keyed by rel ID, populated at every ADD) lets the single delete seam decrement precisely by ID, uniformly across every delete path (single / history / batch / **node-cascade** / purge). The memory store holds live rels, so it decrements with the old rel directly (no sidecar). Counters + sidecar are co-located with the rel property-index maintenance at every full-rel-write site (`putRelationship` — the single badger create choke for validated/co-located/foreign writes — replace, history-replace, batch) and REBUILT from `loadIndexes` at open, so a delete after restart still decrements (the "survives restart" rule).
  - Wiring: badger `RelPropertyTypeClassCounts` + `addRelPropertyTypeClassCounts`/`removeRelPropertyTypeClassCountsByID`; memory `adjustRelPropertyTypeClassCounts`; core `StatOps.RelPropertyTypeClassCounts` (Missing = `RelCountByType` − Present) + the public `g.Stats()` wrapper. Tests: exact mixed-class partition (Numeric=5/String=3/Missing=2) with delete + **node-cascade-delete to zero** (proves the sidecar seam, no drift) on both backends; badger reopen (counters + sidecar rebuild, post-reopen delete still decrements); tiered decline; `-race` clean.

- ADD — `g.Rels().RangeCardinality(typeName, propKey, min, max, inclMin, inclMax, opts)` (+ `g.Stats().RelRangeCardinality` alias) — the relationship mirror of the node `RangeCardinality` (rule 2, BACKLOG 5A), the first of the rel ordering-soundness primitives that unblock the rel ordered-top-k push-down (`ORDER BY r.prop LIMIT k`). Counts the type's rels whose numeric propKey lies in `[min,max]` directly from the REL property index's per-value bucket sizes — O(distinct values in range), NO rel scan — reusing the existing rel property index substrate (the one behind `ForEachByTypePropertyRangeOrdered` [4.18]). `exact=false` declines (caller scans-and-counts) when the fast path is unusable: no store capability (rel indexes are RAM-only → tiered/sharded decline), no/poisoned index, or a temporal filter in opts. New core-internal `relRangeCardinalityScanner` (memory + badger) + `RelOps.RangeCardinality` + the two public wrappers. Rule-2 parity test (memory + badger): pre-index decline, post-index exact count, exclusive bounds, temporal decline, unknown-type zero; `-race` clean.

- PERF — `AllNodes` (whole-graph `MATCH (n) RETURN n`) unbounded scans now ride the parallel decoder too (BACKLOG 3 follow-up). `fetchNodesWithTemporalFilter` (AllNodes' non-paginated path, its single caller) routes `>= parallelDecodeMinIDs` candidates through `collectNodesBulkParallel` — same ~3× decode win as the label scan — with the temporal filter applied post-decode (identical set/order). Paginated/`Limit`'d AllNodes keeps the serial early-stopping door. Test: wired `AllNodes` over `>parallelDecodeMinIDs` nodes spread across labels returns the full ascending set, `-race` clean.

## [4.21.0] - 2026-07-18

- FIX — the as-of columnar aggregation doors (`DocValuesSnapshotAsOf` [4.19.0], `ForEachDocValuesAsOf` [4.20.0]) now actually close the X5-temporal gap. As shipped they built a THROWAWAY column set on every call (`gen` 0, never cached) — sigma-tkgd benchmarked the columnar path within ~3% of the row fallback (noise) and reverted their wiring, because `buildAsOfColumns` materialized the full as-of node structs and then discarded the derived columns. **Root cause:** the DocValues win is a CACHING win (the first build materializes; repeated aggregations reuse the compact primitive column) — the as-of doors skipped the cache. **Fix:** an as-of column cache (`Core.asOfColumns`, `internal/core/docvalues_asof_cache.go`) keyed by `(label, txAt)`, so repeated same-`txAt` aggregation (a dashboard `AS OF SYSTEM TIME $t RETURN count/sum/…`) scans the cached compact column. **MEASURED (M4 Max, 20k nodes, repeated same-`txAt` SUM): ~8.0 ms → ~50 µs (~159×), ~10.3 MB → ~17 B, ~99,934 → 2 allocs** (`BenchmarkForEachDocValuesAsOf{Cached,Uncached}`); the ratio grows with node count and query repetition.
  - **Why this beats the current-state cache for a TKG:** a past belief is IMMUTABLE under forward ingest — a new version (`TxFrom = now > txAt`), a fresh node, a soft delete (`DeletedAt = now > txAt`) cannot change the belief at a past `txAt` (the as-of resolver selects the version with `TxFrom <= txAt`). So this cache SURVIVES write-active ingest, where the current-state column cache is perpetually cold. ONLY a history rewrite below `txAt` invalidates it: compaction, retention purge, truncate/rollback-trim, a past-dated backfill, or an out-of-order (past-dated) replica apply. Each routes through a single `asOfColumns.bump` (or, on the replica, `noteAppliedTx`, which bumps only when an applied `TxFrom` is BELOW the forward high-water mark — a forward apply leaves the cache warm). Choke points wired + individually tested: `advanceCompactionWatermark`, `advanceRetentionWatermark`, `applyHistoryTruncateLocked`, `resolveBackfillTxFrom`, the four entity-writing apply handlers, `Reset`, `Import`/`ImportMerge`.
  - `gen` is now the as-of history-rewrite epoch (stable for a fixed past belief), not the misleading 0 — a caching consumer keys its result on `(txAt, gen)` and re-fetches only when a rewrite occurs. Column sets are size-capped at `MaxDocValuesNodes` (huge labels build one-shot, never cached) and the cache is FIFO-bounded at 64 distinct pins. Correctness gates: the cache SURVIVES forward ingest with the past belief unchanged (pointer-identity + member-count), a history-rewrite bump forces a rebuild, every real choke increments the epoch, the replica forward-vs-past-dated rule, and existing two-phase as-of correctness — all `-race` clean.

- ADD — `g.Temporal().PeekTx()` — a non-reserving read of the transaction clock, the observability-only sibling of `NowTx` (BACKLOG 4e). `NowTx()` ADVANCES the commit clock (reserves an instant) — that reservation is what makes it a sound as-of pin, but it also means a metrics/polling loop calling `NowTx` inflates the clock and burns instants a mutation would take. `PeekTx` returns `max(wall, lastInstant)` with NO CompareAndSwap, so a dashboard/health-check loop can sample "roughly where is the clock" without side effects. It is DELIBERATELY NOT a sound as-of pin — the value can coincide with the instant a concurrent write is about to reserve, so a read pinned at `PeekTx()` would include/exclude that write nondeterministically; for a sound pin use `NowTx()`, a value returned BY a write (e.g. `Tx().RunWithLSN`), or a named `TagAsOf` (all loudly documented). The `NowTx` godoc is hardened to steer polling to `PeekTx`. Test: floor pushed above wall via `AdvanceClock`, then 1000 `PeekTx` calls return the floor exactly and the next `NowTx` is floor+1 (zero instants burned), both backends.

- PERF — badger whole-node label scans (`MATCH (n:L) RETURN n`) now decode the badger-resident nodes ACROSS CORES (BACKLOG 3 parallel-decode lever). `forEachNodeBulk` already made the FETCH one iterator pass (v4.18), leaving the CPU-bound msgpack decode (`SafeUnmarshal` → `decodeNodeWireForKey` → `Freeze`) as the serial floor for a one-shot full scan. `collectNodesBulkParallel` keeps the badger txn/iterator serial (they are not concurrent-safe) — the serial pass only SEEKS + `ValueCopy`s each cache-miss's raw bytes — then fans the decode across `GOMAXPROCS` contiguous chunks into per-node result slots. Decode is data-parallel-safe: it reads the atomic-loaded property-key registry (RLock-protected `Resolve`) and mutates only a per-decode local wire; every node is fresh + frozen independently, written to its own slot (no shared writes). Wired into `fetchNodesByLabelIDs` ONLY for unbounded scans (`Limit == 0`) with `>= parallelDecodeMinIDs` (2048) candidates — Limit'd scans keep the serial early-stopping door (parallel decode has no early stop); the label + temporal filter is applied post-decode, identical set/order to the serial callback. Tiered/sharded inherit it (they fold per-shard through badger). **MEASURED (M4 Max, 16 cores, 50k flushed 8-prop nodes, cache-cold): ~82 ms → ~27 ms/scan (~3×), at ~17% more transient memory (raw-byte staging: 120→141 MB) and ~2% more allocs.** Correctness gate: `collectNodesBulkParallel` == serial `forEachNodeBulk` on an ID list mixing present/tombstoned/absent + per-node property equality, and the wired `NodesByLabel` == serial reference, both `-race` clean; A/B benchmark `BenchmarkNodeScanBulkParallel` vs `BenchmarkNodeScanBulk`. Memory note: the raw-byte staging holds all misses' encoded bytes at once (bounded, proportional to the already-materialized decoded set); a batched-staging refinement for extreme scans is left as a follow-up. The DocValues COLD build (`bulkNodePropGetter`) also rides the parallel collector for large labels (`>= parallelDecodeMinIDs`), so scalar-aggregation cold rebuilds parallelize too (~32 → ~25 ms on the 20k×8-col bench) — the "make cold builds cheap" lever the review flagged as preferable to a riskier per-label DocValues epoch cache patch. **Memory bound (BACKLOG 3 follow-up):** the collector now stages the badger misses' raw value bytes in BOUNDED BATCHES (`parallelDecodeBatch` = 8192) — decode a batch, reuse the buffer — instead of all-at-once, so transient raw-byte memory is O(batch) not O(all misses) even on extreme (multi-million-row) scans (the decoded nodes are the caller's inherently-O(N) result; only the raw staging is bounded). Same ~3× speed; multi-batch equivalence test (>2× batch, cross-boundary seq verified) `-race` clean.

## [4.20.0] - 2026-07-17

- ADD — retention purge R4 for the TIERED backend (ADR-0008), completing purge across all four backends (memory / badger / sharded / tiered). Tiered is deliberately NOT a sharded mirror: it uses a SPLIT-WRITE cross-shard adjacency layout (a rel's entity + out-leg live on the START node's shard, its in-leg on the END node's shard), so the sharded `PurgeAdjacentRelsForNode(purgedNode)` sweep — which relies on both legs being co-located — MISSES tiered's residue (a `survivor→purged` rel leaves its entity+out-leg on the survivor's shard, keyed by the survivor, not the purged node). Design:
  - Phase 1 fans out the per-shard badger purge over ref + archive + every event shard (`forEachOpenShard`). That purge now also returns `store.RetentionPurgeResult.PurgedRels`, decoded from each purged node's adjacency KEYS (new `purgedRelsForNodeLocked` — the `0x05`/`0x06` key encodes BOTH endpoints, so a cross-shard rel whose entity lives on another shard and is thus invisible to a local entity read is still captured with its endpoints).
  - Phase 2 routes each touched rel to its SURVIVING endpoint's shard (a purged endpoint's shard self-cleaned in phase 1) and calls the new recordless badger `PurgeRelationshipByInfo`, which dispatches on where the residue is: entity present here (a survivor→purged full-local rel) → full delete + version-history purge; only a dangling in-leg (a purged→survivor orphan) → an orphan-index purge. Both reuse existing tested primitives (`deleteRelByInfo` / `purgeOrphanRelIDLocked`).
  - `LogRangePurge` emits the ONE `ChangeRangePurge` on the reference shard (shares the store-global LSN allocator + merged feed). New `store.PurgedRel` descriptor on the result.
  - Tests: `TestTieredPurge_CrossShardEdgeSweep` (store-level — both residue shapes: event→ref orphan + ref→event full-local, zero dangling); `TestRetentionPurge_TieredReplicaConvergence` (a tiered primary → tiered replica re-executes the single predicate record, cross-shard sweep on the replica too, dangle-free, watermark advanced). No regression to memory/badger/sharded (they ignore `PurgedRels`; badger fills it, memory leaves it nil).

- ADD — retention purge R5 (`ByValidTo`, ADR-0008) — the world-time-validity purge predicate, completing the ADR-0008 purge surface (all four backends, both modes). `g.Admin().PurgeExpiredNodes(ctx, PurgePolicy{Label, Mode: PurgeByValidTo, Before})` hard-removes nodes whose CURRENT-version validity ENDED before the boundary (`ValidTo != 0 && ValidTo < Before`); an OPEN interval (`ValidTo == 0`) is never purged. This is the retention predicate for facts that are aged out by WHEN THEY STOPPED BEING TRUE, not when they were recorded — orthogonal to `ByAge` (mint-time). Design:
  - New optional store capability `store.RetentionPurgeByValidToCapability.PurgeNodesByLabelValidToBefore(labelToken, before, chunk)` — a SEPARATE optional interface (not folded into `RetentionPurgeCapability`), so the addition is purely additive / non-breaking and a backend could offer age-purge without validity-purge. Implemented by native memory + badger; fanned out by sharded + tiered through the SAME cross-shard mechanism as `ByAge` (both were refactored so only the per-shard predicate closure differs — `purgeNodesFanOut` in each, zero duplicated phase-2 sweep logic).
  - **No mutable-predicate re-confirm needed — a design simplification proven by an existing invariant.** The initial design assumed `ValidTo` is mutable (a concurrent Update could extend validity between the chunked Phase-A selection and the Phase-B cascade) and added an under-write-lock re-confirm. Grounding it in the code showed the re-confirm is unreachable dead code: a node that QUALIFIES is CLOSED (`ValidTo != 0`), and the graph layer freezes a closed entity against EVERY interactive mutation door (`rejectClosedNodeMutation` on Update/UpdateInPlace/AddLabel/CAS/tx — `ErrAlreadyClosed`). So a selected victim's current-version `ValidTo` cannot change under the selection — the predicate is immutable-once-true, exactly like mint-time (the open→closed direction only ADDS a candidate, caught on the next chunk; it never invalidates a selected one). The re-confirm was removed (Testing Rule 5: no untestable defensive paths); the store selectors are selection-only. badger selects via `getNodeLocked` (cache/db read, no idxMu); memory selects under its single store lock (genuinely atomic).
  - Core dispatches on `PurgePolicy.Mode` (`purgeChunk` → the ByValidTo capability, guaranteed non-nil — the admin door and replica apply both verify it, failing closed with `ErrCapabilityNotSupported` / an apply error when a backend lacks it). `validatePurgePolicy` now accepts both modes. Replication is unchanged in shape: the ONE `ChangeRangePurge` record already carried `Mode` (msgpack `m,omitempty` — `ByAge`=0 absent, `ByValidTo`=1 present), and `applyRangePurgeLocked` re-executes with the record's mode against the replica's verbatim-reproduced (ValidTo-preserving) state → identical SELECTIVE convergence. `PurgeByValidTo` re-exported as `admin.PurgeByValidTo`.
  - Tests: exact-set on both native backends (`closedAtCreate` + `closedViaUpdate`-two-phase purged; `keptOpen` + `keptLateClose` kept; other-label untouched; idempotent re-run) — the `closedViaUpdate` case (created open, closed by a later Update) proves the predicate reads the CURRENT version, not the genesis; tiered cross-shard `ByValidTo` sweep (`TestTieredPurge_ByValidTo_CrossShardEdgeSweep` — both split-write residue shapes + an open-interval survivor edge retained); `ByValidTo` replica convergence (`TestRetentionPurge_ByValidToReplicaConvergence` — one predicate record carrying `Mode=ByValidTo`, zero per-entity deletes, replica re-executes SELECTIVELY: 3 early-closed gone, 1 open survivor kept). REMAINING: a later tiered O(1) cold-shard-drop optimization (perf only — functionality complete via the per-shard row scan).

- ADD — `g.Nodes().ForEachDocValuesAsOf(label, keys, txAt, fn) (gen uint64, ok bool, err error)` — the streaming (AS OF) sibling of `ForEachDocValues`, and the transaction-time analogue of `DocValuesSnapshotAsOf` (v4.19.0). It enumerates the label's members AS BELIEVED at `txAt` and invokes `fn(id, vals, present)` per row WITHOUT materializing a node (columnar time-travel aggregation — the sigma `AS OF SYSTEM TIME … RETURN count(*)/sum(...)` target, the ~124,000× win over a row fallback); `fn` returning false stops the scan. Shares `buildAsOfColumns` with `DocValuesSnapshotAsOf` (pinned `ByLabel{TxPin}` resolver — K1 ever-member scoped, history/deleted-aware, chain-resolver-correct — + `indexpkg.BuildLabelDocValues`, then `LabelDocValues.ForEachRow`), so it is pure core over the generic door and works on EVERY backend incl. tiered/sharded, inherits the pinned scan's retention guard, and `ok=false` when a requested key is not a uniform column at `txAt` (caller falls back). `gen` is DELIBERATELY 0 (frozen point-in-time read, same rationale as the snapshot door — no current-state staleness signal). Tests: two-phase streaming SUM (60 at t0 vs 1049 current), membership-at-pin, early-stop, `gen==0`; nodes sub-API + spy updated.

## [4.19.0] - 2026-07-17

- ADD — three sigma-tkgd consumer doors (previously emulated or blocked):
  - **Transactional commit-LSN** (`g.Tx().RunWithLSN(fn) (uint64, error)` + `BatchResult.CommittedLSN` + `GraphTx.CommittedLSN()`) — a write/commit that returns the MAX change-log LSN of ITS OWN commit, a read-your-writes write-bookmark that is exact under concurrency. The global `LastCommittedLSN` head can already reflect another writer's commit; the per-commit value is exactly this writer's max LSN. Pure plumbing: the change-log scope already minted its commit's contiguous LSN range and flushed synchronously — `store.TxChangeLogScope.CommitLogScope() error` was widened to `(uint64, error)` (all four backends compute the max already; sharded/tiered aggregate across shards sharing the one global allocator). 0 when the commit emitted no records (no mutations / change-log off). Unblocks the HS-3 write-bookmark under concurrency.
  - **Dry-run constraint validation** (`g.Constraints().DryRunValidate(ctx, DryRunFacts{Nodes, Rels}) ([]DryRunViolation, error)`) — validate a proposed fact set against the configured unique + temporal constraints and report every violation WITHOUT asserting: no writes, no ID mint, no events, no LSN burn. Reuses the exact read-only check paths the write doors use (`nodeUniqueValueKeys` + `nodesByLabelAndProperty` for UniqueCurrent, `checkTemporalConstraints` for rel-within-endpoints, the batch pre-check's intra-set seen-map); the one new piece is `checkForeverOwnership`, a check-only sibling of `checkAndClaimForever` that consults the UniqueForever owner registry WITHOUT claiming. A rel endpoint resolves FIRST to a CO-PROPOSED node in the same fact set (by non-zero `DryRunNode.ID`, using that node's proposed valid interval) and falls back to the live committed node — so a "new node + new edge between new nodes" set validates correctly, not as "endpoint not found". This FIXES a real correctness bug in the Tx+rollback emulation the ask replaces: a rolled-back tx does NOT release a UniqueForever claim it made, so emulated dry-runs permanently bar the value; this door never makes a claim. Unblocks HP2.5 weak-mode consistency checks.
  - **`DocValuesSnapshotAsOf(label, keys, txAt)`** — the transaction-time (AS OF) analogue of the current-state-only `DocValuesSnapshot`, the columnar target for `AS OF SYSTEM TIME … RETURN count(*)…` time-travel aggregation (previously blocked, on the row fallback — X5-temporal). Reuses the pinned `ByLabel{TxPin}` resolver (K1 ever-member scoped, history/deleted-aware, chain-resolver-correct — a node whose CURRENT label differs from its label-at-txAt is handled) + the existing `indexpkg.BuildLabelDocValues`, so it is pure core over the generic door and works on EVERY backend, INCLUDING tiered/sharded which decline the current-state `nodeDocValuesScanner`. Not cached (a past pin is immutable + one-shot); inherits the pinned scan's retention guard (a pin before a label's retention watermark → `ErrRetentionExpired`). Its `gen` is DELIBERATELY 0 (not the current node-mutation epoch): the snapshot is a frozen point-in-time materialization built under the read lock, and the current-state epoch is wrong in both directions for a past pin (bumps on current writes that don't change a past belief; does NOT bump on compaction/truncate that do) — so the consumer uses it as a point-in-time read and does not gen-recheck. Two-phase time-travel + as-of-membership tests.

## [4.18.0] - 2026-07-17

- ADD — retention PURGE (ADR-0008 R2+R3+R4-sharded) — the range-scale hard-removal door for event-retention workloads. Cybersecurity/observability graphs ingest TB/day of events and must continuously remove aged-out ones; the existing doors are wrong for it (point delete writes a tombstone for the node AND every edge — DOUBLING write volume to delete; history compaction only trims versions of live entities, and events rarely update). `g.Admin().PurgeExpiredNodes(ctx, PurgePolicy{Label, Mode: PurgeByAge, Before})` HARD-removes whole aged-out nodes of a label WITHOUT tombstones. Design:
  - **Store primitive** — new optional `store.RetentionPurgeCapability.PurgeNodesByLabelBefore(labelToken, before, chunk)` (native memory + badger; tiered/sharded decline until R4). Removes up to `chunk` nodes whose IMMUTABLE snowflake mint-time is `< before`, and for each: every connected relationship (both adjacency legs → a surviving endpoint's incoming index is cleaned, NO phantom edge), ALL index entries (label/property/temporal/vector), and the ENTIRE version history of the node and each removed rel — in ONE atomic batch (badger composes the recordless `cascadeDeleteInner` with `historyTruncateDeleteKeys(prefix, 0)` under a single lock span; memory mirrors `DeleteNodeCascade` + history removal). Reports `More` so the caller loops; idempotent. Emits NO per-entity change-log record.
  - **Predicate is snowflake mint-time, not ValidFrom / not a backfilled TxFrom** (`storeutil.SnowflakeInstant`): time-ordered IDs make "label older than T" a clustered key range, and a backfilled fact below the boundary is rejected at write, never silently purged. `RetentionPolicy.Mode` exists from day one so R5 `ByValidTo` needs no signature change.
  - **Graph door** — gated by `Config.AllowRetentionPurge` (off by default — a no-tombstone removal must be opted into; else `ErrRetentionPurgeDisabled`). Chunked so `c.mu` is never held across the whole range. It advances the per-label retention watermark FIRST — over-state is the fail-closed direction, so a crash mid-purge leaves reads below `Before` returning `ErrRetentionExpired` (the R1 guard) rather than a silently-incomplete set, and a re-run finishes the removal. This is the R2 producer that finally FIRES the R1 fail-closed guard shipped earlier.
  - **Replicates as a PREDICATE, not per-entity deletes (R3).** With a change-log enabled the door emits ONE `ChangeRangePurge` record (tag 13, body `RangePurgeBody{LabelToken, Before, Mode}`) via the new optional `store.RangePurgeLogCapability` (native memory + badger), then physically purges. A replica's `applyRangePurgeLocked` RE-EXECUTES the predicate against its own state — because replicas apply LSN-ordered, their pre-purge state for the label below the boundary is byte-identical to the primary's, so the same range removes exactly the same entities (proven onto a differently-seeded replica; the design extends to a different shard count in R4). No per-entity delete records travel; the replica advances its OWN retention watermark via the record apply (the watermark is never a replicated MetaSet). Idempotent. A change-log store that lacks `RangePurgeLogCapability` refuses with `ErrRetentionPurgeChangeLogEnabled` (defensive — no in-tree backend hits it, since the native purge stores implement both).
  - New sentinels `graph.ErrRetentionPurgeDisabled` / `ErrRetentionPurgeChangeLogEnabled` / `ErrInvalidPurgePolicy`. Tests: store-level (both backends: below-boundary nodes+edges+history gone, above-boundary AND other-label survivors intact, survivor phantom-free, chunk drives ≥2 iterations via `More`, idempotent no-op re-run); graph e2e (purge fires `ErrRetentionExpired` on a below-watermark `NodesAsOf`; gates); **byte-exact replica convergence** (`TestRetentionPurge_ReplicaConvergence` — exactly one `ChangeRangePurge` record, zero per-entity deletes, replica re-executes to the same purged state, watermark propagated, idempotent re-apply); admin sub-API forwarding; change-tag pins extended to 13.
  - **Sharded backend (R4).** The slot-sharded store now implements the purge in two phases: (1) fan out the per-shard label purge in parallel (each shard removes its below-boundary nodes + CO-LOCATED edges + history), then (2) a **cross-shard sweep** — for each purged node, remove any edge MINTED IN ANOTHER node's slot that points at it (an event-as-END cross-shard edge lives on a different shard than the event, so phase 1 can't see it; left behind it would dangle as a phantom in a survivor's adjacency fold). New badger primitive `PurgeAdjacentRelsForNode` (recordless) + `RetentionPurgeResult.PurgedNodeIDs` drive the sweep; the record is emitted once on the anchor shard. Proven by a store-level cross-shard-edge test (the co-located edge falls to phase 1, the cross-shard edge to phase 2, zero dangling) AND `TestRetentionPurge_ShardedReplicaConvergence` (a sharded primary → sharded replica converges from the single predicate record — the horizontal-scaling crown). The `UniqueForever` owner-reaping sub-item is also shipped (the purge door reaps a purged owner's claim so its value is reusable, scoped to purged owners, zero cost when no forever constraints exist). **REMAINING: R4-tiered** — deliberately NOT a sharded mirror: tiered's SPLIT-WRITE cross-shard adjacency (rel entity+out-leg on the start shard, in-leg on the end shard) leaves a live-phantom rel entity keyed by the survivor that the purged-node sweep cannot find, so tiered needs a cross-shard-cascade-aware purge (see `tasks/backlog.md` BACKLOG 1); tiered declines the purge (fail-closed) until then. Plus **R5** (`ByValidTo`). See `tasks/backlog.md` BACKLOG 1.

- PERF — badger DocValues (X5 aggregation) COLD BUILD now decodes each node exactly once. The column-major build re-fetched every node once per (column × classify/build pass) via `GetNode` — ~`columns × 2 × N` fetches — and, on a label larger than the LRU, thrashed the cache with fill-on-miss (re-decoding from badger and evicting hot point-read entries). `buildLabelColumns`/`buildMultiColumns` now materialize the label's nodes via ONE bulk scan (`bulkNodePropGetter` → `forEachNodeBulk`, no-fill so it does not pollute the LRU) and feed all columns from that single decode. Same result (all DocValues tests green), same epoch-consistency (build discarded if the epoch advanced). **MEASURED (M4 Max, 20k nodes × 8 columns, flushed/cache-thrashing): ~917 ms → ~32 ms cold build (~29× faster), ~1.05 GB → ~32 MB (~33× less), ~16.5M → ~610k allocs (~27× fewer)** (`badgerstore_docvalues_bench_test.go`). Directly addresses the caching-moot pain on the aggregation path: even when a write invalidates the epoch, the rebuild is now cheap.

- PERF — badger label-scan node materialization now uses ONE read transaction + ONE forward-seeking iterator (`forEachNodeBulk`) instead of a separate `db.View` + `Txn.Get` per node (`fetchNodesByLabelIDs`, `badgerstore_node_query.go`). Transparently speeds `g.Nodes().ByLabel(...)` — and therefore any whole-node scan like Cypher `MATCH (n:L) RETURN n` — with ZERO API change (the sigma X5-wholenode ask, resolved as a transparent scan optimization rather than a new columnar door). The old path opened N distinct read transactions, each acquiring an oracle read-timestamp (a global-lock contention point under concurrent scans); the bulk path pays that once. Cache hits are still served without promotion (scan discipline); correctness rests on the invariant that a cache MISS implies the node was flushed to badger (dirty entries are never evicted), so a still-in-flight write is always a cache hit and never reaches the iterator. **MEASURED (M4 Max, 50k wide nodes, badger-in-memory, `-count`≥2): ~1.4× faster (153→110 ms/scan), ~5% fewer allocs/bytes** single-threaded; the gap widens under concurrent scans (fewer oracle acquisitions). CRITICAL config lesson (locked by a comment + A/B benchmark `badgerstore_node_bulk_bench_test.go`): `PrefetchValues=false` is load-bearing — a Seek-per-ID iterator with prefetch re-fills a discarded value window on every seek and measured ~10× SLOWER than the point-get path; prefetch=false is the ~1.4× win. Next lever (not this change): parallel decode across cores for a one-shot scan (the decode, not the fetch txn, is the single-threaded floor). See `tasks/backlog.md` BACKLOG 3.

- ADD — cross-machine (foreign-endpoint) relationship creation (ADR-0010, the ADR-0007 §2 horizontal-stage seam). A relationship whose END node lives on a slot owned by ANOTHER machine cannot go through the normal create doors: the endpoint-hash ladder resolves both endpoints from the local store, and a foreign end fails closed with `sharded.ErrSlotNotLocal`. `g.Rels().AddByIDForeignEnd(ctx, typeName, startID, foreignEnd, props)` is the door for that case. rho-tkg stays a pure library — the network hop lives entirely in the caller (sigma-tkgd): it RPCs the owning machine and passes the result in as a `store.ForeignEndpoint{NodeID, Hash, AttestTx}` descriptor carrying the attested `tkg_to_hash` + its provenance. Design:
  - The LOCAL start is locked and hashed normally (full R4-F5 on the local half); the FOREIGN end is NOT locked or fetched — its existence is caller-attested and its `tkg_to_hash` is `foreignEnd.Hash`. The rel is minted in the (local) start's slot per ADR-0007 §2 and lands entirely on this machine. A foreign START is not supported by this door — it is executed on the start's own machine as an ordinary local-start create (§3.4).
  - **to-hash relaxation (§4.1, owner-accepted):** for a cross-machine edge `tkg_to_hash` reflects the foreign node's state at ATTEST time, not local-commit time. Safe because `tkg_from_hash`/`tkg_to_hash` are deliberately NOT part of `ComputeRelHash` (they never were) — `Verify*Chain` and replication byte-exactness are unaffected; the staleness window is made explicit by the required `AttestTx` provenance.
  - New optional store capability `generatedcreate.ForeignEndpointRelCapability` (`PutRelationshipForeignEnd`) implemented ONLY by the slot-sharded store — single-machine backends (memory/badger/tiered) have no foreign partition, so the graph door fails closed with `ErrForeignEndpointUnsupported` on them. The sharded impl validates the local start, rejects a genuinely-LOCAL "foreign" end (`sharded.ErrForeignEndpointLocal` — a misuse that would skip a check it can perform), and co-commits the rel + both adjacency legs + one `ChangeRelPut` on the rel's shard (byte-identical to any single-machine create; the attested to-hash rides the wire as an ordinary `RelWire` field, so a replica of this machine reproduces the edge verbatim).
  - Temporal constraints fail closed (`ErrForeignEndpointConstraint`) — they need the live foreign end node, which is not locally available (§4.1), so the create refuses rather than silently skipping the check. New sentinels: `graph.ErrForeignEndpointUnsupported`, `graph.ErrForeignEndpointConstraint`, `store.ErrInvalidForeignEndpoint` (+ `sharded.ErrForeignEndpointLocal`).
  - The relationship create KERNEL (`createRelWithTypeRollback`) gained a third persist mode (`relPersistForeignEnd`) instead of a per-door copy of the delicate validate→persist→rollback sequence — every persist strategy stays in the one kernel (the reason it exists). The bool arg became a `relPersistMode`; the two local-endpoint doors + the batch/concurrent-ingest paths map through `relPersistModeFor` (zero behavior change).
  - Tests: happy-path (attested to-hash stamped, local from-hash captured, `Verify*Chain` passes proving to-hash is not load-bearing, outgoing adjacency finds the edge), the plain `AddByID` still fails closed for a foreign end (`ErrSlotNotLocal` — no silent widening), local-end misuse, missing-local-start, invalid-descriptor table, unsupported-store, constraints-fail-closed, an invalid-proof security-gate direct test, and a change-log emission proof (one replicable rel record) — all in-process via a narrow 2-slot store (no second machine needed). This is the rho-tkg-side foundation for the "full distributed graph" milestone; the incoming-adjacency half-edge (Model A), cascade fan-out, and sigma-tkgd wiring are the next increments (ADR-0010 §6). See `docs/adr/0010-cross-machine-edges.md`.

- ADD — cross-machine INCOMING half-edge stub (ADR-0010 Model A) + its delete cascade. `AddByIDForeignEnd` (above) puts the OUTGOING leg of a cross-machine edge on the start machine; the END machine still needs the edge in its LOCAL incoming adjacency so a cross-partition traversal that lands on the end node reads it there (adjacency is partition-local). `g.Rels().RecordForeignIncoming(ctx, store.ForeignIncomingEdge{RelID, TypeName, StartID(foreign), EndID(local), FromHash, ToHash, Version, temporal…})` records that half-edge as an adjacency-only STUB co-located on the end node's shard. Design:
  - **Registry-realignment + hash byte-identity.** The stub's rel-TYPE token is the start machine's, meaningless in the end machine's independent registry — so the door takes the type NAME and re-tokenizes it locally, then RECOMPUTES the content hash from the same inputs. Because `ComputeRelHash` keys on the type STRING (not the token), the stub's `tkg_hash` is byte-identical to the authoritative edge's — the stub carries the same identity. Mirrors the replication token-refetch discipline (lesson 51).
  - **Foreign-slot placement, adjacency-only.** The stub's rel-ID belongs to a FOREIGN slot but is physically written on the END node's shard (via the new `store.PreEncoded`-independent `PutRelationshipForeignIncoming`), so `IncomingRelationships(END)` folds it in with ZERO change to the adjacency resolver, while a slot-routed `GetRelationship(relID)` on the end machine still fails closed `ErrSlotNotLocal` (E is not the rel's authority). Only the slot-sharded store implements `generatedcreate.ForeignIncomingRelCapability`; single-machine backends decline it (`ErrForeignEndpointUnsupported`).
  - **THE replication blocker, solved.** A foreign-slot stub cannot be reproduced through the ordinary rel-put apply path — it routes by rel-slot and fails `ErrSlotNotLocal`. Two dedicated change-log records route by END-node slot instead: `ChangeForeignIncoming` (tag 11, put) and `ChangeForeignIncomingDelete` (tag 12, cascade delete). `applyForeignIncomingLocked` reconstructs the stub through the same import-trust pipeline (`WireToRelChecked` → token/property validation → hash recompute-AND-COMPARE) and writes via the capability, idempotently; `applyForeignIncomingDeleteLocked` removes it routed by the end slot, idempotently. Both proven BYTE-EXACT: a sharded replica of the end machine reproduces the stub (identical rel-ID, content hash, endpoint hashes) from the feed and reaches the same stub-free state after the end node is deleted.
  - **Cascade (increment 4).** Deleting the END node must remove the stub. The node's own shard cascade physically sweeps it (it is in the end node's adjacency), but badger's with-history tombstone VALIDATION demands a tombstone for every connected rel and the stub has no version chain to tombstone — and its rel-slot is foreign, so a plain `ChangeRelDelete` would not route on a replica. So the sharded `DeleteNodeWithHistory` (the live connected-node delete path — standalone AND in-tx) partitions foreign-slot stubs out of the rel-tombstone set and removes each via `DeleteRelationshipForeignIncoming` (emitting `ChangeForeignIncomingDelete`) BEFORE the node's own with-history delete, so the tombstone validation sees a stub-free adjacency. The hard `DeleteNodeCascade` (import/rollback path) got the symmetric guard. Both non-atomic across the stub-delete and node-delete batches by the SAME crash-recoverability contract as the existing cross-shard cascade (rels first, node last; re-apply idempotent).
  - **tx-rollback stub restore (increment-4 follow-up, now fixed):** a transaction that deletes an end-node-with-stub then ROLLS BACK now restores both the node and the stub. `ErrSlotNotLocal` was promoted to a store-level sentinel (`store.ErrSlotNotLocal`, re-exported as `sharded.ErrSlotNotLocal`, same value) so the partition-agnostic core can recognize a foreign stub: the tx history-snapshot treats a foreign-slot read as "no local history" (a stub is adjacency-only), and `restoreDeletedRelRow` restores it via `RecordForeignIncoming` (routed by END slot, idempotent). Proven by `TestModelA_TxRollbackRestoresStub`. The START-machine → END-machine stub-removal fan-out when the AUTHORITATIVE edge is deleted remains sigma-coordinated (rho-tkg exposes the local primitives `g.Rels().RecordForeignIncoming` + the store `DeleteForeignIncoming`; the RPC is sigma's).
  - Tests: store-level put + delete (`sharded/model_a_test.go` — local visibility, slot-routed Get fails closed, exactly-one dedicated-tag record for BOTH put and delete, local-start misuse guard, idempotent re-delete); graph-level BYTE-EXACT replica convergence for BOTH the put and the delete cascade (`model_a_convergence_test.go` — the delete test seeds a co-located ordinary rel + the stub so the cascade removes both, asserts the end-node delete SUCCEEDS where it previously failed closed, exactly one `ChangeForeignIncomingDelete` emitted, replica reaches node-free + stub-free state, idempotent re-apply); change-tag `String`/`Valid`/wire-number-stable pins extended to 12. Full `-race` suite green. See `docs/adr/0010-cross-machine-edges.md` §3.3.

- ADD — `g.Rels().ForEachByTypePropertyRangeOrdered(typeName, propKey, min, max, inclMin, inclMax, desc, opts, fn)` — the rel mirror of `g.Nodes().ForEachByLabelPropertyRangeOrdered`, closing the B2 rule-2 parity gap (rels had the unordered numeric range door + string-prefix door, but no ORDERED numeric / top-k door). Emits in contractual VALUE ORDER (asc, or desc), ties broken by rel ID ascending in both directions; `fn` returning false stops the scan so `ORDER BY r.prop [ASC|DESC] LIMIT k` is pushed into the index (O(k + log n)). Candidates come from the rel property index's ordered numeric view (over-selects — `fn` re-checks exact bounds). Non-temporal opts take the index fast path (`ErrIndexNotFound` when no usable rel property index / capability); a temporal `QueryOpts` (ValidAt/ValidStart+End/TxAt/TxPin) is served by the same sound full fold (`forEachRelValueOrderedTemporal`) as the rel prefix door — resolve-at-pin, filter on value-AT-t, sort by value; needs no index. New optional store capability `relOrderedRangeScanner` implemented by memory + badger (rel indexes are RAM-only, so no on-disk path); sharded/tiered decline exactly as they do for the sibling ordered/prefix doors. Tests: value-order contract (asc/desc + ID tie-break, negatives, mixed int64/float64 buckets), top-k early-stop + bounded exact-filter, `ErrIndexNotFound`, and a two-phase temporal proof (value in range at t0, out by now → included at t0, excluded now) — memory + badger-RAM, race-clean.

## [4.17.0] - 2026-07-16

- ADD — OPT10 Allen-predicate temporal query doors (`g.Temporal().NodesRelating(from, to, rels)` + rel mirror `RelsRelating`) and the labeled-During Step-1 envelope prune (ADR-0009). Until now the only interval temporal doors were OVERLAP-based (`NodesDuring`) or pairwise-entity (`RelateNodes`); there was no way to ask "which entities' valid-interval `Meets` / is `Before` / `Contains` … the window [from,to)?". `NodesRelating` answers with any subset of Allen's 13 relations (an `types.AllenRelationSet`), history-aware and **predicate-anywhere** (a match on ANY version of an entity's chain counts, so a superseded-before-the-window entity is still found by `{Before}` — the classic case an overlap query misses). Design:
  - New classifier `types.RelateOpen(aStart, aEnd, bStart, bEnd)` — the version-chain door's Allen classifier, treating an END of 0 as OPEN (+∞ via `math.MaxInt64`) so the routinely-open current-version interval `[start, ∞)` and an open query end classify exactly (Before/After/Meets are order-exact; Meets/MetBy are correctly UNREACHABLE across an open end). The existing `Relate` (rejects any zero endpoint) is untouched — two doors, same shape (rule 17).
  - New `probeRelate` chain-resolver kind (`chain_resolver.go`) carrying the relation set; `resolveNodeChainRelating`/`resolveRelChainRelating` classify each version's `[vStart,vEnd)` against the query interval most-recent-first (predicate-anywhere, same seam and rationale as `resolveNodeChainDuring`). UNLIKE `probeInterval`, the query end is passed RAW (0 = open) — never pre-resolved to a concrete "now+" bound, which would corrupt the Before/After boundary. Wired through `findNode/RelVersionRelating` (temporal.go) → the locked folds → the `g.Temporal` sub-API. `to == 0` = open query interval; an empty relation set yields an empty result; `from <= 0` or `from >= to` (closed) → `ErrInvalidTimeRange`.
  - Step-1 envelope prune wired into the labeled interval door `NodesByLabelPropertyDuring` (closes a rule-17 gap: the point-in-time labeled door already pruned). OVERLAP-sound for the During door only (it matches on overlap), so it is deliberately NOT applied to the Relating doors — their Allen set may include non-overlapping relations (Before/After/Meets) that envelope-overlap pruning would wrongly drop. `TestNodesByLabelPropertyDuring_PruneEquivalence` pins with-index == without-index == expected on memory + badger.
  - The store-level valid-time "zone-map" (a new all-node valid-time-ordered substrate) from the original OPT10 sketch was NOT built: index-level min-from pruning already exists (B4 / `TemporalIndex.QueryOverlap`), and a store-global segment map is high build surface for value only rare full-graph temporal scans have — deferred with no substrate to build on.
  - Tests: open-interval classifier table (all 13 relations, closed AND open-end cases, open-start/empty rejection, Meets-unreachable-across-open-end) — `pkg/types/allen_open_test.go`; core adversarial exact-set for every one of the 13 relations incl. the four non-overlapping ones, open-query interval, empty-set, invalid-range, a two-phase predicate-anywhere history proof (an older tile `Before` the window found after the head moved past it), Node/Rel parity — `temporal_relating_test.go`; a white-box resolver test covering the empty-set guard, the eclipsed-tile skip, and open-end classification — `chain_resolver_test.go`; sub-API forwarding + nil-receiver wrappers — `temporal/api_test.go`. Full `-race` suite green.

- PERF — B6 anchor+delta version-history storage (ADR-0009), opt-in via `badger.Config.HistoryDeltaEncoding` (default OFF while it soaks). Version-history rows (badger `0x07`/`0x08`) were FULL entity snapshots — a frequently-UPDATED wide entity (5–20 props, some with large unchanged values: customer/product/contact — NOT append-only SIEM/log events, which are ~1 version and have no history to delta) re-serialized its whole blob on every version bump. B6 keeps a full ANCHOR every 16 versions (`HistoryAnchorInterval`) and stores the rest as DELTAS carrying only the properties that CHANGED vs the interval anchor (plus both hashes + the full temporal block verbatim); a delta first appears at the 3rd+ version, so it benefits UPDATE-heavy reference entities, not single-version events. **MEASURED ~39% less history storage post-block-Snappy** on history-heavy wide entities (a PER-NODE, flag-on, single-store figure — the delta never enters the change-log feed, so a replica that does not also enable the flag stores full history) (the Phase 0.9 gate `wire_b6_history_gate_test.go`; the companion B3 timestamp-delta lever was DROPPED at 1.13% post-Snappy — see lessons 67). Design:
  - Self-describing framing, ZERO migration: an anchor is the raw full marshal (a msgpack map header `0x8x`/`0xde`/`0xdf` leads), a delta carries a 1-byte `'D'` (0x44) prefix that can never be a map header — so a legacy pre-B6 row is transparently an anchor, and reads accept BOTH forms regardless of the flag (safe to toggle on an existing store). No `fv` bump (the entity-row format is unchanged; the delta is a history-value-level representation).
  - Random-access `GetNodeVersion` reconstructs in ≤2 point reads (anchor + target delta); whole-chain scans reconstruct from an in-scan anchor cache (no extra reads); `NodeAsOf`/`RelAsOf` classify on the delta's Meta (temporal carried verbatim — no anchor read) and reconstruct only the winning version.
  - Byte-exact replication is preserved WITHOUT any change-log change: the delta is a storage-internal representation never carried in the change-log feed (a put-with-history logs the current row + a history bit; the replica reproduces history via its own write path), and `DiffNodeHistory` is deterministic, so two backends converge.
  - Truncate anchor-safety: a keep-newest-N truncation re-materializes to full only those kept deltas whose interval anchor is being removed (exact retention count preserved); trim-from is inherently safe (kept deltas' anchors are lower and survive). The current row (`0x01`/`0x02`) is always full. Memory backend stays full-snapshot (the differential oracle).
  - Wired across all three read paths + five write doors (node) / three (rel), gated so `HistoryDeltaEncoding: false` is byte-for-byte the prior behavior. Format primitive (`storeutil/wire_history_delta.go`) has its own reconstruction/determinism/idempotence/classification/size tests; a store-level differential battery drives an identical 20+-version chain into delta-ON and delta-OFF (oracle) stores and asserts every door (`GetNodeVersion`/`GetNodeHistory`/`NodeHistoryVersionsFrom`/`NodeAsOf` + rel mirrors), truncate anchor-safety, and a disk close/reopen all reconstruct byte-identically; full badger `-race` suite green.

- PERF — `Config.DisablePlannerStats bool` (off by default) — an opt-OUT of the query-planner statistics maintained on every node write. When set, the store SKIPS the per-write planner-stat sweep (the exact presence counter `NodeCountByLabelAndPropertyKey`, the NDV+min/max accumulator `NodePropertyStats`/`NodePropertyStatsSketch`, and the exact type-class partition `NodePropertyTypeClassCounts`) AND the equivalent rebuild at store open — both funnel through the single `adjustNodePropertyKeyCounts` choke point, so one gate covers write + open. The four stat capabilities then fail closed with `store.ErrCapabilityNotSupported` (a RUNTIME decline — the store still implements the capability interface, so `c.store.(Capability)` type-asserts true and callers keep their existing `errors.Is(err, ErrCapabilityNotSupported)` branch; no new caller code). NOT a correctness knob: no correctness path reads these counters (unique constraints use the property INDEX, and `NodeRangeCardinality` reads the property index too, so it is NOT declined). Wired uniformly through `badger.Config` / `memory.WithoutPlannerStats()` / `tiered.Config` (passed to every shard) / `sharded.Config` (passed to every slot) / graph `Config`. MEASURED (M4 Max, badger-in-memory, wide 12-property/3-label node, `-count=5` medians): PutNode ~9008 ns → ~5805 ns (**~36% faster**, ~1.55× write throughput), ~72 → ~63 allocs/op — the sweep is O(properties × labels) per write, so wide entities pay (and save) the most. For an insert-dominated workload that never runs a query planner over this store, this is a free write-path win. Store-level (memory/badger) + end-to-end graph-façade (`New(Config{DisablePlannerStats:true})`) tests prove the flag declines the four capabilities, skips the counter maintenance (internal maps stay empty), and leaves node reads/writes byte-correct; all four backends `-race` green.

- ADD — B2 string PREFIX scans (`STARTS WITH`) + temporal ordered scans (sigma port B2; the "ordered (label,property) index" ask). Investigation first: the numeric RANGE / SORTED / top-k access path is ALREADY shipped and publicly exposed in both RAM (a chunked sorted set — `orderedKeys`, effectively a B+ tree, maintained for free inside `PropertyIndex`) and on-disk (the `0x0A` order-preserving keyspace), so a "lock-free skip-list" would DUPLICATE it and was not built. The two GENUINE gaps were string prefix (the ordered view was numeric-only) and temporal ordered scans (declined). Both are now closed:
  - **Ordered STRING view** — the tested chunked sorted set is generalized to a generic `sortedChunks[T cmp.Ordered]` (numeric keeps `sortedChunks[float64]`, byte-behavior preserved — the numeric ulp/precision logic lives outside the structure), and `PropertyIndex` gains a second view keyed by string (`strKeys`/`strBuckets`), maintained alongside the numeric one inside `AddKey`/`removeKey`/`Purge` — ZERO new mutation call sites. It is EXACT (string sort keys never collide — no over-selection, unlike the numeric view). Reused verbatim by NODE and REL property indexes (same `*PropertyIndex` type).
  - **Prefix doors** — `g.Nodes().ForEachByLabelPropertyPrefix(label, propKey, prefix, desc, opts, fn)` and the rel mirror `g.Rels().ForEachByTypePropertyPrefix(...)`: emit in CONTRACTUAL lex value order (asc / desc), ties by id ascending in BOTH directions, `LIMIT` pushed down via `fn` early-stop (O(k + log n) top-k), empty prefix = every string value. NODE prefix is served from the RAM ordered view OR the persisted `0x0A` raw-domain keyspace (a `"s:"+prefix` key-prefix iteration, overlay-merged — `PropertyIndexOnDisk`); REL property indexes are RAM-only. This is the FIRST rel ordered-scan door (rels previously had only the unordered numeric range door — rule 2 parity).
  - **Temporal ordered scans (Stage B)** — the ordered numeric range, node prefix, and rel prefix doors NO LONGER decline temporal `QueryOpts` (the old `ErrOrderedScanTemporal`, now a legacy no-longer-returned sentinel kept for compat). A temporal opts combination (`ValidAt` / `ValidStart`+`ValidEnd` / `TxAt` / `TxPin`) is served by a SOUND FULL FOLD: every label/type member is resolved to its version AT THE PIN (reusing `nodesByLabelLocked`/`relsByTypeLocked` — the same chain resolver + B4 valid-time prune as the temporal `ByLabel`/`ByType` door), the value predicate is applied to the value-AT-t, then sorted by that value. This is the only sound answer (the current-state index is valid-time-agnostic — it would both miss a node in range then-not-now AND over-report the reverse); O(N log N) in temporal membership, needs no property index (reads resolved values directly). Wired generically via `forEach{Node,Rel}ValueOrderedTemporal[K cmp.Ordered]`.
  - Tests: index-level ordered-string (asc/desc + id tie-break in both directions, boundary-successor exclusion, empty/no-match prefix, remove/purge maintenance, non-string values invisible); graph-level node prefix across memory/badger-RAM/badger-disk (value order, top-k early-stop, current-state maintenance through Update, `ErrIndexNotFound`); rel prefix mirror (memory/badger-RAM); and the two-phase temporal proof (a value in range at t0 but out now is correctly included at t0 / excluded now, value reordering across time, string prefix matched-then-not-now, rel mirror) on all backends. Full suite + affected `-race` green; the generic `sortedChunks` refactor preserved all numeric-range behavior (the chunked model-equivalence test passes unchanged).

- PERF — B4 temporal-scan valid-time ENVELOPE prune, WIRED into the core resolver (sigma port B4, stages 3–5; completes the S1/S2 foundation). A temporal `ByLabel` scan (`g.Nodes().ByLabel(label, QueryOpts{ValidAt|ValidStart/ValidEnd})` AND the named `g.Temporal().NodesByLabelAt(label, at)`) now consults the per-label valid-time envelope index to DROP candidates whose `[min(validFrom), maxTo(validTo)]` envelope provably cannot overlap the query — BEFORE the expensive per-id chain resolve. New OPTIONAL `store.TemporalCandidateCapability` (`PruneTemporalCandidates(labelToken, ids, opts) (kept, ok)`): implemented on memory + badger + **sharded** (which routes each candidate id to its OWNING shard's envelope — no cross-shard merge needed, since the prune only asks "can THIS id's envelope overlap?" and every id lives on exactly one slot; closes the S5 parity gap). DECLINED by tiered: its temporal index spans TIME-windowed shards that may be cold/archived, so pruning an id would require checking out the very shard the resolve would open anyway — the prune cannot be cheaper than the resolve it avoids, so tiered folds full history (`c.temporalCandidates` nil). Declining an accelerator never changes the answer. The prune is **positive-evidence-only**: it drops an id ONLY when the index vouches (`EnvelopeOf` ok=true) AND `EnvelopeOverlaps` is false; an id the index does not cover, or a store with no envelope, is always kept. The envelope is an APPEND-ONLY SOUND SUPERSET of every version's interval (union over the whole version chain, extend-on-write, never shrinks — the domain forbids updating a valid-to-closed node, so a multi-version node's non-final versions are open-ended and its envelope is `[minFrom,∞)`, never wrongly pruned), so a kept id may still be rejected by the chain resolver but a pruned id can never have matched — the resolver stays the single correctness authority (mirrors the K1 label-tx-membership sidecar pattern). Both temporal-ByLabel doors funnel the SAME prune, enforced equivalent by test (rule 17). Byte-of-truth: `TestTemporalCandidatePruneEquivalence` runs an adversarial two-phase Doc scenario (open-ended two-version node, bounded single-version node, future "phantom window" node, and the union-soundness trap — a node matching at t via an OLD wide version while its current version does not) on memory AND badger, asserting the index-present result is IDENTICAL to the no-index oracle AND to the pinned expected set (rule 16: a silent double-corruption cannot pass an equivalence-only check), plus a white-box assertion that the prune actually fires (returns ok=true and drops the out-of-window ids) and that named `NodesByLabelAt` agrees with generic `ByLabel{ValidAt}`; `TestTemporalCandidatePruneTieredDeclines` pins the decline + correct-full-fold. MEASURED (`BenchmarkTemporalEnvelopePrune`, M4 Max, memory, 5000-node label, 90% cold/out-of-window): NodesByLabelAt ~1,060 µs/op → ~527 µs/op (**~2.0× faster**), 1,804 KB → 585 KB/op (~3.1× less), 19,548 → 1,549 allocs/op (~12.6× fewer — 4,500 cold chains skipped). Full core/memory/badger/index/storeutil `-race` suites pass. **SCOPE (honest — the ~2.0× is a best-case cold-label figure):** the envelope's upper bound is +∞ for any entity updated at least once (its current version is open-ended), so the prune fires only when a candidate's WHOLE envelope is outside the query — i.e. it drops expired/deleted entities on an "as-of-now" scan (the common case, still a real win) and not-yet-existing entities on a past-window scan; it is INERT for actively-updated entities on a past-window query. Net: an accelerator for scans dominated by closed-interval / expired / single-version members, not a general temporal-scan speedup. (A per-version index would prune updated entities too, but costs memory ∝ total version count — declined; see `tasks/backlog.md` 4a.)

- TEST — `BenchmarkConcurrentLabelScan` (badger) — a permanent A/B regression guard for the sharded entity cache (sigma port B1/OPT3). Confirms the sharded LRU (`indexpkg.ShardedCache`, already in-tree since v4.16.0) solves the concurrent-label-scan contention sigma flagged: 16 workers repeatedly scanning a 20k-node label, all cache hits, MEASURED ~37M nodes/s sharded vs ~5.8M nodes/s single-mutex (`TKG_CACHE_SHARDS=1`) = **~6.4×**. The scan path (`fetchNodesByLabelIDs` → `prefetchNodeScan` → `GetNoPromote`) already takes only a per-shard READ lock with no LRU promotion, so scan hits never serialize on a write lock and spread across shards — the BP-Wrapper GOAL by a simpler mechanism. No production code change: B1's core was already implemented; this quantifies and guards it.

- ADD — `replication.ChangeOp` + `replication.ChangeOpOf(rec store.ChangeRecord) (ChangeOp, error)` (sigma ask A4, op half): the normalized mutation-kind discriminant that pairs with the shipped `DecodeChangeIdentity` `(kind, ID)` to give an out-of-tree CDC mirror the full `(kind, ID, op)` it needs. `ChangeOp` is `Upsert` (put — re-read current state and upsert) / `Delete` (DETACH DELETE by ID) / `HistoryVersion` / `HistoryTruncate`. `ChangeOpOf` classifies from the record TAG ALONE — it never decodes the msgpack payload, so it cannot fail on a corrupt body and is safe to call on every record to route it before deciding whether the ID is even needed; the two control tags (`ChangeMeta`/`ChangeClear`) return `ErrNoEntityIdentity`, an unrecognized tag returns `store.ErrCorruptWire`. Additive: `DecodeChangeIdentity`'s signature is unchanged (a pinned consumer keeps working). Tests drive REAL records tailed from a change-log store (upsert/delete tag→op agreement + every entity op pairs with a decodable identity) plus control/unknown/garbage-payload fail-closed cases, race-clean. NOTE the `labelOrType` field in the original ask sketch is deliberately NOT added to this pure decoder: token→string resolution is a graph-layer responsibility (the record carries only the label/rel-type TOKEN, meaningless to a foreign sink without the registry), and the mirror pattern does not need it (a put re-reads current state, which yields resolved labels; a delete is by-ID). A registry-aware resolver would be a method on `*replication.API`, offered separately if required.

## [4.16.0] - 2026-07-16

- PERF — concurrent-ingest allocation reduction (ADR-0007 lever #2). The concurrent-ingest apply path spent ~30% of CPU in `mallocgc`/GC; three per-node allocation cuts, each safe by construction: (1) **write-only bulk door** `Session.AddNodes(labels, props, count)` — skips the per-node isolation `DeepCopy` that `AddNode` needs for its caller-mutable returned skeleton (the mass-ingestion path does not need endpoints), the single largest core-layer per-node allocation. (2) **shared canonical properties** — `Node.SetPropertiesCanonicalShared` lets a write-only bulk group share one `NewPropertySlice` result across all sibling create nodes (safe: they are read-only until the store deep-copies at Put, and never returned to the caller). (3) **ownership-transfer put** — new OPTIONAL `store.OwnedPreEncodedPutCapability` (`PutNodesBatchOwnedPreEncoded`, native badger + sharded; memory/tiered/wrappers decline and copy as before): when the concurrent apply group is entirely write-only (`result == nil` on every node), no code path reads or mutates the nodes after the put, so the store freezes each node IN PLACE and caches it directly instead of `freezeNodeCopy`'s deep copy (node + property-slice + integrity + temporal — four allocs/node). A single caller-visible skeleton in the group disqualifies it (`syncPendingNodeResult` copies the frozen node into the skeleton after the put), so mixed groups take the copying door — the graph layer enforces the gate; the capability is a pure allocation optimization, never a correctness requirement. (4) **reflection-free property encode** — the entity-row msgpack encoder handed its `[]PropertyWire` slice to reflective `enc.Encode`, whose slice path boxes EVERY element via `reflect.Value.Interface()` to dispatch its `CustomEncoder` (one heap alloc per property, on the applier's serial hot path). `encodePropertyArray` (`wire_encode.go`) now emits the array header directly and calls `PropertyWire.EncodeMsgpack` on each ADDRESSABLE element — a plain method call, no reflection, no boxing — so encode-internal allocations are CONSTANT (2: the escaping output copy + the wire-struct box) regardless of property count, where they were `properties + 2`. Byte-identity is load-bearing (content hash / replica byte-exactness / v1-v2 wire format) and the ordinary marshal-vs-`msgpack.Marshal` test can't prove it (both route through the same `EncodeMsgpack` — it would compare new-against-new), so it is locked by GOLDEN vectors (`wire_encode_golden_test.go`) captured from the reflective encode BEFORE the change, across the full `PropertyWire` optional-field matrix (tokenized/string keys, int/float/bool/blob/custom-pointer values, v1 and v2, node and rel). A rejected experiment kept honest by measurement: passing the wire struct BY POINTER to shed its interface box turned out ~1 alloc WORSE in the realistic fresh-per-call path (the property-slice literal escapes separately), so the value-pass stayed. MEASURED (M4 Max, sharded 8 lanes/8 shards, badger-in-memory): concurrent bulk ingest ~1.68M → ~2.1–2.3M inserts/s; allocs/op 52 → ~32–33 (bytes/op ~4.8K → ~3.8K), the encode cut scaling with property count. Ceiling context (`bench_ceiling_test.go`): a perfectly-sharded 8-store badger ceiling with the core layer stripped away is ~2.5M/s, so the pipeline now runs at ~85% of the durable store ceiling — the residual is badger's own LSM per-op cost (~34 allocs/op, single flush loop + LSM commit), i.e. 10M/s single-process needs more shards/cores or a lighter store engine, not core-layer work. Full core/badger/sharded/memory/graph `-race` suites pass; `TestIngestConcurrentBulkAddNodes` asserts exact count, TxFrom stamp, property-value survival through freeze-in-place, point-read thaw, and hash-chain validity across concurrent sessions.

- PERF — concurrent-ingest write-door scaling: removed the two back-to-back core-layer lock bottlenecks that capped the concurrent ingest door (`IngestOptions.Concurrent`, ADR-0007 lever #1) at ~2× single-thread regardless of core count. (1) `Core.mu` is now a STRIPED RWMutex (`shardedRWMutex`, 32 cache-line-padded stripes) with EXACT `sync.RWMutex` semantics — a writer (`Lock`) still acquires every stripe and fully excludes all readers, but the reader fast path fans out by ingest lane (`RLockShard(lane)` in the prepare path `batch_queue.go` and the concurrent apply `runUnderRLockShard`), so N lanes taking `c.mu.RLock` no longer serialize on one reader-count cache line. Every non-hot reader keeps the drop-in `RLock()/RUnlock()` (stripe 0) and every writer keeps `Lock()/Unlock()`, so the 24 other reader sites and all 31 writer sites are unchanged. (2) The prepare-path `existingLabelsOrNextProbeTokens` no longer takes the single exclusive `c.registryMu` for the common case of one already-registered label (the steady state after declare-on-prepare): the label registry's own internal RWMutex makes `Lookup` safe, and the caller's `c.mu.RLock` already fences the registry POINTER against the import/tx-rollback swap (which takes `c.mu.Lock`), so `registryMu` is needed only for probe-token allocation on the miss/multi-label paths. MEASURED (Apple M4 Max, badger-in-memory, 8 lanes over 8 shards, `-count≥3` medians): sharded-lanes ingest ~1.05M → ~1.68M inserts/s (+60%); the mutex profile confirms `c.mu` and `registryMu` are gone from the hot path (`addNodes` prepare 25% → 2% of contention) with the residual ceiling now store-internal (badger per-shard flush) + allocation/GC (lever #2). Correctness: the `shardedRWMutex` carries its own race battery (writer-excludes-readers data-race probe, exclusion-invariant, cross-stripe reader parallelism, stripe pairing); full core + store + graph `-race` suites and the concurrent unique-storm / byte-exact-replica tests pass. Zero API change; interactive (standalone / tx / plain `g.Batch()`) and strong-mode ingest paths are unaffected (stripe 0, `registryMu` unchanged).

- ADD — sharded store capability parity sweep (ADR-0007 S5): the sharded backend now implements every index + statistics optional capability, closing the gap to memory/badger/tiered. A label's nodes/rels are distributed across slots, so — unlike tiered, which routes property indexes to its ontology reference shard — each capability fans its DDL out to EVERY shard (each badger shard maintains + auto-updates its own index over its local entities, in lockstep) and folds the per-shard results on read. Implemented: **PropertyIndexCapability** (`NodesByLabelAndProperty` — cross-shard ID-sorted paginated fold); **RelPropertyIndexCapability** (accelerated, unlike tiered which declines it — the sharded shards are static and all-open, so a rel-value index per shard is foldable); **CompositePropertyIndexCapability** + **CompositeIndexIntrospectionCapability** (AND-match fold; `ListCompositePropertyIndexes` reads the anchor's identical defs); **TemporalIndexCapability** + **HighFrequencyIndexCapability** (uniform DDL fan-out; shared temporal namespace); **NodePropertyKeyStatsCapability** + **NodePropertyTypeClassCountsCapability** (index-free per-shard counters summed field-wise; graph-computed Missing); the inline `NodeRangeCardinality` scanner (per-shard bit-sliced sum, exact only if every shard is exact else declines→caller scans); and **VectorIndexCapability** + **VectorIndexOptionsCapability** + **FilteredVectorSearchCapability**. The vector index keeps ONE index PER SHARD (reusing badger's proven per-write maintenance — no store-level write-path hooks, avoiding tiered's ~13-site silent-staleness surface) and merges the per-shard top-k globally by distance — EXACT for brute-force, sound-approximate for HNSW; the store keeps only per-index def metadata (dims+metric, persisted to anchor MetaKV + reloaded at open) so it can re-rank via the newly-exposed `index.VectorDistance` (the single distance primitive both engines now route through — no reimplementation, no drift). A uniform per-shard sentinel (ErrIndexExists / ErrIndexNotFound / ErrTemporalIndexExists / ErrVectorIndexExists / …) is coalesced to the one logical outcome via `fanOutUniform`; genuine cross-shard divergence surfaces as a joined error. Every fold writes into its own indexed slot (the shard fold is parallel) and reduces after the barrier — no shared-accumulator race. Batteries (all race-clean, nodes/rels distributed across shards via S4 ingest lanes): cross-shard exact-set folds + phantom-empty + paginated-merge for node/rel/composite; exact type-class partition + presence agreement + range-cardinality decline-then-exact; temporal/high-freq DDL sentinels + cross-shard `NodesAt` correctness with the index present; and for vectors the brute-force EXACTNESS oracle (global top-k in distance order), auto-maintenance (add/update-rerank/delete through the merge), filtered exact top-k, DDL/dim sentinels, and on-disk reopen. Capabilities genuinely declined-with-reason (mirroring tiered / not in the index-stats scope): TransactionTimeQuery, HistoryRollbackTrim, LabelTx/RelTypeTx membership (full-history fold is the correct sharded path), and the depth-iteration accelerators.

- ADD — per-lane UNIFIED ID generators for concurrent-ingest write parallelism (ADR-0007 S4; OFF by default, zero behavior change): new `Config.IngestLanes uint8` (0 = legacy dual model). When >0, `core.New` builds `IngestLanes` extra generators, each pinned to its OWN distinct snowflake node-field (slot) drawn from 0..31 excluding the interactive pair `{SnowflakeNodeID*2, *2+1}`; a concurrent ingest session (`IngestOptions.Concurrent`) pins lane→slot and mints BOTH its nodes AND its rels from that one generator — the sharded catalog's `disciplineUnified` contract — so a whole commit group lands in one slot → one shard → one batched door call. Value-level ID uniqueness is preserved WITHOUT the even/odd node/rel split: a unified generator never mints the same `(time, seq)` twice (so a node and a rel in one slot never collide) and distinct node-fields separate the slots. Requires `2+IngestLanes ≤ 32` (the 5-bit node field); `New` fails closed otherwise. Interactive writes (standalone / tx / plain `g.Batch()`) and strong-mode ingest keep minting from the interactive pair — lane 0 always resolves to it, and with `IngestLanes==0` every lane does. New file `ingest_lanes.go` (`buildLaneGenerators`, `nextNodeIDForLane`/`nextRelIDForLane`, `laneGeneratorIndex`); `BatchBuilder.genLane` routes `batch_queue.go` mints; the concurrent session sets it from its lane counter (round-robin over the lane generators). Gates: the collision battery — 3.5M node+rel IDs across the interactive pair + 6 lanes, global uniqueness (the silent-ID-collision class); concurrent minting across 9 sources under `-race`; per-slot pinning + unified node/rel co-slot; disabled-by-default; slot-exhaustion fail-closed; end-to-end concurrent lane pinning (each session's whole population on one slot); and the sharded integration `TestGraphLevelIngestLanesRouteAcrossShards` (lanes route to distinct claimed shards, no `ErrSlotNotLocal`, group co-location) — all race-clean. NOTE the throughput acceptance bar (owner's M4 Max) is a separate hardware-gated measurement, not claimed here.

- ADD — retention fail-closed read plumbing (ADR-0008 stage R1; no purge yet): new sentinel `ErrRetentionExpired` (canonical in `internal/core`, re-exported from `pkg/graph`) returned by a temporal read whose pin falls before a relevant label's retention watermark. Retention PURGE (a later stage) hard-removes whole entities below a per-label age boundary WITHOUT tombstones, so R1 installs the fail-closed GUARD BEFORE the purge that needs it — a half-built purge can never read as complete. Storage mirrors the compaction watermark (`retention.go`): a per-label `retention_watermark/<labelToken>` MetaKV key + a graph-max fast-gate key (`retention_max_watermark`) rehydrated at open, so the common no-retention path pays nothing (a pin at/above the max is never checked). Point doors (`NodeAtTx`/`NodeAsOf` + rel mirrors) check the queried entity's label watermark(s) — per-label precision, an unrelated label is not rejected; scan doors fail the whole scan against the graph max (`checkScanRetention` wired into `validateTemporalQueryOptsScan` — every generic `ByLabel`/`ByType`/`All` scan — plus the named `NodesAsOf`/`RelsAsOf` seams). `advanceRetentionWatermark(labelToken, w)` is the max-monotonic seam R2's purge will call; `Admin.Reset` reaps it. Tests: two-door watermark (point per-label incl. precision, scan whole-scan), monotonic advance, durable across reopen (badger), reset clears — race-clean on memory/badger; the sentinel is in `docs/errors.md`.

- ADD — `time.Time` accepted directly as a node/rel property value (sigma ask 3, ergonomics): callers no longer pre-convert timestamps at every DTO/Bolt boundary. A TOP-LEVEL `time.Time` is canonicalized at the door (`PropertySlice.Set` / `NewPropertySlice`) to the existing `types.TemporalValue{Kind: TemporalDateTime, Value: RFC-3339}` — the zone is preserved in the ISO rendering, and because the conversion happens BEFORE validation the rest of the pipeline (deep copy, content hash, wire) sees only the already-supported `TemporalValue`, needing no new case. Reading back yields the `TemporalValue` (the documented stored form); it round-trips through the hash chain and export/import. Scoped to top-level deliberately: a `time.Time` nested inside an `[]any`/`map[string]any` is rejected the same as a nested `TemporalValue` (nested temporal values are not a supported wire shape — their content hash does not round-trip, so accepting the sugar there would be a silent corruption). `ValidatePropertyValue` and the core size-limit check accept `time.Time` so the graph create/update doors admit it. Tests: canonicalization (incl. zone), Set==explicit-TemporalValue equivalence, nested-rejection boundary, graph round-trip with hash-verify + export/import.

- ADD — `replication.DecodeChangeIdentity(rec store.ChangeRecord) (EntityKind, snowflake.ID, error)` + exported `replication.EntityKind` (`EntityKindNode`/`EntityKindRelationship`/`EntityKindUnknown`) and `replication.ErrNoEntityIdentity` (sigma ask 4): extracts the entity KIND and Snowflake ID a change-log record concerns WITHOUT the caller needing the internal wire codec (the msgpack `NodeWire`/`RelWire` bodies live in `internal/storeutil`, not importable out-of-tree). Unblocks a durable, restart-safe outbound CDC mirror (e.g. a Memgraph/Neo4j sink riding `g.Replication().Watch`): identity + kind is enough — re-read current state for a put, `DETACH DELETE` for a delete. Covers every single-entity tag (put, delete, history-version, history-truncate — node & rel); the two store-global control tags (`ChangeMeta`, `ChangeClear`) return `ErrNoEntityIdentity`; a corrupt/hostile payload or unknown tag fails closed with `store.ErrCorruptWire` (every decode routes through the existing `storeutil.SafeUnmarshal` per-tag decoders — never a raw panic at the trust boundary). Lives in `pkg/graph/replication` (not `pkg/graph/store`) because the decode needs `internal/storeutil`, which imports `store` — a free function there avoids the import cycle that methods on `store.ChangeRecord` would create. Tests drive REAL records tailed from a change-log store (put/delete/history/cascade) plus fail-closed corrupt/control/unknown-tag cases, race-clean.

- ADD — bitemporal valid-time INTERVAL doors `Temporal().NodesDuringTx(from, to, txAt)` / `RelsDuringTx(from, to, txAt)` (sigma ask 2): the interval siblings of `NodesAtTx`/`RelsAtTx`. Return every entity with a version whose valid window OVERLAPS `[from, to)` AS KNOWN AT `txAt` — each chain is first filtered to versions recorded by `txAt` (`TxFrom <= txAt`; a superseded version is not retracted, so it stays the authority for its valid-time slot at every later `txAt` — lesson 43), then the predicate-anywhere overlap test runs (a version that overlapped earlier still matches even when the belief-head-at-`txAt` no longer does — rule 16). `to == 0` is open-ended-to-now; `txAt == 0` ⇒ exactly `NodesDuring`/`RelsDuring`. Closes the multi-valid-version miss for Cypher `AS OF SYSTEM TIME t BETWEEN a AND b`, the interval analogue of the point-case fix `NodesAtTx` already shipped. The core resolver was ALREADY plumbed — `findNodeVersionMatchingDuringTx` and the generic `QueryOpts{ValidStart,ValidEnd,TxAt}` door (`ByLabel`/`ByType`/`All`) route through the same `chainProbe{kind: probeInterval, tx: txAt}` seam — so rule 17's generic-door equivalent needed no new code; the named doors are thin folds over it. Tests: focused deterministic two-phase (memory) proving the `txAt` recorded-by-then flip AND the older-version-overlap case with exact-set assertions, plus the randomized bitemporal oracle harness extended to cross-check the named doors against the oracle at EVERY `txAt` on all four backends (memory/badger/tiered/sharded), race-clean.

- ADD — `GraphTx.GetOrCreateByKey(label, propertyKey string, value any, extraProps map[string]any) (*types.Node, bool, error)` (sigma ask 1): the transaction-scoped sibling of `NodeOps.GetOrCreateByKey` (the `g.Nodes()` door). Same semantics — a single value stripe (shared with the standalone door AND any active unique constraint on the pair) held across the lookup and the create yields exactly one create under a storm of concurrent same-key callers; `value` must be an indexable scalar (float/non-scalar → `ErrUniqueUnsupportedType`); `extraProps` seed a fresh node only — but the create PARTICIPATES in the caller's open transaction instead of auto-committing: visible to later reads on the same tx (ghost-read consistency), its `EventNodeCreate` buffered and published on `Commit`, and UNDONE by `Rollback`. Closes the MERGE-inside-a-statement-tx correctness gap (the `g.Nodes()` door commits its create immediately, so a later clause failing could not roll it back). Exposed on `pkg/graph` automatically via the `GraphTx = core.GraphTx` alias. Implementation reuses the standalone value-stripe + property-index-probe + `addNodeInternal(heldStripes)` path but reaches the store through the `*Internal` doors under the tx's already-held `c.mu` (no `runUnderRLock` re-entry) and routes the create's event/rollback bookkeeping through `noteNodeCreateResultLocked`. Tests: in-tx visibility, two-phase rollback-undoes-create (rule 15), pre-existing hit, concurrent-txs-exactly-one-create (± constraint), shared-stripe-with-standalone storm, float rejection — memory + badger, race-clean.

- EXPERIMENTAL — slot-sharded store, stage S3 (ADR-0007): the store-global change-log + **topology-independent replica convergence**. `sharded.Config.ChangeLog` opts in a single store-global LSN allocator (`changeLogAllocator`) injected into every shard via the existing `badger.Config.ChangeLogSeqSource`, so every shard co-commits its own records + `LastLSNKey` in its own `WriteBatch` but all records draw from ONE monotonic sequence — a single total commit order across shards. Reseed at open is AUTOMATIC and needs none of tiered's persisted-watermark/poison machinery: every shard is always an open local badger, so each shard's `badger.New` folds its durable `LastLSNKey` into the shared allocator via `ChangeLogSeqSource.Observe` (a shard that cannot open fails `New` outright). `sharded.Store` now satisfies `store.ChangeFeedCapability` (`ForEachChange`/`ChangeFeed`/`LastCommittedLSN` — a barrier-first, W-bounded paged k-way min-heap merge over all shards' `0x09` logs; the `Flush` barrier makes every allocated LSN durable, then `W = LastCommittedLSN` bounds emission so records allocated mid-drain defer to the next poll — ADR-0005 Finding-1), `store.ChangeLogStatusCapability`, and `store.TxChangeLogScope` (per-tx buffer folded over every shard; LSNs minted at commit so a rolled-back tx burns none on any shard); core's `changeFeedCapability` switch admits `*sharded.Store`. **Rel-create record fix (correctness):** the S2 `PutRelationship`/`PutRelationshipsBatch` used the record-FREE partial doors (`PutRelEntityAndOut`/`PutRelIncoming`), so a sharded rel create was invisible to the feed — a tailing replica would never see it. New badger door `PutRelationshipCoLocated` writes the rel entity + BOTH adjacency legs (they live on the rel's shard, ADR-0007) + the co-committed `ChangeRelPut` record in ONE `WriteBatch`, skipping only the same-shard endpoint-existence check (the sharded layer validates endpoints cross-shard first); both sharded rel-create doors now use it. **Crown test:** a 2-shard sharded primary with the change-log on (nodes mint on slot 0/shard 0, rels on slot 1/shard 1, so the feed genuinely MERGES records across two shards in one LSN order) converges BYTE-EXACT — same integrity hashes + full version history — onto BOTH a single non-sharded badger replica AND a 4-shard sharded replica, proving the feed is a topology-independent total order (records carry entities verbatim; each replica routes by its own catalog). Store-level tests assert cross-shard gapless LSN ordering, disabled-by-default inertness, and LSN monotonicity across close/reopen (no reuse). Still pending: S4 lanes + the measured throughput bar, S5 capability parity + release. See `docs/adr/0007-slot-sharded-store.md`.

- EXPERIMENTAL — slot-sharded store, stage S2 (ADR-0007): relationship batched doors, cross-shard cascade delete, and the `VerifyConsistency` crash-window diagnosis door, with a sharded arm added to the bitemporal oracle harness. **Batched doors** (`PutNodesBatch`/`PutNodesBatchPreEncoded`/`PutNodesBatchPreEncodedLog`/`PutRelationshipsBatch`/`DeleteNodesBatch`/`DeleteRelationshipsBatch`) are ATOMIC PER SHARD GROUP — there is no cross-shard `WriteBatch`, so each partitions its input by shard, VALIDATES THE WHOLE INPUT FIRST (structure, slot-locality, no duplicate IDs, creates-not-present, rel endpoints live, node deletes unconnected across ALL shards), then applies per shard group in ascending shard order; a surviving mid-sequence I/O error returns a typed `*PartialBatchError{Op, CommittedShards, FailedShard, Err}` (fail-LOUD, never a silent cross-shard partial — the verify door diagnoses residue). The pre-encoded arrays (ADR-0006 §4.5) are sliced per shard group with INDEX ALIGNMENT PRESERVED — `wireBodies[j]`/`logBodies[j]` always travel with `nodes[j]` (an off-by-one is the silent-wrong-answer class; tested by giving every node a distinct patched `TxFrom` and asserting each reads back its own). **Cross-shard cascade delete** (ADR Risk 1) is crash-RECOVERABLE, not crash-atomic: fold-collect every connected rel ID across all shards, delete each rel on ITS OWN shard in DETERMINISTIC ascending rel-ID order (each a single-shard single-`WriteBatch` atomic op, so a crash stops at a reproducible boundary), then delete the NODE row LAST — a crash mid-cascade leaves dangling RELS but NEVER a ghost-edged node, so recovery always finds a live node to re-drive from; a rel whose row is already gone (torn prior run / index orphan) has its stale adjacency purged (`PurgeOrphanRelationshipIndexes`) so the final node delete does not reject on a phantom edge. **`VerifyConsistency() (Report, error)`** is a read-only, no-repair scan for the dangling references a non-atomic cascade/split-write can leave: `AdjacencyOrphans` (an adjacency entry whose rel row is gone), `RelEndpointOrphans` (a live rel whose endpoint node row is FULLY gone — deleted-WITH-history is NOT flagged, B32), and `ShardMismatches` (a row on a shard its slot does not route to); `Report.OK()`/`Total()`. **Oracle arm**: the sharded backend (BaseSlot 0 / SlotCount 2, so every rel is cross-shard from its endpoints) now runs beside memory/badger in the bitemporal oracle harness, cross-checking every probe class (point, interval, as-of, TxPin) against the same oracle. Still NOT lane-wired (S3 change-log, S4 lanes/perf bar, S5 capability parity pending); the pre-encoded doors are direct-callable but core's router keeps them badger-only until S4. See `docs/adr/0007-slot-sharded-store.md`.

- EXPERIMENTAL — slot-sharded store, stage S1 (ADR-0007, `pkg/graph/store/sharded`). New `sharded.Store` / `sharded.Config` / `sharded.New()` implementing the FULL `store.MandatoryStore` contract (CRUD, adjacency, bulk-read, batch, history, stats, iteration — including relationships and version history) over N `badger.Store` shards routed by the SLOT carried in the snowflake node field of every ID: `shardFor(id) = catalog[decompose(id).Node]`, an immutable O(1) pure function of the ID. `Config{Dir, InMemory, BaseSlot, SlotCount (1..32, base+count<=32), + per-shard badger passthroughs}`. A slot CATALOG (claimed range, slot→shard map, ID-discipline marker, format version) is persisted on the ANCHOR shard (BaseSlot — it also owns MetaKV, the label/reltype/property-key registries, and graph-layer markers, mirroring tiered's refShard); opens FAIL CLOSED (`ErrCatalogConflict`) on a config/catalog mismatch (wrong `SlotCount`, missing mapped shard dir) or a corrupt blob (`ErrCatalogCorrupt`). Any door reached with an ID whose slot is unclaimed fails closed with `ErrSlotNotLocal` (at the horizontal stage this becomes "route to the owning machine"). Point ops route by entity-ID slot; a relationship row AND both its adjacency index entries live on the REL ID's shard, so `GetRelationship`/`DeleteRelationship` are O(1) single-shard while adjacency reads (`Outgoing`/`IncomingRelationships`) are PARALLEL FOLDS over all shards (never assuming rel-slot == start-slot — foreign-ID puts spread the slots) and endpoint existence checks read the endpoint's OWN shard. Scans/counts/stats/iteration fold across shards in parallel with an ID-sorted merge and pagination applied AFTER the merge (straddling shard boundaries). Frozen-row scan / mutable point-read semantics are inherited verbatim from badger. NOT YET LANE-WIRED: the change-log/feed (S3), pre-encoded-put (S4), and vector/temporal/high-frequency/property-index optionals (S5) are declined; per-lane generators (S4) are not wired, so core still mints legacy dual-generator IDs (nodes `SnowflakeNodeID*2`, rels `*2+1`) — a graph-level deployment (`graph.New(Config{Store: sharded.New(...)})`) must therefore claim slots covering BOTH raw values (e.g. `SnowflakeNodeID: 0` → nodes slot 0, rels slot 1 → `BaseSlot: 0, SlotCount: 2`). See `docs/adr/0007-slot-sharded-store.md`.

## [4.15.2] - 2026-07-13

- Fix a data race between `Graph.Close` and a concurrent transaction/import rollback: the registry POINTER swap sites (`GraphTx.restoreRegistries`, import rollback, import-merge rollback) held only the exclusive `c.mu`, which excludes normal doors (`c.mu.RLock`) but NOT the two readers that run outside `c.mu` — `Close`'s final `persistRegistries` (after `Close` has released `c.mu`) and the ingest declare-on-prepare path (`registryMu` only). Caught by `TestLifecycleStormCloseMidFlight` under the full-suite race detector on the v4.15.1 release CI (a slow runner hits the window a fast dev machine never opened in 36 attempts). Every swap site now ALSO holds `registryMu`, and `Close`'s persist takes `registryMu` — one guard class covers all readers outside `c.mu`; lock order `c.mu → registryMu` unchanged, no callee re-entry (audited). NOTE: the `v4.15.1` tag's CI was red with this race; use `v4.15.2`. See `tasks`-internal lesson 66 (full-suite `make test-race` is the pre-release gate, never a targeted subset).

## [4.15.1] - 2026-07-13

- Ingest write-path performance round (profile-driven): the mutex profile at 8 concurrent sessions showed ~94% of all lock wait inside the badger batch-put door's `idxMu` critical section, dominated by two per-node heavyweights that never needed the lock. BOTH MOVED OUT: (1) `freezeNodeCopy`/`freezeRelCopy` (the full defensive deep copy for the entity cache) and (2) the change-log `ChangeNodePut`/`ChangeRelPut` payload encode (an entire second msgpack pass) now run in the pre-serialize phase OUTSIDE `idxMu` in both batch doors — only the order-sensitive appends (cache put, index maps, ops, record buffering) stay under the lock. Semantics unchanged (both are pure functions of the caller-owned finalized entity; record/op ordering still snapshots under one lock window).

- Producer-side change-log payload pre-encode (ADR-0006 §4.5 applied to the log body) — closes the change-log throughput drop. Structural enabler: for a CREATE, `NodePutBody.WithHistory` is omitempty-false, so the nested untokenized v2 wire is the LAST content of the payload and its fixed-width temporal tail is TERMINAL in the payload bytes — `storeutil.PatchWireTemporalTail` patches the whole payload directly, no record-format change, and the crown property `Patch(PreEncodePayload(E,0),T) == NodePutPayload(E@T)` holds byte-for-byte (proven over randomized nodes × boundary stamps incl. `MaxInt64`, plus truncation fail-closed). New `storeutil.PreEncodeNodePutPayloadV2` (CREATE-only by contract); ingest sessions build `pendingNode.logBody` on the PRODUCER thread whenever the change-log is recording; both apply modes patch it with the stamped TxFrom under the same token-equality validity gate as the entity-row buffer (both stale or both valid; per-element nil falls back to encode-at-door, byte-identical); new optional `store.PreEncodedPutLogCapability` (`PutNodesBatchPreEncodedLog`) on badger uses a non-nil payload VERBATIM as the record body — cross-backend feed parity and replica byte-exact convergence are unaffected (the crown convergence tests now exercise this path). MEASURED (Apple M4 Max, badger-in-memory, 10k node-creates, 8 sessions, `-count=3` medians): concurrent-mode change-log penalty drops from ~2.2–2.6x to ~1.12x — log-on ~10.2ms/10k ≈ 983k inserts/s vs log-off ~9.1ms ≈ 1.10M/s; base (log-off) concurrent p8 improves ~7% from the idxMu eviction (9.6ms → ~9.0ms). The strong-mode pipeline improves too (~34.3ms → ~29.5ms log-on) but keeps its serial scope-commit cost — the CONCURRENT door is the recommended throughput path even with replication enabled. New bench `BenchmarkIngestConcurrentChangeLog`.

## [4.15.0] - 2026-07-13

- Composite-index introspection for query planners — `g.Index().HasComposite(label, keys) (bool, error)` and `g.Index().ListComposites(label) ([][]string, error)`. `ListComposites` returns the DECLARED, order-preserving key tuple of every composite definition on the label (distinct orderings of the same key set are distinct definitions and are both listed; caller-owned copies; unregistered labels return an empty slice). `HasComposite` is ORDER-INSENSITIVE key-SET matching — exactly the rule the `NodesByLabelAndProperties` query door uses to decide index-vs-label-scan — so a planner can prove the accelerated path exists BEFORE routing a multi-property equality match through it (without the proof, routing blindly regresses the common single-key-index case, O(first-key matches), to a label scan + post-filter). Probe key sets are deliberately laxer than the DDL validator: any non-empty set is answerable (sizes no definition can have answer false); individual keys still reject shadow keys. O(definitions on the label) per call; there is NO index-DDL epoch/invalidation signal — call per query plan rather than caching across DDL you do not control. Backed by the new optional `store.CompositeIndexIntrospectionCapability` (`ListCompositePropertyIndexes(labelToken)`) on the native memory + badger stores — a SEPARATE interface so existing out-of-tree `CompositePropertyIndexCapability` implementations stay source-compatible; tiered and wrappers decline with `ErrCapabilityNotSupported`. Definitions were already persisted (badger), so a reopened directory answers identically (tested).

- Exact O(1) per-(label, property key) type-class cardinalities — `g.Stats().PropertyTypeClassCounts(label, key) (store.PropertyTypeClassCounts, error)` returning the EXACT partition `{Numeric, NaN, String, Bool, Other, Missing}` of the label's current nodes by the type class of the key's value. The classification rule is `types.PropertyTypeClass` (new, with `Node.ForEachPropertyTypeClass` + Relationship mirror — only key and class are exposed, never the value): Numeric = every int/uint kind + finite floats AND ±Inf (orderable); NaN split out (numeric kind, unorderable); Other = slices (incl. `[]float32`/`[]byte`), maps, registered structs; Missing = nodes carrying the label WITHOUT the key, computed graph-side as `NodeCountByLabel − present`. This closes the documented conflation in `NodeCountByLabelAndPropertyKey`, which counts INDEXABLE SCALAR values only — the probe (4 nodes: int, string, `[]int64`, missing) answers 2 there, making "total − having" mix missing props (sort LAST ascending) with slice values (sort FIRST) — while the partition separates all four exactly. EXACTNESS IS A CORRECTNESS GUARANTEE, not a planner estimate: the counters are adjusted inside the SAME store choke point (`adjustNodePropertyKeyCounts`) every node-mutation door already funnels through for the presence counter and the NDV accumulator — same call, full-property sweep — so the three capabilities' lifecycles cannot drift; badger rebuilds them in the same `loadIndexes` pass at open (reopen tested); tiered folds exact per-shard counters across ref + archive + event shards (the same shard walk as the presence counter). Intended consumers: ordering-soundness gates on the value-ordered top-k fast path — "every present value is orderable-numeric" is `Numeric == Present()`, "the gap is nulls only" is `Present() == Numeric` with `Missing` free — replacing an O(distinct-values) `RangeCardinality(-inf,+inf)` probe with an O(1) read. New optional `store.NodePropertyTypeClassCountsCapability`; memory, badger, and tiered all implement it.

## [4.14.0] - 2026-07-12

- Concurrent ingest mode (ADR-0006 §14 "concurrent mode" — the Lanes:N write door). `IngestOptions.Concurrent: true` switches a session from the prepare-parallel / apply-sequential pipeline (one strong-mode applier, each group exclusive-locked and atomic against readers) to a SELF-APPLYING session: `Submit` applies the group on the CALLER thread under the STANDALONE concurrency discipline — `c.mu.RLock` + 256-shard entity locks + unique value stripes, the same locks any number of concurrent standalone mutations already use — so N sessions apply genuinely in parallel with no applier handoff. Every mutation flows through the SAME internals the standalone doors and the strong-mode batch use (`putGeneratedNodesBatch(PreEncoded)` for creates — including the §4.5 pre-encoded-buffer fast path, always valid here because concurrent sessions DECLARE-ON-PREPARE: an unseen label/rel-type is registered and persisted during `AddNode`/`AddRelationship`, so queued tokens are always real and there is no probe-restamp step — the shared relationship create kernel with ordered `LockTwo` endpoint locks, `update*Internal`, `delete*Internal`); no second write path. Unconstrained groups take ONE batched store door; graphs with unique constraints fall back to per-node creates under their value stripes (same-value storms across sessions still resolve to exactly one winner). Change-log records emit EAGERLY per store door — record + LSN + data staged under one store-write-mutex window (the per-tx scope is exclusive-lock-only machinery, documented on `store.TxChangeLogScope`) — so the feed stays gapless and a tailing replica converges BYTE-EXACT from an N-session concurrent primary (crown test `TestIngestConcurrentReplicaByteExact`). Semantics trade-offs (deliberate, per §14): `Submit` is always synchronous and returns the group's apply outcome directly (`Sync` implied; the returned token carries a nonzero per-session `Lane` and `WaitApplied` on it is already resolved); a group is NOT atomic against concurrent readers (per-entity atomicity only); cross-session `TxFrom` stamps are only ±ε ordered (per-entity monotonicity holds via entity locks); events publish after apply (collected under the read lock, dispatched after unlock); `QueueBound` is ignored (no queue — backpressure is the caller's own apply). MEASURED (Apple M4 Max, badger-in-memory, 10k node-creates, `-count=3` medians, same-run baselines): concurrent 8-session ~9.6ms/10k ≈ 1.04M inserts/s vs strong-mode 8-producer ~15.3ms ≈ 653k/s (~1.6x) vs same-run single-threaded standalone-Add ~610k/s (~1.7x) vs concurrent 1-session ~19.5ms (~2x p1→p8 scaling; strong-mode p8 gains nothing over p1, being applier-bound); memory 8-session ≈ 1.12M/s. Single-session throughput does not regress (slightly better than the strong pipeline — no handoff). The remaining ceiling is store-side (`wbMu` staging + `idxMu` index maintenance), the ADR-predicted next lever. Design guidance: on fsync-bound DURABLE ingest (`SyncWrites`) the strong-mode single applier remains the fsync-amortizing choice (one group flush instead of N sessions' door flushes); concurrent mode is the fast-storage / in-memory scaling door. Bench `BenchmarkIngestConcurrent` beside `BenchmarkIngestPipeline`.

- Ingest apply-side consumption of the pre-encoded v2 wire (ADR-0006 §4.5 Scenario B) — the prepare-side pre-encode is no longer inert. New OPTIONAL `store.PreEncodedPutCapability` (`PutNodesBatchPreEncoded(nodes, wireBodies)`) on the native memory + badger stores (tiered/wrappers decline; routed for badger only, since memory holds live objects and never serializes a row). On the ingest path ONLY (`BatchBuilder.preEncode`, set by `Ingest.NewSession` when the store is native badger), each producer session pre-encodes its node-create rows on its own thread — `pendingNode.wireBody = storeutil.PreEncodeNodeWireV2WithKeys(node, c.propKeys)`, the v2 entity-row wire with a ZERO transaction-time tail and property keys TOKENIZED via the SAME shared registry the store marshals with (new `PreEncode{Node,Rel}WireV2WithKeys` primitives, the tokenized counterpart of the untokenized `PreEncode{Node,Rel}WireV2` that matches the change-log put body). The single applier (`Batch.Execute`), after stamping TxFrom, `PatchWireTemporalTail`s each buffer and hands it to `PutNodesBatchPreEncoded`, skipping the second msgpack pass. CONSERVATIVE validity gate (ADR §8 Risk-2 silent-wrong-answer class): a buffer is used ONLY when the finalized label tokens equal the queued tokens it was encoded with — for a genesis create the label token is the sole field that can diverge between prepare and apply (a probe token re-stamped to a different real token, §4.4) — else (or on a patch failure, or a store that declines) the applier leaves `wireBodies[i]` nil and the store re-encodes that row, byte-identical by construction. Provenance is by the typed in-process buffer, NEVER by sniffing stored bytes (`HasWireTemporalTail` stays a debug helper). Plain `g.Batch()` never sets `preEncode` and pays ZERO new hot-path cost. The change-log put body stays UNTOKENIZED encode-at-flush on both paths, so cross-backend change-feed parity is byte-identical. Threat model (docs/persistence.md): the transaction-time tail is deliberately OUTSIDE the content hash (TxFrom/TxTo were never hashed), so a patched buffer replicates verbatim and passes `Verify*Chain` — the corollary is that tail integrity rests on transport/storage integrity, not the entity hash. Divergence battery: store-level byte-identity of persisted rows AND change feeds (`PutNodesBatch` vs `PutNodesBatchPreEncoded`, plus the nil-fallback case) over a node-shape battery, the tokenized crown property `Patch(PreEncodeWithKeys(E,0),T) == MarshalWithKeys(Eₜ)` over 400 randomized nodes/rels, and core-level end-to-end equivalence of capability-on vs a capability-disabled decorator over the same workload — including undeclared-label probe-restamp cases proving the fallback fires and the persisted row carries REAL tokens. MEASURED (Apple M4 Max, badger-in-memory, `-count>=5` medians): the wiring lifts the 8-producer pipeline node-create from ~543k/s (encode-at-flush base) to ~746k/s — a real ~1.37x from moving the entity-row encode off the serial applier onto the parallel producers. The Scenario-B `2x`-vs-single-standalone-Add bar is NOT met on badger-in-memory (single-Add is itself async-buffered at ~621k/s → 1.20x): the applier remains serial and the CPU profile shows ~1.6/16 cores busy, dominated by the single-applier `c.mu.Lock`/`txMu` + `idxMu` index/cache maintenance and the producer↔applier handoff — exactly the §4.5 prediction that the ceiling moves to idxMu index/cache maintenance (the next lever is Lanes:N, which the format already anticipates). Change-log on/off ratio (pipeline p8 badger) stays ~2.6x because the change-log body encode was deliberately kept on the serial applier for byte-identical feed parity. No regression on the non-pipeline paths (plain standalone-Add / plain `Batch.Execute` time at parity; a small transient `pendingNode`-struct-field allocation increase, freed post-Execute). See `docs/adr/0006-ingest-architecture.md` §4.5, `docs/persistence.md`, and the CLAUDE.md "Ingest Pipeline" section.

- Wire format v2 — patchable transaction-time slot (ADR-0006 §4.5, ingest stage-12b groundwork). `storeutil.CurrentWireFormatVersion` bumped 1 → 2 (encoders + decode + the badger `wire_format_version` marker move together per lesson 39). In v2 the transaction-time tail (`tf`/`tt` — TxFrom/TxTo) changes from omitempty mid-map fields to a FIXED-WIDTH, always-present TRAILING slot (the last two map entries, full-width int64 via msgpack's 9-byte `0xd3` form, which the encoder already emitted for present values), so a zero-in-prepare value occupies byte-for-byte the space the ingest applier later patches. New `storeutil` primitives: `PatchWireTemporalTail(buf, txFrom, txTo)` (in-place, marker-validated, fail-closed with `store.ErrCorruptWire` on a truncated or non-v2 buffer — a mis-fed buffer is never silently overwritten), `HasWireTemporalTail`, and `PreEncodeNodeWireV2`/`PreEncodeRelWireV2` (encode with a zero tail). Node and Relationship move in lockstep (structural mirrors); the five fixed-vector hash anchors are untouched (TxFrom/TxTo are not hashed). BACKWARD COMPATIBLE: decoders accept BOTH v1 (omitempty tail, absent `fv` = legacy) and v2 (fixed tail) transparently — msgpack map keys are self-describing, so no read-side version branch is needed; the `fv` guard still fails a FUTURE version closed with `ErrWireFormatVersionUnsupported`, and the badger marker (driven by the single bumped constant) reopens an old dir read-write and restamps 2. The ingest prepared-intent record (`IntentRecord`) now carries `wireBody` — the create payload pre-encoded on the PRODUCER THREAD with a zero temporal tail (§4.1 "wire encode MINUS temporal tail"), applying the queued temporal metadata (tf/tt zeroed) so the buffer matches the flush-path encode of the finalized node modulo the tail the single applier patches; equivalence is proven byte-for-byte (the CROWN property: `Patch(PreEncode(E, 0), T) == Encode(E, T)`) over 400 randomized nodes + 400 rels at the wire level and node/rel at the intent level, plus v1 golden-fixture decode-under-v2, mixed v1+v2 decode, and future-version fail-closed. Apply-side consumption of this pre-encoded buffer is WIRED in the same release (next bullet). See `docs/adr/0006-ingest-architecture.md` §4.5 and `docs/persistence.md`.

- Ingest pipeline (ADR-0006 stage 1, Lanes:1) — a new `g.Ingest()` sub-API implementing a prepare-parallel / apply-sequential write door for insert-dominated throughput, beside the interactive `g.Tx()` door on the same core. Producer sessions (`g.Ingest().NewSession(IngestOptions)`) validate, build property slices, precompute content hashes, and mint snowflake IDs ON THE CALLER THREAD — fully parallel across sessions (each holds its own builder; shared registry lookups are RLock-only and the ID generator is mutex-guarded). A single applier goroutine (lazily started on the first session, drained and stopped at `Close`) consumes prepared intents in COMMIT GROUPS and applies each group through the existing batch machinery (`Batch.Execute`) — one `c.txMu` + `c.mu.Lock` acquisition (Lanes:1 strong mode: the whole group is atomic against concurrent readers, and the pipeline serializes against the interactive door at GROUP granularity, not per insert), one `TxFrom` stamp from the shared monotonic clock, one co-committed change-log LSN run, one buffered `PublishBatch`, one `flush()` per group. The applier reuses the replica-apply-shaped store doors rather than building a second write path ("replica apply, but as the primary"). `IngestOptions{Sync, DeclareLabels, DeclareRelTypes, QueueBound}`: `Sync` selects the §4.6 freshness contract — a sync `Submit` blocks until the group is applied AND visible (ack ⇒ read-your-writes on any goroutine), an async `Submit` returns a `SubmitToken{Lane, Seq}` and a reader achieves read-your-writes via `g.Ingest().WaitApplied(token)` / `AppliedSeq()`; `DeclareLabels`/`DeclareRelTypes` pre-register vocabulary so prepare is lookup-only (undeclared names fall back to probe tokens re-stamped at apply, safe because the content hash keys on label STRINGS); `QueueBound` bounds the prepare→apply queue and BLOCKS the producer at the bound (synchronous stall — entity writes are never dropped). Enqueue↔`Close` linearization (C1): an enqueue and shutdown share ONE fence (the seq-order mutex), so a `Submit` racing `Close` is EITHER applied (a sync ack / a truthfully-waitable async token) OR rejected cleanly with `ErrIngestClosed` (re-exported as `graph.ErrIngestClosed`) — never accepted-then-dropped, never hung; the applier-startup path is guarded (`ingestClosing`) so a session racing `Close` cannot orphan a fresh applier behind the shutdown sweep. `WaitApplied` is the async FAILURE truth channel (C2): `AppliedSeq() ≥ token.Seq` means the group was PROCESSED (advanced even for a rejected intent so it never wedges later waiters), NOT necessarily committed — `WaitApplied(token)` returns nil on success or the intent's real apply error (e.g. `ErrUniqueViolation`) if it was REJECTED, pruned on read (a per-token failure record retained until the first `WaitApplied` or `Close`, capped so a flood of never-read async failures stays bounded); a sync `Submit` returns its OWN group's error even when a sibling group in the coalesced batch failed. The day-one serializable `IntentRecord` format (`(epoch, lane, seq)` ordering header + half-edge decomposition fields + prepared node/rel wire payload) with a msgpack codec (`EncodeIntent`/`DecodeIntent`, fail-closed with `ErrCorruptWire` at the trust boundary) is included so Lanes:N and the stage-3 distributed topology are a configuration change, not a wire-format break — inert in-process at Lanes:1 (lane 0, seq tracks the applier commit / LSN order). Correctness: pipeline-written graphs pass `Verify*Chain`, carry correct bitemporal metadata (as-of reads reflect the belief state at a pin — two-phase battery), mint gapless LSNs via the existing co-commit, enforce unique constraints identically (concurrent same-value storm → exactly one winner), and replicate BYTE-EXACT to a read-only replica (the crown acceptance test reuses the replica-convergence harness with a pipeline-fed source). Bench (`bench/ingest_pipeline_test.go`): on a durable badger dir with `SyncWrites`, the 8-producer pipeline group-commit is ~8.9x the single-threaded standalone-Add rate (~273k vs ~31k node inserts/s) — the fsync-amortization win; on badger-in-memory the pipeline is applier/encode-bound at the Scenario-A ceiling (~600k/s, matching the feasibility map — the ~2x-over-standalone throughput on a fast backend is a Scenario-B property that the wire-slot change unlocks in a later stage, not this one). Change-log adds ~2.25x per-op apply cost on badger under group commit (the second msgpack encode per node, Scenario A) and ~0 on memory. See `docs/adr/0006-ingest-architecture.md`, `docs/api.md` "Ingest pipeline", and the CLAUDE.md "Ingest Pipeline" section.

## [4.13.0] - 2026-07-11

- `events.AsyncEventBusConfig.OnDrop func(Event)` reports events shed by `BackpressureDropOldest`/`BackpressureDropLatest`, which previously dropped silently with no hook, counter, or depth accessor. DropOldest hands the callback the evicted queue-head event; DropLatest hands it the rejected newcomer. Nil by default (zero cost); when set, always invoked outside any `AsyncEventBus` lock, so the callback may safely re-enter the bus (Publish, PublishBatch, QueueDepth, Subscribe, Close). Added `AsyncEventBus.QueueDepth() int` (total buffered events) and `QueueDepths() [5]int` (per-priority breakdown), both snapshot semantics.
- `graph.DistanceCosine` / `graph.DistanceEuclidean` re-export the `store.DistanceMetric` constant values next to the existing `graph.DistanceMetric` alias, so a consumer avoiding a direct `pkg/graph/store` import can call `g.Index().CreateVector`/`CreateVectorWithOptions`/`SearchNearest` with graph-qualified constants only.

- K3a — a CONTRACTUAL ordered-scan / top-k access path. New streaming door `g.Nodes().ForEachByLabelPropertyRangeOrdered(label, propKey, min, max, inclMin, inclMax, desc, opts, fn)` emits the label's numeric-`propKey` nodes in VALUE ORDER — ascending or descending — with ties always broken by node ID ASCENDING in both directions, so a query layer can serve `ORDER BY n.p [ASC|DESC] LIMIT k` from the index instead of materialize-and-sort. `fn` returning `false` stops the scan at the index level, pushing the LIMIT down: a top-k costs O(k + log n) index work and materializes only k rows. Previously the ordered numeric view iterated value-ascending internally but the only range door (`ForEachByLabelPropertyRange`) RE-SORTED its results by snowflake ID (destroying value order) and the memory store did not implement the range scanner at all — so `ORDER BY prop LIMIT k` could never be index-served. This closes both gaps. Implemented as a paged, fully-lazy scan over the in-RAM ordered view (`PropertyIndex.RangeOrderedPage`, a resumable value-cursor collector — new `orderedKeys.forEachDownFrom` reverse walk) on BOTH the memory and badger backends (closing the pre-existing memory parity gap), and over the persisted `0x0A` keyspace under `PropertyIndexOnDisk` (a value-ordered candidate-ID collection — pending-overlay merged — then streamed node materialization with the same `fn`-driven early stop, so the expensive per-node decode stays bounded by what `fn` consumes). Same over-select contract as the unordered door: bounds are ulp-widened and boundary buckets are never skipped (int64 magnitudes past 2^53 collapse onto neighbouring float64 sort keys), so `fn` re-checks the exact predicate and the `inclMin`/`inclMax` inclusivity. CURRENT-STATE ONLY in v1: any temporal `QueryOpts` (`ValidAt`/`ValidStart`/`ValidEnd`/`TxAt`/`TxPin`) is DECLINED with the new `graph.ErrOrderedScanTemporal` sentinel rather than silently answered against current state (the value ordering is derived from the valid-time-agnostic property index); tiered and store wrappers decline with `ErrIndexNotFound` (not an exact native store). Bench (`bench/ordered_topk_test.go`, top-10 by value over 100k distinct values): the ordered arm vs the pre-K3a collect-then-limit shape (full label scan sorted by value, truncated to k) — memory ~17 µs vs ~354 ms (~20,000x), badger ~45 µs vs ~575 ms (~13,000x). Tests: exact value-order contract (asc + desc, ties, negative floats, mixed int64/float64 magnitude buckets) with three-way memory / badger-RAM / badger-disk(0x0A) equivalence over randomized data, a LIMIT-pushdown counter proof (k=10 over a 20k-value range invokes `fn` exactly k times, not O(n)), direct per-store tests (memory + badger, disk mode pre- AND post-flush overlay), the decline sentinels via `errors.Is`, and an index-level paged-cursor model-equivalence battery. See `docs/query-planners.md` "Ordered / top-k range scan".

- K3b — RELATIONSHIP property indexes: the relationship mirror of the node property-index family, keyed by rel-type token instead of label token (previously the property index was node-only — `PropertyIndexKey{LabelToken, PropertyKey}` and all machinery `*types.Node`-typed, so a `rel.weight` filter scanned by type + decoded + filtered). New OPTIONAL store capability `store.RelPropertyIndexCapability` (`CreateRelPropertyIndex`/`DropRelPropertyIndex`/`RelationshipsByTypeAndProperty`) plus the range scanner `ForEachRelByTypePropertyRange`, surfaced as `g.Index().CreateRelProperty(typeName, propertyKey)` / `DeleteRelProperty`, `g.Rels().ByTypeAndProperty(typeName, key, value, opts)`, and `g.Rels().ForEachByTypePropertyRange(...)`. The value store reuses the ID-generic `internal/index.PropertyIndex` (new `RelPropertyIndexKey` + `RelIDs`/`RangeRelIDs` typed accessors + `AddRelToPropertyIndexes`/`RemoveRelFromPropertyIndexes`/`PurgeRelFromAllPropertyIndexes`), so the ordered-numeric-view range machinery is shared verbatim with the node index (Node/Rel parity). Maintained at every relationship mutation door: the create path (`PutRelationship` / memory `PutRelationshipGeneratedIDWithEndpointHashes`), `ReplaceRelationship`, `ReplaceRelWithHistory`, the single shared delete seam (`deleteRelByInfo` in badger / `deleteRelLocked` in memory — covering standalone delete, delete-with-history, node-cascade delete, and the batch delete path via brute-force purge-by-ID since the seam carries no property values), and the batch create path (`PutRelationshipsBatch`). Create is the 3-phase protocol (install empty live index + snapshot type members under lock → prefetch relationship data unlocked → merge, skipping IDs a concurrent write already handled and rels deleted meanwhile) with backfill from existing relationships. RAM-only v1 on memory and badger: badger persists only the DEFINITIONS (`meta/rel_prop_indexes`) and rebuilds the value maps from current relationship state at open — there is no on-disk value keyspace (a `0x0C` rel keyspace disk mode mirroring the node `0x0A` `PropertyIndexOnDisk` mode is a documented follow-up). The multi-shard tiered store DECLINES index CREATION with the new `store.ErrRelPropertyIndexUnsupported` sentinel (relationships route to event shards by timestamp, so a shard-local rel-value index cannot answer a query whose matches are scattered across every shard) while its `RelationshipsByTypeAndProperty` still answers correctly via a cross-shard type-scan + property filter; the graph-layer `ByTypeAndProperty` also transparently falls back to a type-scan + filter on any backend that lacks the capability, so the query is correct everywhere and only ACCELERATION is optional. Temporal opts on `ByTypeAndProperty` fold current + history versions through the same single chain resolver as the named doors (a relationship whose type and property held at the pinned time is included even if a later version no longer matches). `store.ErrRelPropertyIndexUnsupported` is canonical in `pkg/graph/store`, re-exported from `pkg/graph`, and added to the sentinel anti-drift identity test; `RelPropertyIndexCapability` joins the `IndexAccelerationFacet` composition and the `CapabilitiesOf` structural report. Full parity test battery (creation backfill, equality exact-set with negative/type-safe/phantom assertions, numeric range with over-select + exact recheck, mutation maintenance including Update/UpdateInPlace/CompareAndSetProperty/delete/cascade-delete, drop, badger reopen rebuild, 3-phase concurrent-write visibility, corruption-path purge cleanup, cross-shard tiered decline, and a two-phase temporal query) across memory + badger, plus a concurrent read/write race test; new `bench/rel_property_lookup_test.go` (10k relationships, selective weight) shows ~90x (memory) / ~43x (badger) over the scan path. The bitemporal oracle harness stays green.

- K3c — composite node property indexes. `g.Index().CreateComposite(label, keys)` builds an EQUALITY-only index over an ordered tuple of 2-4 declared property keys under one label (`g.Index().DeleteComposite` drops it); `g.Nodes().ByLabelAndProperties(label, values, opts)` is the query door, matching every `(key, value)` pair in `values` (AND-conjunction). Closes the gap where two equality predicates on the same label forced a caller to pick one single-key index and post-filter the rest — costly when the FIRST key is unselective but the FULL predicate is selective (see `bench/composite_index_test.go`'s `BenchmarkCompositeLookupVsSingleIndexPlusFilter`, ~30-38x faster on a 100k-node fixture). New optional `store.CompositePropertyIndexCapability` (NOT embedded in `Store` — a backend may omit it entirely and the graph layer's mandatory label-scan + post-filter fallback still answers correctly, unaccelerated); implemented on `memory.Store` and `badger.Store` (3-phase creation with concurrent-write-safe backfill on badger, mirroring the single-key property index; definitions persist and rebuild on reopen; entries are RAM-only in v1, no on-disk mode). Index identity is `(labelToken, DECLARED KEY ORDER)` — a different key order for the same key set is a distinct definition; the query's key SET (order-independent, since `values` is a map) selects which definition accelerates it. Composite entry/definition keys use a length-prefixed, injective concatenation (`indexpkg.EncodeCompositeKeyTuple`) so no two distinct ordered key lists can ever alias onto the same map slot (a naive plain-concatenation or single-separator join can — see the collision battery). Floats are supported using the same lesson-25 bit-pattern equality semantics the single-key property index already applies (chosen over the unique-constraint precedent of rejecting floats, since this is an equality accelerator, not a business-identity constraint). Maintenance shares badger's existing `maintainPropertyIndexesAdd/Remove/Purge` seam (every node-mutation door already funnels through it) rather than touching each door individually; memory's mutation doors gained a matching one-line call at each of the 18 existing single-key-index call sites. `tiered.Store` does not implement the capability in v1 (documented follow-up, same shape as `PropertyIndexOnDisk`/`ErrEventPropertyIndex` reference-label scoping) — `CreateComposite` on a tiered-backed graph returns `ErrCapabilityNotSupported`, but `ByLabelAndProperties` still answers correctly via the mandatory fallback. See `docs/query-planners.md` "Composite property indexes" for planner guidance, scope, and the mandatory-fallback contract.

- Error-surface documentation housekeeping (three consumer-reported gaps, all doc/alias only — no behavior change). (1) `graph.ErrBackupExists` was the only `pkg/graph` re-export missing from `docs/errors.md` and its machine-check inventory in `errors_doc_test.go`; added the doc row and inventory entry, plus a new `TestGraphErrorsFileInventoryComplete` that parses `pkg/graph/errors.go` with `go/parser` and asserts every exported `Err*` declaration is present in the inventory `TestErrorsDocumentation` checks against the docs — closing the gap class so a future re-export can no longer land undocumented. (2) `store.ErrNoVersionValidAt` used to leak raw from `g.Temporal().NodeAt`/`RelAt`/`NodeAtTx`/`RelAtTx` with no `pkg/graph`-qualified alias, so a consumer that avoids importing `pkg/graph/store` directly could not `errors.Is` it; added `graph.ErrNoVersionValidAt` as an identity-preserving alias (the `ErrNodeExists` precedent), added it to the sentinel-identity test, and moved its docs/errors.md row out of "Store-Internal Sentinels" into the re-exported temporal section. (3) Fixed a stale facade comment in `pkg/graph/stats/api.go` claiming the tiered backend lacks `NodePropertyStatsCapability` — tiered has implemented the capability (cross-shard HyperLogLog NDV merge + min/max fold) since the tiered-parity wave; also fixed the same stale claim in two spots `docs/query-planners.md` had been missed by that wave (the summary table's tiered-complexity cell and a paragraph in "The capability story for external stores").

- K1 — pinned label/type scans are now OUTPUT-SENSITIVE. A history-aware `ByLabel`/`ByType` scan (one carrying a temporal filter: `TxPin`, `TxAt`, `ValidAt`, or a `ValidStart`/`ValidEnd` interval) previously folded ALL node/relationship history into its candidate set, so a selective pinned scan cost O(everything that ever carried ANY label) instead of O(matches). Two new OPTIONAL store capabilities — `store.LabelTxMembershipCapability` (`ForEachLabelTxMember`) and its relationship mirror `store.RelTypeTxMembershipCapability` (`ForEachRelTypeTxMember`) — maintain a per-token transaction-time membership sidecar (label/type token → the node/rel IDs that EVER carried it, each tagged with a lower bound on the earliest-acquisition transaction time). The core candidate collection (`nodesByLabelLocked`/`relsByTypeLocked`) scopes the fold to the label/type's ever-members and prunes any member whose earliest acquisition post-dates the pin, WITHOUT loading its version chain. The sidecar is a SOUND SUPERSET (append-only: a removed label / deleted entity is retained so a pin before the change still admits it) and the single chain resolver stays the correctness authority, so an over-included candidate is rejected there — never mis-reported. Implemented on the memory and badger backends (RAM sidecar, lazily built on the first pinned scan from the current + history keyspaces plus the pending write-buffer overlay, then maintained incrementally at every label-acquisition door — create, backfilled create, label add, batch, history-version insert; removal/delete are no-ops; reset by `Clear` and rebuilt after reopen, mirroring the OPT15 `relValidIdx` precedent); the multi-shard tiered store and any wrapper decline the capability and take the correct (unaccelerated) full-history fold. Measured on the committed `bench/pinned_scan_test.go` fixture (badger, 100k entities): a selective `ByLabel{TxPin}` at the V5D churn profile improved from ~2.07 s / 18.2M allocs to ~9.6 ms / 140k allocs (~216x), and the fixed-N selective/broad cost ratio collapsed from ~0.6-1.0 to selectivity-proportional (~0.004). The write path is unaffected when no pinned scan runs (the sidecar stays nil — a single nil-guard per label). Correctness net: a pruned-vs-unpruned divergence battery over randomized graphs on both backends, a two-phase stamp-maintenance battery across every label door (including backfilled creates and label re-add after removal), a cross-check against the independent `NodesAsOf` belief-state path, and store-level rebuild-on-reopen / pending-overlay / rel-type tests; the full bitemporal oracle harness stays green.

- Belief-state pinned adjacency — new `g.Rels().OutgoingForNodesAtPin(nodeIDs, type, pin)` / `IncomingForNodesAtPin(...)` doors resolve a batch of seed nodes to their relationships AS THEY WERE BELIEVED at a transaction-time pin, with NO valid-time filtering, agreeing with `ByType(QueryOpts{TxPin: pin})` filtered by endpoint BY CONSTRUCTION (every candidate routes through the SAME as-of resolution the generic `TxPin` door uses — `findRelVersionForOpts`'s `TxPin` arm → `relAsOfLocked` → the chain resolver + `storeutil.SelectAsOf` — never a private re-implementation). This closes a consumer-verified semantics gap: the existing `OutgoingForNodesAtTx`/`IncomingForNodesAtTx` doors resolve each candidate through the `TxAt` arm, which applies a POINT valid-time probe at wall-now when no valid-time opts are set, so any edge whose valid interval lies wholly in the past — a `CloseVersion`-ed edge, or a width-1 `[t, t+1)` point-event edge (the standard point-event encoding) — was SILENTLY DROPPED, the same wall-now footgun class `QueryOpts.TxPin` fixed for the scan doors one layer up. The `*AtPin` doors return every edge believed at the pin regardless of valid time (past-valid facts, point events, and unset-`valid_from` snowflake-fallback edges alike); an edge hard-deleted after the pin is still visible (transaction-time tombstone), one created after the pin is invisible, and a backfilled edge (`AddWithTx`) is visible from its backfilled `TxFrom` onward. SEED TOLERANCE: unlike the current-state / `*AtTx` doors (which hard-error `ErrNodeNotFound` on a non-current seed), a seed that was part of the belief state at the pin but was HARD-DELETED afterwards is accepted — its live adjacency was purged by the cascade so it is excluded from the live-adjacency probe, but its pre-delete edges are recovered through the deleted-relationship fold (rel endpoints are immutable, so a cascade-deleted edge still names the deleted seed as its endpoint) and returned; a seed absent from the belief state at the pin (never created, or created only after it) contributes nothing and is skipped silently, matching `ByType{TxPin}` filtered by endpoint. `pin == 0` delegates to the plain `OutgoingForNodes`/`IncomingForNodes`. The `*AtTx` doc comments were corrected to state explicitly that those doors agree with the `TxAt`-pinned BITEMPORAL door (wall-now valid filter) and are NOT belief-state pins, pointing callers to the `*AtPin` doors for AS-OF-SYSTEM-TIME semantics. Distinguishing-input battery over randomized graphs on memory + badger (+ a tiered cross-shard case): equivalence to `ByType{TxPin}` filtered by endpoint with fixtures covering a past-valid CloseVersion-ed edge, a width-1 point event, an unset-`valid_from` edge, an edge hard-deleted after the pin, an edge created after the pin, a seed node hard-deleted after the pin, and a backfilled edge; plus a direct `*AtTx`-vs-`*AtPin` divergence proof and the never-existed-seed skip-silently contract. See `docs/query-planners.md` "Pinned adjacency".

- K4 — persist the maxTo-augmented temporal interval index's raw entries (badger, opt-in `Config.TemporalIndexOnDisk`), eliminating its rebuild-at-open cost at scale. The index itself always stays fully resident in RAM at runtime (its stabbing/interval queries walk an in-memory subMax-augmented implicit BST with no on-disk analogue) — this is a rebuild-at-open accelerator, not a RAM-vs-disk trade-off like `LabelIndexOnDisk`/`AdjacencyIndexOnDisk`/`PropertyIndexOnDisk`. Historically, reopening a store with an existing temporal index definition required a SECOND full node fetch+decode per entity (on top of the unconditional per-node decode `loadIndexesScan`'s primary pass already pays), purely to extract two `int64` fields. A new `0x0B/<2B labelToken>/<8B order-preserving from>/<8B nodeID>` keyspace (value = raw `to`) is maintained transactionally alongside the node row at every write site that already maintains the in-RAM index (`PutNode`/`DeleteNode`/`ReplaceNode`/`ReplaceNodeWithHistory`/the four label-token doors/`PutNodesBatch`/`DeleteNodesBatch`/the cascade corruption-path purge), plus `CreateTemporalIndex`'s own backfill and `DropTemporalIndex`'s prefix purge (safe unconditionally — a `TemporalIndex` definition is exclusively keyed by label token, unlike the property-index keyspace's cross-label-shared rows). The FROM component is order-preserving-encoded so a plain prefix iteration over one label's sub-keyspace already visits entries in the `(From ASC, ID ASC)` order the index's `Entries` slice requires — `loadIndexesScan` streams straight into `AddKnownAbsent`, no separate sort. An existing directory with pre-flag temporal-index definitions is backfilled from current node state exactly once, guarded by a `temporal_index_on_disk_built` meta marker (mirroring `property_index_on_disk_built`), committed atomically with the backfill rows. Passed through `graph.Config.TemporalIndexOnDisk` and `tiered.Config.TemporalIndexOnDisk` (per-shard, uniform with the other on-disk knobs) — the tiered store needs no shard-logic changes since `CreateTemporalIndex`/`DropTemporalIndex` already delegate per-shard to `badger.Store`. Measured on a 100k-entity/1-label fixture: ~1.8x faster open (795ms → 452ms) — the ratio reflects eliminating the redundant SECOND decode pass specifically, not node decode overall (the primary unconditional pass is shared baseline cost); a corpus with multiple temporal-indexed labels covering overlapping entities would see a larger win. See `docs/persistence.md` "Persistent temporal-index rebuild accelerator" and the CLAUDE.md Badger key-layout table (claims `0x0B`, previously free).

- Added `g.Temporal().NodeMatchesValidTime(n, opts)` — the NODE mirror of the existing `RelMatchesValidTime`, closing the Node/Relationship parity gap (rule 17). Same canonical predicate (`storeutil.MatchesTemporalFilter` over `storeutil.EntityValidFrom`/`MatchesPointInTime`: explicit `ValidFrom` else snowflake fallback, half-open `[from, to)`), so a query engine that already post-filters relationships by valid time can do the identical thing for nodes without hand-rolling the rule. Nil node or not-ready graph returns `false` (the safe "excluded" default); accepts frozen rows (any plural/scan read) identically to mutable ones.

## [4.12.1] - 2026-07-11

### Fixed
- `types.CoerceInstant`: the float64 upper-bound check used an inclusive
  comparison against `float64(math.MaxInt64)`, which rounds up to exactly 2^63
  — one past the largest int64 — so that value slipped through to a
  platform-defined float-to-int conversion (amd64 wraps to `MinInt64`, arm64
  saturates to `MaxInt64`). The bound is now strictly below 2^63, rejecting
  the value identically on every architecture; boundary cases (largest
  integral float64 below 2^63, exact `MinInt64`) are pinned by tests. Found by
  the public CI's amd64 runner diverging from arm64 development machines.

## [4.12.0] - 2026-07-11

- Store capability consolidation (ADR-0003) — internal refactor, ZERO public API change. Adds five composed capability facets to `pkg/graph/store` (`IntegrityAccelerationFacet`, `HistoryAccelerationFacet`, `IndexAccelerationFacet`, `ChangeLogFacet`, `MetadataFacet`) as pure compositions of the existing narrow capability interfaces — every existing interface name is unchanged, so Go structural typing keeps every current implementer (in-tree and external) satisfying them automatically; a compile-time test proves an old-style store implementing only the pre-facet method sets still satisfies `Store` and each facet. Adds `store.CapabilityReport` + `store.CapabilitiesOf(MandatoryStore) CapabilityReport`, a pure, side-effect-free introspection that probes each optional capability once (for diagnostics/tooling; it is a STRUCTURAL probe and deliberately does NOT reproduce the wrapper-visibility guard the graph layer applies to its own cached handles). Core-side: the 23 scattered inline `c.store.(MetaKVCapability)` / `(HistoryCompactionCapability)` asserts (as-of tags, graph epoch, bitemporal migration, unique constraints, unique-forever ownership, replication watermark/lease, compaction) are replaced by two handles (`c.metaKV`, `c.historyCompaction`) resolved once in `core.New` — byte-for-byte behavior-preserving (bare probes, immutable store, identical decline error at every site). The 13 wrapper-visibility-guarded acceleration handles were already resolved once in `New` and are unchanged. See `docs/adr/0003-capability-consolidation.md` (inventory regenerated in-branch: 34 store interfaces, 45 assertion sites, post-parity implementation matrix).
- Tiered change-log (op-log) — the tiered backend now provides a coherent store-global change feed (ADR-0005 §2), so `store.ChangeFeedCapability` / `ChangeLogStatusCapability` / `TxChangeLogScope` are implemented on `tiered.Store` and the core `changeFeedCapability` guard admits `*tiered.Store` into its native switch. This unlocks the full replication/CDC surface on tiered: `g.IO().ExportSince`/`Watermark`/`ImportMerge` (delta backups) and `g.Replication().ChangeFeed`/`ForEachChange`/`LastCommittedLSN`/replica bootstrap+tail all run on a tiered primary. Enabled by `tiered.Config.ChangeLog` (off by default; passed through `badgerCfg` to every shard). Design: a store-global `atomic.Uint64` LSN allocator (`changeLogAllocator`) is injected into each shard via the new additive `badger.Config.ChangeLogSeqSource` (nil keeps badger's self-owned counter — standalone behavior is byte-for-byte identical), so each shard co-commits its own records in its own `WriteBatch` (exactly as standalone badger) while LSNs form ONE total order across shards. Reseed at open reads a single monotonic `changelog_lsn_watermark` MetaKV key on the always-present reference shard (persisted after every log-bearing flush via the new `badger.Config.OnChangeLogFlush`, folding in the open ref/hot shards' own `LastLSNKey`s as belt-and-braces) — never a full-catalog cold-shard scan; an unreadable watermark FAILS CLOSED (fences the change-log with a sticky error, refuses to hand out LSNs so no mutation reuses an LSN; `RecoverChangeLog` clears the fence in place à la `RecoverBackgroundError`). The feed (`ForEachChange`/`ChangeFeed`) runs a FLUSH-BEFORE-READ durability barrier (`tiered.Store.Flush()` folds `shard.Flush()` over every open shard — also satisfying the core `storeFlusher` so Export flushes for free), captures `W = LastCommittedLSN` immediately after the barrier, and runs a W-BOUNDED paged k-way merge (min-heap over per-shard log heads, one shard checked out at a time) emitting ONLY records with `LSN <= W` — records allocated DURING the drain are deferred to the next poll. The barrier + W-bound together eliminate the cross-shard flush-reordering silent-loss the ADR's adversarial review identified (Finding-1): a lower LSN buffered on a slow shard is never skipped by a tail cursor that already passed a higher LSN durable elsewhere. A rotated/cold shard's immutable log segment is paged read-only (no flush on a closed shard); the store allocator is unaffected by rotation. `TxChangeLogScope` lives at the store level (one seam, `SetLogDivert`, marked for the scope-tagged-routing redesign) and buffers per shard, minting LSNs at commit so a rolled-back tx emits nothing and burns no LSN on any shard. A single-shard tiered cascade (`DeleteNodeCascade` where every connected relationship is fully local to the node's shard) delegates to the shard's own cascade so it emits ONE `ChangeNodeDelete` record (byte-identical to badger/memory); only a genuinely cross-shard neighborhood uses the per-relationship split-delete path (one `ChangeRelDelete` per edge). Tests: badger LSN-source injection parity, tiered reseed (reopen reads only the ref watermark; cold-was-hot-then-rotated reopen still seeds above the shard max; unreadable-watermark fail-closed sticky gate), the load-bearing adversarial reorder/W-bound test (slow-shard buffers a lower LSN, another shard flushes a higher LSN first → the barriered feed does NOT skip the lower one; the W-bound defers higher heads), cross-rotation and cold-shard read-only feed, a three-way memory/badger/tiered byte-identical changefeed parity battery over a single-shard fixture, and an end-to-end tiered-primary → change feed → replica-apply → byte-exact convergence check.

- Fixed: `tiered.Store.RecoverChangeLog()` only re-enabled the change-log on the reference shard, leaving an already-open hot/warm event shard's mutations silently absent from the feed after a poison/recover cycle — a shard that opens while the store-global allocator is poisoned never receives `Config.ChangeLogSeqSource` (`badgerCfg` gates that wiring on the allocator's poisoned state at open time), so the pre-fix `EnableChangeLog()` call was documented to stay inert on it ("a shard opened without it stays off"). New `badger.Store.EnableChangeLogWithSource(seq, onFlush)` retroactively wires the seq source (+ the watermark-flush hook for non-reference shards) into an already-open store and turns production on; `RecoverChangeLog` now calls it over every currently-open shard (reference, archive if open, and every open event shard) via the existing `forEachOpenShard` checkout discipline, closing the silent-feed-loss gap the fail-closed poison gate exists to prevent. A shard opened lazily AFTER recovery is unaffected and already wires up correctly via the normal `badgerCfg` path (verified by a new regression covering post-recovery rotation).

- Added `tiered.Config.PropertyIndexOnDisk` (ADR-0005 §3.3): the tiered-store sibling of `badger.Config.PropertyIndexOnDisk`, passed through `badgerCfg` to every shard (reference, hot, warm, lazy cold/archive, and rotation-created) for uniformity. Scope is UNCHANGED — property indexes remain reference-shard-only (`CreatePropertyIndex` still rejects event labels with `ErrEventPropertyIndex`); the flag only changes HOW the reference shard's badger.Store answers property-index reads (its persisted `0x0A` keyspace instead of its in-memory `PropertyIndex.Entries`/`numBuckets` maps), surviving reopen without an in-memory rebuild pass. Event shards never build a property index, so the flag is a harmless no-op there. Off by default (zero-value `Config` keeps every shard in RAM mode, unchanged behavior). New tests cover per-shard config pass-through (reference/hot/warm), reopen persistence through the reference shard's own badger.Store, a direct scan of the persisted `0x0A` keyspace, the event-label rejection staying in force, and flag-on/off result equivalence over an identical mutation sequence (puts, a property update, a delete).


- `tiered.Store` now implements `store.NodePropertyStatsCapability` (ADR-0005 §3.1): `g.Stats().PropertyStats(label, propertyKey)` folds NDV/min/max/count across refShard, refArchive, and every event shard, mirroring the checkout/checkin discipline of the presence-only `NodeCountByLabelAndPropertyKey` sibling. Count and Min/Max fold trivially (sum, min-of-mins/max-of-maxes via the new exported `index.CombineExtrema`), but NDV is a HyperLogLog *estimate* and estimates do not sum — a value present on two shards would be double-counted. The fix: a new store-internal (not public-contract) `NodePropertyStatsSketch` method on `badger.Store` and `memory.Store` exposes a CLONE of the raw per-`(label, propertyKey)` sketch (`PropertyStatsAccumulator.Sketch()`); the tiered fold register-max `Merge`s every shard's sketch into one combined sketch and calls `Estimate()` exactly once. `Merge`'s `ErrHLLPrecisionMismatch` is PROPAGATED, never discarded — every shard uses the same `index.DefaultHLLPrecision` so it cannot fire in practice, but silently swallowing it would silently under-count NDV. Tested: cross-shard NDV where the same value spans two shards (assert the merge counts it once, not the naive per-shard-estimate sum), Min/Max spanning a reference shard and an event shard via a shared secondary label, a ref+archive fold, a cold-shard fold (lazy reopen), the tiered arm added to `property_stats_parity_test.go` (now memory/badger/tiered three-way agreement), and doc-comment updates retiring the "tiered declines" language in `pkg/graph/store/property_stats.go`, `pkg/graph/internal/core/stats.go`, and `docs/query-planners.md`. See lesson 65 (HyperLogLog accuracy tests must feed well-distributed values, not short sequential integers — a test-authoring pitfall found while writing this fold's adversarial NDV test, unrelated to the fold itself).

- History compaction (`g.Admin().CompactHistoryNodes`/`CompactHistoryRels`) now runs on the tiered backend — `store.HistoryCompactionCapability` is implemented on `tiered.Store`, so the core type-assert succeeds and tiered no longer declines with `ErrCapabilityNotSupported` (ADR-0005 §3.2). Each entity's trim routes to its OWNING shard via the existing snowflake-timestamp router (`TruncateNodeHistory`/`TruncateRelHistory`, which already handle checkout/checkin, writable cold-shard reopen, and archived/deleted/multi-source chains); the per-entity stub is written to the GLOBAL reference shard (via the store-level `MetaSet`) because the graph layer reads it back through the store-level `MetaGet`, which tiered delegates to the reference shard — a stub written into an event shard's meta would be invisible. Trim and stub land on different shards so they cannot share one atomic batch: the stub is written BEFORE the trim so a crash between them fails closed (the point-door gate over-rejects with `ErrHistoryCompacted` on a still-intact chain, repaired by an idempotent re-run) rather than leaving a trimmed chain with no stub (a silently-incomplete read). The GLOBAL watermark (`compacted_through_tx`) is no longer bundled into every per-entity compaction batch — it is routed ONCE via the store-level `MetaSet` (core `advanceCompactionWatermark`), so on tiered it lands deterministically on the reference shard instead of scattering across whichever owning shards happened to be compacted (a subsequent reference-shard `MetaGet` would otherwise miss it, and a scan below compacted knowledge would not fail closed). The watermark is written BEFORE the trims (over-stated = fail-closed): the point-door fast gate skips the per-entity stub check whenever the pin is at/above the watermark, so an UNDER-stated watermark would turn a compacted entity's below-boundary point read into a silently-incomplete answer — over-statement only ever over-rejects (conservative), and because it captures the full run maximum up front, a crash-interrupted run's watermark stays correct while an idempotent re-run finishes the outstanding trims. memory/badger behavior is unchanged (the watermark already reached the run maximum on the first per-entity batch there; the per-entity trim+stub stay atomic in one WriteBatch). Tests (`compaction_tiered_test.go`): reference-shard and event-shard end-to-end (node + rel, two-phase reads, stub-aware verify, point+scan `ErrHistoryCompacted`), cross-shard (a reference node and an event node compacted in one call → both trimmed, exactly one global watermark, reopen durability with both stubs readable on the reference shard), a cold-shard entity (rotate + `demoteToCold`, trimmed via a writable cold-shard checkout), the watermark-on-reference-shard regression (event-only compaction still fails the scan door closed), protected-tag refusal, and tampered-stub fail-closed verify.

- Added DocValues (X5 columnar aggregation) support on the tiered backend (ADR-0005 §3.4): `ForEachDocValues` / `ForEachDocValuesMulti` / `DocValuesSnapshot` / `NodeMutationEpoch` now satisfy the core-internal `nodeDocValuesScanner` interface on `tiered.Store`, so `g.Nodes().ForEachDocValues` and friends activate the X5 columnar path on a tiered graph instead of silently falling back to per-node aggregation. Design: CONCATENATE-WITH-ORDINAL-OFFSETS, not fold-into-one-column — each shard (reference, reference archive, every event shard regardless of tier) builds its own columnar snapshot over its own membership exactly as badger/memory already do, and streams its rows directly to the caller's callback; the global ordinal order across shards does not matter, only that every label member is emitted exactly once. A per-shard decline (mixed/unbuildable value types, or over the size cap) forces the WHOLE tiered call to decline whenever that shard's exact `NodeCountByLabel` (the MINIMUM across the label tuple for the multi-label/intersection door) proves it has nonzero members — never a silently partial/incomplete columnar answer; a shard proven empty is skipped without touching its column builder at all. `NodeMutationEpoch` (the Gate-2 staleness fold) is the SUM of every currently-open shard's own node-mutation epoch (a closed cold shard contributes 0 rather than being force-opened, preserving the lazy-open contract for a check callers run after every aggregation — a documented, narrow residual risk if a write reaches a cold shard that is then idle-closed again before the next poll). `DocValuesSnapshot` returns a random-access reader that dispatches a point lookup to whichever shard's own snapshot reports membership (a node lives on exactly one shard at a time — CLAUDE.md B33). Cross-shard exact-set, label-intersection, Gate-2 mid-scan staleness, numeric/string type preservation, per-node-fallback equivalence, cold-shard (closed + lazy-reopened) participation, and closed-store sentinel-error coverage in `pkg/graph/store/tiered/tieredstore_docvalues_test.go`; a compile-time-only cross-package check in `pkg/graph/internal/core/docvalues_tiered_test.go` (the interface is core-internal, so there is no `capabilities.go` compile-assert site for it).

- Enabled unique property constraints (ADR-0002) on the TIERED store for REFERENCE labels (ADR-0005 §3.5 supersedes ADR-0002 Decision 5's outright "tiered declines"). Reference-class entities all live on the reference shard (plus its cold archive), so the ref-shard property index makes ref-shard uniqueness GLOBAL uniqueness — `g.Constraints().CreateUnique` / `CreateUniqueForever` now work on tiered for reference labels, enforced across all three ADR-0002 doors (node-create, value-update, label-add) plus batch/tx and `GetOrCreateByKey`. EVENT-class labels are refused with a new sentinel `ErrUniqueEventLabelUnsupported` (canonical in `internal/core`, re-exported from `pkg/graph`, in `docs/errors.md` and both inventory tests) — an event label's values span unbounded time shards with no global value index, a permanent correctness boundary, not deferred work, and deliberately distinct from the tiered store's `ErrEventPropertyIndex`. Removed the three hard tiered short-circuits: `uniqueMetaKV` no longer declines tiered (it returns refShard's MetaKV, which the durable constraint registry rides), `loadUniqueConstraints` no longer skips tiered (the registry rehydrates from refShard on reopen), and `loadUniqueForeverOwners` no longer skips tiered — the last was a correctness bug waiting to happen: without rehydration a reopen would lose every forever-claim and a value owned before restart would be claimable again. Classification anchors to the tiered store's own ontology via a new `tiered.Store.IsReferenceLabel(name)` (the SAME classifier the router uses), checked before any label token is minted so a rejected event-label create leaves no orphan token. `Admin().Reset` reaps both durable registries on tiered via the ref-shard MetaKV. Tests (memory/badger battery re-targeted at tiered for reference labels): create/violate/free, cross-door enforcement with two ref labels, event-label rejection with `errors.Is`, constraint durability across restart, `UniqueForever` two-phase cross-restart (claim survives, owner keeps, barred-after-delete, `ReleaseOwnership` frees), `GetOrCreateByKey`, `Reset` reaping, and a concurrent same-value create storm (exactly one winner) under `-race`; plus a direct `IsReferenceLabel` unit test. Testing Rule 14 respected — reference-label doors never touch event shards, so the default 1-week `ShardWindow` is used with no rotation.

- Extracted the as-of (transaction-time belief-state) SELECTION rule into one shared `storeutil.SelectAsOf[T TemporalRow]([]T, pin)` so memory, badger, and the core resolver stop re-implementing per-version transaction-time visibility — killing the cross-backend-divergence bug class (two real instances this month: the as-of version-order divergence and the change-log commit-window drop). The rule, now defined once: return the newest version by VERSION order with `0 < TxFrom <= pin`; the entity is ABSENT when no such version exists OR that decisive newest belief was itself retracted (`TxTo != 0 && TxTo <= pin`) or hard-deleted (`DeletedAt != 0 && DeletedAt <= pin`) by the pin — never fall through to an older still-open row (lesson 62). Recency is by VERSION, not `TxFrom`, because an `Update`'s `validInstantAfter`-derived `TxFrom` can exceed a later append-only cascade row's `now()` stamp. `SelectAsOf` is pure selection (no then-visible normalization — that stays with the caller). The core resolver (`resolveNodeChainAsOf`/`resolveRelChainAsOf`) and the memory backend (`nodeAsOfLocked`/`relAsOfLocked`) now DELEGATE to it, deleting their duplicated selection loops and their private `retractedAtTxTime` copies; the badger native reverse-scan (`NodeAsOf`/`RelAsOf`) is an early-stopping optimization now PROVEN equivalent to `SelectAsOf` over randomized version chains (`badgerstore_asof_equivalence_test.go`, node+rel parity) rather than re-deriving the rule. Behavior-preserving refactor (no wire/API change). Coverage: a direct table test of the rule (retraction, deletion, version-vs-`TxFrom` inversion per lesson 62, empty/single-row, boundary pins; node+rel over real entities), the two backend-equivalence tests, a new direct memory-store lesson-62 retraction regression (node+rel), and the full bitemporal oracle harness on both backends — all cross-verified via a mutation gate (reintroducing the lesson-62 fall-through inside `SelectAsOf` alone turns the table test, both equivalence tests, the memory regression, and the oracle harness red from that single point).

- Added history retention & compaction (ADR-0001), stages a–c (nodes + relationships). `g.Admin().CompactHistoryNodes(ctx, policy)` / `CompactHistoryRels(ctx, policy)` trim an entity's OLDEST version-history rows and record a per-entity DETACHED ANCHOR STUB. `RetentionPolicy{KeepVersions int, KeepSince types.Instant}`: a history version is trimmable only when it fails BOTH bounds (neither among the newest `KeepVersions` history versions NOR recorded at/after `KeepSince`); the current row and the newest history version are NEVER trimmable. `CompactReport{EntitiesCompacted, VersionsTrimmed, Watermark}`. The stub `{TrimmedThroughVersion, LastTrimmedHash, LastTrimmedTxTo, CompactedAtTx, StubHash(self)}` is persisted per entity in MetaKV (`SafeUnmarshal` on read, fail-closed on a corrupt blob, an entity-id mismatch, or a self-hash mismatch), so no stored row is ever mutated (append-only, lesson 46). `VerifyNodeChain`/`VerifyRelChain` are stub-aware: when a stub exists, the oldest kept version's `PrevHash` MUST equal the stub's `LastTrimmedHash` (the stub is a virtual predecessor), turning the truncation boundary back into tamper-evidence — a forged trim (no matching stub, wrong `LastTrimmedHash`, a retained genesis under a stub, or a bit-flipped stub) fails closed. The trim + stub + graph watermark commit in ONE store WriteBatch via the new optional `store.HistoryCompactionCapability` (memory + badger; tiered declines with `ErrCapabilityNotSupported`). Answerability (ADR Decision 3): a temporal read whose transaction-time pin falls before compacted knowledge returns the new sentinel `ErrHistoryCompacted`, never a silently-incomplete result — point doors (`NodeAsOf`/`NodeAtTx` + rel mirrors) check the per-entity stub boundary; scan doors (`NodesAsOf`/`RelsAsOf`, `ByLabel`/`ByType`/`All` with `TxAt`/`TxPin`) check the graph-level `CompactedThroughTx` watermark (max over stubs, durable in MetaKV, reloaded into a lock-free atomic at open so an uncompacted graph pays nothing) and fail the whole scan (ADR default (i), correctness-of-defaults). Refusals, all before any write: `ErrInvalidRetentionPolicy` (empty or negative policy), `ErrCompactionProtectedTag` (a registered named as-of tag pins knowledge the policy would trim — remove the tag first), and `ErrCompactionChangeLogEnabled` (compaction records / replica apply / delta interplay are stages d–f, so compaction is REFUSED while the change-log is on, so no replica can silently diverge from a compacted primary). Rejected on a read-only replica; reaped by `Admin().Reset()`. New sentinels canonical in `internal/core`, re-exported from `pkg/graph`, documented in `docs/errors.md` + inventory tests. Memory + badger; node+rel parity; policy-math tables, stub tamper tests, `ErrHistoryCompacted` at point + scan doors (`errors.Is`), stub-aware verify + tamper, protected-tag refusal, change-log refusal, tiered decline, reopen durability, and a two-phase check (compact then query above the watermark stays exact); `-race` clean. The bitemporal oracle harness was NOT extended with a compaction op class (focused two-phase tests were added instead — flagged for a later oracle pass). See `docs/adr/0001-history-retention.md`, `docs/api.md`, `docs/errors.md`.

- Fixed a badger-only, load-dependent read-consistency defect: the full-history readers `getNodeHistoryByPrefix` / `getRelHistoryByPrefix` (behind `GetNodeHistory` / `GetRelHistory`, and thus behind the temporal resolvers `nodeAtLockedTx` / `relAtLockedTx` that back `NodeAtTx` / `RelsAtTx` / `Nodes.All(point)` / `ByLabel`) could DROP an in-flight version across the async-flush commit window. They scanned the Badger `db.View` FIRST and merged the `flushing`/`pending` overlay SECOND; a concurrent `flush()` that committed a parked history row to Badger and then cleared `flushing` in the gap between the reader's Badger snapshot and its overlay read left that version in NEITHER view (invisible to the older Badger snapshot and to the already-cleared overlay), so the returned version chain was missing a row. The resolver then selected the wrong version for a valid-time slot or returned `ErrNodeNotFound` for an entity whose `History()` had rows moments earlier — surfacing as two temporal doors that resolve through the same per-ID function disagreeing (`Nodes.All(point)` vs `NodesAtTx`, `Rels.All` vs `RelsAtTx`). It is not a data race (`flushing`/`pending` are `wbMu`-locked), so `-race` never flagged it; the drop needs the flush's commit+clear to land in the sub-microsecond scan→merge gap, which an idle machine rarely hits (a prior investigation saw it 2/30) but a heavily loaded machine widens by descheduling the flush goroutine mid-window. Fix: snapshot the overlay (via the already-correct `pendingHistoryVersionOverlay`) BEFORE opening the Badger `View`, then merge the strictly-newer overlay over the scan — capturing the overlay at Ta and opening the View at Tb ≥ Ta closes the window (a row committed after Ta was still in `flushing` at Ta; a row committed before Ta is durable and in the View). The sibling retention/purge/repair mutators that shared the same scan-first order (`truncateHistoryByPrefix`, `trimHistoryFromPrefix`, and the on-disk-mode `maintainPropertyIndexesPurge`, `purgePropertyKeyDiskEntriesLocked`, `incomingIndexEntriesFromKeyspaceLocked`) were reordered overlay-first too, where the same window would drop a key from a computed delete/retention set (orphaned index entry / distorted keepVersions window). The already-correct `*HistoryVersionsFromPrefix` and `maxHistoryID` / `pendingHistoryIDOverlay`-backed readers were already overlay-first — the defect was the divergence within one reader family. Deterministic reproduction through the public store doors via a new in-reader test hook that lands a full flush commit inside the scan→merge gap (node+rel parity, mixed persisted/parked versions), a concurrent flush-loop stress read under `-race`, and a full bitemporal cross-door oracle run over a real badger store with a sub-millisecond flush interval and a tiny cache. See lesson 64.

- Added unique property constraints (ADR-0002), CORE stages: a value-lock manager, a durable constraint registry, and enforcement on the STANDALONE node doors. `g.Constraints().CreateUnique(ctx, label, propertyKey)` forbids two CURRENT nodes carrying the same value for `(label, propertyKey)` — `UniqueCurrent` scope only (the `UniqueScope` enum reserves `UniqueForever`/`UniqueValidOverlap`; requesting either returns a not-yet-supported error). History may hold duplicates; a value freed by supersession or delete is immediately reusable. `DropUnique(ctx, label, propertyKey)` and `UniqueConstraints() []constraints.UniqueConstraint` complete the surface. `CreateUnique` follows the 3-phase index protocol: it installs a PENDING entry under lock (concurrent writers enforce immediately), auto-ensures a property index on `(label, propertyKey)`, validates existing data unlocked, then activates + persists — or uninstalls and returns `ErrUniqueViolationExisting` listing up to five offender IDs. Float-typed keys/values are rejected with `ErrUniqueUnsupportedType` (int64/string/bool/temporal supported — bit-pattern float equality is user-hostile). The constraint registry is durable in MetaKV under one msgpack map (`SafeUnmarshal`, fail-closed on a corrupt blob, durable across reopen, reaped by `Admin().Reset`, declines without MetaKV via `ErrCapabilityNotSupported`, rejected on a read-only replica), loaded into an in-memory index at open. A new 256-stripe value-lock manager (`internal/locks.ValueManager`, keyed by `hash(labelToken, keyToken, canonical value bytes)`) establishes the global lock order entity locks -> value locks -> idxMu: a create/update/label-add that introduces or changes a constrained value holds the value stripe across the index check AND the store write, so concurrent same-value writers serialize to exactly one winner (an update changing the value takes both old and new stripes in ascending order). **Enforcement currently covers the STANDALONE node doors** — `Add` / `AddWithTx` / `AddByIDIfAbsent` / `Update` / `UpdateInPlace` / `CompareAndSetProperty` / `AddLabel` (the label-add door binds a constraint without touching the property: a node carrying an offending value acquiring the constrained label is rejected). A violating write returns `ErrUniqueViolation`, wrapped with the label, key, and winning entity ID. Batch/tx/import enforcement and the `UniqueForever` ownership registry are a follow-up wave, landing before any release tag. New sentinels `ErrUniqueViolation` / `ErrUniqueViolationExisting` / `ErrUniqueConstraintExists` / `ErrUniqueConstraintNotFound` / `ErrUniqueUnsupportedType` (canonical in `internal/core`, re-exported from `pkg/graph`). Memory + badger; tests cover create-then-violate, update/CAS/label-add-into-violation, delete-frees-value reuse, supersession-frees, existing-data validation failure (no constraint installed, writes unenforced), reopen durability, `Reset` reaping, a 100-goroutine same-value create storm (exactly one winner) under `-race`, and a two-phase temporal check that history duplicates do not violate. See `docs/adr/0002-unique-constraints.md`, `docs/api.md`, `docs/errors.md`.

- Completed unique property constraints (ADR-0002): enforcement now covers batch/tx/import, the `UniqueForever` value-ownership scope, and a `GetOrCreateByKey` primitive. (1) BATCH/TX enforcement: `BatchBuilder.AddNode` creates are pre-checked under the batch's exclusive `c.mu.Lock` against committed state AND earlier same-value creates in the same batch — the second same-value create fails at op time (`ErrUniqueViolation`) and is removed from the write set while the rest of the batch commits; for a `UniqueForever` constraint the same pre-check also consults the durable ownership registry (the exclusive `c.mu.Lock` fences out concurrent standalone writers, so it reuses the standalone `checkAndClaimForever` seam under `c.uniqueMu`), so a value owned forever by an entity that no longer holds it — after supersession or hard delete, with no current holder — is barred on the batch door too, and a fresh batch create of a `UniqueForever` value claims ownership durably; batch node updates already flowed through the enforced internal update door. `GraphTx.AddNode`/`UpdateNode` enforce through the standalone kernel (a tx applies mutations immediately, so a same-value create inside one tx sees the earlier one and fails); a UniqueCurrent value claimed in a tx is freed automatically on rollback (removing the created node removes its index entry). (2) IMPORT: `g.IO().Import` validates the replayed CURRENT state against every active constraint and rolls the whole import back with `ErrUniqueViolation` on a duplicate (default-strict, matching the hash-mismatch rollback); `io.ImportOptions.SkipUniqueValidation` opts a TRUSTED restore out. Replica apply reproduces rows VERBATIM and does NOT enforce (a record that would violate a locally-active constraint still lands) — documented with a test. (3) `UniqueForever` (`g.Constraints().CreateUniqueForever`): the first entity to hold a value owns it permanently — only that entity (any later version) may ever hold it, every other node is barred forever across supersession, hard delete, and reopen. Backed by a durable ownership registry `ownerKey(labelTok, propKey, valueKey) -> owning NodeID` in MetaKV with a self-hash (`SafeUnmarshal`, fail-closed on a corrupt/tampered blob, loaded at open, reaped by `Admin().Reset`); the kernel consults it under the value stripe (registry hit + different entity → violation; same entity → pass; miss → claim + persist). `CreateUniqueForever` seeds ownership from existing current values after validating no current duplicates; `DropUnique` releases a forever constraint's claims; `Constraints().ReleaseOwnership(ctx, label, propertyKey, value)` is the operator door to free an owned value (idempotent; `ErrUniqueConstraintNotFound` without a forever constraint on the pair). A UniqueForever claim made inside a rolled-back tx is NOT auto-released (the durable claim is not part of the tx snapshot) — the value stays barred (conservative); `ReleaseOwnership` is the remedy. (4) `g.Nodes().GetOrCreateByKey(ctx, label, propertyKey, value, extraProps) (*types.Node, bool, error)`: atomically returns the current node holding value for `(label, propertyKey)` or creates one, under a single value lock so a storm of concurrent callers produces exactly one create — works WITH or WITHOUT an active constraint (the value lock alone makes it atomic); floats rejected with `ErrUniqueUnsupportedType`; the returned node is a mutable copy (Get semantics). Also: three verifier fixes from the core stage — `UpdateInPlace` now passes its pre-mutation state to the kernel so a changed constrained value holds the freed old-value stripe; `CreateUnique`'s label-token resolution and the pending-enforcement window are documented. Memory + badger; new tests cover batch-internal/committed collisions, tx internal-duplicate/rollback-frees-value/tx-vs-standalone race, import strict-reject-and-rollback + skip-validation, replica apply no-op, forever barred-after-delete/same-entity-any-version/reopen-durability/reset-reaping/existing-dup-reject/`ReleaseOwnership`, forever enforcement on the batch door (barred-after-delete, a fresh batch create claiming ownership, a tx create barred by a forever-owned value), the forever ownership blob's self-hash tamper failing closed with `ErrCorruptWire`, a 50-goroutine forever claim storm, a 40-goroutine batch-vs-standalone forever claim storm, and a 100-goroutine `GetOrCreateByKey` idempotency storm — all under `-race`. See `docs/adr/0002-unique-constraints.md`, `docs/api.md`, `docs/errors.md`.

- Backup ergonomics hardening: two fixes to the `BackupTo`/`BackupDeltaTo`/`RestoreInto` surface, both test-first. (1) `renameNoClobber`'s stat-then-rename had a TOCTOU window — two concurrent callers targeting the same dir with nothing mutated between them resolve the identical deterministic filename, both can pass the stat check, and `os.Rename` unconditionally replaces an existing destination, so the second caller silently clobbered the first with no error from either side. Replaced with `os.Link(tmp, final)` (fails closed with `ErrBackupExists` on `EEXIST`, then removes `tmp`) — the kernel serializes concurrent creations of the same path, so at most one caller can ever succeed. New concurrent-callers regressions for both `BackupTo` and `BackupDeltaTo` (N racing goroutines against one dir, exactly one winner) plus a direct `-race` reproduction of the old TOCTOU against `renameNoClobber` itself. (2) Remediated two gosec G304 ("file inclusion via variable") findings in `RestoreInto`'s file-open helpers: `scanBackupDir` now runs every matched directory-entry name through a new `safeBackupPath` guard (rejects embedded path separators, requires `filepath.IsLocal`, and independently re-verifies the resolved absolute path stays under the scanned dir) before ever joining it into a path that gets opened, and the two `os.Open` call sites carry `#nosec G304` comments naming that guard as the actual containment proof (the pre-existing `//nolint:gosec` comments there suppressed golangci-lint's gosec integration, which isn't even enabled in this repo's `.golangci.yml` — they never reached the standalone `gosec` binary `make security-docker` runs, so the finding was live). New adversarial unit tests for `safeBackupPath` (`../`, embedded separators on both OS styles, empty name) plus positive cases proving the legitimate backup filenames are unaffected.

- Vector indexes default to a new pure-Go HNSW (Hierarchical Navigable Small World) approximate nearest-neighbor engine instead of brute-force linear scan, with brute-force retained as an exact-recall escape hatch and as the correctness fallback for filtered searches. New `pkg/graph/internal/index/hnsw.go`: layered graph, `M=16`/`EfConstruction=200`/`EfSearch=64` defaults, deterministic level assignment from a fixed-seed RNG (same insertion order always builds the same graph and returns the same results), the paper's diversity-preserving neighbor-selection heuristic (a naive "keep the M closest" selection was tried first and reverted — it fragments a graph over well-separated clusters into disconnected islands by pruning away long cross-cluster edges in favor of many redundant short ones), soft-delete tombstones with a full rebuild once the tombstone/live ratio exceeds 20%, and both existing distance metrics (cosine, euclidean). All existing `VectorIndex` construction call sites across memory/badger/tiered are unchanged — the engine defaults to HNSW via the struct's own zero-value fields, built lazily on first `Add`/`Remove`. New additive `g.Index().CreateVectorWithOptions(label, propertyKey, dims, metric, storepkg.VectorIndexOptions{UseBruteForce, M, EfConstruction, EfSearch})` and the matching optional `store.VectorIndexOptionsCapability` (implemented by all three in-tree backends; badger/tiered persist the chosen engine/tuning in the vector-index definition so a restart preserves it) let a caller opt a specific index into exact brute-force search or custom HNSW tuning; plain `CreateVector`/`CreateVectorIndex` are unchanged in signature and now equivalent to all-default `VectorIndexOptions{}`. Filtered search over-fetches 4x its effective `ef` worth of candidates then post-filters, falling back to an exhaustive brute-force scan (with the same filter) whenever fewer than k eligible candidates survive, so a highly selective filter never under-returns. Recall gate: recall@10 >= 0.95 over a seeded 10k x 128-dim clustered corpus and 100 query vectors (`pkg/graph/internal/index/hnsw_test.go`), plus exact-top-1, churn (insert/delete/re-search crossing the rebuild threshold), filtered-search equivalence at high/low selectivity, empty/small-index edge cases, concurrent search+mutate (`-race` clean), and determinism coverage. New `bench/ann_test.go` `ANNSearch10k` scenario compares the two engines head-to-head. See `CLAUDE.md` "Vector Indexes" and `docs/api.md`.

- Structural refactor (no behavior change): introduced `resolveNodeChain(chain, probe, pred)` / `resolveRelChain(...)` as the SINGLE core-layer seam through which every temporal read selects a version from a pre-built `(history ‖ current)` chain. A `chainProbe{kind, validAt|validStart/validEnd, tx}` names the query shape — `probePoint` (valid-time point + optional `TxAt` filter), `probeInterval` (predicate-anywhere overlap + optional `TxAt` filter), `probeAsOf` (knowledge-time belief pin). Both the named doors (`NodeAt`/`NodeAtTx`/`NodesDuring`/`NodesAsOf` and rel mirrors) and the generic `QueryOpts` doors (`findNodeVersionForOpts`/`findRelVersionForOpts`, `nodeAtLockedTx`/`relAtLockedTx`, `find*VersionMatchingDuringTx`, `nodeAsOfLocked`/`relAsOfLocked`) now funnel their per-candidate selection here, so the six selection rules — TX visibility (`TxFrom <= txAt`, no `TxTo` bound — lesson 43), pre-delete tombstone normalization (lesson 60), version-interval `[vStart, vEnd)` derivation (lessons 32/33/42), newest-belief-on-overlap selection (lessons 46/62), predicate-anywhere interval matching (rule 16), and the as-of retraction rule (lesson 62) — live in exactly one place and cannot drift between the two doors (rule 17). The canonical store-level predicates in `internal/storeutil` are untouched (store push-down unchanged). Direct table-driven unit tests of the seam (node+rel parity, hand-computed chains) plus the full bitemporal oracle harness (point/interval/as-of/TxPin, memory+badger) stay green.

- Added a persistent property index for the badger backend: `badger.Config.PropertyIndexOnDisk` (and the mirrored `graph.Config.PropertyIndexOnDisk` plumbing) keeps `CreatePropertyIndex`'s entries out of RAM — each `(propertyKeyToken, order-preserving value bytes, nodeID)` entry lives under a new `0x0A` keyspace instead of the in-memory `PropertyIndex.Entries`/`numBuckets` maps, following the LabelIndexOnDisk/AdjacencyIndexOnDisk pattern (persisted keyspace + prefix/range iteration + a pending-write overlay resolving set-vs-delete PER KEY, lesson 57). The on-disk key deliberately omits the label token — a property key shared by index definitions on different labels shares ONE physical row, and every reader (equality and range) re-fetches the candidate node and rechecks `HasLabelTokenRaw`, the SAME over-select-then-recheck contract those readers already used against the RAM-mode indexed path. `DropPropertyIndex` reference-counts by property key (not the full label+key pair) before physically purging rows, so dropping one label's definition never corrupts a sibling definition on another label using the same key. Numeric values (any int/uint/float width) share one order-preserving sort domain via the standard IEEE-754 sign-flip encoding (lesson 25's bit-pattern case, applied to key bytes) with a subtype+exact-bits trailer disambiguating cross-type equality (`int64(5)` and `uint64(5)` share a sort position but remain distinct stored values); string/bool/temporal values use the canonical value-key bytes verbatim (length bounded by the property's own `MaxPropertyValueSize` validation at write time, no additional codec limit). Write-path maintenance lands in every node-mutation door: `PutNode`, `DeleteNode`, `ReplaceNode`, `ReplaceNodeWithHistory`, all four label-token doors (`AddNodeLabelToken{,WithHistory}`/`RemoveNodeLabelToken{,WithHistory}`), `PutNodesBatch`, `DeleteNodesBatch`, and the cascade-delete corruption-path brute-force purge — every site merges the property-index writeOps into the SAME `appendOps` call as the entity row (crash consistency: a property-index entry and the row it describes always land in one WriteBatch). An existing directory with property-index DEFINITIONS but no prior `0x0A` rows (built before the flag existed) is backfilled from current node state exactly once, the first time the flag is turned on, guarded by a new `wire_format_version`-style meta marker so later opens skip the rescan. `NodeRangeCardinality` declines (`exact=false`) in disk mode rather than reimplementing the RAM ordered view's O(1) bucket-sum — never a wrong count, just unavailable acceleration; callers already handle decline by scanning and counting exactly. See `docs/persistence.md` and lessons 23/25/57.

- One-call backup/restore ergonomics over the existing export/delta machinery — no new wire format. `g.IO().BackupTo(dir)` writes a full export to a deterministically named file, `dir/backup-<LSN>-full.tkg` (LSN zero-padded to 20 digits and derived from the export's OWN header cursor, never wall time, so two backups at the same change-log point always produce the same name); the file is fsync'd and `BackupTo` refuses to silently overwrite an existing target (`ErrBackupExists`). On a backend with no active change-log the export still succeeds with a zero cursor (documented — every such backup in a dir shares one name, so only the first call succeeds). `g.IO().BackupDeltaTo(dir, since)` is the `ExportSince` counterpart, writing `dir/backup-<sinceLSN>-to-<toLSN>-delta.tkg`; an EMPTY delta (nothing committed since `since`) writes NO file and returns `since` unchanged (a side-effect-free no-op for a "what's new" poll); it declines with the same wrapped `store.ErrCapabilityNotSupported` `ExportSince` already returns on a change-log-less backend. New package-level `graph.RestoreInto(cfg Config, dir string) (*Graph, error)` opens a graph from `cfg`, discovers the one full backup plus every delta backup in `dir`, validates the whole chain is gapless BEFORE any replay (each file's header read via `HeaderOf` — the full backup's cursor must equal the first delta's `From`, and each delta's `From` must equal the previous file's `To`; a lineage/epoch mismatch fails closed with a wrapped `ErrCursorUnknown` and an LSN gap or out-of-order chain fails closed with a wrapped `ErrDeltaBaseMismatch`, both naming the offending file), then replays `Import` (full) then `ImportMerge` (each delta, oldest first) — closing the graph it opened on any failure. All three are pure orchestration over `Export`/`ExportSince`/`Import`/`ImportMerge`/`HeaderOf`; no store or wire-format change. See `pkg/graph/io/backup.go`, `pkg/graph/restore.go`, `docs/persistence.md`.

- Hardened the bitemporal oracle harness's probe-value generation against a verified ~17% flake in non-short `-race` runs (seeds 47645253332 / 47645253148, both a point/interval probe pinned exactly on a deleted entity's tombstone `ValidTo`). `bitemporaloracle_test.go`'s `buildProbes` now splits recorded stamps into two provenance buckets before turning them into validAt/interval boundary candidates: EXPLICIT caller-chosen `tkg_valid_from`/`tkg_valid_to` values (plain test-chosen sequence numbers, never wall-clock derived) keep exact-boundary and boundary±1 probing unchanged, while SYSTEM-DERIVED stamps (`TxFrom`/`TxTo`/`DeletedAt`/`UpdatedAt`, a delete's tombstone `ValidTo`, and the snowflake-derived effective valid-from fallback — all rooted in the wall-dominated, only-`>=1`ms-monotonic `Core.now()`) are probed at stamp-2ms/stamp+2ms instead of exact equality, dropping any shifted candidate that collides with a different op's system-derived stamp, per the ms-truncation clock hazards documented in `bitemporal_tombstone_test.go`'s header. The `txAt` probe dimension is untouched. Test-only change — zero production diffs; verified by three targeted engine-mutation canaries (disabling the delete-tombstone normalization, reintroducing a `TxTo<=txAt` visibility bound, and preferring `next.UpdatedAt` over an explicit `next.ValidFrom` in `vEnd` derivation) each still tripping the hardened harness, plus a new probe-count sanity check confirming the TxPin/as-of probe classes are not starved by the new filtering.

- Added `QueryOpts.TxPin` (`types.Instant`): a belief-state pin for the generic query door (`ByLabel`/`ByType`/`All`) that performs pure knowledge-time resolution with NO valid-time filtering — identical semantics to `g.Temporal().NodesAsOf`/`RelsAsOf`, reached through the generic `QueryOpts` door and post-filtered by label/type/property. Closes a known consumer footgun: a caller who set only `QueryOpts.TxAt` expecting "everything known at T" got a silent valid-at-wall-now filter that emptied the result of any fact valid only in the past. `TxAt`'s documented behaviour is unchanged (it remains the combined bitemporal filter), but its godoc now carries a loud warning pointing at `TxPin`. Routing does not re-implement the rule: `findNodeVersionForOpts`/`findRelVersionForOpts` delegate to the same `nodeAsOfLocked`/`relAsOfLocked` the named door uses, so the two doors agree by construction (the version-ordered retraction rule is inherited, deleted entities are folded into the candidate set exactly as `NodesAsOf` collects them). Setting `TxPin` together with `ValidAt`, `ValidStart`/`ValidEnd`, or `TxAt` returns the new sentinel `ErrConflictingTemporalOpts` (re-exported from `pkg/graph`) rather than silently mis-resolving. Both backends; node+rel parity; a new focused two-door equivalence + footgun test, a public-layer conflict-sentinel test, and a new `TxPin` probe class in the bitemporal oracle harness cross-checking the generic door against the independent as-of oracle on memory and badger.

- Tx-aware adjacency push-down (RT-1): `g.Rels().OutgoingForNodesAtTx(nodeIDs, typeName, txAt)` / `IncomingForNodesAtTx(...)` are additive, transaction-time-pinned counterparts of `OutgoingForNodes`/`IncomingForNodes`. Previously a pinned edge read had no adjacency-index door — a caller had to fall back to a full history-aware `ByType` scan filtered by endpoint. The new doors resolve each candidate relationship's belief-at-pin version through the SAME chain-resolution seam the generic `QueryOpts.TxAt` scan door uses (`findRelVersionForOpts` -> `filterRelChainByTxAt`, the tombstone-normalization seam shared with `NodeAtTx`/`RelAtTx`/the named as-of door), so a pinned adjacency read agrees with a pinned `ByType` scan filtered by endpoint by construction (never a private re-implementation). The candidate relationship-ID set is the live per-node adjacency (the fast-path seed) unioned with every DELETED relationship ID via `forEachRelAdjacencyCandidateID` — rel endpoints are immutable, so a relationship deleted after the pin is still visible (delete is a transaction-time tombstone), one created after the pin is invisible, one deleted before the pin is invisible, and a backfilled relationship (`AddWithTx`, §4.1) is visible from its backfilled `TxFrom` onward, not from wall-clock creation time. `txAt == 0` delegates to `OutgoingForNodes`/`IncomingForNodes` verbatim (no TX filter, no behavior change, no caller churn); an unregistered `typeName` still validates that every requested node currently exists. Break-tests: a randomized small-graph divergence probe (nodes+rels with updates, deletes, and a backfill) asserting `OutgoingForNodesAtTx`/`IncomingForNodesAtTx` == a pinned `ByType` scan filtered by endpoint across a spread of pins; a deterministic adversarial two-phase scenario covering a rel deleted after the pin (visible), created after the pin (invisible), deleted before the pin (invisible), and updated (belief-at-pin resolves to the correct version); a dedicated backfill scenario; a tiered cross-shard (reference↔event) scenario; memory + badger + tiered, `-race` clean. See `pkg/graph/internal/core/adjacency_at_tx_test.go`, `tasks/hp-workplan-2026-07-04.md` RT-1, `docs/query-planners.md`.

- Fixed a `bench/` benchmark-design flaw: the three write benchmarks (`Ingest1kSingle`, `Ingest10kBatch`, `BulkAddNodes10k`) built one graph outside the `for b.Loop()` loop and kept mutating it every iteration, so ns/op silently drifted upward with `-benchtime` iteration count (measured ~15.5ms/op at `-benchtime=1x` vs ~29.7ms/op at `-benchtime=20x` on `Ingest10kBatch`/badger) and could false-positive a `bench-check` regression on an otherwise-untouched scenario. Each now builds and closes a fresh, empty graph every iteration via the classic `for i := 0; i < b.N; i++ { ... }` loop with construction/teardown excluded from timing via manual `b.StopTimer()`/`b.StartTimer()` — `b.Loop()` itself doesn't support this shape safely (`b.StopTimer()` poisons the loop's internal state and hard-fails the next iteration's `b.Loop()` check unless the timer is resumed first). See `bench/ingest_test.go` and `bench/README.md`.

- Fixed a cross-backend as-of divergence: `NodeAsOf`/`NodesAsOf`/`RelAsOf`/`RelsAsOf` (and the tiered fallback path) resurrected a hard-deleted entity that had undergone an append-only cascade (`SetNodeVersionInterval`/`SetRelVersionInterval`). The cascade demotes the prior current to history without stamping its `TxTo`, so a superseded row keeps `TxTo == 0`; the resolvers selected the newest version whose TX interval *covered* the pin and, when the tombstoned newest belief was excluded by that filter, fell through to the still-open genesis and reported the entity PRESENT — while badger's native reverse-scan correctly reported ABSENT at pins at/after the delete. Aligned the memory-native and core-fallback resolvers with the badger-native semantics (read path only; the append-only write discipline is unchanged): as-of selection is now the newest belief by **version** (highest version recorded by the pin — recency is version order, not `TxFrom`, because an `Update`'s `validInstantAfter` stamp can exceed a later cascade row's `c.now()` stamp), and a decisive belief that is superseded or deleted by the pin means the entity is absent — the resolver never falls through past it. Pins before the delete still reconstruct the pre-delete belief. Regression coverage across all three backends (memory/badger/tiered), node+rel mirrors, plus a new as-of clause in the bitemporal oracle harness cross-checking `NodesAsOf`/`RelsAsOf` on both backends.

- New `bench/` package: a committed cross-backend performance suite (memory
  and Badger in-memory mode) covering point reads, a 10k-node label scan
  (sorted and `QueryOpts.NoSort`), a two-hop `ForEachAdjacentEndpoint`
  traversal over 10k nodes / 30k relationships, a valid-time point query and
  a transaction-time as-of query each over 5-version chains, and three
  ingest paths (single `Add`, `BatchBuilder.AddNode`, and the bulk
  `BatchBuilder.AddNodes`) at 1k/10k scale. Every scenario builds its fixture
  once via the `for b.Loop()` protocol so setup cost is never repeated across
  Go's benchmark timing-calibration passes. New Make targets: `make bench`
  (the suite itself, `-benchtime=0.3s -count=1`), `make bench-baseline`
  (captures a per-machine `bench/local-baseline.txt`, gitignored — baselines
  are never committed), and `make bench-check` (`bench/bench-check.sh`:
  installs `benchstat` if missing, runs a fresh comparison pass, and fails if
  any scenario's time regressed more than 15% — computed from benchstat's
  raw CSV output rather than its human-readable delta column, since that
  column is masked to "~" at `-count=1` regardless of magnitude). The
  pre-existing `bench` target (the `pkg/types` memory-footprint/struct-size
  tests) is renamed `bench-types-footprint` so the `bench` name is free for
  this suite; its command is unchanged. New `.github/workflows/bench.yml` (manual `workflow_dispatch` only — shared
  GitHub-hosted runners are too noisy for a per-PR regression signal; the
  authoritative tooling is local `make bench-baseline`/`bench-check`):
  on a PR touching `pkg/**` or `bench/**`, benchmarks the merge-base and HEAD
  and posts a `benchstat` comparison to the job summary; the compare step is
  `continue-on-error: true` until PR-runner noise is understood, per
  `bench/README.md`.

- Documentation: new `docs/stability.md` documents the v4 API stability promise, deprecation ritual, experimental surfaces (Replication Phase-1 API, `g.Tier()`, DocValues readers, `QueryOpts.IncludeEclipsed`), and release conventions. Linked from README.

- NDV + exact min/max property statistics for query planners. `g.Stats().PropertyStats(label, propertyKey) (store.PropertyStats, error)` returns `{NDV, Min, Max, Count}` for a `(label, property key)` pair — the richer sibling of `NodeCountByLabelAndPropertyKey`, letting a cost-based planner estimate equality selectivity (`Count / NDV`) and prune range predicates outside `[Min, Max]`. `NDV` is an ESTIMATE from a new in-tree, dependency-free HyperLogLog sketch (`pkg/graph/internal/index/hyperloglog.go`; Flajolet et al. 2007; precision 14 default, sparse-then-dense register storage, seeded accuracy regression pinning error < 5% at 10k distinct values / < 3% at 100k). `Min`/`Max` are EXACT over scalar-ordered value families only (numeric and string — bool/`TemporalValue` still count toward `Count`/`NDV` but leave `Min`/`Max` nil); `Count` is the same presence count `NodeCountByLabelAndPropertyKey` returns. Backed by the new optional `store.NodePropertyStatsCapability`, implemented by memory and badger: the sketch + min/max accumulator (`pkg/graph/internal/index/property_stats_accumulator.go`) is maintained on the SAME node-mutation doors as the presence counter and rebuilt from persisted rows at badger index load, so all four fields survive a restart. NDV never decreases on delete (HyperLogLog has no removal); deleting or replacing the node holding the current Min/Max marks the accumulator dirty and defers an exact recompute to the next read (a single O(nodes carrying the label) rescan), rather than paying that cost on every delete. Badger's rescan uses fine-grained locking (label-membership snapshot, then node fetch, then commit, each its own lock window) rather than one lock for the whole call — holding a single lock across the node-fetch loop would self-deadlock, since a cache-cold node fetch itself needs a brief read-lock. That unlocked-collect window is made correct by an optimistic write-generation guard (`PropertyStatsAccumulator.WriteGen`, bumped under the lock on every Observe/Forget): the rescan reads the generation before the collect and re-reads it before committing, redoing the collect (bounded retries) if a concurrent mutation moved it, and on exhaustion returns the live snapshot without committing a possibly-stale rescan and leaves the pair dirty — so a concurrent PutNode landing a new live extremum can never be silently overwritten by a stale collect (the exact Min/Max stays correct on both backends; lesson 62). The memory backend needs no guard: it holds one lock for the whole call (its node lookups are direct in-process map reads with no re-entrant locking risk). `tiered.Store` declines with `ErrCapabilityNotSupported` (v1 limitation — no cross-shard NDV/min-max fold yet). See `docs/query-planners.md` "NDV + min/max statistics".

- Test-only: closed the ValidStart/ValidEnd (interval) + TxAt regression

- Go 1.23+ range-over-func iterators over reads (additive). `g.Nodes().Iter(ctx, opts) iter.Seq2[*types.Node, error]` and `g.Rels().Iter(ctx, opts) iter.Seq2[*types.Relationship, error]` wrap the existing `ForEach` machinery (no new scan paths) so callers can write `for n, err := range g.Nodes().Iter(ctx, opts)`: same row set, same order, and the same temporal/paginated fallback to `All` that `ForEach` itself takes for the given `opts`. `ctx` is checked once per row (non-blocking) — cancellation yields `(nil, ctx.Err())` exactly once and stops the scan; an internal error yields `(nil, err)` exactly once and stops; breaking out of the range loop stops the underlying scan immediately. Closed a standing Testing-Rule-2 Node/Rel parity gap along the way — `RelOps.ForEach` / `g.Rels().ForEach(opts, fn)` did not exist (v4.9.4 added only the node side); it now mirrors `NodeOps.ForEach` exactly (same fast-path/fallback/isolation contract), with its own `TestGraphForEachRels*` battery mirroring the existing node tests. `g.Rels().OutgoingIter(ctx, nodeID, typeName)` / `IncomingIter(...)` wrap the pre-existing `ForEachOutgoing`/`ForEachIncoming` adjacency scanners. Row ownership mirrors `ForEach` exactly and is **not uniform across `opts`** — discovered test-first, not assumed: `ForEach`'s fast path (plain current-state, unpaginated, trusted backend) fetches each row via the point-read `GetNode`/`GetRelationship`, which always deep-copies (an independent, already-mutable row per the store-boundary contract), while a temporal filter or `Limit`/`After` pagination forces the fallback to `All`, which returns shared FROZEN rows on a trusted backend (`DeepCopy` before mutating in that case); `OutgoingIter`/`IncomingIter` always carry the frozen-row contract, matching `ForEachOutgoing`/`ForEachIncoming`'s own documented behavior. Godoc states this precisely instead of the simpler "always frozen" contract, since the latter does not hold for `ForEach`'s fast path. New `pkg/graph/nodes/iter_test.go` / `pkg/graph/rels/iter_test.go` (white-box: parity, early-break stops the underlying scan via an fn-call counter, ctx-cancel-mid-iteration yields exactly one terminal error, no-goroutine-leak) and `pkg/graph/iter_test.go` (black-box, both backends: ID-set parity against `ForEach`/`All`, frozen-row contract via `errors.Is(..., types.ErrFrozenNode/ErrFrozenRelationship)`, ctx cancellation, a `-race` concurrent-writer probe, `OutgoingIter`/`IncomingIter` parity). `ExampleAPI_Iter` added to both `pkg/graph/nodes` and `pkg/graph/rels`. See `docs/api.md` "Iterators (Go 1.23+ range-over-func)".

- `g.Replication().Watch(ctx, fromLSN) iter.Seq2[store.ChangeRecord, error]`
  is a live-tailing convenience layer over `ForEachChange` for `for rec, err :=
  range g.Replication().Watch(ctx, cursor)` consumers: yields committed records
  with LSN >= `fromLSN` in strictly ascending order, then polls with ctx-aware
  backoff (25ms doubling to a 500ms cap while idle, reset to 25ms after any
  record; always a `time.Timer` selected against `ctx.Done()`, never
  `time.Sleep`). Terminates on exactly four conditions: `ctx` cancellation
  (no error yielded — the normal shutdown path for a live tail), the caller's
  range loop breaking, the change-log being inactive (checked once before the
  first pull — see the fail-closed hardening bullet below), or the underlying
  feed erroring on a later pull (`(zero, err)` yielded once, then stop).
  Delivery is at-least-once across separate `Watch` calls
  (a consumer resuming from an older persisted cursor may see a record again)
  and exactly-once within one call. New `pkg/graph/replication/watch.go` +
  `pkg/graph/replication/watch_test.go` (resume exactness, live tail under
  `-race`, ctx-cancel-bounded goroutine exit, no-capability via both a real
  `tiered.Store` and a deterministic fake-ops unit test, nil-API fail-closed —
  every test asserts strict LSN ascending order) and a <=40-line
  `pkg/graph/example_watch_test.go`. README gets a short "Watching changes"
  subsection under the change-feed hero linking the runnable example. No
  changes to `ForEachChange`/`ChangeFeed`/`LastCommittedLSN` or any other
  existing method.

- Fix: `g.Replication().Watch` now fails closed on a PRESENT-BUT-DISABLED
  change-log instead of silently tailing an always-empty feed forever. Both
  `badger.Store` and `memory.Store` implement `store.ChangeFeedCapability`'s
  methods unconditionally (change-log enabled or not — the methods simply
  return nothing when off), so a badger graph opened with `ChangeLog: false`
  or a bare `memory.New()` (no `WithChangeLog()`) previously made `Watch`'s
  first pull "succeed" with zero records and then poll a feed that would
  never produce anything — indistinguishable from a caught-up, healthy tail.
  `Watch` now probes `store.ChangeLogStatusCapability.ChangeLogEnabled()`
  once, before the very first pull (the exact same check
  `g.IO().Watermark()`/`ExportSince()` already use to fail closed), and yields
  `store.ErrCapabilityNotSupported` — assert with `errors.Is` — exactly once
  and stops without ever calling `ForEachChange`, mirroring the "no
  change-log" wording those two doors already return. New
  `(*core.ReplOps).ChangeLogActive()` forwards to the existing
  `(*Core).changeLogActive()` probe so the replication sub-API can reach it
  through its `Ops` interface. New tests: a badger graph with `ChangeLog:
  false`, a bare `memory.New()`, and a deterministic fake-ops unit test
  (asserting `ForEachChange` is never even called) all assert the sentinel via
  `errors.Is`; existing Watch tests (both backends WITH their change-log
  enabled) stay green unchanged. `docs/errors.md`'s `ErrCapabilityNotSupported`
  row gains the `Watch` door reference.

- Test-only: closed the ValidStart/ValidEnd (interval) + TxAt regression

- Dependency license audit: `docs/dependencies.md` catalogs every module in the
  dependency graph (module, version, license — each verified from the module
  cache, not from memory), asserts the permissive-license allowlist
  ({Apache-2.0, BSD-2/3, MIT, ISC}), and documents the `gopkg.in/yaml.v3`
  NOTICE-file adjudication (no obligation attaches — this repo does not
  redistribute dependency source; vendoring would change that).
- Test-only: bitemporal reference oracle + generative cross-door harness
  (`pkg/graph/internal/core/bitemporaloracle_test.go`) — the keystone correctness
  net for the bitemporal resolver, permanently retiring the "two doors disagreed"
  bug class behind lessons 32/33/42/43/46/60. A small, deliberately-dumb O(n)
  reference model of the normative contract (effective valid-from, `[vStart,vEnd)`
  tiling, `TxFrom<=txAt` visibility with no `TxTo` bound, post-pin delete-tombstone
  normalization, newest-belief-on-overlap, predicate-anywhere interval) resolves
  query answers from version chains read BACK from the engine (real stamps, never
  guessed — sidestepping the two test-clock hazards). A seeded, deterministic
  harness (`math/rand/v2` PCG) drives random op sequences (add / backfill-add via
  `AddWithTx` / update / add-label / remove-label / `SetNodeVersionInterval`
  cascade / hard-delete) over nodes and rels (stable anchor endpoints) on BOTH
  backends, then asserts the oracle agrees with BOTH temporal query doors — named
  (`NodesAtTx`/`NodeAtTx`/`NodesAt`/`NodesDuring`) and generic (`All`/`ByLabel`/
  `ByType` with `QueryOpts{ValidAt|ValidStart/ValidEnd, TxAt}`) — across a probe
  grid clustered on recorded stamps (exact boundaries, ±1, 0, far-future). Bounded
  short run (30 seq × 20 probes, <1s); full run behind `testing.Short()` (200 × 40).
  Verified load-bearing: reintroducing lesson 60 (disable `DeletedAt`-normalization),
  lesson 43 (`TxTo<=txAt` visibility bound), or lesson 32/33/42 (prefer next
  `UpdatedAt` over `ValidFrom` in `nodeVersionBounds`) each turns the harness red;
  all restored — zero production code changed. The build surfaced a genuine
  memory-vs-badger divergence in the OUT-OF-CONTRACT `NodesAsOf`/`RelsAsOf` snapshot
  door (a cascaded-then-deleted entity: the cascade leaves the genesis row's
  `TxTo=0`, so at a post-delete pin memory's fallback scan reports the entity
  present while badger's native reverse-scan reports absent); that door is not
  cross-checked here and is recorded as a finding.

- Query-planner statistics documented as a first-class contract: new
  `docs/query-planners.md` lists every planning primitive (`NodeCount`/`RelCount`,
  `AllLabelCounts`/`AllRelTypeCounts`, `NodeCountByLabel`/`RelCountByType`,
  `NodeCountByLabelAndPropertyKey`, `RangeCardinality`, `IncomingDegree`/
  `OutgoingDegree`, `NodeMutationEpoch`/`RelMutationEpoch`) with complexity,
  staleness semantics, and the single documented `ErrCapabilityNotSupported`
  decline story for external stores. Additive alias `g.Stats().RangeCardinality(...)`
  forwards to the same core op `g.Nodes().RangeCardinality` uses (identical
  signature and semantics; `g.Nodes().RangeCardinality` itself is unchanged) —
  a planner reading only `g.Stats()` no longer needs the `nodes` sub-API for this
  one statistic. Direct tests on both memory and badger backends, including a
  decline case that also asserts the documented `graph.ErrGraphClosed` sentinel
  via `errors.Is`. The tiered cross-shard `NodeCountByLabelAndPropertyKey` fold
  already had coverage (`TestTieredStoreNodeCountByLabelAndPropertyKey` in
  `pkg/graph/store/tiered/tieredstore_property_key_counts_test.go`) — not
  duplicated.

- Added `pkg/graph/open.go`: `graph.Open(dir string, opts ...Option) (*Graph, error)` and
  `graph.OpenInMemory(opts ...Option) (*Graph, error)` as additive convenience constructors layered
  purely on top of the existing `Config`/`New` (both remain the primary, fully-documented API —
  nothing new is possible through `Open`/`OpenInMemory` that a `Config` literal could not already do).
  `Option func(*Config)` values are applied in order (later wins) after `dir` seeds `Config.BadgerDir`;
  `OpenInMemory` is defined as `Open("", opts...)` so it can never drift from `New`'s own zero-Config
  in-memory default. New building-block options `WithSnowflakeNodeID(id int)` / `WithValidation(v
  ValidationLimits)`, plus three Badger footprint profiles from `[4.8.0]`'s validated bounds (vlog
  `[1MB,2GB)`, memtable `[8MB,1GB]`, cache `>=0`, compactors `0` or `>=2`): `WithProfileSmall()`
  (64MB/8MB/32MB/2 — many-instance/embedded), `WithProfileServer()` (explicit zeros — Badger stock,
  made discoverable rather than silently unset), `WithProfileBulkLoad()` (256MB memtable/512MB
  vlog/4 compactors — pairs with `BatchBuilder.AddNodes` + `QueryOpts.NoSort` for the write/scan
  sides of a one-shot ingest). Whitespace-only `dir` fails with the identical error `New` returns
  (no sentinel exists for it — asserted via message parity, not `errors.Is`). Tests-first in
  `pkg/graph/open_test.go`.

### Docs

- README.md rewritten outsider-first: opening pitch, install snippet,
  a ~20-line quickstart, three verified-runnable hero examples (bitemporal
  read via `TagAsOf`/`NodesAsOf`, version-chain integrity + time travel via
  `VerifyNodeChain`/`NodeAt`, change-feed tail via `Replication().ForEachChange`
  with a resume LSN), a feature table, a backends table, and a short "Version
  history" section linking to `CHANGELOG.md` in place of the old multi-page
  "What's new in v4" narrative wall (all of which remains in `CHANGELOG.md`
  itself). Added `CONTRIBUTING.md` (build/test/lint workflow, the Testing
  Rules pointer, the lessons.md protocol) and `SECURITY.md` (supported
  versions, private reporting contact, hardening-posture pointers to
  `storeutil.SafeUnmarshal`, import amplification bounds, hash
  recompute-and-compare on import/replica apply, and the existing fuzz
  harnesses). No code changes.

### Added — Public CI via GitHub Actions

- `.github/workflows/ci.yml` — Continuous integration on push to main and pull requests: job "test" runs `make vet && make build && make test-race` via actions/setup-go v5 with module cache; job "lint" (pull requests only, BLOCKING) runs golangci-lint v6 with `only-new-issues: true` — findings a PR introduces fail it, the documented ~40-finding baseline is ignored. `.github/workflows/security.yml` runs gosec + govulncheck WEEKLY (plus manual dispatch) instead of per-PR: gosec has a documented baseline and no new-only mode, and govulncheck alerts track the vulnerability database, not the diff.
- Fuzzing stays OUT of CI (owner decision): the four trust-boundary harnesses (FuzzWireToNodeChecked, FuzzWireToRelChecked, FuzzUnmarshalNodeWireWithKeys, FuzzImport) and their seed corpora remain in the tree — seeds execute as ordinary tests in `make test`, and deep sessions run locally via `go test -fuzz=<target> -fuzztime=<dur>` on the harness's package. No scheduled or PR-triggered fuzz workflow exists.
- `.github/README-badge-TODO.md` — Badge markdown snippets for inclusion in README after review.

### Added — five runnable example tests for pkg.go.dev

Five examples in `pkg/graph/*_test.go` demonstrating core API usage:
- `ExampleNew`: create graph, add nodes and relationships, query by label
- `Example_temporalQueries`: two-phase temporal test (create at t0, mutate, read at t0)
- `Example_transactions`: use `g.Tx()` and `g.Batch()` for atomic and bulk operations
- `Example_changeFeed`: enable and iterate change-log records via `g.Replication().ForEachChange`
- `Example_asOfTags`: capture transaction time with `NowTx()`, tag as `"baseline"`, and query with `NodesAsOf()`

All examples have deterministic output (names/counts, no raw IDs/instants) suitable for `go test -run Example` verification and pkg.go.dev inline display.

### Added — Instant/time.Time conversion helpers

New convenience functions in `pkg/types` for working with millisecond-precision timestamps:

- `InstantFromTime(t time.Time) Instant` — convert a `time.Time` to `Instant` (Unix milliseconds). Fractional milliseconds are truncated. Automatically converts to UTC.
- `(i Instant) Time() time.Time` — convert an `Instant` to a `time.Time` in UTC. Round-trip property: `InstantFromTime(i.Time()) == i` for all Instant values.
- `(i Instant) String() string` — decimal string representation of the millisecond value (e.g. `Instant(1609459200000).String() == "1609459200000"`).
- `CoerceInstant(v any) (Instant, bool)` — coerce various types to `Instant` with type safety. Accepts `Instant`, `int64`, `int`, `float64` (integral values only), `time.Time`, and `*time.Time` (nil pointer returns `false`). All other types return `(0, false)`.

- `docs/errors.md` is a comprehensive error-sentinel reference with machine-checked coverage across all three sentinel sources: `pkg/graph` re-exports, `pkg/graph/store`-only sentinels (never aliased into `pkg/graph`, e.g. `ErrNoVersionValidAt` — returned raw by `g.Temporal().NodeAt()`/`RelAt()` — and `ErrChangesNotAscending`/`ErrCorruptWire`/`ErrStoreClosed`/`ErrVersionNotFound`/`ErrInvalidStoreMutation`), and `pkg/types` sentinels (some aliased verbatim into `graph.ErrXxx`, the rest reachable only via direct `pkg/types` API calls). The table now documents 96 sentinel rows (93 distinct sentinel names; 94 distinct sentinel identities — `pkg/types.ErrNilNode`/`ErrNilRelationship` are intentionally cross-listed under both "Entity Validation" and "pkg/types Sentinels" since `pkg/graph` aliases them verbatim, and `pkg/types.ErrInvalidTimeRange` is called out as a distinct identity from the same-named core/store/graph sentinel). New "Integrity & Wire" section groups the wire-format/corrupt-wire sentinels (reorganized out of the former "Capabilities & Format" section, now "Capabilities"); new "Store-Internal Sentinels" section covers the not-re-exported store sentinels. Test `pkg/graph/errors_doc_test.go` now inventories all three sources as separate hand-maintained lists (each with a comment naming the grep that regenerates it) and asserts every sentinel appears in column 1 of some row — the test will fail if docs drift from the exports. Additive, zero runtime cost, reference-only documentation.

- Encryption-at-rest passthrough for the badger backend. `badger.Config.EncryptionKey` (AES-128/192/256, length must be 0/16/24/32 bytes else `ErrInvalidEncryptionKeyLength`) and `EncryptionKeyRotation` (0 = Badger's stock 10-day rotation) flow through the shared `buildBadgerOptions` path via `WithEncryptionKey`/`WithEncryptionKeyRotationDuration`. New `badger.Config.IndexCacheSize` knob (mirrors `BlockCacheSize`; 0 = Badger stock default of 0 = no index cache) wired via `WithIndexCacheSize`. Verified against the live Badger v4.9.2 source and a real write/flush reproduction rather than assumed: encryption requires BOTH `BlockCacheSize > 0` (Badger panics at `Open` otherwise — `checkAndSetOptions`) AND `IndexCacheSize > 0` (Badger panics on the FIRST encrypted SSTable flush otherwise — `table.go`'s `fetchIndex`, "Index Cache must be set for encrypted workloads" — a distinct, separately-triggered failure mode from the `BlockCacheSize` one, and the one actually reachable through this library's default-preserving config semantics since Badger's own `IndexCacheSize` stock default is 0, unlike `BlockCacheSize`'s nonzero 256MB). `New()` validates both up front (`ErrEncryptionRequiresBlockCache` / `ErrEncryptionRequiresIndexCache`) so neither panic can ever escape the library. Reopening an encrypted dir with the wrong key, or applying a key to a previously-plaintext dir, fails `New` with an error wrapping `badgerv4.ErrEncryptionKeyMismatch` (errors.Is-able) — Badger detects both by decrypting a sanity marker in its `KEYREGISTRY` file at open, before any row is read; the rejected attempt does not touch existing plaintext data. `graph.Config` gains matching `EncryptionKey`/`EncryptionKeyRotation`/`IndexCacheSize` pass-through fields (applied when the badger store is built internally, ignored when `Store` is supplied explicitly); `tiered.Config` gains the same fields, threaded through `badgerCfg` to every shard (reference, hot, warm, lazy cold/archive, rotation-created). New tests: `pkg/graph/store/badger/badgerstore_encryption_test.go` (same-key round trip on a real `t.TempDir()` dir with a forced flush, wrong-key and key-on-plaintext-dir failure modes, a full validation table incl. both cache guards, sentinel `errors.Is` coverage, `EncryptionKeyRotation` applied-to-live-options check) and `pkg/graph/store/tiered/tieredstore_encryption_test.go` (multi-shard smoke across reference/hot/warm shards with a real rotation and reopen, wrong-key failure, validation-at-`New` table, per-shard live-options check). `IndexCacheSize` also added to the existing `TestBadgerTuningBoundaries` boundary table and the `graph.Config` passthrough test.

## [4.11.2] - 2026-07-04

### Added — `Temporal().NowTx()`: the current transaction-time pin (lesson 61)

`g.Temporal().NowTx() (types.Instant, error)` returns the graph's current
transaction-time instant — the pin a caller hands to the AS-OF reads
(`QueryOpts.TxAt` / `NodeAtTx` / `NodesAtTx` / `NodesAsOf`) or names via `TagAsOf`
to snapshot "everything committed so far". Every entity already committed has
`TxFrom <= NowTx()`, and every subsequent mutation is stamped strictly greater, so
pinning at `NowTx()` includes all prior writes and excludes every later one. This
closes a real gap: a consumer had no way to **obtain** a current transaction
instant — every temporal reader took one as input, none returned it, so `TagAsOf`
(which needs an `at`) and any "snapshot as of now" had to hand-derive a pin, unsafely.

Semantics (probed in `pkg/graph/internal/core/nowtx_test.go` — both backends,
node+rel parity, `-race`): reading `NowTx()` **advances the commit clock by one
tick** (reserving the instant) — that reservation is what makes the separation
strict (the next mutation is `> NowTx()`), and because it consults the same
monotonic-floor clock mutations use (`Core.now()`), not a bare wall-clock read, the
value is **correct across a Close/reopen**. The session high-water mark
(`lastInstant`) resets to 0 on reopen, so a pure `lastInstant.Load()` would return
0 — an `AS OF SYSTEM TIME 0` = "no filter" pin that would silently INCLUDE future
writes, the exact anachronism the pin prevents; a bare wall-clock pin is likewise
unsafe because a burst of mutations can outrun the wall. Errors on a nil/closed
graph. Additive, backward-compatible: new method on `g.Temporal()` and its internal
`Ops` interface. See lesson 61.

## [4.11.1] - 2026-07-04

### Fixed — hard Delete is a transaction-time tombstone in the generic TxAt door (lesson 60)

A hard-deleted entity vanished from every **generic** transaction-time read at
EVERY pin — `ByLabel`/`ByType`/`All` with `QueryOpts.TxAt`, `NodesAtTx`/`RelsAtTx`,
and `NodeAtTx`/`RelAtTx` — even at pins **before** the delete, while the **named**
as-of door (`NodeAsOf`/`NodesAsOf`/`RelAsOf`/`RelsAsOf`) correctly reconstructed the
pre-delete belief state (rule 17: two doors, same shape, one had the bug). That was
retroactive history rewriting: a delete recorded at T+1 silently changed what "as
known at T" returned, contradicting `docs/architecture.md`'s "past-time queries
reconstruct deleted entities from history" and breaking downstream `AS OF SYSTEM
TIME` reproducibility (found by sigma-tkgd's Tyla transaction-time pinning, probed
2026-07-03).

Mechanism: Delete stamps `DeletedAt`/`ValidTo`/`TxTo` **in place** on the final
version before moving it to history, so the tombstone row — correctly kept by the
`TxFrom <= txAt` visibility predicate (lesson 43) — carried a post-pin
`ValidTo = DeletedAt` that failed valid-time coverage in the resolver
(`nodeVersionBounds`' `vEnd = ValidTo` override), dropping the entity.

Fix: `filterNodeChainByTxAt`/`filterRelChainByTxAt` — the one seam every
chain-based TxAt resolution funnels through (`nodeAtLockedTx`/`relAtLockedTx` and
`find*VersionMatchingDuringTx`, node + rel) — now detect a surviving row whose
`DeletedAt` post-dates `txAt`, DEEP-COPY it (chain rows may be shared frozen store
rows — never mutated), and normalize it to the belief state as of the pin via the
same `normalizeTemporalVisibleAtTxTime` the named as-of door already applies. The
two doors now agree by construction. Pins at or after the delete keep excluding the
entity (its delete-stamped valid time no longer covers the query instant).
Regressions in `bitemporal_tombstone_test.go`: both backends, node + rel mirrors
(direct delete AND `DeleteNode` cascade tombstones), supersession-then-delete
(pinned read returns the belief current at the pin), post-delete-pin negative
assertions, ValidAt+TxAt combination, and no-delete-stamp assertions on returned
rows. Also documents the two test-clock hazards (monotonic-floor stamps outrunning
the wall clock vs the TxAt-only door's implicit valid-at-wall-now probe) that made
naive sleep-based pins flaky.

## [4.11.0] - 2026-07-02

### Added — transaction-time backfill at ingest (§4.1)

`TxFrom` (the Erkenntniszeit / knowledge time — "the DB recorded this fact at T")
is normally system-stamped with the monotonic clock on every create. That makes
a fresh re-ingest un-reproducible: loading a dataset today stamps `TxFrom = load
time`, so a documented historical knowledge state (e.g. **2026-01-15 12:00**) is
not addressable via `AS OF SYSTEM TIME`. A new **privileged, gated backfill door**
closes that gap for a controlled re-ingest.

- **`Config.AllowTxBackfill bool`** (off by default) is the audit/import-scope
  gate. When set, the create doors honor a caller-supplied transaction time and
  stamp it as the entity's `TxFrom` instead of `c.now()`. In production (gate
  off) any backfill attempt is rejected with the new **`ErrTxBackfillDisabled`**.
- **`g.Nodes().AddWithTx(ctx, labels, props, txFrom)`** and
  **`g.Rels().AddWithTx(ctx, type, start, end, props, txFrom)`** are the explicit
  ergonomic doors; `txFrom` must be a positive instant **not in the future**
  (**`ErrInvalidTxFrom`**) — a knowledge/transaction time is "when the DB learned
  a fact" and cannot exceed wall-clock at write, and a future value (e.g. from
  `Instant`'s Unix-**milli** unit vs the snowflake layer's **micro**second unit)
  would stamp a genesis whose TX interval INVERTS once it is superseded
  (`TxFrom > TxTo`), rendering it invisible to every AS-OF query yet still passing
  `Verify*Chain` (TxFrom is not hashed). The bound is checked at every create door.
- The carrier is the reserved property **`tkg_tx_from`** (now caller-settable on
  create doors under the gate, previously always system-only). It is extracted in
  the one shared `extractTemporal` helper, so backfill lands uniformly across the
  WHOLE create-door family — standalone node `Add`/`Import`, relationship
  `Add`/`Import` (via the create kernel), and the `BatchBuilder` node/rel create
  paths (per-entity distinct backfill instants supported). `AddWithTx` simply
  injects `tkg_tx_from` and reuses `Add`.
- **Create-only by design.** Updates, deletes, label/property mutations, and
  version-interval corrections keep the monotonic system `TxFrom` (a correction
  recorded now is stamped now — lessons 20/46); `tkg_tx_from` on an Update is
  rejected as a reserved key. `TxFrom` is not part of the integrity hash, so a
  backfilled row still passes `Verify*Chain` and replicates VERBATIM through the
  existing replica-apply / delta pipeline (lesson 52 reproduces `TxFrom` exactly).
  The change-log LSN stays monotonic-by-commit; only the stored `TxFrom` is
  backdated — the two axes stay independent and correct. A batch node that
  survives a partial-failure (partial-live) create emits its `EventNodeCreate`
  with the real commit clock, never the backdated backfill instant (the persisted
  `TxFrom` is the backfill value; the EVENT timestamp is wall-clock).

### Added — named knowledge-time (Erkenntniszeit) marks (§4.2)

A small durable alias registry so `AS OF SYSTEM TIME $tag` can run against a
documented, stable knowledge time by name instead of a magic number.

- On `g.Temporal()`: **`TagAsOf(name, at)`**, **`ResolveAsOf(name) (Instant, bool,
  error)`**, **`AsOfTags() map[string]Instant`**, **`RemoveAsOfTag(name)`** (remove
  is idempotent). `at` is a transaction-time instant used with `TxAt` /
  `AS OF SYSTEM TIME`. New sentinels **`ErrInvalidAsOfTag`** (blank name /
  non-positive instant / over-length) and **`ErrTooManyAsOfTags`** (bounded at
  4096 marks).
- Stored as one msgpack map under a new `asof_tags` MetaKV key (decoded through
  `storeutil.SafeUnmarshal` — a corrupt blob fails closed, never panics). The load
  door RE-VALIDATES each decoded entry against the write-door invariants (positive
  instant, non-blank name) and fails closed with `ErrCorruptWire`; a non-positive
  instant is especially dangerous because `Instant(0)` aliases the "no TX filter"
  sentinel, so an un-checked corrupt entry would silently resolve to CURRENT belief
  instead of the named knowledge time. Durable
  across a Close/reopen of the same data dir; reaped by `Admin().Reset()` on every
  backend (badger's scan-reap already covers it, and core now reaps it explicitly
  so memory — whose `Clear` preserves `metaKV` — is consistent). Requires a
  MetaKV-capable backend (every in-tree store); declines with
  `ErrCapabilityNotSupported` otherwise. Rejected on a read-only replica
  (`checkWritable`); reads stay open (`checkOpen`). Not carried by Export/Import —
  tags are store-local metadata, like the graph epoch and the failover lease.

Both features are fully backward-compatible: the gate is off by default and no
existing call site changes behavior. New sentinels are re-exported from
`pkg/graph`. See lesson 59.

### Added — dev tooling (dockerized lint/security/vulncheck)

`golangci-lint`, `gosec`, and `govulncheck` are often not installed on the host,
so `make lint` / `security` / `vulncheck` fail with "command not found". New
Makefile targets run them inside the go.mod-matching `golang:<version>` Docker
image (guaranteeing Go-version compatibility) with cached named volumes:
`make lint-docker` / `security-docker` / `vulncheck-docker`, and `make ci-docker`
for the full gate. `GO_IMAGE` auto-tracks the `go` line in `go.mod`. Documented in
CLAUDE.md ("Running lint/security/vulncheck via Docker"). No library change.

## [4.10.3] - 2026-06-30

### Fixed — deep break-rounds campaign (fuzz / crash-fault / concurrency)

A second 3-round adversarial campaign over the previously un-mined lenses
(change-record/apply decoder fuzz, crash-fault & watermark-ordering torn state,
deep concurrency) found two more defects on the v4.10.x surface; both fixed
test-first.

- **Apply-path wire rejection escaped `ErrCorruptExport` classification (LOW).**
  In `applyChangeRecordLocked`'s doors, a `storeutil.WireTo{Node,Rel}Checked`
  field-validation failure (e.g. a record whose decoded wire has `id == 0`)
  returned a bare error, unlike the bootstrap-import path and the sibling
  `verifyImported*Hash` check, both of which wrap `ErrCorruptExport`. A consumer
  classifying replica-apply / `ImportMerge` failures via
  `errors.Is(err, ErrCorruptExport)` mis-classified this untrusted-stream
  rejection. All seven `WireTo*Checked` apply sites now wrap `ErrCorruptExport`.
  Pinned by a new sub-case of `TestReplicaApply_CorruptRecordFailsClosed`.
- **`PropertyKeyRegistry.GetOrCreate` admitted blank (all-whitespace) keys while
  `AppendNames` rejected them (MEDIUM-consistency).** The create door guarded only
  `key == ""`, but the grow door (and the Label/RelType registries' three doors)
  reject all-whitespace via `isBlankName` — so a blank key the create door minted
  could not pass the grow door. `GetOrCreate` now rejects blank via `isBlankName`
  (write-safe: the wire encoder already ignores this error and falls back to the
  raw key on token 0, so a degenerate blank key still round-trips, it just never
  enters the token table). `ImportNames` stays intentionally lenient (empty-only)
  so a registry that tokenized a blank key before this guard still loads. Pinned
  by `TestPropertyKeyRegistry_BlankNameRejectedByCreateAndGrow` and
  `TestPropertyKeyRegistry_ImportTolerantOfLegacyBlankKey`. Lesson 58.

### Added — deep break-rounds coverage (no behavior change)

- The durable applied-LSN watermark never leads the durable data: a flaky-flush
  store decorator injects a post-apply flush failure and asserts `ApplyChange`
  returns the error with the watermark unmoved, then converges on a clean retry —
  making the flush-before-watermark crash invariant (previously "not expressible
  at the graph façade") executable. `TestReplicaApply_FlushFailureDoesNotAdvanceWatermark`.

## [4.10.2] - 2026-06-30

### Fixed — break-rounds campaign on the change-log / replica / delta surface

A 3-round adversarial break-the-system campaign (per-tx change-log buffer, replica
apply, commit-window overlay, `Clear()` LSN-reuse, delta export/merge) found two
real defects in the released v4.10.0/v4.10.1 surface; both are fixed test-first.

- **Delta-merge rollback silently dropped an untouched relationship (HIGH).** A
  node whose ONLY delta record was a bare `ChangeNodeHistoryVersion` /
  `ChangeNodeHistoryTruncate` (e.g. a `SetNodeVersionInterval` cascade on a
  bounded PAST interval, which leaves the open-ended current row untouched) had
  its adjacency NOT captured by `captureMergeRecord` — unlike `ChangeNodePut` /
  `ChangeNodeDelete`. A corrupt/rolled-back merge then purges that node with
  `DeleteNodeCascade` (dropping ALL its edges) and re-creates only the edges it
  captured, so an edge the delta never touched was destroyed and never restored —
  violating the module's own stated capture invariant. Both history branches now
  call `captureNodeAdjacency`. Pinned by
  `TestImportMerge_HistoryVersionCorruptionPreservesEdges`.
- **`MaxNodeHistoryID`/`MaxRelHistoryID` disagreed with `AllNodeHistoryIDs` during
  the badger commit window (MEDIUM).** When the only history key for an id lived in
  `flushing` as a SET and a pending DELETE masked that exact key (a mid-commit
  concurrent trim) with Badger empty, `maxHistoryID` bumped the max from the
  flushing SET but the pending DELETE only populated `pendingDeletes` (applied to
  the Badger scan alone), so it never un-bumped the buffer-derived max —
  `Max*HistoryID` returned the deleted id while `All*HistoryIDs` (via
  `pendingHistoryIDOverlay`) correctly returned none. `maxHistoryID` now resolves
  set-vs-delete PER KEY exactly like `pendingHistoryIDOverlay`. Pinned by
  `TestFlushingOverlay_PendingDeleteMasksMax_{Node,Rel}DoorsAgree`.

### Added — break-rounds coverage (no behavior change)

- Replica apply reproduces a primary row's temporal metadata (`TxFrom`/`ValidFrom`)
  VERBATIM, asserted by reading the applied row's `Temporal()` directly — the
  integrity-hash convergence oracle is blind to those fields
  (`TestReplicaApply_ReproducesTemporalVerbatim`).
- At-least-once redelivery of a WithHistory label-add `NodePut` no-ops via the
  `nodeWireMatches` guard before the label-add door would reject the already-present
  token (the existing idempotent-reapply test is watermark-gated and never reaches
  it) — `TestReplicaApply_LabelAddRedeliveryNoOps`.
- A badger store opened with `ChangeLog:false` fails closed on `Watermark` /
  `ExportSince` with `ErrCapabilityNotSupported` (present-but-disabled, never a
  silently-empty delta) — `TestExportSince_BadgerChangeLogDisabledFailsClosed`.

## [4.10.1] - 2026-06-30

### Fixed — change-log / replica hardening + per-transaction change-log buffer


Three defects found while reviewing the Phase-0/1 op-log + replica working tree.

- **Badger commit-window overlay was incomplete and leaked.** `flush()` releases
  `idxMu` BEFORE the WriteBatch commit, so for the whole commit window a
  just-written row is gone from the `pending` buffer and not yet visible in a
  Badger `View`. A `flushing` map parks the swapped-out snapshot so overlay
  readers still see those rows during the window — but only ONE reader
  (`pendingHistoryIDOverlay`) had been routed through it, and the snapshot was
  never cleared on a successful commit (nor by `Clear()`). Consequences: history
  point reads (`GetNodeVersion` / `GetRelVersion`), history scans (`GetNodeHistory`
  / `GetRelHistory`, `MaxNodeHistoryID` / `MaxRelHistoryID`, truncate/trim
  retention sets, the `AsOf` version-chain overlay) and the on-disk label /
  adjacency overlays (`LabelIndexOnDisk` / `AdjacencyIndexOnDisk`) momentarily
  DROPPED an in-flight row; and a stale snapshot left after a successful flush let
  `Clear()` RESURRECT phantom history IDs the wipe had removed (visible to history
  scans, history export, and the import empty-probe). Now every overlay reader
  consults both buffers via `rangePending` / `lookupPending` (the latter had been
  dead code), the cascade-delete and incoming-repair MUTATORS that compute delete
  sets also consult `flushing` (otherwise they orphan a persisted index key during
  the window), and `flushing` is cleared on commit success, on `Clear()`, and on
  the failed-flush requeue. Memory and tiered backends are unaffected (no async
  commit window). See lesson 54.
- **`Clear()` could reseed the LSN allocator to 0 after a crash.** With the
  change-log enabled, `Clear()` now wipes via `DropPrefix` while keeping
  `LastLSNKey` continuously durable (`clearAndReanchorChangeLog`) instead of
  `DropAll` + a separate marker write — so a crash mid-`Clear` can no longer leave
  the store with neither change-log records nor a watermark, which would reseed
  the LSN allocator to 0 and silently strand a tailing consumer below its
  pre-`Clear` checkpoint. Stale entity-counter meta keys are deleted BEFORE the
  data drop so a mid-`Clear` crash never leaves a counter claiming rows that no
  longer exist. The change-log-**disabled** `Clear()` arm now ALSO preserves
  `LastLSNKey` whenever a prior change-log-enabled session left one — it used a
  bare `DropAll`, which wiped the watermark, so a `ChangeLog`-on → reopen
  `ChangeLog`-off + `Clear` → reopen `ChangeLog`-on sequence reseeded the LSN
  allocator to 0 and reused LSNs a consumer had already checkpointed past (the same
  silent-divergence hole through a sibling door). Both `Clear()` arms now share one
  `clearDataPreservingLastLSN` helper; only a never-logged store still takes the
  atomic `DropAll`. See lesson 53.
- **`ApplyChanges` silently swallowed a misordered batch.** New sentinel
  **`store.ErrChangesNotAscending`**: a record whose LSN is not strictly greater
  than the previous record's now stops the batch (the strictly-ascending prefix is
  still flushed and watermarked) instead of being skipped by the already-applied
  watermark check, which would leave a permanent gap in the replica's coverage.
- **A rolled-back transaction that allocated a new token poisoned the change-log
  feed and permanently stalled replicas (fixed).** A tx that allocates a NEW
  label/rel-type token emits a durable `ChangeNodePut`/`ChangeRelPut` referencing
  it, then `Rollback()` (`restoreRegistries`) DE-allocated the token by rebuilding
  the registry from the pre-tx snapshot — so the feed permanently referenced a
  token the primary no longer held. A replica, **even with a `ReplicationSource`**,
  could never resolve it (the refetch finds it absent — the primary rolled it back
  too) and stalled forever at that LSN; the number was then reused for a different
  name → silent divergence. The same poison existed in the whole token-deallocation
  family: standalone `restoreNewLabelsOnError`/`restoreNewRelTypeOnError`, the batch
  partial-failure path (deletes already-created nodes whose puts referenced the
  token, then de-allocates), and index creation. **Fix:** the token registries are
  now APPEND-ONLY across rollback when the change-log is enabled — the de-allocation
  chokepoints keep tx-allocated tokens (already persisted; an unused token is
  harmless), so every emitted feed record stays resolvable and replicas converge;
  log-off behavior (exact rollback) is unchanged. The `store.ChangeLogStatusCapability`
  optional probe (`ChangeLogEnabled()`) gates it — `changeFeed != nil` is NOT a
  valid signal (the backends always implement the feed methods). The
  `getOrCreate*` persist-failure rollbacks are intentionally not gated (they fire
  before any record is emitted). Pinned by convergence + white-box tests; see
  lesson 55. The residual physical-redo-log property (rolled-back ENTITY churn
  still ships; replicas converge but transiently materialize the uncommitted
  entity; feed is not a logical committed-tx CDC source) is documented, not a bug.
- **Per-transaction change-log buffer (proper fix; supersedes the stopgap for the
  TX path).** New optional `store.TxChangeLogScope` capability (`BeginLogScope` /
  `SetLogDivert` / `CommitLogScope` / `DiscardLogScope`, on badger + memory). A
  `GraphTx` now BUFFERS the change-log records its mutations produce and emits them
  only on `Commit` (with LSNs minted AT commit, so a rolled-back tx burns none and
  leaves no feed gap) / DISCARDS them on `Rollback` — mirroring `txEventBuffer` for
  events. So a **rolled-back tx emits NOTHING** to the feed: no token-poison (the
  tx `restoreRegistries` reverts to EXACT pre-tx-registry rollback again — the
  append-only stopgap is no longer needed there), no transient replica phantom, no
  CDC-asymmetry residual. The concurrency lever: a change-log-enabled tx takes
  `c.mu.Lock` (EXCLUSIVE) **per-mutation** (not per-lifetime — no in-tx-read
  deadlock) and toggles diversion under it, so a concurrent standalone mutation
  (shared `c.mu.RLock`) can never have its own record misrouted into the tx's
  buffer (pinned by a `-race` test, fail-first verified). Gated on the change-log
  being enabled — non-change-log graphs are untouched (full Path B write
  concurrency). `Batch.Execute` ALSO uses the scope (it holds the global write lock
  for its whole duration, so diversion stays on the whole batch); because a batch
  KEEPS its successful ops on a partial failure it always COMMITS the scope (records
  emitted atomically at batch-end), and the failed-op create+delete cleanup churn
  converges. `IO().Import` rollback is guarded by the append-only stopgap (gated on
  the change-log being enabled); a read-only replica's bootstrap import emits
  nothing anyway. The standalone token-deallocation path and the batch/import paths
  keep the append-only stopgap (they emit through-the-scope-then-commit or eagerly,
  so de-allocating a referenced token would poison). The accepted residual: a
  change-log-enabled tx's DATA still
  flushes during the body (records buffered), so a crash between a mid-tx data
  flush and commit can leave committed-but-unlogged data — invisible to the feed
  (watermark not advanced), and no worse than a tx's pre-existing SyncWrites
  non-atomicity. See lesson 55.
- **Replica apply fails closed on a multi-token label diff.** A `NodePut` whose
  label set differs from the replica's local row is routed to the matching
  label-token door; a correct contiguous feed only ever differs by ONE token (each
  label-token door mutates one token). The diff helper previously collapsed a
  multi-token difference to one arbitrary token, so a malformed/gapped feed could
  silently apply one token and leave the label index diverged from the row. The
  apply path now reports the added/removed counts and rejects any diff that is not
  exactly one token (defense-in-depth at the apply trust boundary).


## [4.10.0] - 2026-06-27

### Fixed — hash-chain verification rejected bitemporal-cascade-corrected graphs

`VerifyNodeChain` / `VerifyRelChain` (and therefore the import trust boundary,
full `Export`+`Import`, replica apply, and the new delta merge) assumed a strictly
LINEAR hash chain (`v_k.PrevHash == v_{k-1}.Hash` in version order). But a
bitemporal correction (`SetNodeVersionInterval` / `SetRelVersionInterval`) appends
a version whose `PrevHash` links to whichever version it supersedes ON THE
VALID-TIME AXIS — a hash DAG, exactly as `temporal_cascade.go` documents. So any
graph that used a cascade correction failed its own chain verification, and a
full `Export`+`Import` of it failed with `ErrCorruptExport` — i.e. **such graphs
could not be backed up/restored at all**. Fixed: linkage is now verified as
"every non-genesis version's `PrevHash` matches the hash of SOME version in the
set" (genesis carries an empty `PrevHash`; the lowest retained version may dangle
for truncation), while the per-version CONTENT-hash recompute (the tamper
evidence) is unchanged — so corrupt/garbage `PrevHash` and altered content are
still detected. Applies to both the in-memory (rows) and badger (paged) verify
paths. See lesson 52.

### Added — delta export / merge: node-level incremental backups (`ExportSince` / `ImportMerge`)

Incremental backups built on the Phase-0/1 change-log: instead of re-exporting a
whole graph when one node changes, ship only what changed since a cursor and
merge it onto a base. The public surface lives on `g.IO()`.

- **`g.IO().Watermark() (Cursor, error)`** returns an opaque `io.Cursor`
  (`{LSN, Epoch}`) marking the graph's current change-log point. `Cursor.Epoch`
  is a durable per-graph lineage id (`meta/graph_epoch`, minted on first use) so
  a cursor from a different graph (e.g. a tenant restored from scratch) is
  detectable rather than silently producing a wrong delta.
- **`g.IO().ExportSince(w, since Cursor) error`** writes a DELTA stream — only
  the mutations committed after `since` — framed exactly like `Export` (delta
  header, full token registry, then one change record per committed mutation,
  the change-log records carried verbatim). The body is produced under the export
  write lock against one consistent point. Returns **`ErrCursorUnknown`** when
  the cursor is from another graph (epoch mismatch) or ahead of the log (the
  caller falls back to a full `Export`); declines with a wrapped
  `store.ErrCapabilityNotSupported` when the graph has no change-log.
- **`g.IO().ImportMerge(r, MergeOptions) error`** merges a delta onto a base,
  reproducing the source's rows VERBATIM by replaying each change record through
  the same foreign-ID apply doors the replica-apply path uses (`WireTo*Checked` →
  token-in-registry validation → property-limits → hash recompute-**and-compare**,
  then a full hash-chain verification of every touched entity). Unlike `Import`
  it does not require an empty target. The delta's registry is **append-only
  merged** onto the base (bidirectional prefix check — a re-applied OLDER delta
  is a no-op, not a divergence). Applying the same (or overlapping) delta more
  than once is **idempotent**. `MergeOptions.ExpectBase` asserts the delta's
  `From` cursor (proving restore-chain order → **`ErrDeltaBaseMismatch`**);
  `MergeOptions.Strict` turns an update/tombstone for an entity absent from the
  base into `ErrDeltaBaseMismatch`. Any failure rolls the touched subgraph back
  to its pre-merge state (purge-and-recreate from captured snapshots).
- **`io.HeaderOf(r) (DeltaHeader, error)`** decodes only the leading header
  record (no entity/change records consumed), so a caller can route a stream
  (full vs delta) and read the base cursor before merging. A FULL export's header
  now also carries the lineage epoch, so `HeaderOf(fullExport).To = {SnapshotLSN,
  Epoch}` is a complete, gapless base cursor for the first delta in a chain.
- **Wire**: a new delta export-record tag (`0x07`, `changeRecordWire` = LSN +
  change tag + verbatim payload); the export header gains additive, omitempty
  delta fields (`IsDelta`, `From*/To*`, `Epoch`) — a full export stays byte-
  identical to v2 (fields zero/omitted). Delta streams stamp format version 3;
  importers accept v1–v3, and a v2 reader fails closed on a v3 delta.
- **Why the change-log, not a temporal diff**: the cursor is the change-log LSN
  (monotonic, store-owned), NOT valid/transaction time. A valid-time diff would
  silently drop **backdated/backfilled** writes from a backup. The change records
  are the primitive mutations the apply path already replays correctly (label-
  token changes, cascade deletes, history versions), which a state-endpoint diff
  cannot reconstruct. Requires a change-log (`graph.Config.ChangeLog` on badger,
  or an injected `memory.WithChangeLog()`); declines on the tiered backend.
- **Fixed (shared apply path)**: a WithHistory put record regenerated the
  superseded prior version from the local current but did NOT restore its `TxTo`
  (= the new version's `TxFrom`), so a replica / delta-merge was only HASH-exact,
  not BYTE-exact, in that one non-hashed field. `applyChangeRecordLocked` now
  reproduces it, making replication's "byte-exact temporal metadata" claim
  actually hold. `Export` and `ExportSince` now flush the async write buffer
  before reading the change feed / `SnapshotLSN`, so a badger source with
  buffered writes no longer stamps a stale (0) snapshot LSN (a replica / first
  delta would otherwise resume from 0 and re-ship the whole graph).
- **New optional `store.ChangeLogStatusCapability` (`ChangeLogEnabled() bool`)**,
  implemented by memory + badger: `ExportSince`/`Watermark` fail closed when the
  change-log is present-but-disabled (the in-tree backends expose the feed methods
  unconditionally), so a delta is never silently empty.
- **Sentinels**: `ErrCursorUnknown`, `ErrDeltaBaseMismatch` (re-exported from
  `pkg/graph`). See lessons 52.

### Added — log-shipped read replicas: replica apply engine + read-only gate (horizontal-scaling Phase 1)

Read scale-out and HA on top of the Phase-0 change-log: a graph can now be
opened as a **log-shipped read replica** that bootstraps from a snapshot and
tails a primary's change-feed to a **byte-exact** copy. See
`tasks/horizontal-scaling.md`.

- **`g.Replication().ApplyChange(rec)` / `ApplyChanges(recs)`** apply change-log
  records received from a primary's feed, reproducing the primary's rows
  **exactly** — the supplied wire is written VERBATIM through foreign-ID store
  doors, so the integrity hash, version, `TxFrom`, and all temporal metadata are
  reconstructed from the record, never re-minted or re-stamped. The apply path
  reuses the import trust pipeline per record (`SafeUnmarshal` → `WireTo*Checked`
  → token-in-registry validation → property-limit validation → hash
  recompute-**and-compare**) and is **idempotent** under at-least-once
  redelivery (an identical row is a no-op; a delete tolerates a missing entity).
  `AppliedLSN()` / `SetAppliedLSN(lsn)` are the durable replica watermark
  (`meta/replica_applied_lsn`), distinct from the store's own `last_lsn`; a
  replica resumes tailing from `AppliedLSN()` after restart.
- **`graph.Config.ReadOnlyReplica bool`** marks a graph as a replica: every USER
  mutation door (node/rel create, update, delete, label/property mutation,
  version-interval edits, index/schema management, `Tx().Begin`, `Batch.Execute`,
  `Admin().Reset`) fails closed with the new **`ErrReadOnlyReplica`** sentinel,
  while reads, the bootstrap importer (`g.IO().Import`), and the apply path stay
  open. It is a CORE-layer gate (`c.checkWritable()`), deliberately NOT the
  badger `ReadOnly` store mode — which would disable the change-log and the
  apply path's own writes.
- **`ChangeNodePut` / `ChangeRelPut` reshaped** (Phase-0 record format, still
  unreleased): the body is now `storeutil.NodePutBody` / `RelPutBody` carrying
  the new-current wire plus a **`WithHistory` bit** that records whether the
  originating door wrote a version-history row (`ReplaceNodeWithHistory` /
  `*NodeLabelTokenWithHistory`) versus an in-place replace or create. A replica
  needs this one bit to reproduce the primary's history depth exactly: with it
  set it applies `ReplaceNodeWithHistory` (regenerating the exact prior row from
  its in-sync local current, which equals the primary's pre-mutation state by
  LSN ordering); without it, `ReplaceNode` (no history) or `PutNode`. The
  put-record wire is now built **untokenized** (`NodeToWireChecked`, property
  keys as strings) in BOTH backends, so badger and memory feeds are now
  byte-identical even for **property-bearing** entities (closing the Phase-0
  property-key tokenization caveat) and the records carry no property-key
  registry dependency. Create-vs-update is inferred from local existence; a
  label mutation is detected by diffing the local label set against the wire and
  routed to the matching label-token door.
- **Bootstrap + tail flow**: bootstrap a replica with `g.IO().Import` from a
  primary export (registry + entities + history), `SetAppliedLSN` to the
  snapshot's `LastCommittedLSN`, then loop `ForEachChange(AppliedLSN())` →
  `ApplyChange`. Records are total-ordered by LSN, so the replica's local
  current row always equals the primary's pre-mutation state when a record
  applies — the invariant that makes regenerated history rows byte-exact.
- **Tests**: end-to-end byte-exact convergence (bootstrap + tail covering
  create / with-history update / in-place update / label add+remove / new rel /
  rel with-history update / connected-node cascade delete, asserted by integrity
  hash + version + full per-entity version history, including the deleted node's
  tombstone chain); idempotent double-apply (history depth unchanged); read-only
  gate parity across every mutation door with `errors.Is(ErrReadOnlyReplica)`.

Phase-1 base layer completed (this increment):

- **Gapless bootstrap→tail handoff** — the export header (bumped to format v2,
  importers accept v1 **and** v2) carries `SnapshotLSN`, the change-log LSN
  captured under the SAME `c.mu.Lock` as the entity snapshot; import records it
  as the replica's initial applied watermark (flush-before-watermark), so a
  bootstrap no longer needs a separate, racy `LastCommittedLSN()` call. Sources
  with no change-log export `SnapshotLSN` 0.
- **Lazy token-registry refetch** — a label / rel-type the primary registers
  AFTER a replica's bootstrap snapshot is now resolved automatically instead of
  failing closed. `g.Replication().RegistrySnapshot()` returns the primary's
  token registries + the LSN they are complete as-of (`store.RegistrySnapshot{Labels,
  RelTypes, PropKeys, CapturedAtLSN}`; the LSN is read BEFORE the names so it can
  never exceed their coverage). A new `store.ReplicationSource` interface
  (`graph.Config.ReplicationSource` / `g.SetReplicationSource`; a primary's
  `g.Replication()` satisfies it directly in-process) lets the replica's apply
  path, on an unresolved token, refetch the primary's registry, guard
  `CapturedAtLSN >= rec.LSN`, **append-only-extend** its own registry, persist,
  and re-validate. Two new sentinels classify a refetch failure for a driver:
  `ErrPrimaryRegistryStale` (retryable — the primary hasn't caught up) and
  `ErrRegistryDiverged` (fatal — the replica's names aren't a prefix of the
  primary's; re-bootstrap). The new `(*LabelRegistry|*RelTypeRegistry|*PropertyKeyRegistry).AppendNames(prefix, suffix)`
  grow primitive (prefix-equality-checked; `(false, nil)` on divergence) is the
  registry-internal mechanism (`ImportNames` is load-only — it rejects a
  non-empty registry). A persist failure rolls the in-memory grow back so the
  registry never runs ahead of durable storage. Property keys are NOT synced
  (records carry untokenized string keys; the replica tokenizes locally).
- **Failover ID-slot lease** — `store.IDSlotLeaseRecord` + `g.Replication().IDSlotLease()` /
  `SetIDSlotLease()` persist/read the orchestrator's durable snowflake-slot
  assignment (MetaKV; `SafeUnmarshal` on read; slot range-validated; last-writer-wins,
  NOT a consensus primitive — the orchestrator serializes writes). Promotion is
  by reopen: the orchestrator reads the lease, `Close()`s, and `New()`s with
  `Config{SnowflakeNodeID: lease.Slot, ReadOnlyReplica: false}` (the snowflake
  generators are built only in `New`, so promotion is always a reopen). A
  promoted node and the node it replaces hold different slots, so their minted
  IDs never collide.

Still deferred (the network/orchestration half, in sigma): the Bolt ROUTE
per-role lists, the session read-your-writes watermark, and the promotion
orchestration that decides slots and triggers the reopen. rho-tkg exposes the
primitives; sigma drives them.

### Added — durable ordered change-log (op-log) + `g.Replication()` (horizontal-scaling Phase 0)

The topology-agnostic foundation for horizontal scaling: a durable, ordered
**change-log (op-log)** that records every committed mutation, usable today as
change-data-capture, an audit trail, and point-in-time recovery, and as the
basis for read-replica streaming (Phase 1). See `tasks/horizontal-scaling.md`.

- **`graph.Config.ChangeLog bool`** (opt-in, default off — zero overhead) enables
  the change-log on the badger-backed store. Every committed mutation appends a
  framed record under a new `KeyChangeLog` (`0x09`) keyspace, tagged with a
  **monotonic cluster LSN**, in the **same Badger `WriteBatch` as the data and
  counters** — so a record and the mutation it describes commit atomically (no
  committed-but-unlogged window). The LSN allocator is seeded from a durable
  `meta/last_lsn` watermark at open, so LSNs continue strictly monotonic across
  restart and are never reissued.
- **`g.Replication()`** is a new nil-safe sub-API exposing
  `ChangeFeed(afterLSN, limit)`, `ForEachChange(afterLSN, fn)` (OOM-safe;
  callback runs outside store locks), and `LastCommittedLSN()` (the
  read-your-writes watermark). It forwards to the new optional
  `store.ChangeFeedCapability`; backends without a change-log (tiered) return
  `ErrCapabilityNotSupported`.
- **Record taxonomy** (`store.ChangeTag` / `store.ChangeRecord`): `NodePut` /
  `RelPut` (new current state), `NodeDelete` / `RelDelete` (hard-cascade vs
  with-history sub-kinds), `NodeHistoryVersion` / `RelHistoryVersion`
  (explicit-version writes — import, bitemporal-cascade correction, migration,
  tx-rollback restore), `NodeHistoryTruncate` / `RelHistoryTruncate`, and
  `Clear`. Record bodies reuse the existing `NodeWire`/`RelWire` format (no wire
  version bump) and are decoded through `SafeUnmarshal` (a corrupt `0x09` row
  fails closed with `ErrCorruptWire`, never a process crash).
- **Memory backend parity** via `memory.WithChangeLog()` (in-RAM, non-durable —
  a testing/parity facility). Both backends route record bodies through one set
  of `storeutil` builders, so their feeds are **byte-identical for property-free
  entities** and decode-equivalent otherwise (badger tokenizes property keys on
  the wire while memory keeps key strings) — proven by a cross-backend parity
  test over property-free entities; a `Clear()` wipes the feed
  and re-anchors it with a `Clear` marker at a fresh monotonic LSN.

Design decision: the log is emitted **in-backend** (not as a `Store` decorator)
because crash-safety requires co-committing the record in the data `WriteBatch`,
and a decorator would also be treated as an untrusted store and lose the
frozen-pointer zero-copy scan path. The change-log **alone does not converge a
replica from empty**: a replica bootstraps from a full snapshot (export, including
the token registry) and then tails the feed. Deferred to a later phase:
`MetaSet` (`ChangeMeta` tag reserved), the tiered backend's change-log, and
LSN-watermark-driven log GC.

## [4.9.4] - 2026-06-20

### Added — node property-key presence stats, streaming ForEach, bulk AddNodes

- **`g.Stats().NodeCountByLabelAndPropertyKey(label, propertyKey)`** returns the
  number of current nodes that carry `label` AND an indexable scalar value for
  `propertyKey`, as an O(1) read backed by per-`(label, propertyKey)` presence
  counters. It is **key-presence only, not value-selectivity** — a planner can
  cheaply prune labels that cannot satisfy a scalar-equality lookup without
  scanning. Exposed through the new optional
  `store.NodePropertyKeyStatsCapability`; the core type-asserts it and returns
  `ErrCapabilityNotSupported` on a backend that does not implement it. Counters
  are maintained on every node-mutation door (`PutNode` / `DeleteNode` /
  `ReplaceNode` / `Add`+`RemoveNodeLabelToken`) and rebuilt during index load so
  they survive restart. Implemented for the memory, badger, and tiered backends;
  the tiered backend folds the reference, archive, and per-event-shard counts
  into one cross-shard total.

- **`g.Nodes().ForEach(opts, fn)`** streams all nodes matching `opts` to `fn`
  without materializing the full result slice: for current-state, unpaginated
  scans it walks the store node-ID iterator and fetches one row at a time, so
  peak memory is O(1) in graph cardinality (`fn` returning false stops early).
  Temporal and paginated scans fall back to `All` to preserve the existing
  history-aware and ordering semantics. Isolation matches `ForEachByLabel`: the
  ID set is snapshotted by the store iterator, then each row is fetched and `fn`
  is called without holding graph locks, so a concurrently deleted row is skipped
  rather than surfaced as an error.

- **`BatchBuilder.AddNodes(labels, props, count)`** queues `count` node creations
  with identical labels and properties as a write-only bulk path (no
  caller-visible node skeletons, so the queued nodes cannot be used as
  relationship endpoints; `Execute` still persists ordinary nodes and reports
  ordinary batch results). Enabled by hash-suffix precomputation
  (`integrity.PrecomputeNodeHashSuffixChecked` / `ComputeNodeHashFromSuffix`):
  the static label+property tail of the node hash is encoded once and reused per
  node instead of re-encoding it for every node in the batch.

### Performance — temporal point queries skip history when the current row answers

`NodeAt` / `NodeAtTx` / `RelAt` / `RelAtTx` (and the plural `NodesAt` / temporal
`ByLabel` paths that call them per entity) materialized and decoded an entity's
**entire** version chain on every query, even when the query asks for a
current/recent valid time that the live current row already answers. On the
badger backend that is the dominant cost — O(versions) of msgpack decode per
entity per query.

`nodeAtLockedTx` / `relAtLockedTx` now short-circuit: on a migrated store, when
the current row is the open tile (the resolver guarantees exactly one, carrying
the max valid-from and highest belief), is visible at `txAt` (recorded by then —
lesson 43), and its open interval covers `validAt`, it is returned without
loading history. No other version can outrank it for that `validAt` (closed tiles
end at or before the open tile's valid-from; no live row has a higher belief over
the open tail). Historical valid times still fall through to the full
materialize+scan, and append-only cascade chains keep the existing belief overlay.

Benchmark (badger in-memory, `NodeAt` at the latest valid time): flat **~95–215
ns / 4 allocs** regardless of history depth, vs the historical (slow-path) query
on the same entity growing 20µs→290µs / 187→5160 allocs from depth 8→256 — the
fast path no longer scales with chain length. Gated by
`TestNodeAt_CurrentRowFastPathSkipsHistory` (a history-faulting store proves the
read is skipped for current/future valid times and still surfaces for historical
ones).

## [4.9.3] - 2026-06-17

### Fixed — import framing: untrusted size/count fields no longer amplify into huge allocations

Fuzzing `Import` end-to-end (a new `FuzzImport` harness) surfaced a
memory-exhaustion DoS at the import trust boundary: the framing/replay path
allocated proportional to **declared** sizes from the (untrusted) stream rather
than to bytes actually delivered. The fuzzer did not crash — it STALLED at 0
execs/sec, the signature of repeated giant allocations and GC thrash. Two sites:

1. **Per-record body.** `readImportStageRecord` did `make([]byte, length)` for
   the declared record length (up to `maxExportRecordSize` = 128 MiB) *before*
   reading the body. A 5-byte record header claiming 128 MiB on an empty stream
   forced a 128 MiB allocation before the truncation was detected (~25-million×
   amplification). Now read via `io.CopyN` into a `bytes.Buffer` (pre-reserving
   at most 64 KiB): the buffer grows only with bytes actually present, and a
   short stream returns `ErrCorruptExport` having allocated essentially nothing.
2. **Header counts.** `reserve()` pre-sized six replay/rollback maps and slices
   from the export header's node/rel counts, capped only at `importPreallocLimit`
   = 1<<20. A ~20-byte header declaring 1M+1M counts forced ~312 MiB of map
   allocation before any entity record was read. The maps are created empty and
   grow naturally, so the count is a pure pre-sizing hint — `importPreallocLimit`
   lowered to 4096 (common-case optimization kept; hostile amplification bounded
   to ~hundreds of KiB).

Tests added: `import_fuzz_test.go` (`FuzzImport` — full pipeline: framing +
replay + rollback + hash-verify, real-export seeds + adversarial framing seeds)
and `import_amplification_test.go` (three `runtime.TotalAlloc`-bounded mutation
pins: oversized empty body, oversized lying body, 1M-count header). Re-fuzzed
post-fix with steady throughput and no slow input (every cached corpus entry
imports in < 1 ms). See `tasks/lessons.md` 48.

## [4.9.2] - 2026-06-17

### Fixed — wire decode trust boundary: hostile/corrupt msgpack can no longer crash the process

The on-disk / import decode boundary decodes untrusted msgpack bytes into
`NodeWire` / `RelWire` before any validation runs. Two crafted-input classes
made the `vmihailenco/msgpack/v5` decoder take down the **whole process**
instead of returning an error — a denial of service on the exact trust
boundary v4.6.1 hardened (lesson 44). Both were found this pass by a new fuzz
harness (`FuzzWireToNodeChecked`) and independently confirmed by an adversarial
decode audit:

1. **Decoder panic via reflect.** A msgpack map that repeats a key bound to the
   interface-typed `PropertyWire.Value` field makes the second decode target an
   unaddressable `reflect.Value`, panicking with
   `reflect: reflect.Value.SetString/SetInt using unaddressable value`. An
   ~17-byte row triggers it. The panic fires *inside* `msgpack.Unmarshal`,
   upstream of `WireToNodeChecked` — so every store-read and import decode site
   (`(*badger.Store).GetNode` inside `db.View`, the importer at `import.go`)
   panicked rather than classifying the row as corrupt.
2. **Fatal stack overflow.** `PropertyWire.Value` is `any`; the decoder recurses
   once per nesting level when decoding a deeply-nested array/map into it. A
   ~hundreds-of-thousands-deep blob aborts the process with
   `fatal error: stack overflow` — which is **not** a panic and **cannot** be
   recovered, so it must be prevented before the decoder runs.

Fix: a new `storeutil.SafeUnmarshal` is now the decode used at every untrusted
-bytes site. It (a) runs `guardMsgpackDepth`, a non-recursive scan of the
msgpack token stream that rejects nesting beyond `maxWireDecodeDepth` (64 — far
above the property allowlist's 32-level limit, far below the overflow point)
*before* handing bytes to the decoder, and (b) recovers any decoder panic. Both
failures now return the new sentinel `store.ErrCorruptWire` (wrapped as
`ErrCorruptExport` at the import boundary, preserving lesson 44 classification).
Rerouted: `UnmarshalNodeWireWithKeys` / `UnmarshalRelWireWithKeys`, the custom
-property blob decode (`reconstructCustomPropertyValue`), every `NodeWire` /
`RelWire` / meta decode in the badger backend, and the node/rel/header/registry
decodes in core import. (Tiered's catalog/registry/index metadata files decode
flat typed structs with no interface or deeply-nestable field and are outside
this bug class — reviewed, unchanged.) `msgpack.Marshal` is panic-free and
needs no wrapper. New regression battery: `wire_fuzz_test.go` (three fuzz
targets + crasher corpus seed) and `safe_decode_test.go` (panic-recover,
deep-nesting rejection, and a guard-accepts-legitimate-depth pin). See
`tasks/lessons.md` 47.

## [4.9.1] - 2026-06-13

### Added — X5 columnar DocValues capability

A per-`(label, property)` columnar value store aligned to a sorted NodeID
ordinal vector, so a consumer's grouped aggregation can stream group keys and
aggregate args from dense columns instead of fetching and decoding each node.
On the badger backend this removes the per-node msgpack decode that dominates a
label-scan aggregation (downstream A/B: a 100k grouped aggregation 206ms → 8.8ms,
~23x; memory backend 12.2ms → 8.8ms).

- **`g.Nodes().ForEachDocValues(label, propKeys, fn) (gen, ok, err)`** streams the
  requested columns in ordinal order; `ok=false` declines (no capability, unknown/
  empty/over-cap label, or a non-uniformly-numeric/string property) and the caller
  falls back. **`g.Nodes().NodeMutationEpoch()`** is the staleness counter the
  caller re-checks after the lock-free scan (Gate 2).
- Columns are **immutable once built**, cover the **full label membership** (the
  unfiltered scan set — never valid-time filtered, so a non-temporal aggregation
  counts expired-but-not-deleted nodes), carry a present bitset for absent
  properties, preserve int64-vs-float64 type, and dictionary-encode strings. A
  per-store node-mutation epoch is bumped on **every** node write including deletes,
  so a property edit (which never touches the label index) correctly invalidates a
  cached column. Both memory and badger backends; declines on tiered and on the
  on-disk label index. Values boxed once at build → allocation-free reads.
- **Multi-label intersection columns** — `g.Nodes().ForEachDocValuesMulti(labels, propKeys, fn)`
  streams the same dense columns for a label INTERSECTION (`(p:A:B)` patterns),
  keyed by an order-independent token-tuple, with the same epoch-validated
  immutable-snapshot model as the single-label path. **`g.Nodes().DocValuesSnapshot(label,
  propKeys)`** returns a random-access `types.NodeColumnReader` for point lookups
  over a column (the expand-aggregation arm that reads adjacency, not just
  membership). A distinct **`g.Rels().RelMutationEpoch()`** counter, bumped on every
  edge write, is the staleness gate for adjacency-reading aggregations — kept
  separate from the node epoch so node-only column caches do not rebuild on
  edge-heavy writes. Both backends; declines on tiered / on-disk index.

### Performance — resident entity cache (no-evict mode for in-memory stores)

`graph.Config.ResidentCache` keeps every decoded node/relationship resident in the
entity cache with no LRU eviction, removing the decode-on-eviction re-`msgpack`
cost that made a deep-traversal working set larger than the cache scale
super-linearly. Downstream A/B (cypher k-hop path benchmark): a sustained path
walk went from 663 → 850 → 1190 ns/path (growing with N) to a flat 189 → 225 →
239 (~5x at the largest size; the deterministic 28375 decode-misses drop to 0).
The in-memory store already holds every row in RAM, so for `BadgerInMemory` the
resident set costs no extra memory. Opt-in (`graph.Config.ResidentCache`, off by
default — LRU eviction preserved for disk-backed use); plumbed through
`badger.Config` / `core.Config` and `Cache.SetNoEvict()` on the LRU and sharded
caches.

## [4.9.0] - 2026-06-12

### Fixed — append-only cascade: bitemporal corrections no longer corrupt transaction time

`SetNodeVersionInterval` / `SetRelVersionInterval` (the cascade timeline edit)
rewrote and split existing version rows **in place** while preserving their
original `TxFrom`, leaving `TxTo` untouched. That made a row claim the DB
believed a world-boundary at a past transaction time it actually decided *now*,
which (1) left a **hole** in `NodeAtTx`/`RelAtTx` at any `txAt` before the
correction (the inserted interval's slot returned nothing), (2) could **leak** a
correction into belief states recorded before it was made, and (3) made the
native badger `NodeAsOf` reverse-scan diverge from the memory/tiered selection
(overlapping transaction-time intervals broke the early-stop's monotonicity
assumption). See `tasks/lessons.md` 45.

The cascade is now **append-only**: it never mutates an existing row's stored
valid-interval or transaction stamps. It appends the inserted tile and a
"resumption" tile (re-asserting the value that held at `validTo`) — both stamped
`TxFrom = UpdatedAt = now` — and recomputes the current KV slot as the rightmost
open tile. Existing rows are untouched, so they reconstruct the pre-correction
belief exactly and the appended rows the post-correction belief: no holes, no
leaks, monotonic transaction time.

Supporting resolver change: `resolveNodeVersionAt` / `resolveRelVersionAt` (and
the `*MatchingDuring` paths) now order the TxFrom-filtered chain by **effective
valid-from** (version as tiebreaker) before tiling, and on an overlap select the
**newer belief** (higher `TxFrom`, then version) rather than the highest version
in array order. This is a no-op for monotonic histories — normal `Update` rejects
backdated valid-from (`ErrValidFromBeforePrevious`) — so only append-only cascade
chains are affected. Gated by `TestCascade_BeliefIsConsistentPerTxAt`
(node + rel × memory/badger/tiered) and `TestCascade_CorrectionSpanningBoundary`
(an interval correction spanning an existing valid-time boundary). `lesson 43`
(`TxTo` does not bound valid-time answerability) is unchanged and still holds.

`NodeAsOf` / `RelAsOf` selection is now deterministic across backends: a cascade
leaves several tiles open (`TxTo == 0`) sharing one `TxFrom`, and the memory and
core-fallback selectors previously broke that tie by Go map order — diverging
from the badger native reverse-scan (which selects by descending version). All
three now select the highest `(TxFrom, version)`. Gated by
`TestCascade_NodeAsOfParityAcrossBackends`. The resolver's per-query chain sort
short-circuits on already-ordered chains (an O(n) check), so normal monotonic
histories pay nothing and only cascade-edited chains incur the sort.

### Performance — concurrent-scan cache, traversal decode, temporal index, key encoding

A downstream cross-engine benchmark round drove a sequence of read/write
path optimizations:

- **Scan-resistant, non-promoting entity cache.** The LRU `index.Cache.Get`
  took an exclusive `sync.Mutex` on every cache HIT (for `MoveToFront`
  recency promotion), so a warm 100k-node label scan paid 100k exclusive
  acquisitions and concurrent scanners on the same label serialized.
  `Cache.mu` is now a `sync.RWMutex` with a new `GetNoPromote` (RLock, no
  promote); the badger scan paths (`prefetchNodeScan`/`prefetchRelScan`) use
  it while point reads keep promoting. Sharded cache gains the same method.
  Mutex delay 27.52s → 0.77s (~36x); concurrent scaling restored.
- **Endpoint-carrying adjacency index — skip the relationship decode.** The
  in-memory adjacency index now carries the opposite endpoint (`outIdx`:
  relID→end node; `inIdx`: relID→{start node, type}); a new streaming
  `ForEachAdjacentEndpoint` yields `(relID, otherEndpoint)` with no
  relationship-row decode (RAM map or disk adjacency key offset 11). One-hop
  expand alloc/query dropped ~60x downstream.
- **`badger DetectConflicts=false`.** The store serializes writes through its
  own buffer and guards same-entity mutations with the entity-lock manager, so
  it owns conflict semantics above Badger (every `db.Update` meta write is a
  blind Set); the per-key conflict oracle is dead weight on every commit.
- **Maxto-augmented temporal interval index.** `QueryAt`/`QueryOverlap`
  scanned every interval starting before the probe filtering on `To` — O(n).
  The sorted-by-From slice is augmented with `subMax[i]` (max effective upper
  bound over the implicit balanced BST rooted at `i`, open-ended → +∞); a
  stabbing query prunes expired subtrees (`subMax <= probe`) and right
  subtrees whose froms all exceed the probe → output-sensitive O(log n + k).
  `Remove` marks dirty to rebuild the augmentation. Bench (100k entries, 16
  live, late probe): `QueryAt` 157 ns/op. Equivalence-gated vs brute force.
- **Single-alloc property index keys (numeric, float, temporal).**
  `IndexablePropertyValueKey` built numeric/float keys as
  `prefix + strconv.FormatX(...)` (two allocs) and temporal keys as
  `"tv:" + FormatUint(kind) + ":" + value` (~four allocs) on every
  property-equality lookup; appending into one stack buffer folds each to a
  single allocation with byte-identical output.
- **Native badger transaction-time query (reverse-scan AsOf).** The
  mandatory-store fallback answered `NodeAsOf`/`RelAsOf`/`NodesAsOf`/`RelsAsOf`
  by materializing an entity's whole version history (every version decoded +
  deep-copied) then linear-scanning. The badger store now implements the
  optional `TransactionTimeQueryCapability` directly (auto-enabled, no wiring
  change): history keys order by ascending version and version tracks `TxFrom`
  monotonically, so a reverse iterator visits versions newest-first and stops
  at the first one visible at the query time — O(versions newer than the query)
  vs O(all versions). The current row is checked first via the cache-backed
  point read; the reverse scan merges the pending-write overlay. Selection
  mirrors the memory backend exactly (3-backend parity contract test); no
  on-disk format change.
- **Inline `PropertySlice` binary search** (drop the `sort.Search` closure)
  and an **opt-in `QueryOpts.NoSort`** scan lever (default off).
- **Inline valid-time adjacency stamps — temporal traversal without the
  decode (OPT15, LiveGraph VLDB 2020).** A `validAt`/interval adjacency
  traversal msgpack-decoded EVERY incident relationship row just to read its
  valid interval and apply `MatchesTemporalFilter`; on a hub whose edges are
  mostly expired versions almost all of those decodes are rejected. A small
  parallel `relValidIdx` (relID → effective `{validFrom, validTo}`, NOT a
  change to `outIdx`'s value type) lets the new streaming
  `ForEachAdjacentEndpointAt` reject (or admit) an edge from the inline stamp
  with no decode, applying the temporal predicate under the snapshot lock so
  only survivors leave the locked section. Stored `validFrom` is the EFFECTIVE
  value (`EntityValidFrom` resolves the snowflake fallback), so the fast path
  reuses the canonical `MatchesTemporalFilter` via a synthetic
  `TemporalMetadata` and can never drift from the decode path's semantics.
  Soundness is "a hit must be fresh; a stampless edge (cross-shard incoming)
  falls back to a decode" — the stamp is seeded/refreshed/dropped at every rel
  lifecycle site including the in-place `ReplaceRelationship`/
  `ReplaceRelWithHistory` version-close paths (which move `valid_to` while
  leaving adjacency untouched). Bench (4k-edge hub, ~90% expired, cache under
  pressure): decode path 315µs / 299 allocs → inline 79µs / 2 allocs (~4x,
  decode-free). Equivalence pinned by `TestRelValidStamp_DivergenceVsDecode`
  (random create/close/replace/delete sequences, inline vs decode vs oracle
  after every mutation) — the gate fails when any lifecycle site is removed.
- **O(buckets) range cardinality from the sorted index (R1).** A `count`
  over a numeric range predicate (`WHERE age > $x`) previously over-selected
  the matching node IDs through the ordered view and then re-fetched/decoded
  every one just to count it — catastrophic on a broad predicate. The new
  `RangeCardinality(label, key, min, max, inclMin, inclMax, opts)` (exposed on
  `g.Nodes()`, both backends) sums the per-key bucket sizes over the sorted
  numeric index in the requested range — a popcount-style walk with zero node
  fetches. It declines (`exact=false`, caller falls back) on temporal opts, a
  poisoned/imprecise index (integers above 2^53 that collide under the float64
  key), or a missing capability. Equivalence-gated by `TestRangeCardinality_VsBruteForce`
  (200 random trials) and the cross-backend `RangeCardinality` divergence suite.

## [4.8.0] - 2026-06-12

### Added — per-instance Badger footprint tuning (vlog / memtable / block cache / compactors)

One Badger instance opens per shard, and Badger's stock per-instance sizes are
sized for a monolithic DB: a ~2 GB apparent value-log file, a ~64 MB memtable
arena allocated upfront in heap, a 256 MB block-cache bound, and 4 compactor
goroutines — **multiplied by shard count**. A tiered deployment with a reference
shard plus a dozen weekly event shards therefore pre-creates tens of GB of
apparent disk and gigabytes of heap arenas even when every shard holds little
data. Four new knobs bound that footprint:

- **`badger.Config`** gains `ValueLogFileSize`, `MemTableSize`, `BlockCacheSize`,
  `NumCompactors`. Validated at `New`: vlog `[1MB, 2GB)`, memtable `[8MB, 1GB]`
  (the 8 MB floor is a real Badger constraint — `Open` fails unless the 1 MB
  `ValueThreshold` is ≤ 15% of the memtable), block cache `>= 0`, compactors `0`
  or `>= 2`. Out-of-range values fail at `New` with a message naming the field,
  instead of surfacing as a cryptic Badger error deep inside `Open`.
- **`tiered.Config`** gains the same four fields, passed through to **every**
  shard (reference, hot, warm, lazy cold/archive, rotation-created) via a single
  new `badgerCfg` choke point.
- **`graph.Config`** (`core.Config`) gains the same four fields for the store it
  constructs from `BadgerDir`/`BadgerInMemory` (ignored when `Store` is supplied).

**Zero keeps Badger's stock defaults** — a deliberate decision for a library
with external consumers: silent default changes would be a behavioral surprise,
so the owner (the SOC, a downstream service) opts in explicitly. The knobs are
applied through one shared options builder (`buildBadgerOptions`) so the normal
open and the WAL-migration open can never drift.

### Added — per-shard entity-cache byte budget on `tiered.Config` (`CacheBudgetBytes`)

`CacheBudgetBytes` (added to `badger.Config` in 4.7.0) is now plumbed through
`tiered.Config` to every shard. `CacheCapacity` alone is an **entry count**
(default 10,000 per cache, two caches per shard), so many open shards can pin
hundreds of thousands of entities in heap regardless of their size; the byte
budget bounds that directly. Soft limit (dirty entries are never evicted); `0`
disables byte accounting. (`CacheCapacity` itself was already a per-shard
`tiered.Config` field — the SOC can lower it today, same as the `ColdAfter`
quick win below.)

### Fixed — shrinking the memtable on an existing data dir could terminate the process

Badger replays each `.mem` WAL into a skiplist arena sized by the **current**
`MemTableSize`; a WAL written under a larger memtable overflows it, and Badger
raises that overflow via `y.AssertTruef` → `log.Fatal` → **`os.Exit`** —
**not** a recoverable error or panic. So the first time a service shrinks
`MemTableSize` on a data dir that still holds live WALs (copied from a running
server, or left by a crash — a clean `Close` deletes WALs, which is why no unit
test against a clean store ever saw it), the open **terminates the whole
process**.

- **`badger.MigrateOversizedWAL(cfg)`** (new, exported) flushes such WALs at
  their original size (recoverable exactly, since Badger creates each WAL at 2×
  the memtable that wrote it) before the real open. `New` runs it automatically,
  gated on `MemTableSize > 0 && !InMemory && !ReadOnly`.
- **Tiered recovery probe fix.** `openBadgerStoreWithRecovery` probes a warm/cold
  shard read-only first — but a **read-only** open replays WALs into the same
  bounded arena (`openMemTables` runs before Badger's read-only branch), so it
  hits the same `os.Exit`, and "Arena too small" is not `ErrTruncateNeeded` so
  the probe could not recover it. The migration now runs **before** the
  read-only probe (idempotent; a no-op on clean dirs, stock sizes, and in-memory
  shards). Without this, reopening a tiered store on a snapshotted/crashed
  DataDir with a tuned-down memtable would crash the host service.

### Tests — break-the-system, mutation-validated

Every new test was validated by mutation: the implementation was temporarily
broken and the relevant test confirmed to fail. Coverage: a boundary table
(zero=stock, floors, caps, negatives, `MinInt64`); **footprint-applied** tests
that assert the on-disk WAL/vlog apparent sizes are exactly `2×` the configured
value (a dropped `With…Size` leaves the stock 128 MB / ~2 GB — caught here and
nowhere else, since a clean close truncates the vlog); reopen-across-tuning-change
(byte-identical, incl. a >1 MB vlog value, both directions); the live-WAL
migration reproduction; a **subprocess** test proving a bare read-only open on an
oversized WAL exits the process; a concurrent flush storm at the memtable floor
(`-race`); and the tiered suite — footprint + cache-budget passthrough to
reference/hot/warm shards (incl. the recovery-reopen path), validation surfacing
at `New`, a cold-shard cross-shard retro-link write (§4d safety check), and the
faithful tiered oversized-WAL warm-reopen migration.

### Note — `ColdAfter` / `IdleTimeout`

No code change: `tiered.Config` already exposes `ColdAfter` (demote warm shards
to cold) and `IdleTimeout` (close idle cold shards). Setting `ColdAfter` closes
old event shards outright (lazy reopen on access) — a zero-tkg-change win for
deployments that never tune it. The cold-shard write path that enabling it
exercises (retro-linking a current entity to an old signal on a cold shard) is
verified by `TestTieredColdShardRetroLinkWrite` and the existing
`TestTieredStore_ColdShard_*` suite.

### Changed — dependencies

- Badger `v4.9.1` → `v4.9.2`. The footprint facts this release relies on are
  unchanged in 4.9.2 (WAL created at `2×MemTableSize`, vlog at
  `2×ValueLogFileSize`, `db.Opts()` public, arena overflow via
  `log.Fatal`/`os.Exit`) — pinned by the `Footprint`/`OversizedWAL` suites.

## [4.7.0] - 2026-06-11

### Performance — scan-resistant entity cache + configurable capacity

Two changes to the badger store's read path, driven by a downstream
cross-engine benchmark finding a 12x per-row regression on label scans just
past the 10k cache capacity (and ~17x over linear at 100k nodes):

- **Scans no longer fill the LRU.** Full-cardinality scans (label scans,
  the by-label-and-property fallback, AllNodes/AllRelationships, by-type,
  temporal range) read through new `prefetchNodeScan`/`prefetchRelScan`
  variants that serve cache hits but do NOT insert decoded entries on miss.
  Fill-on-miss during a sequential scan larger than the cache evicts
  exactly the entries the next pass needs (100% steady-state miss rate)
  and flushes hot point-read entries as collateral. Point reads, adjacency
  (Outgoing/Incoming), and GetByIDs keep the filling `prefetchNode`/
  `prefetchRel` because callers revisit them. Pinned by
  `TestScanLargerThanCache_CorrectAndCachePreserving` (correctness across
  repeated over-capacity scans + point reads after).
- **`core.Config`/`graph.Config` gains `CacheCapacity`** — previously the
  badger store constructed via `BadgerDir`/`BadgerInMemory` was locked to
  the 10k default with no way to size it to the working set. 0 keeps the
  default; ignored when `Store` is supplied explicitly.

Measured downstream (cross-engine latency benchmark, 100k nodes / 500k rels,
cache sized to the dataset): label-scan aggregation 430ms → 40ms,
filtered scan 417ms → 40ms, var-length traversal 1.49ms → 0.6ms p50.

### Fixed — flush cycle walked the entire entity cache (ingestion stall at large capacities)

`Cache.CollectDirty` iterated EVERY cached entry (clean included) under the
cache mutex once per 100ms flush cycle — O(cache size), not O(dirty). With
a multi-million-entry `CacheCapacity` the walk starved writers and
ingestion stalled: a 1M-node UNWIND load with 2M capacity hung
indefinitely (vs flat ~31k rows/s at the 10k default — the capacity knob
looked like the culprit but the flusher was). The cache now maintains a
dirty-set index, making CollectDirty O(dirty), sorted by key for
deterministic flush batches. Same load after the fix: ~100-176k rows/s,
~16s total. Invariant pinned by `TestCache_DirtySetMatchesFullScan`
(2,000-op random sequence, dirty set vs full scan after every op);
suite + race clean.

### Added — ordered numeric index view + streaming range scans

Every property index now transparently maintains an ORDERED view of its
numeric values (sorted distinct float64 sort keys + per-key ID buckets,
`property_index_range.go`): `WHERE n.age > $x` shapes binary-search the
view instead of scanning the label. The view is an over-selecting
candidate filter by design (float64 keys, ulp-widened bounds) — consumers
post-filter with exact comparison semantics. Self-protecting: past 100k
distinct numeric keys the view disables itself (sorted-slice maintenance
would go quadratic; a B-tree lifts the cap later) and range queries report
unsupported. Index creation backfill now routes through AddKey so the
view covers pre-existing nodes.

Exposed as `ForEachNodeByLabelPropertyRange` on the badger store and
`g.Nodes().ForEachByLabelPropertyRange` on the facade (optional
capability; `ErrIndexNotFound` signals fallback). Pinned by
`TestForEachByLabelPropertyRange` (candidate completeness vs a ByLabel
reference, bound inclusivity, mixed int/float keys, delete maintenance,
early stop, no-index signal). Measured downstream:
filtered-scan 19ms → 10.2ms at 100k — parity with Memgraph.

### Added — streaming label scans (`ForEachByLabel`)

`g.Nodes().ForEachByLabel(label, opts, fn)` streams a label's nodes in
snowflake-ID order without materializing the result slice — peak memory
O(1) in the label's cardinality, the requirement for scan pipelines
(count/filter/aggregate) over very large labels where `ByLabel`'s
`[]*types.Node` materialization alone is hundreds of MB. Backed by an
OPTIONAL store capability (`ForEachNodeByLabel`, implemented by the memory
and badger stores; others fall back to the materializing path), wired
through `core.NodeOps` and the nodes sub-API.

Isolation is deliberately RELAXED relative to `ByLabel`: the ID set is
snapshotted, then rows are fetched and fn runs without graph locks — fn
may call back into the graph (pinned by `TestForEachByLabel_CallbackReentry`),
concurrent writers are neither blocked nor observed atomically, and rows
are shared frozen pointers. Temporal-filter queries route through the
history-aware materializing path. New sentinel `grapherr.ErrNilCallback`
for nil callbacks. Pinned by the `TestForEachByLabel_*` suite (equality
with ByLabel across an over-capacity cache, early stop, Limit, temporal
fallback, unknown label).

### Added — streaming relationship scans (`ForEachByType`, `ForEachOutgoing`, `ForEachIncoming`)

The rel-side mirror of `ForEachByLabel` (enterprise-scale ceiling 2 —
slice-materializing query APIs):

- **`g.Rels().ForEachByType(typeName, opts, fn)`** streams a type's
  relationships in snowflake-ID order — peak memory O(1) in the type's
  cardinality, for scan pipelines over very large rel types where
  `ByType`'s slice materialization alone is hundreds of MB.
- **`g.Rels().ForEachOutgoing(nodeID, typeName, fn)` /
  `ForEachIncoming`** stream a node's adjacency (optionally type-filtered)
  for hub-degree consumers — power-law hubs with 100k+ edges no longer
  force a full adjacency slice per visit. `ErrNodeNotFound` parity with
  the materializing siblings, including under an unregistered type name.

Same contract as the node side throughout: OPTIONAL store capabilities
(`ForEachRelByType` / `ForEachOutgoingRel` / `ForEachIncomingRel`,
implemented by the memory and badger stores; others fall back to the
materializing path), relaxed isolation (ID-set snapshot, then lock-free
fetch — fn may call back into the graph), frozen shared rows, no-fill
scan reads on the badger arm, temporal filters routed through the
history-aware materializing path. Pinned by the `TestForEachRel*` /
`TestForEachAdjacent_*` suites — every probe runs against BOTH in-tree
stores (equality with the materializing sibling past the badger cache
capacity, early stop + Limit, callback re-entry, temporal fallback,
missing node, unregistered type, nil callback).

### Added — byte-budgeted entity caches (`CacheBudgetBytes`)

Enterprise-scale ceiling 4: `CacheCapacity` counts ENTRIES, but entries
vary 100B-64KB, so a count alone cannot bound memory under mixed payloads.
`Config.CacheBudgetBytes` (badger store + `graph.Config` plumb) bounds
each entity cache (nodes, rels) by estimated resident bytes:

- The LRU tracks per-entry size (`NewCacheWithBudget` — a sizer estimate
  plus fixed per-entry overhead) and evicts clean LRU entries while EITHER
  the count capacity or the byte budget is exceeded. Soft limits as
  before: dirty entries are never evicted, so write pressure can
  temporarily exceed the budget; the flush cycle sheds back down.
- Sizing uses new `types.(*Node/​*Relationship).ApproxHeapBytes()` —
  allocation-free O(properties) resident-heap estimates (struct layouts,
  string/slice/map payload bytes; registered custom types fall back to a
  pessimistic constant).
- `CacheBudgetBytes` with `CacheCapacity` 0 makes the byte budget alone
  govern (count limit effectively unbounded). 0 disables byte accounting
  — the default path is byte-for-byte unchanged.
- Eviction now also runs after `MarkFlushed`, after value-growing `Put`
  updates, and after tombstone inserts — previously only inserts evicted,
  which let a grown update or a flush-clean batch leave the cache over
  its limits until the next insert.

Pinned by the `TestCacheBudget_*` LRU suite (full-scan accounting
invariant over 2,000 random ops with both eviction triggers churning,
bytes-not-count eviction, dirty-exceeds-then-flush-sheds, update resize
deltas, tombstone lifecycle, off-switches) plus store-level
`TestCacheBudgetBytes_*` (mixed-payload point reads stay correct and
within budget end-to-end; dirty writes exceed then flush sheds) and the
`TestApproxHeapBytes_*` estimator probes (nil-safety, payload
monotonicity, recursion bound, unknown-type fallback).

### Added — disk-resident label index (`LabelIndexOnDisk`, opt-in)

Enterprise-scale ceiling 1: the in-memory `labelIdx` map costs ~50-100B of
permanent RAM per label entry — THE memory ceiling at hundreds of millions
of nodes. The persisted label keyspace (`KeyLabel + token + nodeID`) has
always been written transactionally with node rows in the same flush
batch; `Config.LabelIndexOnDisk` (badger store + `graph.Config` plumb)
stops materializing the RAM map and answers label snapshots from that
keyspace directly:

- All label consumers (NodesByLabel, ForEachNodeByLabel, the
  by-label-and-property fallback, property/temporal/HF/vector index
  backfills, the delete-scrub fallback) route through one snapshot helper
  (`labelNodeIDsSnapshotLocked`) — the disk arm prefix-iterates the
  keyspace and overlays the unflushed write buffer, so reads see writes
  immediately, exactly as the map did (idxMu → wbMu, the flush cycle's
  established lock order).
- Existing data directories need NO migration — the keyspace is already
  complete (loadIndexes has always used it as the persisted source of
  truth). Open builds the map transiently for the index/counter rebuilds,
  then drops it; making open fully streaming is the documented follow-up.
- Per-label counts stay O(1) (the counter map is independent and tiny).
- Trade-off: each label snapshot costs a disk prefix iteration instead of
  a map copy — opt in when the working set's RAM matters more than label
  scan latency (the entity cache in front is unaffected).

Pinned by the `TestLabelIndexOnDisk_*` suite (both-arms parity:
identical mutation sequences compared against the map mode after EVERY
operation, pre-flush — pending-overlay visibility — and post-flush;
map-stays-empty RAM guarantee; legacy-directory reopen with the flag;
property-index backfill interplay).

### Added — disk-resident adjacency index (`AdjacencyIndexOnDisk`, opt-in)

The adjacency sibling of `LabelIndexOnDisk`: the
`OutKey`/`InKey` keyspaces have always been written transactionally with
relationship rows, so the opt-in stops materializing the `outIdx`/`inIdx`
maps (~2 entries per relationship — the largest index maps) and answers
adjacency from the keyspaces:

- One snapshot helper (`adjacentRelIDsSnapshotLocked`) behind every
  consumer — Outgoing/Incoming (single + ForNodes batched), both degree
  queries, the streaming `ForEach*Rel` arms, the TieredStore repair
  surface (`IncomingRelIDs`/`OutgoingRelIDs`/`IncomingIndexEntries`), and
  the node-delete cascade collectors. Typed queries use an 11-byte prefix
  (node + relType), replacing the in-memory mode's full typeIdx
  intersection. Pending (unflushed) ops are overlaid, so reads see writes
  immediately.
- The connected-relationships guard on non-cascade `DeleteNode` consults
  the keyspace (`nodeHasAdjacentRelsLocked`) — with empty RAM maps the old
  `len(map)` check would have let connected nodes delete through, leaving
  dangling relationships (caught during implementation, pinned by
  `TestAdjacencyOnDisk_ConnectedNodeDeleteStillRejected`).
- Open builds the maps transiently for counter rebuilds, then drops them
  (same discipline as the label index). Legacy directories reopen with the
  flag with no migration.

Pinned by the `TestAdjacencyOnDisk_*` parity suite (both modes through
identical mutation sequences pre/post-flush, cascade delete, degree
parity, maps-stay-empty, legacy-dir reopen).

### Added — storage-typed temporal property values (`types.TemporalValue`)

A first-class temporal property kind: `types.TemporalValue{Kind, Value}`
carries a temporal category (date / local time / zoned time / local
datetime / zoned datetime / duration — wire-stable ordinals) plus the
canonical ISO-8601 rendering. Engines layered on the graph previously
stored temporals as plain strings and had to GUESS on read whether a
string was a temporal — which mistyped genuine string properties that
merely look like dates and broke their literal equality (the downstream
a downstream consumer bug this fixes).

Full property-pipeline support: validation (kind range, bounded
rendering, graph size limits), deep copy (value type), integrity hashing
(type-prefixed — never collides with the equal-rendering string), wire
round-trip (`ptTemporal = 26`, encoded `[kind, iso]` — additive within
wire format version 1, same compatibility class as previous tag
additions), index value keys (`tv:<kind>:<iso>` — type-prefixed per the
key-encoding rule), and heap-size estimation. Export/import round-trips
through the shared wire structs unchanged. Pinned by `TestTemporalValue_*`
(shape rejection, property round-trip, index-key and hash distinctness
from the equal string) and a badger write→flush→evict→decode round-trip
asserting the temporal comes back typed AND the same-rendering string
comes back a plain string.

### Performance — reflection sorts removed, ordered-view key cap lifted

- **`slices.SortFunc` everywhere hot**: the storeutil ID/row
  sorts (`SortNodesByID`, `SortRelsByID`, `SortNodeIDs`, `SortRelIDs`,
  `SortSnowflakeIDs`) and the flush cycle's dirty-batch sort went from
  `sort.Slice` (reflect.Swapper + boxed comparator — ~14% of var-length
  traversal in the 2026-06 profile) to monomorphized generic sorts.
  Measured downstream: 2Hop 164→152 allocs/op, VarLength 199→157.
- **Ordered numeric view: 100k distinct-key cap LIFTED**: the
  flat sorted `[]float64` (O(D) memmove per new key — the reason for the
  cap) became a chunked sorted set (`orderedKeys` — a flat-directory B+
  tree, 4KB half-chunks, O(log chunks + chunk) insert/remove). Range
  push-down now stays active at any cardinality; `rangeDisabled` is gone.
  Pinned by a 200×100-op model-equivalence churn probe (split aliasing,
  directory order, lost keys), chunk-boundary iteration probes, and
  `TestRangeNodeIDs_Past100kDistinctKeys` (150k keys: supported, exact,
  delete-consistent).

## [4.6.1] - 2026-06-10

Break-the-system rounds 2–4 (post-v4.6.0 adversarial campaign): four real
defects found and fixed in rounds 2–3, round 4 found none — the batteries
from all rounds are now permanent regression detectors. Round-by-round
record in `tasks/todo.md`; new lessons 43 and 44 in `tasks/lessons.md`.

### Added — round-4 adversarial test batteries (no defects found)

A fourth break-the-system round attacked the remaining cross-doors:
indexed-vs-unindexed query equivalence on twin graphs (including the
lesson-23 float-equality matrix and an index built under concurrent
writes), pagination union-vs-unpaged exactness with hostile cursors,
`ConstraintRelWithinEndpoints` parity across all five creation doors,
hostile event handlers (panicking, re-entrant read+write, tx
rollback/commit delivery), and the tiered store under forced rotation,
cross-shard relationships, archive/restore round trips, class-flip
rejection, and repair false-positive checks. Every subsystem held; the
batteries are now permanent regression detectors
(`index_cross_door_test.go`, `constraint_door_equivalence_test.go`,
`events_hostile_test.go`, `tiered_adversarial_test.go`).

### Fixed — import is now a real trust boundary (hostile streams fail closed, classified)

Two gaps found by the round-3 hostile-stream battery
(`io_adversarial_test.go`):

- **Truncated streams were unclassifiable.** A stream cut mid-record
  surfaced as a raw `unexpected EOF` matching neither `ErrCorruptExport`
  nor `ErrIncompatibleExport`, although the io contract promises
  `ErrCorruptExport` wraps every structural-validity failure. Truncated
  record headers and bodies now classify as `ErrCorruptExport` (clean
  `io.EOF` at a record boundary remains the end-of-stream signal).
- **Transport corruption could import cleanly.** A single bit flip in a
  property value (or in a stored `PrevHash` link) produced a graph that
  imported successfully but failed its own `Verify*Chain`. Import now
  (1) recomputes every imported row's content hash against the hash the
  stream claims (node/rel, current/history) and (2) runs a full
  hash-chain verification pass over every imported entity after replay —
  link corruption included. Any mismatch fails the import with
  `ErrCorruptExport` and the existing rollback restores the pre-import
  state (zero partial rows, verified by truncation/bit-flip tests at many
  offsets). See lesson 44.

### Fixed — frozen scan rows can no longer poison the canonical cache

The v4.5.0 frozen-row guard covered entity METHODS, but `Temporal()` and
`Integrity()` returned the shared internal pointer — and on a frozen SCAN row
that pointer aliases the store's canonical cache entry. `TemporalMetadata`
and the integrity structs have exported fields (plus the `Signature` []byte
backing), so one careless reader writing `row.Temporal().ValidTo = x` (or
flipping signature bytes) silently corrupted query results for every
subsequent reader, on all three backends — exactly the silent-corruption
class the frozen design exists to prevent. On a FROZEN entity those
accessors now return independent copies (Signature cloned); unfrozen
entities keep the shared-pointer contract the graph layer relies on. Found
by the new break-the-system battery (`frozen_poisoning_test.go`,
`frozen_adversarial_test.go` — including a reader-vs-writer race stress that
detects any internal in-place mutation of published rows).

### Fixed — bitemporal point queries after supersession (lesson 43)

`NodeAtTx(validAt, txAt)` (and `RelAtTx` / `NodesAtTx` / `RelsAtTx` /
`QueryOpts.TxAt`) treated every supersession as a retraction: the visibility
predicate `TxFrom <= txAt < TxTo` excluded superseded versions from all
later txAt, so after ANY update the prior version could no longer answer
historical valid-time — including the flagship 4.3.0 scenario (explicit-VT
tiled update, then `NodeAtTx(oldVT, now)` → `ErrNoVersionValidAt`). No test
had ever asked the (historical VT, current txAt) question. The predicate is
now recorded-by-then (`TxFrom <= txAt`): TxTo marks when a version stopped
being the CURRENT record; the row remains the authority for its valid-time
slot in every later belief state. Belief reconstruction at a past txAt is
unchanged (versions recorded later are still absent, so the then-latest
version is open-ended exactly as believed then). Regression tests:
`bitemporal_supersession_test.go` (node + rel),
`TestNodeAtTxSeesPreMutationLabelState`.

### Fixed — `AddByIDIfAbsent` found-branch returns a mutable result again

Since v4.5.0 the found branch handed back the store's frozen shared row
(while the created branch returned a fresh mutable relationship) — an
incidental asymmetry from the frozen-rows migration. The found branch now
deep-copies (one row; negligible next to the scan that found it).

## [4.6.0] - 2026-06-10

Architecture-review remediation release: every issue from the 2026-06-10
full architecture review fixed (see `tasks/todo.md` for the phase-by-phase
record and `docs/architecture.md` "Deferred Architectural Decisions" for the
consciously deferred items).

Also hardened two flaky test deadlines: the v3.0.56 cache-miss-prefetch
regression tests used a 200ms wall-clock wait that intermittently timed out
under fully-parallel `-race` runs; the wait (which is itself the failure
detector — a wrongly-ordered prefetch blocks forever) now allows 5s.

### Added — on-disk wire format versioning (fail closed on newer data)

The persisted row format was previously unversioned: a future schema change
would have decoded old rows silently with zero-filled fields, and data written
by a newer release would have been misread instead of rejected. Two layers of
protection, both backward compatible with every existing store directory:

- **Per-row version.** `NodeWire`/`RelWire` now carry `FormatVersion`
  (`msgpack:"fv"`, emitted by the hand-written encoders too — lesson 39).
  Rows written before versioning decode with `FormatVersion == 0` and are
  treated as version 1 (identical layout). Checked decodes of a row with a
  version newer than the binary supports fail closed with the new sentinel
  `store.ErrWireFormatVersionUnsupported` (re-exported as
  `graph.ErrWireFormatVersionUnsupported`).
- **Store-level marker.** The badger backend writes a `wire_format_version`
  meta key at open (absent on pre-versioning directories ⇒ stamped; lower ⇒
  raised on read-write open; read-only opens never write). A marker written
  by a newer release makes `New()` fail closed with the same sentinel before
  a single row is decoded. A marker that exists but cannot be parsed is
  treated as corruption and also fails the open. Tiered shards inherit the
  check per shard.
- **Load-time fail-closed.** `loadIndexes` tolerates corrupt rows (skip +
  warn, as before) but now distinguishes a future per-row format version:
  that is not damage but newer data, and the open fails closed instead of
  silently dropping the row and healing the entity counter down (which would
  have been data loss masquerading as a clean open).

Bump protocol documented on `storeutil.CurrentWireFormatVersion`: a version
bump must update the custom encoders, the decode path, and the marker logic
together.

### Fixed — label/property mutations no longer inherit the previous version's valid time

Lesson 33 ("Update must not inherit valid-time from the previous version")
was fixed on the `Update` door in 4.3.0 but the same deep-copy-then-stamp
pattern lived on in four other mutation doors: `AddLabel`, `RemoveLabel`, and
the node/rel property mutations (`SetProperty` / `DeleteProperty` /
`CompareAndSetProperty`). The new current version kept the previous version's
explicit `tkg_valid_from`, so it covered the previous version's world-time
interval and historical queries resolved to the POST-mutation state:

```
n := Add(labels=[Thing], tkg_valid_from=1000)
RemoveLabel(n, "Thing")
NodesByLabelAt("Thing", 1200)   // before: n missing — the label-less current
                                // version claimed [1000, ∞) and shadowed the
                                // genesis version. after: n returned.
```

All four sites now clear `ValidFrom`/`ValidTo` on the new version (these
mutations accept no caller-supplied valid time), matching the Update path.
Tx and batch doors route through the same internals and are fixed with it.
Found by the new cross-door equivalence test
(`TestTemporalTwoDoorsAgreeOnLabelQueries`), which asserts the named door
(`NodesByLabelAt`), the generic door (`ByLabel` + temporal `QueryOpts`), and
the per-ID resolver (`NodeAt`) return the exact same set on an adversarial
dataset, on memory AND badger. Regression tests per door in
`inherited_validtime_regression_test.go`. See lesson 42.

### Changed — temporal predicates have a single canonical definition

The effective-valid-from derivation and the point/interval predicates were
defined twice: once in the core graph layer (`nodeValidFrom`, `isNodeValidAt`)
and once in `storeutil` (`EntityValidFrom`, `MatchesTemporalFilter`) for the
store-level push-down — a semantic-drift hazard across the layer boundary.
The core helpers now delegate to the storeutil predicates, which are exported
as the canonical definitions (`MatchesPointInTime`, `MatchesInterval`) with
boundary-exact direct tests. No behavior change: the Core ID generators and
the shared snowflake layout were verified identical (same epoch, microsecond
precision, 5/10 bit split).

### Added — bounded async write buffer (badger backend)

The badger backend's async write pipeline had no ceiling: dirty cache entries
are never evicted (correct for the flush design) and the pending-op map grew
without limit, so a sustained write burst faster than the 100ms flush
interval grew memory until OOM. New `Config.MaxPendingWrites` (default
`DefaultMaxPendingWrites` = 100,000 ops; negative disables; ignored under
`SyncWrites`): when the pending buffer reaches the bound, the writing call
flushes synchronously — backpressure instead of unbounded growth — and a
failing backpressure flush surfaces its error to the writer (ops are requeued
for retry, nothing is dropped). `Store.PendingWriteCount()` exposes the
pressure signal. Tiered shards inherit the default per shard. The 20 inline
`if bs.syncWrites { flush }` sites and `flushIfSyncWrites` were unified into
one `flushIfNeeded` hook so the bound covers every write path.

### Added — sentinel anti-drift guard; hardened doc contracts

- `pkg/graph/errors.go` is now documented as the canonical consumer surface
  for sentinel errors, and a new identity test
  (`TestSentinelAliasesShareIdentity` + behavioral counterpart) asserts that
  every exported sentinel surface (graph, store, io, index, tiered aliases)
  shares the SAME error value as its canonical declaration — replacing an
  alias with a fresh `errors.New` (same message, broken `errors.Is`) now
  fails the build instead of silently breaking consumers. Audit confirmed no
  existing duplicates: store/core/io/index each own their sentinels and
  every other surface aliases them.
- Doc contracts hardened where misuse silently corrupts state:
  `Node/Relationship.Temporal()`/`Integrity()` now state MUST-NOT-mutate
  outside the graph layer (shared pointers feed store caches and the hash
  chain); `AppendPropertyHashBytes` documents its panic-recovery contract;
  `Index().CreateVector` documents that entries (not definitions) are
  rebuilt on restart; the `Store` interface documents the sharded-backend
  primary-label class-immutability invariant (B33).

### Changed — `types` sentinel messages carry the right package prefix

`types.ErrNilNode` / `types.ErrNilRelationship` messages were prefixed
`graph:` although they are declared (and can be returned) by `pkg/types`.
Now `types:`. Sentinel identities are unchanged — `errors.Is` checks are
unaffected; only the message text differs.

### Added — tiered store background-error recovery + repair/contention observability

- `tiered.Store.RecoverBackgroundError()`: the sticky background error
  (recorded on idle/transient cold-shard close failures) previously poisoned
  the store for the process lifetime — a transient NFS hiccup required a full
  close/re-open. Recovery re-probes the persistence path with an atomic
  catalog save: on success the gate clears in place; on failure the original
  cause is retained (probe failure joined in) and returned. Fail-closed
  defaults are unchanged — recovery is explicit and operator-driven, and it
  clears the lifecycle gate only (run `VerifyShard`/`RunRepair` for data
  confidence).
- `RunRepair` now logs a warning with exact counts when it fixed cross-shard
  inconsistencies — repaired residue from a crash window is operator-visible
  instead of silent.
- The bounded TOCTOU retry loops (node delete, relationship endpoint lock)
  now report what kept changing on the final attempt when they exhaust their
  10 retries, instead of a bare "changed after 10 retries".

### Changed — relationship creation has a single shared kernel

Relationship creation was implemented four times: `Add`, `AddByID`,
`AddByIDIfAbsent`, and inline in `BatchBuilder.Execute` — each carrying its
own copy of the endpoint-hash/constraint ladder, rel-type token allocation
with registry rollback, persistence, and partial-failure handling. The MR
protocol's "standalone fixes must also land in batch paths" rule existed to
compensate for this by process. All four doors now share
`relationship_create_kernel.go` (`prepareRelCreate`, `relEndpointHashLadder`,
`createRelWithTypeRollback`), so an invariant added to creation semantics
lands behind every door by construction. No behavior change; the new
door-equivalence suite (`batch_door_equivalence_test.go`) pins self-loop
policy, missing-endpoint rollback (including rel-type token rollback),
valid-time stamping/resolution, integrity/endpoint-hash capture, and
reserved-prefix rejection to identical outcomes across doors.

## [4.5.0] - 2026-06-10

### Performance — frozen rows: zero-copy scan reads

Store query/scan paths no longer deep-copy every returned row. Cache and
canonical-map entries are now **frozen** (`types.Node.Freeze()` /
`types.Relationship.Freeze()`) when published, and scan paths return the
shared frozen pointer directly. Measured on a read-heavy graph workload
(5k nodes / 25k rels, BadgerInMemory): label-scan aggregation
−19% time / −35% allocations, 2-hop traversal −14%, var-length −10%.

Contract changes:

- **Frozen entities.** `Freeze()`/`IsFrozen()` added to `types.Node` and
  `types.Relationship` (the flag occupies former struct padding — sizes are
  unchanged, pinned by `TestFrozen_FlagFitsInPadding`). Error-returning
  mutators (`SetProperty`, `DeleteProperty`, `SetProperties`,
  `SetOwnedProperties`) return the new sentinels `types.ErrFrozenNode` /
  `types.ErrFrozenRelationship` on frozen entities; void/bool mutators
  (`SetVersion`, `SetTemporal`, `SetIntegrity`, `SetTypeTokenRaw`,
  `Add/Remove/SetLabelTokensRaw`) panic — a silent no-op would corrupt
  store caches invisibly. `DeepCopy()` always returns a mutable copy and is
  the thaw operation.
- **Scan reads return frozen rows.** All plural/scan reads (`*ByLabel*`,
  `AllNodes`/`AllRelationships`, `GetNodesByIDs`/`GetRelationshipsByIDs`,
  `Outgoing`/`Incoming` traversals, temporal and index scans) on memory,
  badger, and tiered stores return shared frozen pointers. Rows for
  duplicate requested IDs may alias the same pointer — harmless because
  frozen. Callers that mutated scan results must `DeepCopy()` first.
- **Point reads stay mutable.** `GetNode`/`GetRelationship` still return
  independent deep copies — graph-core write flows mutate what they fetch.
- Memory/badger/tiered publish sites freeze at `Put`/`LoadClean`; badger
  decode paths freeze in place because the decoded object is shared between
  the cache and the caller. Tiered relationship fan-out
  (`collectRelationshipsFromStore`) freezes before installing into the
  shared result map (previously aliased mutable rows across duplicate
  slots — latent corruption hazard, now impossible).
- Duplicate-ID contract tests now pin "aliasing is only legal when frozen"
  instead of "rows never alias".

Validated by the full test suite and race detector across all three store
backends (memory, badger, tiered).

## [4.4.2] - 2026-06-09

### Documentation — pin down valid-time vs transaction-time semantics

Added a "Three timestamps, three claims" section to `CLAUDE.md`, `AGENTS.md`,
and `docs/architecture.md` (Temporal Queries), pinning down the bitemporal
contract that a consumer analysis briefly misread as a bug:

- `tkg_tx_from` — "the DB recorded this fact at T". Asserted by the system,
  unconditionally: every Add allocates `TemporalMetadata` and stamps `TxFrom`.
- `tkg_created_at` — "the entity record came into existence at T".
  System-derived from the snowflake ID timestamp (the shadow resolver applies
  this fallback); caller may override at Add.
- `tkg_valid_from` — "the fact holds **in the world** from T". A domain
  assertion only a recorder/curator with actual knowledge can make.
  `0` = no world-time claim made.

The docs now also state the two-doors asymmetry explicitly: the shadow
resolver returns the RAW asserted valid-from — `(Instant(0), ok=true)` when
never asserted (check the zero value, not `ok`) — while temporal queries use
the EFFECTIVE valid-from (explicit `ValidFrom`, else snowflake fallback).
This is deliberate, not a missing fallback: shadow props report
stored/asserted state, queries report effective state. Writers must never
default `tkg_valid_from := now()` — that conflates TX with VT (lesson 32).

Also fixed two stale `since [Unreleased]` references in `CLAUDE.md` that
should have read `since v4.3.0` (bitemporal support), and annotated the
Shadow Properties tables with the VT/TX category of each temporal key.

Documentation-only — no code or public-API changes.

## [4.4.1] - 2026-06-09

### Fixed — sync stale doc version strings missed in the 4.4.0 release

The 4.4.0 entry claimed the current-state docs were updated, but two version
strings were left at `v4.3.2`: `AGENTS.md` (`Status:`) and `docs/architecture.md`
(the title heading). `TestDocsMetadataMatchesSourceOfTruth` — which asserts those
two files carry the latest CHANGELOG version — failed as a result.

Both are now bumped to the current release, and the `Status:` lines in
`CLAUDE.md` and `README.md` updated to match. Documentation-only change — no code
or public-API changes.

## [4.4.0] - 2026-06-09

### Changed — repository moved to GitHub, Go module path renamed

The canonical repository moved from
`gitlab2024.bds421-cloud.com/bds421/rho/tkg` to
[`github.com/data-insights-ai/rho-tkg`](https://github.com/data-insights-ai/rho-tkg).
Accordingly the **`go.mod` module path** was renamed from
`gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4` to
`github.com/data-insights-ai/rho-tkg/v4`. The major-version suffix (`/v4`) is
unchanged — this is a host move, not a major bump.

All 387 Go source files were updated to the new import path, alongside `go.mod`
and the current-state docs (`README.md`, `CLAUDE.md`, `AGENTS.md`,
`docs/architecture.md`, `docs/persistence.md`). Historical CHANGELOG entries
retain their original `gitlab2024…` references as accurate snapshots of those
releases.

**Consumer migration:** replace
`gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4` with
`github.com/data-insights-ai/rho-tkg/v4` in every import and in `go.mod`
(`go mod edit -replace` or a direct find-and-replace), then `go mod tidy`. The
public API is otherwise unchanged.

## [4.3.2] - 2026-06-02

### Fixed — heal an undercounted persisted node/rel counter on reopen

A hard crash (SIGKILL / `docker kill`) can leave a shard's persisted entity
counter BELOW the number of clean, current rows actually on disk — increments
that were live in memory but whose counter write was lost. Because every entity
row decodes cleanly (`liveRows == rawEntityRows`), no data is missing, yet
`reconcilePersistedCounter` previously fataled this with `counter N does not
match M live rows`, making the store impossible to reopen.

`reconcilePersistedCounter` now heals an **undercount** up to the live row count
(`persisted < liveRows && liveRows == rawEntityRows`), logging a warning so the
operator knows a crash was recovered. The opposite direction — `persisted >
liveRows`, where the counter claims more rows than exist (rows genuinely missing
→ data loss) — stays fatal, preserving the corruption detector
(`TestBadgerStoreLoadRejectsMismatchedPersistedCounters`).

This, together with the 4.3.1 property-key registry fix, recovers tiered stores
left inconsistent by an unclean shutdown: both the tokenized-row decode and the
counter divergence are now resolved on open. Verified against a real 372,986-node
crash dump (all nodes, 2,885 cases, properties intact).

## [4.3.1] - 2026-06-02

### Fixed — property-key registry crash on tiered reload (`node counter does not match N live rows`)

The property-key tokenization shipped in 4.3.0 could fatal a tiered store on
reopen with `node counter does not match N live rows`. Two independent causes,
both fixed:

- **Per-shard empty registry (propagation).** The single canonical property-key
  registry was persisted only on the reference shard, but on reopen each *event*
  shard loaded its own (empty) meta copy. Tokenized event rows then failed to
  decode during `loadIndexes`, dropped from the live-node map, and the persisted
  counter no longer matched — fatal. The tiered store now injects the **one
  canonical registry instance** into every shard at open (hot, warm, lazy
  cold/archive, and rotation-created shards), so all tokenized rows decode.
- **Missing write-ahead durability.** New property-key tokens are allocated by
  the core engine during validation but were only persisted at `Close`. A crash
  between a tokenized row flushing and `Close` left the row durable while its
  token was not — undecodable on reload → same fatal. `flush()` now write-aheads
  the registry (with `fsync`) to its canonical location **before** the row
  `WriteBatch`, so every durable row's tokens are themselves durable. Cost is
  O(flush cycles), not O(keys): a lock-free length watermark skips the common
  no-growth case, and the `fsync` fires only on the rare cycle where a genuinely
  new key first appears.

### Added

- `badger.Config.PropertyKeyRegistry` — lets an owner (the tiered store) install
  ONE canonical registry shared by all shards; installed before `loadIndexes` so
  row decoding can resolve tokens. When nil, the store loads its own meta copy
  (standalone behaviour, unchanged).
- `badger.Config.OnPropertyKeyGrow` — write-ahead hook invoked from `flush()`
  before the row `WriteBatch` when the registry has grown; the tiered store wires
  it to commit the shared registry to the reference shard with `fsync`.
- `badger.Store.PropertyKeyRegistry()` — accessor the tiered store uses to obtain
  the reference shard's canonical registry instance.

### Migration notes

- No on-disk format change. Existing Badger / Tiered DBs work unchanged. Stores
  written by 4.3.0 that were not yet reopened are recovered correctly on the
  first open under 4.3.1 (all rows decode against the injected canonical
  registry).
- `SavePropertyKeyRegistry` now `fsync`s on disk-backed, async-write stores. The
  extra `fsync` only occurs when the property-key set grows (rare; bounded by the
  schema). In-memory and `SyncWrites` stores are unaffected.

## [4.3.0] - 2026-06-01

### Added — O(1) relationship degree (count-from-index fast path)

- New `DegreeCapability` on the Store contract: `IncomingDegree` /
  `OutgoingDegree` count a node's relationships of a given type directly from the
  relationship index entries, without resolving the relationship entities.
  Implemented on `badger.Store`, `memory.Store`, and `tiered.Store`.
- `RelOps` degree queries use the capability when available and fall back to the
  traversal count otherwise, so callers get O(1) degree on backends that support
  it with no behaviour change elsewhere.

### Added — bitemporal queries + caller-controlled valid time on Update

Five-phase bitemporal rollout. Storage was already bitemporal (separate
TxFrom / TxTo / ValidFrom / ValidTo per version), but the write API and
resolver conflated transaction time with valid time. This release adds
the second axis to the read API, lets callers control valid time on
Update, and tiles version timelines via the resolver.

**Phase 0 — bitemporal queries (read-only).**
- `QueryOpts.TxAt types.Instant` filters the version chain to entries
  visible at the given transaction time (`TxFrom <= TxAt < TxTo`).
  `TxAt == 0` keeps today's "no TX filter" behaviour.
- `QueryOpts.IncludeEclipsed bool` reserved field; not consulted yet.
- New `TempOps.NodeAtTx` / `RelAtTx` / `NodesAtTx` / `RelsAtTx` —
  bitemporal point queries. `txAt == 0` matches the existing
  valid-time-only variants.
- All `*ByLabel` / `*ByType` queries that take `QueryOpts` now honour
  `TxAt` automatically via `findNodeVersionForOpts`.

**Phase 1 — resolver de-conflation (no schema bump).**
- `updateNodePreparedInternal` / `updateRelationshipPreparedInternal`
  no longer copy `ValidFrom` / `ValidTo` from the previous version.
  New non-genesis versions have `ValidFrom == 0`, so the resolver
  derives `vStart` from `UpdatedAt` cleanly.
- `nodeVersionInheritedValidFrom` / `relVersionInheritedValidFrom`
  remain as back-compat shims for pre-Phase-1 data. They are harmless
  on new data (predicate fails on `tm.ValidFrom == 0`).
- No on-disk schema change; no migration.

**Phase 2 — Update accepts `tkg_valid_from` / `tkg_valid_to`.**
- Standalone, batch, and tx Update paths all accept the two shadow keys
  via the new `extractTemporalTracked` extractor and `updateTemporal`
  struct (mirrors `updateProvenance`).
- **`UpdateInPlace`** (both nodes + rels) also accepts the temporal
  shadow keys. Because UpdateInPlace preserves version (no chain entry),
  the temporal coords overwrite the current row's `TemporalMetadata`
  directly. Useful for data-fix corrections without polluting history.
- `tkg_created_at` remains rejected on Update (`ErrReservedPrefix`) —
  per-entity, not per-version.
- New sentinel `ErrValidFromBeforePrevious` (re-exported on
  `pkg/graph`): caller-supplied `tkg_valid_from` must be strictly
  greater than the previous version's effective `ValidFrom`. Applies to
  the version-bumping `Update`; `UpdateInPlace` does not enforce this
  (it's a direct rewrite, not a new tile).

**Phase 3 — full cascade timeline edit.**
- Resolver `nodeVersionBounds` / `relVersionBounds` derive each
  version's `vEnd` from `next.ValidFrom` when explicit (fall back to
  `next.UpdatedAt`), skipping eclipsed rows. Adjacent versions tile
  automatically once the caller sets explicit `ValidFrom`.
- New `TempOps.SetNodeVersionInterval(id, validFrom, validTo, props)`
  and `SetRelVersionInterval` — full cascade implementation.
  Classifies every existing history version vs the target interval and
  applies one of five actions:
  - **keep** — no overlap, untouched
  - **closeRight** — close existing at `newVF`
  - **openLeft** — re-open existing from `newVT`
  - **eclipse** — existing fully contained → zero-width sentinel
    (`ValidTo = ValidFrom + 1`); resolver skips eclipsed rows
  - **split** — existing spans target → left fragment keeps original
    version, right fragment gets a fresh version
- Existing history rows rewritten in place via `PutNodeVersion` /
  `PutRelVersion`. Hashes preserved because `TemporalMetadata` is not
  part of the content hash.
- Mid-history insertion (backdating between existing versions)
  supported. Atomicity bounded by the entity lock — partial-state
  crashes are visible to readers but resolver tolerates them.

**Property-key tokenization (report 6.5).**
- New `registrypkg.PropertyKeyRegistry` (`internal/registry/property_key_registry.go`)
  mirrors `LabelRegistry` for property keys. Capacity-soft: when the
  uint16 ceiling is reached, `GetOrCreate` returns `(0, nil)` so wire
  encoders fall back to raw-key writes instead of failing.
- New optional `propertyKeyPersister` interface on the Store contract;
  implemented on `badger.Store` (via `MetaKey("property_keys")`),
  `tiered.Store` (delegates to refShard), and the in-memory backend (no
  persistence needed). Registry is loaded on `Core.New` and saved on
  `Core.Close` alongside the existing label / reltype registries.
- Registry is populated automatically via `validateOwnedPropertyEntryForCreate`
  and `validatePropertyUpdates` — every distinct property key seen on
  Add or Update gets a token.
- New `g.Stats().PropertyKeyCount()` exposes the current cardinality
  for monitoring against the uint16 ceiling.
- **Wire-format integration.** New `PropertyWire.KeyToken uint16
  msgpack:"kt,omitempty"` field. Encoder (`MarshalNodeWireWithKeys` /
  `MarshalRelWireWithKeys`) tokenizes property keys via the registry
  and omits the string Key when a token is allocated. Decoder
  (`ResolvePropertyKeyTokens`) resolves tokens back to keys via the
  registry. V1 reads (Key string only) keep working — Key is preferred
  when both are present. Backends gain `SetPropertyKeyRegistry` via a
  new `propertyKeyRegistrySetter` capability; `Core.New` installs the
  loaded registry on the store after persistence load.
- **Critical init ordering**: `badger.New` loads the persisted
  property-key registry BEFORE `loadIndexes` so index rebuild can decode
  tokenized rows. Without this, V2 rows would silently drop from the
  live-node map.
- Custom `EncodeMsgpack` on `PropertyWire` (`wire_encode.go`) updated to
  emit the new `kt` field with omitempty semantics.

**Phase 1 proper — bitemporal data migration.**
- New OPTIONAL `MetaKVCapability` interface on the Store contract
  (`MetaGet` / `MetaSet`). Implemented on `memory.Store`, `badger.Store`
  (via existing `MetaKey` pattern), `tiered.Store` (delegates to the
  reference shard).
- One-shot, idempotent migration runs in `Core.New`: for every node +
  rel history row with `Version > 0`, clear `ValidFrom` iff it equals
  the immediately-preceding version's `ValidFrom`. Gated by
  `meta/schema_version` key.
- After successful migration the resolver's inheritance heuristic is
  bypassed via `c.bitemporalMigrated`. Backends without `MetaKVCapability`
  (or stores where migration fails) keep the heuristic active —
  back-compat preserved.

**Phase 4 — docs + lessons.**
- Three new entries in `tasks/lessons.md`: TX vs VT separation in the
  resolver; valid-time tiling via `next.ValidFrom`; Update no longer
  inherits valid-time.
- `CLAUDE.md` shadow-properties table notes Update support; temporal
  queries section gains the AsOf-TX paragraph.
- README "What's new" updated.

### Migration notes

- No on-disk format change. Existing Badger / Tiered DBs work
  unchanged.
- Pre-Phase-1 data with inherited `ValidFrom` on history rows continues
  to resolve correctly via the back-compat shim
  (`nodeVersionInheritedValidFrom`). Heuristic-free behaviour requires
  data rewritten by post-Phase-1 code; no automatic rewrite is
  performed.
- Existing query callers that ignore `QueryOpts.TxAt` see no behaviour
  change — `TxAt == 0` is the no-filter default.
- Edge case: a Phase 2 caller who sets `tkg_valid_from` exactly equal
  to the previous version's `ValidFrom` will be rejected with
  `ErrValidFromBeforePrevious`. The strict-greater-than rule keeps the
  back-compat shim from misclassifying an explicit value as inherited.

## [4.2.3] - 2026-05-15

### Changed - Full documentation sync (2026-05-15)

Docs-only patch. No code change, no API surface change, no behavior
change. Brings every `.md` file in the repository current with the
v4.0 → v4.2.x release map.

- **CLAUDE.md / AGENTS.md status lines** updated to v4.2.x; replaced
  the "see CHANGELOG `[Unreleased]` for the v3.4 → v4.0 migration
  recipe" pointer with explicit per-version anchors
  (`[4.0.0]` / `[4.1.0]` / `[4.2.0]` / `[4.2.1]` / `[4.2.2]`).
- **README.md** `### What's new in Unreleased` heading renamed to
  `### What's new in v4 (v4.0.0 → v4.2.2)` and prepended summaries
  for v4.2.2, v4.2.1, v4.2.0 (field→method), v4.1.0 (Path B), and
  v4.0.x (tx-read deadlock fixes). Two "Current Unreleased code
  supersedes" prose-staleness lines in the 3.1.x historical sections
  rephrased as "Superseded by later releases".
- **docs/api.md** sub-API table updated: field-shape headings
  (`g.Nodes`, …) rewritten to method-shape (`g.Nodes()`, …); count
  corrected `13 → 14`; "v3.4 sub-API surface" heading bumped to
  `v4.2`; consumer-ergonomics alias paragraph added pointing to
  `graph.QueryOpts` and the new sentinel aliases.
- **docs/architecture.md** "Sub-API Accessors (v3.4.0)" section
  heading and intro updated to mention the v4.2.0 field→method
  conversion; file-map row for `graph.go` rewritten to note the
  unexported sub-API pointers + 14 accessor methods + the v4.2.1
  type-alias additions (`QueryOpts`, `ShardDepth`, `DistanceMetric`).
- **docs/SPEC.md** `tkg/v3` → `tkg/v4`; `13 sub-API field accessors`
  → `14 sub-API accessor methods`.
- **tasks/lessons.md** lesson 9 BAD/GOOD example updated to the
  current accessor-method shape and a real method name
  (`g.Nodes().Get`); a one-line note added that v4.1.0's Path B
  removed the lifetime-Lock but the underlying principle still
  applies inside legitimately-locked internal code paths.

Historical CHANGELOG entries describing earlier releases (`[3.x]`,
`[4.0.x]`, `[4.1.0]`, `[4.2.0]`, etc.) keep their original prose with
field-shape examples and version-specific phrasing — those are
accurate snapshots of each release at its time and must not be
rewritten.

## [4.2.2] - 2026-05-15

### Added - Alias ErrNodeExists / ErrRelExists on graph package (2026-05-15)

Follow-on to v4.2.1's consumer-ergonomics aliases. The entity-conflict
sentinels (returned by `Import` and `AddByIDIfAbsent` create paths when
a caller-supplied ID is already present) were not yet aliased on
`pkg/graph`, forcing consumers to retain a `storepkg` import for those
two `errors.Is` checks. Two-line addition; no behavior change.

- **`graph.ErrNodeExists`** — alias for `store.ErrNodeExists`.
- **`graph.ErrRelExists`** — alias for `store.ErrRelExists`.

After upgrading, the conflict-409 paths in downstream DTO mappers no
longer need to import `pkg/graph/store` alongside `pkg/graph`.

## [4.2.1] - 2026-05-15

### Added - Public aliases for QueryOpts + index sentinels (2026-05-15)

Consumer-ergonomics patch — lets the Cypher engine (and other
downstream callers) use `graph.QueryOpts` + the index-sentinel errors
without importing `pkg/graph/store` alongside `pkg/graph`. No
behavior change; purely additive type/var aliases on the `graph`
package.

- **`graph.QueryOpts`** — alias for `store.QueryOpts`. Configures
  pagination, depth, and temporal filtering for read methods on
  `g.Nodes()`, `g.Rels()`, `g.Temporal()`, `g.Index().SearchNearest`.
- **`graph.ShardDepth`** — alias for `store.ShardDepth` (the `Depth`
  field of `QueryOpts`). Use the `storepkg.Depth*` constants —
  they stay where they are since they're tiered-store specific.
- **`graph.DistanceMetric`** — alias for `store.DistanceMetric`,
  accepted by `g.Index().SearchNearest`. Use the `storepkg.Distance*`
  constants for values.
- **`graph.ErrIndexExists`** — alias for `store.ErrIndexExists`
  (property index already exists).
- **`graph.ErrIndexNotFound`** — alias for `store.ErrIndexNotFound`.
- **`graph.ErrTemporalIndexExists`** — alias for `store.ErrTemporalIndexExists`.
- **`graph.ErrTemporalIndexNotFound`** — alias for `store.ErrTemporalIndexNotFound`.

The vector-index sentinels (`ErrVectorIndexExists`,
`ErrVectorIndexNotFound`) were already exported; this patch closes
the parallel gap for property and temporal indexes.

After upgrading, consumer-side import of `storepkg` is no longer
required for the common read-and-error-check pattern; just import
`pkg/graph`.

## [4.2.0] - 2026-05-15

### Changed - Sub-APIs are accessor methods, not exported fields (2026-05-15)

The 14 sub-API field accessors on `*Graph` (`g.Nodes`, `g.Rels`,
`g.Temporal`, `g.Index`, `g.Events`, `g.Constraints`, `g.IO`,
`g.Admin`, `g.Tier`, `g.Stats`, `g.Hash`, `g.Resolve`, `g.Tx`, `g.Batch`)
are now **methods** (`g.Nodes()`, `g.Rels()`, …). The exported fields
are gone; the underlying sub-API pointers live in unexported fields and
the accessor methods return them.

#### Why

- More idiomatic Go. Stdlib uses methods on receivers; exported struct
  fields as the primary API is a Java/C#-style accent. See
  CLAUDE.md "Code Style" notes from the v4.1.0 review thread.
- Centralizes nil-safety. `(*Graph)(nil).Nodes()` returns nil cleanly;
  zero-value `(&Graph{}).Nodes()` returns nil; both yield nil sub-API
  pointers whose methods fail closed with `ErrNilGraph` via the
  existing zero-value contract.
- Removes a class of footgun: customers can no longer accidentally
  assign or zero out `g.Nodes` from outside the package.

#### Migration recipe (mechanical sed)

```
sed -i '' -E 's/\.(Nodes|Rels|Temporal|Index|Events|Constraints|IO|Admin|Tier|Stats|Hash|Resolve|Tx|Batch)\./.\1()./g' **/*.go
```

Run from any consumer repo's root. Catches the dot-prefixed access
pattern (`g.Nodes.Add(...)` → `g.Nodes().Add(...)`). Bare field
references (`var x = g.Nodes`) need a one-line follow-up: replace with
`var x = g.Nodes()`. The compiler will flag every remaining occurrence.

Inside `tkg` itself the migration touched 34 files (~5,218 call-site
updates).

#### Tests

- `pkg/graph/accessor_test.go` (new) — 8 test functions covering:
  - Reflection-driven enumeration: every exported method on `*Graph` is
    checked for nil-receiver safety.
  - Zero-value `Graph{}` returns nil for every accessor.
  - Chained `(*Graph)(nil).Nodes().Add(...)` fails closed with
    `ErrNilGraph`.
  - After `Close()`, accessors still return live sub-API pointers,
    but calls return `ErrGraphClosed`.
  - Pointer stability: `g.Nodes() == g.Nodes()` on consecutive calls.
  - Struct-shape regression: `*Graph` must have **zero** exported
    fields (catches accidental re-introduction).
  - Reflection-driven signature check: all 14 expected accessor methods
    exist and return a single pointer.
- Existing `subapi_zero_value_test.go` (~70+ table-driven cases for
  zero-value sub-API behavior) is unchanged — the field-vs-method
  conversion didn't alter the sub-API zero-value contract.

#### What was NOT changed

- Each sub-API's `ok bool` zero-value guard. Could have been simplified
  to a single nil-check, but that would force `grapherr.IsNil` (reflect)
  on every method call. Keep the field, keep the constant-time check.
- The sub-API package layout under `pkg/graph/{nodes,rels,…}`.
- Internal-core references to `*Core.Nodes`, `*Core.Rels` etc. (those
  are still exported fields on `*Core` because the internal sub-API
  packages need them — that surface is not customer-facing).

## [4.1.0] - 2026-05-14

### Changed - Tx isolation: drop c.mu.Lock from tx lifetime (Path B) (2026-05-14)

**Eliminates the entire `tx-vs-c.mu.RLock` deadlock class.** v4.0.1 and
v4.0.2 added tx-side mirrors for every read accessor that took
`c.mu.RLock` — that worked but only by patching one accessor at a time
and was missable. v4.1.0 attacks the root: `BeginTx` no longer holds
`c.mu.Lock` for the transaction lifetime.

#### What changed

- **New `c.txMu sync.Mutex`** (`core.go`) replaces `c.mu.Lock` as the
  serialization mechanism between concurrent transactions and batches.
  `BeginTx` and `BatchBuilder.Execute` both acquire `c.txMu`.
- **Each tx method takes `c.mu.RLock` briefly** around its body
  (via new `tx.lockActiveCore` / `tx.unlockActiveCore` helpers) so
  the `*Internal` / `*Locked` helpers see the same lock context the
  standalone call paths provide via `runUnderRLock` / `readUnderRLock`.
  `tx.lockActiveCore` checks `tx.done` and `c.closed` together.
- **Commit / Rollback** take `c.mu.Lock` briefly (for registry pointer
  mutation safety), then release `c.txMu`. They no longer hold
  `c.mu.Lock` for the tx duration.
- **Batch execution** also takes `c.txMu` first then `c.mu.Lock` (held
  for the whole batch — batches are short-lived and atomic).

#### What this fixes

- `g.Nodes.ByLabel`, `g.Rels.Outgoing`, `g.Temporal.NodesAt`, every
  `*By*(opts QueryOpts)` / `*At(...)` / `*AsOf(...)` method, `Stats.*`,
  `Index.SearchNearest`, and every other public read accessor now
  **work correctly inside an open `*GraphTx`**. The non-tx accessors
  acquire `c.mu.RLock` — under v4.1.0 nothing holds `c.mu.Lock` during
  the tx, so RLock succeeds immediately.
- The bug class is gone going forward. Future read accessors that take
  `c.mu.RLock` automatically work inside a tx with no tx-side mirror
  required. Lesson 31's "mirror every accessor" audit recipe is
  superseded.

#### Isolation semantics (minor-version-bump price)

- **Before (v3.4 / v4.0.x):** tx holds `c.mu.Lock` for its lifetime →
  ALL concurrent standalone mutations and ALL reads from other
  goroutines block until the tx ends. "Serializable graph-wide."
- **After (v4.1.0):** tx holds `c.txMu` for its lifetime and `c.mu.RLock`
  briefly per call. Concurrent standalone mutations on DIFFERENT
  entities run in parallel; entity-level conflicts still serialize via
  the existing entity-lock manager. Concurrent reads from other
  goroutines proceed in parallel. **"Serializable per touched entity,
  snapshot-isolated elsewhere."**
- **Visible side effect:** a concurrent reader can see an in-progress
  tx's allocated labels / types before the tx commits or rolls back. On
  rollback, those allocations are revoked. If a reader captured a token
  during the tx that the tx later rolled back, the token's name lookup
  will return `(0, false)` after rollback; the token itself is no
  longer mapped. Code that depended on "tx blocks all concurrent
  observation" must take an external lock to preserve that guarantee.

#### Tests that pinned the old semantics

- `TestMutationBlockedDuringTx` → renamed `TestMutationProceedsAlongsideTx`,
  now asserts the standalone mutation completes promptly.
- `TestGraphTx_PublicResolverReadsWaitForRollback` →
  `TestGraphTx_PublicResolverReadsDoNotBlock`, asserts concurrent reads
  complete and post-rollback the in-progress allocations are revoked.
- The three "deadlock regression" tests added in v4.0.1 / v4.0.2 were
  inverted: the non-tx accessors now WORK inside a tx and the tests
  assert that.

#### What was NOT changed

- The tx-side read mirrors added in v4.0.1 and v4.0.2 are kept. They
  no longer prevent deadlock (there's nothing to deadlock against) but
  remain functional and provide a clearer call-site signal that "this
  read is inside the tx I'm holding." They take `c.mu.RLock` via the
  same helper used by tx mutations.
- `Temporal.DiffCallback` still releases `c.mu.RLock` between callbacks
  by design (handlers may re-enter the graph). Under v4.1.0 this no
  longer deadlocks inside a tx, so its v4.0.2-deferred mirror is no
  longer needed.

## [4.0.2] - 2026-05-14

### Fixed - Tx-read deadlock against c.mu (bulk read methods) (2026-05-14)

- **v4.0.1 covered only the metadata-resolution accessors.** The actual
  data-retrieval reads (`Nodes.ByLabel`, `Rels.Outgoing`, `Temporal.NodesAt`,
  every `*By*(opts QueryOpts)` and `*At(...)` method, the `*AsOf` set,
  `Stats.*`, `Index.SearchNearest`, `Tier.ListShards`, `Events.GetSync`,
  `Constraints.Get`) all go through `c.mu.RLock` either directly or via
  `c.readUnderRLock(func(){…})`, and still deadlocked inside an open
  `*GraphTx` for the same reason: `BeginTx` holds `c.mu.Lock` for the tx
  lifetime, `sync.RWMutex` is not reentrant.
- Operationally critical second incident: Cypher
  `CREATE (n:Widget {…}) RETURN n; MATCH (n:Widget) RETURN n.name` hung at
  the `MATCH` because the engine keeps one tx open across all statements
  and the second statement called `g.Nodes.ByLabel` inside it.
- **Fix.** Extracted ~20 `(*Core).fooLocked` helpers from the
  `readUnderRLock(func(){…})` closure bodies in `queries.go`,
  `graph_property_query.go`, `temporal_queries.go`, `txtime.go`,
  `stats.go`. Both the public methods (under `c.mu.RLock`) and the new
  tx mirrors (under `tx.mu` only) now route through these helpers.
- Audit recipe is now in lessons.md #31: any new public read method
  that takes `c.mu.RLock` or `c.readUnderRLock` owes a tx mirror.
  The audit grep is `c\.mu\.RLock\|c\.readUnderRLock` in pkg/graph/internal/core/.
- **Known follow-up.** `Temporal.DiffCallback` retains its multi-segment
  `readUnderRLock` design (lock released between callbacks so handlers
  can re-enter the graph). It is not yet mirrored — its lock-release
  semantics make the tx-side contract non-trivial. Deferred to v4.1.0
  (Path B: drop `c.mu.Lock` from tx lifetime), which eliminates the
  whole bug class.

### Added - Tx-side bulk read accessors (2026-05-14)

- **27 new `(*GraphTx)` methods** in
  `pkg/graph/internal/core/tx_consistent_reads.go`:
  - Nodes: `GetNodesByIDs`, `AllNodes`, `NodesByLabel`,
    `NodesByLabelAndProperty`, `NodeCount`, `NodeCountByLabel`.
  - Rels: `GetRelsByIDs`, `AllRels`, `RelsByType`, `OutgoingRels`,
    `IncomingRels`, `OutgoingRelsForNodes`, `IncomingRelsForNodes`,
    `RelCount`, `RelCountByType`.
  - Temporal: `NodesAt`, `RelsAt`, `NodesByLabelAt`, `NodesDuring`,
    `RelsDuring`, `NodeAt`, `RelAt`, `NeighborsAt`, `OutgoingRelsAt`,
    `IncomingRelsAt`, `NodesByLabelPropertyAt`,
    `NodesByLabelPropertyDuring`, `RelsByTypePropertyAt`,
    `RelsByTypePropertyDuring`.
  - AsOf (bitemporal): `NodeAsOf`, `RelAsOf`, `NodesAsOf`, `RelsAsOf`.
  - Stats: `Stats`, `AllLabelCounts`, `AllRelTypeCounts`.
  - Misc: `SearchNearest`, `IndexProviders`, `ListShards`, `EventBus`,
    `AsyncEventBus`, `Constraints`.
- Each guards only `tx.mu`, dispatches to the lock-free
  `*Locked`/`*Unlocked` helper, and matches the corresponding non-tx
  accessor's zero-value return contract after Commit/Rollback.

## [4.0.1] - 2026-05-14

### Fixed - Tx-read deadlock against c.mu (2026-05-14)

- **`g.Nodes.Labels`, `g.Nodes.HasLabel`, `g.Nodes.PrimaryLabel`,
  `g.Rels.Type`, `g.Rels.HasType`, `g.Resolve.NodeProperty`, and
  `g.Resolve.RelProperty` deadlocked when called from inside an open
  `*GraphTx`.** Every accessor takes `c.mu.RLock`; `BeginTx` holds
  `c.mu.Lock` for the whole tx lifetime; `sync.RWMutex` is not reentrant
  (lesson 9), so the read accessor blocked forever waiting for a lock the
  same goroutine already owns through the tx. Operationally critical:
  every Cypher `CREATE ... RETURN n` flow that resolves labels on the
  returned entity inside the tx hit this.
- **Fix.** Added tx-side mirrors that call the lock-free `*Unlocked`
  helpers directly without re-entering `c.mu`. See "Added".
- The non-tx accessors are unchanged — they remain correct outside a tx.
  Inside a tx, callers must now use the `*GraphTx` mirrors.

### Added - Tx-side resolution and shadow-property accessors (2026-05-14)

- **`(*GraphTx).Labels(n)`, `PrimaryLabel(n)`, `HasLabel(n, name)`,
  `RelType(r)`, `HasType(r, name)`, `NodeProperty(n, key)`,
  `RelProperty(r, key)`.** Mirror the seven `g.{Nodes,Rels,Resolve}.*`
  read accessors that previously deadlocked when called inside a
  transaction. Each guards only `tx.mu` (returns the zero value of its
  return type after Commit/Rollback) and dispatches to the existing
  `nodeLabelsUnlocked`, `nodePrimaryLabelUnlocked`, `nodeHasLabelUnlocked`,
  `relTypeUnlocked`, `relHasTypeUnlocked` helpers plus two new
  `nodePropertyUnlocked` / `relPropertyUnlocked` extracted from the
  inline `ResolveOps.NodeProperty` / `RelProperty` switch bodies.
- Lives in `pkg/graph/internal/core/tx_consistent_reads.go` next to the
  existing `tx.Export` / `tx.Snapshot` / `tx.VerifyShard` consistent-read
  family.

## [4.0.0] - 2026-05-14

### Changed - Module path bump v3 → v4 (2026-05-14)

- **`go.mod` module path** rewritten from `gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3` to `gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4`. All 372 Go source files updated to the new import path. Historical CHANGELOG entries under `[3.2.0]` and `[3.0.54]` retain their original `/v3` references as accurate snapshots of those releases.
- **Documentation refresh for v4.** README, AGENTS, CLAUDE, and `docs/{architecture,persistence,SPEC}.md` updated to v4 branding: heading, intro prose, ecosystem-table labels, status paragraph. `docs/architecture.md` adds a `g.Tier` row to the sub-API table and corrects the `g.Admin` row to its v4 backend-agnostic surface (`Reset`, `DecomposeNodeID`, `DecomposeRelID`). Sub-API field-accessor count updated `13 → 14` across CLAUDE.md, AGENTS.md, and `docs/architecture.md`.
- **CLAUDE.md consolidation.** Collapsed the duplicated `pkg/graph/<sub-api>/` and `pkg/graph/<types-package>/` tables into a single table; merged the `BadgerStoreConfig` / `Graph.Config` duplication; replaced the inline v4 migration recipe with a pointer to this section. ~30 lines tighter, no rule removed. `git log --follow` still works.

### Changed - API 4.0 surface cleanup (2026-05-14)

Breaking changes — all customer call sites need updating. The shape changes
below are concentrated in `pkg/graph/{nodes,rels,admin,resolve,io,index,temporal,events}`
and the new `pkg/graph/tier` sub-API.

**Migration recipe (sed-driven):**

```
# 1. Collapse *WithContext methods to context-aware base names.
sed -i '' '
s/\.AddWithContext(/.Add(/g
s/\.AddByIDWithContext(/.AddByID(/g
s/\.AddByIDIfAbsentWithContext(/.AddByIDIfAbsent(/g
s/\.GetWithContext(/.Get(/g
s/\.UpdateWithContext(/.Update(/g
s/\.UpdateInPlaceWithContext(/.UpdateInPlace(/g
s/\.DeleteWithContext(/.Delete(/g
s/\.CompareAndSetPropertyWithContext(/.CompareAndSetProperty(/g
' **/*.go

# 2. Insert context.Background() in any remaining no-context call to the
# collapsed methods. Use a context-aware editor or compile-loop driven sed.

# 3. Tiered-only Admin methods moved to a new g.Tier sub-API.
sed -i '' '
s/\.Admin\.Archive(/.Tier.Archive(/g
s/\.Admin\.Restore(/.Tier.Restore(/g
s/\.Admin\.ForceRotate(/.Tier.ForceRotate(/g
s/\.Admin\.ListShards(/.Tier.ListShards(/g
s/\.Admin\.RebuildCatalog(/.Tier.RebuildCatalog(/g
s/\.Admin\.Repair(/.Tier.Repair(/g
s/\.Admin\.VerifyShard(/.Tier.VerifyShard(/g
' **/*.go

# 4. Temporal long-form Relationships* names collapsed to short Rels*.
sed -i '' '
s/\.Temporal\.RelationshipsAt(/.Temporal.RelsAt(/g
s/\.Temporal\.RelationshipsByTypeAt(/.Temporal.RelsByTypeAt(/g
s/\.Temporal\.RelationshipsDuring(/.Temporal.RelsDuring(/g
' **/*.go

# 5. NextVersion/PreviousVersion renamed for unambiguous ordering.
sed -i '' '
s/\.NextVersion(/.VersionAfter(/g
s/\.PreviousVersion(/.VersionBefore(/g
' **/*.go

# 6. io.Import collapsed to single signature.
# For old IO.Import(r) no-options calls: append ", tkgio.ImportOptions{}".

# 7. Admin.DecomposeID(snowflake.ID) split into typed variants.
# Replace .Admin.DecomposeID(nodeID.SnowflakeID()) with .Admin.DecomposeNodeID(nodeID).
# Replace .Admin.DecomposeID(relID.SnowflakeID()) with .Admin.DecomposeRelID(relID).

# 8. Stats.Get now returns (GraphStats, error). Callers that ignored the
# error before need `, _ :=` (or `, _ =` for re-assignment). Inline field
# accessors like `g.Stats.Get().NodesRead` must split into two lines:
#   snap, _ := g.Stats.Get()
#   v := snap.NodesRead
```

**Complete change list:**

- `pkg/graph/nodes`: `Add`, `Get`, `Update`, `UpdateInPlace`, `Delete`,
  `CompareAndSetProperty` now take `ctx context.Context` as first arg.
  `*WithContext` variants removed. `NextVersion` renamed `VersionAfter`,
  `PreviousVersion` renamed `VersionBefore`.
- `pkg/graph/rels`: same collapse for `Add`, `AddByID`, `AddByIDIfAbsent`,
  `Get`, `Update`, `UpdateInPlace`, `Delete`, `CompareAndSetProperty`. Same
  version-helper rename.
- `pkg/graph/temporal`: `RelationshipsAt` → `RelsAt`, `RelationshipsByTypeAt`
  → `RelsByTypeAt`, `RelationshipsDuring` → `RelsDuring` (matches the
  package, ID-type, and sub-API-field name `Rels`).
- `pkg/graph/admin` → split into `admin` (generic: Reset, DecomposeNodeID,
  DecomposeRelID) plus new `pkg/graph/tier` (tiered-only: Archive, Restore,
  ForceRotate, ListShards, RebuildCatalog, Repair, VerifyShard).
  `g.Admin.DecomposeID(snowflake.ID)` replaced with typed
  `g.Admin.DecomposeNodeID(types.NodeID)` and `g.Admin.DecomposeRelID(types.RelID)`.
  Top-level `graph.DecomposeID` package var likewise replaced by typed
  `graph.DecomposeNodeID` / `graph.DecomposeRelID` helpers.
- `pkg/graph/resolve`: token methods `LabelToken`, `RelTypeToken`,
  `LookupLabel`, `LookupRelType` removed (leaked uint16 representation).
  Shadow-property accessors `NodeProperty`, `RelProperty` kept.
- `pkg/graph/index`: `LegacyIndexProvider` interface + `RegisterLegacyProvider`
  removed. External providers must migrate to `IndexProvider` (with optional
  `Initializable` for bulk-load).
- `pkg/graph/io`: `IO.Import(r)` and `IO.ImportWithOptions(r, opts)`
  collapsed into single `IO.Import(r, opts)`. Pass `ImportOptions{}` for
  the previous default behaviour.
- `pkg/graph/subapi.go`: new `BatchAPI.Run(fn) (*BatchResult, error)` —
  parallel to `TxAPI.Run`. `TxAPI.Begin` docstring strengthened to direct
  callers to `Run`.
- `pkg/graph/stats`: `Stats.Get()` now returns `(GraphStats, error)`
  instead of `GraphStats`. Every other `g.Stats.*` method already returned
  an error; collapsing this last odd-one-out lets callers treat the sub-API
  uniformly. Migrate with `, _ :=` (or split into two lines when the result
  was field-accessed inline — `snap, _ := g.Stats.Get(); v := snap.Field`).
  In practice the error path is `ErrNilGraph` (zero-value API) or
  `ErrGraphClosed`; runtime callers can keep ignoring it with `, _`.
- `graph.ErrDepthTemporalUnsupported` removed (legacy sentinel, never
  returned in production).

### Fixed - Temporal adjacency O(history) fold (2026-05-14)

- **`g.Temporal.OutgoingRelsAt`, `IncomingRelsAt`, `NeighborsAt`** no longer
  scan the entire rel-history ID space to catch deleted-but-historically-
  valid edges. The adjacency-at-t fold now uses a new optional store
  capability, `DeletedIterationCapability` (with depth-aware companion
  `DepthDeletedIterationCapability`), that yields ONLY IDs whose history
  exists but whose current row is absent. Cost drops from O(total history)
  to O(deleted_count). Rel endpoints are immutable, so a rel that ever
  pointed at a node still does if alive — the only rels missing from the
  live adjacency index are deleted ones.
- **Store contract additions (`pkg/graph/store/capabilities.go`).**
  - `ForEachDeletedNodeID(fn) error` / `ForEachDeletedRelID(fn) error`
  - `ForEachDeletedNodeIDByDepth(depth, fn) error` /
    `ForEachDeletedRelIDByDepth(depth, fn) error`
  - All three in-tree backends (memory, badger, tiered) implement the
    capability. External stores that omit it transparently fall back to
    the previous history-scan path — correct, just slower.
- Label/property temporal queries (`g.Nodes.ByLabel(opts)`,
  `ByLabelAndProperty(...)`, etc.) keep the full-history candidate fold
  because an entity's CURRENT label/property can differ from its at-t state
  — those queries can match entities whose history overlaps the predicate
  even when the entity is currently live with different labels.

### Fixed - PublishBatch priority ordering under queue saturation (2026-05-14)

- **`events.AsyncEventBus.PublishBatch`** now preserves its documented
  priority guarantee even when a priority queue saturates mid-batch under
  `BackpressureBlock`. Previously, the in-batch wake-up that drains space
  (necessary to avoid deadlock when `QueueSize < per-priority batch size`)
  could let the dispatcher pick up a pre-existing lower-priority event
  between batch enqueues, inverting the "no lower-priority event before all
  higher-priority batch events are visible" contract.
- A per-batch priority ceiling (atomic `batchPriorityCeiling` on the bus)
  is raised for each priority pass and cleared at end-of-batch; the
  dispatcher honours it so the in-batch wake-up drains only same-or-higher
  priorities. Liveness preserved (existing
  `TestAsyncEventBusPublishBatchBlockWakesBeforeFullQueueWait` still passes).

### Added - Temporal directional accessors (2026-05-14)

- **`g.Temporal.OutgoingRelsAt(nodeID, t)` and `g.Temporal.IncomingRelsAt(nodeID, t)`.**
  Return the relationships incident to `nodeID` in the chosen direction that
  were valid at instant `t`. History-aware: includes rels that have since
  been deleted but were valid at `t`, and returns the version-at-`t` (not
  the most recent) of each. Sorted by relationship ID. Returns
  `ErrNodeNotFound` if the node was not valid at `t`. Endpoint immutability
  is exploited — start/end on the returned versions equal the routing
  endpoint by construction.

### Fixed - Hardening sweep correctness items (2026-05-14)

- **Relationship endpoint-lock retry now bounded.** `lockRelationshipCurrentEndpoints`
  in `pkg/graph/internal/core/relationship_update.go` retries at most 10 times
  before returning an "endpoints changed after N retries" error, matching
  `deleteNodeInternal`'s adjacency-retry pattern. The previous unbounded loop
  could deadlock the caller under a hostile concurrent delete+reimport workload.
- **Relationship import event-dispatch parity.** `RelOps.Import` guards its
  `dispatchEvent` call with `ep != nil` to match every other create path.
  Functionally safe before (dispatchEvent is nil-safe) — pure parity fix.
- **Property hash contract documented for NaN.** New `TestAppendPropertyValue_Float{32,64}_NaN_BitsPreserved`
  tests document the intentional design: equal-by-CAS NaN values can hash
  differently because the hash preserves the IEEE-754 bit pattern. Mirrors
  the existing tested `±0` case. `PropertyValueEqual` and the property-hash
  function carry expanded doc comments calling out the cross-system implication.
- **Registry capacity warning fires on import.** `LabelRegistry.ImportNames`
  and `RelTypeRegistry.ImportNames` now trip `warnOnce` when a restore
  pushes the registry past `tokenCapacityWarning`. Before this fix, large
  restores could exceed the threshold without any operator-visible signal.
- **Tiered store doc-only hardening.** `temporalIndexShardRefsLocked`,
  `EndpointIntegrityHashes`, `recordBackgroundError`/`backgroundError`,
  and `DeleteNodeWithHistory` now document — respectively — the
  `ts.mu → archiveMu` lock-ordering contract, the self-loop / same-shard /
  cross-shard branch semantics, the sticky poison-pill behaviour and its
  network-filesystem implication, and the partial-state error semantics
  for the "shard delete failed but node is gone" race.
- **`reconstructTypedValue` documents the trust contract.** The unchecked
  path is intentionally best-effort for legacy data (Type=ptUnknown=0); the
  checked path (`validatePropertyWire`) rejects unknown tags before reaching
  this function.

### Fixed - Index create rollback cleanup (2026-05-14)

- **Graph-level index creates now remove partial indexes after backend failure.**
  `Index.CreateProperty`, `CreateTemporal`, `CreateHighFrequency`, and
  `CreateVector` no longer leave a stale index behind after a failed create,
  including the new-label path where the label token is rolled back and later
  reused. If cleanup itself fails after a new-label allocation, the label token
  is retained and the combined error is returned so the stale index cannot be
  inherited by a different label. Duplicate/conflict create errors preserve the
  existing index instead of treating it as cleanup state.

### Fixed - Relationship create rollback cleanup (2026-05-14)

- **Relationship create paths now remove partial rows after backend failure.**
  `Rels.Add`, `AddByIDWithContext`, `AddByIDIfAbsentWithContext`,
  `Import`, and batch relationship creates delete a row that may have been
  installed before a backend returned an error, then roll back newly allocated
  relationship-type tokens. If row cleanup itself fails after a new reltype was
  allocated, the token is retained and the combined error is returned so the
  stale row cannot be inherited by a later relationship type; transaction
  callers also receive the live partial relationship so rollback can delete it.

### Fixed - Node create rollback cleanup (2026-05-14)

- **Node create and import paths now remove partial rows after backend
  failure.** `Nodes.Add`, `Import`, and batch node creates delete node rows
  that may have been installed before a backend returned an error, then roll
  back newly allocated label tokens. If row cleanup itself fails after a new
  label allocation, the label token is retained and the combined error is
  returned so the stale row cannot be inherited by a later label; transaction
  callers also receive the live partial node so rollback can delete it.
- **Create operation counters now count committed rows even when a trailing
  registry checkpoint fails.** Standalone and transaction create paths now
  update `NodesAdded`/`RelsAdded` at the point a row is known to be live, instead
  of relying on transaction-only compensating increments.
- **Batch create result counters now distinguish live rows from errored
  operations.** `BatchResult.Created` and graph create stats now include rows
  that were actually installed even when the operation also reports a trailing
  checkpoint or cleanup error through `Failed`/`Errors`.
- **Standalone create events now follow committed row state.** `Nodes.Add`,
  `Nodes.Import`, `Rels.Add`, `AddByID`, `AddByIDIfAbsent`, and `Import` now
  publish create events when a row is committed but a trailing registry
  checkpoint error is returned, matching transaction and batch event semantics.

### Fixed - Property query value validation (2026-05-14)

- **Property query APIs now reject malformed query values before empty result
  shortcuts.** `NodesByLabelAndProperty`, `NodesByLabelPropertyAt`,
  `NodesByLabelPropertyDuring`, `RelsByTypePropertyAt`, and
  `RelsByTypePropertyDuring` validate unsupported property values such as
  structs or pointers instead of silently treating them as non-indexable misses;
  valid non-indexable values still return an empty result. Store-level
  `NodesByLabelAndProperty` implementations in MemoryStore, BadgerStore, and
  TieredStore enforce the same boundary.

### Fixed - Typed nil property container preservation (2026-05-14)

- **Property deep-copy and vector-copy boundaries now preserve typed nil
  built-in slices and maps.** `PropertySlice.Set`, `NewPropertySlice`,
  `DeepCopy`, `ToMap`, and the node/relationship `Float32SlicePropertyCopy`
  helpers no longer turn values such as `[]float32(nil)` or
  `map[string]any(nil)` into non-nil empty containers, keeping exact property
  equality and CAS semantics stable. Property wire encoding now carries an
  explicit typed-nil marker for slice/map tags, and integrity hashing now
  distinguishes typed nil containers from empty containers.

### Fixed - Transaction label failed preflight snapshots (2026-05-14)

- **`GraphTx.AddNodeLabel` and `GraphTx.RemoveNodeLabel` now check no-op and
  rejection cases before capturing rollback history.** Idempotent add-label,
  too-many-labels, closed-node, missing-label, and last-label failures no longer
  copy node history or record update snapshots, and unknown remove-label inputs
  now match the standalone error precedence.

### Fixed - Absent-property delete update no-ops (2026-05-14)

- **Deleting a property that is already absent is now a true read-only no-op.**
  Standalone, in-place, transaction, and batch node/relationship update paths no
  longer write rows, append version history, increment update counters, publish
  update events, or count batch updates for absent-property delete requests.
  Metadata-only provenance updates and deletes of existing nil-valued
  properties remain real mutations.

### Fixed - Update/CAS invalid-ID error precedence (2026-05-14)

- **Existing-entity update and property-CAS paths now reject zero/negative IDs
  before validating update payloads.** Standalone, in-place, transaction, and
  batch node/relationship updates now consistently surface the store invalid-ID
  sentinel even when the supplied update map or CAS key/value is also malformed.

### Changed - Property equality helper for CAS (2026-05-13)

- **Property CAS now compares through the type layer without copying stored
  reference values.** `types.PropertyValueEqual`,
  `(*types.Node).PropertyValueEqual`, and
  `(*types.Relationship).PropertyValueEqual` expose the exact-type, NaN-aware
  property equality used by node and relationship CAS for callers that need
  compare-only semantics without receiving a defensive copy.

### Fixed - Tiered cold-shard read fanout descriptor pressure (2026-05-13)

- **Bulk tiered read fanout now processes cold event shards sequentially while
  keeping hot/warm shards parallel.** Depth-all scans still close transient
  cold shard handles after each read, and they no longer open up to 16 cold
  Badger stores at once while walking historical shards.
- **Tiered temporal-index create/drop operations now walk shards one at a time
  instead of pinning every cold shard for the full operation.** Store-wide
  temporal and high-frequency index changes still block `Close` until rollback
  or metadata persistence is complete, but historical cold shard handles are no
  longer accumulated across the whole shard set.

### Fixed - Tiered archive reference-entity contract (2026-05-13)

- **`tiered.Store.ArchiveNode` and `RestoreNode` now explicitly reject
  non-reference nodes with `ErrNotReferenceEntity`.** Event nodes no longer
  surface as missing simply because they are physically stored outside the
  reference shard, and malformed archive/ref-shard rows with event labels are
  rejected without moving data.

### Fixed - Tiered catalog rebuild cold-shard handle pressure (2026-05-13)

- **`tiered.Store.RebuildCatalog` and the shared all-shard admin enumerator now
  close cold event shards that they open only for the active operation.**
  Rebuilding stats, repairing shards, or applying tier-wide index changes
  across historical shards no longer leaves transient Badger handles open after
  release.
- **Tiered point routing now also releases transient cold event shards after the
  routed operation checks in.** Single-node lookups and relationship owner
  resolution no longer keep a lazy-opened cold shard resident until idle close.

### Fixed - Tiered clear catalog consistency (2026-05-13)

- **`tiered.Store.Clear` now keeps the live shard catalog aligned with cleared
  shard data when the final catalog save fails.** The error is still returned,
  but the in-process catalog no longer restores stale pre-clear counts and
  verification flags after the shards have already been wiped.

### Fixed - Hash verification nil history rows (2026-05-13)

- **`g.Hash.VerifyNodeChain` and `g.Hash.VerifyRelChain` now reject nil
  history rows without panicking.** Malformed backend history slices and paged
  history results now return the corresponding nil-entity sentinel instead of
  dereferencing a nil node or relationship while verifying the hash chain.

### Fixed - Export history nil-row fail-closed behavior (2026-05-13)

- **`g.IO.Export` now rejects nil node and relationship history rows without
  panicking.** Corrupt or custom backend history slices with nil entries now
  return the corresponding nil-entity sentinel instead of dereferencing the
  row while formatting the encode error.

### Fixed - Closed graph event getter lifecycle (2026-05-13)

- **`g.Events.GetSync()` and `g.Events.GetAsync()` now fail closed after
  `Graph.Close`.** No-error event-bus getters no longer expose stale
  graph-owned event pointers after the graph lifecycle has ended.

### Fixed - Index provider Close panic isolation (2026-05-13)

- **Graph provider lifecycle paths now isolate index provider `Close` panics.**
  A panicking provider close during `Graph.Close`, `g.Index.UnregisterProvider`,
  or failed-`Init` rollback no longer aborts surrounding cleanup; the panic is
  returned as part of the lifecycle error.

### Fixed - Index provider Init panic rollback (2026-05-13)

- **`g.Index.RegisterProvider` now rolls back providers whose `Init` panics.**
  A panicking initializable index provider no longer remains registered or
  subscribed to future graph events after `RegisterProvider` returns an error.

### Fixed - If-absent relationship row ownership (2026-05-13)

- **`g.Rels.AddByIDIfAbsent` now returns a defensive copy when the existing
  relationship comes from a custom store.** The duplicate-check path no longer
  exposes adjacency rows owned by external store implementations.

### Fixed - Delete-node tombstone row ownership (2026-05-13)

- **Cascade node deletes now build relationship tombstones from deep copies.**
  Failed `DeleteNodeWithHistory` calls no longer mutate adjacency relationship
  rows returned by custom stores while preparing tombstone history.

### Fixed - Registry persistence panic cleanup (2026-05-13)

- **Registry token allocation paths now release the registry mutex if backend
  registry persistence panics.** Callers that recover from a custom store's
  `SaveRegistries` panic no longer leave later label or relationship-type
  operations deadlocked.

### Fixed - Tiered index metadata load validation (2026-05-13)

- **Tiered temporal and vector index metadata loads now reject definitions that
  the save path would reject.** Duplicate temporal labels, duplicate
  high-frequency labels, temporal/HFI conflicts, and duplicate vector
  definitions no longer get silently accepted or de-duplicated on restart.

### Changed - Badger temporal vector search validation order (2026-05-13)

- **Badger vector search with temporal filters now validates query vectors
  before reading candidate node rows.** Invalid query dimensions or non-finite
  query values are no longer masked by unrelated candidate-row corruption, and
  invalid requests avoid the eager temporal-filter candidate scan. Valid
  temporal searches still propagate corrupt candidate rows instead of silently
  dropping them.

### Fixed - Async event batch backpressure wake-up (2026-05-13)

- **`AsyncEventBus.PublishBatch` now wakes the dispatcher before blocking on a
  full priority queue.** Batched async event publishing with
  `BackpressureBlock` no longer deadlocks when one priority group is larger
  than the configured queue size while the dispatcher is asleep.

### Fixed - Import registry corruption sentinels (2026-05-13)

- **Malformed IO import registry records now wrap `ErrCorruptExport`.**
  Invalid reserved registry slots, duplicate label names, duplicate relationship
  type names, and other registry wire-shape failures now match the corrupt
  export sentinel contract instead of surfacing as raw registry validation
  errors. Incompatible but well-formed non-empty destination registries still
  return `ErrIncompatibleRegistry`.

### Changed - Hash verification history paging (2026-05-13)

- **`g.Hash.VerifyNodeChain` and `g.Hash.VerifyRelChain` now stream history
  through wrapper-aware version pages when safe.** Deep hash-chain verification
  no longer has to materialize full per-entity history on native or direct
  pager stores, while wrapper stores that override full-history reads still
  keep those hooks observable.

### Fixed - Badger read-only mutation guard (2026-05-13)

- **Read-only `badger.Store` handles now reject mutating APIs before touching
  in-memory state.** Direct writes, non-empty batch writes, history mutations,
  index definition changes, registry saves, `Clear`, and split relationship
  repair helpers now fail with `ErrInvalidStoreMutation` instead of appearing to
  succeed in memory while being impossible to flush to the read-only Badger DB.
- **Empty tiered put batches no longer rotate shards.** `PutNodesBatch(nil)`,
  `PutNodesBatch([])`, `PutRelationshipsBatch(nil)`, and
  `PutRelationshipsBatch([])` now return as read-only no-ops on an open store,
  even when the hot shard window has expired.
- **Tiered shard catalog accessors now deep-copy slice fields at catalog
  ingress and egress.** `AddShard`, `GetShard`, `HotEventShard`, `EventShards`,
  and `ColdEventShards` no longer let caller-owned or returned `Labels` /
  `RelTypes` slices mutate catalog-owned metadata outside the catalog lock.
- **Tiered shard catalog saves now validate the current topology before
  writing.** `ShardCatalog.Save` no longer persists invalid in-memory shard
  metadata that `Load` would reject on restart, and rejected saves leave the
  existing catalog file unchanged.
- **Tiered shard catalog loads now validate before mutating live catalog state.**
  A corrupt or invalid catalog file no longer leaves rejected shard metadata in
  the in-memory catalog after `Load` returns an error.
- **In-memory tiered shard catalogs now treat `Load` as a no-op, matching
  `Save`.** Direct `NewShardCatalog("")` callers no longer get an OS read error
  for the explicit non-persistent catalog mode.
- **Tiered registry file saves now validate registry names before writing.**
  The flat registry writer no longer persists invalid label or relationship
  type slices that the loader would reject on the next open, and rejected saves
  leave the previous registry file bytes intact.
- **Tiered temporal and vector index metadata saves now validate definitions
  before writing.** Invalid label tokens, conflicting tracked temporal/HFI
  definitions, invalid HFI bucket sizes, invalid vector dimensions/metrics, and
  invalid vector property keys now fail before the metadata files are changed.

### Fixed - Custom property integrity hash type separation (2026-05-13)

- **Custom property integrity hashing now includes the registered custom type
  name and pointer/value shape before the custom `HashBytes()` payload.** Two
  different registered custom property types that intentionally or accidentally
  return identical `HashBytes()` output no longer collapse to the same property
  hash bytes.
- **`RegisterPropertyStructType` now rejects non-struct element types.** Named
  scalar or other non-struct types that implement the custom property
  interfaces can no longer enter the struct registry and then behave
  inconsistently across validation and wire reconstruction.

### Fixed - Badger split relationship delete liveness (2026-05-13)

- **`badger.Store.DeleteRelEntityAndOut` now verifies the relationship is still
  live in the in-memory relationship set before reading or deleting the
  persisted row.** Split cross-shard cleanup no longer lets a stale Badger row
  drive type/outgoing index cleanup or relationship-counter changes after the
  live relationship set has already rejected the ID.
- **`badger.Store.DeleteRelIncoming` now verifies the stored incoming-entry
  relationship type before dropping the in-memory index entry.** A stale or
  mismatched split-delete descriptor now fails with `ErrRelNotFound` instead of
  deleting memory while leaving the persisted incoming key behind.
- **Badger incoming-index repair deletes every pending and persisted key for an
  end-node/relationship pair.** Repair cleanup no longer leaves duplicate
  incoming keys behind when memory can represent only one entry for that pair.
- **Tiered `RunRepair` now validates incoming-index entries against the actual
  relationship row.** Stale incoming entries with the wrong end shard or
  relationship type are removed before missing correct entries are recreated.

### Fixed - Interface-embedded store wrapper capability dispatch (2026-05-13)

- **Graph optional-capability fast paths now distinguish declared wrapper
  methods from methods merely promoted through an embedded native `store.Store`
  interface.** Custom stores that directly implement a capability keep the fast
  path, while wrappers that only inherit native optional methods fall back
  through the graph-layer path so wrapper overrides remain observable.
- **Import rollback history-suffix snapshots now use the same wrapper-aware
  history pager selection as export.** Trim-capable custom wrappers that only
  inherit an in-tree history pager no longer bypass their mandatory
  `Get*History` hooks while preparing rollback state.
- **Depth-scoped temporal history iteration now uses cached, wrapper-aware
  capability selection.** Tiered-store wrappers that only inherit the in-tree
  `DepthHistoryIterationCapability` no longer bypass their mandatory
  `ForEach*HistoryID` hooks during graph-layer depth-filtered temporal scans,
  and Core avoids repeated reflection on this temporal-query path.

### Fixed - IO short-write handling (2026-05-13)

- **Export record writes, import stage spills, and tiered atomic file writes
  now handle short `io.Writer` writes explicitly.** Partial writes with nil
  errors are retried until complete, and zero-progress writers fail closed with
  `io.ErrShortWrite` instead of reporting success after producing truncated
  export or persistence bytes.

### Fixed - Tiered write lifecycle sentinels (2026-05-13)

- **Tiered node, relationship, label-mutation, and history write APIs now check
  store lifecycle before validating malformed mutation payloads.** Nil tiered
  stores return `ErrNilStore` and closed tiered stores return `ErrStoreClosed`
  consistently across these write surfaces instead of leaking payload
  validation errors first.

### Fixed - Relationship rollback endpoint restore (2026-05-13)

- **Transaction rollback now restores relationship type/start/end index tuples
  atomically after delete/import-same-ID sequences.** If a transaction deletes a
  relationship, imports the same relationship ID with the same type but
  different endpoints, and then rolls back, the original current row and
  outgoing/incoming adjacency indexes are restored instead of leaking the
  store-layer indexed-field mutation error.

### Fixed - Tiered clear metadata failure ordering (2026-05-13)

- **Tiered `Store.Clear()` now proves persistent vector and temporal index
  metadata can be cleared before deleting shard data.** If metadata deletion
  fails, `Clear` returns the metadata error without erasing existing entities or
  dropping the in-memory index tracking, avoiding a partially cleared store when
  the failure is known before destructive shard work begins.

### Fixed - Tiered migration destination emptiness (2026-05-13)

- **`MigrateFromBadger` now rejects tiered destinations with node or
  relationship history even when they have no current rows.** Migration no
  longer treats live counts alone as proof of an empty destination, preventing
  stale history from being mixed with newly migrated current rows.

### Added - Async event bus getter parity (2026-05-13)

- **`g.Events.GetAsync()` now mirrors `GetSync()` for asynchronous event buses.**
  The events sub-API can now retrieve either installed bus kind without exposing
  the internal publisher interface.

### Fixed - GraphTx relationship read parity (2026-05-13)

- **`GraphTx.GetRelationship` now mirrors `GraphTx.GetNode` for transaction-scoped
  reads.** Relationship reads inside transaction callbacks no longer need to use
  standalone APIs that wait behind the transaction's write lock.
- **Committed transaction-scoped reads now update read counters.** `GraphTx.GetNode`
  increments `NodesRead` on success, `GraphTx.GetRelationship` increments
  `RelsRead` on success, and rollback restores the pre-transaction counter
  snapshot.
- **Transaction-scoped reads now validate IDs before store lookup.**
  `GraphTx.GetNode` and `GraphTx.GetRelationship` reject zero or negative IDs
  at the graph layer, matching the standalone direct-read APIs.
- **Bulk direct-ID reads now update read counters.** Successful
  `g.Nodes.GetByIDs` and `g.Rels.GetByIDs` calls increment `NodesRead` and
  `RelsRead` by the number of rows returned, matching the scalar direct-read
  APIs.

### Fixed - Admin reset consistency (2026-05-13)

- **`g.Admin.Reset()` now clears graph operation counters and checkpoints the
  preserved registry snapshot after clearing the backing store.** This aligns
  Reset with its API contract on both in-memory and persistent stores.

### Fixed - Checked property wire serialization (2026-05-13)

- **Checked property wire conversion now rejects unsupported non-nil values
  instead of writing them with the legacy unknown type tag.** Normal public
  entity setters already prevent those values, but invariant-broken property
  slices now fail closed at the serialization boundary instead of producing
  ambiguous `ptUnknown` wire data.

### Fixed - Property equality index canonicalization (2026-05-13)

- **Float property equality indexes now canonicalize signed zero and NaN for
  `float32` and `float64` values.** `Nodes.ByLabelAndProperty` now treats
  stored `-0.0` and queried `+0.0` as the same exact-type scalar value, and
  treats NaN payload variants as equal within the same concrete float type, on
  both fallback scans and property-index lookups. `float32` and `float64`
  values remain distinct, matching the compare-and-set equality contract.

### Fixed — Checked integrity hash recovery (2026-05-13)

- **Checked node and relationship integrity hashing now recovers unsupported
  property-value invariant breaks, not only panics from custom `HashableValue`
  implementations.** This keeps graph mutation and verification paths on the
  error-returning checked hash surface when an external or corrupt row carries
  a property shape that normal public setters would have rejected.

### Changed — Tiered read fanout cold-shard handles (2026-05-13)

- **Tiered read fanout now closes cold shards that were opened only for a
  single read scan.** Depth-wide node, relationship, label/property, and history
  reads still include cold event shards, but a scan no longer leaves every
  lazily opened historical Badger handle resident until the idle-close timer.
  Cold shards that were already open keep the normal idle-close behavior.

### Changed — Badger transaction rollback history trim (2026-05-13)

- **Badger-backed transactions now use version-range history trimming during
  rollback instead of snapshotting full per-entity history chains on first
  mutation.** `badger.Store` implements the internal
  `HistoryRollbackTrimCapability`, and Core enables that fast path only for the
  exact native Badger store so wrapper stores still observe their
  `Truncate*History` / `Put*Version` hooks.
- **Badger history maintenance now honors `SyncWrites`.** `TruncateNodeHistory`,
  `TruncateRelHistory`, and the rollback trim helpers flush their queued history
  deletes immediately when sync writes are enabled, matching the rest of the
  Badger write surface.
- **Transaction update rollback preserves pre-existing current-version history.**
  Update snapshots now materialize history instead of using the trim fast path
  when the store already has a history row for the entity's current version,
  matching the delete rollback guard and preserving Node/Relationship parity.
- **Import replay rollback now snapshots only touched history suffixes on
  trim-capable stores.** Non-empty imports that touch a high version no longer
  need to deep-copy an entity's entire pre-existing history chain; rollback
  trims from the first imported version and restores the captured suffix. Stores
  without the trim capability keep the full-history fallback.
- **Export history paging now respects store-wrapper boundaries.** `IO.Export`
  still uses `HistoryVersionPageCapability` for exact native stores and direct
  external implementations, but concrete wrappers that only inherit an in-tree
  pager now fall back through their mandatory `Get*History` methods so wrapper
  faults, validation, and policy hooks remain observable.

### Fixed — Tiered history iterator snapshot bounds (2026-05-13)

- **Tiered node and relationship history iterators no longer let callback-created
  higher IDs extend the active scan.** `ForEachNodeHistoryID*` and
  `ForEachRelHistoryID*` now snapshot the highest eligible history ID before
  invoking user callbacks, matching the Badger iterator contract while keeping
  callbacks outside shard locks and checkouts. The default `DepthAll` path uses
  per-shard max-history probes rather than a full pre-iteration, preserving the
  bounded scan shape for large histories. Badger max-history probes now also
  honor pending history deletes, so async-buffered truncation cannot leave a
  stale deleted upper bound that admits callback-created history IDs.
- **Tiered `DepthAll` history `ForEach` now emits each logical history ID once
  even when the same ID has history split between the reference shard and
  archive.** The iterator now follows the bounded paginated ID merge used by
  `All*HistoryIDsFrom`, keeping callback delivery consistent with the other
  history-ID APIs.
- **Tiered restored reference entities now resolve archive-only history
  versions through `GetNodeVersion` and `GetRelVersion`.** Point version reads
  now match `Get*History` and paged history reads when archive/restore leaves
  older history in `refArchive` while the live row is back on the reference
  shard.

### Fixed — Persistent vector-index rebuild failures (2026-05-13)

- **Badger and Tiered vector-index definitions now fail closed when restart
  rebuild finds stored vectors that violate the persisted index shape.**
  Reopening a store with inconsistent vector-index metadata now returns the
  rebuild error instead of silently dropping mismatched nodes from the in-memory
  k-NN index.
- **Tiered vector-index creation now backfills only nodes carrying the indexed
  label.** This avoids full-row reads for unrelated labels and keeps corrupt
  unrelated rows from blocking creation of an otherwise valid vector index.

### Fixed — Tiered clear lifecycle errors (2026-05-13)

- **Tiered event-shard clear now propagates checkout and close-race errors
  instead of treating skipped shards as successfully cleared.** This prevents
  `Store.Clear` from reporting success after an event shard was not actually
  cleared because the store was closing or had a recorded lifecycle failure.

### Fixed — Relationship update endpoint liveness (2026-05-13)

- **Relationship update and relationship property-CAS now fail closed when an
  endpoint row is missing during endpoint-hash refresh.** These mutation paths
  now propagate `ErrNodeNotFound` instead of committing the property change with
  empty endpoint hashes.

### Fixed — Materialized temporal diff ordering (2026-05-13)

- **`g.Temporal.Diff` now returns each change slice sorted by entity ID.**
  `DiffCallback` remains a streaming API with implementation-defined callback
  order, while the materialized `SnapshotDiff` now matches the deterministic
  ordering used by snapshot and temporal query result slices.

### Fixed — Public stats snapshot alias (2026-05-12)

- **`graph.GraphStats` now aliases the type returned by `g.Stats.Get()`.**
  The top-level public alias previously pointed at the internal core stats
  struct while the sub-API returned `pkg/graph/stats.GraphStats`, so callers
  could not assign `g.Stats.Get()` to a `graph.GraphStats` variable without an
  explicit conversion despite the identical fields.

### Fixed — Public index provider sentinels (2026-05-12)

- **`pkg/graph` now re-exports the index-provider sentinel errors returned by
  `g.Index`.** `ErrIndexProviderExists`, `ErrIndexProviderNotFound`, and
  `ErrIndexProviderEmptyName` were already available from `pkg/graph/index`,
  but unlike the other `g.Index` errors they were missing from the top-level
  graph package.

### Fixed — Constraint lifecycle sentinel ordering (2026-05-12)

- **`g.Constraints.Add` and `Set` now fail closed before validating caller
  input after graph close.** Closed graphs return `ErrGraphClosed` and leave
  the installed constraint set unchanged even when the supplied constraint set
  is malformed.

### Fixed — Temporal Allen lifecycle sentinel ordering (2026-05-12)

- **`g.Temporal.NodeInterval`, `RelInterval`, `RelateNodes`, and `RelateRels`
  now participate in the graph read lifecycle gate.** After graph close these
  error-returning temporal helpers return `ErrGraphClosed` before inspecting
  caller-supplied node or relationship values.

### Fixed — External store query row ownership (2026-05-12)

- **Graph query APIs now defensively copy valid rows returned by untrusted
  external stores before exposing them to callers.** Native memory, badger, and
  tiered stores keep their existing hot-path allocation profile, while custom
  store implementations can no longer leak caller-mutable backing rows through
  validated node, relationship, adjacency, property-query, vector, or history
  read paths.

### Fixed — Strict property wire metadata validation (2026-05-12)

- **Checked store/import wire reads now reject contradictory custom-property
  metadata.** Non-custom property type tags can no longer carry ignored
  `CustomType` or `CustomPointer` fields, so malformed wire records fail closed
  instead of being silently normalized into a different property shape.
- **Tiered shard catalog loads now require canonical relative shard paths.**
  Persisted catalog entries such as `events/../reference` are rejected instead
  of being accepted as aliases for a different physical shard directory.
- **Badger index-definition reloads now de-duplicate property and temporal
  definitions before rebuilding.** Duplicate persisted definitions no longer
  trigger repeated full-row rebuild scans for the same logical index.
- **Badger temporal ID enumeration avoids full entity materialization.**
  `AllNodeIDs` and `AllRelIDs` with temporal filters now inspect temporal
  metadata and return IDs directly instead of allocating full deep-copy result
  slices that are immediately discarded, while still surfacing corrupt rows.
- **Badger index and vector query APIs now check lifecycle before argument
  validation.** Closed or nil stores consistently return the lifecycle
  sentinels on these surfaces even when callers also pass malformed index,
  vector, or query arguments.
- **Badger mutation and history APIs now use the same lifecycle-first
  sentinel ordering.** Entity writes, deletes, split relationship index
  helpers, and history write helpers no longer validate malformed payloads
  before detecting nil or closed stores.
- **Memory store mutation, history, index, and vector APIs now match the same
  lifecycle-first contract.** Typed-nil memory stores fail with `ErrNilStore`
  instead of panicking on these surfaces, and closed stores return
  `ErrStoreClosed` before malformed argument errors.
- **Memory store read and query APIs now also fail closed on nil receivers.**
  Direct reads, bulk reads, adjacency reads, history reads, iteration helpers,
  and transaction-time reads now return `ErrNilStore` instead of panicking
  before the store lifecycle gate.
- **Tiered index, vector, and property-query APIs now also check lifecycle
  first.** Nil or closed tiered stores no longer leak argument-validation
  errors on these public surfaces when the receiver itself is unusable.

### Fixed — Property CAS NaN matching (2026-05-12)

- **Node and relationship property CAS now matches accepted NaN property values.**
  `CompareAndSetProperty` keeps exact-type comparison and nil-as-absent
  semantics, but no longer treats stored `NaN` values as permanently
  unmatchable when they appear as scalar float properties or inside supported
  property containers.

### Fixed — Relationship property API parity (2026-05-11)

- **Relationship properties now support atomic compare-and-set.**
  `g.Rels.CompareAndSetProperty` and `CompareAndSetPropertyWithContext` mirror
  the node CAS contract: exact-type comparison, nil-as-absent semantics,
  reserved shadow-key rejection, final property-count validation, version
  history, endpoint-hash refresh, update stats, and relationship update events.

### Fixed — Monotonic mutation transaction timestamps (2026-05-11)

- **Core mutation timestamps are monotonic per graph instance.**
  `TxFrom`, `TxTo`, `UpdatedAt`, `DeletedAt`, and graph mutation event
  timestamps now advance when the underlying millisecond clock repeats or moves
  backwards, preserving transaction-time version intervals for fast consecutive
  node and relationship mutations without wall-clock sleeps.

### Fixed — Store wrapper native fast paths (2026-05-11)

- **Generated-ID create shortcuts no longer bypass wrapper Store overrides.**
  Node create, relationship create, and batch node create still use the native
  tiered fast path for exact in-tree stores, but embedded wrapper stores now
  route through `PutNode`, `PutRelationship`, and `PutNodesBatch` so
  instrumentation, fault injection, and custom policy hooks are honored.
- **Endpoint-hash shortcuts no longer bypass wrapper Store reads.**
  Relationship create and update paths still use native endpoint-hash reads for
  exact in-tree stores, but wrapper stores now fall back to `GetNode` so
  injected endpoint read failures and custom read policies are preserved.
- **Transaction-time and rollback-trim shortcuts now respect wrapper stores.**
  Concrete wrappers around `memory.Store` now use the mandatory
  `Get*`/history/iteration and truncate+restore rollback paths instead of
  inheriting native shortcuts that could bypass injected read or history faults.
- **Temporal vector search respects concrete Store wrappers.**
  Filtered vector search is still used for exact in-tree stores and direct
  external implementations, but wrappers that merely inherit an in-tree
  `SearchNearestFiltered` now use the graph over-fetch fallback so their
  `SearchNearestNodes` override remains visible.
- **Property equality queries respect concrete Store wrappers.**
  `Nodes.ByLabelAndProperty` still uses exact in-tree and direct external
  property-query capabilities, but wrappers that merely inherit an in-tree
  `NodesByLabelAndProperty` now use the graph fallback so their `NodesByLabel`
  override remains visible.
- **Nested Store wrappers no longer re-enable inherited native shortcuts.**
  Anonymous wrapper layers are inspected recursively before enabling native
  optional capabilities, so promoted in-tree methods do not leak through
  multi-layer instrumentation, fault-injection, or policy wrappers.
- **Tiered Store wrappers now participate in tiered graph integration.**
  Admin operations, injected-store registry rehydration, and transaction/import
  rollback label-registry rewiring now dispatch through narrow tiered capability
  interfaces instead of exact `*tiered.Store` assertions.

### Changed — Count and delete hot paths (2026-05-11)

- **Count reads avoid generic helper overhead on the graph and MemoryStore hot
  paths.**
  `Count`, `CountByLabel`, and `CountByType` still participate in graph
  lifecycle isolation and validate malformed names before empty-result
  shortcuts, but no longer allocate closures or run MemoryStore lazy-init
  checks for pure `len` reads.
- **Delete tombstones reuse caller-owned defensive copies.**
  Graph delete paths now mutate the defensive copies already returned by Store
  reads instead of deep-copying them again before `Delete*WithHistory`; native
  in-tree stores also skip the Phase A node snapshot read for unconnected node
  deletes while wrapper stores keep the conservative two-read behavior.

### Changed — MemoryStore generated relationship create fast path (2026-05-11)

- **MemoryStore relationship creates can capture endpoint hashes during the
  generated-ID write.**
  The graph-generated `AddRelationship` path now uses a MemoryStore capability
  that verifies endpoints, captures their current integrity hashes, and persists
  the relationship under one store lock instead of taking a separate endpoint
  hash read before the write.

### Fixed — Transaction-time query ordering (2026-05-11)

- **`NodesAsOf` and `RelsAsOf` now return deterministic ID-sorted results.**
  The memory-store transaction-time query path and the graph-layer fallback for
  stores without native transaction-time queries now sort by entity ID instead
  of exposing Go map iteration order.

### Fixed — Transaction rollback same-ID replacement (2026-05-11)

- **Rollback preserves pre-transaction entities when caller-specified imports
  reuse a deleted ID inside the same transaction.**
  Transaction create bookkeeping now distinguishes rows that were absent at
  transaction start from replacements of rows deleted earlier in the same
  transaction. Rollback restores original node and relationship rows, labels,
  relationship types, and registry state without deleting the restored row as a
  newly-created entity.

### Fixed — MemoryStore lazy initialization race (2026-05-11)

- **Zero-value MemoryStore concurrent first use is race-free.**
  The lazy initialization marker is now atomic, so concurrent read-path first
  use of a zero-value `memory.Store` cannot race while `sync.Once` initializes
  the backing maps.

### Changed — Read-path performance pass (2026-05-11)

- **Hot read and resolver paths avoid unnecessary helper overhead.**
  Graph read helpers no longer route through mutation/event dispatch setup,
  no-error ID allocators skip the graph lifecycle lock while preserving
  post-close zero results, initialized registries and MemoryStore instances
  skip repeated `sync.Once` checks on normal hot paths, and MemoryStore valid
  read/count calls avoid invoking full validator helpers unless an input is
  malformed.

### Fixed — Transaction-time deleted entity visibility (2026-05-11)

- **Transaction-time queries preserve deleted entities before deletion.**
  Node and relationship delete tombstones now keep the deleted live version's
  original `TxFrom` and set only `TxTo` to the delete time. `g.Temporal.NodeAsOf`,
  `NodesAsOf`, `RelAsOf`, and `RelsAsOf` can now resolve deleted entities at
  transaction times when those entities were still live without exposing future
  delete markers (`TxTo`, `ValidTo`, `DeletedAt`) before the delete transaction,
  including relationships removed by cascade node delete.

### Changed — Maintainability review round 9 (2026-05-09)

- **Allen relation sets ignore invalid relation bits.**
  `AllenRelation.Set`, `AllenRelationSet.Add`, `Union`, `Intersection`,
  `IsEmpty`, and `Len` now mask to the 13 defined Allen relations. Invalid
  relation enum values can no longer create hidden bits where `Len()` reports a
  member but `ToSlice()` and `String()` show an empty set.
- **Query depth rejects invalid enum values.**
  MemoryStore, BadgerStore, and TieredStore query methods that accept
  `QueryOpts` now return `ErrInvalidShardDepth` for unknown
  `QueryOpts.Depth` values instead of silently treating them as `DepthAll`.
  Valid `DepthHot`/`DepthWarm` values remain equivalent to `DepthAll` on
  single-shard stores because all data is in the only shard.
- **Graph-level queries reject invalid depth before empty shortcuts.**
  `g.Nodes.ByLabel`, `ByLabelAndProperty`, `All`, `g.Rels.ByType`, `All`, and
  `g.Index.SearchNearest` now validate `QueryOpts.Depth` before unregistered
  label/type/index shortcuts can return an empty successful result.
- **Graph-level interval filters require positive bounds.**
  Graph temporal query detection now matches the Store contract:
  `ValidStart`/`ValidEnd` form an interval filter only when both bounds are
  greater than zero. Non-positive pairs no longer force history-aware filtering
  that can hide otherwise-visible current entities.
- **Temporal interval ranges fail closed.**
  Active `QueryOpts` intervals now require `ValidStart < ValidEnd`; MemoryStore,
  BadgerStore, TieredStore, and graph-level query methods return
  `ErrInvalidTimeRange` for empty or reversed active intervals. Explicit
  `g.Temporal.*During` APIs also return `ErrInvalidTimeRange` when `start >= end`
  after resolving `end == 0` to the query's open-ended upper bound.
- **Negative query limits fail closed.**
  `QueryOpts.Limit` now accepts only `0` (unbounded) or positive limits.
  MemoryStore, BadgerStore, TieredStore, and graph-level query methods return
  `ErrInvalidQueryLimit` for negative limits before empty label/type/index
  shortcuts can widen the query to all results.
- **Negative query cursors fail closed.**
  `QueryOpts.After` now accepts only `0` (from start) or a non-negative entity
  cursor. MemoryStore, BadgerStore, TieredStore, and graph-level query methods
  return `ErrInvalidQueryCursor` for negative cursors before empty
  label/type/index shortcuts can widen the query to the first page.
- **History cursor scans validate raw pagination parameters.**
  `AllNodeHistoryIDsFrom` and `AllRelHistoryIDsFrom` now share the same
  non-negative cursor/limit contract: `limit == 0` means all remaining,
  positive limits cap the page, negative limits return `ErrInvalidQueryLimit`,
  and negative cursors return `ErrInvalidQueryCursor` before empty-history fast
  paths.
- **History truncation rejects negative retention.**
  `TruncateNodeHistory` and `TruncateRelHistory` still accept
  `keepVersions == 0` as the explicit clear-all request, but negative retention
  values now return `ErrInvalidStoreMutation` instead of deleting all history.
- **Import, Store, and graph mutations reject invalid entity IDs.**
  `g.Nodes.Import`, `g.Rels.Import`, and their transaction wrappers now return
  `ErrInvalidID` for negative caller-supplied IDs. Store mutation invariants now
  reject negative node IDs, relationship IDs, and relationship endpoints with
  `ErrInvalidStoreMutation` before current-row, history, index, delete/cascade,
  archive/restore, or batch mutation begins. Graph standalone, transaction,
  batch update, label mutation, property CAS, close-version, delete, and admin
  archive/restore entry points now reject zero/negative targets before lookup,
  lock, snapshot, capability checks, or queue work.
  Relationship create/import paths that accept endpoint IDs or endpoint node
  pointers now reject zero/negative endpoint IDs before endpoint locks, duplicate
  probes, or relationship-type token allocation.
- **Explicit-ID reads reject invalid IDs.**
  Graph read sub-APIs and MemoryStore, BadgerStore, and TieredStore now reject
  zero or negative IDs with `ErrInvalidStoreMutation` on `GetNode`,
  `GetRelationship`, `GetNodesByIDs`, `GetRelationshipsByIDs`, and adjacency
  read methods before not-found handling or partial batch work. Token `0` still
  means "all types" only for adjacency relationship-type filtering.
- **History reads and truncation reject invalid IDs.**
  Graph `g.Nodes.History` / `g.Rels.History` and direct Store
  `Get*Version`, `Get*History`, and `Truncate*History` methods now reject zero
  or negative entity IDs with `ErrInvalidStoreMutation` before empty-history,
  version-not-found, or no-op truncation handling.
- **Badger repair helpers reject negative IDs.**
  `DeleteRelEntityAndOut`, `PurgeOrphanRelationshipIndexes`,
  `DeleteIncomingByRelID`, and `ScanAndDeleteIncoming` now use the same
  positive-ID validators as other Store mutation boundaries, so negative
  relationship or end-node IDs return `ErrInvalidStoreMutation` before
  not-found, scanner, or no-op repair behavior.
- **Exported entity constructors no longer panic on reserved tokens.**
  `types.NewNode(..., 0, ...)`, `types.NewNode(..., labelsWithZero)`, and
  `types.NewRelationship(..., 0, ...)` now return regular entity values instead
  of panicking. Store write invariants reject those reserved token-0 payloads
  with `ErrInvalidStoreMutation` before persistence, so invalid direct-store
  inputs fail at the write boundary instead of crashing the caller.
- **Registry metadata is persisted when new tokens commit.**
  Successful graph mutations, batch execution, index creation, and IO import
  that introduce new label or relationship-type tokens now save the current
  registries to persistent Badger/Tiered stores before returning. Import and
  transaction rollback paths also persist the restored registry snapshots, so a
  restart that did not pass through `Graph.Close()` no longer reopens durable
  entity rows without their token mappings. Batch post-write registry checkpoint
  failures are reported as batch errors without re-finalizing registry locks or
  rolling caller-visible node/relationship pointers back to pre-commit
  skeletons after their rows have already been written. Transaction create and
  import methods also record created nodes/relationships for rollback whenever
  the Store write committed, even if the trailing registry checkpoint returned
  an error. `GraphTx.Commit` now retries the registry checkpoint before making
  the transaction irreversible; if that checkpoint fails, the transaction
  remains open so callers can retry commit or roll back.
- **Internal registries are zero-value safe.**
  Zero-value `LabelRegistry` and `RelTypeRegistry` now initialize the reserved
  token-0 entry lazily. Direct internal use reports `Len() == 0`, exports
  `[]string{""}`, imports persisted names, and allocates the first new token as
  `1` instead of panicking or returning token `0`.
- **Internal no-error index helpers tolerate nil and zero values.**
  `PropertyIndex` now initializes its value map on first `Add`, and nil
  property-index receivers no-op or return nil. Zero-value
  `HighFrequencyIndex` now initializes its bucket map on first `Add`; nil HFI
  receivers no-op or return zero/nil values from no-error helper methods.
  Nil `TemporalIndex` receivers also no-op or return nil/zero, and nil
  temporal-index map entries are ignored during purge helpers. Nil
  `VectorIndex` cleanup/read helpers no-op or return nil/zero, while
  vector writes/searches fail closed with `ErrInvalidVectorIndexConfig`.
  Zero-value internal LRU caches now lazily initialize as minimum-capacity
  caches instead of panicking on `Put`, `LoadClean`, `MarkDeleted`,
  `CollectDirty`, or `ResetForTest`.
- **By-ID relationship creates capture endpoint hashes.**
  `g.Rels.AddByID`, `AddByIDIfAbsent`, and their transaction variants now fetch
  live endpoints under the endpoint lock even when no graph-level constraints are
  configured. By-ID creation is now only an endpoint input-form variant: it
  verifies live endpoints, captures `FromNodeHash`/`ToNodeHash`, enforces
  configured constraints, and preserves relationship integrity metadata with the
  same semantics as `g.Rels.Add`.
- **Import staging size caps are non-negative.**
  `g.IO.ImportWithOptions` now rejects `ImportOptions.MaxStagedBytes < 0` with
  `ErrImportSizeLimit` before creating a staging file. `0` remains the explicit
  unlimited value. The cap check uses overflow-safe arithmetic, so a very large
  staged byte count cannot wrap the `staged + recordSize` calculation and bypass
  the configured cap.
- **Versioned mutations reject `uint32` version overflow.**
  Node/relationship updates, node label add/remove, node/relationship property
  CAS, transactions, and batch updates now return `ErrVersionOverflow` when the
  current entity version is already `math.MaxUint32`. The mutation fails before
  writing history, wrapping the current version to `0`, or registering a new
  label token for a rejected label add.
- **Version-chain successor lookup does not wrap.**
  `g.Nodes.NextVersion(id, math.MaxUint32)` and the relationship mirror now
  return `nil, nil` instead of wrapping the requested successor to version `0`
  and potentially returning genesis history as a false next version.
- **Version-chain navigation validates explicit IDs.**
  `PreviousVersion` and `NextVersion` for nodes and relationships now return
  `ErrNodeNotFound`/`ErrRelNotFound` when the requested ID is unknown. A missing
  neighboring version still returns `nil, nil` for entities that exist in the
  current row set or in version history.
- **Temporal constraint registration fails closed.**
  `g.Constraints.Add` and `Set` now return an error and reject unknown
  `TemporalConstraint.Kind` values before changing the configured set. Post-close
  `Add`/`Set` return `ErrGraphClosed` instead of silently dropping the mutation.
- **Event bus setters fail closed after graph close.**
  `g.Events.SetSync` and `SetAsync` now return an error. Open graphs preserve the
  same attach/detach behavior, while post-close calls return `ErrGraphClosed` and
  leave the installed publisher unchanged.
- **Public sub-API zero values fail closed.**
  Zero-value or nil public sub-API wrappers (`Nodes`, `Rels`, `Temporal`,
  `Index`, `Events`, `Constraints`, `IO`, `Admin`, `Stats`, `Hash`, and
  `Resolve`) now return `ErrNilGraph` from error-returning methods instead of
  panicking on an unwired or typed-nil `Ops` value. No-error helpers return their
  natural zero values.
- **Public entity nil receivers fail closed.**
  Nil `*types.Node` and `*types.Relationship` accessors now return zero values,
  no-error mutators no-op, and error-returning mutators return
  `ErrNilNode`/`ErrNilRelationship` instead of panicking. `graph.ErrNilNode` and
  `graph.ErrNilRelationship` now alias the type-layer sentinels. Nil
  `*types.PropertySlice` pointer mutators return `ErrNilPropertySlice`.
  Nil `*types.TemporalMetadata` helpers fail closed, and nil integrity
  `DeepCopy` calls return nil.
- **Event bus nil receivers no-op.**
  Nil `*events.EventBus` and `*events.AsyncEventBus` receivers no longer panic
  on no-error methods. `Subscribe` returns a no-op unsubscribe, and publish or
  close calls return without side effects.
- **Integrity hashing returns errors for panicking custom properties.**
  Graph mutation, batch, label, close-version, hash-verification, and checked
  store wire-encoding paths now use checked integrity hash helpers. A registered
  custom property whose `HashBytes` implementation panics is rejected with an
  `ErrUnsupportedValueType`-wrapped error instead of crashing the caller or
  leaving partially committed graph state.
- **Checked wire reconstruction rejects panicking custom deep copies.**
  `ValidatePropertyWireSlice`, `WireToNodeChecked`, and `WireToRelChecked` now
  verify reconstructed property values through the normal property install and
  deep-copy boundary. A persisted or imported custom property whose
  `DeepCopyValue` panics now returns an `ErrUnsupportedValueType`-wrapped error
  instead of escaping through the checked wire path.
- **Ontology mapping nil receivers classify as empty.**
  Nil `*ontology.OntologyMapping` receivers now return `ClassEvent` from
  classification helpers, return nil from `SetLabelRegistry`, and return nil
  reference labels instead of panicking.
- **Store lifecycle nil receivers fail closed.**
  Nil `*memory.Store`, `*badger.Store`, and `*tiered.Store` lifecycle calls now
  return the public `store.ErrNilStore` sentinel from `Close` and `Clear`
  instead of panicking in common deferred cleanup paths. Graph-level
  `ErrNilStore` aliases the same store-layer sentinel.
- **MemoryStore zero value is usable.**
  A zero-value `memory.Store` now lazily initializes its maps at the lifecycle
  gate, so direct callers can read, write nodes and relationships, create
  indexes, and write history without first calling `memory.New()`.
- **Persistent Store zero values fail closed.**
  Zero-value `badger.Store` and `tiered.Store` values now return
  `ErrStoreClosed` from lifecycle and checked read/write entry points instead
  of panicking on nil database handles, stop channels, or reference shards.
- **Resolver reads respect transaction isolation.**
  No-error resolver helpers (`LookupLabel`, `LookupRelType`, node label/type
  resolution, and shadow-property resolution) now take the graph read lock before
  reading registry pointers, so they cannot observe token names created inside an
  active transaction or race rollback/import registry restoration. Once graph
  close is visible, those no-error helpers return zero values instead of exposing
  teardown-owned registry state. Internal mutation and hash paths use explicit
  lock-free resolver helpers when they already hold the graph lock.
- **Provider listing re-checks close under the graph lock.**
  `g.Index.Providers()` now checks the closed flag after acquiring the graph read
  lock, so a call that began before `Close()` but waited behind teardown returns
  an empty list instead of stale provider names.
- **Provider teardown waits for in-flight initialization.**
  `g.Index.RegisterProvider` still wires `Initializable` providers before
  calling `Init`, but `g.Index.UnregisterProvider` and `Graph.Close()` now wait
  for that synchronous `Init` callback to finish before calling provider
  `Close()`. Teardown no longer closes provider resources while bulk-load is
  still running.
- **Temporal diff callbacks run outside graph locks.**
  `g.Temporal.DiffCallback` now collects known IDs and resolves each entity
  version pair under short graph read-lock windows, then invokes caller-supplied
  `DiffHandlers` outside the lock. A diff handler can safely call graph read
  APIs even while a writer is waiting on `g.mu`, avoiding the `sync.RWMutex`
  writer-preference deadlock that occurred when handlers ran under the outer
  diff scan lock.
- **Store iteration callbacks run outside backend locks.**
  MemoryStore, BadgerStore, and TieredStore `ForEach*ID` iterators now invoke
  caller callbacks after releasing backend map locks, Badger transactions, and
  Tiered shard checkouts. Callbacks can call back into Store methods without
  deadlocking on the iterator's internal lock window. Badger history iteration
  keeps memory bounded by paging history IDs through `All*HistoryIDsFrom` and
  caps each direct iteration at the history-ID high-water mark captured at start,
  so callback-created higher IDs cannot extend the same iterator.
- **Badger history ID scans honor pending truncation deletes.**
  `AllNodeHistoryIDsFrom`, `AllRelHistoryIDsFrom`, and the Badger history
  `ForEach*HistoryID` wrappers now overlay pending delete writeOps on top of
  persisted history keys. A `Truncate*History` call is visible to history ID
  scans immediately, even before the async Badger write buffer flushes.
- **Adjacency reads fail closed for missing explicit nodes.**
  `OutgoingRelationships`, `IncomingRelationships`, and their `ForNodes`
  variants now return `ErrNodeNotFound` when any requested node ID is absent.
  Existing isolated nodes still return empty results, and batched reads no
  longer silently drop missing IDs while returning partial maps. History-aware
  `g.Temporal.NeighborsAt` first validates the target at the queried instant,
  then still traverses relationship history when the target has no current row.
- **Graph query names validate before empty shortcuts.**
  `g.Nodes.ByLabel`, `CountByLabel`, `g.Rels.ByType`, `CountByType`, and
  relationship adjacency type filters now reject empty, whitespace-only, or
  overlong names before an unregistered-name or empty-input shortcut can report
  a successful empty result.
- **Authorization tier shadow input accepts all bounded numeric forms.**
  `tkg_auth_level` parsing now accepts every signed/unsigned integer type plus
  whole-number `float32`/`float64` values in `[0, 255]`, and rejects fractional
  or out-of-range values before mutation. This removes the previous mismatch
  where in-range `int8`, `int16`, `uint`, `uint16`, `uint32`, `uint64`, and
  `float32` callers were rejected even though the tier stores as `uint8`.
- **Temporal shadow inputs accept all safe millisecond numeric forms.**
  `tkg_valid_from`, `tkg_valid_to`, and `tkg_created_at` parsing now accepts
  `types.Instant`, every signed integer type, unsigned integer values that fit
  in `int64`, and whole-number `float32`/`float64` values inside each type's
  contiguous exact integer range. Fractional, non-numeric, and unsafe
  out-of-range or precision-losing values still fail before mutation.
- **Mutation names validate before unrelated failures.**
  Node label mutations and relationship type creation now reject empty,
  whitespace-only, or overlong names before property validation, entity lookup,
  registry lookup, or transaction rollback snapshots can return unrelated
  errors.
- **Vector search target validation precedes the zero-k shortcut.**
  `g.Index.SearchNearest` now validates label/property targets and reports
  unknown labels before returning the empty result for `k <= 0`, so malformed
  vector-search targets are not hidden by the non-positive-k fast path.
- **Transaction updates validate inputs before rollback snapshots.**
  `GraphTx.UpdateNode` and `UpdateRelationship` now run the same update-map
  validation as standalone updates before reading the entity snapshot for
  rollback. Malformed update keys or values on a missing entity now return the
  validation sentinel instead of being masked by `ErrNodeNotFound` or
  `ErrRelNotFound`. `BatchBuilder` update queues also share this validation,
  so provenance shadow keys are accepted consistently with standalone updates.
- **Batch empty updates are accounting no-ops.**
  `BatchBuilder.Execute` still checks entity existence for queued empty
  `UpdateNode`/`UpdateRelationship` operations, but successful empty updates no
  longer increment `BatchResult.Updated` or publish update events. This matches
  standalone update semantics, where an empty update is a read-only no-op.
- **Batch create statistics match created results.**
  Successful `BatchBuilder.Execute` node and relationship creates now increment
  `g.Stats.Get().NodesAdded` and `RelsAdded` in the same units as
  `BatchResult.Created`, instead of publishing create events while leaving the
  add counters unchanged.
- **Temporal close statistics count as updates.**
  Successful `g.Nodes.CloseVersion` and `g.Rels.CloseVersion` now increment
  `NodesUpdated`/`RelsUpdated`, matching their store write and update event.
  Rejected repeat-close calls still leave counters unchanged.
- **Transaction rollback restores graph stats.**
  `GraphTx` now snapshots operation counters at `BeginTx` and restores them
  after a successful `Rollback`, so rolled-back create/update/delete writes do
  not leak into `g.Stats.Get()` after the transaction is discarded.
- **Transaction rollback snapshots distinguish entity kind.**
  `GraphTx` now tracks rollback snapshots by node-vs-relationship identity plus
  snowflake ID. Caller-supplied imports can no longer make an updated node and
  relationship with the same underlying ID collapse into one snapshot and leave
  the relationship update committed after rollback.
- **High-frequency indexes backfill and maintain existing nodes.**
  MemoryStore and BadgerStore `CreateHighFrequencyIndex` now populate buckets
  from current matching nodes, and node write/history/delete paths maintain HFI
  buckets after creation. Badger HFI creation uses the same placeholder +
  backfill shape as temporal indexes, so corrupt existing rows fail creation and
  TieredStore rolls back earlier shard-local HFI creates instead of committing a
  partial index.
- **High-frequency index definitions survive restart.**
  BadgerStore now persists HFI label/bucket definitions and rebuilds buckets on
  open; drop and clear remove the definition. TieredStore persists its
  temporal/HFI tracking metadata so reopened stores still propagate active
  indexes to rotated hot shards and lazily opened archives. Store temporal
  index creation now rejects labels that already have an HFI, preserving the
  one-temporal-index-kind-per-label invariant in MemoryStore and BadgerStore.
- **Tracked temporal index apply rolls back partial shard setup.**
  TieredStore now rolls back temporal/HFI indexes that `applyTrackedTemporalIndexes`
  created on a target shard if a later tracked definition conflicts or fails.
  Lazy archive open and hot-shard rotation can no longer return an error after
  leaving a subset of tracked temporal indexes installed on that shard.
- **Index reads ignore in-flight create placeholders.**
  Badger property, temporal, and high-frequency query paths now fall back to
  complete row scans while a phased index create is still backfilling. Badger
  and Tiered vector searches return `ErrVectorIndexNotFound` for an in-flight
  vector placeholder until creation finalizes. Visible placeholders still catch
  concurrent writes, but callers no longer see partial index results.
- **Store-level node row deletes no longer orphan relationships.**
  MemoryStore, BadgerStore, and TieredStore `DeleteNode` and `DeleteNodesBatch`
  now return `ErrInvalidStoreMutation` when any target node still has connected
  relationships. The nodes and relationships remain intact; connected deletes
  must use the cascade or delete-with-history APIs that remove relationship rows
  atomically.
- **Graph node delete tombstones use the fully locked current node.**
  `deleteNodeInternal` now re-reads the node after `LockMany` succeeds and
  builds the node tombstone from that Phase B row. A node update that lands
  between the initial adjacency scan and the final delete can no longer be lost
  from delete history.
- **Tiered node batch deletes roll back prior shard deletes.**
  `DeleteNodesBatch` now keeps preflight node snapshots and restores earlier
  accepted shard deletes when a later shard bucket fails. Store-level vector
  indexes are updated only after every shard bucket delete succeeds.
- **Tiered relationship batch deletes roll back prior shard deletes.**
  `DeleteRelationshipsBatch` now keeps preflight relationship snapshots and
  restores earlier accepted same-shard or cross-shard relationship deletes when
  a later bucket or split-delete fails. Normal correctness no longer depends on
  a follow-up repair scan after this batch path returns an error.
- **Tiered cascade deletes roll back prior relationship deletes.**
  `DeleteNodeCascade` now snapshots connected relationships before mutation and
  restores already-deleted relationship rows when a later relationship delete or
  node row delete fails while the node still exists.
- **Tiered cascade deletes purge orphan adjacency.**
  Plain `DeleteNodeCascade` now skips stale adjacency entries whose relationship
  row is already missing and uses the shard cascade path to remove those
  leftover index entries while deleting the node.
- **Tiered archive/restore purges orphan adjacency.**
  `ArchiveNode` and `RestoreNode` now skip adjacency entries whose relationship
  row is already missing while planning relationship placement and use the
  source shard's cascade delete path for final node removal, so stale
  source adjacency-only entries do not make a reference node unarchivable or
  unrestorable. Destination preflight also purges stale adjacency-only entries
  whose relationship row is already gone before writing the temporary
  destination node. Badger orphan cleanup verifies the relationship row itself
  instead of trusting index-derived state, and scans persisted type/out/in keys
  directly so ignored stale disk keys are removed too.
- **Tiered relationship routing skips stale type-index owners.**
  Relationship ID routing and repair shard probes now verify that a candidate
  shard has the relationship entity row, not only index-derived membership. A
  stale type-index key in an earlier-probed shard no longer hides a live
  relationship row in the timestamp owner or another event/archive shard. Orphan
  incoming repair now calls the Badger orphan-index purge path so stale local
  type/out/in keys and counters are removed together.
- **Badger restart rebuild uses entity rows for node/relationship liveness.**
  `loadIndexes()` now rebuilds `nodeIDs` from node entity rows and `relIDs` from
  relationship entity rows, then derives label/type/outgoing indexes from those
  rows. Stale label/type/outgoing keys without entity rows are ignored instead
  of becoming live IDs; incoming-only relationship keys remain visible for
  TieredStore cross-shard repair.
- **Badger fixed-width key scans require exact key sizes.**
  Restart rebuild, history-ID scans, pending history-ID overlays, per-entity
  history reads, and history truncation now ignore overlong keys even when they
  share a valid fixed-width prefix. Corrupt or future-format keys can no longer
  be truncated into live node, relationship, or history state.
- **Archive/restore preflights destination relationship collisions.**
  Tiered `ArchiveNode` and `RestoreNode` now reject duplicate destination
  relationship placement or live destination adjacency before writing the
  temporary destination node, so a failed move does not leave a duplicate node
  behind when rollback cleanup correctly refuses to row-delete connected nodes.
- **Rejected label additions no longer create registry tokens.**
  `g.Nodes.AddLabel` and `GraphTx.AddNodeLabel` now check the post-addition
  `MaxLabelsPerNode` limit before creating a token for a previously unseen
  label. Failed add-label attempts no longer leave unreachable label names in
  the registry when the transaction commits or when the standalone call returns.
- **Constraint-rejected relationship creates do not register rel types.**
  `g.Rels.Add`, `AddByID`, `AddByIDIfAbsent`, `Import`, and
  `BatchBuilder.AddRelationship`/`Execute` now run endpoint and temporal
  constraint rejection paths before allocating a token for a previously unseen
  relationship type. Rejected relationship creates no longer leave unreachable
  type names in the registry. If the final relationship store write fails after
  a new type token is allocated, the registry snapshot is restored before the
  error returns.
- **Failed index creates do not keep new label tokens.**
  `g.Index.CreateProperty`, `CreateTemporal`, `CreateHighFrequency`, and
  `CreateVector` still create label tokens for successful future-label indexes,
  but now snapshot and restore the label registry when the backend create call
  fails. Failed index creates no longer leave unreachable label names behind.
- **Failed node label writes do not keep new label tokens.**
  `g.Nodes.Add`, `Import`, and `AddLabel` now restore newly allocated label
  registry entries when the final Store write fails. Registry rollback windows
  are also panic-safe, so a panicking backend cannot leave the graph stuck on the
  registry allocation mutex. Multi-label create/import paths also roll back a
  partially allocated label suffix if a later label hits registry capacity
  before any Store write starts.
- **Batch node queueing no longer registers labels early.**
  `BatchBuilder.AddNode` now uses non-zero probe tokens for previously unseen
  labels and allocates real label tokens only during `Execute`. The returned
  node pointer is retokenized in place before persistence, and failed,
  panicking, or capacity-rejected execute-time allocations restore newly
  allocated labels.
- **Failed batch relationship creates restore queue-time relationship state.**
  `BatchBuilder.Execute` now restores the caller-visible relationship's
  `TxFrom`, endpoint hashes, and queued type token when endpoint refresh,
  temporal constraints, type allocation, or the final relationship Store write
  rejects the create. Failed batch relationships no longer look partially
  committed through the pointer returned from `AddRelationship`.
- **Batch event flushes are atomic publisher batches.**
  `BatchBuilder.Execute` now dispatches buffered mutation events with one
  `Publisher.PublishBatch` call after releasing graph and builder locks. Async
  event buses therefore preserve Critical -> High -> Normal -> Low -> Deferred
  priority ordering across all events from one batch execution.
- **Batch update queues snapshot caller update maps.**
  `BatchBuilder.UpdateNode` and `UpdateRelationship` now deep-copy queued
  update maps, including nested property values and provenance signatures.
  Mutating the caller's map or slice values after queueing no longer changes
  what `Execute` applies.
- **CloseVersion preserves existing integrity metadata.**
  `g.Nodes.CloseVersion` and `g.Rels.CloseVersion` still recompute the entity
  content hash after setting `ValidTo`, but now preserve non-hash integrity
  fields. Closing a relationship no longer erases `FromNodeHash`/`ToNodeHash`,
  and closing either entity no longer drops provenance, signature, or
  authorization metadata.
- **CloseVersion rejects the open-ended sentinel.**
  `g.Nodes.CloseVersion(id, 0)` and `g.Rels.CloseVersion(id, 0)` now return
  `ErrInvalidTimeRange`. `ValidTo == 0` means open-ended/current, so a close call
  with zero time cannot mark an entity closed.
- **Explicit temporal creation ranges must be non-empty.**
  Node/relationship create, import, and batch queue paths now reject caller
  payloads where both `tkg_valid_from` and `tkg_valid_to` are set but
  `valid_from >= valid_to`, returning `ErrInvalidTimeRange` before ID generation
  or batch queueing. Checked persisted wire reconstruction applies the same
  invariant to raw durable rows. Direct Store current-row writes, replacements,
  history snapshots, delete tombstones, and node label-token helpers also
  reject explicit finite ranges where `ValidFrom >= ValidTo` with
  `ErrInvalidStoreMutation`.
- **Store history snapshots are validated at the boundary.**
  Direct Store `PutNodeVersion`, `PutRelVersion`, `DeleteNodeWithHistory`, and
  `DeleteRelWithHistory` calls now reject malformed history/tombstone payloads
  before accepting them. Node snapshots must have valid non-zero label tokens,
  relationship snapshots must have valid type/endpoints, and readable node
  delete tombstones must preserve the stored node labels instead of smuggling a
  label mutation into history. Corrupt-row delete cleanup still proceeds when
  the current row cannot be decoded, so stale indexes are purged.
- **Store label/type reads reject reserved token zero.**
  Direct Store `NodesByLabel`, `RelationshipsByType`, `NodeCountByLabel`, and
  `RelCountByType` now reject token `0` with `ErrInvalidStoreMutation` instead
  of returning a successful empty result for the registry sentinel.
- **Metadata-blind rehash paths preserve existing integrity metadata.**
  Node label add/remove, node/relationship property CAS, and node/relationship
  in-place updates now preserve non-hash integrity fields while recomputing
  hash-chain values. These APIs have no provenance shadow-key channel, so they
  no longer silently erase existing provenance, signature, authorization, or
  relationship endpoint-hash metadata.
- **Temporal label queries validate malformed labels.**
  `g.Temporal.NodesByLabelAt` now rejects empty, whitespace-only, and overlong
  label names before registry lookup, matching `g.Nodes.ByLabel` and the
  temporal label+property helpers instead of returning a successful empty
  result for malformed targets.
- **Registry resolution APIs enforce graph name limits.**
  `g.Resolve.LabelToken` / `RelTypeToken` now reject empty, whitespace-only,
  and overlong names before allocating registry tokens. Non-error boolean
  helpers (`LookupLabel`, `LookupRelType`, `Nodes.HasLabel`, `Rels.HasType`)
  fail closed for malformed names instead of consulting the registry.
- **Imported and rehydrated registries enforce graph name limits.**
  `IO.Import` and `graph.New` registry rehydration from Badger or Tiered stores
  now validate loaded label/type names against the active `MaxNameLength` before
  the mappings become graph state. Store-level registry loaders remain primitive
  persistence helpers; the graph layer owns configured name-limit enforcement.
- **Index provider names reject blank input.**
  `g.Index.RegisterProvider`, `RegisterLegacyProvider`, and
  `UnregisterProvider` now reject whitespace-only provider names with
  `ErrIndexProviderEmptyName` before mutating or probing the provider registry.
- **Index provider lists fail closed during graph teardown.**
  `g.Index.Providers()` now returns an empty list as soon as the graph closed
  flag is visible. The no-error list API no longer reads provider registry
  state in the window after `Close()` starts but before provider entries are
  drained under the graph lock.
- **Transaction rollback restores multi-label node mutations.**
  `GraphTx.Rollback` now restores a node's label sequence by transforming the
  current row back to the pre-transaction snapshot through exact one-token Store
  label writes before replacing the remaining node state. Multiple label adds,
  multiple label removes, and mixed remove/add transactions on the same node no
  longer fail rollback or leave label indexes out of sync.
- **`tkg_base_entity` resolves as `types.EntityID`.**
  `g.Resolve.NodeProperty` and `RelProperty` now return the public
  `types.EntityID` wrapper for `tkg_base_entity` instead of leaking the raw
  `snowflake.ID` dependency through the shadow-property surface.
- **Transaction rollback now restores endpoint nodes before relationships.**
  `GraphTx.Rollback` no longer tries to recreate standalone-deleted
  relationships before nodes deleted later in the same transaction. Rollback
  first restores all deleted node rows, then cascade-deleted relationships,
  then standalone-deleted relationships, so referential integrity holds during
  the undo. Regression coverage deletes a relationship, deletes one endpoint,
  rolls back, and verifies both endpoint nodes and the relationship are present.
- **Transaction rollback restores version history too.**
  `GraphTx.Rollback` now snapshots node and relationship history before the
  first transactional update/delete, restores those history rows on rollback,
  and truncates history for entities created and then rolled back. Rolled-back
  updates and deletes no longer leave phantom version entries behind.
- **Import replay errors roll back partial writes.**
  `IO.ImportWithOptions` now snapshots touched current rows, version-history
  rows, and registry mappings while replaying staged records under the graph
  write lock. Corrupt or inconsistent streams that fail after earlier records
  were applied now restore the pre-import graph state, including transient
  history rows and imported tokens.
- **Import rejects malformed entity wire invariants.**
  Snapshot import now rejects zero/negative entity IDs, negative versions,
  non-canonical label token lists, reserved `tkg_` property keys,
  unsorted/duplicate property keys, unknown property type tags, and negative
  base-entity IDs before constructing entities.
- **Badger reads now validate semantic wire invariants.**
  Current and history node/relationship reads now use checked wire
  reconstruction after MsgPack decode. Semantically corrupt rows with invalid
  IDs, token-0, out-of-range tokens, non-canonical label lists, or malformed
  property wire now return read errors instead of panicking in type constructors
  or truncating token values. Badger reads also reject rows whose decoded entity
  ID does not match the key being read, so corrupt key/value pairs cannot return
  an entity under the wrong ID.
- **Float32 wire validation preserves special values.**
  Checked property wire validation now treats `float32` NaN and infinities as
  valid `float32` values for scalar and slice properties, while still rejecting
  finite `float64` values that would overflow or round when reconstructed as
  `float32`. Valid persisted float32 properties no longer fail checked reads
  just because their value is non-finite, but corrupt float32-tagged wire can no
  longer silently narrow finite `float64` payloads.
- **Numeric wire reconstruction preserves tag type.**
  Property wire tagged as `float64` or `[]float64` now reconstructs decoded
  `float32` payloads as `float64` values, and `[]int` tags reconstruct decoded
  `[]int64` payloads as `[]int`. Integer tags also reconstruct decoded `uint`
  values that the validator accepts. Reconstruction now matches the checked
  validation contract instead of returning scalar `float32`, nil slices, or
  zero-filled numeric values.
- **Vector search heap allocation is bounded by index size.**
  `VectorIndex.SearchNearest` now caps its heap capacity at the number of
  indexed entries instead of blindly using caller-provided `k`. A tiny vector
  index searched with a huge `k` no longer attempts a huge allocation or panics;
  it returns all available ranked entries.
- **Temporal vector over-fetch buffers are bounded by the probe ceiling.**
  The graph-level temporal vector fallback now sizes its resolved-candidate
  buffer from the clamped over-fetch probe size, not raw caller `k`. External
  vector backends without `FilteredVectorSearchCapability` no longer make
  `g.Index.SearchNearest(..., k=math.MaxInt, temporalOpts)` panic before the
  bounded backend probe.
- **Badger deletes prefetch old state outside the index write lock.**
  `BadgerStore.DeleteRelationship` now mirrors the node delete path by reading
  the relationship row before taking `idxMu.Lock`, so cache-miss deletes no
  longer hold the global index write lock across Badger I/O. Node and
  relationship deletes re-read the current cached row after the write lock is
  acquired, so index cleanup uses the row that is actually being deleted even
  if a direct Store caller reused the same ID in the prefetch window.
- **Badger sync writes flush index definition metadata too.**
  In `SyncWrites` mode, Badger property, temporal, high-frequency, and vector index
  create/drop operations now flush their persisted definition metadata before
  returning. The flush runs after releasing `idxMu`, preserving the synchronous
  durability contract without deadlocking the flush snapshot path. Badger split
  relationship helper writeOps used for Tiered relationship routing and repair
  now use the same synchronous flush path.
- **Index definition persistence skips in-flight creates.**
  Badger property, temporal, high-frequency, and vector definition persistence and Tiered
  vector definition persistence now exclude indexes whose phased create is
  still backfilling. A concurrent index create/drop can no longer persist an
  unfinished placeholder that later fails or has not merged its backfill.
- **Tiered temporal index DDL rejects cross-kind shard conflicts.**
  `tiered.Store` now distinguishes same-kind retry from a shard that already
  has the other temporal index kind. Retrying a partial temporal or
  high-frequency create can finish only when the HFI bucket size also matches,
  but attempting to create HFI over an untracked interval index, interval
  indexing over an untracked HFI, or HFI over a different shard-local HFI bucket
  returns `ErrTemporalIndexExists` without installing mixed per-shard index
  kinds or mixed per-shard HFI buckets.
  If a later shard fails during temporal/HFI create after earlier shards were
  updated, the earlier shard-local creates are rolled back before the error is
  returned. Drop paths now do the symmetric rollback by recreating indexes that
  were removed from earlier shards when a later shard fails.
- **Checked wire encoders reject nil entities.**
  `storeutil.NodeToWireChecked` and `RelToWireChecked` now return conversion
  errors for nil payloads instead of dereferencing before the checked error
  path. Direct regressions cover both helpers.
- **Bulk ID reads now fail on missing explicit IDs.**
  `g.Nodes.GetByIDs`, `g.Rels.GetByIDs`, and the Store-level `Get*ByIDs`
  methods now return wrapped `ErrNodeNotFound` / `ErrRelNotFound` when any
  requested ID is absent. They no longer report success with partial results
  for explicit caller input.
- **Tiered cold-shard timing config fails closed.**
  `tiered.New` now rejects fractional-millisecond `ShardWindow` values,
  negative `ColdAfter`, negative `IdleTimeout`, and `IdleTimeout` values that
  are not positive whole milliseconds before starting the idle-close goroutine.
  This prevents a zero-duration ticker panic, keeps shard windows aligned with
  millisecond snowflake timestamps, and avoids silently disabling cold-shard
  cleanup through negative durations.
- **Sub-day Tiered shard windows use real duration buckets.**
  Minute/hour `ShardWindow` values now start at the enclosing fixed-duration
  boundary instead of the first day of the month. Accepted sub-day hot shards no
  longer start already expired for most timestamps or rotate on every write.
- **Recurrence validation rejects impossible calendar selectors.**
  `types.RecurrencePattern.Validate` now rejects unsupported `WeekdayMask`
  bits for daily/weekly patterns and invalid `Month` values for yearly
  patterns. `DayStart` and `DayEnd` must be whole milliseconds so expansion
  cannot silently truncate offsets to the `Instant` precision. `Expand` now
  returns a `types.ErrInvalidTimeRange` sentinel for empty or inverted windows.
- **Index creation materializes future-label indexes.**
  `g.Index.CreateProperty`, `CreateTemporal`, `CreateHighFrequency`, and
  `CreateVector` now validate and create the label token before creating the
  backing store index. Calling create before the first matching node no longer
  returns success over a no-op; future matching writes are indexed. Empty labels
  now return `ErrEmptyName`, and property/vector index keys respect
  `MaxPropertyKeyLength`.
- **High-frequency temporal index buckets must be whole milliseconds.**
  `g.Index.CreateHighFrequency` and Store-level `CreateHighFrequencyIndex` now
  reject bucket sizes that are not positive whole milliseconds with
  `ErrInvalidTemporalIndexConfig` before installing the index. A successful HFI
  create therefore has a time-bucket width that matches the millisecond
  `Instant` precision instead of collapsing or truncating buckets.
- **Unknown temporal constraint kinds now fail closed.**
  Relationship writes now return wrapped `ErrTemporalConstraint` and
  `ErrInvalidTemporalConstraint` when the graph's constraint set contains an
  unsupported kind, including the zero-value `TemporalConstraint{}`. Invalid
  constraint configuration can no longer be silently treated as no constraint.
- **Tiered reference labels reject empty names.**
  `tiered.New` now rejects empty or whitespace-only `Config.RefLabels` before
  opening shards, and `ontology.NewOntologyMapping` ignores empty names when
  used directly. Tiered catalog metadata and reference/event routing can no
  longer record an impossible reference label that the registry would reject.
- **Property construction and setters are defensive.**
  `NewPropertySlice`, `PropertySlice.Set`, `Node.SetProperty`, and
  `Relationship.SetProperty` now deep-copy accepted slice/map/custom values
  before storing them, while preserving the pointer shape of registered custom
  property values. Mutating a caller-owned property value after property
  construction or `SetProperty` can no longer rewrite entity state outside the
  graph/store mutation boundary.
- **Store index APIs reject token-0 labels.**
  Direct MemoryStore, BadgerStore, and TieredStore property, temporal,
  high-frequency, and vector index operations now reject label token 0 with
  `ErrInvalidStoreMutation`. Badger and Tiered reopen paths also reject
  persisted index definitions that target token 0.
- **Property/vector index targets reject shadow keys.**
  Graph-level `g.Index.*`, `g.Index.SearchNearest`, and
  stored-property query methods (`g.Nodes.ByLabelAndProperty`,
  `g.Temporal.NodesByLabelProperty*`, and
  `g.Temporal.RelsByTypeProperty*`) now reject reserved `tkg_` property keys
  with `types.ErrReservedPrefix` instead of creating or querying impossible
  stored property targets. Direct Store-level property and vector index APIs
  and persisted index definitions apply the same rule.
- **Badger index-definition metadata fails closed.**
  BadgerStore now returns an open-time error when persisted property,
  temporal, or vector index-definition records cannot be decoded as MsgPack.
  Corrupt metadata no longer silently opens as if no indexes were defined.
- **Badger persisted counters reject invalid values.**
  Reopen now treats negative `node_count` or `rel_count` metadata, and positive
  counters that match neither admitted live rows nor the exact raw entity-row
  count skipped as corrupt during replay, as corrupt store state and returns
  `ErrInvalidStoreMutation`. Public `NodeCount`/`RelationshipCount` results are
  derived from admitted live rows, so stale counters do not over-report damaged
  entities.
- **Index target operations reject invalid or unknown labels.**
  `g.Index.DropProperty`, `DropTemporal`, `DropHighFrequency`, and `DropVector`
  now validate labels before Store delegation and return the matching not-found
  sentinel when the label has never been registered. Property/vector drops also
  enforce `MaxPropertyKeyLength`. `g.Index.SearchNearest` now applies the same
  label/property-key validation and returns `ErrVectorIndexNotFound` for unknown
  labels, so typoed labels and invalid keys no longer report a successful empty
  result.
- **Vector index configuration is validated at every boundary.**
  `CreateVectorIndex` now rejects non-positive dimensions and unsupported
  distance metrics with `ErrInvalidVectorIndexConfig` before installing a
  placeholder or persisting a definition. BadgerStore and TieredStore also
  reject invalid persisted vector-index definitions during open instead of
  rebuilding indexes with disabled dimension checks.
- **Vector definition metadata rejects conflicting duplicates.**
  Badger vector-index metadata and Tiered `vector_indexes.msgpack` are persisted
  map-shaped contracts. Reopen now deduplicates identical repeated definitions
  and rejects conflicting duplicate `(label, property)` entries with
  `ErrVectorIndexExists` instead of silently letting the last corrupt record
  choose the active index configuration.
- **Tiered vector definition persistence is rollback-safe.**
  TieredStore now snapshots the raw `vector_indexes.msgpack` file before
  vector index create/drop persistence, restores or removes it on persisted
  definition write failure, and fsyncs the parent directory after removing the
  last definition so restart cannot resurrect a dropped vector index.
- **Tiered catalog saves restore raw files on write failure.**
  `ShardCatalog.Save` now snapshots the raw catalog JSON before atomic writes
  and restores or removes it if the write reports an error, so caller-level
  in-memory catalog rollback is matched by file-level rollback.
- **Tiered catalog loads validate topology before opening shards.**
  Persisted `shard_catalog.json` now rejects duplicate shard names, multiple
  hot event shards, invalid kind/tier combinations, unsafe relative paths,
  negative stats, and invalid event time windows before `tiered.New` opens
  shard handles from catalog metadata.
- **Tiered registry saves restore raw files on write failure.**
  The flat `registry.msgpack` helper now snapshots the previous raw file and
  restores or removes it when an atomic write reports an error, giving direct
  registry save APIs the same all-or-error file semantics as vector metadata
  and catalog saves.
- **Tiered registry files validate both persisted halves on load.**
  The flat `registry.msgpack` loader now rejects missing token-0 sentinels,
  empty or whitespace-only names, duplicate names, and over-capacity label or
  relationship-type slices before returning metadata to load or deprecated
  single-registry save paths. `SaveLabelRegistry` and `SaveRelTypeRegistry`
  therefore fail rather than preserving a corrupt other half.
- **Tiered rotation aborts on inherited-index setup failure.**
  Hot-shard rotation now closes and removes the newly opened shard and returns
  the index setup error if tracked temporal or high-frequency indexes cannot be
  installed. The catalog and live hot-shard pointer remain unchanged.
- **Tiered temporal tracking load rolls back earlier shard changes.**
  Restart-time replay of `temporal_indexes.msgpack` now records shard-local
  temporal/HFI definitions it creates and removes them from earlier shards if a
  later shard rejects the persisted tracking metadata. A failed open no longer
  leaves newly persisted index definitions behind on shards that replayed first.
- **Store registry APIs reject nil registry pointers.**
  BadgerStore and TieredStore registry save/load methods now return
  `ErrInvalidStoreMutation` for nil label or relationship-type registry
  pointers on open stores, while closed stores still return `ErrStoreClosed`
  first. `tiered.Store.SetLabelRegistry(nil)` is a no-op, so a direct caller
  cannot accidentally clear ontology routing state.
- **Tiered migration validates caller-owned pointers and rolls back failures.**
  `tiered.MigrateFromBadger` now matches the public two-argument signature and
  loads label/relationship-type registries from the source BadgerStore. Nil
  source or destination stores return `ErrInvalidStoreMutation`; missing source
  registry metadata for non-empty data and entity tokens outside the loaded
  source registries also fail closed. The destination must be empty, source
  entities and relationship endpoints are preflighted before destination
  writes, and mid-copy failures roll back inserted entities, restore the
  previous ontology registry, and preserve the prior destination registry file
  if the final metadata save fails after a partial filesystem update. Ontology
  registry swaps now clear the token cache so stale token classifications
  cannot survive a migration rollback. The
  destination lifecycle is checked before registry wiring or iteration, so an
  empty migration to a closed TieredStore returns `ErrStoreClosed` instead of
  success; a closed source BadgerStore also propagates `ErrStoreClosed`.
- **Vector Store sentinels live in the public Store contract.**
  `ErrVectorIndexExists`, `ErrVectorIndexNotFound`, `ErrDimensionMismatch`,
  and `ErrInvalidVectorIndexConfig` are now canonical in `pkg/graph/store`.
  The root `graph` package, `pkg/graph/index`, and concrete backends keep
  aliases, so callers can use stable `errors.Is` checks without importing a
  concrete backend.
- **Restarted warm/cold event shards stay writable.**
  TieredStore now reopens warm event shards and lazy-opened cold event shards
  with writable Badger handles. Warm/cold remains a routing and idle-close tier
  marker, but existing event entities still route updates/deletes to their
  owner shard after rotation and restart. Direct regressions cover a restarted
  warm-shard `ReplaceNode` and `DeleteRelationship` surviving another reopen.
- **Weekly shard windows follow ISO week-one boundaries.**
  TieredStore weekly `ShardWindow` starts now anchor ISO week 1 on the week
  containing January 4. Years where January 1 falls on Friday or Saturday no
  longer route ISO week-one event entities into the previous week's shard.
- **Backfilled event node creates route to their timestamp owner.**
  TieredStore `PutNode` and `PutNodesBatch` now route event nodes by the
  caller-supplied snowflake ID timestamp instead of unconditionally writing to
  the current hot shard. A successful create with an older/imported event ID is
  therefore immediately readable by `GetNode`, which resolves the same ID by
  timestamp.
- **Tiered creates reject IDs already parked in closed cold shards.**
  TieredStore duplicate checks now use cold-shard-aware ID routing before
  accepting caller-supplied node or relationship IDs. A duplicate ID already
  persisted in an idle-closed cold event shard is rejected with `ErrNodeExists`
  or `ErrRelExists` instead of allowing a second entity with the same ID in a
  reference or hot shard. Relationship duplicate checks resolve the actual
  owner shard, not only the relationship ID timestamp, because a current-ID
  relationship can still be owned by an old start-node shard. Graph-generated
  single and batch creates use a new internal generated-ID fast path so
  fresh IDs keep the hot path and do not eagerly open unrelated cold shards.
- **Generated-ID create optimization is internal-only.**
  The TieredStore generated-ID fast path now requires a proof token from
  `pkg/graph/internal/generatedcreate` and is no longer exposed through the
  public Store capability contract. Direct Store callers always get the safe
  duplicate-checking `Put*` methods; graph-generated creates keep the optimized
  path without documenting a caller-side precondition.
- **Cold-shard idle-close errors are surfaced.**
  TieredStore now records and logs errors from background cold-shard idle
  close. A failed idle close blocks later lazy checkouts with the recorded
  error and is joined into the next `Store.Close` result, so a failed final
  Badger flush during idle eviction cannot disappear as an unobservable
  background failure.
- **Tiered close synchronizes event-shard handles.**
  `tiered.Store.Close` now takes each event shard's mutex before closing or
  clearing its `BadgerStore` pointer. Explicit close and background idle-close
  now use the same synchronization before touching `EventShard.store`.
- **Tiered post-close routing fails closed.**
  Checked node/relationship routing and create-time rotation now return
  `ErrStoreClosed` after `tiered.Store.Close` starts. Direct Store callers no
  longer route into closed reference state or nil event shard handles after
  shutdown.
- **Tiered index operations fail closed after close.**
  Tiered property, temporal, high-frequency, and vector index create/drop/search
  paths now return `ErrStoreClosed` for valid requests after `Store.Close`
  starts. Vector drop/search can no longer mutate the store-level index map or
  return stale in-memory vector results after shutdown.
- **Tiered direct reads fail closed after close.**
  Tiered direct Store read/count/bulk/history APIs now return `ErrStoreClosed`
  after `Store.Close` starts, before touching reference shard state or returning
  stale cached data for otherwise valid requests.
- **Tiered admin and metadata APIs fail closed after close.**
  Tiered public admin, archive/restore, repair, clear, and registry
  save/load entry points now return `ErrStoreClosed` after `Store.Close`
  starts instead of touching closed reference state, catalog metadata, or
  registry files.
- **Tiered empty batch and reference-history writes fail closed after close.**
  Tiered empty batch deletes and reference node history writes now check
  `Store.Close` state before returning success or touching `refShard`.
- **Badger public APIs fail closed after close.**
  BadgerStore public read/write/history/index/metadata APIs now return
  `ErrStoreClosed` after `Store.Close`, including cache-hit reads, O(1)
  counters, empty batch/query fast paths, vector search, public `Flush`, and
  split relationship helper operations. No-error diagnostic/routing helpers
  return inert zero values after close instead of exposing stale in-memory
  cache, index, or rebuild state.
- **Memory public APIs fail closed after close.**
  MemoryStore now marks itself closed on `Close` and returns `ErrStoreClosed`
  from public read/write/history/index/count/iteration APIs after shutdown,
  including empty fast paths. Exported test-only tampering helpers return
  no data or no-op after close, so direct callers cannot keep reading or
  mutating process-local maps after lifecycle teardown.
- **Vector searches re-check close after index scans.**
  MemoryStore, BadgerStore, and TieredStore vector search paths now re-check
  the Store close state after the in-memory vector scan/filter callback
  completes and before returning empty or raw-ID results. A close triggered
  during a filtered vector search can no longer race into a successful
  post-close result.
- **AsyncEventBus stops accepting publishes once close starts.**
  `AsyncEventBus.Close` now marks the bus closing, closes `stopCh` to unblock
  any `BackpressureBlock` publisher waiting on a full priority queue, then waits
  for the publish gate and dispatcher drain. `Publish` and `PublishBatch`
  re-check the closing gate under `publishMu`, and `Subscribe` after close
  returns a no-op unsubscribe, so post-close callers cannot enqueue or install
  work into a stopped dispatcher.
- **AsyncEventBus serializes dispatch to preserve batch priority.**
  `AsyncEventBusConfig.Workers` values greater than one are now capped to one
  dispatcher. `Close` can wake every configured worker, and multiple drainers
  let lower-priority batch events run before a higher-priority handler has
  completed. Serialized dispatch keeps `PublishBatch` priority order true
  during normal drain and close.
- **Property ingress validates custom deep-copy output.**
  `PropertySlice.Set`, `NewPropertySlice`, and entity `SetProperties` now
  validate registered custom property values after `DeepCopyValue` runs and
  convert deep-copy panics into errors. A faulty custom `DeepCopier` can no
  longer smuggle an unsupported value into entity state after the original
  value passed validation. The copy path also preserves the caller's
  value-vs-pointer shape for registered custom values.
- **AsyncEventBus invalid backpressure preserves delivery.**
  `NewAsyncEventBus` and lazy async-bus start now normalize unknown
  `BackpressureStrategy` values to `BackpressureBlock`. A bad enum value can no
  longer fall through the enqueue switch and silently drop every published event.
- **Event buses ignore nil subscriptions.**
  `events.EventBus.Subscribe(nil)` and `events.AsyncEventBus.Subscribe(nil)`
  now return no-op unsubscribe functions without installing a nil handler.
  The async zero value also avoids starting workers for a nil subscription.
- **Store iteration rejects nil callbacks instead of panicking.**
  MemoryStore, BadgerStore, and TieredStore `ForEach*ID` iteration methods now
  return `ErrInvalidStoreMutation` for a nil callback on an open store. Closed
  stores still return `ErrStoreClosed` first. Depth-limited Tiered history
  iterators apply the same contract before entering shard callbacks.
- **ConstraintSet iteration rejects nil callbacks consistently.**
  `temporal.ConstraintSet.ForEach(nil)` now returns
  `ErrInvalidTemporalConstraint` for both empty and non-empty sets instead of
  returning nil for empty sets and panicking once a constraint exists.
- **Transaction and context entry points reject nil inputs.**
  `g.Tx.Run(nil)` and `g.Tx.RunContext(ctx, nil)` now return
  `ErrNilTxCallback` before beginning a transaction, and `RunContext(nil, fn)`
  plus graph mutation context helpers now return `ErrNilContext` instead of
  panicking on `ctx.Err()` or `ctx.Done()`.
- **IO entry points reject nil readers and writers.**
  `g.IO.Export(nil)` and `tx.Export(nil)` now return `ErrNilWriter`, while
  `g.IO.Import(nil)` and `ImportWithOptions(nil, opts)` return `ErrNilReader`.
  Typed nil reader/writer values are rejected too, before staging files or
  stream writes are attempted.
- **Index provider registration rejects typed nil providers.**
  `g.Index.RegisterProvider` and deprecated `RegisterLegacyProvider` now reject
  typed nil provider interfaces before calling `Name()`. Invalid extension
  inputs no longer panic during provider-name validation.
- **Configured stores reject typed nils.**
  `graph.New(Config{Store: typedNil})` now returns `ErrNilStore` instead of
  constructing a graph with a nil backend hidden inside the Store interface.
  A nil `Config.Store` still selects the default backend path.
- **Graph façade nil receivers return errors.**
  `(*Graph)(nil).Close()`, zero-value `Graph.Close`, `NewBatchBuilder(nil)`,
  zero-value `TxAPI`, and zero-value `BatchAPI` now return `ErrNilGraph`
  instead of panicking on a nil internal core pointer.
- **Node property CAS enforces final property-count validation.**
  `g.Nodes.CompareAndSetProperty` now rechecks `MaxPropertiesPerEntity` after
  a matched add/delete/set mutation and before version-history persistence. A
  CAS add cannot create a node shape that ordinary update paths would reject.
- **Entity property setters validate and canonicalize bulk input.**
  `types.Node.SetProperties` and `types.Relationship.SetProperties` now return
  an error, reject reserved/unsupported values, sort by key, de-duplicate with
  last-value-wins semantics, and deep-copy values before installation. Direct
  `PropertySlice` literals can no longer bypass the `Set`/`NewPropertySlice`
  safety checks and later break lookup, hashing, or persistence.
- **Registered custom properties have a typed wire form.**
  Persisted/exported property wire now stores registered custom struct values
  as a registered type name plus MsgPack payload, preserving value and pointer
  forms across Badger/Tiered/export round-trips. Wire encoding verifies that the
  MsgPack round-trip preserves the custom value's `HashBytes`; unencodable or
  lossy custom values return an error instead of silently decoding as generic
  maps.
- **Custom property registration rejects untyped nil.**
  `types.RegisterPropertyStructType(nil)` now returns an
  `ErrUnsupportedValueType`-wrapping error and leaves the registry unchanged.
  Untyped nil cannot identify a custom property type, so it is no longer
  reported as a successful registration.
- **Store relationship replacements preserve indexed fields.**
  `ReplaceRelationship`, `ReplaceRelWithHistory`, `DeleteRelWithHistory`, and
  relationship tombstones supplied to `DeleteNodeWithHistory` now reject type or
  endpoint changes with `ErrInvalidStoreMutation` before mutating rows, history,
  or indexes. Store history writes also reject node/relationship snapshots whose
  payload ID does not match the history key.
- **Store node replacements preserve label indexes.**
  `ReplaceNode` and `ReplaceNodeWithHistory` now reject node label-token changes
  with `ErrInvalidStoreMutation`; label changes must go through the dedicated
  label-token mutation paths that maintain label indexes and tier routing.
  Badger `ReplaceNodeWithHistory` also cleans property, temporal, high-frequency, and vector
  indexes from the stored current row rather than trusting the caller-supplied
  previous snapshot.
- **Store label-token helpers validate exact deltas.**
  `AddNodeLabelToken`, `RemoveNodeLabelToken`, and their history variants now
  verify that the supplied current-row payload adds or removes exactly the
  requested token before mutating label indexes, history, caches, or vector
  indexes. Direct Store callers can no longer desynchronize a label index from
  the stored node row by passing an unchanged or differently changed node.
- **Store node delete-with-history validates relationship tombstone coverage.**
  `DeleteNodeWithHistory` now rejects duplicate relationship tombstones,
  tombstones for relationships not connected to the deleted node, and missing
  tombstones for live connected relationships before deleting rows or writing
  history. TieredStore now matches the MemoryStore and BadgerStore trust
  boundary instead of silently deleting connected relationships without their
  tombstone history.
- **Store mutations reject zero entity IDs.**
  `PutNode`, `ReplaceNode`, `PutNodesBatch`, `PutRelationship`,
  `ReplaceRelationship`, `PutRelationshipsBatch`, delete/cascade, and batch
  delete paths now reject zero node or relationship identities with
  `ErrInvalidStoreMutation`. Relationship writes also reject zero start/end node
  IDs before endpoint lookup or partial batch mutation. Badger split
  relationship helpers used by TieredStore cross-shard routing apply the same
  validation to entity/out, incoming-index writes, and repair-only
  incoming-index deletes. Direct incoming-index scan deletes now remove the
  in-memory entry as well as pending/persisted keys.
- **Store history replacements reject nil payloads.**
  `ReplaceNodeWithHistory` and `ReplaceRelWithHistory` now validate current and
  previous snapshot payloads before reading IDs or marshaling data, returning
  `ErrInvalidStoreMutation` instead of panicking on nil input.
- **Import rejects incomplete and unknown-tag streams.**
  Export streams with a format version accepted by this runtime must contain
  a header, a registry, and only the known current-entity/history record tags.
  Missing required records or unknown record tags now return `ErrCorruptExport`
  instead of reporting a successful no-op or partial import.
- **Import enforces header counts and per-stream uniqueness.**
  Current node/relationship record counts must match the export header, and a
  single stream may not repeat singleton header/registry records, the same
  current entity, or the same history-version record. Truncated exports and
  duplicate-record streams now return `ErrCorruptExport` instead of importing a
  partial graph.
- **Import wraps malformed MsgPack records with `ErrCorruptExport`.**
  Header, registry, current-entity, and history-record decode failures now
  satisfy `errors.Is(err, ErrCorruptExport)` instead of surfacing raw decoder
  errors that callers could not classify through the public sentinel.
- **Legacy temporal-depth sentinel no longer carries a stale unsupported contract.**
  `ErrDepthTemporalUnsupported` is retained only for source compatibility with
  older callers. Current query and vector paths compose temporal filters with
  tiered-store `Depth`.
- **Security and vulnerability gates now scan module packages only.**
  `make security` feeds `gosec` the directories from `go list -f '{{.Dir}}'
  ./...`, and `make vulncheck` feeds `govulncheck` the package list from
  `go list ./...`. Hidden agent worktrees and stale scratch copies no longer
  affect CI security signals.
- **Formatting targets now operate only on tracked Go sources.**
  `make fmt` and `make fmt-check` use `git ls-files '*.go'` instead of walking
  the repository directory tree, so hidden worktrees and scratch copies cannot
  be reformatted or break formatting checks.
- **Stale review artifacts removed from live docs.**
  Historical maintainability-review reports and an outdated optimization-target
  note were removed from `docs/` because they described already-fixed bugs and
  speculative caller/implementation workarounds as if they were current
  contracts. Live documentation now carries the current API and architecture
  state without those stale findings.
- **Property size limits now apply recursively.**
  `MaxPropertyValueSize` is enforced for every string nested inside accepted
  property values, including string slices, `[]any`, `map[string]any`,
  `map[string]string`, and nested map keys. Snapshot import applies the
  destination graph's property-count and property-size limits before replaying
  entity records, so direct wire imports cannot bypass normal mutation limits.
- **Property validation now rejects type-erasing aliases and unsupported slices.**
  `types.ValidatePropertyValue` now admits only the exact scalar, slice, and
  map types that integrity hashing, deep-copy, and type-tagged MsgPack support,
  plus explicitly registered custom struct types. Named scalar/container
  aliases and unsupported concrete slices such as `[]uint` or `[]int32` now
  return validation errors instead of reaching the hash path and panicking.
- **Provenance shadow inputs now reject wrong value types.**
  `extractProvenance` now validates `tkg_author_id`, `tkg_signature`, and
  `tkg_authorized_by` before stripping them from props/updates. Non-nil values
  of the wrong type now return an error instead of silently clearing the
  caller-supplied metadata.
- **Tiered high-frequency indexes now inherit across shard topology changes.**
  `tiered.Store` tracks high-frequency index definitions alongside temporal
  index definitions, applies them to rotated hot shards, applies both tracked
  temporal index kinds to lazily opened reference archives, and clears the
  tracking maps on `Clear()`. Creating a high-frequency index for a label with
  a tracked temporal index now returns `ErrTemporalIndexExists` instead of
  silently doing nothing.
- **Tiered delete-with-history rolls back pre-node relationship deletes.**
  `tiered.Store.DeleteNodeWithHistory` snapshots each connected relationship
  and its history before deleting it. If a later relationship delete or the node
  tombstone write fails while the node is still live, the store restores the
  already-deleted relationships and their pre-call history instead of leaving a
  live node with missing edges.
- **Tiered rollback failures now surface with the primary error.**
  `PutNodesBatch`, `PutRelationshipsBatch`, `ArchiveNode`, and `RestoreNode`
  no longer discard errors from cleanup after a partial cross-shard write.
  Rollback attempts continue across completed steps where possible, and any
  rollback failure is included in the returned error alongside the original
  write failure.
- **Cascade deletes purge orphan relationship indexes.**
  Memory and Badger cascade-delete paths now clean stale relationship IDs out
  of type, outgoing, and incoming indexes when adjacency points at a
  relationship entity that is already missing. The delete still completes, but
  indexes no longer retain phantom rel IDs.
- **Vector index creation rejects wrong-dimension backfill rows.**
  Memory, Badger, and Tiered `CreateVectorIndex` now return
  `ErrDimensionMismatch` and remove the placeholder index when an existing
  vector property has a length different from the requested index dimension.
  The method no longer returns success over a partial index that silently
  excludes those nodes.
- **Vector index maintenance rejects wrong-dimension writes.**
  Memory, Badger, and Tiered node create/update/label/history paths now
  validate active vector indexes before mutating store state. Matching vector
  properties with the wrong length return `ErrDimensionMismatch` and leave the
  node row unchanged instead of silently excluding the node from k-NN results.
- **Persistent stores keep vector index definitions across restart.**
  BadgerStore now persists vector index definitions in Badger metadata, and
  TieredStore persists its store-level definitions in `meta/vector_indexes.msgpack`.
  Both rebuild vector entries from node properties on open. `DropVectorIndex`
  and `Clear()` remove those definitions so deleted indexes do not reappear
  after restart.
- **IndexProvider event errors are logged instead of discarded.**
  `g.Index.RegisterProvider` still treats `OnEvent` errors as non-veto
  diagnostics because the graph mutation has already committed, but the
  registration wrapper now logs the provider name, event type, entity ID, and
  error instead of silently dropping the returned error.
- **Phased index create cleanup is placeholder-scoped.**
  Badger property, temporal, high-frequency, and vector index builds, plus Tiered vector index
  builds, now delete only the placeholder created by the failing call and reject
  stale finalization if a concurrent drop or recreate replaced it. A failed
  create can no longer remove a newer index under the same key.
- **Tiered repair scans incoming index entries directly.**
  `RunRepair` Phase 1 now snapshots each shard's incoming adjacency index
  entries instead of first enumerating live node IDs. Orphaned `in/` entries
  whose endpoint node row is already missing are now visible to repair and
  removed. `DeleteIncomingByRelID` also matches the endpoint ID when rewriting
  pending incoming-key writes.
- **Tiered catalog updates are save-or-rollback.**
  Lazy reference-archive creation now persists the archive shard catalog entry
  before publishing `refArchive`; if the save fails, the catalog is restored and
  the newly opened archive store is closed and removed. `RebuildCatalog` and
  immutable-shard `VerifyShard` cache updates now do the same for stats,
  tiers, `Verified`, and count fields, returning the save error instead of
  silently keeping in-memory-only metadata.
- **Tiered clear wipes closed historical shards.**
  `tiered.Store.Clear` now serializes against shard topology changes, clears
  closed cold event shards by opening them for the wipe, handles restarted warm
  shards, and resets catalog verification/stat caches with save-or-rollback
  semantics.
- **Tiered catalog rebuild counts closed cold shards.**
  `tiered.Store.RebuildCatalog` now opens closed cold event shards and derives
  their node/relationship counts from durable shard data instead of preserving
  stale catalog counts for shards whose `store` pointer is nil.
- **Tiered shard listing reports closed-shard counts.**
  `tiered.Store.ListShards` now uses catalog counts for closed cold event
  shards and closed archives instead of reporting `0/0` merely because the
  shard is idle. Open shards still report live counts from their store handle.
- **Transaction rollback restores registries.**
  `GraphTx.Rollback` now restores label and relationship-type registries to
  their `BeginTx` snapshots after undoing entity/index mutations. Rolled-back
  transactions no longer leak tokens created by `AddNode`, `AddNodeLabel`, or
  relationship creation.
- **Delete batches now coalesce duplicate IDs.**
  Memory, Badger, and Tiered `DeleteNodesBatch` / `DeleteRelationshipsBatch`
  normalize duplicate IDs before validation and mutation. A duplicated delete
  target is applied once instead of panicking, double-decrementing Badger
  counters, or routing duplicate work across Tiered shards.
- **Batch partial failures now surface through the error return.**
  `BatchBuilder.Execute` still returns `BatchResult` with per-operation
  details, but any failed queued operation now also returns an error wrapping
  `ErrBatchFailed`. Callers that perform only the normal error check can no
  longer miss partial batch failure.
- **Batch builders now serialize their own lifecycle.**
  `BatchBuilder` queue methods and `Execute` are guarded by a builder mutex,
  and `Execute` marks the builder done as soon as replay begins. Later queue
  calls or repeat `Execute` calls return `ErrBatchDone`, preventing concurrent
  slice mutation and accidental replay of already-applied operations.
- **Event buses are now zero-value ready.**
  `events.EventBus.Subscribe` initializes its handler map lazily, and
  `events.AsyncEventBus` starts its dispatcher and queues lazily with default
  configuration on first use. Plain `var bus events.EventBus` and
  `var bus events.AsyncEventBus` values can subscribe, publish, batch-publish,
  close, and unsubscribe without panicking or blocking forever. Constructors
  remain convenience/configuration helpers, not required initialization steps.
- **Tiered label-removal coverage now exercises the direct backend APIs.**
  `tiered.Store.RemoveNodeLabelToken` and `RemoveNodeLabelTokenWithHistory`
  are covered with regression tests for label-index removal, vector-index
  refresh, history snapshots, and rejection of primary-label reference/event
  class changes.
- **Public transaction and batch wrappers now have direct coverage.**
  `g.Tx.Begin`, top-level `graph.NewBatchBuilder`, and `GraphTx.VerifyShard`
  are exercised directly instead of relying on examples or internal call sites
  to cover the exported wrapper methods.
- **Small forwarding/conversion helpers now have direct tests.**
  `pkg/graph/io.API.ImportWithOptions` is covered with an options-forwarding
  unit test, and `storeutil.ToRelIDs` is covered for nil and ordered conversion.
- **Index-provider graph reader adapters now have direct coverage.**
  `graphReaderView` read methods are exercised against real graph state so
  legacy and initializable providers no longer rely only on partial adapter
  coverage.
- **Badger incoming-index repair scan now has direct coverage.**
  `badger.Store.ScanAndDeleteIncoming` is covered with a persisted raw
  `in/` key deletion test.
- **Dead helper code removed from lock, routing, and history cursor paths.**
  Unused `runUnderLock`, `readUnderRLock`, and `timestampToEventShard` helpers
  were removed. Badger history cursor overflow handling now returns the only
  possible result directly instead of routing through unreachable pagination
  helper branches; max-cursor regression tests pin the behavior.
- **Unused test-export hooks removed from store packages.** Dead `*ForTest`
  accessors in the Badger and Tiered store packages were pruned instead of
  being carried as uncovered normal-package API surface.
- **Manual graph-lock paths now re-check lifecycle under the lock.**
  `IO.ImportWithOptions` now verifies `ErrGraphClosed` under the graph write
  lock before replaying staged records, closing a race where `Close` could
  complete during phase-1 reader I/O and phase 2 would still write to the
  already-closed graph. `IO.Export`, admin lock-taking methods, and provider
  unregister now use the same under-lock closed-state guard. Public read and
  index-management paths now also participate in the graph lifecycle lock, so
  `Close` drains them before closing Badger/tiered backing stores. The audit
  also covers node/relationship history reads, property queries, vector search,
  registry token creation, batch-builder queue methods, and no-error constraint
  mutation APIs, preventing registry/queue state changes from racing graph
  teardown. Remaining no-error ID allocation helpers (`Nodes.NextID`,
  `Rels.NextID`) are post-close zero-value reads instead of mutating closed graph
  state; event-bus setters now return `ErrGraphClosed` after close.
- **Temporal Allen helpers now reject nil entity pointers instead of panicking.**
  `g.Temporal.NodeInterval(nil)` and `RelateNodes` with a nil side return
  `ErrNilNode`; `RelInterval(nil)` and `RelateRels` with a nil side return the
  new `ErrNilRelationship` sentinel. Direct `errors.Is` regression tests cover
  both interval helpers and the relation wrappers.
- **Temporal vector search now surfaces candidate-resolution store errors.**
  Both the in-tree `FilteredVectorSearchCapability` path and the graph-layer
  over-fetch fallback now skip only expected temporal misses
  (`ErrNoVersionValidAt` / `ErrNodeNotFound`). History/store read failures from
  `findNodeVersionForOpts` are returned to the caller instead of being silently
  treated as vector ineligibility.
- **Current vector search now surfaces candidate-resolution store errors.**
  Badger and Tiered `SearchNearestNodes` now skip only `ErrNodeNotFound` while
  resolving ranked vector IDs. Corrupt or otherwise unreadable candidate rows
  are returned to the caller instead of collapsing the result set to `nil`.
- **Store-level vector search honors cursor pagination.**
  MemoryStore, BadgerStore, and TieredStore direct
  `SearchNearestNodes(..., QueryOpts{After, Limit})` calls now apply cursor
  pagination over distance-ranked results. Direct Store vector search also
  applies `ValidAt` / `ValidStart`+`ValidEnd` filters before heap selection, so
  a near-but-temporally-ineligible current node cannot occupy the k-cut and
  hide a farther eligible node. Graph-level `g.Index.SearchNearest` strips
  `After`/`Limit` before backend search and paginates the final
  current/historical result slice once, so direct Store callers and graph
  callers share the same contract without double-pagination.
- **ID-only queries honor temporal filters.**
  Store-level `AllNodeIDs(QueryOpts{ValidAt/ValidStart/ValidEnd})` and
  `AllRelIDs(...)` now return only current-row IDs that match the temporal
  filter before applying cursor pagination. The no-deserialization fast path is
  preserved for non-temporal ID scans; temporal ID scans fetch entity metadata
  because correctness depends on `TemporalMetadata`.
- **Badger temporal pages verify cache misses before consuming `Limit`.**
  BadgerStore `NodesByLabel`, `AllNodes`, `NodesByLabelAndProperty`,
  `RelationshipsByType`, and `AllRelationships` now apply `After` by ID first,
  then fetch cache-miss candidates and consume `Limit` only after the row passes
  the temporal filter. Cold-cache temporal pages after reopen can no longer let
  an expired early ID hide a later valid entity.
- **Tiered corrupt delete-with-history purges Store-level vector entries.**
  If the underlying Badger shard completes corrupt-node cleanup and returns the
  corruption error, `tiered.Store.DeleteNodeWithHistory` now still removes the
  node from the TieredStore-level vector index before returning the error. This
  prevents a deleted near vector from occupying the k-cut and hiding valid
  farther results.
- **Snapshot-style scans now exclude standalone mutations.**
  `g.IO.Export`, `g.Temporal.Snapshot`, and `g.Admin.VerifyShard` now hold the
  graph write lock while composing multi-read snapshots/scans. Standalone
  mutations can no longer interleave between export record groups, snapshot node
  and relationship reads, or shard hash-chain verification reads. The
  `GraphTx` variants remain for code that is already running under a transaction
  and must avoid re-entering `sync.RWMutex`; `VerifyShard` uses lock-free
  hash-chain helpers internally so its store-level scan does not re-enter the
  graph lock.
- **`RemoveLabelTokenRaw` contract now matches behavior.**
  `types.Node.RemoveLabelTokenRaw` has always refused to remove a single-label
  node's last label by returning `false`; the public comment no longer pushes
  that invariant onto callers, and direct `pkg/types` coverage pins the refusal.
- **Tiered node replacement enforces primary-label class immutability.**
  `tiered.Store.ReplaceNode` and `ReplaceNodeWithHistory` now reject
  reference↔event primary-label changes with `ErrPrimaryLabelClassMutation`,
  matching the label-token mutation guard. Tiered node writes also propagate
  old-state read errors before vector-index maintenance instead of treating
  them as a purge-only fallback.
- **Badger node update paths surface corrupt current rows.**
  `badger.Store.ReplaceNode`, label-token writes, and their history variants
  now return old-state read failures after confirming the node still exists,
  instead of silently purging secondary indexes and overwriting the corrupted
  durable row. Cascade delete keeps its explicit cleanup-and-return-corruption
  behavior.

### Changed — Maintainability review round 8 (2026-05-09)

- **Tiered vector depth filtering now covers event shards before the k-cut.**
  `tiered.Store.SearchNearestNodes` no longer treats `DepthHot` and
  `DepthWarm` as archive-only filters. Vector candidates on warm/cold
  event shards are filtered by shard tier before heap selection, so a
  near cold event vector cannot crowd out a farther hot/warm candidate
  when `k` is small. Regression coverage verifies `DepthAll`, `DepthWarm`,
  and `DepthHot` against cold, warm, and hot event-shard candidates.
- **Archive helper regression tests no longer rely on timing for distinct
  IDs.** Missing-endpoint cases now mint related node IDs from one
  snowflake generator instance so same-microsecond test setup cannot
  accidentally create a self-loop and skip the intended error path.
- **Tiered bulk `GetByIDs` now preserves the sorted result contract.**
  `tiered.Store.GetNodesByIDs` and `GetRelationshipsByIDs` sort by
  ascending ID before returning, matching the memory and Badger backends.
  Regression coverage passes reversed input with missing IDs through both
  the store backend and Graph sub-API entry points.
- **Tiered create paths now reject cross-shard duplicate IDs.**
  `PutNode`, `PutRelationship`, `PutNodesBatch`, and
  `PutRelationshipsBatch` probe ref/archive placement, the timestamp-routed
  event shard, and already-open event shards before writing, so the common
  caller-supplied duplicate cases cannot bypass the destination shard check
  just by changing the primary-label class or relationship start shard.
  Batch preflight rejects internal and resident cross-shard duplicates before
  any entity is written without waking unrelated closed cold shards.
- **Archive-close safety now uses stable pinned-snapshot identity.**
  `RunRepair` resolves relationship endpoints from the shard snapshot it
  already pinned instead of re-routing through live archive state. History
  reads and truncation use checked routers that return a stable
  "archive-owned" placement bit, so a concurrent `Close` that temporarily
  nils `refArchive` cannot make archived current entities skip their
  pre-archive history fan-out. Vector depth filtering and duplicate-ID probes
  also pin the archive before dereferencing it.
- **Vector index creation is now failure-atomic.** Badger no longer swallows
  operational `GetNode` errors during vector-index backfill, and Tiered removes
  its Store-level placeholder if the all-shard scan fails. Vector indexes now
  track IDs mutated during backfill, matching the property-index dirty-map
  pattern, so stale backfill cannot resurrect a vector that a concurrent update
  removed.
- **Depth-limited history iteration now includes restored reference entities.**
  `ForEachNodeHistoryIDByDepth` and `ForEachRelHistoryIDByDepth` no longer treat
  old archive history as proof that an ID is still archive-only. If the current
  node or relationship has been restored to `refShard`, `DepthHot` and
  `DepthWarm` include its history IDs while still excluding IDs whose only
  remaining state is archived.

### Changed — Maintainability review round 7 (2026-05-09)

- **Archive/restore now migrates cross-shard relationship placement.**
  `tiered.Store.ArchiveNode` no longer rejects relationships whose
  other endpoint remains live on `refShard` or an event shard. It moves
  the node row to `refArchive`, then moves each touching relationship's
  entity/out leg and incoming leg to the shards implied by the
  endpoints' new locations. `RestoreNode` applies the reverse move.
  Post-archive `PutRelationship`/`g.Rels.Add` with one archived
  endpoint now writes the same split placement instead of returning the
  old guard sentinel. Regression coverage verifies R→E, E→R, and R→R
  archive shapes plus Graph API archive/add/restore traversal.

### Changed — Maintainability review round 6 (2026-05-09)

- **Vector search now composes temporal filters with shard depth.**
  `g.Index.SearchNearest(..., QueryOpts{ValidAt: ..., Depth: DepthHot})`
  no longer returns `ErrDepthTemporalUnsupported`. For `DepthAll`,
  the existing `FilteredVectorSearchCapability` fast path still applies
  temporal eligibility before the k-cut. For depth-limited searches,
  the graph layer now drives the backend vector search with `Depth`
  first and then resolves temporal eligibility through the existing
  over-fetch loop, so archived-but-near candidates cannot crowd out
  farther hot/warm candidates. Regression coverage verifies `DepthAll`
  sees the closer archived node while `DepthHot` returns the eligible
  live node under the same temporal filter.
- **Bulk history-aware queries now compose temporal filters with shard
  depth.** `Nodes.All`, `Rels.All`, `Nodes.ByLabel`,
  `Nodes.ByLabelAndProperty`, and `Rels.ByType` now pass depth through
  both the current-index seed and the history-ID side of their
  candidate union. `tiered.Store` exposes a depth-aware history
  iteration capability so archived/cold history is excluded when a
  caller asks for `DepthHot` or `DepthWarm`; single-shard stores keep
  their existing "Depth is ignored" behavior. Regression coverage uses
  an archived self-loop node and verifies every bulk query includes the
  live entity and excludes the archived entity under `ValidAt+DepthHot`.

### Changed — Maintainability review round 5 (2026-05-09)

- **Post-close protection extended to every public sub-API entry
  point.** `*core.Core.checkOpen()` is the single primitive every
  sub-Core method calls before contacting the store, registries, or
  indexes. 30+ public APIs (Nodes/Rels reads, ByLabel/All/Count
  variants, Temporal point/interval queries, Snapshot, IO Export,
  Hash verification, Index management, Stats, Admin, Constraints,
  BeginTx, Batch.New, every BatchBuilder queue method, and
  Batch.Execute) now uniformly return `ErrGraphClosed` instead of
  racing the lifecycle teardown. `BatchBuilder.DeleteNode` /
  `DeleteRelationship` now return `error` so the close gate has a
  surface to report on (R5-F1, R5-F5).
- **Transaction-scoped snapshot APIs added on `*GraphTx`.**
  `sync.RWMutex` is not reentrant, so code that is already inside
  `g.Tx.Run` cannot call standalone snapshot-style APIs that acquire the
  graph lock. `(*GraphTx).Export(w)`, `(*GraphTx).Snapshot(at)`, and
  `(*GraphTx).VerifyShard(name)` call lock-free internal variants
  (`exportLocked`, `snapshotAt`, `verifyShardLocked`) under the
  transaction's already-held write lock (R5-F2).
- **Import validates label/reltype tokens against the registry.**
  `ImportWithOptions` rejects entity records whose `PrimaryLabel`,
  `ExtraLabels[i]`, or `RelType` token exceeds the imported
  registry's `Len()`. Pre-fix, a corrupt or hostile export carrying
  out-of-range tokens imported successfully and every label/type
  query against the entity silently resolved to "" (R5-F3).
- **History records get the same idempotent / conflict-rejection
  contract as current entities (R4-F12).** `PutNodeVersion` /
  `PutRelVersion` silently overwrite, so the import path now reads
  the existing version first; identical content is skipped, divergent
  content is rejected with `ErrCorruptExport`. Re-importing a graph's
  own export remains the supported idempotent workflow (R5-F4).
- **`AddByID` / `AddByIDIfAbsent` enforce graph-level constraints.**
  When `ConstraintRelWithinEndpoints` (or any endpoint-dependent
  constraint) is configured on the graph, the ByID variants
  transparently fetch the live endpoints under the endpoint lock and
  run the same constraint check that `Rels.Add` runs. Silent constraint
  bypass via the ByID entry point is no longer possible (R5-F7).
  A later round made the live-endpoint fetch unconditional so the ByID
  variants also capture endpoint hashes without requiring constraints.
- **Rel-type token allocation deferred past every endpoint-fetch
  failure path.** Round 4 (R4-F14) deferred allocation past cheap
  validation gates; round 5 pushes it past the operational store
  failures too. `addRelationshipInternal` allocates only after both
  GetNode calls succeed; `importRelWithIDInternal` allocates only
  after the collision probe AND the live-endpoint fetches succeed;
  `addRelationshipByIDIfAbsentInternal` uses `Lookup` to skip the
  duplicate-existence check entirely when the type is unknown,
  allocating only on the create path. The remaining unavoidable
  pollution path (PutRelationship store-write failure) is documented
  inline because the rel object literally needs the token at
  construction time (R5-F6).
- **Async event bus exposes `PublishBatch(events ...Event)`.** A
  single `publishMu`-protected enqueue lets a publisher insert N
  events into the priority-queue array atomically before the dispatcher
  is woken up, restoring strict global priority ordering under
  bursty publishes. Sequential `Publish` calls do not guarantee
  ordering (and never did under burst); the strict-ordering test
  was migrated to the new API and now passes 100/100 iterations
  (R5-F12).
- **`TestExportGraph_HistoryBoundedMemory` is no longer flaky.**
  The export retains nothing past return; the test was misreading
  transient cursor state as retention. Forcing GC after the export
  call (`runtime.GC(); runtime.GC()`) makes the retained-heap delta
  measurement deterministic. 20 consecutive runs pass.
- **`resolveTemporalVectorMatches` moved out of production.** The
  test-only helper now lives in `vector_correctness_test.go` and
  no longer compiles into the production binary (R5-F11).
- **Production files split for review-by-feature (R5-F9).** Eight files
  greater than 568 LOC carved into behavior-aligned siblings without
  changing exported APIs:
  - `pkg/graph/internal/core/batch.go` 727 → `batch.go` 114 +
    `batch_queue.go` 277 + `batch_execute.go` 355.
  - `pkg/graph/internal/core/export.go` 707 → `export.go` 275 +
    `import.go` 449 (wire format / Export path vs. Import +
    per-record validators).
  - `pkg/graph/internal/storeutil/wire.go` 657 → `wire.go` 283 +
    `wire_value.go` 382 (entity converters vs. property type-tag
    reconstruction).
  - `pkg/graph/store/tiered/tieredstore_read_history.go` 568 → 341 +
    `tieredstore_read_history_rel.go` 240 (node-history vs.
    rel-history).
  - `pkg/graph/store/tiered/tieredstore_read_bulk.go` 603 → 317 +
    `tieredstore_read_bulk_rel.go` 297.
  - `pkg/graph/store/badger/badgerstore_rel.go` 761 → `badgerstore_rel.go`
    299 + `badgerstore_rel_query.go` 322 + `badgerstore_rel_batch.go`
    161 (CRUD vs. query vs. batch).
  - `pkg/graph/store/badger/badgerstore_node.go` 964 →
    `badgerstore_node.go` 448 + `badgerstore_node_query.go` 262 +
    `badgerstore_node_batch.go` 279.
  - `pkg/graph/store/badger/badgerstore_history.go` 1109 → 105
    (shared helpers) + `badgerstore_history_node.go` 562 +
    `badgerstore_history_rel.go` 393.
- **Wall-clock sleep migration in tests (R5-F10).** 64 of 121
  `time.Sleep` calls eliminated via three reusable patterns:
  - `useTestClock(t, g)` + `clk.PeekInstant()` for "after-mutation"
    query anchors — works wherever the c.now()-driven UpdatedAt or
    TxFrom is what the test was waiting on.
  - `clk.Advance(d)` to widen the gap between UpdatedAt values so a
    midpoint anchor lands strictly between two versions.
  - Explicit `tkg_valid_from` on Add to pin temporal ordering when
    the test was relying on wall-clock-spread snowflake IDs (vector
    eligibility tests, diff-window tests).

  Files now at 0 sleeps: `temporal_test.go` (22→0),
  `temporal_queries_rel_parity_test.go` (8→0),
  `findings_extra_regression_test.go` (6→0), `txtime_test.go` (3→0),
  `vector_correctness_test.go` (14→0), `diff_test.go` (3→0),
  `graph_temporal_test.go` (2→0), `foreach_test.go` (2→0),
  `diff_callback_test.go` (2→0), `v3061_fixes_test.go` (1→0),
  `bench_production_test.go` (1→0).

  The remaining ~57 sleeps test genuinely wall-clock-scheduled
  behaviour: tiered-store hot/warm/cold shard rotation depends on
  wall-clock shard window boundaries; async event bus tests assert
  worker dispatch latency; badger flush tests wait for the auto-flush
  goroutine; production code in `tieredstore.go` waits on
  `activeReqs` to drain on Close. Migrating these would require
  injecting a clock into the snowflake generator (cross-repo) and the
  shard rotation scheduler (intrusive) — accepted as out of scope for
  R5-F10.
- **Documentation drift corrected.** The `UpdateRelationship` lock
  table in `docs/architecture.md` reflects the actual `LockMany(rel,
  start, end)` triple instead of the obsolete `LockEntity(id)`
  single-lock; `docs/design.md` now describes `NodeID` / `RelID` /
  `EntityID` as the customer-facing exported wrapper types (the
  pre-v3.4.0 unexported aliases are noted as historical). The
  misleading "hold c.mu.RLock for strong consistency" advice on
  `TempOps.Snapshot` was removed; snapshot-style scans now take the graph
  write lock directly, and the `GraphTx` methods are for transaction-scoped
  callers that already hold it (R5-F8).

### Changed — Maintainability review round 2 (2026-05-08)

- **Admin mutators now serialise against tx/batch.** `g.Admin.ForceRotate`,
  `RebuildCatalog`, `Repair`, and `VerifyShard` take `c.mu.Lock`;
  `ListShards` takes `c.mu.RLock`. Previously these bypassed the graph
  write lock, so `Reset` could race a concurrent `ForceRotate` (R2-F1).
- **Hot-shard rotation is transactional around catalog persist.**
  `RotateHotShard` now opens the new badger shard, snapshots the
  catalog, applies tentative catalog mutations, persists, and only then
  switches the live `hotShard` pointer. On `catalog.Save` failure the
  in-memory catalog is restored from the snapshot, the new shard is
  closed and removed, and live topology is unchanged. Previously a
  Save failure left the running process with a switched-over hotShard
  but a durable catalog describing the old topology — split-brain on
  restart (R2-F2). New helpers: `ShardCatalog.snapshotShards()` /
  `restoreShards(snapshot)`.
- **Public mutation surface is panic-safe.** Every public mutation
  entry point on `g.Nodes`, `g.Rels`, `g.Constraints`, the version-chain
  helpers, and the property-CAS helpers is wrapped with a defer-backed
  `RUnlock` via the new `(*Core).runUnderRLock(fn)` /
  `runUnderLock(fn)` helpers. Includes the `BatchBuilder.Execute`
  per-relationship endpoint locks (`LockTwo`/`UnlockTwo`) and the
  two-phase `deleteNodeInternal` (Phase A node lock, Phase B
  `LockMany`) — both now release on every panic path. Previously a
  panic from a custom Store could leak the graph read lock or the
  shard locks for the rest of the process lifetime, deadlocking
  `Close` and any subsequent mutation hashing to the same shard
  (R2-F3, generalised across 20 sites).
- **`g.Nodes.ByLabelAndProperty` works on mandatory-only backends.**
  The capability split previously bundled the correctness-level query
  with optional index management; backends that satisfied
  `MandatoryStore` but not `PropertyIndexCapability` got
  `ErrCapabilityNotSupported`. The graph layer now falls back to a
  label scan + property filter via the mandatory `NodesByLabel` surface
  when the optional capability is absent. Index management
  (`g.Index.CreateProperty` / `DropProperty`) remains optional and
  still surfaces the typed sentinel (R2-F4).
- **Vector search top-k correctness is preserved on backends that
  cannot pre-filter.** `FilteredVectorSearchCapability` is now a
  public optional capability in `pkg/graph/store`. Backends that
  implement it get pre-filter semantics (top-k taken from the
  eligible-only set); backends that do not get iterative over-fetch
  (`k → 2k → 4k …` up to 65536) until either k eligible results are
  accumulated or the backend is exhausted. Previously the same public
  API silently returned fewer than k results when the nearest k
  vectors were all temporally ineligible (R2-F5).
- **`g.IO.ImportWithOptions(r, ImportOptions)` for staging control.**
  New `io.ImportOptions{StagingDir, MaxStagedBytes}` lets callers
  redirect the per-import temp file off the platform default temp
  volume and bound the staged-disk usage. `MaxStagedBytes > 0` causes
  Phase 1 to surface the new `core.ErrImportSizeLimit` sentinel before
  any live mutation (R2-F6). `g.IO.Import(r)` keeps prior behaviour
  via default options.
- **Public docs realigned with the v3.4 sub-API surface.** `docs/api.md`
  and `docs/SPEC.md` rewritten to reference `g.Nodes.*` / `g.Rels.*` /
  `g.IO.*` / `g.Admin.*` / `g.Tx.*` / `g.Batch.*` / `g.Resolve.*` /
  etc. instead of the removed direct `Graph.AddNode` style methods.
  Cypher integration is now correctly described as a downstream
  (`rho/tkgd-v3`) concern, not a feature of this library.
  `docs/architecture.md` "Two-Phase Operations" updated: batch is per-
  operation result reporting, not all-or-nothing (R2-F7).

Lessons logged to `tasks/lessons.md`: B49 (mutating admin must join
exclusion domain), B50 (every secondary lock needs an immediate
defer), B51 (optional capabilities must separate correctness from
acceleration), B52 (public docs must be compiled against the current
public surface).

### Changed — Maintainability review (2026-05-08)

- **`TxAPI.Run` / `RunContext` are now panic-safe.** A panic inside the
  user callback used to leak the graph write lock and silently drop any
  rollback error. Both wrappers now use a `defer` guard that runs
  `Rollback` on every non-success path and joins any rollback error
  with the caller's. Pinned by `pkg/graph/tx_run_test.go`.
- **Property validation now matches what hashing and the wire format
  support.** Maps with non-string keys, concrete `[]any{}`-style
  value-form maps, named scalar/container aliases, and unsupported concrete
  slices are rejected at `Set`/`NewPropertySlice` rather than panicking later
  in the integrity hash. Pointer-only registered custom property types now
  reject value-form storage at validation.
- **Documented isolation contract.** `docs/architecture.md` no longer
  implies snapshot isolation. Read APIs do NOT acquire `g.mu` and
  concurrent reads can observe a transaction's already-applied (and
  possibly rolled-back) mutations.
- **Relationship endpoint hash refresh propagates errors.** The
  standalone update path and the batch path used to swallow every
  non-nil error from the endpoint `GetNode`, writing relationships
  with empty `FromNodeHash`/`ToNodeHash` on operational failures. Both
  paths now distinguish `ErrNodeNotFound` (silent — endpoint was
  cascade-deleted) from any other error (surfaced).
- **`IO.Import` memory is bounded.** Phase 1 streams records to an
  `os.CreateTemp("tkg-import-*.stage")` staging file instead of an
  in-memory buffer. Phase-1 staging memory is `O(maxExportRecordSize)`
  regardless of export size; replay still keeps rollback state for
  touched rows.
- **`Store` decomposed into capability interfaces.** The 65-method
  contract is now an embedded composition of 13 narrower capabilities
  in `pkg/graph/store/capabilities.go`. Four optional capabilities
  (`PropertyIndexCapability`, `TemporalIndexCapability`,
  `VectorIndexCapability`, `HighFrequencyIndexCapability`) can be
  omitted by future backends; the graph layer's internal store field
  is `MandatoryStore` and call sites that need an optional capability
  type-assert and surface a new `ErrCapabilityNotSupported` sentinel
  on miss. Per-backend compile-time assertions live in
  `{memory,badger,tiered}/capabilities.go`. Public contract unchanged
  — every in-tree backend still satisfies the full composition.
- **Backend file splits.** `memorystore.go` 1925 LOC → 119 + 5
  satellites; `tieredstore.go` 1237 → 532 + 3; `tieredstore_read.go`
  1787 → 190 + 4; `tieredstore_write.go` 1545 → base + 5. Moves-only,
  byte-identical bodies.
- **Badger index-rebuild diagnostics.** `loadIndexes` silent skips
  replaced with `IndexRebuildStats().PropertySkipped` /
  `TemporalSkipped` counters and per-skip `Warningf` log entries; new
  `(*badger.Store).IndexRebuildStats()` accessor.
- **Sub-API wrapper coverage.** Every wrapper in the 13 sub-API
  accessor packages is now invoked by
  `pkg/graph/subapi_smoke_test.go` +
  `pkg/graph/subapi_smoke_extra_test.go`. Total `./pkg/...` coverage
  is 86.6%. New `make cover-gate` target enforces a configurable floor
  (default 80%); `make ci` runs it.

Lessons logged to `tasks/lessons.md`: B45 (property validation must
match hash/wire/copy), B46 (transaction wrappers must be panic-safe),
B47 (`err == nil { use }` is not the same as tolerating one sentinel),
B48 (tolerated-on-rebuild errors must be counted, not continued).

### Changed — BREAKING

- **`Graph.Core()` escape hatch removed.** The experimental accessor
  that returned the underlying `*core.Core` is gone. Use the sub-API
  fields (`g.Nodes`, `g.Rels`, `g.Stats`, …) for everything that was
  previously reachable through it. The full `GraphStats` snapshot
  (operation counters + cache metrics) is now reachable as
  `g.Stats.Get()`; the `pkg/graph/stats` package re-declares the
  `GraphStats` struct so the sub-API import does not pull in
  `pkg/graph/internal/core`.
- **All 11 sub-API packages now use short, suffix-free names.** The
  earlier "collapse temporal/events/index" change has been extended to
  the remaining six sub-API packages:
  - `pkg/graph/temporalapi`    -> `pkg/graph/temporal`    (collapsed; both types and API in one package)
  - `pkg/graph/eventsapi`      -> `pkg/graph/events`
  - `pkg/graph/indexapi`       -> `pkg/graph/index`
  - `pkg/graph/adminapi`       -> `pkg/graph/admin`
  - `pkg/graph/constraintsapi` -> `pkg/graph/constraints`
  - `pkg/graph/hashapi`        -> `pkg/graph/hash`        (shadows stdlib `hash`)
  - `pkg/graph/ioapi`          -> `pkg/graph/io`          (shadows stdlib `io`)
  - `pkg/graph/resolveapi`     -> `pkg/graph/resolve`
  - `pkg/graph/statsapi`       -> `pkg/graph/stats`
  Customers that imported any of these packages must update their
  import path and the package selector accordingly (`adminapi.API` ->
  `admin.API`, etc.). The `g.<Field>` accessors are unchanged except
  for `Statistics` (see next entry).
- **`Graph.Statistics` field renamed to `Graph.Stats`.** The
  `Statistics` workaround field name was needed only because the old
  `Graph.Stats() GraphStats` method (removed in v3.4.0) used the same
  identifier. With the package now named `pkg/graph/stats`, the field
  is renamed to match. Migration: `g.Statistics.NodeCount()` ->
  `g.Stats.NodeCount()`. The full `GraphStats` struct (atomic
  operation counters + cache metrics) is reachable via
  `g.Stats.Get()`.

#### Stdlib aliasing for hash and io

The new `pkg/graph/hash` and `pkg/graph/io` packages shadow stdlib
`hash` and `io`. Inside the local packages no aliasing is required —
Go resolves `hash.Hash` / `io.Reader` to the imported stdlib package.
At consumer sites that import BOTH stdlib `"hash"` (or `"io"`) AND the
local sub-API package, alias the local one with a `tkg` prefix:

```go
import (
    "io"
    tkgio "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/io"
)

var _ tkgio.API
```

In practice few sites need the alias because customers usually reach
the sub-API via `g.IO` / `g.Hash` (field access — no package import
needed) and only import the package directly when they need the
exported type.

#### Migration cheat sheet

```go
// Before                         // After
g.Statistics.NodeCount()         g.Stats.NodeCount()
g.Statistics.AllLabelCounts()    g.Stats.AllLabelCounts()
g.Admin.Archive(id)              g.Admin.Archive(id)               // unchanged
g.Hash.VerifyNodeChain(id)       g.Hash.VerifyNodeChain(id)        // unchanged
import "...tkg/v3/pkg/graph/adminapi"      ->  "...tkg/v3/pkg/graph/admin"
import "...tkg/v3/pkg/graph/statsapi"      ->  "...tkg/v3/pkg/graph/stats"
import "...tkg/v3/pkg/graph/hashapi"       ->  "...tkg/v3/pkg/graph/hash"
import "...tkg/v3/pkg/graph/ioapi"         ->  "...tkg/v3/pkg/graph/io"
adminapi.API                     admin.API
adminapi.New(c)                  admin.New(c)
```

- **`*core.Core` implementation split into 11 sub-Core types matching the public sub-API surface (internal only).**
  The 130+ method bodies that used to live directly on `*Core` moved
  to dedicated `*NodeOps`, `*RelOps`, `*TempOps`, `*IndexOps`,
  `*EventOps`, `*AdminOps`, `*ConstraintOps`, `*HashOps`, `*IOOps`,
  `*ResolveOps`, `*StatOps` types declared in
  `pkg/graph/internal/core/subops.go`. `*Core` is now a coordinator
  holding these as exported fields wired in `core.New`. Method names
  drop their type prefix (`AddNode → NodeOps.Add`, `GetRelationship →
  RelOps.Get`, `VerifyNodeHashChain → HashOps.VerifyNodeChain`, etc.)
  so the call chain `g.Nodes.Add()` → `nodes.API.Add()` →
  `core.NodeOps.Add()` no longer renames the method across the
  wrapper boundary. Each sub-API wrapper at `pkg/graph/<name>/api.go`
  was restructured from a `Core` interface to an `Ops` interface that
  matches the new sub-Core method shapes 1:1. **No public-API change**
  — `*core.Core` is internal, customers only see the unchanged
  sub-API surface (`g.Nodes.Add`, `g.Stats.Get`, etc.). Internal
  callers (tx, batch, store wrappers) and tests were updated; the
  `Store` / `BatchBuilder` / `GraphTx` types kept their old method
  names because they do not expose the sub-API surface.
- **`pkg/graph/temporalapi`, `pkg/graph/eventsapi`, `pkg/graph/indexapi` collapsed into their types-only siblings.**
  The three sub-API packages were merged into the existing types-only
  packages they used to forward to:
  - `pkg/graph/temporalapi` -> `pkg/graph/temporal`
  - `pkg/graph/eventsapi`   -> `pkg/graph/events`
  - `pkg/graph/indexapi`    -> `pkg/graph/index`
  Customers that imported `temporalapi.API`, `eventsapi.API`, or
  `indexapi.API` must update their import path and rename to
  `temporal.API`, `events.API`, or `index.API` respectively. The
  `Graph.Temporal`, `Graph.Events`, `Graph.Index` field accessors
  themselves are unchanged — only the underlying package names move.
  No semantics change.

### Added

- **`make lint` target and `.golangci.yml` configuration** integrated
  into `make ci` (after `vet`, before `build`). Linters enabled:
  `errcheck`, `govet`, `ineffassign`, `staticcheck`, `unused`,
  `revive`, `misspell`, `unconvert`, `unparam` plus `gofmt` /
  `goimports` formatters. Dead helpers, redundant type conversions,
  and missing doc comments fixed inline (no broad suppressions).

### Security

- **`toolchain go1.26.2` directive added to `go.mod`.** govulncheck flagged
  `GO-2026-4865` (JsBraceDepth context-tracking bugs in `html/template`)
  as a stdlib-level concern, fixed in `html/template@go1.26.2`. The
  codebase does not import `html/template` directly — the trace runs
  through `fmt.Sprintf`'s error formatter — so the vulnerability is
  practically unreachable, but bumping the preferred toolchain to
  `go1.26.2` is the documented mitigation. The `go 1.26.1` minimum
  requirement is unchanged.

### Documented

- **`race_off_test.go` / `race_on_test.go` cannot collapse via
  `runtime.RaceEnabled`** — that constant lives in the build-tagged
  `runtime/race` internal package and is not reachable from external
  code without using the same `//go:build race / !race` tags the pair
  already employs. The two files retain a one-line note explaining the
  rationale so future maintainers do not redo the experiment.

### Added

- **Example tests in every sub-API package.** Each of the 11 sub-API
  packages (`nodes`, `rels`, `temporal`, `index`, `events`,
  `constraintsapi`, `ioapi`, `adminapi`, `statsapi`, `hashapi`,
  `resolveapi`) plus the in-package `Tx` and `Batch` sub-APIs now has
  an `example_test.go` demonstrating the most-common operations, so
  godoc renders idiomatic usage hints. Examples are compile-only
  (no `// Output:` blocks because snowflake IDs are nondeterministic).
- **`tutorials_test.go` in every tutorial directory.** Empty
  `TestCompiles` placeholder ensures `go test ./tutorials/...` exercises
  the build of every tutorial, catching public-API breakage immediately
  instead of when a user runs the tutorial.

### Changed

- **`pkg/graph/internal/core/graph_test.go` (4970 LOC) split into themed
  files.** Functions moved byte-identically into `graph_node_test.go`,
  `graph_node_validation_test.go`, `graph_node_history_test.go`,
  `graph_node_hash_test.go`, `graph_rel_test.go`,
  `graph_rel_validation_test.go`, `graph_rel_history_test.go`,
  `graph_rel_hash_test.go`, `graph_temporal_test.go`,
  `graph_index_test.go`, `graph_cas_test.go`, `graph_label_test.go`,
  `graph_lifecycle_test.go`, `graph_tx_test.go`, `graph_misc_test.go`.
  All split files are below 1500 LOC. No test bodies were changed.
- **`pkg/graph/store/badger/badgerstore.go` (4262 LOC) split into themed
  files.** Methods moved byte-identically into `badgerstore.go` (struct,
  lifecycle: `New`, `Close`, `Clear`, `loadIndexes`),
  `badgerstore_node.go`, `badgerstore_rel.go`, `badgerstore_index.go`,
  `badgerstore_history.go`, `badgerstore_temporal.go`,
  `badgerstore_meta.go`, `badgerstore_flush.go`. All split files are
  below 1500 LOC. No method bodies were changed.

- **Doc cleanup post-v3.4.0**:
  - `CLAUDE.md` and `AGENTS.md` Status lines now describe the v3.4.0
    reality (sub-APIs are the only public surface; the 130+ direct
    `*Graph` methods were removed). Previous wording incorrectly
    described the change as "additive".
  - Stale `Graph.Stats()` comments in `pkg/graph/internal/core/stats.go`
    and `pkg/graph/statsapi/api.go` updated to point at
    `g.Core().Stats()` (the public method was removed in v3.4.0; the
    `Core()` escape hatch itself was subsequently removed in the same
    Unreleased cycle, see "Changed — BREAKING" above).
  - `README.md`, `docs/api.md`, `docs/architecture.md`,
    `docs/persistence.md`, and `docs/design.md` updated to use the
    `memory.Store` / `badger.Store` / `tiered.Store` names introduced
    in v3.3.0. Historical "What's new" entries are intentionally left
    untouched since they describe state at the time of those releases.

### Added

- **Streaming `DiffSnapshotsCallback` (resolves the `TODO(v3.1.0)` in
  `pkg/graph/internal/core/temporal.go`).** A new method on `*core.Core`
  surfaces entity diffs between two instants via handler callbacks
  instead of materialising both `*GraphSnapshot` results plus the two
  ID-keyed maps `buildDiff` previously built. The peak working set is
  now O(|distinct entity IDs| × ~24B for the dedup map) plus one
  before/after entity pair at a time, down from O(|entities valid at
  t1| + |entities valid at t2|). Asymptotically — 5M nodes valid at both
  timestamps — this eliminates ~1.5 GB of transient allocation.
  - The new `temporal.DiffHandlers` struct in `pkg/graph/temporal`
    declares one optional callback per change class
    (`OnNodeCreated`, `OnNodeUpdated`, `OnNodeDeleted`,
    `OnRelCreated`, `OnRelUpdated`, `OnRelDeleted`). nil fields are
    skipped. Returning a non-nil error from any callback aborts
    iteration and propagates the error verbatim.
  - The existing `DiffSnapshots(t1, t2)` API stays — it now delegates
    to `DiffSnapshotsCallback` with handlers that accumulate into a
    `*SnapshotDiff`. Callers that need the materialised result are
    unaffected; callers willing to consume changes streaming-style
    avoid the snapshot allocations entirely.
  - The Temporal sub-API exposes `g.Temporal.DiffCallback(t1, t2, h)`
    forwarding to the new method.
  - Relationship endpoint filtering preserves parity with `snapshotAt`:
    a relationship is reported only when both endpoints are valid at
    the queried instant. The streaming path applies this filter
    per-entity rather than via two whole-graph node-validity sets.
- **Cursor-paginated history-ID scans (`AllNodeHistoryIDsFrom` / `AllRelHistoryIDsFrom`).**
  Two new methods on the `Store` interface return the IDs of nodes /
  relationships with version-history entries, sorted ascending, starting
  strictly after a caller-supplied cursor and capped at a caller-supplied
  limit. Implemented in `pkg/graph/store/{memory,badger,tiered}`.
  - **MemoryStore** sorts the in-memory history map and applies
    `storeutil.PaginateNodeIDs` / `PaginateRelIDs` for an O(N log N + N)
    one-shot result.
  - **BadgerStore** seeks the `0x07` (node-history) / `0x08`
    (rel-history) prefix at `[prefix, after+1]` and merges the seek stream
    with the pending write-buffer entries strictly greater than `after`,
    deduplicating same-ID version-suffix repeats. Memory is bounded by
    `limit`, not total history depth.
  - **TieredStore** walks the reference shard, the reference archive, and
    every event shard sequentially via `checkout/checkin` — only one
    shard's iterator is open at a time. Cross-shard duplicates collapse
    via a `seen` set bounded by the IDs returned in the current page.
  - The legacy `AllNodeHistoryIDs()` / `AllRelHistoryIDs()` methods are
    retained for backward compatibility and now delegate to the
    paginated form (`limit == 0` means "all remaining").

### Changed

- **`ExportGraph` history phase is now bounded-RAM** (resolves the
  `TODO(v3.1.0)` in `pkg/graph/internal/core/export.go`). The export
  loop pages history-ID scans at `exportHistoryBatchSize = 4096` IDs per
  call, eliminating the OOM risk at large history depths
  (e.g., 10K nodes × 1K versions = 10M IDs would previously have
  materialised in a single slice).
- **TieredStore parallel-merge transient eliminated.** The previous
  `AllNodeHistoryIDs` / `AllRelHistoryIDs` implementations spun a
  goroutine per event shard and held all per-shard ID slices in RAM
  simultaneously before dedup-mapping at the end (~400MB transient on
  52-shard year-long graphs). The new sequential checkout/checkin walk
  removes both the goroutine fan-out and the global dedup map. The
  legacy methods are now thin wrappers that page the new API at a
  generous in-memory page size.

### Tests

- `pkg/graph/store/memory/history_cursor_test.go` — empty graph, limit=0
  parity, after-past-last, paginated-walk-equals-unpaginated for both
  node and rel history.
- `pkg/graph/store/badger/history_cursor_test.go` — same coverage plus
  pending-only / persisted / mixed buffer scenarios and the
  same-ID-multiple-versions dedup case.
- `pkg/graph/store/tiered/history_cursor_test.go` — empty graph,
  limit=0 parity, paginated walk across ref + event shards, cross-shard
  same-ID dedup.
- `pkg/graph/internal/core/export_history_bounded_test.go` — seeds
  1K nodes × 50 versions = 50K history records and asserts that
  `ExportGraph`'s heap delta stays under 16 MiB. Without the cursor the
  history-ID slice alone would allocate 40+ MiB.
- `pkg/graph/internal/core/diff_callback_test.go` — covers the streaming
  `DiffSnapshotsCallback` path: parity with `DiffSnapshots`, two-phase
  rule-15 lifecycle (create-update-delete with three diff windows
  asserting the correct before/after state in each), handler abort
  propagation, nil-handler safety, empty-graph short-circuit, invalid
  time-range sentinel (`ErrInvalidTimeRange`), relationship endpoint
  filter, and an informational RAM measurement at 20K stable nodes + 10
  updates that logs `TotalAlloc` and peak `HeapInuse` for both paths.
  (The asymptotic snapshot-buffer savings the design intends are
  dominated by per-entity deep-copy traffic at the in-process scale this
  test can exercise; the asymptotic improvement is documented above.)

## [3.4.0] - 2026-05-07

### Sub-API accessors on Graph (Option 3) — BREAKING

The 130+ public methods that previously lived directly on `*Graph` have been
**removed** and reorganized into 13 discoverable sub-API accessors. Customers
MUST migrate every call site to the sub-API form — see the migration table
below. The implementation has been extracted into `pkg/graph/internal/core`;
`*Graph` is now a thin façade with the sub-API field accessors plus `New`,
`Close`, and `Core` (escape hatch).

#### Migration

```go
// REMOVED in v3.4.0                            // Replacement
g.AddNode(labels, props)                        g.Nodes.Add(labels, props)
g.GetNode(id)                                   g.Nodes.Get(id)
g.NodesByLabel("Person", opts)                  g.Nodes.ByLabel("Person", opts)
g.UpdateNode(id, updates)                       g.Nodes.Update(id, updates)
g.DeleteNode(id)                                g.Nodes.Delete(id)
g.AddRelationship(typ, a, b, props)             g.Rels.Add(typ, a, b, props)
g.OutgoingRelationships(id, "")                 g.Rels.Outgoing(id, "")
g.GetNodesValidAt(t)                            g.Temporal.NodesAt(t)
g.GetNodesByLabelValidAt(label, t)              g.Temporal.NodesByLabelAt(label, t)
g.Snapshot(t)                                   g.Temporal.Snapshot(t)
g.DiffSnapshots(t1, t2)                         g.Temporal.Diff(t1, t2)
g.CreatePropertyIndex(label, key)               g.Index.CreateProperty(label, key)
g.SearchNearestNodes(label, key, vec, k, opts)  g.Index.SearchNearest(label, key, vec, k, opts)
g.RegisterIndexProvider(p)                      g.Index.RegisterProvider(p)
g.SetEventBus(eb)                               g.Events.SetSync(eb)
g.SetAsyncEventBus(b)                           g.Events.SetAsync(b)
g.RunRepair()                                   g.Admin.Repair()
g.ListShards()                                  g.Admin.ListShards()
g.ExportGraph(w)                                g.IO.Export(w)
g.ImportGraph(r)                                g.IO.Import(r)
g.VerifyNodeHashChain(id)                       g.Hash.VerifyNodeChain(id)
g.VerifyRelHashChain(id)                        g.Hash.VerifyRelChain(id)
g.SetTemporalConstraints(cs)                    g.Constraints.Set(cs)
g.AddTemporalConstraint(c)                      g.Constraints.Add(c)
g.TemporalConstraints()                         g.Constraints.Get()
g.NodeCount()                                   g.Stats.NodeCount()
g.RelationshipCount()                           g.Stats.RelCount()
g.NodeCountByLabel("Person")                    g.Stats.NodeCountByLabel("Person")
g.AllLabelCounts()                              g.Stats.AllLabelCounts()
g.ResolveNodeProperty(n, "tkg_hash")            g.Resolve.NodeProperty(n, "tkg_hash")
g.GetOrCreateLabel("Person")                    g.Resolve.LabelToken("Person")
g.LookupLabel("Person")                         g.Resolve.LookupLabel("Person")
g.BeginTx()                                     g.Tx.Begin()
                                                g.Tx.Run(func(tx) error { ... })
NewBatchBuilder(g)                              g.Batch.New()
```

The 13 sub-API accessors:

| Field          | Package                              | Methods | Purpose                                                                |
|----------------|--------------------------------------|---------|------------------------------------------------------------------------|
| `g.Nodes`      | `pkg/graph/nodes`                    | 31      | Node CRUD, label, property, version chain                              |
| `g.Rels`       | `pkg/graph/rels`                     | 30      | Relationship CRUD, adjacency, property, version chain                  |
| `g.Temporal`   | `pkg/graph/temporal`                 | 24      | Point-in-time, interval, bitemporal, snapshot/diff, Allen relations    |
| `g.Index`      | `pkg/graph/index`                    | 13      | Property/vector/high-frequency index management + IndexProvider        |
| `g.Events`     | `pkg/graph/events`                   | 3       | Sync/async EventBus management                                         |
| `g.Constraints`| `pkg/graph/constraints`              | 3       | Temporal-constraint set management                                     |
| `g.IO`         | `pkg/graph/io`                       | 2       | Export / Import                                                        |
| `g.Admin`      | `pkg/graph/admin`                    | 9       | Tiered-store admin (archive, repair, shards, rotate, reset)            |
| `g.Stats`      | `pkg/graph/stats`                    | 6       | Count helpers                                                          |
| `g.Hash`       | `pkg/graph/hash`                     | 2       | Hash-chain verification (shadows stdlib `hash` — alias as `tkghash` if needed) |
| `g.Resolve`    | `pkg/graph/resolve`                  | 6       | Shadow-property + registry resolution                                  |
| `g.Tx`         | `pkg/graph` (TxAPI in subapi.go)     | 3       | Transaction begin / Run / RunContext                                   |
| `g.Batch`      | `pkg/graph` (BatchAPI in subapi.go)  | 1       | New BatchBuilder                                                       |

#### Why

Discoverability for 130+ methods on a single type was poor. After tab-completing
`g.` in any LSP-aware editor, customers saw an alphabetic dump of every method.
Now customers see 13 categories. The pattern matches Kubernetes client-go
(`clientset.CoreV1()`), GitHub Go SDK (`client.Issues`), and AWS SDK v2 service
clients.

#### Implementation note

Each sub-API package declares a local `Core` interface listing only the methods
its wrappers forward to. The interface is satisfied implicitly by
`*core.Core` (the new internal type holding all state and method bodies).
Sub-API constructors take a `*core.Core` directly. Wrappers are 1-2 lines
each, single indirect dispatch — no devirtualization, but the benchmark
gate measured no regression vs v3.3.0.

#### Performance

`benchstat` over n=3 iterations on every Reads/Writes sub-benchmark
(MemoryStore, Apple M4 Max). Geomean Δ +0.44% on time, +0.01% on bytes/op,
+0.00% on allocs/op — well within the 2% tolerance gate. Allocations per op
are byte-for-byte identical for every existing-method benchmark. The sub-API
additions are effectively free at runtime — only customers who actually call
`g.Nodes.X(...)` pay the indirect-dispatch cost, and only once per call.

#### Public surface on `*Graph` after this MR

- `New(cfg Config) (*Graph, error)`
- `(*Graph).Close() error`
- `(*Graph).Core() *core.Core` (escape-hatch, not part of the stable customer surface)
- The 13 sub-API fields listed in the table above
- `NewBatchBuilder(g *Graph) *BatchBuilder` (free function, equivalent to `g.Batch.New()`)
- The type aliases re-exported for convenience: `Config`, `ValidationLimits`,
  `GraphTx`, `BatchBuilder`, `BatchResult`, `BatchError`, `GraphStats`,
  `StoreStats`, `IDComponents`, `ConstraintSet`.

The implementation moved into a new internal package
`pkg/graph/internal/core` (the `Core` type holds all the heavy state, locks,
generators, and method bodies). `*Graph` is now a thin façade that holds a
`*core.Core` and the 13 sub-API field accessors. The package layout reflects
this — `pkg/graph/graph.go` is 118 LOC (from 380), and the entire
implementation lives in `pkg/graph/internal/core/` (≈7.5K LOC across the
27 implementation files plus the 53 test files).

### Documentation

- `CLAUDE.md`, `AGENTS.md`, `docs/architecture.md` — file map updated to list
  the 11 new sub-API package directories under `pkg/graph/` plus the two
  in-package sub-API types (`TxAPI`, `BatchAPI`) in `pkg/graph/subapi.go`.
- Status lines bumped to v3.4.0.

## [3.3.0] - 2026-05-07

### Public API reorganization (Option A — audience-based sub-packages)

This is a major API restructure. External customers (notably `tkgd-v3`) must update import paths.

#### New sub-packages

- `pkg/graph/store` — `Store` interface, `QueryOpts`, `ShardDepth`, `RelTombstone`, `DistanceMetric`, the `Depth*` and `Distance*` constants, and 12 store-layer sentinel errors (was `pkg/graph.{Store,QueryOpts,...}`).
- `pkg/graph/store/memory` — `memory.Store`, `memory.New()` (was `graph.MemoryStore`, `graph.NewMemoryStore`).
- `pkg/graph/store/badger` — `badger.Store`, `badger.Config`, `badger.New()` (was `graph.BadgerStore`, `graph.BadgerStoreConfig`, `graph.NewBadgerStore`).
- `pkg/graph/store/tiered` — `tiered.Store`, `tiered.Config`, `tiered.New()`, `tiered.MigrateFromBadger()`, `tiered.ShardInfo`, `tiered.VerifyResult`, `tiered.RepairResult`, plus the four TieredStore sentinels (`ErrEventPropertyIndex`, `ErrPrimaryLabelClassMutation`, `ErrNotReferenceEntity`, `ErrCrossShardArchiveRel`).
- `pkg/graph/events` — `Event`, `EventBus`, `AsyncEventBus`, `EventType`, `EventPriority`, `BackpressureStrategy`, `Publisher`, plus constructors and constants.
- `pkg/graph/index` — `IndexProvider`, `Initializable`, `GraphReader`, `LegacyIndexProvider`, plus the three `ErrIndexProvider*` sentinels.
- `pkg/graph/temporal` — `GraphSnapshot`, `SnapshotDiff`, `NodeUpdate`, `RelUpdate`, `TemporalConstraint`, `TemporalConstraintKind`, `ConstraintSet`, `NewConstraintSet`, `ConstraintRelWithinEndpoints`, plus seven temporal-constraint sentinels.
- `pkg/graph/ontology` — `EntityClass`, `OntologyMapping`, `NewOntologyMapping`, `ClassEvent`, `ClassReference`.

#### Breaking changes

- **`LegacyIndexProvider.OnEvent` signature** — was `OnEvent(events.Event, *Graph)`, now `OnEvent(events.Event, index.GraphReader)`. The interface had to move out of `pkg/graph` to break a cycle with `pkg/graph/index`; the read-only contract is now enforced at the type level.
- **All public re-exports for store backends, events, index providers, temporal constraints, and ontology classification are removed from `pkg/graph`**. Customers must import the new sub-packages directly.

#### Migration guide

```go
// Before (v3.2.0)                              // After (v3.3.0)
graph.QueryOpts{...}                            store.QueryOpts{...}
                                                // import .../pkg/graph/store

graph.NewMemoryStore()                          memory.New()
                                                // import .../pkg/graph/store/memory

graph.NewBadgerStore(graph.BadgerStoreConfig{}) badger.New(badger.Config{})
                                                // import .../pkg/graph/store/badger

graph.NewTieredStore(graph.TieredStoreConfig{}) tiered.New(tiered.Config{})
                                                // import .../pkg/graph/store/tiered

graph.MigrateFromBadger(src, dst)               tiered.MigrateFromBadger(src, dst)

graph.NewEventBus()                             events.NewEventBus()
graph.SetEventBus(...)                          g.SetEventBus(events.NewEventBus())
graph.PriorityHigh                              events.PriorityHigh
                                                // import .../pkg/graph/events

graph.IndexProvider                             index.IndexProvider
graph.LegacyIndexProvider                       index.LegacyIndexProvider
graph.GraphReader                               index.GraphReader
                                                // import .../pkg/graph/index

graph.GraphSnapshot                             temporal.GraphSnapshot
graph.SnapshotDiff                              temporal.SnapshotDiff
graph.NewConstraintSet(...)                     temporal.NewConstraintSet(...)
graph.ConstraintRelWithinEndpoints              temporal.ConstraintRelWithinEndpoints
                                                // import .../pkg/graph/temporal

graph.NewOntologyMapping([]string{"Case"})      ontology.NewOntologyMapping([]string{"Case"})
graph.ClassReference                            ontology.ClassReference
                                                // import .../pkg/graph/ontology

errors.Is(err, graph.ErrNodeNotFound)           errors.Is(err, store.ErrNodeNotFound)
errors.Is(err, graph.ErrEventPropertyIndex)     errors.Is(err, tiered.ErrEventPropertyIndex)
errors.Is(err, graph.ErrTemporalConstraint)     errors.Is(err, temporal.ErrTemporalConstraint)
```

#### Internal restructure

- `internal/events` deleted (contents promoted to `pkg/graph/events`).
- `internal/temporal` deleted (contents promoted to `pkg/graph/temporal`).
- `internal/memorystore` deleted (contents promoted to `pkg/graph/store/memory`).
- `internal/badgerstore` deleted (contents promoted to `pkg/graph/store/badger`).
- `internal/tieredstore` deleted (contents promoted to `pkg/graph/store/tiered`).
- `internal/store` renamed to `internal/storeutil` (helpers only — key encoding, msgpack wire types, pagination, temporal-filter push-down). The public `Store` contract now lives in `pkg/graph/store`.

#### File consolidation in pkg/graph

Pre-restructure consolidation merged 11 over-split per-operation files into 4 cohesive files organized by domain:

- `context_node_{add,update,read_delete}.go` (3 files) → `node.go` (~750 LOC)
- `context_relationship_{add,update,read_delete,import}.go` (4 files) → `relationship.go` (~900 LOC)
- `temporal_{snapshot,diff}.go` → folded into `temporal.go` (~660 LOC)
- `graph.go` + `lifecycle.go` + `config.go` → `graph.go` (~330 LOC)

## [3.2.0] - 2026-05-07

### Breaking changes

- **`MigrateFromBadger(src, dst, labels)` → `MigrateFromBadger(src, dst)`**. The `*LabelRegistry` parameter is dropped; the function now loads the registry from the source `*BadgerStore` directly via `LoadLabelRegistry`. Callers no longer have to allocate and populate a registry before invoking the migrator.
- **`graph.ComputeNodeHash` and `graph.ComputeRelHash` are removed from the public API**. They were thin re-exports of the hash primitives in `pkg/graph/internal/integrity`. Use `Graph.VerifyNodeHashChain` / `Graph.VerifyRelHashChain` for chain verification; the primitives themselves remain available inside `pkg/graph/internal/integrity` for internal use.
- **`graph.LabelRegistry` and `graph.RelTypeRegistry` are removed from the public API**. The registries live in `pkg/graph/internal/registry/` and are managed entirely through the `*Graph` (creating, resolving, persisting through `Close`/`MigrateFromBadger`). No public function references them after the `MigrateFromBadger` signature change above.
- **TieredStore catalog types removed from the public API**: `graph.ShardCatalog`, `graph.ShardEntry`, `graph.ShardKind`, `graph.ShardTier`, `graph.NewShardCatalog`, `graph.EventShard`, plus the constants `graph.ShardReference`, `graph.ShardEvent`, `graph.TierHot`, `graph.TierWarm`, `graph.TierCold`. They had been re-exported from `internal/tieredstore` "for tests" but were never customer-facing — `tkgd-v3` does not reference them. Tests that used them now import `gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/tieredstore` directly.
- **`graph.RelDeleteInfo` removed from the public API**. The struct is a `BadgerStore` cascade-delete payload; tests that need it import `gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/badgerstore` directly.

### API consolidation (no behaviour change)

- `pkg/graph/aliases.go` (382 lines, 50 declarations) is split into themed files at the `pkg/graph/` top level. The public API surface for the kept symbols is unchanged — every alias still resolves to the same canonical `internal/*` declaration as before. The new layout is:
  - `pkg/graph/store.go` — `Store`, `QueryOpts`, `ShardDepth`, `RelTombstone`, `DistanceMetric` aliases plus the 12 store-layer sentinel errors (`ErrNodeNotFound`, `ErrRelNotFound`, `ErrNodeExists`, `ErrRelExists`, `ErrVersionNotFound`, `ErrNoVersionValidAt`, `ErrIndexExists`, `ErrIndexNotFound`, `ErrTemporalIndexExists`, `ErrTemporalIndexNotFound`, `ErrTxDone`, `ErrStoreClosed`).
  - `pkg/graph/events.go` — `Event`, `EventType`, `EventPriority`, `EventHandler`, `EventBus`, `AsyncEventBus`, `AsyncEventBusConfig`, `BackpressureStrategy`, plus the six `EventNode*`/`EventRel*` constants, the five `Priority*` constants, the three `Backpressure*` constants, and the `NewEventBus` / `NewAsyncEventBus` constructors.
  - `pkg/graph/ontology.go` — `EntityClass`, `OntologyMapping`, `NewOntologyMapping`, plus the `ClassEvent` / `ClassReference` constants.
  - `pkg/graph/snowflake.go` — `IDComponents`, `DecomposeID`, plus the package-level `snowflakeEpoch` / `snowflakeLayout` helpers used internally by `lifecycle.go` and ID-decomposition code.
  - `pkg/graph/errors.go` — vector-index sentinel errors (`ErrVectorIndexExists`, `ErrVectorIndexNotFound`, `ErrDimensionMismatch`) and the registry sentinels (`ErrEmptyName`, `ErrRegistryNotEmpty`).
  - `pkg/graph/backends.go` — concrete `Store` impls: `MemoryStore` + `NewMemoryStore`, `BadgerStore` + `BadgerStoreConfig` + `NewBadgerStore`, `TieredStore` + `TieredStoreConfig` + `NewTieredStore`, the admin return types `ShardInfo` / `VerifyResult` / `RepairResult`, the four TieredStore-specific sentinels (`ErrEventPropertyIndex`, `ErrPrimaryLabelClassMutation`, `ErrNotReferenceEntity`, `ErrCrossShardArchiveRel`), and the new-signature `MigrateFromBadger(src, dst)`.
  - `pkg/graph/temporal_constraint.go` — temporal-constraint aliases (`TemporalConstraintKind`, `TemporalConstraint`, `ConstraintSet`, `ConstraintRelWithinEndpoints`, `NewConstraintSet`, the seven sentinel errors) sit alongside the Graph-coupled enforcement methods (`checkTemporalConstraints` and helpers).
- `pkg/graph/pagination.go` is deleted. The lowercase `paginateNodes` / `paginateRels` / `sortNodesByID` / `sortRelsByID` wrappers were one-line forwarders; the three call sites (`queries.go`, `graph_property_query.go`, `temporal_queries.go`) now invoke `internal/store.PaginateNodes` / `PaginateRels` / `SortNodesByID` / `SortRelsByID` directly. The other wrappers (`paginateIDs`, `paginateNodeIDs`, `paginateRelIDs`, `toNodeIDs`, `toRelIDs`) had no remaining callers and are gone.

### Internal

- `Store`, `QueryOpts`, `RelTombstone`, the 12 store sentinels, and the in-memory index types (`EntityClass`, `OntologyMapping`, vector-index errors, registry errors) keep their canonical declarations inside `pkg/graph/internal/{store,index}` due to the import-cycle constraint with the helpers (`PaginateIDs`, `EntityValidFrom`, `MatchesTemporalFilter`) that depend on them. The public type aliases at `pkg/graph/*.go` preserve the customer-facing names.

### Internal cleanup (Phase 7a follow-up)

- Renamed local `integrity` variables in `pkg/graph/batch.go` to `nodeIntegrity`/`relIntegrity` so the `internal/integrity` package can be imported under its bare name. The `integritypkg` import alias is gone.
- All `pkg/graph/*_test.go` files unified to `package graph` (no more `package graph_test`). Eliminates the !45 caveat where tests landing in `findings_regression_test.go` should have been in `remove_label_test.go` but couldn't due to package boundary.

## [3.1.23] - 2026-05-07

### Added

- `Graph.GetRelationshipsByTypeValidAt(relType, at)` — point-in-time relationship convenience query (parity with the Node side's `GetNodesByLabelValidAt`).
- `Graph.RelationshipsByTypePropertyAndTime(relType, key, value, at)` — point-in-time predicate query for relationships.
- `Graph.RelationshipsByTypePropertyDuring(relType, key, value, start, end)` — interval predicate query with predicate-during-interval semantics (matches any version whose property held during any portion of the requested interval, not just the most-recent overlap).
- `BadgerStore.SaveRegistries(labelReg, relTypeReg)` — atomic single-transaction registry persistence (parity with `TieredStore.SaveRegistries`); the previous `SaveLabelRegistry` + `SaveRelTypeRegistry` pair persisted across two separate transactions and could leave the on-disk state inconsistent on crash.
- IndexProvider redesign (Phase 6): the `IndexProvider` interface no longer takes a `*Graph` parameter and now returns `error`; new optional `Initializable` interface lets providers participate in bulk-load on startup; new narrow `GraphReader` interface gives providers a stable read surface without coupling to the full `*Graph`; AsyncEventBus subscription is now supported. Direct test coverage added in `index_provider_test.go`.

### Changed (backward-compatible)

- IndexProvider migration: the pre-Phase-6 interface shape is preserved as `LegacyIndexProvider`. Existing customers must change their registration call from `RegisterIndexProvider` to `RegisterLegacyIndexProvider`. The provider implementation itself is identical — only the registration call site changes.

### Changed (structural, no behaviour change)

- **Restructure phase 6 — `internal/snowflake` extracted from `internal/store`** (`pkg/graph/internal/store/id_decompose.go` → `pkg/graph/internal/snowflake/id_decompose.go`, package `snowflake`). The shared `SnowflakeEpoch`, `SnowflakeLayout`, `IDComponents`, and `DecomposeID` symbols moved to a new dedicated package so locks, tieredstore, and badgerstore stop depending on `internal/store` purely for layout decoding. Public surface unchanged — `pkg/graph/aliases.go` still re-exports `DecomposeID`, `IDComponents`, `snowflakeEpoch`, `snowflakeLayout`. The `internal/store` symbols `SnowflakeLayout` and `SnowflakeEpoch` are gone (no caller outside the same package referenced them after the move).
- **`internal/registry` extracted from `internal/index`** (`label_registry.go`, `label_registry_test.go`, `reltype_registry.go`, `reltype_registry_test.go` → `pkg/graph/internal/registry/`, package `registry`). `internal/index/aliases.go` re-exports `LabelRegistry`, `RelTypeRegistry`, `NewLabelRegistry`, `NewRelTypeRegistry`, `ErrEmptyName`, `ErrRegistryNotEmpty`, and `TokenCapacityMax` so existing callers (graph layer, badgerstore, tieredstore, tests) keep compiling unchanged.
- **`internal/temporal` package**: pure constraint types (`TemporalConstraintKind`, `TemporalConstraint`, `ConstraintSet`, the seven sentinel errors, `NewConstraintSet`) extracted from `pkg/graph/temporal_constraint.go` into a dedicated subpackage; the Graph-coupled enforcement method `checkTemporalConstraints` stays in `pkg/graph/temporal_constraint.go`. Direct unit tests added — `internal/temporal/` is now at 100% direct coverage (was 0% direct, only exercised through Graph integration tests).
- **`pkg/graph/graph.go` split into 14 files** (graph.go 1880L → 48L). Concerns split into `config.go`, `lifecycle.go`, `validation.go`, `resolution.go`, `crud.go`, `property_cas.go`, `queries.go`, `graph_indexes.go`, `vector_search.go`, `graph_property_query.go`, `admin.go`, `events_dispatch.go`, `node_label.go`, `version_chain.go`. Function bodies are byte-identical.
- **`pkg/graph/context.go` split per entity**: `context.go` (helpers) + `context_node_add.go`, `context_node_update.go`, `context_node_read_delete.go`, `context_relationship_add.go`, `context_relationship_read_delete.go`, `context_relationship_update.go`, `context_relationship_import.go`. Function bodies are byte-identical.
- **`pkg/graph/temporal.go` split by feature**: `temporal.go` (internal helpers, 446L) + `temporal_queries.go`, `temporal_snapshot.go`, `temporal_diff.go`. Function bodies are byte-identical.
- **Test relocation across the restructure**: 11 test-relocation MRs and the Phase-6 follow-ups moved tests next to their backend code in `internal/{memorystore, badgerstore, tieredstore, events, store, index, integrity}`. Some Graph-integration tests were intentionally kept in `pkg/graph/` where they thread `New(Config{})` through their scenarios — splitting along that seam would have required pulling Graph machinery into the internal test fixtures (B42 moves-only contract). `tieredstore_test.go` and `tieredstore_history_routing_test.go` remain in `pkg/graph/` for the same reason; deferred to a follow-up MR per the project's "if unsure, defer" convention.
- `pkg/graph/lifecycle.go` uses a unified `registriesPersister` interface for both `BadgerStore` and `TieredStore` — neither backend's atomic registry-persistence call is referenced by name, so adding a third backend in the future requires only implementing `SaveRegistries(labelReg, relTypeReg)`.

### Test coverage

- `internal/temporal/` — 100% direct coverage (was 0% direct).
- `internal/integrity/` — 100% direct coverage with five SHA-256 fixed-vector anchors locking the on-disk hash format (was 32.9% direct, only exercised through Graph integration tests).

### Documentation

- `tasks/lessons.md` — B42 deduped (renamed to B44), B22-B29 reordered ascending, B31-B35 reordered ascending. Block bodies are byte-identical; only their position changed.
- File-map sync in `CLAUDE.md` and `docs/architecture.md` reflects the post-restructure `pkg/graph/` layout (graph.go split + internal-package extractions).
- This MR adds the Phase-6 entry above to the long-running restructure narrative; previous restructure phases are described in their own version sections (3.1.17 — phase 1, 3.1.18 — phase 2, 3.1.19 — phase 3, 3.1.21 — phase 4, 3.1.22 — phase 5).

## [3.1.22] - 2026-05-07

### Changed (structural, no behaviour change)

- **Restructure phase 5 — split `pkg/graph/integrity.go` (helpers → `pkg/graph/internal/integrity`)** (`pkg/graph/integrity.go` → `pkg/graph/integrity.go` + `pkg/graph/internal/integrity/integrity.go`, package `integrity`). The two `*Graph` methods (`VerifyNodeHashChain`, `VerifyRelHashChain`) stay in `pkg/graph/integrity.go` because they have a `*Graph` receiver and call `g.store.GetNode` / `g.NodeLabels` / `g.RelationshipType`; the four pure helpers (`ComputeNodeHash`, `ComputeRelHash`, `appendProperties`, `appendPropertyValue`) plus `hashBufPool` move into the new `internal/integrity` subpackage. Cross-boundary exports: `ComputeNodeHash` and `ComputeRelHash` were already exported and stay so under the new `integrity.` qualifier; `hashBufPool`, `appendProperties`, and `appendPropertyValue` stay unexported because the only callers are now same-package. Function bodies are byte-identical to the pre-split implementation — hash output for the same input is unchanged (verified by the existing `integrity_test.go` table of `TestComputeNodeHash*` / `TestComputeRelHash*` tests, all green). **Judgement call**: `integrity_test.go` stayed in `pkg/graph/` rather than splitting along the helper / Graph-method seam because the file mixes pure-helper unit tests (`TestComputeNodeHashDeterministic`, `TestComputeRelHashChangesWithType`, etc.) with `*Graph` integration tests (`TestVerifyNodeHashChain_GenesisOnly`, `TestVerifyRelHashChain_*`) that thread `New(Config{})` through their scenarios. Splitting the file would have required either pulling Graph machinery into `internal/integrity` test fixtures or fragmenting the test helpers, both violating the moves-only contract from B42 — same precedent applied in Phase 4 to `events_test.go`. The pure-helper tests remain green by relying on the `var ComputeNodeHash = integrity.ComputeNodeHash` / `var ComputeRelHash = integrity.ComputeRelHash` re-exports added to `pkg/graph/aliases.go`. The same alias keeps the ~19 internal `pkg/graph/` call sites in `batch.go` (2), `graph.go` (5), `context.go` (12) compiling unchanged — no qualified-name updates were needed at the call sites, keeping the diff to a verifiable copy-paste split. The `pkg/graph/integrity.go` retained file imports `pkg/graph/internal/integrity` directly and uses the qualified `integrity.ComputeNodeHash` / `integrity.ComputeRelHash` calls inside its two methods.
- **Dependency arrows** after the move: `pkg/graph` → `internal/integrity`, `internal/events`, `internal/store`, `internal/locks`, `internal/index`, `internal/memorystore`, `internal/badgerstore`, `internal/tieredstore`; `internal/integrity` → `internal/store`, `pkg/types`. No `internal/*` package imports `pkg/graph` (verified by grep). `go build ./...`, `go vet ./...`, `gofmt -l .` empty, and `go test -race -short -count=1 -timeout 600s ./...` all green.

## [3.1.21] - 2026-05-07

### Changed (structural, no behaviour change)

- **Restructure phase 4 — extract `pkg/graph/internal/events`** (`pkg/graph/events.go` → `pkg/graph/internal/events/events.go`, package `events`). Cross-boundary exports: the package-private `eventPublisher` interface became `events.Publisher` because it is the type of `Graph.events`, which is set by the public `SetEventBus`/`SetAsyncEventBus` methods and consumed by the publish path that crosses the boundary; the corresponding interface method `publish(Event)` was renamed to `Publish(Event)` so both `*EventBus` and `*AsyncEventBus` satisfy it from the new package. All other identifiers in `events.go` were already exported (`EventType` + the six `EventNode*`/`EventRel*` constants, `EventPriority` + the five `Priority*` constants, `Event`, `EventHandler`, `EventBus`, `NewEventBus`, `AsyncEventBus`, `AsyncEventBusConfig`, `NewAsyncEventBus`, `BackpressureStrategy` + the three `Backpressure*` constants); package-private helpers used cross-file inside `events.go` only (`safeInvoke`, `numPriorityLevels`, `priorityOrder`, the `dispatch`/`drainAll`/`worker` methods on `*AsyncEventBus`) stayed lowercase. **Judgement call**: `events_test.go` and `async_eventbus_test.go` stayed in `pkg/graph/` rather than moving with the source, mirroring Phase 3's treatment of the BadgerStore/TieredStore integration tests — both files mix pure-bus tests (`TestEventBus_Subscribe_Unsubscribe`, `TestEventBus_PanicHandler`, `TestAsyncEventBus_BackpressureBlock`, etc.) with `*Graph` integration tests (`TestGraph_NodeCreate_Event`, `TestPriority_GraphCreateIsHigh`, `TestSetAsyncEventBus_GraphIntegration`, etc.) that thread `New(Config{})` and `g.AddNode`/`g.AddRelationship` through their scenarios. Splitting the files would have required either pulling Graph machinery into the `internal/events` test files or fragmenting the test helpers, both violating the moves-only contract from B42. The remaining call sites inside `pkg/graph/` (the `Graph.events` field declaration in `graph.go`, `publishEvent` and `dispatchEvent` in `graph.go`, the `ep.publish(e)` lines in `tx.go` Commit and `batch.go` Run, plus one in-test `bus.publish` in `v3056_fixes_test.go`) were updated to call `Publish` on the new `events.Publisher` interface; the doc-comment reference to `eventPublisher` in `index_provider.go` was updated to `events.Publisher`. `pkg/graph/aliases.go` re-exports `Event`, `EventType` (+ constants), `EventPriority` (+ constants), `EventHandler`, `EventBus`, `AsyncEventBus`, `AsyncEventBusConfig`, `BackpressureStrategy` (+ constants), and the `NewEventBus`/`NewAsyncEventBus` constructors so the public API surface used by `tkgd-v3` (`graph.EventBus`, `graph.NewEventBus`, `graph.SetEventBus`, the `EventNode*` constants, etc.) is unchanged.
- **Dependency arrows** after the move: `pkg/graph` → `internal/events`, `internal/store`, `internal/locks`, `internal/index`, `internal/memorystore`, `internal/badgerstore`, `internal/tieredstore`; `internal/events` → `pkg/types` only. No `internal/*` package imports `pkg/graph` (verified by grep). `go build ./...`, `go vet ./...`, `gofmt -l .` empty, and `go test -race -short -count=1 -timeout 600s ./...` all green.

## [3.1.20] - 2026-05-07

### Fixed

- **Vector index stale after `UpdateNode`** (`pkg/graph/internal/memorystore`, `pkg/graph/internal/badgerstore`, `pkg/graph/internal/tieredstore`): `ReplaceNodeWithHistory` — the path called by every `UpdateNode` — did not update in-memory vector indexes. The old vector entry was never removed; the new one was never inserted. After any node update, `SearchNearestNodes` returned pre-update distances for the modified node. Fixed in all three store backends.
- **Batch operations missing temporal and vector index maintenance** (same packages): `PutNodesBatch` and `DeleteNodesBatch` did not maintain temporal or vector indexes. Batch-inserted nodes were invisible to temporal queries and vector searches; batch-deleted nodes remained as phantom candidates. Fixed in `MemoryStore`, `BadgerStore`, and `TieredStore` (six locations total). `TieredStore.ReplaceNodeWithHistory` also fixed — it delegated to a shard but did not update the TieredStore-level vector index map.
- **Dead code `shardForRelID` (unchecked variant) removed** (`pkg/graph/internal/tieredstore`): the function was never called — all callers use `shardForRelIDChecked`. It contained a checkout-without-pin bug in its cold-shard probe loop. Deleted.

## [3.1.19] - 2026-05-07

### Changed (structural, no behaviour change)

- **Restructure phase 3 — extract `pkg/graph/internal/memorystore`, `pkg/graph/internal/badgerstore`, and `pkg/graph/internal/tieredstore`**: three sequential moves landed as their own commits, each leaving the tree green.
  - **Step A — `internal/memorystore`** (`pkg/graph/memorystore.go` → `pkg/graph/internal/memorystore/memorystore.go`). Cross-boundary exports: `searchNearestFiltered` → `SearchNearestFiltered` on all three Store impls (renamed in this commit on `BadgerStore` and `TieredStore` too because Go interface satisfaction matches by method name; the package-internal `filteredVectorSearchStore` interface in `graph.go` now declares `SearchNearestFiltered`). Pagination/sort helpers (`paginateIDs`, `paginateNodes`, `paginateRels`, `paginateNodeIDs`, `paginateRelIDs`, `toNodeIDs`, `toRelIDs`, `sortNodesByID`, `sortRelsByID`) moved into `pkg/graph/internal/store/pagination.go` with capitalised names; `pkg/graph/aliases.go` retains lowercase wrappers so existing call sites and tests are untouched. New `MemoryStore` test-export helpers (`GetNodeHistoryEntry`, `SetNodeHistoryEntryForTest`, `SetNodeForTest`) added so the one tampering test that previously poked unexported maps (`findings_regression_test.go`) keeps working through a narrow surface.
  - **Step B — `internal/badgerstore`** (`pkg/graph/badgerstore.go` and `pkg/graph/badgerstore_partial.go` → `pkg/graph/internal/badgerstore/`). Tests that exercise BadgerStore in isolation (`badgerstore_test.go`, `badgerstore_partial_test.go`, `badgerstore_temporal_test.go`) move with the source; integration tests using BadgerStore as one component of a Graph stay in `pkg/graph/`. Cross-boundary exports for the partial-write helpers TieredStore reaches into: `hasNodeID/HasRelID/IncomingRelIDs/OutgoingRelIDs/PutRelEntityAndOut/PutRelIncoming/DeleteRelIncoming/DeleteRelEntityAndOut/DeleteIncomingByRelID/ScanAndDeleteIncoming` and the `RelDeleteInfo` struct (with `ID`, `RelType`, `StartID`, `EndID` fields exported). Default constants exported as `DefaultCacheCapacity`/`DefaultFlushInterval`/`DefaultGCInterval`/`DefaultGCDiscardRatio`. Test-only exports added on `BadgerStore`: `SetDBClosedForTest`, `SetNodeCountForTest`, `LockFlushMuForTest`/`UnlockFlushMuForTest`, `DBForTest`, `NodeCacheForTest`/`RelCacheForTest`, `LabelIndexForTest`, `HasNodeIDForTest`/`HasRelIDForTest`, `HasTemporalIndexForTest`/`HasHFIndexForTest`, `SyncWritesForTest`/`FlushIntervalForTest`, `LockIdxMuRForTest`/`UnlockIdxMuRForTest`, `ReadOnlyForTest`, `FlushDoneForTest`/`GCDoneForTest`.
  - **Step C — `internal/tieredstore`** (8 source files: `tieredstore.go`, `tieredstore_write.go`, `tieredstore_read.go`, `tieredstore_admin.go`, `tieredstore_repair.go`, `tieredstore_migrate.go`, `shard_catalog.go`, `registry_file.go` → `pkg/graph/internal/tieredstore/`). Standalone tests `shard_catalog_test.go` and `registry_file_test.go` move with the source; the integration tests `tieredstore_test.go` and `tieredstore_history_routing_test.go` stay in `pkg/graph/` because they thread `*Graph` from `newTestTieredGraph` through their scenarios — splitting them would have required pulling >1000 LOC of helpers across the boundary. Cross-boundary exports: `eventShard` → `EventShard`, `registryFileData` → `RegistryFileData`. **Dependency-inversion for `VerifyShard`**: the signature changed from `VerifyShard(g *Graph, ...)` to `VerifyShard(g HashChainVerifier, ...)`, where the new `HashChainVerifier` interface declares `VerifyNodeHashChain` and `VerifyRelHashChain`; `*Graph` satisfies it implicitly. This is the only API-shape change in Phase 3 and it preserves backward compatibility — every existing call site passes a `*Graph` value. A comprehensive `*ForTest` accessor surface was added on `TieredStore` and `EventShard` so the integration tests can keep poking at unexported state without exporting the underlying fields. The `pkg/graph/aliases.go` extension covers `TieredStore`, `TieredStoreConfig`, `ShardInfo`, `VerifyResult`, `RepairResult`, `ShardEntry`, `ShardCatalog`, `ShardKind`, `ShardTier`, `EventShard`, `NewTieredStore`, `NewShardCatalog`, `MigrateFromBadger`, the `ShardReference`/`ShardEvent`/`TierHot`/`TierWarm`/`TierCold` constants, and the four TieredStore-specific sentinels (`ErrEventPropertyIndex`, `ErrPrimaryLabelClassMutation`, `ErrNotReferenceEntity`, `ErrCrossShardArchiveRel`).
  - **Dependency arrows** after the moves: `pkg/graph` → `internal/tieredstore` → `internal/badgerstore` → {`internal/store`, `internal/index`, `internal/locks`}; `internal/tieredstore` also depends on `internal/store` and `internal/index` directly; `pkg/graph` also depends on `internal/memorystore`, `internal/badgerstore`, `internal/store`, `internal/index`, and `internal/locks`. No `internal/*` package imports `pkg/graph` (verified by grep). `go build ./...`, `go vet ./...`, `gofmt -l .` empty, and `go test -race -short -count=1 -timeout 600s ./...` all green.
- **Phase 4 deferred**: there is no Phase 4 in the original restructure plan — Phase 3 completes the structural extraction. Future work (identifier cleanup, file splits within each subpackage) was always slated for follow-up MRs per B42.

## [3.1.18] - 2026-05-06

### Changed (structural, no behaviour change)

- **Restructure phase 2 — extract `pkg/graph/internal/locks` + `pkg/graph/internal/index`, plus relocate the temporal-filter helpers into `internal/store`**: three sequential moves landed as their own commits, each leaving the tree green.
  - **Step A — temporal-filter helpers into `internal/store`** (`pkg/graph/temporal_filter.go` → `pkg/graph/internal/store/temporal_filter.go`). `entityValidFrom` → `EntityValidFrom`, `matchesTemporalFilter` → `MatchesTemporalFilter`. `matchesPointInTime`/`matchesInterval` stayed unexported (only same-file callers). The move pre-empts a cycle that would have appeared once `internal/index` referenced `EntityValidFrom`/`MatchesTemporalFilter`, since Phase 3 will move backends out of `pkg/graph` too.
  - **Step B — entity locks into `internal/locks`** (`pkg/graph/entity_locks.go` → `pkg/graph/internal/locks/entity_locks.go`, package `locks`). `entityLockManager` → `Manager`, `newEntityLockManager` → `NewManager`. Public methods (`LockEntity`, `LockTwo`, `LockMany`) and constants stayed exported. Callers in `pkg/graph/` qualify as `locks.Manager` / `locks.NewManager()`.
  - **Step C — index types into `internal/index`** (`pkg/graph/lru.go`, `label_registry.go`, `reltype_registry.go`, `ontology.go`, `property_index.go`, `temporal_index.go`, `hf_index.go`, `vector_index.go` → `pkg/graph/internal/index/`, package `index`). Cross-package symbols exported: `entityLRU` → `Cache`, `lruEntry` → `Entry`, the cache-status constants → `CacheHit`/`CacheMiss`/`CacheDeleted`; `labelRegistry` → `LabelRegistry`, `relTypeRegistry` → `RelTypeRegistry`, both with their `New*` constructors and `Err*` sentinels; `propertyIndex` → `PropertyIndex`, `temporalIndex` → `TemporalIndex`, `highFrequencyIndex` → `HighFrequencyIndex`, `vectorIndex` → `VectorIndex` (plus their constructors and `Err*` sentinels). `OntologyMapping` and `EntityClass` were already exported; their internal `labelReg *labelRegistry` field tracked the registry rename to `*LabelRegistry`. `internal/index/vector_index.go` references `DistanceMetric` from `internal/store` (Phase 1) qualified as `storepkg.DistanceMetric`. **Judgement call**: `index_provider.go` stays in `pkg/graph` — the `IndexProvider` interface depends tightly on `Graph`, `Event`, `EventBus`, `eventPublisher`, and the `g.indexProviders`/`g.events` fields, and moving it would have required pulling in chunks of `events.go` and `graph.go` that violate the moves-only contract.
  - **`pkg/graph/aliases.go`** extended with `EntityClass`, `OntologyMapping`, `NewOntologyMapping`, the registry+vector-index sentinels (`ErrEmptyName`, `ErrRegistryNotEmpty`, `ErrVectorIndexExists`, `ErrVectorIndexNotFound`, `ErrDimensionMismatch`) so the public API surface stays unchanged.
  - **Dependency arrows** after the move: `pkg/graph` → `internal/store`, `internal/locks`, `internal/index`; `internal/index` → `internal/store`, `pkg/types`; `internal/locks` → `pkg/types`; `internal/store` → `pkg/types`. No `internal/*` package imports `pkg/graph` (verified by grep). `go build ./...`, `go vet ./...`, `gofmt -l .` empty, and `go test -race -short -count=1 -timeout 600s ./...` all green.
- **Phase 3-4 deferred**: extracting `memorystore.go`, `badgerstore.go`, and `tieredstore*.go` into `internal/memorystore`/`internal/badgerstore`/`internal/tieredstore` is left for follow-up MRs.

## [3.1.17] - 2026-05-06

### Changed (structural, no behaviour change)

- **Restructure phase 1 — extract `pkg/graph/internal/store`**: the persistence-contract types (`Store` interface, `QueryOpts`, `ShardDepth`, `RelTombstone`, `DistanceMetric`, the `Err*` sentinels), the binary key encoding (`keys.go`), the msgpack wire format (`wire.go`), the `id_decompose.go` helpers, and the package-level `snowflakeEpoch`/`snowflakeLayout` are now in `pkg/graph/internal/store`. Identifiers that cross the new package boundary were exported (`nodeKey` → `NodeKey`, `nodeWire` → `NodeWire`, `propertyTypeTag` → `PropertyTypeTag`, etc.); identifiers that stay package-private inside `pkg/graph/internal/store` (e.g. `propertiesToWire`, `toInt64`) keep their original lowercase names. `pkg/graph/aliases.go` re-exports `Store`, `QueryOpts`, `ShardDepth`, `RelTombstone`, `DistanceMetric`, the depth and metric constants, the sentinel errors, `IDComponents`, and `DecomposeID` so the public API surface (`graph.Store`, `graph.ErrNodeNotFound`, etc.) is unchanged. `(QueryOpts).hasTemporalFilter` became a free function `hasTemporalFilter(opts)` because Go forbids methods on a non-local aliased type. No semantic changes; `go test -race -short ./...` is green.
- **Phase 2-4 deferred**: the `internal/index`, `internal/locks`, `internal/memorystore`, `internal/badgerstore`, and `internal/tieredstore` extractions described in the restructure plan remain in `pkg/graph` for now and will land in subsequent MRs to keep individual diffs reviewable.

## [3.1.16] - 2026-05-06

### Fixed

- **`BadgerStore.Clear` flush race** (`pkg/graph/badgerstore.go`): a flush goroutine that snapshotted pending writes under `idxMu.RLock` but had not yet submitted its `WriteBatch` could race ahead of `DropAll()` and resurrect pre-Clear entities after a restart. `Clear` now acquires `flushMu` first (same ordering as `flush()`), serialising the two paths end-to-end.
- **`BadgerStore.Clear` `sync.Map` field-replacement race** (`pkg/graph/badgerstore.go`): `bs.labelCounts = sync.Map{}` replaced the struct field while concurrent `NodeCountByLabel` calls read from it without holding `idxMu` — a data race on the field itself. Fixed with `Range+Delete` (concurrency-safe by contract).
- **Missing secondary-index resets in `Clear`** (all three stores): `BadgerStore`, `MemoryStore`, and `TieredStore.Clear` left `temporalIndexes`, `hfIndexes`, and `vectorIndexes` populated. Subsequent `CreateTemporalIndex`/`CreateHighFrequencyIndex`/`CreateVectorIndex` returned "already exists" on a logically empty store; stale vector entries occupied top-k slots in `SearchNearestNodes`. `TieredStore.Clear` also left `tempIdxLabels` set, causing the next rotation to re-install temporal indexes for already-dropped labels.

## [3.1.15] - 2026-05-06

### Fixed

- **`SearchNearestNodes` k ≤ 0 panic** (`pkg/graph/vector_index.go`, `pkg/graph/graph.go`): negative or zero k could reach `make(knnHeap, 0, k)` and panic. Both the Graph layer and `vectorIndex.searchNearest` now return `nil, nil` for k ≤ 0.
- **`SearchNearestNodes` ignores `QueryOpts`** (`pkg/graph/graph.go`, `pkg/graph/tieredstore_write.go`): `ValidAt`/`ValidStart`/`ValidEnd` temporal filters, `After`/`Limit` cursor pagination, and `Depth` gating were silently dropped. Temporal filtering now applies an eligibility predicate **before** the k-cut via `filteredVectorSearchStore`; distance-order cursor pagination applies after the k-cut; `Depth != DepthAll` + temporal filter returns `ErrDepthTemporalUnsupported`; TieredStore `depthFilter` excludes archive-resident nodes from `DepthHot`/`DepthWarm` before heap selection.

## [3.1.14] - 2026-05-06

### Fixed

- **`ImportGraph` panic safety** (`pkg/graph/export.go`): `wireToNode`/`wireToRel` panic on token 0 (reserved). `ImportGraph` reads from an untrusted `io.Reader`; a corrupt or malicious export becomes a process crash. New `validateNodeWire`/`validateRelWire` validate all four record types (node, nodeHist, rel, relHist) before constructing, returning the new `ErrCorruptExport` sentinel on token-0 or out-of-uint16-range values.
- **`RunRepair` silent operational error swallow** (`pkg/graph/tieredstore_repair.go`): Phase 2 conflated `ErrRelNotFound` (legitimate TOCTOU skip) with I/O failures, routing failures, and closed-shard errors — returning "Repair succeeded" while needed `in/` repairs were missed. Now `errors.Is(err, ErrRelNotFound)` continues; all other errors propagate.

## [3.1.13] - 2026-05-06

### Added

- **`DeepCopier` interface** (`pkg/types/property_registry.go`): custom property struct types registered via `RegisterPropertyStructType` must now implement `DeepCopyValue() any`. Enforced at registration; the interface is dispatched in `deepCopyValue` before the generic type switch so registered types with nested mutable state (slices, maps, pointers) get a proper deep copy at the store boundary instead of a shallow struct copy.

### Changed

- **`RegisterPropertyStructType` now returns `error`** (`pkg/types/property_registry.go`): registration validates that the type implements both `HashableValue` (new: `ErrTypeNotHashable`) and `DeepCopier` (new: `ErrTypeNotDeepCopyable`). The check uses the form actually passed — registering a value form when methods are on the pointer receiver only is rejected, preventing a non-addressable runtime type-assert failure in the hash and deep-copy paths.

## [3.1.12] - 2026-05-06

### Fixed

- **Admin-path event-shard pinning** (`tieredstore_admin.go`, `tieredstore.go`, `tieredstore_write.go`): five latent Close-race surfaces closed — `ListShards`, `RebuildCatalog`, and `Clear` called `es.store.NodeCount/RelCount/Clear()` without a `checkoutStore` pin; a concurrent `Close` could free the BadgerStore mid-call. `CreateTemporalIndex`, `DropTemporalIndex`, `CreateHighFrequencyIndex`, `DropHighFrequencyIndex` used `allActiveShards` (unpinned pointers); all four now use `allShardStoresWithLazyOpen`. `findRelInAnyShardStore` changed to accept the caller's pre-pinned snapshot rather than re-resolving via a fresh `checkoutArchive`, closing the window where `Close` nil'd `refArchive` between resolution and scan.
- **`ArchiveNode` / `RestoreNode` graph-level mutex** (`graph.go`): both now acquire `g.mu.Lock()` — same exclusion class as a transaction — preventing a concurrent `AddRelationship` from sneaking past the adjacency pre-scan between archive and cascade.
- **Cross-shard archive guard in `PutRelationship`** (`tieredstore_write.go`): this release returned `ErrCrossShardArchiveRel` when one endpoint was on `refArchive` and the other was not. The Unreleased archive-placement migration supersedes this guard.

## [3.1.11] - 2026-05-06

### Fixed (refArchive parity follow-up — MR !6 + audit)

MR !6 (Markus Nissl) closes refArchive parity gaps left over after MR !4. Pre-MR, an archived reference entity stayed `GetNode`-addressable but silently disappeared from indexed/bulk reads, while a concurrent `Close` could free the archive while a query was still using it.

- **Indexed and bulk reads now see archived entities** (`pkg/graph/tieredstore_read.go`, `pkg/graph/tieredstore.go`):
  - `NodesByLabel` (reference label): refShard ∪ refArchive (was refShard only).
  - `NodesByLabelAndProperty` (reference label): refShard ∪ refArchive.
  - `NodeCountByLabel` (reference label): refShard + refArchive.
  - `RelationshipsByType`: refShard ∪ refArchive ∪ events.
  - `RelCountByType`: refShard + refArchive + events.
  - `AllNodes` / `AllRelationships`: refShard ∪ refArchive ∪ events at `DepthAll`.
  - `AllNodeIDs` / `AllRelIDs`: refShard ∪ refArchive ∪ events at `DepthAll`.
  - Archive merge is gated on `opts.Depth == DepthAll`. `DepthHot` and `DepthWarm` exclude archive — caller explicitly asked to exclude colder tiers. Mirrors event-shard depth handling.
- **Point-lookup ID routing now pins the archive** (`pkg/graph/tieredstore.go`): `shardForNodeIDChecked` and `shardForRelIDChecked` previously resolved archived IDs via raw `refArchive.Load()` and returned a no-op checkin. A concurrent `Close` could free the archive while a public `GetNode`/`UpdateNode`/`DeleteNode` was still holding it, because `archiveActiveReqs` was never incremented. Both routers now go through `checkoutArchive`, mirroring the `activeReqs` discipline already used for event shards.
- **`forEachHistoryShard` now pins the archive** (`pkg/graph/tieredstore.go`): same Close-race risk on the history fan-out path. Switched to `checkoutArchive` with a checkin scoped around the callback.
- **`ArchiveNode` rejected cross-shard relationships** (`pkg/graph/tieredstore_write.go`, new `ErrCrossShardArchiveRel`): this release failed loud rather than silently fragmenting relationship placement when an archived node still had live partners. The Unreleased archive-placement migration supersedes this rejection by moving the entity/out and incoming legs to the correct shards.
- **Admin & repair paths**: `ListShards`, `RebuildCatalog`, `resolveShardStore`, `allShardStoresWithLazyOpen` all use `checkoutArchive`. `Clear` skips lazy-open when no archive exists in the catalog. `CreateTemporalIndex` and `CreateHighFrequencyIndex` now also propagate the index to refArchive (otherwise archived entities are absent from the temporal index).

### Fixed (audit-found refArchive sites missed by MR !6)

Post-merge audit caught two remaining sites with the same Close-race pattern that MR !6 fixed elsewhere:

- **`findRelInAnyShardStore`** (`pkg/graph/tieredstore_admin.go`): probe used raw `ts.refArchive.Load()` then `archive.hasRelID(relID)` without `checkoutArchive`. Used by `RunRepair` Phase 1 — the function's own doc comment notes that missing the archive probe causes silent data loss, but the implementation didn't pin the probe. Fixed: archive probe now runs under `checkoutArchive`; the returned pointer is used only for identity comparison.
- **`ArchiveNode`** and **`RestoreNode`** (`pkg/graph/tieredstore_write.go`): both called `ensureRefArchive()` then `ts.refArchive.Load()` and dereferenced the result for `PutNode`/`PutRelationship`/`DeleteNodeCascade` calls. Concurrent `Close` racing between the Load and the writes could free the archive `BadgerStore` under the operation. Fixed: both paths now call `checkoutArchive()` after `ensureRefArchive()` and `defer archiveCheckin()` for the duration of the cross-store moves. `archiveActiveReqs` makes Close wait.

### Tests Added (MR !6)

- **`pkg/graph/tieredstore_history_routing_test.go`**: 15 new regression tests covering each fix above. Notable: pinning tests assert `archiveActiveReqs > 0` *during* the callback / resolve, proving the pin is held across the boundary. Tests:
  - `TestTieredStore_IndexedPublicQueries_IncludeArchive`
  - `TestTieredStore_BulkQueries_DepthGatesArchive`
  - `TestTieredStore_IndexedQueries_DepthGatesArchive`
  - `TestTieredStore_AllCurrentIDAPIs_IncludeArchiveAtDepthAll`
  - `TestTieredStore_ShardForNodeIDChecked_PinsArchive`
  - `TestTieredStore_ForEachHistoryShard_PinsArchive`
  - `TestTieredStore_ArchiveNode_RejectsCrossShardRel_REtoE`
  - `TestTieredStore_ArchiveNode_RejectsCrossShardRel_EtoR`
  - `TestTieredStore_ArchiveNode_RejectsRefRefRel`
  - `TestTieredStore_Clear_NoArchive_SkipsLazyOpen`
  - `TestTieredStore_TemporalIndexCreate_CoversArchive`
  - `TestTieredStore_ResolveShardStore_PinsArchive`
  - `TestTieredStore_FindRelInAnyShardStore_ProbesArchive`
  - `TestTieredStore_AllShardStoresWithLazyOpen_IncludesArchive`
  - `TestTieredStore_HighFrequencyIndexCreate_CoversArchive`

## [3.1.10] - 2026-05-05

### Fixed (history-aware regressions and batch hardening — MR !5)

- **History-aware indexed candidate planning** (`pkg/graph/graph.go`, `pkg/graph/temporal.go`): `NodesByLabel`, `NodesByLabelAndProperty`, `RelationshipsByType`, `GetNodesByLabelValidAt`, `NodesByLabelPropertyAndTime`, `NodesByLabelPropertyDuring`, and `GetNeighborsValidAt` now derive candidates from the appropriate index (label / property / type / adjacency) and merge them with history IDs. Previously they fell back to `Store.ForEachNodeID` / `ForEachRelID` over every entity even when a narrow index was available — O(N) where O(matches+history) was achievable. New helpers `forEachNodeCandidateID` / `forEachRelCandidateID` (typed callbacks `func(types.NodeID) error` / `func(types.RelID) error`) capture the merge.
- **`AllNodes` / `AllRelationships` history-aware temporal opts** (`pkg/graph/graph.go`): when the caller passes a temporal `QueryOpts` (`ValidAt` or `ValidStart` / `ValidEnd`), the union of current and history IDs is now resolved through `findNodeVersionForOpts` / `findRelVersionForOpts` instead of a current-only scan. Closes a hole in v3.1.7's history-aware sweep.
- **`hasTemporalFilter` rejects one-sided `ValidStart` / `ValidEnd`** (`pkg/graph/temporal_filter.go`): half-set ranges (only `ValidStart` set, or only `ValidEnd` set) used to be treated as "no filter"; now correctly classified as a temporal filter so the history-aware path runs.
- **Combined property+temporal queries seeded from the property index** (`pkg/graph/temporal.go`): `NodesByLabelPropertyAndTime` and `NodesByLabelPropertyDuring` now seed candidates from the property index when one exists, instead of scanning every entity with the label.
- **`BatchBuilder.AddRelationship` enforces `AllowSelfLoops`** (`pkg/graph/batch.go`): the batch path used to bypass the `ValidationLimits.AllowSelfLoops` check that the non-batch `Graph.AddRelationship` enforces. Self-loops in batches with `AllowSelfLoops: false` now fail with `ErrSelfLoop` at validation time.
- **Batch metadata stamped at execute time, not queue time** (`pkg/graph/batch.go`): `TxFrom` / `CreatedAt` / `UpdatedAt` are now set when `Execute()` runs, not when the operation is queued. Queue-time stamping let `Execute()` see entities with `TxFrom > now` if the queue was held open across the boundary.
- **Batch rels with failed-create endpoints are skipped with diagnostics** (`pkg/graph/batch.go`): if `AddNode` failed earlier in the batch, subsequent `AddRelationship` calls referencing that node used to silently produce orphans. Now a `BatchError` is recorded with the offending rel and the failed endpoint's name.
- **Endpoint integrity hashes captured under endpoint lock** (`pkg/graph/batch.go`): `RelIntegrity.FromNodeHash` / `ToNodeHash` are now refreshed via `GetNode` while holding the endpoint locks, mirroring the non-batch path. Pre-fix code captured the hash before the lock and could miss a concurrent label/property mutation.
- **Cross-shard rel rollback on batch failure rolls back `TxFrom`** (`pkg/graph/tieredstore_write.go`): the rollback path that restores the entity+out write also restores the original `TxFrom` so a re-read sees the rel's original transaction window, not the half-applied batch's. Open-end resolution is hoisted out of the inner loop so it computes once per operation.
- **`hasTemporalFilter` open-end resolution hoisted** (`pkg/graph/temporal.go`): `findNodeVersionMatchingDuring` / `findRelVersionMatchingDuring` now resolve `end == 0` (open-ended interval) once at function entry instead of per-iteration. Documented inline.
- **Empty tx update kept inside store boundary** (`pkg/graph/tx.go`): a tx with no actual mutations no longer leaks to event publish — the empty-update guard now lives at the store boundary, not above it.

### Tests Added (MR !5)

- **`pkg/graph/findings_extra_regression_test.go`**: 14 regression tests paired 1:1 with the fixes above. Multi-entity scenarios covering history-aware candidate planning across all 7 entry points; one-sided ValidStart/ValidEnd; combined property+temporal queries; batch self-loop rejection; batch metadata stamping; failed-endpoint diagnostics; cross-shard rel rollback under failure injection; empty-tx-update boundary; open-end resolution hoist correctness.

## [3.1.9] - 2026-05-05

### Added (out-of-tree extension points — MR !1)

- **`graph.IndexProvider` interface** (`pkg/graph/index_provider.go`): plugin contract for auxiliary indexes that live outside Store's built-in index types (property, temporal, high-frequency, vector). Providers register on the Graph, receive lifecycle events through the existing `EventBus`, and own their persistence and query routing. Designed for tkgd's spatial R-tree.
  - `Graph.RegisterIndexProvider(p IndexProvider) error` — auto-creates a synchronous `EventBus` if none is attached; rejects `AsyncEventBus` (providers need the sync `Subscribe` API).
  - `Graph.UnregisterIndexProvider(name string) error` — detaches and calls `Close`.
  - `Graph.IndexProviders() []string` — lexicographic list for admin / snapshot tests.
  - `Graph.Close()` closes all registered providers before the store; errors joined.
  - 3 new sentinel errors: `ErrIndexProviderExists`, `ErrIndexProviderNotFound`, `ErrIndexProviderEmptyName`.
- **`types.HashableValue` interface** (`pkg/types/property_registry.go`): contract that lets external packages register custom property struct types whose values participate in node/relationship integrity hashing. Values that implement `HashableValue` and whose type is registered via `RegisterPropertyStructType` are accepted by `PropertySlice.Set` (previously rejected as unsupported).
  - `types.RegisterPropertyStructType(v any)` — declares that values of `v`'s type (and pointer-to-that-type) are valid property values. Idempotent. Both value and pointer forms accepted.
  - `types.RegisteredPropertyStructTypes() []string` — admin / diagnostic listing.
  - `pkg/graph/integrity.go appendPropertyValue` now dispatches custom types via `HashableValue.HashBytes()` instead of panicking.
  - **HashableValue is treated as a wire format** — output bytes feed the hash chain; once written, you cannot change the encoding without breaking every existing chain that contains the value. Doc comment in `property_registry.go` spells out the determinism / stability requirements.

### Fixed (MR !1)

- **TOCTOU race in `Graph.RegisterIndexProvider`** (`pkg/graph/index_provider.go`): the original implementation unlocked `g.mu` between the duplicate-name check and the entry insertion, allowing concurrent goroutines registering the same `Name()` to all pass the dup check, all subscribe to the bus, and overwrite each other's map entries — leaving N-1 orphaned subscriptions whose unsubscribe closures were lost. Fixed by holding `g.mu` through the entire critical section (dup check → auto-bus creation → type assertion → `Subscribe` → entry insertion). `EventBus.Subscribe` is non-reentrant w.r.t. graph mutations, so holding the lock through it is deadlock-safe.
- **Nil property values in hash computation** (`pkg/graph/integrity.go`): `appendPropertyValue` previously panicked in its default switch arm when called with `v == nil`. Common case from loaders that map SQL NULL to Go nil. Now nil hashes to its type tag alone (deterministic, stable).

### Changed (MR !1)

- **`PropertySlice.Set` accepts registered struct/pointer types** (`pkg/types/propertyslice.go`): `reflect.Ptr` and `reflect.Struct` previously rejected wholesale; now accepted when the type has been registered via `RegisterPropertyStructType`. Backwards-compatible: unregistered structs still rejected with `ErrUnsupportedValueType`.
- **`graph.IndexProvider.OnEvent` doc comment** clarifies that the `Event.EntityID` is `types.EntityID` and lookups should go through `g.GetNode(types.NodeID(ev.EntityID))` or `g.GetRelationship(types.RelID(ev.EntityID))` (corrects a stale doc reference to non-existent `g.Node` / `g.Relationship` methods).

### Tests Added (MR !1)

- **IndexProvider regression suite** (`pkg/graph/index_provider_test.go`): 12 tests covering Register/Unregister, duplicate/empty/nil name rejection, event fan-out, auto-bus-creation, Close propagation from `Graph.Close`, error joining, async-bus incompatibility, and a concurrent-registration race-safety test that pre-fix code would have failed (50 goroutines register the same `Name()`; exactly 1 succeeds, exactly 1 receives events).
- **Property-registry suite** (`pkg/types/property_registry_test.go`): 7 tests covering value/pointer registration, registering pointer also accepts value, unregistered rejection, nil-pointer rejection, idempotent re-registration, lexicographic listing.

### Documentation (post-v3.1.8 polish)

- **`pkg/types/temporal.go`**: explicit `EntityID` zero-value semantics — `0` is the universal sentinel for "unset" across `Event.EntityID`, `BatchError.ID`, `QueryOpts.After`, and `TemporalMetadata.baseEntityID`. Go's untyped-constant rule keeps `if id == 0` and `if opts.After != 0` working unchanged.
- **`CLAUDE.md` TieredStore section**: added "Primary-label class is immutable" rule documenting that the `ErrPrimaryLabelClassMutation` guard is enforced at the `TieredStore` Store-impl boundary only — `MemoryStore` and `BadgerStore` are single-shard and don't care; if you add another sharded backend, replicate the guard there.
- **`tasks/lessons.md`**: renumbered the duplicate `B26 Performance Tests Need Production Shape` to `B34` (collision with `B26 Lock Acquisition Without Defer Leaks on Panic`).
- **`pkg/graph/store_contract_test.go`**: dropped the `nodeIDsToSnowflake` / `relIDsToSnowflake` adapter functions left over from MR !4's typed-ID merge. Replaced the assertion helpers with Go-generic versions (`assertIDSet[T orderedID]` / `assertIDSetPreserveOrder[T orderedID]`) so callers can pass `[]types.NodeID`, `[]types.RelID`, `[]types.EntityID`, or `[]snowflake.ID` directly.

## [3.1.8] - 2026-05-05

### Changed (typed entity IDs)

This release pushes typed entity wrappers (`types.NodeID`, `types.RelID`, `types.EntityID`) through every public method signature, struct field, and internal storage map in `pkg/graph`. The wrappers were already public-shaped via the `SnowflakeID()` accessor; this release exports them and makes them the lingua franca of the package.

**Architecture invariant**: only `keys.go` (binary key encoding), `wire.go` (msgpack on-disk format), the `snowflake.Node` library boundary, the LRU cache (`pkg/graph/lru.go`, type-agnostic infrastructure), and a small set of deliberately type-agnostic surfaces (`entityLockManager`, `collectDeleteIDs`, `sameIDSet`, `Graph.DecomposeID`) see raw `snowflake.ID`. Everything else flows typed.

#### Public type surface

- **Exported entity ID wrappers** (`pkg/types/node.go`, `pkg/types/relationship.go`, `pkg/types/temporal.go`): `nodeID` → `NodeID`, `relID` → `RelID`, `entityID` → `EntityID`. The wrappers and their `SnowflakeID()` accessor were already public-shaped — only the type names became exported.
- **`ID()` accessors** (`pkg/types/node.go`, `pkg/types/relationship.go`): `func (n *Node) ID() NodeID` and `func (r *Relationship) ID() RelID`. `InternalID()` retained as a deprecated alias (scheduled for removal in this release).
- **`TemporalMetadata.SetBaseEntityID`** (`pkg/types/temporal.go`): now takes `types.EntityID` instead of `snowflake.ID`. Symmetric with the existing `BaseEntityID() EntityID` getter.

#### Public method signatures

- **`Graph` methods** (`pkg/graph/graph.go`, `pkg/graph/context.go`, `pkg/graph/temporal.go`, `pkg/graph/txtime.go`, `pkg/graph/integrity.go`, `pkg/graph/tx.go`, `pkg/graph/batch.go`): 56+ methods now take `types.NodeID` / `types.RelID` parameters and return typed values. ~950 `n.InternalID().SnowflakeID()` patterns at callsites collapse to `n.ID()`.
- **`Store` interface** (`pkg/graph/store.go`): all 35+ methods now use typed IDs. `MemoryStore`, `BadgerStore`, and `TieredStore` implementations updated.
- **`GraphTx` and `BatchBuilder`**: typed mirrors of the `Graph` API.

#### Public struct fields (Tier A — public-API leaks closed)

- **`Event.EntityID`** (`pkg/graph/events.go`): `snowflake.ID` → `types.EntityID`. ~30 `.SnowflakeID()` unwraps dropped at event-publication sites in `tx.go`, `batch.go`, `context.go`, `graph.go`.
- **`BatchError.ID`** (`pkg/graph/batch.go`): `snowflake.ID` → `types.EntityID`. 6 producer sites updated; `snowflake` import dropped from `batch.go` and `batch_test.go`.
- **`QueryOpts.After`** (`pkg/graph/store.go`): `snowflake.ID` → `types.EntityID`. The 5 paginate helpers (`paginateIDs`, `paginateNodes`, `paginateRels`, `paginateNodeIDs`, `paginateRelIDs`) also take typed cursors now; each extracts `afterRaw := after.SnowflakeID()` once at the top.

#### Internal helpers (Tier C — chokepoint consolidation)

- **`highFrequencyIndex`** (`pkg/graph/hf_index.go`): bucket storage + `add`/`remove`/`pointQuery`/`rangeQuery` typed.
- **`Graph.forEachKnownNodeID/RelID`** (`pkg/graph/temporal.go`): callback parameter types migrated; 11 caller closures + 6 in-scope helpers updated.
- **`TieredStore.shardForNodeID/RelID/Checked`** (`pkg/graph/tieredstore.go`): ~43 callers across 5 tieredstore files updated.
- **`Graph.publishEvent`** (`pkg/graph/graph.go`): parameter type migrated; 18 callsites updated as part of the `Event.EntityID` change.
- **BadgerStore unexported helpers** (`pkg/graph/badgerstore.go`): `prefetchNode`, `getNodeLocked`, `getRelLocked`, `cascadeDeleteInner`, `cascadeDeleteLocked`, `filterNodeIDsByTemporalPeek`, `filterRelIDsByTemporalPeek`, `fetchNodesWithTemporalFilter`, `fetchRelsWithTemporalFilter` all take typed parameters.
- **MemoryStore storage maps** (`pkg/graph/memorystore.go`): 8 maps (`nodes`, `rels`, `labelIdx`, `typeIdx`, `outIdx`, `inIdx`, `nodeHistory`, `relHistory`) keyed by typed IDs. ~25 top-of-method `id := nid.SnowflakeID()` shims dropped entirely. 5 internal helpers retyped (`deleteRelLocked`, `filterNodeIDsByTemporal`, `filterRelIDsByTemporal`, `sortNodesByID`, `sortRelsByID`). `.SnowflakeID()` count: 46 → 17 (-63%).
- **BadgerStore storage maps** (`pkg/graph/badgerstore.go`): 6 maps (`nodeIDs`, `relIDs`, `labelIdx`, `typeIdx`, `outIdx`, `inIdx`) keyed by typed IDs. Map type-safety prevents accidental cross-kind lookups (compiler now catches `bs.nodeIDs[someRelID]` mistakes).
- **Pagination helpers** (`pkg/graph/pagination.go`): added `paginateNodeIDs`, `paginateRelIDs`, `toNodeIDs`, `toRelIDs` typed equivalents alongside the existing raw `paginateIDs`.

#### Audit snapshot post-migration

- 207 raw `snowflake.ID` references remain in production code (down from ~600+ pre-migration); all at deliberate Tier D boundaries (LRU cache, `keys.go`, `wire.go` format, `snowflake.Node` library calls, type-agnostic helpers).
- 17 references in chokepoint files (`keys.go` + `wire.go`) — all justified.
- Public API surface is fully typed; no exported field, parameter, or return type uses raw `snowflake.ID` except `Graph.DecomposeID` / `DecomposeID` (deliberately type-agnostic — accepts either node or relationship IDs).

#### Migration notes for downstream consumers

`engram` is the only known consumer (currently pinned at `v3.1.1`). Migration steps when bumping:

1. `n.InternalID().SnowflakeID()` → `n.ID().SnowflakeID()` (still works during the alias window) or just `n.ID()` if passing into a Graph method that has already been migrated.
2. Anywhere a raw `snowflake.ID` was passed to `g.GetNode`, `g.AddNodeLabel`, `g.DeleteNode`, etc.: wrap as `types.NodeID(id)` — or, preferred, switch the variable's type to `types.NodeID` at its declaration.
3. Same pattern for relationships with `types.RelID`.
4. `EntityID` is now public if you store base-entity references from `TemporalMetadata.BaseEntityID()`.
5. `Event.EntityID`, `BatchError.ID`, `QueryOpts.After` are now `types.EntityID` — wrap raw IDs at the construction site or pass typed values directly.
6. `TemporalMetadata.SetBaseEntityID` now takes `types.EntityID`; wrap with `types.EntityID(rawID)` at callsites.

### Documentation (Tier D chokepoint invariant)

- **`pkg/graph/keys.go`**: package-doc-style preamble stating the chokepoint invariant — only this file (binary key encoding), `wire.go` (msgpack on-disk format), `lru.go` (type-agnostic LRU infrastructure), `entity_locks.go` (type-agnostic lock pool), and direct `snowflake.Node` library calls legitimately consume raw `snowflake.ID`. Everything else flows typed.
- **`pkg/graph/wire.go`**: explicit doc explaining why `nodeWire.ID int64` / `relWire.ID int64` cannot become typed — these are the on-disk msgpack format, and changing the field type would break unmarshalling of every existing Badger db file. Graph layer wraps these int64 values into typed IDs at the deserialization boundary (`wireToNode` / `wireToRel`).
- **`pkg/graph/entity_locks.go`**: doc on `entityLockManager` explaining its type-agnostic design — the same 256-shard pool serves both node and rel IDs by hashing the snowflake bits. A typed wrapper would imply distinct lock domains; they aren't.

### Fixed (TieredStore cross-shard hardening — MR !4)

- **TieredStore cross-shard rel reachable after start shard cold demotion** (`pkg/graph/tieredstore.go`, `pkg/graph/tieredstore_read.go`): `shardForRelID` and `shardForRelIDChecked` now probe cold event shards in addition to hot/warm during fallback resolution. Previous "skip cold shards" fast-path silently lost a live cross-shard rel once its start-node shard aged warm→cold — the rel's entity stayed on the original shard but the lookup excluded it. `GetRelationship`, `IncomingRelationships`, and `IncomingRelationshipsForNodes` now resolve via `shardForRelIDChecked` so the cold shard remains pinned for the duration of the read.
- **TieredStore empty-history fan-out no longer wakes cold shards** (`pkg/graph/tieredstore_read.go`, `pkg/graph/tieredstore_write.go`): `GetNodeHistory`, `GetRelHistory`, `GetNodeVersion`, `GetRelVersion`, `TruncateNodeHistory`, and `TruncateRelHistory` skip the cross-shard fan-out when the live entity is present on its home shard. The fan-out is only needed to recover history of a deleted entity whose home shard no longer holds the live index — for an alive entity with no history, the empty result is authoritative locally. Exception: when the home shard is `refArchive`, the fan-out still runs because `ArchiveNode` only migrates the live entity and pre-archive history versions remain on `refShard`.
- **TieredStore checkout pinning on rel write paths and node-keyed adjacency reads** (`pkg/graph/tieredstore_read.go`, `pkg/graph/tieredstore_write.go`): `ReplaceRelationship`, `DeleteRelationship`, `DeleteRelWithHistory`, `OutgoingRelationships`, `OutgoingRelationshipsForNodes`, `IncomingRelationships`, and `IncomingRelationshipsForNodes` now resolve their owner shards via `shardForRelIDChecked`/`shardForNodeIDChecked` and `defer checkin()`. Previously they used the unchecked variants and dereferenced the returned `*BadgerStore` pointer — once the rel-fallback probe started opening cold shards, that pointer could be closed by `closeIdleShards` mid-operation.
- **TieredStore checkout pinning on remaining write paths** (`pkg/graph/tieredstore_write.go`): `PutRelationship`, `PutRelationshipsBatch`, `DeleteNodesBatch`, `DeleteNodeCascade`, `DeleteNodeWithHistory`, `ReplaceNodeWithHistory`, `ReplaceRelWithHistory`, `PutNodeVersion`, and `PutRelVersion` now also resolve their owner shards via the checked resolvers and pin the cold owner for the duration of the write.
- **TieredStore relationship resolver probes refArchive** (`pkg/graph/tieredstore.go`): `shardForRelID` and `shardForRelIDChecked` now check `refArchive` (between `refShard` and the timestamp candidate) so any rel that ends up on the archive — e.g. when both endpoints get archived together by `ArchiveNode` — remains reachable through every public read/write path. Mirrors the equivalent `shardForNodeID(Checked)` archive probe.
- **`ReplaceRelWithHistory` routes by relationship ID, not start-node ID** (`pkg/graph/tieredstore_write.go`): every other rel-write path resolves the owner via `shardForRelIDChecked(rel ID)`; this method previously used `shardForNodeIDChecked(StartNodeID)`. For rels whose entity has been migrated independently of the start node the start-node-keyed lookup picks the wrong shard and skips the `refArchive` probe.
- **`DeleteRelationship` cross-shard rollback parity** (`pkg/graph/tieredstore_write.go`): the cross-shard delete previously deleted the entity+out from the entity shard then attempted the in/ delete on the end-node shard with no rollback — a failure of the second leg would leave a phantom incoming-index entry. Now mirrors the rollback already in place on the create path: on second-leg failure the entity+out write is restored via `putRelEntityAndOut(r)`.
- **`ensureRefArchive` ↔ `Close` race** (`pkg/graph/tieredstore.go`, new `ErrStoreClosed` sentinel): a reader that observed `refArchive==nil` could lazy-open a fresh archive after `Close` had already closed the store, leaking the freshly opened DB handle. `Close` now sets a `closed atomic.Bool` under `archiveMu` before tearing the archive down; `ensureRefArchive` consults the flag (also under `archiveMu`) and returns `ErrStoreClosed` instead of opening.
- **TieredStore deleted entity history routing** (`pkg/graph/tieredstore.go`, `pkg/graph/tieredstore_read.go`, `pkg/graph/tieredstore_write.go`): node and relationship history reads/truncation now fall back to probing history-owning shards when live indexes no longer identify the owner after delete. Node history writes route reference snapshots to the reference shard; relationship history writes route by the relationship start-node shard, matching cross-shard entity ownership. This restores parity with `MemoryStore` and `BadgerStore` for tombstones after deleting reference nodes and `Case → Signal` relationships.
- **TieredStore relationship history cold-shard race** (`pkg/graph/tieredstore.go`, `pkg/graph/tieredstore_read.go`, `pkg/graph/tieredstore_write.go`): added `shardForRelIDChecked` paralleling `shardForNodeIDChecked` — increments `activeReqs` on event shards so `closeIdleShards` cannot close the DB while a relationship history read is in flight. `GetRelVersion`, `GetRelHistory`, and `TruncateRelHistory` now use the checked variant.
- **TieredStore primary-label class invariant** (`pkg/graph/tieredstore_write.go`, new `ErrPrimaryLabelClassMutation`): `AddNodeLabelToken{,WithHistory}` and `RemoveNodeLabelToken{,WithHistory}` now reject mutations that would change the primary label's ontology class (reference ↔ event). Such mutations would leave the live entity on its original shard while subsequent history snapshots routed to a different shard, fragmenting the version chain.
- **TieredStore `refArchive` data race** (`pkg/graph/tieredstore.go`, `pkg/graph/tieredstore_read.go`, `pkg/graph/tieredstore_write.go`, `pkg/graph/tieredstore_admin.go`): `TieredStore.refArchive` is now `atomic.Pointer[BadgerStore]`. Concurrent reads from `shardForNodeID`, `shardForNodeIDChecked`, the `ForEach*ID` iterators, and admin helpers no longer race with `ensureRefArchive`'s lazy-open write. `archiveMu` is retained as a single-flight guard for the open operation.
- **`forEachHistoryShard` cold-shard probe** restored to `DepthAll` from a previous narrowing to `DepthWarm` that would have lost history of cross-shard rels whose start-node shard aged to cold post-rotation.

### Tests Added (TieredStore hardening)

- **Shared store contract suite** (`pkg/graph/store_contract_test.go`): reusable behaviour tests run against `MemoryStore`, `BadgerStore`, and `TieredStore` for current visibility, version history visibility, delete tombstones/history IDs, cursor pagination, temporal filters, synchronous events through the public `Graph` API, and graph stats/cache metrics where supported.
- **TieredStore history-routing regression suite** (`pkg/graph/tieredstore_history_routing_test.go`): direct coverage for `shardForRelIDChecked` paths, primary-label-class rejection, deleted ref-node history, deleted cross-shard rel history, no-op truncate, post-rotation rel on cold start-node shard, public read paths after cold demotion (`GetRelationship`, `OutgoingRelationships`, `IncomingRelationships`, `IncomingRelationshipsForNodes`), empty-history lookups not lazy-opening cold shards, archived-node history survival, cross-shard delete checkout pinning, and update/delete after start-shard cold demotion.

### Fixed

- **TieredStore cross-shard relationship rollback** (`pkg/graph/tieredstore_write.go`): `PutRelationship` and `DeleteRelationship` now reverse the partial cross-shard write when the second step fails. Previously a duplicate `PutRelationship` (or `deleteRelIncoming` failure on delete) could leave orphaned `in/` entries on the end-node shard or an out-of-sync entity/out side. Closes the B7 partial-cross-shard-write window for the create and delete paths.
- **Batch creation shadow-key handling** (`pkg/graph/batch.go`): `BatchBuilder.AddNode` and `BatchBuilder.AddRelationship` now extract `tkg_author_id`, `tkg_signature`, `tkg_authorized_by`, `tkg_auth_level` (provenance) and populate `Integrity` accordingly, mirroring `AddNodeWithContext`/`AddRelationshipWithContext`. `BatchBuilder.AddRelationship` also records `FromNodeHash`/`ToNodeHash` matching the standalone path. Batch-created entities now carry `TxFrom` on `TemporalMetadata`. Previously batch creation rejected provenance shadow keys with an `ErrReservedPrefix` validation error, never set `TxFrom`, and dropped the endpoint hashes.
- **Batch metadata stamped at commit time, not queue time** (`pkg/graph/batch.go`): `TxFrom` for both batch-created nodes and relationships, and `FromNodeHash`/`ToNodeHash` for batch-created relationships, are now populated inside `Execute()` rather than at queue time. `TxFrom` reflects when the batch actually commits — a builder assembled at T0 and executed minutes later records the execute-time clock, not T0. Endpoint hashes are re-read from the live store under the per-rel endpoint locks, so an `UpdateNode` that fires between `AddRelationship` and `Execute` is reflected. Without this, a queued rel could record stale endpoint hashes that never matched the committed endpoint state.
- **Batch rels short-circuit on failed node dependencies** (`pkg/graph/batch.go`): when `PutNodesBatch` fails (all-or-nothing), step 2 now skips relationships referencing those failed nodes and reports a `"skipped — start/end node N failed to create in this batch"` diagnostic instead of letting the rel write surface a generic `"node not found"` that hides the real cause. Self-loops use a single `GetNode` for endpoint-hash refresh instead of two.
- **`GraphTx.UpdateNode`/`UpdateRelationship` empty-update boundary** (`pkg/graph/tx.go`): the empty-update fast path now reads via `tx.g.store.GetNode`/`GetRelationship` instead of the exported `*WithContext` wrappers. The tx already holds `g.mu.Lock()`, so any future `g.mu.RLock()` on the exported wrappers would deadlock the tx — keeping the tx/internal boundary clean preserves the convention that tx code never crosses through exported entry points.
- **`AllNodes`/`AllRelationships` history-aware temporal opts** (`pkg/graph/graph.go`): when the caller passes a temporal `QueryOpts` (`ValidAt` or `ValidStart`/`ValidEnd`), the union of current and history IDs is now resolved through `findNodeVersionForOpts`/`findRelVersionForOpts` so deleted entities that were valid at the query time appear in the result. Non-temporal calls retain the existing fast store-pushdown path.
- **History-aware indexed query candidate planning** (`pkg/graph/graph.go`, `pkg/graph/temporal.go`): `NodesByLabel`, `NodesByLabelAndProperty`, `RelationshipsByType`, `GetNodesByLabelValidAt`, `NodesByLabelPropertyAndTime`, `NodesByLabelPropertyDuring`, and `GetNeighborsValidAt` now derive their candidate set from the appropriate index (label / property / type / adjacency) and merge it with history IDs, instead of running `Store.ForEachNodeID`/`ForEachRelID` over every entity. New helpers `forEachNodeCandidateID` and `forEachRelCandidateID` capture the merge pattern. The combined label+property paths (`NodesByLabelAndProperty`, `NodesByLabelPropertyAndTime`, `NodesByLabelPropertyDuring`) seed candidates from `Store.NodesByLabelAndProperty` so an installed property index narrows the current set before history merge — previously they did a label-wide scan and filtered the property in Go.

### Tests Added

- **Un-fixed regression coverage** (`pkg/graph/findings_extra_regression_test.go`): rebased subset of the original history-aware regression test suite, kept only for bugs that were not yet fixed on `main` after MR !2 / v3.1.7. Each of `TestHistoryAwareIndexedNodeQueries_DoNotScanAllCurrentIDs`, `TestHistoryAwareNeighborQuery_DoesNotScanAllCurrentRelIDs`, `TestTieredStore_PutRelationshipRollsBackIncomingOnEntityFailure`, `TestGenericAllTemporalOpts_UseHistoricalDeletedEntities`, and `TestBatchCreation_UsesSharedMetadataPreparation` is now paired with a focused fix in this MR. `TestHistoryAwarePropertyTemporalQueries_UsePropertyIndexCandidates` additionally guards the property-index pushdown on the combined label+property temporal paths — counts on `NodesByLabel` vs `NodesByLabelAndProperty` show the new code seeds from the tighter property index.

### Benchmarks Added

- **Graph performance baseline suite** (`pkg/graph/bench_baseline_test.go`, `pkg/graph/bench_production_test.go`, `Makefile`): added `BenchmarkGraphBaseline/...` coverage for memory-store reads/writes, temporal queries, batch and transaction operations, Badger async/sync writes, Badger indexed reads, and TieredStore reference/event/cross-shard writes. Added `BenchmarkGraphProduction/...` scenarios for public `Graph` APIs covering large graph reads, high-degree traversal, temporal and bitemporal queries, node and relationship history chains, public method surface checks, export/import, sync/async event buses, TieredStore multi-shard queries, and batch/transaction write shapes. Added small and large production profiles: `make bench-graph-production-small` keeps the routine 10K-node/30-version suite, while `make bench-graph-production-large` raises the stress profile to 100K nodes, 1M regular relationships, a 10K-degree hub, 3,000-version node and relationship history chains, larger export/import, TieredStore, batch, and public-surface fixtures. `make bench-graph-production` remains an alias for the small profile; `make bench-graph-all` and `make bench-graph-all-large` run the baseline with the respective production profile for `benchstat` comparisons against `main`.

## [3.1.7] - 2026-05-05

### Fixed

- **History-aware graph semantics** (`pkg/graph/integrity.go`, `pkg/graph/graph.go`, `pkg/graph/temporal.go`, `pkg/graph/context.go`, `pkg/graph/tx.go`):
  - `VerifyNodeHashChain` now recomputes each node version with that version's own labels instead of reusing the current tip's labels, so hash-chain verification remains valid after label add/remove mutations.
  - `AddNodeLabel` / `RemoveNodeLabel` now set `TxTo` on the previous version and `TxFrom` on the new version, and compute the new hash after the version bump.
  - `GetNodesByLabelValidAt`, `NodesByLabelPropertyAndTime`, `NodesByLabelPropertyDuring`, and `GetNeighborsValidAt` now resolve historical versions instead of relying only on current label/property/adjacency indexes.
  - `NodesByLabel`, `NodesByLabelAndProperty`, and `RelationshipsByType` are now history-aware when called with a temporal `QueryOpts` (`ValidAt` or `ValidStart`/`ValidEnd`). Previously these generic entry points routed temporal queries through store-side pushdown that consults only current indexes, so a label/type membership that held at `t` but no longer holds was invisible. Non-temporal calls keep the existing fast pushdown path.
  - No-op mutations no longer publish update events for idempotent label adds, empty property updates, empty in-place updates, or successful no-op compare-and-delete operations.
  - `ImportNodeWithID` and `ImportRelationshipWithID` now extract temporal/provenance shadow properties, populate transaction time, increment stats, and publish create events consistently with generated-ID creation paths.

### Changed

- **Documentation metadata alignment** (`README.md`, `AGENTS.md`, `docs/architecture.md`, `docs/api.md`): updated documented Go/version metadata to match `go.mod` and the latest changelog entry, and corrected combined temporal query docs to describe history-aware behavior.
- **Behavior change** (`pkg/graph/graph.go`): `NodesByLabel(label, opts)`, `NodesByLabelAndProperty(label, key, value, opts)`, and `RelationshipsByType(typeName, opts)` now scan history when called with a temporal `QueryOpts` (`ValidAt` and/or `ValidStart`/`ValidEnd`). Callers who relied on the previous (incorrect) current-only behavior will see different results: nodes/rels that matched the predicate at the requested time but no longer do are now included, and entities that match now but did not match at the requested time are excluded. Non-temporal calls retain the existing fast pushdown path. The during-interval semantic is "predicate held on any version overlapping [start, end)" — implementations that need only the most-recent version overlapping should call `getNodeVersionDuring`/`getRelVersionDuring` directly.

### Tests Added

- `TestVerifyNodeHashChain_LabelMutations` — regression coverage for hash-chain verification after label add/remove. Three sub-tests: a 3-distinct-label-set chain (every version is a witness), a discriminating history tamper that pre-fix code would have accepted but per-entry-label code rejects, and a deleted-entity path that exercises the `chain[len(chain)-1]` fallback.
- `TestNodeHashChain_InspectsHashValues` — probes actual hash bytes (not just the boolean from `VerifyNodeHashChain`) and walks the persisted chain to verify per-version Hash/PrevHash linkage independently.
- `TestGetNodesByLabelValidAt_UsesHistoricalLabelVersion` — verifies label point-in-time queries use historical label sets.
- `TestNodesByLabelPropertyTemporalQueries_UseHistoricalPropertyVersion` — verifies combined label/property temporal queries use historical property values.
- `TestGetNeighborsValidAt_UsesHistoricalRelationships` — verifies temporal neighbor traversal sees deleted historical relationships.
- `TestLabelMutations_UpdateTransactionTimeBounds` — verifies label mutations update bitemporal transaction bounds.
- `TestNoOpMutations_DoNotPublishUpdateEvents` — verifies no-op mutation paths do not publish update events.
- `TestImportNodeWithID_MatchesAddNodeMetadataEventsAndStats` / `TestImportRelationshipWithID_MatchesAddRelationshipMetadataEventsAndStats` — verifies import-by-ID public methods match creation semantics for metadata, stats, and events.
- `TestGraphTx_PropertyConvenienceMethods`, store-level label-add tests, `TestDocsMetadataMatchesSourceOfTruth`, and `TestRecurrence_Monthly_LastDay` — close direct coverage gaps and prevent docs/version drift.
- `TestNodesByLabel_TemporalOpts_Adversarial`, `TestNodesByLabelAndProperty_TemporalOpts_Adversarial`, `TestRelationshipsByType_TemporalOpts_Adversarial` — verify the generic `*By*(opts QueryOpts)` entry points are history-aware when temporal filters are set. Multi-entity scenarios with diverging lifecycles, exact-set assertions (`assertNodeSet`/`assertRelSet`) catching over-reporting and omission, the decisive "predicate-anywhere-in-interval" case (a node whose label held during part of the interval but not on the most-recent version), and pagination on the temporal path.

## [3.1.6] - 2026-04-10

### Added

- **Node label mutation after creation** (`pkg/types/node.go`, `pkg/graph/graph.go`, `pkg/graph/store.go`, `pkg/graph/memorystore.go`, `pkg/graph/badgerstore.go`, `pkg/graph/tieredstore_write.go`, `pkg/graph/tx.go`):
  - `Graph.AddNodeLabel(id snowflake.ID, label string) error` — mirror of `RemoveNodeLabel`. Validates label name length, enforces `MaxLabelsPerNode`, advances the hash chain, writes a version history entry, updates the label index, and publishes `EventNodeUpdate`. Idempotent: a no-op (no version bump, no history) when the node already has the label. Returns `ErrNodeNotFound` if the node does not exist, `ErrTooManyLabels` if adding would exceed the configured maximum, and `ErrNameTooLong` if the label name exceeds `MaxNameLength`.
  - `GraphTx.AddNodeLabel` / `GraphTx.RemoveNodeLabel` — transactional wrappers that snapshot the node for rollback, call the lock-free internal implementations under `g.mu.Lock`, and track label deltas so `Rollback()` can restore label-mutated rows through Store label-token helpers.
  - `types.Node.AddLabelTokenRaw(tok uint16) bool` — counterpart to `RemoveLabelTokenRaw`. Appends `tok` as an extra label; returns `false` if `tok == 0` or already present.
  - `Store.AddNodeLabelTokenWithHistory(id, tok, updatedNode, prevVersion, prevState)` — atomic label-add + history + persist, mirroring `RemoveNodeLabelTokenWithHistory`. Implemented in `MemoryStore`, `BadgerStore`, `TieredStore`.
  - `Store.AddNodeLabelToken(id, tok, updatedNode)` — non-history variant used by `GraphTx.Rollback` to reverse label deltas without polluting version history.

### Fixed

- **Transaction rollback label index consistency** (`pkg/graph/tx.go`): `ReplaceNode` deliberately leaves the label index alone (labels are immutable on that path), so a rollback after `GraphTx.AddNodeLabel` / `RemoveNodeLabel` must restore label-mutated rows through `Store.AddNodeLabelToken` / `RemoveNodeLabelToken`. `NodesByLabel` queries could otherwise return a node that no longer had the label (or miss one that did). `GraphTx` tracks label deltas separately and applies them while restoring the pre-transaction node snapshot. Exposed by two regression tests before the fix.

### Tests Added

- `TestAddNodeLabel_AddsExtraLabel` — basic add path
- `TestAddNodeLabel_IdempotentIfAlreadyPresent` — no version bump when label already present
- `TestAddNodeLabel_EmptyNameRejected` — empty label rejected
- `TestAddNodeLabel_NameTooLong` — `ErrNameTooLong` sentinel
- `TestAddNodeLabel_TooManyLabelsRejected` — `ErrTooManyLabels` sentinel when crossing `MaxLabelsPerNode`
- `TestAddNodeLabel_NodeNotFound` — `ErrNodeNotFound` for unknown ID
- `TestAddNodeLabel_HashChainAdvances` — new hash linked via `PrevHash` to previous hash
- `TestAddNodeLabel_WritesHistoryEntry` — pre-mutation snapshot written to history at version 0, current bumped to version 1
- `TestAddNodeLabel_NodesByLabelUpdated` — new label index entry visible via `NodesByLabel`
- `TestAddNodeLabel_PublishesEvent` — `EventNodeUpdate` published after commit
- `TestGraphTx_AddNodeLabel_Commit` — transactional commit persists
- `TestGraphTx_AddNodeLabel_Rollback` — rollback restores node state
- `TestGraphTx_AddNodeLabel_RollbackRestoresLabelIndex` — rollback also restores the label index (regression)
- `TestGraphTx_RemoveNodeLabel_RollbackRestoresLabelIndex` — remove-then-rollback restores the label index (regression)
- `TestGraphTx_AddNodeLabel_AfterCommitReturnsTxDone` — `ErrTxDone` after commit
- `TestGraphTx_RemoveNodeLabel_Commit` — transactional remove + commit
- `TestGraphTx_RemoveNodeLabel_Rollback` — rollback restores the removed label
- `TestGraphTx_RemoveNodeLabel_LastLabelError` — `ErrLastLabel` sentinel inside tx
- `TestGraphTx_RemoveNodeLabel_AfterRollbackReturnsTxDone` — `ErrTxDone` after rollback

### Benchmarks Added

- `BenchmarkAddNodeLabel` — ~1.2µs/op, 22 allocs/op (MemoryStore, Apple M4 Max) — parity with `BenchmarkRemoveNodeLabel`
- `BenchmarkAddNodeLabelIdempotent` — ~112ns/op, 4 allocs/op — idempotent fast path
- `BenchmarkRemoveNodeLabel` — ~1.2µs/op, 24 allocs/op

## [3.1.5] - 2026-04-02

### Added

- **Batch incoming adjacency query** (`pkg/graph/store.go`, `pkg/graph/memorystore.go`, `pkg/graph/badgerstore.go`, `pkg/graph/tieredstore_read.go`, `pkg/graph/graph.go`): `IncomingRelationshipsForNodes(nodeIDs, typeToken)` returns incoming relationships for multiple nodes in a single batched operation. Symmetric counterpart to `OutgoingRelationshipsForNodes`. BadgerStore leverages early type filtering from `inIdx` (stores relID -> typeToken). TieredStore handles cross-shard entity fetches (relIDs from node's shard, entities resolved via `shardForRelID`). Same return contract: `map[snowflake.ID][]*types.Relationship`, per-node sorted, absent for zero incoming.

### Tests Added

- `TestMemoryStoreIncomingRelationshipsForNodes` — basic, type filter, empty input, no-match
- `TestMemoryStoreIncomingForNodesDuplicateInput` — duplicate nodeIDs in input
- `TestMemoryStoreIncomingForNodesSorted` — per-node sort order
- `TestBadgerStoreIncomingForNodesAll` — all types, multiple nodes
- `TestBadgerStoreIncomingForNodesFiltered` — type filter
- `TestBadgerStoreIncomingForNodesEmpty` — nil and empty input
- `TestBadgerStoreIncomingForNodesSorted` — per-node sort order
- `TestBadgerStoreOutgoingForNodesCorruptionError` — corruption error propagation (outgoing)
- `TestBadgerStoreIncomingForNodesCorruptionError` — corruption error propagation (incoming)
- `TestBadgerStoreOutgoingForNodesOrphanSkipped` — index orphan silently skipped (outgoing)
- `TestBadgerStoreIncomingForNodesOrphanSkipped` — index orphan silently skipped (incoming)
- `TestBadgerStoreOutgoingForNodesNonexistentNode` — nonexistent node returns nil
- `TestBadgerStoreIncomingForNodesNonexistentNode` — nonexistent node returns nil
- `TestGraphIncomingRelationshipsForNodes` — Graph layer integration
- `TestGraphIncomingForNodesUnregisteredType` — unregistered type returns nil
- `TestTieredStore_IncomingRelationshipsForNodes` — cross-shard incoming, mixed ref+event nodes

## [3.1.4] - 2026-04-02

### Added

- **Batch outgoing adjacency query** (`pkg/graph/store.go`, `pkg/graph/memorystore.go`, `pkg/graph/badgerstore.go`, `pkg/graph/tieredstore_read.go`, `pkg/graph/graph.go`): `OutgoingRelationshipsForNodes(nodeIDs, typeToken)` returns outgoing relationships for multiple nodes in a single batched operation. Amortizes lock acquisition (one `idxMu.RLock` instead of N) and shard resolution (groups nodeIDs by shard in TieredStore). Returns `map[snowflake.ID][]*types.Relationship` — per-node slices sorted by ID; nodes with zero outgoing rels absent from map. Graph layer accepts `typeName string` with single token resolution.

### Tests Added

- `TestMemoryStoreOutgoingRelationshipsForNodes` — basic, type filter, empty input, no-match
- `TestMemoryStoreOutgoingForNodesPartialResults` — mixed nodes with/without rels
- `TestMemoryStoreOutgoingForNodesDuplicateInput` — duplicate nodeIDs in input
- `TestMemoryStoreOutgoingForNodesSorted` — per-node sort order
- `TestBadgerStoreOutgoingForNodesAll` — all types, multiple nodes
- `TestBadgerStoreOutgoingForNodesFiltered` — type filter
- `TestBadgerStoreOutgoingForNodesEmpty` — nil and empty input
- `TestBadgerStoreOutgoingForNodesSorted` — per-node sort order
- `TestGraphOutgoingRelationshipsForNodes` — Graph layer integration
- `TestGraphOutgoingForNodesUnregisteredType` — unregistered type returns nil
- `TestTieredStore_OutgoingRelationshipsForNodes` — cross-shard grouping, mixed ref+event nodes

## [3.1.3] - 2026-04-02

### Improved

- **Property index fallback observability** (`pkg/graph/memorystore.go`, `pkg/graph/badgerstore.go`): `NodesByLabelAndProperty` now emits a `slog.Debug` log when using the full label-scan fallback path instead of a property index. Helps operators identify missing property indexes that degrade query performance from O(1) to O(N). The hint includes `labelToken` and `propertyKey` for targeted `CreatePropertyIndex` calls.

## [3.1.2] - 2026-03-28

### Fixed

- **WAL corruption recovery for warm/cold shards** (`pkg/graph/tieredstore.go`): Shards opened in read-only mode (warm at startup, cold on lazy-open) now auto-recover from corrupt WAL files left by unclean shutdowns (SIGKILL, Ctrl-C, OOM kill). Previously, a partially written `.mem` file caused `ErrTruncateNeeded` — a fatal startup error requiring manual intervention. The new `openBadgerStoreWithRecovery` detects the truncation error, opens the shard read-write (Badger auto-truncates the corrupt WAL tail), closes it, and reopens read-only. At most one flush window (~100ms) of buffered writes is lost. Applied at all three read-only open sites (L1 pattern): warm shard startup, cold shard `getStore`, cold shard `checkoutStore`. Includes `isTruncateNeeded` fallback for Badger v4's broken `errors.Is` chain (`y.Wrap` uses `%+v` not `%w`).

### Tests Added

- `TestTieredStore_WarmShard_WALCorruptionRecovery` — warm shard with corrupt WAL recovers on startup, data survives, shard remains read-only
- `TestTieredStore_ColdShard_WALCorruptionRecovery` — cold shard with corrupt WAL recovers on lazy-open (L1 pattern)
- `TestTieredStore_WALCorruption_NonTruncateError` — real errors (permission denied) are not masked by recovery path
- `TestTieredStore_WALCorruption_DataIntegrity` — 10 nodes written before crash all survive truncation recovery
- `TestTieredStore_WALCorruption_ConcurrentColdAccess` — 50 concurrent goroutines on corrupt cold shard, no panics or deadlocks

## [3.1.1] - 2026-03-15

### Added

- **Compression configuration** (`pkg/graph/badgerstore.go`, `pkg/graph/graph.go`, `pkg/graph/tieredstore.go`): `BadgerStoreConfig.Compression` (`options.None`/`options.Snappy`/`options.ZSTD`) and `BadgerStoreConfig.ZSTDCompressionLevel` (1-15) control Badger SSTable compression. Zero values keep Badger defaults (Snappy, level 1). Threaded through `Graph.Config` (convenience BadgerStore path) and `TieredStoreConfig` (applied to all shards via `openBadgerStore`).

### Tests Added

- `TestCompression_BadgerStore_None` — store with compression disabled
- `TestCompression_BadgerStore_Snappy` — store with explicit Snappy
- `TestCompression_BadgerStore_ZSTD` — store with ZSTD level 3
- `TestCompression_BadgerStore_ZeroKeepsDefault` — zero value keeps Badger default
- `TestCompression_BadgerStore_InMemory` — compression with InMemory mode
- `TestCompression_Graph_ConfigPassthrough` — Config.Compression flows to BadgerStore
- `TestCompression_TieredStore_Passthrough` — TieredStoreConfig fields stored and passed through
- `TestCompression_ZSTD_DataSurvivesReopen` — ZSTD-compressed data persists across reopen

## [3.1.0] - 2026-03-14

### Changed

- **Key function signatures** (`pkg/graph/keys.go`): Refactored 10 key functions (`nodeKey`, `relKey`, `labelIndexKey`, `relTypeIndexKey`, `outKey`, `inKey`, `histNodeKey`, `histRelKey`, `histNodePrefix`, `histRelPrefix`) from `int64` to `snowflake.ID` for entity ID parameters. Parser functions (`parseIDFromKey`, `parseRelIDFromAdjKey`) now return `snowflake.ID`. Eliminates ~35 `int64(id)` conversions in `badgerstore.go`/`badgerstore_partial.go` and ~14 `snowflake.ID(parseIDFromKey(...))` wrappings. `putUint64` remains `int64` (generic binary helper). Wire format unchanged — same bytes on disk.
- **Test-only key helpers** (`pkg/graph/keys_helpers_test.go`): Same `int64` → `snowflake.ID` refactoring for 4 prefix functions, 2 temporal key functions, and 2 parser functions.

## [3.0.68] - 2026-03-14

### Added

- **CompareAndSetProperty / CompareAndSetPropertyWithContext** (`pkg/graph/graph.go`): Atomic compare-and-swap on a single node property for optimistic locking patterns. Returns `(true, nil)` on match+update, `(false, nil)` on mismatch, `(false, error)` on real error. `expected == nil` means "property must not exist"; `newVal == nil` means "delete the property". Value comparison uses exact-type property equality — type must match exactly (`int(42) != int64(42)`). Current releases also match accepted `NaN` property values. Follows the `UpdateNode` pattern: entity lock serialization, pre-mutation snapshot, version bump, temporal metadata, hash chain, `ReplaceNodeWithHistory`.

### Tests Added

- `TestCAS_Match` — match → update → verify persisted
- `TestCAS_Mismatch` — wrong expected → `(false, nil)`, unchanged
- `TestCAS_NilExpected_Absent` — absent prop + nil expected → sets value
- `TestCAS_NilExpected_Present` — existing prop + nil expected → `(false, nil)`
- `TestCAS_DeleteOnMatch` — `newVal=nil` → deletes property
- `TestCAS_NilBoth_Absent` — both nil, absent → `(true, nil)` no-op
- `TestCAS_ShadowKey` — `tkg_` key → error
- `TestCAS_NodeNotFound` — non-existent ID → `ErrNodeNotFound`
- `TestCAS_VersionBump` — successful CAS increments version
- `TestCAS_NoVersionBumpOnMismatch` — mismatch doesn't bump
- `TestCAS_History` — successful CAS adds history entry
- `TestCAS_TypeMismatch` — `int` vs `int64` → no swap
- `TestCAS_DeleteMismatch` — delete with wrong expected → no swap

### Changed

- Bump Go version to 1.26.1
- Bump rho-snowflake-2026 dependency from v1.3.0 to v1.3.2
- Bump rho-mclock dependency from v0.2.0 to v0.2.1
- Tighten TieredStore data directory permissions from 0755 to 0750

### Fixed

- Resolve all 46 gosec findings: annotate G115 integer conversions (integrity hashing, snowflake IDs, registry imports), G703 path traversal false positives, G304 file inclusion false positives, G404 weak RNG in tutorial, G301 directory permissions

## [3.0.67] - 2026-03-13

### Fixed

- **Cross-shard incoming relationship type filter** (`pkg/graph/badgerstore.go`, `pkg/graph/badgerstore_partial.go`): `IncomingRelationships(nodeID, typeToken)` returned empty results for cross-shard relationships in TieredStore. The in-memory `inIdx` stored relationship IDs as a bare set (`struct{}`). When filtering by type, `incomingRelIDs` called `GetRelationship(relID)` on its own shard to read the type token — but for cross-shard relationships (e.g., Signal in event shard → Case in reference shard), the entity lives in the event shard while the incoming index is on the reference shard. `GetRelationship` returned `ErrRelNotFound`, silently skipping the relationship. Changed `inIdx` from `map[snowflake.ID]struct{}` to `map[snowflake.ID]uint16` (relID → typeToken). The type token is already available at index write time — filtering now happens directly on the index without fetching the entity.

## [3.0.66] - 2026-03-12

### Added

- **GraphTx.GetNode(id)** (`pkg/graph/tx.go`): Read a node by snowflake ID within a transaction. Safe because the tx holds the write lock — no concurrent modifications possible. Used by callers that need to inspect node state mid-transaction.

- **GraphTx.AddRelationshipByID(typeName, startID, endID, props)** (`pkg/graph/tx.go`): Create a relationship using endpoint snowflake IDs within a transaction. Mirrors the standalone `Graph.AddRelationshipByID` but participates in tx rollback tracking. The relationship ID is tracked for rollback on `tx.Rollback()`.

- **GraphTx.AddRelationshipByIDIfAbsent(typeName, startID, endID, props)** (`pkg/graph/tx.go`): Atomic check-then-create for relationships within a transaction. Returns `(rel, created, err)` where `created=true` if a new relationship was created, `false` if one already existed. Only tracks for rollback when `created=true`. Enables idempotent relationship creation patterns (e.g., TECHNIQUE_OBSERVED) inside atomic transactions.

### Tests Added

- `TestTxGetNode` — read node within tx, verify properties visible.
- `TestTxGetNode_AfterDone` — GetNode returns `ErrTxDone` after commit.
- `TestTxAddRelationshipByID` — create by ID in tx, commit, verify persisted with properties.
- `TestTxAddRelationshipByID_Rollback` — create by ID, rollback, verify absent.
- `TestTxAddRelationshipByIDIfAbsent` — first call creates, second returns `created=false`, count stays 1.
- `TestTxAddRelationshipByIDIfAbsent_Rollback` — create, rollback, verify absent.

### Documentation

- Updated `docs/api.md` Transactions section with `tx.GetNode`, `tx.AddRelationshipByID`, and `tx.AddRelationshipByIDIfAbsent`.

## [3.0.65] - 2026-03-12

### Added

- **AddRelationshipByIDIfAbsent / AddRelationshipByIDIfAbsentWithContext** (`pkg/graph/context.go`, `pkg/graph/graph.go`): Atomic check-then-create for relationships. Prevents TOCTOU race where concurrent callers both see "absent" and create duplicate relationships. The existence check and creation are serialized under entity locks, guaranteeing exactly one relationship per (type, from, to). Returns `(rel, created, err)` where `created=true` if a new relationship was created, `false` if one already existed.

### Tests Added

- `TestGraphAddRelationshipByIDIfAbsent` — first call creates, second returns `created=false`.
- `TestGraphAddRelationshipByIDIfAbsent_Concurrent` — concurrent callers produce exactly one relationship.

## [3.0.64] - 2026-03-09

### Added

- **AddRelationshipByID / AddRelationshipByIDWithContext** (`pkg/graph/context.go`, `pkg/graph/graph.go`): Relationship creation using endpoint `snowflake.ID` values directly, introduced as an ID-form alternative to passing endpoint node objects. At introduction this path skipped endpoint fetches; current `Unreleased` behavior fetches live endpoints, captures `FromNodeHash`/`ToNodeHash`, and enforces configured constraints.

### Tests Added

- `TestGraphAddRelationshipByID` — happy path: type, endpoints, properties, store retrieval, integrity metadata, adjacency query.
- `TestGraphAddRelationshipByID_SelfLoop` — self-loop rejected with `ErrSelfLoop`.
- `TestGraphAddRelationshipByID_TemporalProps` — `tkg_valid_from` and `tkg_created_at` propagated correctly.

### Documentation

- Updated `docs/api.md` with `AddRelationshipByID` / `AddRelationshipByIDWithContext` documentation.

## [3.0.63] - 2026-03-09

### Added

- **Caller-provided temporal metadata via `tkg_` props** (`pkg/graph/context.go`, `pkg/graph/batch.go`): `AddNode`/`AddRelationship` (and `BatchBuilder` equivalents) now accept `tkg_valid_from`, `tkg_valid_to`, `tkg_created_at` in the props map. Values are extracted before validation (same pattern as provenance), then merged into `TemporalMetadata` alongside auto-set `TxFrom`. Zero API signature changes — fully backward compatible.

### Tests Added

- `TestAddNodeWithTemporal` — `tkg_valid_from`, `tkg_valid_to`, `tkg_created_at` propagated to `TemporalMetadata`; keys not stored as regular properties.
- `TestAddNodeWithoutTemporal` — default behavior unchanged when no temporal props provided.
- `TestAddRelWithTemporal` — temporal props propagated on relationship creation.
- `TestTemporalProps_InvalidType` — non-`int64` values rejected with error.

### Documentation

- Updated tutorial 002 with props-based temporal metadata section demonstrating `tkg_valid_from`, `tkg_valid_to`, `tkg_created_at` usage.

## [3.0.62] - 2026-03-07

### Fixed (5 Defects — production readiness)

- **Fix U — Batch lock-leak on panic** (`pkg/graph/batch.go`, MAJOR): `BatchBuilder.Execute()` acquired `g.mu.Lock()` without defer. A panic between lock acquisition and unlock (e.g., in a Store implementation) would leak the lock and leave `txEventBuffer` non-nil, permanently deadlocking the Graph. Fixed by adding deferred cleanup with an `unlocked` flag.

- **Fix V — Graph validation accepts negative limits** (`pkg/graph/graph.go`, MAJOR): `New()` resolved zero `ValidationLimits` fields to defaults but accepted negative values (e.g., `MaxLabelsPerNode: -1`), which passed through silently and broke downstream comparisons. Fixed by rejecting negative values after zero-to-default resolution.

- **Fix W — BadgerStore accepts invalid config** (`pkg/graph/badgerstore.go`, MAJOR): `NewBadgerStore()` accepted negative `FlushInterval`/`GCInterval`, out-of-range `GCDiscardRatio`, and empty `Dir` when `InMemory` is false. Fixed by adding upfront validation before opening Badger.

- **Fix X — Auth level silently truncates fractional float64** (`pkg/graph/context.go`, MAJOR): `extractProvenance` accepted `tkg_auth_level: 5.9` and silently truncated to `uint8(5)`. Fixed by adding `math.Trunc` check before range check — fractional values now return an error.

- **Fix Y — RemoveNodeLabel crash-consistency gap** (`pkg/graph/graph.go`, `pkg/graph/store.go`, all 3 Store implementations, MAJOR): `removeNodeLabelInternal` performed two separate writes: `PutNodeVersion` then `RemoveNodeLabelToken`. A crash between them would leave a phantom history entry. Fixed by adding `RemoveNodeLabelTokenWithHistory` to the Store interface, implemented atomically in MemoryStore (single lock), BadgerStore (single `appendOps` call), and TieredStore (delegate to shard).

### Tests Added

- `pkg/graph/v3062_fixes_test.go` — 19 tests:
  - `TestNew_NegativeValidationLimits` — each field at -1 rejected; zero/positive accepted.
  - `TestNewBadgerStore_InvalidConfig` — table-driven: negative flush, negative gc, bad ratio, empty dir.
  - `TestExtractProvenance_FractionalAuthLevel` — 5.9 rejected, 5.0 accepted, 0.1 rejected.
  - `TestBatchExecute_PanicRecovery` — inject panicking store, verify lock released.
  - `TestBatchExecute_ConcurrentAccess` — verify lock released after normal batch.
  - `TestBadgerStore_CreateTemporalIndex` — create, duplicate error.
  - `TestBadgerStore_DropTemporalIndex` — drop, double-drop error.
  - `TestBadgerStore_CreateHighFrequencyIndex` — success, duplicate, temporal conflict.
  - `TestBadgerStore_DropHighFrequencyIndex` — drop, double-drop, re-create after drop.
  - `TestBadgerStore_RemoveNodeLabelTokenWithHistory` — atomic label removal + history entry.
  - `TestTieredStore_CreateTemporalIndex_Store` — create across shards, verify query.
  - `TestTieredStore_DropTemporalIndex_Store` — drop, double-drop.
  - `TestTieredStore_CreateHighFrequencyIndex_Store` — create across shards.
  - `TestTieredStore_DropHighFrequencyIndex_Store` — drop, double-drop.
  - `TestTieredStore_SaveLabelRegistry_Deprecated` — in-memory no-op path.
  - `TestTieredStore_SaveRelTypeRegistry_Deprecated` — in-memory no-op path.
  - `TestTieredStore_RemoveNodeLabelTokenWithHistory` — atomic path on TieredStore.
  - `TestRemoveNodeLabel_AtomicHistory` — verifies history + version via Graph API.

### Historical Known Limitations (resolved later)

- **Cursor-based history ID queries** (`export.go:124`, `badgerstore.go:3050`): At this release point, `AllNodeHistoryIDs` and `AllRelHistoryIDs` returned all IDs at once and could exceed memory on large graphs. This was resolved later by `AllNodeHistoryIDsFrom` / `AllRelHistoryIDsFrom` and the bounded-RAM export history loop.
- **Streaming DiffSnapshots** (`temporal.go:618`): At this release point, `DiffSnapshots` materialized all nodes into RAM before computing the diff. This was resolved later by `DiffSnapshotsCallback`; `DiffSnapshots` now delegates to the streaming path and materializes only for callers that request the `SnapshotDiff`.
- **Cross-shard relationship batching** (`tieredstore_write.go:313`): `PutRelationshipsBatch` iterates relationships one-by-one across shards. Partitioning by shard would enable store-level batching for same-shard relationships.

### Documentation

- Refactored `README.md` by moving detailed technical documentation into the `docs/` directory (`api.md`, `persistence.md`, `design.md`), producing a cleaner high-level overview.
- Added updated Snowflake configuration details (microsecond precision, 5-bit node IDs, max ID 15) to both `README.md` and `CLAUDE.md`.

## [3.0.61] - 2026-03-07

### Fixed (6 Defects — pre-release hardening)

- **Fix O — Signature aliasing in DeepCopy** (`pkg/types/node.go`, `pkg/types/relationship.go`, MAJOR): `Node.DeepCopy()` and `Relationship.DeepCopy()` shallow-copied integrity structs — `Signature []byte` shared the same backing array between original and copy. Caller mutation of the copy corrupted the original. Fixed by adding `CloneBytes`, `NodeIntegrity.DeepCopy()`, and `RelIntegrity.DeepCopy()` in `pkg/types/integrity.go`.

- **Fix P — Signature aliasing at input boundary** (`pkg/graph/context.go`, MAJOR): `extractProvenance` assigned `tkg_signature` from the props map without copying. Caller mutation after `AddNode`/`AddRelationship` corrupted stored integrity. Fixed by cloning with `types.CloneBytes`.

- **Fix Q — Signature aliasing in wire encode/decode** (`pkg/graph/wire.go`, MAJOR): `nodeToWire`, `wireToNode`, `relToWire`, `wireToRel` all assigned Signature directly. After msgpack deserialization, Signature could alias Badger's internal value buffer. Fixed by wrapping all 4 assignments with `types.CloneBytes`.

- **Fix R — Data race in SetEventBus/SetAsyncEventBus** (`pkg/graph/graph.go`, MAJOR): `SetEventBus`, `SetAsyncEventBus`, and `GetEventBus` read/wrote `g.events` without synchronization. Fixed by wrapping writes in `g.mu.Lock()` and reads in `g.mu.RLock()`. Post-lock dispatch uses captured `eventPublisher` reference (`dispatchEvent` helper) to avoid reading `g.events` outside the lock.

- **Fix S — Data race in SetTemporalConstraints/AddTemporalConstraint** (`pkg/graph/graph.go`, MAJOR): `SetTemporalConstraints` and `AddTemporalConstraint` wrote `g.constraints` without synchronization. `checkTemporalConstraints` read it under `g.mu.RLock`. Fixed by wrapping writes in `g.mu.Lock()` and the `TemporalConstraints()` getter in `g.mu.RLock()`.

- **Fix T — Synchronous event handler deadlock** (`pkg/graph/context.go`, `pkg/graph/graph.go`, MAJOR): All `publishEvent` calls executed under `g.mu.RLock` (via defer). A synchronous event handler calling graph write methods would deadlock (RLock held, write needs Lock). Fixed by moving event dispatch outside mutation locks: `*WithContext` wrappers capture `g.events` under lock, release lock, then dispatch via `dispatchEvent`. Same pattern applied to `RemoveNodeLabel`, `CloseNodeVersion`, `CloseRelVersion`, `BatchBuilder.Execute`, and `GraphTx.Commit`.

### Changed

- **Config validation**: `TieredStoreConfig.ShardWindow` now rejects values below `time.Minute` (previously accepted any positive duration including sub-millisecond).
- **Doc fix**: `Config.SnowflakeNodeID` comment corrected from "0-511" to "0-15" to match actual validation (5-bit node ID).
- Retagged stale TODOs: `TODO(v3.0.57)` and `TODO(v3.0.58)` → `TODO(v3.1.0)` across `export.go`, `badgerstore.go`, `temporal.go`, `graph.go`, `tieredstore_write.go`.

### Tests Added

- `pkg/types/integrity_test.go` — `CloneBytes` (nil, empty, isolation), `NodeIntegrity.DeepCopy`, `RelIntegrity.DeepCopy`, `Node.DeepCopy` and `Relationship.DeepCopy` signature isolation.
- `pkg/graph/v3061_fixes_test.go` — 13 tests:
  - `TestExtractProvenance_SignatureIsolation` — caller mutation after AddNode cannot corrupt stored signature.
  - `TestWireRoundTrip_NodeSignatureIsolation`, `TestWireRoundTrip_RelSignatureIsolation` — wire encode/decode signature isolation.
  - `TestSetEventBus_NoRace`, `TestSetTemporalConstraints_NoRace` — concurrent config toggles with mutations under `-race`.
  - `TestSyncEventHandler_GraphRead_NoDeadlock` — sync handler calling `GetNode` during AddNode callback.
  - `TestNew_SnowflakeNodeID_Bounds` — boundary validation for 5-bit node ID (15 accepted, 16 rejected, -1 rejected).
  - `TestNewTieredStore_ShardWindow_Invalid` — negative and sub-minute windows rejected.
  - `TestTx_ImportRelationshipWithID` — tx import, commit, duplicate error, rollback.
  - `TestGetRelsAsOf` — temporal query returns correct version at each transaction time.
  - `TestCreateDropTemporalIndex`, `TestDropHighFrequencyIndex` — create/drop lifecycle, idempotency error.
  - `TestToFloat32SliceWire` — wire helper coverage for `[]any`, `[]float32`, nil, unsupported.

## [3.0.60] - 2026-03-06

### Fixed (2 Defects — post-v3.0.59 audit)

- **Fix M — Temporal index concurrent sort race** (`pkg/graph/temporal_index.go`, MAJOR): `sortIfDirty` was called under `idxMu.RLock` (shared) but `sort.Slice` mutates the slice in place. Two concurrent readers calling `queryAt`/`queryOverlap` could race on the sort. Fixed by adding `sortMu sync.Mutex` which serializes `sortIfDirty` — callers still enter under `RLock` but the sort itself is single-threaded.

- **Fix N — NodesByLabel temporal fast path nil fallthrough** (`pkg/graph/memorystore.go`, `pkg/graph/badgerstore.go`, MINOR): When a temporal index existed but `queryAt`/`queryOverlap` returned a nil result (no matches), the nil fell through the `if ids != nil` guard and proceeded to the O(N) label scan — defeating the index. Fixed by tracking `temporalQuery bool` separately from the result slice, so an empty-but-valid temporal result correctly short-circuits the label scan.

### Tests Added

- `pkg/graph/temporal_index_stress_test.go` — 11 stress tests covering concurrent sort races, interleaved reads/writes, and high-contention temporal index operations (Fix M).

### Changed

- Tutorial `005_performance`: added sections 8-11 (temporal index benchmarks, high-frequency index comparison, vector index throughput, TieredStore shard rotation overhead).
- **Snowflake Layout Upgrade**: Upgraded `github.com/bds421/rho-snowflake-2026` to `v1.3.0`. Switched to microsecond precision (`snowflake.WithMicroseconds()`) and adjusted bit layout: node bits reduced from 10 to 5 (max `SnowflakeNodeID` is now 15), step bits reduced from 12 to 10.
- **Removed hardcoded bit-shifting**: Replaced manual bitwise operations via native `snowflakeLayout.CreatedAt(id)` and `snowflakeLayout.Decompose(id)` APIs across `id_decompose.go`, `shadow.go`, `temporal.go`, `temporal_filter.go`, `entity_locks.go`, and `tieredstore.go`.

### Added

- `AGENTS.md` — comprehensive guidelines, project overview, testing rules, and architecture map for AI agents assisting with the codebase.

### Documentation

- Updated `CLAUDE.md` to include a new lesson on using library APIs instead of reimplementing internals.
- Updated `CLAUDE.md`, `README.md`, `docs/architecture.md`, `docs/SPEC.md`, `pkg/types/shadow.go` to reflect v3.0.60 changes.

## [3.0.59] - 2026-03-04

### Fixed (4 Defects — external audit)

- **Fix I — Transaction isolation gap** (`pkg/graph/context.go`, `pkg/graph/graph.go`, BLOCKER): `BeginTx()` holds `g.mu.Lock()` but individual mutations (`AddNode`, `UpdateNode`, etc.) did not acquire `g.mu`, so concurrent standalone mutations bypassed tx isolation — torn reads were possible during snapshot operations. Fixed via internal/external method split: all 13 exported mutation methods (`*WithContext`, `RemoveNodeLabel`, `CloseNodeVersion`, `CloseRelVersion`) now acquire `g.mu.RLock()` at entry. New unexported `*Internal` variants (lock-free) are called directly by `GraphTx` and `BatchBuilder` (which already hold `g.mu.Lock()`). Lock ordering: `g.mu` → entity locks (safe: entity locks never acquire `g.mu`).

- **Fix J — Rollback event desync** (`pkg/graph/graph.go`, `pkg/graph/tx.go`, MAJOR): Events were emitted immediately during tx mutations via `publishEvent`. On rollback, state was restored but no compensating events were published, leaving EventBus subscribers inconsistent. Fixed by adding `txEventBuffer *[]Event` field to `Graph`. During a transaction, `publishEvent` appends to the buffer instead of dispatching. On `Commit`, events are published after `g.mu.Unlock()` (so handlers can safely call Graph read methods). On `Rollback`, the buffer is discarded.

- **Fix K — Missing directory fsync** (`pkg/graph/registry_file.go`, MINOR): `atomicWriteFile` did `tmp.Sync()` + `os.Rename()` but omitted the directory `fsync`. On crash, the rename could be lost on some filesystems (ext4 with delayed allocation). Fixed by opening the parent directory and calling `Sync()` after rename.

- **Fix L — Dead code removal** (`pkg/graph/temporal.go`, `pkg/graph/tieredstore.go`, TRIVIAL): Removed 3 unused functions: `isNodeValidDuring`, `isRelValidDuring` (temporal.go), `classifyNodeID` (tieredstore.go). Zero callers confirmed via grep.

### Changed

- `pkg/graph/batch.go`: `Execute` now calls `*Internal` variants directly (was calling exported methods that would deadlock under `g.mu.Lock`). Added `"context"` import.
- `pkg/graph/export.go`: Updated `ExportGraph` doc comment to reflect that individual mutations are now also blocked by `g.mu`.

### Tests Added

- `TestMutationBlockedDuringTx` — verifies standalone `AddNode` blocks while a tx holds `g.mu.Lock`.
- `TestSnapshotConsistencyDuringMutation` — concurrent reads and writes under race detector.
- `TestTxCommitPublishesBufferedEvents` — 3 events buffered during tx, all published on Commit in order.
- `TestTxRollbackNoEvents` — zero events published after Rollback.
- `TestTxCommitHandlerCanReadGraph` — event handler successfully calls `GetNode` on Commit (proves events fire after `g.mu.Unlock`).
- `TestBatchEventsNotBuffered` — standalone mutations still emit events immediately.

## [3.0.58] - 2026-03-04

### Fixed (2 Defects — post-v3.0.57 code review triage)

- **Fix G — temporalIndex O(N²) memmove under store write lock** (`pkg/graph/temporal_index.go`, MAJOR): `add()` used binary search + `copy()` shift to maintain sorted order on every insert. For N batch inserts (e.g. bulk node import, N temporal index adds per label) this was O(N²) total memmove, all performed while holding the store's write lock. Fixed via lazy sort: `add()` now appends unsorted and sets `dirty=true`; new `sortIfDirty()` runs `sort.Slice` once at the start of `queryAt()` and `queryOverlap()`. Complexity: N inserts → O(N) appends + O(N log N) sort at first query (vs. O(N²) sorted insertions). The `dirty bool` field adds 1 byte to `temporalIndex`; no external API change.

- **Fix H — RemoveLabelTokenRaw leaves non-nil empty extraLabels** (`pkg/types/node.go`, MINOR): Two removal paths in `RemoveLabelTokenRaw` left `n.extraLabels` as an empty slice with non-zero capacity, violating the convention that an absent label set is `nil`. Case 1 (removing the only extra label via `append([:i], [i+1:]...)`) and Case 2 (promoting the only extra to primary via `extraLabels[1:]`) both produced `[]labelToken{}` with cap > 0. Since `DeepCopy` only copies when `len(n.extraLabels) > 0`, and `ExtraLabelTokens()` already guards with the same check, a non-nil empty slice was functionally harmless but inconsistent and retained the backing array unnecessarily. Fixed by adding an explicit `n.extraLabels = nil` after each removal path that empties the slice.

### Documentation

- `pkg/types/propertyslice.go` (`Set`): added note that `NewPropertySlice` (O(N log N)) is preferred over repeated `Set` calls for bulk construction.
- `pkg/types/recurrence.go` (`RecurrencePattern`): strengthened UTC-only note — DST transitions are invisible to this type; expanded instants stay in UTC and local-time presentation happens after expansion.
- `pkg/graph/vector_index.go` (`toFloat32Slice`): added slow-path note on the `[]any` branch; `[]float32` remains the fast path for high-frequency vector nodes.

### Tests Added

- `TestTemporalIndex_LazySort_BatchInsert` — 100 out-of-order inserts followed by a single `queryAt`; verifies all IDs returned and `dirty` transitions correctly (Fix G).
- `TestTemporalIndex_LazySort_InterleavedReadsWrites` — interleaved `add`/`queryAt`/`queryOverlap` calls; verifies correct results at each step and `dirty` flag transitions (Fix G).
- `TestRemoveLabelTokenRaw_ExtraLabelsNilAfterLastRemoval` — three sub-tests: remove only extra, promote only extra to primary, remove one of two extras (last case verifies non-nil is preserved) (Fix H).

## [3.0.57] - 2026-03-03

### Fixed (6 Production Defects — v3.0.57)

- **Fix A — Rollback/Commit panic leaks graph write lock** (`pkg/graph/tx.go`, CRITICAL): `tx.g.mu.Unlock()` was the last explicit statement in `Rollback()`. Any panic in one of the six rollback phases (store `PutRelationship`, `PutNode`, `ReplaceRelationship`, `ReplaceNode`, `DeleteRelationship`, `DeleteNodeCascade`) left the graph write lock permanently held, deadlocking all subsequent `BeginTx`/`Batch`/`Reset` callers. Fixed by replacing the explicit unlock with `defer tx.g.mu.Unlock()` placed immediately after `tx.done = true`, ensuring the lock is released on both normal and panic paths. Same fix applied to `Commit()` for forward safety.

- **Fix B — RemoveNodeLabel violates temporal guarantees** (`pkg/graph/graph.go`, BLOCKER): `RemoveNodeLabel` overwrote the node in-place via `RemoveNodeLabelToken` with no version bump and no history entry (explicit "No version bump; no history entry" comment). Past-time queries (`GetNodeAt`, `GetNodesValidAt`, `DiffSnapshots`) could not observe that the node ever had the removed label. Fixed following the `UpdateNodeWithContext` version-bump pattern: capture `prevVersion`/`prevState` before mutation, call `PutNodeVersion(id, prevVersion, prevState)` to save the old state, bump `copy.SetVersion(prevVersion + 1)`, then call `RemoveNodeLabelToken`. Also corrected the hash chain to use `current.Integrity().Hash` (not `PrevHash`) as the new `PrevHash`, matching the chain in all other update paths. Stale comment removed from `badgerstore.go:RemoveNodeLabelToken`. The crash window was later closed by the atomic `RemoveNodeLabelTokenWithHistory` Store method.

- **Fix C — DiffSnapshots holds g.mu.RLock during O(N) materialization** (`pkg/graph/temporal.go`, BLOCKER): `DiffSnapshots` held `g.mu.RLock()` across two calls to `snapshotLocked`, each materialising ALL valid nodes/rels into RAM. All `g.mu.Lock()` callers (`BeginTx`, `Batch`, `Reset`) were blocked for the full O(N) duration. `GetNodesValidAt` uses `forEachKnownNodeID` which does its own store-level locking — `g.mu` is not required for correctness of individual snapshot reads. Fixed by removing `g.mu.RLock()` from `DiffSnapshots`. Renamed `snapshotLocked` → `snapshotAt` (no longer requires the caller to hold `g.mu`). `Snapshot()` continues to hold `g.mu.RLock()` for strong consistency. Trade-off documented: a concurrent backdated write that commits between the two reads may appear as a spurious Created/Deleted entry. The RAM concern was later addressed by `DiffSnapshotsCallback`; `DiffSnapshots` now delegates to that streaming path.

- **Fix D — searchNearest O(N log N) sort under RLock** (`pkg/graph/vector_index.go`, MAJOR): `searchNearest` allocated a `[]scored` of all N entries, sorted it in O(N log N) while holding `vi.mu.RLock()`, blocking concurrent vector insertions. Replaced with a max-heap of size k (`knnHeap` via `container/heap`) that runs in O(N log k) time. For k ≪ N the lock is held significantly shorter; for k=N behaviour is equivalent. The heap drains in ascending distance order (closest first) matching the previous sort contract. Removed `"sort"` import; added `"container/heap"`.

- **Fix E — checkTemporalConstraints allocates on every relationship write** (`pkg/graph/temporal_constraint.go`, MINOR): `g.constraints.Items()` copies the constraint slice on every relationship write (even with a single constraint). Added unexported `forEach(fn func(TemporalConstraint) error) error` method on `ConstraintSet` that iterates `cs.items` directly with zero allocation. `checkTemporalConstraints` updated to use `forEach`. Exported `Items()` retained for external callers.

- **Fix F — TOCTOU retry loop has no backoff** (`pkg/graph/context.go`, MINOR): The TOCTOU retry in `DeleteNodeWithContext` called `continue` immediately after `UnlockMany` on a failed attempt. Under sustained concurrent rel-add/remove to the same node, all 10 retries could exhaust without the competing goroutine making progress. Fixed by adding `runtime.Gosched()` after `UnlockMany`, yielding the processor to let the competing rel-writer commit. Added `"runtime"` to import block.

### Tests Added

- `TestRemoveNodeLabel_PreservesHistory` — verifies `GetNodeHistory` returns 1 entry after `RemoveNodeLabel`, version 0 retains the removed label (Fix B).
- `TestGraphTx_RollbackPanicSafe` — injects a store panic during rollback via `deleteRelPanicStore`; verifies `BeginTx` completes within 2s after recovery, confirming the graph write lock was released by the deferred unlock (Fix A).
- `TestDiffSnapshots_DoesNotBlockWrites` — runs 8 concurrent `DiffSnapshots` goroutines alongside 8 `BeginTx` goroutines; verifies all complete within 10s without deadlock (Fix C).
- `TestSearchNearest_HeapCorrectness` — verifies top-k results from the heap implementation match brute-force distances for k=1 (exact nearest), k=3 (set equality + non-decreasing order), and k=N (set equality + non-decreasing order, allowing ties within equal-distance groups) (Fix D).

## [3.0.56] - 2026-03-03

### Fixed (7 Production Defects — v3.0.56)

- **Fix #1 — Cold shard use-after-close** (`pkg/graph/tieredstore.go`): `shardForNodeID` returned a `*BadgerStore` pointer without incrementing `activeReqs`, creating a race with `closeIdleShards` that could panic on cold-shard Badger access. Fixed by adding `shardForNodeIDChecked(id snowflake.ID) (*BadgerStore, func(), error)` which wraps the shard resolution with `checkoutStore`/`checkinStore`. The returned `checkin` function is deferred at every callsite. All six read/write callsites updated: `GetNode`, `GetNodeHistory` (`tieredstore_read.go`), `DeleteNode`, `ReplaceNode`, `RemoveNodeLabelToken` (`tieredstore_write.go`). The refShard and refArchive return a no-op checkin (they are never closed by `closeIdleShards`).

- **Fix #2 — stripDepth drops pagination** (`pkg/graph/tieredstore_read.go`): `stripDepth` was returning `QueryOpts{Depth: stripped}` — a fresh struct that dropped `Limit`, `After`, and all temporal fields. Every per-shard call from `NodesByLabel`, `AllNodes`, and `AllRelationships` received `Limit=0` (unbounded), causing full-shard scans and potential OOM materialisation. Fixed by modifying `opts` in-place (`opts.Depth = 0; return opts`) so all other fields are preserved.

- **Fix #3 — PutNodesBatch / PutRelationshipsBatch no rollback** (`pkg/graph/tieredstore_write.go`): When the second-shard write in a batch failed, first-shard writes were committed, leaving orphaned entities in the store. Fixed by adding rollback: on hot-shard `PutNodesBatch` failure, each successfully written refShard node is removed via `DeleteNode`; on `PutRelationshipsBatch` failure, each completed shard batch is rolled back. Rollback errors are surfaced with the primary failure.

- **Fix #4 — idxMu held over disk I/O** (`pkg/graph/badgerstore.go`): `DeleteNode`, `ReplaceNode`, and `RemoveNodeLabelToken` called `idxMu.Lock()` then immediately `getNodeLocked(id)`, which on cache miss issued a synchronous Badger `db.View()` read under the global write lock — blocking all concurrent readers (including `NodesByLabel`, `ForEach`) for up to 5ms per operation under `SyncWrites=true`. Fixed by adding `prefetchNode(id snowflake.ID) (*types.Node, error)` helper that checks the cache under `RLock`, then reads from Badger without any lock, then re-verifies under `RLock`. All three methods call `prefetchNode` before acquiring `idxMu.Lock()`; the TOCTOU window is guarded by re-checking `nodeIDs[id]` under the write lock.

- **Fix #5 — ImportGraph holds lock during streaming I/O** (`pkg/graph/export.go`): `ImportGraph` previously acquired `g.mu.Lock()` at function entry and held it for the entire `io.Reader` streaming loop, freezing all graph operations (reads, writes, queries) for potentially minutes during large or networked imports. Fixed with a two-phase implementation: Phase 1 (no lock) buffers all records from the `io.Reader` into a `[]importRecord` slice; Phase 2 (under `g.mu.Lock()`) deserialises the buffer and applies store writes. I/O latency no longer extends the lock scope.

- **Fix #6 — AllNodeHistoryIDs / AllRelHistoryIDs OOM** (`pkg/graph/export.go`, `badgerstore.go`): Deferred at this point because fixing it required cursor-based history-ID scans across all Store implementations. This was later resolved by `AllNodeHistoryIDsFrom` / `AllRelHistoryIDsFrom` and the bounded-RAM export history loop.

- **Fix #7 — BackpressureDropOldest CPU livelock** (`pkg/graph/events.go`): The `BackpressureDropOldest` inner `for {}` loop had an empty `default:` branch — tight spinning under contention saturated a CPU core and starved the worker goroutine (livelock). Fixed by adding `runtime.Gosched()` in the default case. No cost on the uncontended path; yields the scheduler slot on contention.

### Tests Added

- `TestTieredStore_NodesByLabel_PaginationBounded` — verifies Limit/After are honoured across a multi-shard TieredStore query (fix #2).
- `TestAsyncEventBus_DropOldest_TerminatesQuickly` — 50×20 concurrent publishes on a full channel complete within 2s (fix #7).
- `TestTieredStore_PutNodesBatch_RollbackOnHotShardError` — verifies ref shard has no orphan node after hot-shard failure (fix #3).
- `TestBadgerStore_DeleteNode_NoDiskIOUnderWriteLock` — DeleteNode on a cache-miss node does not hold idxMu during db.View (fix #4).
- `TestImportGraph_DoesNotBlockReadsWhileStreaming` — concurrent GetNode succeeds during ImportGraph Phase 1 (fix #5).

All in `pkg/graph/v3056_fixes_test.go`.

## [3.0.55] - 2026-03-03

### Added (Atomic Delete with Tombstone History — Phase 4.23)

- **`RelTombstone`** — new struct in `pkg/graph/store.go` packaging a relationship's tombstone data for atomic delete operations: `ID snowflake.ID`, `PrevVersion uint32`, `Tombstone *types.Relationship` (pre-built deep copy with `DeletedAt`/`ValidTo`/`TxFrom`/`TxTo` set by the caller).
- **`Store.DeleteNodeWithHistory(id snowflake.ID, prevNodeVersion uint32, nodeTombstone *types.Node, relTombstones []RelTombstone) error`** — new interface method. Atomically combines `PutRelVersion×N` + `PutNodeVersion` + `DeleteNodeCascade` into a single storage transaction. Eliminates orphaned tombstone history entries that could result from a crash between the previous N+2 separate store calls. `relTombstones` may be nil when the node has no connected relationships.
- **`Store.DeleteRelWithHistory(id snowflake.ID, prevVersion uint32, tombstone *types.Relationship) error`** — new interface method. Atomically combines `PutRelVersion` + `DeleteRelationship` into a single storage transaction. Eliminates the crash window between the previous two separate calls.
- **`MemoryStore.DeleteNodeWithHistory` / `DeleteRelWithHistory`** — implemented under a single `ms.mu.Lock()`, ensuring history write and entity deletion are atomic with respect to concurrent readers.
- **`BadgerStore.DeleteRelWithHistory`** — serializes tombstone outside `idxMu`, acquires `idxMu.Lock()` once, calls `deleteRelByInfo` (queues cascade delete ops into `pending`), appends tombstone history op to the same `pending` map, releases lock, then flushes. All ops land in the same Badger `WriteBatch.Flush()` call — atomic.
- **`BadgerStore.DeleteNodeWithHistory`** — serializes all tombstones outside `idxMu`, acquires `idxMu.Lock()` once, calls `cascadeDeleteInner` (queues all cascade delete ops into `pending`), appends node + rel tombstone history ops to the same `pending` map before releasing the lock. Single `WriteBatch.Flush()` commits cascade + tombstone history atomically.
- **`cascadeDeleteInner`** — unexported helper extracted from `cascadeDeleteLocked`. Same body, but without `idxMu.Lock()/Unlock()`; it runs only inside paths that already hold the lock. Enables `DeleteNodeWithHistory` to extend the lock scope across cascade + tombstone append without a second lock acquisition.
- **`marshalNodeToBytes` / `marshalRelToBytes`** — unexported helpers in `badgerstore.go` wrapping `msgpack.Marshal(nodeToWire(n))` / `msgpack.Marshal(relToWire(r))`. Used by both existing `PutNodeVersion`/`PutRelVersion` paths and the new tombstone serialization in `DeleteNodeWithHistory`/`DeleteRelWithHistory`.
- **`TieredStore.DeleteRelWithHistory`** — delegates to the relationship's entity shard for tombstone + entity/typeIdx/outIdx ops. Cross-shard inIdx cleanup follows the same split-write pattern as `DeleteRelationship`; if the entity-shard tombstone/delete fails after the incoming leg was removed, the incoming leg is restored.
- **`TieredStore.DeleteNodeWithHistory`** — deletes connected relationships through `DeleteRelWithHistory`, then delegates the node tombstone to `shard.DeleteNodeWithHistory(id, prevNodeVersion, nodeTombstone, nil)`. Current code snapshots pre-call relationship state and restores already-deleted relationships if a later relationship delete or node tombstone write fails before the node row is removed.
- **`context.go:deleteNodeLocked`** rewritten — builds `[]RelTombstone` in one pass (dedup via `seen` map), builds node tombstone, calls single `g.store.DeleteNodeWithHistory`. Replaces the previous loop of `PutRelVersion` calls + `PutNodeVersion` + `DeleteNodeCascade` (N+2 separate store calls).
- **`context.go:DeleteRelationshipWithContext`** rewritten — calls single `g.store.DeleteRelWithHistory`. Replaces the previous `PutRelVersion` + `DeleteRelationship` (2 separate store calls).
- 6 tests in `pkg/graph/atomic_delete_test.go`: `TestDeleteRelWithHistory_HistoryAndLiveConsistent`, `TestDeleteRelWithHistory_NotFound`, `TestDeleteNodeWithHistory_HistoryAndLiveConsistent`, `TestDeleteNodeWithHistory_EmptyRelTombstones`, `TestDeleteNodeWithHistory_BadgerStore`, `TestDeleteNodeWithHistory_TieredStore`.

### Design Notes

- Atomicity guarantee (BadgerStore): holding `idxMu.Lock()` across both `cascadeDeleteInner` and tombstone `appendOps` prevents the background flush goroutine (which acquires `idxMu.RLock()`) from draining `pending` between the two phases. All ops land in one `WriteBatch.Flush()`.
- TieredStore keeps per-shard atomic batches and wraps cross-shard split writes in local rollback where the inverse operation is available; `RunRepair` remains the reconciliation path for externally corrupted or unrecoverable states.
- `DeleteNodeCascade`, `PutNodeVersion`, `PutRelVersion` are unchanged — they are still used by repair/migration tools (`tieredstore_repair.go`, `tieredstore_migrate.go`) which do not write tombstones.
- After deletion, `TieredStore.GetNodeVersion`/`GetRelVersion` cannot resolve the shard via the high-level routing (relies on live in-memory presence). Access the underlying shard directly (`ts.refShard.GetNodeVersion(...)`) for post-delete history verification — as demonstrated in `TestDeleteNodeWithHistory_TieredStore`.

## [3.0.54] - 2026-03-03

### Added (AllowSelfLoops Validation — Phase 4.22)

- **`ValidationLimits.AllowSelfLoops bool`** — new field on `ValidationLimits`. Default zero value (`false`) rejects self-loop relationships (where `startNode == endNode`). Set to `true` to permit them. This aligns rho/tkg/v3 with the tkg-2025-v2 and tkg-2026-v3 reference implementations.
- **`ErrSelfLoop`** — new graph-layer sentinel error: `"graph: self-loop relationship not allowed; set AllowSelfLoops in ValidationLimits to permit"`. Returned by `AddRelationshipWithContext` and `ImportRelationshipWithID` when `startID == endID && !g.validation.AllowSelfLoops`.
- **`context.go:AddRelationshipWithContext`** — self-loop guard added after `endID` extraction, before `LockTwo`: rejects when `startID == endID && !g.validation.AllowSelfLoops`.
- **`context.go:ImportRelationshipWithID`** — same self-loop guard added at the equivalent position.
- 5 tests in `pkg/graph/self_loop_test.go`: `TestSelfLoop_ZeroValueRejects`, `TestSelfLoop_ExplicitFalseRejects`, `TestSelfLoop_AllowedByConfig`, `TestSelfLoop_ImportRejected`, `TestSelfLoop_DifferentNodesStillWork`.

### Fixed

- **`TestGraphDeleteNodeSelfLoopCascade`** (`pkg/graph/graph_test.go`) — updated to use `Config{Validation: ValidationLimits{AllowSelfLoops: true}}` since the default now rejects self-loops.
- **`TestEndpointHashSelfLoop`** (`pkg/graph/rel_endpoint_hash_test.go`) — same fix.

## [3.0.53] - 2026-03-03

### Fixed (Code Review Bugs — Phases 4.13–4.16)

- **`extractProvenance` silent truncation** (`pkg/graph/context.go`): Integer values of `tkg_auth_level` outside `[0, 255]` previously silently wrapped via modulo cast to `uint8`, corrupting the stored `AuthorizationLevel`. Unrecognised non-nil types (e.g. `string("5")`) silently stored 0. Fixed by adding bounds checks for all integer/float64 cases (`int`, `int32`, `int64`, `float64`) and a `default` case that returns an explicit error. The function signature gains a 6th `error` return; all 4 callers in `context.go` now propagate the error. All `#nosec G115` comments on the cast sites removed — bounds are now checked explicitly. 5 new tests: `TestExtractProvenance_OutOfBoundsInt`, `TestExtractProvenance_NegativeInt`, `TestExtractProvenance_OutOfBoundsFloat`, `TestExtractProvenance_InvalidType`, `TestExtractProvenance_ValidBoundary`.

- **`ImportGraph` registry mismatch** (`pkg/graph/export.go`): When importing into a graph whose label/reltype registry was already populated with different token mappings, `ErrRegistryNotEmpty` was swallowed and the import continued, silently assigning wrong labels and relationship types to all imported entities. Fixed by comparing the existing registry with the incoming one via `reflect.DeepEqual` when `ErrRegistryNotEmpty` is returned from `ImportNames`. Identical registries (idempotent re-import) continue without error; conflicting registries return the new sentinel `ErrIncompatibleRegistry`. 2 new tests: `TestImportGraph_IncompatibleLabelRegistry`, `TestImportGraph_CompatibleRegistryIdempotent`. Existing `TestImport_IdempotentRegistry` continues to pass.

- **`readExportRecord` DoS / OOM** (`pkg/graph/export.go`): The 4-byte length header in the export stream was trusted unconditionally. A crafted export file with `length = 4 GiB` caused an immediate OOM allocation before any data was read. Fixed by adding `maxExportRecordSize = 128 MiB` constant and a guard that returns an error before the `make([]byte, length)` call. 1 new test: `TestReadExportRecord_OversizeRecord`.

## [3.0.52] - 2026-03-03

### Added (HighFrequencyIndex — Phase 4.21)

- **`highFrequencyIndex`** — new time-bucketed index providing O(1) amortized insertion versus the sorted-slice `temporalIndex`'s O(log n). Designed for high-write-rate scenarios (thousands of event writes/sec into TieredStore event shards).
- **`newHighFrequencyIndex(bucketSize time.Duration, origin types.Instant) *highFrequencyIndex`** — constructor. `bucketSize` controls the time width of each bucket (e.g., `time.Hour`). `origin` sets the baseline for bucket 0.
- **`(*highFrequencyIndex).add(id snowflake.ID, validFrom types.Instant)`** — O(1) amortized insertion; bucket index = `(validFrom - origin) / bucketSize`. Thread-safe via internal `sync.RWMutex`.
- **`(*highFrequencyIndex).remove(id snowflake.ID, validFrom types.Instant)`** — O(n/num_buckets) amortized removal from a single bucket.
- **`(*highFrequencyIndex).pointQuery(t types.Instant) []snowflake.ID`** — returns all IDs in the bucket containing `t`. These are candidates for the graph/query filtering layer that checks full interval semantics.
- **`(*highFrequencyIndex).rangeQuery(start, end types.Instant) []snowflake.ID`** — returns all IDs in buckets overlapping `[start, end)`. Candidates — same re-filter note as `pointQuery`.
- **`(*highFrequencyIndex).bucketFor(validFrom types.Instant) int64`** — unexported helper; instants before `origin` map to negative bucket indices using correct floor division.
- **`Store.CreateHighFrequencyIndex(labelToken uint16, bucketSize time.Duration) error`** — new method on the `Store` interface. Returns `ErrTemporalIndexExists` if any temporal index (sorted-slice or bucket) already exists for this label — only one type per label at a time.
- **`Store.DropHighFrequencyIndex(labelToken uint16) error`** — new method on the `Store` interface. Returns `ErrTemporalIndexNotFound` if no high-frequency index exists.
- **`MemoryStore.CreateHighFrequencyIndex` / `DropHighFrequencyIndex`** — implemented; stored in a new `hfIndexes map[uint16]*highFrequencyIndex` field (separate from `temporalIndexes`).
- **`BadgerStore.CreateHighFrequencyIndex` / `DropHighFrequencyIndex`** — implemented; stored in a new `hfIndexes map[uint16]*highFrequencyIndex` field under `idxMu`. The index is in-memory and `CreateHighFrequencyIndex` builds it from current store state.
- **`TieredStore.CreateHighFrequencyIndex` / `DropHighFrequencyIndex`** — delegates across all active shards (ref + event) following the same pattern as `CreateTemporalIndex`. Current code tracks HFI definitions so rotated hot shards and lazily opened archives inherit them.
- **`Graph.CreateHighFrequencyIndex(label string, bucketSize time.Duration) error`** — public Graph API. Current behavior creates the label token if needed so the index applies to future matching nodes. Returns `ErrTemporalIndexExists` on conflict.
- **`Graph.DropHighFrequencyIndex(label string) error`** — public Graph API. Current behavior returns `ErrTemporalIndexNotFound` when the label or HFI is absent.
- 12 tests in `pkg/graph/hf_index_test.go`: `TestHFIndex_Add_PointQuery`, `TestHFIndex_RangeQuery`, `TestHFIndex_Remove`, `TestHFIndex_HighWriteRate` (10k concurrent adds, race-clean), `TestCreateHighFrequencyIndex_Graph`, `TestHFIndex_ReplacesTemporalIndex`, `TestHFIndex_DuplicateCreate`, `TestHFIndex_DropNotFound`, `TestHFIndex_ConflictsWithTemporalIndex`, `TestHFIndex_UnknownLabel`, `TestHFIndex_BucketFor_BeforeOrigin`, `TestHFIndex_RangeQuery_EmptyResult`.

### Design Notes

- HFI stores `validFrom` buckets and returns candidates for a filtering layer that checks full interval semantics.
- HFI entries are rebuilt in memory; BadgerStore persists label/bucket definitions, and TieredStore persists temporal/HFI tracking so restarted stores, rotated hot shards, and lazily opened archives keep the active HFI contract.
- Only one temporal index type can exist per label at a time: a `temporalIndex` and a `highFrequencyIndex` cannot coexist. Drop one before creating the other.
- `time.Duration` added to `store.go` imports (used by new interface methods).

## [3.0.51] - 2026-03-03

### Added (Event Priority Levels — Phase 4.20)

- **`EventPriority uint8`** — new type controlling delivery queue routing in `AsyncEventBus`. Five named constants (zero value = `PriorityNormal` for backward compatibility):
  - `PriorityNormal` (0) — default; all existing `Event{}` literals remain valid without change.
  - `PriorityHigh` (1) — create events (`EventNodeCreate`, `EventRelCreate`).
  - `PriorityCritical` (2) — delete/cascade events (`EventNodeDelete`, `EventRelDelete`).
  - `PriorityLow` (3) — available for caller-side lower-priority events.
  - `PriorityDeferred` (4) — available for caller-side background/analytics events.
- **`numPriorityLevels = 5`** — unexported constant sizing the per-priority queue array.
- **`Event.Priority EventPriority`** — new field on `Event`. Zero value is `PriorityNormal`; all existing `Event{}` struct literals compile unchanged (backward-compatible).
- **`AsyncEventBus.queues [numPriorityLevels]chan Event`** — replaces the single `queue chan Event` with one buffered channel per priority level. `QueueSize` is applied uniformly to each channel.
- **`priorityOrder [numPriorityLevels]EventPriority`** — package-level drain order array: `[Critical, High, Normal, Low, Deferred]`. Used by workers to implement best-effort priority ordering.
- **Per-priority `publish` routing** — `AsyncEventBus.publish` routes each event to `queues[e.Priority]`. Out-of-range priority values fall back to `PriorityNormal`. Backpressure strategies (`BackpressureBlock`, `BackpressureDropOldest`, `BackpressureDropLatest`) apply per-queue.
- **Priority-ordered worker drain** — workers perform a non-blocking check through `priorityOrder` before blocking on a multi-channel select. When multiple queues have events, higher-priority events are served first (best-effort; Go scheduler is non-deterministic).
- **`drainAll()`** — helper draining all per-priority queues in priority order on stop signal. Called from worker on `stopCh` closure (replaces the previous inline drain loop).
- **`Graph.publishEvent` signature updated** — fourth parameter `priority EventPriority` added. All 11 internal call sites updated with appropriate priorities:
  - `EventNodeCreate` / `EventRelCreate` → `PriorityHigh`
  - `EventNodeDelete` / `EventRelDelete` → `PriorityCritical`
  - `EventNodeUpdate` / `EventRelUpdate` (all paths including in-place, RemoveLabel, CloseVersion) → `PriorityNormal`
- 4 new tests in `pkg/graph/async_eventbus_test.go`: `TestPriority_ZeroValueIsNormal`, `TestPriority_GraphDeleteIsCritical`, `TestPriority_GraphCreateIsHigh`, `TestPriority_CriticalBeforeNormal`.

## [3.0.50] - 2026-03-03

### Added (Async EventBus with Worker Pool + Backpressure — Phase 4.19)

- **`eventPublisher` interface** — unexported interface with a single `publish(Event)` method. Both `*EventBus` (sync) and `*AsyncEventBus` (async) implement it. `Graph.events` field changed from `*EventBus` to `eventPublisher`, enabling transparent substitution.
- **`AsyncEventBus`** — asynchronous event bus that decouples handler latency from graph write latency. Handlers are invoked in a bounded worker pool, not on the caller's goroutine. A slow handler no longer stalls mutations.
- **`NewAsyncEventBus(cfg AsyncEventBusConfig) *AsyncEventBus`** — creates and starts the bus. Workers begin consuming immediately. Defaults: `Workers=1`, `QueueSize=256`.
- **`AsyncEventBusConfig`** — configuration struct: `Workers int` (goroutine count), `QueueSize int` (channel buffer), `Backpressure BackpressureStrategy` (full-queue behavior).
- **`BackpressureStrategy`** — enum with three values:
  - `BackpressureBlock` — blocks the caller until queue space is available (zero event loss, max back-pressure to writers).
  - `BackpressureDropOldest` — evicts the oldest queued event and enqueues the new one (preserves newest events under load).
  - `BackpressureDropLatest` — discards the incoming event when the queue is full (zero blocking, may lose events).
- **`AsyncEventBus.Subscribe(h EventHandler) func()`** — registers a handler; returns an idempotent unsubscribe closure (B11, `sync.Once`). Safe for concurrent use.
- **`AsyncEventBus.Close()`** — signals workers to stop, drains all pending queue entries before returning, then waits for all workers to exit. Safe to call multiple times (B11, `sync.Once`). Guarantees at-most-once delivery of all events enqueued before `Close()`.
- **`Graph.SetAsyncEventBus(bus *AsyncEventBus)`** — attaches an `AsyncEventBus`. Nil-safe (typed-nil guard prevents interface-wrapping a nil pointer from defeating the `g.events == nil` check in `publishEvent`).
- **`Graph.SetEventBus(bus *EventBus)`** — updated with explicit nil-guard (same typed-nil safety fix).
- **`Graph.GetEventBus() *EventBus`** — updated to type-assert against `eventPublisher` interface; returns `nil` when an `AsyncEventBus` is attached.
- **`dispatch` (internal)** — copies handler slice under `RLock` before invoking (B15 copy-outside-lock pattern). Uses `safeInvoke` — panics inside handlers are recovered and logged, never crashing the worker goroutine.
- 8 new tests in `pkg/graph/async_eventbus_test.go`: `TestAsyncEventBus_HandlerReceivesEvent`, `TestAsyncEventBus_SlowHandlerDoesNotBlockPublish`, `TestAsyncEventBus_BackpressureBlock`, `TestAsyncEventBus_BackpressureDropOldest`, `TestAsyncEventBus_BackpressureDropLatest`, `TestAsyncEventBus_Close_DrainsQueue`, `TestAsyncEventBus_MultipleWorkers`, `TestSetAsyncEventBus_GraphIntegration`.

## [3.0.49] - 2026-03-03

### Added (Transaction Time / Bitemporality — Phase 4.18)

- **`TemporalMetadata.TxFrom Instant`** is now populated on every write path. `AddNodeWithContext` and `AddRelationshipWithContext` set `TxFrom = nowInstant()` after hash computation (TxFrom/TxTo are NOT fed into `ComputeNodeHash`/`ComputeRelHash`). `UpdateNodeWithContext` and `UpdateRelationshipWithContext` set `TxFrom = now` on the new version (the same `now` used for `UpdatedAt`) and `TxTo = now` on the prevState deep-copy before it is written to history.
- **`TemporalMetadata.TxTo Instant`** is set to `now` on the tombstone created by `deleteNodeLocked` (node + connected rels) and `DeleteRelationshipWithContext`. Tombstones have both `TxFrom = now` and `TxTo = now` (committed and immediately superseded at the same instant, matching the deleted-entity contract).
- **`Graph.GetNodeAsOf(id, txTime)`** — returns the node version whose transaction time window covered `txTime`. Checks the current tip first (`TxFrom <= txTime && TxTo == 0`), then scans history for the highest-`TxFrom` version satisfying `TxFrom <= txTime && (TxTo == 0 || TxTo > txTime)`. Returns `ErrNoVersionAsOf` if no version was recorded at that time.
- **`Graph.GetRelAsOf(id, txTime)`** — mirrors `GetNodeAsOf` for relationships.
- **`Graph.GetNodesAsOf(txTime)`** — returns all nodes that existed in the graph at the given transaction time. Uses the two-phase ForEach pattern: Phase 1 collects all current + history node IDs through Store iteration; Phase 2 calls `GetNodeAsOf` per ID after the iterator returns. Skips `ErrNoVersionAsOf`.
- **`Graph.GetRelsAsOf(txTime)`** — mirrors `GetNodesAsOf` for relationships.
- **`ErrNoVersionAsOf`** — new sentinel: `"graph: no entity version recorded at the given transaction time"`.
- **`Config.SyncWrites bool`** — also now wired through to `BadgerStoreConfig.SyncWrites` in `New()` (pre-existing gap fixed alongside Phase 4.18).
- **Shadow resolver tests updated** — `TestResolveNodePropertyNilTemporal` and `TestResolveRelPropertyNilTemporal` now construct entities directly (bypassing `Add*`) to test the nil-temporal code path. `AddNode`/`AddRelationship` always set `TxFrom` after Phase 4.18, so the test must use manually constructed entities to exercise the nil branch.
- 8 new tests in `pkg/graph/txtime_test.go`: `TestTxFromSetOnAdd`, `TestTxToSetOnUpdate`, `TestTxToSetOnDelete`, `TestGetNodeAsOf_BeforeCreate`, `TestGetNodeAsOf_CurrentVersion`, `TestGetNodeAsOf_HistoricalVersion`, `TestGetNodesAsOf_FiltersCorrectly`, `TestGetRelAsOf`.

## [3.0.48] - 2026-03-03

### Added (Sync-Write Config Flag — Phase 4.17)

- **`BadgerStoreConfig.SyncWrites bool`** — when true, opens Badger with `WithSyncWrites(true)` (fsync-on-every-write at the disk level) and forces `FlushInterval=0` so the background flush goroutine is never started. Every mutating method calls `bs.flush()` immediately after releasing `idxMu`, persisting writes to stable storage before returning. Eliminates the 100ms async flush window at the cost of higher write latency. Ignored in ReadOnly mode.
- **`Config.SyncWrites bool`** — propagated to `BadgerStoreConfig.SyncWrites` when `New()` creates a `BadgerStore`. No-op when using `MemoryStore` or an explicitly injected `Store`.
- **`BadgerStore.syncWrites bool`** — unexported field on BadgerStore; checked after every `appendOps` call in all 16 mutating methods: `PutNode`, `DeleteNode`, `ReplaceNode`, `RemoveNodeLabelToken`, `PutRelationship`, `ReplaceRelationship`, `DeleteRelationship`, `ReplaceNodeWithHistory`, `ReplaceRelWithHistory`, `PutNodeVersion`, `PutRelVersion`, `DeleteNodeCascade`, `PutNodesBatch`, `PutRelationshipsBatch`, `DeleteNodesBatch`, `DeleteRelationshipsBatch`.
- **Lock ordering preserved** — all `defer bs.idxMu.Unlock()` patterns in mutating methods were converted to explicit unlock before the sync flush call, maintaining the invariant: `idxMu` is released before `flush()` acquires `wbMu` then `idxMu.RLock()` internally (B15, lock-ordering rule).
- **B22 compliance** — flush path already guards with `bs.dbClosed.Load()`, so sync writes cannot hang on closed DB.
- 5 new tests in `pkg/graph/sync_write_test.go`: `TestSyncWrite_ConfigPassthrough`, `TestSyncWrite_DataSurvivesWithoutClose`, `TestSyncWrite_FlushIntervalIgnored_WhenSyncWrites`, `TestSyncWrite_ReadOnly_SyncWritesIgnored`, `TestSyncWrite_Graph_ConfigPassthrough`.

## [3.0.47] - 2026-03-03

### Added (AuthorizedBy + AuthorizationLevel — Phase 4.16)

- **`NodeIntegrity.AuthorizedBy string`** / **`NodeIntegrity.AuthorizationLevel uint8`** — optional caller-supplied authorization fields. Set by passing `"tkg_authorized_by"` (string) and `"tkg_auth_level"` (uint8 or int for JSON round-trip safety) in the `props`/`updates` map of any Add or Update call. Stripped before `PropertySlice.Set` (never stored in PropertySlice).
- **`RelIntegrity.AuthorizedBy string`** / **`RelIntegrity.AuthorizationLevel uint8`** — same pattern on relationship integrity.
- **`ShadowAuthorizedBy = "tkg_authorized_by"`** / **`ShadowAuthLevel = "tkg_auth_level"`** — new shadow constants in `pkg/types/shadow.go`. Both accessible via `ResolveNodeProperty` and `ResolveRelProperty`.
- **`extractProvenance(props)`** extended — now also extracts `tkg_authorized_by` and `tkg_auth_level` from any props/updates map. Zero-allocation fast path preserved (B23 compliant): no allocation when none of the 4 reserved keys are present. Accepts `int`, `int32`, `int64`, `float64` for `tkg_auth_level` for JSON round-trip compat.
- **Wire persistence** — `AuthorizedBy` (`msgpack:"aby"`) and `AuthorizationLevel` (`msgpack:"al"`) added to both `nodeWire` and `relWire`. Backward-compatible (`omitempty`).
- **Layout test updated** — `NodeIntegrity` size: 72 → 96 bytes; `RelIntegrity` size: 104 → 128 bytes.
- 10 new tests in `pkg/graph/integrity_authz_test.go`: `TestAuthorizedBySetOnAdd_Node`, `TestAuthorizedBySetOnAdd_Rel`, `TestAuthLevelSetOnAdd_Node`, `TestAuthLevelSetOnAdd_Rel`, `TestAuthLevelAcceptsInt_Node`, `TestAuthzPreservedOnUpdate`, `TestAuthzViaShadow_Node`, `TestAuthzViaShadow_Rel`, `TestAuthLevelViaShadow_Node`, `TestNoAuthz_DefaultsZero`.

## [3.0.46] - 2026-03-03

### Added (Portable Export/Import — Phase 4.15)

- **`Graph.ExportGraph(w io.Writer) error`** — writes a portable format-independent snapshot of the entire graph to `w`. Snapshot includes: header, label/reltype registries, all current nodes and rels, and their full version history. Holds `g.mu.RLock` for the duration (consistent snapshot).
- **`Graph.ImportGraph(r io.Reader) error`** — reads an export stream and restores it into the graph. Registries are imported if empty; if already populated, the existing registry is kept (idempotent). Holds `g.mu.Lock` for the duration (serialised restore).
- **Wire format** — length-prefixed msgpack record stream with 1-byte type tags: `0x01` header, `0x02` registry, `0x03` node, `0x04` node history, `0x05` rel, `0x06` rel history. Each record is `[tag(1)] [len(4BE)] [msgpack body]`. Unknown tags are rejected with `ErrCorruptExport`.
- **`ErrIncompatibleExport`** — returned when the export stream version is not supported by this binary.
- **Two-phase ForEach pattern (C4)** — collect IDs in ForEachNodeID/ForEachRelID/ForEachNodeHistoryID/ForEachRelHistoryID callbacks; fetch entities after callback returns. Current Store implementations invoke callbacks outside backend locks and shard checkouts. OOM-safe on large graphs.
- 12 new tests in `pkg/graph/export_test.go`: `TestExportImport_RoundTrip_MemoryStore`, `TestExportImport_RoundTrip_BadgerStore`, `TestExport_Empty_Graph`, `TestExport_WithNodeHistory`, `TestExport_RelHistory`, `TestImport_IdempotentRegistry`, `TestExport_Writer_Error`, `TestImport_InvalidHeader`, `TestExportImport_IntegrityPreserved`, `TestExportImport_EndpointHashesPreserved`, `TestExportImport_AuthorIDPreserved`, `TestExport_ShadowProperty_Survives`.

## [3.0.45] - 2026-03-03

### Added (AuthorID + Signature on Integrity — Phase 4.14)

- **`NodeIntegrity.AuthorID string`** / **`NodeIntegrity.Signature []byte`** — caller-supplied provenance fields. Set by passing `"tkg_author_id"` (string) and `"tkg_signature"` ([]byte) in the `props`/`updates` map of any Add or Update call. Stripped before `PropertySlice.Set` (never stored in PropertySlice).
- **`RelIntegrity.AuthorID string`** / **`RelIntegrity.Signature []byte`** — same pattern on relationship integrity.
- **`ShadowAuthorID = "tkg_author_id"`** / **`ShadowSignature = "tkg_signature"`** — new shadow constants in `pkg/types/shadow.go`. Both accessible via `ResolveNodeProperty` and `ResolveRelProperty`.
- **`extractProvenance(props)`** (unexported) — helper in `context.go` that extracts `tkg_author_id` and `tkg_signature` from any props/updates map without mutating the caller's map. Zero-allocation fast path when neither key is present.
- **Wire persistence** — `AuthorID` (`msgpack:"aid"`) and `Signature` (`msgpack:"sig"`) added to both `nodeWire` and `relWire`. Backward-compatible (`omitempty`); old data reads as zero values.
- **Layout test updated** — `NodeIntegrity` size: 32 → 72 bytes; `RelIntegrity` size: 32 → 104 bytes.
- 11 new tests in `pkg/graph/integrity_author_test.go`: SetOnAdd (node + rel), SignatureSetOnAdd (node + rel), PreservedOnUpdate, ViaShadow (node + rel, both fields), DefaultsEmpty, DoesNotAffectHash.

## [3.0.44] - 2026-03-03

### Added (RelIntegrity Endpoint Hashes — Phase 4.13)

- **`RelIntegrity.FromNodeHash string`** — hash of the start node at the time this relationship version was written. NOT fed into `ComputeRelHash` (prevents cascading hash invalidation on node updates). Used for cross-validation.
- **`RelIntegrity.ToNodeHash string`** — hash of the end node at write time.
- **`ShadowFromHash = "tkg_from_hash"`** / **`ShadowToHash = "tkg_to_hash"`** — new shadow constants. Accessible via `ResolveRelProperty`; return `(nil, false)` on nodes (rel-only).
- **`AddRelationshipWithContext`** — captures `startNode.Integrity().Hash` → `ig.FromNodeHash` and `endNode.Integrity().Hash` → `ig.ToNodeHash` under the endpoint lock. Empty string if endpoint has no integrity.
- **`UpdateRelationshipWithContext`** — refreshes `FromNodeHash`/`ToNodeHash` from the store on each update, capturing the current endpoint hashes at write time.
- **Wire persistence** — `FromNodeHash` (`msgpack:"fnh"`) and `ToNodeHash` (`msgpack:"tnh"`) added to `relWire`. Backward-compatible (`omitempty`).
- 5 new tests in `pkg/graph/rel_endpoint_hash_test.go`: `TestFromNodeHashStoredOnAdd`, `TestEndpointHashFromShadow`, `TestEndpointHashPreservedOnUpdate`, `TestEndpointHashSelfLoop`, `TestEndpointHashNotOnNode`.

## [3.0.43] - 2026-03-02

### Added (VectorField Index — Phase 4.9)

- **`DistanceMetric uint8`** — `DistanceCosine` and `DistanceEuclidean` constants.
- **`vectorIndex`** (unexported) — in-memory brute-force k-NN index: `add`, `remove`, `searchNearest`. O(n × dims) per query. Thread-safe via `sync.RWMutex`. `add` replaces existing entry for same ID (upsert).
- **`[]float32` property support** — added to `wire.go` (`ptSliceF32 = 24`), `integrity.go` (hash computation), and `propertyslice.go` (deep copy). `[]float32` values are now fully round-trip serializable.
- **`Store.CreateVectorIndex(labelToken, propertyKey, dims, metric)`** — creates in-memory k-NN index on nodes with the given label. Scans existing nodes to populate. Returns `ErrVectorIndexExists` on duplicate and `ErrInvalidVectorIndexConfig` for `dims <= 0` or unsupported distance metrics. Implemented in MemoryStore, BadgerStore, TieredStore.
- **`Store.DropVectorIndex(labelToken, propertyKey)`** — removes the index. Returns `ErrVectorIndexNotFound` if not present.
- **`Store.SearchNearestNodes(labelToken, propertyKey, query, k, opts)`** — returns the k closest nodes by vector distance in ranked order. Returns `ErrVectorIndexNotFound` / `ErrDimensionMismatch` on error; nil slice (no error) if index is empty.
- **`Graph.CreateVectorIndex(label, propertyKey, dims, metric)`** / **`DropVectorIndex`** / **`SearchNearestNodes`** — Graph-layer API resolving label string to token. Current `CreateVectorIndex` creates the label token if needed so the index applies to future matching nodes.
- **Auto-maintenance** — all mutation paths (PutNode, ReplaceNode, DeleteNode, RemoveNodeLabelToken) update vector indexes in MemoryStore, BadgerStore, and TieredStore.
- **`ErrVectorIndexExists`** / **`ErrVectorIndexNotFound`** / **`ErrDimensionMismatch`** / **`ErrInvalidVectorIndexConfig`** — vector sentinel errors canonical in `pkg/graph/store` and aliased by the `graph` and `pkg/graph/index` packages.
- **TieredStore** holds vector indexes at the store level (not per-shard) with its own `vectorIdxMu sync.RWMutex`.
- **Persistence shape** — vector entries are in-memory; current BadgerStore and TieredStore behavior persists index definitions and rebuilds entries from node properties on open.
- Internal-package tests in `vector_badger_test.go` covering BadgerStore and TieredStore implementations directly (12 tests). External-package tests in `vector_index_test.go` (12 tests).

## [3.0.42] - 2026-03-02

### Added (Recurrence Patterns — Phase 4.7)

- **`RecurrenceFrequency uint8`** — `RecurrenceDaily`, `RecurrenceWeekly`, `RecurrenceMonthly`, `RecurrenceYearly`.
- **`WeekdayMask uint8`** — bit-per-weekday bitmask: `MaskMonday` (bit 0) through `MaskSunday` (bit 6), plus `MaskWeekdays`, `MaskWeekend`, `MaskAllDays` composites.
- **`Interval`** — `{Start, End Instant}` — closed-open `[Start, End)` temporal interval.
- **`RecurrencePattern`** — struct with `Frequency`, `Days` (WeekdayMask), `DayOfMonth` (1–28; 0 = last day of month), `Month` (time.Month for Yearly), `DayStart`/`DayEnd` (whole-millisecond time.Duration from UTC midnight).
- **`RecurrencePattern.Validate()`** — validates frequency, non-empty Days for Daily/Weekly, supported weekday-mask bits only, DayStart < DayEnd with whole-millisecond offsets, DayOfMonth ∈ [0, 28], and Yearly Month ∈ [January, December] or 0.
- **`RecurrencePattern.Expand(from, to)`** — walks days from `TruncateInstant(from, GranDay)` to `TruncateInstant(to, GranDay)`, checks day-of-week / day-of-month / month match, emits `[day+DayStart, day+DayEnd)` clipped to `[from, to)`. All calculations UTC. Returns `types.ErrInvalidTimeRange` if `from >= to`.
- 8 direct tests in `pkg/types/recurrence_test.go`: Daily_Weekdays, Weekly_Monday, Monthly_NthDay, Yearly, Clipped, EmptyResult, Validate_Errors (7 sub-cases), Expand_InvalidRange.

## [3.0.41] - 2026-03-02

### Added (Remove Label from Node — Phase 4.10)

- **`Node.RemoveLabelTokenRaw(tok uint16) bool`** — removes a label token from the node's label set. If `tok` is the primary label, promotes `extraLabels[0]` to primary. Returns false if `tok == 0`, not present, or would remove the last remaining label.
- **`Store.RemoveNodeLabelToken(id, tok, updatedNode)`** — removes `tok` from the label index for `id` and persists `updatedNode` (no version bump, no history entry). Implemented in MemoryStore, BadgerStore, and TieredStore.
- **`Graph.RemoveNodeLabel(id snowflake.ID, label string)`** — resolves label string to token, locks the entity, validates the node has the label and has more than one label, deep-copies + mutates, recomputes hash (preserving PrevHash), delegates to `store.RemoveNodeLabelToken`, publishes `EventNodeUpdate`, increments `opNodeUpdates`.
- **`ErrLabelNotFound`** — new sentinel: `"graph: node does not have the specified label"`.
- **`ErrLastLabel`** — new sentinel: `"graph: cannot remove the last label from a node"`.
- 8 new tests in `pkg/graph/remove_label_test.go`: ExtraLabel, PrimaryPromotesExtra, LastLabelError, LabelNotFoundError, NodeNotFoundError, HashUpdated, NodesByLabelUpdated, PublishesEvent.
- Internal-package tests in `vector_badger_test.go` covering BadgerStore and TieredStore `RemoveNodeLabelToken` directly.

## [3.0.40] - 2026-03-02

### Added (Time Granularity + In-Place Update + Graph Stats — Phases 4.8, 4.11, 4.12)

**Time Granularity (4.8)**
- **`TimeGranularity uint8`** — 8 levels: `GranMillisecond` (1) through `GranYear` (8).
- **`TruncateInstant(t, g)`** — floors `t` to the nearest `g` boundary (UTC). Week truncation floors to Monday midnight.
- **`RoundInstant(t, g)`** — rounds to nearest boundary (ties ceil).
- **`CeilInstant(t, g)`** — smallest boundary ≥ `t`.
- Table-driven tests in `pkg/types/granularity_test.go` covering all 3 functions × 8 granularities, plus on-boundary and week-day edge cases.

**In-Place Update (4.11)**
- **`Graph.UpdateNodeInPlace(id, updates)`** / **`UpdateNodeInPlaceWithContext`** — updates node properties without bumping the version or writing a history entry. Uses `store.ReplaceNode` (not `ReplaceNodeWithHistory`). Preserves existing `PrevHash`. Publishes `EventNodeUpdate`. Increments `opNodeUpdates`.
- **`Graph.UpdateRelInPlace(id, updates)`** / **`UpdateRelInPlaceWithContext`** — rel mirror.
- 12 new tests in `pkg/graph/inplace_test.go`: NoHistoryEntry, VersionUnchanged, PropertiesUpdated, NoOp, PublishesEvent (× Node/Rel), WithContext_Cancelled, CountedAsUpdate.

**Graph Stats (4.12)**
- **`GraphStats`** — struct with 8 operation counters (`NodesAdded`, `NodesRead`, `NodesUpdated`, `NodesDeleted`, `RelsAdded`, `RelsRead`, `RelsUpdated`, `RelsDeleted`) and 4 cache metrics (`NodeCacheHits`, `NodeCacheMisses`, `RelCacheHits`, `RelCacheMisses`).
- **`StoreStats`** (unexported interface) — optional interface type-asserted in `Graph.Stats()`. Avoids polluting the `Store` interface. `BadgerStore` implements it.
- **8 `atomic.Int64` fields** on `Graph` struct: `opNode{Adds,Reads,Updates,Deletes}` + rel mirrors. Incremented after every successful store write in `context.go`.
- **LRU hit/miss tracking** — `entityLRU` gains `hits` and `misses atomic.Int64`. `Get()` increments on cacheMiss (miss) and on cacheHit/cacheDeleted (hit). `Hits()`/`Misses()` accessors.
- **`BadgerStore` implements `StoreStats`** via `nodeCache.Hits()`/`Misses()` + rel mirrors.
- 8 new tests in `pkg/graph/stats_test.go`: InitialState, NodeCounters, RelCounters, EmptyUpdate_NoUpdateIncrement, CacheMetrics_MemoryStore_Zero, CacheMetrics_BadgerStore, UpdateNodeInPlace_CountsAsUpdate, UpdateRelInPlace_CountsAsUpdate.

## [3.0.39] - 2026-03-02

### Added (CRUD Diff Exporter — Phase 4.6)

- **`NodeUpdate`** / **`RelUpdate`** — pair structs holding `Before` and `After` snapshots of a changed entity.
- **`SnapshotDiff`** — result type with `T1`, `T2`, `NodesCreated`, `NodesUpdated`, `NodesDeleted`, `RelsCreated`, `RelsUpdated`, `RelsDeleted`.
- **`Graph.DiffSnapshots(t1, t2)`** — compares two temporal snapshots under a single `g.mu.RLock` (prevents torn reads). Returns `*SnapshotDiff` classifying each entity as Created (present only at t2), Deleted (present only at t1), or Updated (hash changed). Unchanged entities are omitted. Returns `ErrInvalidTimeRange` if `t1 >= t2` or either is zero.
- **`ErrInvalidTimeRange`** — new sentinel error in `graph` package.
- **`snapshotLocked`** (unexported) — inner body of `Snapshot(t)` extracted without the lock, allowing `DiffSnapshots` to hold the RLock across both snapshot reads (B15 compliance: no nested RLock).
- 15 new tests in `pkg/graph/diff_test.go`: invalid range, empty graph, created/deleted/updated/unchanged for nodes and rels, mixed scenario, nil integrity branches.

## [3.0.38] - 2026-03-02

### Added (Event / Notification System — Phase 4.5)

- **`EventType uint8`** — 6 constants: `EventNodeCreate`, `EventNodeUpdate`, `EventNodeDelete`, `EventRelCreate`, `EventRelUpdate`, `EventRelDelete`.
- **`Event`** — struct with `Type EventType`, `EntityID snowflake.ID`, `Timestamp types.Instant`.
- **`EventHandler`** — type alias for `func(Event)`.
- **`EventBus`** — dispatcher with `Subscribe(handler) func()` (returns idempotent unsubscribe via `sync.Once`) and unexported `publish(e Event)`. Handlers are copied under `RLock`, then invoked outside the lock to prevent deadlocks when handlers re-enter the Graph.
- **`NewEventBus()`** — constructor.
- **`Graph.SetEventBus(bus)`** / **`Graph.GetEventBus()`** — attach/retrieve the event bus. Nil by default (zero overhead for callers not using events).
- 6 hook points wired in `context.go` after each successful store write: `AddNode`→`EventNodeCreate`, `AddRelationship`→`EventRelCreate`, `UpdateNode`→`EventNodeUpdate`, `UpdateRelationship`→`EventRelUpdate`, `DeleteNode`→`EventNodeDelete`, `DeleteRelationship`→`EventRelDelete`.
- `CloseNodeVersion` / `CloseRelVersion` also publish `EventNodeUpdate` / `EventRelUpdate`.
- 13 new tests in `pkg/graph/events_test.go`: subscribe/unsubscribe, idempotent unsubscribe, multiple handlers, nil-default graph, all 6 CRUD event types, async handler, CloseNodeVersion/CloseRelVersion events.

## [3.0.37] - 2026-03-02

### Added (Version Chain Navigation — Phase 4.4)

- **`Graph.GetPreviousNodeVersion(id, version)`** — returns the version immediately before `version`. Returns `nil, nil` if `version == 0` (genesis has no predecessor) or the predecessor does not exist in history.
- **`Graph.GetNextNodeVersion(id, version)`** — returns the version immediately after `version`. Checks the history store first for `version+1`, then falls back to the current entity (which may itself be `version+1`). Returns `nil, nil` if no newer version exists (current tip or deleted node with a version gap).
- **`Graph.CloseNodeVersion(id, t)`** — sets `ValidTo = t` on the current node in-place via `ReplaceNode` (no new version, no history entry). Recomputes the integrity hash preserving `PrevHash`. Returns `ErrAlreadyClosed` if `ValidTo` is already non-zero; returns `ErrNodeNotFound` if the node does not exist. Updates temporal indexes via `ReplaceNode`.
- **`GetPreviousRelVersion`** / **`GetNextRelVersion`** / **`CloseRelVersion`** — exact mirrors of the node methods for relationships.
- **`ErrAlreadyClosed`** — new sentinel error in `graph` package.
- 18 new tests in `pkg/graph/version_chain_test.go`: genesis/tip boundaries, normal prev/next traversal, through-history path, deleted node/rel edge cases, version gap after truncation, CloseNodeVersion sets ValidTo, ErrAlreadyClosed on second close, ErrNodeNotFound on missing entity, rel mirrors.

## [3.0.36] - 2026-03-02

### Added (Temporal Constraints + Advanced Temporal Indexes — Phases 4.2 + 4.3)

**Temporal Constraints (4.2)**

- **`TemporalConstraintKind`** — enum type for constraint kinds. Initial kind: `ConstraintRelWithinEndpoints`.
- **`TemporalConstraint`** — struct binding a `TemporalConstraintKind` to optional parameters.
- **`ConstraintSet`** — value type (zero value = no constraints). Holds a slice of `TemporalConstraint`. Passed via `Graph.Config`; zero overhead when unused.
- **`ConstraintRelWithinEndpoints`** — enforces that a relationship's validity interval (`[ValidFrom, ValidTo)`) is a subset of the intersection of both endpoint nodes' validity intervals. Evaluated in `AddRelationshipWithContext` and `ImportRelationshipWithID` before the store write.
- **`ErrTemporalConstraint`** — base sentinel error; 6 specific leaf errors wrap it so callers can use `errors.Is` on either the outer or the specific leaf: `ErrConstraintStartNodeOpen`, `ErrConstraintEndNodeOpen`, `ErrConstraintRelBeforeStart`, `ErrConstraintRelAfterEnd`, `ErrConstraintRelStartTooEarly`, `ErrConstraintRelEndTooLate`.
- 13 new tests in `pkg/graph/temporal_constraint_test.go`: no-op on zero ConstraintSet, valid rel within both endpoints, rel starts before node, rel ends after node, open-ended node (no ValidTo), import path enforcement, errors.Is wrapping for each leaf error, interaction with existing temporal filters.

**Advanced Temporal Indexes (4.3)**

- **`temporalIndex`** (unexported) — sorted-slice interval index on `[ValidFrom, ValidTo)`. Binary search insertion (O(log n)); point-in-time and interval queries are O(n) scan with early exit. Thread-safe via `sync.RWMutex`. Stored per label token on the Store.
- **`Store.CreateTemporalIndex(labelToken)`** — installs a temporal index for the given label; scans existing nodes to populate. Returns `ErrTemporalIndexExists` on duplicate. Implemented in MemoryStore, BadgerStore, and TieredStore.
- **`Store.DropTemporalIndex(labelToken)`** — removes the temporal index. Returns `ErrTemporalIndexNotFound` if not present.
- **`Graph.CreateTemporalIndex(label)`** / **`Graph.DropTemporalIndex(label)`** — Graph-layer API resolving label string to token before delegation.
- **`NodesByLabel` temporal fast path** — when a temporal index is active for the queried label and `QueryOpts` carries a `ValidAt` or `ValidStart`/`ValidEnd` filter, `NodesByLabel` uses the index to narrow candidate IDs before fetching entities, avoiding a full label scan.
- **BadgerStore persistence** — temporal index label tokens are persisted to Badger under a meta key (same 3-phase creation pattern as property indexes). On startup `loadIndexes()` reads persisted token set and rebuilds index data by scanning matching nodes; indexes survive restart.
- **TieredStore delegation** — `CreateTemporalIndex`/`DropTemporalIndex` delegate to all currently open shards. `tempIndexLabels` set on `TieredStore` ensures each new hot shard created during rotation inherits all active temporal indexes immediately.
- **MemoryStore** — full integration: index created/dropped/maintained across all node mutation paths (`PutNode`, `ReplaceNode`, `DeleteNode`, `RemoveNodeLabelToken`).
- **`ErrTemporalIndexExists`** / **`ErrTemporalIndexNotFound`** — new sentinel errors in `graph` package.
- 11 new unit tests in `pkg/graph/temporal_index_test.go` + helper tests: create/duplicate/drop/not-found, NodesByLabel fast path (point-in-time, interval), BadgerStore persistence round-trip, TieredStore delegation, new hot shard inherits index on rotation, MemoryStore mutation maintenance.

### Fixed

- **B22: Badger `WriteBatch.Flush` blocks on closed DB** — `flush()` in `badgerstore.go` now checks `bs.closed` (atomic flag) before calling `WriteBatch.Flush()`. Previously, a flush triggered after `Close()` would block indefinitely because Badger's `WriteBatch.Flush` waits on an internal channel that is never drained once the DB is closed. Fix: early-return with `ErrDBClosed` when the closed flag is set; re-queue is skipped. `flushLoop` goroutine exit path updated accordingly.

## [3.0.35] - 2026-03-02

### Fixed (Carry-Forward Coverage Gaps)

- **`ImportNames` validation** (`label_registry.go`, `reltype_registry.go`) — added rejection of whitespace-only and empty strings at positions > 0, and duplicate names. Uses `strings.TrimSpace` to match the invariant enforced by `GetOrCreate`. Previously, importing `["", "Foo", "Foo"]` silently corrupted the reverse map; importing `["", " ", "Foo"]` mapped token 1 to a whitespace-only string that `GetOrCreate` would refuse to produce, creating an inconsistent registry.
- **`propertyTypeTag` coverage** — direct unit tests covering all 24 branches: `int8`, `int16`, `int32`, `uint`, `uint8`–`uint32`, `uint64`, `float32`, and the `default` unknown-type branch (64% → 100%).
- **`toInt64` / `toUint64` coverage** — direct unit tests covering all 9 type cases plus the `default` zero-return branch each (54.5% → 100%).
- **`normalizeIntegersRecursive` coverage** — direct unit tests for `int8`, `int16`, `int32`, `uint8`, `uint16`, `uint32`, `[]any` recursion, `map[string]any` recursion, and the `default` passthrough (40% → 100%).
- **`flush()` WriteBatch error path** (`badgerstore.go`) — test closes the underlying Badger DB before `WriteBatch.Flush()`, exercising the requeue-on-error path with correct goroutine lifecycle via `t.Cleanup` (73% → covered).

## [3.0.34] - 2026-03-02

### Added (Allen's 13 Interval Relations — Gap A1)

- **`types.AllenRelation`** — `uint8` iota (1..13) representing Allen's 13 interval relations: Before, After, Meets, MetBy, Overlaps, OverlappedBy, Starts, StartedBy, During, Contains, Finishes, FinishedBy, Equals. Zero value is invalid (catches uninitialized usage). Methods: `String()`, `Symbol()`, `Inverse()`, `Set()`.
- **`types.AllenRelationSet`** — compact `uint16` bitset for sets of relations. Methods: `Contains`, `Add`, `Union`, `Intersection`, `IsEmpty`, `Len`, `ToSlice`, `String`, `InverseSet`. `AllRelations()` returns the full 13-element set.
- **`types.Relate(aStart, aEnd, bStart, bEnd)`** — classifies the Allen relation between two `[start, end)` intervals. Returns `ErrOpenInterval` for zero endpoints, `ErrInvalidInterval` for start >= end.
- **`types.Compose(r1, r2)`** / **`types.ComposeSets(a, b)`** — composition table (computed at `init()` via exhaustive enumeration of 21 intervals over a 7-point timeline). Returns the set of possible relations when chaining two known relations.
- **`graph.NodeInterval(n)`** / **`graph.RelInterval(r)`** — extracts effective `[start, end)` from a node/relationship, deriving start from snowflake ID timestamp when no explicit `ValidFrom` is set. Returns `ErrOpenInterval` if `ValidTo == 0`.
- **`graph.RelateNodes(a, b)`** / **`graph.RelateRels(a, b)`** — classifies Allen relation between two entities via their effective intervals.
- **`ErrOpenInterval`** / **`ErrInvalidInterval`** — new sentinel errors in `pkg/types`.
- 38 new tests in `pkg/types/allen_test.go`: error cases, all 13 relations, inverse symmetry, string/symbol, set operations, composition identity/known results/never-empty/singleton consistency.
- 10 new tests in `pkg/graph/temporal_allen_test.go`: open-ended error propagation, resolved intervals, snowflake-derived start, all 13 relations through Graph, inverse consistency, relationship intervals.

### Notes

- Purely additive — no existing files modified, no existing tests affected.
- Foundation for temporal constraints (A2) and advanced temporal indexes (A7).

## [3.0.33] - 2026-03-02

### Fixed (Pre-Release Code Review — 1 BLOCKER + 10 MAJORs + 16 MINORs)

**BLOCKER:**
- **checkoutStore TOCTOU race** — `activeReqs` increment moved inside `shardMu` for cold shards, eliminating race window between `getStore` return and ref-count increment that allowed `closeIdleShards` to close an in-use store.

**MAJORs:**
- **Atomic file persistence** — `atomicWriteFile` helper with `fsync` before rename in `shard_catalog.go` and `registry_file.go`. Prevents crash-induced corruption of catalog and registry files.
- **Registry save race** — new `SaveRegistries(labels, relTypes)` writes both registries atomically in a single file operation, eliminating read-modify-write race between `SaveLabelRegistry` and `SaveRelTypeRegistry`.
- **ShardCatalog thread safety** — added `sync.RWMutex` to `ShardCatalog`. All read methods take RLock, all write methods take Lock. `GetShard`/`HotEventShard` return copies instead of internal pointers.
- **append backing array corruption** — replaced `append(outRels, inRels...)` with explicit allocation in `context.go` and `tx.go` to prevent writing into spare capacity of the `outRels` slice.
- **ReplaceNode property index cleanup** — added `purgeNodeFromAllPropertyIndexes` fallback in `badgerstore.go` when `getNodeLocked` fails during Replace, preventing orphaned property index entries.
- **Float formatting precision** — replaced `fmt.Sprintf("%v")` with `strconv.FormatFloat` for float32/float64 in `property_index.go`, ensuring deterministic round-trip-safe index keys.
- **NodesByLabel event label fan-out** — `NodesByLabel`, `NodesByLabelAndProperty`, and `NodeCountByLabel` now fan out across all event shards (respecting `opts.Depth`), not just the hot shard.
- **Constructor warm shard leak** — added cleanup loop in `NewTieredStore` to close already-opened warm shard stores when a subsequent warm shard fails to open.
- **TieredStore.Close() error accumulation** — replaced `closeErr = fmt.Errorf(...)` with `errors.Join` so all close errors are reported, not just the last one.

**MINORs:**
- Removed unused `_ = attempt` variable in `context.go` retry loop
- `result.Failed += 1` → `result.Failed++` in `batch.go`
- Documented `Execute` return pattern and thread-safety in `batch.go`
- Defined `ErrNotTieredStore` sentinel, replaced 7 ad-hoc `fmt.Errorf` strings in `graph.go`
- Added `slog.Error` in `persistPropertyIndexDefs` for marshal failures
- Distinguished `ErrNodeNotFound` from real errors in property scan fallback
- Moved test-only `contains()` from `property_index.go` to `property_index_test.go`
- Removed redundant `archiveWritten`/`refWritten` guard variables in `tieredstore_write.go`
- Fixed `tkg_version` comment: `int` → `uint32` in `pkg/types/shadow.go`
- Documented `ValidStart+ValidEnd` both-required for interval filtering in `temporal_filter.go`
- Added `panic("unreachable")` in `writePropertyValue` default case in `integrity.go`
- Rewrote `MigrateFromBadger` to use `ForEachNodeID`/`ForEachRelID` pagination instead of materializing all entities

## [3.0.32] - 2026-03-02

### Added

- **`ImportNodeWithID(ctx, id, labels, props)`** — creates a node with a caller-specified snowflake ID. Returns `ErrNodeExists` if the ID is already in use, `ErrZeroID` if id == 0. Used for backup/restore where ID preservation is required.
- **`ImportRelationshipWithID(ctx, id, typeName, startNode, endNode, props)`** — creates a relationship with a caller-specified snowflake ID. Returns `ErrRelExists` on collision.
- **`GraphTx.ImportNodeWithID` / `GraphTx.ImportRelationshipWithID`** — transaction wrappers for both import methods, tracked for rollback.
- **`ErrZeroID`** — new sentinel error for zero ID validation in import methods.
- 8 new tests in `import_test.go`: basic, collision, zero ID, validation, rel import, tx commit, tx rollback.

## [3.0.31] - 2026-03-02

### Added (OOM Fix — Lazy ForEach Iterators for Temporal Pipeline)

- **`ForEachNodeID(fn)` / `ForEachRelID(fn)`** — lazy iterator over all current entity IDs. Callback returns `true` to continue, `false` to stop. Implemented on MemoryStore (map iteration under RLock), BadgerStore (index map iteration under `idxMu.RLock`), and TieredStore (sequential shard iteration with checkout/checkin — one shard open at a time).
- **`ForEachNodeHistoryID(fn)` / `ForEachRelHistoryID(fn)`** — lazy iterator over all entity IDs with version history entries. BadgerStore: pending buffer scan + Badger prefix scan with dedup. TieredStore: sequential shard iteration.
- **`forEachKnownNodeID` / `forEachKnownRelID`** — two-phase temporal helpers replacing `allKnownNodeIDs`/`allKnownRelIDs`. Phase 1: collect unique IDs via ForEach callbacks into `seen`. Phase 2: process IDs after Store iteration returns.

### Changed

- **`GetNodesValidAt`**, **`GetRelationshipsValidAt`**, **`GetNodesValidDuring`**, **`GetRelationshipsValidDuring`** — rewritten to use `forEachKnownNodeID`/`forEachKnownRelID` instead of materializing all IDs into slices.
- **`Snapshot`** — benefits transitively (calls `GetNodesValidAt` + `GetRelationshipsValidAt`).

### Removed

- **`allKnownNodeIDs`** / **`allKnownRelIDs`** — replaced by `forEachKnownNodeID`/`forEachKnownRelID`.

### Memory Impact

- Eliminates per-shard `[]snowflake.ID` slices and `mergeIDSlices` allocations in the temporal query pipeline. For 10M nodes across 12 shards, reduces peak memory from ~928 MB to ~160 MB (~83% reduction for the ID collection phase).

### Tests

- 20 new tests: MemoryStore ForEach (node IDs, early stop, empty, rel IDs, node history, rel history), BadgerStore ForEach (node IDs, early stop, rel IDs, node history with dedup, node history early stop, rel history), TieredStore ForEach (all shards, early stop, with rotation, rel IDs, rel ID early stop, node history, rel history), Graph-level temporal query integration.

## [3.0.30] - 2026-03-02

### Fixed (5 Concurrency & Data Consistency Bugs)

- **`idleCloseLoop` race condition** — `getStore()` returned a `*BadgerStore` pointer and released `shardMu`, allowing `closeIdleShards()` to close the store while callers were still using it. Added `checkoutStore()` / `checkinStore()` with `atomic.Int64` `activeReqs` per shard. `closeIdleShards()` now skips shards with `activeReqs > 0`. Applied to all 10 parallel merge goroutines and 3 sequential count methods.
- **`shardForRelID` unnecessary cold shard probing** — fallback probe opened ALL event shards including cold ones. Cross-shard rels are only created on hot/warm shards, so cold probing is unnecessary and expensive. Fallback now skips `TierCold` shards.
- **`ArchiveNode`/`RestoreNode` missing rollback** — if step 5 (write to destination) partially succeeded then failed, or step 6 (delete from source) failed, data was duplicated across both shards. This release added destination-shard cleanup on failure; current Unreleased code supersedes it with relationship-placement rollback and surfaced rollback errors.
- **`CreatePropertyIndex` concurrent delete resurrection** — Phase 3 used `liveIdx.contains(id)` which returned false after a concurrent delete (ID removed from all value buckets), causing Phase 3 to re-add the stale backfill value. Added `propertyIndex.mutated` dirty-map: `add()` and `remove()` track all mutated IDs during Phase 2. Phase 3 checks `mutated[id]` instead of `contains(id)`.
- **`BatchBuilder.AddNode` hash mismatch** — hashed raw user-supplied labels (potentially with duplicates) instead of canonical deduplicated labels from the registry. `VerifyNodeHashChain` later used canonical labels, causing permanent verification failure. Now uses `b.g.NodeLabels(n)` for hash computation.

### Changed

- **`eventShard` struct** — new field `activeReqs atomic.Int64` for reference counting cold shard access. New methods `checkoutStore()` / `checkinStore()`.
- **`propertyIndex` struct** — new field `mutated map[snowflake.ID]struct{}` (non-nil only during index creation Phase 2).

### Tests

- 12 new tests: idle-close blocked by active request, concurrent checkout/checkin during idle-close, shardForRelID skips cold shards, shardForRelID finds in warm shard, ArchiveNode/RestoreNode rollback (2), CreatePropertyIndex concurrent delete/update (2), BatchBuilder duplicate label hash, BatchBuilder hash chain verification.

## [3.0.29] - 2026-03-02

### Added (Phase 3e — Repair + Tooling)

- **`DecomposeID(snowflake.ID)`** — extracts `IDComponents{CreatedAt, NodeID, Sequence}` from snowflake ID using `snowflakeLayout.Decompose()`. Package-level function, also accessible via `Graph.DecomposeID`.
- **`TieredStore.ForceRotate()`** — safe hot-shard rotation with internal locking (unlike `RotateHotShard()` which expects the caller to hold `ts.mu.Lock`). Accessible via `Graph.ForceRotate()`.
- **`TieredStore.ListShards()`** — returns `[]ShardInfo` for all shards (reference, archive, event), enriched with live node/rel counts from open stores. Accessible via `Graph.ListShards()`.
- **`TieredStore.RebuildCatalog()`** — reconstructs the shard catalog from live in-memory state, updating node/rel counts and tier info for all open shards. Accessible via `Graph.RebuildCatalog()`.
- **`TieredStore.VerifyShard(g, shardName)`** — runs hash chain verification on all entities in a named shard. For immutable shards (warm/cold) that have already passed verification, returns the cached result without re-scanning. Caches successful results in the catalog. Accessible via `Graph.VerifyShard(name)`.
- **`TieredStore.RunRepair()`** — cross-shard split-write consistency repair. Phase 1: detects orphaned in/ entries (entity missing from all shards) and deletes them. Phase 2: detects missing in/ entries (entity exists but in/ missing in end shard) and re-creates them. Returns `RepairResult` with counts. Accessible via `Graph.RunRepair()`.
- **`MigrateFromBadger(src, dst, labels)`** — copies all nodes and relationships from a single BadgerStore into a TieredStore with automatic ontology-based routing. No history migration (hash chains would need re-creation).
- **`ErrEventPropertyIndex`** — sentinel error returned when `CreatePropertyIndex` is called for an event label in TieredStore. Property indexes are only supported for reference entities.
- **`ShardInfo`** struct — describes a shard for admin queries: `Name`, `Kind`, `Tier`, `TimeStart`, `TimeEnd`, `Nodes`, `Rels`, `Open`, `Verified`.
- **`VerifyResult`** struct — holds per-shard hash chain verification outcome: `ShardName`, `NodesOK`, `RelsOK`, `NodesFailed`, `RelsFailed`, `Cached`.
- **`RepairResult`** struct — holds repair scan outcome: `OrphanedInEntries`, `MissingInEntries`, `ShardsScanned`, `CrossShardRelsChecked`.
- **`deleteIncomingByRelID`** on BadgerStore — removes an orphaned in/ entry by scanning for matching relID when relType and startID are unknown (entity is gone). Scans pending buffer first, falls back to Badger prefix scan.
- **`UpdateShardVerified`** / **`UpdateShardStats`** on ShardCatalog — field updates for verification caching and catalog rebuild.
- **~29 new tests** — ID decomposition (known values, time precision, node field, temporal filter consistency), property index restriction (ref label, event rejected, errors.Is), catalog extensions, admin API (ForceRotate, ListShards initial/after rotation/with cold/live stats, RebuildCatalog, admin not tiered), per-shard verification (hot, immutable cached, unknown shard), repair (no orphans, orphaned incoming, missing incoming, via Graph), migration (empty, nodes only, with rels, cross-shard rel).

## [3.0.28] - 2026-03-02

### Added (Phase 3d — Cold Shard Lifecycle, Parallel Queries, Reference Archive)

- **Cold shard lazy-open** — `eventShard.getStore(ts)` opens cold shards on first access with per-shard `shardMu` mutex and `atomic.Int64` `lastAccess` tracking. Cold shards are NOT opened on startup (recovered from catalog with `store=nil`).
- **Idle-close goroutine** — `idleCloseLoop()` periodically checks cold shards and closes those idle longer than `IdleTimeout` (default 5min when `ColdAfter > 0`), reclaiming memory.
- **`ColdAfter`** / **`IdleTimeout`** config — `ColdAfter` sets the warm→cold demotion threshold (0=never, non-negative). `IdleTimeout` sets cold shard auto-close delay (0=disabled/defaulted when cold is enabled; positive values must be >=1ms).
- **Parallel shard queries** — 10 merge query methods (`AllNodes`, `AllRelationships`, `AllNodeIDs`, `AllRelIDs`, `NodeCount`, `RelationshipCount`, `NodeCountByLabel`, `RelCountByType`, `AllNodeHistoryIDs`, `AllRelHistoryIDs`) launch concurrent goroutines per event shard via `sync.WaitGroup`. Reference shard runs sequentially first.
- **Reference archive** (`refArchive`) — lazy-opened BadgerStore at `data/archive/` for archiving closed/inactive reference entities.
- **`Graph.ArchiveNode(id)`** — moves a reference node and all connected relationships from `refShard` to `refArchive`. Returns `ErrNotReferenceEntity` for event entities.
- **`Graph.RestoreNode(id)`** — moves an archived reference node and relationships back to `refShard`.
- **`shardForNodeID` archive fallback** — node lookup probes `refArchive` when `refShard` misses, enabling transparent reads of archived entities.

### Changed

- **`TieredStore.Close()`** — now closes `refArchive` if open, signals `closeCh` to stop idle-close goroutine.
- **`eventShardSnapshot(depth)`** — returns depth-filtered `[]*eventShard` under `mu.RLock` for merge queries.

## [3.0.27] - 2026-03-02

### Added (Phase 3a — TieredStore Infrastructure)

- **`TieredStore`** — new `Store` implementation routing entities across multiple BadgerStore instances by ontology classification. Reference entities (configured via `RefLabels`) go to a single reference shard; event entities go to time-windowed event shards. Shard resolution is O(1) via snowflake ID timestamp extraction (bits 22–62).
- **`TieredStoreConfig`** — `DataDir`, `InMemory`, `RefLabels`, `ShardWindow` (default 1 week), `CacheCapacity` (default 10K), `FlushInterval` (default 100ms), `ColdAfter` (warm→cold demotion threshold, non-negative), `IdleTimeout` (default 5min when `ColdAfter > 0`; positive values must be >=1ms).
- **`OntologyMapping`** / **`EntityClass`** — classifies labels as `ClassReference` (long-lived) or `ClassEvent` (time-windowed, default). Lazy token cache backed by label registry. Token 0 returns `ClassEvent`.
- **`ShardCatalog`** / **`ShardEntry`** / **`ShardKind`** / **`ShardTier`** — JSON-persisted catalog tracking all shards with atomic write (write-tmp + rename). Tracks time windows, labels, rel types, tier (hot/warm/cold), kind (reference/event/archive). `UpdateShardTier`, `UpdateShardTimeEnd`, `HotEventShard`, `EventShards`, `ColdEventShards`.
- **`ShardDepth`** type — `DepthAll` (0, default), `DepthHot` (1), `DepthWarm` (2). Controls which shard tiers are included in TieredStore merge queries. `QueryOpts.Depth` field. Ignored by MemoryStore/BadgerStore (backward-compatible).
- **`BadgerStoreConfig.ReadOnly`** — opens Badger with `WithReadOnly(true)`, skips flushLoop and gcLoop. Used by TieredStore for warm/cold shards.
- **`registry_file.go`** — flat msgpack registry file save/load with atomic rename (write-tmp + rename). Used by TieredStore for label/reltype registry persistence separate from BadgerStore's in-DB persistence.
- **`badgerstore_partial.go`** — unexported helpers on `*BadgerStore` for TieredStore cross-shard relationship routing: `putRelEntityAndOut` (entity+typeIdx+outIdx), `putRelIncoming` (inIdx only), `deleteRelEntityAndOut`/`deleteRelIncoming` (split delete), `hasNodeID`/`hasRelID` (O(1) existence), `incomingRelIDs`/`outgoingRelIDs` (sorted ID snapshots).
- **`Graph.ArchiveNode(id)`** / **`Graph.RestoreNode(id)`** — move reference nodes and connected relationships between ref shard and ref archive. TieredStore only.
- **`ErrNotReferenceEntity`** sentinel error.

### Added (Phase 3b+3c — Shard Rotation, Warm Recovery, Cold Tier)

- **`TieredStore.RotateHotShard()`** — demotes current hot shard to warm (flush, mark read-only, ms-aligned boundary), creates new hot shard with contiguous window. Handles same-window collision via disambiguating suffix.
- **`checkRotation()`** on all new-entity write paths — fast-path time comparison (~1ns), slow-path Lock + double-check + rotate.
- **Warm shard recovery** — constructor reopens warm event shards from catalog as ReadOnly BadgerStore on restart. Mid-window restart via catalog `HotEventShard` resolution.
- **Cold shard support** — shards older than `ColdAfter` are demoted to `TierCold` during rotation. Cold shards are NOT opened on startup (lazy-open on first access via `eventShard.getStore()`). Idle-close goroutine reclaims memory by closing cold shards after `IdleTimeout`.
- **Depth-aware reads** — merge queries (`AllNodes`, `AllRelationships`, `AllNodeIDs`, `AllRelIDs`, `RelationshipsByType`, counts, history IDs) use `eventShardSnapshot(opts.Depth)` under `mu.RLock` to filter shard tiers.
- **Cross-shard relationships** — `PutRelationship` split-write with shard-based routing (`shardForNodeID`, not class-based) for correct E→E cross-shard after rotation. E→R: ref-first in/ per §12. `DeleteRelationship` split-delete. `DeleteNodeCascade` cross-shard aware. `IncomingRelationships` fetches each rel entity via `shardForRelID`.
- **Parallel shard queries** — merge queries launch concurrent goroutines per event shard.
- **~50 new tests** — ontology classification, shard catalog CRUD + persistence, registry file save/load/atomic, badgerstore partial helpers (split write/delete/existence/adjacency), TieredStore end-to-end (ref+event routing, rotation, warm recovery, cross-shard rels, counts, archive/restore, depth filtering, cold shards, idle-close).

### Changed (Phase 3a — GraphTx Full CRUD)

- **`GraphTx`** — upgraded from create-only to full CRUD with snapshot-based rollback. New methods: `UpdateNode`, `UpdateRelationship`, `SetNodeProperty`, `DeleteNodeProperty`, `SetRelationshipProperty`, `DeleteRelationshipProperty`, `DeleteNode` (cascade), `DeleteRelationship`. Rollback restores pre-mutation state in reverse order: deleted rels → deleted nodes → updated rels → updated nodes → created rels → created nodes. Current Unreleased code also restores pre-transaction history snapshots and truncates history for entities created then rolled back.
- **`Graph.Close()`** — type switch now handles `*TieredStore` in addition to `*BadgerStore` for registry persistence.
- **`Graph.New()`** — wires `TieredStore.SetLabelRegistry` and loads registries when Store is `*TieredStore`.

## [3.0.26] - 2026-03-02

### Fixed (Concurrency & Integrity — 3 Bugs)

- **`CreatePropertyIndex` concurrent data loss** — rewrote 3-phase approach: Phase 1 now installs an empty live index under write Lock (not RLock) so concurrent `PutNode`/`ReplaceNode` writes are captured immediately via `addNodeToPropertyIndexes`. Phase 2 builds backfill outside lock. Phase 3 merges backfill into live index, skipping IDs already handled by concurrent writes and deleted nodes. Previously, writes between Phase 1 (RLock) and Phase 3 (Lock) were silently dropped.
- **`ComputeNodeHash` hashed raw user labels** — `AddNodeWithContext` now calls `ComputeNodeHash(n, g.NodeLabels(n))` using canonical deduplicated labels from the node's internal tokens. Previously hashed the raw `labels` slice which could contain duplicates (e.g., `["Person", "Person"]`), causing `VerifyNodeHashChain` to fail because verification resolves canonical labels `["Person"]`.
- **`VerifyNodeHashChain`/`VerifyRelHashChain` failed on deleted entities** — both methods now tolerate `ErrNodeNotFound`/`ErrRelNotFound` for the current entity. When current is nil but history exists, the chain is built from history alone and labels/type name are extracted from the last history entry (tombstone). Returns `ErrNodeNotFound`/`ErrRelNotFound` only when neither current nor history exists.

### Added

- **`propertyIndex.contains(id)`** — O(V) check if a node ID exists in any value bucket. Used during `CreatePropertyIndex` merge phase to avoid overwriting concurrent writes.
- **6 new tests** — duplicate-label hash verification, duplicate-label deduplication, deleted entity hash chain verification (node + rel), never-existed hash chain (node + rel), concurrent `CreatePropertyIndex` with simultaneous writes, `propertyIndex.contains` unit test.

## [3.0.25] - 2026-03-02

### Added (Phase 2i — Temporal Query Push-Down + Graph Transactions)

- **Temporal push-down to Store layer** — `QueryOpts` gains `ValidAt types.Instant` (point-in-time filter) and `ValidStart`/`ValidEnd types.Instant` (interval filter). Zero values = no filter (backward-compatible). Both MemoryStore and BadgerStore filter entities at the persistence layer before deep-copy, dramatically reducing allocations for temporal queries.
- **`temporal_filter.go`** — package-level helpers `entityValidFrom(id, tm)` (derives valid-from from explicit `ValidFrom` or snowflake ID bit extraction) and `matchesTemporalFilter(id, tm, opts)` (evaluates point-in-time or interval overlap). Used by both Store implementations.
- **`entityLRU.Peek(key)`** — returns cached value and status without deep-copy or MRU promotion. Used by BadgerStore temporal pre-filter for zero-allocation cache-hit checks.
- **`entityLRU.Cap()`** — returns capacity. Used by `BadgerStore.Clear()` to recreate caches.
- **`GraphTx`** — mutation transaction holding graph write lock for duration. `Graph.BeginTx()` acquires write lock. `AddNode`/`AddRelationship` delegate to Graph and track IDs. `Commit()` releases lock. `Rollback()` deletes created entities in reverse order via `store.Delete*` (no tombstones — rolled-back creates vanish). `CreatedNodeIDs()`/`CreatedRelIDs()` for inspection. All methods return `ErrTxDone` after Commit/Rollback. (Later upgraded to full CRUD in v3.0.27.)
- **`Graph.Reset()`** — acquires write lock, calls `store.Clear()`, preserves registries. For atomic graph clearing.
- **`Store.Clear()`** — removes all entities, indexes, history, counters. MemoryStore reinitializes all maps. BadgerStore resets indexes, counters, caches, pending buffer, then calls `db.DropAll()`.
- **`ErrTxDone`** sentinel error — returned by GraphTx methods after Commit/Rollback.
- **~45 new tests** — 9 temporal filter unit tests, 4 LRU Peek/Cap tests, 12 MemoryStore/BadgerStore temporal push-down tests (ValidAt, pagination, AllNodes, NodesByLabelAndProperty, RelationshipsByType, interval), 12 GraphTx tests (commit/rollback/double-commit/double-rollback/add-after-done/concurrent/empty), 4 Reset tests (empty/clears entities/preserves registries/clears history), 4 Store.Clear tests.

### Changed

- **`GetNodesByLabelValidAt`** — now pushes `ValidAt` into `store.NodesByLabel(tok, QueryOpts{ValidAt: t})` instead of materializing all label matches and filtering in Go.
- **`NodesByLabelPropertyAndTime`** / **`NodesByLabelPropertyDuring`** — push temporal filters into `store.NodesByLabelAndProperty` via QueryOpts.
- **MemoryStore** — 5 paginated methods (`NodesByLabel`, `RelationshipsByType`, `AllNodes`, `AllRelationships`, `NodesByLabelAndProperty`) now apply temporal filtering before deep-copy using `matchesTemporalFilter` on in-memory entity pointers.
- **BadgerStore** — 5 paginated methods use two-stage filtering: `Peek` pre-filter for zero-allocation cache-hit temporal checks, then post-filter cache-miss candidates after `GetNode`/`GetRelationship`.

## [3.0.24] - 2026-03-02

### Added (Phase 2f — Cursor-Based Pagination)

- **`QueryOpts` struct** — `Limit int` (max results, 0 = no limit) and `After snowflake.ID` (keyset cursor, 0 = from start). Zero values mean "return all" for backward compatibility.
- **`paginateIDs` helper** (`pagination.go`) — shared binary-search cursor over sorted `snowflake.ID` slices, used by both MemoryStore and BadgerStore.
- **~27 pagination tests** — 8 `paginateIDs` unit tests, 8 MemoryStore integration (limit, multi-page walk, zero opts, indexed/fallback property query), 8 BadgerStore integration (mirrored), 3 Graph-layer passthroughs. All pass with race detector.

### Added (Phase 2g — Combined Label+Property+Temporal Queries)

- **`NodesByLabelPropertyAndTime(label, key, value, t)`** — intersects property index results with point-in-time temporal filter in a single call. Returns nodes matching all three axes.
- **`NodesByLabelPropertyDuring(label, key, value, start, end)`** — intersects property index results with interval temporal filter. Returns nodes valid during the given range.
- **7 combined query tests** — found (all axes match), property mismatch, temporal mismatch, unregistered label, with property index, interval overlap, no overlap.

### Changed

- **`Store` interface** — 5 unbounded query methods now accept `QueryOpts` parameter: `NodesByLabel(token, opts)`, `RelationshipsByType(token, opts)`, `AllNodes(opts)`, `AllRelationships(opts)`, `NodesByLabelAndProperty(token, key, value, opts)`.
- **`MemoryStore`** — 5 methods refactored to sort IDs before fetch, paginate via `paginateIDs`, then deep-copy only the paginated subset. For `Limit=100` on a 1M-node label, goes from 1M deep copies to 100.
- **`BadgerStore`** — 5 methods refactored with pagination. `NodesByLabelAndProperty` lock scope fixed: changed from holding `idxMu.RLock` during entity I/O to snapshot-and-release pattern. Fallback scan path applies cursor skip first, then early-stops when limit reached.
- **`Graph` layer** — 5 passthrough methods gain `QueryOpts` parameter: `NodesByLabel`, `RelationshipsByType`, `AllNodes`, `AllRelationships`, `NodesByLabelAndProperty`.
- **Internal callers** — `temporal.go` internal methods pass `QueryOpts{}` (zero = return all). All tutorials updated.

## [3.0.23] - 2026-03-02

### Fixed (Phase 2 Review — 6 Issues)

- **Hash chain verification truncation resilience** — `VerifyNodeHashChain`/`VerifyRelHashChain` now detect genesis by `entry.Version() == 0` instead of `i == 0`. After `TruncateNodeHistory`, the oldest chain entry may not be genesis; the old `i == 0` check caused verification to permanently return false. Non-genesis entries at chain position 0 (truncated history) now skip the PrevHash link check while still verifying content hash integrity.
- **`GetNodeAt` truncation resilience** — version start time derivation now checks `entry.Version() == 0` instead of `i == 0`. After truncation, the first entry in the chain may be a non-genesis version whose validity should start at `UpdatedAt`, not at snowflake creation time.
- **`CreatePropertyIndex` lock scope** — rewrote to 3-phase approach: (1) RLock to check existence + snapshot IDs, (2) fetch node data outside any lock via public `GetNode`, (3) write Lock to install index with double-check for concurrent creation. Previously held `idxMu.Lock` during Badger I/O, blocking all concurrent reads/writes. Non-`ErrNodeNotFound` errors are now propagated instead of silently swallowed.
- **Property index persistence** — index definitions now survive BadgerStore restart. Definitions are serialized to `0x0F/prop_indexes` meta key via msgpack on create/drop. `loadIndexes()` reads definitions back and rebuilds index data by scanning matching nodes. Previously, indexes were lost on restart, silently degrading `NodesByLabelAndProperty` to O(N) scan.
- **Delete preserves version history** — all delete paths (`DeleteNode`, `DeleteRelationship`, `DeleteNodeCascade`, `DeleteNodesBatch`, `DeleteRelationshipsBatch`) no longer erase 0x07/0x08 history entries. `DeleteNodeWithContext`/`DeleteRelationshipWithContext` now save tombstone versions (with `DeletedAt`/`ValidTo` set) for all affected entities before deletion. This preserves the temporal history tape for past-time queries. Removed `deleteHistoryByPrefix` function entirely.
- **Temporal queries are history-aware** — `GetNodesValidAt`, `GetRelationshipsValidAt`, `GetNodesValidDuring`, `GetRelationshipsValidDuring`, and `Snapshot` now include deleted entities that were valid at the queried time. Previously, these methods only scanned current tip versions via `AllNodes()`/`AllRelationships()`, making deleted nodes invisible to temporal queries.

### Added

- **`GetRelAt(id, t)`** — returns the version of a relationship valid at instant `t`. Mirrors `GetNodeAt` for relationships. Handles deleted entities via history chain reconstruction.
- **`AllNodeHistoryIDs()`** / **`AllRelHistoryIDs()`** — new Store interface methods returning IDs of all entities with version history entries (including deleted entities whose history was preserved). Implemented in both MemoryStore and BadgerStore (BadgerStore scans both pending buffer and persisted 0x07/0x08 keys).
- **~19 new tests** — hash chain verification after truncation (node + rel), GetNodeAt after truncation, deleted entity temporal queries (GetNodeAt deleted/after-deletion, GetNodesValidAt deleted/updated, GetRelAt basic/deleted/not-found, GetRelationshipsValidAt deleted, Snapshot includes deleted, GetNodesValidDuring deleted, GetRelationshipsValidDuring deleted), BadgerStore AllHistoryIDs (node/rel with pending buffer tests), Badger-backed temporal query integration tests.

### Changed

- **`Store` interface** — added `AllNodeHistoryIDs() ([]snowflake.ID, error)` and `AllRelHistoryIDs() ([]snowflake.ID, error)`.
- **`GetNodeAt`** — now handles deleted entities (tolerates `ErrNodeNotFound`, builds chain from history only). Refactored version resolution into `resolveNodeVersionAt`/`nodeVersionBounds` helpers.
- **`DeleteNodeCascade`** — simplified to single-phase (preflight + apply). Removed Phase 3 (history cleanup) since history is now preserved.
- **`context.go`** — `DeleteNodeWithContext` saves tombstone versions for all connected relationships and the node before cascade delete. `DeleteRelationshipWithContext` saves tombstone version before delete.
- **`keys.go`** — added `propIndexDefsKey` meta key for property index definition persistence.

## [3.0.22] - 2026-03-02

### Added (Phase 2e — Configurable Validation Limits)

- **`ValidationLimits` struct** on `Graph.Config` — configurable limits for graph operations: `MaxLabelsPerNode` (default 50), `MaxPropertiesPerEntity` (default 1000), `MaxPropertyKeyLength` (default 256), `MaxPropertyValueSize` (default 65536, strings nested inside property values), `MaxNameLength` (default 256, label and reltype names). Zero values resolve to defaults in `New()`.
- **5 sentinel errors** — `ErrTooManyLabels`, `ErrTooManyProperties`, `ErrKeyTooLong`, `ErrValueTooLarge`, `ErrNameTooLong` (all in `graph.go`).
- **Validation enforcement** in all 4 `WithContext` mutation methods (`AddNodeWithContext`, `AddRelationshipWithContext`, `UpdateNodeWithContext`, `UpdateRelationshipWithContext`) and all 4 `BatchBuilder` mutation methods (`AddNode`, `AddRelationship`, `UpdateNode`, `UpdateRelationship`). Update methods use two-phase validation: pre-lock entry checks + post-mutation `MaxPropertiesPerEntity` under entity lock.
- **`PropertyCount()`** on `Node` and `Relationship` — returns `properties.Len()` without deep copy. Used by update-path post-mutation property count validation.
- **~30 new tests** — defaults, zero-uses-defaults, custom limits, boundary tests (at-limit succeeds, one-over fails) for all 5 limits across AddNode/AddRelationship/UpdateNode/UpdateRelationship, batch mirroring. All sentinel errors tested with `errors.Is`.

### Changed (Phase 2d upgrade — Per-Label/Per-Type Statistics O(1))

- **`Store` interface** — added `NodeCountByLabel(token uint16) (int, error)` and `RelCountByType(token uint16) (int, error)` for O(1) per-label/per-type counting at the Store level.
- **`MemoryStore`** — O(1) via `len(labelIdx[token])` / `len(typeIdx[token])` under existing RWMutex.
- **`BadgerStore`** — `sync.Map` of `*atomic.Int64` counters, maintained incrementally in 9 mutation sites (`PutNode`, `DeleteNode`, `PutRelationship`, `deleteRelByInfo`, `PutNodesBatch`, `PutRelationshipsBatch`, `DeleteNodesBatch`, `cascadeDeleteLocked` normal + corruption paths). Counters rebuilt from index sizes in `loadIndexes()` — no new Badger keys.
- **Graph layer** — `NodeCountByLabel`, `RelCountByType`, `AllLabelCounts`, `AllRelTypeCounts` now delegate to Store-level O(1) methods instead of materializing all entities via `NodesByLabel`/`RelationshipsByType`. `AllLabelCounts`/`AllRelTypeCounts` use `uint16(i)` as token directly.
- **18 new tests** — 8 MemoryStore counter tests, 8 BadgerStore counter tests (including persistence round-trip verifying counters rebuilt from indexes), 2 graph-level integration tests (batch add, cascade delete).

### Added (Phase 2c — Property Indexes)

- **`CreatePropertyIndex(label, propertyKey)`** — creates an in-memory index on a property for a given label. Scans existing nodes to populate the index. Returns `ErrIndexExists` if already defined.
- **`DropPropertyIndex(label, propertyKey)`** — removes a property index. Returns `ErrIndexNotFound` if missing.
- **`NodesByLabelAndProperty(label, key, value)`** — O(1) indexed lookup of nodes matching a label+property value. Falls back to scan if no index is defined.
- **`propertyValueKey(v any)`** — type-prefixed canonical string for safe cross-type value comparison (`"s:Alice"`, `"i:42"`, `"f64:3.14"`, `"b:true"`). Only primitives are indexed; complex types (maps, slices) return `""`.
- **Auto-update hooks** — property indexes are automatically maintained across all 7 node mutation paths: `PutNode`, `DeleteNode`, `ReplaceNode`, `ReplaceNodeWithHistory`, `PutNodesBatch`, `DeleteNodesBatch`, `DeleteNodeCascade`.
- **24 new tests** — 8 MemoryStore (create/duplicate/drop/not-found/hit/miss/no-index-fallback/auto-update), 8 BadgerStore (mirrored), 8 Graph-layer (end-to-end: create/drop/found/not-found/unregistered-label/multiple-values/update-reflected/delete-removes). Plus `TestPropertyValueKey_AllTypes` (table-driven, all 14 type branches + 3 non-indexed types).
- **`ErrIndexExists`** / **`ErrIndexNotFound`** sentinel errors in `store.go`.

### Changed

- **`Store` interface** — added 3 property index methods (`CreatePropertyIndex`, `DropPropertyIndex`, `NodesByLabelAndProperty`). Both MemoryStore and BadgerStore implement them.

## [3.0.21] - 2026-03-02

### Added (Phase 2a — Temporal Queries)

- **`GraphSnapshot`** struct — represents the complete graph state at a point in time (`Timestamp`, `Nodes`, `Relationships`, `NodeCount`, `RelCount`).
- **`GetNodesValidAt(t)`** — returns all nodes valid at instant `t`. Nodes without explicit temporal metadata derive valid-from from snowflake ID timestamp and are treated as open-ended.
- **`GetRelationshipsValidAt(t)`** — returns all relationships valid at instant `t`.
- **`GetNodesByLabelValidAt(label, t)`** — returns nodes with the given label that are valid at `t`.
- **`GetNodesValidDuring(start, end)`** — returns nodes whose validity overlaps `[start, end)`.
- **`GetRelationshipsValidDuring(start, end)`** — returns relationships whose validity overlaps `[start, end)`.
- **`GetNodeAt(id, t)`** — returns the version of a node that was valid at `t`. Builds the full version chain (history + current), computes validity periods from `UpdatedAt` timestamps, with explicit `ValidFrom`/`ValidTo` overrides. Returns `ErrNoVersionValidAt` if no version covers `t`.
- **`GetNeighborsValidAt(nodeID, t)`** — returns neighbor nodes reachable via relationships valid at `t`, where the neighbors themselves are also valid at `t`.
- **`Snapshot(t)`** — returns a `GraphSnapshot` at instant `t`. Relationships are only included if both endpoints are in the valid node set (no dangling rels).
- **31 new tests** — 12 point-in-time, 6 interval (including open-ended rels), 5 version-specific, 3 neighbor, 5 snapshot.
- **`ErrNoVersionValidAt`** sentinel error in `store.go`.

### Changed

- **No Store interface change** — all temporal queries are Graph-layer filters over existing `AllNodes`/`AllRelationships`/`GetNodeHistory` methods.

## [3.0.20] - 2026-03-02

### Added (Phase 2d — Per-Label / Per-Type Statistics)

- **`NodeCountByLabel(label)`** — returns the count of nodes with the given label. Returns 0 for unregistered labels.
- **`RelCountByType(typeName)`** — returns the count of relationships with the given type. Returns 0 for unregistered types.
- **`AllLabelCounts()`** — returns a map of label name to node count for all registered labels. Skips token 0 (reserved).
- **`AllRelTypeCounts()`** — returns a map of relationship type name to count for all registered types. Skips token 0 (reserved).
- **12 new tests** — empty/unregistered/single/multiple/after-delete for both labels and types, plus AllLabelCounts and AllRelTypeCounts with mixed counts.

### Changed

- **No Store interface change** — statistics are scan-based, delegating to existing `NodesByLabel`/`RelationshipsByType` methods.

## [3.0.19] - 2026-03-02

### Added (Phase 2b — Hash Chain Verification)

- **`VerifyNodeHashChain(id)`** — verifies the full hash chain for a node. Retrieves history + current version, validates genesis `PrevHash == ""`, verifies each version's `PrevHash` links to the previous version's `Hash`, and recomputes each hash via `ComputeNodeHash` to detect tampering. Returns `(true, nil)` if valid, `(false, nil)` on any mismatch, `(false, err)` on I/O failure.
- **`VerifyRelHashChain(id)`** — mirrors `VerifyNodeHashChain` for relationships using `ComputeRelHash`.
- **14 new tests** — 7 node (genesis-only, multiple updates, tampered hash, broken PrevHash, non-existent, nil integrity, property change) + 7 mirrored relationship tests.

### Changed

- **No Store or API changes** — verification methods are pure reads over existing `GetNodeHistory`/`GetRelHistory` + `ComputeNodeHash`/`ComputeRelHash`.

## [3.0.18] - 2026-03-01

### Fixed (Pre-Release Code Review)

- **BLOCKER: Update atomicity** — `UpdateNode`/`UpdateRelationship` now use `ReplaceNodeWithHistory`/`ReplaceRelWithHistory` to atomically write version history and entity data in a single store call. Prevents orphaned history entries on crash between `PutNodeVersion` and `ReplaceNode`. New `Store` interface methods: `ReplaceNodeWithHistory(current, prevVersion, prevState)` and `ReplaceRelWithHistory(current, prevVersion, prevState)`.
- **BLOCKER: Hash serialization** — `writeProperties` in `integrity.go` replaced `fmt.Sprintf("%v")` with typed binary serialization using wire.go type tags. Maps now sort keys before hashing (deterministic). Type-distinct: `int(1)` vs `string("1")` produce different hashes. Breaking change for computed hashes (pre-release, no production data).
- **MAJOR: Cascade lock scope** — `DeleteNodeCascade` in BadgerStore releases `idxMu.Lock` before Phase 3 history cleanup. `deleteHistoryByPrefix` does Badger `db.View()` iterator scans — these no longer block concurrent reads/writes.
- **MINOR: Hash error handling** — Added `mustWrite`/`mustWriteString` helpers in `integrity.go`. All `_ = binary.Write()` and `_, _ = io.WriteString()` calls replaced with panicking wrappers. hash.Hash.Write never errors, but errors are no longer silently discarded.
- **MINOR: BatchBuilder docstring** — Changed "persisted atomically" to "executed sequentially; partial success is possible" to accurately describe behavior.
- **MINOR: MemoryStore RLock comment** — Added comment to `AllNodes`/`AllRelationships` documenting the RLock-for-iteration design choice.
- **MINOR: Tutorial 005 resource leak** — `bsQuery` is now explicitly closed if `graph.New` fails.

### Added (Phase 1g — Context-Aware Operations)

- **`AddNodeWithContext(ctx, labels, props)`** — creates a node with context support. Checks context at entry (pre-flight) and before the store write. Returns `context.Canceled` or `context.DeadlineExceeded` on cancellation.
- **`AddRelationshipWithContext(ctx, typeName, startNode, endNode, props)`** — creates a relationship with context support. Checks context at entry, before acquiring endpoint locks, and before the store write.
- **`UpdateNodeWithContext(ctx, id, updates)`** — updates a node with context support. 5 context checks: entry, before entity lock, before store read, before version history write, before final store write.
- **`UpdateRelationshipWithContext(ctx, id, updates)`** — mirrors `UpdateNodeWithContext` for relationships.
- **`DeleteNodeWithContext(ctx, id)`** — cascade-deletes a node with context support. Checks context at entry and under the entity lock before cascade.
- **`DeleteRelationshipWithContext(ctx, id)`** — deletes a relationship with context support. Checks context at entry before the store call.
- **`GetNodeWithContext(ctx, id)`** — retrieves a node with context support. Single pre-flight check.
- **`GetRelationshipWithContext(ctx, id)`** — retrieves a relationship with context support. Single pre-flight check.
- **`checkCtx(ctx)` helper** — non-blocking `select` with `default` branch. Zero overhead when context is not cancelled.
- **28 new tests** — 8 pre-flight cancellation (all methods return `context.Canceled`), 8 happy path (identical behavior to non-context methods), 4 deadline exceeded, 4 delegation regression (non-context methods still work), 4 edge cases (empty updates no-op, validation priority, checkCtx on Background). All pass with race detector.

### Changed

- **8 Graph methods refactored to thin wrappers** — `AddNode`, `AddRelationship`, `UpdateNode`, `UpdateRelationship`, `DeleteNode`, `DeleteRelationship`, `GetNode`, `GetRelationship` now delegate to their `WithContext` variants with `context.Background()`. Backward-compatible — existing callers require no changes.
- **No Store interface change** — Badger v4 does not support `context.Context` in its core API (`View`/`Update`/`Txn`). Context checks are best-effort at the Graph layer: pre-flight and between phases (before locks, before store calls). In-memory CPU-bound steps complete in microseconds and are not interrupted.

## [3.0.17] - 2026-03-01

### Added (Phase 1f — Batch Operations)

- **`PutNodesBatch(nodes []*types.Node)`** on Store interface — two-phase (validate then apply) atomic batch create. Phase 1 checks for duplicates vs existing store AND within the batch. Phase 2 deep-copies each, stores, and updates indexes. Any duplicate returns `ErrNodeExists` with zero mutations. Empty/nil input returns nil error. MemoryStore holds `mu.Lock()` for entire operation; BadgerStore holds `idxMu.Lock()` with pre-serialization outside the lock.
- **`PutRelationshipsBatch(rels []*types.Relationship)`** on Store interface — two-phase atomic batch create. Phase 1 verifies endpoints exist and checks for duplicate rel IDs. Phase 2 deep-copies, stores, updates type + adjacency indexes. MemoryStore and BadgerStore both use single lock for atomicity.
- **`DeleteNodesBatch(ids []snowflake.ID)`** on Store interface — two-phase atomic batch delete. Phase 1 verifies all IDs exist. Phase 2 removes entities, cleans label indexes, removes history. Missing ID returns `ErrNodeNotFound` with zero mutations.
- **`DeleteRelationshipsBatch(ids []snowflake.ID)`** on Store interface — two-phase atomic batch delete. Phase 1 verifies all IDs exist and pre-reads metadata. Phase 2 deletes via mutation-only helpers (type/adjacency/history cleanup). Missing ID returns `ErrRelNotFound` with zero mutations.
- **`BatchBuilder` fluent API** — `NewBatchBuilder(g)` creates a builder that queues operations with eager validation and deferred persistence. `AddNode(labels, props)` validates and creates fully-formed nodes (with hash + integrity) but doesn't persist. `AddRelationship(typeName, startNode, endNode, props)` validates type and properties. `UpdateNode(id, updates)` / `UpdateRelationship(id, updates)` pre-validate shadow key rejection and property types. `DeleteNode(id)` / `DeleteRelationship(id)` queue deletes.
- **`BatchResult`** — reports batch outcome with `Created`, `Updated`, `Deleted`, `Failed` counts, per-operation `Errors` slice, and `Duration`. Execute order: create nodes → create rels → update nodes → update rels → delete rels → delete nodes.
- **`BatchError`** — describes a single operation failure with `Op` (operation name), `ID` (entity ID), and `Err` (underlying error). Implements `error` interface.
- **41 new tests** — 12 MemoryStore batch tests (empty/happy/duplicate/internal-duplicate for Put, empty/happy/missing for Delete — node and rel parity), 12 BadgerStore batch tests (mirrored), 17 BatchBuilder tests (AddNode validation, AddRelationship validation, UpdateNode/UpdateRelationship validation, Execute empty/nodes/nodes+rels/updates/deletes/mixed/1000-nodes/partial-failure/update-rel, BatchError.Error). All pass with race detector.

### Changed

- **`Store` interface** — added 4 batch methods (`PutNodesBatch`, `PutRelationshipsBatch`, `DeleteNodesBatch`, `DeleteRelationshipsBatch`). Both MemoryStore and BadgerStore implement them.

## [3.0.16] - 2026-03-01

### Fixed (Phase 1e — FlushInterval Policy + LRU evictClean Fix)

- **FlushInterval defaulting for InMemory mode** — `NewBadgerStore` now defaults `FlushInterval` to 100ms for both InMemory and OnDisk modes. Previously, InMemory mode had no periodic flushing by default.
- **LRU `evictClean()` O(N²) degradation** — added `cleanCount` field for O(1) early exit when all entries are dirty. Prevents repeated O(N) scans of a fully-dirty cache.

## [3.0.15] - 2026-03-01

### Added (Phase 1d — Bulk Query Methods)

- **`AllNodes()`** on Store interface — returns all stored nodes, sorted by snowflake.ID. MemoryStore iterates the node map under `RLock` with DeepCopy. BadgerStore snapshots `nodeIDs` under `idxMu.RLock`, then fetches each via `GetNode` (cache + existence check + Badger fallback + DeepCopy). Returns `nil, nil` for empty stores.
- **`AllRelationships()`** on Store interface — returns all stored relationships, sorted by snowflake.ID. Same patterns as `AllNodes`.
- **`GetNodesByIDs(ids []snowflake.ID)`** on Store interface — originally returned nodes matching the given IDs, sorted by snowflake.ID, and omitted missing IDs. Current Unreleased behavior supersedes this by returning `ErrNodeNotFound` for any missing explicit node ID.
- **`GetRelationshipsByIDs(ids []snowflake.ID)`** on Store interface — originally returned relationships matching the given IDs, sorted by snowflake.ID, and omitted missing IDs. Current Unreleased behavior supersedes this by returning `ErrRelNotFound` for any missing explicit relationship ID.
- **Graph-layer passthroughs** — `Graph.AllNodes()`, `Graph.AllRelationships()`, `Graph.GetNodesByIDs(ids)`, `Graph.GetRelationshipsByIDs(ids)`. Pure delegation to the store (no string resolution needed).
- **32 new tests** — 12 MemoryStore (empty/count/sorted for each method), 12 BadgerStore (mirrored), 8 graph-layer (empty + populated for each method, including skip-missing verification). All pass with race detector.

### Added (Phase 1c — Hash Chain Computation)

- **`ComputeNodeHash(n, labels)`** — computes a SHA-256 hash of a node's content (id, version, sorted labels, sorted properties). Returns a 64-character hex string.
- **`ComputeRelHash(r, typeName)`** — computes a SHA-256 hash of a relationship's content (id, version, type name, start/end IDs, sorted properties). Returns a 64-character hex string.
- **Integrity hooks in `AddNode`/`AddRelationship`** — newly created entities now have `Integrity()` populated with a computed `Hash` and empty `PrevHash` (genesis).
- **Integrity hooks in `UpdateNode`/`UpdateRelationship`** — updated entities get a new `Hash` computed on their final state, with `PrevHash` set to the previous version's hash. Forms a verifiable hash chain across the entity's version history.
- **22 new tests** — 10 unit tests for hash functions (determinism, property/version/label/type/endpoint sensitivity, label order independence), 12 graph-layer integration tests (integrity set on create, hash determinism, hash chain linking across updates, multiple-update chain verification, genesis zero PrevHash — all with node/relationship parity).

### Added

- **`UpdateNode(id, updates)`** — graph-layer read-modify-write under entity lock. Pre-validates all keys (`tkg_` prefix rejected) and values (`ValidatePropertyValue`) before acquiring the lock. Under the lock: reads current state, applies property updates (nil value = delete key), bumps version, sets `temporal.UpdatedAt`, persists via `ReplaceNode`. Empty updates map is a no-op (no lock, no version bump).
- **`UpdateRelationship(id, updates)`** — same pattern as `UpdateNode`. Entity lock on the relationship ID only — property changes don't affect adjacency, so endpoint locking is unnecessary.
- **`SetNodeProperty(id, key, value)`** / **`DeleteNodeProperty(id, key)`** — convenience wrappers around `UpdateNode`.
- **`SetRelationshipProperty(id, key, value)`** / **`DeleteRelationshipProperty(id, key)`** — convenience wrappers around `UpdateRelationship`.
- **`ReplaceNode(n)`** / **`ReplaceRelationship(r)`** on Store interface — overwrite existing entities. Returns `ErrNodeNotFound`/`ErrRelNotFound` if the entity does not exist. Deep-copies at the store boundary. No index changes (labels and type/endpoints are immutable after creation).
- **`ValidatePropertyValue` exported** — renamed from `validatePropertyValue` in `pkg/types/propertyslice.go` for use in graph-layer update pre-validation paths.
- **44 new tests** — 6 MemoryStore Replace, 6 BadgerStore Replace, 28 graph-layer (13 UpdateNode + 11 UpdateRelationship + 4 convenience), 4 Badger integration (including persistence round-trip). All pass with race detector.
- **Version history** — pre-mutation entity state saved automatically on every `UpdateNode`/`UpdateRelationship`. Queryable by version or as a full ascending history. Truncatable. Cleaned up on entity deletion (including cascade).
- **`PutNodeVersion(id, version, n)`** / **`PutRelVersion(id, version, r)`** on Store interface — persist a versioned snapshot. Deep-copies at the store boundary. Initial creation (AddNode/AddRelationship) does NOT write history; first update saves version 0.
- **`GetNodeVersion(id, version)`** / **`GetRelVersion(id, version)`** — retrieve a specific historical version. Returns `ErrVersionNotFound` when the version doesn't exist.
- **`GetNodeHistory(id)`** / **`GetRelHistory(id)`** — return all saved versions in ascending order. Empty slice for never-updated entities.
- **`TruncateNodeHistory(id, keepVersions)`** / **`TruncateRelHistory(id, keepVersions)`** — keep only the N most recent versions. `keepVersions == 0` clears all history.
- **`Graph.GetNodeHistory(id)`** / **`Graph.GetRelHistory(id)`** — graph-layer passthrough to the Store.
- **`ErrVersionNotFound`** sentinel error — returned by `GetNodeVersion`/`GetRelVersion` for non-existent versions.
- **History key promotion** — `keyHistNode` (0x07) and `keyHistRel` (0x08) promoted from test-only stubs to production keys in `keys.go`. Added `histNodePrefix`/`histRelPrefix` for prefix scanning.
- **~50 new history tests** — 17 MemoryStore (8 node + 9 rel), 19 BadgerStore (mirrored + 2 restart persistence), 14 graph-layer (5 node + 5 rel + 4 Badger persistence). All pass with race detector.

### Changed

- **`Store` interface** — added `Close() error` (resource cleanup contract). All Store implementations must satisfy it. `MemoryStore.Close()` is a no-op (returns nil). `BadgerStore.Close()` was already implemented. `Graph.Close()` now calls `store.Close()` universally instead of the previous `closeFn` indirection — custom stores injected via `Config.Store` are now properly closed.
- **`Store` interface** — added `ReplaceNode(n *types.Node) error` and `ReplaceRelationship(r *types.Relationship) error`. Replace semantics are the opposite of Put: Put rejects duplicates (`ErrNodeExists`/`ErrRelExists`), Replace requires existence (`ErrNodeNotFound`/`ErrRelNotFound`).
- **`Store` interface** — added 8 version history methods (`PutNodeVersion`, `GetNodeVersion`, `GetNodeHistory`, `TruncateNodeHistory` + relationship mirrors). Both MemoryStore and BadgerStore implement them.
- **`UpdateNode`/`UpdateRelationship`** — now save pre-mutation state to version history via `PutNodeVersion`/`PutRelVersion` before applying mutations.
- **`DeleteNode`/`DeleteNodeCascade`/`DeleteRelationship`** — all delete paths now clean up associated version history entries. BadgerStore uses a three-phase cascade: (1) preflight, (2) rel mutations, (3) history cleanup.
- **`Graph.Close()` error handling** — replaced `&& closeErr == nil` guards with `errors.Join`, preserving all errors (registry save + store close) instead of dropping subsequent ones.
- **`Graph` struct** — removed `closeFn func() error` field. No longer needed since `Store.Close()` is called directly.

### Fixed

- **`BadgerDir` whitespace silent fallback** — `New(Config{BadgerDir: "   "})` previously fell through to MemoryStore silently (whitespace-only string is non-empty, passes `!= ""` check, but Badger would fail). Now rejects whitespace-only `BadgerDir` with an explicit error message. Empty string `""` still correctly defaults to MemoryStore.

## [3.0.14] - 2026-03-01

### Fixed

- **`flushLoop` silently discards errors** — `_ = bs.flush()` in the background flush loop now logs failures via `slog.Error` instead of silently discarding them. Removed `#nosec G104` annotations. Persistent Badger failures (disk full, corruption) are now observable.
- **Shared entity pointers between cache and caller** — `PutNode`/`PutRelationship` and `GetNode`/`GetRelationship` in both `BadgerStore` and `MemoryStore` now deep-copy entities at the store boundary. Previously, caller and cache shared the same pointer; mutations via `SetProperty` on the returned entity would silently corrupt cached state.
- **`DeleteNodeCascade` partial mutation on mid-loop error** — refactored to a two-phase approach: (1) preflight reads all relationship metadata, aborting with zero state changes on any read failure; (2) applies all deletions atomically via the new `deleteRelByInfo` helper. Previously, a corrupted relationship mid-cascade left indexes in a permanently split state.
- **`Close()` masks `db.Close()` error** — replaced `if e != nil && err == nil { err = e }` with `errors.Join(err, e)` to preserve both flush and database close errors.
- **`DeleteNodeCascade` returns nil on node data corruption** — now returns `fmt.Errorf("graph: cascade completed with corrupt node data: %w", err)` while still completing index cleanup. Callers can detect and log corruption.
- **Entity lock shard index uses step bits** — `shardIndex()` was masking bits 0-7 (step counter), which resets to 0 every millisecond. All entities created in separate milliseconds mapped to shard 0, reducing 256 shards to a single global mutex. Now shifts right by 22 to extract the low 8 bits of the timestamp field. Entities created >256ms apart land in different shards.
- **Node and rel generators share snowflake node ID** — both `nodeIDGen` and `relIDGen` used the same `SnowflakeNodeID`, allowing value-level ID collisions within the same millisecond. Now mapped to an even/odd pair (`ID*2` for nodes, `ID*2+1` for rels). Valid `SnowflakeNodeID` range reduced from 0-1023 to 0-511 (512 concurrent instances). **Breaking**: existing databases with IDs generated under the old scheme remain readable (key prefixes distinguish entity types), but new IDs will have different node fields.
- **`ImportNames` integer overflow on corrupted data** — both `labelRegistry.ImportNames` and `relTypeRegistry.ImportNames` cast slice indices to `uint16` without bounds checking. If persisted data exceeded 65,535 entries, the cast silently truncated, causing token collisions. Now returns an error if `len(names)-1 > tokenCapacityMax`. Removed `#nosec G115` annotations.

### Added

- **`Node.DeepCopy()`** — returns a fully independent clone (extraLabels, properties, temporal, integrity all deep-copied).
- **`Relationship.DeepCopy()`** — returns a fully independent clone (properties, temporal, integrity all deep-copied).
- **`tkg_created_at` derived from snowflake ID** — when `TemporalMetadata` is nil or `CreatedAt` is zero, `ResolveNodeProperty`/`ResolveRelProperty` derive the creation timestamp from the entity's snowflake ID via `Decompose()`. Explicit non-zero `CreatedAt` takes priority (historical import). Every entity now has an automatic, accurate creation timestamp without requiring `SetTemporal()`.

### Changed

- **`Config.SnowflakeNodeID` range** — valid range is now 0-511 (was 0-1023). Mapped to even/odd generator pair for value-level ID uniqueness.
- **Sort comments corrected** — `sortNodesByID`/`sortRelsByID` and query method doc comments no longer claim "chronological" order. Sort is time-dominant (ms timestamp in high bits) with node field and step as tiebreakers.

## [3.0.13] - 2026-02-28

### Fixed

- **Graph.Close() data race** — `closeFn` was read/written without synchronization; two concurrent `Close()` calls would race on the nil check. Replaced with `sync.Once` (`closeOnce`) for race-free idempotency.
- **Query methods swallow corruption errors** — `NodesByLabel`, `RelationshipsByType`, `OutgoingRelationships`, `IncomingRelationships` used bare `continue` on all errors, silently eating I/O and corruption errors. Now only `ErrNodeNotFound` / `ErrRelNotFound` (index orphans) are skipped; all other errors propagate to the caller.
- **Dead variable `relDeleteCount`** — written but never read in `DeleteNodeCascade`. Removed.

### Changed

- **Test-only code relocated** — 12 functions and 6 constants (`histNodeKey`, `histRelKey`, `tempNodeKey`, `tempRelKey`, `labelIndexPrefix`, `relTypeIndexPrefix`, `outPrefix`, `outTypedPrefix`, `inPrefix`, `inTypedPrefix`, `parseNodeIDFromLabelIdx`, `parseRelIDFromTypeIdx`) moved from `keys.go` to `keys_helpers_test.go`. Reduces production binary size.
- **`toIntSlice` test coverage** — added `TestWireRoundTripIntSlice` exercising `[]int` property wire round-trip. No longer at 0% coverage.

## [3.0.12] - 2026-02-28

### Fixed

- **Atomic counter persistence** — counter keys (`meta/node_count`, `meta/rel_count`) are now written in the same `WriteBatch` as entity data. Previously, `persistCounters()` was a separate transaction, creating a TOCTOU window where counters could drift from actual entity count on crash recovery.
- **O(N) → O(1) evictClean** — LRU `evictClean()` was O(N²) due to restarting the inner scan from `Back()` after each eviction. Now a single-pass backward scan with `prev` pointer — O(N) worst case.
- **Cascade delete error propagation** — `DeleteNodeCascade` now checks `errors.Is(err, ErrRelNotFound)` and propagates non-sentinel errors (data corruption). Previously all `deleteRelLocked` errors were silently swallowed.
- **Close() InMemory flush** — `Close()` now calls `bs.flush()` unconditionally, ensuring pending ops are persisted even when `flushLoop` was never spawned (InMemory mode or zero FlushInterval).

### Changed

- **O(1) existence checks** — added `relIDs` map (mirrors `nodeIDs`) for O(1) relationship existence lookups. `GetRelationship()` and `PutRelationship()` now short-circuit via `relIDs` instead of scanning `typeIdx`. Removed dead `relExistsInIndex()` O(N) scan.
- **`GetNode()` / `GetRelationship()` bloom filter** — on cache miss, both methods check `nodeIDs`/`relIDs` (O(1)) before opening a Badger `db.View()` transaction, avoiding disk I/O for non-existent entities.
- **`persistCounters()` removed** — counter persistence is now inlined into `flush()` as part of the atomic WriteBatch.

## [3.0.11] - 2026-02-28

### Fixed

- **Version-aware dirty tracking** — LRU `CollectDirty()` is now read-only; `MarkFlushed()` only clears entries matching the collected `dirtyVer`. Prevents data loss when new writes land between `CollectDirty()` and `MarkFlushed()`.
- **Map-based pending write buffer** — replaced `[]writeOp` with `map[string]writeOp` for last-write-wins deduplication. `requeueOps()` preserves newer writes over failed ops. Prevents chronological write inversion on flush retry.
- **Cascade index scrub** — `DeleteNodeCascade` now scrubs `labelIdx` by scanning all label sets when entity data is unreadable, preventing ghost index entries.

## [3.0.10] - 2026-02-28

### Added

- **LRU entity caches** (`pkg/graph/lru.go`) — generic `entityLRU[V]` with dirty tracking, tombstone support, and soft capacity (dirty entries never evicted until flushed). BadgerStore maintains separate caches for nodes and relationships. Configurable via `BadgerStoreConfig.CacheCapacity` (default 10,000 per cache).
- **Entity lock manager** (`pkg/graph/entity_locks.go`) — 256-shard `sync.Mutex` array for write-skew prevention. `LockTwo(a, b)` acquires shards in ascending order (deadlock-free). Self-loops and same-shard IDs handled correctly with single lock acquisition.
- **Async batch persistence** — write operations update in-memory state immediately and queue `writeOp` structs. Background flush loop drains the buffer via Badger `WriteBatch` (blind writes = zero OCC conflicts) every `FlushInterval` (default 100ms). Failed ops are re-queued for retry.
- **Background value log GC** — periodic `RunValueLogGC()` loop (default 5min interval, configurable via `GCInterval`). Skipped entirely in in-memory mode.
- **In-memory indexes** — `nodeIDs`, `labelIdx`, `typeIdx`, `outIdx`, `inIdx` maps rebuilt from Badger on startup via `loadIndexes()`. In-memory state is the source of truth while running. Badger is the durable backing store.
- **`BadgerStoreConfig`** new fields: `CacheCapacity`, `FlushInterval`, `GCInterval`, `GCDiscardRatio`.
- **`BadgerStore.Flush()`** — explicit flush for tests that need to verify durable persistence.
- **Write-skew regression test** (`TestGraphAddRelDeleteNodeConcurrency`) — 100 iterations of concurrent `AddRelationship` + `DeleteNode` on overlapping entities, verifying no dangling edges.

### Changed

- **BadgerStore architecture** — replaced synchronous per-operation Badger transactions with cache-first reads + async batch writes. Read path: LRU cache hit → return; cache miss → `db.View()` → populate cache; tombstone → `ErrNotFound`. Write path: update cache (dirty) + update indexes + queue writeOps.
- **Counter implementation** — replaced `incrCounter()` inside Badger transactions with `atomic.Int64` fields on the BadgerStore struct. Counters are persisted by the flush loop piggyback. Eliminates all OCC contention on concurrent writes.
- **`Graph.AddRelationship`** — now acquires entity locks on both endpoints via `LockTwo(startID, endID)` before ID generation. Prevents write-skew where concurrent `AddRelationship(→X)` + `DeleteNodeCascade(X)` both commit, producing a dangling edge.
- **`Graph.DeleteNode`** — now acquires entity lock on the target via `LockEntity(id)` before cascade.
- **`DeleteNodeCascade` (BadgerStore)** — atomic in-memory under `idxMu` write lock with async Badger writes, replacing single `db.Update()` transaction.
- **`Close()`** — idempotent via `sync.Once`. Stops background goroutines, performs final flush, persists counters, closes Badger.

### Removed

- `incrCounter()` — replaced by atomic counters.
- `initCounters()` — replaced by `loadIndexes()` + counter loading from meta keys.

## [3.0.9] - 2026-02-27

### Fixed

- **Close() file handle leak** — `Graph.Close()` now always calls `closeFn()` even if registry saves fail. Previously, a failed `SaveLabelRegistry` or `SaveRelTypeRegistry` would exit early, leaving the Badger file handle open. `closeFn()` now runs unconditionally; the first error is collected and returned.
- **Type erasure in wire format** — msgpack serialization destroyed Go type fidelity (`[]string` → `[]any`, `int64` → `int8`). Added a `Type byte` tag to `propertyWire` that records the concrete Go type during serialization and reconstructs it during deserialization. 24 type tags cover all allowlisted property types. Backward compatible: old data without the tag (decoded as `Type: 0`) falls through to integer normalization.
- **Silent data loss on query errors** — Store query methods (`NodesByLabel`, `RelationshipsByType`, `OutgoingRelationships`, `IncomingRelationships`, `NodeCount`, `RelationshipCount`) now return `error`. `BadgerStore` propagates I/O and corruption errors instead of swallowing them. `MemoryStore` always returns `nil`.
- **TOCTOU cascade-delete** — `Graph.DeleteNode` was executing N+2 separate lock acquisitions/transactions, creating a window where concurrent `AddRelationship` could produce dangling edges. Both `MemoryStore.DeleteNodeCascade` and `BadgerStore.DeleteNodeCascade` now execute the entire cascade atomically — single write lock / single `db.Update()` transaction.
- **O(N) counts** — `BadgerStore.NodeCount()` and `RelationshipCount()` previously did full prefix scans. Now maintained as atomic metadata counters (`meta/node_count`, `meta/rel_count`) incremented/decremented within each mutating transaction — O(1) reads. Counter initialization scans on first open for backward compatibility with existing databases.

### Changed

- **`Store` interface** — all 6 query methods now return `error`. Added `DeleteNodeCascade(id snowflake.ID) error`. `Graph.DeleteNode` delegates to `DeleteNodeCascade`.
- **`propertyWire`** struct gained `Type byte` field (msgpack tag `"t"`).

## [3.0.8] - 2026-02-27

### Added

- **Badger persistence** (`pkg/graph/badgerstore.go`) — `BadgerStore` implementing the `Store` interface using [Badger v4](https://github.com/dgraph-io/badger) as the storage backend. Supports in-memory mode for testing and on-disk persistence. Includes type index, label index, and adjacency index maintenance.
- **Msgpack wire formats** (`pkg/graph/wire.go`) — `nodeWire`, `relWire`, `propertyWire` structs with conversion functions for serialization boundary. Handles temporal metadata, integrity hashes, and type normalization (msgpack compact integer encoding).
- **Binary key encoding** (`pkg/graph/keys.go`) — fixed-width binary keys with single-byte prefix tags for correct sort order. All snowflake IDs stored as big-endian uint64; tokens as big-endian uint16. 10 key types covering entities, indexes, adjacency, history, temporal, and metadata.
- **Registry persistence** — `ExportNames()` / `ImportNames(names)` on both `labelRegistry` and `relTypeRegistry`. `BadgerStore` persists registries as msgpack `[]string` under `meta/label_tokens` and `meta/reltype_tokens`.
- **`Graph.Close()`** — saves registries to Badger (if applicable), then closes the database. Idempotent. No-op for `MemoryStore`.
- **`Config.BadgerDir`** / **`Config.BadgerInMemory`** — creates a `BadgerStore` automatically when `Config.Store` is nil. Loads persisted registries on startup; fail-fast on corrupt data.
- **`ErrRegistryNotEmpty`** sentinel error — returned when `ImportNames` is called on a non-empty registry.

### Dependencies

- Added `github.com/vmihailenco/msgpack/v5` v5.4.1
- Added `github.com/dgraph-io/badger/v4` v4.9.1

## [3.0.7] - 2026-02-27

### Changed

- **Snowflake dependency migrated** from `gitlab2024.bds421-cloud.com/bds421/rho/snowflake-2026 v0.1.3` to [`github.com/bds421/rho-snowflake-2026 v1.0.1`](https://github.com/bds421/rho-snowflake-2026). All import paths updated across 14 `.go` files, `go.mod`, `SPEC.md`, and documentation.

### Fixed

- **Deterministic query results** — `NodesByLabel`, `RelationshipsByType`, `OutgoingRelationships`, and `IncomingRelationships` now sort results by snowflake.ID for deterministic output. Previously, Go map iteration randomized the output on every call.
- **Cascade-delete outgoing tolerance** — `Graph.DeleteNode` now skips `ErrRelNotFound` in both the outgoing and incoming loops. Previously, a concurrently-deleted outgoing relationship would abort the cascade, leaving a partially severed node.
- **TOCTOU documentation** — `Graph.DeleteNode` documents the per-call locking limitation: without a transactional store API, a concurrent `AddRelationship` can create a dangling edge during cascade. The Badger implementation must wrap the entire cascade in a single `Update()` transaction.

## [3.0.6] - 2026-02-27

### Added

- **`Store` interface** (`pkg/graph/store.go`) — pure persistence contract with `PutNode`/`GetNode`/`DeleteNode`, `PutRelationship`/`GetRelationship`/`DeleteRelationship`, index queries (`NodesByLabel`, `RelationshipsByType`), adjacency queries (`OutgoingRelationships`, `IncomingRelationships`), and counts. Keys are `snowflake.ID`.
- **`MemoryStore`** (`pkg/graph/memorystore.go`) — thread-safe in-memory `Store` implementation. Uses nested hash-set adjacency indexes (`map[snowflake.ID]map[snowflake.ID]struct{}`) for O(1) insert/delete. `PutRelationship` validates start/end nodes exist.
- **`Graph.AddNode(labels, props)`** — creates a node with auto-generated snowflake ID, resolves labels to tokens, and bulk-loads properties via `NewPropertySlice`. Validates input before generating IDs to prevent snowflake waste.
- **`Graph.AddRelationship(typeName, startNode, endNode, props)`** — creates a directed relationship with auto-generated snowflake ID, resolves type to token, validates endpoints are non-nil, and bulk-loads properties.
- **`Graph.DeleteNode(id)`** — cascade-deletes all outgoing and incoming relationships before removing the node. Handles self-loops correctly by skipping `ErrRelNotFound` on the incoming pass.
- **`Graph.DeleteRelationship(id)`** — passthrough to store.
- **Store passthrough queries on Graph**: `GetNode`, `GetRelationship`, `NodesByLabel` (string-based, resolves to token), `RelationshipsByType` (string-based), `NodeCount`, `RelationshipCount`.
- **Shadow property resolution** (`pkg/graph/shadow.go`): `ResolveNodeProperty(n, key)` and `ResolveRelProperty(r, key)` dispatch all 15 `tkg_*` shadow keys. Non-`tkg_` keys delegate to `GetProperty`. Nil-guards on `Temporal()` and `Integrity()` prevent nil-pointer panics on new entities.
- **`NewPropertySlice(map[string]any)`** — O(N log N) bulk loader. Allocates once, validates all values (reserved-prefix + recursive allowlist), sorts once. Replaces the O(N²) per-property `SetProperty` loop for bulk construction.
- **`Node.SetProperties(ps)`** / **`Relationship.SetProperties(ps)`** — assign a pre-built `PropertySlice` directly, bypassing per-property validation (already done by `NewPropertySlice`).
- **SnowflakeID bridge methods**: `nodeID.SnowflakeID()`, `relID.SnowflakeID()`, `entityID.SnowflakeID()` — exported methods on unexported wrapper types for cross-package persistence key extraction.
- **Sentinel errors**: `ErrNodeNotFound`, `ErrRelNotFound`, `ErrNodeExists`, `ErrRelExists`, `ErrNoLabels`, `ErrNilNode`.
- **`Config.Store`** field — pluggable persistence backend. Defaults to `NewMemoryStore()` when nil.

### Changed

- `Graph` struct now holds a `store Store` field alongside registries and generators.
- `New(Config)` initializes a `MemoryStore` when `Config.Store` is nil.
- Implementation phases updated: Phase 2A (Store, MemoryStore, entity management, shadow resolution) complete.

## [3.0.5] - 2026-02-27

### Added

- **`HasLabelTokenRaw(uint16)`** on Node — zero-allocation label check for the graph layer. Accepts raw `uint16` instead of opaque `labelToken`, avoiding the heap allocation from `AllLabelTokens()`.
- **`HasTypeTokenRaw(uint16)`** on Relationship — zero-allocation type check for the graph layer. Mirrors `Node.HasLabelTokenRaw`. Token 0 always returns false.
- **`ErrMaxDepthExceeded` sentinel error.** `PropertySlice.Set()` returns this when a value exceeds 32 levels of nesting, preventing stack overflow from self-referential or deeply nested structures.
- **`ErrEmptyName` sentinel error.** Both `labelRegistry.GetOrCreate` and `relTypeRegistry.GetOrCreate` reject empty strings, preventing ambiguous token resolution.

### Changed

- **Explicit snowflake configuration.** Both snowflake generators now pass `WithEpoch(2026-01-01)`, `WithNodeBits(10)`, `WithStepBits(12)` explicitly instead of relying on defaults.
- **Allowlist property validation.** `validateReflectValue` switched from denylist (reject Ptr/Struct) to allowlist (accept only primitives + safe containers). Arrays, channels, functions, and unsafe pointers are now rejected at any nesting depth.
- **Registry capacity: token 65535 is now assignable.** Fixed off-by-one in `GetOrCreate` that blocked the final token. Capacity check uses `len(toLabel/toName) > 65535` instead of `nextToken >= 65535`.
- **Recursion depth limit.** `validateReflectValue`, `deepCopyValue`, and `reflectCopyValue` now thread a `depth` counter and stop at `maxPropertyDepth` (32). Validation returns an error; copy functions fall back to shallow return.
- `NodeHasLabel` and `RelationshipHasType` use `HasLabelTokenRaw`/`HasTypeTokenRaw` for zero-allocation matching with token-0 defense-in-depth.
- **`deepCopyValue` nil short-circuit.** Nil values return immediately without entering the type switch or reflect path.
- **`Temporal()`/`Integrity()` doc comments** on both Node and Relationship now document the shared-pointer intent (no defensive copy).
- **`ErrUnsupportedValueType` message updated** from "pointer and struct values are not supported" to the generic "unsupported property value type" to reflect the allowlist model.
- `Set()` doc comment updated to describe the allowlist approach and depth limit.

### Fixed

- Empty-string labels and relationship types no longer silently assigned tokens.
- Registry capacity boundary corrected (65535 tokens, not 65534).
- `pkg/graph/doc.go` updated to reflect current state (snowflake generators exist, not "will hold").

## [3.0.3] - 2026-02-27

### Added

- **Snowflake ID generators.** `Graph` now holds two independent `snowflake.Node` generators. `NextNodeID()` and `NextRelID()` produce unique `snowflake.ID` values. `Config.SnowflakeNodeID` is validated; out-of-range values return an error from `New()`. *(Range changed from 0-1023 to 0-511 in v3.0.14.)*
- **Recursive property validation.** `PropertySlice.Set()` traverses slices, maps, and `any`/interface wrappers to reject pointers and structs at any nesting depth (`validatePropertyValue` + `validateReflectValue`).
- **`Instant` type** (`pkg/types`): semantic wrapper for Unix-millisecond timestamps used by all temporal fields.
- **`nodeID` / `relID` opaque types** (`pkg/types`): unexported wrappers around `snowflake.ID`. `InternalID()`, `StartNodeID()`, `EndNodeID()` return these instead of `snowflake.ID` directly.
- **`TemporalMetadata` fields**: `ValidFrom`, `ValidTo`, `TxFrom`, `TxTo`, `CreatedAt`, `UpdatedAt`, `DeletedAt` (all `Instant`), `CreatedBy`, `UpdatedBy` (`string`), `BaseEntityID` (`snowflake.ID`).
- **`NodeIntegrity` / `RelIntegrity` fields**: `Hash`, `PrevHash` (`string`).

### Changed

- `reflectCopyValue` nil-value handling: map keys with nil values are preserved using `reflect.Zero()` instead of silently deleted.
- Registry capacity warning fires exactly once via `sync.Once` (was per-token for 60000-65534).

## [3.0.2] - 2026-02-27

### Added

- **`pkg/graph` package**: Graph layer with label and relationship type registries (Phase 1).
  - `Graph` struct with `Config`, registry ownership, and string resolution methods.
  - `labelRegistry` / `relTypeRegistry`: thread-safe bidirectional string ↔ uint16 token mappings (RWMutex, double-check on write miss). Independent token namespaces.
  - Resolution: `NodeLabels`, `NodePrimaryLabel`, `NodeHasLabel`, `RelationshipType`, `RelationshipHasType`.
  - Registry passthrough: `GetOrCreateLabel`, `GetOrCreateRelType`, `LookupLabel`, `LookupRelType`.
  - Capacity: warning at 60K tokens, error at 65535.
- `labelToken.Value()` and `relTypeToken.Value()` bridge methods for cross-package token access.
- Pointer/struct rejection in `PropertySlice.Set()` with `ErrUnsupportedValueType` sentinel.
- `PropertySlice.Delete(key)` method with `tkg_` prefix guard.
- `Node.DeleteProperty(key)` and `Relationship.DeleteProperty(key)`.

### Changed

- `deepCopyValue` expanded to all common slice/map types plus reflect-based fallback for exotic types.
- `ToMap()` now deep-copies all values.
- Sentinel errors (`ErrReservedPrefix`) properly wrapped for `errors.Is` discrimination.

## [3.0.1] - 2026-02-27

### Added

- `PropertiesMap()` on Node and Relationship.
- `TemporalMetadata` stub struct with `Temporal()`/`SetTemporal()` accessors.
- `NodeIntegrity` and `RelIntegrity` stub structs with `Integrity()`/`SetIntegrity()` accessors.
- Comprehensive `PropertySlice` test suite.

### Changed

- Shadow properties aligned to spec (final 15 `tkg_*` keys).
- Opaque token types (`labelToken`/`relTypeToken`) replace raw `uint16` in public API.
- Token 0 validation: constructors panic, `Has*Token(0)` returns false.
- Extra label deduplication in `NewNode`.

## [3.0.0] - 2026-02-16

### Added

- Initial implementation of `pkg/types`: `Node`, `Relationship`, `PropertySlice`, shadow constants.
- Snowflake ID integration via `github.com/bds421/rho-snowflake-2026`.
- Token interning with `labelToken` and `relTypeToken` (uint16).
- Shadow property protection (`tkg_` prefix rejection).
- Defensive copying on all slice accessors.
