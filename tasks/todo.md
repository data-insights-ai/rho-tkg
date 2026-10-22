# todo — what is next. Shipped = `docs/CHANGELOG.md`; state = `HANDOVER.md`; old ledger (done rows, releases, review log, v5 WP history) = `git show aeb7f8e0:tasks/todo.md`.

## Next (ordered)
1. (117) Per-tier data directories (René via ai-soc 2026-10-22: hot RAM, warm NVMe, cold HDD). Today every tier is a badger shard under one `DataDir` (`tieredstore.go:574-583`, shard dir `:1470-1472`); "hot" is on disk, tiers are catalog labels (`shard_catalog.go:30-35`). Plan: one `Config.ColdDir` (empty = `DataDir`, byte-identical catalog), `ShardEntry.Volume`; hot, warm, reference, `meta/` stay on NVMe. Move warm→cold = badger stream copy into `ColdDir/events/<name>.moving-<nonce>` (fsync), then a short switch under `shardMu` + store-wide-op lock (abort on a write since the snapshot), catalog save = commit point, New cleans orphans; recompress ZSTD on the copy. Steps, each with its red test: (a) XS `ts.shardDir` helper for `:1471` and the 4 `RemoveAll` on `ts.dataDir` (`tieredstore_catalog.go:83,129,379`, `retention_purge_drop.go:207`), patch; (b) S lazy-open refuses a missing shard dir, New stats catalog dirs (today badger creates an empty shard on an unmounted HDD), patch; (c) S `ColdDir` + `Volume` + catalog format marker, minor; (d) M copy without switch, `ExportSince` parity; (e) M switch + crash recovery at each step; (f) S races: drop, write, reader during a move; (g) S `ColdCompression`; (h) S mover on the idle-sweep tick, one shard at a time, migrates today's cold shards; (i) XS architecture.md contract + persistence.md layout. ~1.5-2 weeks, one minor (a, b may ship first). Open: decision 7. Unverified: write-since-snapshot marker, `StreamWriter` fsync, older key versions in a stream copy. ai-soc's side (`-cold-data` flag, `graphmgr/manager.go:97-103,495-505`, `cmd/aisoc/core.go`, `serve.go:172`) sent to ai-soc-main-45.
2. (108) Cold-start test 2 on the finished md set (13 files): fresh agent, new questions (not test 1's 12). Check: each gap it finds is fixed in the owning file, docs tests green.

## Waiting (trigger → action)
- (92) ai-soc's reopen profile (ReadHistory via `readUnderRLock` 526 s vs 57 s for fix5) arrives with rho version + callee under `fn` → act on that only; `readUnderRLock` (locks.go:80) unchanged since v4.66.0.
- A consumer report against v4.70.0 (ai-soc's full Flux rerun, row 106 fix; sigma O1-3 on `LowestAnsweredPins` (P1) and `RelEffectiveTimelinesByProperties`: tiered, driver = first indexed key, device before estate, segments whole, DepthAll only, ai-soc declares (type, device) rel indexes) → build only what blocks them (lesson 79 (10)).
- Origin watch (HANDOVER Appendix A, at session start): a Markus release → merge origin/main, check his code against the rules (one valid start, Void rows, Elements, format markers, head cache, version allocation), fix his CHANGELOG heading.
- D1 (102) ai-soc raises priority, or nothing else is open → resume branch `row102-pending-ranged` (base 6ac9ea5f; 8fa7a967 = the two red tests, f4b27842 = WIP `badgerstore_write_buffer.go`, call sites half converted), rebase onto main first; badger `rangePending` walks all of pending + flushing per per-entity overlay read. Check: `TestPendingOverlayCostIndependentOfPendingSize` + `TestRollbackManyCreatedRelsLinearInPending` green → patch.
- D2 ai-soc memory pressure (BA RESHEAP 0.53 → 1.13 GB in 24 h; 0.18 GB ristretto under `Session.checkEntities → latestStamps`) → one store-wide badger cache budget or a documented per-shard size (each open shard owns its cache, `badgerstore_options.go:182`).
- D3 (105 (b)) `RelEffectiveTimelinesByProperties` measures too slow on BA → rel composite tx-membership sidecar (2,500-3,500 LOC, 4 backends).
- D4 decision 3, or sigma/ai-soc needs P24 → tombstone writer `DeleteKeptValidTo` (fixes backlog 4 (ii), 7, 25): branch `tombstone-writer-v4.63` (`37a1ff56`), rebuild on the v4.66 axis code (`stampDeleteTombstone`, `EndValidity`), ~½ day, a minor; heads-up to sigma + ai-soc before its tag.
- D5 a consumer hits one → backlog 80-82 and the review nits not taken (v4.69.2: 4, git 82558ea9; v4.70.0: 3, git 3c370aab).
- D6 next touch → repoint stale code cites of gone todo sections: `tasks/todo.md "R3 sizing"` (badger/tiered adjacency-disk tests; sizing in git f94060df), `tasks/todo.md, Tier D` (`core/node_delete.go:353,378`).

## Decisions for René (my default)
1. Review findings not taken under the scope rule (tests review #5-#10, code review #4-#7, docs #9-#10, old rows 77-82). Default: untaken until a consumer hits one.
2. v5 restart (row 61) and the first v5 tag. Default: paused; `v5.0.0-alpha.1` at S2 exit once restarted.
3. Tombstone writer rebuild (D4). Default: parked until sigma or ai-soc needs P24.
4. Retention purge vs retained pins (row 104; a purged entity answers not-found at pins it answered before): (a) no rho change, sigma treats not-found for a seen id as purged; (b) `ErrRetentionExpired` via a per-entity purge tombstone (behaviour change, deprecation ritual). Default: (a).
5. `UntimedWholeAxis` default for new stores: announced in CHANGELOG `[4.65.0]` Deprecated for "the next minor", held for five minors. Default: stays held; the next minor flips it or rewords the notice.
6. Backlog 16 (range folds: resolved version vs predicate-anywhere). Default: keep today's documented behaviour.
7. Per-tier dirs (Next 1): (a) hot = NVMe + badger caches, not RAM (a RAM tier breaks DurableCommit/SyncWrites crash safety); (b) `ColdDir` only, archive stays on NVMe; (c) a cold shard promoted at open stays on HDD; (d) the catalog format marker is an accepted one-way door (older rho refuses a moved store); (e) ZSTD on cold is opt-in (`ColdCompression`). Default: all as written.

## Parked backlog (no requester)
Numbers are stable (code cites "backlog N"); closed ones are in `docs/CHANGELOG.md`; details: `git show 6e20157c:tasks/backlog.md`.
- 0 — column segments on NVMe (ADR-0011; `pkg/graph/internal/segment`, `Config.RelSegments`): S4 merge, S6, S7 open
- 1 — failed `IO().Import` should emit no change-log record; import takes `c.mu` only, needs `c.txMu` (`importRollback.restoreRegistries`)
- 2 — test gaps 22a-f: tiered crash matrix, clock skew vs rotation, tiered metadata decoder fuzz, NaN/Inf/huge-dim inputs, race soak, `ValidationLimits`
- 3 — `ForEachAdjacentRelAt` / `ForEachAdjacentEndpointAt` bypass badger's decode-skip, fold deleted rels O(deleted) per call
- 4 — `core/temporal.go`: (i) `NodesDuring`/`RelsDuring` `end == 0` misses future-only; (ii) delete of a future-ValidFrom row (`TestDeleteAheadOfClock4`, fix = D4); (iii) `SelectAsOf` after a cascade; (iv) `NodeMatchesValidTime` unset ValidFrom; (v) snowflake horizon 2034-12-02
- 5 — P7 row compaction follow-ups, 388 B per HOP row left (key tokens, history versions, badger cache of frozen rows, string interning)
- 7 — `DeleteWithTx(id, t)` refuses a recorded `ValidTo >= t` (`core/tx_order.go`); goes with D4 and 25
- 10 — tiered lacks node-side `TemporalCandidateCapability`
- 13 — async ingest attributes group errors by numeric id, node and rel with one number land in the wrong group (`groupApplyError`, `core/ingest.go`)
- 16 — under `ValidStart`+`ValidEnd` range folds test the resolved version, `ByLabelAndProperty` matches anywhere in the interval (decision 6)
- 20 — `BenchmarkRelAtTx/{plain,cascaded}/{hot,cold}` missing
- 23 — `changeFeedPage` prefetches unused values, may read past the keyspace (`badgerstore_changelog.go:648-659`)
- 25 — `CloseVersion(far-1)` → pin → `Delete`: as-of rewind reopens `[1000,inf)` (`core/temporal.go:149`, `storeutil/asof_select.go:172`); with D4 and 7
- 26 — pre-allocator chains with two rows of one version keep the old order
- 31 — `NodeAsOf` misses a row above a version gap past current + 1 (`core/version_alloc.go`)
- 37 — tiered error-rollback restore: reader sees empty history between truncate and put (`tieredstore_write_history.go:913`)
- 39 — compaction stubs survive `Admin().Reset`; Import of an old ID can restart at version 0 (`core/compaction.go:285`)
- 40 — cascade resumption considers bounds before the last tombstone, one extra split row (`temporal_cascade.go:425,444`)
- 45 — `ErrNilCallback`, `ErrCommitClockExhausted`, `ErrForeignStampImplausible` lack an exported alias; `ErrMixedNumericColumn` outside the errors inventory test
- 46 — stale comments: ADR-0006 cites, `badgerstore_property_stats.go:38` lesson 62 (is 63), old B-numbers, `effective_timeline.go` header
- 47 — (a) `NodesByLabelAt` "folds history instead of K1" unverified; (b) RAM budget for property sidecars
- 71 — hull of current rows in O(1)/O(log n) (`ValidHullByLabel` is one pass per call)
- 76 — number equality lookups: one store call per width key (up to 12)
- 78 — `EndValidity` lacks `*WithTx`, `BatchBuilder` and ingest `Session` twins (`tombstoneSpec.keepPast`)
- 80 — whole-axis entities patched while closed keep a stale current row (`cascadeNewCurrent`, `core/temporal_cascade.go:798`)
- 81 — sharded rel row with local start, remote end: no in-key repair at open
- 82 — tiered ex-adjacency of a reference node at `DepthAll` opens every cold shard

## v5 (paused, row 61; restart needs René)
Local branch `v5-takeover` (not pushed). Open: WP-16 per-partition allocation + `Graph.Owner`; WP-18 WAL write-EIO fail-stop (Markus F2); WP-22 one byte framing `v5/internal/wire`; WP-28 `Recorded` evidence through dist op log + range-move export; WP-33 Scan streams index pages (65,536-row cap).
Breaking API-pass candidates for v5 (from docs/architecture.md "Deferred Architectural Decisions" 2026-06-10): split
`internal/core` into tx/temporal/mutate; rename `pkg/graph/hash`/`io` (stdlib shadowing); one parameterized iteration interface
instead of the history × deleted × depth × paged capability grid; `tier` stops returning `tiered.*` types and exposing
`RecoverBackgroundError`; pending/dirty counts in `StoreStats` (today `badger.Store.PendingWriteCount()`).
