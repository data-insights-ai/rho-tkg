# todo — what is next (v4 line)

Open work only, in order. Done work: `docs/CHANGELOG.md` (same row number). State and René's standing requests:
`HANDOVER.md`; old ledger (done rows, releases, review log, v5 WP history): `git show aeb7f8e0:tasks/todo.md`. Each step:
red test first (short, one named mutation), then the change; a door is a minor, a fix a patch; consumer data stays on
Flux (counts, sizes, relation names only); long runs only on René's word ("run").

## 0. Now — René 23 Oct: "i think we have to rewrite the full data chain, we wasted 3 up to 4x data storage and memory"
Evidence so far (row 121, sigma's one-fact run on the ai-soc BA slice-gate copy `/srv/fast/rene/rho-analysis-k10230918/store`):
a 2-row bar fact stores 1,132 B for a fact nobody sees (row 2 = a full copy + `ov_void`, sigma's old write-back); 72% of
each row is identity repeated per version; a re-derived fact keeps its first write as the current row and grows 2 history
rows per re-derivation; a history row read costs ~3.7 kB full, 6-7 kB as a delta (~560 B of properties).
1. [ ] The split per layer, one aggregate run on the kept Flux copy (owner sigma, `projects/derived-writeback` wbscan
   with rho v4.74.0's `Admin().NodeStoredSizes`; on René's "run"): property bytes vs stored bytes, history share,
   void/retraction rows, per relation. Proof: sigma's report, counts and sizes only.
2. [ ] René decides the rewrite per layer from 1, against a target (e.g. stored bytes per fact <= 1.5x its values).
   rho's levers, each a red test first: (a) the believed stretch becomes the current row (a re-derived fact reads
   current, not history); (b) an excise piece stores no row copy; (c) one anchor decode per history read (a delta row
   re-reads and fully decodes its anchor, per row); (d) a decoded row nearer its stored size (Node struct, a Temporal
   copy per skeleton, msgpack strings). sigma's: identity once instead of per version, one validity source, the write
   pattern of shifted re-derivations. ai-soc's: whether every re-derivation must be kept.
3. [ ] Then the rewrite, each layer its own release; ai-soc's cold /bars guard (`TestDerivedRowsColdBytesPerFactBoundedBySize`,
   at most 4x the fact's values) passes on Flux.

## 1. Row 121: listing bytes (ai-soc R37), after 0.2
Shipped v4.71.0-v4.74.0: the page door `ForEachNodeEffectiveByLabelAndProperties`, tiered read snapshot, winners read in
place (rho's cold guard `TestForEachNodeEffectiveByLabelAndPropertiesColdBytes`: 5.3x values at one version, 10.8x
rewritten, 14.9x with history winners). ai-soc measured v4.73.0: cold 9.1x / 32.6x, warm memory 4.0x / 7.8x.
- [ ] Per-entity skeleton cache keyed by the history epoch (badger/tiered `HistoryEpochCapability`): warm rewritten
  listings re-read the skeleton (~2 kB fixed per call once history exists, ~0.87 kB per version).
- [ ] Relationship decode re-copies its properties on install (no `SetPropertiesCanonicalShared` on Relationship).

## 2. Docs and guards
- [ ] (108) Cold-start test 2 on the finished md set (13 files): fresh agent, new questions (not test 1's 12). Check:
  each gap it finds is fixed in the owning file, docs tests green.
- [ ] (118) Guards for the rules the docs no longer carry as prose (René 2026-10-23: tests, not prose), each red-first:
  (a) `check-ledger.sh` fails on attribution lines in `git log <last tag>..HEAD`, and runs in `ci-docker` too; (b) docs
  test: every `lesson N` cite resolves to an index line 1-88 in CONTRIBUTING § Lessons; (c) source-scan test: raw
  `msgpack.Unmarshal` only in `SafeUnmarshal` + the 4 allowlisted flat-struct loaders (lesson 47); (d) reflection test:
  every msgpack-tagged wire field round-trips (lesson 39); (e) golangci `forbidigo` for `sort.Slice` /
  `reflect.DeepEqual` in non-test code (lesson 70) and `errorlint`; (f) design-rule gaps from the Opus audit:
  relationship-create kernel reachability, canonical hash, tombstone cache-miss, rotation boundary, reflection
  close-sweep of every public method. Check: each guard red on a planted violation.
- [ ] (119) (a) `TestTemporalPropertySeekAnswersEverLabelMembers/badger` misses the relabelled node at `TxPin: pinBefore`
  only beside other parallel tests (passes 20x alone; kept serial): check whether a write can share the pin's instant
  before calling it test-only. (b) `Compression: options.None` cannot disable compression (None is the zero value =
  Snappy); a `DisableCompression` option is a door (minor), only on a consumer request. (c) the rest of the shared store
  suite (history_generation, ordinal_lookup, propertytxmembers_range, reltype_temporal_deleted, compaction,
  labeltxmembers, retention_purge, property_bounds, ~5k lines) moves into pkg/graph/store as the scoped batteries did
  (c9e78bf6).

## 3. Waiting, each with its trigger
- (92) ai-soc's reopen profile (ReadHistory via `readUnderRLock` 526 s vs 57 s for fix5) arrives with rho version + callee
  under `fn` → act on that only; `readUnderRLock` (locks.go:80) unchanged since v4.66.0.
- A consumer report against v4.70.0+ (ai-soc's full Flux rerun; sigma O1-3 on `LowestAnsweredPins` and
  `RelEffectiveTimelinesByProperties`) → build only what blocks them (lesson 79 (10)).
- Origin watch (HANDOVER Appendix A, at session start): a Markus release → merge origin/main, check his code against the
  rules (one valid start, Void rows, Elements, format markers, head cache, version allocation), fix his CHANGELOG heading.
- D1 (102) ai-soc raises priority, or nothing else is open → resume branch `row102-pending-ranged` (base 6ac9ea5f;
  8fa7a967 = the two red tests, f4b27842 = WIP `badgerstore_write_buffer.go`), rebase onto main first; badger
  `rangePending` walks all of pending + flushing per per-entity overlay read. Check:
  `TestPendingOverlayCostIndependentOfPendingSize` + `TestRollbackManyCreatedRelsLinearInPending` green → patch.
- D2 ai-soc memory pressure (BA RESHEAP 0.53 → 1.13 GB in 24 h; 0.18 GB ristretto under `Session.checkEntities →
  latestStamps`) → one store-wide badger cache budget or a documented per-shard size (`badgerstore_options.go:182`).
- D3 (105 (b)) `RelEffectiveTimelinesByProperties` measures too slow on BA → rel composite tx-membership sidecar
  (2,500-3,500 LOC, 4 backends).
- D4 decision 3, or sigma/ai-soc needs P24 → tombstone writer `DeleteKeptValidTo` (fixes backlog 4 (ii), 7, 25): branch
  `tombstone-writer-v4.63` (`37a1ff56`), rebuild on the v4.66 axis code, ~½ day, a minor; heads-up to sigma + ai-soc.
- D5 a consumer hits one → backlog 80-82 and the review nits not taken (v4.69.2: 4, git 82558ea9; v4.70.0: 3, git 3c370aab).
- D6 next touch → repoint stale code cites of gone todo sections: `tasks/todo.md "R3 sizing"` (badger/tiered
  adjacency-disk tests; sizing in git f94060df), `tasks/todo.md, Tier D` (`core/node_delete.go:353,378`).

## 4. Waiting for René (my default)
1. Section 0.2: the rewrite per layer, after the split. Default: none until 0.1's numbers.
2. Review findings not taken under the scope rule (tests review #5-#10, code review #4-#7, docs #9-#10, old rows 77-82).
   Default: untaken until a consumer hits one.
3. v5 restart (row 61) and the first v5 tag. Default: paused; `v5.0.0-alpha.1` at S2 exit once restarted.
4. Tombstone writer rebuild (D4). Default: parked until sigma or ai-soc needs P24.
5. Retention purge vs retained pins (row 104; a purged entity answers not-found at pins it answered before): (a) no rho
   change, sigma treats not-found for a seen id as purged; (b) `ErrRetentionExpired` via a per-entity purge tombstone.
   Default: (a).
6. `UntimedWholeAxis` default for new stores: announced in CHANGELOG `[4.65.0]` Deprecated for "the next minor", held
   for five minors. Default: stays held; the next minor flips it or rewords the notice.
7. Backlog 16 (range folds: resolved version vs predicate-anywhere). Default: keep today's documented behaviour.
8. Shard upgrade (row 117) and its defaults, below in 5. Default: parked until René restarts it.

## 5. Parked, each with its trigger
### Backlog (no requester: a consumer hits one, or René's word)
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
- 16 — under `ValidStart`+`ValidEnd` range folds test the resolved version, `ByLabelAndProperty` matches anywhere in the interval (decision 7)
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

### v5 (row 61; trigger: René restarts it)
Local branch `v5-takeover` (not pushed). Open: WP-16 per-partition allocation + `Graph.Owner`; WP-18 WAL write-EIO
fail-stop (Markus F2); WP-22 one byte framing `v5/internal/wire`; WP-28 `Recorded` evidence through dist op log +
range-move export; WP-33 Scan streams index pages (65,536-row cap). Breaking API-pass candidates (architecture.md
"Deferred Architectural Decisions" 2026-06-10): split `internal/core` into tx/temporal/mutate; rename `pkg/graph/hash`/`io`
(stdlib shadowing); one parameterized iteration interface instead of the history × deleted × depth × paged capability
grid; `tier` stops returning `tiered.*` types and exposing `RecoverBackgroundError`; pending/dirty counts in `StoreStats`.

### Shard upgrade (row 117; trigger: René restarts it, parked 2026-10-23)
- Plan: (117) Per-tier data directories (René via ai-soc 2026-10-22: hot RAM, warm NVMe, cold HDD). Today every tier is a badger shard under one `DataDir` (`tieredstore.go:574-583`, shard dir `:1471`); "hot" is on disk, tiers are catalog labels (`shard_catalog.go:30-35`); demotion to cold does not close the store (`tieredstore_catalog.go:162-166`; `architecture.md:780` wrongly says it does). Plan (Opus analysis, Fable review 2026-10-23): one `Config.ColdDir` (empty = `DataDir`, byte-identical catalog), `ShardEntry.Volume`; hot, warm, reference, archive, `meta/` stay on NVMe; the mover takes only cold event shards, never ref/archive/warm (sigma keeps Derived in RefLabels). Move = `Flush`, record badger `MaxVersion`, stream copy (`NewStream`→`StreamWriter`, keeps versions; rho reads no older MVCC versions; Flush msyncs tables + dirs) into a fresh `ColdDir/events/<name>.moving-<nonce>`, close, rename, fsync parent. Switch under `ts.mu.Lock` → `shardMu` (the store-wide-op lock is only an RLock, `tieredstore.go:1182`): re-check `ts.eventShards[name]==es`, `Flush` + same `MaxVersion` (respect `SharedFlushHold`), drain `activeReqs` like drop (`retention_purge_drop.go:120-135`), close, `store=nil`, catalog save = commit point, then remove source. Steps, each with its red test: (a) XS `ts.shardDir` for `:1471` and the 4 `RemoveAll` on `ts.dataDir` (`tieredstore_catalog.go:83,129,379`, `retention_purge_drop.go:207`), patch; (b) S New/lazy-open refuse a missing shard dir (today badger creates an empty shard) — behaviour change, ships in the minor with CHANGELOG Changed; (c) S `ColdDir` + `Volume` + format marker as a sentinel entry the current validator rejects (`{name:"__format__",kind:"catalog/2"}`; a JSON field would be ignored by older rho, `shard_catalog.go:98`), written only at a store's first move (ai-soc), named in the CHANGELOG as a one-way door (sigma); (d) M copy without switch, `ExportSince` + encryption parity; (e) M switch + orphan rule in New: `*.moving-*` → remove, `Volume=cold` + NVMe dir → remove source, `Volume=""` + ColdDir dir → remove dest; crash at each of copy/rename/save/remove; (f) S races: drop (also `dropOneShard` saves without `ts.mu`, `retention_purge_drop.go:180-190`, and rotation's `restoreShards` can undo a concurrent commit), write, reader, rotation, `RebuildCatalog` keeps `Volume`, `PromoteColdShardsAtOpen` on a moved shard; (g) S `ColdCompression` (per-table codec in the badger manifest, no persistence; document `BlockCacheSize==0` trap); (h) S mover on the idle-sweep tick, one shard at a time, migrates today's cold shards; (i) S cold-shard opens per read as a read-cost/EXPLAIN field (sigma p2/p3), can ship first; (j) XS architecture.md contract + `:780` fix, persistence.md:372 layout. ~2 weeks, one minor. Alternative to note for René: symlink `events/<name>` → ColdDir (no catalog field, no one-way door, older rho reads it; drop must `EvalSymlinks`). Blocker: reference shard growth below. ai-soc's side (`-cold-data`, `internal/graphmgr/manager.go:97-103,495-505`, `cmd/aisoc/core.go`, `serve.go:172`) sent to ai-soc-main-45.
- Defaults: (a) hot = NVMe + badger caches, not RAM (a RAM tier breaks DurableCommit/SyncWrites crash safety); (b) `ColdDir` only, archive stays on NVMe; (c) a cold shard promoted at open stays on HDD; (d) the catalog format marker is an accepted one-way door (older rho refuses a moved store); (e) ZSTD on cold is opt-in (`ColdCompression`). Default: all as written; sigma and ai-soc agree to (a)-(e) (2026-10-23).
- Reference shard growth (blocker; ai-soc measured on Flux 2026-10-23): ref shard ~2/3 of store bytes (~60 GB per BA day estimated, never moves; 28 days ≈ 1.7 TB vs 870 GB `/srv/fast`), so `ColdDir` alone does not fit NVMe. Cause (Fable, from code): not history (ai-soc never updates nodes) but one ref node per instance (Process/File/RegValue/LocalObject keyed per record, ai-soc `readers/defender/inputs*.go`) because a unique key needs a reference label (`core/unique_constraints.go:75-83`), plus cut nodes (~80-120 B/record). Options: (1) old ref versions to day shards — no versions to move, L, reject; (2) purge ref by age — exists for nodes (`tiered/retention_purge.go`), safe for cut/ended/voided, unsafe for instances with newer edges; (3) keyed event labels (index on ref, row on mint day shard) — M-L, ~2 weeks, a minor; (4) no rho change: ai-soc makes per-instance labels event-class, dedupes via its RAM window (`DedupeWindow`), purges cut nodes with (2). Default: (4) now, (3) only if a consumer needs instance identity beyond the dedupe window; first a prefix-size scan of the Flux ref dir to confirm instances vs cut nodes. Product question: may a Process re-seen after the dedupe window become a new node?
