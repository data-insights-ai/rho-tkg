# todo

What rho does next, in order. RH's open decisions are the items that wait for him, each with my default. Standing orders:
`AGENTS.md`; code rules: `docs/CONTRIBUTING.md`. Optional local state (gitignored): `HANDOVER.md`. Done work:
`docs/CHANGELOG.md`, under the same tag; measurements: `tasks/evidence/<ITEM>/`.

## Now

- **DDLMETER-REST** — the composite, temporal and high-frequency index DDL have no ctx door, so Sigma's
  createPageComposite and its state and overview pages build on no meter. Done when Sigma's three calls take one. **rho/sigma:** Sigma's todo DDLMETER-REST.
- **SCALE** (the standing goal, AGENTS.md § Standing orders) — Done when `docs/architecture.md` lists what grows
  with total volume instead of per shard (RAM per entity; open, crash recovery, Clear and compaction cost per store
  size; hot-shard write rate against peaks), each measured and bounded or queued here. Bounded so far: existence
  maps (EXISTMAP v4.84.0, ~8 B per node, reopen ~0.25 s per 1 M), open walks (OPENWALK v4.87.0), crash reopen
  (OPENIMAGE v4.88.0/v4.89.0). Open walls: the reference shard keeps every Derived fact and keyed node for ever;
  lazy per-entity sidecars (`relValidIdx` 43-67 B per rel on plain badger, belief watermarks, tx membership, history
  presence, idTable); hot-shard write rate (WRITERATE). The path for RAM: FIXED-MEMORY (Next). Evidence: `tasks/evidence/SCALE/`. **rho/sigma:** names the walls that grow with total volume, so each is bounded or queued.
- **GATEBUDGET** — warm gate to 60 s or less. 19 Nov: `make check` 170.60 s (load 16-26; 193.98 s at load 54; 139.46 s before the CRASHFIX fix round's 14 tests); `make dev`
  warm 1.9-2.3 s unchanged, 2.1 s leaf edit, 28.1 s core edit (core test compile ~21 s). Done when a normal warm
  edit gate meets 60 s. Waits for measured reductions. **rho/sigma:** none (validation turnaround).
## Next
- **TAGFLAKES** (22 Nov, seen in the tag CI and a Linux load run) — three tiered tests flake under CI load. (a)
  `TestReadFanoutClosesTransientColdShardAfterOverlappingReads`: the first reopen of the closed cold shard was refused
  "Cannot acquire directory lock" (CI run 44999803833). Not reproduced: macOS about 2,500 runs alone (cpu 1, 2), 200
  of the fanout group (cpu 4), 2 full package runs; Linux (golang:1.27.4 container, RHO_GATE=1) 15 full package runs (8 at 4 CPUs, 7 at 2 CPUs with -cpu 4).
  Every open and close of an event shard handle reads as serialized by shardMu or the move gate; a forked child
  holding the lock measured 0 refusals in 300 reopens under 64 forking goroutines. (b) `TestCrashFix_CloseDuringAStep`
  (`crashfix_settle_test.go:999`: 2 settle steps, want 1): red in 1 of 7 full Linux runs at 2 CPUs with -cpu 4. (c) `TestEventClockHotWarmReadsStampNoAccess`
  (`event_clock_test.go:80`: "shard 2026-W47 is cold, open false: want an open cold shard"): red in v4.107.4's tag CI
  run 45009839982 attempt 1, green on the rerun. Done when
  each has a measured cause and fix. **rho/sigma:** none (release CI).
- **READCOST** (RH 19 Nov: "analyse and fix this in a general way") — a read door can do work its caller's ReadMeter has
  not charged (H1, H2, COMPWALK, COMPPAGE, range doors: a nil or optional tally, a count after the work, private walk
  loops); one mechanism every native iterator and open goes through, with a required tally, and every public door on
  it. Done when ai-soc's `TestCompositeWalkStopsAtTheLimit` and `TestReadDoorsChargeBeforeWork` (one row per public
  door, by reflection) are green. Stopped by RH 19 Nov mid-build (73 files changed, not compiling) in `.worktrees/readcost-20261119`; analysis kept. Detail:
  tasks/evidence/READCOST/, COMPWALK/. **rho/sigma:** read memory per cut.
- **KILLTEST** (ai-soc todo 15, design accepted 19 Nov) — a cut killed mid-write on ai-soc's cycle path reopens with
  equal answers and no refusal. Done when ai-soc's kill_test.go on todo 14's layout is green. First rho's
  `badger.SetFlushHookForTest` and an OpenStats field, each tested and reviewed; then the ai-soc test (rho's scratch,
  003-aisoc commits). Detail: tasks/evidence/KILLTEST/design.txt. **rho/sigma:** none (crash safety).

- **ASOFCOST-L3** — a plain badger or sharded read that pinned the as-of view before `Close` returns badger's untyped error, not `ErrStoreClosed` (tiered waits for shard pins). Low; ASOFCOST closed 19 Nov by 003-aisoc. Detail: tasks/evidence/ASOFCOST/final-review.txt. **rho/sigma:** none.
- **R2** (SCALE; accepted by 003-aisoc 19 Nov) — rho's per-entity RAM (live ID sets, history presence and stamps, four caches without configured caps; 664-850 B a logon) is capped or moved to badger keys (S1-S3). Paused 19 Nov (RH: necessary work first); S1 parked uncommitted in `.worktrees/r2s1-20261119`. Detail: tasks/evidence/R2/design.txt, s1-build.txt. **rho/sigma:** RAM per entity, rho side.
- **R1-a** — after a reopen, `NodeEffectiveTimelineFromCtx` with no head entry folds the whole chain. Paused 19 Nov (RH: necessary work first). Detail: tasks/evidence/R1-a/row.txt. **rho/sigma:** read CPU per entity after a restart.
- **R1-b** — the head entry is capped at 2,048 pieces (`effective_head.go:97`), so beyond that the tail read goes whole. Paused 19 Nov (RH: necessary work first). Detail: tasks/evidence/R1-b/row.txt. **rho/sigma:** read CPU per long chain.
- **R3** — an untyped adjacency read of a reference node lists every shard with rows, not only those holding its edges. Paused 19 Nov (RH: necessary work first). Detail: tasks/evidence/R3/row.txt. **rho/sigma:** shard opens per adjacency read.
- **SHORTSCAN** — `testtier.Slow` becomes the only skip in place of bare `testing.Short()` skips, with a guard test that fails on a bare one. Paused 19 Nov (RH: necessary work first). Detail: tasks/evidence/SHORTSCAN/scan.txt. **rho/sigma:** none (the gate runs the long-tier tests).
- **ARENASTAT** — `ShardAccessStats` gives one arena figure per open shard, its open image included (5 B a node), for ai-soc's D9 (its todo 12). Paused 19 Nov (RH: memory growth after the small BA example works); design stopped unfinished. Detail: tasks/evidence/ARENASTAT/row.txt. **rho/sigma:** fixed memory per open shard, reported.
- **CLEARRELCOL** (R2 S1 builder, 19 Nov; unverified) — badger `Clear` bumps neither `relEpochCoarse` nor the rel-type stripes and keeps `relColumns`, so a rel-type column snapshot from before Clear may read as fresh. Parked 19 Nov: not on ai-soc's path. Detail: tasks/evidence/CLEARRELCOL/row.txt. **rho/sigma:** none.
- **WRITEMETER** (R2 design risk 5) — write-door probes and endpoint row reads run outside any meter. Waits for 003-aisoc's design, after R2. Detail: tasks/evidence/WRITEMETER/row.txt. **rho/sigma:** read memory on writes.
- **Round 2 read items** (ai-soc's read-back inventory, 3 Nov) — UNIQUEMISS, OWNERDROP, EDGEROW, CASCADEROW, FLOORDECODE, DBVIEW. Waits for ai-soc's one measuring run on main's tip, not before ai-soc v5.6.0 (a Flux run needs RH's word). Detail: tasks/evidence/ROUND2/read-items.txt. **rho/sigma:** CASCADEROW, FLOORDECODE, maybe DBVIEW under sigma's load.
- **FIXED-MEMORY** (RH 31 Oct; the RAM path for SCALE; badger stays) — per-entity answers leave the Go heap: heap fixed, data on NVMe and block, RAM is page cache. Step 1 (existence) moved to R2 S2; next: R2. Detail: tasks/evidence/FIXED-MEMORY/step1-design.txt. **rho/sigma:** small today; keeps rho's share of RAM flat as data grows.
- **WRITERATE** (SCALE) — a hot shard's ingest is capped by one serial flush path (durable 16-38 k facts/s with one writer, goal 46 k/s sustained). No measured wall today. Detail: tasks/evidence/WRITERATE/results.txt. **rho/sigma:** headroom only: sigma's tkgd defaults to async, so ASYNCSTAGE already serves it.

## Waiting
- **H2** — `Rels().ByTypeAndPropertyCtx` without a valid window on an on-disk rel index walks with a nil budget. Waits for the first metered rel property read without a valid window on a type in RelPropertyIndexOnDiskTypes (003-aisoc 19 Nov; Sigma's reads use a window and are charged). Detail: tasks/evidence/INTEGRATE/h2-design.txt. **rho/sigma:** none today.
- **TAKEOVERREVIEW** — fix the findings of the Fable review of `007ab394..8301282d` in its § 5 order
  (`tasks/evidence/INTEGRATE/takeover-review.txt`); CRASHFIX H3 and H4 are fixed, the H4 cleanup with its two fix
  rounds (`tasks/evidence/CRASHFIX/cleanup-review.txt` C2-C14, `cleanup-rereview.txt` R1-R9) and BUILDHEAP are
  committed (bd1257f6), ASOFCOST too (46bb977b); left are H1 (paged member door under a budget takes no tally) and H2 (on-disk rel
  property index: rejected entries not counted), both rho's, not on ai-soc v6's path (tasks/evidence/INTEGRATE/h1h2-reach.txt, h2-design.txt § 0): H1 is READCOST's, H2 has its row in Waiting.
  Done when both are fixed (ai-soc's reopen run is filed, `cleanup-aisoc-reopen.txt`). Waits for READCOST and a caller of H2. **rho/sigma:** the handle sweep
  repairs a closed cold shard's missing seal, former-label set or recovery.
- **RELHIST** — reads and writes stop rebuilding a key's whole history; P1-P3, P5 phase 0 and option C released
  (v4.103.0-v4.105.2), C2, first-read misses, pre-flight and option F left. Done when the next profile of ai-soc's
  workload shows history reads below 10 % of alloc_space. Moved to Waiting 19 Nov (memory and profiling work after the
  small BA example, RH). Detail: tasks/evidence/RELHIST/design.txt. **rho/sigma:** sigma's alloc_space.
- **WARMMEM** — measure writable and inactive shard costs: R3-2 FLOOR (designed) and R3-4 B0 (ai-soc v6 on three BA
  examples). Done when RH's F0–F4 choice is implemented and measured, and RSS, swap time and per-tier writes are
  measured on three BA examples. Decided 19 Nov (003-aisoc): F0 (today's write arenas), no Flux run; after the small BA example (RH). Detail:
  tasks/evidence/WARMMEM/row.txt. **rho/sigma:** lowers fixed memory per open handle.
- **CATROLLBACK** — a catalog rollback (`restoreShards`) puts back entries another lock holder changed and drops entries added since its snapshot (cleanup-review C11). Waits for a failing test or a consumer request. Detail: tasks/evidence/CATROLLBACK/row.txt. **rho/sigma:** none.
- **COMMENTREF** — about 50 comments added by the takeover cite a review section or design file, against the Rules digest comment rule. Waits for a code item in those files. Detail: tasks/evidence/COMMENTREF/row.txt. **rho/sigma:** none.
- **14 Nov handover follow-up** — verify comment IDs, BeginTx lock comments, `closeRelVersionInternal` endpoint hashes, three CONTRIBUTING lesson gaps. Open until each is reproduced or cleared. Detail: tasks/evidence/HANDOVER-14NOV/row.txt. **rho/sigma:** undetermined.
- **R4WAIT** (known red) — a second `WaitApplied` on a failed async token returns nil, since the first read prunes the failure (`ingest.go:399`). Waits for an async-session consumer (ai-soc uses sync). Detail: tasks/evidence/R4WAIT/row.txt. **rho/sigma:** none.
- **CORESPLIT** — core test package split: 21 s compile path of a core edit (`make dev` 28 s). Deferred (003-aisoc 18 Nov), revisit if core edits dominate. Detail: tasks/evidence/CORESPLIT/row.txt (measures: TESTSPLIT/). **rho/sigma:** none.
- **APPENDLIST** (ai-soc V1r, 10 Nov; question) — an append-only list per node with a header, since a list append via nodes.Update rewrites the list twice. Needs ai-soc's spec and RH's word (5-8 days, a minor). Detail: tasks/evidence/APPENDLIST/row.txt. **rho/sigma:** write CPU and bytes per list append.
- **COLDLRU** (ai-soc RHOCOLD a, 9-10 Nov) — one advance reads every cold day shard, so the cap grows with the archive unless sigma's read-back stops reaching old days (sigma's COLDSET); a larger cap is no correction. Waits for sigma's COLDSET. Detail: tasks/evidence/COLDLRU/row.txt. **rho/sigma:** shard opens and RAM per advance.
- **COLDHEAP** (ai-soc RHOCOLD b) — +970 MB over 3 days from what is never cold (reference shard, hot/warm, demoted open handles, degree tracker) and per-shard entity caches (224 MB of decoded rows). Waits for ai-soc's retainer view. Detail: tasks/evidence/COLDHEAP/row.txt. **rho/sigma:** rho's RAM over days that no tier can free.
- **LISTREAD** (sigma 31 Oct) — label-and-property pages read through one forward scan since v4.105.4; left: a batch `HasHistory(ids)` through one scan. Waits for sigma's restore timing on v4.105.4 (only on RH's word). Detail: tasks/evidence/LISTREAD/measure.txt. **rho/sigma:** restart time.
- **ASOFSEEKCOST** (SCALE/WRITERATE; 003-aisoc 19 Nov) — the disk transaction-time seek index (RH-R2-ASOF) is built on every badger history append, so on ai-soc's path: 52 B per indexed history row and 51 B per entity proof; owner's in-memory sample: append 2.0→7.9 µs per node, 2.5→6.3 µs per rel (22→63 allocs).
  No work now; waits for a write-path profile of ai-soc's that shows the cost. Detail: ai-soc arch-v6-rh/_attic/records-20261119/RH-changes-sigma-rho.md:592-598. **rho/sigma:** write CPU and disk per history row.

On consumer runs (ai-soc, sigma):
- **VALUETHRESH** (v4.104.0) — sigma's 2-day budget profile with 32 KiB on tiered should lose the compaction CPU of its 64 KiB DerivedPage blocks (+900 of +2,640 s) at acceptable storage growth. Waits for ai-soc's run (RH says "run"). Detail: tasks/evidence/VALUETHRESH/measure.txt. **rho/sigma:** direct: ~900 of 2,640 extra CPU s; the price is an uncompressed value log.
- **STATEREC** (sigma's design review 30 Oct) — after WHOLEROW (v4.100.0): the TxPin walk by IDs with a cross-shard merge (ASOFWALK later steps), and DEPTHLOOK; also REPORT. Waits for sigma wiring WHOLEROW after RAMPAGE. Detail: tasks/evidence/STATEREC/row.txt. **rho/sigma:** direct: sigma's state restore and as-of cuts stay flat with churn.
- **CLEANOPEN** (ai-soc 27 Oct) — a clean reopen of a BA store spends 11-31 s in tiered.New, mostly the reference shard, and grows between opens; sigma's RESTART asks open time flat as the store grows. Waits for ai-soc's RESTART item and RH's word before rho reads profiles on Flux. Detail: tasks/evidence/CLEANOPEN/row.txt. **rho/sigma:** direct for restart.
- **TIERCLOCK** (v4.98.0) — ai-soc's BA replay should rotate shards as live data would (pin: Clock = newest committed valid time). Waits for that run. Detail: tasks/evidence/TIERCLOCK/row.txt. **rho/sigma:** none (ai-soc's replay).
- **WRITEAMP** (ai-soc 3 Nov) — measure compaction input bytes over flushed bytes per shard for the cut-shaped write, then set BaseLevelSize, BaseTableSize and MemTableSize per tier. Waits for a quiet PC and ai-soc's tiered d2. Detail: tasks/evidence/WRITEAMP/row.txt. **rho/sigma:** possible: per-tier settings for sigma's tiered store; unmeasured.
- **PURGEIDS** (committed 5ea08266) — `Nodes().PurgeByIDsCtx`: purge by ID, metered before any open; PURGERESIDUE fixed for every purge; a by-ID purge copies no label list (PURGECURSOR stays for the label purge). Done when Sigma's PAGEGC calls it on ai-soc's path. Waits for Sigma's PAGEGC. **rho/sigma:** RAM and disk freed when Sigma's PAGEGC purges retired pages by ID.
- **PURGEIDS-XSHARD** (review 19 Nov) — tiered `PurgeByIDsCtx` cleans an edge whose other end lies on another shard after the purge: that open is uncharged, and a stop between the shards leaves the edge's half for good (a re-run skips the purged node). Sigma's pages carry no edge. Waits for a caller with such edges. **rho/sigma:** none now.

On RH:
- **CONTRACT** (5 Nov analysis, 5a) — run ai-soc's tests against the rho commit before rho's tag. Waits for RH (default: skip; ai-soc's own gate catches such a break before it pins). Detail: tasks/evidence/CONTRACT/row.txt. **rho/sigma:** none (release process).
- **RACEAFTER** (RH 5 Nov: "it creates load on the PC") — the release starts `make race` detached after each patch push. Waits for RH (default: detached after release only); options: drop it, `nice` it, cap at `-p 2`. Detail: tasks/evidence/RACEAFTER/row.txt. **rho/sigma:** none (shared PC load).
- **ASOFVALID** (ai-soc 5 Nov) — a `valid=` page costs ~8 s on the masked copy (v4.98.4), 76 % one point row read per member of a cold shard. Waits for RH: (b) a cold checkout loads the rel-type envelope (~2 days) or (a) FIXED-MEMORY step 2; default (b). Detail: tasks/evidence/ASOFVALID/row.txt. **rho/sigma:** none direct (ai-soc's /tkg/asof door).
- **CHAIN** — which layer (ai-soc, sigma, rho) wastes how much of the 3-4x storage and memory; memory split known (26 Oct), storage split open. Waits for RH's word and a free Flux (default: none before the numbers). Detail: tasks/evidence/CHAIN/row.txt. **rho/sigma:** says where the 3-4x goes; the storage split is the missing half.
- **STORE** — ai-soc's store layout (delta encoding per label, Derived/Session placement, instance nodes ~2/3 of the BA store); only p4b's settings are agreed. Waits for RH with ai-soc and sigma (STORECFG). Detail: tasks/evidence/STORE/row.txt. **rho/sigma:** some: decides which tier sigma's pages live on.
- **PREEPOCH** (ai-soc 29 Oct, product question) — a customer backfill with valid times before the 2026-01-01 snowflake epoch is refused (ErrValidFromOutsideIDRange, ADR-0013). Waits for RH (default: stays refused). Detail: tasks/evidence/PREEPOCH/row.txt. **rho/sigma:** none.
- **BRANCHES** (RH 3 Nov: not now) — seven archived branches, all contained in `origin/archive/unmerged-2026-10-27` (93c43a64), to delete. Waits for RH to run `git push origin --delete`. Detail: tasks/evidence/BRANCHES/row.txt. **rho/sigma:** none (housekeeping).

On Markus:
- **ORIGIN** — every release Markus makes on origin is merged and checked against rho's rules. Waits for Markus's releases. Detail: tasks/evidence/ORIGIN/row.txt. **rho/sigma:** keeps one rho line, so sigma never pins a tag that lacks the other's fixes.
- **CIGATES** — CodeQL runs from `codeql.yml` only. Done when the default setup is off (org setting, RH got HTTP 422) and `codeql.yml`'s first run is green. Detail: tasks/evidence/CIGATES/row.txt. **rho/sigma:** none.
