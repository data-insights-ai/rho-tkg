# todo

What rho does next, in order. The maintainer's open decisions are the items that wait for the maintainer, each with a default. Standing orders:
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
  presence, idTable); hot-shard write rate (WRITERATE). The path for RAM: FIXED-MEMORY (Next). **rho/sigma:** names the walls that grow with total volume, so each is bounded or queued.
- **GATEBUDGET** — warm gate to 60 s or less. 19 Nov: `make check` 170.60 s (load 16-26; 193.98 s at load 54; 139.46 s before the CRASHFIX fix round's 14 tests); `make dev`
  warm 1.9-2.3 s unchanged, 2.1 s leaf edit, 28.1 s core edit (core test compile ~21 s). Done when a normal warm
  edit gate meets 60 s. Waits for measured reductions. **rho/sigma:** none (validation turnaround).
## Next
- **READCOST** (19 Nov: "analyse and fix this in a general way") — a read door can do work its caller's ReadMeter has
  not charged (H1, H2, COMPWALK, COMPPAGE, range doors: a nil or optional tally, a count after the work, private walk
  loops); one mechanism every native iterator and open goes through, with a required tally, and every public door on
  it. Done when ai-soc's `TestCompositeWalkStopsAtTheLimit` and `TestReadDoorsChargeBeforeWork` (one row per public
  door, by reflection) are green. Stopped by the maintainer 19 Nov mid-build (73 files changed, not compiling); analysis kept.
 **rho/sigma:** read memory per cut.
- **KILLTEST** (requested by ai-soc, design accepted 19 Nov) — a cut killed mid-write on ai-soc's cycle path reopens with
  equal answers and no refusal. Done when ai-soc's kill test on its cut layout is green. First rho's
  `badger.SetFlushHookForTest` and an OpenStats field, each tested and reviewed; then the ai-soc test (rho's scratch,
  the coordinating session commits). Since v4.107.9 (COMMITORDER) the tiered
  store commits the reference shard last on every flush path, because a kill between the shard flushes had kept a
  cut record whose relationships on the day shards were lost (found by ai-soc's restart fault test); a kill can now leave event rows
  without their record, never the record without its rows. **rho/sigma:** none (crash safety).

- **ASOFCOST-L3** — a plain badger or sharded read that pinned the as-of view before `Close` returns badger's untyped error, not `ErrStoreClosed` (tiered waits for shard pins). Low; ASOFCOST closed 19 Nov by the coordinating session. **rho/sigma:** none.
- **R2** (SCALE; accepted by the coordinating session 19 Nov) — rho's per-entity RAM (live ID sets, history presence and stamps, four caches without configured caps; 664-850 B a logon) is capped or moved to badger keys (S1-S3). Paused 19 Nov (maintainer: necessary work first); S1 parked uncommitted. **rho/sigma:** RAM per entity, rho side.
- **R1-a** — a bounded correction in a cold process lists the whole chain once (`nodeMissPlan`). review ruling 23 Nov: the stamps sidecar proves `Rising` (each row starts at or above every finite own bound below it) and `Void`, so a suffix entry carries bounds from the lowest read row and plans. Not built. **rho/sigma:** write CPU per fact after a restart.
- **R1-b** — the head entry is capped at 2,048 pieces (`effective_head.go:97`), so beyond that the tail read goes whole. Paused 19 Nov (maintainer: necessary work first). **rho/sigma:** read CPU per long chain.
- **ADJHIST** (ADJDOOR review, 22 Nov) — tiered: a deleted relationship's history read asks every unsealed history shard and each sealed one whose ID span holds it; the ex-adjacency walk could name the shard. Waits for a cut refused at shard opens on ai-soc's path. **rho/sigma:** shard opens per endpoint probe.
- **R3** — an untyped adjacency read of a reference node lists every shard with rows, not only those holding its edges. Paused 19 Nov (maintainer: necessary work first). **rho/sigma:** shard opens per adjacency read.
- **SHORTSCAN** — `testtier.Slow` becomes the only skip in place of bare `testing.Short()` skips, with a guard test that fails on a bare one. Paused 19 Nov (maintainer: necessary work first). **rho/sigma:** none (the gate runs the long-tier tests).
- **MEMACCT** — `tier.API.MemoryStats()` gives per tier the RAM of the open stores by holder, badger's arenas included (requested by ai-soc; a sum per tier, not a row per shard). Built 22 Nov; waits for ai-soc and sigma reading it. **rho/sigma:** fixed memory per open shard, reported.
- **MEMHEAP** (MEMACCT's heap arm) — at a 512 MiB memtable 200,000 node writes grow the live heap 1.23 of the account (6.95 against 5.67 MB, 23 Nov); the rest is the entity cache's items map and list above `perEntryOverhead` (160 B an entry, unsplit). Next: size it per Go map layout. **rho/sigma:** unaccounted RAM per write.
- **FLUSHPASSCOST** (COMMITORDER review, 22 Nov) — every reference flush with rows now runs a flush pass over each open writable shard, and no test bounds its cost as the open shards grow. Waits for a cost test. **rho/sigma:** flush time per open shard.
- **PARKSPIN** (COMMITORDER review, 22 Nov) — `coParkedInWait` (tiered commit_order_crash_test.go) reads goroutine stacks into a 1 MB buffer, so with thousands of goroutines the wait test can spin. Test only. **rho/sigma:** none.
- **BLOCKCACHEFLAKE** (v4.107.10 tag CI) — `TestStorageStats_CompressedReadsCountInTheBlockCache` failed once in CI (431 misses, nothing added); not reproduced in 400 runs (-cpu 1,4) and 800 as 8 concurrent GOMAXPROCS=1 runs (23 Nov). Suspected, unproven: ristretto applies Sets asynchronously. Waits for a second failure. **rho/sigma:** none (test flake).
- **LENDLAYOUT** (v4.107.9 check, 22 Nov) — `TestLendSortedLayeredAgreesWithPointReads` failed its layout precondition under load (tables per level [2 0 0 3], no L1 or L2): buildLayeredLendStore leaves L1 and L2 to badger's compactor timing. Passes 10 of 10 alone. **rho/sigma:** none (test only).
- **CLEARRELCOL** (R2 S1 builder, 19 Nov; unverified) — badger `Clear` bumps neither `relEpochCoarse` nor the rel-type stripes and keeps `relColumns`, so a rel-type column snapshot from before Clear may read as fresh. Parked 19 Nov: not on ai-soc's path. **rho/sigma:** none.
- **WRITEMETER** (R2 design risk 5) — write-door probes and endpoint row reads run outside any meter. Waits for the coordinating session's design, after R2. **rho/sigma:** read memory on writes.
- **Round 2 read items** (ai-soc's read-back inventory, 3 Nov) — UNIQUEMISS, OWNERDROP, EDGEROW, CASCADEROW, FLOORDECODE, DBVIEW. Waits for ai-soc's one measuring run on main's tip (a test-host run needs the maintainer's word). **rho/sigma:** CASCADEROW, FLOORDECODE, maybe DBVIEW under sigma's load.
- **FIXED-MEMORY** (31 Oct; the RAM path for SCALE; badger stays) — per-entity answers leave the Go heap: heap fixed, data on NVMe and block, RAM is page cache. Step 1 (existence) moved to R2 S2; next: R2. **rho/sigma:** small today; keeps rho's share of RAM flat as data grows.
- **WRITERATE** (SCALE) — a hot shard's ingest is capped by one serial flush path (durable 16-38 k facts/s with one writer, goal 46 k/s sustained). No measured wall today. **rho/sigma:** headroom only: sigma's tkgd defaults to async, so ASYNCSTAGE already serves it.

## Waiting
- **H2** — `Rels().ByTypeAndPropertyCtx` without a valid window on an on-disk rel index walks with a nil budget. Waits for the first metered rel property read without a valid window on a type in RelPropertyIndexOnDiskTypes (the coordinating session 19 Nov; Sigma's reads use a window and are charged). **rho/sigma:** none today.
- **TAKEOVERREVIEW** — fix the findings of the review of `007ab394..8301282d` in its § 5 order; CRASHFIX H3 and H4 are fixed, the H4 cleanup with its two fix
  rounds and BUILDHEAP are
  committed (bd1257f6), ASOFCOST too (46bb977b); left are H1 (paged member door under a budget takes no tally) and H2 (on-disk rel
  property index: rejected entries not counted), both rho's, not on ai-soc's path: H1 is READCOST's, H2 has its row in Waiting.
  Done when both are fixed (ai-soc's reopen run is filed). Waits for READCOST and a caller of H2. **rho/sigma:** the handle sweep
  repairs a closed cold shard's missing seal, former-label set or recovery.
- **RELHIST** — reads and writes stop rebuilding a key's whole history; P1-P3, P5 phase 0 and option C released
  (v4.103.0-v4.105.2), C2, first-read misses, pre-flight and option F left. Done when the next profile of ai-soc's
  workload shows history reads below 10 % of alloc_space. Moved to Waiting 19 Nov (memory and profiling work after the
  small production-shaped example, the maintainer). **rho/sigma:** sigma's alloc_space.
- **WARMMEM** — measure writable and inactive shard costs: R3-2 FLOOR (designed) and R3-4 B0 (ai-soc on three production-sized
  examples). Done when the maintainer's F0–F4 choice is implemented and measured, and RSS, swap time and per-tier writes are
  measured on three production-sized examples. Decided 19 Nov (the coordinating session): F0 (today's write arenas), no test-host run; after the small production-shaped example (the maintainer).
 **rho/sigma:** lowers fixed memory per open handle.
- **CATROLLBACK** — a catalog rollback (`restoreShards`) puts back entries another lock holder changed and drops entries added since its snapshot . Waits for a failing test or a consumer request. **rho/sigma:** none.
- **COMMENTREF** — about 50 comments added by the takeover cite a review section or design file, against the Rules digest comment rule. Waits for a code item in those files. **rho/sigma:** none.
- **14 Nov handover follow-up** — verify comment IDs, BeginTx lock comments, `closeRelVersionInternal` endpoint hashes, three CONTRIBUTING lesson gaps. Open until each is reproduced or cleared. **rho/sigma:** undetermined.
- **R4WAIT** (known red) — a second `WaitApplied` on a failed async token returns nil, since the first read prunes the failure (`ingest.go:399`). Waits for an async-session consumer (ai-soc uses sync). **rho/sigma:** none.
- **CORESPLIT** — core test package split: 21 s compile path of a core edit (`make dev` 28 s). Deferred (the coordinating session 18 Nov), revisit if core edits dominate. **rho/sigma:** none.
- **APPENDLIST** (ai-soc, 10 Nov; question) — an append-only list per node with a header, since a list append via nodes.Update rewrites the list twice. Needs ai-soc's spec and the maintainer's word (5-8 days, a minor). **rho/sigma:** write CPU and bytes per list append.
- **COLDLRU** (ai-soc, 9-10 Nov) — one advance reads every cold day shard, so the cap grows with the archive unless sigma's read-back stops reaching old days (sigma's COLDSET); a larger cap is no correction. Waits for sigma's COLDSET. **rho/sigma:** shard opens and RAM per advance.
- **COLDHEAP** (ai-soc) — +970 MB over 3 days from what is never cold (reference shard, hot/warm, demoted open handles, degree tracker) and per-shard entity caches (224 MB of decoded rows). Waits for ai-soc's retainer view. **rho/sigma:** rho's RAM over days that no tier can free.
- **LISTREAD** (sigma 31 Oct) — label-and-property pages read through one forward scan since v4.105.4; left: a batch `HasHistory(ids)` through one scan. Waits for sigma's restore timing on v4.105.4 (only on the maintainer's word). **rho/sigma:** restart time.
- **ASOFSEEKCOST** (SCALE/WRITERATE; the coordinating session 19 Nov) — the disk transaction-time seek index  is built on every badger history append, so on ai-soc's path: 52 B per indexed history row and 51 B per entity proof; in-memory sample: append 2.0→7.9 µs per node, 2.5→6.3 µs per rel (22→63 allocs).
  No work now; waits for a write-path profile of ai-soc's that shows the cost. **rho/sigma:** write CPU and disk per history row.

On consumer runs (ai-soc, sigma):
- **VALUETHRESH** (v4.104.0) — sigma's 2-day budget profile with 32 KiB on tiered should lose the compaction CPU of its 64 KiB DerivedPage blocks (+900 of +2,640 s) at acceptable storage growth. Waits for ai-soc's run (the maintainer says "run"). **rho/sigma:** direct: ~900 of 2,640 extra CPU s; the price is an uncompressed value log.
- **STATEREC** (sigma's design review 30 Oct) — after WHOLEROW (v4.100.0): the TxPin walk by IDs with a cross-shard merge (ASOFWALK later steps), and DEPTHLOOK; also REPORT. Waits for sigma wiring WHOLEROW after RAMPAGE. **rho/sigma:** direct: sigma's state restore and as-of cuts stay flat with churn.
- **CLEANOPEN** (ai-soc 27 Oct) — a clean reopen of a production-sized store spends 11-31 s in tiered.New, mostly the reference shard, and grows between opens; sigma's RESTART asks open time flat as the store grows. Waits for ai-soc's RESTART item and the maintainer's word before rho reads profiles on the test host. **rho/sigma:** direct for restart.
- **TIERCLOCK** (v4.98.0) — ai-soc's replay should rotate shards as live data would (pin: Clock = newest committed valid time). Waits for that run. **rho/sigma:** none (ai-soc's replay).
- **WRITEAMP** (ai-soc 3 Nov) — measure compaction input bytes over flushed bytes per shard for the cut-shaped write, then set BaseLevelSize, BaseTableSize and MemTableSize per tier. Waits for a quiet machine and ai-soc's tiered run. **rho/sigma:** possible: per-tier settings for sigma's tiered store; unmeasured.
- **PURGEIDS** (committed 5ea08266) — `Nodes().PurgeByIDsCtx`: purge by ID, metered before any open; PURGERESIDUE fixed for every purge; a by-ID purge copies no label list (PURGECURSOR stays for the label purge). Done when Sigma's PAGEGC calls it on ai-soc's path. Waits for Sigma's PAGEGC. **rho/sigma:** RAM and disk freed when Sigma's PAGEGC purges retired pages by ID.
- **PURGEIDS-XSHARD** (review 19 Nov) — tiered `PurgeByIDsCtx` cleans an edge whose other end lies on another shard after the purge: that open is uncharged, and a stop between the shards leaves the edge's half for good (a re-run skips the purged node). Sigma's pages carry no edge. Waits for a caller with such edges. **rho/sigma:** none now.

On the maintainer:
- **CONTRACT** (5 Nov analysis, 5a) — run ai-soc's tests against the rho commit before rho's tag. Waits for the maintainer (default: skip; ai-soc's own gate catches such a break before it pins). **rho/sigma:** none (release process).
- **RACEAFTER** (5 Nov: "it creates load on the machine") — the release starts `make race` detached after each patch push. Waits for the maintainer (default: detached after release only); options: drop it, `nice` it, cap at `-p 2`. **rho/sigma:** none (shared machine load).
- **ASOFVALID** (ai-soc 5 Nov) — a `valid=` page costs ~8 s on the anonymised copy (v4.98.4), 76 % one point row read per member of a cold shard. Waits for the maintainer: (b) a cold checkout loads the rel-type envelope (~2 days) or (a) FIXED-MEMORY step 2; default (b). **rho/sigma:** none direct (ai-soc's /tkg/asof door).
- **CHAIN** — which layer (ai-soc, sigma, rho) wastes how much of the 3-4x storage and memory; memory split known (26 Oct), storage split open. Waits for the maintainer's word and a free test host (default: none before the numbers). **rho/sigma:** says where the 3-4x goes; the storage split is the missing half.
- **STORE** — ai-soc's store layout (delta encoding per label, Derived/Session placement, instance nodes ~2/3 of the production-sized store); only one profile's settings are agreed. Waits for the maintainer with ai-soc and sigma (STORECFG). **rho/sigma:** some: decides which tier sigma's pages live on.
- **PREEPOCH** (ai-soc 29 Oct, product question) — a customer backfill with valid times before the 2026-01-01 snowflake epoch is refused (ErrValidFromOutsideIDRange, ADR-0013). Waits for the maintainer (default: stays refused). **rho/sigma:** none.
- **BRANCHES** (3 Nov: not now) — seven archived branches, all contained in `origin/archive/unmerged-2026-10-27` (93c43a64), to delete. Waits for the maintainer to run `git push origin --delete`. **rho/sigma:** none (housekeeping).
- **ORIGIN** — every release made on origin is merged and checked against rho's rules. Waits for the maintainer's releases. **rho/sigma:** keeps one rho line, so sigma never pins a tag that lacks the other's fixes.
- **CIGATES** — CodeQL runs from `codeql.yml` only. Done when the default setup is off (org setting; the API returned HTTP 422) and `codeql.yml`'s first run is green. **rho/sigma:** none.
