# todo

What rho does next, in order. René's open decisions are the items that wait for him, each with my default. State,
consumers and the numbered backlog: `HANDOVER.md`; rules: `docs/CONTRIBUTING.md` § Rules digest. Done work:
`docs/CHANGELOG.md`, under the same tag; measurements: `tasks/evidence/<ITEM>/`.

Each item: one sentence, then when it is done and what it waits for. **rho/sigma** says what the item brings to the
consolidated rho + sigma stack (sigma-tkgd running on rho): memory, CPU, storage or restart time in sigma's profile, or
"none" when it serves only ai-soc, the SCALE goal or rho's own process. Next is ordered by sigma's gain per unit of
work (re-analysed 4 Nov with a Fable review against code and evidence).

**The standing goal, SCALE** (René 26 Oct): rho holds realtime ingest at about 2 TB a day (about 23 MB/s sustained,
4 billion 500-byte facts a day) and grows only by more RAM, NVMe and block storage. Done when `docs/architecture.md`
lists what grows with total volume instead of per shard (RAM per entity; open, crash recovery, Clear and compaction
cost per store size; hot-shard write rate against peaks), each measured and bounded or queued here. Bounded so far:
existence maps (EXISTMAP v4.84.0, ~8 B per node, reopen ~0.25 s per 1 M), open walks (OPENWALK v4.87.0), crash reopen
(OPENIMAGE v4.88.0/v4.89.0). Open walls: the reference shard keeps every Derived fact and keyed node for ever; lazy
per-entity sidecars (`relValidIdx` 43-67 B per rel on plain badger, belief watermarks, tx membership, history
presence, idTable); hot-shard write rate (WRITERATE). The path for RAM: FIXED-MEMORY (Next). Evidence:
`tasks/evidence/SCALE/`.

## Now

- **READBUDGET, RELDISKWIN follow-ups** (v4.107.2) — refuse native iterator steps, buffers, sorts and historical
  membership before the work, in every shard and fallback; stale late-axis deletes stay visible until compaction.
  Done when the limits refuse first, ai-soc's membership guards pass and stale entries are measured on a consumer
  store (`tasks/evidence/READBUDGET/followups.txt`). **rho/sigma:** bounds native work.
- **WARMMEM (R3: cheaper writable and inactive shards)** — Finish measured sizing and ownership of writable arenas
  across hot, warm, reference, demoted and late-write handles; separately account for already-open demoted handles
  that a smaller warm-open setting does not shrink. Done when allocation, live heap, peak RSS and CPU improve on
  matched fixtures with late writes, WAL recovery, durability and copied reopen unchanged; no GC tuning or larger
  cache cap stands in for the saving. Waits for candidate review, then the host baseline. **rho/sigma:** lowers the
  fixed memory cost per live handle without multiplying reads or writes.
- **EMPTYBYID (keep proven-empty shards closed)** — Finish skipping NoRows shards in ID, history, adjacency and
  create-existence probes, including batched paths. Done when repeated idle sweeps and misses cause no empty-shard
  opens, cross-shard relationships remain findable, and a real late write safely reopens its shard; measure handle
  and allocation counts. Waits for candidate review. **rho/sigma:** removes repeated open/close work after demotion.

- **RELHIST** (sigma's profile: getRelHistory 32 of 160 GB; `tasks/evidence/RELHIST/design.txt`) — reads and writes
  stop rebuilding a key's whole history. Released: P3 HISTDECODE, HEADLEND, P1 HISTTRIM, CASCADEMETA (v4.103.0), P2
  (v4.104.1), P5 phase 0 (v4.105.1) and option C (v4.105.2: pin-below misses 516 -> 52, skeletons 8,413 -> 1,145).
  Left: C2 (a fill at a lower pin records a generation), first-read "no entry" misses, the caller-instant pre-flight
  and cascadeMissing*Err whole-history reads, option F (the skeleton read is O(versions)); caveat design.txt §4.
  ai-soc 9 Nov (sigma v0.17.14-16 + v4.105.9): history frames 0 samples, the page door 40 % of a view build,
  NodeHistoryTemporalMeta 1.7 %. Waits for ai-soc's cold-build arm (reopen cycles). Done when the next profile of
  ai-soc's workload shows history reads below 10 % of alloc_space. **rho/sigma:** P5 targets NodeHistoryTemporalMeta,
  10.9 of sigma's 160 GB alloc_space.

## Next

- **Round 2 read items** (ai-soc's read-back inventory, 3 Nov, 60-min BA on tiered) — UNIQUEMISS, OWNERDROP,
  EDGEROW, CASCADEROW, FLOORDECODE, DBVIEW; code and estimates: `tasks/evidence/ROUND2/read-items.txt`. Done when
  each is measured and fixed or dropped. ai-soc measures all in one run on main's tip (René 3 Nov: no per-item
  benches; bisect only on a regression), not before ai-soc v5.6.0; a Flux run needs René's word. **rho/sigma:**
  CASCADEROW, FLOORDECODE, maybe DBVIEW under sigma's load; UNIQUEMISS, OWNERDROP, EDGEROW are ai-soc's write path.
- **FIXED-MEMORY** (René 31 Oct; the RAM path for SCALE; badger stays) — per-entity answers leave the Go heap: heap
  fixed, data on NVMe and block, RAM is page cache. Step 0 measured (3 Nov, `tasks/evidence/FIXED-MEMORY/step0.txt`:
  sorted run files pass every bar, hash files refuted; cold reads through a bounded pread pool). Path (a), run files
  under `DataDir/idx` with a bounded delta in heap (~64 MB), badger the truth (a missing or torn file is rebuilt at
  open, a store without files keeps working; own readers use SetPanicOnFault), each step a minor with a red test:
  1 existence on the reference shard and archive (~2-3 B per ID; refuted if ai-soc's push loses over 5 %); 2 rel
  record with envelope (52 B), which also bounds RELPAGEVALID's cold pages (refuted if ai-soc's RelsEffectivePage p99
  rises over 20 %); 3 entity record (36 B) for history presence, stamps, belief watermark (refuted if sigma's restore
  pass passes 1.5x today); 4 hot shard sealed by bytes, heap flat through a 10x write replay; 5 image ID sections
  retired. Path (b), later (René 3 Nov, after (a) is measured on real data): sealed shards convert to mapped segments
  (points 6-10 in step1-design.txt). Folds in TEMPIDX-2 (node TemporalIndex ~120 B a node, 554 MB; existence sets 388
  of 389 MB of sortedChunks.add; candidate: the rel-type TemporalIndex to byID only, ~120 -> 50-60 B a rel, patch or
  minor by the open image's use) and IDTABLE (badger idTable 41 -> 160 MB, lazily built, no cap; a closing shard drops
  it; bounded per valid day once TIERCLOCK rotates). Next: step 1 on a branch (design `step1-design.txt`), after
  ai-soc's probe locality on the write path. Done when a test holds reopen heap of 10 N entities within 0.1 B per
  entity of N and a long ai-soc run shows RAM flat with data. **rho/sigma:** small today: rho is 3.3 of sigma's 30 GB
  (CHAIN, 26 Oct), but sigma measured live heap +570 MB/day, rho about +300 of it (k10302100); this keeps rho's share
  flat as data grows, and step 3 speeds LISTREAD's HasHistory.
- **WRITERATE** (SCALE; `tasks/evidence/WRITERATE/results.txt`) — a hot shard's ingest is capped by one serial
  flush path (graph write lock, flush mutex, badger's single writer): async 57-72 k facts/s whatever the writer count,
  durable 16-38 k/s with one writer and 49-91 k/s with eight, against the goal's 46 k/s sustained; separate stores
  scale near linearly (4 stores 228 k/s). Levers: ASYNCSTAGE shipped (v4.86.1, async 1.55-2.0x); GROUPFSYNC overlaps
  the next batch's apply with the fsync, only for what no reader sees (René 26 Oct: readers never see a fact before
  its fsync, durability follows commit order); LANES, several hot write lanes, an ADR first. ai-soc runs durable with
  3-5 writers, BA peak 2,789 rows/s (at most ~52 k writes/s by WRITEPER: 18.7 writes per input row). No measured wall
  today. Done when one graph ingests 500 B facts durably at ~140 k/s (3x the goal) here. **rho/sigma:** headroom
  only: sigma's tkgd defaults to async, so ASYNCSTAGE already serves it; 15.3 of the 18.7 writes per BA row are
  sigma's paged rows, so cutting those (WRITEWHY, sigma's side) does more than a rho lever.

## Waiting
- **APPENDLIST** (ai-soc V1r, 10 Nov; question) — append-only list per node with a header. A list append via
  nodes.Update rewrites the list twice (wire_history_delta.go:115). Possible: write-once entry keys (node, list, tx,
  seq) plus a header record per tx; as-of = last header ≤ tx; O(1), ~100-150 B per append. Cost is integrations
  (tiered, purge, export, integrity, rollback): 5-8 days, a minor. Needs ai-soc's spec and René's word.
- **COLDLRU** (ai-soc RHOCOLD a, 9-10 Nov) — one advance reads every cold day shard (cap 2/3 thrash, cap 64 badger
  heap +640 MB): the cap grows with the archive unless sigma's read-back stops reaching old days (sigma's COLDSET).
  A larger cap is no correction; READBUDGET bounds the remaining opens, EMPTYBYID the empty-shard reopens.
- **COLDHEAP** (ai-soc RHOCOLD b) — +970 MB over 3 days; never cold: reference shard, hot/warm, demoted open handles,
  degree tracker. Decoded rows (224 MB) sit in per-shard entity caches, 10,000 entries each (CacheBudgetBytes bounds
  bytes). Waits for ai-soc's retainer view.
- **LISTREAD** (sigma 31 Oct) — label-and-property pages read through one forward scan since v4.105.4
  (`tasks/evidence/LISTREAD/measure.txt`). Left: a batch `HasHistory(ids)` through one scan (72 % of HasHistory's ~3 µs
  a node). Waits for sigma's restore timing on v4.105.4 (only on René's word). **rho/sigma:** restart time.

On consumer runs (ai-soc, sigma):
- **VALUETHRESH** (v4.104.0) — done when sigma's 2-day budget profile with 32 KiB on tiered shows the compaction CPU
  of its 64 KiB DerivedPage blocks gone (+900 s of +2,640 s) and its storage growth is acceptable. Waits for ai-soc
  (sigma 4 Nov: the store config and the run are ai-soc's). ai-soc 4 Nov: a small A/B on the 60-min BA lines first
  (page budget 256 MiB, threshold 32768 against 0); the 3-day run carries the A/B once René says "run", plus a
  NOCOMPRESS arm if that minor is tagged before the start.
  **rho/sigma:** direct: ~900 of 2,640 extra CPU s; the price is an uncompressed value log (2.7x the bytes on
  compressible text) whose GC reclaims nothing on the replace shape (measure.txt § Findings).
- **STATEREC** (sigma's design review 30 Oct) — WHOLEROW shipped (v4.100.0; done when sigma's writer runs on it; left:
  the reopen cycle on masked rows once sigma wires the door; no batch, ingest or `WithTx` twin, no rel mirror); then
  the TxPin walk by IDs and a cross-shard merge (ASOFWALK later steps; TxAt stays the skeleton walk with an O(1)
  belief watermark; done when sigma's 50+ cuts of worst churn bench flat), and DEPTHLOOK (a TxPin property lookup
  below DepthAll takes the label-wide fold). Also carries REPORT: ai-soc and sigma report on WHOLEROW and ASOFWALK;
  rho builds only what blocks them (ai-soc 3 Nov, rho v4.100.0 + sigma v0.11.159: BLOCKDOORS 1,699 -> 1,471 ms a cut
  paging on, 694 -> 672 off). Waits for sigma wiring WHOLEROW after RAMPAGE. **rho/sigma:** direct: sigma's state
  restore and as-of cuts stay flat with churn instead of growing per cut.
- **CLEANOPEN** (ai-soc 27 Oct) — a clean reopen of a BA store spends 11-31 s in tiered.New, mostly the reference
  shard, and grows between opens of the same copy (k10271440, 4.11 GB: 16.7 -> 30.8 s; k10270301 11.4 -> 21.9 s;
  k10270846 17.5 -> 26.0 s); 7-10 % of the time to serve, the rest sigma's restore and backlog. Profiles on Flux
  (fast-archive-20261027, runs/reopen-ba-*). sigma's RESTART (27 Oct) asks next: open time flat as the store grows,
  and an as-of-tx page door for a crash restart (its stamp question is answered: unique, strictly increasing across
  reopen and crash, with the listed exceptions). Waits for ai-soc's RESTART item (after goal 2) and René's word before
  rho reads profiles on Flux. **rho/sigma:** direct for restart: sigma reopens the same tiered stores and its two
  asks are open here.
- **TIERCLOCK** (v4.98.0) — done when ai-soc's BA replay rotates shards as live data would (pin: Clock = newest
  committed valid time). Waits for that run. **rho/sigma:** none (ai-soc's replay).
- **WRITEAMP** (ai-soc 3 Nov) — measure compaction input bytes over flushed bytes per shard for the cut-shaped
  write, then set BaseLevelSize, BaseTableSize and MemTableSize per tier (with PERTIER). Waits for a quiet PC and
  ai-soc's tiered d2. **rho/sigma:** possible: the per-tier settings would apply to sigma's tiered store; unmeasured.

On René:
- **CONTRACT** (5 Nov analysis, 5a) — run ai-soc's tests against the rho commit before rho's tag. sigma's guard set
  (7 tests via `-modfile`) is ~15 s warm but would not have caught REFFANOUT's break (ai-soc's
  `TestTieredDerivedReadOpensNoColdShard`, only in its full short suite, 5-8 min at load 30-50). Default: skip; ai-soc's
  own gate catches such a break before it pins. **rho/sigma:** none (release process).
- **RACEAFTER** (René 5 Nov: "it creates load on the PC") — the release starts `scripts/race.sh` detached after
  each patch push, and cron runs it every 6 h. Options: cron only; `nice` it; cap it at `-p 2`. Default: unchanged
  until René says. **rho/sigma:** none (shared PC load).
- **ASOFVALID** (ai-soc 5 Nov) — a `valid=` page costs ~8 s on the masked copy (v4.98.4), 76 % one point row read
  per member of a cold shard, which carries no relationship temporal index (shard_index_fanout.go:240-244, :486-490).
  Either (b) a cold checkout loads the rel-type envelope (byID, ~50-60 B a rel, bounded by MaxOpenColdShards; ~2
  days) or (a) FIXED-MEMORY step 2's rel record (zero heap, ~3 days after step 1). Default: (b) first, the only
  wall-time item rho can act on alone. Red test: a valid page of limit L over a cold shard reads at most L+k rows.
  **rho/sigma:** none direct (ai-soc's /tkg/asof door).
- **CHAIN** — which layer (ai-soc, sigma, rho) wastes how much of the 3-4x storage and memory; memory split known
  (26 Oct, k10260011: heap 30 GB, sigma 89 %, rho 3.3 GB, badger 0.77), storage split open. Then each layer decides
  its share; rho's options: believed row stays current, excise without a row copy, delta anchor decoded once per read,
  decoded rows nearer stored size (default: none before the numbers). Waits for René's word and a free Flux.
  **rho/sigma:** the consolidation itself: it says where the 3-4x goes, so sigma and rho stop guessing which layer
  to cut; the storage split is the missing half.
- **STORE** — ai-soc's store layout (delta encoding per label, Derived/Session placement, instance nodes ~2/3 of the
  BA store); agreed so far only p4b's settings (PropertyIndexOnDisk, a bounded entity cache, DerivedPage as a
  reference label on ai-soc's tiered stores). Waits for René with ai-soc and sigma (STORECFG). **rho/sigma:** some:
  Derived placement is sigma's data; decides which tier sigma's pages live on.
- **PREEPOCH** (ai-soc 29 Oct, product question) — a customer backfill with valid times before the 2026-01-01
  snowflake epoch is refused (ErrValidFromOutsideIDRange, ADR-0013). Done when René decides. Default: stays refused.
  **rho/sigma:** none.
- **PURGECURSOR, PURGERESIDUE** (patches) — a label purge copies the label's whole ID list per 256-node call
  (quadratic at 10 M); a purge leaves ~30-50 B per ID in an open shard. No consumer purges today. Default: leave.
  **rho/sigma:** only when sigma's PAGEGC purges (see PURGEIDS).
- **BRANCHES** (René 3 Nov: not now) — seven archived branches (five `codex/*`, `v5`, `round10`), all contained in
  `origin/archive/unmerged-2026-10-27` (93c43a64), to delete; René runs `git push origin --delete` (my run was
  refused). **rho/sigma:** none (housekeeping).

On Markus:
- **ORIGIN** — every release Markus makes on origin is merged and checked against rho's rules. **rho/sigma:** keeps
  one rho line, so sigma never pins a tag that lacks the other's fixes.
- **CIGATES** — CodeQL runs from `codeql.yml` only. Done when the default setup is off (org setting, René got HTTP
  422) and `codeql.yml`'s first run is green:
  `gh api -X PATCH repos/{owner}/{repo}/code-scanning/default-setup -f state=not-configured`. **rho/sigma:** none.
