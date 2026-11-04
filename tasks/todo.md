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

- **RELHIST** (sigma's profile: getRelHistory 32 of 160 GB; `tasks/evidence/RELHIST/design.txt`) — reads and writes
  stop rebuilding a key's whole history. Released: P3 HISTDECODE, HEADLEND, P1 HISTTRIM, CASCADEMETA (v4.103.0), P2
  (v4.104.1: the plural door reads and fills the head entry; `p2.txt`). Next, P5 (design.txt § P5, checked): bound
  NodeHistoryTemporalMeta (10.9 GB). Phase 0 shipped (v4.105.1): on sigma's shape the misses are pins below the
  entry's maxTx (plural door) and no entry; deletes, retractions, stale tokens and node-at doors count zero. Option C
  shipped (v4.105.2): pin-below misses 516 -> 52 on sigma's shape, skeletons read 8,413 -> 1,145. Left: the 52 pins
  below the lowest generation (C2: a fill at a lower pin records a generation) and first-read "no entry" misses;
  Phase 1 (deletes and retractions) only if a store shows them. Also open: the caller-instant pre-flight and cascadeMissing*Err read whole histories; the skeleton read
  itself is O(versions) (option F). Caveat (design.txt §4): compaction between tx ops can trim a row above the current
  version that rollback puts back. Done when sigma's next profile shows history reads below 10 % of alloc_space.
  sigma's data answers: on hold (sigma). **rho/sigma:** the largest open item: P5 targets NodeHistoryTemporalMeta,
  10.9 of sigma's 160 GB alloc_space; P1 and CASCADEMETA (v4.103.0) already took the 32 GB getRelHistory share; the
  rest shows in sigma's next profile.
- **MEMGATE** (René 4 Nov: "make a better option") — no suite test gates pass/fail on process-wide `runtime.MemStats`
  or wall time: each such gate becomes a deterministic count (rows read, keys visited, store calls, copies) with the
  bytes kept as a log, or moves to the measurement level (`//go:build measure`, `make measure`). 42 test files read
  MemStats (about 16 compare bytes against a bound). Done so far: `TestCascadeMetaCost` (counts),
  `TestExistMapResidentBytesPerNode` and `TestForEachNodeEffectiveByLabelAndPropertiesColdBytes` (measure level).
  Done when none is left. **rho/sigma:** indirect: no flaky red gate holds back a release sigma waits for; count gates
  such as `TestCascadeMetaCost` pin sigma's read costs in rho's suite.

## Next

- **LISTREAD** (sigma 31 Oct; no rush, sigma goes on without it; changes no answer) — sigma's restore
  (ByLabelAndProperties pages plus HasHistory per node; ai-soc's store: index 8.1 s, HasHistory 6.4 s of 23.3 s on
  1,048,294 nodes) gets cheaper. sigma reads at DepthHot on the reference shard, so cross-shard paging and the cold
  probe do not explain its times. Measure first on a store reopened cold: uncached reference-shard rows, the stamps
  re-read by tiered HasNodeHistory (tieredstore_history_presence.go:65), the archive ask, the one-time presence build
  on the first HasHistory (a whole-keyspace scan). Then: a batch HasHistory(ids) (a minor); cross-shard ID paging
  (patch) only if the measurement shows it; a no-copy read-only page only if both leave it worth a door. Done when
  measured and fixed or dropped. **rho/sigma:** restart time: the two phases are 14.5 of 23.3 s per 1 M nodes; the
  saving is unmeasured.
- **Round 2 read items** (ai-soc's read-back inventory, 3 Nov, 60-min BA on tiered; estimates, measure each first):
  UNIQUEMISS (GetOrCreateByKey looks the key up, then the unique pass again on create, core/unique_constraints.go:628;
  claimForever re-marshals the whole owner map per claim, unique_forever.go:120-132); OWNERDROP (tiered
  DropUniqueOwnerIndex's DropPrefix, owner_index.go:198, blocks writes, so a racing Add can get ErrBlockedWrites);
  EDGEROW (a rel create reads each endpoint row 3-4 times, est. <= 5 s of 14.4 s); CASCADEROW (the cascade reads the
  current node 2-3 times; cascadeNode 5 s, 27 s on d2; est. 2 s); FLOORDECODE (batchCommitFloor decodes the tx stamps
  of every value, DecodeWireTxStamps); DBVIEW (DB.View +41 s in query doors, readUnderRLock, not Get/Lend: measure
  readTxnsTotal per door on one BA run, ask sigma which door; `tasks/evidence/ROUND2/r3-design.txt` item 3). Done when
  each is measured and fixed or dropped. ai-soc measures them all in one combined run on main's tip (René 3 Nov: no
  per-item benches; bisect only on a regression). **rho/sigma:** CASCADEROW, FLOORDECODE and possibly DBVIEW run
  under sigma's writes and queries; UNIQUEMISS, OWNERDROP and EDGEROW are ai-soc's write path.
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
- **VDICTDISK step 3** — a budget/LRU in place of resident value tokens (a 'row' dictionary ~136 MB per open day
  shard at Cap 1 M). Done when an entry's resident cost is measured and bounded. Waits for ai-soc's next run.
  **rho/sigma:** unverified: applies only if sigma's stores set ValueDictionaries (HANDOVER lists ai-soc's).
- **TIERCLOCK** (v4.98.0) — done when ai-soc's BA replay rotates shards as live data would (pin: Clock = newest
  committed valid time). Waits for that run. **rho/sigma:** none (ai-soc's replay).
- **PERTIER, RAM doors** (each a minor) — per-tier MemTableSize (64 to 128 MB on BO: push -2.9 %, GC mark -20.4 s,
  heap +170 MB); a listed label keeps Elements definitions in RAM and moves scalar ones to disk; Elements rows on
  disk; COMPOSITEDISK; PROPHOT (only if Derived's index goes to disk). Waits for ai-soc's ask with numbers.
  **rho/sigma:** none unless sigma asks.
- **WRITEAMP** (ai-soc 3 Nov) — measure compaction input bytes over flushed bytes per shard for the cut-shaped
  write, then set BaseLevelSize, BaseTableSize and MemTableSize per tier (with PERTIER). Waits for a quiet PC and
  ai-soc's tiered d2. **rho/sigma:** possible: the per-tier settings would apply to sigma's tiered store; unmeasured.
- **PUSHSLOW, RELPAGEVALID leftovers** (ai-soc 29 Oct, 1 Nov) — the mask read is 14-17 µs per returned node, half of
  it the row read and decode (sigma filters on the whole row), a third tiered HasNodeHistory's stamps re-read; the
  valid-instant fetch fell 306-445 s -> ~8 s on the 12 GB masked store, but a page still costs ~8 s on cold shards (no
  envelope there; FIXED-MEMORY step 2; a larger read batch measured no gain, each tiered row read opens its own view).
  Reopen when ai-soc says either still matters. RHOBATCH: not to be built (sigma rejected the key-to-id map).
  **rho/sigma:** small: the stamps re-read is LISTREAD's; fixed there, it falls here too.
- **SHARD step 10, later slices** — label and property postings and the rel owner map on the fast volume (slice 1,
  UNIQIDX, shipped v4.81.0). Sizing data (ai-soc 25 Oct, k10251802, 3 data days): 1,038,092 keyed nodes (Process
  632,653 at 40.6 B a key, File 229,863 at 110.9 B, RegValue 74,934 at 125.2 B); ~1.9 M BA + 1.0 M BO edges;
  reference shard 828 MB, day shard 1.2 GB. Waits for a measured need.
  **rho/sigma:** none measured.

On René:
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

## Later

- **PURGEIDS** (sigma, for PAGEGC) — a hard purge by id. Waits for sigma's answers (nodes/rels, history, id reuse,
  batch size, stores). **rho/sigma:** direct once sigma garbage-collects pages: dead pages leave the store.
- **TIMEWINDOW** (a minor) — reads bounded by a mint-time range, for one-shot evaluation only (half-open mint range,
  hot shard always included); sigma's other candidate: a ValidEndAfter-style bound on ByTypeAndProperty. Done when
  ai-soc and sigma say whether one-shots matter (metric lead `aisoc_sigma_edge_shard_checkouts_total`).
  **rho/sigma:** only if sigma runs one-shot evaluations or asks for the ValidEndAfter bound.
- **DICTMERGE**, **PROPPOS** (after DICT) — merge per-shard dictionaries on the cold move; a positional property form
  (~7 B less per property, a new row format). Done when measured as a share of stored bytes on a real store.
  **rho/sigma:** storage only, by CHAIN's storage split; sigma's blob pages carry few properties, so little.
- **GETWALK** — point gets on hot read paths (about 45-52 `Txn.Get` sites in badger and tiered, ~4x the row each)
  become forward walks. Done when measured on the hot paths. **rho/sigma:** possible on sigma's as-of reads;
  unmeasured.
- **DEVREF** (ai-soc 23 Oct) — find a node's edges by its endpoint in one index read, so edges drop the `device`
  text property. Waits for ai-soc. **rho/sigma:** none.
- **COLDWRITE** — a test writes into a cold shard with DurableCommit, drops the store without Close and reads it back
  (a write can land in a cold shard; ai-soc's writes never do). **rho/sigma:** none (a test).
- **METASYNCRIG** — a power-loss rig (LazyFS or dm-flakey) shows METASYNC's loss without `SyncWrites` and the guard.
  **rho/sigma:** none (proof of an existing guard).
- **COLDSTART** — a fresh agent works from the docs alone; every gap it finds is fixed in its owner file.
  **rho/sigma:** none (rho's docs).
- **GUARDS** — each rule the docs no longer state has a test that fails on a planted violation. **rho/sigma:** none.

## Parked

Each reopens on a consumer request, a measurement, or René:
- **R37** — a cold listing below 4x a fact's values needs a column door with sigma's segment-based joined(); v4.83.0
  cut rho's part (warm 8.5x -> 4.0x, cold 14.9x -> 10.5x). sigma: not now.
- **TOMBSTONE** — a delete keeps the believed end of validity (backlog 4, 7, 25); branch `tombstone-writer-v4.63`
  (37a1ff56, archived), ~½ day. No consumer needs it (sigma excises validity).
- **RELDISK phase 2** — a composite rel index and Rels().ByTypeAndProperties; held by ai-soc and sigma (nothing reads
  a multi-key rel probe).
- **PAGEBLOB** — a block store on the tiers and batched index inserts, only if sigma's blocks outgrow one byte
  property (100,000 B default). Separately: current-only Elements (a minor; no member entries for an append-only
  label, half the block write, `tasks/evidence/ELEMDISK/block-cost.txt`; needs a defined answer for every pinned door;
  sigma asks with PAGEBLOB's BA numbers); relationship types on disk refuse Elements (build on an ask); WRITEGROW's
  open question: why sigma's `wholeWrites` mode ended at 299 segments.
- **CREATEPEAK** — on-disk index creates and drops collect every entry before writing (~120 B a posting); rare.
- **SUITE** — fold the near-copy memory and badger tests into the shared store suite (René stopped it 4 Nov; branch
  `consolidate`).
- **DURABLEVIS** (René 3 Nov) — a reader can see a DurableCommit group before its fsync; holding readers measured too
  costly; alternative: report the last durable LSN, an LSN lease, idempotent replay by the consumer
  (`tasks/evidence/DURABLEVIS/`).
- **CACHE** — per-shard memtable, block cache and compactors (88 MB a writable shard); under 1 GB to save today.
  Also holds BADGER step 3's leftover (per-tier caches and compactors).
- **PENDING** — reading one entity walks badger's whole write buffer (half built, `row102-pending-ranged`, archived).
- **RELSIDECAR** — multi-property rel lookups may be slow on BA; waits for a measurement.
- **NITS** — small findings no consumer has hit (backlog 80-82, review findings).
- **STALECITE** — comments citing removed todo sections; fixed with the next change to that code.
- **V5** — paused on `v5-takeover` (3bd4d13a, archived; HANDOVER); waits for the consumers' use of it.
- **BACKLOG** — 26 numbered items without a requester (HANDOVER), cited by code.
- **BADGERPANIC** — badger parses a damaged block trailer before its checksum and panics in its own goroutines
  (table.go:575-604; `tasks/evidence/VERIFY/trailer_test.go.txt`); René 24 Oct: no fork; rho recovers what it can.

## Closed 4 Nov (René: "are they still necessary")

- **ASOFREAD**: covered by RELHIST (P2 read the head entry in the plural door; P5 bounds the rest).
- **REOPEN**: ai-soc's own reopen proof; no rho work waited on it.
- **PROPSTR**: all three fixes shipped (v4.88.1, PROPSEEK v4.89.1, PROPREWRITE v4.89.2); reopen if a BO run stalls.
- **OPENIMAGE**: steps 1-2 shipped (v4.88.0, v4.89.0); reopen if a crash reopen is not bounded by recent writes.
- **SCALEPROFILE**: superseded by ai-soc's round runs on main's tip and FIXED-MEMORY.
- **FOLDS**: today's documented behaviour kept; no consumer reported a problem.
- **REVIEWS**: carried by NITS (same rule).
- **WRITTENAT**: withdrawn by ai-soc 26 Oct (sigma's PUBIDS).
- **AISOC**: a rule, not an item: each ai-soc request becomes its own item (CONTRIBUTING line 22).
- **UNIQIDX adoption**: ai-soc's own data work; rho's part shipped in v4.81.0.
- **ITERPREFIX**: measured 25 Oct, no change; reopen on an L0 pile-up.
- **ROUND2 measure**: folded into Round 2 read items (one combined ai-soc run).
- **TEMPIDX-2, IDTABLE**: folded into FIXED-MEMORY.
- **ASOFWALK later steps, REPORT**: folded into STATEREC.
- **CHAIN-2**: folded into CHAIN.
- **PAGEBLOB follow-ups, WRITEGROW**: folded into PAGEBLOB (parked); WRITEGROW shipped in v4.99.0.
- **BADGER step 3**: steps 1-2 done 24 Oct; shipped COLDRO (v4.78.3), PREFETCH (v4.78.4); measured, no change:
  ITERPREFIX, VLOGGC, CompactL0OnClose, the mover's copy. Left: GETWALK (Later), per-tier caches (CACHE, parked).
