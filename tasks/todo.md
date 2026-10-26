# todo

What rho does next, in order. Last release: v4.84.1 (2026-10-26). René's open decisions are the items that wait for
him, each with my default. State, consumers and the numbered backlog: `HANDOVER.md`; rules: `docs/CONTRIBUTING.md`
§ Rules digest. Done work: `docs/CHANGELOG.md`, under the same tag.

Each item: one sentence, then when it is done and what it waits for.

## Now

Order (René 24 Oct: fixes first, then BADGER and SHARD; open decisions settled from the data and the consumer agents).
Released 24-26 Oct: v4.75.0 to v4.84.1 (`docs/CHANGELOG.md`). Now: SCALE, then BADGER step 3's last pieces (R37,
rho's lever in ai-soc's order of 26 Oct, shipped in v4.83.0 and waits for ai-soc's re-measure). Ranked by the goal in the rules digest
(realtime at about 2 TB a day): a wall to it ranks above a constant-factor win.

- **SCALE** (René 26 Oct, the standing goal) — rho holds realtime ingest at about 2 TB a day (about 23 MB/s
  sustained, peaks higher; about 4 billion 500-byte facts a day) and grows only by more RAM, NVMe and block storage.
  Done when: architecture.md lists what grows with total volume instead of per shard (RAM structures that grow with
  entity count; open, crash recovery, Clear and compaction cost that grow with store size; hot-shard write
  throughput against peaks; sub-day shard windows), each measured and either bounded or queued here as its own item
  ranked by how soon it walls 2 TB a day. Measured so far (`tasks/evidence/SCALE/existence_maps.txt`, probe beside
  it, idle runs at the end): every open shard keeps ~63 B per node in RAM (`nodeIDs` 30 B, `nodeRevs` 32.5 B; rels
  the same pair), and a clean open walks every node key to fill them (~1.5-1.9 s per 1 M, linear): a 4-billion-fact
  day-shard needs ~250 GB of heap and ~2 h to reopen, and the always-open reference shard grows for ever. A
  composite index adds ~8 B per node and at most ~0.6 s per 2 M at open. Durable writes (SyncWrites +
  DurableCommit, 500 B facts, one writer, batches of 1000): 64 k/s against the goal's 46 k/s sustained. Items, in
  order: EXISTMAP (a compact ID set saved in the open image and a bounded striped revision table instead of the two
  maps: RAM and open time no longer grow per entity), then WRITERATE (peak headroom, several writers), then
  COMPOSITEDISK (postings out of RAM; small today). EXISTMAP shipped in v4.84.0: ~8 B per node in an open shard,
  clean reopen ~0.25 s per 1 M (`tasks/evidence/EXISTMAP/results.txt`); a read that misses a listed row is now
  `ErrCorruptWire`. Next walls found on the way: the rel-type index `typeIdx` stays in RAM per rel (~23.6 B per
  rel, a map per type, no on-disk mode; ai-soc: ~71 MB at its 3 M rels, below sigma's items, no request yet), the
  lazily built `relValidIdx` costs 43-67 B per rel, and a crash reopen still decodes every row (OPENIMAGE). ai-soc's walls (26 Oct, k10260011, 3 BA days): sigma's resident state (25.4 GB
  census, growing with data; sigma's PROBEMASK, PAGESWITCH, JOINFIT page it); the reference shard keeps every
  Derived fact and every keyed node for ever (Process, File, RegValue, LocalObject: 93 % of 1.04 M keys in 3 days;
  UNIQIDX adoption with SHARD step 10 moves the keyed nodes to event shards, Derived stays; a purge for result rows
  is STORECFG, René later); restart re-reads all past results (625 s at 3 days, mostly sigma's; rho's share from
  OpenStats of k10260915). So EXISTMAP and COMPOSITEDISK bite first on the reference shard.
- **ROWPROBE** (sigma, 26 Oct: ai-soc's PageRows spends 10.7 of 70 s in sigma's paged-row probes; batching them
  through `ByLabelAndPropertyValues` gains nothing on v4.84.1) — three rho costs, confirmed in the code: badger with
  `PropertyIndexOnDisk` looks each value up on its own (a pending-overlay pass each) and point-reads every matched
  row; `NodeHasAnyIndexKey` matches every node against every key (quadratic in a big value list: 1,000 values in one
  call 1.75 M allocs against 249 k unbatched); the pager reads one effective timeline per node (tiered: 2.81 of
  3.06 s). Plan: (a) patch — a key set built once per call, one disk pass over all values, matched rows through one
  scan snapshot; (b) minor — `ForEachNodeEffectiveByLabelAndPropertyValues` (the row-121 page door with one key's
  value list: candidates and segments at one pin through one snapshot), shape asked of sigma 26 Oct. Budget (lesson
  89): one call with 1,000 values costs per value at most 1.2x a call with 16, and sigma's all-in-one bench beats
  its unbatched 74 ms. Done when: both released with the budget met; sigma's branch (one pager call per join level)
  adopts them.
- **BADGER** (René 24 Oct, the next milestone) — rho fully understands badger, verifies its own badger use by tests,
  then optimises badger for the multi-tier storage. Done when: the three steps below are ticked.

### BADGER plan

1. [x] Full badger API understanding (2026-10-24): every claim in `docs/badger.md` cites badger's source or a measurement, and every
   claim rho relies on has a test in `tasks/evidence/BADGER/` that tries to break it. Done so far: the API reference
   (section 8), the batch API (B1-B9), the source check of sections 1-7 (one false claim fixed), the read side R1-R5
   (prefix pruning, compaction, read-only open, directory lock, MaxVersion; R3 corrected section 1: a read-only open
   refuses every unclean WAL, not only a torn one). Fable's review (24 Oct) reopened it: R3, R5 and B9 can pass with
   their claim false; now fixed, all 15 evidence tests can fail and pass. Re-checked in the source by the main
   session (Fable is not used for badger, René 24 Oct): MaxVersion falls when compaction drops a tombstone (R5: 4 to
   2); badger refuses a value above `ValueLogFileSize` or, in memory, at or above `ValueThreshold` (B10); a crash
   prefix holds per WAL file only under power loss without `SyncWrites`; the `Discard` rules; citations; section 8
   passed against the source (uncited rows are doc comments of features rho does not use).
2. [x] rho's current badger use, fully verified in tests (2026-10-24): the call map and the 9-category analysis exist
   (`tasks/evidence/USAGE/`); confirmed by real tests (24 Oct): CLEARMETA (a crash inside `Clear`), KEYSIZE and
   VALUESIZE (one oversized entry blocks all later persistence), VERIFY (silent corruption, panics, silent
   truncation) and BADGERLOG; ai-soc and sigma asked (24 Oct) which configurations they run (on-disk indexes, value-log
   size, in-memory stores, row and string sizes, a logger, Reset), to order the fixes; DISCARDCLOSE measured (the
   101st discard after Close blocks); sigma answered (VALUESIZE real,
   KEYSIZE latent, no logger, Reset reachable, no corruption seen); ai-soc answered (cut records up to ~5 MB in the
   value log, default value-log size, no on-disk property index, no logger, no Reset); each finding (CLEARMETA, BADGERLOG, DROPALL, METASYNC, the per-shard costs) gets a test
   that shows the harm or its absence; fixes come after the milestone (René 24 Oct). Also measured: DROPALL at rho
   level, a writable shard open 86.5 MB and 18 goroutines against 3.3 MB and 10 read-only. Not testable here:
   METASYNC (needs a power-loss filesystem; source-confirmed).
3. [ ] Badger optimised for the multi-tier storage (SHARD): per-tier options (read-only cold opens, caches, compactors,
   `CompactL0OnClose`), iterator prefixes and prefetch, point reads, the shard mover's copy method, measured per tier.
   Read-only cold opens released in v4.78.3 (COLDRO, `tasks/evidence/COLDRO/`: 1.3 ms and 3.3 MB against 27.6 ms
   and 94 MB per open).  PREFETCH released in v4.78.4 (`tasks/evidence/PREFETCH/`: the open
   without an image 7-18 % faster). ITERPREFIX, VLOGGC, `CompactL0OnClose` and the mover's copy measured, no change
   (`tasks/evidence/ITERPREFIX/`, `VLOGGC/`, `COMPACTL0/`, `MOVERCOPY/`). Left: GETWALK, and per-tier caches and
   compactors (worth less since COLDRO: a read-only cold open starts no compactor).

## Next

- **SHARD** — René wants three storage tiers on three volumes (hot on fast NVMe, warm on SSD, cold on block devices)
  built while ai-soc cleans up its data waste; the design is ADR-0012 in `docs/architecture.md`. Done when: the steps
  below are ticked and released. Steps 1-8 shipped (v4.76.1, v4.78.0; MOVEREAD v4.78.2, COLDRO v4.78.3). Step 10
  approved by René (26 Oct); slice 1 (UNIQIDX) in v4.81.0, the next slices on a measured need.

### SHARD plan

The main session builds and reviews each step (red test, change, tests green, self-review); a step is ticked when it
is released. The plan moves to `docs/CHANGELOG.md` when the last box is ticked.

1. [x] A shard whose directory is missing fails to open instead of opening empty; every shard path goes through one
   function. Released in v4.76.1 (`tasks/evidence/SHARD/`).
2. [x] The shard listing shows each shard's volume, path and bytes on disk. Done when: in the volume minor (red test
   `TestListShardsDiskBytesMatchesDirectory`). Released in v4.78.0 (`tasks/evidence/SHARD/step2.txt`): bytes on disk are allocated
   blocks, as `du` counts them, since an open shard's value log is a sparse 2 GB file.
3. [x] The config names a warm and a cold volume and where the reference shard and archive live; the catalog records
   each shard's volume and refuses an unknown one. Done when: in the volume minor (red tests
   `TestShardOpensFromItsVolume`, `TestUnmappedVolumeFailsNew`, `TestSentinelRejectedByOldValidator`). Released in v4.78.0 (`tasks/evidence/SHARD/step3.txt`): the sentinel is written whenever a shard is off hot (also a new store's
   reference shard or archive), and v4.77.2 refuses such a store and writes nothing.
4. [x] A closed shard moves to another volume crash-safely. Done when: in the volume minor (red tests: the crash
   matrix after copy, rename, catalog save and removal; a drop of a moved shard removes it there). Released in v4.78.0 (`tasks/evidence/SHARD/step4.txt`): `tiered.Store.MoveShard`.
5. [x] An open warm shard moves too: its readers and writers wait, nothing written is lost. Done when: in the volume
   minor (red test `TestMoveWarmShard_WriterWaitsAndWriteLandsOnNewVolume`). Released in v4.78.0
   (`tasks/evidence/SHARD/step5.txt`; found and fixed a reopen/temporal-DDL deadlock before it shipped, and in the
   pre-release review a nil store when Close stops a warm shard's move).
6. [x] The idle sweep moves one shard per tick to its tier's volume and never fails the store. Done when: in the
   volume minor (red tests `TestSweepMovesWarmToWarmDirAndColdToColdDir`, `TestMoverErrorIsNotStoreWide`). Released in
   v4.78.0 (`tasks/evidence/SHARD/step6.txt`): a step down only (a shard moved by hand to a slower volume
   stays), a failed shard retried after the others; `IdleTimeout` defaults to 5m when a volume is mapped;
   `g.Tier().MoveShard` is the door.
7. [x] The reference shard and the archive move offline. Done when: in the volume minor (red test: the reference
   shard on the warm volume still answers unique checks and the watermark).
   Released in v4.78.0 (`tasks/evidence/SHARD/step7.txt`): `tiered.MoveShardOffline`, under badger's directory
   lock on the reference shard (unix only).
8. [x] Each read reports how many cold shards it opened. Done when: in the volume minor (red test: a closed cold
   shard counts once, a retained one not).
   Released in v4.78.0 (`tasks/evidence/SHARD/step8.txt`): `ShardInfo.ColdOpens` per shard (a read's opens are
   the difference around it; per call needs a context the store doors do not take).
9. [x] The volume minor is released with its one-way door named in the CHANGELOG, and ai-soc and sigma are
   told. Done when: tagged.
10. [ ] The global index on the fast volume, unique keys first (keyed event labels), then label and property
    postings, then the relationship owner map. Done when: each slice released. Waits for: ai-soc's measurement after
    its data cleanup.
    ai-soc's counts (25 Oct, run k10251802, reduced BA set over 3 data days, one mint-day shard): 1,038,092 keyed
    nodes (Process 632,653 at 40.6 B average key, File 229,863 at 110.9 B, RegValue 74,934 at 125.2 B, 7 more labels
    under 32,000 each; cap 4,096 B); about 1.9 M BA edges plus 1.0 M BO; reference shard 828 MB, the day shard 1.2 GB;
    indexed rel properties `aisoc_record` (~22 B) on every edge, `device`, `remote_ip`, `src`/`action`; no on-disk
    property index; ErrEntryTooLarge never. René 26 Oct: yes, build it (unique keys first).
    Slice 1 (UNIQIDX), shaped by ai-soc (26 Oct): Process, File, RegValue and LocalObject (966,709 of 1,038,092 keyed
    nodes) become event labels; Device, Account, Address, DnsName, FileHash, AisocMaintText stay reference. Doors:
    `tx.GetOrCreateByKey`, `CreateUnique` (current scope), `ByLabelAndProperty(label, "key", v)`; a re-seen key must
    find its old row (24.8 % of rows need lineage older than 25 h); no change log, no replicas. Design: an owner index
    `(label, key, value) -> ID` in a badger under `DataDir/index` (hot volume), kept by core under the value locks it
    already takes: the claim is written (synced as the store's writes are) before the row, every lookup reads the
    owner's row and ignores a claim the row no longer backs (a crash, a rollback, a value change, a delete, a dropped
    shard leave at most such a claim; none is ever missing), so no change-log replay is needed. Steps:
    (a) [x] the index store and its capability on tiered (open at New when declared, Clear wipes it); red: claims
        survive a reopen, Clear empties them. Green on branch `uniqidx` (`tasks/evidence/UNIQIDX/step_a.txt`).
    (b) [x] `CreateUnique` on a tiered event label declares the index and seeds it from the current rows (it is
        refused today); the unique check and `GetOrCreateByKey` read the owner index for such a label; red: a key
        whose row is on a closed cold shard is found (no new node) opening only that shard, a duplicate is refused.
    (c) [x] `ByLabelAndProperty(label, key, v)` (current, DepthAll) on such a label answers from the index.
        (b) and (c) green on branch `uniqidx` (`tasks/evidence/UNIQIDX/step_b.txt`; one lookup path serves both).
    (d) [x] stale claims: value change, delete, label removal, retention drop, rollback, crash between claim and row:
        the value is free again; red tests for each. Green on branch `uniqidx` (`step_d.txt`).
    (e) [x] measured on ai-soc's shape (lookup cost against shard count, write cost per keyed row), released as a
        minor, ai-soc and sigma told. Released in v4.81.0 (`tasks/evidence/UNIQIDX/`). Next slices (label and property
        postings, the relationship owner map) wait for a measured need.

## Waiting

- **R37** (ai-soc's lever for rho, 26 Oct) — listing changed facts costs more than ai-soc's 4x a fact's values (ai-soc
  on v4.79.0: cold 24.7x for 2 versions; rho 97 % of the bytes). Done when: ai-soc's warm and cold listing tests are
  under 4x. v4.83.0 cut rho's part (`TestR37ListingBytesPerFact`): 2 versions 8.5x -> 4.0x warm, 14.9x -> 10.5x
  cold; sigma's fallback 18.5x -> 15.8x. Left: a cold listing decodes each current row into a Node (~3.6x by
  itself), so below 4x cold needs a column door (cols incl. ov_session, ov_sid; ValidFrom/To and TxFrom per segment;
  values shared when unchanged) together with sigma's segment-based joined(): one item, queued where ai-soc ranks it
  (sigma 26 Oct: not now). ai-soc on v4.83.0 (26 Oct, derived_list_cost_test.go): cold 2 versions 21.6x -> 18.3x;
  cold 1 version 7.4x, warm 2 versions 6.2x (13 props) and 4.3x (20 props) unchanged. Its warm test lists through
  sigma's joined(), maybe not through the door rho cut; ai-soc ranks the column door below sigma's PAGESWITCH and
  JOINFIT. ai-soc's warm profile (26 Oct): the listing goes through the page door, but its warm store is memory,
  whose `GetNodeVersion` deep-copies every version (`PropertySlice.DeepCopy`, 1.5 of 3.5 MB); R37 changed only the
  badger and tiered paths. Candidate: memory lends its frozen version rows to the timelines (a door, a minor); not
  requested by ai-soc (its goal 2 is sigma's state). Waits for: a consumer asking, or the column door.
- **CHAIN** (memory split known 26 Oct: k10260011 heap 30.0 GB, sigma 89 %, rho 3.3 GB, badger 0.77 GB; storage
  split still open) — René wants the whole data chain rewritten because it wastes 3 to 4 times the storage and memory, so first
  measure on the BA copy on Flux which layer (ai-soc, sigma, rho) wastes how much. Done when: the split per layer is
  known. Waits for: René's word and a free Flux (sigma's ENDPOINT and EDGEREF shipped in v0.11.66 and v0.11.68, 26 Oct;
  ai-soc's day run k10260915 on v4.82.0 runs today), René's word and a free Flux (ai-soc: free after about 15:30 on 24 Oct; tell ai-soc before a start). sigma (24 Oct): the scan tool (`projects/derived-writeback`, wbscan)
  is ready, but Flux runs one big job at a time (ai-soc's k10241303 now) and a Flux run needs René's word.
- **CHAIN-2** — Each layer's share of the waste decides what that layer changes, against a target for stored bytes per
  fact. rho's options: (a) the believed row stays the current row, so a re-derived fact reads the current row, not
  history; (b) an excise piece stores no copy of the row; (c) a delta's anchor is decoded once per read, not once per
  row; (d) decoded rows closer to their stored size. Default: none before CHAIN's numbers. Done when: René has
  decided per layer. Waits for: CHAIN.
- **REPORT** — ai-soc and sigma have not yet reported on rho's newer doors, and rho builds only what blocks
  them. 26 Oct: sigma v0.11.68 (ENDPOINT, EDGEREF) pins v4.80.0 (VALUES, TKGID not wired yet); ai-soc pins v4.79.0 and takes v4.81.1
  and v4.82.0 with its next pin (ShardWindowLocation UTC, OpenStats read at the next run). Done when: both have reported. Waits for: ai-soc, sigma.
- **ORIGIN** — Every release Markus makes on origin has to be merged and checked against rho's rules. Done when: no
  release of his is unmerged. Waits for: Markus.
- **TOMBSTONE** — A delete cannot keep the believed end of validity (backlog 4, 7, 25). Done when: released as a minor.
  Parked 24 Oct: neither sigma nor ai-soc needs it (sigma removes by excising validity). Plan: rebuild branch
  `tombstone-writer-v4.63` (`37a1ff56`) on the v4.66 axis code, ~½ day, heads-up to sigma and ai-soc (v4.64.0 was
  never cut). Waits for: a consumer needing it (sigma's P24).
- **STORE** (ai-soc, 23 Oct) — Store layout choices for ai-soc's remodel: history delta encoding per label, Derived and
  Session on the never-purged reference shard, and the instance nodes (~2/3 of the BA store). Done when: René has
  decided with ai-soc and sigma (their STORECFG talk). sigma (24 Oct): agreed with René for p4b only:
  `PropertyIndexOnDisk`, a bounded entity cache, DerivedPage as a reference label on ai-soc's tiered stores
  (`LabelIndexOnDisk` once TIERLABELDISK ships); delta encoding per label, Derived/Session placement and the instance
  nodes are not agreed. ai-soc (24 Oct): René works on it with them directly (STORECFG). Waits for: René.
- **REVIEWS** — Review findings were left out under the scope rule: tests review #5-#10, code review #4-#7, docs
  #9-#10, old rows 77-82. Done when: each is decided on its evidence. Default: leave each until a consumer hits it.
  Waits for: the main session.
- **FOLDS** — Range lookups check the resolved version, not every version in the range (backlog 16). Done when: René
  has decided. sigma (24 Oct) uses both forms: `ForEachByLabelPropertyRange` (resolved version) and
  `ForEachByLabelPropertyRangeAnyVersion` (graphhost/bind.go:528), and `NodesByLabelPropertyDuring` in its write-back.
  Default: keep today's documented behaviour. Waits for: ai-soc's check of its history range reads, then decided on
  that data.

## Later

- **PURGEIDS** (sigma, for PAGEGC; not urgent) — a hard purge by id on every store. rho has PurgeExpiredNodes/Rels
  (by policy) and Void/ExciseNodeValidity (by validity), no purge by id. Waits for: sigma's answers (nodes, rels or
  both; history too or current row only; ids reusable or not; batch size; which stores).
- **UNIQIDX adoption** (ai-soc, 26 Oct) — ai-soc moves Process, File, RegValue and LocalObject to event labels only
  with SHARD step 10 on René's word, after counting duplicate keys on a store copy (ErrUniqueViolationExisting).

- **AISOC** — ai-soc remodels its wasteful data model and may send rho requests; each becomes its own item here, queued
  after the items above (René 23 Oct), worked on its own branch with clean commits. Done when: none is open. Waits for:
  ai-soc's requests.
- **DEVREF** (ai-soc, 23 Oct) — ai-soc wants a device id stored once, on its Device node, so rho must find a node's
  edges by that endpoint in one index read, as cheaply as today's estate+device property index. Done when: ai-soc
  drops the `device` text property from edges and still answers "rows of device X" in one index read. Waits for:
  FLUSHPART and SHARD.
- **WRITTENAT** (ai-soc, 24 Oct; withdrawn by ai-soc 26 Oct) — not built: sigma's PUBIDS gives ai-soc the ids of the
  facts a publication wrote (Delta.Written), with no store index. Waits for: ai-soc, only if PUBIDS fails.

- **DICTMERGE** (ai-soc, 25 Oct; after DICT) — with per-shard dictionaries a value seen daily is stored once per day
  shard (~365 a year). Could cold shards share or merge their dictionaries when they move cold, so a value is stored
  once per cold volume? Done when: answered from DICT's measured per-shard dictionary bytes.
- **PROPPOS** (sigma's question, 25 Oct; after DICT) — every stored property is a msgpack map with named fields:
  about 12 B (map header, `kt` + uint16, `v`, `t` + type byte) before its value. A positional form ([kt, v|dt, t])
  would cut about 7 B from every property in every store. A one-way door (a new row format). Done when: the
  per-property overhead is measured as a share of stored bytes on a real store (ai-soc's tracker), then decided.
- **GETWALK** — Entity reads on a cache miss, history version and anchor reads and committed-stamp reads use point gets
  (about 4x the row each; 45 `Txn.Get` sites) where a forward walk costs almost nothing (rule 2). Done when: the hot paths walk, measured.
- **ITERPREFIX** (measured 25 Oct, no change) — 49 iterators set no `IteratorOptions.Prefix`; on a compacted store
  a short walk costs about 3.5 us with or without it (`tasks/evidence/ITERPREFIX/`). Waits for: a shard showing an
  L0 pile-up under ai-soc's load (badger `Levels()`), where a Prefix would skip L0 tables.
- **COLDWRITE** (ai-soc, Fable G4, 26 Oct; for other consumers, ai-soc asks nothing) — can a tiered write land in a cold shard, and is it lost to recovery?
  Answered from the code: yes it can (an update or late event older than ColdAfter opens the shard writable,
  `tieredstore_lifecycle.go` checkout); rho's recovery does not scan by time (each shard replays its WAL at its next
  open; the clock floor is one reference-shard key raised before any shard's rows). Done when: a test writes INTO a
  cold shard with DurableCommit, drops the store without Close, and reads it back after reopen (for other
  consumers). ai-soc (26 Oct): the recovery "by time" is ai-soc's own (graphmgr), and no ai-soc write opens a cold
  shard (reference labels, RelsByMint edges on the hot shard, recent rows only); nothing more asked.
- **METASYNCRIG** (René 26 Oct: a later item) — the power-loss case of METASYNC (without `SyncWrites` a meta write
  in a retired memtable can be lost; source-confirmed, docs in v4.81.0) has no test: it needs a power-loss filesystem
  (LazyFS or dm-flakey). Done when: the rig shows the loss and the documented guard (`SyncWrites`, `MetaSetDurable`).
- **OPENIMAGE** — A reopen after a crash rebuilds every row although the open image holds most of the state;
  `IteratorOptions.SinceTs` reads only the keys newer than the image. Done when: such a reopen reads only those keys,
  measured on a large shard.

- **COLDSTART** — A fresh agent has to be able to work from the 13 docs alone. Done when: every gap it finds is fixed
  in the file that owns it.
- **GUARDS** — The rules the docs no longer state need tests that fail when the rule is broken. Done when: each guard
  fails on a planted violation.
- **SUITE** — About 5,000 lines of memory and badger tests are near copies of each other. Done when: they run as the
  shared store suite.

## Parked

- **CACHE** — Every open badger shard keeps its own memtable arena, block cache and 4 compactors: 88 MB of heap and
  18 goroutines per writable shard (rho probe, 26 Oct; read-only 3.3 MB). ai-soc's k10260011 heap (26 Oct): 30.0 GB
  in use, sigma 26.6 GB (89 %), rho 3.3 GB (1.4 GB not via sigma), badger and ristretto 0.77 GB, so a store-wide
  budget or read-only warm shards would save under 1 GB. Parked by ai-soc's ranking; reopen on a consumer whose
  badger share is large.
- **REOPEN** — ai-soc saw a slow history read after a reopen. Waits for: ai-soc's profile naming the slow call.
- **PENDING** — Reading one entity walks the whole write buffer on badger (half built on branch
  `row102-pending-ranged`). Waits for: ai-soc raising its priority, or nothing else open.
- **RELSIDECAR** — Relationship lookups by several properties may be too slow on BA. Waits for: a BA measurement.
- **NITS** — Small findings no consumer has hit (backlog 80-82, review nits). Waits for: a consumer hitting one.
- **NOCOMPRESS** — Compression cannot be turned off, because "none" is the default value. Waits for: a consumer asking.
- **STALECITE** — Some code comments cite todo sections that no longer exist. Waits for: the next change to that code.
- **V5** — The v5 line is paused on the local branch `v5-takeover` (state: HANDOVER; open WPs and the breaking API-pass
  candidates: `git show 791c1aa3:tasks/todo.md`). Default: paused; `v5.0.0-alpha.1` at its S2 exit once restarted.
  Waits for: sigma's and ai-soc's use of it (asked 24 Oct), then decided on that data.
- **BACKLOG** — 26 numbered items without a requester, listed in `HANDOVER.md` because code cites their numbers. Waits
  for: a consumer hitting one, or René.
- **BADGERPANIC** — badger panics in its own goroutines (table open, compaction) when a table block's trailer is
  damaged: it parses the trailer before it verifies the block's checksum (`table/table.go:575-604`), so rho cannot
  recover and the process dies (evidence: `tasks/evidence/VERIFY/trailer_test.go.txt`). René (24 Oct): use badger as
  it is, no fork. rho recovers the panics it can (its own read paths and the open scan). Waits for: a badger release
  that verifies before parsing, or René.
