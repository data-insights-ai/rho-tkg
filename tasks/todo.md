# todo

What rho does next, in order. Last release: v4.77.2 (2026-10-25). Background, René's open decisions with my defaults,
and the numbered backlog: `HANDOVER.md`. Done work: `docs/CHANGELOG.md`, under the same tag.

Each item: one sentence, then when it is done and what it waits for.

## Now

Order (René 24 Oct: fixes first; open decisions settled from the data and the consumer agents): the fixes are released
in v4.75.0 to v4.76.1, DICT in v4.77.0, RESUME in v4.77.1, ARM32 and LONGTIER in v4.77.2, SHARD steps 2-8 (the volume minor) in v4.78.0 (tagged, consumers
build, ai-soc and sigma told; CI running; the badger race package took 582 of its 600 s in the gate; ai-soc
measures DICT before configuring it; `docs/CHANGELOG.md`). Next: BADGER step 3, MOVEREAD (after ai-soc's shard
sizes), SHARD step 10 (after ai-soc's measurement); then DROPALL, DISCARDCLOSE and METASYNC.

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

## Next

- **SHARD** — René wants three storage tiers on three volumes (hot on fast NVMe, warm on SSD, cold on block devices)
  built while ai-soc cleans up its data waste; the design is ADR-0012 in `docs/architecture.md`. Done when: the steps
  below are ticked and released. Waits for: nothing (fixes shipped); BADGER step 3
  (per-tier badger options) leads in. Step 1 shipped in v4.76.1; DICT in v4.77.0; RESUME in v4.77.1.

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

- **MOVEREAD** (found in the volume minor's review, 25 Oct) — a move makes the shard's readers and writers wait for
  its whole copy (its `DiskBytes` at the volumes' copy rate), and the sweep moves a warm shard soon after rotation.
  Done when: reads of a moving shard go on from the source during the copy (writers wait), or the measured stall is
  accepted. Waits for: ai-soc's shard sizes on disk and whether its reads and writes reach a rotated-out shard
  (asked 25 Oct); no consumer maps a volume yet.

- **DISCARDCLOSE** (after SHARD; confirmed, low; `tasks/evidence/USAGE/discard_test.go.txt`) — The scan snapshot and the rel-type
  build walk always `Discard` their transaction; after `db.Close` up to 100 such discards return and the 101st blocks
  forever. Done when: they skip `Discard` after `Close` as the read snapshot does, tested with 101.
- **DROPALL** (after SHARD) — `Clear` of a store that never had the change log calls badger's `DropAll` while cache-miss reads and
  read snapshots may hold a badger transaction; measured at rho level (`tasks/evidence/USAGE/dropall_rho_test.go.txt`):
  a GetNode that found the key of a 1.4 MB row before `Clear` fails with "corrupt wire data" (badger swallows the
  value-log read error and returns an empty value); an inline row reads back whole. Done when: such a read reports the real cause, and a read racing `Clear` reports not-found or the row.
- **METASYNC** (after SHARD; source-confirmed; not testable by a process kill) — `MetaSetDurable` and the property-key registry
  save promise durability after `db.Sync`, which syncs only the active WAL, so after a memtable rotation the write
  may not survive a power loss without `SyncWrites` (`badgerstore_meta.go:324-334`, `:420-434`). Done when: the doors
  and `docs/persistence.md` state what holds, and a power-loss test (LazyFS or dm-flakey) shows it, if René wants
  that rig.
- **CHAIN-2** — Each layer's share of the waste decides what that layer changes, against a target for stored bytes per
  fact. Done when: René has decided per layer. Waits for: CHAIN.

## Waiting

- **CHAIN** — René wants the whole data chain rewritten because it wastes 3 to 4 times the storage and memory, so first
  measure on the BA copy on Flux which layer (ai-soc, sigma, rho) wastes how much. Done when: the split per layer is
  known. Waits for: René's word and a free Flux (ai-soc: free after about 15:30 on 24 Oct; tell ai-soc before a start). sigma (24 Oct): the scan tool (`projects/derived-writeback`, wbscan)
  is ready, but Flux runs one big job at a time (ai-soc's k10241303 now) and a Flux run needs René's word.
- **R37** — Listing facts that were changed still costs more than ai-soc's limit of 4 times a fact's values. Done when:
  ai-soc's cold /bars test passes on Flux. Waits for: CHAIN-2.
- **REPORT** — ai-soc and sigma have not yet reported on rho's newer doors, and rho builds only what blocks
  them. Done when: both have reported. Waits for: ai-soc, sigma.
- **ORIGIN** — Every release Markus makes on origin has to be merged and checked against rho's rules. Done when: no
  release of his is unmerged. Waits for: Markus.
- **CACHE** — Every open badger shard keeps its own 256 MB block cache, 4 compactors and a per-minute directory walk,
  so ai-soc's memory grows with the shards it opens (`docs/badger.md` section 9, item 11). Done when: one cache budget
  for the whole store (`CacheMaxCost`) and per-role options, or a documented cost per shard. Waits for: ai-soc's memory numbers.
- **TOMBSTONE** — A delete cannot keep the believed end of validity (backlog 4, 7, 25). Done when: released as a minor.
  Parked 24 Oct: neither sigma nor ai-soc needs it (sigma removes by excising validity). Waits for: a consumer
  needing it.
- **STORE** (ai-soc, 23 Oct) — Store layout choices for ai-soc's remodel: history delta encoding per label, Derived and
  Session on the never-purged reference shard, and the instance nodes (~2/3 of the BA store). Done when: René has
  decided with ai-soc and sigma (their STORECFG talk). sigma (24 Oct): agreed with René for p4b only:
  `PropertyIndexOnDisk`, a bounded entity cache, DerivedPage as a reference label on ai-soc's tiered stores
  (`LabelIndexOnDisk` once TIERLABELDISK ships); delta encoding per label, Derived/Session placement and the instance
  nodes are not agreed. ai-soc (24 Oct): René works on it with them directly (STORECFG). Waits for: René.
- **REVIEWS** — Review findings were left out under the scope rule. Done when: René has decided. Waits for: the main session, which decides each finding on its evidence.
- **PURGE** — A purged entity answers "not found" at a time it answered before. Decided 24 Oct from the data (René:
  open decisions by data and agents): no change. sigma reads below a purge through a floor that gates the cut, and
  ai-soc has no purge yet; neither expects an old answer from before a purge. Leaves todo with the next release.
- **AXIS** — The announced change of the default valid-time start for new stores is held. Decided 24 Oct from the
  data: dropped, the default stays. ai-soc creates a fresh store per run and relies on the default (graphmgr
  manager.go:473); sigma sets `UntimedWholeAxis` explicitly. Changing it would silently change ai-soc's answers.
  Leaves todo with the next release; the announcement is withdrawn in the CHANGELOG then.
- **FOLDS** — Range lookups check the resolved version, not every version in the range (backlog 16). Done when: René
  has decided. sigma (24 Oct) uses both forms: `ForEachByLabelPropertyRange` (resolved version) and
  `ForEachByLabelPropertyRangeAnyVersion` (graphhost/bind.go:528), and `NodesByLabelPropertyDuring` in its write-back.
  Waits for: ai-soc's check of its history range reads, then decided on that data.

## Later

- **AISOC** — ai-soc remodels its wasteful data model and may send rho requests; each becomes its own item here, queued
  after the items above (René 23 Oct), worked on its own branch with clean commits. Done when: none is open. Waits for:
  ai-soc's requests.
- **DEVREF** (ai-soc, 23 Oct) — ai-soc wants a device id stored once, on its Device node, so rho must find a node's
  edges by that endpoint in one index read, as cheaply as today's estate+device property index. Done when: ai-soc
  drops the `device` text property from edges and still answers "rows of device X" in one index read. Waits for:
  FLUSHPART and SHARD.
- **TIERLABELDISK** (sigma, 24 Oct) — tiered has `PropertyIndexOnDisk` but no `LabelIndexOnDisk`, so a paged row costs
  about 1.2 kB of RAM there instead of about 200 B (sigma's p4b measurement on badger). Done when: tiered passes
  `LabelIndexOnDisk` to every shard, released as a minor. Waits for: the agreed fixes and SHARD (may ride SHARD's
  per-tier options).
- **WRITTENAT** (ai-soc, 24 Oct) — ai-soc wants the nodes of one label (and one property value) written at one
  transaction, without a scan, so its records can drop the list of changed keys. Done when: such a lookup costs
  O(rows written at that transaction), with its write cost per row measured. Waits for: FLUSHPART and SHARD.
- **VALUES** (sigma, 24 Oct) — sigma wants one lookup by many values of one property instead of one call per value.
  Done when: `nodes.ByLabelAndPropertyValues` is released on every backend. Waits for: FLUSHPART and SHARD.
- **TKGID** (sigma, 24 Oct) — sigma wants the shadow name `tkg_id` (an entity's id) reserved, best resolved by rho.
  Done when: `NodeProperty` and `RelProperty` answer `tkg_id`. Waits for: FLUSHPART and SHARD.

- **IDPROPS** (sigma for ai-soc, 24 Oct; the larger form of CHAININTERN) — Immutable properties per (label, key), set
  at `AddNode`, refused by every update and cascade patch, stored once per node and attached to every version on read.
  A new door (a minor) and a format change (a one-way door). Waits for: René.
  Measured (ai-soc, 25 Oct, run k10251802 after KEYBIN and DICT): 470,239 older Derived versions carry 13.2 MB of
  identity (28 B binary keys), 1.6 % of 818.6 MB stored values. Recommendation to René: withdraw (a one-way door
  for 1.6 %). CHAININTERN, its heap form, is withdrawn: decoded history is 0.31 % of the heap (101.1 of 32,565.5 MB, ai-soc,
  heap-end profile of run k10251802, 25 Oct). WBA-8 (sigma, 52,031 duplicate rows) was RESUME, fixed in v4.77.1. Design answer to
  sigma (24 Oct): an identity freezes from its declaration on; sigma restructures Derived identities (key terms as
  typed columns plus an int hash, the DerivedPage shape) with ordinary writes first, then declares. No re-key door
  unless a need after the declaration appears.
- **DICTMERGE** (ai-soc, 25 Oct; after DICT) — with per-shard dictionaries a value seen daily is stored once per day
  shard (~365 a year). Could cold shards share or merge their dictionaries when they move cold, so a value is stored
  once per cold volume? Done when: answered from DICT's measured per-shard dictionary bytes.
- **PROPPOS** (sigma's question, 25 Oct; after DICT) — every stored property is a msgpack map with named fields:
  about 12 B (map header, `kt` + uint16, `v`, `t` + type byte) before its value. A positional form ([kt, v|dt, t])
  would cut about 7 B from every property in every store. A one-way door (a new row format). Done when: the
  per-property overhead is measured as a share of stored bytes on a real store (ai-soc's tracker), then decided.
- **GETWALK** — Entity reads on a cache miss, history version and anchor reads and committed-stamp reads use point gets
  (about 4x the row each; 45 `Txn.Get` sites) where a forward walk costs almost nothing (rule 2). Done when: the hot paths walk, measured.
- **PREFETCH** — 14 iterators prefetch values (13 explicitly, plus `badgerstore_planner_stats_deferred.go:137` by
  default), one goroutine per row for values that are almost always inline (rule 3). Done when: none does, the open-time node scan measured.
- **ITERPREFIX** — 49 of rho's 71 iterators seek a prefix without setting `IteratorOptions.Prefix`, so badger cannot
  skip the tables outside it (rule 4; `tasks/evidence/USAGE/usage-map.txt`). Done when: each sets at least its
  keyspace byte, measured on a multi-level store.
- **OPENIMAGE** — A reopen after a crash rebuilds every row although the open image holds most of the state;
  `IteratorOptions.SinceTs` reads only the keys newer than the image. Done when: such a reopen reads only those keys,
  measured on a large shard.
- **VLOGGC** — The value-log GC loop runs on every shard although only rows of 1 MB or more reach the value log (rule
  9); ai-soc's cut records (up to ~5 MB, rewritten often) do, so there it is needed. Done when: it runs where the
  value log holds data and its reclaim is measured on such a shard.

- **COLDSTART** — A fresh agent has to be able to work from the 13 docs alone. Done when: every gap it finds is fixed
  in the file that owns it.
- **GUARDS** — The rules the docs no longer state need tests that fail when the rule is broken. Done when: each guard
  fails on a planted violation.
- **FLAKE** — One property test misses a node only when it runs beside other tests. Done when: explained and fixed.
- **SUITE** — About 5,000 lines of memory and badger tests are near copies of each other. Done when: they run as the
  shared store suite.

## Parked

- **REOPEN** — ai-soc saw a slow history read after a reopen. Waits for: ai-soc's profile naming the slow call.
- **PENDING** — Reading one entity walks the whole write buffer on badger (half built on branch
  `row102-pending-ranged`). Waits for: ai-soc raising its priority, or nothing else open.
- **RELSIDECAR** — Relationship lookups by several properties may be too slow on BA. Waits for: a BA measurement.
- **NITS** — Small findings no consumer has hit (backlog 80-82, review nits). Waits for: a consumer hitting one.
- **NOCOMPRESS** — Compression cannot be turned off, because "none" is the default value. Waits for: a consumer asking.
- **STALECITE** — Some code comments cite todo sections that no longer exist. Waits for: the next change to that code.
- **V5** — The v5 line is paused on the local branch `v5-takeover`. Waits for: sigma's and ai-soc's use of it (asked 24 Oct), then decided on that data.
- **BACKLOG** — 26 numbered items without a requester, listed in `HANDOVER.md` because code cites their numbers. Waits
  for: a consumer hitting one, or René.
- **BADGERPANIC** — badger panics in its own goroutines (table open, compaction) when a table block's trailer is
  damaged: it parses the trailer before it verifies the block's checksum (`table/table.go:575-604`), so rho cannot
  recover and the process dies (evidence: `tasks/evidence/VERIFY/trailer_test.go.txt`). René (24 Oct): use badger as
  it is, no fork. rho recovers the panics it can (its own read paths and the open scan). Waits for: a badger release
  that verifies before parsing, or René.
