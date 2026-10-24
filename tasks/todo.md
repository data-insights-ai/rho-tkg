# todo

What rho does next, in order. Last release: v4.74.0 (2026-10-23). Background, René's open decisions with my defaults,
and the numbered backlog: `HANDOVER.md`. Done work: `docs/CHANGELOG.md`, under the same tag.

Each item: one sentence, then when it is done and what it waits for.

## Now

- **FLUSHPART** — A flush above one badger transaction (104,855 entries or about 10 MB at default options) was several
  transactions, so a crash kept a random part of it (`docs/badger.md` rule 1). Done when: merged and released as
  v4.75.0 (a minor: format marker 9 is a one-way door), ai-soc and sigma told. Waits for: René's go to merge and tag.

### FLUSHPART plan

1. [x] Red tests on branch `flushpart` (`tasks/evidence/FLUSHPART/red.txt`): a crash lost 7,582 of 20,000 change-log
   records; a crashed removal flush left a store that refused to open.
2. [x] Fable reviewed the design (2026-10-23): one `Txn` when the flush fits, else staged under `0x0E` and committed
   by a marker that raises the format marker to 9; `New` applies a committed flush; fail closed after the marker.
3. [x] Built on `flushpart` with tests that try to break it (2026-10-24): a crash at every step compared with an
   uncrashed store, crashes inside recovery, damaged and torn staging, readers during the flush, every door after a
   failure, a tiered warm shard. 27 of 27 code mutations fail a test
   (`tasks/evidence/FLUSHPART/mutations.txt`); `make ci-docker` green before the last review round.
4. [x] Fable reviewed twice; all points fixed: a torn unsynced flush (power loss) is discarded at open, a synced one
   refuses and names the new `badger.DiscardStagedFlush`, which also drops the counters.
5. [ ] `make ci-docker` on the final branch passed (2026-10-24, 900f0a78, coverage 87.4%); Fable's last verdict, then
   René's go, merge, tag v4.75.0, tell ai-soc and sigma, move the item to `docs/CHANGELOG.md`. Parked by René while
   the badger work runs.

- **BADGER** (René 24 Oct, the next milestone) — rho fully understands badger, verifies its own badger use by tests,
  then optimises badger for the multi-tier storage. Done when: the three steps below are ticked. The items USAGE,
  BADGERDOC, CLEARMETA, BADGERLOG, DROPALL and METASYNC are its parts.

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
2. [ ] rho's current badger use, fully verified in tests: the call map and the 9-category analysis exist
   (`tasks/evidence/USAGE/`); confirmed by real tests (24 Oct): CLEARMETA (a crash inside `Clear`), KEYSIZE and
   VALUESIZE (one oversized entry blocks all later persistence), VERIFY (silent corruption, panics, silent
   truncation) and BADGERLOG; each finding (CLEARMETA, BADGERLOG, DROPALL, METASYNC, the per-shard costs) gets a test
   that shows the harm or its absence, then its fix.
3. [ ] Badger optimised for the multi-tier storage (SHARD): per-tier options (read-only cold opens, caches, compactors,
   `CompactL0OnClose`), iterator prefixes and prefetch, point reads, the shard mover's copy method, measured per tier.

- **USAGE** (René 24 Oct; phase 1 done: 9 categories, `tasks/evidence/USAGE/results.txt`) — Re-analyse rho's current badger use in the main session, without relying on Fable's
  review: a map of every badger call by kind (writes, point reads, iterators and their options, sync, drops, GC, open
  options), and each finding checked in the code and, where it can fail, by a test. Done when: the map is in
  `tasks/evidence/USAGE/`, each finding is confirmed or refuted with its evidence, and the todo items say so.
- **BADGERDOC** (René 24 Oct; claims of sections 1-7 checked: one more false claim fixed, the bloom filter) — René does not trust rho's understanding of badger: a claim that one row above about
  10 MB cannot commit stood for a day and was false. Done when: every claim in `docs/badger.md` cites badger's source
  or a measurement, every claim rho relies on has a test in `tasks/evidence/BADGER/` that tries to break it, and Fable
  has reviewed the result. The batch API is done (`batch_test.go.txt`, B1-B9).

## Next

- **SHARD** — René wants three storage tiers on three volumes (hot on fast NVMe, warm on SSD, cold on block devices)
  built while ai-soc cleans up its data waste; the design is ADR-0012 in `docs/architecture.md`. Done when: the steps
  below are ticked and released. Waits for: FLUSHPART; step 1 with option (b), pre-creating `reference/` and
  `archive/` only for a new store (René 23 Oct), from branch `shard-step1`.

### SHARD plan

The main agent builds each step (red test, change, tests green), the Fable agent reviews it and monitors the plan; a
step is ticked when it is released. The plan moves to `docs/CHANGELOG.md` when the last box is ticked.

1. [ ] A shard whose directory is missing fails to open instead of opening empty; every shard path goes through one
   function. Done when: released as a patch (red test `TestLazyOpenRefusesMissingShardDir`).
2. [ ] The shard listing shows each shard's volume, path and bytes on disk. Done when: in the volume minor (red test
   `TestListShardsDiskBytesMatchesDirectory`).
3. [ ] The config names a warm and a cold volume and where the reference shard and archive live; the catalog records
   each shard's volume and refuses an unknown one. Done when: in the volume minor (red tests
   `TestShardOpensFromItsVolume`, `TestUnmappedVolumeFailsNew`, `TestSentinelRejectedByOldValidator`).
4. [ ] A closed shard moves to another volume crash-safely. Done when: in the volume minor (red tests: the crash
   matrix after copy, rename, catalog save and removal; a drop of a moved shard removes it there).
5. [ ] An open warm shard moves too: its readers and writers wait, nothing written is lost. Done when: in the volume
   minor (red test `TestMoveWarmShard_WriterWaitsAndWriteLandsOnNewVolume`).
6. [ ] The idle sweep moves one shard per tick to its tier's volume and never fails the store. Done when: in the
   volume minor (red tests `TestSweepMovesWarmToWarmDirAndColdToColdDir`, `TestMoverErrorIsNotStoreWide`).
7. [ ] The reference shard and the archive move offline. Done when: in the volume minor (red test: the reference
   shard on the warm volume still answers unique checks and the watermark).
8. [ ] Each read reports how many cold shards it opened. Done when: in the volume minor (red test: a closed cold
   shard counts once, a retained one not).
9. [ ] The volume minor is released with its one-way door named in the CHANGELOG, and ai-soc and sigma are
   told. Done when: tagged.
10. [ ] The global index on the fast volume, unique keys first (keyed event labels), then label and property
    postings, then the relationship owner map. Done when: each slice released. Waits for: ai-soc's measurement after
    its data cleanup.

- **CLEARMETA** (confirmed by a real crash test on branch `clearmeta`, `tasks/evidence/CLEARMETA/red.txt`) — `Clear`
  with the change log on (or once on) deletes every meta key except `last_lsn` before it drops the data; a process
  that dies there reopens a store that looks empty while its rows remain, and the first new label collides with
  their tokens (its query fails: "property-key token 1 not in registry"). Done when: `Clear` drops the data before
  the meta that interprets it (the order is drafted, not applied), the test passes, released as a patch. The fix
  waits until the BADGER milestone's step 2 is through (René 24 Oct: verify first).
- **KEYSIZE** (confirmed, data loss; `tasks/evidence/USAGE/size_test.go.txt`) — A string value rho accepts (65,100
  bytes, under its 65,536 limit) on a label with an on-disk property index makes an index key above badger's 65,000
  (`property_index_key.go:136-139`, `txn.go:352`): the write is acknowledged, every later flush fails on that key, and
  nothing written afterwards reaches disk. Done when: such a write is refused at the door or stored (capped or hashed
  key), the test passes, released as a patch.
- **VALUESIZE** (confirmed, data loss; same evidence) — A 1.4 MB row (every value under rho's limits) with
  `ValueLogFileSize` 1 MB, or in an in-memory badger store, is acknowledged and then refused by badger at every flush
  ("Value with size 1440490 exceeded 1048576"), blocking all later writes like KEYSIZE; with default options it
  commits. Done when: the write door refuses a row the store cannot hold, the test passes, released as a patch.
- **DISCARDCLOSE** (candidate) — The scan snapshot and the rel-type build walk always `Discard` their transaction,
  which can block after `db.Close` (`y/watermark.go:60`, `:77-79`). Done when: a test closes the store under them.
- **BADGERLOG** (confirmed, same evidence) — No backend gives badger a logger, so badger's reports are discarded: a
  failed value-log read shows rho only "corrupt wire data", while the cause ("Unable to read … ErrEOF") reaches only
  a configured logger. Done when: badger's errors reach rho's logging by default, tested with a failed value-log
  read.
- **DROPALL** — `Clear` of a store that never had the change log calls badger's `DropAll` while cache-miss reads and
  read snapshots may hold a badger transaction; measured: a value-log value (a row of 1 MB or more) read across it
  comes back empty with a nil error, since badger swallows value-log read errors; rho then reports "corrupt wire
  data", never a wrong row. The same holds for any value-log read failure, its cause unlogged under rho's nil badger
  logger. Done when: such a read reports the real cause, and a read racing `Clear` reports not-found or the row.
- **METASYNC** — `MetaSetDurable` and the property-key registry save promise durability after `db.Sync`, which syncs
  only the active WAL, so after a memtable rotation the write may not survive a power loss without `SyncWrites`
  (`badgerstore_meta.go:324-334`, `:420-434`). Done when: a test shows the gap or its absence, and the doors and
  `docs/persistence.md` state what holds.
- **CHAIN-2** — Each layer's share of the waste decides what that layer changes, against a target for stored bytes per
  fact. Done when: René has decided per layer. Waits for: CHAIN.

## Waiting

- **RESUME** — sigma reports that a valid-time write ending exactly where an existing row starts still writes a copy
  of that row as a "resumption" row (one wasted row per rewritten step, part of CHAIN's waste). Done when: rho writes
  no resumption row when nothing needs resuming, with the same answers, released as a patch. Waits for: SHARD step 1.
- **CHAIN** — René wants the whole data chain rewritten because it wastes 3 to 4 times the storage and memory, so first
  measure on the BA copy on Flux which layer (ai-soc, sigma, rho) wastes how much. Done when: the split per layer is
  known. Waits for: René's "run" (sigma runs it).
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
  Waits for: René, or sigma or ai-soc needing it.
- **STORE** (ai-soc, 23 Oct) — Store layout choices for ai-soc's remodel: history delta encoding per label, Derived and
  Session on the never-purged reference shard, and the instance nodes (~2/3 of the BA store). Done when: René has
  decided with ai-soc and sigma (their STORECFG talk). Waits for: René.
- **REVIEWS** — Review findings were left out under the scope rule. Done when: René has decided. Waits for: René.
- **PURGE** — A purged entity answers "not found" at a time it answered before. Done when: René has decided. Waits
  for: René.
- **AXIS** — The announced change of the default valid-time start for new stores is still held. Done when: René has
  decided. Waits for: René.
- **FOLDS** — Range lookups check the resolved version, not every version in the range (backlog 16). Done when: René
  has decided. Waits for: René.

## Later

- **AISOC** — ai-soc remodels its wasteful data model and may send rho requests; each becomes its own item here, queued
  after the items above (René 23 Oct), worked on its own branch with clean commits. Done when: none is open. Waits for:
  ai-soc's requests.
- **DEVREF** (ai-soc, 23 Oct) — ai-soc wants a device id stored once, on its Device node, so rho must find a node's
  edges by that endpoint in one index read, as cheaply as today's estate+device property index. Done when: ai-soc
  drops the `device` text property from edges and still answers "rows of device X" in one index read. Waits for:
  FLUSHPART and SHARD.
- **WRITTENAT** (ai-soc, 24 Oct) — ai-soc wants the nodes of one label (and one property value) written at one
  transaction, without a scan, so its records can drop the list of changed keys. Done when: such a lookup costs
  O(rows written at that transaction), with its write cost per row measured. Waits for: FLUSHPART and SHARD.
- **VALUES** (sigma, 24 Oct) — sigma wants one lookup by many values of one property instead of one call per value.
  Done when: `nodes.ByLabelAndPropertyValues` is released on every backend. Waits for: FLUSHPART and SHARD.
- **TKGID** (sigma, 24 Oct) — sigma wants the shadow name `tkg_id` (an entity's id) reserved, best resolved by rho.
  Done when: `NodeProperty` and `RelProperty` answer `tkg_id`. Waits for: FLUSHPART and SHARD.

- **GETWALK** — Entity reads on a cache miss, history version and anchor reads and committed-stamp reads use point gets
  (about 4x the row each; 45 `Txn.Get` sites) where a forward walk costs almost nothing (rule 2). Done when: the hot paths walk, measured.
- **PREFETCH** — 14 iterators prefetch values (13 explicitly, plus `badgerstore_planner_stats_deferred.go:137` by
  default), one goroutine per row for values that are almost always inline (rule 3). Done when: none does, the open-time node scan measured.
- **ITERPREFIX** — 49 of rho's 71 iterators seek a prefix without setting `IteratorOptions.Prefix`, so badger cannot
  skip the tables outside it (rule 4; `tasks/evidence/USAGE/usage-map.txt`). Done when: each sets at least its
  keyspace byte, measured on a multi-level store.
- **VERIFY** (confirmed by damage tests, `tasks/evidence/USAGE/vlog_test.go.txt`) — With rho's options badger checks
  neither value-log values nor table blocks. One flipped byte: in an older value-log file it reaches the caller AS
  DATA, no error, no log (rows of 1 MB or more); in the newest value-log file the next writable open silently cuts
  the file there, losing that row and every later one in it; in a table, 3 of 7 positions made badger PANIC (the
  process dies), 3 made the open refuse on the counter check, 1 failed reads. Done when: value checksums and block
  checksums are verified (`VerifyValueChecksum`, `ChecksumVerificationMode` OnBlockRead, measured cost), a corrupt
  read is an error and never a panic, and an admin door runs `VerifyChecksum` per shard.
- **OPENIMAGE** — A reopen after a crash rebuilds every row although the open image holds most of the state;
  `IteratorOptions.SinceTs` reads only the keys newer than the image. Done when: such a reopen reads only those keys,
  measured on a large shard.
- **VLOGGC** — The value-log GC loop runs on every shard although only rows of 1 MB or more reach the value log (rule
  9). Done when: it runs only where the value log holds data.

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
- **V5** — The v5 line is paused on the local branch `v5-takeover`. Waits for: René.
- **BACKLOG** — 26 numbered items without a requester, listed in `HANDOVER.md` because code cites their numbers. Waits
  for: a consumer hitting one, or René.
