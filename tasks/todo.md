# todo

What rho does next, in order. Last release: v4.75.1 (2026-10-24). Background, René's open decisions with my defaults,
and the numbered backlog: `HANDOVER.md`. Done work: `docs/CHANGELOG.md`, under the same tag.

Each item: one sentence, then when it is done and what it waits for.

## Now

Order (René 24 Oct, "fixes first", then "the given order"): FLUSHPART + KEYSIZE + VALUESIZE shipped in v4.75.0
(tagged, ai-soc and sigma told); DECODEBOUND shipped in v4.75.1; next VERIFY + BADGERLOG (built), CLEARMETA, then BADGER step 3 /
SHARD; DROPALL, DISCARDCLOSE and METASYNC after SHARD.

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

- **VERIFY + BADGERLOG** (built on branch `verifylog`, 24 Oct; release v4.75.2 pending) — With rho's options badger
  checked no value-log value and no table block, swallowed a failed value-log read into an empty value, cut the
  newest value-log file at a bad entry on open without a word, and got no logger (tiered and sharded have no logger
  field), so one flipped byte could reach a caller as data. Now: value and block checksums on (a few us per
  block-cache miss, about 1 ms per MB-sized row read, `tasks/evidence/VERIFY/cost.txt`), badger's reports go to `slog`
  by default in every backend, a value-log cut at open is reported, and rho recovers badger panics on its own read
  paths and the open scan. Tests: 6, each red without its part (72 flipped table positions: none silent with
  checksums, 11 without). Full short suite and `-race` on badger, tiered and sharded green (24 Oct). Committed
  with docs and CHANGELOG on `verifylog` (rebased on main). Gate 1 red on lint (an unchecked `Close` in the new report file),
  fixed; gate 2 refused it as a minor: `scripts/patchgate` counted the exported methods of
  the unexported `slogBadgerLogger` (badger's Logger interface) as API. The checker now skips methods of unexported
  types (red case added); gate 3 red once on `TestHistoryScanAllocGate` (24 vs 19 allocations;
  it counts process-wide, passes alone with checksums on and off). Likely cause: the new badger logger formatted lines
  slog then dropped; it now formats only kept levels; three package runs green, cause not proven. Gate 4 running. Done when: released as a patch, ai-soc and sigma told.
- **CLEARMETA** (confirmed by a real crash test on branch `clearmeta`, `tasks/evidence/CLEARMETA/red.txt`) — `Clear`
  with the change log on (or once on) deletes every meta key except `last_lsn` before it drops the data; a process
  that dies there reopens a store that looks empty while its rows remain, and the first new label collides with
  their tokens (its query fails: "property-key token 1 not in registry"). Done when: `Clear` drops the data before
  the meta that interprets it (the order is drafted), the test passes, released as a patch. Next after VERIFY +
  BADGERLOG. sigma (24 Oct): reachable through
  its daemon's admin reset with the change log on.

## Next

- **SHARD** — René wants three storage tiers on three volumes (hot on fast NVMe, warm on SSD, cold on block devices)
  built while ai-soc cleans up its data waste; the design is ADR-0012 in `docs/architecture.md`. Done when: the steps
  below are ticked and released. Waits for: VERIFY + BADGERLOG and CLEARMETA (René 24 Oct); then BADGER step 3
  (per-tier badger options) leads in; step 1 with option (b), pre-creating `reference/` and `archive/` only for a new
  store (René 23 Oct), from branch `shard-step1`.

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

- **CHAININTERN** (sigma for ai-soc, 24 Oct; heap profile, sigma v0.11.60 on rho v4.75.1) — A node with n versions
  holds every unchanged property value n times in decoded history rows: a Derived node's identity properties
  (`ov_namespace`, `ov_rel`, `ov_key`, about 146 B) once per version, 5 copies for one fact. `GetNodeHistory` and the
  paged and timeline reads decode each version alone (`badgerstore_history_node.go:772`); rho's entity cache holds the
  current version only. Fix: a chain's decoded versions reuse the previous version's value when the bytes are equal (no
  API, no format change, a patch). Done when: a test shows n versions sharing one copy of an unchanged value
  (allocation count), every history read path (badger, tiered, sharded, memory) covered. Order: René.
- **IDPROPS** (sigma for ai-soc, 24 Oct; the larger form of CHAININTERN) — Immutable properties per (label, key), set
  at `AddNode`, refused by every update and cascade patch, stored once per node and attached to every version on read.
  A new door (a minor) and a format change (a one-way door). Waits for: CHAININTERN's measured saving, then René.
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
- **V5** — The v5 line is paused on the local branch `v5-takeover`. Waits for: René.
- **BACKLOG** — 26 numbered items without a requester, listed in `HANDOVER.md` because code cites their numbers. Waits
  for: a consumer hitting one, or René.
- **BADGERPANIC** — badger panics in its own goroutines (table open, compaction) when a table block's trailer is
  damaged: it parses the trailer before it verifies the block's checksum (`table/table.go:575-604`), so rho cannot
  recover and the process dies (evidence: `tasks/evidence/VERIFY/trailer_test.go.txt`). René (24 Oct): use badger as
  it is, no fork. rho recovers the panics it can (its own read paths and the open scan). Waits for: a badger release
  that verifies before parsing, or René.
