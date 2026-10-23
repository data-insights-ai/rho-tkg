# todo

What rho does next, in order. Last release: v4.74.0 (2026-10-23). Background, René's open decisions with my defaults,
and the numbered backlog: `HANDOVER.md`. Done work: `docs/CHANGELOG.md`, under the same tag.

Each item: one sentence, then when it is done and what it waits for.

## Now

- **SHARD** — René wants three storage tiers on three volumes (hot on fast NVMe, warm on SSD, cold on block devices)
  built while ai-soc cleans up its data waste; the design is ADR-0012 in `docs/architecture.md`. Done when: the steps
  below are ticked and released. Waits for: nothing.

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

## Next

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
- **CACHE** — Every open badger shard keeps its own cache, so ai-soc's memory grows with the shards it opens. Done when:
  one cache budget for the whole store, or a documented size per shard. Waits for: ai-soc's memory numbers.
- **TOMBSTONE** — A delete cannot keep the believed end of validity (backlog 4, 7, 25). Done when: released as a minor.
  Waits for: René, or sigma or ai-soc needing it.
- **REVIEWS** — Review findings were left out under the scope rule. Done when: René has decided. Waits for: René.
- **PURGE** — A purged entity answers "not found" at a time it answered before. Done when: René has decided. Waits
  for: René.
- **AXIS** — The announced change of the default valid-time start for new stores is still held. Done when: René has
  decided. Waits for: René.
- **FOLDS** — Range lookups check the resolved version, not every version in the range (backlog 16). Done when: René
  has decided. Waits for: René.

## Later

- **FLUSHPART** — A flush above badger's batch limit (about 10 MB or 104,855 entries) is several transactions, so a
  crash keeps a random subset of its rows, loses its change-log records for good, and after a purge leaves a store that
  refuses to open (`docs/badger.md` rule 1). Done when: a crash test at a planted split reopens consistent and loses
  no record, released as a patch.
- **GETWALK** — Entity reads on a cache miss, history version and anchor reads and committed-stamp reads use point gets
  (about 4x the row each) where a forward walk costs almost nothing (rule 2). Done when: the hot paths walk, measured.
- **PREFETCH** — 13 iterators turn on value prefetching, one goroutine per row for values that are inline anyway (rule
  3). Done when: none does, the open-time node scan measured.
- **ITERPREFIX** — The snapshot walkers merge every table of every level because they set no prefix (rule
  4). Done when: each sets at least its keyspace byte.
- **VLOGGC** — The value-log GC loop runs for nothing, since every row is stored inline (rule 9). Done when: removed
  or made conditional.

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
