# todo

What rho does next, in order. Last release: v4.98.3 (2026-10-30). René's open decisions are the items that wait for
him, each with my default. State, consumers and the numbered backlog: `HANDOVER.md`; rules: `docs/CONTRIBUTING.md`
§ Rules digest. Done work: `docs/CHANGELOG.md`, under the same tag.

Each item: one sentence, then when it is done and what it waits for.

## Now

- **CIGATES** (René via ai-soc, 31 Oct, "fix all agents ci pipeline"): the guard, patchgate fixes, hook script and gate log are on main; the hook is installed (René's go, 31 Oct); one step waits for Markus as org admin: René's run of the switch got HTTP 422, the setting is org-controlled (31 Oct). `codeql.yml` run 41221392670 analysed all three languages; only its upload was refused, because default setup is on. Done when CodeQL's default setup is off and `codeql.yml`'s first run is green. Waits for Markus: `gh api -X PATCH repos/{owner}/{repo}/code-scanning/default-setup -f state=not-configured` (default setup cannot drop its push trigger, so `.github/workflows/codeql.yml` replaces it, weekly; until the switch that workflow's upload is expected to be refused). Hook runs no package suite, because red tests are committed first on a branch. Guard not on `test-race.sh` shards: their names come from `go test -list`. Seen, not fixed: patchgate misses a new exported struct field or interface method. `make ci-fast` refuses until the next tag: ASOFWALK on main is a door, so that round is a minor.

- **NEXT ROUND** (ai-soc's release round; a minor, as ASOFWALK adds exported API) — ASOFWALK step 1 is on main, untagged (`589a8fbd`, 199 rows per read to 0), with COLDEDGE's patch to follow. Before the tag: consumer check, scale probe, full race suite (v4.98.3 went without it or the probe). Tags only in ai-soc's round; v5.1.0 shipped on v4.98.3 + sigma v0.11.155 (31 Oct).
- **COLDEDGE** (ai-soc 30 Oct, SCALE; blocks its 7-10 day run) — a reference node's Outgoing under RelsByMint opens cold shards per call. On local branch `coldedge`, unmerged: empty cold shards leave the fan-out (30 shards, 29 empty: 1.65 s to about 90 ms), mint fan-out opens read-only (30 shards with rows: 2.9 s to about 0.5 s, one run), changelog, property stats and rel misses skip empty shards. `TestFlatAdjacency_OnlyListedShardOpens` stays red on purpose and must leave the branch before it ships. Done when Outgoing's cost is flat in the number of cold shards, or a run shows it no longer matters. Waits for the release round; cold-open counts come from the first run after that tag.
- **REOPEN** (René via ai-soc, 31 Oct: "it should mostly be read from rho for sigma chases and traces") — ai-soc runs the reopen proof on the pinned pair; no rho door or tag waited on. Measured (FIRSTTOUCH, `tasks/evidence/FIRSTTOUCH/`, one run each, synthetic): per node index 8-12 µs, HasHistory 4-7 µs, timeline 4-5 µs at 200 k and 2 M nodes; presence build 31 and 319 ms; open 193 ms and 3.7 s; page cache dropped, first batch 85 µs a node, then 16 µs, open 5.0 s. k10302100: committedRowStamps about 231 s CPU, presence build 3 s; cutting the re-read needs LISTREAD, since liveness from the ID index would hide a corrupt row (pinned by `has_history_live_test.go`). Done when ai-soc's reopen is in; a fix only if it shows slow first cuts. Waits for ai-soc's run. Open look: what sigma's type scans and property probes open across cold shards at default depth; open-time growth (19x for 10x nodes, one run).
- **TIMEWINDOW** (minor, deferred) — reads bounded by a mint-time range. Serves one-shot evaluation only (ForEachRelEffectiveAtNodes, no time bound, no Depth); a session's maintainer does not reach it. If built: half-open mint range, window intersected with Depth, hot shard always included, caller translates valid to mint. Done when ai-soc and sigma say whether one-shots matter. Waits for the first run after COLDEDGE is in a tag; metric lead `aisoc_sigma_edge_shard_checkouts_total`. Sigma's other candidate: a ValidEndAfter-style bound on ByTypeAndProperty.
- **ASOFWALK later steps** (STATEREC round) — step 1 delegates to the owner shard; next a TxPin batch by IDs, then a cross-shard merge; TxAt stays the skeleton walk with an O(1) belief watermark for a pin at or after current. Done when sigma's 50+ cuts of worst churn bench flat. Waits for the round; parked on the crash path with WHOLEROW and DEPTHLOOK.
- **PROJREAD** (ai-soc, sigma 30 Oct; a later minor) — a label and property lookup returning IDs plus named properties, for sigma's GraphDerivedWriter.find; no door today. Done when the door ships. Waits for the STATEREC round.
- **VDICTDISK step 3** (ai-soc 29 Oct, goal 2 RAM) — steps 1-2 shipped (v4.97.3, v4.98.0); a budget/LRU in place of resident value tokens, decided from the next run's re-measure (at Cap 1,048,576 a 'row' dictionary is about 136 MB per open day shard). Done when an entry's resident cost is measured and bounded. Waits for ai-soc's next run.
- **TIERCLOCK measurement** (ai-soc 29 Oct, goal 2) — released in v4.98.0; ai-soc's pin: Clock = newest committed valid time, ColdAfter 3 d, RelsByMint on. Done when ai-soc's BA replay rotates as live data would. Waits for that run.
- **PURGECURSOR, PURGERESIDUE** (28 Oct; patches) — a label purge resumes from a cursor instead of copying the label's whole ID list per 256-node call (quadratic at 10 M); a purge or history truncate leaves about 30-50 B per ID in the `unknown` map of a shard that stays open. No consumer purges today. Done when each is fixed or dropped. Waits for René's word (STORECFG).
- **PERTIER, RAM doors** (28 Oct; each a minor, on ai-soc's ask with BA numbers) — per-tier MemTableSize (64 to 128 MB on BO: push -2.9 %, GC mark -20.4 s, heap +170 MB); a listed label keeps Elements definitions in RAM and moves scalar ones to disk; Elements rows on disk; COMPOSITEDISK; PROPHOT (only if Derived's index goes to disk). Done when ai-soc asks or drops them. Waits for ai-soc.
- **STATEREC asks** (sigma's design review, 30 Oct; no hurry, after the freeze) — (1) WHOLEROW, blocking for S5: replace a page
  node's current row whole as a new version, any validity, TxFrom and belief kept, indexes following, every backend, listed
  labels only. Today's Update refuses a closed row. Needs a design: what the effective timeline shows of the older pieces.
  Sigma's five tests, tiered and badger: a pin at or after the replace returns that version's validity only, no older piece
  (else sigma's rows stay in pieces and S5 stops); a pin before it reads as before; the current row from an index read is the
  newest version with its TxFrom; TxFrom rises with version across a crash, a closed row and an equal or earlier start accepted;
  ov_k, ov_h and the validity-end index follow the new version, an older pin still finds the node. Other labels' reads unchanged.
  (2) the TxPin walk and batch, as planned. (3) DEPTHLOOK: a TxPin property lookup below DepthAll takes the label-wide fold.
- **TEMPIDX-2** (ai-soc 29 Oct, goal 2 RAM, same run) — the node TemporalIndex is setByID 299 + setPos 131 +
  Extend 124 = 554 MB live at the end (~120 B a node), resident for hot and warm shards (TemporalIndexOnDisk only
  speeds the open); sortedChunks.add is 389 MB (8 B a member, ~49 M members: postings not on the disk lists,
  numKeys/strKeys, numeric buckets, IDSet; never spilled). Asks: fold posByID into byID; a disk form for warm shards
  (the hot day in RAM); numeric buckets and IDSet behind PropertyIndexOnDisk. ai-soc's -peek of sortedChunks.add:
  388 of 389 MB are existence sets (nodeIDs 305 MB EXISTMAP, typeIdx 44 MB OPENWALK, relIDs 38 MB), ~38 M node
  IDs over the open shards; property postings are 11 MB. Scope: existence answered by badger for warm shards (hot
  day in RAM), typeIdx on disk. Done when: each part is measured and fixed or listed with its cost.
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
  COMPOSITEDISK (postings out of RAM; small today: ai-soc's BA k10272220, AddNodeToCompositeIndexes 44.2 MB at
  900 s, +31.1 MB from 300 s, 0.9 % of the growth; ai-soc lists it in every BA heap diff). EXISTMAP shipped in v4.84.0: ~8 B per node in an open shard,
  clean reopen ~0.25 s per 1 M (`tasks/evidence/EXISTMAP/results.txt`); a read that misses a listed row is now
  `ErrCorruptWire`. OPENWALK (v4.87.0): with the label and adjacency indexes on disk a clean open walks no key per
  entity and an open shard keeps ~16 B per rel (the rel ID set and the rel-type index as ID sets). Next walls found
  on the way: the lazily built `relValidIdx` costs 43-67 B per rel (plain badger graphs only; tiered never builds
  it), the other lazy per-entity sidecars (belief watermarks, tx membership, history presence; which ones ai-soc
  and sigma trigger comes with ai-soc's next Flux heap profile), and a crash reopen still decodes every row
  (OPENIMAGE). ai-soc's walls (26 Oct, k10260011, 3 BA days): sigma's resident state (25.4 GB
  census, growing with data; sigma's PROBEMASK, PAGESWITCH, JOINFIT page it); the reference shard keeps every
  Derived fact and every keyed node for ever (Process, File, RegValue, LocalObject: 93 % of 1.04 M keys in 3 days;
  UNIQIDX adoption with SHARD step 10 moves the keyed nodes to event shards, Derived stays; a purge for result rows
  is STORECFG, René later); restart re-reads all past results (625 s at 3 days, mostly sigma's; rho's share from
  OpenStats of k10260915). So EXISTMAP and COMPOSITEDISK bite first on the reference shard.
- **WRITERATE** (SCALE, measured 26 Oct, `tasks/evidence/WRITERATE/results.txt`) — a hot shard's ingest is capped by
  one serial flush pipeline (the graph write lock around each batch, the store's flush mutex, badger's single writer):
  57-91 k facts/s of 500 B here whatever the writer count, 1.1-2x the goal's 46 k/s sustained, while separate stores
  scale near linearly (4 stores 228 k/s). Levers, cheapest first: (1) ASYNCSTAGE is in v4.86.1 (async 1.55-2.0x, 111-146 k/s; durable
  one writer 66 k/s, CPU-bound on one serial path, profile in the results file);
  (2) GROUPFSYNC: a durable batch holds the graph lock through its fsync; overlap the next batch's apply
  with it (needs the rule for what a reader sees before the fsync); (3) LANES: several hot write lanes (K badger DBs
  for the hot window, IDs striped by lane), an ADR first. Done when: one graph ingests 500 B facts at 3x the goal
  (~140 k/s durable) on this machine, measured with the probe. Consumers (26 Oct): ai-soc runs every store durable
  (SyncWrites + DurableCommit), 3-5 writers on one graph, BA peak 2,789 rows/s; sigma's tkgd defaults to async, so
  ASYNCSTAGE serves tkgd only. René's word (26 Oct, relayed by ai-soc): readers never see a fact before its fsync,
  durability stays as it is (so GROUPFSYNC may overlap only what no reader sees: Commit returns once its batch is
  durable, durability follows commit order); the per-row writes are cut instead (WRITEPER, ai-soc and sigma: on a
  BA slice at most 18.7 writes per input row, 15.3 of them sigma's paged rows, an upper bound until sigma's
  WRITEWHY; BA's peak minute 2,789 rows/s is then at most ~52 k writes/s); LANES is the later item for the 2 TB/day
  target, so ingest grows with the hardware.
- **PREEPOCH** (ai-soc 29 Oct, product question, later) — a customer backfill with valid times before the
  2026-01-01 snowflake epoch: ADR-0013 refuses it with ErrValidFromOutsideIDRange. Done when: René decides.
- **IDTABLE** (ai-soc 29 Oct, goal 2) — badger idTable 41 -> 80 -> 160 MB live over the run. Asks: what it maps,
  per shard or not, its bound, whether it leaves with a cold shard. Read (sent 29 Oct): four per store (history
  presence 12 B and stamps 28 B a slot, nodes and rels), ID -> top version (+ stamp fold), lazily built, no cap,
  doubling (41 -> 80 -> 160 MB); a cold shard (probe-only) never builds them, a closing shard drops them. Bounded
  per valid day once TIERCLOCK rotates. Done when: bounded by the open shards (TIERCLOCK) or probe-only for warm.
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

- **LISTREAD** (sigma 31 Oct; later round, no rush, sigma goes on without it; changes no answer) — sigma's restore pages
  ByLabelAndProperties and asks HasHistory per node; ai-soc's store: index 8.1 s, HasHistory 6.4 s of 23.3 s on 1,048,294
  nodes. From the code, not measured: (a) each shard answers a full page, every row decoded, and the merge keeps one page,
  so a page decodes up to shards x limit rows: first measure, then page IDs across shards and fetch only the winners
  (patch). (b) tiered HasNodeHistory re-reads the current row for its stamps, and the listing fills no cache; a cold shard
  also probes with an iterator. Batch door HasHistory(ids), one route per shard, liveness known (a minor). (c) a no-copy
  read-only page only if (a) and (b) leave it worth a door. Measure on a store reopened cold, not sigma's synthetic.
  Sigma 31 Oct: restore reads at DepthHot, row nodes on the reference shard. So (a) and the cold probe don't explain its
  times. Measure first: uncached rows on the reference shard, the stamps re-read, the archive ask, and the one-time build of
  the reference shard's presence set on the first HasHistory (a whole-keyspace scan, possibly inside the 6.4 s).

- **CREATEPEAK** (fix, later; same family, not in STALEBUILD) — the on-disk creates collect every entry of a
  populated label before writing (single-key: diskOps and memberOps, badgerstore_index.go; composite:
  createCompositePropertyIndexOnDiskLocked, ~120 B a posting), and a drop collects every key of a definition
  (entityIndexDropOpsLocked). Chunking the merge needs care: the member scan's propTxGen retry assumes one merge
  under the lock. Rare (definitions are created at startup); scope when a consumer creates on a big store.

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

- **RELDISK phase 2** (door, minor; ON HOLD 29 Oct, ai-soc and sigma) — a composite rel index and
  Rels().ByTypeAndProperties. Design sent 29 Oct; sigma accepted the shape (reads: TxAt plus the whole valid window,
  unpaged, like ByTypeAndProperty; no prefix reads), then both asked to hold: nothing reads a multi-key rel probe
  today (sigma's doors read one selective single-key index and filter; ai-soc declares single-key rel indexes only).
  The NETWORK (device, local_port) ask was ai-soc's own (28 Oct, RELDISK) and predates sigma's SRCMASK door. Resume
  only if ai-soc's Flux run shows a lazy edge source with many candidates per kept row and real read volume; sigma
  then asks with the numbers.
- **FABLEBUILD** (René's yes in chat, 31 Oct): a one-off trial with Fable as builder on the next state-restore item, from a brief naming the host's call path. Done when the trial is reported. Badger claims in its work are checked against source or a measurement (René ruled Fable out for badger, 24 Oct). Waits for the next state-restore item, and for the masked sample from ai-soc when that item changes a reopen read.

- **BRANCHES** (René via ai-soc, 29 Oct: "clean this also up") — step 1 done: origin/archive/unmerged-2026-10-27
  now at 93c43a64 (was 6554d8ad; tree unchanged, one commit with the 5 codex tips added as parents). Each tip
  verified with `git merge-base --is-ancestor`. Step 2, René 29 Oct ("delete the temporary and old parts"):
  ancestry rechecked; my `git push origin --delete` of the seven was refused by the session's permission check,
  so René runs it (command in the chat). Scratch done: ~/cleanup.sh's rho section ran 15:17 (12G -> 64M);
  probe/, consumers/ and mut/mut.py kept. Table:

  | branch | tip | commits not in main | archived |
  |---|---|---|---|
  | codex/compact-reverse-buffered-prefix-20261012 | f0e7a5a0 | 24 | yes |
  | codex/disk-entity-identity-20261011 | 11a11064 | 20 | yes |
  | codex/legal-native-endpoint-presence-20261011 | e3075796 | 11 | yes |
  | codex/pending-overlay-prefix-20261012 | fc57e1bc | 21 | yes |
  | codex/scalable-legal-indexes-20261011 | 3aeccb62 | 16 | yes |
  | v5 | 91426024 | 723 | yes |
  | round10 | f5827c3b | 0 | yes |

  The untracked `warm` file (10 B, 21 Oct) stays: René's own; ai-soc asked to move it, listed for René instead.

- **TEMPIDX** (ai-soc 29 Oct, SCALE; analysis, no work plan yet) — BA k10282355 on v4.93.0: live heap 1,691 ->
  2,600 MB from 3,000 to 7,500 s. Answered from code (no Flux read): the RAM property-index frames (AddKey,
  addOrdered, IDPostings sortedChunks add/insert) leave RAM per listed label (KEYDISK) and composite (COMPDISK);
  the TemporalIndex is one envelope a node (~110-130 B: Entries, subMax, byID, posByID), resident at runtime
  (TemporalIndexOnDisk only speeds up the open rebuild), freed per cold shard on tiered; decodePropertyWireShared
  is decoded nodes held (entity caches, dirty entries, caller); valueDict bounded by each dictionary's Cap; badger
  builder bursts bounded only by NumCompactors x (8 + splits) builders x 2-3 table sizes per DB (BaseTableSize and
  TableSizeMultiplier not exposed). Asked ai-soc for pprof -peek callers of sortedChunks add/insert and of
  decodePropertyWireShared, the temporal-indexed labels with node counts, badger vs tiered. Candidates once the
  data is in: fold posByID into byID (~-40 B a node), a disk form for the temporal index. ai-soc's callers (tiered):
  the postings are Derived's (KEYDISK and COMPDISK take them); the decoded values are rels held by each open
  shard's rel cache (prefetchRel -> LoadCleanAt; 10,000 a cache, no byte budget): told to set CacheBudgetBytes.
  No builder door (ai-soc fixes the one-cut day, COVHOLD). Waiting: temporal-indexed labels and counts next run.
- **ASOFREAD** (SCALE, sigma 27 Oct, BenchmarkStateRefOwnersRewritten at 500 cuts: 5 -> 40 ms a cut, 70 % of the CPU in
  StatePage.LoadWritten -> NodeEffectiveTimelines -> badger NodeHistoryTemporalMeta, which decodes every version of a
  node rewritten every cut). rho's record door `NodeAsOf` walks history newest-first only down to the pin (500
  versions: 325 ns at the newest, 4.7 us one back; NodeHistoryTemporalMeta 166 us at any pin). sigma (27 Oct):
  state units take NodeAsOf (sigma's side, no rho work, no batch door yet); paged rows need the state door. The
  head-pin cache (core/effective_head.go) answers a head pin in O(segments) but an Update drops its entry (headValid: token,
  current version and temporal block change), so a read after each rewrite folds the whole history. Candidate
  (core work): carry the entry across an Update when the current row was its only believed row. sigma (27 Oct):
  units are a pure Update chain (closeOpenRun) but read through NodeAsOf now; rows mix in-place closes (Update),
  SetNodeVersionInterval corrections and ExciseNodeValidity, and are rarely rewritten (BO: 3.5 % of 350,057 read
  back). Not to be built until sigma's 500-cut re-measure after REWRITEBACK shows the slope and NodeEffectiveTimelines' share.
  Done when: sigma's per-cut time stays flat over 500 cuts. sigma (27 Oct, REWRITEBACK step 0 on v4.89.4): single-pin
  reads moved to the singular NodeEffectiveTimeline (head cache): unit benches flat (4.7 -> 33 ms becomes 4.2 -> 4.5).
  Equal answers of singular, plural and page door are now in the head oracle (effective_head_test.go checkManyDoors).
  HEADPLURAL (asked, no hurry): the plural door could read and fill the cache for pins at or above the entry's maxTx
  (plain reads); the page door reads through a store snapshot, and the cache is checked against the live history
  token, so it needs a snapshot-consistent token first. Cold reads still fold (0.7 us a version); sigma sends a
  restart bench before asking for ASOFREAD. Gap seen: no test has a current row with a delete stamp after the pin
  (nodeRowAtTx on the current row in the plural fold survives mutation; core never writes such a row today).
- **CLEANOPEN** (SCALE, ai-soc 27 Oct) — a clean reopen of a reduced BA store spends 11-31 s in tiered.New, almost all
  in the reference shard, and grows from open 1 to open 2 of the same copy (k10271440, 4.11 GB: 16.7 -> 30.8 s;
  k10270301 11.4 -> 21.9 s; k10270846 17.5 -> 26.0 s). It is 7-10 % of the time to serve; the rest is sigma's restore
  and backlog. CPU profiles from the start of each open exist on Flux (RAID fast-archive-20261027, fluxkit-1027o/c/j,
  runs/reopen-ba-*/cpu-start-<open>-<nn>.pprof; function names only, profile 01 possibly missing). Waits for: ai-soc's
  RESTART item, which René placed after goal 2, and René's word before rho reads anything on Flux.
  sigma's RESTART (27 Oct, design in sigma's docs/architecture.md "Restart without a rebuild"): answered that stamps
  from rho's clock are unique and strictly increasing in reservation order across reopen and crash, on every backend.
  The exceptions: concurrent writers, caller instants (*WithTx, backfill, import, replica), two graphs, and a lost or
  unwritten reservation after a crash. Later asks: open time flat as the store grows, and an as-of-tx page door for a
  crash restart.
- **OPENIMAGE** (SCALE, ai-soc's crash reopen of the k10262212 store, 5.67 GB, ran 22+ min in tiered.New, 53 % in
  loadNodeFromBadger) — step (1) shipped in v4.88.0 (a crash open applies the older image and decodes only the
  entities touched since it), step (2) in v4.89.0 (`Config.OpenImageInterval`: a periodic image, so a store that
  never closes cleanly reopens from the last one; at 1 M nodes / 1 M rels crash reopen 2.7 s against 18.0 s for the
  full rebuild). Waits for ai-soc's crash reopen measured on copies of k10262212 (step 1 from the reopen step of its
  reduced BA run k10270301, step 2 with an interval set). Done when: that reopen is bounded by the writes since the
  last image.
- **PROPSTR** (SCALE, ai-soc 27 Oct, BO run k10270113: an on-disk string equality lookup walked every longer string it
  prefixes) — shipped in v4.88.1 (the walk is bounded by the snapshot's smallest and largest node row IDs; one
  lookup beside 10,000 longer strings visits 2 keys). ai-soc's BO run k10270452-p4b on v4.89.0 still stalled (37 of
  55 files in 30 min, RAM index 480 s): 99.9 % inside badger's Seek, which skips deleted entries and old versions
  until two live keys (PROPSEEK, shipped in v4.89.1: AllVersions, 325x -> 0.9-1.2x at 20,000 deleted entries). An
  update wrote every unchanged entry again, a version per update that each lookup visits (PROPREWRITE, shipped in
  v4.89.2: only changed entries are written; sigma's state units keep ov_k and ov_h<mask> across versions). Done when: ai-soc's BO run with PropertyIndexOnDisk finishes at the RAM index's time.
- **SCALEPROFILE** (SCALE, ai-soc 26 Oct) — which RAM structures grow with data in ai-soc's daemon plus sigma.
  Flux run k10261904 on v4.86.0 is the baseline (heap, memstats and smaps_rollup every 600 s and after the final
  GC, reopen times, the top-40 inuse_space with node and rel counts); the run after the PAGEREF pin carries
  v4.87.0 (OPENWALK) and shows the difference. Done when: each per-entity structure in the profile is bounded or
  queued as its own item.
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
  `tombstone-writer-v4.63` (`37a1ff56`, in `archive/unmerged-2026-10-27`) on the v4.66 axis code, ~½ day, heads-up to sigma and ai-soc (v4.64.0 was
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
- **COLDSTART** — A fresh agent has to be able to work from the 13 docs alone. Done when: every gap it finds is fixed
  in the file that owns it.
- **GUARDS** — The rules the docs no longer state need tests that fail when the rule is broken. Done when: each guard
  fails on a planted violation.
- **SUITE** — About 5,000 lines of memory and badger tests are near copies of each other. Done when: they run as the
  shared store suite.

## Parked

- **PUSHSLOW** (ai-soc 29 Oct; René: RAM first, performance later) — BA push 13,747 s on rho v4.97.1 (k10291501)
  against 9,004 s on the pre-COMPDISK rho (k10282355): sigma's Advance 1.60 -> 2.44 s per cut with less CPU; badger
  DB.get 6.1 -> 10.4 s and tiered GetNode 1.9 -> 5.5 s per 60 s profile. Ruled out from code: write-path reads,
  composite over-selection (tuple keys exact), the ID+value doors (decline on tiered both nights). Candidates:
  ai-soc's KEYDISK point reads against rho's GetNode, badger L0 stalls from the extra composite writes, entity-cache
  fill. Done when: the cause is measured and fixed or handed to its owner. Waits for: René's word and ai-soc's
  -peek callers.

- **CACHE** — Every open badger shard keeps its own memtable arena, block cache and 4 compactors: 88 MB of heap and
  18 goroutines per writable shard (rho probe, 26 Oct; read-only 3.3 MB). ai-soc's k10260011 heap (26 Oct): 30.0 GB
  in use, sigma 26.6 GB (89 %), rho 3.3 GB (1.4 GB not via sigma), badger and ristretto 0.77 GB, so a store-wide
  budget or read-only warm shards would save under 1 GB. Parked by ai-soc's ranking; reopen on a consumer whose
  badger share is large.
- **REOPEN** — ai-soc saw a slow history read after a reopen. Waits for: ai-soc's profile naming the slow call.
- **PENDING** — Reading one entity walks the whole write buffer on badger (half built on branch
  `row102-pending-ranged`, `f4b27842` in `archive/unmerged-2026-10-27`). Waits for: ai-soc raising its priority, or nothing else open.
- **RELSIDECAR** — Relationship lookups by several properties may be too slow on BA. Waits for: a BA measurement.
- **NITS** — Small findings no consumer has hit (backlog 80-82, review nits). Waits for: a consumer hitting one.
- **NOCOMPRESS** — Compression cannot be turned off, because "none" is the default value. Waits for: a consumer asking.
- **STALECITE** — Some code comments cite todo sections that no longer exist. Waits for: the next change to that code.
- **V5** — The v5 line is paused on `v5-takeover` (`3bd4d13a`, in `archive/unmerged-2026-10-27`; state: HANDOVER; open WPs and the breaking API-pass
  candidates: `git show 791c1aa3:tasks/todo.md`). Default: paused; `v5.0.0-alpha.1` at its S2 exit once restarted.
  Waits for: sigma's and ai-soc's use of it (asked 24 Oct), then decided on that data.
- **BACKLOG** — 26 numbered items without a requester, listed in `HANDOVER.md` because code cites their numbers. Waits
  for: a consumer hitting one, or René.
- **BADGERPANIC** — badger panics in its own goroutines (table open, compaction) when a table block's trailer is
  damaged: it parses the trailer before it verifies the block's checksum (`table/table.go:575-604`), so rho cannot
  recover and the process dies (evidence: `tasks/evidence/VERIFY/trailer_test.go.txt`). René (24 Oct): use badger as
  it is, no fork. rho recovers the panics it can (its own read paths and the open scan). Waits for: a badger release
  that verifies before parsing, or René.
