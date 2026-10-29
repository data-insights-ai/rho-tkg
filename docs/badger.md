# badger in rho (badger v4.9.2)

rho's disk stores are `github.com/dgraph-io/badger/v4` v4.9.2: `badger.Store` is one badger database, `tiered.Store`
and `sharded.Store` run one per shard or slot. This file is how badger stores and reads data, what it costs, the
rules rho follows when it uses it, its complete API (section 8) and what rho should integrate (section 9). Facts are
read from badger's source at that version (file:line in the module directory,
`go list -m -f '{{.Dir}}' github.com/dgraph-io/badger/v4`); measured numbers come from the experiments in
`tasks/evidence/BADGER/` (raw badger, default options, NVMe). Re-check this file when the badger version changes.

## 1. How badger stores data

```
Dir/
  MANIFEST       the set of tables per level; every change appended and fsynced (manifest.go addChanges)
  KEYREGISTRY    data keys, encrypted with the master key when encryption is on (key_registry.go)
  LOCK           pid file of a writable open, removed at Close; the lock itself is a flock on the directory
                 (exclusive for writable, shared for read-only opens; dir_unix.go acquireDirectoryLock)
  NNNNN.mem      memtable write-ahead log, mmapped at 2 x MemTableSize (sparse, 128 MB at defaults)
  NNNNNN.sst     tables of the LSM tree, mmapped
  NNNNNN.vlog    value log, mmapped at 2 x ValueLogFileSize while open (sparse, 2 GB), truncated at Close to its write offset (`value.go:627-641`)
  DISCARD        value-log discard statistics, 1 MB
```

- **An LSM tree.** A write goes to the active memtable (a skiplist in one preallocated arena) and its WAL. A full
  memtable (skiplist or WAL at `MemTableSize`) becomes immutable and a background flusher writes it as an L0 table.
  Compactors merge tables down levels L1..L6 (`memtable.go:179-185` WAL first, then the skiplist; `db.go:1068-1102`).
- **Values live in the LSM, not the value log.** A value of at least `ValueThreshold` goes to the value log and the LSM
  holds a pointer; smaller values are inline (`structs.go:147-162`). The default and maximum threshold is 1 MB
  (`options.go:201`), so every rho row under 1 MB is inline (open-image chunks are 512 KB). rho's default limits allow
  a row of up to about 64 MB (1,000 properties of 64 KB, `core.go:795-797`): a row of 1 MB or more goes to the value
  log. A single row fits one transaction's size limit, but badger refuses a value above `ValueLogFileSize` and, in
  memory, one at or above `ValueThreshold` (section 2, "One entry and its limits").
- **Versions.** Every key carries a commit timestamp (`y/y.go:113-121`). A read sees the newest version at or below its
  read timestamp (`iterator.go:629`).
- **A cleanly closed directory** holds `MANIFEST`, `KEYREGISTRY`, `DISCARD`, the `.sst` files and a tiny `.vlog`: no
  `.mem` (Close flushes the memtable) and no `LOCK`. A `.mem` file means an unclean close; the next open replays it into
  a full memtable arena (`db.go:325`, `memtable.go:99`), read-only or not. A writable open cuts the WAL after the last
  complete transaction (an entry of an unfinished transaction is dropped with it, `memtable.go:481-519`), silently. A read-only open refuses EVERY unclean WAL, torn or not, because a WAL file is
  preallocated and always ends short of its size after a crash ("end offset 57913 < size 16777216",
  `memtable.go:209-212`; measured, `tasks/evidence/BADGER/read_test.go.txt` R3); the error wraps `ErrTruncateNeeded`
  with `%+v`, so `errors.Is` fails on it (section 8.7). After any crash a shard needs one writable open before it
  can be opened read-only. A leftover `LOCK` file is only a stale pid file: whether the directory is open is told by its
  flock on the directory, exclusive when writable, shared when read-only; only a writable open writes the pid file
  (`dir_unix.go:35-63`).

## 2. Writes and durability

- **Transaction limits.** A transaction holds at most `maxBatchSize` = 15% of `MemTableSize` bytes and
  `maxBatchCount` = `maxBatchSize` / 96 (one skiplist node) entries (`db.go:136-137`): 10,066,329 bytes and 104,857
  entries at defaults. The count starts at 1 for the commit marker (`txn.go` newTransaction) and `Set` returns
  `ErrTxnTooBig` at `count >= maxBatchCount` (`txn.go:335-344`), so one transaction carries at most `maxBatchCount` - 2 = 104,855 entries.
  An entry's size is `len(key) + len(value) + 12` for a value under `ValueThreshold` and `len(key) + 24` for one at or
  above it (value-log pointer), starting from 21 bytes (`structs.go:147-157`, `txn.go:338`, `txn.go:770-771`).
  Measured (`tasks/evidence/BADGER/batch_test.go.txt`, B1-B2): 13,105 empty entries or 1,232 entries of 1,000 bytes
  per transaction at `MemTableSize` 8 MB.
- **One entry and its limits.** badger refuses to open when `ValueThreshold` > `maxBatchSize` (`db.go:151-157`), so an
  inline value is smaller than the size limit and a value-log value costs its key + 24 bytes, whatever its size. At
  rho's smallest `MemTableSize` (8 MB: limit 1,258,291 bytes) the margin over the largest inline value is 209,715
  bytes. Measured (B3, B4): a 20 MB value and a 1 MB - 1 inline value each commit in one transaction at 8 MB; at 4 MB
  the open fails. So the size accounting never rejects one entry. But `Txn.modify` refuses outright a key above
  65,000 bytes, a value above `ValueLogFileSize`, and in memory a value at or above `ValueThreshold`
  (`txn.go:352-371`; measured B10: 1 MB + 1 refused at `ValueLogFileSize` 1 MB and in memory). rho allows property
  values of 65,536 bytes and puts a string value verbatim into an on-disk property index key. Since v4.75.0 every
  write door refuses, before any mutation, an entry above these limits less the staging overhead (key 64,991 bytes,
  value `ValueLogFileSize` - 1, in memory 1 MB - 1) with `ErrEntryTooLarge` (`badgerstore_entry_size.go`); before, it
  was acknowledged and then failed every flush, and nothing written after it persisted (KEYSIZE, VALUESIZE).
- **A `WriteBatch` is not atomic.** On `ErrTxnTooBig` it commits the transaction it has so far (`CommitWith`,
  asynchronous, up to 16 in flight: `SetMaxPendingTxns`) and retries the entry in a new one (`batch.go:137-207`); each
  part commits on its own. Parts enter the WAL in the order they were committed: `commitAndSend` assigns the commit
  timestamp and queues the request under one lock (`txn.go:521-593`). The first failed part's error is kept and no
  later part is committed after it is seen (`batch.go:79-89`, `:193`); `Flush` commits the last part and waits for all
  (`:211-230`); `Cancel` discards only the open part, parts handed off still commit (`:62-77`). Measured: 105,857
  entries cancelled before `Flush` leave 104,855 visible (the first full transaction); with 8 MB memtables (B5, B6) a
  batch of 45,867 entries is 4 parts whose versions rise in insertion order, and a cancel after 32,762 keeps exactly
  the 2 committed parts. A process killed while a batch commits (B9, a child process, 10 runs with and without
  `SyncWrites`) always reopens with whole parts in insertion order: a prefix.
- **Small transaction facts.** A transaction that read nothing never conflicts, even with `DetectConflicts` on; one
  that read a key written meanwhile does (`txn.go:128`, B7). A commit or `Flush` without writes writes nothing and
  leaves `MaxVersion` (`CommitWith`, `txn.go:705-712`, B8). A key set twice in one transaction is written once, the
  last value (B8).
- **The commit pipeline.** One transaction is one write request, stored contiguously (`db.go:888-912`). Requests queue
  on one channel; `doWrites` collects the requests waiting and hands them as one group to `writeRequests`, one group at
  a time; it writes the value log (values above the threshold only), then each request's entries into the memtable and
  its WAL (`db.go` doWrites, writeRequests, writeToLSM; `memtable.go` Put). Conflict detection is off in rho
  (`WithDetectConflicts(false)`).
- **Stalls.** A full memtable is queued for flushing and a new one allocated; with `NumMemtables` (5) waiting, writers
  poll every 10 ms (`db.go` ensureRoomForWrite). With 15 L0 tables (`NumLevelZeroTablesStall`) the flusher waits for
  compaction (`levels.go:1588-1597`).
- **A split batch after a crash.** Parts commit in order, one write request each. After a process crash the page cache
  keeps every written page and a crash keeps a prefix of the parts (measured, B9). After a power loss that holds only
  under `SyncWrites` (each request msynced before the next, `db.go:826-828`): without it pages reach the disk in
  writeback order, each WAL file is replayed and cut on its own in file order (`memtable.go:53-63`), so the result is a
  prefix per WAL file, not of the batch. Each part holds whatever entries the caller added first.
- **Durability.** `SyncWrites` msyncs the active WAL once per write request (`writeToLSM`, `db.go:826-827`) and, through
  a deferred sync in `vlog.write`, the current value-log file once per write group, even when it received nothing.
  Measured on NVMe, one request per group: 630-774 us per synced request, of which 444 us is the WAL msync and 53 us the
  msync of the empty value log (11.5 us unsynced).
  `db.Sync()` (`db.go:673`) syncs only the ACTIVE WAL and the value log: rows in a retired memtable that is not yet
  flushed are not covered, so without `SyncWrites` a power loss can lose them even after `Sync`. L0 tables are msynced
  when written (`table.go:258`) and the manifest is fsynced per change.
- **Deletes.** `Delete` writes a tombstone (`txn.go:427`). `DropPrefix` / `DropAll` block every write (a concurrent write gets
  `ErrBlockedWrites`), flush the memtables, stop compactions and rewrite every table holding the prefix
  (`db.go:1662-1813`): a store-wide pause.

## 3. Reads and their cost

- **A read transaction waits for commits in flight.** `NewTransaction(false)` takes the oracle lock and waits until
  every commit at or below its read timestamp is written (`txn.go:78-95`).
- **A point `Get` probes every level** (`db.go` get, `levels.go:1611-1646`). It stops at the first match only when that
  version equals the read timestamp, i.e. the key was written by the newest commit (the read timestamp is the newest
  commit's, `txn.go:78-95`). In a level without a version of the key the cost is a bloom-filter check; in each table
  that holds one it is a table iterator, a seek and a copy of the value (`level_handler.go:266-294`); `Txn.Get` copies
  it again into the item and `Item.Value` copies it a third time (`txn.go:434-488`, `iterator.go:139-153`). Measured,
  900-byte row, warm block cache: 3,667 B per `Get` inside one read transaction (one table), 4,581 B with versions in
  four tables, 3,795 B with its own read transaction per `Get` (rho's cold point-read pattern).
- **A forward walk is nearly free.** `Iterator.Next` reuses the previous item and its buffers (`iterator.go` Next);
  measured 7 B per row (21-23 B with four tables). `Seek` returns the prefetched items to the reuse list but not the
  current one (`prefetch` sets it to nil, `iterator.go:726-760`), so each `Seek` allocates one item with fresh buffers,
  on top of re-seeking every table iterator: measured 2,315 B per row for one `Seek` per row, between a walk and a
  `Get`. Profiled per seek: ~1,000 B key and value copied into the new item (`SafeCopy`), ~1,000 B its value buffer
  (`Slice.Resize`), ~180 B the item, ~110 B table-block positioning.
- **`Prefix` prunes tables** by their smallest and biggest keys (`iterator.go:332-376`); without it the iterator merges
  every table of every level. Bloom filters are checked by a point `Get` per table (`level_handler.go:270-273`) and by a
  key iterator (`iterator.go:348`, `:409`), not by a prefix iterator. (An earlier version of this file said "only by
  key iterators": false.)
- **`PrefetchValues`** (default true, 100 items) starts one goroutine per item to copy its value (`iterator.go:696-715`).
  With inline values that is pure overhead: measured 2x the time of a walk without it.
- **Old versions.** A non-`AllVersions` iterator still walks every stored version of a key and skips the older ones
  (`iterator.go:620-660`); versions stay until compaction drops them (section 4).
- **Blocks.** The block cache (ristretto, `BlockCacheSize`) holds decompressed blocks. A miss reads the block from the
  mmapped table and Snappy-decodes it into a fresh buffer (`table.go:527-568`, `785-806`). Table indexes and bloom
  filters are read from the mmapped table (page cache, zero-copy, `table.go:455-458`) unless encryption is on, which
  requires `IndexCacheSize`. Block checksums are checked only under `ChecksumVerificationMode` `OnBlockRead` /
  `OnTableAndBlockRead` (`table.go:603`; at table open under `OnTableRead` / `OnTableAndBlockRead`, `table.go:301`);
  rho sets `OnBlockRead` and `VerifyValueChecksum` (v4.75.2). Badger parses a block's trailer (its entry count) before
  it verifies the checksum (`table.go:575-604`): a damaged trailer panics, also with checksums on. rho recovers that
  panic on its own reads; in badger's goroutines (table open, compaction) it ends the process (BADGERPANIC).

## 4. Compaction and space

- **Levels** are sized dynamically from the last level down by `LevelSizeMultiplier` (10); the levels above the last
  hold about 11% of it. L0 tables are memtable-sized (64 MB), base-level tables 2 MB, doubling per level below
  (`levels.go:371-438`).
- **Compactors** (`NumCompactors`, 4) tick every 50 ms; L0 is compacted at 5 tables, other levels when their size
  exceeds the target (`levels.go:440-625`). So a quiet DB keeps up to four L0 tables uncompacted for good, with the
  old versions in them; `Flatten` does nothing while one level holds all data (`db.go:1611-1636`; measured, R2).
- **What compaction drops** (`levels.go:650-760`; measured, R2: 3 versions kept while an older read transaction was
  open, 1 after it closed): versions newer than the oldest open read transaction stay. At or
  below it, only the newest `NumVersionsToKeep` (1) version stays; a tombstone is dropped only when no lower level
  overlaps it. So a rewritten row keeps its old versions in lower levels until a compaction merges them, and every
  point `Get` copies each of them; compaction keeps every version newer than the oldest open read transaction's timestamp plus the newest one at or
  below it (`levels.go:660`), so a long-held read transaction keeps every later version of the keys it could read.

## 5. Lifecycle

- **Open** (`db.go:160-388`): a missing directory is created unless `ReadOnly` (`createDirs`, `db.go:1974-1992`; no
  option forbids it); the directory is flocked (exclusive when writable, shared when read-only, so a writable open
  fails while any read-only handle on the directory is open, even in the same process); the manifest replayed; every
  table opened (3 at a time, mmap, key range; the index checksum is always verified, `table.go:447-449`; block data
  only with `ChecksumVerificationMode` `OnTableRead`); every `.mem` WAL replayed; a writable open allocates a new
  memtable arena. Measured: a writable open adds 83 MB of heap and 16 goroutines (among them 4 compactors with 50 ms
  tickers, the memtable flusher, the write loop, value-log GC, the publisher and the caches' own); a read-only open adds
  2 MB and 10 goroutines.
- **Close** (`db.go:531-660`): blocks writes, flushes the current memtable to an L0 table, waits for a running
  compaction, closes the value log and tables, removes `LOCK`, syncs the directories.
- **Read-only** opens allocate no new memtable and run no compactors, so a read-only shard never compacts; they still
  replay every leftover `.mem` into a full arena (a cleanly closed directory has none: the 2 MB measured), refuse a torn
  WAL, and refuse a missing directory. On a read-only mount only a read-only open works.
- **No integrity check of data.** With `ChecksumVerificationMode` off, a changed byte in a table's data block reaches
  the reader undetected (the table index alone is checksummed at open); rho's decoder fails closed only on a malformed
  row, and only `VerifyShard` sees a silently changed value.
- **Mappings.** Every `.sst`, `.mem` and `.vlog` is a separate mmap; a 1 GB shard of 2 MB base tables is about 500,
  well under Linux's default `vm.max_map_count`.

## 6. Options

| Option | Default | rho | Verdict |
|---|---|---|---|
| `MemTableSize` | 64 MB | `Config.MemTableSize` (0 = default) | Sets the arena (`MemTableSize` + `maxBatchSize` + `maxBatchCount` x 96, `db.go:1043-1045`: ~84 MB at defaults, measured +83 MB per writable DB) and the batch limit. |
| `NumMemtables` | 5 | default | Up to 5 arenas under write load. |
| `BlockCacheSize` | 256 MB | `Config.BlockCacheSize` (0 = default) | Per DB, required with Snappy; resizable at runtime (`CacheMaxCost`). |
| `IndexCacheSize` | 0 | `Config.IndexCacheSize` | Required with encryption only. |
| `ValueThreshold` | 1 MB | default | Rows under 1 MB are inline; a row of 1 MB or more (rho allows ~64 MB) goes to the value log. |
| `Compression` | Snappy | `Config.Compression` | `options.None` is the zero value, so rho cannot turn it off. |
| `SyncWrites` | false | `Config.SyncWrites` | Two msyncs per write request (section 2). |
| `NumVersionsToKeep` | 1 | default | Right for rho (history is rho's own keyspace). |
| `DetectConflicts` | true | false | Right: rho serialises its own writes. |
| `NumCompactors` | 4 | `Config.NumCompactors` | 4 goroutines with 50 ms tickers per writable DB. |
| `CompactL0OnClose` | false | default | Close leaves L0 as it is. |
| `ChecksumVerificationMode` | none | default | Block data is not verified; table indexes are verified at open regardless. |
| `EncryptionKey`, `...RotationDuration` | off, 10 days | `Config.EncryptionKey*` | Needs both caches (validated by rho). |
| `ReadOnly` | false | `Config.ReadOnly` | The cheap way to read a shard (section 5). |
| `BypassLockGuard` | false | default | Must stay false. |

## 7. Rules for rho

Each rule says what rho does; a gap is a `tasks/todo.md` item.

1. **A flush is atomic or self-consistent per part.** A `WriteBatch` above the limits is several transactions, so a
   crash keeps a prefix of it. rho's flush (`badgerstore_flush.go:296-355`) adds its write buffer's ops in map order
   (`pending` is a map, `badgerstore.go:549`), then the counters, then the change-log records and `LastLSNKey`, and its
   comments call that atomic. A crash after the first part keeps a random subset of the flush's rows and keys (a
   current row without its history row, an index key without its row), the old counters, and none of the change-log
   records: replicas and the feed miss those mutations for good (LSNs are not reused, `seedLogLSN` takes the highest
   record, `badgerstore_changelog.go:346-368`). An undercount heals at open; a flush that removed rows (purge, erasure)
   reopens with counters above the live rows and the store refuses to open (`reconcilePersistedCounter`,
   `badgerstore.go:1947-1966`). A torn entity is not detected at open. Trigger: a flush above about 10 MB of entries
   (about 6,000-10,000 rows of 1 KB with their index keys) or 104,855 entries: bulk imports, large batches, purge
   chunks, fast ingest without `SyncWrites`. Closed by FLUSHPART (v4.75.0): a flush that fits commits as one `Txn`,
   a larger one is staged under `0x0E` and committed by a marker that `New` re-applies
   (`badgerstore_flush_staged.go`, `docs/persistence.md` § Durability). The index backfills
   (`badgerstore_property_disk.go`, `badgerstore_temporal_disk.go`) stay one `WriteBatch` with their built marker
   last: a crash keeps a prefix without the marker, and the backfill runs again.
2. **Read by walking, not by point gets, where IDs come in order.** A `Get` costs about 4x the row and a read
   transaction; a forward walk costs almost nothing. The read snapshot walks; entity point reads on a cache miss,
   history version and anchor reads, and committed-stamp reads still `Get`. Gap: GETWALK.
3. **No `PrefetchValues`.** Values are inline, so prefetching only adds a goroutine per row (`iterator.go:707-713`).
   No iterator of rho's turns it on since v4.78.4 (PREFETCH: 15 did, the full node scan at open without an open image
   among them; that open is 7-18 % faster without, `tasks/evidence/PREFETCH/`); `TestNoIteratorPrefetchesValues`
   guards it.
4. **Every iterator gets the narrowest `Prefix`**, at least its keyspace byte, so other keyspaces' tables are not
   merged. 49 of rho's 71 iterators set none, the read-snapshot and scan-snapshot walkers included
   (`tasks/evidence/USAGE/usage-map.txt`). Gap: ITERPREFIX.
5. **Open read-only when no write follows.** A writable open of a cleanly closed directory costs 83 MB and 6 more
   goroutines than a read-only one; cold shards open writable for reads. A read-only handle must be closed before a
   writable open of the same directory (the flock), and a shard left with `.mem` files needs one writable open to
   recover before it opens read-only. Gap: SHARD step 4a.
6. **Badger creates a missing directory; rho must refuse it** for a shard it already knows. Gap: SHARD step 1.
7. **Hold read transactions briefly.** A read transaction pins versions from compaction and the value log; release it
   before user callbacks (the read snapshot does, per chunk).
8. **Size caches per shard role.** Defaults give every open DB a 256 MB block cache and a 75 MB arena. Gap: CACHE.
9. **The value-log GC loop has work only for rows of 1 MB or more** (`badgerstore_flush.go` gcLoop); without such rows
   a pass costs 28 ns. It reclaims a dead value only after compaction has dropped it (discard statistics): measured
   114 MB to 32 MB once deletes were compacted, nothing before; a value kept by history is never garbage
   (`tasks/evidence/VLOGGC/`, VLOGGC: measured, no change).
10. **Batch synced writes.** Under `SyncWrites` every request costs a WAL msync and every write group a value-log msync;
    rho's group commit writes one request per group, which is right, unless the flush is split (rule 1): each part is
    a request of its own.
11. **Store-wide operations pause writes, not reads.** `DropPrefix` / `DropAll` refuse concurrent writes; rho calls
    only `DropPrefix`, only in `Clear`, under its own locks, while cache-miss reads still run
    (`badgerstore_node.go:132-143`). `DropAll` would delete the value-log files under such a read, whose value then
    comes back empty; `DropPrefix` leaves them to the value-log GC (DROPALL, v4.78.1).
12. **A walk with its own end bound sees every version.** Without `AllVersions` an iterator skips older versions and
    deleted entries inside `Seek` and `Next` (`parseItem`, `iterator.go:653-656`, `:668-671`) until it holds the next
    live key and prefetches one more (`prefetch`, `:726-743`); only `IteratorOptions.Prefix` stops that skip (`:735`,
    `:581`), a loop's own upper bound never does. A per-call walk therefore pays for every deleted entry and old
    version between its start and the second live key after it, wherever that is. The on-disk equality lookup
    iterates with `AllVersions`, takes each key's newest version and stops at its own bound (PROPSEEK, v4.89.1: 200
    lookups of absent values before 20,000 deleted entries took 325x their time without them, now 0.9-1.2x,
    `TestPropSeekLookupDoesNotWalkDeletedEntries`). It still visits each version of the keys inside its bound, so an
    update writes only the entries whose value changed (PROPREWRITE, v4.89.2: 20 updates with an unchanged value
    leave the one version the create wrote, before 21). A seek to the first or last live key of a keyspace skips
    the same way, so the lookup keeps the node ID bounds of its string walk in memory instead of seeking both ends of
    the node keyspace per call (PROPBOUNDS, v4.89.3: 200 lookups beside 19,999 deleted node rows took 3,150x their
    time without them, now 0.7-0.8x, `TestPropBoundsLookupDoesNotWalkDeletedRows`). A walk bounded by a key prefix sets
    that prefix as `IteratorOptions.Prefix` (ITERTOMB, v4.97.1): 200 on-disk adjacency lookups beside 20,000 deleted
    entries after the node's took 1,066x their time without it, the label-index scan 655x, the property prefix scan
    301x; now 0.4-1.4x (`TestIterTomb*`). The property range walks end at a value, which `Prefix` cannot express,
    so they take PROPSEEK's `AllVersions` shape (ITERTOMB, unreleased: 424x and 442x, now 0.7-0.9x); the history
    walks of truncation, trim, compaction, purge and erasure set their prefix (594x, 787x and 543x, now 0.9-1.0x).
    Gap: the retention purge's adjacency walk per purged node and the erasure and repair walks (ITERTOMB in
    tasks/todo.md).

## 8. The complete API

Every exported symbol of badger v4.9.2, read from `go doc -all` of each package and, where the doc comment is thin
or misleading, from the source (file:line). A row without a source line restates badger's doc comment for a feature
rho does not use; everything rho uses is cited or measured. "rho" says where rho uses it (rho imports only `badger` and
`badger/options`); "-" means unused. Section 9 lists what rho should integrate.

### 8.1 Opening, closing, sizing (`DB`)

| Symbol | What it does | rho |
|---|---|---|
| `Open(Options)` | Opens or creates the DB; creates missing `Dir`/`ValueDir` on a writable open; takes the directory flock (section 5). Panics if compression or encryption is on and `BlockCacheSize` is 0 (`db.go:170-172`). | `buildBadgerOptions` + `Open` in `New`, `MigrateOversizedWAL`, `DiscardStagedFlush` |
| `OpenManaged(Options)` | Open in managed mode: the caller supplies every read and commit timestamp. | - |
| `DefaultOptions(dir)` / `LSMOnlyOptions(dir)` | Defaults (table in 8.6); LSM-only sets `ValueThreshold` to its maximum (1 MB, the same as the default in v4.9.2). | `DefaultOptions` |
| `(Options).FromSuperFlag(s)` | Parses `"key=value; ..."` into the fields it names. | - |
| `Close()` | Pushes a non-empty memtable to L0 (`db.go:561-573`), stops compactors, closes files; idempotent. With `CompactL0OnClose` it then compacts L0 (`db.go:597`). | `Store.Close` |
| `IsClosed()` | Whether `Close` ran. | - (rho keeps `dbClosed`) |
| `Opts()` | A copy of the options. | - |
| `Sync()` | msyncs the **active** memtable's WAL and the value log only (`db.go:673`); a rotated memtable's WAL is not synced (`ensureRoomForWrite`, `db.go:1015-1041`). | `DurableFlush`, durable meta writes |
| `Size()` | LSM and value-log bytes from expvar counters that a goroutine refreshes **once a minute** by walking the directory (`db.go:1148-1262`); always 0 when `MetricsEnabled` is false. | - (rho reports sizes itself) |
| `EstimateSize(prefix)` | On-disk and uncompressed bytes of the tables whose **both** ends carry the prefix (`db.go:1394`): table-granular, misses data in tables that straddle the prefix and everything in memtables. | - |
| `Levels()`, `LevelsToString()`, `Tables()` | Per-level and per-table info (`LevelInfo`, `TableInfo`: key count, on-disk, stale and uncompressed size, max version, index and bloom size). | - |
| `Ranges(prefix, n)` | Rough key ranges from the last level's tables, to split an iteration. | - |
| `MaxVersion()` | The highest version held by a memtable or a table (`db.go:415-434`; a table records only the keys it holds, `table/builder.go:212-213`). It can FALL: compaction that drops the store's newest version, a tombstone with no overlap below, lowers it (measured R5: 4 to 2). The commit marker never counts: it is written to the WAL only (`memtable.go:187-190`). | open-image trust rule: sound, since the header is a live key and every flush after it writes live counters; a periodic image's since version (read under the flush lock): every later commit, also after a reopen, is above the header, which is above since and live |
| `MaxBatchCount()`, `MaxBatchSize()` | The per-transaction limits (section 2). | - (section 9) |
| `BlockCacheMetrics()`, `IndexCacheMetrics()` | ristretto metrics of the two caches. | - |
| `CacheMaxCost(cache, cost)` | Resizes the block or index cache at runtime; a negative cost reads it (`db.go:2042`). | - (section 9) |
| `PrintHistogram(prefix)` | Prints a key/value size histogram to the logger. | - |
| `VerifyChecksum()` | Reads and verifies every block of every table (`levels.go:1750-1765`, `table/table.go:702-713`); not memtables, not the value log. With `ChecksumVerificationMode` none nothing else verifies blocks. | - (section 9) |
| `BanNamespace(ns)`, `BannedNamespaces()` | With `NamespaceOffset` >= 0: refuse reads and writes of keys in a namespace (`ErrBannedKey`). | - |

### 8.2 Transactions (`Txn`) and entries

| Symbol | What it does | rho |
|---|---|---|
| `NewTransaction(update)` | A transaction at the current read timestamp; must be `Discard`ed. Not safe for concurrent use. | flush path A (`commitFlushEntries`) |
| `NewTransactionAt(readTs, update)` | Managed mode. | - |
| `Update(fn)`, `View(fn)` | Run `fn` in a read-write / read-only transaction and commit / discard it. | 15 / 16 call sites |
| `Txn.Get(key)` | Point read, `ErrKeyNotFound` if absent; probes memtables then levels with bloom filters (section 3). | everywhere |
| `Txn.Set`, `SetEntry`, `Delete` | Buffer a write; return `ErrTxnTooBig` when the transaction would pass `MaxBatchCount`/`MaxBatchSize` (section 2). Keep a reference to the slices until commit. | flush, meta writes |
| `Txn.Commit()` | Conflict check (a transaction that read nothing never conflicts, `txn.go:128`; with `DetectConflicts` false nothing is tracked), then one write request: one WAL record set ending in a finish entry, so replay applies all or none of it (`memtable.go:433-530`). | flush path A, `Update` |
| `Txn.CommitWith(cb)` | `Commit` that returns at once and runs `cb` after the write. | - |
| `Txn.CommitAt(ts, cb)` | Managed mode. | - |
| `Txn.Discard()` | Releases the transaction; idempotent; required. Panics while an iterator of the transaction is open (`txn.go:512-513`); after `db.Close` it can block, since the read mark is sent to a stopped watermark's 100-slot channel (`y/watermark.go:60`, `:77-79`). | deferred; the read snapshot, the scan snapshot and the rel-type build walk skip it once the store is closing or closed (`badgerstore_read_snapshot.go:126`, `badgerstore_scan_snapshot.go:133`, `badgerstore_reltype_temporal_index.go:303`; DISCARDCLOSE, v4.78.1) |
| `Txn.ReadTs()` | The read timestamp. | - |
| `Txn.NewIterator(opt)` | Iterator over the snapshot (8.3). | 71 call sites |
| `Txn.NewKeyIterator(key, opt)` | All versions of one key; uses the bloom filter on the exact key to skip tables (`iterator.go:496-504`, `:348`). | - (section 9) |
| `NewEntry(k, v)`, `WithMeta`, `WithTTL`, `WithDiscard` | An entry with a user-meta byte, an expiry, or "older versions may be dropped". | `NewEntry` only |
| `Entry` fields | `Key`, `Value`, `ExpiresAt`, `UserMeta`. | - |

### 8.3 Iterators and items

| Symbol | What it does | rho |
|---|---|---|
| `IteratorOptions` | `PrefetchValues`/`PrefetchSize` (a goroutine per item, section 3), `Reverse`, `AllVersions`, `Prefix` (skips tables outside it), `SinceTs` (skips tables whose max version is below it and versions at or below it, `iterator.go:334-361`, `:629`), `InternalAccess`. | `Prefix` rarely set (todo ITERPREFIX), prefetch in 13 places (todo PREFETCH) |
| `DefaultIteratorOptions` | Prefetch on, 100 items. | 59 call sites |
| `Iterator.Rewind`, `Seek`, `Next`, `Valid`, `ValidForPrefix`, `Item`, `Close` | Cursor over keys in order; `Item` is valid until `Next`. `ThreadId` and `Alloc` serve `Stream`. | everywhere |
| `Item.Key`, `KeyCopy` | The key (valid until `Next`) or a copy. | 26 `KeyCopy` |
| `Item.Value(fn)`, `ValueCopy` | The value inside the transaction, or a copy. **A value-log read error is logged and swallowed: the value comes back empty with a nil error** (`iterator.go:158-187`); measured after `DropAll` (`tasks/evidence/USAGE/`). Inline values cannot fail this way. | everywhere |
| `Item.Version()` | The commit timestamp. | open image |
| `Item.EstimatedSize()` | Key + inline value, or the value-log record length for a value-log value (`iterator.go:216`): no value read. | - (section 9) |
| `Item.KeySize()`, `ValueSize()` | The key's length without its timestamp (`iterator.go:230-232`; badger's comment says + 8, the code does not add it); the value's length (approximate for value-log values, `iterator.go:238`). | `ValueSize` once |
| `Item.UserMeta()`, `ExpiresAt()`, `IsDeletedOrExpired()`, `DiscardEarlierVersions()`, `String()` | Entry metadata. | - |

### 8.4 Batches, bulk load and copy

| Symbol | What it does | rho |
|---|---|---|
| `NewWriteBatch()` | Blind writes packed into transactions; above the limits it commits and starts another: **not atomic** (section 2). `Set`, `SetEntry`, `Delete`, `Flush`, `Cancel` (must be called if `Flush` may not be; safe after `Flush`, its finish runs once, `y/y.go:237-251`), `Error`, `SetMaxPendingTxns` (default 16), `Write(buf)`, `WriteList(kvList)`. | staged flush steps 1, 3, 5; index backfills; open image; Clear |
| `NewWriteBatchAt`, `NewManagedWriteBatch`, `SetEntryAt`, `DeleteAt` | Managed mode. | - |
| `NewKVLoader(maxPending)`, `KVLoader.Set`, `Finish` | Writes `pb.KV`s (a backup) in batches. | - |
| `Backup(w, since)` / `Stream.Backup` | Protobuf dump of every entry with version >= `since` (`backup.go:54-77`); returns the last version, so `last+1` makes the next dump incremental. | - (section 9) |
| `Load(r, maxPending)` | Restores a `Backup` into a DB that runs nothing else. | - |
| `NewStream()`, `NewStreamAt(ts)` | Concurrent snapshot iteration (`NumGo`, default 8) over key ranges, `ChooseKey`, `KeyToList` / `KeyToListWithThreadId` + `FinishThread`, `Send` (serial), `Prefix`, `SinceTs`, `MaxSize`; keys are NOT in order. `Orchestrate(ctx)` runs it, `ToList` is the default `KeyToList`, `SendDoneMarkers`. | - |
| `NewStreamWriter()` | Writes sorted, non-overlapping streams straight into SSTables, no WAL and no compaction: `Prepare` (deletes all existing data through `dropAll`, `stream_writer.go:65`), `PrepareIncremental`, `Write(buf)`, `Flush` (syncs directories), `Cancel`. Only for bootstrapping a DB. | - |
| `StreamDB(outOptions)` | Copies a snapshot of every key into a new DB at `outOptions.Dir`: `OpenManaged` + `StreamWriter.Prepare` + `NewStreamAt(MaxUint64)` + `Flush` (`db.go:1996-2024`). The copy is compacted SSTables only (no WAL, no value log). | - (section 9) |
| `Flatten(workers)` | Compacts all tables onto one level (after a restore). | - |
| `DropAll()` | Deletes every table and value-log file; blocks writes (`ErrBlockedWrites`); **concurrent reads may panic** (doc comment). | - (`Clear` of a never-logged store until v4.78.1; DROPALL) |
| `DropPrefix(prefixes...)` | Flushes memtables skipping the prefixes, compacts them away; blocks writes meanwhile. | `Clear` paths |
| `BufferToKVList`, `KVToBuffer` | Convert between `z.Buffer` and `pb.KVList` (Stream framing). | - |

### 8.5 Other services

| Symbol | What it does | rho |
|---|---|---|
| `Subscribe(ctx, cb, matches)` | Calls `cb` with batches of written KVs that match `pb.Match` prefixes; published right after the write reaches the LSM, before the writer's `Commit` returns (`db.go:880`), held in memory only (channel of 1,000 lists per subscriber, `publisher.go:112`): lost on a crash, no replay. | - (cannot replace rho's change log) |
| `GetSequence(key, bandwidth)`, `Sequence.Next`, `Release` | Monotonic integers leased in blocks; panics in managed mode and on a read-only DB (`db.go:1358-1362`). | - (rho has snowflake ids and its LSN allocator) |
| `GetMergeOperator(key, f, dur)`, `MergeOperator.Add`, `Get`, `Stop` | Appends values merged by `MergeFunc` in a background goroutine. | - |
| `RunValueLogGC(ratio)` | Rewrites a value-log file when a sample shows `ratio` of it reclaimable; `ErrNoRewrite`, `ErrRejected`; `ErrGCInMemoryMode`, `ErrGCInReadOnlyMode`, and `ErrInvalidRequest` outside (0, 1) (`db.go:1234-1246`). | `gcLoop` (todo VLOGGC: only rows of 1 MB or more reach the value log) |
| `SetDiscardTs(ts)` | Managed mode: versions at or below `ts` may be dropped. | - |
| `KeyRegistry`, `OpenKeyRegistry`, `WriteKeyRegistry`, `KeyRegistryOptions`, `KeyRegistryFileName` | The encrypted data-key file (`KEYREGISTRY`); `WriteKeyRegistry` rewrites it, the way to change the master key offline. | - (section 9) |
| `Manifest`, `ReplayManifestFile`, `TableManifest`, `ManifestFilename` | The `MANIFEST` file (table set per level) and its parser. | `ManifestFilename` (`DiscardStagedFlush`) |
| `InitDiscardStats` | Value-log discard statistics file. | - |
| `Logger`, `DEBUG`...`ERROR`, `WithLoggingLevel` | Logging interface and the default logger's level. | `Config.Logger` (nil = silent) |
| `KVList` (= `pb.KVList`), `CacheType` (`BlockCache`, `IndexCache`), `LevelInfo`, `TableInfo` | Types of the calls above. | - |
| `ValueThresholdLimit` | 65,520: dead in v4.9.2, read by nothing; the effective maximum is `maxValueThreshold`, 1 MB (`options.go:201`, `db.go:140`). | - |

### 8.6 Options (all fields)

Section 6 judges the ones rho sets; these are all of them, with the v4.9.2 default (`options.go:125-175`).

| Field | Default | Field | Default |
|---|---|---|---|
| `Dir`, `ValueDir` | the path | `SyncWrites` | false |
| `NumVersionsToKeep` | 1 | `ReadOnly` | false |
| `Logger` | stderr, INFO | `Compression` | Snappy |
| `InMemory` | false | `MetricsEnabled` | true (expvar counters per directory) |
| `NumGoroutines` (Stream) | 8 | `MemTableSize` | 64 MB |
| `BaseTableSize` | 2 MB | `BaseLevelSize` | 10 MB |
| `LevelSizeMultiplier` | 10 | `TableSizeMultiplier` | 2 |
| `MaxLevels` | 7 | `VLogPercentile` | 0 |
| `ValueThreshold` | 1 MB | `NumMemtables` | 5 |
| `BlockSize` | 4 KB | `BloomFalsePositive` | 0.01 |
| `BlockCacheSize` | 256 MB | `IndexCacheSize` | 0: without encryption the index parsed from the mmapped table at open is used (`table.go:502-505`); the cache serves encryption only |
| `NumLevelZeroTables` / `...Stall` | 5 / 15 | `ValueLogFileSize` | 1 GB - 1 |
| `ValueLogMaxEntries` | 1,000,000 | `NumCompactors` | 4 |
| `CompactL0OnClose` | false (forced off read-only) | `LmaxCompaction` | false |
| `ZSTDCompressionLevel` | 1 | `VerifyValueChecksum` | false |
| `EncryptionKey` / `...RotationDuration` | none / 10 days | `BypassLockGuard` | false |
| `ChecksumVerificationMode` | `NoVerification` (`OnTableRead`, `OnBlockRead`, `OnTableAndBlockRead`) | `DetectConflicts` | true |
| `NamespaceOffset` | -1 (off) | `ExternalMagicVersion` | 0 (checked against the MANIFEST at open, `manifest.go:122-158`) |

### 8.7 Errors

`ErrKeyNotFound`, `ErrTxnTooBig`, `ErrConflict` (impossible with `DetectConflicts` false), `ErrReadOnlyTxn`,
`ErrDiscardedTxn`, `ErrEmptyKey`, `ErrInvalidKey` (the reserved `!badger!` prefix), `ErrBannedKey`,
`ErrThresholdZero`, `ErrNoRewrite`, `ErrRejected`, `ErrInvalidRequest`, `ErrManagedTxn`, `ErrNamespaceMode`,
`ErrInvalidDump`, `ErrZeroBandwidth`, `ErrWindowsNotSupported` / `ErrPlan9NotSupported` (read-only opens),
`ErrTruncateNeeded` (a torn log on a read-only open; rho also wraps it for a pending staged flush), `ErrBlockedWrites`
(during `DropAll`/`DropPrefix`/`Close`), `ErrNilCallback`, `ErrEncryptionKeyMismatch`, `ErrInvalidDataKeyID`,
`ErrInvalidEncryptionKey`, `ErrGCInMemoryMode`, `ErrGCInReadOnlyMode`, `ErrDBClosed`, `ErrValueLogSize`. rho tests
`ErrKeyNotFound`, `ErrTxnTooBig`, `ErrTruncateNeeded` and `ErrDBClosed` by identity (`ErrEncryptionKeyMismatch` is
named in a comment, not matched); the rest surface wrapped. Outside badger's debug mode `y.Wrap` formats the cause with `%+v` (`y/error.go:55-63`), so
`errors.Is` fails on errors badger wrapped itself; rho's tiered probe also matches `ErrTruncateNeeded` by message.

### 8.8 Packages beside `badger`

`options` (`CompressionType`: `None`, `Snappy`, `ZSTD`; `ChecksumVerificationMode`) is used by rho's configs. `pb`
holds the protobuf types of backups, streams and the manifest (`KV`, `KVList`, `Match`, `DataKey`, `Checksum`,
`ManifestChange`, `ManifestChangeSet`). `y` (shared helpers, `y.Wrapf` formats with `%+v`, so `errors.Is` may fail
on errors badger wrapped, section 8.7), `table` (SSTable builder and
reader), `skl` (the memtable skiplist and arena), `trie` (prefix matching for `Subscribe`) and `fb` (flatbuffers of
table indexes) are badger's internals: exported, not meant for applications, unused by rho.

## 9. Integration findings

Badger features rho does not use yet that fit a rho need, and one risk. Each is a todo item; none is built.

1. **Moving shards between volumes (SHARD)**: `StreamDB` (or `Stream` + `StreamWriter`) copies a snapshot into a
   new directory as compacted SSTables, without a WAL; `ToList` keeps the newest version and drops deleted keys
   (right for rho, one version per key). A byte copy of a closed directory instead keeps the open image, the key
   registry and the compression state and needs no second DB; the mover chooses between them.
2. **Limits from badger, not from formulas**: `MaxBatchSize()` / `MaxBatchCount()` give the exact per-transaction
   limits; rho needs them only for the number of entries (one entry always fits, section 2), e.g. to size a staged
   flush's parts.
3. **Per-shard caches (CACHE)**: `CacheMaxCost` resizes an open DB's block cache, so the tiered store can share one
   budget across its open shards; `BlockCacheMetrics` shows the hit rate.
4. **Point reads (GETWALK)**: a forward walk over sorted IDs replaces per-ID `Get`s; `NewKeyIterator` has no bloom
   advantage over `Get` (both check the filter per table, section 3).
5. **Iterator pruning (ITERPREFIX, PREFETCH)**: `IteratorOptions.Prefix` skips tables outside the prefix;
   `PrefetchValues` false avoids a goroutine per item.
6. **Admin views**: `Tables()`, `Levels()`, `EstimateSize(prefix)` (table-granular), `Item.EstimatedSize()` (no
   value read) and `VerifyChecksum()` (every block of every table, beyond the blocks reads touch) can back size and
   integrity reports per shard.
7. **Backups**: `Backup(w, since)` with the returned version makes incremental dumps; `Load` restores them. rho
   has no backup door; a store-level one would be thin.
8. **Changed since a version (WRITTENAT)**: `IteratorOptions.SinceTs` skips whole tables older than a badger
   version. Badger versions are commit timestamps, not rho transaction times, so it serves "changed since a
   badger version", not "written at a rho tx".
9. **Master-key change**: `WriteKeyRegistry` rewrites the key registry under a new master key offline; rho has no
   tool for it.
10. **Fixed (DROPALL, v4.78.1)**: `Clear` of a store that never had the change log called `DropAll` (badger:
    concurrent reads may panic, `db.go:1699-1700`) while a cache-miss point read or a read snapshot may hold a badger
    transaction outside `idxMu`/`flushMu`. Measured (`tasks/evidence/USAGE/`): a value-log value (a row of 1 MB or
    more) looked up before `DropAll` came back empty with a nil error, because badger swallows value-log read errors
    (`iterator.go:158-187`). Every `Clear` now wipes by `DropPrefix`, which leaves the value-log files to the GC
    (`TestClearUnderHeldReadKeepsTheValue`), and an empty value fails as `ErrCorruptWire` naming a failed value-log
    read; badger's log, in `slog` since v4.75.2, names the cause. The space of the wiped values returns with the
    value-log GC, not at once.
11. **Per-shard defaults (CACHE, SHARD)**: every writable open starts the writer, the flusher, 4 compactors with
    50 ms tickers and a directory walk every minute (`db.go:303-387`, `:458`, not guarded by `MetricsEnabled`);
    `CompactL0OnClose` was measured for warm and cold shards: about 5 % faster read-only reads for 70-100 ms more per
    close, not adopted (`tasks/evidence/COMPACTL0/`);
    `BloomFalsePositive` below 0.01 may pay on the reference shard, whose unique checks read absent keys.
12. **Done (OPENIMAGE, v4.88.0): reopen after a crash.** An open whose image header is older than the store's newest
    version iterates the versions above it (`IteratorOptions.SinceTs`: versions > SinceTs, `iterator.go:320`, `:629`;
    a table whose `MaxVersion` is below it is skipped, `:334`, `:361`; memtables are always read) with `AllVersions`
    (deletes too) to find the touched entities, and decodes only their rows (`docs/persistence.md` "Clean reopen").
    A table compaction mixed with newer keys is still read key by key, so the scan is bounded by the tables holding
    versions newer than the image, not by the touched count.
13. **To measure**: `MetricsEnabled` (default, rho leaves it on) registers expvar counters per directory; with many
    shards opened over a process's life they accumulate. `Subscribe` is not durable and cannot replace the change
    log. `ExternalMagicVersion` could guard the format at badger level beside rho's `wire_format_version`; changing
    it is a one-way door.
