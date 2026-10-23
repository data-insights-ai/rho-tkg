# badger in rho (badger v4.9.2)

rho's disk stores are `github.com/dgraph-io/badger/v4` v4.9.2: `badger.Store` is one badger database, `tiered.Store`
and `sharded.Store` run one per shard or slot. This file is how badger stores and reads data, what it costs, and the
rules rho follows when it uses it. Facts are read from badger's source at that version (file:line in the module
directory, `go list -m -f '{{.Dir}}' github.com/dgraph-io/badger/v4`); measured numbers come from the experiments in
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
  NNNNNN.vlog    value log, mmapped at 2 x ValueLogFileSize while open (sparse, 2 GB), truncated at Close
  DISCARD        value-log discard statistics, 1 MB
```

- **An LSM tree.** A write goes to the active memtable (a skiplist in one preallocated arena) and its WAL. A full
  memtable (skiplist or WAL at `MemTableSize`) becomes immutable and a background flusher writes it as an L0 table.
  Compactors merge tables down levels L1..L6.
- **Values live in the LSM, not the value log.** A value of at least `ValueThreshold` goes to the value log and the LSM
  holds a pointer; smaller values are inline (`structs.go:147-162`). The default and maximum threshold is 1 MB
  (`options.go:201`), so every rho row under 1 MB is inline (open-image chunks are 512 KB). rho's default limits allow
  a row of up to about 64 MB (1,000 properties of 64 KB, `core.go:795-797`): a row of 1 MB or more goes to the value
  log, and one above `maxBatchSize` (about 10 MB) cannot be written at all (section 2).
- **Versions.** Every key carries a commit timestamp. A read sees the newest version at or below its read timestamp.
- **A cleanly closed directory** holds `MANIFEST`, `KEYREGISTRY`, `DISCARD`, the `.sst` files and a tiny `.vlog`: no
  `.mem` (Close flushes the memtable) and no `LOCK`. A `.mem` file means an unclean close; the next open replays it. A
  leftover `LOCK` file is only a stale pid file: whether the directory is open is told by its flock, not by the file.

## 2. Writes and durability

- **Transaction limits.** A transaction holds at most `maxBatchSize` = 15% of `MemTableSize` bytes and
  `maxBatchCount` = `maxBatchSize` / 96 (one skiplist node) entries (`db.go:136-137`): 10,066,329 bytes and 104,857
  entries at defaults. The count starts at 1 for the commit marker (`txn.go` newTransaction) and `Set` returns
  `ErrTxnTooBig` at `count >= maxBatchCount` (`txn.go:335-344`), so one transaction carries at most 104,855 entries. A
  single entry above the size limit cannot be committed at all.
- **A `WriteBatch` is not atomic.** On `ErrTxnTooBig` it commits the transaction it has so far and starts a new one
  (`batch.go:137-205`); each part commits on its own. Measured: 105,857 entries, the batch cancelled before `Flush`,
  104,855 of them visible afterwards: exactly the first full transaction.
- **The commit pipeline.** One transaction is one write request, stored contiguously (`db.go:888-912`). Requests queue
  on one channel; `doWrites` collects the requests waiting and hands them as one group to `writeRequests`, one group at
  a time; it writes the value log (values above the threshold only), then each request's entries into the memtable and
  its WAL (`db.go` doWrites, writeRequests, writeToLSM; `memtable.go` Put). Conflict detection is off in rho
  (`WithDetectConflicts(false)`).
- **Stalls.** A full memtable is queued for flushing and a new one allocated; with `NumMemtables` (5) waiting, writers
  poll every 10 ms (`db.go` ensureRoomForWrite). With 15 L0 tables (`NumLevelZeroTablesStall`) the flusher waits for
  compaction (`levels.go:1588-1597`).
- **Durability.** `SyncWrites` msyncs the active WAL once per write request (`writeToLSM`, `db.go:826-827`) and, through
  a deferred sync in `vlog.write`, the current value-log file once per write group, even when it received nothing.
  Measured on NVMe, one request per group: 630-774 us per synced request, of which 444 us is the WAL msync and 53 us the
  msync of the empty value log (11.5 us unsynced).
  `db.Sync()` (`db.go:673`) syncs only the ACTIVE WAL and the value log: rows in a retired memtable that is not yet
  flushed are not covered, so without `SyncWrites` a power loss can lose them even after `Sync`. L0 tables are msynced
  when written (`table.go:258`) and the manifest is fsynced per change.
- **Deletes.** `Delete` writes a tombstone. `DropPrefix` / `DropAll` block every write (a concurrent write gets
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
  measured 7 B per row (21-23 B with four tables). `Seek` drops the current item and allocates a new one with fresh
  buffers (`iterator.go:726-760`): measured 2,315 B per row for one `Seek` per row, between a walk and a `Get`.
- **`Prefix` prunes tables** by their smallest and biggest keys (`iterator.go:332-376`); without it the iterator merges
  every table of every level. The bloom filter is used only by key iterators.
- **`PrefetchValues`** (default true, 100 items) starts one goroutine per item to copy its value (`iterator.go:696-715`).
  With inline values that is pure overhead: measured 2x the time of a walk without it.
- **Old versions.** A non-`AllVersions` iterator still walks every stored version of a key and skips the older ones;
  versions stay until compaction drops them (section 4).
- **Blocks.** The block cache (ristretto, `BlockCacheSize`) holds decompressed blocks. A miss reads the block from the
  mmapped table and Snappy-decodes it into a fresh buffer (`table.go:527-568`, `785-806`). Table indexes and bloom
  filters are read from the mmapped table (page cache, zero-copy, `table.go:455-458`) unless encryption is on, which
  requires `IndexCacheSize`. Block checksums are checked only under `ChecksumVerificationMode` `OnBlockRead` /
  `OnTableAndBlockRead`.

## 4. Compaction and space

- **Levels** are sized dynamically from the last level down by `LevelSizeMultiplier` (10); the levels above the last
  hold about 11% of it. L0 tables are memtable-sized (64 MB), base-level tables 2 MB, doubling per level below
  (`levels.go:371-438`).
- **Compactors** (`NumCompactors`, 4) tick every 50 ms; L0 is compacted at 5 tables, other levels when their size
  exceeds the target (`levels.go:440-625`).
- **What compaction drops** (`levels.go:650-760`): versions newer than the oldest open read transaction stay. At or
  below it, only the newest `NumVersionsToKeep` (1) version stays; a tombstone is dropped only when no lower level
  overlaps it. So a rewritten row keeps its old versions in lower levels until a compaction merges them, and every
  point `Get` copies each of them; a long-held read transaction pins every version newer than its read timestamp.

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
- **Read-only** opens replay the WALs read-only, allocate no memtable, run no compactors, and refuse a missing directory.

## 6. Options

| Option | Default | rho | Verdict |
|---|---|---|---|
| `MemTableSize` | 64 MB | `Config.MemTableSize` (0 = default) | Sets the arena (~75 MB per writable DB) and the batch limit. |
| `NumMemtables` | 5 | default | Up to 5 arenas under write load. |
| `BlockCacheSize` | 256 MB | `Config.BlockCacheSize` (0 = default) | Per DB, required with Snappy; resizable at runtime (`CacheMaxCost`). |
| `IndexCacheSize` | 0 | `Config.IndexCacheSize` | Required with encryption only. |
| `ValueThreshold` | 1 MB | default | Rows are inline; the value log is unused. |
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
   crash keeps a prefix of it. rho's flush (`badgerstore_flush.go`) writes rows, then counters, then change-log records
   and the LSN watermark in one batch and calls that atomic: above about 10 MB or 105,000 entries a crash can keep
   rows without their change-log records. Gap: FLUSHPART.
2. **Read by walking, not by point gets, where IDs come in order.** A `Get` costs about 4x the row and a read
   transaction; a forward walk costs almost nothing. The read snapshot walks; entity point reads on a cache miss,
   history version and anchor reads, and committed-stamp reads still `Get`. Gap: GETWALK.
3. **No `PrefetchValues`.** Values are inline, so prefetching only adds a goroutine per row. 13 iterators set it,
   including the full node scan at open when there is no open image. Gap: PREFETCH.
4. **Every iterator gets the narrowest `Prefix`**, at least its keyspace byte, so other keyspaces' tables are not
   merged. The read-snapshot and scan-snapshot walkers have none. Gap: ITERPREFIX.
5. **Open read-only when no write follows.** A writable open costs 83 MB and 6 more goroutines than a read-only one;
   cold shards open writable for reads. Gap: SHARD step 4a.
6. **Badger creates a missing directory; rho must refuse it** for a shard it already knows. Gap: SHARD step 1.
7. **Hold read transactions briefly.** A read transaction pins versions from compaction and the value log; release it
   before user callbacks (the read snapshot does, per chunk).
8. **Size caches per shard role.** Defaults give every open DB a 256 MB block cache and a 75 MB arena. Gap: CACHE.
9. **The value-log GC loop has work only for rows of 1 MB or more** (`badgerstore_flush.go` gcLoop); without such rows
   it finds nothing. Gap: VLOGGC.
10. **Batch synced writes.** Under `SyncWrites` every request costs a WAL msync and every write group a value-log msync;
    rho's group commit writes one request per group, which is right.
11. **Store-wide operations pause writes.** `DropPrefix` / `DropAll` refuse concurrent writes; rho calls them only in
    `Clear`, under its own locks.

## 8. Read at the API level only

rho uses none of these, so they were read as API, not in depth: discard statistics and value-log GC internals,
ristretto's internals, the publisher (`Subscribe`), `Sequence`, `MergeOperator`, managed mode (`OpenManaged`),
`Stream` / `StreamWriter` / `Backup` / `Load`, `Flatten`, `EstimateSize`, `Ranges`.
