# Compact historical reverse postings: feasibility prototype

Base: `11a1106426e779a9b6f926e3b4c687a63af5e934`.
Branch: `codex/compact-historical-reverse-prototype-20261012`.
No production code is changed. The test-only codec does not activate a new
format, migrate a directory, omit properties or change source/integrity hashes.

## Encoding

Keep every forward posting exactly as it is:
`0x0c | kind:u8 | token:u16 | optional digest:32B | owner:u64 | tx:u64`.
The optional digest is the existing SHA256 of the exact property/value pair.

Proposed auxiliary reverse v2 key:
`0x0e | kind:u8 | owner:u64 | token:u16 | optional digest:32B | tx:u64`.
Value: four-byte big-endian CRC32C over the entire reverse key. An alternate
32-byte SHA256 value is measured too. At this base commit, `0x0e` is unused;
`0x0f` is metadata. Prefix availability must be rechecked at integration.

The decoder checks the requested kind and owner before constructing a forward
key, then checks reconstructed shape, kind, owner, nonzero token, positive
signed-range owner and nonnegative signed-range stamp. Thus even an explicitly
checksummed malicious entry cannot redirect one owner's erase to another owner
or a node erase to a relationship. Corruption returns an error before deletion.

CRC32C changes auxiliary corruption detection strength. It is not a source
authenticator; original entity, proposition, source and archive hashes remain
SHA256. Native negative raw-store stamps are already conservatively represented
as zero by the unchanged forward writer, and that exact stored zero is retained.

| Reverse entry | Legacy | CRC32C | SHA256 |
| --- | ---: | ---: | ---: |
| Label/type raw key+value bytes | 62 | 24 | 52 |
| Property raw key+value bytes | 94 | 56 | 84 |

CRC32C saves 38 raw bytes per reverse posting. Including the unchanged forward
posting, raw label/type pairs become 45 bytes instead of 83; property pairs
become 109 instead of 147. No membership values or stamps are coalesced.

## Bounded genuine measurement

Read-only input: closed Erv derivative at
`/private/var/tmp/rho-disk-identity-profiles-20261011/genuine/erv-graph`.
It contains 305,082 current nodes and completed historical membership. The
earlier fixture's recorded whole-current answer digest is
`e31750bab9612c9e4d9428591b9bde585c3a93b82e7d026f28c614079248dbd9`;
this prototype measures reverse entries rather than rerunning that answer scan.

Selection: first 25,000 node-label and first 25,000 node-property reverse
entries in native order per kind. All 50,000 legacy ownership/hash checks and
byte-exact CRC32C/SHA256 round trips pass. Input key/value digest:
`1b6a2cd4c84ce99ebc83a39dffc6ace47ed1b185d2046b977cbcb2025f22a6f5`.
Selection is deliberately bounded and is not a population estimate.

The actual fixed Badger table builder measures sorted keyspace, native MVCC
versions, 4 KiB blocks, 0.01 bloom probability and default auxiliary value
metadata. ZSTD uses level 3, as the legal graph does. This measures table bytes,
including block/index/checksum overhead; it is not a live LSM compaction or
filesystem reclaim measurement. Forward postings, primary/history rows and
operational ledger bytes are excluded.

| 50,000 reverse entries | Legacy | CRC32C | SHA256 |
| --- | ---: | ---: | ---: |
| Raw bytes | 3,900,000 | 2,000,000 | 3,400,000 |
| Snappy SST bytes | 3,309,640 | 1,670,093 | 3,120,289 |
| ZSTD3 SST bytes | 2,991,826 | 1,341,335 | 2,844,052 |

CRC32C reduces this reverse-only ZSTD table by 55.17%; SHA256 reduces it by
4.94%. Both use the same input entries. Relationship kinds have adversarial
codec tests, but this genuine measurement contains node postings only.

Reproduce:

```sh
RHO_REVERSE_GENUINE_FIXTURE=/private/var/tmp/rho-disk-identity-profiles-20261011/genuine/erv-graph \
  go test ./pkg/graph/store/badger -run '^TestPrototypeReverse' -count=1 -v
go test -race ./pkg/graph/store/badger \
  -run '^TestPrototypeReverse(RoundTripAndOwnerIsolation|RefusesMalformedSelectors|WriterFence)$' -count=1
```

## Explicit migration design, not implemented

1. Require explicit offline sole ownership and serialize all writers,
   GraphTx/group-commit windows, Clear and exact-erasure/retention operations.
   Flush accepted pending/flushing work using mandatory SyncWrites. Require
   completed, valid existing forward membership before conversion. Opening a
   format5 store must never silently select this migration.
2. Atomically persist minimum writer fence 6 and a separate auxiliary v2
   migration descriptor identifying `direct_crc32c`, phase `copy`, source
   format1 and an initially empty cursor. Sync it before admitting any new
   mutation. Entity row codec stays4. Format5 engines, including readonly
   engines, deliberately decline format6. Fresh compact directories and
   partially migrated directories need explicitly defined open modes.
3. In bounded legacy `0x0d` pages, validate legacy hash/owner/shape, reconstruct
   the existing forward key and write the idempotent `0x0e` reverse entry.
   Durably flush all additions before publishing the small cursor transaction
   and Sync. A crash before the cursor replays the same page. Keep all legacy
   entries during copying. Normal writer opens must decline incomplete
   migration; only explicit migration resume may continue it.
4. After copy completion is durable, enter cleanup phase. Remove only validated
   legacy reverse entries in bounded pages. Do not delete forward/current/
   history rows. Until cleanup is durably complete, erasure must scan both
   reverse prefixes, validate ownership and deduplicate exact forward deletes.
   Cleanup cursor must acknowledge already durable deletes; replays are safe.
5. Publish auxiliary v2 complete only after cleanup is durable. Keep writer6
   continuously across Clear, logged Clear and LSN-preserving reset. Reset
   drops both reverse prefixes, resets forward progress safely and keeps the
   compact descriptor consistent. Older engine reopen deliberately declines;
   downgrade requires a pre-migration copy, not merely removing a marker.

Integration must audit all mutation/rollback/erase/retention doors, include
`0x0e` in historical posting additions-before-primary flush classification and
post-primary destructive cleanup, preserve pending/flushing precedence, and
reuse bootstrap exclusion. Forward query order, exact chain resolver, arbitrary
property semantics, TX stamps and original accepted pins remain unchanged.

Before adoption, require crash gates for fence/copy/cursor/cleanup/reset,
mixed-prefix corrupt and valid-checksum foreign-pointer erasure tests, public
old/new pinned node and relationship oracle tests across delete/retrospection/
rollback/retention, and full native CI on the intended toolchain. These gates
are not supplied by this feasibility prototype. No canonical publication is
authorized by this evidence.
