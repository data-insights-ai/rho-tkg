# ADR-0011 — Column segments on NVMe for bulk relationship types

Status: ACCEPTED (René, 2026-09-24). S0, S1, S2 and the rho-tkg side of S5 done (2026-09-24, see §6); S2's ai-soc parity gate, the sigma release of S5 and S3 next.
Date: 2026-09-24
Base: v4.36.0 (`6d9b97a`). Every `file:line` below refers to that tree unless it
names another repository.
Depends on: the relationship DocValues/column scan (RC, CP1–CP3), ADR-0008
(retention purge; whole-segment drop), ADR-0009 (history delta encoding; the
anchor precedent).

Every number in this document is marked either **measured** (from the
2026-09-24 data-model analysis or from the scratch sizing prototype described
in §8) or **arithmetic** (derived here, not yet run). Arithmetic numbers are
targets for the gates in §6. They are not results.

---

## Decision (René, 2026-09-24)

- **Accepted.** Steps S0–S7 enter the backlog in the order of §6.
- **Integrity block size is a setting,** not a constant: `IntegrityBlockRows` per declared
  type, default 64, a power of two from 1 to 4,096, written into each segment's footer so a
  reader never assumes it. 1 gives one stored hash per row (exact location of a damaged row,
  ~32 B/row more on disk); 64 locates it to within 64 rows and recomputes the row's hash on
  demand (§4.3).
- **Every step is scale-tested,** not only S3: its gate is measured on the three synthday
  sizes (790 K, 3.15 M, 12.6 M rows) for resident bytes per relationship, on-disk bytes per
  relationship and wall time, and must show no growth beyond the step's stated bound; from S2
  on, ai-soc's parity golden and cross-check agree on all shapes; at S3, S5 and S7 also one
  BA day on Flux (masked twin in git).
- **P7** (compacting today's row records) shrinks to what stays in the row store (nodes,
  undeclared types) and starts only after S2.

## 1. Goal and non-goals

### 1.1 Why

rho-tkg's row model was built for correctness per entity, not for bulk
calculation. For the ai-soc workload, the analysis measured:

| what | measured |
|---|---|
| memory store, bytes per relationship | ~744 B, linear in the day's size |
| one ai-soc HOP relationship (memory store) | ~852 B |
| heap objects per relationship | ~13 |
| badger store as ai-soc configures it (`engine/engine.go:225-237` in ai-soc: defaults, stats on) | 1,043 B/rel resident |
| badger "lean" (stats off, label/adjacency/property indexes on disk, 256 MB cache) | 191 B/rel resident, still growing with the day |

Where the 744 B go (analysis, component sizes):

- `Relationship` struct: 80 B (72 B declared at `pkg/types/relationship.go:29-40`, rounded by the allocator).
- `TemporalMetadata`: 96 B. Ten fields (`pkg/types/temporal.go:28-49`); HOP uses 3 (`ValidFrom`, `ValidTo`, `TxFrom`).
- `RelIntegrity`: 128 B (`pkg/types/integrity.go:50-76`), plus the 64-character hex hash string: 64 B.
- `PropertySlice`: 8 properties × 32 B = 256 B (`pkg/types/propertyslice.go:76-83`, `Value any`), plus the boxed values.
- Store maps: ~110 B. `rels`, `typeIdx`, `outIdx`, `inIdx` (`pkg/graph/store/memory/memorystore.go:60-70`).

Why the disk mode is worse, not better: the badger store keeps a per-relationship
planner-statistics memo in RAM, `relTypeClassContrib` and `relStatsContrib`
(`pkg/graph/store/badger/badgerstore.go:594`, `:622`). It also keeps `relIDs`,
`relRevs` and `typeIdx` (`:416-427`) whatever the disk flags say. In the
analysis heap profile `heapdisk1600k` (measured, the analysis's ~1.6 M-HOP synthetic day),
`addRelPropertyStatsCounts` and `addRelPropertyTypeClassCounts` retain 1.32 GB and
0.80 GB of 8.04 GB in use. Opening a badger store also decodes every relationship
row (`loadIndexesScan`, `pkg/graph/store/badger/badgerstore.go:1255-1312`).

The direction set by René (2026-09-24): customers with up to ~1 TB raw
telemetry per day. Deployments always have a fast 1–2 TB NVMe as warm memory.
BA's biggest day is 22.8 M rows, ~3.5 M HOP edges, ~6.7 M relationships of all
types. At 744 B/rel, one such day already needs ~5 GB (arithmetic) before the
reasoner copies anything. At 1 TB/day, that model does not fit in RAM.

### 1.2 Goal

For relationship types that are **declared bulk** (append-mostly, fixed
property schema, very many rows), resident memory must be **independent of the
number of rows**. It may depend on:

- a fixed memtable budget;
- the number of segments (bounded by compaction);
- the number of distinct values per dictionary column;
- the number of updates and deletes not yet compacted.

It must not depend on the number of appended rows. The rows themselves live in
immutable column segments on NVMe, memory-mapped, so the OS page cache is the
warm cache.

### 1.3 Non-goals

- **The general-purpose row store stays.** Nodes, undeclared relationship types,
  indexes on nodes, the registries, the change-log and the tiered and sharded
  topologies are unchanged. A graph without declarations behaves exactly as today.
- No new query language and no planner inside rho-tkg. rho-tkg still only exposes
  statistics (`docs/query-planners.md:1-18`; `docs/SPEC.md:905`).
- No lossy encoding. Every stored value round-trips bit-exactly, including its Go
  type, because the type tag is part of the hash (§4.3).
- Not a replacement for badger's durability in the first steps. Durability of the
  unsealed tail is the row store's job (§3.1).
- Tiered and sharded integration comes after the memory and badger stores (§6, S7).

---

## 2. Segment format

### 2.1 Shape

- One segment holds rows of **one relationship type**. It is immutable once
  written.
- Rows are sorted by **(start node, end node, valid_from)**. Rows with the same
  relationship ID but different versions are separate rows, keyed `(id, version)`.
- A segment is self-contained. Its node table maps segment-local dense ordinals
  (`uint32`) to `NodeID`s. The relationship's dense ID is its row ordinal. The
  external `RelID` (a snowflake) is a stored column, because callers see it
  (`Relationship.ID()`, `pkg/types/relationship.go:55`).
- The row order serves out-adjacency directly: the rows of start node *s* are one
  contiguous run. In-adjacency is a row permutation (CSR). Point lookup by
  `RelID` is a sorted ID index.
- Pages are **4,096 rows**, the same block size the DocValues zone map already uses
  (`pkg/graph/internal/index/docvalues.go:198`, `zoneBlockSize = 4096`). So a
  skipped page is a skipped scan batch.

### 2.2 Byte layout (little-endian; every section carries a CRC32C)

```
+--------------------------------------------------------------------+
| Header (64 B)                                                      |
|   magic "TKGSEG\x00\x01" | format u16 | flags u16 | typeToken u16  |
|   rowCount u32 | pageRows u16 | nodeCount u32                      |
|   id min/max i64 | valid_from min/max i64 | tx_from min/max i64    |
+--------------------------------------------------------------------+
| Node table: sorted distinct endpoint NodeIDs, delta + FOR per 1024 |
| Out-CSR:    per local start ordinal: row offset (FOR u32)          |
+--------------------------------------------------------------------+
| Column chunks, column-major; each column = pages of 4096 rows      |
|   end        local ordinal; delta inside a start-run, FOR bitpack  |
|   valid_from FOR bitpack (base = page min)                         |
|   valid_to   (valid_to - valid_from) FOR; open-ended bitmap if any |
|   tx_from    zigzag(tx_from - valid_from) FOR                      |
|   rel_id     FOR bitpack                                           |
|   version    FOR (0 bits when constant)                            |
|   <declared property columns>:                                     |
|     string   dictionary code, FOR bitpack; dict in footer          |
|     int64    FOR / delta bitpack                                   |
|     float64  raw or XOR (Gorilla)                                  |
|     bool     bitmap                                                |
|     presence bitmap only if some row lacks the property            |
|   fallback   per-row msgpack blob of properties outside the        |
|              schema (existing wire tags); section absent if empty  |
|   sparse system columns (absent when all rows hold the default):   |
|     tx_to, created_at (explicit only), updated_at, deleted_at,     |
|     created_by, updated_by, base_entity, prev_hash (32 B raw),     |
|     author_id, signature, authorized_by, auth_level,               |
|     endpoint-hash refs (codes into the endpoint-hash dictionary)   |
+--------------------------------------------------------------------+
| In-CSR:   per local end ordinal: offset; row permutation (log2 N b)|
| ID index: sorted rel_id (delta FOR) + row number (log2 N bits)     |
| Integrity: SHA-256 root per 64-row group; segment root             |
| Bloom (optional): (start,end) pairs; rel_id                        |
+--------------------------------------------------------------------+
| Footer                                                             |
|   type name (the hash needs the NAME, not the token, §4.3)        |
|   declared schema: column name, Go kind, encoding                  |
|   dictionaries (sorted, so a value lookup or prefix/range maps to  |
|     a code range)                                                  |
|   endpoint-hash dictionary: (local node, node version) -> 32 B     |
|   per-column stats: count, nulls, exact min/max, HLL sketch,       |
|     type-class counts                                              |
|   page index, one entry per page: first key, min/max of            |
|     valid_from/valid_to/tx_from/rel_id, open-ended flag, and the   |
|     byte offset of every column page (80 B/page at 8 columns)      |
|   section directory + CRCs | footer length u32 | magic             |
+--------------------------------------------------------------------+
```

Encodings are chosen per page and recorded in the page index: constant (0 bits),
FOR bit-pack, or plain. A column whose value is the same on every row of a page
costs nothing on that page. That is how `family`, `asset_class`, `orch`, `version`
and the empty integrity fields disappear on most pages.

Strings: a column whose distinct count in the segment exceeds a threshold (for
example more than 1/16 of the rows) is written as plain offsets plus bytes instead
of a dictionary. This keeps the heap bound in §3.5. ai-soc's `support` list would
have hit it; P6 in ai-soc drops it (see §4.9).

This is a real on-disk format. The existing persisted DocValues blob is
deliberately a rebuild accelerator with no format contract
(`pkg/graph/internal/index/docvalues_codec.go:9-26`). Its own comment says that
if persisted columns ever became authoritative, "all of that inverts"
(`:24`). Segments are authoritative. So they get a versioned magic, fail closed on
an unknown version (the same rule as `ErrWireFormatVersionUnsupported`, see
AGENTS.md "On-disk wire format is versioned"), and follow the trust-boundary rules
of lessons 44, 47 and 48 (`tasks/lessons.md:769`, `:871`, `:926`): no allocation
sized from an untrusted count, checked decode, and hash verification on entry.

### 2.3 Bytes per HOP

HOP as ai-soc writes it today (`engine/materialize.go:311-322` in ai-soc):
endpoints, `scenario`, `family`, `actor`, `asset_class`, `orch`, `t_lo`, `t_hi`,
`support`, and `tkg_valid_from`/`tkg_valid_to`, with `TxFrom` from `AddWithTx`
(`:329`). After ai-soc P6 (`tasks/todo.md:27` in ai-soc), HOP keeps endpoints,
`actor`, `family`, `asset_class`, `orch`, valid from/to and tx from. The table
sizes that shape.

| column | encoding | measured, synthday 12.6 M rows (1.58 M HOP) | BA, arithmetic |
|---|---|---|---|
| start | out-CSR runs | 0.08 | ~0.1 |
| end | local ordinal, delta in run, FOR | 2.00 | ~2–2.5 |
| valid_from | FOR per page | 3.38 | ~3.4 (27 bits cover one day in ms) |
| valid_to | vt − vf, FOR | 1.45 | ~1.5 |
| tx_from | tx − vf, zigzag FOR | 1.23 | ~1.2–3 (depends on ingest lag) |
| rel_id | FOR | 4.88 | ~5–6.4 (IDs minted across a whole day need ~51 bits) |
| actor | dictionary (6,301 values) | 1.70 | ~1.5–2 |
| family | dictionary | 0 (1 value in synthday) | ≤0.4 (≤8 values) |
| asset_class | dictionary | 0 (1 value in synthday) | ~0.13 (2 values) |
| orch | dictionary | 0 (1 value in synthday) | ≤1 (≤256 values) |
| in-CSR | permutation + offsets | 2.78 | ~2.8–3 |
| page index | 80 B / 4096 rows | 0.02 | 0.02 |
| **subtotal** | | **17.51** | **~19–23** |
| ID index | sorted IDs + row numbers | 5.17 (measured at 3.15 M rows: 2.79 + 2.38) | ~5.5 |
| integrity roots | 32 B / 64 rows | — | 0.5 |
| constant system columns | 0 bits per page | — | ~0 |
| bloom (optional) | 10 bits per (start,end) pair | 0.55 | ~0.55 |
| **total on NVMe** | | | **~25–30 B/HOP** |

What the table does and does not show:

- The encoding bytes are **before** general-purpose compression. A page-level zstd
  or Snappy pass could only make them smaller. The comparison point is badger's
  on-disk size, which is already block-Snappy compressed (lesson 67,
  `tasks/lessons.md:2058`). For the 790 K-row synthday, badger held 198,688
  relationships (all types) plus 3,950 nodes in 49.9 MB: ~251 B/rel on disk
  (measured, `du -sb` of the analysis run directory divided by the relationship
  count).
- The measured subtotal grows only with log N: 15.51, 16.50 and 17.51 B/HOP at
  107 K, 408 K and 1.58 M HOP rows (measured). The growth comes from the ID and
  permutation bit widths.
- Synthday has one value each for `family`, `asset_class` and `orch`. BA does not.
  The BA column is therefore arithmetic until S1 runs on a masked BA twin on Flux.

**Resident per HOP once sealed** (arithmetic):

- page index 0.02 B;
- bloom 0.55 B if kept in heap (0 if read from the mapping);
- dictionaries: O(distinct values) per segment, e.g. 6,301 actors ≈ 0.2 MB per
  segment, not per row.

That is **≤0.6 B/HOP**, against 852 B/HOP today. The unsealed tail costs today's
row price, but only up to the fixed memtable budget.

**BA's biggest day** (arithmetic): 3.5 M HOP × ~30 B ≈ 105 MB on NVMe. A 1 TB NVMe
holds ~33 billion HOP rows at that size.

---

## 3. Tiers

### 3.1 Memtable = the row store's unsealed tail

The memtable is **not a new structure**. New rows of a declared type are written by
the existing create doors into the existing row store:

- the create kernel, `pkg/graph/internal/core/relationship_create_kernel.go:12-19` and `:254-322`;
- memory `putRelationshipRouted`, `pkg/graph/store/memory/memorystore_rel.go:35-93`;
- badger `putRelationship`, `pkg/graph/store/badger/badgerstore_rel.go:56-161`.

Every contract of today's write path still applies to them unchanged: locks,
constraints, endpoint hashes, change-log co-commit (lesson 49,
`tasks/lessons.md:970`) and badger async durability.

**Sealing** happens when the declared type's unsealed rows exceed the memtable
budget. Seal is:

1. snapshot the type's unsealed rows (ID set, as in `ForEachRelByType`,
   `pkg/graph/store/memory/memorystore_rel_scan.go:12-16`);
2. sort by (start, end, valid_from), encode, and verify (§4.3);
3. write `seg-<type>-<seq>.tkgs.tmp`, fsync, rename;
4. append the segment to the manifest atomically (write temp, fsync, rename; the
   same all-or-error pattern as `ShardCatalog.Save`,
   `pkg/graph/store/tiered/shard_catalog.go:183`);
5. remove the sealed rows from the row store **without** logging them as deletes
   (§4.8).

Crash recovery: a row present in both a manifest segment and the row store is
removed from the row store at open. Removal is idempotent. A `.tmp` file that
is not in the manifest is deleted.

### 3.2 Warm: sealed segments on NVMe, memory-mapped

- Segments are written to `SegmentDir`, which is the NVMe, and mapped read-only.
- Column pages are read straight from the mapping. Dictionaries are decoded into
  the Go heap when the segment is opened. Strings handed to callers must never
  point into a mapping that can be unmapped.
- A segment is pinned by reference count while it is being read and unmapped only
  at zero. This is the checkout/checkin discipline the tiered store already uses
  for cold shards (AGENTS.md "Checkout/checkin for cold shards").

### 3.3 Cold: RAID

- Segments older than `SegmentColdAfter` are copied to `SegmentColdDir` with
  copy, fsync, manifest swap, then deleting the source.
- Cold segments open lazily, with a cap on how many are open at once. This is the
  same shape as `MaxOpenColdShards` (`pkg/graph/store/tiered/tieredstore.go:55-76`).
- Because segments are immutable, moving one is a file copy.

### 3.4 Compaction

- **Levels.** L0 is one segment per seal. When a type has 8 segments on one level
  in the same time partition, they are merged into one segment on the next level.
  The merge is a k-way merge on (start, end, valid_from). It re-encodes
  dictionaries and folds in the overlay (§4.1, §4.5).
- **Time partitions.** Segments never merge across a partition boundary (default:
  one UTC day of `tx_from`). Retention can then drop whole files (§4.5).
- **Maximum size.** A merge never produces a segment larger than 64 M rows or
  2 GB. This bounds the cost of one rewrite.
- **Memory.** A merge streams page by page. Its RAM is O(fan-in × page), not
  O(rows).
- **Scale** (arithmetic): a 256 MB memtable at today's ~744 B/row seals every
  ~340 K rows (~10 MB segment). L1 is ~2.7 M rows, L2 ~22 M. One BA day per type
  ends at no more than ~24 segments.

### 3.5 How the RAM budget is enforced

- **One budget.** `SegmentMemoryBudget` is the sum of three things:
  - the memtable, counted with `Relationship.ApproxHeapBytes`
    (`pkg/types/heapsize.go:137`, already used to size the badger cache);
  - open-segment metadata (dictionaries, page indexes, blooms);
  - merge buffers.
- **Order of response when it is exceeded.** First seal. If sealing cannot keep
  up, appends block. Badger's `MaxPendingWrites` already works this way: at the
  bound the writer flushes synchronously (AGENTS.md "Configuration").
- **Blooms leave the heap first.** Under pressure they are read from the mapping
  instead of held in heap.
- **Page cache is outside the budget.** Clean file-backed pages are reclaimable.
  Under a cgroup `MemoryMax` (ai-soc's day runs use systemd `MemoryMax`) they are
  still charged to the cgroup. That is open question Q8.

---

## 4. Contracts that must carry over

### 4.1 Bitemporality (valid time and transaction time)

- **Today.** `ValidFrom`, `ValidTo`, `TxFrom` and `TxTo` live on
  `TemporalMetadata` (`pkg/types/temporal.go:28-49`). The effective valid-from falls back to
  the snowflake mint time (`EntityValidFrom`,
  `pkg/graph/internal/storeutil/temporal_filter.go:13`). The point and interval
  predicates are `MatchesPointInTime` (`:82`) and `MatchesInterval` (`:96`).
- **In segments.** `valid_from`, `valid_to` and `tx_from` are columns. `tx_to` is
  a sparse column. The canonical predicates run unchanged on decoded values. There
  is no second definition of them (AGENTS.md "Canonical temporal predicates live in
  storeutil"). Page min/max lets a scan skip pages. The existing DocValues zone map
  (`pkg/graph/internal/index/docvalues.go:176-190`) already does this, including the open-ended trap
  (`zoneOpenEnded`).
- **Updates.** An update of a sealed row never rewrites it. This is lesson 46
  (`tasks/lessons.md:837`).
  - Today's update writes the previous state to history with `TxTo = now`
    (`pkg/graph/internal/core/version_chain.go:391-399`; `pkg/graph/internal/core/relationship_update.go:120`, `:230`).
  - With segments, the new version goes to the memtable like any write. The
    supersession stamp `(id, version, TxTo)` goes to a small **overlay**: a
    per-type map in RAM, persisted in the row store's metadata keyspace.
  - Compaction folds the stamp into the `tx_to` column of the rewritten segment.
    The row then carries exactly what today's history snapshot carries.
  - Lesson 43 still holds: `TxTo` marks supersession, not retraction.
- **As-of reads.** Rows are filtered by `TxFrom <= txAt` and chains are tiled
  exactly as today, over the union of memtable, overlay and segments.

### 4.2 Isolation: immutable segments need no defensive copy

- **Today.**
  - A put deep-copies and freezes the row (`pkg/graph/store/memory/frozen.go:18-22`,
    `pkg/graph/store/memory/memorystore_rel.go:65`).
  - Point reads deep-copy (`pkg/graph/store/memory/memorystore_rel.go:217`).
  - Scans hand out shared frozen pointers (`docs/architecture.md:917-919`). A
    frozen row's `Integrity()` deep-copies on every call
    (`pkg/types/relationship.go:395-403`).
- **With segments.** A reader decodes from immutable bytes. Every
  `*types.Relationship` handed out is freshly built:
  - point reads return it unfrozen (mutable, independent: the contract kept);
  - scans return it frozen (the contract kept).

  No shared mutable state exists, so no copy is needed to protect the store. The
  only lock is the segment pin (§3.2).
- **What changes.** Two scans no longer return the same pointer for the same row.
  The contract only allows aliasing ("may alias"); it never promised it. Scans now
  allocate per row per call, where the memory store used to return cached frozen
  pointers (`pkg/graph/store/memory/memorystore_query.go:195`). Bulk consumers should move to the
  streaming and columnar doors (§5).

### 4.3 Integrity: hash chain

- **Today.**
  - `ComputeRelHash` hashes, in order: id, version, the type **name**, start,
    end, and the sorted properties with their type tags
    (`pkg/graph/internal/integrity/integrity.go:205-220`).
  - Temporal fields, `PrevHash`, endpoint hashes and provenance are **not**
    hashed (`pkg/graph/internal/core/relationship_create_kernel.go:157-159`).
  - The result is stored as a 64-character hex string on `RelIntegrity.Hash`
    (`pkg/graph/internal/integrity/integrity.go:215-219`; `pkg/graph/internal/core/relationship_create_kernel.go:153-169`).
  - `VerifyRelChain` recomputes the hash and compares it with the stored one
    (`pkg/graph/internal/core/integrity.go:402-418`).
- **In segments.** The per-row hash is **not stored**. It is a pure function of
  columns the segment keeps: `rel_id`, `version`, the footer's type name, the
  endpoints, and properties with their exact Go kind. The declared schema records
  that kind. A value whose kind differs from the declared one goes to the fallback
  column with its original wire tag, so the hash bytes are identical. Values
  nested in an `[]any` / `map[string]any` keep their exact kind too (the entity
  wire's nested envelopes); until backlog item 3 was fixed (2026-09-24) the wire
  widened nested small integers and the seal refused such rows — the fallback
  now accepts them.
- **The witness.** If the hash is only recomputed from the columns, verifying it
  against itself proves nothing. So the stored witness is a **SHA-256 root per
  64-row group**, and a segment root in the footer.
  - The roots are built at seal time from the memtable rows' *stored* `Hash`
    values, not from the re-encoded columns.
  - Sealing re-derives every row hash from the encoded columns and refuses to seal
    on any mismatch. This is lesson 44's rule (`tasks/lessons.md:769`) applied to
    our own encoder.
  - `VerifyRelChain` on a sealed row recomputes the 64 row hashes of its group and
    compares them with the stored root. Cost ~64 SHA-256 over ~100 B each
    (arithmetic, ~30 µs). Storage 0.5 B/row, against today's 128 + 64 B.
- **`PrevHash`.** It is empty at create (`pkg/graph/internal/core/relationship_create_kernel.go:162`).
  For version > 1 it is stored as a 32-byte raw sparse column.
- **Endpoint hashes (`tkg_from_hash`/`tkg_to_hash`).** These capture each
  endpoint's hash at write time (`pkg/graph/internal/core/relationship_create_kernel.go:168-169`, refreshed
  from the store reply at `:294-295`). The segment keeps a per-segment dictionary
  keyed by (local node, node version), with the 32-byte hash. Rows reference it
  through the endpoint ordinals, so the cost is 0 bits when no endpoint changed
  during the memtable window. This does not depend on node history surviving
  compaction. That was the other option, rejected.
- **ai-soc `-provenance`.** The check at `engine/cmd/xcheck/main.go:1626` in ai-soc
  (flag `-provenance`, defined at `:67`) counts anchors whose `tkg_hash` is
  non-empty. It does not recompute anything. sigma-tkgd fills the anchor from
  `g.Resolve().RelProperty(r, types.ShadowHash)`
  (`internal/tyla/source/graph/provenance.go:31-47` in sigma-tkgd).
  - That resolver returns `Integrity().Hash` (`pkg/graph/internal/core/shadow.go:225-229`).
  - For a segment row, `Integrity()` is materialized on demand, with the hash
    computed from the columns, hex-encoded identically.
  - The check therefore passes unchanged. The cost is one SHA-256 per anchored
    row, ~1–2 s per 3.5 M rows (arithmetic).
  - The columnar scan (§5.3) offers the hash as an opt-in column, so it is paid
    only when asked for.

### 4.4 History queries

- **Today.**
  - Memory history is `relHistory[id][version]` holding deep copies
    (`pkg/graph/store/memory/memorystore.go:74`; `pkg/graph/store/memory/memorystore_history.go:929-983`).
  - `GetRelHistory` sorts the versions and deep-copies them
    (`pkg/graph/store/memory/memorystore_history.go:689-719`).
  - The Store surface is `HistoryCapability` (`pkg/graph/store/capabilities.go:207-228`)
    plus the optional accelerators at `:235-240`, `:274-276`, `:312-314`,
    `:338-340` and `:585-588`.
- **With segments.** A sealed row *is* its version: the key is (id, version).
  `GetRelHistory(id)` unions three sources:
  - the row store's history and current row;
  - the overlay stamps;
  - every segment whose ID index holds `id`. Each segment's header ID range and
    the optional ID bloom narrow the probe.

  Versions come out sorted by version and freshly built, as today. The
  genesis rule `Version() == 0` is unchanged.
- **Optional accelerators.** A backend may keep declining them for segment types
  at first. Core already falls back when a capability is missing. That fallback is
  slower, never wrong.

### 4.5 Deletes and retention

- **Today.** A delete turns the current row into a tombstone
  (`DeletedAt = ValidTo = TxTo = now`, `pkg/graph/internal/core/relationship_delete.go:99-110`) and writes
  it with one atomic `DeleteRelWithHistory` (`:130`; memory
  `pkg/graph/store/memory/memorystore_history.go:226-263`). History is never erased (AGENTS.md "Version
  History").
- **Sealed rows.**
  - The tombstone goes to the overlay, persisted in the same row-store batch as
    the change-log record.
  - Every read door consults the overlay. Per lesson 57 (`tasks/lessons.md:1541`),
    set-versus-delete is resolved per key at every reader door.
  - Compaction writes the tombstone into the rewritten segment's `deleted_at`,
    `valid_to` and `tx_to` columns. The row stays as history.
- **Retention.** ADR-0008 purge is node-only today (`PurgeExpiredNodes`,
  `pkg/graph/internal/core/retention_purge.go:90`). Relationship retention on
  segments is a manifest edit plus an unlink of whole time partitions. It follows
  the same policy gate (`AllowRetentionPurge`) and watermark rules as ADR-0008.
  This is an extension, not part of the first steps.

### 4.6 Uniqueness of generated IDs

- **Today.**
  - Core mints IDs after validation (`relEndpointHashLadder`,
    `pkg/graph/internal/core/relationship_create_kernel.go:108-131`). It does no duplicate probe for
    generated IDs.
  - The stores always check: memory `pkg/graph/store/memory/memorystore_rel.go:61-63`, `:149-151` and
    batch `:751-756`; badger `relIDs` under `idxMu`, `pkg/graph/store/badger/badgerstore_rel.go:105-109`.
  - `AddByIDIfAbsent` probes by endpoint pair, not by ID
    (`pkg/graph/internal/core/relationship_add.go:229-301`).
- **With segments.** The probe checks the memtable map, then the segments.
  - Generated IDs: the manifest keeps a per-type **maximum sealed ID**. A
    generated ID above it cannot be in any segment, so the check is one compare.
    `IngestLanes` mint from separate snowflake slots (`pkg/graph/internal/core/ingest_lanes.go:27`,
    `:123`), so a lane that falls behind fails this compare. It then takes the
    full path.
  - Caller-supplied IDs (`AddByID`, import) and the fallback: header ID range,
    then the optional ID bloom, then a binary search of the segment's ID index.
    The cost is O(overlapping segments × log rows), with no resident per-row
    state.
  - `AddByIDIfAbsent` keeps its endpoint-pair probe. The out-CSR run of the start
    node is exactly that probe.

### 4.7 Planner statistics and indexes

The consumer is sigma-tkgd's Cypher planner, through its graph-source adapter
(`internal/cypher/source/graphsource/statistics.go:7-59` and `internal/cypher/source/graphsource/ordered.go:57-69`
in sigma-tkgd). ai-soc reads no statistics.

| primitive (rho-tkg) | today | from segments |
|---|---|---|
| `RelCountByType` (`pkg/graph/stats/api.go:236`) | `len(typeIdx[tok])` | sum of segment row counts, minus overlay deletes, plus memtable. Exact. |
| `AllRelTypeCounts` (`:294`) | same | same |
| `RelPropertyStats` (NDV via HLL, exact min/max, count; `pkg/graph/store/property_stats.go:15-34`, `:77-79`) | per-rel accumulator and memo | per-column HLL, min/max and count in each footer, merged at open (HLL merges losslessly), plus the memtable accumulator. No per-row memo. |
| `RelPropertyTypeClassCounts` (`pkg/graph/stats/api.go:181`; `pkg/graph/store/capabilities.go:857-859`) | per-rel memo | exact from the declared kinds, plus the fallback column's per-value classes. |
| `RelRangeCardinality` (`pkg/graph/stats/api.go:272`) | property-index sorted view | page zone maps skip pages; count exactly on dictionary-code or FOR ranges. Declines on temporal opts exactly as today (`pkg/graph/internal/core/queries.go:135`). |
| `HasRelPropertyIndex` (`pkg/graph/store/capabilities.go:808-813`) / `CreateRelProperty` (`pkg/graph/index/api.go:98`) | RAM-only value→ID sets (`pkg/graph/internal/index/property_index.go:15-46`) | dictionary columns: the sorted dictionary plus the code column is the index (value → code → a scan of the bit-packed codes; segments without the value are skipped by their dictionary). Numeric: zone maps. Open question Q6: whether this counts as "an index" for the planner. |

Everything the planner relies on stays in the same doors with the same exactness.
The per-relationship memos that made badger worse than memory (§1.1) do not exist
for segment types.

### 4.8 Change-log and replication

- A **seal is a physical move, not a logical mutation.** It must not emit delete
  records. Otherwise a replica would delete rows (lesson 55,
  `tasks/lessons.md:1370`: the change-log is a physical redo log).
- Replicas reproduce rows (lesson 50, `:1016`) and seal on their own schedule.
- Updates and deletes of sealed rows are logged as today. The overlay write shares
  the batch with its change-log record.
- Open question Q7: ship whole segments to replicas instead.

### 4.9 What ai-soc must do first (not rho-tkg work)

- **P6 (ai-soc `tasks/todo.md:27`).** Drop `support`, `t_lo`/`t_hi` and
  `scenario`. The segment schema is then the six declared columns. Without P6,
  `support` goes to plain string encoding and `scenario` is a constant column,
  which is correct but wasteful.
- **Declarations.** ai-soc declares HOP (and ORIGIN, VATTR, ...) in `graph.Config`.

---

## 5. API

### 5.1 Stays as it is: the row API is a view over segments

Every door keeps its signature and semantics:

- `g.Rels()`: `Add`, `AddWithTx`, `AddByID`, `AddByIDIfAbsent`, `ByType`,
  `ForEachByType`, `ByTypeAndProperty`, the property-range doors, `History`,
  `CloseVersion`, `ForEachAdjacentRelAt`, `ForEachAdjacentEndpointAt`.
  Wrapper lines: `pkg/graph/rels/api.go:98-701`.
- The Store capabilities `RelationshipCRUDCapability`, `AdjacencyCapability`,
  `BulkReadCapability`, `IterationCapability` and `HistoryCapability`
  (`pkg/graph/store/capabilities.go:80-228`, `:617-631`).

Store-side mechanics:

- **Reads** union memtable, overlay and segments, and return rows sorted by ID as
  today (AGENTS.md "Store is pure persistence ... sort by ID").
- **Adjacency.** For node *n* in each segment: a binary search in the node table,
  then the out-CSR run or the in-CSR run.
- **Existence.** "Missing requested node" errors keep coming from the row store,
  which still owns nodes (`pkg/graph/store/capabilities.go:92-98`).

### 5.2 New: declarations and bulk append

```go
type Config struct {
    // ...existing fields...
    SegmentDir          string        // NVMe; empty = segments kept in anonymous memory
    SegmentColdDir      string        // RAID; empty = no cold tier
    SegmentColdAfter    time.Duration // 0 = never
    SegmentMemoryBudget int64         // bytes; 0 = default (256 MiB)
    RelSegments         []RelSegmentSpec
}

type RelSegmentSpec struct {
    Type    string
    Columns []SegmentColumn // declared schema; order is irrelevant
}

type SegmentColumn struct {
    Name string
    Kind ColumnKind // ColInt64 | ColFloat64 | ColString | ColBool, plus an exact Go-kind tag
}
```

- **Declarations are persisted.** An open with a different declaration fails
  closed, following the rule for persisted index definitions (AGENTS.md "Persisted
  entity wire is a trust boundary"; "Store index definitions require real
  targets").
- **Bulk append**, a new door that skips the per-row `map[string]any`:

  ```go
  func (a *rels.API) AppendColumns(ctx context.Context, relType string, b *RelAppendBatch) (RelIDRange, error)
  // RelAppendBatch: StartIDs, EndIDs []NodeID; ValidFrom, ValidTo, TxFrom []int64;
  // Columns map[string]TypedColumn (one typed slice per declared column).
  ```

  - Validation runs per batch, not per row: each distinct endpoint is checked
    once, and kinds are checked against the schema.
  - It goes through the same kernel invariants: constraints, temporal shape, type
    token after the rejection paths (`pkg/graph/internal/core/relationship_create_kernel.go:12-19`).
  - It fails closed with the same sentinels. A batch is all-or-error, like
    `PutRelationshipsBatch` (`pkg/graph/store/capabilities.go:134-139`).
  - The ingest session (`pkg/graph/internal/core/ingest.go:784`) routes declared types to it. Today
    it writes relationships one per row (`pkg/graph/internal/core/batch_execute.go:329-450`).

### 5.3 New: segment scan for the reasoner

```go
func (g *Graph) ScanRelSegments(relType string, props []string, opts QueryOpts,
    fn func(*RelSegmentBatch) bool) (ok bool, err error)

type RelSegmentBatch struct {
    Segment   uint64              // changes when a new segment starts
    Sorted    bool                // rows ordered by (start, end, valid_from) within Segment
    IDs       []types.RelID
    StartIDs  []types.NodeID
    EndIDs    []types.NodeID
    ValidFrom []int64
    ValidTo   []int64
    TxFrom    []int64
    Dicts     [][]string          // per string column; set on the first batch of a segment
    Codes     [][]uint32          // per string column, indexes into Dicts
    ColumnData                    // ints/floats/bools, as in RelColumnBatch
    Hashes    [][32]byte          // only if opts request it
}
```

- **Why a new door.** `ScanRelColumns` promises batches "in ID order"
  (`pkg/graph/store/capabilities.go:1062-1071`). Segments are ordered by (start, end,
  valid_from). `ScanRelColumns` keeps its contract: it is served natively from the
  ID index, where the memory store today goes through the row path
  (`pkg/graph/store/memory/memorystore_rel_column_scan.go:24-38`). The new door hands over segment order
  plus dictionary codes, so the consumer interns each distinct value once per
  segment, not once per row.
- **Amended 2026-09-25 (v4.39.1): ID order.** Segment order made the door's
  output order differ from the row doors' and, for the unsealed rows (map
  order), differ between calls; sigma builds facts, and so answers and witness
  choice, in scan order, and ai-soc saw findings move between runs.
  `ScanRelSegments` now hands rows out in ascending ID order (the order of
  `ByType` / `ForEachByType`), identical on every call: each segment is decoded
  once into flat columns and walked through its ID index, segments are merged
  by ID with the ID-sorted unsealed rows, and a segment is decoded only when
  the scan reaches its lowest ID and released after its last row, so the scan
  holds the segments whose ID ranges overlap the current ID, not the type.
  `Sorted` is always false. Pinned by `TestSegments_ScanRelSegmentsIsInIDOrder`
  and the order-sensitive graph oracle (both red before).
- **sigma-tkgd today.** It reads HOP through `ByTypeAndProperty(TYPE, "scenario",
  $sc)` into a full slice (`internal/tyla/source/graph/sources.go:179-192` in
  sigma-tkgd). It copies each row's terms with `GetProperty`
  (`internal/tyla/source/graph/terms.go:21-33`, `internal/tyla/source/graph/values.go:32-44`). Then `storeFromPredRows` interns every
  value into its slot table and builds the trie
  (`internal/tyla/runtime/input/store.go:184-306`).
- **sigma-tkgd with the new door.**
  1. Map each segment dictionary to slots once.
  2. Translate codes with a per-segment `[]uint32` table.
  3. Append straight into the `[][]uint32` rows.
  4. Build the trie by k-way merge of runs that are already sorted on the
     (start, end) prefix of HOP's source columns (`engine/programs.go:45` in
     ai-soc).

  There is then no `*types.Relationship` and no boxed value. The anchor hash
  becomes the opt-in `Hashes` column.
- **Where the seam goes.** In sigma-tkgd, it is a new branch in
  `materializeSourceEdges` (`internal/tyla/source/graph/sources.go:160`), next to the node-column branch
  (`internal/tyla/source/graph/columns.go:22`).

### 5.4 What changes for callers

| caller-visible change | who notices |
|---|---|
| Scans of segment types return fresh frozen rows per call (no pointer reuse, allocation per row) | anyone calling `ByType` on huge types. They should use `ForEachByType`, `ScanRelColumns` or `ScanRelSegments`. |
| `UpdateInPlace` (`pkg/graph/internal/core/relationship_update.go:338-347`) on a sealed row writes a shadowing row to the memtable (same id and version; the newest layer wins), folded at compaction | nobody functionally. It costs RAM until compaction. |
| A graph whose declarations differ from the persisted ones refuses to open | deployments changing a schema. They need a migration (rewrite segments). |
| New doors: `AppendColumns`, `ScanRelSegments` | opt-in |

---

## 6. Build order

Each step starts with a failing test that is run red, and ends with a measured
gate, taken at the three synthday sizes (Decision, 2026-09-24). Nothing is merged on an estimate. Measurements are kept as executable
decision records (lesson 67, rule 3). Customer data runs only on Flux, with masked
twins in git.

| step | what | failing test first | gate (measured) |
|---|---|---|---|
| **S0 — DONE 2026-09-24** | Port the analysis harness into `bench/`: a HOP-shaped type written through `g.Rels().AddWithTx`; resident B/rel; on-disk B/rel after badger block compression; ByType/scan rows/s. | none. It records the baseline: 744 B/rel memory, 1,043 and 191 B/rel badger. | reproduces the analysis numbers within ±5 %. **Measured** (`bench/segment_baseline_test.go`, `internal/synthhop`; 790 K / 3.15 M / 12.6 M): memory ai-soc mix 723 / 719 / 718 B/rel (−3.5 %); HOP only 852 / 855 / 856 B/HOP (analysis 852); badger default 1,066 B/rel at 12.6 M (+2.2 %); on disk 259 / 257 / 257 B/rel (+3 %). Badger lean 301 B/rel at 12.6 M does **not** reproduce 191: the analysis's own program measures 304 today; lean is a bounded 0.4–1.4 GB cache, not a per-row cost. HOP ByType: memory 6–9, badger 0.11–0.15, lean 0.12–5.6 M rows/s. Details: CHANGELOG Unreleased. |
| **S1 — DONE 2026-09-24** | Codec, pure package `pkg/graph/internal/segment`: writer and reader for a declared schema, page encodings, footer, CRCs, ID index, CSR, integrity roots. | round-trip bit-exactness incl. Go kinds; `ComputeRelHash(row from segment) == stored Hash` for every row; fuzzed decode never panics or over-allocates (lessons 47, 48); a mixed-kind column goes to fallback with identical hashes | ≤30 B/HOP on the synthday HOP corpus (prototype: 17.5 B); decode ≥ the row path's scan rate. **Measured** on the real synthday HOP rows, 790 K / 3.15 M / 12.6 M: 22.14 / 23.42 / 24.83 B/HOP on disk, all sections (core columns 15.53 / 16.56 / 17.61 against the prototype's 15.51 / 16.50 / 17.51; ID index ~5, node hashes ~1.2, integrity roots 0.5) — gate met. Encode 0.42–0.45 M rows/s incl. verify; per-row cost ×1.06 (encode) and ×0.79 (decode) from 790 K to 12.6 M. Decode gate **not met against the memory store**: rows with recomputed hash 1.3–1.6 M rows/s (2.2–2.7 M without hash) against memory `ByType` 7–11 M rows/s (zero-copy pointers); ~10× badger's 0.11–0.15 M; the column decode alone 24–28 M rows/s. Stats in the footer (HLL, min/max) are left to S2, where the planner doors consume them. |
| **S2 — DONE 2026-09-24 (rho-tkg side); ai-soc gate open** | Memory store: a declared type seals its unsealed rows into **in-RAM** segments (`SegmentDir` empty). Sealed rows leave `rels`, `typeIdx`, `outIdx`, `inIdx` and the stats maps. Every read door becomes the union view. | a differential oracle: the same writes into a store with and without the declaration give identical answers on every read door (point, ByType, ForEachByType, adjacency, `*At`/`*AsOf`, History, VerifyRelChain, ScanRelColumns order, counts, stats). Two-phase temporal tests (testing rule 15). Update/delete of a sealed row (overlay). | ablation harness: resident B/HOP ≤ 60 (arithmetic target ~20 + amortized memtable); ai-soc xcheck all shapes AGREE with identical counts on the three synthday days, then on BA on Flux, with `engine.Open` declaring HOP. **Built:** `Config.RelSegments` / `SegmentMemoryBudget`, `g.Admin().SealRelSegments` / `RelSegmentStats`, `store.RelSegmentCapability` (memory only; other backends fail `New` closed). Overlay as built: a write to a sealed row faults it back into the memtable and marks the sealed copy dead; a faulted-in ID is not resealed before S4. Stats maps and the ID-keyed property/temporal indexes are unchanged by a seal; a declared type declines the rel DocValues builder (§7). Budget seals run on a background sealer (one per store, on demand); a writer twice the budget ahead seals itself and blocks (§3.5). The segments of a store share one endpoint dictionary and one dictionary per declared string column (in-RAM only; S3 spills self-contained segments). **Tests** (each red first): graph-level oracle over two replicas of one primary's change-log (every door, 3 seeds, updates/deletes/cascades/corrections after seals, nested kinds, background seals racing the reads; since v4.38.1 no door skipped), store-level twin oracle, endpoint/string bytes at a fixed value set for 1 vs 8 segments, writer-off-the-seal and Close-waits tests, `-race`. **Measured** (HOP only, 256 MiB budget; 790 K / 3.15 M / 12.6 M): P6 resident **24.0 / 24.8 / 25.7 B/HOP** beyond the memtable (row store 608 / 610 / 611; 26.5 at 37 segments with a 32 MiB budget), encoded 20.2 / 20.9 / 21.8 — gate met; the remaining +1.7 B/HOP is bit width (actor codes 9 → 13 bits, end ordinals, permutation) and the per-segment adjacency directory (0.22 → 0.68 B/HOP, S4 bounds it). Legacy 70.1 / 70.7 / 71.4 — over the gate (`support` fallback 38.8 B/HOP; ai-soc P6 drops it). Row doors on sealed rows: `ByType` 1.1–1.3 M rows/s (row store 6.3–11), `ForEachByType` 0.6–0.7 M, `Get` 0.28–0.44 M/s; `ScanRelSegments` 7.7–7.8 M rows/s. Write 12.6 M: 8.4 s declared, 6.2 s row store (12.5 s with seals in the writer). Details: CHANGELOG Unreleased. |
| S3 | Segments on NVMe: mmap, manifest, crash recovery, `.tmp` cleanup. | kill at every seal step (after write, after fsync, after the manifest, before row removal); the reopened store equals the oracle | Go heap after materialize grows ≤1 B/HOP between the 790 K, 3.15 M and 12.6 M synthday days beyond the fixed budget; wall time within 10 % of S2 |
| S4 | Compaction, overlay folding, time partitions, cold tier. | compaction preserves the oracle, including history and tombstones; a segment moved to cold stays readable; the RAM bound holds while a merge runs | ≤24 segments per type-day; merge RAM ≤ fan-in × page × columns |
| S5 — rho-tkg side DONE 2026-09-24; sigma branch built, not released | `ScanRelColumns` served natively; `ScanRelSegments`; sigma-tkgd branch (a sigma release). | ID-order contract kept; batch equality with the row path; sigma facts identical from both paths | sigma HOP materialize heap and time against the row path; ai-soc parity. **Built:** `g.ScanRelSegments` (`RelSegmentBatch`, segment order, codes into the shared dictionaries, `Row(k)` for values stored outside their column) and `ScanRelColumns` from segment columns through the shared driver `store.ScanColumnsFromRelSource`. **Tests** (red first): batch equality with the row path incl. fallback values and rows without temporal metadata, native `ScanRelColumns` equal to the row path with zero rows built, graph oracle after every chunk; sigma `TestDeclaredEdgeSourceColumnsEqualTheRowFeed` (700 facts, term and interval equal). **Measured:** `ScanRelColumns` over segment columns 2.3–2.7 M rows/s (row store 3.1–4.1 M, rows door on sealed rows 1.1–1.3 M); sigma hop @source at 1.58 M facts 1.00 s / 428 B per fact through the columns, 1.75 s / 490 B row feed on an undeclared type, 3.34 s / 1,052 B row feed over sealed rows. Open: sigma release after a rho-tkg tag; ai-soc parity; the BA day on Flux. |
| S6 | `AppendColumns` and ingest routing. | kernel invariants per batch (constraints, temporal shape, token rollback); all-or-error | append rows/s against today's per-row `AddWithTx` |
| S7 | Badger: the row store as a durable memtable, the seal protocol with change-log silence, planner memo removal for declared types. Tiered and sharded after. | replica converges without delete records; restart with segments present | badger lean + segments: resident independent of day size; ai-soc parity |

ai-soc P6 (the property diet) should land before S2's ai-soc gate, so the gate
measures the declared schema, not the legacy one.

---

## 7. Risks and open questions

For René:

- **Q1. Integrity witness granularity.** One stored root per 64 rows (0.5 B/row)
  replaces a stored hash per row (192 B/row). A corrupted row is then located to
  within 64 rows, not to the row. Is that acceptable? One root per 16 rows would
  cost 2 B/row.
- **Q2. ID minting in ranges.** If the snowflake generator could hand out a
  contiguous ID range per bulk append (`rho-snowflake-2026`; 1,024 IDs per µs per
  slot), the `rel_id` column would be derivable, saving ~5 B/HOP, and the ID index
  would become a permutation (~2.6 B/HOP measured). This needs a library feature.
  Is it worth asking for?
- **Q3. Which types to declare.** HOP clearly. ORIGIN, VATTR, CATTR, QLAB and WOCC
  have small schemas too (ai-soc `engine/materialize.go:355`, `:534`, `:724`,
  `:805`). Nodes stay in the row store. Their count also grows with the day, as
  more assets appear. For 1 TB/day, is the node count bounded? This needs a
  measurement on BA, not a guess.
- **Q4. Order of work against ai-soc P6/P7.** P7 (compact rows in rho-tkg) and
  this design overlap. P7 makes every row cheaper; this design removes the rows of
  declared types from RAM. P7 is still worth doing for undeclared types. Should
  its scope shrink to that?

For whoever maintains rho-tkg:

- **Q5. Frozen-pointer reuse.** Scans of segment types allocate per row per call.
  Does any in-tree or downstream consumer depend on the memory store's zero-copy
  `ByType`? sigma-tkgd's Cypher runtime is the candidate to check.
- **Q6. What counts as an index.** Is a dictionary column "an index" for
  `HasRelPropertyIndex` and planner costing? If yes, the planner may choose
  index-ordered walks (`internal/cypher/runtime/streaming/index_ordered_walk.go:59-62` in sigma-tkgd) that are
  scans in practice.
- **Q7. Replication.** Seal locally on each replica (proposed), or ship segment
  files?
- **Q8. Page cache under cgroups.** Mapped segment pages are reclaimable but
  charged to the cgroup. ai-soc runs days under systemd `MemoryMax`. Throttling
  under `memory.high` needs a measurement on Flux (S3 gate), not an assumption.
- **Q9. Memory without error correction.** ai-soc's run machine has no ECC
  (ai-soc `doc/MEMORY-FAULTS-20260920.md`). Segments carry CRC32C per section. Is
  verifying on first page touch worth its cost (arithmetic: CRC32C runs at many
  GB/s per core), or should verification happen only on scrub and on
  `VerifyRelChain`?
- **Q10. Format ownership.** Segments are the first authoritative columnar format
  in the repo. They need a compat marker, a version-bump protocol like
  `CurrentWireFormatVersion`, and a migration story from the first release on.

Known risks:

- **The union read path is where bugs will be.** Every reader door must consult
  memtable, overlay and segments per key (lessons 57, 58, 64:
  `tasks/lessons.md:1541`, `:1574`, `:1904`). The S2 differential oracle over
  every door is the mitigation, not an afterthought.
- **High-cardinality strings** would grow the dictionaries, and so the heap, with
  the day. The plain-encoding threshold (§2.2) is the guard. It needs its own test.
- **The seal must keep up with ingest.** Measured: BA's biggest day (14 Aug)
  materializes in 43–52 s per shape today (ai-soc day run, 2026-09-24).
  Arithmetic: encoding ~20 B/row is not expected to bound that. S3 and S6 measure
  it.
- **`MaxDocValuesNodes = 10_000_000`** (`pkg/graph/internal/index/docvalues.go:35`) caps the existing column
  cache per type. Segments have no global cap; the per-segment cap is 2³² rows
  through `uint32` ordinals. Declared types must not be routed through the
  DocValues builder.

---

## 8. Measurement provenance

- **Analysis (2026-09-24).** The numbers for memory store B/rel, B/HOP,
  objects/rel, component sizes and badger B/rel are quoted from ai-soc
  `tasks/todo.md:23-29` and the analysis heap profiles `heap400k`, `heapdisk1600k`
  and `heaplean1600k`.
  - Harness: `real` materializes a synthday day through ai-soc
    `engine.MaterializeWithConfig` into `engine.Open`/`OpenAt` or a lean badger
    config.
  - Harness: `ablate` writes N HOP-shaped rows with one property variant.
- **Scratch sizing prototype (this ADR).**
  - Method: materialize synthday 790 K, 3.15 M and 12.6 M rows through ai-soc's
    real path; read HOP back with `g.Rels().ByType`; sort by (start, end,
    valid_from); compute each column's encoded size with the §2.2 encodings (per
    page of 4,096 rows: FOR bit widths, dictionary widths, CSR and permutation
    widths).
  - Results, measured (107,113 / 408,282 / 1,584,150 HOP; 3,950 / 15,750 / 63,000
    nodes): 15.51 / 16.50 / 17.51 B/HOP for the subtotal row of §2.3. The ID index
    is 5.17 B/HOP at 408 K HOP. Distinct (start, end) pairs: 2.29–2.52 rows per
    pair.
  - It is a sizing calculation over real encodings, not a reader. S1 replaces it
    with an in-repo gate over a real writer and reader.
- **Badger on disk.** 49.9 MB for the 790 K synthday: 198,688 relationships plus
  3,950 nodes (analysis run directory, `du -sb`).
