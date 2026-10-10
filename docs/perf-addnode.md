# AddNode Performance Profile

> **Status (2026-10-10, v4.49.0): historical snapshot, partly superseded.** The
> tables and the optimization history below were measured on 2026-03-06/07 (Apple
> M4 Max, ARM64, Go 1.26.0, the then-current v3/v4 tree) and are kept as
> recorded. The code path moved afterwards: the pipeline diagram below is the
> current one, and "Re-measured at v4.49.0" gives current allocation counts. The
> March ns/op, B/op and "ops/sec" figures are not comparable with any run on
> other hardware or a later tree; the optimization history (pooled-buffer
> `sha256.Sum256`) is still how the hash is computed
> (`pkg/graph/internal/integrity/integrity.go`).

## End-to-End Results (2026-03-07, as recorded)

| Benchmark | Store | ns/op | allocs | B/op | ops/sec |
|---|---|---|---|---|---|
| **AddNode** | MemoryStore | **889** | **14** | **1,029** | **1.12M** |
| **AddNode** | Badger (in-memory) | 2,243 | 46 | 4,282 | 446K |
| **AddRelationship** | MemoryStore | **1,306** | **15** | **1,478** | **765K** |
| **AddRelationship** | Badger (in-memory) | 3,663 | 73 | 8,995 | 273K |

Badger adds ~32 extra allocs and ~1,350 ns per AddNode due to msgpack
serialization, skiplist insertion, and write batch overhead.

## Re-measured at v4.49.0 (2026-10-10, allocation counts only)

AMD Ryzen 9 7950X3D (x86-64, `GOARCH=amd64`), Go 1.26.9, shared host under load,
`-benchtime=0.3s`, one run (`go test -run '^$' -bench ... -benchmem
./pkg/graph/internal/core`; badger through the public `g.Nodes().Add` door with
`Config{BadgerInMemory: true}`, same labels and properties as the benchmark
environment below). `allocs/op` and `B/op` are what carries over between
machines; ns/op is omitted because it varied about 3x between runs on the loaded
host (AddNode, memory: 2.8 us in one run, 9.4 us in the next).

| Benchmark | Store | allocs (2026-03-07) | allocs (v4.49.0) | B/op (2026-03-07) | B/op (v4.49.0) |
|---|---|---|---|---|---|
| AddNode | MemoryStore | 14 | 10 | 1,029 | 801 |
| AddNode | Badger (in-memory) | 46 | 42-43 | 4,282 | ~3,800 |
| AddRelationship | MemoryStore | 15 | 9 | 1,478 | 832 |
| AddRelationship | Badger (in-memory) | 73 | 74-79 | 8,995 | ~5,700 |

Components at v4.49.0 (same run): `SnowflakeIDGen` 0 allocs (28 ns on this x86
host against 7 ns recorded on the M4 Max: ID generation is platform dependent),
`NewPropertySlice` 1 alloc / 64 B, `ComputeNodeHash` 1 alloc / 64 B (the hex
string), `NodeDeepCopy` 4 allocs / 352 B, `MemoryStorePutNode` 4 allocs /
304 B, `ValidateProperties`, `LabelRegistryGetOrCreate` and `checkCtx` 0 allocs.
Not re-measured: the Badger-specific split (serialization / skiplist / write
batch) and everything in "Optimization History".

## Pipeline Overview (current, v4.49.0)

Nodes().Add executes a linear pipeline through `addNodeInternal`
(`pkg/graph/internal/core/node_add.go`). The March 2026 version of this diagram
named `validateName`, `NewPropertySlice`, `SetTemporal` and a plain
`PutNode (DeepCopy + ...)`; those steps have been reshaped as below.

```
Nodes().Add(ctx, labels, props)
  │
  ├─ 1. Validate        checkCtx, validateNodeCreateLabels, extractProvenance,
  │                      extractTemporal, resolveBackfillTxFrom, validateProperties
  ├─ 2. Prepare          types.NewOwnedPropertySlice, label registry
  │                      (getOrCreateLabelsWithSnapshot)
  ├─ 3. ID Generation    nextNodeID (snowflake)
  ├─ 4. Hash             integrity.ComputeNodeHashChecked: SHA-256 of
  │                      (id, version, labels, props) from a pooled buffer
  ├─ 5. Metadata         SetIntegrity, then temporal metadata
  │                      (TxFrom = c.now(), or the privileged backfill instant)
  ├─ 6. Unique           enforceUniqueForNodeHeld (value stripes held across the
  │                      write; a no-op without unique constraints)
  └─ 7. Store            putGeneratedNode -> Store.PutNode
```

What `PutNode` does on the memory store today (`memorystore_node.go`): a
compact frozen copy of the node that also assigns its dense ordinal
(`storedNode`), the map insert, the label index, the transaction-time
label-membership record (K1 sidecar), the belief-watermark bump, the
planner-statistics counters (`addNodePropertyKeyCounts`), the property,
composite, temporal, high-frequency and vector indexes where configured, and
the change-log record.

## Isolated Component Benchmarks (MemoryStore)

(Recorded 2026-03-07, M4 Max, Go 1.26.0.) These measure each component in isolation. Their sum (~694 ns) aligns well with
the MemoryStore e2e result (889 ns). The remaining ~195 ns is function call
overhead, lock acquisition, and cache effects from running the full pipeline.

| Component | ns/op | Allocs | B/op |
|---|---|---|---|
| NewPropertySlice | 121 | 4 | 168 |
| ComputeNodeHash | 128 | 2 | 128 |
| MemoryStorePutNode | 364 | 4 | 475 |
| NodeDeepCopy (inside PutNode) | 61 | 4 | 336 |
| SnowflakeIDGen | 7 | 0 | 0 |
| LabelRegistryGetOrCreate | 8 | 0 | 0 |
| ValidateProperties | 31 | 0 | 0 |
| checkCtx | 3 | 0 | 0 |

## Optimization History

| Change | Store | ns/op | allocs | ops/sec | Date |
|---|---|---|---|---|---|
| Baseline (streaming hash) | Badger | 2,589 | 62 | 386K | 2026-03-06 |
| Zero-alloc hash (buffer pool + Sum256) | Badger | 2,243 | 46 | 446K | 2026-03-07 |
| Zero-alloc hash (buffer pool + Sum256) | MemoryStore | 889 | 14 | 1.12M | 2026-03-07 |

The hash optimization replaced streaming `h.Write()` calls with a pooled `[]byte`
buffer and a single `sha256.Sum256(buf)` call. This eliminated 16 allocations per
AddNode (from 62 to 46 on Badger) and improved Badger throughput by 17%.

## Remaining Opportunities

- **DeepCopy in PutNode** (61 ns, 4 allocs, recorded 2026-03-07): AddNode builds
  a node then PutNode copies it for store isolation. An ownership-transfer API
  would skip the copy. *Status at v4.49.0: not built.* The memory store still
  makes one compact frozen copy per put (`storedNode`, which also carries the
  ordinal); `NodeDeepCopy` re-measures at 4 allocs / 352 B. The one ownership
  transfer that did get built is the ingest bulk door's owned pre-encoded batch
  put (`store.OwnedPreEncodedPutCapability`, write-only creates; badger and
  sharded implement it), which does not apply to `Nodes().Add` or to the memory
  store.

## Key Observations

- **Badger dominates cost** — 32 of 46 allocs (70%) come from Badger's write
  path (msgpack serialization, skiplist arena, write batch). The graph logic
  itself is only 14 allocs.
- **Snowflake ID generation is essentially free** at 7 ns (M4 Max, 2026-03-07;
  28 ns on an x86-64 Ryzen host at v4.49.0) — ARM64 `CNTVCT_EL0` provides
  sub-nanosecond monotonic timestamps with no syscall.
- **Hash cost is near the SHA-256 floor** — ComputeNodeHash at 128 ns is only
  50 ns above raw `sha256.Sum256` (78 ns). The gap is label sort + buffer setup.
- **Index maintenance is zero-cost** when no property/temporal/high-frequency/vector indexes
  are configured (all index update functions return immediately). *Superseded
  as the whole story (v4.49.0):* the query-planner statistics (presence
  counter, NDV/min/max accumulator, type-class partition) are maintained on
  every put unless `Config.DisablePlannerStats` is set (4.17.0 measured that
  opt-out at about 36 % of a wide 12-property node's put, on an M4 Max with
  badger in memory), and the K1 membership record, the belief watermark and the
  ordinal assignment also run on every put.
- **Label registry fast path** (RLock + map lookup) adds negligible overhead
  after the first call.

## Benchmark Environment

- Tables dated 2026-03-06/07: Apple M4 Max, ARM64, Go 1.26.0
- Re-measurement 2026-10-10: AMD Ryzen 9 7950X3D, x86-64, Go 1.26.9, shared host
- 1 label ("LoadTest"), 2 properties ({"seq": 42, "group": "g7"})
- Reproduce: `go test -run '^$' -bench 'Benchmark(AddNode|AddRelationship)$' -benchmem ./pkg/graph/internal/core`
  (the core benchmarks use `memory.New()`; since the March run, which used
  `BadgerInMemory: true`, the explicit `...MemStore` variants are identical to
  them, so the Badger rows have no benchmark of their own in that package any
  more: `bench/` measures Badger ingest as `Ingest1kSingle` and
  `Ingest10kBatch`)
