# AddNode Performance Profile

## End-to-End Results

| Benchmark | Store | ns/op | allocs | B/op | ops/sec |
|---|---|---|---|---|---|
| **AddNode** | MemoryStore | **889** | **14** | **1,029** | **1.12M** |
| **AddNode** | Badger (in-memory) | 2,243 | 46 | 4,282 | 446K |
| **AddRelationship** | MemoryStore | **1,306** | **15** | **1,478** | **765K** |
| **AddRelationship** | Badger (in-memory) | 3,663 | 73 | 8,995 | 273K |

Badger adds ~32 extra allocs and ~1,350 ns per AddNode due to msgpack
serialization, skiplist insertion, and write batch overhead.

## Pipeline Overview

AddNode executes a linear pipeline through `addNodeInternal` (context.go:120).

```
AddNode(labels, props)
  │
  ├─ 1. Validate        checkCtx, extractProvenance, validateName, validateProperties
  ├─ 2. Prepare          NewPropertySlice, label registry GetOrCreate
  ├─ 3. ID Generation    snowflake via ARM64 CNTVCT_EL0
  ├─ 4. Hash             SHA-256 of (id, version, labels, props)
  ├─ 5. Metadata         SetIntegrity + SetTemporal (TxFrom = wall clock)
  └─ 6. Store            PutNode (DeepCopy + map insert + index updates)
```

## Isolated Component Benchmarks (MemoryStore)

These measure each component in isolation. Their sum (~694 ns) aligns well with
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

- **DeepCopy in PutNode** (61 ns, 4 allocs): AddNode builds a node then PutNode
  copies it for store isolation. An ownership-transfer API would skip the copy.

## Key Observations

- **Badger dominates cost** — 32 of 46 allocs (70%) come from Badger's write
  path (msgpack serialization, skiplist arena, write batch). The graph logic
  itself is only 14 allocs.
- **Snowflake ID generation is essentially free** at 7 ns — ARM64 `CNTVCT_EL0`
  provides sub-nanosecond monotonic timestamps with no syscall.
- **Hash cost is near the SHA-256 floor** — ComputeNodeHash at 128 ns is only
  50 ns above raw `sha256.Sum256` (78 ns). The gap is label sort + buffer setup.
- **Index maintenance is zero-cost** when no property/temporal/vector indexes
  are configured (all index update functions return immediately).
- **Label registry fast path** (RLock + map lookup) adds negligible overhead
  after the first call.

## Benchmark Environment

- Apple M4 Max, ARM64
- Go 1.26.0
- 1 label ("LoadTest"), 2 properties ({"seq": 42, "group": "g7"})
