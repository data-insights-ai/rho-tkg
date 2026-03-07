# Performance Optimization Targets

Identified from profiling AddNode and AddRelationship on Apple M4 Max.

## Completed Optimizations

### 1. Zero-Allocation Hash Computation (Done)

**Impact**: Eliminated 13 allocs/call (AddNode) / 8 allocs/call (AddRelationship)

Replaced streaming `hash.Hash` approach (`sha256.New()` + many small `.Write()` calls + `.Sum(nil)`) with a single buffer built via `append` and `sha256.Sum256(buf)` returning `[32]byte` on stack.

Key changes in `pkg/graph/integrity.go`:
- `sync.Pool` of reusable `[]byte` buffers for hash input construction
- `binary.BigEndian.AppendUint*` instead of `PutUint*` + `h.Write`
- `append(buf, s...)` instead of `io.WriteString(h, s)` (zero-alloc memcpy)
- `sha256.Sum256(buf)` → stack `[32]byte`, `hex.Encode` into stack `[64]byte`
- Deleted 6 streaming helpers (`hashU8/U16/U32/U64/I64/Str`) and `writeProperties`/`writePropertyValue`

Remaining 2 allocs per call: label sort copy (`make([]string)`) + return `string(hexBuf[:])`.

| Metric | Before | After | Delta |
|---|---|---|---|
| ComputeNodeHash | 312 ns / 18 allocs | 128 ns / 2 allocs | -59% ns, -89% allocs |
| ComputeRelHash | 254 ns / 15 allocs | 116 ns / 2 allocs | -54% ns, -87% allocs |
| AddNode (Badger) | 2,589 ns / 62 allocs | 2,243 ns / 46 allocs | -13% ns, -26% allocs |
| AddNode (MemoryStore) | — | 889 ns / 14 allocs | graph logic only: 14 allocs |
| AddRelationship (Badger) | 3,860 ns / 86 allocs | 3,663 ns / 73 allocs | -5% ns, -15% allocs |
| AddRelationship (MemoryStore) | — | 1,306 ns / 15 allocs | graph logic only: 15 allocs |

## Remaining Optimization Opportunities

### 2. Ownership Transfer in PutNode/PutRelationship

**Impact**: Eliminate DeepCopy (~61 ns, 4 allocs per node / ~40 ns, 3 allocs per rel)

Currently, `addNodeInternal` builds a node, then `PutNode` calls `DeepCopy()` because the store cannot assume the caller won't mutate the object. But in AddNode, the caller immediately returns the node and never mutates it.

Options:
- Add a `PutNodeOwned(n)` method that takes ownership (no copy)
- Mark nodes as frozen after store insertion (copy-on-write)

## Summary Table

| Optimization | Status | Allocs Saved | ns Saved |
|---|---|---|---|
| Zero-alloc hash computation | **Done** | 13-16/call | ~160-365 |
| Ownership transfer | Planned | 3-4/call | ~50-60 |

## Current Benchmark Results (Apple M4 Max)

### End-to-End by Store Backend

| Benchmark | Store | ns/op | allocs | B/op | ops/sec |
|---|---|---|---|---|---|
| **AddNode** | **MemoryStore** | **889** | **14** | **1,029** | **1.12M** |
| AddNode | Badger (in-memory) | 2,243 | 46 | 4,282 | 446K |
| **AddRelationship** | **MemoryStore** | **1,306** | **15** | **1,478** | **765K** |
| AddRelationship | Badger (in-memory) | 3,663 | 73 | 8,995 | 273K |

Badger adds ~32 allocs per AddNode (msgpack serialization, skiplist, write batch).
The graph logic itself is only 14 allocs.

### Isolated Components (MemoryStore)

| Benchmark | ns/op | allocs | B/op | ops/sec |
|---|---|---|---|---|
| SnowflakeIDGen | 6.6 | 0 | 0 | 152M |
| NewPropertySlice | 121 | 4 | 168 | 8.3M |
| NewNode | 1.6 | 0 | 0 | 625M |
| ComputeNodeHash | 128 | 2 | 128 | 7.8M |
| NodeDeepCopy | 61 | 4 | 336 | 16.4M |
| MemoryStorePutNode | 364 | 4 | 475 | 2.7M |
| ComputeRelHash | 116 | 2 | 96 | 8.6M |
| MemoryStorePutRel | 602 | 4 | 583 | 1.7M |
| RawSHA256 (baseline) | 78 | 2 | 128 | 12.8M |
| PropertySliceDeepCopy | 21 | 1 | 64 | 48M |
| ValidateProperties | 31 | 0 | 0 | 32M |
| EntityLockUnlock | 5.3 | 0 | 0 | 189M |
| LabelRegistryGetOrCreate | 7.6 | 0 | 0 | 132M |
| CheckCtx | 2.7 | 0 | 0 | 370M |
