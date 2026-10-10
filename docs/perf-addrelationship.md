# AddRelationship Performance Profile

> **Status (2026-10-10, v4.49.0): historical snapshot, partly superseded.** The
> tables below were measured on 2026-03-06/07 (Apple M4 Max, ARM64, Go 1.26.0)
> and are kept as recorded. The code path was restructured afterwards (the
> pipeline diagram below is the current one) and the allocation counts moved:
> see "Re-measured at v4.49.0". The March ns/op and ops/sec figures are not
> comparable with other hardware or a later tree. For the shared methodology and
> the node-side component benchmarks see `docs/perf-addnode.md`.

## End-to-End Results (2026-03-07, as recorded)

| Benchmark | Store | ns/op | allocs | B/op | ops/sec |
|---|---|---|---|---|---|
| **AddRelationship** | MemoryStore | **1,306** | **15** | **1,478** | **765K** |
| **AddRelationship** | Badger (in-memory) | 3,663 | 73 | 8,995 | 273K |

Badger adds ~58 extra allocs and ~2,350 ns per AddRelationship due to msgpack
serialization (node + rel entries), skiplist insertion, and write batch overhead.

## Re-measured at v4.49.0 (2026-10-10, allocation counts only)

AMD Ryzen 9 7950X3D (x86-64), Go 1.26.9, shared host under load,
`-benchtime=0.3s`, one run. Memory: `BenchmarkAddRelationship` in
`pkg/graph/internal/core` (a bounded pool of 8,192 pre-created endpoint pairs,
type "KNOWS", one property). Badger: the public `g.Rels().AddByID` door on
`Config{BadgerInMemory: true}` over 4,096 pre-created pairs. ns/op is omitted:
it varied about 3x between runs on the loaded host.

| Benchmark | Store | allocs (2026-03-07) | allocs (v4.49.0) | B/op (2026-03-07) | B/op (v4.49.0) |
|---|---|---|---|---|---|
| AddRelationship | MemoryStore | 15 | 9 | 1,478 | 832 |
| AddRelationship | Badger (in-memory) | 73 | 74-79 | 8,995 | ~5,700 |

The Badger allocation count is unchanged within the run-to-run spread while B/op
fell; that row went through `AddByID` rather than `Add` with node objects, so it
is indicative, not a like-for-like change.

## Pipeline Overview (current, v4.49.0)

`RelOps.Add` / `AddByID` reach `createRelationshipLocked`
(`pkg/graph/internal/core/relationship_add.go`; `addRelationshipInternal` is now
a thin wrapper that checks for nil endpoints and delegates) and the shared
create kernel (`relationship_create_kernel.go`).

```
Rels().Add(ctx, typeName, startNode, endNode, props)
  │
  ├─ 1. Validate/Prepare prepareRelCreate: validateName, extractProvenance,
  │                      extractTemporal, resolveBackfillTxFrom, validateProperties,
  │                      types.NewOwnedPropertySlice, endpoint-ID and self-loop checks
  ├─ 2. Endpoint Lock    entityLocks.LockTwo (2 shard mutexes, deadlock-free ordering)
  ├─ 3. ID + endpoints   relEndpointHashLadder: with temporal constraints configured,
  │                      the live endpoints and checkTemporalConstraints on a probe
  │                      (no constraints: fast exit); otherwise the endpoint hashes
  │                      (FromNodeHash / ToNodeHash) are read, or delegated to the
  │                      store write (endpointHashWrite); the ID is minted only
  │                      after the endpoint checks pass
  ├─ 4. Build            buildRelFromSpec: NewRelationship, SetOwnedProperties,
  │                      integrity.ComputeRelHashChecked (pooled-buffer SHA-256 of
  │                      id, version, typeName, startID, endID, props), SetIntegrity
  │                      (+ endpoint hashes), applyRelCreateTemporal
  └─ 5. Store            createRelWithTypeRollback allocates the rel-type token,
                         then putGeneratedRelationship -> Store.PutRelationship
                         (compact frozen copy carrying the relationship and both
                         endpoint ordinals, map insert, out and in adjacency
                         indexes, type index, K1 rel-type membership record,
                         change-log record)
```

The rel-type token is allocated inside the kernel, after the endpoint locks and
every endpoint-fetch failure path, so an operational failure does not leave a
permanent rel-type registration.

## Why AddRelationship is Slower Than AddNode

(Recorded 2026-03-07; the reasons still describe the structure of the path, but
the memory-store gap in allocations has narrowed: 9 against 10 allocs/op at
v4.49.0.)

1. **Entity lock pair** — `LockTwo` acquires 2 shard mutexes for deadlock-free
   endpoint protection against concurrent DeleteNode.
2. **3 index writes vs 1** — PutRelationship maintains `typeIdx`, `outIdx`,
   `inIdx` (3 nested map inserts) vs PutNode's single `labelIdx` (v4.49.0:
   both puts also record the K1 membership sidecar, see `docs/perf-addnode.md`).
3. **Endpoint existence checks** — 2 extra map lookups to verify start/end
   nodes exist before inserting.
4. **Endpoint hash capture** — reads `Integrity()` from both endpoint nodes
   for cross-validation fields (`FromNodeHash`, `ToNodeHash`).
5. **Temporal constraint check** — fast-path `Len()==0` but still a method
   call and nil check.

## Optimization History

| Change | Store | ns/op | allocs | ops/sec | Date |
|---|---|---|---|---|---|
| Baseline (streaming hash) | Badger | 3,860 | 86 | 259K | 2026-03-06 |
| Zero-alloc hash (buffer pool + Sum256) | Badger | 3,663 | 73 | 273K | 2026-03-07 |
| Zero-alloc hash (buffer pool + Sum256) | MemoryStore | 1,306 | 15 | 765K | 2026-03-07 |

## Benchmark Environment

- Tables dated 2026-03-06/07: Apple M4 Max, ARM64, Go 1.26.0
- Re-measurement 2026-10-10: AMD Ryzen 9 7950X3D, x86-64, Go 1.26.9, shared host
- Pre-created node pairs, 1 property ({"weight": 1})
- Relationship type: "KNOWS"
