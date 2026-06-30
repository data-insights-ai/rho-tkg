# AddRelationship Performance Profile

## End-to-End Results

| Benchmark | Store | ns/op | allocs | B/op | ops/sec |
|---|---|---|---|---|---|
| **AddRelationship** | MemoryStore | **1,306** | **15** | **1,478** | **765K** |
| **AddRelationship** | Badger (in-memory) | 3,663 | 73 | 8,995 | 273K |

Badger adds ~58 extra allocs and ~2,350 ns per AddRelationship due to msgpack
serialization (node + rel entries), skiplist insertion, and write batch overhead.

## Pipeline Overview

AddRelationship executes through `addRelationshipInternal` (relationship_add.go:88).

```
AddRelationship(typeName, startNode, endNode, props)
  │
  ├─ 1. Validate        checkCtx, nil checks, extractProvenance, validateName, validateProperties
  ├─ 2. Prepare          NewPropertySlice, relType registry GetOrCreate
  ├─ 3. Endpoint Lock    entityLocks.LockTwo (2 shard mutexes, deadlock-free ordering)
  ├─ 4. ID Generation    snowflake via ARM64 CNTVCT_EL0
  ├─ 5. Hash             SHA-256 of (id, version, typeName, startID, endID, props)
  ├─ 6. Metadata         SetIntegrity (+ endpoint hashes) + SetTemporal
  ├─ 7. Constraints      checkTemporalConstraints (fast exit if none configured)
  └─ 8. Store            PutRelationship (DeepCopy + map insert + 3 index updates)
```

## Why AddRelationship is Slower Than AddNode

1. **Entity lock pair** — `LockTwo` acquires 2 shard mutexes for deadlock-free
   endpoint protection against concurrent DeleteNode.
2. **3 index writes vs 1** — PutRelationship maintains `typeIdx`, `outIdx`,
   `inIdx` (3 nested map inserts) vs PutNode's single `labelIdx`.
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

- Apple M4 Max, ARM64
- Go 1.26.0
- Pre-created node pairs, 1 property ({"weight": 1})
- Relationship type: "KNOWS"
