# tkg-v3 — Current Tasks

## Completed: Final Hardening + Code Review — v3.0.12

- [x] BLOCKER: Atomic counter persistence — counters in WriteBatch, `persistCounters()` deleted
- [x] BLOCKER: O(N²) `evictClean` → O(N) single pass from Back()
- [x] MAJOR: `DeleteNodeCascade` propagates non-`ErrRelNotFound` errors (corrupted rel detection)
- [x] MAJOR: `Close()` explicitly flushes when flushLoop was never started (InMemory mode)
- [x] MINOR: O(1) `nodeIDs` bloom filter in `GetNode()` — short-circuit before Badger read
- [x] MINOR: O(1) `relIDs` bloom filter in `GetRelationship()` + `PutRelationship()` — replaces O(N) `relExistsInIndex()` scan
- [x] Tests: LRU single-pass eviction, cascade corrupt propagation, atomic counter persistence, InMemory close flush
- [x] Final code review: READY FOR PRODUCTION
- [x] Verification: `make ci` green, 92.3% coverage, race-clean, 0 gosec, 0 vulncheck

## Completed: Async Flush Hardening — 3 Concurrency Fixes (v3.0.11)

- [x] BLOCKER: Version-aware dirty tracking (`dirtyVer uint64`) — `CollectDirty()` read-only, `MarkFlushed()` only clears matching versions
- [x] MAJOR: Map-based pending write buffer (`map[string]writeOp`) — last-write-wins dedup, `requeueOps()` preserves newer writes
- [x] MAJOR: `DeleteNodeCascade` error path scrubs `labelIdx` when entity data unreadable — no ghost entries
- [x] Tests: LRU version-aware tests, requeue tests, cascade corruption tests
- [x] Verification: `make ci` green, 92.6% coverage, race-clean, 0 gosec, 0 vulncheck

## Completed: BadgerStore LRU Cache + Async Batch Persistence + Entity Locks (v3.0.10)

- [x] Generic LRU cache (`entityLRU[V]`) with dirty tracking, tombstones, soft capacity
- [x] Sharded entity lock manager (256 shards, deadlock-free `LockTwo`)
- [x] BadgerStore refactored: in-memory indexes + LRU caches + async WriteBatch flush loop
- [x] Atomic int64 counters (no OCC contention) — `nodeCount`/`relCount` on struct
- [x] Entity locks in Graph layer: `AddRelationship` locks both endpoints, `DeleteNode` locks target
- [x] Background flush loop (100ms ticker) + value log GC loop (5min ticker)
- [x] `loadIndexes()` rebuilds in-memory state from Badger on startup
- [x] Write-skew regression test (`TestGraphAddRelDeleteNodeConcurrency`)
- [x] Verification: `make ci` green, 561 tests, 86.0% coverage, race-clean, 0 gosec, 0 vulncheck
- [x] Documentation: CHANGELOG.md (v3.0.10), CLAUDE.md (architecture table, invariants, phases), doc.go, tasks/lessons.md, MEMORY.md updated

## Completed: Phase 2B Hardening — 5 Architectural Fixes (v3.0.9)

- [x] Fix Close() file handle leak — `closeFn()` now runs unconditionally even if registry saves fail
- [x] Fix type erasure in wire format — added `Type byte` tag to `propertyWire` for Go type fidelity across msgpack round-trips
- [x] Add error returns to Store query methods — 6 methods now return `error`; BadgerStore propagates I/O errors
- [x] Atomic `DeleteNodeCascade` — single write lock (MemoryStore) / single `db.Update()` transaction (BadgerStore), no TOCTOU window
- [x] O(1) counts — `BadgerStore` maintains atomic metadata counters; `initCounters()` migrates existing databases
- [x] Simplify `Graph.DeleteNode` — delegates to `Store.DeleteNodeCascade`
- [x] Documentation: CHANGELOG.md (v3.0.9), CLAUDE.md, tasks/todo.md updated
- [x] Verification: `make ci` green, 378 tests, 88.9% coverage, race-clean, 0 gosec issues, 0 vulncheck findings

## Completed: Phase 2B — Badger Persistence with Msgpack Serialization (v3.0.8)

- [x] Binary key encoding, msgpack wire formats, registry persistence
- [x] `BadgerStore` with full `Store` interface, `Graph.Close()` lifecycle
- [x] 355 tests, 94.2% coverage

## Completed: Phase 2A — Store, MemoryStore, Entity Management, Shadow Resolution (v3.0.6-v3.0.7)

- [x] `Store` interface, `MemoryStore`, sentinel errors
- [x] `AddNode`/`AddRelationship` with bulk `NewPropertySlice`
- [x] `DeleteNode` cascade, `DeleteRelationship` passthrough
- [x] Shadow resolution (all 15 `tkg_*` keys), passthrough queries
- [x] SnowflakeID bridges, deterministic query ordering

## Next up

- Phase 3: Cypher & Graph API Integration — Cypher token-based matching, REST/gRPC API layer
