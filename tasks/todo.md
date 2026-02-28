# tkg-v3 — Current Tasks

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
