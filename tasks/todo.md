# tkg-v3 — Current Tasks

## Completed: Phase 2B — Badger Persistence with Msgpack Serialization (v3.0.8)

- [x] Add `github.com/vmihailenco/msgpack/v5` and `github.com/dgraph-io/badger/v4` dependencies
- [x] Binary key encoding (`keys.go`) — 10 key types, single-byte prefix tags, big-endian IDs/tokens, fixed-width keys
- [x] Msgpack wire formats (`wire.go`) — `nodeWire`, `relWire`, `propertyWire` with temporal/integrity support
- [x] Registry persistence — `ExportNames()`/`ImportNames()` on both registries, `ErrRegistryNotEmpty` sentinel
- [x] `BadgerStore` (`badgerstore.go`) — full `Store` interface: CRUD, label/type/adjacency indexes, prefix scanning, sorted output
- [x] Graph integration — `Config.BadgerDir`/`BadgerInMemory`, `Graph.Close()`, registry load on startup / save on close
- [x] gosec clean (16 `#nosec` annotations for intentional binary encoding casts)
- [x] Documentation: CHANGELOG.md (v3.0.8), README.md, CLAUDE.md updated
- [x] Verification: `make ci` green, 355 tests, 94.2% coverage, race-clean, 0 gosec issues, 0 vulncheck findings

## Completed: Phase 2A — Store, MemoryStore, Entity Management, Shadow Resolution (v3.0.6-v3.0.7)

- [x] `Store` interface, `MemoryStore`, sentinel errors
- [x] `AddNode`/`AddRelationship` with bulk `NewPropertySlice`
- [x] `DeleteNode` cascade, `DeleteRelationship` passthrough
- [x] Shadow resolution (all 15 `tkg_*` keys), passthrough queries
- [x] SnowflakeID bridges, deterministic query ordering, TOCTOU documentation

## Next up

- Phase 3: Cypher & Graph API Integration — Cypher token-based matching, REST/gRPC API layer
