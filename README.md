# tkg/v3

**Temporal Knowledge Graph v3** — the internal Go library powering the core graph engine for temporal knowledge graphs.

tkg/v3 is a **pure library** (no main binary, no HTTP server, no query language). It provides the low-level graph types, persistence layer, and entity management that higher-level products build on.

For the full product with Cypher queries, Vadalog reasoning, and an HTTP server, see **tkgd-v3**.

| Layer | Repository | What it provides |
|---|---|---|
| **tkg/v3** (this repo) | `rho/tkg/v3` | Graph types, registries, MemoryStore, BadgerStore, TieredStore, entity locks |
| **tkgd-v3** | `rho/tkgd-v3` | Cypher engine, Vadalog reasoning, HTTP server, REST API |

## Module

```
gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3
```

**Go:** 1.26.1
**License:** Apache-2.0
**Dependencies:** [`rho-snowflake-2026`](https://github.com/bds421/rho-snowflake-2026) (IDs), [`msgpack/v5`](https://github.com/vmihailenco/msgpack) (serialization), [`badger/v4`](https://github.com/dgraph-io/badger) (persistence)

## Documentation

Detailed documentation has been split into the `docs/` directory:

- [API & Core Types](docs/api.md) — Graph layer, registries, validation limits, temporal queries, transactions, and shadow properties.
- [Architecture & Concurrency](docs/architecture.md) — System boundaries, entity lock managers, multi-phase iteration, and thread safety.
- [Persistence](docs/persistence.md) — Storage interfaces, BadgerStore, and TieredStore multi-shard persistence.
- [Design Invariants](docs/design.md) — Protocol guarantees, referential integrity, defensive copying, and error sentinels.
- [Specifications](docs/SPEC.md) — Formal specifications and algorithms.

### What's new in 3.1.12

**Admin-path event-shard pinning.** `ListShards`, `RebuildCatalog`, `Clear`, and four index admin methods (`CreateTemporalIndex`, `DropTemporalIndex`, `CreateHighFrequencyIndex`, `DropHighFrequencyIndex`) now pin event shards via `checkoutStore`/`checkinStore` before touching their BadgerStores. Pre-fix, a concurrent `Close` could free a shard's DB while an admin call was still reading or writing it. `findRelInAnyShardStore` now consults the caller's pre-pinned snapshot instead of re-resolving, closing a `Close`-race window in `RunRepair`.

**`ArchiveNode` / `RestoreNode` serialised under `g.mu.Lock()`.** Both admin methods now take the full write lock, the same exclusion class as a transaction. Pre-fix, a concurrent `AddRelationship` could slip between `ArchiveNode`'s adjacency pre-scan and its cascade, creating a cross-shard rel the cascade then partially destroyed.

**`PutRelationship` cross-shard archive guard.** If one endpoint lives on `refArchive` and the other does not, `PutRelationship` returns `ErrCrossShardArchiveRel` before any writes, closing the window where a post-archive `AddRelationship` bypassed the M2 invariant.

### What's new in 3.1.11

**refArchive parity in indexed and bulk reads.** `NodesByLabel`, `NodesByLabelAndProperty`, `NodeCountByLabel`, `RelationshipsByType`, `RelCountByType`, `AllNodes`, `AllRelationships`, `AllNodeIDs`, `AllRelIDs` now include archived entities at `DepthAll`. Pre-fix, archived nodes stayed `GetNode`-addressable but vanished from indexed/bulk reads. `DepthHot`/`DepthWarm` continue to exclude archive (caller explicitly asked for hotter tiers).

**Close-race protection on archive paths.** `shardForNodeIDChecked` / `shardForRelIDChecked` / `forEachHistoryShard` / `findRelInAnyShardStore` / `ArchiveNode` / `RestoreNode` now pin the archive via `checkoutArchive`, mirroring the `activeReqs` discipline used for cold event shards.

**`ArchiveNode` rejects cross-shard rels** with new `ErrCrossShardArchiveRel`. Archiving a node with a relationship whose other endpoint is not co-archived would fragment the version chain; the operation now fails loud before any mutation.

### What's new in 3.1.10

**History-aware indexed candidate planning.** `NodesByLabel`, `NodesByLabelAndProperty`, `RelationshipsByType`, `GetNodesByLabelValidAt`, `NodesByLabelPropertyAndTime`, `NodesByLabelPropertyDuring`, and `GetNeighborsValidAt` now derive candidates from the appropriate index (label / property / type / adjacency) and merge them with history IDs. Previously they fell back to `Store.ForEachNodeID` over every entity. Performance fix for any temporal query with a narrow predicate.

**`AllNodes` / `AllRelationships` history-aware temporal opts.** Closes a hole in v3.1.7's history-aware sweep: temporal `QueryOpts` on these generic entry points now resolve through the union of current + history IDs.

**Batch hardening.** `BatchBuilder.AddRelationship` now enforces `AllowSelfLoops`; batch metadata is stamped at execute time (not queue time); rels with failed-create endpoints are skipped with diagnostic errors; endpoint integrity hashes captured under endpoint lock; cross-shard rel rollback on batch failure restores `TxFrom`. See `CHANGELOG.md` `[3.1.10]` for the full list.

### What's new in 3.1.9

**`IndexProvider` extension point.** Out-of-tree indexes can now plug into the graph through a small interface (`Name()`, `OnEvent(ev, g)`, `Close()`). Providers register via `g.RegisterIndexProvider`, receive lifecycle events through the sync `EventBus`, and own their persistence + query routing. First consumer is tkgd's spatial R-tree.

**`HashableValue` interface.** Custom property struct types can now participate in node/relationship integrity hashing. Register the type via `types.RegisterPropertyStructType`; implement `HashableValue.HashBytes() []byte` for a deterministic binary representation. Treat `HashBytes` like a wire format — once shipped, the encoding is locked because every existing hash chain depends on it.

### What's new in 3.1.8

**Typed entity IDs.** Public Graph API and all internal plumbing now use typed wrappers `types.NodeID` / `types.RelID` / `types.EntityID` instead of raw `snowflake.ID`. The compiler now catches NodeID/RelID/EntityID mixups that previously passed silently. Migration for downstream callers is mostly mechanical: `n.InternalID().SnowflakeID()` → `n.ID()` at typed Graph callsites. See `CHANGELOG.md` `[3.1.8]` → "Migration notes for downstream consumers" for the full upgrade guide. `InternalID()` retained as a deprecated alias for source compatibility.

**TieredStore cross-shard hardening.** Seven distinct correctness fixes around cold-shard rel reachability, history fan-out, checkout pinning, refArchive race protection, and a primary-label-class invariant. Restores parity with `MemoryStore` and `BadgerStore` for tombstones after deleting reference nodes and cross-shard relationships. New shared store-contract test suite runs the same behavioural guarantees against all three Store implementations.

### Behavior change in 3.1.7

`NodesByLabel(label, opts)`, `NodesByLabelAndProperty(label, key, value, opts)`, and `RelationshipsByType(typeName, opts)` now scan history when called with a temporal `QueryOpts` (`ValidAt` and/or `ValidStart`/`ValidEnd`). Previously these generic entry points routed temporal queries through store-side pushdown that consults only current indexes, so a node that had a label at the requested time but no longer carries it (or a relationship that has since been deleted) was silently missed. Callers using temporal opts will now see different — and correct — results. Non-temporal calls retain the original fast pushdown. See `CHANGELOG.md` `[3.1.7]` for details.

## Snowflake Configuration

Both generator sets (nodes and relationships) are initialized with explicit parameters:

```text
+---------------------------------------------------------------+
|  1 bit  |       48 bits        |   5 bits   |     10 bits     |
|  zero   |     time (usec)      |   node ID  |    sequence     |
+---------------------------------------------------------------+
```

| Parameter | Value |
|-----------|-------|
| Epoch | `2026-01-01 00:00:00 UTC` |
| Precision | Microseconds (`snowflake.WithMicroseconds()`) |
| Node bits | 5 (max `SnowflakeNodeID` is 15 since it maps to `id*2` and `id*2+1`) |
| Step bits | 10 (1024 unique IDs per microsecond) |

Each concurrent graph instance **must** use a different `Config.SnowflakeNodeID` (0-15). Generators are stateless — no counter persistence, no crash recovery.



## Build & Test

```bash
make build          # verify compilation
make test           # unit tests (short mode, no cache)
make test-v         # verbose tests
make test-race      # race detector enabled
make test-integration  # integration tests (long-running)
make bench-graph-baseline    # repeatable graph API benchmark baseline
make bench-graph-production-small  # production-shaped graph benchmark suite
make bench-graph-production-large  # large stress graph benchmark suite
make cover          # coverage report -> coverage.html
make check          # pre-commit: vet + build + test
make ci             # full pipeline: fmt-check + vet + build + test-race + security + vulncheck
make fmt            # format code
make security       # gosec static analysis
make vulncheck      # govulncheck for known CVEs
```

Run a single test:

```bash
go test -run TestFoo ./pkg/types/
```

## Tutorials

Progressive tutorials in `tutorials/`, each a standalone `main.go`:

| Tutorial | Topic |
|----------|-------|
| `001_basic_graph` | Create nodes, relationships, and query the graph |
| `002_temporal` | Temporal metadata (ValidFrom/ValidTo, CreatedAt, UpdatedAt) |
| `003_badger_persistence` | On-disk BadgerStore, close/reopen, registry persistence |
| `004_full_features` | Update operations, version history, hash chain integrity |
| `005_performance` | Benchmark MemoryStore vs BadgerStore (throughput, memory, storage) |

Run any tutorial: `go run ./tutorials/001_basic_graph/`

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
