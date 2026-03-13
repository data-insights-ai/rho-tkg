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

**Go:** 1.26.0
**License:** Apache-2.0
**Dependencies:** [`rho-snowflake-2026`](https://github.com/bds421/rho-snowflake-2026) (IDs), [`msgpack/v5`](https://github.com/vmihailenco/msgpack) (serialization), [`badger/v4`](https://github.com/dgraph-io/badger) (persistence)

## Documentation

Detailed documentation has been split into the `docs/` directory:

- [API & Core Types](docs/api.md) — Graph layer, registries, validation limits, temporal queries, transactions, and shadow properties.
- [Architecture & Concurrency](docs/architecture.md) — System boundaries, entity lock managers, multi-phase iteration, and thread safety.
- [Persistence](docs/persistence.md) — Storage interfaces, BadgerStore, and TieredStore multi-shard persistence.
- [Design Invariants](docs/design.md) — Protocol guarantees, referential integrity, defensive copying, and error sentinels.
- [Specifications](docs/SPEC.md) — Formal specifications and algorithms.

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
