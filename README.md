# tkg-v3

**Temporal Knowledge Graph v3** -- a Go library defining core domain types for a temporal knowledge graph.

This is a pure library (no main binary). All entity identification uses `snowflake.ID` from the internal `snowflake-2026` package.

## Module

```
gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3
```

**Go:** 1.26.0
**Dependencies:** `rho/snowflake-2026` (IDs)

## Architecture

### Core Types (`pkg/types`)

| Type | Purpose |
|------|---------|
| `Node` | Graph vertex with snowflake ID, primary + extra labels (uint16 tokens), properties, version |
| `Relationship` | Directed edge with snowflake ID, type token (uint16), start/end node IDs, properties, version |
| `PropertySlice` | Sorted key-value store with binary search; rejects `tkg_` prefix keys |
| `TemporalMetadata` | Temporal lifecycle metadata (stub, populated by graph layer) |
| `NodeIntegrity` / `RelIntegrity` | Hash-chain integrity metadata (stub, populated by graph layer) |

### Design Invariants

- **Pure-data structs**: Node and Relationship hold no references to Graph, registries, or resolvers. They are self-contained data containers.
- **snowflake.ID everywhere**: All entity and reference IDs are `snowflake.ID`.
- **Strict encapsulation**: All struct fields are unexported. Access through methods only.
- **Defensive copying**: `ExtraLabelTokens()`, `AllLabelTokens()`, `Properties()`, and `DeepCopy()` always return independent copies.
- **Token 0 reserved**: Token 0 is invalid. `HasLabelToken(0)` and `HasTypeToken(0)` always return false. Constructors panic on token 0.
- **Shadow property protection**: The `tkg_` prefix is reserved. `PropertySlice.Set()` rejects any key starting with `tkg_`.
- **Opaque token types**: Label and relationship type tokens use unexported `labelToken` and `relTypeToken` types, preventing accidental misuse of raw `uint16` values.

### Shadow Properties (15)

Read-only virtual properties managed by the graph layer:

| Key | Type | Applies To |
|-----|------|------------|
| `tkg_labels` | `[]string` | Node |
| `tkg_type` | `string` | Relationship |
| `tkg_valid_from`, `tkg_valid_to` | `Instant` | Both |
| `tkg_tx_from`, `tkg_tx_to` | `Instant` | Both |
| `tkg_created_at`, `tkg_updated_at`, `tkg_deleted_at` | `Instant` | Both |
| `tkg_created_by`, `tkg_updated_by` | `string` | Both |
| `tkg_version` | `int` | Both |
| `tkg_hash`, `tkg_prev_hash` | `string` | Both |
| `tkg_base_entity` | `int64` | Both |

## Build & Test

```bash
make build          # verify compilation
make test           # unit tests (short mode, no cache)
make test-v         # verbose tests
make test-race      # race detector enabled
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

## License

Proprietary. See LICENSE file for details.
