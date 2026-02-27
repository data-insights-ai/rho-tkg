# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [3.0.3] - 2026-02-27

### Added

- **Snowflake ID generators.** `Graph` now holds two independent `snowflake.Node` generators. `NextNodeID()` and `NextRelID()` produce unique `snowflake.ID` values. `Config.SnowflakeNodeID` is validated (0-1023); out-of-range values return an error from `New()`.
- **Recursive property validation.** `PropertySlice.Set()` traverses slices, maps, and `any`/interface wrappers to reject pointers and structs at any nesting depth (`validatePropertyValue` + `validateReflectValue`).
- **`Instant` type** (`pkg/types`): semantic wrapper for Unix-millisecond timestamps used by all temporal fields.
- **`nodeID` / `relID` opaque types** (`pkg/types`): unexported wrappers around `snowflake.ID`. `InternalID()`, `StartNodeID()`, `EndNodeID()` return these instead of `snowflake.ID` directly.
- **`TemporalMetadata` fields**: `ValidFrom`, `ValidTo`, `TxFrom`, `TxTo`, `CreatedAt`, `UpdatedAt`, `DeletedAt` (all `Instant`), `CreatedBy`, `UpdatedBy` (`string`), `BaseEntityID` (`snowflake.ID`).
- **`NodeIntegrity` / `RelIntegrity` fields**: `Hash`, `PrevHash` (`string`).

### Changed

- `reflectCopyValue` nil-value handling: map keys with nil values are preserved using `reflect.Zero()` instead of silently deleted.
- Registry capacity warning fires exactly once via `sync.Once` (was per-token for 60000-65534).

## [3.0.2] - 2026-02-27

### Added

- **`pkg/graph` package**: Graph layer with label and relationship type registries (Phase 1).
  - `Graph` struct with `Config`, registry ownership, and string resolution methods.
  - `labelRegistry` / `relTypeRegistry`: thread-safe bidirectional string ↔ uint16 token mappings (RWMutex, double-check on write miss). Independent token namespaces.
  - Resolution: `NodeLabels`, `NodePrimaryLabel`, `NodeHasLabel`, `RelationshipType`, `RelationshipHasType`.
  - Registry passthrough: `GetOrCreateLabel`, `GetOrCreateRelType`, `LookupLabel`, `LookupRelType`.
  - Capacity: warning at 60K tokens, error at 65535.
- `labelToken.Value()` and `relTypeToken.Value()` bridge methods for cross-package token access.
- Pointer/struct rejection in `PropertySlice.Set()` with `ErrUnsupportedValueType` sentinel.
- `PropertySlice.Delete(key)` method with `tkg_` prefix guard.
- `Node.DeleteProperty(key)` and `Relationship.DeleteProperty(key)`.

### Changed

- `deepCopyValue` expanded to all common slice/map types plus reflect-based fallback for exotic types.
- `ToMap()` now deep-copies all values.
- Sentinel errors (`ErrReservedPrefix`) properly wrapped for `errors.Is` discrimination.

## [3.0.1] - 2026-02-27

### Added

- `PropertiesMap()` on Node and Relationship.
- `TemporalMetadata` stub struct with `Temporal()`/`SetTemporal()` accessors.
- `NodeIntegrity` and `RelIntegrity` stub structs with `Integrity()`/`SetIntegrity()` accessors.
- Comprehensive `PropertySlice` test suite.

### Changed

- Shadow properties aligned to spec (final 15 `tkg_*` keys).
- Opaque token types (`labelToken`/`relTypeToken`) replace raw `uint16` in public API.
- Token 0 validation: constructors panic, `Has*Token(0)` returns false.
- Extra label deduplication in `NewNode`.

## [3.0.0] - 2026-02-16

### Added

- Initial implementation of `pkg/types`: `Node`, `Relationship`, `PropertySlice`, shadow constants.
- Snowflake ID integration via `rho/snowflake-2026`.
- Token interning with `labelToken` and `relTypeToken` (uint16).
- Shadow property protection (`tkg_` prefix rejection).
- Defensive copying on all slice accessors.
