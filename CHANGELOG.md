# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [3.0.1] - 2026-02-27

### Fixed

- **DeepCopy correctness bug**: `PropertySlice.DeepCopy()` now deep-copies slice and map values. Previously, a shallow copy allowed mutations of the "copy" to corrupt the original, violating the pure-data-struct isolation guarantee.
- **Token 0 fail-fast**: `NewNode` and `NewRelationship` now panic immediately when given reserved token 0, instead of silently creating invalid entities.
- **DRY shadow key check**: `PropertySlice.Set()` now calls `IsShadowKey()` instead of duplicating the `tkg_` prefix check inline.

### Changed

- **Shadow properties aligned to spec (final 15)**:
  - Removed: `ShadowPrimaryLabel`, `ShadowID`, `ShadowPreviousVersion`, `ShadowNextVersion`, `ShadowIntegrity`
  - Renamed: `ShadowTransactionFrom` -> `ShadowTxFrom`, `ShadowTransactionTo` -> `ShadowTxTo`, `ShadowRelType` -> `ShadowType`
  - Added: `ShadowDeletedAt`, `ShadowCreatedBy`, `ShadowUpdatedBy`, `ShadowHash`, `ShadowPrevHash`
- **Opaque token types**: `PrimaryLabelToken()`, `ExtraLabelTokens()`, `AllLabelTokens()`, `HasLabelToken()`, `TypeToken()`, and `HasTypeToken()` now return/accept the unexported `labelToken`/`relTypeToken` types instead of raw `uint16`, preventing type confusion across token namespaces.

### Added

- `PropertiesMap()` method on both `Node` and `Relationship` (delegates to `PropertySlice.ToMap()`).
- `TemporalMetadata` stub struct shared by Node and Relationship, with `Temporal()`/`SetTemporal()` accessors.
- `NodeIntegrity` and `RelIntegrity` stub structs with `Integrity()`/`SetIntegrity()` accessors.
- Comprehensive `PropertySlice` test suite: sorted invariant, overwrite, Get edge cases, ToMap, Len, DeepCopy nil/empty/independence, DeepCopy slice/map isolation.

## [3.0.0] - 2026-02-16

### Added

- Initial implementation of `pkg/types`: `Node`, `Relationship`, `PropertySlice`, shadow constants.
- Snowflake ID integration via `rho/snowflake-2026`.
- Token interning with `labelToken` and `relTypeToken` (uint16).
- Shadow property protection (`tkg_` prefix rejection).
- Defensive copying on all slice accessors.
