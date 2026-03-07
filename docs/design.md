# Design Invariants

## Core Invariants

- **Pure-data structs**: Node and Relationship hold no references to Graph, registries, or resolvers. They are self-contained data containers.
- **snowflake.ID everywhere**: All entity and reference IDs are `snowflake.ID`. Opaque wrapper types (`nodeID`/`relID`) prevent external construction or comparison.
- **Strict encapsulation**: All struct fields are unexported. Access through methods only.
- **Defensive copying**: `ExtraLabelTokens()`, `AllLabelTokens()`, `Properties()`, `DeepCopy()`, `ToMap()`, and `PropertiesMap()` always return independent copies.
- **Token 0 reserved**: Token 0 is invalid. `HasLabelToken(0)`, `HasTypeToken(0)`, `HasLabelTokenRaw(0)`, and `HasTypeTokenRaw(0)` always return false. Constructors panic on token 0 (both primary and extra labels).
- **Extra label deduplication**: `NewNode` deduplicates extra labels and removes the primary label from extras.
- **Allowlist property validation**: `PropertySlice.Set()` recursively validates values using an allowlist. Only primitives (`bool`, `int*`, `uint*`, `float*`, `string`), slices, and maps with safe element types are accepted. Pointers, structs, arrays, channels, functions, and unsafe pointers are rejected at any nesting depth (`ErrUnsupportedValueType`).
- **Shadow property protection**: The `tkg_` prefix is reserved. `PropertySlice.Set()` and `Delete()` reject any key starting with `tkg_`. Errors wrap `ErrReservedPrefix` for programmatic discrimination via `errors.Is`.
- **Opaque token types**: Label and relationship type tokens use unexported `labelToken` and `relTypeToken` types, preventing accidental misuse of raw `uint16` values.
- **Zero-allocation token checks**: `HasLabelTokenRaw(uint16)` on Node and `HasTypeTokenRaw(uint16)` on Relationship for graph-layer hot paths. Token 0 returns false.
- **Depth-limited validation**: Recursive validation and deep-copy stop at `maxPropertyDepth` (32). `Set()` returns `ErrMaxDepthExceeded` for deeper structures.
- **Registry input validation**: `GetOrCreate("")` returns `ErrEmptyName`. Empty strings are never assigned tokens.
- **Shared-pointer accessors**: `Temporal()` and `Integrity()` return the internal pointer — no defensive copy. The graph layer needs mutation access; external callers should treat as read-only.
- **Bulk property construction**: `NewPropertySlice(map[string]any)` is O(N log N) — allocate once, validate all, sort once. Avoids the O(N²) per-property `SetProperty` loop for entity creation.
- **Store is pure persistence**: The `Store` interface handles entity storage, index maintenance, and resource cleanup (`Close()`). Shadow resolution, referential integrity (cascade-delete), and string resolution live on Graph. Three implementations: MemoryStore (in-memory), BadgerStore (persistent), TieredStore (multi-shard).
- **TieredStore shard routing**: Reference entities go to `refShard`; event entities go to time-windowed event shards. Shard resolution for existing entities is O(1) via `shardForNodeID` (ref probe + snowflake timestamp extraction). Cross-shard relationships use split writes: entity+out/ in start shard, in/ in end shard. `shardForRelID` probes all event shards for cross-shard entities. Merge queries use parallel goroutines per shard.
- **Update operations**: `UpdateNode(id, updates)` / `UpdateRelationship(id, updates)` perform read-modify-write under entity lock. Property keys with `nil` values are deleted; non-nil values are set/overwritten. Each update bumps the version counter and sets `temporal.UpdatedAt`. Empty updates map is a no-op (no lock, no version bump). Pre-validates all keys (`tkg_` prefix rejected) and values (`ValidatePropertyValue`) before acquiring the lock.
- **Replace vs Put semantics**: `ReplaceNode`/`ReplaceRelationship` (Store interface) require existence — return `ErrNodeNotFound`/`ErrRelNotFound` if missing. `PutNode`/`PutRelationship` reject duplicates — return `ErrNodeExists`/`ErrRelExists` if present. Replace overwrites entity data only; labels and relationship type/endpoints are immutable after creation — no index changes.
- **Cascade-delete on node removal**: `Graph.DeleteNode` removes all outgoing and incoming relationships before the node. Self-loops are handled by skipping `ErrRelNotFound` in the incoming pass. The tombstone history entries for all cascaded relationships and the node itself are written atomically in a single store call (`DeleteNodeWithHistory`) — no orphaned history entries on crash.
- **SnowflakeID bridges**: `nodeID.SnowflakeID()`, `relID.SnowflakeID()`, `entityID.SnowflakeID()` — exported methods on unexported wrapper types allow cross-package persistence key extraction without leaking the `snowflake.ID` dependency into entity method signatures.
- **Shadow resolution nil-guards**: `ResolveNodeProperty` / `ResolveRelProperty` check `Temporal() != nil` and `Integrity() != nil` before accessing fields. New entities without metadata return `(nil, false)` instead of panicking.

## Shadow Properties (21)

Read-only virtual properties managed by the graph layer:

| Key | Type | Applies To |
|-----|------|------------|
| `tkg_labels` | `[]string` | Node |
| `tkg_type` | `string` | Relationship |
| `tkg_valid_from`, `tkg_valid_to` | `Instant` | Both |
| `tkg_tx_from`, `tkg_tx_to` | `Instant` | Both |
| `tkg_created_at`, `tkg_updated_at`, `tkg_deleted_at` | `Instant` | Both |
| `tkg_created_by`, `tkg_updated_by` | `string` | Both |
| `tkg_version` | `uint32` | Both |
| `tkg_hash`, `tkg_prev_hash` | `string` | Both |
| `tkg_base_entity` | `snowflake.ID` | Both |
| `tkg_from_hash`, `tkg_to_hash` | `string` | Relationship only |
| `tkg_author_id` | `string` | Both |
| `tkg_signature` | `[]byte` | Both |
| `tkg_authorized_by` | `string` | Both |
| `tkg_auth_level` | `uint8` | Both |

`tkg_author_id`, `tkg_signature`, `tkg_authorized_by`, and `tkg_auth_level` are write-path shadow keys: pass them in the `props`/`updates` map of any Add or Update call to store provenance/authorization on the integrity struct. They are stripped before `PropertySlice` construction (never stored as real properties) and readable back via `ResolveNodeProperty` / `ResolveRelProperty`. `tkg_auth_level` accepts `uint8`, `int`, `int32`, `int64`, or `float64`; values outside `[0, 255]` or non-numeric types return an error.
