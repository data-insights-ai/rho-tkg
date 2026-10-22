# SPEC: Token Internment, Snowflake IDs & Pure Data Structs

Formal specification of the entity identity, token registries, entity
structs, msgpack wire format and Badger key layout of `rho-tkg` v4. Verified
against the code at HEAD; the code wins on conflict. Owned elsewhere (this file
points, it does not repeat): concurrency, locks and backend structure
`docs/architecture.md`; storage and durability contracts, format markers and
upgrades `docs/persistence.md`; public API usage `docs/api.md`; statistics and
planner doors `docs/query-planners.md`; store and graph boundary invariants
`docs/architecture.md` (System Overview, "Design invariants").

---

## 1. Overview

Entity IDs are snowflake `int64`s; labels, relationship types and property keys
are interned `uint16` tokens. Users interact through string-based public APIs;
tokens never cross the public API (§2).

**Node and Relationship are pure data structs with no back-references to the
graph.** String resolution is done by the Graph layer (`g.Nodes().Labels`,
`g.Rels().Type`, `g.Resolve()`) or a downstream consumer holding a
`*graph.Graph`.

Properties this design provides:
- fixed-width, byte-comparable Badger keys for every index (§10);
- globally unique, time-ordered IDs without coordination (§3);
- entities that are self-contained, serializable, cacheable and testable
  without mock infrastructure (§4).

---

## 2. Internal Types

Four identity types in `pkg/types` (`node.go`, `relationship.go`): two
exported ID wrappers, two unexported token types.

```go
type NodeID       snowflake.ID  // exported wrapper
type RelID        snowflake.ID  // exported wrapper
type labelToken   uint16        // unexported: interned label, max 65,535
type relTypeToken uint16        // unexported: interned type, max 65,535
```

- `NodeID` / `RelID` are the identity type of every public door
  (`g.Nodes().Get(ctx, id)`, …); the snowflake layout stays hidden behind
  `SnowflakeID()`, the only bridge to persistence keys. IDs come from the graph
  generators (`g.Nodes().NextID` / `g.Rels().NextID`, 0 after close).
- `EntityID` (`pkg/types/temporal.go`) is the third, type-agnostic snowflake
  wrapper (version-chain base entity `tkg_base_entity`, `QueryOpts.After`,
  event IDs).
- `NodeID.MintInstant()` / `RelID.MintInstant()` (`pkg/types/mint_instant.go`)
  return the ID's mint instant as a `types.Instant` (Unix ms, floored from the
  µs time field; §3.3). A zero or negative ID returns 0.
- `labelToken` / `relTypeToken` are never accepted or returned by a public API.
  Token 0 is reserved as "unset/invalid"; registries assign from 1 (§6).

Registry zero values, nil-pointer and persistence contracts: architecture.md
("Registry zero values preserve token invariants", "Store registry inputs are
lifecycle-checked", Admin & Repair `MigrateFromBadger`); persistence.md
(registries saved on token commit and at `Close`).

---

## 3. Snowflake IDs

### 3.1 Why Snowflake Instead of Atomic Counters

| Property | Atomic counter | Snowflake |
|----------|---------------|-----------|
| Globally unique | No (per-process only) | Yes (node field embedded) |
| Distributed-safe | No (collision on multi-instance) | Yes (no coordination) |
| Time-ordered | No | Yes (48-bit µs timestamp) |
| Crash recovery / counter persistence | Required (max-key scan, `next_*_id` keys) | Not needed (stateless) |
| Temporal locality in the LSM | No | Yes (recent IDs cluster) |
| Decomposable | No | Yes (time, node field, step) |

### 3.2 Layout and Generators

The epoch and bit layout have one definition, `pkg/internal/idlayout`
(`idlayout.Epoch` = 2026-01-01 UTC; `idlayout.Layout` = 1 zero bit | 48 bits µs
since the epoch | 5 bits node field | 10 bits step). `pkg/types` and every
`pkg/graph/internal/**` decoder share it; `pkg/graph/internal/snowflake`
re-exports it. It is package-level, not per `Config`.

`core.New` (`pkg/graph/internal/core/core.go`, invoked via `graph.New`) builds
two generators from `github.com/bds421/rho-snowflake-2026` with that layout:
nodes on node field `SnowflakeNodeID*2`, relationships on `SnowflakeNodeID*2+1`.
`Config.SnowflakeNodeID` must be 0–15 (`graph: SnowflakeNodeID must be 0-15`);
each concurrent graph instance writing to shared data must use a different
value. `Config.IngestLanes > 0` adds that many unified lane generators
(`ingest_lanes.go`), each on its own node field from 0..31 excluding the
interactive pair, minting both nodes and relationships; `2 + IngestLanes <= 32`
or `New` fails. The sharded store routes by the node field. Generator topology
and locks: architecture.md "Snowflake IDs".

### 3.3 ID Generation

`(*Core).nextNodeID` / `nextRelID` (`validation.go`) wrap `Generate()`; lane
mints use `nextNodeIDForLane` / `nextRelIDForLane`.

- Thread-safe; 1024 IDs per µs per generator.
- No counter persistence: no `next_node_id` / `next_rel_id` key exists.
- Horizon: 2^48 µs ≈ 8.92 years, so the time field is exhausted on
  **2034-12-02** UTC; the generator has no guard against the time field
  reaching the sign bit (`tasks/todo.md` "Backlog (open)" item 4 (v)).
- Clock: the generator reads a monotonic clock anchored at the epoch; a
  backward step reuses the last timestamp and advances the step (it waits only
  on step overflow). No cross-restart guard.
- Mint instant: `MintInstant()` decodes the time field through
  `idlayout.MintInstantMillis` (floored to ms). Node (even field) and
  relationship (odd field) IDs carry the same time bits, so the derivation is
  identical and monotone in ID order. It is the `tkg_created_at` fallback
  (§8.2) and the last fallback of a row's derived valid-from on the legacy
  valid axis (after `TxFrom` and a legacy row's `UpdatedAt`; api.md "Valid-time
  start (Config.UntimedValidStart)").

### 3.4 Badger Key Ordering

IDs are stored big-endian in keys (§10), so the high time bits make recently
created entities cluster in the LSM tree: prefix scans over recent data touch
fewer SSTables and compaction benefits from the locality.

---

## 4. Pure Data Structs

### 4.1 The Rule

**Node and Relationship never hold references to the Graph, registries, or any
external resolver.** They hold tokens and never resolve them to strings
themselves.

### 4.2 Rationale

| Property | Requirement | Pure data struct? |
|----------|-------------|-------------------|
| Self-contained | No hidden initialization state | Yes |
| Safely serializable | Serialize/deserialize without losing capability | Yes |
| Safely cacheable | Can outlive any graph reference | Yes |
| Safe across goroutines | No hidden shared state | Yes |
| Testable standalone | Token-level methods work without a mock graph | Yes |
| Plugin/extension safe | No injection ceremony | Yes |

An injected resolver (e.g. a `SetGraph()`) violates all six: a node that
silently returns empty strings when the resolver is missing is a latent bug
after deserialization, caching or a package-boundary hop.

### 4.3 Resolution Ownership

| Consumer | Resolution |
|----------|-----------|
| Graph layer (owns the registries) | `g.Nodes().Labels(n)`, `g.Rels().Type(r)`, `g.Resolve().NodeProperty(n, key)` |
| Downstream query engine / REST / gRPC (holds `*graph.Graph`) | the same accessors, before encoding |

Index lookups, label matching, adjacency traversal and hash computation work on
tokens. String resolution is a presentation concern.

---

## 5. Entity Structs

Every field is unexported (access through methods) and ordered by descending
alignment. Sizes are pinned by `pkg/types/layout_test.go`
(`TestNodeStructSize` … `TestRelIntegrityStructSize`): `Node` 96B,
`Relationship` 96B, `TemporalMetadata` 104B, `NodeIntegrity` 96B,
`RelIntegrity` 128B; the type table is architecture.md "Core Types".

### 5.1 Node (`pkg/types/node.go`)

```go
type Node struct {
    id           NodeID
    properties   PropertySlice     // sorted []Property
    extraLabels  []labelToken      // nil for single-label nodes
    temporal     *TemporalMetadata
    integrity    *NodeIntegrity    // SHA-256 hash chain
    meta         *nodeMeta         // compact frozen metadata (store-cached first versions)
    version      uint32
    primaryLabel labelToken        // always set (nodes require >= 1 label)
    frozen       bool              // store-cached row; mutation is rejected
    sharedProps  bool              // properties' backing array shared; copy before in-place writes
    ordinal      uint32            // store-assigned dense ordinal; 0 = none
}
```

Token-level methods (no resolver needed): `PrimaryLabelToken()`,
`ExtraLabelTokens()`, `HasLabelToken(tok)`, `LabelTokenCount()`,
`AllLabelTokens()`, `ID()`, `InternalID()` (legacy alias of `ID()`).

Property methods: `GetProperty(key)` (binary search), `PropertyValueEqual(key,
expected) (found, equal)` (no copy), `SetProperty(key, value)` (rejects the
`tkg_` prefix, deep-copies reference values), `SetProperties(ps)` (validate,
sort, de-duplicate, deep-copy), `Properties()` (defensive deep copy),
`PropertiesMap()`. No string-level label method exists on `Node` (§7).

### 5.2 Relationship (`pkg/types/relationship.go`)

```go
type Relationship struct {
    id           RelID
    startID      NodeID
    endID        NodeID
    properties   PropertySlice
    temporal     *TemporalMetadata
    integrity    *RelIntegrity
    meta         *relMeta
    version      uint32
    relType      relTypeToken      // exactly one type
    frozen       bool
    sharedProps  bool
    ordinal      uint32
    startOrdinal uint32            // endpoints' dense ordinals on a store-kept current row; 0 = none
    endOrdinal   uint32
}
```

Token-level methods: `TypeToken()`, `HasTypeToken(tok)`, `StartNodeID()`,
`EndNodeID()`, `ID()`, `InternalID()`. Property methods as on `Node`.

---

## 6. Registries

Three independent registries in `pkg/graph/internal/registry`, each with its
own token namespace (a label `"KNOWS"` and a type `"KNOWS"` get independent
tokens): `LabelRegistry`, `RelTypeRegistry`, `PropertyKeyRegistry` (interns
property key strings so wire rows carry a `uint16` key token, §9.1). They work
on raw `uint16`; the typed tokens live in `pkg/types`.

| Method | Lock | Description |
|--------|------|-------------|
| `GetOrCreate(string) (uint16, error)` | read; on miss write lock with double-check | existing or new token; `ErrEmptyName` for blank names |
| `Resolve(uint16) string` | read | token → string; `""` for 0 or out of range |
| `ResolveAll([]uint16) []string` | read | batch resolve (label and rel-type registries only) |
| `Lookup(string) (uint16, bool)` | read | existence check without creating |

Token 0 is reserved (`[0] = ""`); assignment starts at 1. Capacity is 65,535
tokens (`TokenCapacityMax`):

- label / rel-type `GetOrCreate` return an error when full;
- property-key `GetOrCreate` returns token **0** (one warning,
  `property-key registry full — falling back to raw key on wire`), and the
  wire encoder writes the raw key for token 0, so a property write never fails
  on registry exhaustion.

Each registry logs one `slog.Warn` once a token at or above 60,000 is assigned:
`label registry approaching capacity` / `reltype registry approaching
capacity` / `property-key registry approaching capacity` with attributes
`count` and `max`.

Persistence: one meta key per registry (§10.4), `msgpack([]string)` indexed by
token value; the tiered store keeps label and type names in
`registry.msgpack`.

---

## 7. Graph Layer: String Resolution

Implementations: `*core.NodeOps` / `*core.RelOps`
(`pkg/graph/internal/core/resolution.go`) and `*core.ResolveOps` (`shadow.go`);
public sub-APIs `pkg/graph/{nodes,rels,resolve}/api.go`.

```go
func (a *nodes.API) Labels(n *types.Node) []string
func (a *nodes.API) PrimaryLabel(n *types.Node) string
func (a *nodes.API) HasLabel(n *types.Node, label string) bool  // false for unregistered or malformed names
func (a *rels.API)  Type(r *types.Relationship) string
func (a *rels.API)  HasType(r *types.Relationship, typ string) bool
func (a *resolve.API) NodeProperty(n *types.Node, key string) (any, bool)
func (a *resolve.API) RelProperty(r *types.Relationship, key string) (any, bool)
```

`NodeProperty` / `RelProperty` dispatch on the `tkg_` prefix: shadow keys
resolve from metadata (§8), user keys delegate to `GetProperty`. `GraphTx` has
transaction-consistent twins (`Labels`, `RelType`, `NodeProperty`, …;
`tx_consistent_reads.go`). No public door returns a token;
a query engine matches per candidate (`HasLabel`, `HasType`) or scans by name
(`g.Nodes().ByLabel`, `g.Rels().ByType`, `g.Nodes().ByLabelAndProperty`).
There is no user-supplied entity identity: application identity is a property
looked up through `ByLabelAndProperty` (api.md).

---

## 8. Shadow Properties

21 shadow keys (`pkg/types/shadow.go`). `tkg_id`, `tkg_start_id` and
`tkg_end_id` do not exist: internal IDs are not user-meaningful.

| Shadow Key | Resolved From | Type | Applies To | Category |
|------------|--------------|------|-----------|----------|
| `tkg_labels` | label tokens → strings | `[]string` | Node | Structural |
| `tkg_type` | type token → string | `string` | Relationship | Structural |
| `tkg_valid_from` | `Temporal.ValidFrom` | `Instant` | Both | Temporal |
| `tkg_valid_to` | `Temporal.ValidTo` | `Instant` | Both | Temporal |
| `tkg_tx_from` | `Temporal.TxFrom` | `Instant` | Both | Temporal |
| `tkg_tx_to` | `Temporal.TxTo` | `Instant` | Both | Temporal |
| `tkg_created_at` | `Temporal.CreatedAt` | `Instant` | Both | Temporal |
| `tkg_updated_at` | `Temporal.UpdatedAt` | `Instant` | Both | Temporal |
| `tkg_deleted_at` | `Temporal.DeletedAt` | `Instant` | Both | Temporal |
| `tkg_created_by` | `Temporal.CreatedBy` | `string` | Both | Provenance |
| `tkg_updated_by` | `Temporal.UpdatedBy` | `string` | Both | Provenance |
| `tkg_version` | `version` | `uint32` | Both | Provenance |
| `tkg_hash` | `Integrity.Hash` | `string` | Both | Integrity |
| `tkg_prev_hash` | `Integrity.PrevHash` | `string` | Both | Integrity |
| `tkg_from_hash` | `RelIntegrity.FromNodeHash` | `string` | Relationship | Integrity |
| `tkg_to_hash` | `RelIntegrity.ToNodeHash` | `string` | Relationship | Integrity |
| `tkg_author_id` | `Integrity.AuthorID` | `string` | Both | Provenance |
| `tkg_signature` | `Integrity.Signature` | `[]byte` | Both | Integrity |
| `tkg_authorized_by` | `Integrity.AuthorizedBy` | `string` | Both | Authorization |
| `tkg_auth_level` | `Integrity.AuthorizationLevel` | `uint8` | Both | Authorization |
| `tkg_base_entity` | `Temporal.BaseEntityID()` | `types.EntityID` | Both | Version chain |

Resolution rules:
- `tkg_created_at` falls back to `ID().MintInstant()` (§3.3) when the row has
  no `CreatedAt`; the other temporal keys return the raw stored value, `(nil,
  false)` without temporal metadata.
- Stored valid-time values are read on the store's valid axis (api.md "The
  valid axis"): on the legacy axis (`UntimedFromTxFrom`, `UntimedUnbounded`) a
  stored `ValidFrom` of 0 is "no claim" (the row starts at its `TxFrom`) and an
  untimed first version under `UntimedUnbounded` stores
  `types.UnboundedValidFrom` (`math.MinInt64`); on the literal axis
  (`UntimedWholeAxis`) every stored value is the instant it names, the open end
  is `types.InfiniteValidTo` (`math.MaxInt64`).
- Shadow keys as write inputs (provenance, authorization, temporal, the
  `AllowTxBackfill`-gated `tkg_tx_from`): architecture.md "Shadow Properties (21)"
  and api.md (`AddWithTx`, `UpdateWithTx`, `DeleteWithTx`).

---

## 9. Serialization

### 9.1 Msgpack Wire Formats

Source: `pkg/graph/internal/storeutil/wire.go`.

```go
type NodeWire struct {
    FormatVersion uint8          `msgpack:"fv,omitempty"` // per-row format version (§9.1a); absent = 1
    ID            int64          `msgpack:"id"`
    PrimaryLabel  int            `msgpack:"pl"`
    ExtraLabels   []int          `msgpack:"el,omitempty"`
    Properties    []PropertyWire `msgpack:"p,omitempty"`
    Version       int            `msgpack:"v"`
    HasTemporal   bool           `msgpack:"ht,omitempty"`
    ValidFrom     int64          `msgpack:"vf,omitempty"`
    ValidTo       int64          `msgpack:"vt,omitempty"`
    TxFrom        int64          `msgpack:"tf,omitempty"` // written as the fixed trailing slot (§9.1a)
    TxTo          int64          `msgpack:"tt,omitempty"` // written as the fixed trailing slot
    CreatedAt     int64          `msgpack:"ca,omitempty"`
    UpdatedAt     int64          `msgpack:"ua,omitempty"`
    DeletedAt     int64          `msgpack:"da,omitempty"`
    Retracted          bool   `msgpack:"rx,omitempty"` // retraction tombstone; only with da != 0
    Void               bool   `msgpack:"vd,omitempty"` // excise void piece
    DeleteKeptValidTo  bool   `msgpack:"dk,omitempty"` // delete tombstone that kept its ValidTo; only with da != 0
    CreatedBy          string `msgpack:"cb,omitempty"`
    UpdatedBy          string `msgpack:"ub,omitempty"`
    BaseEntityID       int64  `msgpack:"be,omitempty"`
    Hash               string `msgpack:"h,omitempty"`  // 32 raw bytes on the wire at fv >= 4
    PrevHash           string `msgpack:"ph,omitempty"`
    AuthorID           string `msgpack:"aid,omitempty"`
    Signature          []byte `msgpack:"sig,omitempty"`
    AuthorizedBy       string `msgpack:"aby,omitempty"`
    AuthorizationLevel uint8  `msgpack:"al,omitempty"`
}

type RelWire struct {
    FormatVersion uint8          `msgpack:"fv,omitempty"`
    ID            int64          `msgpack:"id"`
    RelType       int            `msgpack:"rt"`
    StartID       int64          `msgpack:"s"`
    EndID         int64          `msgpack:"e"`
    Properties    []PropertyWire `msgpack:"p,omitempty"`
    Version       int            `msgpack:"v"`
    // temporal, rx/vd/dk, provenance and integrity fields as NodeWire, plus:
    FromNodeHash  string         `msgpack:"fnh,omitempty"` // start-node hash at write time
    ToNodeHash    string         `msgpack:"tnh,omitempty"` // end-node hash at write time
}

type PropertyWire struct {
    Key           string `msgpack:"k,omitempty"`  // empty when KeyToken carries the key
    KeyToken      uint16 `msgpack:"kt,omitempty"` // property-key token; 0 = raw Key on the wire
    Value         any    `msgpack:"v"`
    Type          byte   `msgpack:"t"`            // property type tag (exact Go type on decode)
    Nil           bool   `msgpack:"n,omitempty"`  // typed nil slice/map marker
    CustomType    string `msgpack:"ct,omitempty"` // registered custom struct type
    CustomPointer bool   `msgpack:"cp,omitempty"` // original form was *T
}
```

All three types have hand-written `EncodeMsgpack` implementations
(`wire_encode.go`; no reflective omitempty checks). A new field must be added
to the custom encoder as well as the struct tag (lesson 39).

Nested property values whose Go kind msgpack would lose (inside `[]any` /
`map[string]any`) are written as marker-led arrays: `["\x00tkg.tv", kind, iso]`
for a `TemporalValue`, `["\x00tkg.k", tag, payload]` / `["\x00tkg.k", tag]` for
a typed scalar/container or typed nil, `["\x00tkg.c", type, pointer, msgpack]`
for a registered struct (`wire_nested_temporal.go`). They carry no `fv` bump;
older rows decode as before.

Decode-time validation (semantic `NodeWire` / `RelWire` invariants, property
tags, `float32` round-trip, custom types, nil encode inputs) and write-boundary
validation (finite valid ranges `ValidFrom < ValidTo`, token 0, positive IDs,
relationship indexed identity): persistence.md "Persistence (Badger)",
architecture.md "Type-Tagged MsgPack Serialization", api.md (Store contracts).

### 9.1a Wire Format Versioning

Two levels carry the version. Markers, the one tracker, which release reads
which marker and the upgrade path: persistence.md "Format versions and
markers" and "Upgrading a store".

- **Per row**: `fv` (uint8). Absent (rows before row versioning) is 1, layout
  identical to 2. A row is written at the highest `fv` its features need
  (`emittedRowFormatVersion`). Every decoder (checked, selection-scope partial
  and its scanner, delta-history `Meta`, import, change feed, replica apply)
  fails closed with `store.ErrWireFormatVersionUnsupported` on an `fv` above
  what it reads (now 6); a row is never zero-fill-misdecoded.
- **Per store**: the Badger meta key `wire_format_version` (big-endian
  uint16), verified at open before any row decode; tiered and sharded check it
  per shard. Store markers run 2–8 (`StoreFormatVersion` 3 …
  `KeptValidToStoreFormatVersion` 8); their meanings are persistence.md's.

Row format versions:

| `fv` | Constant | Meaning |
|------|----------|---------|
| 2 | `CurrentWireFormatVersion` | transaction-time tail `tf`/`tt` is a fixed-width, always-present trailing slot: the last two map entries, each a full int64 (`0xd3` + 8B), written even without temporal metadata (ADR-0006 §4.5). The ingest applier patches the stamped values in place (`PatchWireTemporalTail`, `wire_temporal_tail.go`). v1 and v2 decode transparently. |
| 3 | `RetractedWireFormatVersion` | the row carries `rx` (retraction tombstone) |
| 4 | `BinaryHashWireFormatVersion` | integrity hashes (`h`, `ph`, `fnh`, `tnh`) as 32 raw bytes (msgpack bin, 34B) instead of 64 hex characters (66B); decoders read both forms |
| 5 | `VoidWireFormatVersion` | the row carries `vd` (an excise void piece) |
| 6 | `KeptValidToWireFormatVersion` | the row carries `dk`; read since v4.62.0, written by no release yet |

A row's `fv` is the max over its features (6 > 5 > 4 > 3). A key an older
reader must not skip (`rx`, `vd`, `dk`, binary hashes) raises `fv`; an
optional key it may skip is added without a bump. Bump protocol: encoders
(lesson 39), decode path and marker logic move together. Checked validation
(`ValidateNodeWire` / `ValidateRelWire`) refuses `rx` or `dk` on a row without
`da`, `dk` on a close before `da`, and `vd` on a row that cannot be a void
piece (`checkVoidWire`).

Old data: retraction tombstones written by v4.49.1 (`fv=2` plus `rx`) still
decode as retractions, but a release that reads `fv <= 2` skips the unknown key
and answers them as a plain delete; upgrade every reader of such data. Measured
fail-closed behaviour of an old binary on `fv=3`:
`tasks/evidence/retraction-fv3/05-old-binary-fails-closed.txt`.

### 9.2 Integer Widths

| Token range | Bytes (`EncodeInt`) |
|-------------|-------|
| 0–127 | 1B |
| 128–255 | 2B |
| 256–65535 | 3B |
| 65536+ | 5B (uint32 room) |

`pl`, `el` elements, `rt` and `v` use `EncodeInt` (smallest form). `kt` uses
`EncodeUint16` (always 3B). IDs (`id`, `s`, `e`, `be`) and every instant
(`vf`, `vt`, `tf`, `tt`, `ca`, `ua`, `da`) use `EncodeInt64`, always 9B
(`0xd3` + 8B).

---

## 10. Badger Key Layout

Every key starts with a 1-byte prefix (`pkg/graph/internal/storeutil/keys.go`,
`property_index_key.go`, `temporal_index_key.go`). IDs are big-endian int64
(8B) and tokens big-endian uint16 (2B), so byte order is sort order. Assigned
prefixes: `0x01`–`0x0B`, `0x0D`, `0x0F`; `0x0C` and `0x0E` are unused.

| Prefix | Key | Value | Key size |
|--------|-----|-------|----------|
| `0x01` `KeyNode` | `<8B nodeID>` | `msgpack(NodeWire)` | 9B |
| `0x02` `KeyRel` | `<8B relID>` | `msgpack(RelWire)` | 9B |
| `0x03` `KeyLabel` | `<2B labelToken>/<8B nodeID>` | empty | 11B |
| `0x04` `KeyRelType` | `<2B relTypeToken>/<8B relID>` | empty | 11B |
| `0x05` `KeyOut` | `<8B start>/<2B type>/<8B end>/<8B relID>` | empty | 27B |
| `0x06` `KeyIn` | `<8B end>/<2B type>/<8B start>/<8B relID>` | empty | 27B |
| `0x07` `KeyHistNode` | `<8B nodeID>/<8B version>` | history row | 17B |
| `0x08` `KeyHistRel` | `<8B relID>/<8B version>` | history row | 17B |
| `0x09` `KeyChangeLog` | `<8B LSN>` | `tag(1B) ‖ msgpack(body)` | 9B |
| `0x0A` `KeyPropertyIndex` | `<2B propKeyToken>/<payload>/<8B nodeID>` | empty | var |
| `0x0B` `KeyTemporalIndex` | `<2B labelToken>/<8B from>/<8B nodeID>` | 8B big-endian TO instant | 19B |
| `0x0D` `KeyExAdj` | `<8B nodeID>/<1B dir>/<8B relID>` | empty | 18B |
| `0x0F` `KeyMeta` | `<name>` | per key (§10.4) | var |

### 10.1 Prefix scans

- all outgoing from node X: `0x05/<X>/`; of type T: `0x05/<X>/<T>/`;
  all incoming to Y: `0x06/<Y>/`;
- an entity's history in version order: `0x07/<id>/` (`HistNodePrefix`);
- the change-log in commit order: `0x09` (persistence.md "Change-log / op-log");
- a node's ex-adjacency per direction: `0x0D/<node>/<dir>/` (backlog 50; the
  relationships that named the node in a row its live adjacency no longer
  holds; `store.ExAdjacencyCapability`, api.md).

### 10.2 Value encodings

- **History** (`0x07`/`0x08`): a full row, or with `Config.HistoryDeltaEncoding`
  a delta led by the 1-byte `'D'` (`0x44`) tag (`wire_history_delta.go`); reads
  accept both. The anchor interval is pinned by the `history_anchor_interval`
  meta key (persistence.md "History delta encoding").
- **Property index** (`0x0A`, `PropertyIndexOnDisk`): `payload` is
  domain-tagged. Numeric: `0x01 | sortKey(8B) | subtypeTag(1B) | rawBits(8B)`,
  `sortKey` the order-preserving sign-flip of the float64 magnitude so every
  numeric width shares one ordered domain. Raw (string / bool /
  `TemporalValue`): `0x02 | canonical value-key bytes`. The key omits the
  label: definitions on two labels over one property key share rows
  (persistence.md "Persistent property index").
- **Temporal index** (`0x0B`, `TemporalIndexOnDisk`): `from` is
  order-preserving (sign bit flipped), so a prefix scan yields (From ASC, ID
  ASC) (persistence.md "Persistent temporal-index rebuild accelerator").

### 10.3 What is on disk and what is RAM

- Label and adjacency keys (`0x03`–`0x06`) are always written;
  `LabelIndexOnDisk` / `AdjacencyIndexOnDisk` answer reads from them instead
  of RAM maps.
- `PropertyIndexOnDisk` moves property-index entries into `0x0A`;
  `TemporalIndexOnDisk` keeps `0x0B` purely as a rebuild accelerator (the
  temporal index stays resident). Each keyspace is backfilled from current
  rows once, under its build marker (§10.4).
- Every other index (vector, relationship property, relationship-type
  temporal, high-frequency, composite) persists only its definition (§10.4);
  its contents are installed at open from the open image after a clean
  `Close`, else rebuilt from the rows (persistence.md "Clean reopen: keys plus
  the open image").
- RAM sidecars (history presence, history stamps, label / rel-type / property
  tx-membership) are built lazily from the keyspaces above and never get keys
  of their own; the property tx-membership sidecars travel in the open image
  (persistence.md "History rows: versions, lives and RAM sidecars").

### 10.4 Metadata

`0x0F/<name>` (`MetaKey(name)`):

| Name | Value |
|------|-------|
| `label_tokens`, `reltype_tokens`, `property_keys` | `msgpack([]string)`, index = token value (§6) |
| `wire_format_version` | 2B big-endian store format marker (§9.1a) |
| `history_anchor_interval` | pinned delta anchor interval (§10.2) |
| `last_lsn` | 8B big-endian change-log watermark (with `ChangeLog`) |
| `node_count`, `rel_count` | counters |
| `tx_commit_floor` | 8B big-endian newest `TxFrom`/`TxTo`/`DeletedAt` of every row written, first entry of each flush that carries rows; tiered keeps its other shards' floor in the reference shard's `tx_commit_floor_shards` (backlog 44; persistence.md "Durability and crash semantics") |
| `prop_indexes`, `rel_prop_indexes`, `temporal_index_defs`, `reltype_temporal_index_defs`, `high_frequency_index_defs`, `vector_index_defs`, `composite_index_defs` | index definitions |
| `property_index_on_disk_built`, `temporal_index_on_disk_built_v2` | one-shot build markers (`temporal_index_on_disk_built` is the pre-v4.54.0 marker, deleted by the rebuild) |
| `tkg_open_image`, `tkg_open_image_c<i>` | open image header and 512 KiB chunks (persistence.md "Clean reopen") |

Plus the graph layer's `MetaKVCapability` entries under the same encoding
(e.g. `schema_version`, `graph_epoch`, `tx_instant_floor`,
`unique_constraints`, `compacted_through_tx`, `id_slot_lease`,
`rel_exadj_built`, `untimed_valid_start`) and persisted column chunks
(`ColumnsOnDisk`). There is no `next_node_id` / `next_rel_id` key.

---

## 11. Open Sequence

1. Badger `New`: verify / stamp `wire_format_version` and
   `history_anchor_interval` before any row is decoded (§9.1a); load
   `property_keys` before the index rebuild (tokenized rows need it); learn the
   valid axis from the persisted untimed mode; install indexes from a trusted
   open image or rebuild them from the rows (persistence.md "Clean reopen");
   with `ChangeLog`, reseed the LSN allocator from `last_lsn`.
2. `graph.New`: validate the config, build the snowflake generators (stateless:
   no counter load, no max-key scan), load the label and rel-type registries
   from the store, raise the commit clock to `tx_commit_floor`.
3. Deserialized `NodeWire` / `RelWire` rows are complete entities; nothing is
   injected (§4).

---

## 12. Decisions Log

| # | Decision | Rationale |
|---|----------|-----------|
| 1 | `NodeID` = snowflake int64 behind an exported wrapper; layout hidden (§2) | Globally unique, time-ordered, no counter persistence, distributed-safe |
| 2 | `RelID` likewise | Same as `NodeID` |
| 3 | `labelToken uint16`, unexported | 65K labels sufficient, 1–3B on disk |
| 4 | `relTypeToken uint16`, unexported | Same as `labelToken` |
| 5 | Separate label, rel-type and property-key registries | Independent namespaces |
| 6 | Smallest-form msgpack integers for label/type tokens | Room for uint32 without migration |
| 7 | No `tkg_id`, `tkg_start_id`, `tkg_end_id` shadow keys | Internal IDs are not user-meaningful |
| 8 | 21 shadow keys | All resolve to user-meaningful metadata |
| 9 | Registry growth warning at 60K | Logged via slog, informational |
| 10 | `primaryLabel` + `extraLabels` split | No allocation for single-label nodes |
| 11 | Token 0 reserved as invalid | Zero-value detection without option types |
| 12 | Snowflake replaces atomic counters | No counter persistence or crash recovery; distributed deployment |
| 13 | **Node and Relationship are pure data structs with no back-references** | Self-contained, serializable, cacheable, testable; resolution is a presentation concern |
| 14 | Snowflake epoch 2026-01-01, 48-bit µs time field | Horizon 2034-12-02 (§3.3; the original 41-bit ms plan assumed 69 years) |
| 15 | Separate generators for nodes and relationships | Independent node fields, no cross-entity collision |

Store and graph boundary contracts (fail-closed validation, rollback,
lifecycle, tiered behaviour, event buses) are not design decisions of this
spec; their single home is architecture.md (System Overview, "Design invariants"), with tiered detail in
architecture.md "tiered.Store" / persistence.md "Tiered Persistence" and the
Store sentinels in api.md. Vector nearest-neighbour retrieval returns nodes in
ranked order; a backend may skip IDs that disappeared between index scan and
node fetch, but an unreadable candidate row is a storage error returned to the
caller.
