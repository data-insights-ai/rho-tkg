# rho-tkg v5 public API (v5.0)

The versioned public surface of the `github.com/data-insights-ai/rho-tkg/v5`
module and the `v5compat` module. Anything not listed here (everything under
`internal/`) is not API. This file is the owner of the surface: a test
(`v5/pkg/graph/docs_api_test.go`) fails when an exported identifier of
`pkg/graph` is missing below, so a new export is documented in the same commit.
Methods are written `Type.Method`.

Scope of v5.0: one partition, bound by `pkg/graph` over the graphstore and
raftlog seams. The distributed engine (`internal/dist`) is not part of it.

## Stability tiers

| Tier | Meaning | Packages and concepts |
|---|---|---|
| stable | no signature change inside v5.x | `pkg/graph` (all), `pkg/types`, `pkg/temporal`: Axis, Position, Bound, Scope, Limits, Allen, Integer, Rational, the position/scope codecs |
| may change in v5.x | no consumer imports it yet; signatures may move to sub-packages | `pkg/temporal`: point knowledge and confidence (`PointKnowledge`, `*PointKnowledge` constructors, `AppendPointKnowledge`, `DecodePointKnowledge`), opaque descriptors (`OpaqueDescriptor*`, `DescriptorID`, `DescriptorReference`, `DescriptorSupport`, `CorrelationID`, `PreserveOpaqueDescriptor`), regions (`Region`, `Scope` region pieces), lexicographic positions (`LexPosition`) |
| compat | v4 shaped doors over v5 for consumers that switch first | `v5compat` |

## pkg/graph

### Opening and writing

- `Open(ctx, Config) (*Graph, error)`, `Graph.Close`, `Graph.Update(ctx, func(Tx) error) (Receipt, error)`, `Graph.OpenSnapshot`.
- `Config`: `Dir` or `Memory`, `GraphID`, `DefaultAxis` and `KnowledgeAxis` (two different axes, `types.DefaultAxisBinding`), `Allocation{Seed}`, `Durability` (`DurabilityBuffered`, `DurabilityCommit`; a memory graph cannot be `DurabilityCommit`), `Limits` (`temporal.Limits`), property seeds `PropertySchema{Key, Kind, Unique}`. `Config.Validate`.
- `Tx` (interface, valid inside `Update` only): `CreateNode`, `CreateRel`, `Correct`, `End`, `AddLabel`, `RemoveLabel`, `Set`, `Unset`, `Retract`, `GetOrCreate`, `Reopen`, `SetMeta`. Ops: `CreateNode`, `CreateRel`, `Correct`, `End`, `AddLabel`, `RemoveLabel`, `Set`, `Unset`, `Retract`, `GetOrCreate`, `Reopen`; each has `Validate(temporal.Limits)` (`CreateNode.Validate`, `CreateRel.Validate`, `Correct.Validate`, `End.Validate`, `AddLabel.Validate`, `RemoveLabel.Validate`, `Set.Validate`, `Unset.Validate`, `Retract.Validate`, `GetOrCreate.Validate`, `Reopen.Validate`); `Op` is the closed interface, `CheckOp(Config, Op)` validates without an engine.
- Every op carries an explicit `Scope` (valid time) and an optional `Recorded` position on `Config.KnowledgeAxis`; zero `Recorded` means "at commit". There is no implicit "now".
- A key is declared by the first `Set` that writes it (add-only, same commit); a later value of another kind is `ErrInvalidValue`, an exact numeric converts (3.0 into an int key).
- IDs come from the engine allocator (`Config.Allocation`); `types.NodeID` / `types.RelID` are 1..MaxInt64. Nodes and relationships share one ID space.
- Import doors: `CreateNode.ID` / `CreateRel.ID` (zero = allocate) may name an ID in the reserved range `1..Allocation.Seed` (above it or negative: `ErrInvalidID`; an existing ID, also earlier in the same Update: `ErrAlreadyExists`). `Reopen` / `Tx.Reopen` starts the next life of the same ID over a scope outside every earlier life (`ErrInvalidScope` on overlap); later ops resolve to the life their scope falls in, a scope spanning two lives is `ErrInvalidScope`. A relationship refers to its endpoints by identity: its scope may extend beyond their lives; `ViewDeclared` shows all of it, `ViewEffective` only where both endpoints are present. `Tx.SetMeta(key, value)` writes an engine KV row (`MaxMetaKeyBytes`, `MaxMetaValueBytes`) in the same commit; `Snapshot.Meta(ctx, key)` reads it at the cut. A scan with no index walks the whole ID space `1..last minted`.

### Reading

- `SnapshotSpec` built by `Fresh()`, `Stable()`, `At(Cut)`, `After(Receipt)`, `AtKnown(types.Instant)` (the newest commit whose knowledge instant is at or before it; an earlier instant reads the empty graph; its `Cut` reopens with `At`); `SnapshotSpec.Validate`.
- `Snapshot`: `Snapshot.Scan`, `Snapshot.Lookup`, `Snapshot.Timeline`, `Snapshot.Resume`, `Snapshot.Meta`, `Snapshot.Cut`, `Snapshot.Close`.
- Requests: `ScanRequest`, `LookupRequest`, `TimelineRequest`, each with `Validate(temporal.Limits)` (`ScanRequest.Validate`, `LookupRequest.Validate`, `TimelineRequest.Validate`); `Selector` (`View`, `Valid`, `Known`, `Labels`, `RelType`, `Equals []PropertyEq`; `Selector.Validate`, `CheckSelector(Config, Selector)`); `Projection`; `PageLimits` (`Rows`, `MaxBytes`; `PageLimits.Validate`); results `Page` and `Entity`.
- `View`: `ViewDeclared`, `ViewEffective`. Every read names its view, its valid scope and its knowledge scope.
- Tokens are opaque, serialisable and minted by the engine only: `Receipt` (`Receipt.Bytes`, `Receipt.IsZero`, `Receipt.KnowledgeInstant` (the commit's strictly increasing knowledge instant on `Config.KnowledgeAxis`), `ParseReceipt`), `Cut` (`Cut.Bytes`, `Cut.IsZero`, `ParseCut`), `Cursor` (`Cursor.Bytes`, `Cursor.IsZero`, `Cursor.Validate`, `ParseCursor`).
- `Ref`: `NodeRef`, `RelRef`, `Ref.IsRel`, `Ref.Node`, `Ref.Rel`, `Ref.Validate`.

### Values

`Value` is typed, never `any`. `ValueKind`: `KindInvalid`, `KindNull`, `KindString`, `KindBool`, `KindInt`, `KindFloat`, `KindBytes`, `KindList`. Constructors `Null`, `String`, `Bool`, `Int`, `Float` (refuses NaN and Inf), `Bytes`, `List` (flat, no null, no nesting). Accessors `Value.Kind`, `Value.IsNull`, `Value.AsString`, `Value.AsBool`, `Value.AsInt`, `Value.AsFloat`, `Value.AsBytes`, `Value.AsList`; `Value.Validate`; `Value.Equal` is Cypher equality across widths (`3 == 3.0`, `[3] == [3.0]`, bytes bytewise, `nil == empty`).

### Limits (constants)

| Constant | Value | Meaning |
|---|---|---|
| `MaxPageRows` | 4096 | rows per page |
| `MaxLookupIDs` | 4096 | IDs per Lookup |
| `MaxCursorBytes` | 4096 | cursor encoding |
| `MaxTokenBytes` | 4096 | Receipt / Cut encoding |
| `DefaultMaxBytes` | 1 MiB | default page byte limit |

One `Update` is bounded by the store stage-record limit (about 4096 records; a created node with a property writes several): a larger Update is `ErrInvalidRequest`, nothing is written. `temporal.Limits` (`DefaultLimits`): input 65536 B, magnitude 4096 bits, value 65536 B, region pieces 4096, descriptor 65536 B; hard ceilings 1 MiB / 65536 bits / 65536 pieces.

### Sentinels

Match with `errors.Is`.

| Sentinel | Meaning |
|---|---|
| `ErrNotImplemented` | door not built (provenance projection; no engine attached) |
| `ErrInvalidConfig`, `ErrInvalidScope`, `ErrInvalidID`, `ErrInvalidBound`, `ErrInvalidOp`, `ErrInvalidSelector`, `ErrInvalidSnapshotSpec`, `ErrInvalidValue`, `ErrInvalidCut`, `ErrInvalidReceipt`, `ErrInvalidCursor`, `ErrInvalidRequest` | argument refused before any work; `ErrInvalidID` is also `types.ErrInvalidID` |
| `ErrClosed` | nil, zero or closed `Graph` / `Snapshot` |
| `ErrTxDone` | `Tx` used after its `Update` returned |
| `ErrNotFound` | target absent |
| `ErrAlreadyExists` | explicit `CreateNode.ID` / `CreateRel.ID` already exists |
| `ErrConflict` | write conflict |
| `ErrUniqueViolation` | unique claim held by another node |
| `ErrCommitNotDurable` | commit accepted, durability not confirmed |
| `ErrSnapshotExpired` | snapshot cut no longer readable |

`pkg/temporal` sentinels: `ErrInvalidLimits`, `ErrResourceLimit`, `ErrInvalidValue`, `ErrZeroDenominator`, `ErrInvalidAxis`, `ErrUnknownProfile`, `ErrUnknownVersion`, `ErrIncompatibleDomain`, `ErrAxisMismatch` (see `pkg/temporal/errors.go` for the full set). `pkg/types`: `ErrInvalidID`.

## pkg/types

`NodeID`, `RelID` (`NewNodeID`, `NewRelID`, `ParseNodeID`, `ParseRelID`; zero refused with `ErrInvalidID`), `GraphID`, `DefaultAxisBinding` (`NewDefaultAxisBinding`, `BindDefaultAxis`), `Instant` (`InstantPosition`, `InstantFromPosition`; integral milliseconds, compatibility value).

## pkg/temporal

Exact coordinates and their axis identity: `Axis`, `AxisID`, `AxisDescriptor`, `NewAxis`, `Profile`; `Position` (`IntegerPosition`, `RationalPosition`, `LexPosition`), `ComparePositions`, `ConvertUnits`; `Integer`, `Rational`, `Fraction`, `ParseInteger`, `ParseRational`; `Bound` (`FiniteBound`, `NegativeInfinity`, `PositiveInfinity`, `BoundKind`); `Scope` (`Empty`, `All`, `Unplaced`, `Point`, `Span`, `Region`, `ScopeKind`); Allen relations (`ClassifyAllen`, `AllenRelation`); `Limits`, `DefaultLimits`; codecs `AppendScope`, `DecodeScope`, `ScopeHash`, `AppendPosition`, `DecodePosition`, `PositionHash`. Infinity is a `Bound`, never MinInt64 or 0; zero is a coordinate. The "may change" concepts are listed in the tiers table.

## v5compat

Separate module (`github.com/data-insights-ai/rho-tkg/v5compat`) mirroring the 14 v4 import paths (about 106 doors) over `v5/pkg/graph`, so a consumer switches by changing the import root. Declined sub-APIs (Stats, Index, Constraints, Admin, Replication, IO, Events, Ingest) compile and return `ErrNotSupportedInV5` (`errors.Is`). Tuning `Config` fields are accepted and ignored. Bytes and flat lists are mapped. Compat profile: 72 pass / 0 fail / 0 skipped.

## Known v5.0 limits

Documented, not defects; each names the work package that lifts it.

- `v5compat` keeps a side commit ledger, stores `~tf` / `~dt` shadow time properties, and commits per instant (WP-35 removes the per-instant commits and the ledger using `AtKnown` and `Receipt.KnowledgeInstant`).
- `v5compat` `GetOrCreate` serialises with an in-process lock only (WP-34 adds the directory lock and runtime uniqueness).
- `v5compat` Stats and `Tx.Begin` are declined (WP-35).
- `Selector.Known` is an upper bound ("known by t"; lower bound ignored): a revision is known at its latest `Recorded` instant, else its commit knowledge instant; an entity shows the state after its longest known revision prefix. A bound below the view's newest commit bypasses the index pages (full scan). Scan serves label and equality pages with a per-candidate recheck. Under `ViewEffective` a relationship under a `Known` bound reads its endpoints at the relationship's own known index, not at each endpoint's known prefix (v5.0 limit, follow-up WP-30c). Stores created before this branch (no revision rows, no commit-instant rows) do not open; there is no migration before release. Range moves refuse a source holding revision rows until they are carried (WP-28).
- Op log and range-move export refuse caller-instant evidence (`Recorded`), and the distributed codecs refuse `LifeEnd`, Bytes and List until WP-28; v5.0 is one partition.
- `OpenSnapshot` has no partition or graph read scope; `SnapshotSpec` keeps unexported fields so one can be added.
- Retired index markers accumulate until compaction (WP-08 note).
- A `Recorded` position must lie on `Config.KnowledgeAxis` and not after the commit instant (`GraphLimits.RecordedCeiling`).
- Lists are flat (no null, no nesting); maps and temporal property values are not a `Value` kind.
- Names in `pkg/graph` stay drafts for API review until v5.0.0-beta.1.

## Evidence tooling outside the v5.0 gate

`tools/v5-substrate` (cgo FoundationDB, not buildable without `fdb_c.h`), `tools/vendor-compare` (needs Docker and environment) and `tools/graph-compare` (reference-pin and v4 baselines) are evidence tooling. They are not part of `make -C v5 check` or the v5 workflow.
