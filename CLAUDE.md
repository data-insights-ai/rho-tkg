# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Session Protocol

- **Session start**: Read `tasks/lessons.md` and `CHANGELOG.md` (source of truth for version history) before doing any work
- **Before planning**: Read the full API of ALL files involved in the change — not snippets around suspected bug locations. Understanding the complete API prevents plans based on wrong assumptions
- **Challenge the plan**: Before implementing any method, ask: "Does this algorithm deliver what the method name promises?" and "Does this interact with an existing feature that could break it?"
- **After each implementation step**: Update all affected documentation (CLAUDE.md, README.md, CHANGELOG.md, docs/architecture.md, docs/SPEC.md). If not specified whether changes belong to the current version or a new version in CHANGELOG.md, ask the user before writing
- **After corrections**: Update `tasks/lessons.md` with the pattern and a rule to prevent recurrence
- **Session end**: Update `tasks/lessons.md` with new lessons

## MR Review Protocol

Execute these three phases in order when reviewing a merge request.

### Phase 1 — Correctness of the MR itself

1. Read the full diff: `git diff origin/main...origin/<branch>`.
2. Verify each new method/test delivers what its name promises; check edge cases the author may have assumed away.
3. Lessons & CHANGELOG hygiene:
   - New lesson entries use the correct next sequential number (`grep '^## B' tasks/lessons.md`).
   - No duplicate lesson body (same title or same code pattern).
   - CHANGELOG section (`[Unreleased]` or explicit version) is above the current latest release, not above an older one.
4. Test quality (apply all 17 rules in "Testing Rules"):
   - Two-phase tests for every temporal/history-aware method (rule 15).
   - Adversarial scenarios with exact-set assertions, not just happy-path (rule 16).
   - Negative assertions: "must NOT contain Y" and phantom-value returns-empty cases.
   - For interval queries: the "predicate held during part of interval but not on most-recent version" case must be asserted.
   - Sentinel errors tested with `errors.Is` at every call layer (rule 4).
5. Run `make test-race` on the branch. A suite that fails for one backend is not mergeable.

### Phase 2 — Is the issue also present elsewhere?

1. Grep for the same pattern across the whole codebase (see "Audit Checklists" and lesson A1).
2. Check symmetric types: Node and Relationship are structural mirrors; same for `Get*ValidAt` (named) vs `*By*(opts QueryOpts)` (generic) — rule 17.
3. Check all Store implementations: a fix to `MemoryStore` must be checked against `BadgerStore` and `TieredStore`, and vice versa.
4. Check batch paths: standalone mutation fixes must also land in `BatchBuilder` paths.

### Phase 3 — Are the tests sufficient?

1. `make cover`. Every new public method must appear in coverage. No new code below 80%.
2. Missing scenarios:
   - Cross-shard relationships (for TieredStore) — not just same-shard.
   - Deleted entities: history must be queryable after deletion (B32).
   - Concurrent access: if shared state is touched, confirm a `test-race` run exists.
   - Cold→warm shard transitions: use `demoteToCold` helper, not sub-second `ShardWindow`.
3. For each test ask: "If the implementation silently returned current state instead of historical state, would my assertions fail?" If not, the test is happy-path regardless of coverage.
4. State whether the MR is mergeable. List blocking and non-blocking issues separately.

## Project Overview

**Temporal Knowledge Graph v4** — internal Go library providing the core graph engine for temporal knowledge graphs. Pure library (no main binary, no HTTP server, no query language).

Module: `github.com/data-insights-ai/rho-tkg/v4`
Go: 1.26.1 | License: Apache-2.0
Dependencies: `rho-snowflake-2026` (IDs), `msgpack/v5` (serialization), `badger/v4` (persistence)
Status: v4.12.1 — patch: platform-independent float64 bounds in `types.CoerceInstant` (amd64/arm64 divergence at 2^63, caught by CI). Prior: v4.12.0 — major additive release: bitemporal correctness program (single chain-resolver seam, generative oracle harness, `QueryOpts.TxPin` belief-state pins, pinned adjacency `OutgoingForNodesAtTx`/`IncomingForNodesAtTx`; fixed three latent defects incl. the badger commit-window read-consistency drop, lessons 62-64); unique property constraints (`UniqueCurrent`+`UniqueForever`, `GetOrCreateByKey`); history compaction with tamper-evident stubs (`ErrHistoryCompacted`); HNSW vector search; on-disk property index; encryption-at-rest; one-call backup/restore; CDC `Replication().Watch`; Go iterators; `graph.Open` + config profiles; NDV planner stats; `time.Time` properties; FULL TIERED PARITY (change-log with barriered W-bounded feed, DocValues, NDV fold, compaction, unique constraints — tiered primaries replicate byte-exact); store capability facets + `CapabilitiesOf`. See `CHANGELOG.md` `[4.12.0]` for the complete list.

See `CHANGELOG.md` `[4.0.0]` for the v3.4.0 → v4.0.0 migration recipe; `[4.1.0]` for the tx-isolation change (Path B); `[4.2.0]` for the field→method conversion; `[4.2.1]`/`[4.2.2]` for the consumer-ergonomics alias additions.

**Stdlib aliasing convention.** `pkg/graph/hash` and `pkg/graph/io` shadow stdlib `hash` and `io`. Inside the local package no aliasing is needed. At consumer sites that import BOTH stdlib AND the local package, alias the LOCAL one with a `tkg` prefix (`tkghash` / `tkgio`) — leave stdlib unaliased.

## Build & Test Commands

```bash
make build          # verify compilation
make test           # unit tests (short mode, no cache)
make test-v         # verbose tests
make test-race      # race detector — always run for concurrent code
make test-integration  # integration tests (long-running)
make cover          # coverage report -> coverage.html
make check          # pre-commit: vet + build + test
make ci             # full pipeline: fmt-check + vet + lint + build + test-race + security + vulncheck + cover-gate
make fmt            # format code
make lint           # golangci-lint (errcheck, govet, staticcheck, revive, ...)
make security       # gosec static analysis
make vulncheck      # govulncheck for known CVEs
```

### Running lint/security/vulncheck via Docker (they are NOT installed locally)

`golangci-lint`, `gosec`, and `govulncheck` are usually absent from the host, so
`make lint` / `make security` / `make vulncheck` fail with "command not found".
**Do NOT report the gate as un-runnable — Docker is always available.** Run the
tools inside the go.mod-matching toolchain image (guarantees Go-version
compatibility; the pre-built tool images may bundle an older Go). Dedicated
targets do this with cached named volumes (fast after the first run):

```bash
make lint-docker        # golangci-lint v2 (reads .golangci.yml) in golang:<go.mod>
make security-docker    # gosec
make vulncheck-docker   # govulncheck
make ci-docker          # full gate: fmt-check + vet + lint-docker + build + test-race + security-docker + vulncheck-docker + cover-gate
```

`GO_IMAGE` auto-tracks the `go` line in `go.mod` (`golang:1.26.1`). The raw form,
if you need it ad hoc:

```bash
docker run --rm -v "$PWD":/src -w /src \
  -v rho-tkg-gocache:/go -v rho-tkg-buildcache:/root/.cache/go-build \
  golang:$(awk '/^go /{print $2}' go.mod) \
  sh -c 'go install golang.org/x/vuln/cmd/govulncheck@latest && govulncheck ./...'
```

When reviewing a change, run the gate and then **filter the findings to the files
the change actually touched** (`git diff --name-only`) — the repo carries a
pre-existing baseline (a stdlib-only `govulncheck` vuln fixed in a later Go patch,
some `#nosec`-worthy `gosec` G115s, and ~39 `golangci-lint` findings), so "the gate
is non-empty" is not "the change is dirty". Report only findings inside the diff.

Single test: `go test -run TestFoo ./pkg/types/`
Coverage check: `go tool cover -func=coverage.out` (after `make cover`)

## Testing Rules (hard requirements)

These rules exist because every single one was violated at least once. Do not skip them.

1. **Every public method gets a direct test.** Indirect coverage via delegation does NOT count. Any public method at 0% is a blocker.
2. **Node and Relationship must have test parity.** These types are structural mirrors.
3. **Every type-switch branch gets its own test.** If a branch shows 0% in `cover`, add a test.
4. **Sentinel errors are tested with `errors.Is`, not just `err != nil`.** At every call layer.
5. **Fallback/reflect paths must be tested or removed.**
6. **Deep copy means deep copy.** Must truly clone all nested reference types.
7. **Run `make cover` before marking any step complete.** Any public method at 0% or new code below 80% is a blocker.
8. **Validation must be recursive/adversarial.** `[]any{&myStruct{}}` must be rejected.
9. **Test nil values in reflect-based code.** `reflect.ValueOf(nil)` returns zero `reflect.Value`. `SetMapIndex` with zero Value deletes the key.
10. **One-time warnings must use `sync.Once`.** Never use `>=` for one-shot warnings.
11. **No empty stubs when the spec defines the fields.**
12. **Public method return types must not leak dependencies.** Use `type nodeID snowflake.ID`, NOT `type nodeID int64`.
13. **Config fields must be used or removed.**
14. **DO NOT use sub-millisecond or millisecond `ShardWindow` in tests.** ShardWindow boundaries are truncated to millisecond precision for alignment with snowflake timestamp extraction, so sub-second windows create boundary gaps. Use 1-week window (`newTestTieredStore`) and test cold/warm via manual rotation + `demoteToCold` helper.
15. **History-aware code needs two-phase tests.** Any method that answers a question about the past, a different version, or a comparative state (`*ValidAt`, `*ValidDuring`, `*AsOf`, `Verify*Chain`, `Snapshot`, `Diff`, `*At`, or any `*ByLabel`/`*ByType` accepting temporal `QueryOpts`) requires at least one test that (1) creates an entity in state X at t0, (2) mutates it after t0, (3) queries with t = t0 and asserts the result reflects X, not the post-mutation state. Single-mutation tests verify the API exists; only mutation-then-query tests verify it remembers. See `tasks/lessons.md` B30 (per-version metadata) and B31 (two-phase tests).
16. **Adversarial test shape, not happy-path.** Multi-entity scenarios with diverging lifecycles. Exact-set assertions (`assertNodeSet`/`assertRelSet`) catch over-reporting and omission. Negative assertions ("must NOT contain Y", "phantom value returns empty"). For interval queries, the "predicate-anywhere-in-interval" case (a node whose label held during part of the interval but not on the most-recent version) MUST be one of the asserted scenarios.
17. **When fixing a temporal-flavored named method, audit the generic equivalent.** Two doors, same shape: a fix in `Get*ValidAt(...)` (named method) without the matching fix in `*By*(opts QueryOpts)` (generic with temporal opts) leaves the bug behind a different door. Grep for `g\.store\.\w+ByLabel\|g\.store\.\w+ByType` in any file you touch — that's the generic-API door.

## Architecture

### `pkg/types`

| File | Purpose |
|---|---|
| `node.go` | Node (graph vertex, 80B) — `nodeID` wrapping `snowflake.ID`, labels as `labelToken`, properties, version, temporal, integrity |
| `relationship.go` | Relationship (directed edge, 72B) — `relID`, `relTypeToken`, start/end as `nodeID`, properties, version, temporal, integrity |
| `propertyslice.go` | Sorted key-value store with binary search; recursive allowlist validation; depth-limited to 32 levels; `[]float32` support; `deepCopyValue` dispatches to `DeepCopier.DeepCopyValue()` for registered types before the generic type switch |
| `property_registry.go` | `RegisterPropertyStructType(v any) error` — validates `HashableValue` + `DeepCopier` at registration; `DeepCopier` interface (`DeepCopyValue() any`); `ErrTypeNotHashable`, `ErrTypeNotDeepCopyable` sentinels |
| `shadow.go` | Constants for virtual read-only `tkg_*` properties |
| `temporal.go` | `Instant` type (Unix ms), `entityID`, `TemporalMetadata` struct |
| `integrity.go` | `NodeIntegrity` / `RelIntegrity` — hash chain (`Hash`, `PrevHash`) |
| `allen.go` | Allen's 13 interval relations — `AllenRelation`, `AllenRelationSet`, `Relate()`, `Compose()`, `ComposeSets()`, composition table |
| `granularity.go` | `TimeGranularity` (8 levels), `TruncateInstant`, `RoundInstant`, `CeilInstant` — ISO 8601 week truncation |
| `recurrence.go` | `RecurrencePattern`, `RecurrenceFrequency`, `WeekdayMask`, `Interval` — `Validate()` + `Expand(from, to)` |

### `pkg/graph` (thin façade)

`Graph` holds `core *core.Core` + 15 unexported sub-API pointers. The exported surface is 15 accessor methods (`g.Nodes()`, `g.Temporal()`, etc.). Customers interact via the sub-APIs (`g.Nodes().Add(...)`, `g.Temporal().NodesAt(...)`, etc.); the old direct `g.AddNode(...)` form was removed in v3.4.0 and the field form (`g.Nodes`) was removed in v4.2.0.

| File | Purpose |
|---|---|
| `graph.go` | Thin façade — `Graph` struct with `core *core.Core` + 15 unexported sub-API pointers. Methods: `New`, `Close`, 15 nil-safe sub-API accessor methods (`Nodes() *nodes.API`, etc.), and `SetReplicationSource`. Plus `Config`, `ValidationLimits`, `IDComponents`, `ConstraintSet` type aliases re-exported from internal/core. |
| `subapi.go` | `TxAPI` and `BatchAPI` — sub-API accessors for `g.Tx` and `g.Batch`. Live in `pkg/graph` itself (not a sibling) because they wrap the pkg/graph-private `*GraphTx` / `*BatchBuilder` types defined inside `pkg/graph/internal/core`. `TxAPI.Run` / `TxAPI.RunContext` add closure-style helpers. |
| `errors.go` | Public sentinel re-exports — store sentinels (`ErrNodeNotFound`, … 12 entries), vector-index sentinels, registry sentinels (`ErrEmptyName`, `ErrRegistryNotEmpty`), index-provider sentinels. Canonical declarations in `internal/core/core.go`. |
| `subapi_smoke_test.go` | Compile-and-run smoke test exercising every sub-API accessor end-to-end. |
| `doc.go` | Package documentation. |

### Sub-API & types packages under `pkg/graph/`

Each sub-API package declares a local `Ops` interface listing only the methods its wrappers forward to. `*core.Core` (in `internal/core`) satisfies each interface implicitly. Wrappers are 1–2 lines. Some packages (temporal, index, events) export both the sub-API and the public types customers reference; others are pure types-only or pure wrapper-only. Customer-facing names use the field on Graph (column 2), not the import path.

| Package | Field on Graph | Purpose |
|---|---|---|
| `pkg/graph/nodes` | `g.Nodes` | Node CRUD, label, property, version chain (~35 wrappers). Includes `AddWithTx` (§4.1 transaction-time backfill, gated by `Config.AllowTxBackfill`). |
| `pkg/graph/rels` | `g.Rels` | Relationship CRUD, adjacency, property, version chain (~37 wrappers). Includes `AddWithTx` (§4.1). |
| `pkg/graph/temporal` | `g.Temporal` | Point-in-time, interval, bitemporal, snapshot/diff, Allen relations, named as-of tags (~37 wrappers). Also exports `GraphSnapshot`, `SnapshotDiff`, `NodeUpdate`, `RelUpdate`, `TemporalConstraint`, `ConstraintSet`, 7 constraint sentinels. Named knowledge-time marks (§4.2): `TagAsOf` / `ResolveAsOf` / `AsOfTags` / `RemoveAsOfTag` (durable `asof_tags` MetaKV registry for `AS OF SYSTEM TIME $tag`). |
| `pkg/graph/index` | `g.Index` | Property/vector/high-freq index management + `SearchNearest` + provider registration (~13 wrappers). Also exports `IndexProvider`, `Initializable`, `GraphReader`, IndexProvider sentinels. |
| `pkg/graph/events` | `g.Events` | Sync/async EventBus management (~3 wrappers). Also exports `Event`, `EventType`, `EventPriority`, `EventHandler`, `EventBus`, `AsyncEventBus`, `BackpressureStrategy`, constructors. |
| `pkg/graph/constraints` | `g.Constraints` | Temporal-constraint set management (~3 wrappers). |
| `pkg/graph/io` | `g.IO` | Export / Import + delta backups: `Watermark()` (current change-log `Cursor{LSN,Epoch}`), `ExportSince(w, since)` (delta stream of changes after `since`), `ImportMerge(r, opts)` (replay a delta onto a base, verbatim+idempotent, with rollback), `HeaderOf(r)` (decode a stream's `DeltaHeader` without consuming records). Also exports `Cursor`, `MergeOptions`, `DeltaHeader`, `ErrCursorUnknown`, `ErrDeltaBaseMismatch`. Shadows stdlib `io` — alias `tkgio` at consumer sites. |
| `pkg/graph/admin` | `g.Admin` | Backend-agnostic admin: `Reset`, `DecomposeNodeID`, `DecomposeRelID`. |
| `pkg/graph/tier` | `g.Tier` | Tiered-store admin: `Archive`, `Restore`, `ForceRotate`, `ListShards`, `RebuildCatalog`, `Repair`, `VerifyShard`. Reuses `core.AdminOps` as its `Ops`. |
| `pkg/graph/stats` | `g.Stats` | Count helpers (~6 wrappers). |
| `pkg/graph/hash` | `g.Hash` | Hash-chain verification (~2 wrappers). Shadows stdlib `hash` — alias `tkghash` at consumer sites. |
| `pkg/graph/resolve` | `g.Resolve` | Shadow-property accessors: `NodeProperty`, `RelProperty`. |
| `pkg/graph/store` | — | `Store` interface, `QueryOpts`, `ShardDepth`, `RelTombstone`, `DistanceMetric`, `ChangeFeedCapability` + `ChangeRecord`/`ChangeTag` (optional op-log), `ChangeLogStatusCapability` (`ChangeLogEnabled()` — recording vs present-but-off), `TxChangeLogScope` (per-tx record buffer: a rolled-back tx/batch emits nothing), `ReplicationSource`/`RegistrySnapshot`/`IDSlotLeaseRecord` (replica refetch + failover), 30 store sentinels. The contract is one mandatory composition (`MandatoryStore` — the CRUD/adjacency/bulk-read/batch/history/stats/iteration core `Store` composes, plus four Store-embedded index capabilities) and ~18 OPTIONAL capabilities the graph layer type-asserts for at runtime. ADR-0003 groups the optionals into 5 composed FACETS (`IntegrityAccelerationFacet`, `HistoryAccelerationFacet`, `IndexAccelerationFacet`, `ChangeLogFacet`, `MetadataFacet` in `facets.go`) — pure interface compositions of the existing narrow interfaces (every name kept; zero API change) — plus `CapabilitiesOf(MandatoryStore) CapabilityReport`, a pure STRUCTURAL introspection for diagnostics/tooling (it does NOT reproduce the wrapper-visibility guard; the graph layer's cached handles remain authoritative for routing). Post-parity almost every optional is universally implemented in-tree; only 4 are genuinely declined somewhere (tiered declines `TransactionTimeQuery`/`HistoryRollbackTrim`; the two depth-iteration capabilities are tiered-only), so facets stay optional, never folded into `MandatoryStore`. See `docs/adr/0003-capability-consolidation.md`. |
| `pkg/graph/replication` | `g.Replication` | Change-log / op-log accessor: `ChangeFeed`, `ForEachChange`, `LastCommittedLSN` (forwards to `store.ChangeFeedCapability`; `ErrCapabilityNotSupported` when absent). Replica apply (Phase 1): `ApplyChange`/`ApplyChanges` (write the primary's wire verbatim, bypass the read-only gate), `AppliedLSN`/`SetAppliedLSN` (durable replica watermark), `RegistrySnapshot` (primary token registries + LSN, for the replica refetch hook; `*replication.API` satisfies `store.ReplicationSource`), `IDSlotLease`/`SetIDSlotLease` (durable failover slot hint). |
| `pkg/graph/store/memory` | — | `memory.Store`, `memory.New()`. |
| `pkg/graph/store/badger` | — | `badger.Store`, `badger.Config`, `badger.New()`. |
| `pkg/graph/store/tiered` | — | `tiered.Store`, `tiered.Config`, `tiered.New()`, `MigrateFromBadger`, `ShardInfo`, `VerifyResult`, `RepairResult`, 4 sentinels. |
| `pkg/graph/ontology` | — | `EntityClass`, `OntologyMapping`, `NewOntologyMapping`, `ClassEvent`, `ClassReference`. |

### `pkg/graph/internal/*`

| Package | Purpose |
|---|---|
| `internal/core` | `Core` type holding shared unexported state (mu, store, registries, locks, generators, indexProviders, …) plus 12 sub-Core types (`NodeOps`, `RelOps`, `TempOps`, `IndexOps`, `EventOps`, `AdminOps`, `ConstraintOps`, `HashOps`, `IOOps`, `ResolveOps`, `StatOps`, `ReplOps`) declared in `subops.go`. Sub-Core types hold a `c *Core` back-reference; method bodies live on the sub-Core types. Wired in `core.New` as exported fields (`c.Nodes`, `c.Rels`, …) so the wrapper packages can satisfy each sub-API's local `Ops` interface. Method names on sub-Core types drop their type prefix (`AddNode → NodeOps.Add`, `GetRelationship → RelOps.Get`, `VerifyNodeHashChain → HashOps.VerifyNodeChain`, etc.) so the call chain is uniform across the wrapper boundary. ~21K LOC of implementation across ~56 files; ~62K LOC of internal tests across ~142 test files. `g.Tier` reuses `c.Admin` (there is no `TierOps`). |
| `internal/snowflake` | Snowflake `Epoch`, `Layout`, `IDComponents`, `DecomposeID`. Single source of truth for ID-bit decomposition. Imported by `internal/locks`, `internal/storeutil`, `internal/core`, `pkg/graph/store/{badger,tiered}`. |
| `internal/storeutil` | Store-internal helpers: key encoding (`NodeKey`, `RelKey`, `LabelIndexKey`, etc.), msgpack wire types (`NodeWire`, `RelWire`), pagination helpers (`PaginateIDs`, `PaginateNodes`, etc.), temporal-filter push-down (`EntityValidFrom`, `MatchesTemporalFilter`). The public Store contract lives in `pkg/graph/store`. |
| `internal/locks` | `Manager` — 256-shard entity-lock manager, `LockEntity`/`LockTwo`/`LockMany` in ascending order. |
| `internal/registry` | `LabelRegistry`, `RelTypeRegistry`, and `PropertyKeyRegistry` — thread-safe string-to-uint16 token registries (`AppendNames`/`RollbackNames`/`ImportNames` grow/restore primitives). Internal — not public API. |
| `internal/index` | In-memory indexes only: property index, vector index, high-frequency temporal index, `OntologyMapping`. The label/reltype registries live in `internal/registry`. |
| `internal/integrity` | Pure SHA-256 hash primitives — `ComputeNodeHash`, `ComputeRelHash`, `appendProperties`, `appendPropertyValue`. Five fixed-vector hash anchors lock the on-disk hash format. |

### Configuration

- **`Graph.Config`**: `SnowflakeNodeID` (0–15), `Store`, `BadgerDir`, `BadgerInMemory`, `Validation` (ValidationLimits). Whitespace-only `BadgerDir` rejected. Also accepts `SyncWrites bool`, `Compression`, `ZSTDCompressionLevel`, `CacheCapacity`, `CacheBudgetBytes`, `LabelIndexOnDisk`, `AdjacencyIndexOnDisk`, `PropertyIndexOnDisk`, `ValueLogFileSize`, `MemTableSize`, `BlockCacheSize`, `IndexCacheSize`, `NumCompactors`, `EncryptionKey`, `EncryptionKeyRotation` — these pass through to the underlying `BadgerStoreConfig` (ignored when `Store` is supplied explicitly). Core-level (not store pass-throughs): `ChangeLog`, `ReadOnlyReplica`, `ReplicationSource`, and **`AllowTxBackfill bool`** (off by default) — the §4.1 transaction-time backfill gate: when set, create doors honor a caller-supplied `tkg_tx_from` / `AddWithTx(…, txFrom)` and stamp it as the entity's `TxFrom` instead of the system clock; when off, any such override is rejected with `ErrTxBackfillDisabled`. Applies to CREATE doors only.
- **`ValidationLimits`**: `MaxLabelsPerNode` (50), `MaxPropertiesPerEntity` (1000), `MaxPropertyKeyLength` (256), `MaxPropertyValueSize` (64K strings), `MaxNameLength` (256). `AllowSelfLoops` (default `false` — reject self-loop relationships where start == end; set `true` to permit). Zero = default for numeric limits.
- **`BadgerStoreConfig`**: `Dir`, `InMemory`, `Logger` (nil = default), `CacheCapacity` (10K), `FlushInterval` (100ms), `GCInterval` (5min), `GCDiscardRatio` (0.5), `ReadOnly` (warm/cold shards), `SyncWrites` (fsync each write — disables async buffer, forces `FlushInterval=0`), `MaxPendingWrites` (100K ops — async write-buffer bound; at the bound the writer flushes synchronously; negative disables; moot under SyncWrites), `Compression` (`options.None`/`Snappy`/`ZSTD`, zero = Badger default Snappy), `ZSTDCompressionLevel` (1–15, zero = Badger default 1), `CacheBudgetBytes` (0 = off — byte-budgeted entity caches via `types.ApproxHeapBytes`; soft limit, dirty entries never evicted; with `CacheCapacity` 0 the byte budget alone governs), `LabelIndexOnDisk` / `AdjacencyIndexOnDisk` (0/false = RAM maps — opt-ins answer label/adjacency snapshots from the persisted keyspaces via prefix iteration with a pending-write overlay; no migration for existing dirs), `PropertyIndexOnDisk` (false = RAM `PropertyIndex.Entries`/`numBuckets` maps for `CreatePropertyIndex` entries — the opt-in answers equality/range property-index reads from the persisted `0x0A` keyspace instead, via prefix/range iteration with the same per-key pending-write overlay; UNLIKE `LabelIndexOnDisk`/`AdjacencyIndexOnDisk` the `0x0A` keyspace is new, so an existing directory with prior property-index definitions is backfilled from current node state exactly once, the first time the flag is turned on — see `badgerstore_property_disk.go`; requires a wired property-key registry, `CreatePropertyIndex` fails closed with `ErrInvalidStoreMutation` without one; `NodeRangeCardinality` always declines (`exact=false`) in this mode), `ValueLogFileSize` / `MemTableSize` / `BlockCacheSize` / `IndexCacheSize` / `NumCompactors` (0 = Badger stock; validated vlog `[1MB,2GB)`, memtable `[8MB,1GB]`, block/index cache `>=0`, compactors `0` or `>=2`; one shared `buildBadgerOptions` path; shrinking `MemTableSize` on a live-WAL dir triggers `MigrateOversizedWAL` — a read-only or above-1GB-cap open over an oversized WAL fails closed with `ErrOversizedWAL` rather than `os.Exit`), `EncryptionKey` (AES encryption-at-rest; length must be 0/16/24/32 bytes, else `ErrInvalidEncryptionKeyLength`) / `EncryptionKeyRotation` (0 = Badger stock 10-day rotation) — encryption REQUIRES both `BlockCacheSize > 0` (else Badger panics at `Open`: `ErrEncryptionRequiresBlockCache`) and `IndexCacheSize > 0` (else Badger panics on the first encrypted SSTable flush: `ErrEncryptionRequiresIndexCache`; empirically verified — Badger's own `IndexCacheSize` stock default is 0, unlike `BlockCacheSize`'s nonzero 256MB stock default); a wrong key or a key applied to a previously-plaintext dir fails `New` with a wrapped `badgerv4.ErrEncryptionKeyMismatch` (errors.Is-able).
- **`TieredStoreConfig`**: `DataDir`, `InMemory`, `RefLabels`, `ShardWindow` (1 week), `CacheCapacity` (10K), `FlushInterval` (100ms), `ColdAfter` (0=never), `IdleTimeout` (5min when cold enabled), `Compression`, `ZSTDCompressionLevel`, `ValueLogFileSize` / `MemTableSize` / `BlockCacheSize` / `IndexCacheSize` / `NumCompactors` (per-shard Badger footprint; 0 = stock; same bounds as `BadgerStoreConfig`, validated at `New` via the reference-shard open), `EncryptionKey` / `EncryptionKeyRotation` (per-shard AES encryption-at-rest; same validation and cache requirements as `BadgerStoreConfig`), `CacheBudgetBytes` (per-shard entity-cache byte budget; the per-shard heap knob across many open shards), `PropertyIndexOnDisk` (reference-shard property indexes answered from the persisted `0x0A` keyspace instead of RAM maps; scope unchanged — reference labels only), `ChangeLog` (opt-in store-global change-log/op-log — ADR-0005 §2; passes a store-owned LSN allocator into every shard via `badger.Config.ChangeLogSeqSource` so LSNs are one total order, with a refShard reseed watermark and a flush-before-read + W-bounded k-way-merge feed). All pass through `badgerCfg` to every shard (reference, hot, warm, lazy cold/archive, rotation-created).

### Snowflake Configuration

Both generator sets (nodes and relationships) use the v1.3.0 microsecond layout:

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

Each concurrent graph instance **must** use a different `Config.SnowflakeNodeID` (0–15).

## Design Rules

### Data Model

- **Pure-data structs**: Node/Relationship never hold references to Graph, registries, or resolvers. String resolution is always the Graph layer's responsibility.
- **snowflake.ID everywhere**: All IDs are `snowflake.ID` wrapped in opaque types (`nodeID`, `relID`, `entityID`). Never use `int64` or `string` for entity IDs.
- **Dual generators**: Nodes use even node field (`SnowflakeNodeID*2`), rels use odd (`*2+1`). Guarantees value-level uniqueness. Range: 0-15 (16 instances). Epoch: `2026-01-01`.
- **Strict encapsulation**: All fields unexported. Access through methods only.
- **Struct alignment**: Node (80B), Relationship (72B) packed by descending alignment. Verify with `unsafe.Sizeof`.
- **Token 0 reserved**: `HasLabelToken(0)` and `HasTypeToken(0)` always return false.
- **Validate before generating IDs**: `AddNode`/`AddRelationship` validate before `NextNodeID()`/`NextRelID()`.

### Defensive Copying

- **Accessors**: `ExtraLabelTokens()`, `AllLabelTokens()`, `Properties()`, `PropertiesMap()`, `ToMap()`, `DeepCopy()` always return independent copies.
- **Store boundary (since v4.5.0 — frozen rows)**: `Put*` deep-copy before caching, then freeze the cached entry. Point reads (`GetNode`/`GetRelationship`) deep-copy on return — callers get mutable, independent copies. Plural/scan reads (`*ByLabel*`, `All*`, `Get*ByIDs`, adjacency, temporal/index scans) return shared FROZEN pointers — zero-copy, safe because frozen entities reject mutation (`ErrFrozenNode`/`ErrFrozenRelationship` from error-returning mutators, panic from void/bool mutators). `DeepCopy()` thaws. Rows for duplicate requested IDs may alias the same frozen pointer.
- **Exception**: `Temporal()` and `Integrity()` return internal pointer (graph layer needs mutation access).

### Properties

- **Allowlist validation**: Recursive at insertion time. Primitives, slices, maps with safe elements only. Depth-limited to 32 (`ErrMaxDepthExceeded`).
- **`tkg_` prefix reserved**: `PropertySlice.Set()` rejects any key starting with `tkg_`.
- **Sorted invariant**: Always use `Set()` — never modify the slice directly.
- **Bulk construction**: `NewPropertySlice(map)` is O(N log N). `SetProperties(ps)` assigns directly. `AddNode`/`AddRelationship` use this path.

### Concurrency

- **Entity locks**: 256-shard `entityLockManager` for write-skew prevention. `shardIndex` uses low 8 bits of snowflake timestamp via `snowflakeLayout.Decompose(id).Time`.
- **Lock ordering**: entity locks -> value locks -> idxMu. Always. Value locks (`internal/locks.ValueManager`, 256 stripes keyed by `hash(labelToken, keyToken, canonical value bytes)`) guard unique-property-constraint enforcement: a create/update/label-add that introduces or changes a constrained value holds the value stripe across the index check AND the store write, so concurrent same-value writers serialize to exactly one winner. An update that changes a constrained value takes BOTH the old and new stripes in ascending order (`LockStripes` sorts+dedups). A fresh generated-ID create holds no other op's entity lock, so taking a value stripe before its unshared store write introduces no cycle.
- **Two-phase delete with TOCTOU retry**: Phase A reads adjacency under node lock. Phase B locks all entities via `LockMany`, re-verifies adjacency, retries if changed (max 10).
- **Ascending shard order**: `LockTwo` normalizes. `LockMany` deduplicates + sorts. Deadlock-free.
- **Transaction isolation via c.txMu (v4.1.0+)**: `Core.txMu` serializes tx-vs-tx and tx-vs-batch. `Core.mu` (an RWMutex) is taken with RLock by each tx method around its body (via `tx.lockActiveCore` / `tx.unlockActiveCore`), and is no longer held for the tx lifetime. Concurrent standalone mutations and reads from other goroutines proceed in parallel with an open tx — only entity-level conflicts block, via the existing 256-shard entity-lock manager. Admin ops that read adjacency and cascade (`ArchiveNode`, `RestoreNode`) acquire `g.mu.Lock()` to fence against concurrent writers. Tx Rollback briefly takes `c.mu.Lock` while replacing the registry pointers via `restoreRegistries`. Isolation level under v4.1.0: "serializable per touched entity, snapshot-isolated elsewhere" — a concurrent reader can observe in-progress tx-allocated labels/types until commit/rollback. Code requiring "tx blocks all concurrent observation" must take an external lock.
- **sync.RWMutex is NOT reentrant**: If A holds RLock and calls B which RLocks, and a writer waits between them, deadlock. Inner methods must be lock-free.
- **Inside a tx, both forms work (v4.1.0+)**: Under v3.4 / v4.0.x, `BeginTx` held `c.mu.Lock` for the tx lifetime, so any read accessor that opened with `c.mu.RLock` deadlocked. Path B (v4.1.0) replaced `c.mu.Lock`-for-tx-lifetime with brief per-call `c.mu.RLock`, so both `g.Nodes().ByLabel(...)` and the tx-side mirror `tx.NodesByLabel(...)` work correctly inside an open tx. The tx-side mirrors in `pkg/graph/internal/core/tx_consistent_reads.go` remain for call-site clarity but are no longer required for correctness. See lessons.md #31 (now marked SUPERSEDED).
- **sync.Once for idempotent Close()**: Never nil-guard a function pointer across goroutines.

### Persistence

- **Store is pure persistence**: Shadow and string resolution are Graph-layer responsibilities. All queries return `error` and sort by ID.
- **Async batch**: Writes update in-memory immediately, queue `writeOp` structs (last-write-wins dedup). Background flush via `WriteBatch` every `FlushInterval`.
- **LRU dirty tracking**: Dirty entries never evicted (soft capacity). `CollectDirty()` is read-only. `MarkFlushed()` is CAS on `dirtyVer`. `Peek()` for zero-alloc cache hits.
- **Tombstones in cache-first**: A cache miss must not fall through to stale Badger data.
- **Close() must flush unconditionally**: Even when `flushLoop` was never started (InMemory mode).
- **In-memory state must survive restart**: If it's in memory and it matters, it needs a persistence path and a rebuild path. If `loadIndexes()` doesn't rebuild it, it doesn't survive restart.
- **Counters in same WriteBatch as data**: Separate transactions = crash inconsistency.
- **On-disk wire format is versioned**: `NodeWire`/`RelWire` carry `FormatVersion` (`fv`; absent = legacy = v1) and badger verifies/stamps a `wire_format_version` meta marker at open. Data written by a newer release fails closed with `ErrWireFormatVersionUnsupported` (store-level at open; per-row at checked decode and during `loadIndexes`). Bump protocol on `storeutil.CurrentWireFormatVersion`: encoders (lesson 39) + decode + marker together.
- **Canonical temporal predicates live in storeutil**: `EntityValidFrom` / `MatchesPointInTime` / `MatchesInterval` are the ONLY definitions of effective-valid-from and the point/interval predicates; core delegates, stores push down the same functions. `TestTemporalTwoDoorsAgreeOnLabelQueries` enforces cross-door agreement. `SelectAsOf[T TemporalRow]([]T, pin)` (`asof_select.go`) is the layer ABOVE them — the ONE definition of the as-of (transaction-time belief-state) SELECTION rule: newest version by VERSION order with `0 < TxFrom <= pin`, ABSENT when none exists or that decisive newest belief was retracted (`TxTo != 0 && TxTo <= pin`) or hard-deleted (`DeletedAt != 0 && DeletedAt <= pin`) — never fall through to an older still-open row (lesson 62). The core resolver (`resolveNodeChainAsOf` / `resolveRelChainAsOf`) and the memory backend (`nodeAsOfLocked` / `relAsOfLocked`) DELEGATE to it; the badger native reverse-scan (`NodeAsOf` / `RelAsOf`) is an early-stopping optimization proven equivalent to it by randomized-chain tests (`badgerstore_asof_equivalence_test.go`), so no backend re-implements the rule (killing the cross-backend-divergence bug class: the as-of version-order divergence and the commit-window drop). `SelectAsOf` is pure selection — normalization to the then-visible state stays with the caller.
- **Relationship creation goes through one kernel**: all create doors (Add / AddByID / AddByIDIfAbsent / batch Execute) share `relationship_create_kernel.go` (`prepareRelCreate` → `relEndpointHashLadder` → `createRelWithTypeRollback`). Add creation invariants THERE, not in a door.
- **Badger WriteBatch.Flush() blocks forever on closed DB**: Badger v4 uses `context.Background()` in `oracle.readTs()` → `WaitForMark()`, so closing the DB stops oracle goroutines and any in-flight `WriteBatch.Flush()` blocks forever. Fix: `BadgerStore.dbClosed atomic.Bool` is set to `true` in `Close()` BEFORE `db.Close()`. `flush()` checks it and returns `ErrDBClosed` immediately. Any test that calls `bs.db.Close()` directly MUST set `bs.dbClosed.Store(true)` first. Never call `WriteBatch.Flush()` after `db.Close()`.
- **Decode untrusted msgpack only through `storeutil.SafeUnmarshal`**: Raw `msgpack.Unmarshal` of persisted/imported bytes is unsafe at the trust boundary — the vmihailenco decoder PANICS via reflect on a duplicate map key bound to an interface field (`PropertyWire.Value any` → `SetString/SetInt using unaddressable value`) and FATALLY stack-overflows (unrecoverable) on deep nesting. `SafeUnmarshal` runs `guardMsgpackDepth` (non-recursive, rejects nesting > `maxWireDecodeDepth`=64 before the decoder runs) then recovers any panic, returning `store.ErrCorruptWire`. Every `NodeWire`/`RelWire`/meta/custom-property decode in badger + core import routes through it (`msgpack.Marshal` is panic-free, no wrapper). When adding a new decode-from-bytes site (or a sharded backend), use `SafeUnmarshal`, never raw `msgpack.Unmarshal`. Audit recipe: `grep -rn 'msgpack.Unmarshal(' pkg/ | grep -v _test`. See lessons.md 47.
- **Change-log (op-log) is IN-BACKEND and co-committed, never a Store decorator**: when `ChangeLog` is enabled, each backend appends a framed record under `0x09/<LSN>` in the SAME `WriteBatch` as the data + counters + a `last_lsn` watermark — crash-consistent, atomic with the mutation. A decorator over `Store` cannot reach the inner `WriteBatch` (committed-but-unlogged window) and would be treated as an untrusted store (loses frozen-pointer scans), so the log lives inside badger/memory as the optional `store.ChangeFeedCapability`, type-asserted by core (`changeFeedCapability` guards against a wrapper promoting a shard's per-shard feed). The LSN is assigned + the record buffered under ONE `wbMu` window with the entity ops (doors lacking `idxMu` use `appendOpsLogged`); every early-return-after-snapshot in `flush()` requeues logs; the empty guard is `len(ops)==0 && len(logs)==0`. Both backends share the `storeutil` body builders so feeds are byte-identical (cross-backend parity test). The log ALONE does not converge a replica — bootstrap from an export snapshot (registry included), then tail. When adding a mutation door, emit exactly ONE logical record from the PUBLIC door (shared helpers like `deleteRelByInfo`/`cascadeDeleteInner` stay record-free). The `ChangeNodePut`/`ChangeRelPut` body is `storeutil.NodePutBody`/`RelPutBody` (wire + `WithHistory` bit); the put-record wire is built UNTOKENIZED via `NodeToWireChecked` (property keys as strings) in BOTH backends — so feeds are byte-identical even for property-bearing entities, and a put record carries no property-key registry dependency. A new put-door must set `WithHistory` to true IFF it wrote a version-history row (`ReplaceNodeWithHistory` / `*LabelTokenWithHistory`), false for create / in-place `ReplaceNode` / no-history label doors; badger and memory MUST agree (the replica reproduces history depth from this bit). See lessons.md 49–50 and `tasks/horizontal-scaling.md`.
- **Tiered change-log is a store-global feed over per-shard co-committed logs (ADR-0005 §2)**: `tiered.Store` now satisfies `ChangeFeedCapability`/`ChangeLogStatusCapability`/`TxChangeLogScope` (core's `changeFeedCapability` switch admits `*tiered.Store`), enabled by `tiered.Config.ChangeLog`. There is no single `WriteBatch` on tiered, so a store-global `atomic.Uint64` allocator (`changeLogAllocator`) is INJECTED into every shard via `badger.Config.ChangeLogSeqSource` (nil = badger self-owned counter, standalone byte-for-byte identical); each shard still co-commits its own records + `LastLSNKey` in its own batch, but LSNs form ONE total order across shards. Reseed at open reads ONLY the monotonic `changelog_lsn_watermark` MetaKV key on the always-present refShard (persisted after every log-bearing flush via `badger.Config.OnChangeLogFlush`, plus a belt-and-braces fold of the open ref/hot `LastLSNKey`s) — NEVER a full cold-shard scan; an unreadable watermark FAILS CLOSED (poisons the allocator so it refuses LSNs, sticky error on every feed door, `RecoverChangeLog` clears in place). The feed (`ForEachChange`/`ChangeFeed`) is barrier-first + W-bounded: `Flush()` folds `shard.Flush()` over every open shard (also satisfies the core `storeFlusher`), then captures `W = LastCommittedLSN` and runs a paged k-way min-heap merge over every catalog shard emitting ONLY `LSN <= W` (one shard checked out at a time; cold-shard log segments paged read-only, no flush on a closed shard; heads > W deferred to the next poll). The barrier + W-bound TOGETHER close the cross-shard flush-reordering silent-loss (ADR Finding-1); the barrier alone is insufficient under a concurrent writer. `TxChangeLogScope` is store-level (`SetLogDivert` is the ONE divert seam, marked for the scope-tagged-routing redesign — measurements 2026-07-11) buffering per shard, LSNs minted at commit so a rolled-back tx burns none. A cascade whose whole neighborhood is LOCAL to the node's shard delegates to `shard.DeleteNodeCascade` (ONE cascade record, byte-identical to single-shard backends); a cross-shard neighborhood emits one `ChangeRelDelete` per edge (ADR §2.4). Reseed/reorder/rotation/parity/replica-convergence tests live under `pkg/graph/store/tiered/tieredstore_changelog*_test.go` and the three-way `changefeed_parity_test.go`.
- **Replica apply reproduces the primary's rows VERBATIM, never re-stamps**: `g.Replication().ApplyChange` (in `apply_record.go`, under `c.mu.Lock`, bypassing `checkWritable`) decodes each record, runs the SAME import-trust pipeline per record (`WireTo*Checked` reconstructs version/`TxFrom`/temporal/integrity verbatim → `validate*TokensInRegistry` → `validatePropertySliceLimits` → `verifyImported*Hash` recompute-AND-COMPARE), then writes through a FOREIGN-ID store door (`PutNode`/`ReplaceNode`/`ReplaceNodeWithHistory`/`Delete*`/`PutNodeVersion`/`Truncate*`/`Trim*`/label-token doors) that persists the supplied entity byte-for-byte. NEVER route apply through `NodeOps.Add/Update` (they re-mint IDs, re-stamp `TxFrom`, recompute hashes). Create-vs-update is inferred from local existence; a `NodePut` whose label set differs from the local row by one token is a label mutation → routed to the matching label-token door. Apply is IDEMPOTENT (identical row = no-op via `nodeWireMatches`/`relWireMatches`; delete tolerates `ErrNodeNotFound`/`ErrRelNotFound`) because the watermark advances via a SEPARATE `MetaSet` after the door commits — the crash window is closed by re-apply, not co-commit. Per-record delete `PrevVersion`s are derived from the replica's OWN local current rows (correct because LSN total-ordering guarantees the replica's local current == the primary's pre-mutation state). The read-only gate is `c.checkWritable()` on user doors; apply and `IOOps.Import` use `checkOpen` only. See lessons.md 50.
- **Token-registry refetch is append-only and prefix-guarded; property keys are never synced**: a label/rel-type the primary allocates AFTER a replica's bootstrap is resolved by `g.Replication().RegistrySnapshot()` (capture the LSN BEFORE the names so `CapturedAtLSN ≤ names coverage`) + the apply-path hook (`validate*TokensWithRefetch` in `apply_record.go`), which guards `CapturedAtLSN ≥ rec.LSN`, then `AppendNames(prefix, suffix)` (the registries' append-only grow — `ImportNames` is load-only; prefix must DeepEqual current, else `(false,nil)`), persists, and re-validates. The refetch runs UNDER `c.mu.Lock` (held by apply) and takes ONLY `c.registryMu` (order `c.mu → registryMu`; the source is a DIFFERENT/primary Core taking its own locks — never re-take `c.mu`). On a persist failure it `RollbackNames` the in-memory grow so the registry never runs ahead of disk (else a crash leaves an applied entity referencing an unpersisted token). Property keys are NOT refetched — records carry UNTOKENIZED string keys, so the replica tokenizes them locally in its own independent registry; appending the primary's propkeys would misalign it. See lessons.md 51.
- **Bootstrap LSN is captured under the export lock; the failover lease is a durable HINT not CAS**: the export header (v2; importers accept v1 OR v2) carries `SnapshotLSN`, read via `c.changeFeed.LastCommittedLSN()` UNDER the same `c.mu.Lock` as the entity snapshot (gapless), and import records it as the replica's initial applied watermark (flush-before-watermark). The ID-slot lease (`MetaKey("id_slot_lease")`, `SafeUnmarshal` on read, slot 0-15 validated) is last-writer-wins (`DetectConflicts=false`) — rho-tkg persists/reads it; the EXTERNAL orchestrator serializes writes, picks slots, and triggers promotion = `Close()`+`New()` with the leased `SnowflakeNodeID` (generators are built only in `New`). A promoted node and the node it replaces MUST hold different slots so minted IDs never collide. See lessons.md 51.

### Version History

- **Pre-mutation snapshots**: `Update*` captures deep copy, applies mutations, writes both via `ReplaceNodeWithHistory`/`ReplaceRelWithHistory` atomically.
- **Append-only**: Delete paths save tombstone versions (with `DeletedAt`/`ValidTo`) before deletion. History is never erased on delete.
- **Replace vs Put**: Put rejects duplicates. Replace requires existence. Replace overwrites data only — no index changes (labels/type/endpoints are immutable).
- **Genesis detection**: Use `entry.Version() == 0`, not array position. Array position changes after truncation.
- **History compaction (ADR-0001)**: `g.Admin().CompactHistoryNodes/Rels(ctx, RetentionPolicy{KeepVersions, KeepSince})` trims OLDEST history rows (current row + newest history version never trimmable) and writes a per-entity detached-anchor STUB `{TrimmedThroughVersion, LastTrimmedHash, LastTrimmedTxTo, CompactedAtTx, StubHash(self)}` in MetaKV — per-entity trim + stub in ONE WriteBatch via `store.HistoryCompactionCapability` (memory + badger commit them atomically; tiered routes the trim to the owning shard and the stub to the reference shard — see `tieredstore_compaction.go`), with the graph watermark routed ONCE via the store-level MetaSet (`advanceCompactionWatermark`). NEVER mutates a stored row (append-only). `Verify*Chain` is stub-aware: oldest kept version's `PrevHash` must equal `LastTrimmedHash` (virtual predecessor). A temporal read pinned before compacted knowledge returns `ErrHistoryCompacted` (point doors: per-entity stub boundary; scan doors: graph `CompactedThroughTx` watermark, fail whole scan) — never silently incomplete. REFUSES while a change-log is enabled (`ErrCompactionChangeLogEnabled` — replication interplay lands later), on a protected as-of tag (`ErrCompactionProtectedTag`), and on an empty policy (`ErrInvalidRetentionPolicy`). Sentinels canonical in `internal/core`, re-exported from `pkg/graph`. See `docs/adr/0001-history-retention.md`.

### Temporal Queries

- **Three timestamps, three claims (VT vs TX)**: Do not conflate them.

  | Timestamp | Claim | Who can assert it | State on an unstamped entity |
  |---|---|---|---|
  | `tkg_tx_from` | "the DB recorded this fact at T" | the system — automatically (or a privileged backfill under `Config.AllowTxBackfill`, §4.1) | always stamped: every Add allocates `TemporalMetadata` and sets `TxFrom` |
  | `tkg_created_at` | "the entity record came into existence at T" | system-derived (snowflake ID timestamp); caller may override at Add | always derivable — the shadow resolver applies the snowflake fallback |
  | `tkg_valid_from` | "the fact holds **in the world** from T" | only the domain — a recorder/curator with actual knowledge | `0` = no world-time claim made |

  Writers must NEVER default `tkg_valid_from := now()` without domain knowledge — that silently conflates TX with VT (lesson 32): recording latency leaks into world-time semantics and backfilled facts become indistinguishable from real assertions. If a consumer wants "unstamped ⇒ valid since recorded", implement it as an explicit, flagged heuristic on the consumer side (lesson 36 pattern), not by stamping at write time and not by changing the shadow resolver.
- **Two doors, two views (asserted vs effective)**: The shadow resolver (`g.Resolve().NodeProperty(n, "tkg_valid_from")`, `shadow.go`) returns the RAW asserted value — `(Instant(0), ok=true)` when never asserted. `ok` is true because `TemporalMetadata` always exists (TxFrom stamping); consumers must check the zero value, not `ok`. Temporal queries instead use the EFFECTIVE valid-from (`nodeValidFrom`/`relValidFrom`: explicit `ValidFrom`, else snowflake fallback) — so an entity with unset `ValidFrom` is "eternal" through the shadow door but time-bounded through the query door. This asymmetry is deliberate: shadow props report *stored/asserted* state, temporal queries report *effective* state. `tkg_created_at` is the only temporal shadow key with a fallback in the resolver.
- **Effective valid-from**: Derived from explicit `ValidFrom` or snowflake ID timestamp. Every entity is queryable temporally without `SetTemporal()`.
- **Point-in-time**: `effectiveValidFrom <= t AND (ValidTo == 0 OR ValidTo > t)`.
- **Interval overlap**: `effectiveValidFrom < end AND (ValidTo == 0 OR ValidTo > start)`.
- **Bitemporal (since v4.3.0)**: `QueryOpts.TxAt` filters the chain to versions RECORDED by the given TX time (`TxFrom <= TxAt` — recorded-by-then). TxTo deliberately does NOT bound visibility: superseded is not retracted, and a superseded version remains the authority for its valid-time slot at every later txAt (lesson 43; the old `< TxTo` clause made `NodeAtTx(oldVT, now)` return nothing after any update). `TxAt == 0` keeps "no TX filter" backward-compat. Bitemporal point queries: `g.Temporal().NodeAtTx(id, validAt, txAt)` and counterparts. The resolver computes `vEnd` from `next.ValidFrom` (falls back to `next.UpdatedAt`), so adjacent versions auto-tile once the caller supplies explicit `tkg_valid_from` on Update. See lessons 32–34. `QueryOpts.TxPin` is the pure knowledge-time BELIEF-STATE pin (NO valid-time filter): the generic doors (`ByLabel`/`ByType`/`All`) route each candidate through the SAME as-of resolver as `NodesAsOf`/`RelsAsOf` (`findNodeVersionForOpts`→`nodeAsOfLocked`), so `ByLabel{TxPin:T}` equals `NodesAsOf(T)` filtered by label — closing the `TxAt`-alone footgun (that door still valid-filters at wall-now and drops past-valid facts). `TxPin` is mutually exclusive with `ValidAt`/`ValidStart`/`ValidEnd`/`TxAt` (`ErrConflictingTemporalOpts`).
- **Single resolution seam**: `resolveNodeChain(chain, probe, pred)` / `resolveRelChain(...)` in `chain_resolver.go` are the ONE place all core-layer temporal reads select a version from a `(history ‖ current)` chain — every named door (`NodeAt`/`NodeAtTx`/`NodesDuring`/`NodesAsOf` + rel mirrors) and every generic `QueryOpts` door (`findNodeVersionForOpts`, `nodeAtLockedTx`, `find*VersionMatchingDuringTx`, `nodeAsOfLocked`) funnels here via a `chainProbe{kind: probePoint|probeInterval|probeAsOf, ...}`, so TX visibility (lesson 43), pre-delete tombstone normalization (lesson 60), `[vStart,vEnd)` derivation (lessons 32/33/42), newest-belief selection (lessons 46/62), predicate-anywhere interval matching (rule 16), and the as-of retraction rule (lesson 62) live in exactly one seam and cannot drift between the two doors (rule 17). The canonical entity-level predicates in `internal/storeutil` (store push-down) are separate and unchanged.
- **History-aware merging**: Temporal queries merge current + history IDs via lazy ForEach iterators (two-phase: collect IDs under store locks, process after release).
- **ForEach for OOM-safe iteration**: Never materialize all per-shard slices + merge. Use `ForEach*ID` callbacks. Constraint: callback must NOT call store methods (deadlock via B15). Two-phase: collect IDs, then process. ~83% memory reduction.
- **Deleted entity verification**: Any verification reading entity state must tolerate deletion — if entity has history but no current state, proceed using history alone.
- **Adjacency-at-t fold uses deleted-only iteration**: `g.Temporal().OutgoingRelsAt` / `IncomingRelsAt` / `NeighborsAt` go through `forEachRelAdjacencyCandidateID` which folds in only DELETED rel IDs (via the store's optional `DeletedIterationCapability`) on top of the live adjacency index. Rel endpoints are immutable, so a rel that ever pointed at the queried node still does if alive — therefore only deleted rels can be missing from the candidate set. Label/property temporal queries must keep using the full-history `forEachNodeCandidateID` / `forEachRelCandidateID` because entities can have their CURRENT label/property differ from their at-t state.

### TieredStore

- **Timestamp routing**: Resolve the actual shard via snowflake ID timestamp or ref probe — never by `EntityClass`. Class tells you where new entities go; shard tells you where existing ones live.
- **Cross-shard split-write ordering (section 12)**: E->R: write ref shard inIdx first. R->E: write entity shard first. Both endpoints verified before any writes.
- **Checkout/checkin for cold shards**: `getStore()` returning a pointer without ref counting races with `closeIdleShards()`. Use `checkoutStore()`/`checkinStore()` to increment/decrement `activeReqs`.
- **Rotation boundary alignment**: Truncate to millisecond + add one unit. Nanosecond precision creates gaps with ms-resolution snowflake IDs.
- **Catalog sync on rotation**: Both in-memory `eventShard.timeEnd` AND catalog `ShardEntry.TimeEnd` must be updated on hot->warm rotation. Catalog is persisted — warm shard recovery depends on it.
- **Rollback on partial failure**: Cross-shard moves (archive/restore) must undo completed steps when a later step fails. Otherwise partial failure leaves data duplicated or orphaned.
- **Sequential ForEach**: One shard open at a time via checkout/checkin. No goroutines, no `mergeIDSlices`. Trades parallelism for memory safety.
- **Property indexes on reference entities only**: `CreatePropertyIndex` rejects event labels (`ErrEventPropertyIndex`).
- **Background-error recovery**: idle/transient cold-shard close failures set a sticky background error (fail closed on every path). `tiered.Store.RecoverBackgroundError()` re-probes persistence via an atomic catalog save and clears the gate in place on success — no close/re-open. Lifecycle gate only; run `VerifyShard`/`RunRepair` for data confidence.
- **Primary-label class is immutable**: `AddNodeLabelToken{,WithHistory}` and `RemoveNodeLabelToken{,WithHistory}` reject any mutation that would change the primary label's ontology class (reference ↔ event) and return `ErrPrimaryLabelClassMutation`. Enforced at the `TieredStore` Store-impl boundary only — `MemoryStore` and `BadgerStore` are single-shard and don't care. If you add another sharded backend, replicate the guard. Reason: routing decisions depend on primary-label class; flipping the class would leave the live entity on its original shard while subsequent history snapshots route to a different shard, fragmenting the version chain. See lessons.md B33.

### Integrity & Indexes

- **Canonical hash inputs**: Hash canonical internal state, not raw user input. Token deduplication/normalization happens during construction — hash must reflect the canonical state.
- **Index cleanup on corruption**: When a corruption fallback skips entity data, it must still clean ALL indexes (label, property, adjacency). Leaving stale entries causes phantom results.
- **3-phase index creation**: (1) Install empty placeholder under Lock (visibility for concurrent writes), (2) unlocked I/O to build, (3) Lock to install with dirty-map tracking (`mutated[id]` not `contains(id)` — prevents re-adding concurrently deleted values).
- **API design rules**: Config fields must be used or removed. Opaque wrappers must wrap the real type. Graph is sole external API. Doc comments must match behavior.

### Unique Property Constraints (ADR-0002)

- **Enforce at the finalized-state seam, not per door**: `enforceUniqueForNode`/`enforceUniqueForNodeHeld` (`unique_constraints.go`) is consulted by every standalone node door that can introduce or change a constrained `(label, value)` pair — `Add`/`AddWithTx`/`AddByIDIfAbsent`, `Update`/`UpdateInPlace`/`CompareAndSetProperty`, AND `AddLabel` (the door everyone forgets: a node carrying an offending value acquiring the constrained label is rejected). `UpdateInPlace` passes its pre-mutation state so a value CHANGED in place also holds the freed old-value stripe (ADR Decision 3). Batch/tx enforce too: `GraphTx.AddNode`/`UpdateNode` flow through the same internal doors (a tx applies mutations immediately, so a same-value create inside one tx sees the earlier one); `BatchBuilder.AddNode` creates are pre-checked (`partitionBatchNodesByUnique`, `unique_enforce_batch.go`) under the batch's exclusive `c.mu.Lock` against committed state, earlier same-batch creates, AND the durable `UniqueForever` ownership registry (a fresh batch create claims ownership durably; the exclusive lock fences concurrent standalone writers, so no value stripe is needed). `g.IO().Import` validates the replayed current state post-replay (`validateUniqueAfterImportLocked`) and rolls back with `ErrUniqueViolation`; `ImportOptions.SkipUniqueValidation` opts a trusted restore out. Replica apply does NOT enforce (verbatim reproduction — lesson 50).
- **3-phase create, two live scopes**: `CreateUnique` installs a PENDING entry under `uniqueMu` (concurrent writers enforce immediately — by design), validates existing data unlocked via the property index (auto-ensured), then activates + persists — or uninstalls + returns `ErrUniqueViolationExisting` (≤5 offenders). `UniqueCurrent` (default) consults CURRENT state only (history duplicates legal; a freed value is reusable). `UniqueForever` (`CreateUniqueForever`, `unique_forever.go`) adds a durable value→owner registry: the first entity to hold a value owns it forever (barred from every other node across supersession, hard delete, reopen). The registry is its own self-hashed MetaKV blob (`SafeUnmarshal`, fail-closed on tamper, reaped by `Reset`); the kernel consults it under the value stripe (hit+different entity→violation; same entity→pass; miss→claim+persist). `CreateUniqueForever` seeds owners from current values; `DropUnique` releases a forever constraint's claims; `Constraints().ReleaseOwnership(label, key, value)` is the operator free door (idempotent; `ErrUniqueConstraintNotFound` without a forever constraint). A forever claim made in a rolled-back tx stays barred (durable claim not part of the tx snapshot — `ReleaseOwnership` remedies). `UniqueValidOverlap` still returns not-yet-supported. Float keys/values → `ErrUniqueUnsupportedType`.
- **`GetOrCreateByKey`**: `g.Nodes().GetOrCreateByKey(ctx, label, key, value, extraProps)` takes the value stripe, returns a property-index hit as a mutable copy, else creates under the same held stripe (passed through `addNodeInternal`'s `heldStripes` so the create's own kernel does not re-lock it). Works WITHOUT a constraint (the value lock alone makes it atomic). Float/non-scalar value → `ErrUniqueUnsupportedType`.
- **Lock discipline**: hold the value stripe(s) across the index check + store write (see the lock-ordering note in Concurrency). Sentinels are canonical in `internal/core/core.go`, re-exported in `pkg/graph/errors.go`.

### Events & Stats

- **EventBus is opt-in**: `Graph.SetEventBus(bus)` — nil by default (zero overhead). Handlers are copied under RLock, invoked outside the lock (prevents deadlocks from re-entrant Graph calls in handlers).
- **Tx event buffering**: During a transaction (`txEventBuffer != nil`), `publishEvent` appends to a buffer instead of dispatching. On `Commit`, events are published after `g.mu.Unlock()` so handlers can safely call Graph read methods. On `Rollback`, buffered events are discarded — subscribers never see rolled-back mutations.
- **AsyncEventBus for async delivery**: `Graph.SetAsyncEventBus(bus)` — worker pool with per-priority `[5]chan Event` queues. `BackpressureStrategy` controls full-queue behavior (Block/DropOldest/DropLatest). `Close()` drains all pending events before stopping workers. `Graph.events` is typed as `eventPublisher` interface (unexported) — allows either bus type without breaking the external API.
- **EventPriority**: 5 levels — `PriorityNormal` (0, zero value), `PriorityHigh` (1), `PriorityCritical` (2), `PriorityLow` (3), `PriorityDeferred` (4). Graph assigns internally: creates→High, deletes→Critical, updates→Normal. Backward-compatible: existing `Event{}` literals default to PriorityNormal. Priority ordering in `AsyncEventBus` worker uses non-blocking drain per level (Critical first) before blocking select.
- **PublishBatch priority ceiling**: `AsyncEventBus.PublishBatch` raises a per-batch priority ceiling for each priority pass (atomic `batchPriorityCeiling`) and clears it at end-of-batch. The dispatcher's priority scan honours the ceiling so an in-batch wake-up triggered by `BackpressureBlock` filling a queue cannot dispatch a pre-existing lower-priority event before later same-batch higher-priority events have been enqueued. Liveness is preserved: the saturating-batch wake-up still drains same-or-higher priorities.
- **StoreStats opt-in**: Type-asserted in `(*Core).Stats()` (reachable via `g.Stats().Get()`) — avoids polluting the `Store` interface.
- **Atomic operation counters**: 8 `atomic.Int64` fields on Graph — incremented after every successful store write.

### Vector Indexes

- **Approximate by default**: `CreateVectorIndex`/`CreateVector` build a pure-Go HNSW (Hierarchical Navigable Small World) graph — `pkg/graph/internal/index/hnsw.go` — layered skip-list-style, params `M=16` (max bidirectional links per node above layer 0; `M0=2M=32` at layer 0), `EfConstruction=200` (candidate list size while building), `EfSearch=64` (candidate list size while searching). `SearchNearest` is therefore APPROXIMATE, not exact — recall target: recall@10 >= 0.95 on a well-clustered corpus (real embeddings have cluster structure; see the recall gate in `hnsw_test.go`, which deliberately uses a clustered synthetic corpus rather than i.i.d. noise — high-dimensional i.i.d. random vectors are a degenerate case for ANY nearest-neighbor notion, approximate or exact, due to distance concentration, not a fair recall benchmark).
- **Brute-force escape hatch**: `VectorIndexOptions{UseBruteForce: true}` via `g.Index().CreateVectorWithOptions(label, propertyKey, dims, metric, opts)` reverts that one index to the exact O(n × dims)-per-query linear scan — for exact-recall requirements (compliance, small indexes, correctness oracles). `M`/`EfConstruction`/`EfSearch` (also in `VectorIndexOptions`) tune the HNSW engine; zero = the documented defaults above. Additive: the plain `CreateVectorIndex`/`CreateVector` door keeps its original signature and is exactly `CreateVectorWithOptions(..., VectorIndexOptions{})` — HNSW by construction default, not an opt-in.
- **Deterministic construction**: level assignment draws from a `VectorIndex`-owned `*rand.Rand` seeded with a FIXED constant (not time/wall-clock derived), so inserting the same vectors in the same order always assigns the same per-node levels and therefore always builds the identical graph and returns identical search results — see `TestHNSWDeterministic*` in `hnsw_test.go`. A `VectorIndex` constructed via a bare struct literal (every in-tree backend's existing construction call sites — unchanged by this WP) transparently defaults to HNSW: the graph is lazily built from whatever entries exist the first time an `Add`/`Remove` needs it, so no backend code had to change for the default-HNSW behavior; only the additive `CreateVectorIndexWithOptions` capability was added for the brute-force/tuning override.
- **Neighbor selection is the paper's heuristic, not "keep-M-closest"**: `selectNeighborsHeuristic` (Algorithm 4, Malkov & Yashunin) keeps a candidate only if it is closer to the base element than to every OTHER candidate already selected, backfilling from the discarded set if that leaves fewer than the target count. This is NOT a stylistic choice — a naive "keep the M/M0 closest" selection (tried first, reverted) preferentially prunes long-range/cross-cluster edges in favor of many redundant short intra-cluster ones, which silently FRAGMENTS the graph into per-cluster islands with no path between them (verified via a BFS-reachability regression during development: a purely-closest-pruned 2000-node/20-cluster graph left only ~109 of 2000 nodes reachable from the entry point). The heuristic is applied both to the forward (newly inserted node's) neighbor list AND to the reverse-edge pruning in `connect()` when a peer's neighbor list overflows its layer cap — pruning only one side reintroduces the fragmentation.
- **Soft-delete tombstones + threshold rebuild**: `Remove` tombstones the HNSW node (marks it deleted, does not unlink it — its edges keep the graph connected THROUGH it) rather than physically removing it. Tombstoned nodes are excluded from search RESULTS but still traversed for connectivity. When the tombstone/live ratio exceeds 20% (`hnswRebuildTombstoneRatio`), the next `Add`/`Remove` on that index triggers a full graph rebuild from the current live entry set (fresh RNG at the same fixed seed, replayed in the entries' current slice order — deterministic given that order, not a continuation of the pre-rebuild RNG stream).
- **Filtered search**: a filtered `SearchNearest` over-fetches (asks the HNSW graph for `4x` its effective `ef` worth of candidates — `hnswOverfetchFactor`), applies the filter to that candidate list, and falls back to an exhaustive brute-force scan (with the same filter) over every entry whenever fewer than `k` candidates survive the over-fetch — so a highly selective filter never silently under-returns relative to what a full scan would find. Unfiltered search does not fall back (a lower unfiltered recall is the documented approximate-search trade-off; see the recall gate).
- **Not persisted**: Vector indexes (both engines) are rebuilt from node properties on restart — an HNSW graph is rebuilt from scratch via the same lazy-build-on-first-Add path, not serialized/deserialized. Documented limitation, unchanged by this WP. `badger`/`tiered` DO persist the DEFINITION (dims, metric, `UseBruteForce`, `M`/`EfConstruction`/`EfSearch`) so a restart rebuilds the SAME engine/tuning rather than silently reverting to default HNSW tuning; a definition written before these fields existed decodes them to zero, which is exactly "default HNSW with default tuning" (backward compatible).
- **Store-level scope in TieredStore**: Vector indexes live at the `TieredStore` level (not per-shard) with their own `vectorIdxMu sync.RWMutex`.
- **Auto-maintenance**: All mutation paths (`PutNode`, `ReplaceNode`, `DeleteNode`, `RemoveNodeLabelToken`) update vector indexes — unchanged by this WP; `VectorIndex.Add`/`Remove` internally route to the HNSW graph (or the brute-force entry list) transparently, so every existing call site keeps working with no signature change.
- **Thread safety, no new lock class**: HNSW mutation (insert/tombstone/rebuild) happens only from `VectorIndex.Add`/`Remove`, already under `VectorIndex.mu` (write lock); HNSW search happens under the SAME mutex's read lock (or after a snapshot copy for a filtered search, mirroring the pre-existing brute-force filtered-search contract — the filter callback is always invoked OUTSIDE the lock, since a store-backed filter may need to acquire the store lock and holding the vector lock across that call would invert the established store-lock-then-vector-lock order).

### Code Review Meta-Lessons

(Beyond what Testing Rules 15–17 already capture.)

- **Use library APIs, never reimplement internals**: If a dependency provides an API (e.g. `snowflake.Node.Decompose()`), use it. Never duplicate internal knowledge like bit layouts or hardcoded shifts in the consumer.
- **Every fix needs a grep audit**: When fixing a pattern in one call site, grep for the same pattern across all files. The canonical hash bug (A1) was fixed in `context.go` but missed in `batch.go`, requiring a second review round.
- **Review by feature, not by file**: Single-file reviews miss cross-file interactions.
- **Repair tools don't replace correctness**: The cross-shard rollback issue (B7) was once accepted as "mitigated" by `RunRepair` when it should have been fixed inline.
- **Most-recent-overlap is wrong for predicate-during-interval**: A "during [start,end)" query that checks the predicate only on the most-recent overlapping version misses entities whose label/property held earlier in the interval. Use `findNodeVersionMatchingDuring(id, start, end, pred)` which scans all overlapping versions.

## Audit Checklists

Run these after any change to the relevant subsystem.

**Hash computation**: `grep -rn 'ComputeNodeHash\|ComputeRelHash' pkg/` — every call site must pass canonical (registry-resolved) labels/type, not raw input.

**Entity locks**: `grep -rn 'store\.Put\|store\.Delete\|store\.Replace' pkg/graph/` — every Store write must have `LockEntity`/`LockTwo`/`LockMany` before it.

**Index cleanup**: Every delete path handling missing entities must purge label, property, AND adjacency indexes. Brute-force purge is acceptable for corruption paths.

**Index creation visibility**: `CreatePropertyIndex` must install empty placeholder BEFORE I/O phase. Phase 3 must check `mutated[id]`, not `contains(id)`.

## Registries

Three independent registries with independent token namespaces (label, rel-type, property-key). Methods: `GetOrCreate`, `Resolve`, `ResolveAll`, `Lookup`. `GetOrCreate` rejects empty strings (`ErrEmptyName`), warns at 60K tokens, errors at 65535. Persisted in Badger as `meta/label_tokens`/`meta/reltype_tokens`/`meta/property_keys`, or in TieredStore as `data/meta/registry.msgpack` (atomic write).

## Shadow Properties (21)

| Key | Type | Applies To | Category |
|---|---|---|---|
| `tkg_labels` | `[]string` | Node | Structural |
| `tkg_type` | `string` | Relationship | Structural |
| `tkg_valid_from`, `tkg_valid_to` | `Instant` | Both | Temporal — world-time (VT) assertion, caller-only, NO fallback: resolves to `(Instant(0), ok=true)` when never asserted. Accepted by Add **and** Update (since v4.3.0); rejected by `UpdateInPlace` |
| `tkg_tx_from`, `tkg_tx_to` | `Instant` | Both | Temporal — transaction time (TX), stamped by the system: every Add sets `TxFrom`. `tkg_tx_from` is caller-settable on CREATE doors only under `Config.AllowTxBackfill` (backfill, §4.1); rejected elsewhere |
| `tkg_created_at` | `Instant` | Both | Temporal (auto-derived from snowflake ID when unset — the only temporal shadow key with a resolver fallback) |
| `tkg_updated_at`, `tkg_deleted_at` | `Instant` | Both | Temporal |
| `tkg_created_by`, `tkg_updated_by` | `string` | Both | Provenance |
| `tkg_version` | `uint32` | Both | Provenance |
| `tkg_hash`, `tkg_prev_hash` | `string` | Both | Integrity |
| `tkg_base_entity` | `entityID` | Both | Version chain |
| `tkg_from_hash` | `string` | Relationship | Integrity — start-node hash at write time |
| `tkg_to_hash` | `string` | Relationship | Integrity — end-node hash at write time |
| `tkg_author_id` | `string` | Both | Provenance — caller-supplied author identifier |
| `tkg_signature` | `[]byte` | Both | Integrity — caller-supplied cryptographic signature |
| `tkg_authorized_by` | `string` | Both | Authorization — caller-supplied authorizing entity |
| `tkg_auth_level` | `uint8` | Both | Authorization — caller-supplied authorization tier |

## Badger Key Layout

| Key pattern | Purpose | Key size |
|---|---|---|
| `0x01/<8B nodeID>` | Node entity | 9B |
| `0x02/<8B relID>` | Relationship entity | 9B |
| `0x03/<2B labelToken>/<8B nodeID>` | Label index | 11B |
| `0x04/<2B relTypeToken>/<8B relID>` | Type index | 11B |
| `0x05/<8B startID>/<2B relType>/<8B endID>/<8B relID>` | Outgoing adjacency | 27B |
| `0x06/<8B endID>/<2B relType>/<8B startID>/<8B relID>` | Incoming adjacency | 27B |
| `0x07/<8B nodeID>/<8B version>` | Node version history | 17B |
| `0x08/<8B relID>/<8B version>` | Rel version history | 17B |
| `0x09/<8B LSN>` | Change-log (op-log) record — opt-in `ChangeLog`; value = `tag(1B) ‖ msgpack(body)` | 9B |
| `0x0A/<2B propKeyToken>/<domain-tagged value bytes>/<8B nodeID>` | Property index entry — opt-in `PropertyIndexOnDisk`; numeric domain payload is fixed 18B (order-preserving sortKey + subtype + exact bits), raw domain (string/bool/temporal) payload is the canonical value-key bytes, variable length | 29B (numeric) / variable (raw) |
| `0x0F/*` | Metadata (registries, counters, prop index defs, `last_lsn` watermark, `property_index_on_disk_built` marker) | varies |

## Ecosystem

| Module | Role |
|---|---|
| `rho/tkg/v4` | Internal library — graph types, persistence, registries (this repo) |
| `rho/tkgd-v3` | Full product — Cypher engine, Vadalog reasoning, HTTP/gRPC server |
| `rho/kit` | Service toolkit — app builder, logging, tracing, resilience, database |

tkg/v3 does **not** depend on kit. tkgd-v3 depends on both.
