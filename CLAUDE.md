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
Status: [Unreleased] — horizontal-scaling Phase 1: log-shipped read replicas. A graph opened with `graph.Config.ReadOnlyReplica` bootstraps from an export snapshot then tails a primary's change-feed to a BYTE-EXACT copy. `g.Replication().ApplyChange(rec)` / `ApplyChanges(recs)` reproduce the primary's rows VERBATIM through foreign-ID store doors — integrity hash / version / `TxFrom` / temporal metadata reconstructed from the record, never re-minted or re-stamped (apply reuses the import trust pipeline per record: `SafeUnmarshal`→`WireTo*Checked`→token-validate→prop-limits→hash recompute-AND-COMPARE; idempotent under at-least-once redelivery). `AppliedLSN()`/`SetAppliedLSN(lsn)` are the durable replica watermark (`meta/replica_applied_lsn`, distinct from `last_lsn`); resume tailing from `AppliedLSN()` after restart. The read-only gate is a CORE-layer `c.checkWritable()` (new `ErrReadOnlyReplica`) on every user mutation door + `Tx().Begin`/`Batch.Execute`/`Admin().Reset` — reads, `g.IO().Import` (bootstrap), and `ApplyChange` stay open; NOT the badger `ReadOnly` store mode (which kills the change-log). The `ChangeNodePut`/`ChangeRelPut` body was reshaped to `storeutil.NodePutBody`/`RelPutBody` = wire + a `WithHistory` bit (so a replica reproduces history depth exactly: with-history→`ReplaceNodeWithHistory` regenerating the exact prior row from its in-sync local current; else `ReplaceNode`/`PutNode`; label changes diffed and routed to the label-token doors); the put-record wire is now built UNTOKENIZED (`NodeToWireChecked`) in both backends → byte-identical feeds even for property-bearing entities. Deferred to the next Phase-1 increment: gapless export-header `SnapshotLSN`, lazy token-registry refetch for post-snapshot labels/rel-types, failover (ID-slot lease + promotion-by-reopen). See `CHANGELOG.md` `[Unreleased]`, `tasks/horizontal-scaling.md`, lessons 49–50. Phase 0 — durable ordered change-log (op-log) + `g.Replication()`. Opt-in `graph.Config.ChangeLog` records every committed mutation under a new `0x09` keyspace, tagged with a monotonic cluster LSN, in the SAME Badger `WriteBatch` as the data + counters (atomic; LSN reseeds crash-consistently from a `meta/last_lsn` watermark at open). Surfaced via `g.Replication().ChangeFeed/ForEachChange/LastCommittedLSN` (the new optional `store.ChangeFeedCapability`; tiered declines with `ErrCapabilityNotSupported`). Tags: `NodePut`/`RelPut`, `NodeDelete`/`RelDelete` (hard-cascade vs with-history), `NodeHistoryVersion`/`RelHistoryVersion`, `NodeHistoryTruncate`/`RelHistoryTruncate`, `Clear`; bodies decode through `SafeUnmarshal` (corrupt `0x09` → `ErrCorruptWire`). Memory parity via `memory.WithChangeLog()` (in-RAM, non-durable; both backends share `storeutil` body builders → byte-identical feeds; cross-backend parity test). Emitted IN-BACKEND (not a `Store` decorator) for crash-safety + native trust; the log ALONE does not converge a replica (bootstrap from an export snapshot, then tail). Deferred: `MetaSet`/`ChangeMeta`, tiered change-log, LSN-watermark log GC. See `CHANGELOG.md` `[Unreleased]`, `tasks/horizontal-scaling.md`, and lesson 49. v4.9.4 — node property-key presence stats + streaming ForEach + bulk AddNodes. `g.Stats().NodeCountByLabelAndPropertyKey(label, propertyKey)` is an O(1) per-`(label, property-key)` presence count over current nodes carrying an indexable scalar value for the key (key-presence only, NOT value-selectivity — a planner can cheaply prune labels that cannot satisfy scalar-equality lookups). Backed by the new optional `store.NodePropertyKeyStatsCapability` (core type-asserts; returns `ErrCapabilityNotSupported` on a mandatory-only backend); counters maintained on every node-mutation door (`PutNode`/`DeleteNode`/`ReplaceNode`/`Add`+`RemoveNodeLabelToken`) and rebuilt at index load so they survive restart; memory/badger/tiered (tiered folds reference + archive + per-event-shard counts into one cross-shard total). `g.Nodes().ForEach(opts, fn)` streams current-state unpaginated scans one row at a time (O(1) peak memory; `fn` returning false stops early; temporal/paginated opts fall back to `All`; concurrently deleted rows skipped, not surfaced). `BatchBuilder.AddNodes(labels, props, count)` is a write-only bulk node create (no caller-visible skeletons → not usable as rel endpoints) enabled by hash-suffix precomputation (`integrity.PrecomputeNodeHashSuffixChecked`/`ComputeNodeHashFromSuffix` encode the static label+property tail once and reuse it per node). Plus: temporal point queries skip history when the current row answers — `nodeAtLockedTx`/`relAtLockedTx` short-circuit on the open current tile (visible at `txAt`, covering `validAt`) without loading the version chain (flat ~95–215 ns / 4 allocs regardless of history depth; historical valid times still fall through). See `CHANGELOG.md` `[4.9.4]`. v4.9.3 — import framing amplification hardening. The import trust boundary no longer lets a tiny untrusted stream amplify into a huge allocation (a memory-exhaustion DoS): `readImportStageRecord` reads each record body with `io.CopyN` into a growing buffer (pre-reserving at most 64 KiB) instead of `make([]byte, declaredLength)`, so a 5-byte header claiming 128 MiB costs ~64 KiB not 128 MiB; and `importPreallocLimit` (the cap on replay/rollback map+slice prealloc derived from the export header's untrusted node/rel counts) is lowered `1<<20` → `4096`, so a ~20-byte header claiming 1M counts costs ~hundreds of KiB not ~312 MiB. Both fail closed with `ErrCorruptExport`. Found by a new end-to-end `FuzzImport` harness (`import_fuzz_test.go`) whose 0-execs/sec stall flagged the DoS, pinned by `runtime.TotalAlloc`-bounded regression tests (`import_amplification_test.go`). See `CHANGELOG.md` `[4.9.3]` and lesson 48. v4.9.2 — wire decode trust-boundary hardening. Untrusted msgpack bytes (a corrupt disk row or a hostile import stream) can no longer crash the process at the decode boundary: `storeutil.SafeUnmarshal` is the decode at every untrusted-bytes site. It runs `guardMsgpackDepth` (a non-recursive scan that rejects container nesting beyond `maxWireDecodeDepth` = 64 BEFORE the decoder runs — preventing the **fatal, unrecoverable** stack overflow the msgpack decoder hits on a deeply-nested `PropertyWire.Value any`) and recovers the decoder's reflect panic (`SetString/SetInt using unaddressable value`, triggered by a duplicate map key for the interface-typed value field). Both fail closed with the new `store.ErrCorruptWire` sentinel (wrapped as `ErrCorruptExport` at the import boundary). Rerouted: `UnmarshalNodeWireWithKeys`/`UnmarshalRelWireWithKeys`, the custom-property blob decode, every `NodeWire`/`RelWire`/meta decode in the badger backend, and the node/rel/header/registry decodes in core import (tiered's catalog/registry/index metadata files decode flat typed structs with no interface or deeply-nestable field — outside this bug class). Found by a new fuzz harness (`FuzzWireToNodeChecked`/`FuzzWireToRelChecked`/`FuzzUnmarshalNodeWireWithKeys` in `wire_fuzz_test.go`, crasher kept as a corpus seed) and an adversarial decode audit. See `CHANGELOG.md` `[4.9.2]` and lesson 47. v4.9.1 — X5 columnar DocValues + resident entity cache. A per-`(label, property)` columnar value store aligned to a sorted NodeID ordinal vector: `g.Nodes().ForEachDocValues(label, propKeys, fn)` streams group keys + aggregate args from dense columns instead of fetching/decoding each node (downstream grouped 100k aggregation 206ms → 8.8ms, ~23x on badger); `ForEachDocValuesMulti(labels, …)` does the same for a label INTERSECTION (`(p:A:B)`), `DocValuesSnapshot(label, propKeys)` returns a random-access `types.NodeColumnReader` for point lookups; `NodeMutationEpoch()` / `RelMutationEpoch()` are the staleness counters (node-only vs adjacency-reading aggregations) re-checked after the lock-free scan. Columns immutable once built, full label membership (never valid-time filtered), present-bitset for absent props, int64-vs-float64 preserved, dictionary-encoded strings; memory + badger backends, declines on tiered / on-disk index. Resident entity cache `graph.Config.ResidentCache` keeps every decoded node/rel resident (no LRU eviction), removing the decode-on-eviction re-msgpack cost that made a working set larger than the cache scale super-linearly (downstream cypher k-hop path 663→850→1190 ns flattened to 189→225→239, ~5x; opt-in, off by default — LRU preserved for disk-backed use). See `CHANGELOG.md` `[4.9.1]`. v4.9.0 — read/write performance + bitemporal cascade correctness. Reads: scan-resistant non-promoting entity cache (`GetNoPromote` — scans no longer fill the LRU, concurrent scanners stop serializing on the cache mutex); endpoint-carrying adjacency index + streaming `ForEachAdjacentEndpoint` (decode-free one-hop expand); inline valid-time adjacency stamps + `ForEachAdjacentEndpointAt` / `ForEachAdjacentRelAt` + canonical `g.Temporal().RelMatchesValidTime` (decode-free temporal traversal — OPT15); `g.Nodes().RangeCardinality` O(buckets) range count over the sorted numeric index, declines on temporal/imprecise opts (R1); native badger reverse-scan `AsOf` (O(versions newer than the query)); maxto-augmented temporal interval index (output-sensitive stabbing); single-allocation property index keys (numeric/float/temporal); opt-in `QueryOpts.NoSort` scan lever. Writes: `badger DetectConflicts=false` (the store owns conflict semantics above Badger). Fixed: append-only cascade — `SetNodeVersionInterval` / `SetRelVersionInterval` corrections no longer mutate stored rows (the in-place rewrite left holes and leaks in `NodeAtTx`/`RelAtTx` and diverged the native `NodeAsOf` reverse-scan from memory/tiered); cross-backend `NodeAsOf` / `RelAsOf` selection now deterministic (highest `(TxFrom, version)`; the resolver orders the TxFrom-filtered chain by effective valid-from and selects the newer belief on overlap) — lesson 46. See `CHANGELOG.md` `[4.9.0]`. v4.8.0 — per-shard Badger footprint tuning. `ValueLogFileSize` / `MemTableSize` / `BlockCacheSize` / `NumCompactors` added to `badger.Config`, `tiered.Config`, and `graph.Config`; **zero keeps Badger stock** (deliberate library decision — one instance opens per shard, so stock sizes multiply by shard count). Validated at `New` (vlog `[1MB,2GB)`, memtable `[8MB,1GB]`, cache `>=0`, compactors `0` or `>=2`) and applied through one shared `buildBadgerOptions` path. Per-shard entity-cache byte budget `CacheBudgetBytes` plumbed through `tiered.Config` (the per-shard heap knob when many shards are open). **Fixed**: shrinking `MemTableSize` on a data dir that still holds live WALs replayed them into a too-small arena and Badger `log.Fatal`/`os.Exit`-ed the WHOLE process (not a recoverable error/panic) — `badger.MigrateOversizedWAL` flushes such WALs at their original size first, `New` runs it on the writable open, the tiered recovery path migrates BEFORE its read-only probe (a read-only open replays WALs too, so it would crash identically), and a read-only or above-1GB-cap open fails closed with `ErrOversizedWAL` instead of crashing (lesson 45). Badger bumped v4.9.1 → v4.9.2. See `CHANGELOG.md` `[4.8.0]`. v4.7.0 — performance: scan-resistant badger entity cache (scans no longer fill the LRU), configurable `core.Config.CacheCapacity`, ordered numeric index view + streaming range/label/rel scans (`ForEachByLabel`/`ForEachByType`/`ForEachOutgoing`/`ForEachIncoming`/`ForEachByLabelPropertyRange`), byte-budgeted caches, O(dirty) dirty-set flush index. See `CHANGELOG.md` `[4.7.0]`. v4.6.1 — adversarial-campaign fixes (rounds 2–4): frozen scan rows can no longer poison the canonical cache (`Temporal()`/`Integrity()` return independent copies on FROZEN entities — the exported-field pointer escape was the one hole the frozen guard couldn't cover); bitemporal TX visibility is recorded-by-then (`TxFrom <= txAt`; superseded ≠ retracted — lesson 43; the old `< TxTo` clause broke `NodeAtTx(oldVT, now)` after any update); `AddByIDIfAbsent` found-branch returns a mutable copy again; import is a real trust boundary (truncations classify as `ErrCorruptExport`; per-row content hashes recomputed + full chain verification post-replay, mismatch → rollback — lesson 44). Round 4 (index/constraint/pagination/events/tiered batteries) found no defects. See `CHANGELOG.md` `[4.6.1]`. v4.6.0 — architecture-review remediation: on-disk wire format versioned (per-row `FormatVersion` + `wire_format_version` meta marker; newer data fails closed with `ErrWireFormatVersionUnsupported`); canonical temporal predicates in `storeutil` shared by core resolver and store push-down, with the cross-door equivalence test that found and fixed the inherited-valid-time bug in label/property mutations (lesson 42); one shared relationship-create kernel for all four doors; badger async write buffer bounded (`MaxPendingWrites`, writer backpressure, `PendingWriteCount()`); `tiered.Store.RecoverBackgroundError()` + repair/contention observability; sentinel anti-drift identity test; `types:` error prefixes. See `CHANGELOG.md` `[4.6.0]`. v4.5.0 — performance: frozen rows, zero-copy scan reads. `types.Node.Freeze()` / `types.Relationship.Freeze()` + `IsFrozen()`; stores freeze cache/canonical-map entries when published and all plural/scan reads (`*ByLabel*`, `All*`, `Get*ByIDs`, adjacency traversals, temporal/index scans) return shared frozen pointers instead of per-row deep copies. Point reads (`GetNode`/`GetRelationship`) still return mutable deep copies. Error-returning mutators return `types.ErrFrozenNode`/`types.ErrFrozenRelationship` on frozen entities; void/bool mutators panic; `DeepCopy()` is the thaw operation. See `CHANGELOG.md` `[4.5.0]`. v4.4.2 pinned down valid-time vs transaction-time semantics ("Three timestamps, three claims" in Temporal Queries; shadow resolver returns RAW asserted `tkg_valid_from` — `(Instant(0), ok=true)` when never asserted — while temporal queries use the EFFECTIVE valid-from with snowflake fallback; see `CHANGELOG.md` `[4.4.2]`). v4.4.1 synced stale doc version strings. v4.4.0 moved the repository to `github.com/data-insights-ai/rho-tkg`; Go module path renamed `gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4` → `github.com/data-insights-ai/rho-tkg/v4` (public API unchanged, `/v4` suffix unchanged; see `CHANGELOG.md` `[4.4.0]`). Bitemporal support shipped in v4.3.0 — Update accepts `tkg_valid_from` / `tkg_valid_to`, `QueryOpts.TxAt` + `NodeAtTx` / `RelAtTx` / `NodesAtTx` / `RelsAtTx` for bitemporal point queries, `SetNodeVersionInterval` / `SetRelVersionInterval` for tile-clean timeline edits, resolver no longer conflates TX with VT in `vEnd` derivation, new sentinel `ErrValidFromBeforePrevious`. See `CHANGELOG.md` `[4.3.0]` for details and lessons 32–34. Thin `*Graph` façade — only `New` / `Close` plus 15 sub-API accessor methods: `g.Nodes()`, `g.Rels()`, `g.Temporal()`, `g.Index()`, `g.Events()`, `g.Constraints()`, `g.IO()`, `g.Admin()`, `g.Tier()`, `g.Stats()`, `g.Hash()`, `g.Resolve()`, `g.Replication()`, `g.Tx()`, `g.Batch()`. The accessors are nil-safe (calling on a nil or zero-value `*Graph` returns nil; chained calls fail closed with `ErrNilGraph`). Implementation lives on `*core.Core` in `pkg/graph/internal/core/`.

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
make ci             # full pipeline: fmt-check + vet + lint + build + test-race + security + vulncheck
make fmt            # format code
make lint           # golangci-lint (errcheck, govet, staticcheck, revive, ...)
make security       # gosec static analysis
make vulncheck      # govulncheck for known CVEs
```

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

`Graph` holds `core *core.Core` + 14 unexported sub-API pointers. The exported surface is 14 accessor methods (`g.Nodes()`, `g.Temporal()`, etc.). Customers interact via the sub-APIs (`g.Nodes().Add(...)`, `g.Temporal().NodesAt(...)`, etc.); the old direct `g.AddNode(...)` form was removed in v3.4.0 and the field form (`g.Nodes`) was removed in v4.2.0.

| File | Purpose |
|---|---|
| `graph.go` | Thin façade — `Graph` struct with `core *core.Core` + 14 unexported sub-API pointers. Methods: `New`, `Close`, and 14 nil-safe sub-API accessor methods (`Nodes() *nodes.API`, etc.). Plus `Config`, `ValidationLimits`, `IDComponents`, `ConstraintSet` type aliases re-exported from internal/core. |
| `subapi.go` | `TxAPI` and `BatchAPI` — sub-API accessors for `g.Tx` and `g.Batch`. Live in `pkg/graph` itself (not a sibling) because they wrap the pkg/graph-private `*GraphTx` / `*BatchBuilder` types defined inside `pkg/graph/internal/core`. `TxAPI.Run` / `TxAPI.RunContext` add closure-style helpers. |
| `errors.go` | Public sentinel re-exports — store sentinels (`ErrNodeNotFound`, … 12 entries), vector-index sentinels, registry sentinels (`ErrEmptyName`, `ErrRegistryNotEmpty`), index-provider sentinels. Canonical declarations in `internal/core/core.go`. |
| `subapi_smoke_test.go` | Compile-and-run smoke test exercising every sub-API accessor end-to-end. |
| `doc.go` | Package documentation. |

### Sub-API & types packages under `pkg/graph/`

Each sub-API package declares a local `Ops` interface listing only the methods its wrappers forward to. `*core.Core` (in `internal/core`) satisfies each interface implicitly. Wrappers are 1–2 lines. Some packages (temporal, index, events) export both the sub-API and the public types customers reference; others are pure types-only or pure wrapper-only. Customer-facing names use the field on Graph (column 2), not the import path.

| Package | Field on Graph | Purpose |
|---|---|---|
| `pkg/graph/nodes` | `g.Nodes` | Node CRUD, label, property, version chain (~31 wrappers). |
| `pkg/graph/rels` | `g.Rels` | Relationship CRUD, adjacency, property, version chain (~30 wrappers). |
| `pkg/graph/temporal` | `g.Temporal` | Point-in-time, interval, bitemporal, snapshot/diff, Allen relations (~24 wrappers). Also exports `GraphSnapshot`, `SnapshotDiff`, `NodeUpdate`, `RelUpdate`, `TemporalConstraint`, `ConstraintSet`, 7 constraint sentinels. |
| `pkg/graph/index` | `g.Index` | Property/vector/high-freq index management + `SearchNearest` + provider registration (~13 wrappers). Also exports `IndexProvider`, `Initializable`, `GraphReader`, IndexProvider sentinels. |
| `pkg/graph/events` | `g.Events` | Sync/async EventBus management (~3 wrappers). Also exports `Event`, `EventType`, `EventPriority`, `EventHandler`, `EventBus`, `AsyncEventBus`, `BackpressureStrategy`, constructors. |
| `pkg/graph/constraints` | `g.Constraints` | Temporal-constraint set management (~3 wrappers). |
| `pkg/graph/io` | `g.IO` | Export / Import (~2 wrappers). Shadows stdlib `io` — alias `tkgio` at consumer sites. |
| `pkg/graph/admin` | `g.Admin` | Backend-agnostic admin: `Reset`, `DecomposeNodeID`, `DecomposeRelID`. |
| `pkg/graph/tier` | `g.Tier` | Tiered-store admin: `Archive`, `Restore`, `ForceRotate`, `ListShards`, `RebuildCatalog`, `Repair`, `VerifyShard`. Reuses `core.AdminOps` as its `Ops`. |
| `pkg/graph/stats` | `g.Stats` | Count helpers (~6 wrappers). |
| `pkg/graph/hash` | `g.Hash` | Hash-chain verification (~2 wrappers). Shadows stdlib `hash` — alias `tkghash` at consumer sites. |
| `pkg/graph/resolve` | `g.Resolve` | Shadow-property accessors: `NodeProperty`, `RelProperty`. |
| `pkg/graph/store` | — | `Store` interface, `QueryOpts`, `ShardDepth`, `RelTombstone`, `DistanceMetric`, `ChangeFeedCapability` + `ChangeRecord`/`ChangeTag` (optional op-log), 12 store sentinels. |
| `pkg/graph/replication` | `g.Replication` | Change-log / op-log accessor: `ChangeFeed`, `ForEachChange`, `LastCommittedLSN` (forwards to `store.ChangeFeedCapability`; `ErrCapabilityNotSupported` when absent). Replica apply (Phase 1): `ApplyChange`/`ApplyChanges` (write the primary's wire verbatim, bypass the read-only gate), `AppliedLSN`/`SetAppliedLSN` (durable replica watermark). |
| `pkg/graph/store/memory` | — | `memory.Store`, `memory.New()`. |
| `pkg/graph/store/badger` | — | `badger.Store`, `badger.Config`, `badger.New()`. |
| `pkg/graph/store/tiered` | — | `tiered.Store`, `tiered.Config`, `tiered.New()`, `MigrateFromBadger`, `ShardInfo`, `VerifyResult`, `RepairResult`, 4 sentinels. |
| `pkg/graph/ontology` | — | `EntityClass`, `OntologyMapping`, `NewOntologyMapping`, `ClassEvent`, `ClassReference`. |

### `pkg/graph/internal/*`

| Package | Purpose |
|---|---|
| `internal/core` | `Core` type holding shared unexported state (mu, store, registries, locks, generators, indexProviders, …) plus 11 sub-Core types (`NodeOps`, `RelOps`, `TempOps`, `IndexOps`, `EventOps`, `AdminOps`, `ConstraintOps`, `HashOps`, `IOOps`, `ResolveOps`, `StatOps`) declared in `subops.go`. Sub-Core types hold a `c *Core` back-reference; method bodies live on the sub-Core types. Wired in `core.New` as exported fields (`c.Nodes`, `c.Rels`, …) so the wrapper packages can satisfy each sub-API's local `Ops` interface. Method names on sub-Core types drop their type prefix (`AddNode → NodeOps.Add`, `GetRelationship → RelOps.Get`, `VerifyNodeHashChain → HashOps.VerifyNodeChain`, etc.) so the call chain is uniform across the wrapper boundary. ~7.5K LOC of implementation across ~30 files; ~28K LOC of internal tests across 53 test files. `g.Tier` reuses `c.Admin` (there is no `TierOps`). |
| `internal/snowflake` | Snowflake `Epoch`, `Layout`, `IDComponents`, `DecomposeID`. Single source of truth for ID-bit decomposition. Imported by `internal/locks`, `internal/storeutil`, `internal/core`, `pkg/graph/store/{badger,tiered}`. |
| `internal/storeutil` | Store-internal helpers: key encoding (`NodeKey`, `RelKey`, `LabelIndexKey`, etc.), msgpack wire types (`NodeWire`, `RelWire`), pagination helpers (`PaginateIDs`, `PaginateNodes`, etc.), temporal-filter push-down (`EntityValidFrom`, `MatchesTemporalFilter`). The public Store contract lives in `pkg/graph/store`. |
| `internal/locks` | `Manager` — 256-shard entity-lock manager, `LockEntity`/`LockTwo`/`LockMany` in ascending order. |
| `internal/registry` | `LabelRegistry` and `RelTypeRegistry` — thread-safe string-to-uint16 token registries. Internal — not public API. |
| `internal/index` | In-memory indexes only: property index, vector index, high-frequency temporal index, `OntologyMapping`. The label/reltype registries live in `internal/registry`. |
| `internal/integrity` | Pure SHA-256 hash primitives — `ComputeNodeHash`, `ComputeRelHash`, `appendProperties`, `appendPropertyValue`. Five fixed-vector hash anchors lock the on-disk hash format. |

### Configuration

- **`Graph.Config`**: `SnowflakeNodeID` (0–15), `Store`, `BadgerDir`, `BadgerInMemory`, `Validation` (ValidationLimits). Whitespace-only `BadgerDir` rejected. Also accepts `SyncWrites bool`, `Compression`, `ZSTDCompressionLevel`, `CacheCapacity`, `CacheBudgetBytes`, `LabelIndexOnDisk`, `AdjacencyIndexOnDisk`, `ValueLogFileSize`, `MemTableSize`, `BlockCacheSize`, `NumCompactors` — these pass through to the underlying `BadgerStoreConfig` (ignored when `Store` is supplied explicitly).
- **`ValidationLimits`**: `MaxLabelsPerNode` (50), `MaxPropertiesPerEntity` (1000), `MaxPropertyKeyLength` (256), `MaxPropertyValueSize` (64K strings), `MaxNameLength` (256). `AllowSelfLoops` (default `false` — reject self-loop relationships where start == end; set `true` to permit). Zero = default for numeric limits.
- **`BadgerStoreConfig`**: `Dir`, `InMemory`, `Logger` (nil = default), `CacheCapacity` (10K), `FlushInterval` (100ms), `GCInterval` (5min), `GCDiscardRatio` (0.5), `ReadOnly` (warm/cold shards), `SyncWrites` (fsync each write — disables async buffer, forces `FlushInterval=0`), `MaxPendingWrites` (100K ops — async write-buffer bound; at the bound the writer flushes synchronously; negative disables; moot under SyncWrites), `Compression` (`options.None`/`Snappy`/`ZSTD`, zero = Badger default Snappy), `ZSTDCompressionLevel` (1–15, zero = Badger default 1), `CacheBudgetBytes` (0 = off — byte-budgeted entity caches via `types.ApproxHeapBytes`; soft limit, dirty entries never evicted; with `CacheCapacity` 0 the byte budget alone governs), `LabelIndexOnDisk` / `AdjacencyIndexOnDisk` (0/false = RAM maps — opt-ins answer label/adjacency snapshots from the persisted keyspaces via prefix iteration with a pending-write overlay; no migration for existing dirs), `ValueLogFileSize` / `MemTableSize` / `BlockCacheSize` / `NumCompactors` (0 = Badger stock; validated vlog `[1MB,2GB)`, memtable `[8MB,1GB]`, cache `>=0`, compactors `0` or `>=2`; one shared `buildBadgerOptions` path; shrinking `MemTableSize` on a live-WAL dir triggers `MigrateOversizedWAL` — a read-only or above-1GB-cap open over an oversized WAL fails closed with `ErrOversizedWAL` rather than `os.Exit`).
- **`TieredStoreConfig`**: `DataDir`, `InMemory`, `RefLabels`, `ShardWindow` (1 week), `CacheCapacity` (10K), `FlushInterval` (100ms), `ColdAfter` (0=never), `IdleTimeout` (5min when cold enabled), `Compression`, `ZSTDCompressionLevel`, `ValueLogFileSize` / `MemTableSize` / `BlockCacheSize` / `NumCompactors` (per-shard Badger footprint; 0 = stock; same bounds as `BadgerStoreConfig`, validated at `New` via the reference-shard open), `CacheBudgetBytes` (per-shard entity-cache byte budget; the per-shard heap knob across many open shards). All pass through `badgerCfg` to every shard (reference, hot, warm, lazy cold/archive, rotation-created).

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
- **Lock ordering**: entity locks -> idxMu. Always.
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
- **Canonical temporal predicates live in storeutil**: `EntityValidFrom` / `MatchesPointInTime` / `MatchesInterval` are the ONLY definitions of effective-valid-from and the point/interval predicates; core delegates, stores push down the same functions. `TestTemporalTwoDoorsAgreeOnLabelQueries` enforces cross-door agreement.
- **Relationship creation goes through one kernel**: all create doors (Add / AddByID / AddByIDIfAbsent / batch Execute) share `relationship_create_kernel.go` (`prepareRelCreate` → `relEndpointHashLadder` → `createRelWithTypeRollback`). Add creation invariants THERE, not in a door.
- **Badger WriteBatch.Flush() blocks forever on closed DB**: Badger v4 uses `context.Background()` in `oracle.readTs()` → `WaitForMark()`, so closing the DB stops oracle goroutines and any in-flight `WriteBatch.Flush()` blocks forever. Fix: `BadgerStore.dbClosed atomic.Bool` is set to `true` in `Close()` BEFORE `db.Close()`. `flush()` checks it and returns `ErrDBClosed` immediately. Any test that calls `bs.db.Close()` directly MUST set `bs.dbClosed.Store(true)` first. Never call `WriteBatch.Flush()` after `db.Close()`.
- **Decode untrusted msgpack only through `storeutil.SafeUnmarshal`**: Raw `msgpack.Unmarshal` of persisted/imported bytes is unsafe at the trust boundary — the vmihailenco decoder PANICS via reflect on a duplicate map key bound to an interface field (`PropertyWire.Value any` → `SetString/SetInt using unaddressable value`) and FATALLY stack-overflows (unrecoverable) on deep nesting. `SafeUnmarshal` runs `guardMsgpackDepth` (non-recursive, rejects nesting > `maxWireDecodeDepth`=64 before the decoder runs) then recovers any panic, returning `store.ErrCorruptWire`. Every `NodeWire`/`RelWire`/meta/custom-property decode in badger + core import routes through it (`msgpack.Marshal` is panic-free, no wrapper). When adding a new decode-from-bytes site (or a sharded backend), use `SafeUnmarshal`, never raw `msgpack.Unmarshal`. Audit recipe: `grep -rn 'msgpack.Unmarshal(' pkg/ | grep -v _test`. See lessons.md 47.
- **Change-log (op-log) is IN-BACKEND and co-committed, never a Store decorator**: when `ChangeLog` is enabled, each backend appends a framed record under `0x09/<LSN>` in the SAME `WriteBatch` as the data + counters + a `last_lsn` watermark — crash-consistent, atomic with the mutation. A decorator over `Store` cannot reach the inner `WriteBatch` (committed-but-unlogged window) and would be treated as an untrusted store (loses frozen-pointer scans), so the log lives inside badger/memory as the optional `store.ChangeFeedCapability`, type-asserted by core (`changeFeedCapability` guards against a wrapper promoting a shard's per-shard feed). The LSN is assigned + the record buffered under ONE `wbMu` window with the entity ops (doors lacking `idxMu` use `appendOpsLogged`); every early-return-after-snapshot in `flush()` requeues logs; the empty guard is `len(ops)==0 && len(logs)==0`. Both backends share the `storeutil` body builders so feeds are byte-identical (cross-backend parity test). The log ALONE does not converge a replica — bootstrap from an export snapshot (registry included), then tail. When adding a mutation door, emit exactly ONE logical record from the PUBLIC door (shared helpers like `deleteRelByInfo`/`cascadeDeleteInner` stay record-free). The `ChangeNodePut`/`ChangeRelPut` body is `storeutil.NodePutBody`/`RelPutBody` (wire + `WithHistory` bit); the put-record wire is built UNTOKENIZED via `NodeToWireChecked` (property keys as strings) in BOTH backends — so feeds are byte-identical even for property-bearing entities, and a put record carries no property-key registry dependency. A new put-door must set `WithHistory` to true IFF it wrote a version-history row (`ReplaceNodeWithHistory` / `*LabelTokenWithHistory`), false for create / in-place `ReplaceNode` / no-history label doors; badger and memory MUST agree (the replica reproduces history depth from this bit). See lessons.md 49–50 and `tasks/horizontal-scaling.md`.
- **Replica apply reproduces the primary's rows VERBATIM, never re-stamps**: `g.Replication().ApplyChange` (in `apply_record.go`, under `c.mu.Lock`, bypassing `checkWritable`) decodes each record, runs the SAME import-trust pipeline per record (`WireTo*Checked` reconstructs version/`TxFrom`/temporal/integrity verbatim → `validate*TokensInRegistry` → `validatePropertySliceLimits` → `verifyImported*Hash` recompute-AND-COMPARE), then writes through a FOREIGN-ID store door (`PutNode`/`ReplaceNode`/`ReplaceNodeWithHistory`/`Delete*`/`PutNodeVersion`/`Truncate*`/`Trim*`/label-token doors) that persists the supplied entity byte-for-byte. NEVER route apply through `NodeOps.Add/Update` (they re-mint IDs, re-stamp `TxFrom`, recompute hashes). Create-vs-update is inferred from local existence; a `NodePut` whose label set differs from the local row by one token is a label mutation → routed to the matching label-token door. Apply is IDEMPOTENT (identical row = no-op via `nodeWireMatches`/`relWireMatches`; delete tolerates `ErrNodeNotFound`/`ErrRelNotFound`) because the watermark advances via a SEPARATE `MetaSet` after the door commits — the crash window is closed by re-apply, not co-commit. Per-record delete `PrevVersion`s are derived from the replica's OWN local current rows (correct because LSN total-ordering guarantees the replica's local current == the primary's pre-mutation state). The read-only gate is `c.checkWritable()` on user doors; apply and `IOOps.Import` use `checkOpen` only. See lessons.md 50.

### Version History

- **Pre-mutation snapshots**: `Update*` captures deep copy, applies mutations, writes both via `ReplaceNodeWithHistory`/`ReplaceRelWithHistory` atomically.
- **Append-only**: Delete paths save tombstone versions (with `DeletedAt`/`ValidTo`) before deletion. History is never erased on delete.
- **Replace vs Put**: Put rejects duplicates. Replace requires existence. Replace overwrites data only — no index changes (labels/type/endpoints are immutable).
- **Genesis detection**: Use `entry.Version() == 0`, not array position. Array position changes after truncation.

### Temporal Queries

- **Three timestamps, three claims (VT vs TX)**: Do not conflate them.

  | Timestamp | Claim | Who can assert it | State on an unstamped entity |
  |---|---|---|---|
  | `tkg_tx_from` | "the DB recorded this fact at T" | the system — automatically | always stamped: every Add allocates `TemporalMetadata` and sets `TxFrom` |
  | `tkg_created_at` | "the entity record came into existence at T" | system-derived (snowflake ID timestamp); caller may override at Add | always derivable — the shadow resolver applies the snowflake fallback |
  | `tkg_valid_from` | "the fact holds **in the world** from T" | only the domain — a recorder/curator with actual knowledge | `0` = no world-time claim made |

  Writers must NEVER default `tkg_valid_from := now()` without domain knowledge — that silently conflates TX with VT (lesson 32): recording latency leaks into world-time semantics and backfilled facts become indistinguishable from real assertions. If a consumer wants "unstamped ⇒ valid since recorded", implement it as an explicit, flagged heuristic on the consumer side (lesson 36 pattern), not by stamping at write time and not by changing the shadow resolver.
- **Two doors, two views (asserted vs effective)**: The shadow resolver (`g.Resolve().NodeProperty(n, "tkg_valid_from")`, `shadow.go`) returns the RAW asserted value — `(Instant(0), ok=true)` when never asserted. `ok` is true because `TemporalMetadata` always exists (TxFrom stamping); consumers must check the zero value, not `ok`. Temporal queries instead use the EFFECTIVE valid-from (`nodeValidFrom`/`relValidFrom`: explicit `ValidFrom`, else snowflake fallback) — so an entity with unset `ValidFrom` is "eternal" through the shadow door but time-bounded through the query door. This asymmetry is deliberate: shadow props report *stored/asserted* state, temporal queries report *effective* state. `tkg_created_at` is the only temporal shadow key with a fallback in the resolver.
- **Effective valid-from**: Derived from explicit `ValidFrom` or snowflake ID timestamp. Every entity is queryable temporally without `SetTemporal()`.
- **Point-in-time**: `effectiveValidFrom <= t AND (ValidTo == 0 OR ValidTo > t)`.
- **Interval overlap**: `effectiveValidFrom < end AND (ValidTo == 0 OR ValidTo > start)`.
- **Bitemporal (since v4.3.0)**: `QueryOpts.TxAt` filters the chain to versions RECORDED by the given TX time (`TxFrom <= TxAt` — recorded-by-then). TxTo deliberately does NOT bound visibility: superseded is not retracted, and a superseded version remains the authority for its valid-time slot at every later txAt (lesson 43; the old `< TxTo` clause made `NodeAtTx(oldVT, now)` return nothing after any update). `TxAt == 0` keeps "no TX filter" backward-compat. Bitemporal point queries: `g.Temporal().NodeAtTx(id, validAt, txAt)` and counterparts. The resolver computes `vEnd` from `next.ValidFrom` (falls back to `next.UpdatedAt`), so adjacent versions auto-tile once the caller supplies explicit `tkg_valid_from` on Update. See lessons 32–34.
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

### Events & Stats

- **EventBus is opt-in**: `Graph.SetEventBus(bus)` — nil by default (zero overhead). Handlers are copied under RLock, invoked outside the lock (prevents deadlocks from re-entrant Graph calls in handlers).
- **Tx event buffering**: During a transaction (`txEventBuffer != nil`), `publishEvent` appends to a buffer instead of dispatching. On `Commit`, events are published after `g.mu.Unlock()` so handlers can safely call Graph read methods. On `Rollback`, buffered events are discarded — subscribers never see rolled-back mutations.
- **AsyncEventBus for async delivery**: `Graph.SetAsyncEventBus(bus)` — worker pool with per-priority `[5]chan Event` queues. `BackpressureStrategy` controls full-queue behavior (Block/DropOldest/DropLatest). `Close()` drains all pending events before stopping workers. `Graph.events` is typed as `eventPublisher` interface (unexported) — allows either bus type without breaking the external API.
- **EventPriority**: 5 levels — `PriorityNormal` (0, zero value), `PriorityHigh` (1), `PriorityCritical` (2), `PriorityLow` (3), `PriorityDeferred` (4). Graph assigns internally: creates→High, deletes→Critical, updates→Normal. Backward-compatible: existing `Event{}` literals default to PriorityNormal. Priority ordering in `AsyncEventBus` worker uses non-blocking drain per level (Critical first) before blocking select.
- **PublishBatch priority ceiling**: `AsyncEventBus.PublishBatch` raises a per-batch priority ceiling for each priority pass (atomic `batchPriorityCeiling`) and clears it at end-of-batch. The dispatcher's priority scan honours the ceiling so an in-batch wake-up triggered by `BackpressureBlock` filling a queue cannot dispatch a pre-existing lower-priority event before later same-batch higher-priority events have been enqueued. Liveness is preserved: the saturating-batch wake-up still drains same-or-higher priorities.
- **StoreStats opt-in**: Type-asserted in `(*Core).Stats()` (reachable via `g.Stats().Get()`) — avoids polluting the `Store` interface.
- **Atomic operation counters**: 8 `atomic.Int64` fields on Graph — incremented after every successful store write.

### Vector Indexes

- **Not persisted**: Vector indexes are rebuilt from node properties on restart. Documented limitation — acceptable for brute-force k-NN.
- **Store-level scope in TieredStore**: Vector indexes live at the `TieredStore` level (not per-shard) with their own `vectorIdxMu sync.RWMutex`.
- **Auto-maintenance**: All mutation paths (`PutNode`, `ReplaceNode`, `DeleteNode`, `RemoveNodeLabelToken`) update vector indexes.

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

Two independent registries with independent token namespaces. Methods: `GetOrCreate`, `Resolve`, `ResolveAll`, `Lookup`. `GetOrCreate` rejects empty strings (`ErrEmptyName`), warns at 60K tokens, errors at 65535. Persisted in Badger as `meta/label_tokens`/`meta/reltype_tokens`, or in TieredStore as `data/meta/registry.msgpack` (atomic write).

## Shadow Properties (21)

| Key | Type | Applies To | Category |
|---|---|---|---|
| `tkg_labels` | `[]string` | Node | Structural |
| `tkg_type` | `string` | Relationship | Structural |
| `tkg_valid_from`, `tkg_valid_to` | `Instant` | Both | Temporal — world-time (VT) assertion, caller-only, NO fallback: resolves to `(Instant(0), ok=true)` when never asserted. Accepted by Add **and** Update (since v4.3.0); rejected by `UpdateInPlace` |
| `tkg_tx_from`, `tkg_tx_to` | `Instant` | Both | Temporal — transaction time (TX), stamped by the system: every Add sets `TxFrom` |
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
| `0x0F/*` | Metadata (registries, counters, prop index defs, `last_lsn` watermark) | varies |

## Ecosystem

| Module | Role |
|---|---|
| `rho/tkg/v4` | Internal library — graph types, persistence, registries (this repo) |
| `rho/tkgd-v3` | Full product — Cypher engine, Vadalog reasoning, HTTP/gRPC server |
| `rho/kit` | Service toolkit — app builder, logging, tracing, resilience, database |

tkg/v3 does **not** depend on kit. tkgd-v3 depends on both.
