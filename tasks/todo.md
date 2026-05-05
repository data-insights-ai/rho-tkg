# Test Store Cleanup — Explicit MemoryStore + Organization

## Goal
1. Make MemoryStore usage explicit (not hidden behind `Config{}` default)
2. Clean up Badger test data properly (ensure t.Cleanup/defer Close)
3. Move misplaced BadgerStore tests to badgerstore_test.go
4. Convert tests that don't need Badger to MemoryStore

## Phase 1: Make MemoryStore explicit in helpers + graph-layer tests
- [x] Update `newTestGraph()` in batch_test.go
- [x] All `New(Config{})` → `New(Config{Store: NewMemoryStore()})` in graph-layer tests
- [x] All `graph.New(graph.Config{})` → explicit MemoryStore in export_test.go
- [x] v3061_fixes_test.go, v3062_fixes_test.go updated
- [x] pagination_test.go graph-layer tests updated

## Phase 2: Migrate unnecessary Badger tests to MemoryStore
- [x] graph_test.go: TestGraphWithBadgerInMemory → deleted (redundant)
- [x] graph_test.go: TestGraphBadgerUpdateNode → MemoryStore (renamed)
- [x] graph_test.go: TestGraphBadgerUpdateRelationship → MemoryStore (renamed)
- [x] graph_test.go: TestGraphBadgerDeleteNodeCascade → MemoryStore (renamed)
- [x] graph_test.go: TestGraphCloseIdempotent → deleted (duplicate)
- [x] graph_test.go: TestGraphCloseConcurrent → MemoryStore
- [x] graph_test.go: Concurrency stress tests → MemoryStore
- [x] export_test.go: TestExportImport_RoundTrip_BadgerStore → deleted (redundant)
- [x] bench_ingest_test.go: benchGraph → MemoryStore

## Phase 3: Move BadgerStore tests to badgerstore_test.go
- [x] foreach_test.go → badgerstore_test.go: 6 ForEach tests
- [x] pagination_test.go → badgerstore_test.go: 8 pagination tests + seedBadgerStore

## Phase 4: Verify
- [x] `make test` passes (7.9s, down from 9.2s)
- [x] `make test-race` passes (21.9s, down from 25.1s)

# History-aware Graph Semantics — Hash Chain + Temporal Queries (v3.1.7)

## Status
Uncommitted. `make check` green. Local `main` is one FF commit ahead of `origin/main` (MR !2, 03731d4); fix work is unstaged on top.

## Landed in MR !2 (03731d4)
- [x] `VerifyNodeHashChain` rebuilds per-version labels
- [x] `AddNodeLabel` / `RemoveNodeLabel` set bitemporal `TxFrom` / `TxTo`
- [x] History-aware named queries: four `Get*ValidAt` rewrites
- [x] No-op event suppression
- [x] `ImportNodeWithID` / `ImportRelationshipWithID` aligned with normal create paths
- [x] Lesson B30 (per-version metadata)

## Residual gaps closed in this session
- [x] Generic methods detect temporal opts and route through history-aware path: `NodesByLabel(opts)`, `NodesByLabelAndProperty(opts)`, `RelationshipsByType(opts)`
- [x] `findNodeVersionMatchingDuring` / `findRelVersionMatchingDuring` — predicate-aware during-interval helpers (most-recent-overlap was wrong for predicate-during-interval)
- [x] `NodesByLabelPropertyDuring` switched to predicate-aware helper
- [x] `paginateNodes` / `paginateRels` helpers
- [x] Adversarial regression tests + `TestNodeHashChain_InspectsHashValues` (probes hash bytes, prev-hash links, version-distinct hashes; mutation-tested against three breakages)
- [x] Lesson B31 (two-phase tests for history-aware code)
- [x] CLAUDE.md testing rules 15-17 (history-aware two-phase tests, adversarial shape, two-doors-same-shape audit)
- [x] CHANGELOG.md [Unreleased] entry + behavior-change note
- [x] README.md behavior-change callout

## Outstanding
- [x] Commit + push (released as v3.1.7, 2026-05-05)

# Typed Entity IDs — `NodeID` / `RelID` at Graph API Boundary (v3.2.0)

## Goal
Replace raw `snowflake.ID` parameters on Graph methods with typed `NodeID` / `RelID` wrappers. Today the unexported `nodeID` / `relID` wrappers earn nothing: every Graph method takes `snowflake.ID`, so the type system can't catch passing a relID where a nodeID was expected. 902 callsites unwrap via `n.InternalID().SnowflakeID()` — `Internal` is a name that lies about accessibility. Either commit to the wrappers (this plan) or remove them; the current state is friction without safety.

## Why
- Dual-generator scheme (even/odd snowflake node bits) prevents value collision but not type confusion at the API surface.
- Every new Graph method that takes `snowflake.ID` is a fresh opportunity to mix nodeID / relID at runtime (silent `ErrNotFound`).
- Future entity kinds (event entities, schema partitions) compound the problem.

## Decision points (need user input before starting)
- [ ] **Hard-cut vs two-phase**: hard-cut breaks compile and fixes all callsites in one commit; two-phase adds typed siblings, deprecates old, removes next release. Hard-cut wins if `tkgd-v3` release cadence is controlled here.
- [ ] **Scope**: `pkg/graph` + `pkg/types` only. `Store` stays raw `snowflake.ID` (serialization boundary).
- [ ] **Version**: v3.2.0 (breaking).

## Phase 1: Export wrappers in pkg/types
- [ ] Rename `nodeID` → `NodeID` (pkg/types/node.go + cross-package fixups)
- [ ] Rename `relID` → `RelID` (pkg/types/relationship.go + cross-package fixups)
- [ ] `NewNodeID(snowflake.ID) NodeID` constructor (tests, import paths)
- [ ] `NewRelID(snowflake.ID) RelID` constructor
- [ ] `(NodeID).Underlying() snowflake.ID`, `(RelID).Underlying() snowflake.ID` — boundary-only accessors

## Phase 2: Node / Relationship accessors
- [ ] `func (n *Node) ID() NodeID`
- [ ] `func (r *Relationship) ID() RelID`
- [ ] Mark `InternalID()` deprecated (keep one release for downstream migration if two-phase)

## Phase 3: Migrate Graph method signatures
- [ ] graph.go: `GetNode`, `GetNodeHistory`, `GetNodesByIDs`, `AddNodeLabel`, `RemoveNodeLabel`, `CompareAndSetProperty`, `OutgoingRelationshipsForNodes`, `IncomingRelationshipsForNodes`, `UpdateNode`, `DeleteNode`, ~20 more
- [ ] context.go: `*WithContext` + `*Internal` variants (~30 methods)
- [ ] temporal.go: `GetNodeAt`, `GetNodeValidAt`, `GetNeighborsValidAt`, ~12 more
- [ ] tx.go: `GraphTx` CRUD methods
- [ ] batch.go: `BatchBuilder` Add/Update/Delete signatures
- [ ] Mirror for relationship methods (`AddRelationship`, `GetRelationship`, `DeleteRelationship`, `ReplaceRel`, ...)

## Phase 4: Migrate callsites
- [ ] ~902 `InternalID().SnowflakeID()` → `.ID()` across pkg/graph tests
- [ ] Downstream callsites in `tkgd-v3`

## Phase 5: Cleanup + audit
- [ ] Remove deprecated `InternalID()` (next release if two-phase, this release if hard-cut)
- [ ] CLAUDE.md audit rule: no `snowflake.ID` parameters on Graph methods that name a node or rel
- [ ] CHANGELOG.md v3.2.0 breaking-change callout + migration notes
- [ ] README.md migration table

## Out of scope
- `Store` interface (persistence boundary; raw `snowflake.ID` stays)
- `BadgerStore` / `TieredStore` internals
- Wire format / msgpack serialization
