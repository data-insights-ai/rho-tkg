# Repository Maintainability Review - Round 2

Date: 2026-05-08
Branch reviewed: `main`
Commit reviewed: `793dee1`

This pass was run after checking out `main` and fast-forward pulling the latest
changes. It does not repeat the already-fixed first-round findings except where
the follow-up review found a remaining adjacent risk.

## Verification

- `make test` passed.
- `make cover` passed and produced total statement coverage of 80.1%.
- `make cover-gate` passed with `86.6% >= 80%`.

## Findings

### R2-F1 - Tiered admin mutators are not consistently serialized

Severity: High

`AdminOps.Archive`, `AdminOps.Restore`, and `AdminOps.Reset` take `c.mu.Lock()`,
but `ForceRotate`, `RebuildCatalog`, `Repair`, and `VerifyShard` call into the
tiered store directly (`pkg/graph/internal/core/admin.go:21`,
`pkg/graph/internal/core/admin.go:52`, `pkg/graph/internal/core/admin.go:68`,
`pkg/graph/internal/core/admin.go:77`, `pkg/graph/internal/core/admin.go:86`,
`pkg/graph/internal/core/admin.go:95`). That makes some public admin operations
part of the graph transaction exclusion domain while others are not.

The most concrete bug is `Reset` vs. `ForceRotate`. `Reset` claims to
atomically clear all entities, but `tiered.Store.Clear` explicitly documents
that its snapshot-then-clear pattern can miss a concurrent rotation and leave
the new hot shard uncleared (`pkg/graph/store/tiered/tieredstore.go:386`,
`pkg/graph/store/tiered/tieredstore.go:398`). `AdminOps.Reset` cannot prevent
that because `AdminOps.ForceRotate` bypasses `c.mu`.

`Repair` is also a mutating admin path without the graph lock. Phase 2 reads a
relationship, checks the end shard for a missing incoming index entry, and may
recreate it (`pkg/graph/store/tiered/tieredstore_repair.go:73`,
`pkg/graph/store/tiered/tieredstore_repair.go:118`,
`pkg/graph/store/tiered/tieredstore_repair.go:124`). A concurrent relationship
delete can remove the entity and incoming entry after `Repair` has read the
relationship but before it calls `PutRelIncoming`, recreating an orphaned
incoming entry that the delete just removed.

Recommendation: put all mutating admin operations in the same exclusion class
as tx/batch/reset. At minimum, wrap `ForceRotate`, `RebuildCatalog`, and
`Repair` in `c.mu.Lock()` at the `AdminOps` boundary, then add concurrency tests
for `Reset` racing `ForceRotate` and `Repair` racing a cross-shard delete.

### R2-F2 - Hot-shard rotation mutates live topology before durable catalog save

Severity: High

`RotateHotShard` changes the in-memory topology before persisting the catalog:
it demotes the old hot shard, mutates the catalog entry, opens the new shard,
adds the new `EventShard` to `eventShards`, switches `hotShard`, adds the
catalog entry, optionally demotes warm shards, and only then calls
`catalog.Save()` (`pkg/graph/store/tiered/tieredstore_catalog.go:17`,
`pkg/graph/store/tiered/tieredstore_catalog.go:29`,
`pkg/graph/store/tiered/tieredstore_catalog.go:36`,
`pkg/graph/store/tiered/tieredstore_catalog.go:55`,
`pkg/graph/store/tiered/tieredstore_catalog.go:79`,
`pkg/graph/store/tiered/tieredstore_catalog.go:105`).

If `catalog.Save()` fails, the caller receives an error, but the current process
has already rotated. After restart, the durable catalog can still describe the
old topology. That creates a split-brain failure mode: runtime routing accepts
writes into a shard that the persisted catalog may not know about, while
restart recovery sees stale `TimeEnd` and hot/warm state.

Recommendation: make rotation transactional around catalog persistence. Stage
the new shard and catalog changes, save them atomically before exposing the new
`hotShard`, or add a rollback path that closes/removes the new shard and
restores the old in-memory catalog and shard state if `Save` fails. Add an
injected catalog-save failure test.

### R2-F3 - Batch relationship creation still has panic-unsafe endpoint locks

Severity: High

The batch relationship loop manually locks endpoints and unlocks only on known
error branches or after `PutRelationship` returns
(`pkg/graph/internal/core/batch.go:443`,
`pkg/graph/internal/core/batch.go:450`,
`pkg/graph/internal/core/batch.go:505`). If a custom store panics during
`GetNode` or `PutRelationship`, `BatchBuilder.Execute` releases `g.mu` via its
outer defer, but the endpoint shard locks stay held forever.

Standalone relationship paths use the safer local pattern: lock, then
immediately `defer UnlockTwo` (`pkg/graph/internal/core/relationship.go:88`,
`pkg/graph/internal/core/relationship.go:226`). The existing batch panic test
only covers a `PutNodesBatch` panic and verifies `g.mu`; it does not exercise
per-relationship endpoint locks.

Recommendation: wrap each pending relationship execution in an inner function
with a defer-backed endpoint unlock, or introduce a helper mirroring
`addRelationshipInternal`. Add tests with a store that panics from
`GetNode` and `PutRelationship`, then verify a later relationship mutation on
the same endpoints does not deadlock.

### R2-F4 - Basic label/property query is coupled to optional property-index capability

Severity: Medium

The capability split treats `PropertyIndexCapability` as optional, but the
interface combines index management with the basic public query
`NodesByLabelAndProperty` (`pkg/graph/store/capabilities.go:157`,
`pkg/graph/store/capabilities.go:165`). `NodeOps.ByLabelAndProperty` asserts
that optional capability before doing any work and returns
`ErrCapabilityNotSupported` when it is absent
(`pkg/graph/internal/core/graph_property_query.go:25`,
`pkg/graph/internal/core/graph_property_query.go:31`).

That is stricter than the in-tree store behavior. Both memory and Badger stores
implement the query as "use the property index if present, otherwise scan the
label set and filter properties" (`pkg/graph/store/memory/memorystore_index.go:250`,
`pkg/graph/store/memory/memorystore_index.go:288`,
`pkg/graph/store/badger/badgerstore_node.go:743`,
`pkg/graph/store/badger/badgerstore_node.go:780`). The current mandatory-only
backend test even pins that the graph returns unsupported for this query
(`pkg/graph/internal/core/store_capabilities_test.go:43`,
`pkg/graph/internal/core/store_capabilities_test.go:56`).

Impact: future out-of-tree backends that implement mandatory CRUD and
`NodesByLabel` cannot serve a normal graph query that the graph layer could
answer by scanning. They lose correctness, not just acceleration.

Recommendation: split property query from property index management, or add a
graph-layer fallback using `c.store.NodesByLabel` plus property comparison when
`PropertyIndexCapability` is absent.

### R2-F5 - Temporal vector search has backend-dependent top-k correctness

Severity: Medium

`IndexOps.SearchNearest` documents that external stores without the
package-internal `filteredVectorSearchStore` hook fall back to post-filtering:
the backend returns raw top-k, then the graph drops temporally ineligible
matches (`pkg/graph/internal/core/vector_search.go:39`,
`pkg/graph/internal/core/vector_search.go:80`,
`pkg/graph/internal/core/vector_search.go:87`). The hook that provides correct
pre-filtering is unexported and package-internal
(`pkg/graph/internal/core/vector_search.go:111`).

That makes the same public API produce different semantics by backend. In-tree
stores filter before the k-cut; external stores can return fewer than `k`
results even when eligible candidates exist farther away. The documentation
suggests callers over-fetch, but callers do not know a backend-specific safe
over-fetch factor.

Recommendation: promote filtered vector search into the public optional store
capability, or implement an iterative over-fetch path in the graph layer that
continues until it has `k` eligible results or the backend is exhausted.

### R2-F6 - Import staging has no caller-controlled location or capacity boundary

Severity: Medium

`IOOps.Import` was improved to avoid keeping the full import in memory, but it
now stages the entire stream into the process default temp directory via
`os.CreateTemp("", "tkg-import-*.stage")`
(`pkg/graph/internal/core/export.go:233`,
`pkg/graph/internal/core/export.go:244`,
`pkg/graph/internal/core/export.go:258`). For large backups, that can write
multi-GB data to `/tmp` or the platform default temp volume instead of the
graph data volume. There is no configured temp directory, quota, or free-space
preflight.

The import remains non-transactional once replay starts under the graph write
lock (`pkg/graph/internal/core/export.go:247`). A disk-full error during
staging is safe, but a replay error can leave a partially populated graph after
the caller waited for the full staging write.

Recommendation: add import options for staging directory and max staged bytes,
prefer the graph data directory when available, and document failure semantics
in the public API docs. For restore use cases that need atomicity, consider a
fresh-store import helper that swaps the store only after replay succeeds.

### R2-F7 - Public docs still describe removed APIs and product features

Severity: Medium

The source now has a thin `*Graph` facade with sub-API accessors
(`pkg/graph/graph.go:1`, `pkg/graph/graph.go:84`), but `docs/api.md` still
describes `Graph` as owning direct methods such as `AddNode`,
`AddRelationship`, `UpdateNode`, `GetNode`, `Reset`, `ExportGraph`, and
`ImportGraph` (`docs/api.md:21`, `docs/api.md:36`, `docs/api.md:50`,
`docs/api.md:72`, `docs/api.md:74`). `docs/SPEC.md` also shows direct
`tkg.AddNode` calls and Cypher execution even though this repo is a pure
library with no query language (`docs/SPEC.md:567`, `docs/SPEC.md:577`,
`docs/SPEC.md:587`).

There is also a correctness mismatch in `docs/architecture.md`: it says batch
operations are all-or-nothing with "no error exits" in phase 2, but
`BatchBuilder.Execute` records per-operation failures and normally returns a
`BatchResult` with `nil` error (`docs/architecture.md:581`,
`pkg/graph/internal/core/batch.go:330`,
`pkg/graph/internal/core/batch.go:337`).

Impact: new contributors and downstream consumers will follow stale examples,
and future fixes may be designed against the wrong public contract.

Recommendation: update `docs/api.md` and `docs/SPEC.md` to the v3.4 sub-API
surface (`g.Nodes.*`, `g.Rels.*`, `g.IO.*`, `g.Admin.*`, `g.Tx.*`,
`g.Batch.*`), remove Cypher/product examples from this library spec, and align
batch semantics with the current partial-failure result model.

### R2-F8 - Test coverage is broad, but several files are too large to review safely

Severity: Low

The codebase has strong regression coverage, but some critical test files have
grown past the point where local reasoning is cheap:

- `pkg/graph/store/badger/badgerstore_test.go`: 5453 lines.
- `pkg/graph/store/tiered/tieredstore_test.go`: 4362 lines.
- `pkg/graph/internal/core/tieredstore_history_routing_test.go`: 2444 lines.
- `pkg/graph/internal/core/tx_test.go`: 1318 lines.
- `pkg/types/propertyslice_test.go`: 1243 lines.

Large mixed-purpose test files make it easier to miss feature-local coverage
holes, especially in this repo where many bugs have required targeted grep
audits across call sites.

Recommendation: split the largest files by feature boundary, matching the
implementation layout: badger node/rel/history/index/clear, tiered routing/
admin/repair/archive, core tx/batch/admin/vector, and type-specific property
validation/hash/wire cases. Keep regression tests next to the feature they pin
once a fix graduates from a review-specific file.

## Positive Notes

- The pulled `main` includes substantial first-round cleanup: panic-safe
  transaction wrappers, endpoint hash error propagation, explicit optional
  capability tests, import staging, and index rebuild diagnostics.
- The sub-API wrapper coverage is now protected by smoke tests and the coverage
  gate.
- The tiered store code is heavily documented around checkout/checkin and close
  races, which made the remaining admin-locking gaps easier to localize.
