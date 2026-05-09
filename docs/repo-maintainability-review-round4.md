# Repository Maintainability Review - Round 4

Date: 2026-05-09  
Branch: `main`  
Reviewed commit: `5d214f4 fix(core): admin lock discipline + panic-safe public mutation surface (R2-F1+R2-F3)`

This report supersedes `docs/repo-maintainability-review-round3.md`. It intentionally carries forward still-valid round-3 findings and adds every additional issue substantiated in this pass. It is not a top-N list.

## Scope

- Reviewed lifecycle, transaction, batch, import/export, registry, temporal, vector, event, and documentation paths.
- Used source inspection plus grep audits rather than sampling only one file.
- Focus was maintainability, future scalability, correctness bugs, and contract/documentation drift.

## Findings

### R4-F1 - High - Injected Badger stores bypass registry loading and can overwrite persisted registries

Evidence:

- `pkg/graph/internal/core/core.go:250-277` loads persisted registries only when `Core.New` constructs the Badger store from `Config.BadgerDir` / `BadgerInMemory`.
- `pkg/graph/internal/core/core.go:285-294` has a special registry load path for injected `*tiered.Store`.
- `pkg/graph/internal/core/core.go:283` assigns any other injected `Config.Store` directly.
- `pkg/graph/store/badger/badgerstore_meta.go:107-123`, `140-160`, and `177-190` show Badger supports save/load of both registries.
- `pkg/graph/internal/core/core.go:300-313` saves registries on `Close` for any store implementing `registriesPersister`.

Impact:

Opening an existing Badger store separately and passing it via `graph.Config{Store: bs}` starts the graph with empty in-memory registries even though persisted entities use tokenized labels and relationship types. Label/type resolution can return empty strings, label/type queries can miss data, and `Close` can then save the empty/new registry state back over the persisted mappings.

Recommendation:

Introduce a registry loader interface parallel to `registriesPersister` and load registries for any injected store implementing it. If a store is registry-aware but cannot be loaded safely, reject it at construction time rather than silently operating with mismatched token maps.

### R4-F2 - High - `GraphTx` lifecycle checks do not cover in-flight transaction methods

Evidence:

- `pkg/graph/internal/core/tx.go:44-66` documents `GraphTx` lifecycle and has `tx.mu`, `done`, and rollback tracking fields.
- `pkg/graph/internal/core/tx.go:82-99` checks `done`, releases `tx.mu`, mutates the graph, buffers an event, then reacquires `tx.mu` to record rollback metadata.
- `pkg/graph/internal/core/tx.go:493-509` sets `done`, clears the event buffer, and unlocks `g.mu` during commit.
- `pkg/graph/internal/core/tx.go:536-612` sets `done`, clears events, and rolls back based on the slices recorded so far.
- `pkg/graph/internal/core/events_dispatch.go:60-69` appends through `txEventBuffer` without additional synchronization and relies on callers holding `c.mu`.

Impact:

If the same `*GraphTx` is used from more than one goroutine, `Commit` or `Rollback` can run after a method passes the `done` check but before it records rollback state. A rollback can miss a newly created entity; a commit can publish an incomplete event list; `g.mu` can be unlocked while another tx method still assumes it is operating under the transaction's write lock. Concurrent tx methods can also race on `pendingEvents`.

Recommendation:

Either make `GraphTx` explicitly not goroutine-safe in public docs and tests, or make the implementation robust by holding `tx.mu` across each whole tx method, including mutation and rollback-log append. The safer library contract is to serialize all tx methods, including `Commit` and `Rollback`, through the same operation mutex.

### R4-F3 - High - `Core.Close` is not serialized with live operations and there is no closed-state gate

Evidence:

- `pkg/graph/internal/core/core.go:300-313` runs `closeOnce`, closes providers, saves registries, and closes the store without acquiring `c.mu` for the whole close lifecycle.
- `pkg/graph/internal/core/locks.go:18-22` says Close takes a write lock, but `Core.Close` itself does not.
- Public mutations take only `c.mu.RLock()` through `pkg/graph/internal/core/locks.go:23-28`; for example node creation in `pkg/graph/internal/core/node.go:25-33`.
- `pkg/graph/internal/core/index_provider.go:245-260` drains and closes providers, but releases `c.mu` before provider `Close` calls finish.
- `pkg/graph/internal/core/index_provider.go:138-165` can register a provider later because there is no `closed` flag.

Impact:

`Close` can race with add/update/delete/import/export/batch/tx operations and can close the underlying store while an operation is still using it. It can also race with index provider registration, leaving a provider registered after the close path drained the map. Post-close behavior then depends on the backend: memory may still appear to work, while Badger/tiered stores can return closed-store errors.

Recommendation:

Add a `closed` state to `Core` and check it in public entry points. Serialize close with the graph lifecycle lock, but avoid re-locking through `closeIndexProviders` by splitting locked collection from provider closing. Decide on one public sentinel, likely `ErrGraphClosed` or `store.ErrStoreClosed`, and make post-close behavior consistent.

### R4-F4 - High - Export, Snapshot, and VerifyShard claim stronger consistency than their locks provide

Evidence:

- `pkg/graph/internal/core/export.go:85-90` says `Export` holds `c.mu.RLock` and blocks individual add/update/delete mutations.
- `docs/api.md:128` documents `g.IO.Export` as holding `g.mu.RLock` for a consistent snapshot.
- Standalone mutations also use `RLock`, for example `pkg/graph/internal/core/node.go:25-33`, so they are not excluded by export's `RLock`.
- `pkg/graph/internal/core/temporal.go:447-459` says `Snapshot` uses `RLock`, and `snapshotAt` says callers needing strong consistency should hold `RLock`.
- `pkg/graph/internal/core/admin.go:119-128` says `VerifyShard` blocks writers with `RLock`.
- `docs/architecture.md:185-187` correctly says read isolation is not provided, which conflicts with the export/snapshot/admin comments.

Impact:

These APIs block tx/batch/write-lock operations, but they do not block standalone mutations. `Export` can write counts, current nodes, histories, relationships, and relationship histories from different moments. `Snapshot` can be torn across its composed temporal reads. `VerifyShard` can observe a graph that is changing under standalone updates and produce false positives or false negatives.

Recommendation:

Either downgrade the documented contract to "best-effort, excludes tx/batch but not standalone mutations", or provide an actual exclusion mechanism for consistent operations. For export, a materialized ID/version snapshot or a dedicated snapshot lock would be clearer than relying on `RWMutex.RLock`.

### R4-F5 - High - Relationship creation/import uses caller-supplied endpoint snapshots after locking endpoints

Evidence:

- `pkg/graph/internal/core/relationship.go:91-94` locks endpoint IDs during `RelOps.Add`, but the code never fetches current endpoint nodes under the lock.
- `pkg/graph/internal/core/relationship.go:113-118` records endpoint hashes from the caller's `startNode` and `endNode` pointers.
- `pkg/graph/internal/core/relationship.go:142-144` checks temporal constraints against those same caller pointers.
- `pkg/graph/internal/core/relationship.go:877-879`, `898-903`, and `923-925` repeat the same pattern in relationship import.
- The stores only verify endpoint existence: memory at `pkg/graph/store/memory/memorystore_rel.go:24-30`, Badger at `pkg/graph/store/badger/badgerstore_rel.go:39-47`.
- Batch relationship creation already refreshes endpoint hashes from the live store under endpoint locks in `pkg/graph/internal/core/batch.go:461-499`.

Impact:

Callers can keep an old `*types.Node`, another operation can update or close that node, and a later relationship creation can still use the stale pointer for `FromNodeHash`, `ToNodeHash`, and temporal-constraint checks. That can store relationship integrity metadata that was not true at write time and can bypass `ConstraintRelWithinEndpoints` by passing stale endpoint validity.

Recommendation:

After acquiring endpoint locks, fetch the current endpoint nodes from `c.store` and use those for endpoint hashes and temporal constraints. Treat missing endpoints as `ErrNodeNotFound` and surface operational store errors.

### R4-F6 - High - Batch relationship creation bypasses temporal constraints

Evidence:

- `pkg/graph/internal/core/relationship.go:142` and `923` enforce `checkTemporalConstraints` for standalone create/import.
- `pkg/graph/internal/core/temporal_constraint.go:20-33` centralizes constraint enforcement.
- `pkg/graph/internal/core/batch.go:186-271` queues relationships but does not call `checkTemporalConstraints`.
- `pkg/graph/internal/core/batch.go:440-510` executes relationship creation with endpoint locking, hash refresh, temporal stamp, and `PutRelationship`, but still does not call `checkTemporalConstraints`.
- `rg -n "checkTemporalConstraints" pkg/graph/internal/core` shows no use from `batch.go`.
- `docs/api.md:124` says `ConstraintRelWithinEndpoints` is checked during relationship creation and import.

Impact:

A graph configured with `ConstraintRelWithinEndpoints` can reject an invalid relationship through `g.Rels.Add` but accept the same invalid relationship through `g.Batch.New().AddRelationship(...).Execute()`. That breaks API parity and makes batch mode a semantic bypass for configured graph constraints.

Recommendation:

During batch execution, after endpoint locks and live endpoint fetches, call `checkTemporalConstraints` before `PutRelationship`. Add parity tests that construct the same invalid relationship through standalone and batch paths.

### R4-F7 - Medium - Relationship updates refresh endpoint hashes without locking endpoints

Evidence:

- `pkg/graph/internal/core/relationship.go:492-494` locks only the relationship ID during update.
- `pkg/graph/internal/core/relationship.go:577-593` fetches start and end nodes to refresh endpoint hashes without locking those endpoint node IDs.
- Node updates lock their own entity ID in `pkg/graph/internal/core/node.go:334-336`.

Impact:

An endpoint node update can race with a relationship update's hash refresh. The relationship can record endpoint hashes from a stale or mixed endpoint state even though the shadow properties describe these as write-time endpoint hashes.

Recommendation:

Lock the relationship and both endpoint IDs in deterministic order before fetching endpoints and replacing the relationship, or document endpoint hashes on relationship updates as best-effort rather than write-time consistent.

### R4-F8 - Medium - Documented sentinel errors are not publicly reachable from the documented packages

Evidence:

- IO sentinels are declared in internal core at `pkg/graph/internal/core/export.go:55-71` and `228-231`.
- `docs/api.md:130` documents `ErrIncompatibleExport`, `ErrIncompatibleRegistry`, `ErrImportSizeLimit`, and `ErrCorruptExport`.
- `pkg/graph/errors.go:30-47` re-exports many core sentinels but not the IO sentinels.
- `pkg/graph/io/api.go:6-57` exposes `ImportOptions` and IO methods but no error variables.
- `pkg/graph/internal/core/txtime.go:12-14` defines `ErrNoVersionAsOf`, while `docs/api.md:83` names it as the bitemporal sentinel.
- `docs/api.md:111` documents `ErrTxDone` unqualified; the graph package does not re-export it, though `store.ErrTxDone` exists.

Impact:

External callers cannot use `errors.Is` with the documented IO or bitemporal sentinels because they live in `internal/core`. For transaction completion, callers have to discover that the sentinel is in the store package even though the docs do not say so.

Recommendation:

Re-export documented sentinels from `pkg/graph/errors.go` or from the relevant public subpackage. If the intended home is another package, update docs to use fully qualified names such as `store.ErrTxDone`.

### R4-F9 - Medium - Mandatory-only label/property fallback treats unindexable values as equal

Evidence:

- `pkg/graph/internal/core/store_capabilities.go:66-92` implements the graph fallback for stores that lack `PropertyIndexCapability`.
- It computes `wantKey := indexpkg.PropertyValueKey(value)` at `pkg/graph/internal/core/store_capabilities.go:80`.
- It compares each candidate via `PropertyValueKey(v) != wantKey` at `pkg/graph/internal/core/store_capabilities.go:87`.
- `pkg/graph/internal/index/property_index.go:30-35`, `64-70`, and `74-112` define an empty property key as "not indexable".
- In-tree stores guard this case before fallback scans: memory at `pkg/graph/store/memory/memorystore_index.go:296-299`, Badger at `pkg/graph/store/badger/badgerstore_node.go:795-798`.

Impact:

For an out-of-tree store implementing only mandatory label scans, a query like "label X where property p equals []any{...}" gets `wantKey == ""`. Every candidate whose stored value is also unindexable also maps to `""`, so the fallback treats distinct slices/maps as equal and returns false positives.

Recommendation:

If `PropertyValueKey(value) == ""`, return no matches or perform a real deep-equality comparison for supported-but-unindexable property values. The behavior should match the in-tree backends.

### R4-F10 - Medium - Temporal vector-search fallback mishandles large `k` near the over-fetch ceiling

Evidence:

- `pkg/graph/internal/core/vector_search.go:116` sets `overfetchCeiling = 65536`.
- `pkg/graph/internal/core/vector_search.go:119-120` initializes `rawK := k` and only loops while `rawK <= overfetchCeiling`.
- `pkg/graph/internal/core/vector_search.go:145-146` doubles `rawK` without clamping to the ceiling.

Impact:

For `k > 65536`, the fallback loop never runs and returns an empty result even if eligible matches exist. For values just below the ceiling, such as `k=50000`, the next probe doubles past the ceiling, so the final capped search at `65536` is skipped. This creates incorrect results only for external vector backends that do not implement `FilteredVectorSearchCapability`, but that is exactly the compatibility path the fallback is intended to support.

Recommendation:

Clamp each probe size to the ceiling and decide explicitly what `k > overfetchCeiling` means. Options are returning a clear error, documenting a capped result, or repeatedly paging if the backend supports it.

### R4-F11 - Medium - IO import accepts tokenized entity records without requiring a header and registry first

Evidence:

- Export always writes header and registry first in `pkg/graph/internal/core/export.go:92-118`.
- Import accepts records in any order in `pkg/graph/internal/core/export.go:321-419`.
- `validateNodeWire` checks only token shape/range in `pkg/graph/internal/core/export.go:425-459`.
- `validateRelWire` checks only relationship type token shape/range in `pkg/graph/internal/core/export.go:462-472`.
- Registry resolution returns an empty string for unknown tokens in `pkg/graph/internal/registry/label_registry.go:96-105`.
- `NodeOps.Labels` resolves raw tokens directly in `pkg/graph/internal/core/resolution.go:31-40`.

Impact:

A malformed or custom stream can import nodes/relationships with token IDs but no registry mapping. The import succeeds, but labels/types resolve to empty strings and label/type queries cannot reliably find the imported data. Since `IO.Import` accepts arbitrary `io.Reader`, this is semantic corruption at a public boundary, not just a crash-safety issue.

Recommendation:

Track `seenHeader` and `seenRegistry` during replay. Reject tokenized entity or history records before a compatible registry has been imported, and reject tokens that exceed the loaded registry length with `ErrCorruptExport`.

### R4-F12 - Medium - IO import silently ignores duplicate current entity conflicts

Evidence:

- `pkg/graph/internal/core/export.go:372-373` ignores `ErrNodeExists` when replaying current node records.
- `pkg/graph/internal/core/export.go:399-400` ignores `ErrRelExists` when replaying current relationship records.
- History records are still written afterward at `pkg/graph/internal/core/export.go:386-387` and `413-414`.
- `pkg/graph/internal/core/export_test.go:487-496` intentionally tests idempotent re-import by relying on skipped existing nodes.
- Imperative import APIs reject duplicate IDs in `pkg/graph/internal/core/import_test.go:43-57` and `108-124`.

Impact:

Importing into a non-fresh graph with the same entity ID but different content returns success. The existing current entity is kept, while imported history can still be appended/overwritten. That can create a hybrid current/history graph that never existed in either source.

Recommendation:

On `ErrNodeExists` or `ErrRelExists`, fetch the existing current entity and compare canonical serialized content. Ignore only exact idempotent replay; otherwise return a typed conflict error.

### R4-F13 - Medium - Batch queue methods mutate persistent registries and consume IDs before `Execute`

Evidence:

- `pkg/graph/internal/core/batch.go:100-183` says `AddNode` only queues a node, but it calls `labels.GetOrCreate` at `138-149`, generates an ID at `152`, computes hash, and returns the node.
- `pkg/graph/internal/core/batch.go:186-271` does the same for relationships, calling `relTypes.GetOrCreate` at `219-222` and generating an ID at `235`.
- `pkg/graph/internal/core/batch.go:340-345` does not acquire `g.mu.Lock()` until `Execute`.
- `docs/api.md:75-77` describes queue methods as validation/queueing and says `Execute` persists the queued operations.

Impact:

Constructing and abandoning a batch can permanently add label/type tokens and consume IDs without persisting any entity. It can also mutate registries while a transaction holds the graph write lock, because queue methods do not take `g.mu`. The registry changes are then saved on `Close`, even though the corresponding batch never executed.

Recommendation:

Move token creation and ID generation into `Execute`, or explicitly document queue-time side effects and protect them through the same graph lifecycle/closed-state discipline as other mutating operations.

### R4-F14 - Medium - Failed validation/collision paths allocate registry tokens before final rejection

Evidence:

- `pkg/graph/internal/core/relationship.go:75-84` creates a relationship type token before rejecting a self-loop.
- `pkg/graph/internal/core/relationship.go:861-870` does the same in relationship import.
- `pkg/graph/internal/core/node.go:207-224` creates label tokens before checking node-ID collision in node import.
- `pkg/graph/internal/core/batch.go:219-231` creates a relationship type token before batch self-loop rejection.

Impact:

Rejected operations can permanently consume registry tokens and persist unused names. Duplicate imports and invalid self-loop attempts can therefore exhaust registry capacity over time or make persisted registry state diverge from actual graph contents.

Recommendation:

Perform all validation and collision checks that do not require token creation before `GetOrCreate`. Where token creation is unavoidable, consider rollback of newly-created tokens on failure or an explicit "registry token allocation is side-effectful" contract.

### R4-F15 - Medium - Import collision probes treat non-not-found store errors as absence

Evidence:

- `pkg/graph/internal/core/node.go:221-223` treats any `GetNode(id)` result other than `err == nil` as "no duplicate".
- `pkg/graph/internal/core/relationship.go:881-884` treats any `GetRelationship(id)` result other than `err == nil` as "no duplicate".

Impact:

Operational store errors during collision probing are ignored and the import proceeds to construct entities and attempt writes. In in-tree stores a later `Put` often catches duplicates or closed-store state, but external stores can surface different semantics, and the graph loses the original read error. This also compounds R4-F14 by allocating registry tokens before a failed collision path.

Recommendation:

Proceed only when the error is `store.ErrNodeNotFound` or `store.ErrRelNotFound`. Return all other errors immediately.

### R4-F16 - Low - Async event priority ordering is documented as strict but the idle select is unordered

Evidence:

- `pkg/graph/events/events.go:267-279` defines and documents priority drain order.
- `pkg/graph/events/events.go:291-303` does a non-blocking priority scan.
- When no event was immediately available, `pkg/graph/events/events.go:305-321` blocks on a `select` across all priority queues.
- `docs/api.md:98` says the worker drains in Critical, High, Normal, Low, Deferred order.

Impact:

When the worker is idle and multiple priority queues become ready together, Go's `select` chooses a ready case pseudo-randomly. A low-priority event can therefore dispatch before a high/critical event even though docs imply strict priority ordering.

Recommendation:

Use a wake-up channel or a single receive path that wakes the worker, then always drains queues using the explicit priority scan before dispatch. Alternatively, document the ordering as best-effort.

### R4-F17 - Low - Vector fallback documentation and tests still describe the old top-k-then-filter helper

Evidence:

- `pkg/graph/internal/core/vector_search.go:39-45` says external stores return raw top-k and callers must over-fetch manually, but the implementation now does iterative over-fetch internally at `101-148`.
- `pkg/graph/store/capabilities.go:188-202` contains both the old "top-k-then-filter" wording and the newer iterative over-fetch description.
- `pkg/graph/internal/core/vector_search.go:189-195` documents `resolveTemporalVectorMatches` as the fallback path, but production code no longer calls it.
- Tests still target the stale helper directly at `pkg/graph/internal/core/vector_correctness_test.go:537`.

Impact:

The comments send future backend implementers in two different directions and keep tests anchored to a helper that is no longer the production fallback. That increases the chance of future fixes landing in tests/helpers instead of the real path.

Recommendation:

Update comments to describe the iterative fallback, remove or unexport the stale helper if possible, and test `SearchNearest` through an external-store fake that lacks `FilteredVectorSearchCapability`.

### R4-F18 - Low - Documentation still mixes pre-v3.4 direct Graph API and obsolete product scope

Evidence:

- `docs/design.md:23` still refers to `Graph.DeleteNode`, while current public usage is through `g.Nodes.Delete`.
- `docs/SPEC.md:725-740` still lists a Cypher/API integration phase with `pkg/cypher/*` and `pkg/api/api.go`, even though this repo is now documented as a pure library.
- `docs/SPEC.md:736` still says "all 15 shadow keys", while current docs list 21 shadow properties.
- `docs/design.md:6` says `nodeID`/`relID` prevent external construction or comparison.
- `pkg/types/node.go:12-19` and `pkg/types/relationship.go:12-19` define exported `NodeID`/`RelID` aliases of `snowflake.ID`, which are comparable and constructible via conversion.

Impact:

New contributors get mixed signals about the current API, repository scope, and ID model. That is especially risky in this repo because old direct `*Graph` methods were intentionally removed, and docs are the migration guide.

Recommendation:

Run a docs-only cleanup pass against `docs/design.md`, `docs/SPEC.md`, and any stale examples. Prefer documenting current sub-API names and explicitly stating what this repo does not contain.

### R4-F19 - Low - Several production and test files are large enough to slow future changes

Evidence:

Largest production files:

- `pkg/graph/store/badger/badgerstore_history.go`: 1109 lines.
- `pkg/graph/store/badger/badgerstore_node.go`: 964 lines.
- `pkg/graph/internal/core/relationship.go`: 937 lines.
- `pkg/graph/internal/core/node.go`: 788 lines.
- `pkg/graph/internal/core/temporal.go`: 726 lines.
- `pkg/graph/internal/core/tx.go`: 701 lines.
- `pkg/graph/internal/core/batch.go`: 622 lines.

Largest test files:

- `pkg/graph/store/tiered/tieredstore_test.go`: 3307 lines.
- `pkg/graph/store/memory/store_test.go`: 2571 lines.
- `pkg/graph/store/badger/badgerstore_test.go`: 1550 lines.
- `pkg/graph/internal/core/temporal_test.go`: 1387 lines.
- `pkg/graph/store/badger/badgerstore_rel_test.go`: 1224 lines.
- `pkg/graph/internal/core/graph_rel_test.go`: 1132 lines.

Impact:

Large files make feature-level review harder and hide cross-path parity issues, especially between node/relationship, standalone/batch/tx, and memory/badger/tiered implementations. The batch temporal-constraint gap and relationship endpoint-state gap are examples of parity issues that become easier to miss when behavior is spread across very large files.

Recommendation:

Split by behavior and invariant rather than by historical file boundaries. Good candidates are relationship creation vs update, import/export replay, temporal query helpers, and backend index maintenance.

### R4-F20 - Low - Tests rely heavily on wall-clock sleeps for temporal separation and concurrency timing

Evidence:

- `rg -n "time\\.Sleep" pkg/graph | wc -l` reports 125 sleep call sites.
- Examples include temporal/version tests in `pkg/graph/internal/core/temporal_queries_rel_parity_test.go`, `txtime_test.go`, `diff_test.go`, `vector_correctness_test.go`, and many tiered-store tests.
- Some sleeps are only 1-2 ms, while the project already documents timestamp boundary precision constraints.

Impact:

Short wall-clock sleeps are a CI flake risk under load, especially when tests assert ordering across millisecond-resolution instants. They also slow the suite in aggregate and make failures difficult to reproduce.

Recommendation:

Prefer explicit temporal metadata (`tkg_valid_from`, `tkg_valid_to`, transaction-time injection where available, or deterministic ID/test-clock helpers) over sleeping. Keep sleeps only where the behavior being tested is truly asynchronous and add polling with deadlines instead of fixed waits.

## Verification

Commands run for this review:

- `make lint` - passed (`0 issues`).
- `make test` - passed.
- `make cover` - passed; `go tool cover -func=coverage.out` reported total statement coverage at 80.1%.
- `make cover-gate` - passed with `total=86.5% >= min=80%`.
- `git diff --check` - passed.
