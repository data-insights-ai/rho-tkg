# Repository Maintainability Review - Round 3

Date: 2026-05-09
Branch reviewed: `main`
Commit reviewed: `5d214f4`

This pass was run after checking out `main` and fast-forward pulling the latest
changes. It focuses on issues remaining after the round-2 fixes now present on
`main`; it does not repeat the round-2 findings that were already addressed.

## Verification

- `git checkout main` passed.
- `git pull` fast-forwarded to `5d214f4`.
- `make test` passed.
- `make cover` passed and produced total statement coverage of 80.0% via
  `go tool cover -func=coverage.out`.
- `make cover-gate` passed with `86.4% >= 80%`.

## Findings

### R3-F1 - `GraphTx` lifecycle is not atomic with in-flight transaction methods

Severity: High

`GraphTx` documents that it holds the graph write lock for the transaction
duration and that all methods check `done` after commit/rollback
(`pkg/graph/internal/core/tx.go:44`, `pkg/graph/internal/core/tx.go:53`). The
transaction mutex is also documented as protecting `done` and snapshot tracking
(`pkg/graph/internal/core/tx.go:65`). However, public transaction methods only
hold that mutex for the initial `done` check, release it, mutate the graph, and
then reacquire it to record rollback state. `AddNode` is the clearest example:
the `done` check ends before `addNodeInternal`, event buffering, and
`createdNodes` tracking (`pkg/graph/internal/core/tx.go:82`,
`pkg/graph/internal/core/tx.go:90`, `pkg/graph/internal/core/tx.go:95`,
`pkg/graph/internal/core/tx.go:97`).

`Commit` and `Rollback` use the same mutex, set `done`, clear the event buffer,
and release the graph write lock (`pkg/graph/internal/core/tx.go:493`,
`pkg/graph/internal/core/tx.go:500`, `pkg/graph/internal/core/tx.go:505`,
`pkg/graph/internal/core/tx.go:509`, `pkg/graph/internal/core/tx.go:536`,
`pkg/graph/internal/core/tx.go:543`, `pkg/graph/internal/core/tx.go:547`). That
means a concurrent `Rollback` can run after `AddNode` passed its `done` check
but before the created node is appended to `createdNodes`. The rollback then
iterates an incomplete rollback log (`pkg/graph/internal/core/tx.go:602`,
`pkg/graph/internal/core/tx.go:608`), releases `g.mu`, and the in-flight
transaction method can still write the node or publish an event. If the rollback
already cleared `txEventBuffer`, `publishEvent` dispatches immediately instead
of buffering (`pkg/graph/internal/core/events_dispatch.go:60`,
`pkg/graph/internal/core/events_dispatch.go:65`,
`pkg/graph/internal/core/events_dispatch.go:69`).

Impact: imperative transaction users can get committed data after rollback,
events for rolled-back mutations, and graph writes continuing after `g.mu` was
released. Even if `GraphTx` is intended to be single-goroutine only, that
contract is not stated; `BatchBuilder` explicitly says it is not safe for
concurrent use (`pkg/graph/internal/core/batch.go:21`), while `GraphTx` has a
mutex and concurrency-flavored comments that imply at least lifecycle safety.

Recommendation: serialize `Commit`/`Rollback` against the full body of every
public transaction method, including mutation execution and rollback-log
tracking. If full serialization would deadlock current snapshot helpers, split
the lifecycle lock from the snapshot lock or add an in-flight operation counter
that commit/rollback must wait on before setting `done` and releasing `g.mu`.
Add adversarial tests where one goroutine calls `tx.AddNode` or
`tx.AddRelationshipByID` while another calls `Rollback`, then assert no created
entity or event survives.

### R3-F2 - Public import sentinels are documented but not exported

Severity: Medium

The import/export sentinels are defined inside `pkg/graph/internal/core`:
`ErrIncompatibleExport`, `ErrIncompatibleRegistry`, and `ErrCorruptExport`
(`pkg/graph/internal/core/export.go:55`,
`pkg/graph/internal/core/export.go:59`,
`pkg/graph/internal/core/export.go:65`), plus the new `ErrImportSizeLimit`
(`pkg/graph/internal/core/export.go:228`). `ImportWithOptions` wraps
`ErrImportSizeLimit` when the stage file would exceed the configured cap
(`pkg/graph/internal/core/export.go:298`).

The public docs advertise these as user-visible sentinels
(`docs/api.md:130`), but external callers cannot import
`pkg/graph/internal/core`. `pkg/graph/errors.go` re-exports many core
sentinels and stops at `ErrSelfLoop` (`pkg/graph/errors.go:30`,
`pkg/graph/errors.go:47`); none of the import/export sentinels are present.
The public `pkg/graph/io` sub-API exposes `ImportOptions` and
`ImportWithOptions`, but also has no error exports (`pkg/graph/io/api.go:6`,
`pkg/graph/io/api.go:52`).

Impact: downstream users cannot write the documented
`errors.Is(err, ErrImportSizeLimit)` style checks for size-cap failures,
incompatible exports, registry conflicts, or corrupt streams. They are forced
to treat typed import failures as opaque errors or string-match messages.

Recommendation: re-export the import/export sentinels from the public
`pkg/graph` package, and optionally from `pkg/graph/io` if the desired API is
`tkgio.ErrImportSizeLimit`. Update `docs/api.md` to use the actual package
qualifier and add an external-package compile test that imports only
`pkg/graph`/`pkg/graph/io` and checks every documented sentinel with
`errors.Is`.

### R3-F3 - Mandatory-only label/property fallback treats all unindexable values as equal

Severity: Medium

The round-2 fallback for stores without `PropertyIndexCapability` scans
`NodesByLabel` and compares canonical property keys
(`pkg/graph/internal/core/store_capabilities.go:66`,
`pkg/graph/internal/core/store_capabilities.go:80`,
`pkg/graph/internal/core/store_capabilities.go:87`). It does not handle the
sentinel empty key returned for unindexable values. `PropertyValueKey` returns
`""` for maps, slices, structs, and other complex values
(`pkg/graph/internal/index/property_index.go:74`,
`pkg/graph/internal/index/property_index.go:110`). The property index code
treats that as "not indexed" and returns no matches
(`pkg/graph/internal/index/property_index.go:30`,
`pkg/graph/internal/index/property_index.go:64`).

The in-tree fallback paths already preserve that contract by returning no
matches when the queried value is unindexable
(`pkg/graph/store/memory/memorystore_index.go:296`,
`pkg/graph/store/memory/memorystore_index.go:297`,
`pkg/graph/store/badger/badgerstore_node.go:795`,
`pkg/graph/store/badger/badgerstore_node.go:796`). The graph-layer fallback
does not. For a mandatory-only backend, a query like
`ByLabelAndProperty("Doc", "embedding", []float32{1})` can match every labeled
node that has any unindexable `embedding` value, because both sides canonicalize
to `""`.

Impact: the public query has backend-dependent correctness exactly where the
fallback was added to remove backend-dependent behavior. It can return false
positives for vector properties, nested metadata, map properties, and any future
property type that remains valid for storage but intentionally unindexed.

Recommendation: make `nodesByLabelAndProperty` return `nil, nil` immediately
when `wantKey == ""`, matching the in-tree stores and property index lookup.
Add mandatory-only fallback tests for slice/map query values and for stored
slice/map values that must not match each other through the empty key.

### R3-F4 - Temporal vector fallback mishandles large `k` near the over-fetch ceiling

Severity: Medium

External vector backends that implement `VectorIndexCapability` but not
`FilteredVectorSearchCapability` now use an iterative over-fetch fallback
(`pkg/graph/internal/core/vector_search.go:101`). The loop starts with
`rawK := k` and only runs while `rawK <= 65536`
(`pkg/graph/internal/core/vector_search.go:116`,
`pkg/graph/internal/core/vector_search.go:119`,
`pkg/graph/internal/core/vector_search.go:120`).

For `k > 65536`, the loop never executes and the API returns an empty result
set even when the backend has matching nodes. For values near the ceiling, the
loop can also stop before the promised final over-fetch. Example: with
`k == 50000`, a first raw search of 50000 can leave fewer than 50000 eligible
nodes, then `rawK *= 2` jumps to 100000 and exits without asking for the 65536
ceiling (`pkg/graph/internal/core/vector_search.go:145`,
`pkg/graph/internal/core/vector_search.go:146`). Eligible candidates ranked
between 50001 and 65536 are never considered.

Impact: temporal vector search correctness is still backend-dependent for
external stores at high `k`, and the failure mode is silent. In-tree stores are
less exposed because they implement the pre-filtered capability, but the public
optional backend contract is where scalability pressure is most likely to land.

Recommendation: guarantee at least one raw search for positive `k`, clamp the
last iteration to the ceiling instead of skipping it, and either return a typed
error for `k` above the fallback ceiling or document and test that the fallback
returns at most the ceiling's worth of eligible results. Add a fake external
backend test for `k > 65536` and for a near-ceiling value where eligible
candidates appear only after the first raw `k`.

### R3-F5 - Vector fallback comments and tests now describe a removed behavior

Severity: Low

The implementation now over-fetches for external vector backends without
`FilteredVectorSearchCapability`, but the `SearchNearest` comment still says
those stores fall back to a single top-k search followed by post-filtering and
may return fewer than `k` unless callers over-fetch themselves
(`pkg/graph/internal/core/vector_search.go:39`,
`pkg/graph/internal/core/vector_search.go:43`). The public store capability
comment contains both the old statement and the new iterative-over-fetch
statement in the same block (`pkg/graph/store/capabilities.go:188`,
`pkg/graph/store/capabilities.go:191`,
`pkg/graph/store/capabilities.go:196`).

There is also a stale helper named `resolveTemporalVectorMatches` whose comment
still describes the old post-filter fallback
(`pkg/graph/internal/core/vector_search.go:189`). It is no longer called by
production code and is only referenced by tests
(`pkg/graph/internal/core/vector_correctness_test.go:537`).

Impact: future maintainers reading the comments will design against the wrong
contract, and tests may keep a dead helper alive instead of testing the actual
fallback loop. This is how the top-k caveat can reappear after it was already
fixed in round 2.

Recommendation: update the comments to say the graph performs bounded
iterative over-fetch, document the exact ceiling semantics after R3-F4 is
fixed, and either remove `resolveTemporalVectorMatches` or move its coverage to
the production fallback path.
