# Contributing to tkg/v4

Rulebook for the code: code and test rules, release, review, fuzz/bench, lessons. Procedure, commands
and git rules: [AGENTS.md](../AGENTS.md). Product: [README.md](../README.md). Layers:
[architecture.md](architecture.md). API: [api.md](api.md). Current work: [tasks/todo.md](../tasks/todo.md).

Read the Rules digest (code and tests) every session and § Release checklist before a tag. How to work
(procedure, standing orders, git) is [AGENTS.md](../AGENTS.md) alone. One fact, one place, and never a new `.md` file: fold facts into the owning file
([README.md § Documentation](../README.md#documentation)).

## Rules digest

**Code**

- Red test first: one compact break-the-code test, red output under `tasks/evidence/<item>/` (local, gitignored), then the fix
  at the shared seam. No "known limitation" unless the sound fix was shown impossible. A crash-safety change
  also gets a mutation sweep: each defended property broken once must fail a test. The sweep runs every test
  that guards the code (long tier too, not only the new tests); a timeout counts as caught; a fault hook
  fires once (a requeue re-runs it).
- A cost test gates on a count no load moves: rows read, store calls, keys visited, versions copied.
  `runtime.MemStats` and wall time are evidence, never a suite pass/fail. Allocation and time claims live in
  benchmarks or `//go:build measure` (`go test -tags measure`, by hand).
- Validate before any side effect: IDs, decoded bytes and property values at every entry, before an ID is
  minted, a token interned, a snapshot taken or an op queued. Decode untrusted bytes only through
  `storeutil.SafeUnmarshal`; allocate by bytes delivered, never by a declared count; verify imported hashes.
- A contract over a family of doors lands in every door (named, generic, batch, tx, ingest; every backend),
  and a test asserts the doors agree.
- Transaction time from the commit clock (`c.now()` / `readNow()`), never the wall; corrections append rows;
  valid times compare only through `types.ValidAxis`.
- Locks: architecture.md § 4; entity locks before store writes; `sync.RWMutex` is not reentrant; bound every
  retry; swap a shared pointer only while holding every lock class its readers use.
- Persistence: every in-memory structure has a rebuild path at open and fails closed on bad data; flush data
  before advancing a watermark.
- Sentinel errors are precise and tested with `errors.Is`; a caller skips only the sentinel the operation
  tolerates.
- Reproduce before relying: profile before optimizing; reproduce an external performance diagnosis before
  touching the component it names; check a dependency or review claim against the source and give it a test
  that tries to break it before code, docs or a plan builds on it.
- Cost is part of done: a door, format or stored structure states its cost budget in the todo
  "Done when" (bytes allocated per fact, RAM per entity, open time per million rows, durable writes per
  second, whichever it touches), measured by a rho test on the consumer's shape (row size, versions, warm and
  cold) before first release. Design from `docs/badger.md` measured costs; profile before review, not after a
  consumer's complaint.
- Hash the canonical state: registry-resolved labels and types, never raw ones; a relationship change that
  recomputes integrity refreshes its endpoint hashes first.
- A public query built from a map or dedup set sorts by entity ID before returning; never expose map order.
- A new field on a struct with a hand-written `EncodeMsgpack` goes into that encoder too (omitempty and the
  map length by hand); a plain round trip does not catch a miss.
- No `reflect`-based sort or comparison (`sort.Slice`, `reflect.DeepEqual`) on a per-write or per-query path;
  use `slices`, `cmp`, `maps` or typed code. `msgpack.Marshal` reflects unless the type has a custom encoder.
- Consolidate, do not layer: when two code paths or helpers do the same job, merge them and delete the rest.
- A comment says what and why in words. It never cites a number from a document (lesson, rule, backlog item,
  todo row, handover section): documents point at code, never the other way round.
- Behaviour changes go through the deprecation ritual (`docs/api.md` § Stability and deprecation).
- Trim comments by hand, never by script; never drop lock-order, crash-ordering or ownership contracts.
- `#nosec` needs a reason at the call site.

**Tests**

- New tests run under `-short` in seconds (no test of 1 s or more in the fast run, a package ≲5 s). A test of
  1 s or more calls `testtier.Slow(t)` first: it skips in `make dev` and runs in `make check` and `make race`
  (`RHO_GATE=1`). A new test uses no bare `testing.Short()` skip (todo SHORTSCAN). A cap or growth test lowers
  the cap through a test switch.
- History-aware code: create state X at t0, mutate after t0, query at t0 and assert X. Assert exact sets
  and negatives. Would the test fail if the code returned the current instead of the historical version?
- An accelerator is tested against the slow path it replaces.
- A contract change greps every test pinning the old outcome and runs those by name with `-race` before
  the branch is done.

## Gates and release

Commands, procedure and git rules: [AGENTS.md](../AGENTS.md) and `make`.

### Release checklist

One gate per release, chosen by kind, run once to the end, listing every red.

1. Clean up: finished scratch on the rho section of `~/cleanup.sh` (RHO_DONE and named lines; never WIP,
   never by age), then `bash ~/cleanup.sh`; remove merged worktrees/branches; `git status` clean.
2. Every branch of the batch is merged (each reviewed, § Review).
3. CHANGELOG and README committed with the batch (the agent writes the CHANGELOG from the finished items of
   `tasks/todo.md`): `## [X.Y.Z] - date` above the entries, `[Unreleased]` left empty; shipped rows leave
   `tasks/todo.md`. Not pushed alone: `make release` pushes it with the tag. The tag is the version; no other
   doc names it (`git tag --contains <commit>`).
4. `make release*`: `scripts/release` checks, gates, tags and pushes; its header comment
   (`scripts/release/main.go`) lists the sequence.
   - **Major** = a removed or changed public name (a signature's types, type parameters, an embedded field,
     alias or defined type, a const's type; parameter names aside) or the module path's `/vN`. It needs
     go.mod and every import on `/v(N+1)` first.
   - **On red** the release stops; the fix goes on `dev` and the release runs again. Never move a bad tag:
     the fix is the next tag, and `retract` in go.mod if needed.
5. One line to sigma-tkgd and ai-soc: tag, gate, seconds.
6. Clean up after (release scratch), then `bash ~/cleanup.sh` again.

Not part of a release: consumers' pin build/vet, scale probe (unless it is the item), a freeze message,
data runs (the maintainer's word only).

## Review

The main session reads the full diff before the gate: does each new method do what its name promises; is
the same defect in sibling doors, other backends, batch/tx/ingest; would each new test fail on a plausible
bug (current instead of historical, skipped rollback, reordered write), shown by breaking the code once per
defended property. Every badger claim cites source or a measurement.

## Documentation

CHANGELOG under `[Unreleased]` in the same change as the code. A new public error sentinel needs a row in
[errors.md](errors.md). The docs tests (`internal/docscheck`) run in every gate.

## Fuzzing

Trust-boundary harnesses: [SECURITY.md](SECURITY.md) ("Fuzz harnesses"). After a decode or import change,
run a deep session and commit any new crasher (under the package's `testdata/fuzz/`) with the fix:

```bash
go test -fuzz=FuzzImport -fuzztime=30m -run='^$' ./pkg/graph/internal/core
```

## Benchmarks

```bash
make bench                      # informal (-benchtime=0.3s)
make bench BENCH=smoke          # every scenario once
make bench BENCH=baseline       # -> bench/local-baseline.txt (gitignored)
make bench BENCH=check          # fail on >15 % time or >10 % allocs vs local baseline
bench/bench-compare-revisions.sh  # two tags or commits on a quiet machine
```

Baselines are per machine and gitignored; never compare across machines. Measurements belong in CHANGELOG
entries, not here.

## Lessons

A lesson is a rule learned from a real defect, kept as the rule and the test or file that enforces it. The
list points at the code; code never cites a lesson. Rules that no file enforces yet are bullets in the digest
(Code). **(gap)** marks an entry whose named test would not fail on the defect. Add a lesson only for a defect
class that recurred or reached a release; write the test first and name it here.

**Validation and wire**

- Zero and negative IDs are rejected at every entry → `TestGraphReadAPIsRejectInvalidIDs`,
  `core/delete_validation_test.go`
- Untrusted bytes are decoded only through `SafeUnmarshal` (depth guard, then recover) →
  `TestSafeUnmarshalRejectsDeepNesting`, `storeutil/safe_decode.go`
- Allocation follows bytes delivered, never a declared size → `TestImportOversizedRecordWithLyingBody`
- An import verifies every row hash and every imported chain; a mismatch is `ErrCorruptExport` with no partial
  state → `TestImportHostileStreamsFailClosedWithoutPartialState`, `core/import_hash_verification_test.go`
- A decoded row is checked for semantic corruption before it becomes a live entity →
  `TestWireToNodeCheckedRejectsSemanticCorruption`
- A nested wire value keeps its kind at every depth → `TestWire_NestedKindsRoundTripExact`
- Rebuilt wire properties are sorted by key string, not by token → `TestHistoryDeltaCoreDifferentialAndIntegrity`
- A full property-key registry returns token 0 and the raw key is written; the write never fails →
  `TestPropertyKeyRegistryCapacityIsSoft`

**Time**

- Transaction time comes from the commit clock, never the wall → `TestTxTimeMonotonicWhenClockRepeats`
- The clock floor is persisted, reseeded at open and advanced by every foreign stamp →
  `TestNowTx_ReopenSafe/burst`, `TestCrashReopenNowTxAboveStoredStamps`,
  `TestNowTx_ReplicaCoversAppliedFutureTxFrom`, `TestInstantFloor_PreservedAcrossReset`
- Superseded is not retracted, and a version ends where the next one starts →
  `TestBitemporalPointQueryAfterSupersession`
- Every door that copies the current version clears inherited valid time →
  `core/inherited_validtime_regression_test.go`
- A correction appends rows stamped at now; stored rows are never rewritten →
  `pkg/graph/ingest_interval_doors_test.go`, `core/cascade_patch_base_test.go`
- A delete is a transaction-time tombstone: its stamps are disregarded at pins before the delete →
  `TestResolveNodeChain_TombstoneNormalizedAtPreDeletePin`, `core/bitemporal_tombstone_test.go`
- An as-of read picks the newest version recorded by the pin, then judges its retraction →
  `TestAsOfBoundedCascadeThenDelete`, `storeutil/asof_select_test.go`
- Temporal, snapshot and integrity reads see deleted and history-only entities →
  `TestSnapshot_IncludesDeletedNodes`, `TestNodesAsOfDeletedEntityBeforeDelete`,
  `TestGraphVerifyNodeChain_DeletedEntity`
- The chain resolver's input order is semantic; each call gets its own copy →
  `TestSkeletonResolve_RandomizedEquivalence`, `TestSkeletonResolve_CascadeReopenDelete`
- A transaction-time backfill lands at the one shared seam, gated and bounded against the clock →
  `TestTxBackfill*`, `TestAsOfTags_ResetReapsTags`
- The pre-migration resolver shim stays behind its runtime flag → `core/migration_bitemporal_test.go`

**Transactions and locks**

- Rollback restores the exact pre-transaction state; its log tracks pre-transaction existence →
  `TestTxRollbackRestoresExactPreTxState`
- After an unlocked peek, lock, then revalidate identity and retry →
  `TestUpdateRelationship_RetriesWhenReusedIDHasDifferentEndpoints`
- A transaction does not hold the core lock for its lifetime → `TestGraphTx_PublicResolverReadsDoNotBlock`
- Preflight without writes, then apply; a fallible step withdraws what it wrote →
  `TestCascadeDeleteFatalPreflightAppliesNothing`
- A native fast path never bypasses a store wrapper → `core/native_capability_wrapper_test.go`
- An unlocked collect window before a rebuild-commit needs a write-generation guard →
  `TestBadgerStoreNodePropertyStatsStaleRescanOverwrite`
- **(gap)** A multi-shard move rolls back destination writes when a later step fails →
  `tiered/rollback_helpers_test.go` (the helper alone; no test injects the failing step)

**Open, close and reset**

- Every in-memory structure is rebuilt identically at reopen → `TestCleanReopenMatchesFullRebuild` (long tier)
- A closing store fails public operations → `TestBadgerStoreClosingStateFailsPublicOperations`
- Persisted counters match the rows: an undercount heals, anything else fails closed →
  `TestBadgerStoreLoadRejectsMismatchedPersistedCounters`, `TestBadgerStoreLoadHealsUndercountedPersistedCounters`
- State written under a larger bound still opens after the bound shrinks →
  `TestBadgerOversizedWAL/read-only open fails closed`, `TestBadgerOversizedWAL/reopen across memtable shrink`
- A reset keeps the LSN watermark present, and deletes registries only after the data →
  `TestChangeLog_LogOffClearPreservesWatermark`, `TestClearCrashAtEveryDurableStep`
- **(gap)** The property-key registry loads before the index rebuild → `badger/badgerstore.go` (open path)

**Badger write buffers**

- The in-flight buffer is cleared on success and read by every reader → `badger/badgerstore_flushing_test.go`,
  `badger/badgerstore_flushing_break_test.go`
- Set against delete resolves per key at every reader →
  `TestFlushingOverlay_PendingDeleteMasksMax_NodeDoorsAgree`
- The mutable overlay is captured before the badger view opens →
  `badger/badgerstore_flushing_commit_window_test.go`

**Change-log and replication**

- One record per public mutation door, emitted in the backend and co-committed →
  `TestMemoryChangeLog_AllDoorsEmit`, `TestChangeFeedParity_BadgerVsMemory`
- A replica reproduces the primary's rows and flushes them before advancing its watermark →
  `TestReplicaConvergence_IdempotentReapply`, `pkg/graph/replica_flush_ordering_test.go`
- Registry sync appends behind a prefix guard and persists before use → `registry/append_names_test.go`,
  `TestRefetch_PersistFailureRollsBackThenRetryConverges`
- A delta export reuses the change-log, never a temporal diff → `core/export_delta_test.go`
- With the change-log on, registries are append-only across rollback →
  `core/registry_rollback_changelog_test.go`, `pkg/graph/replica_rollback_token_test.go`

**Indexes and queries**

- An index build is phased and tracks rows mutated during the build → `TestPropRewriteDuringIndexBuildKeepsEntry`
- Property index keys follow property equality → `TestPropertyEqualityDoorsAgree`,
  `TestPropertyIndexAgreesWithBruteForceTwin`
- The integrity hash keeps float bit patterns that equality collapses → `pkg/types/property_hash.go`
- Adjacency folds need only the seed's ex-adjacency; label folds stay all-history →
  `TestOutgoingIncomingForNodesAtTx_RandomizedDivergenceProbe`, `core/pinned_adjacency_reimport_test.go`
- Sibling doors agree (strict create and change, lenient load) → `pkg/graph/numeric_equality_doors_test.go`,
  `core/batch_door_equivalence_test.go`, `core/constraint_door_equivalence_test.go`
- Shard fan-out is bounded → `TestRunShardPoolBoundsConcurrency`
- A batch publish keeps its priority ceiling under saturation → `TestAsyncEventBusPublishBatch_PriorityCeiling`
- A door states and measures its cost budget → `TestR37ListingBytesPerFact` (measure tag)
- **(gap)** An overlap-based prune is not used where the predicate includes non-overlapping Allen relations →
  `TestNodesRelating_AllThirteenRelations` (runs without a temporal index, so the prune is never active)
