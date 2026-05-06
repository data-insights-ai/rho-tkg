# tkg-v3 — open work

Latest release: **v3.1.11** — refArchive parity in indexed/bulk reads +
Close-race protection (MR !6 + audit fixes). Built on v3.1.10
(history-aware indexed candidate planning + batch hardening), v3.1.9
(IndexProvider + HashableValue), v3.1.8 (typed entity IDs + TieredStore
cross-shard hardening). See `CHANGELOG.md` for details.

## Optional follow-ups

These would further reduce the residual `.SnowflakeID()` count but are
not blocking. All are Tier D infrastructure refactors.

- **`entityLRU[K, V]`** (`pkg/graph/lru.go`): make the LRU generic over
  its key type. Would drop ~14 `.SnowflakeID()` handoffs in
  `badgerstore.go`.
- **`keys.go` typed signatures**: `nodeKey(id snowflake.ID)` →
  `nodeKey(id types.NodeID)` and unwrap inside. Would drop ~38 handoffs
  in `badgerstore.go`. Boundary decision: where exactly does
  "binary encoding" begin?
- **`relDeleteInfo` typed fields** (`pkg/graph/badgerstore_partial.go`):
  retype struct fields; update `tieredstore_*.go` builders. Would drop
  4 handoffs.
- **Property/Temporal/Vector index helpers**
  (`pkg/graph/property_index.go`, `pkg/graph/temporal_index.go`,
  `pkg/graph/vector_index.go`, `pkg/graph/temporal_filter.go`):
  migrate signatures to typed. Would drop the 17 calls in
  `memorystore.go`.
- **Remove `InternalID()`** from `pkg/types/node.go` and
  `pkg/types/relationship.go`. Currently kept as a deprecated alias for
  downstream source-compat. Schedule for the next minor bump or once
  engram updates off `v3.1.1`.
- **`findRelInAnyShardStore` API improvement** (`pkg/graph/tieredstore_admin.go`):
  current contract is "returned pointer is for identity comparison only"
  — caller MUST NOT dereference. A safer signature would return
  `(*BadgerStore, func(), error)` with the pin held for the caller's use.
  Defer until a real second caller exists; today only `tieredstore_repair.go`
  uses it and only does a nil check.

## MR queue

All open MRs from the original review backlog have been merged:

- **MR !1** ✅ merged in v3.1.9 (IndexProvider + HashableValue).
- **MR !2** ✅ closed earlier (regression coverage rolled into v3.1.7).
- **MR !3** ✅ merged earlier (graph perf baseline benchmarks in v3.1.7).
- **MR !4** ✅ merged in v3.1.8 (TieredStore cross-shard hardening).
- **MR !5** ✅ merged in v3.1.10 (history-aware regressions + batch hardening).
- **MR !6** ✅ merged in v3.1.11 (refArchive parity + Close-race protection).

## Out of scope

- engram update (pinned at `v3.1.1`).
- Badger on-disk format changes (`wire.go` `int64` IDs unchanged).
- Snowflake bit layout (unchanged).
- `tkgd-v3` (consumes via the same module path; will pick up changes at
  its own cadence).
- govulncheck `GO-2026-4865` (`html/template` XSS in Go 1.26.1 stdlib):
  fix is in 1.26.2; not yet available. Trace goes through transitive
  Badger error formatting; not exploitable for our pure-data graph
  engine.

## Audit commands

```sh
# raw snowflake.ID in production code (target: only at Tier D
# boundaries — keys.go, wire.go, lru.go, snowflake.Node lib calls):
grep -rn 'snowflake\.ID' pkg/graph/*.go pkg/types/*.go | \
  grep -v '_test\.go' | wc -l

# distribution of remaining .SnowflakeID() calls per file:
grep -rc '\.SnowflakeID()' pkg/graph/*.go pkg/types/*.go | \
  grep -v ':0$' | sort -t: -k2 -nr

# chokepoint check — only keys.go + lru.go + entity_locks.go are high:
grep -c 'snowflake\.ID' pkg/graph/keys.go pkg/graph/wire.go \
  pkg/graph/lru.go pkg/graph/entity_locks.go

# refArchive callsite audit — every refArchive.Load() outside
# checkoutArchive itself should be either a pointer-comparison-only
# pattern (shard != ts.refArchive.Load()) or be inside a function that
# already pinned the archive:
grep -n 'refArchive\.Load()' pkg/graph/tieredstore*.go | grep -v '_test\.go'
```

## Lessons (validated 2026-05-05 / 2026-05-06 across 8 MR integrations)

Five rounds of parallel-agent work + three single-agent rounds + four
parallel merge resolutions (MR !1, !4, !5, !6) validated this prompt
structure for cross-cutting type migrations and substantive merges.

1. **Natural unit of change**, not "one file per agent". Round-1 Agent A
   tried to migrate `sameIDSet` while pinned to `context.go` only and
   produced a type-lie. Reverted; moved `sameIDSet` to Tier D.
2. **Pre-list files in scope** with a separate off-limits list. Don't
   say "touch only X.go" if the natural unit spans X+Y; widen scope or
   split differently.
3. **Scope-exceeded protocol**: tell the agent to STOP and report when
   the natural unit exceeds the listed scope. Better a clean stop than
   a build-clean type-lie.
4. **Race condition with parallel agents**: the faster agent will see
   the slower agent's mid-edit state. Always do an external joint-verify
   after both finish; don't trust either solo verify.
5. **Pre-break-then-fix pattern**: for type-ripple migrations, have the
   main agent apply the kernel change (struct field + a few function
   signatures), then spawn parallel agents on file-disjoint compile-error
   fixes.
6. **Helper placement**: when helpers a parallel agent adds belong
   elsewhere, accept the duplication and relocate post-merge rather than
   re-spawning with a wider scope.
7. **Merge boundary mismatches**: when N agents independently resolve
   conflicts in different files of the same package, signature
   assumptions can drift between agents. Post-merge boundary audit is
   non-optional.
8. **Concrete regression tests for race-class bugs**: MR !1's TOCTOU
   fix in `RegisterIndexProvider` pairs with
   `TestIndexProvider_ConcurrentRegisterRaceSafe` — 50 goroutines
   register the same name, assert exactly 1 success and exactly 1
   receives events. Run with `-race -count=10`.
9. **Post-MR audit for the same bug class**: when an MR fixes a
   pattern (e.g. "pin refArchive via checkoutArchive"), grep for the
   same pattern across the codebase before tagging. MR !6 fixed
   indexed/bulk reads + point-lookup pinning + history fan-out, but
   missed `findRelInAnyShardStore` and `ArchiveNode`/`RestoreNode`. The
   post-merge audit caught both before tagging v3.1.11. Pattern for
   any "consistency MR": run `grep -n 'refArchive\.Load()'` (or the
   equivalent) and verify each remaining site is either a pointer-
   comparison-only optimization or already pinned.
