# tkg-v3 — open work

v3.2.0 typed-entity-ID migration: public API surface, Store interface, all
three Store implementations, all internal helpers, all storage maps, and
all Tier A public exposures (`Event.EntityID`, `BatchError.ID`,
`QueryOpts.After`, `SetBaseEntityID`) migrated. Build/vet/race+short tests
green. See `CHANGELOG.md` `[Unreleased]` for the full log.

## Phase 8 — ship v3.2.0

- [ ] Remove `InternalID()` from `pkg/types/node.go` and
      `pkg/types/relationship.go`. Original deal: keep until everything
      works. Everything works. Zero in-repo callsites; engram pinned at
      v3.1.1 unaffected until it bumps.
- [ ] CHANGELOG.md: cut `[Unreleased]` → `[3.2.0] - 2026-05-XX`.
- [ ] CLAUDE.md / AGENTS.md / docs/architecture.md: status string
      `v3.1.7` → `v3.2.0`.
- [ ] README.md: migration callout for downstream callers.
- [ ] Tag `v3.2.0` and push.

## Tier D — final decisions

These surfaces deliberately stay raw `snowflake.ID`. Action items are
documentation, not code changes.

- [ ] **`entityLockManager`** (`pkg/graph/entity_locks.go`): keep raw +
      add a doc comment stating "type-agnostic by design — same 256-shard
      pool serves both node and rel IDs by hashing snowflake bits."
      (Recommended over the interface-typed alternative.)
- [ ] **`Graph.DecomposeID`** (`pkg/graph/graph.go`,
      `pkg/graph/id_decompose.go`): keep raw single function (recommended)
      — it's a kind-agnostic decomposition utility.
- [ ] **`keys.go`** package doc: add a one-line statement of the
      chokepoint invariant.
- [ ] **`wire.go`**: audit each `int64` field is a wire-format requirement
      and document at the type definition.

## Optional follow-ups (post-v3.2.0)

These would further reduce the residual `.SnowflakeID()` count but are
not blocking ship. All are Tier D infrastructure refactors.

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

## Out of scope

- engram update (pinned at `v3.1.1`).
- Badger on-disk format changes (`wire.go` `int64` IDs unchanged).
- Snowflake bit layout (unchanged).
- `tkgd-v3` (consumes via the same module path; will pick up changes
  at its own cadence).

## Audit commands

```sh
# raw snowflake.ID in production code (target: only at Tier D
# boundaries — keys.go, wire.go, lru.go, snowflake.Node lib calls):
grep -rn 'snowflake\.ID' pkg/graph/*.go pkg/types/*.go | \
  grep -v '_test\.go' | wc -l

# distribution of remaining .SnowflakeID() calls per file:
grep -rc '\.SnowflakeID()' pkg/graph/*.go pkg/types/*.go | \
  grep -v ':0$' | sort -t: -k2 -nr

# chokepoint check — only keys.go + wire.go should be high:
grep -c 'snowflake\.ID' pkg/graph/keys.go pkg/graph/wire.go
```

## Lessons (validated 2026-05-05 across 7 migration rounds)

Five rounds of parallel-agent work + two single-agent rounds validated
this prompt structure for cross-cutting type migrations.

1. **Natural unit of change**, not "one file per agent". Round-1
   Agent A tried to migrate `sameIDSet` while pinned to `context.go`
   only and produced a type-lie (`toRelIDs` adapter casting
   heterogeneous IDs). We reverted and moved `sameIDSet` to Tier D.
2. **Pre-list files in scope** with a separate off-limits list. Don't
   say "touch only X.go" if the natural unit spans X+Y; widen scope or
   split differently.
3. **Scope-exceeded protocol**: tell the agent to STOP and report when
   the natural unit exceeds the listed scope. Better a clean stop than
   a build-clean type-lie. Round-4 Agent A correctly stopped at two
   off-limits test files and reported.
4. **Race condition with parallel agents**: the faster agent will see
   the slower agent's mid-edit state. Always do an external
   joint-verify after both finish; don't trust either solo verify.
5. **Pre-break-then-fix pattern** (round 6): for type-ripple migrations
   like `QueryOpts.After`, have the main agent apply the kernel change
   (struct field + a few function signatures), then spawn parallel
   agents on file-disjoint compile-error fixes. Each agent's territory
   becomes naturally isolated by the file boundaries of where the
   compile errors land.
6. **Helper placement**: round-5 Agent B introduced 4 pagination
   helpers in `badgerstore.go` to bridge typed callers and a still-raw
   `paginateIDs` (because `pagination.go` was off-limits). Helpers
   were correct but in the wrong file; main agent moved them to
   `pagination.go` post-merge. Lesson: when helpers a parallel agent
   adds genuinely belong elsewhere, accept the duplication and
   relocate post-merge rather than re-spawning with a wider scope.
