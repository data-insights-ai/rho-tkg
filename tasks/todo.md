# tkg-v3 — open work

v3.1.8 shipped 2026-05-05: typed entity IDs across the public Graph API,
Store interface, all three Store implementations, internal helpers, and
storage maps; plus MR !4's TieredStore cross-shard hardening. See
`CHANGELOG.md` `[3.1.8]` for the full log.

## Tier D — final documentation

These surfaces deliberately stay raw `snowflake.ID`. Action items below are
documentation, not code changes.

- [ ] **`entityLockManager`** (`pkg/graph/entity_locks.go`): add a doc
      comment stating "type-agnostic by design — same 256-shard pool serves
      both node and rel IDs by hashing snowflake bits."
- [ ] **`Graph.DecomposeID`** (`pkg/graph/graph.go`,
      `pkg/graph/id_decompose.go`): keep raw single function (recommended)
      — it's a kind-agnostic decomposition utility.
- [ ] **`keys.go`** package doc: add a one-line statement of the
      chokepoint invariant.
- [ ] **`wire.go`**: audit each `int64` field is a wire-format requirement
      and document at the type definition.

## Optional follow-ups (post-v3.1.8)

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
  engram updates.

## MR queue (review pending)

- **MR !1** "feat(graph): IndexProvider interface for out-of-tree indexes"
  (Markus Nissl) — analysed. Issues: stale base (will conflict heavily on
  rebase), TOCTOU race in `RegisterIndexProvider`, mixes two unrelated
  concerns (IndexProvider + HashableValue/property registry), one wrong
  doc comment (`g.Node(id)` doesn't exist). Send back asking for rebase +
  TOCTOU fix + split.
- **MR !5** "Draft: fix(graph): close history-aware regressions and
  cross-shard rel rollback" — not yet analysed.

## Out of scope

- engram update (pinned at `v3.1.1`).
- Badger on-disk format changes (`wire.go` `int64` IDs unchanged).
- Snowflake bit layout (unchanged).
- `tkgd-v3` (consumes via the same module path; will pick up changes at
  its own cadence).

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
```

## Lessons (validated 2026-05-05 across 7 migration rounds + MR !4 merge)

Five rounds of parallel-agent work + two single-agent rounds + one
parallel merge resolution validated this prompt structure for
cross-cutting type migrations and substantive merges.

1. **Natural unit of change**, not "one file per agent". Round-1 Agent A
   tried to migrate `sameIDSet` while pinned to `context.go` only and
   produced a type-lie (`toRelIDs` adapter casting heterogeneous IDs).
   We reverted and moved `sameIDSet` to Tier D.
2. **Pre-list files in scope** with a separate off-limits list. Don't
   say "touch only X.go" if the natural unit spans X+Y; widen scope or
   split differently.
3. **Scope-exceeded protocol**: tell the agent to STOP and report when
   the natural unit exceeds the listed scope. Better a clean stop than
   a build-clean type-lie. Round-4 Agent A correctly stopped at two
   off-limits test files and reported.
4. **Race condition with parallel agents**: the faster agent will see
   the slower agent's mid-edit state. Always do an external joint-verify
   after both finish; don't trust either solo verify.
5. **Pre-break-then-fix pattern** (round 6): for type-ripple migrations
   like `QueryOpts.After`, have the main agent apply the kernel change
   (struct field + a few function signatures), then spawn parallel
   agents on file-disjoint compile-error fixes.
6. **Helper placement**: round-5 Agent B introduced 4 pagination helpers
   in `badgerstore.go` that genuinely belonged in `pagination.go`. Main
   agent moved them post-merge. Lesson: when helpers a parallel agent
   adds belong elsewhere, accept the duplication and relocate
   post-merge rather than re-spawning with a wider scope.
7. **Merge boundary mismatches** (MR !4): when 4 agents independently
   resolve conflicts in 4 files of the same package, signature
   assumptions can drift between agents — Agent A retypes
   `shardForRelIDChecked` to `types.RelID` while Agent C calls it with
   `.SnowflakeID()`. The main agent had to fix ~10 boundary mismatches
   inline after joint verification. Lesson: post-merge boundary audit
   is non-optional.
