# Typed Entity IDs — `NodeID` / `RelID` (v3.2.0)

## Status

In progress, on the migration branch. Public Graph API surface migrated. `Store`
interface and all three implementations (Memory / Badger / Tiered) migrated.
~72 files touched in the callsite sweep, ~25 test files updated. Build green;
full + race suites pass with `go clean -testcache && go test -count=1`.

Tier A (3 public exposures still leaking raw `snowflake.ID`), Tier C (internal
chokepoint consolidation), Tier D (reanalysis of justified raw uses), and final
cleanup remain.

Smoke tests 2026-05-05:
- Round 1 (parallel, file-scoped prompt): 1 clean win (`highFrequencyIndex` →
  typed NodeID), 1 reverted (`sameIDSet` — natural-unit mismatch surfaced as
  a type-lie; moved to Tier D as justified raw).
- Round 2 (single-agent, natural-unit prompt with scope-exceeded protocol):
  clean — `forEachKnownNodeID/RelID` callback migration + 11 caller closures.
- Round 3 (parallel, natural-unit prompt): both agents clean — TieredStore
  `shardFor*` family + BadgerStore `prefetchNode`/`getNodeLocked`/`getRelLocked`
  helpers. Surfaced a race condition where the faster agent saw transient
  mid-edit state from the slower; documented in the prompt template section.
- Round 4 (parallel, Tier A + paired Tier C): Agent A migrated `Event.EntityID`
  + `Graph.publishEvent` + 35 callsites; correctly hit scope-exceeded on 2
  test files and stopped per protocol (vs round-1 type-lie). Agent B migrated
  BadgerStore `cascadeDelete*` helpers cleanly. Main agent applied the 3
  flagged trivial test-file fixes. Joint-verify clean.

See "Parallel-agent prompt template" below for the validated structure.

## Decisions taken (2026-05-05)

- Hard-cut, single v3.2.0 release.
- Export `EntityID` alongside `NodeID` / `RelID`.
- `n.ID()` / `r.ID()` added; `InternalID()` kept as legacy alias **for now**.
- engram update is out of scope: its `tkg/v3` dep is pinned at `v3.1.1`, and
  `InternalID()` preserves source compat anyway when it bumps.
- **Architecture invariant**: only `keys.go` (binary key encoding) and `wire.go`
  (msgpack serialization) plus the `snowflake.Node` library boundary should see
  raw `snowflake.ID`. Everything else flows typed.

## Done

### Phase 1 — `pkg/types` exported wrappers
- [x] `nodeID` → `NodeID`, `relID` → `RelID`, `entityID` → `EntityID` (gopls rename).
- [x] `(NodeID).SnowflakeID() snowflake.ID` boundary accessor (already existed).

### Phase 2 — `ID()` accessors + bulk callsite rename
- [x] `func (n *Node) ID() NodeID`, `func (r *Relationship) ID() RelID`.
- [x] 950 `.InternalID()` → `.ID()` across 72 files.

### Phase 3 — Graph public API + Store interface
- [x] `graph.go`: 35+ `Graph` methods take `types.NodeID` / `types.RelID`.
- [x] `context.go`: `*WithContext` + `*Internal` variants.
- [x] `temporal.go`, `txtime.go`, `integrity.go`, `tx.go`, `batch.go`: signature migrations.
- [x] `Store` interface: 35+ methods (`GetNode`, `PutNodeVersion`, `ForEachNodeID`,
      `OutgoingRelationshipsForNodes`, etc.) migrated to typed.
- [x] MemoryStore: signatures migrated; bodies use `id := nid.SnowflakeID()` shim.
- [x] BadgerStore: same pattern.
- [x] TieredStore (`tieredstore_read/write/repair/migrate.go`): signatures + cross-shard
      `nodeIDsToRaw` / `rawToNodeIDs` helpers for merge plumbing.
- [x] `wire.go`: deserialization wraps int64 IDs as typed at the boundary.
- [x] `RelTombstone.ID` → `types.RelID`.
- [x] `pendingRel.startID/endID` → `types.NodeID`; split `pendingUpdate` into
      `pendingNodeUpdate` / `pendingRelUpdate`.

### Phase 4 — Test sweep
- [x] ~25 test files updated.
- [x] Custom `Store` mock (`deleteRelPanicStore.DeleteRelationship`) updated.
- [x] Helper signatures migrated: `setNodeTemporal` / `setRelTemporal`,
      `containsNodeID`, `assertNodeSet` / `assertRelSet`, `indexOfID`.

## Open

### Phase 5 — Tier A: public exposures (must close before tagging)

These three public types still surface raw `snowflake.ID` to external callers.
Each defeats type-safety at the API boundary the migration was meant to give us;
each currently forces production code to do `.ID().SnowflakeID()` at the
callsite (~30 of those are still in `pkg/graph` solely because of this).

- [x] `events.go:43` `Event.EntityID` → `types.EntityID` and
      `(g *Graph).publishEvent` → typed. Parallel-agent run 2026-05-05.
      35 callsites migrated across `events.go`, `graph.go`, `context.go`,
      `tx.go`, `batch.go`, plus 3 test-file fixes. Agent stopped at
      scope-exceeded for two test files; main agent fixed those manually.
- [ ] `batch.go:67` `BatchError.ID` → `types.EntityID`, OR split into
      `BatchNodeError` / `BatchRelError`. Today's `BatchError` is one struct
      shared by both kinds.
- [ ] `store.go:25` `QueryOpts.After` → polymorphic via an interface
      (`{ SnowflakeID() snowflake.ID }`), OR document as deliberately raw and
      keep the friction. Pagination cursor maps to either entity kind, so
      typed-via-interface is the cleanest fit.

### Phase 6 — Tier C: internal chokepoint consolidation

The architectural invariant says `snowflake.ID` should only appear in
`keys.go` + `wire.go` + the `snowflake.Node` library boundary + a small set of
deliberately type-agnostic surfaces (Tier D). Everything else should be typed;
`.SnowflakeID()` is called once at the encoding edge, not hundreds of times in
random helpers.

Currently raw inside `pkg/graph`:
- [x] `highFrequencyIndex.add` / `remove` / `pointQuery` / `rangeQuery` and
      `buckets` field → `types.NodeID`. Smoke-test agent run 2026-05-05.
- [x] `Graph.forEachKnownNodeID` / `forEachKnownRelID` callbacks → typed.
      Single-agent prompt-validation run 2026-05-05. Migrated 2 defs + 11
      caller closures + 6 in-scope helpers in `temporal.go`.
- [x] `TieredStore.shardForNodeID` / `shardForRelID` /
      `shardForNodeIDChecked` → typed. Parallel-agent run 2026-05-05.
      ~43 callers migrated across 5 tieredstore files.
- [x] BadgerStore unexported helpers `prefetchNode` / `getNodeLocked` /
      `getRelLocked` → typed. Parallel-agent run 2026-05-05. 14 callers
      migrated; wrapping shims kept (their bodies still need raw `id` for
      `bs.nodeIDs`/`labelIdx`/`keys.go`).
- [x] `Graph.publishEvent(typ, id types.EntityID, …)` and all 18 callsites
      migrated as part of the paired `Event.EntityID` Tier A migration above.
- [x] BadgerStore `cascadeDeleteInner` / `cascadeDeleteLocked` → typed
      `types.NodeID`. Parallel-agent run 2026-05-05.
- [ ] BadgerStore remaining helpers: `filter*ByTemporalPeek`,
      `fetch*WithTemporalFilter`.
- [ ] Internal store maps (`bs.nodeIDs map[snowflake.ID]struct{}`, `bs.outIdx`,
      `ms.nodes`, `ms.nodeHistory`, etc.) → typed keys. This is the big one:
      it drops the `id := nid.SnowflakeID()` shim from every store method
      (the shims that round-3 agent B reported as un-droppable today).

Estimate: large but mostly mechanical. Each migration eliminates a fixed number
of `.SnowflakeID()` calls. Track by `grep -c '.SnowflakeID()' pkg/graph/*.go`
before/after.

### Phase 7 — Tier D: justified raw uses

What genuinely must stay `snowflake.ID`:

- **`keys.go`** (`nodeKey`, `relKey`, `labelIndexKey`, `histNodeKey`,
  `histRelKey`, `histNodePrefix`, `histRelPrefix`) — produce big-endian uint64
  in byte slices. Needs raw int64 bits.
- **`wire.go`** (`nodeWire.ID int64`, `relWire.ID int64`,
  `BaseEntityID int64`) — msgpack on-disk format. Cannot change without breaking
  every Badger db file already on disk.
- **`snowflake.Node` library boundary** — `nodeIDGen.CreatedAt(id)`,
  `snowflakeLayout.Decompose(id)`. Third-party API. Convert at callsite.
- **`Graph.DecomposeID(id snowflake.ID)`** — public method that's deliberately
  type-agnostic (works for either kind, returns time/node/sequence
  components). Defensible.

Probably justified (decide):
- **`entityLockManager`** (`LockEntity`, `LockTwo`, `LockMany` taking
  `snowflake.ID`) — same 256-shard pool serves both node and rel IDs by hashing
  the snowflake bits. Type-agnostic by design. Two options:
  - Keep raw, document as type-agnostic (simplest).
  - Take `interface{ SnowflakeID() snowflake.ID }` so callers stay typed.
- **`shardIndex(id snowflake.ID)`** (`entity_locks.go:27`) — the hash function
  itself. Same call.
- **`collectDeleteIDs` / `sameIDSet`** (`context.go:782` / `context.go:799`) —
  the slice deliberately mixes a node ID with rel IDs because LockMany is
  type-agnostic. A typed wrapper would be a lie. Documented inline. Decided
  during the 2026-05-05 smoke-test agent run (option 1).

Tier D action items:
- [ ] Decide entityLockManager: raw + documented, or interface-typed.
- [ ] Decide DecomposeID: keep raw (current), or split into `DecomposeNodeID` /
      `DecomposeRelID` for symmetry. Recommend: keep raw, single function.
- [ ] Audit `keys.go` to confirm every function justifies raw. Add a one-line
      package doc stating the invariant.
- [ ] Audit `wire.go` to confirm every `int64` is a wire-format requirement.

### Phase 8 — Cleanup + ship

- [ ] Remove `InternalID()` from `pkg/types/node.go` and
      `pkg/types/relationship.go`. The original deal: keep until everything
      works. Everything works. There are zero call-sites in this repo (engram
      pinned at v3.1.1 keeps using it but is unaffected by removal here until
      it bumps).
- [ ] CHANGELOG.md `[Unreleased]` → `[3.2.0]`: replace the "Phase 3+4 pending"
      block with what actually shipped, the Tier A migrations, and the
      one-chokepoint architecture invariant. Migration notes for downstream.
- [ ] CLAUDE.md / AGENTS.md / docs/architecture.md: status `v3.1.7` → `v3.2.0`.
- [ ] README.md: migration callout for downstream callers.
- [ ] tag `v3.2.0` and push.

## Out of scope

- engram update (pinned at `v3.1.1`).
- Badger on-disk format changes (`wire.go` `int64` IDs unchanged).
- Snowflake bit layout (unchanged).
- `tkgd-v3` (consumes via the same module path; will pick up changes at its own
  cadence).

## Parallel-agent prompt template

Lessons from the 2026-05-05 smoke test (Agent A on `sameIDSet`, Agent B on
`highFrequencyIndex`): scope must match the **natural unit of the change**, not
"one file per agent". Agent A built clean and tests passed but produced a
type-lie because the natural unit (`collectDeleteIDs` + `sameIDSet` + LockMany)
crossed the file constraint. Agent B's task was naturally file-local and the
result was a clean win.

Prompt structure that worked:

1. **One-line goal** (the migration the agent owns).
2. **Background** (mid-migration, what's the invariant, where the boundaries are).
3. **Current state** with explicit file:line + signatures the agent will encounter.
4. **Migration target** with the new signatures spelled out.
5. **Cross-cutting boundary check**: list every callsite/dependency the agent
   should expect to encounter, and explicitly say which they own and which are
   off-limits. Tell the agent: *if the natural unit of change exceeds the scope,
   STOP and report rather than work around it*.
6. **Verify-before-report** commands.
7. **Report-back template** under 200 words.

Anti-patterns to avoid:
- "Touch only X.go" when the natural unit spans X.go + Y.go. Forces local-but-
  wrong fixes (round-1 Agent A's `toRelIDs` adapter).
- Letting two agents pick which file to modify. Either pre-list the files or
  use isolated worktrees.
- Open-ended "migrate all the things". Agents need a finite scope to verify.

Race condition observed in round 3:
- Two parallel agents in the same workspace will both run `go build` /
  `go test` independently. The agent that finishes first will see the slower
  agent's mid-edit state and report a transient build failure. This is NOT a
  bug in the faster agent's work — its own files are consistent, but the
  package as a whole isn't.
- The fix is to ALWAYS do an external joint verify after both agents finish.
  Don't rely on either agent's solo verify when they share a package.
- Alternative: isolated worktrees per agent. More setup overhead, no false
  red signals. Pick based on task size — for 1-2 file scopes, joint-verify is
  faster; for 5+ file scopes that risk overlap, worktrees pay for themselves.

## Audit commands

```sh
# how much raw `snowflake.ID` survives in production code:
grep -rn 'snowflake\.ID' pkg/graph/*.go pkg/types/*.go | grep -v '_test\.go' | wc -l

# how many `.SnowflakeID()` calls remain (Tier C / D progress indicator):
grep -rc '\.SnowflakeID()' pkg/graph/*.go pkg/types/*.go | grep -v ':0$' | sort -t: -k2 -nr

# verify the chokepoint invariant — only keys.go + wire.go should have many:
grep -c 'snowflake\.ID' pkg/graph/keys.go pkg/graph/wire.go
```
