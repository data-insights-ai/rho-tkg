# API 4.0 cleanup tracker — 2026-05-14

This is a v4.0 breaking-change pass. Every change here breaks the public surface
in some way. Customers will need a one-time migration.

Status legend: `[ ]` pending · `[~]` in progress · `[x]` done · `[-]` dropped after deeper look.

The order below is the order I plan to land changes. Each block is meant to be
mergeable in isolation so we can pause/abort at any point.

---

## Block 1 — Naming consistency

### 1.1 — `temporal.Relationships*` → `temporal.Rels*`
- [ ] `temporal.RelationshipsAt` → `temporal.RelsAt`
- [ ] `temporal.RelationshipsByTypeAt` → `temporal.RelsByTypeAt`
- [ ] `temporal.RelationshipsDuring` → `temporal.RelsDuring`
- [ ] `RelsByTypePropertyAt` / `RelsByTypePropertyDuring` — already short, leave
- [ ] `temporal.RelsAsOf` — already short, leave
- [ ] `temporal.RelateRels` — already short, leave
- [ ] `temporal.RelInterval` — already short, leave

Why: package is `rels`, sub-API field is `Rels`, ID type is `RelID`. Long
form is the outlier.

### 1.2 — `Nodes.NextVersion` / `PreviousVersion` → `VersionAfter` / `VersionBefore`
- [ ] `Nodes.NextVersion(id, version)` → `Nodes.VersionAfter(id, version)`
- [ ] `Nodes.PreviousVersion(id, version)` → `Nodes.VersionBefore(id, version)`
- [ ] `Rels.NextVersion(id, version)` → `Rels.VersionAfter(id, version)`
- [ ] `Rels.PreviousVersion(id, version)` → `Rels.VersionBefore(id, version)`

Why: `NextVersion(id, version)` reads as "next version of id" which is wrong —
it returns the version immediately after the supplied version number.
`VersionAfter` / `VersionBefore` is unambiguous.

### 1.3 — Drop `Legacy` from index provider API
- [ ] `Index.RegisterLegacyProvider` → remove (if truly legacy) OR rename to
      `Index.RegisterIndexProvider(p LegacyIndexProvider)` overload-style with
      union type, OR keep as `Index.RegisterProvider` with a wider interface
- [ ] `LegacyIndexProvider` interface → fold into `IndexProvider` if functionally
      compatible; else `Index.RegisterIndexProviderV1` (explicit version) is
      better than "Legacy" (vague)

Decision needed: do legacy providers still need to exist? If yes, name them
honestly (`RegisterIndexProviderV1`). If no, delete.

---

## Block 2 — Type leakage at the boundary

### 2.1 — `Admin.DecomposeID(snowflake.ID)` leaks third-party type
- [ ] Replace with `Admin.DecomposeNodeID(types.NodeID) IDComponents`
- [ ] Add `Admin.DecomposeRelID(types.RelID) IDComponents`
- [ ] Top-level `graph.DecomposeID` (the package-level var) — same fix
- [ ] Remove the `snowflake.ID`-typed variant entirely from the public surface

Why: every other public method speaks `types.NodeID` / `types.RelID`. Leaking
`snowflake.ID` couples customers to the internal ID-generator library.

### 2.2 — `Resolve` token-leakage methods
- [ ] `Resolve.LabelToken(name string) (uint16, error)` → move to internal-only
- [ ] `Resolve.RelTypeToken(name string) (uint16, error)` → move to internal-only
- [ ] `Resolve.LookupLabel(name string) (uint16, bool)` → move to internal-only
- [ ] `Resolve.LookupRelType(name string) (uint16, bool)` → move to internal-only
- [ ] Keep `Resolve.NodeProperty` / `Resolve.RelProperty` — they are the
      legitimate shadow-property accessors

If anyone genuinely needs token access (probably no one outside tkgd-v3), we
can re-add a typed `LabelToken` opaque type at that point.

---

## Block 3 — Surface size: collapse `*WithContext` siblings

Pattern: every `Foo(args...)` has a `FooWithContext(ctx, args...)` sibling. We
collapse to context-only.

- [ ] `Nodes.Add` / `Nodes.AddWithContext` → `Nodes.Add(ctx, labels, props)`
- [ ] `Nodes.Get` / `Nodes.GetWithContext` → `Nodes.Get(ctx, id)`
- [ ] `Nodes.Update` / `Nodes.UpdateWithContext`
- [ ] `Nodes.UpdateInPlace` / `Nodes.UpdateInPlaceWithContext`
- [ ] `Nodes.Delete` / `Nodes.DeleteWithContext`
- [ ] `Nodes.CompareAndSetProperty` / `CompareAndSetPropertyWithContext`
- [ ] `Rels.Add` / `Rels.AddWithContext`
- [ ] `Rels.AddByID` / `Rels.AddByIDWithContext`
- [ ] `Rels.AddByIDIfAbsent` / `Rels.AddByIDIfAbsentWithContext`
- [ ] `Rels.Get` / `Rels.GetWithContext`
- [ ] `Rels.Update` / `Rels.UpdateWithContext`
- [ ] `Rels.UpdateInPlace` / `Rels.UpdateInPlaceWithContext`
- [ ] `Rels.Delete` / `Rels.DeleteWithContext`
- [ ] `Rels.CompareAndSetProperty` / `CompareAndSetPropertyWithContext`

Convention: every mutation takes `ctx context.Context` as the first parameter.
Reads (`Get`, `GetByIDs`, `All`, `ByLabel`, `Count`, history readers, temporal
queries) — also take ctx, since they may need to honor cancellation on tiered
stores. Passing `context.Background()` is one keystroke.

Risk: every customer call site changes. Mitigation: provide a sed/gofmt-driven
migration recipe in the CHANGELOG.

Open question: do `Has*`, `Type()`, `PrimaryLabel`, `Labels` (pure getters on
already-fetched entities) need ctx? **No** — they take a `*types.Node` /
`*types.Relationship` and do no I/O. Keep them ctx-free.

---

## Block 4 — Sentinel error surface audit

- [ ] Remove `ErrDepthTemporalUnsupported` (explicitly deprecated in errors.go:65-68)
- [ ] Audit which of the 30+ exported sentinels are actually returned from any
      public path. Use `grep -rn "return.*Err<X>" pkg/`. Anything 0-hit goes.
- [ ] Sentinel naming: ensure pattern `Err<Subject><Predicate>` consistently
      (e.g. `ErrZeroID` vs `ErrInvalidID` — which one wins for invalid input?
      consolidate)
- [ ] Re-exports between `pkg/graph` and sub-API packages should not duplicate
      identifiers — pick one canonical location and import alias from the
      other

---

## Block 5 — Inconsistent error-return shapes

- [ ] `Stats.Get() GraphStats` → `Stats.Get() (GraphStats, error)`
      OR `NodeCount() (int, error)` → `NodeCount() int` (if it's a pure local
      counter read with no store call). Investigate which it actually is and
      pick the consistent shape.
- [ ] Same audit across `Constraints.Get`, `Resolve.NodeProperty` (returns
      `(any, bool)` — the bool is reasonable here, but verify pattern)

---

## Block 6 — Structural splits

### 6.1 — `Resolve` sub-API split
- [ ] Keep `Resolve.NodeProperty` / `Resolve.RelProperty` on `g.Resolve`
- [ ] Move token methods to internal (Block 2.2 handles this)
- [ ] After the move, `Resolve` is just shadow-property accessors. Could rename
      to `g.Shadow` or `g.VirtualProps`. **Decision needed:** keep `Resolve`
      name (familiar) or rename to `Shadow` (precise)?

### 6.2 — `Admin` sub-API split
- [ ] Move tiered-specific methods to a new `g.Tiered` sub-API:
      - `Archive`, `Restore`, `ForceRotate`, `ListShards`, `RebuildCatalog`,
        `Repair`, `VerifyShard`
- [ ] Keep generic methods on `g.Admin`:
      - `Reset`, `DecomposeNodeID`, `DecomposeRelID` (post Block 2.1)
- [ ] `g.Tiered` returns `ErrNotTieredStore` for memory/badger backends; the
      method existence becomes a compile-time hint that it only works on
      tiered, instead of a hidden runtime trap

### 6.3 — `Events.Set*` / `Get*` consolidation
- [ ] Replace `SetSync` / `SetAsync` / `GetSync` / `GetAsync` with:
      - `Events.Set(any) error` — accepts `*EventBus` or `*AsyncEventBus` via
        type switch
      - `Events.Get() any` — returns the configured bus or nil
      - Or: a single `EventPublisher` interface that both implement, with
        `Events.Set(EventPublisher)` / `Events.Get() EventPublisher`
- [ ] Document that setting both is forbidden (mutually exclusive)

---

## Block 7 — `io.Import` / `io.ImportWithOptions` collapse

- [ ] Remove `io.Import(r)` — keep only `io.Import(ctx, r, opts ImportOptions)`
- [ ] Customers using default options pass `ImportOptions{}`
- [ ] `io.Export` similarly takes `ctx` (currently does not)
- [ ] Same for the `Tx.Export` method on `*GraphTx`

---

## Block 8 — `Tx.Begin` footgun documentation

- [ ] Strengthen the docstring on `Tx.Begin` — make `Tx.Run` the obvious
      default. Document explicitly: "Prefer Tx.Run unless you need control over
      tx lifecycle that spans multiple function boundaries. Begin requires you
      to call Rollback() in every error path; Run handles this with defer."
- [ ] No code change beyond docstring. This is the one non-breaking change.

---

## Block 9 — `Rels.Outgoing` / `Incoming` temporal counterpart

Not strictly inconsistent, but a discoverability gap:
- [ ] Add `temporal.OutgoingAt(nodeID, typeName, t)` 
- [ ] Add `temporal.IncomingAt(nodeID, typeName, t)`

OR alternatively (less new surface):
- [ ] Add `Rels.Outgoing(opts QueryOpts)` and `Rels.Incoming(opts QueryOpts)`
      where `QueryOpts.ValidAt` does the temporal filtering

**Decision needed:** prefer adding a single QueryOpts-flavored overload over
two new sub-API methods.

---

## Block 10 — `BatchAPI.Run` mirror of `TxAPI.Run`

- [ ] Add `BatchAPI.Run(fn func(*BatchBuilder) error) (*BatchResult, error)`
      — panic-safe, deferred `Abort()` if fn errors
- [ ] Document `BatchAPI.New` as the imperative escape hatch (parallel to
      `TxAPI.Begin`)

---

## Block 11 — `GraphTx` direct methods naming alignment

`GraphTx` has methods like `AddNode`, `AddNodeLabel`, `SetNodeProperty`, etc.,
named in the pre-restructure style. Now that customers reach mutations through
`g.Nodes.Add(ctx, ...)`, the parallel `tx.AddNode(...)` is a small inconsistency.

- [ ] Decision: keep `tx.AddNode` / `tx.AddRelationship` shape (familiar, mirrors
      pre-3.4 code) — DROP this block. Customers already inside a tx callback
      know the context is the tx itself.
- [-] (Default to dropping unless someone has a strong opinion)

---

## Block 12 — Wire-up follow-ups

After Blocks 1-11 land:
- [ ] Update `pkg/graph/doc.go` package documentation to reflect the v4.0 surface
- [ ] Update `CLAUDE.md` — re-write Architecture section to match new shape
- [ ] Update `README.md` — re-write quick-start example
- [ ] Update `docs/api.md`
- [ ] Bump `CHANGELOG.md` — add `## [4.0.0]` section with full migration guide
- [ ] All 5 tutorials in `tutorials/` need to be rewritten against the new API
- [ ] All test files need to be updated (this is mechanical — ~285 test files
      touched the v3.4.0 release, similar scope expected)

---

## Execution order

I will land these in the order above, one block at a time:
1. Block 1 (naming) — pure renames, low risk
2. Block 2 (type leakage) — small new methods, small removals
3. Block 8 (Tx.Begin doc) — non-breaking, free
4. Block 3 (collapse WithContext) — large mechanical sweep, high churn
5. Block 4 (sentinel audit) — investigative
6. Block 5 (error shape) — investigative, small fix
7. Block 7 (IO collapse) — small
8. Block 6.1 (Resolve split) — small
9. Block 6.2 (Tiered split) — medium
10. Block 6.3 (Events.Set) — small
11. Block 9 (temporal Outgoing/Incoming) — small additive
12. Block 10 (BatchAPI.Run) — small additive
13. Block 11 — drop unless asked
14. Block 12 (docs/tests/tutorials) — large mechanical sweep, runs last

After each block: `make test-race` and `make ci` must pass. If a block touches
the bench harness, run the v3.1.20 comparison.

The bug-fix sweep from the three deep-review agents is **independent of this
plan** and will be landed in separate commits in `[Unreleased]`. The 4.0
version bump only happens once both this API cleanup AND any HIGH/CRITICAL
findings from the deep-review are resolved.
