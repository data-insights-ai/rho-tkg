# rho-tkg backlog

**STATUS: hardening-pass findings open (added 2026-07-18).** Every previously-tracked
design item (BACKLOG 1-5) is still shipped and unaffected. A full-library hardening
review (16 parallel subsystem audits covering every package under `pkg/`, ~100K LOC)
found the items below: real bugs (several data-loss/crash/silent-wrong-answer class),
concurrency edges, performance issues, code smells, test-coverage gaps, and a short
list of missing library-level capabilities. Nothing here is sigma-tkgd's — these are
all rho-tkg-owned. Full per-item detail (mechanism, exact repro, suggested fix) is
richer than what's captured below; re-run the relevant subsystem audit if a summary
line isn't enough to start a fix.

**Severity legend:** CRITICAL = crash / data loss / replica divergence / silent
corruption. HIGH = silent wrong answer or a real, reachable correctness bug. MEDIUM =
concurrency edge case, perf cliff, or API/contract inconsistency. LOW = code smell,
doc drift, or narrow-impact issue. TEST-GAP = a real behavior is unverified (may be
hiding a bug). FEATURE = a capability the library plausibly should have but doesn't.

**Recommended triage order:** the CRITICAL items in BACKLOG 19 (TieredStore) and
BACKLOG 12 (`ChangeClear` replica divergence) risk crashes/replica corruption in
production and should go first; the HIGH items touching bitemporal correctness
(BACKLOG 10) and the write-path kernel (BACKLOG 9) are next because they're silent
wrong-answer bugs in the library's core value proposition.

## Shipped (was the backlog)

- **BACKLOG 1 — Retention purge (ex-ADR-0008 R2–R5).** Single-store age purge
  (R2) + `UniqueForever` owner reaping, `ChangeRangePurge` record + replica
  re-execution (R3), sharded + tiered cross-shard sweep (R4) incl. the tiered
  O(1) cold-shard-drop drain-protocol optimization, and `ByValidTo` (R5).
- **BACKLOG 2 — Cross-machine incoming half-edge "Model A" (ADR-0010 §3.3).**
  Store write, replica apply, graph door + byte-exact convergence, cascade, and
  the tx-rollback stub restore. All increments shipped and byte-exact verified.
- **BACKLOG 3 — Columnar / streaming whole-node fetch (sigma X5-wholenode).**
  The one-iterator bulk substrate (`forEachNodeBulk`, `PrefetchValues=false`
  load-bearing), parallel decode across cores (`collectNodesBulkParallel` with
  batched staging), the same substrate under `AllNodes` and the DocValues
  cold-build (`bulkNodePropGetter`), and the streaming door
  `g.Nodes().ForEachByLabel` + `IterByLabel` (O(1) peak memory). The as-of
  columnar sibling (`DocValuesSnapshotAsOf`) shipped + corrected with the
  (label, txAt) cache.
- **BACKLOG 4 — Review-driven adaptations.** 4b per-label DocValues epoch,
  4c configurable `HistoryAnchorInterval` (persisted compat marker), 4d ingest
  `IntentRecord` cleanup, 4e `PeekTx`. (4a per-version temporal-envelope prune
  was owner-decided **DO NOT BUILD** — net-negative for the confirmed workload.)
- **BACKLOG 5 — Rel-side ordering-soundness primitives (sigma).**
  `g.Stats().RelRangeCardinality` (5A) and `g.Stats().RelPropertyTypeClassCounts`
  (5B) — rule-2 mirrors of the node doors.

## Not tracked here (cross-team)

Sigma-coordinated RPCs are sigma's to build; rho-tkg already exposes the local
primitives they call, so they are intentionally **not** rho-tkg backlog items:
the START→END foreign-stub-delete fan-out (BACKLOG 2 Inc 4c) and the
consumer-gated constraint dry-run (HP2.5). When sigma pins a shape that needs a
new rho-tkg primitive, it re-enters here as a fresh, concrete item.

---

## Open — Hardening Pass (2026-07-18)

### BACKLOG 6 — `pkg/types` data-model hardening

- **6a. [FIXED — pkg/types/propertyslice.go, temporal_time_property_test.go] `SetProperties` skips `time.Time` canonicalization (HIGH).** `propertyslice.go:1037,1084`,
  used by `node.go:208`/`relationship.go:152`. `Set()`/`NewPropertySlice()` canonicalize a
  raw `time.Time` to `TemporalValue` before validation; `SetProperties`'s `canonicalPropertySlice`
  path never does, so an entity built via `SetProperties` with a `time.Time` value stores it
  uncanonicalized, silently diverges in hash from an equivalent `Set()`-built entity, and later
  panics in `AppendPropertyHashBytes`. Fix: call `canonicalizeTemporalValue` inside
  `canonicalPropertyValue`.
- **6b. [FIXED — pkg/types/heapsize.go, heapsize_test.go] `heapsize.go` struct-size constants stale — `CacheBudgetBytes` under-counts (HIGH).**
  `heapsize.go:25` (`approxTemporalMeta=72`, actual 96B) and `:27` (`approxIntegrity=96`, correct
  for `NodeIntegrity` but `RelIntegrity` is actually 128B — same constant wrongly reused for both,
  under-counting every relationship's integrity metadata by 33%). Also never adds
  `CreatedBy`/`UpdatedBy` string content. Systematically undermines the `CacheBudgetBytes` soft
  limit. Fix: use `unsafe.Sizeof` instead of hardcoded literals (see 6c), split
  `approxNodeIntegrity`/`approxRelIntegrity`, add string-content length.
- **6c. [FIXED alongside 6b] Node/Relationship `ApproxHeapBytes` hardcode struct sizes independent of `layout_test.go`'s
  pinned truth (LOW, drift risk).** `heapsize.go:107,126`. Fix: `const nodeBaseSize =
  unsafe.Sizeof(Node{})` instead of a bare literal — 6b shows this drift already happened twice.
- **6d. [FIXED — `pkg/types/node.go`, `pkg/types/node_test.go`] `RemoveLabelTokenRaw` could promote
  reserved token 0 to primary label (MEDIUM).** `node.go:498`.

  **Bug.** `NewNode`'s documented/tested contract (`TestNewNodeAllowsZeroExtraLabelForStoreValidation`)
  permits token 0 to sit in a node's extra-label set — it's simply dedup'd against the primary and
  otherwise treated as an ordinary (if permanently invisible-to-`HasLabelToken`) extra. `RemoveLabelTokenRaw`'s
  primary-removal branch, however, unconditionally promoted `extraLabels[0]` to primary with no check
  for token 0. Since `HasLabelToken`/`HasLabelTokenRaw` hard-code `tok == 0 → false` (token 0 is
  reserved), a node built as `NewNode(id, primary, []uint16{0, realLabel})` whose primary was then
  removed ended up with `primaryLabel == 0` — a primary label that can never again be observed as
  present via `HasLabelToken`, while `LabelTokenCount()` still silently counted it. A perfectly good
  non-zero candidate (`realLabel`) sat right behind the 0 in the extras list and was ignored.

  **Fix.** The promotion step now scans for the first NON-ZERO extra and promotes that one, leaving any
  skipped token-0 extras in place (not discarded — they're still legitimate, if invisible, extras per
  `NewNode`'s own contract). If every remaining extra is token 0 (or there are none), the removal is
  refused and returns `false` — extending the exact same "no candidate to promote" refusal the
  `len(extraLabels)==0` case already used, rather than silently promoting 0 anyway. No sibling
  `RemoveLabelToken(labelToken)` door exists (grep confirmed `RemoveLabelTokenRaw` is the only removal
  door — Node's label set has no generic-vs-named-door duplication the way temporal queries do), and
  `Relationship` has no structural mirror (a single `tkg_type` token, not a primary+extras set), so this
  is a single-site fix.

  **Tests** (`pkg/types/node_test.go`): `TestRemoveLabelTokenRawSkipsZeroTokenWhenPromoting` — a node
  with extras `[0, 20]`; removing the primary promotes 20 (not 0), asserts `HasLabelToken(20)` is now
  true and the skipped `0` survives as the sole remaining extra.
  `TestRemoveLabelTokenRawRefusesPromotionWhenOnlyZeroExtraRemains` — a node with only `[0]` as its
  extra; removal is refused, primary and extras stay unchanged (mirrors the pre-existing
  `TestRemoveLabelTokenRawRefusesLastLabel` shape for the new edge case the fix introduces).

  **Verification.** RED confirmed via `git stash push -- pkg/types/node.go`: promotion test failed with
  `PrimaryLabelToken() = 0, want 20`; refusal test failed with `RemoveLabelTokenRaw(primary) = true...
  want false`. Stash popped, GREEN confirmed for both new tests plus the pre-existing
  `TestRemoveLabelTokenRawRefusesLastLabel` and `TestNewNodeAllowsZeroExtraLabelForStoreValidation`
  (non-regression). Full `go build ./...` and `go vet ./...` clean. `go test ./pkg/types/...` full
  package pass. Full-repo `go test ./...` clean across every package — no regressions. Not
  concurrency-sensitive (pure single-goroutine struct mutation matching every other `Node` mutator's
  contract), so `-race` was not required.
- **6e. [FIXED — `pkg/types/recurrence.go`, `pkg/types/recurrence_test.go`] `RecurrencePattern.Expand`
  had no bound on iteration count (MEDIUM, DoS, currently unreachable in-repo).**
  `recurrence.go:110,128,161`.

  **Bug.** `expandByDay`/`expandMonthly`/`expandYearly` only validated `from < to`, never the SPAN.
  `expandByDay` (the Daily/Weekly path) iterates day-by-day between `from` and `to`; an arbitrary
  caller-supplied multi-million-year `[from, to)` — trivially constructible since `Instant` is a plain
  `int64` millisecond count with no domain-level range check — drives an unbounded loop/allocation, the
  same DoS class as lesson 48 (untrusted-size-driven amplification) but via loop iteration count rather
  than a single `make()` call. `RecurrencePattern.Expand` is not wired to any `pkg/graph` door today
  (confirmed: `pkg/graph`'s own `ErrInvalidTimeRange` is a distinct `store`-package sentinel, not
  `types.ErrInvalidTimeRange` — the two never alias), but it's a public `pkg/types` API another
  ecosystem module (e.g. `tkgd-v3`) could call directly with an attacker- or misconfiguration-derived
  range.

  **Fix.** Added a `maxExpandSpan` cap (1000 years in milliseconds) checked in `Expand` immediately
  after the existing `from >= to` check and before `Validate()`/any loop runs — bounding the DoS
  surface at the API's own entry point rather than inside each frequency's loop body. Exceeding the cap
  returns the new `ErrRecurrenceSpanTooLarge` sentinel. 1000 years was chosen (not a smaller,
  loop-tighter cap) because two PRE-EXISTING passing regressions — `TestRecurrence_YearlyLargeRange`
  and `TestRecurrence_WeeklySparseLargeRange` — already exercise a legitimate 200-year span
  successfully; 1000 years preserves 5x headroom over the largest real test case while still rejecting
  a "multi-million-year" span roughly 1000x over the cap, and bounds `expandByDay`'s absolute worst
  case to ~365,000 iterations (cheap, not a DoS vector). `RecurrencePattern.Expand` has no re-export
  requirement in `pkg/graph/errors.go` since it isn't reachable through any `pkg/graph` door.

  **Tests** (`pkg/types/recurrence_test.go`): `TestRecurrence_Expand_RejectsExcessiveSpan` — a ~1
  million year span (year 1 to year 1,000,000) is rejected with `ErrRecurrenceSpanTooLarge`, checked
  via `errors.Is` (rule 4). `TestRecurrence_Expand_AllowsSpanUpToCap` — a 999-year Daily/Monday span
  (just under the cap, using the tightest loop) still succeeds and returns intervals — the
  non-regression counterpart proving the fix didn't lower the ceiling below what real callers already
  rely on.

  **Verification.** RED confirmed via `git stash push -- pkg/types/recurrence.go`: the new tests failed
  to even COMPILE (`undefined: types.ErrRecurrenceSpanTooLarge`) — the strongest possible confirmation
  the guard didn't exist, and deliberately avoids ever running a genuine unbounded 1-million-year loop
  inside a test (which would hang the suite rather than fail fast). Stash popped, GREEN confirmed for
  both new tests plus every pre-existing `TestRecurrence_*` test, including the 200-year
  `YearlyLargeRange`/`WeeklySparseLargeRange` non-regressions. Full `go build ./...` and `go vet ./...`
  clean. `go test ./pkg/types/...` full package pass. Full-repo `go test ./...` clean across every
  package — no regressions. Not concurrency-sensitive (pure value-receiver computation), so `-race` was
  not required.
- **6f. No `unsafe.Sizeof` cross-check test for `heapsize.go` constants (TEST-GAP).** Root cause of 6b
  shipping undetected twice.

### BACKLOG 7 — Public façade & thin sub-API wrapper hardening

- **7a. [FIXED — internal/core/core.go, r5_close_completeness_test.go] `Core.Close()` never closes an installed `AsyncEventBus` — goroutine leak (HIGH).**
  `events/api.go:42-48` → `internal/core/events_dispatch.go:68-84` → `internal/core/core.go:1659-1699`.
  The async dispatcher goroutine only exits via its own `Close()`, which `Core.Close()` never calls.
  Every open/close cycle with an async bus configured leaks a goroutine permanently; no test asserts
  `Close()` stops it. Fix: `Core.Close()` calls `c.events.Close()` if capable, or document that the
  caller owns the bus's lifecycle.
- **7b. [FIXED — `pkg/graph/errors.go`, `pkg/graph/errors_doc_test.go`, `pkg/graph/errors_identity_test.go`,
  `docs/errors.md`] `ErrNilSession` was never re-exported (MEDIUM).** `internal/core/ingest.go:33-34` /
  `pkg/graph/ingest/api.go:28` / `pkg/graph/errors.go`.

  **Bug.** `errors.go`'s own header claims "every sentinel a public Graph operation can return is
  re-exported here," yet `ErrNilSession` was missing. Worse, `errors_doc_test.go`'s `ingestSentinels`
  list carried an EXPLICIT, INCORRECT justification for the omission: "`ErrNilSession` is a
  session-nil-receiver guard used only inside internal/core (never surfaced through pkg/graph)." That
  claim doesn't hold: `pkg/graph/ingest/api.go` declares `type Session = core.Session` — a TYPE ALIAS,
  not a wrapper struct — so every exported `Session` method (`AddNode`, `AddRelationship`,
  `UpdateNode`, `UpdateRelationship`, `DeleteNode`, `DeleteRelationship`, `Submit`, and `Close`'s own
  explicit check) is directly reachable on a `*ingest.Session` value from ANY `pkg/graph` consumer. A
  caller holding a nil `*ingest.Session` (e.g. from a failed `NewSession` call handled incorrectly, or
  a zero-value struct field) and calling any mutator hits `core.Session.lockOpen()`'s nil-receiver
  guard and gets back `core.ErrNilSession` — with no way to classify it via `errors.Is(err,
  graph.ErrNilSession)` since the alias didn't exist.

  **Fix.** Added `ErrNilSession = core.ErrNilSession` to `pkg/graph/errors.go`. Corrected
  `errors_doc_test.go`'s `ingestSentinels` list and its comment to reflect the actual reachability
  (removed the incorrect justification, added `"ErrNilSession"` to the list). Added the identity pair
  to `errors_identity_test.go`'s `TestSentinelAliasesShareIdentity` battery
  (`"NilSession/graph=core": {graphpkg.ErrNilSession, core.ErrNilSession}`). Added a row to
  `docs/errors.md`'s Ingest Pipeline table.

  **Tests.** RED confirmed via `git stash push -- pkg/graph/errors.go`: `pkg/graph` package tests
  failed to COMPILE (`graphpkg.ErrNilSession undefined`) once the identity-test reference was added —
  the strongest possible confirmation the re-export was genuinely absent. Stash popped, GREEN confirmed
  for `TestSentinelAliasesShareIdentity`, `TestErrorsDocumentation` (docs/errors.md row-presence check),
  and `TestGraphErrorsFileInventoryComplete` (go/parser sweep of errors.go against the canonical
  inventory list) — all three of the doc-completeness meta-tests this codebase uses to prevent exactly
  this class of drift. The actual end-to-end reachability (a nil `*ingest.Session` method call
  genuinely returning `ErrNilSession`) is covered by BACKLOG 7g's new `pkg/graph/ingest/api_test.go`
  (`TestSessionNilReceiverReturnsErrNilSession`), fixed together with this item since both close the
  same test-gap in the ingest sub-API. Full `go build ./...` and `go vet ./...` clean. `go test
  ./pkg/graph/...` full pass. Full-repo `go test ./...` clean — no regressions.
- **7c. [FIXED — `pkg/graph/errors.go`, `pkg/graph/errors_doc_test.go`, `pkg/graph/errors_identity_test.go`,
  `docs/errors.md`] Tiered-store sentinels (`ErrNotReferenceEntity`, `ErrEventPropertyIndex`,
  `ErrPrimaryLabelClassMutation`) were reachable via `tier`/`index`/`nodes` sub-APIs but never
  re-exported (MEDIUM).**

  **Bug.** All three sentinels are declared in `pkg/graph/store/tiered` and confirmed reachable through
  a genuine public door each: `ErrNotReferenceEntity` via `g.Tier().Archive()`/`Restore()` on an
  event-classed node (`requireReferenceArchiveNode`); `ErrEventPropertyIndex` via
  `g.Index().CreatePropertyIndex()` on an event-classed label (CLAUDE.md's "Property indexes on
  reference entities only" rule); `ErrPrimaryLabelClassMutation` via `g.Nodes().AddLabelToken()` /
  `RemoveLabelToken()` (and their `WithHistory` variants) attempting to flip a primary label's
  reference↔event class (CLAUDE.md's "Primary-label class is immutable" rule, lesson B33). None had a
  `pkg/graph`-level alias, so a caller had no way to classify them via `errors.Is(err, graph.ErrXxx)`
  without importing `pkg/graph/store/tiered` directly and knowing to type-assert the concrete backend —
  `pkg/graph/tier` had no `errors.go` at all, unlike `index`/`temporal`.

  **Fix.** Because the three sentinels span THREE different sub-APIs (not just `tier`), re-exported
  them centrally in `pkg/graph/errors.go` (matching how every other backend-optional sentinel in this
  file is handled) rather than growing a single sub-API package's own `errors.go` for a subset. Added
  the `tieredpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"` import (new — `pkg/graph`
  didn't previously import the tiered package directly) and a `var (...)` block aliasing all three.
  Updated the doc-completeness meta-tests together: `errors_doc_test.go` gained a `tieredOntologySentinels`
  list wired into `graphReexportSentinelNames()`; `errors_identity_test.go` gained three identity pairs
  in `TestSentinelAliasesShareIdentity`; `docs/errors.md` gained a new "TieredStore Reference/Event
  Ontology" section with all three rows.

  **Verification.** RED confirmed via `git stash push -- pkg/graph/errors.go`: `pkg/graph` package
  tests failed to COMPILE (`undefined: graphpkg.ErrNilSession`, `ErrNotReferenceEntity`,
  `ErrEventPropertyIndex`, `ErrPrimaryLabelClassMutation` — the identity-test references added
  alongside 7b's fix all failed at once, confirming both re-exports were genuinely absent together).
  Stash popped, GREEN confirmed for `TestSentinelAliasesShareIdentity`, `TestErrorsDocumentation`, and
  `TestGraphErrorsFileInventoryComplete`. The underlying tiered-store enforcement behavior itself
  (primary-label-class immutability, event-property-index rejection, non-reference-entity archive
  rejection) already has dedicated coverage in the `tiered` package's own test suite — this fix is
  purely the re-export/classification layer, verified by identity rather than a redundant integration
  test. Full `go build ./...` and `go vet ./...` clean. `go test ./pkg/graph/...` full pass. Full-repo
  `go test ./...` clean — no regressions.
- **7d. `index/errors.go` doc comment overclaims completeness (LOW-MEDIUM).** `index/errors.go:23-26`
  — missing `ErrIndexExists`, `ErrIndexNotFound`, `ErrTemporalIndexExists/NotFound`,
  `ErrInvalidShardDepth`, `ErrInvalidQueryLimit/Cursor`, `ErrOrderedScanTemporal`,
  `ErrRelPropertyIndexUnsupported` (all re-exported at `pkg/graph` level but not here).
- **7e. Stale "Drop\*" doc comments on `Delete*` methods, 4 places (LOW).** `index/api.go:63-64,
  156-157, 174-175, 216-217` — leftover from a `Drop*`→`Delete*` rename.
- **7f. Package doc's "complete public surface" list omits `Replication()` (LOW).** `graph.go:1-31`
  vs `:171-176`.
- **7g. [FIXED — `pkg/graph/ingest/api_test.go`] `pkg/graph/ingest` had zero direct tests;
  `subapi_zero_value_test.go` skipped it entirely (MEDIUM, Rule 1 violation).** The
  `ready()`→`ErrNilGraph` branch of `NewSession`/`AppliedSeq`/`WaitApplied` was untested, unlike every
  other sub-API.

  **Fix.** New `pkg/graph/ingest/api_test.go`, mirroring `pkg/graph/tier/api_test.go`'s established
  pattern for this exact class of sub-API (nil-receiver battery + spy-backed forwarding battery), plus
  a third battery specific to ingest's two-type-surface shape (`API` wraps `Ops`; `Session` is a type
  alias, not wrapped):
  - `TestAPINilReceiversReturnErrNilGraph` — nil `*API`, zero-value `API`, and a typed-nil `Ops`
    (`New((*ingestOpsSpy)(nil))`) all correctly return `ErrNilGraph` from `NewSession`/`WaitApplied`,
    and `0` from the no-error `AppliedSeq`.
  - `TestAPIForwardsEveryMethod` — a spy `Ops` implementation proves `NewSession` forwards `opts`
    verbatim and returns the spy's error, `AppliedSeq` forwards the spy's return value, and
    `WaitApplied` forwards the token verbatim.
  - `TestSessionNilReceiverReturnsErrNilSession` — the direct proof BACKLOG 7b's re-export fix needed:
    every mutator (`AddNode`, `AddNodes`, `AddRelationship`, `UpdateNode`, `UpdateRelationship`,
    `DeleteNode`, `DeleteRelationship`, `Submit`, `Close`) on a nil `*ingest.Session` returns
    `core.ErrNilSession` (checked via `errors.Is`, rule 4), and the nil-safe no-error `Pending()`
    returns `0` rather than panicking.

  **Verification.** All three tests pass immediately against the current (already-correct) `ingest`/
  `core.Session` implementation — this item is a pure test-gap closure (Rule 1: every public method
  gets a direct test), not a behavior fix; the RED counterpart for the ONE genuine behavior gap this
  investigation surfaced (the missing `ErrNilSession` re-export) is filed and fixed separately as
  BACKLOG 7b. `go build ./...` and `go vet ./...` clean. `go test ./pkg/graph/ingest/...` full pass.
  Full-repo `go test ./...` clean — no regressions.
- **7h. Duplicated generic helpers across sub-API packages (LOW, code smell).**
  `iterateForEach`/`cloneStrings` copy-pasted verbatim between `nodes/api.go`, `rels/api.go`,
  `index/api.go`, plus structurally-identical `cloneShardInfo`/`cloneCounts` in `tier`/`stats`. A fix
  in one copy risks not landing in the other (lesson A1 class). Fix: extract to a shared internal
  package.
- **7i. No relationship-side mirror of `PropertyStats` (LOW, see BACKLOG 21 feature list).**

### BACKLOG 8 — Dry-run / constraints / replication convenience-API correctness

- **8a. `DecodeChangeIdentity`/`ChangeOpOf` misclassify 3 shipped change-tag families as corrupt
  [FIXED — pkg/graph/replication/change_identity.go, change_identity_test.go] (HIGH).** `replication/change_identity.go:97-112,128-180`. `ChangeForeignIncoming` (11),
  `ChangeForeignIncomingDelete` (12), `ChangeRangePurge` (13) — all shipped features (ADR-0010 Model
  A, ADR-0008 R3) — fall through to `default: ErrCorruptWire` in this public CDC-decode convenience
  layer (the real `apply_record.go` handles all 13 correctly; the bug is isolated here). Breaks any
  out-of-tree CDC consumer once those features fire. Fix: add the 3 cases; add a test iterating every
  `ChangeTag` asserting none of the known tags return `ErrCorruptWire`.
- **8b. `DryRunValidate` silently accepts self-loop relationships and skips label/name validation
  [FIXED — internal/core/dry_run_validate.go, dry_run_validate_test.go] (HIGH).** `internal/core/dry_run_validate.go:106-156,199-220`. The real kernel checks
  `validateRelationshipEndpointIDs` + `AllowSelfLoops` unconditionally before temporal constraints;
  the dry-run only calls `checkTemporalConstraints`, gated on `constraints.Len()>0`. A self-loop with
  `AllowSelfLoops=false` reports zero violations in dry-run but fails `ErrSelfLoop` for real. Also
  never validates labels (empty/oversized/too-many) or type name. This backs sigma's consumer-gated
  constraint dry-run RPC per CLAUDE.md — a direct "faithful predictor" promise violation. Fix: run the
  endpoint/self-loop/label/name checks unconditionally inside `DryRunValidate`; add adversarial tests.
- **8c. [FIXED — `pkg/graph/constraints/api.go`, `pkg/graph/constraints/unique.go`,
  `pkg/graph/constraints/capability_sentinel_test.go`] `constraints` sub-API type-assertion failure
  returned wrong sentinel `ErrNilGraph` (LOW-MEDIUM, was dead code, untested per Rule 5).**
  `constraints/api.go:78-81`, `unique.go:133-143`.

  **Bug.** Both `DryRunValidate` (`api.go`) and `uniqueReady()` (`unique.go`, feeding `CreateUnique`/
  `CreateUniqueForever`/`ReleaseOwnership`/`DropUnique`) returned `grapherr.ErrNilGraph` when the
  underlying `Ops` implementation failed a type assertion to `DryRunOps`/`UniqueOps` — i.e. when the
  capability simply isn't supported by that `Ops` implementation. `grapherr.ErrNilGraph` is explicitly
  documented as "returned when a public graph or sub-API wrapper is nil or unwired" — the WRONG
  sentinel for a capability gap, and doubly wrong here since `a.ready()` (called immediately above,
  one line earlier in both functions) already returns the correct `ErrNilGraph` for the actual
  nil/unwired case. Both sites shared the identical bug shape (two doors, same mistake).

  **Fix.** Both sites now return `storepkg.ErrCapabilityNotSupported` — the established sentinel this
  codebase uses everywhere else for "the configured backend/implementation doesn't support this
  optional operation."

  **Tests** (`pkg/graph/constraints/capability_sentinel_test.go`, new file): reused the existing
  `constraintsOpsSpy` fixture (`api_test.go`) — it implements only the base `Ops` interface
  (`Set`/`Add`/`Get`), never `DryRunOps`/`UniqueOps`, making it the exact fixture needed to trigger the
  previously-dead branch. `TestDryRunValidate_UnsupportedOpsReturnsCapabilityNotSupported` and
  `TestUniqueMethods_UnsupportedOpsReturnCapabilityNotSupported` (covering all four `uniqueReady()`
  callers) assert `errors.Is(err, storepkg.ErrCapabilityNotSupported)`.

  **Verification.** RED confirmed via `git stash push -- constraints/api.go constraints/unique.go`:
  both tests failed with `err = graph: graph must not be nil, want ErrCapabilityNotSupported`. Stash
  popped, GREEN confirmed for both new tests plus every pre-existing test in the package (the real
  nil-graph path via `a.ready()` is unaffected). Full `go build ./...` and `go vet ./...` clean.
  `go test ./pkg/graph/constraints/...` full package pass. Full-repo `go test ./...` clean — no
  regressions.
- **8d. [FIXED — `pkg/graph/io/backup.go`, `pkg/graph/io/backup_durability_test.go`]
  `BackupTo`/`BackupDeltaTo` fsync the file but not the containing directory (MEDIUM, durability
  gap).** `io/backup.go:75-117,138-189,234-248`.

  **Bug.** Both `BackupTo` and `BackupDeltaTo` call `tmp.Sync()` on the staged file before publishing
  it — durable DATA — then publish via `renameNoClobber`, which does `os.Link(tmpPath, finalPath)`
  (create the final-name directory entry) followed by a best-effort `os.Remove(tmpPath)` (drop the
  temp-name entry). Both are directory-METADATA mutations in the SAME containing directory, and on
  POSIX filesystems that don't implicitly journal directory metadata alongside file data, that
  metadata is only guaranteed durable across a crash after a SEPARATE fsync of the directory itself —
  `fsync(file)` says nothing about the directory entry that makes the file reachable under its final
  name. A crash immediately after a successful `BackupTo`/`BackupDeltaTo` return could therefore leave
  the backup's bytes on disk but the directory entry publishing them lost on recovery.

  **Fix.** Added `fsyncDir(dir string) error` (opens the directory and calls `.Sync()`) and call it
  once at the end of `renameNoClobber` — the single call site both `BackupTo` and `BackupDeltaTo`
  already route through, so one fix covers both doors without duplicating the sync step per caller.
  Placed AFTER both the `Link` and the `Remove` so one directory fsync flushes both pending metadata
  changes. Failure is NOT swallowed (unlike the best-effort `os.Remove`) — it propagates as an error
  from `renameNoClobber`/`BackupTo`/`BackupDeltaTo`, matching the existing `tmp.Sync()` error-returning
  contract rather than silently downgrading the durability guarantee the doc comments already claimed.
  Updated both functions' doc comments to state the directory-fsync guarantee explicitly.

  **Tests** (`pkg/graph/io/backup_durability_test.go`, new internal `package io` file — crash-durability
  itself isn't observable in a unit test, so this proves the helper is correctly wired instead):
  `TestFsyncDir_ExistingDirSucceeds` / `TestFsyncDir_NonexistentDirReturnsError` — the new helper's
  success and error paths in isolation. `TestRenameNoClobber_PublishesAndSyncsDir` — direct proof
  `renameNoClobber` reaches the new fsync step on its normal success path (publishes the file, removes
  the temp name, returns nil). The untouched pre-existing `TestBackupTo_*`/`TestBackupDeltaTo_*`
  end-to-end batteries (including the concurrent-callers race tests) give regression coverage that the
  new unconditional `fsyncDir` call doesn't break normal operation or the exactly-once-wins publish
  guarantee.

  **Verification.** RED confirmed via `git stash push -- pkg/graph/io/backup.go`: the new test file
  failed to COMPILE (`undefined: fsyncDir`) — the strongest possible confirmation the helper was
  genuinely absent. Stash popped, GREEN confirmed for all three new tests. Full `go build ./...` and
  `go vet ./...` clean. `go test ./pkg/graph/io/...` full pass (all 20 pre-existing + 3 new tests,
  including `TestBackupTo_ConcurrentCallersExactlyOneWins` /
  `TestBackupDeltaTo_ConcurrentCallersExactlyOneWins` / `TestRenameNoClobber_ConcurrentCallersExactlyOneWins`
  — the atomicity guarantee is unaffected by the added fsync). `go test -race ./pkg/graph/io/...`
  clean. Full-repo `go test ./...` clean — no regressions.
- **8e. `(*TempOps).Diff` doc comment claims a continuously-held RLock; actual impl does per-entity
  `readUnderRLock` (LOW, stale doc, safety property still holds).** `internal/core/temporal_snapshot.go:88-96`
  vs `:188-293`.
- **8f. `countStreamChangeRecords` doc describes a precondition its only caller doesn't satisfy
  (informational, harmless today, landmine for future refactor).** `io/backup.go:191-196` vs `:171`.

### BACKLOG 9 — Write-path kernel hardening (CRUD, unique constraints, version chains)

- **9a. [FIXED — internal/core/version_chain.go, version_chain_test.go] `CloseVersion` writes a history row without bumping the version counter — silent chain
  corruption (CRITICAL).** `internal/core/version_chain.go:120-186,341-411`. Neither
  `closeNodeVersionInternal` nor `closeRelVersionInternal` calls `SetVersion`/`nextEntityVersion`,
  unlike every sibling `*WithHistory` door. `Get(id).Version()` stays identical pre/post close;
  `GetNodeVersion(id,0)` returns the pre-close snapshot while `Get(id)` returns post-close;
  `VersionAfter(id,0)` returns nil — the close is invisible to chain-walking consumers. Violates
  lesson 13. No test covers `History()`/`GetNodeVersion`/`VersionAfter` after a close. Fix: bump the
  version like every other `*WithHistory` door, with the overflow check.
- **9b. `UpdateInPlace` mutates `ValidFrom`/`ValidTo` in place with no `TxFrom` bump — violates lesson
  [RESOLVED — documentation fix only, no behavior change; CLAUDE.md, node_update.go, relationship_update.go] 46's append-only bitemporal discipline (HIGH).** `internal/core/relationship_update.go:439-451`
  (node side structurally identical). No new row, no `TxFrom` stamp — a bitemporal read pinned between
  the original write and the in-place correction silently sees the *corrected* values as if always
  believed. Also: CLAUDE.md's Shadow Properties table claims these fields are "rejected by
  UpdateInPlace," directly contradicted by passing tests — doc/code have drifted. Fix: either revert
  to rejecting these fields (match the doc) or bump `TxFrom` on in-place valid-time mutation; add a
  two-phase before/after AS-OF test.
- **9c. [FIXED — `internal/core/unique_constraints.go`, `unique_constraints_toctou_test.go`]
  Unique-constraint 3-phase install has a TOCTOU window against standalone writers (MEDIUM-HIGH,
  standalone-only — batch path is fenced).** `internal/core/unique_constraints.go:529-623` vs
  `:256-365`.

  **Bug**: `enforceUniqueForNodeHeld`'s fast-path exit (`if !c.hasUniqueConstraints.Load() { return
  noop, nil }`) runs OUTSIDE any lock. A standalone write's `addNodeInternal` holds `c.mu.RLock()` for
  its ENTIRE duration (the enforcement check through the actual store write, via
  `NodeOps.Add`→`c.runUnderRLock`). If that check ran while `hasUniqueConstraints()` was still false, it
  took NO value-stripe lock — so nothing stopped that same write's ACTUAL COMMIT from landing arbitrarily
  later, including after `CreateUnique`'s Phase 1 (install pending) → Phase 2 (validate existing data) →
  Phase 3 (activate) had already run to completion. Since `createUnique` touched no `c.mu` at all, its
  Phase 2 validation scan could complete BEFORE such a write's late commit, see no duplicate, and
  activate — after which the late-committing write's duplicate value landed under a constraint nothing
  had ever checked it against.

  **Fix**: Phase 1's install now runs under `c.mu.Lock()` (in addition to the existing `c.uniqueMu.Lock()`
  for the registry bookkeeping itself — lock order `c.mu` outer, `c.uniqueMu` inner, matching the
  ALREADY-established order `enforceUniqueForNodeHeld` uses when called from inside `addNodeInternal`'s
  `c.mu.RLock()`). Since EVERY standalone write holds `c.mu.RLock()` for its whole duration,
  `c.mu.Lock()` cannot be granted while any such write is still in flight — so by the time
  `installConstraintLocked` runs, every write that checked "no constraint yet" has ALREADY fully
  committed (and is therefore visible to Phase 2's scan, which runs afterward using its own
  pre-existing internal `c.mu.RLock()` via `validateUniqueExisting`/`readUnderRLock` — deliberately NOT
  covered by the new `c.mu.Lock()`, since a non-reentrant RWMutex would self-deadlock if it were), and
  every write starting AFTER the install observes the now-installed PENDING entry and correctly
  self-enforces via the stripe lock. The `c.mu.Lock()` window is intentionally minimal — JUST Phase 1's
  install, not the whole 3-phase sequence — since that's the only step whose ordering relative to
  in-flight writes matters; extending it further (e.g. around `c.Index.CreateProperty`, which ALSO calls
  `readUnderRLock` internally) would have reintroduced the same self-deadlock risk for no additional
  correctness benefit.

  **Test** (`unique_constraints_toctou_test.go`,
  `TestCreateUnique_BlocksInstallUntilInFlightWriteCompletes`): an internal `package core` test (direct
  field access to `c.mu`) simulates an in-flight write deterministically — takes `c.mu.RLock()` directly
  to stand in for "a write that already passed its check, about to commit" — then starts `CreateUnique`
  in a goroutine and asserts it does NOT complete within 150ms (must be blocked on the install's
  `c.mu.Lock()`), writes a duplicate-valued node DIRECTLY to the store (simulating the in-flight write's
  late commit) while still holding the fake RLock, releases it, and asserts `CreateUnique` then returns
  `ErrUniqueViolationExisting` — the late-arriving duplicate correctly caught by Phase 2's scan, which by
  construction cannot run until after the release.

  RED confirmed via `git stash push -- unique_constraints.go`: the test failed with `CreateUnique
  completed (err=<nil>) while a simulated in-flight write held c.mu.RLock()` — the constraint was created
  successfully with NO wait, exactly the "wins the race, activates before the late write is visible"
  symptom the finding describes. Popped the stash, confirmed GREEN. Full `go build ./...` + `go vet
  ./...` clean; `go test ./pkg/graph/internal/core/...` clean; `go test -race
  ./pkg/graph/internal/core/...` clean (141s — this IS the concurrency-sensitive class rule 7 exists
  for); full repo `go test ./...` clean.
- **9d. [FIXED — `internal/core/relationship_create_kernel.go`, `internal/core/relationship_add.go`,
  `internal/core/relationship_add_id_ordering_test.go`] Relationship ID minted before endpoint
  existence is validated (MEDIUM, low practical impact — violates the "validate before generating
  IDs" rule).** `internal/core/relationship_add.go:171-172` — `nextRelID()` ran before
  `relEndpointHashLadder` confirmed both endpoints exist.

  **Bug.** Both standalone create doors that share `relEndpointHashLadder` (`createRelationshipLocked`
  used by `Add`/`AddByID`, and `addRelationshipByIDIfAbsentInternal`) called `id := c.nextRelID()`
  BEFORE handing `id` to the ladder, which is where endpoint existence actually gets confirmed
  (`liveEndpointNodes`/`liveEndpointHashes`, both store fetches that fail with `ErrNodeNotFound` for a
  missing endpoint). A create attempt against a nonexistent endpoint therefore always minted a fresh
  snowflake ID before discovering the failure — violating CLAUDE.md's Design Rules invariant "Validate
  before generating IDs: AddNode/AddRelationship validate before NextNodeID()/NextRelID()", which the
  NODE creation path already honors. Investigation surfaced a related, PRE-EXISTING, out-of-scope
  detail while writing the regression tests: the ladder's `endpointHashWrite`-capability branch (wired
  only for `*memory.Store`/`*tiered.Store` per `nativeRelationshipEndpointHashWrite`'s backend switch)
  skips existence validation in the ladder entirely, relying on a later store-write failure instead —
  that's a separate concern from THIS finding's ID-minting-order issue and was left untouched.

  **Fix.** Changed `relEndpointHashLadder`'s signature from taking an already-minted `id
  types.RelID` parameter to taking a `mintID func() types.RelID` callback (normally `c.nextRelID`,
  passed as a method value) and returning the (possibly minted) `id` as its first result. Each of the
  ladder's three branches now calls `mintID()` only AFTER its own endpoint-existence check has already
  succeeded — the `constraints.Len() > 0` branch mints after `liveEndpointNodes` succeeds (it also
  needs the ID to build the temporal-constraint probe, so minting must happen there, just after
  validation rather than before); the `endpointHashWrite` branch mints unconditionally (unchanged
  behavior — this branch was never validating existence, in or out of the ladder); the default branch
  mints after `liveEndpointHashes` succeeds. Both call sites (`createRelationshipLocked`,
  `addRelationshipByIDIfAbsentInternal`) updated identically (grep-confirmed exhaustive — no other
  caller exists) to `id, fromHash, toHash, useEndpointHashWrite, err := c.relEndpointHashLadder(c.nextRelID, ...)`.

  **Tests** (`internal/core/relationship_add_id_ordering_test.go`, new file): exercises
  `relEndpointHashLadder` directly with a call-counting spy in place of `c.nextRelID` — the precise
  mechanism the fix changes. `TestRelEndpointHashLadder_DoesNotMintIDWhenEndpointMissing` /
  `TestRelEndpointHashLadder_MintsIDAfterValidationPasses` — a badger-backed `Core` (badger is never
  routed through the `endpointHashWrite` fast path per `nativeRelationshipEndpointHashWrite`'s backend
  switch, so it genuinely exercises the default `liveEndpointHashes` existence-checking branch, unlike
  a memory-backed `Core`, discovered mid-investigation and documented in a new
  `newTestGraphBadgerNoEndpointHashWrite` helper's comment): the spy is called 0 times on a missing
  endpoint, exactly 1 time (with the correct returned ID) on success.
  `TestRelEndpointHashLadder_ConstraintsBranch_DoesNotMintIDWhenEndpointMissing` — the same proof for
  the `constraints.Len() > 0` branch (which always runs regardless of backend), checked via
  `errors.Is(err, storepkg.ErrNodeNotFound)`. `TestAddByID_MissingEndpointStillReturnsExpectedError` —
  the end-to-end non-regression counterpart via the public `Rels.AddByID` door, proving the reordering
  doesn't change the observable error.

  **Verification.** RED confirmed via `git stash push -- internal/core/relationship_create_kernel.go
  internal/core/relationship_add.go`: the new test file failed to COMPILE (`cannot use mint (variable
  of type func() types.RelID) as types.RelID value`) against the old signature — the strongest possible
  confirmation of the ordering bug's exact mechanism. Stash popped, GREEN confirmed for all four new
  tests. Full `go build ./...` and `go vet ./...` clean. `go test ./pkg/graph/internal/core/...` full
  package pass. `go test -race ./pkg/graph/internal/core/...` clean (concurrency-relevant — this touches
  the entity-locked create path). Full-repo `go test ./...` clean — no regressions.
- **9e. `UniqueForever` claim leaked when a multi-key node's checks are interleaved with claims
  [FIXED — internal/core/unique_constraints.go, unique_enforce_batch.go, unique_forever_test.go] (HIGH).** `unique_constraints.go:596-621`, `unique_enforce_batch.go:111-150`. Per-tuple loop
  claims+persists `UniqueForever` immediately; if a *later* tuple fails, the whole create aborts but
  the earlier tuple's claim is **not** rolled back — the value is now permanently owned by a
  never-created entity ID. Not tx-rollback (undocumented case); nondeterministic trigger (map
  iteration order). Fix: two-pass — validate all tuples read-only (`checkForeverOwnership`) first,
  claim only after every tuple passes.
- **9f. [INVESTIGATED — DEFERRED, needs an owner design decision] Same `UniqueForever` leak class:
  durable claim not rolled back when the entity write fails AFTER the claim persists (MEDIUM,
  standalone + amplified in batch).** `unique_forever.go:246-258`; batch path
  `unique_enforce_batch.go:136-149` has no confirmed reconciliation on partial-batch failure either.

  **Confirmed mechanism.** `enforceUniqueForNodeHeld` (`unique_constraints.go:563-668`, post-9e) runs
  its two-pass validate-then-claim sequence and returns a `release func()` that ONLY unlocks the value
  stripes (`c.valueLocks.UnlockStripes(ordered)`) — it does NOT reverse `checkAndClaimForever`'s
  durable ownership-registry claim. Every standalone caller (`node_add.go:229`, `node_update.go:218,396`,
  `property_cas.go:201`, `node_label.go:164`) does `defer uniqueRelease()` UNCONDITIONALLY — called on
  both the success AND failure path of the subsequent store write. So: unique-constraint enforcement
  (including any `UniqueForever` claim) succeeds → the value stripe lock releases → THEN the actual
  entity write (`putGeneratedNode` etc.) fails for an unrelated reason (store I/O error) → the durable
  claim is now permanently orphaned, exactly as `checkAndClaimForever`'s own comment documents:
  "a rare failed write leaves a conservative claim, correctable via ReleaseOwnership."

  **Why this is deferred rather than fixed.** The leak is an EXPLICIT, DOCUMENTED design trade-off —
  crash-safety (never silently lose or under-claim a value) over auto-rollback complexity — not an
  oversight, unlike 9e (which fixed a genuine ordering bug the code never claimed to handle). Building
  a correct compensating auto-release requires either (a) extending `enforceUniqueForNodeHeld`'s return
  signature to expose exactly which `UniqueForever` tuples were newly claimed so callers can compensate
  on write failure, touching every one of the 5+ call sites above plus the batch/ingest paths (a broad,
  invasive signature change — CLAUDE.md's "every fix needs a grep audit" applies at maximum scope
  here), or (b) making the `release` closure itself failure-aware (`release(success bool)`), same
  breadth of call-site change. Either approach introduces a NEW correctness risk that does not exist
  today: a compensating release racing a DIFFERENT concurrent claimant for the same value between the
  original claim and the write-failure discovery would need the SAME value-stripe-held discipline the
  original claim used, and getting that ordering wrong could let two entities believe they own the same
  `UniqueForever` value — strictly worse than today's "conservative over-claim, manually correctable"
  contract. This is a genuine architecture/product decision (invest in auto-compensation vs. keep the
  documented manual-`ReleaseOwnership` contract), not an implementation-scope judgment call — logged for
  a dedicated design session, matching the precedent set by BACKLOG 10b (also investigated, also
  deliberately deferred rather than force-patched). The batch-path reconciliation question
  (`unique_enforce_batch.go:136-149`) shares the identical root cause and the identical fix-or-defer
  decision, so it is not tracked as a separate item.
- **9g. [FIXED — `internal/core/temporal_cascade.go`, `internal/core/temporal_cascade_overflow_test.go`]
  `temporal_cascade.go` version-split logic bypassed the `nextEntityVersion` overflow guard used
  everywhere else (MEDIUM, needs ~4B versions to trigger but a genuine invariant gap). Also closes 9q
  (cascade-path overflow was untested).** `internal/core/temporal_cascade.go:113,158-159,181,323,356-357,382`.

  **Bug.** `cascadeNodeVersionInterval`/`cascadeRelVersionInterval` each compute
  `nextVersion := maxVersion + 1` via raw `uint32` arithmetic, then (when the resumption-row branch
  fires, i.e. `newVT != 0` and a pre-correction belief resolves at `newVT`) allocate a SECOND version
  via a bare `nextVersion++`. Every OTHER versioned mutation door (`Update`, `CompareAndSetProperty`,
  `AddLabel`, `RemoveLabel`, per `version_overflow_test.go`) routes through `nextEntityVersion`, which
  explicitly checks for `math.MaxUint32` and returns `ErrVersionOverflow` rather than silently wrapping
  to 0 — colliding with the genesis-version sentinel (`Node.Version()==0` means "first version" per
  CLAUDE.md's "Genesis detection" rule). The cascade path's two raw increments bypassed this guard at
  BOTH allocation sites.

  **Fix.** Replaced both raw increments in both functions (4 sites total — 2 per function) with calls
  to `nextEntityVersion`, propagating `ErrVersionOverflow` immediately via early return. The initial
  `nextVersion := maxVersion + 1` becomes `nextVersion, err := nextEntityVersion(maxVersion)` (reusing
  the function-scope `err` already declared by the preceding `getCurrentNode`/`getCurrentRelationship`
  call); the resumption branch's `nextVersion++` becomes `nextVersion, err = nextEntityVersion(nextVersion)`
  (assigning to the `if`-statement-scoped `err` shadow from `resolveNodeVersionAt`/`resolveRelVersionAt`,
  checked immediately in the same scope). Purely additive — no behavior change below the overflow
  boundary.

  **Tests** (`internal/core/temporal_cascade_overflow_test.go`, new file, modeled on the existing
  `version_overflow_test.go` helpers — `forceStoredNodeVersion`/`forceStoredRelVersion`/
  `assertNodeVersionAndProperty`/`assertRelVersionAndProperty`): four tests covering both entity types
  and BOTH allocation sites. `TestCascadeVersionOverflow_{Node,Rel}InitialAllocation` — version forced
  to `math.MaxUint32`, `SetNodeVersionInterval`/`SetRelVersionInterval` called with `newVT=0` (so the
  resumption branch never fires, isolating the FIRST allocation site) — asserts `ErrVersionOverflow`
  and that the entity's stored version/property are unchanged.
  `TestCascadeVersionOverflow_{Node,Rel}ResumptionAllocation` — version forced to `math.MaxUint32-1`
  (so the FIRST `nextEntityVersion` call succeeds, yielding exactly `MaxUint32`) with a nonzero `newVT`
  in the future (so the resumption branch fires and resolves the current row as its source) — asserts
  the SECOND allocation site's overflow is caught too, distinguishing it from the first via the
  one-below-ceiling setup.

  **Verification.** RED confirmed via `git stash push -- internal/core/temporal_cascade.go`: all four
  tests failed with `err = <nil>, want ErrVersionOverflow` — the raw arithmetic silently wrapped with no
  error surfaced, exactly the described bug (verified the entity's version subsequently WOULD have
  wrapped to a colliding value had the write proceeded, though the test only needed to prove the missing
  error to confirm the gap). Stash popped, GREEN confirmed for all four. Full `go build ./...` and
  `go vet ./...` clean. `go test ./pkg/graph/internal/core/...` full package pass. Targeted
  `go test -race ./pkg/graph/internal/core/... -run 'TestCascade|TestVersionOverflow|TestTemporal'`
  clean (the fix itself is pure single-goroutine arithmetic, not a new concurrency surface — the
  existing entity-lock discipline the cascade already runs under is unchanged). Full-repo
  `go test ./...` clean — no regressions.
- **9h. [FIXED — `internal/core/relationship_foreign.go`, `internal/core/relationship_foreign_validation_test.go`]
  `RecordForeignIncoming` skipped `ValidationLimits` checks every other create door enforces
  (MEDIUM).** `internal/core/relationship_foreign.go:157-212`.

  **Bug.** `recordForeignIncomingInternal` called only `edge.Validate()` — purely STRUCTURAL
  well-formedness (nonzero IDs, nonempty type name, nonzero attest time; confirmed via
  `store/foreign_endpoint.go:89-103`) — before building the property slice and persisting. Unlike
  `prepareRelCreate` (the kernel every OTHER standalone/foreign-endpoint create door funnels through,
  including the sibling `addRelationshipByIDForeignEndInternal` which explicitly calls it — "Reuse the
  canonical validation order" per its own comment), it never called `c.validateName(edge.TypeName)` or
  `c.validateProperties(edge.Properties)` — this graph instance's configured `ValidationLimits`
  (`MaxNameLength`, `MaxPropertiesPerEntity`, and per-property `MaxPropertyKeyLength`/
  `MaxPropertyValueSize`/`MaxPropertyContainerLength` via `validateOwnedPropertyEntryForCreate`).
  `RecordForeignIncoming` is a DIRECTLY CALLABLE public door (ADR-0010 §3.3 — the caller RPCs edge
  fields in from another machine's authoritative create), unlike `apply_record.go`'s replica-apply
  path, which has a documented exemption because it reproduces a PRIMARY's already-validated state
  verbatim. A cross-machine stub sourced from a machine configured with looser local limits could
  therefore exceed THIS machine's caps entirely unrejected.

  **Fix.** Added `c.validateName(edge.TypeName)` and `c.validateProperties(edge.Properties)` calls
  immediately after `edge.Validate()` and before the property slice is built — mirroring
  `prepareRelCreate`'s validation order exactly.

  **Tests** (`internal/core/relationship_foreign_validation_test.go`, new file, using a real
  `sharded.Store`-backed `Core` — required for `c.foreignIncomingRel` to be wired):
  `TestRecordForeignIncoming_RejectsOversizedTypeName` (`MaxNameLength: 5`, oversized type name →
  `ErrNameTooLong`, and confirms no stub was recorded via `Incoming(endID, "")`, bypassing `Incoming`'s
  own name-length check on the type filter). `TestRecordForeignIncoming_RejectsTooManyProperties`
  (`MaxPropertiesPerEntity: 1`, two properties → `ErrTooManyProperties`, confirms no stub recorded).
  `TestRecordForeignIncoming_SucceedsWithinLimits` — the non-regression counterpart proving an edge
  within configured limits still succeeds and is visible via `Incoming`.

  **Verification.** RED confirmed via `git stash push -- internal/core/relationship_foreign.go`: both
  new rejection tests failed with `err = <nil>, want ErrNameTooLong`/`ErrTooManyProperties` — the
  oversized edge was silently accepted pre-fix. Stash popped, GREEN confirmed for all three new tests.
  Full `go build ./...` and `go vet ./...` clean. `go test ./pkg/graph/internal/core/...` full package
  pass. The pre-existing end-to-end Model A convergence suite (`go test ./pkg/graph/ -run 'ModelA'`:
  `TestModelA_ForeignIncomingConvergence`, `TestModelA_ForeignIncomingDeleteConvergence`,
  `TestModelA_TxRollbackRestoresStub`) and the sharded-store-level `RecordForeignIncoming` tests all
  still pass — the new validation calls don't disturb any well-formed edge already exercised.
  `go test -race ./pkg/graph/internal/core/... -run TestRecordForeignIncoming` clean. Full-repo
  `go test ./...` clean — no regressions.
- **9i. [FIXED — `store/foreign_endpoint.go`, `store/foreign_endpoint_test.go`] `AttestTx` on
  `ForeignEndpoint` was validated for presence only — the staleness-window check the doc comment
  seemed to promise doesn't exist (MEDIUM).** `relationship_foreign.go:71-120`,
  `store/foreign_endpoint.go:37-39,85`.

  **Investigation.** Confirmed via grep that `AttestTx` is read ONLY for a non-zero presence check
  (`Validate()` on both `ForeignEndpoint` and `ForeignIncomingEdge`) — nowhere in the codebase is it
  compared against `c.now()` or any staleness threshold. The pre-fix doc comment described AttestTx as
  "a cross-validation aid whose staleness window is made explicit by AttestTx (ADR-0010 §4.1)" —
  ambiguous wording a reasonable reader could parse as "the staleness window is CHECKED/enforced,"
  which no code anywhere does. Given CLAUDE.md's own architecture stance that cross-team sigma-tkgd
  orchestration RPCs are "sigma's to build — rho-tkg already exposes the local primitives," and that
  an acceptable-staleness bound is inherently a deployment POLICY decision (how large a gap is
  tolerable, clock-skew allowance, reject-vs-warn) rather than a fixed invariant this library could
  bake in — the right fix is NOT to invent a staleness-enforcement mechanism (a new configurable
  threshold + design question outside implementation scope) but to make the documentation say
  precisely what the code does: capture `AttestTx` as required, non-zero PROVENANCE so the gap is
  always knowable in persisted data, with NO local staleness bound — bounding it is the
  caller's/orchestrator's responsibility, enforced before calling these doors.

  **Fix.** Rewrote `ForeignEndpoint`'s and `ForeignIncomingEdge`'s type/field/`Validate` doc comments in
  `store/foreign_endpoint.go` to state explicitly: rho-tkg enforces no staleness bound; any positive
  `AttestTx` passes `Validate`; the caller must check `AttestTx` against its own clock before invoking
  `AddByIDForeignEnd`/`RecordForeignIncoming` if a bound is needed. Zero behavior change — `Validate`'s
  logic was already correct for what the library actually promises; only the doc's ambiguous phrasing
  changed.

  **Tests** (`store/foreign_endpoint_test.go`, new file — neither type had ANY direct test before this,
  a Rule 1 gap alongside the doc issue): `TestForeignEndpointValidate_AcceptsArbitrarilyOldAttestTx` /
  `TestForeignIncomingEdgeValidate_AcceptsArbitrarilyOldAttestTx` — pin the now-accurately-documented
  behavior (an `AttestTx` of `1`, the oldest possible positive `Instant`, is accepted). Plus full
  presence-check coverage for both types: zero `AttestTx`/`NodeID`/`RelID`/`StartID`/`EndID` and empty
  `Hash`/`TypeName` are each rejected with the correct sentinel (`ErrInvalidForeignEndpoint`/
  `ErrInvalidForeignIncoming`, checked via `errors.Is`).

  **Verification.** This is a documentation-precision fix with no behavior change (the "accepts old
  AttestTx" tests would pass identically before and after — there was never a RED state to reproduce,
  since the code never implemented a staleness check the doc arguably implied), so the fix is verified
  by full test coverage rather than a RED→GREEN cycle: `go test ./pkg/graph/store/...` full pass (all 6
  new tests plus every pre-existing test in the package). Full `go build ./...` and `go vet ./...`
  clean. Full-repo `go test ./...` clean — no regressions.
- **9j. [VERIFIED — already closed by BACKLOG 9c; regression test added:
  `internal/core/update_in_place_unique_race_test.go`] `UpdateInPlace`'s old-value snapshot decision
  reads `hasUniqueConstraints` BEFORE the mutation, ahead of the enforcement call it feeds — narrow
  race (MEDIUM).** `internal/core/node_update.go:336-339`.

  **Investigation.** The concern: `updateNodeInPlaceInternal` reads `c.hasUniqueConstraints.Load()` at
  line 344 (BEFORE the property mutation) to decide whether to snapshot `prevState` for
  `enforceUniqueForNode`, which runs AFTER the mutation and does its own independent
  `c.uniqueConstraints` read. If a constraint were installed strictly BETWEEN these two reads,
  `prevState` would stay nil (no constraint existed at snapshot time) while enforcement then found one
  active — enforcing without the freed-old-value stripe the snapshot exists to provide.

  Traced the actual locking: `NodeOps.UpdateInPlace` wraps its ENTIRE body (both reads) in ONE
  continuously-held `c.mu.RLock()` via `c.runUnderRLock`. `CreateUnique`'s Phase 1 install — the ONLY
  place `hasUniqueConstraints` transitions false→true — was fixed by BACKLOG 9c (earlier in this same
  session) to run under `c.mu.Lock()` specifically so it cannot complete while any standalone write
  holds `c.mu.RLock()` for its duration. Since `c.mu.Lock()` cannot be granted while ANY `RLock()` is
  outstanding, `CreateUnique`'s install cannot complete anywhere between `UpdateInPlace`'s two reads —
  `hasUniqueConstraints` can only change value strictly BEFORE or strictly AFTER a given `UpdateInPlace`
  call, never DURING one. 9c's fix (made for a different reason — closing a TOCTOU window in
  `CreateUnique` itself) closed 9j's race as a side effect, because both rely on the identical `c.mu`
  serialization primitive. (The reverse direction — a constraint REMOVED mid-call via `DropUnique`,
  which only needs `c.uniqueMu` — is harmless: `enforceUniqueForNodeHeld`'s own early-exit simply
  no-ops on the now-unconfigured constraint, at worst wasting the `prevState` `DeepCopy`.)

  **Verification test** (`internal/core/update_in_place_unique_race_test.go`, new file — no production
  code change, since the invariant already held): reproduces the EXACT interleaving 9j worried about
  using the same direct `c.mu.RLock()` simulation technique as `unique_constraints_toctou_test.go`'s 9c
  test. Takes `c.mu.RLock()` to simulate being inside an in-flight `UpdateInPlace`, starts `CreateUnique`
  in a goroutine and confirms it blocks (150ms), then calls `updateNodeInPlaceInternal` directly (still
  under the held RLock) to run the FULL real snapshot-then-mutate-then-enforce sequence, confirming it
  completes correctly (the value changes, no spurious enforcement) precisely because
  `hasUniqueConstraints` is provably still `false` at BOTH read points. Releases the RLock, confirms
  `CreateUnique` then proceeds and its Phase 2 scan correctly sees only the POST-update state (no
  observation of a half-transitioned constraint), and confirms the constraint is genuinely active
  afterward (a later duplicate-value `Add` is rejected).

  To confirm this test is a genuine, non-vacuous regression guard (not accidentally passing for an
  unrelated reason), it was run against the 9c fix REVERTED (`git stash push --
  internal/core/unique_constraints.go`): it FAILS exactly as expected (`CreateUnique completed... while
  a simulated in-flight UpdateInPlace held c.mu.RLock()`), proving the test would catch a regression if
  9c's `c.mu.Lock()` guard were ever weakened or removed. Stash popped, GREEN restored. Full
  `go build ./...` and `go vet ./...` clean. `go test ./pkg/graph/internal/core/...` full package pass.
  `go test -race ./pkg/graph/internal/core/... -run 'TestUpdateInPlace|TestCreateUnique|TestUnique'`
  clean. Full-repo `go test ./...` clean — no regressions.
- **9k. CLAUDE.md's Concurrency section omits `LockThree` (LOW, doc gap only — implementation sound).**
- **9l. `relationship_import.go` hand-rolls temporal stamping + create/rollback instead of reusing
  the kernel's `applyRelCreateTemporal`/`createRelWithTypeRollback` (LOW, lesson-17/58 drift risk).**
  `internal/core/relationship_import.go:206-224`.
- **9m. No test exercises `CloseVersion`→`VersionAfter`/`GetNodeVersion`/`History()` (TEST-GAP, root
  cause of 9a shipping).**
- **9n. `AddLabel`'s unique-constraint enforcement — CLAUDE.md's "door everyone forgets" — has ZERO
  test coverage (TEST-GAP, high risk: a future refactor dropping it ships a critical bypass silently).**
- **9o. `TestDeleteNodeWithContext_ConcurrentAddRel` never enumerates surviving relationships after
  the race (TEST-GAP).**
- **9p. No adversarial concurrent test pins 9c's TOCTOU window (TEST-GAP).**
- **9q. [FIXED — closed together with 9g, see `internal/core/temporal_cascade_overflow_test.go`]
  Version-overflow untested on the cascade path (TEST-GAP, direct consequence of 9g).**
- **9r. `node_delete.go`'s two-phase retry backs off with a bare `runtime.Gosched()`, no jitter
  (LOW).**
- **9s. `GetOrCreateByKey`'s atomicity only holds against concurrent `GetOrCreateByKey`/constraint-
  enforced writers, not a plain concurrent `Add`/`Update` with no active constraint — doc could be
  clearer (LOW).**

### BACKLOG 10 — Bitemporal resolution engine hardening

- **10a. [FIXED — internal/core/chain_resolver.go, chain_resolver_test.go] `resolveNodeChainAsOf`/`resolveRelChainAsOf` mutate shared/frozen chain rows in place — a
  live lesson-60 regression on TieredStore (CRITICAL).** `internal/core/chain_resolver.go:171-177,
  237-242`. `nodeVisibleAtTxTime` zeroes `TxTo`/`DeletedAt`/`ValidTo` directly on the shared object with
  no `DeepCopy()`, unlike the sibling `filterNodeChainByTxAt` which explicitly deep-copies and cites
  lesson 60 by name. Reachable via `tiered.Store` (declines `TransactionTimeQueryCapability`) falling
  through to `getNodeHistory`, which skips its own copy because `storeRowsTrust` is true for exact
  native stores — relying on a "frozen rows, `Temporal()` is the one mutation-access exception"
  contract this function violates. Two concurrent `NodeAsOf`/`RelAsOf` calls resolving the same
  underlying version object can race-write the same `*TemporalMetadata` under only `c.mu.RLock`. Fix:
  `return nodeVisibleAtTxTime(best.DeepCopy(), txPin), nil` (and the rel mirror).
- **10b. [CONFIRMED REAL via reproducible test; fix ATTEMPTED and REVERTED — needs a dedicated
  design session, do not attempt a quick patch] An open-ended cascade correction starting *before* an
  untouched open "current" row is silently capped and never wins, even though it's the newer belief
  (HIGH).** `internal/core/temporal_cascade.go:218-233` + `temporal.go:358-406,167-184,420-442`.
  `nodeVersionBounds` derives a version's end **positionally** (next sorted entry's `ValidFrom`), not
  by belief recency. Repro (now in `TestCascade_OpenCorrectionBeforeUntouchedOpenCurrent` history —
  reverted alongside the fix, see below): current v0 `ValidFrom=2000,open,TxFrom=T0`;
  `SetNodeVersionInterval(id,1000,0,corrected)` at `T1>T0` creates v1 `ValidFrom=1000,open,TxFrom=T1`;
  v1's effective end is computed as v0's `ValidFrom`(2000) since v0 sorts after it, so v1 loses the
  "current" slot to the untouched v0 and is demoted to history. `NodeAtTx(id,2500,txAt>=T1)` then
  returns v0's **pre-correction** content. Empirically confirmed with a live test (RED without a fix,
  100% reproducible, no flakiness).
  **Attempted fix (reverted):** (1) `nodeVersionBounds`/`relVersionBounds` skip a later-sorted `next`
  entry whose `TxFrom` is older than the entry being bounded, so `next` can't wrongly truncate a
  newer, wider-reaching correction; (2) the cascade's `newCurrent` selection picks the open-ended
  candidate with the *newest belief* (`nodeBeliefNewerThan`/`relBeliefNewerThan`) instead of "last in
  valid-from order"; (3) discovered that fix (1) alone breaks the **resumption row** the cascade
  constructs for a *bounded* correction (`newVT != 0`) — a resumption row re-asserting old content
  from `newVT` onward is STRUCTURALLY INDISTINGUISHABLE, via its own stored ValidFrom/ValidTo/TxFrom,
  from a genuine override like the one in the repro (both are "open, later TxFrom than some
  pre-existing older-belief row that should — or shouldn't — bound them" — the two cases require
  OPPOSITE treatment and cannot be told apart from per-row temporal metadata alone). Refined fix (3b):
  compute the resumption row's `ValidTo` EXPLICITLY at construction time from its source row's own
  effective end in the **pre-correction chain** (via `nodeVersionBounds` over `preChain` alone, never
  touched by fix (1)'s skip logic) instead of leaving it `0` and relying on positional tiling at read
  time. This fixed both the original repro AND the previously-passing `TestCascade_MidHistoryInsertion`
  — **but** running the full suite surfaced `TestBitemporalOracleHarness` /
  `TestBitemporalOracle_BadgerCommitWindow` (a property-based fuzz harness comparing an independent
  oracle model against live `NodesAtTx` results over long randomized operation sequences, including
  repeated/interleaved cascades) failing on roughly HALF of all random seeds — extra/missing/wrong-
  version nodes in the door's answer vs. the oracle. This means fix (1)'s blanket "skip older-belief
  next" rule, even with refinement (3b) patching the single-cascade resumption case, still produces
  wrong answers once MULTIPLE cascades (each contributing their own newVer/resumption pairs with
  distinct TxFrom values) accumulate on the same chain — the pairwise TxFrom comparison in
  `nodeVersionBounds` doesn't know "which cascade batch" a candidate pair belongs to, and applying it
  indiscriminately across an arbitrarily-merged multi-cascade chain has failure modes beyond the
  single-resumption case this session found and fixed. **All three changes were reverted** (`git
  checkout -- temporal.go temporal_cascade.go cascade_test.go`) — full suite is back to green.
  **For the next attempt:** the oracle harness (`bitemporaloracle_test.go`,
  `bitemporaloracle_commitwindow_test.go`) is the load-bearing regression gate — any fix MUST pass it
  at full iteration count (not just the two hand-written repro cases) before being considered done.
  The core open design question: how to durably distinguish, at read time, "a row asserting a genuine
  override of an older belief within its claimed domain" from "a row merely continuing/resuming an
  older belief that legitimately still bounds it" — per-row temporal metadata alone is provably
  insufficient (both shapes are structurally identical); the fix likely needs either a persisted
  marker distinguishing the two row roles, or a fundamentally different (non-positional,
  non-pairwise-TxFrom) algorithm for interval-bounds derivation in a chain with cascade-inserted rows.
- **10c. [FIXED — `internal/core/queries.go`, `pkg/graph/stats_rangecardinality_test.go`,
  `pkg/graph/stats_rel_rangecardinality_test.go`] `RangeCardinality` ignores `TxAt`/`TxPin` and
  answers a bitemporal-pinned query from the CURRENT-state exact index — silent undercount (HIGH).**
  `internal/core/queries.go:89-113,130-154` (node + rel mirrors). The decline guard checked only
  `ValidAt`/`ValidStart`/`ValidEnd`; a `TxAt`/`TxPin`-pinned call fell through to the live
  BSI/property-index fast path and returned `exact=true` from *current* state — violating the
  documented "correctness guarantee, NOT an estimate" contract used by ordering-soundness gates.
  Reachable via `g.Nodes()`/`g.Rels()`/`g.Stats().RangeCardinality`. Fix: added
  `|| opts.TxAt != 0 || opts.TxPin != 0` to both guards (node + rel doors). Added
  `TestRangeCardinality_DeclinesOnBitemporalTxFilter` and
  `TestRelRangeCardinality_DeclinesOnBitemporalTxFilter`, confirmed RED against the pre-fix guard
  (both TxAt and TxPin incorrectly returned `exact=true` from current state), GREEN after the fix.
  Full `go test ./...` and `go test -race ./pkg/graph/... ./pkg/graph/internal/core/...` pass.
- **10d. [FIXED — `internal/core/context.go`, `internal/core/txtime.go`, `internal/core/core.go`,
  `pkg/graph/errors.go`, `docs/errors.md`, `internal/core/advance_clock_test.go`,
  `internal/core/nowtx_test.go`] `AdvanceClock` had no upper-bound sanity check — the same bug class
  lesson 59 explicitly closed for `AllowTxBackfill`, left open here (HIGH).**
  `internal/core/txtime.go:216-222`, `context.go:72-83`. Only ever moved `c.lastInstant` forward; no
  bound against wall-clock. A single bad call (e.g. a unit mixup, lesson 59's exact trigger — passing
  `UnixMicro()` where `UnixMilli()` is expected, inflating a near-now value by ~1000x) permanently
  poisoned the clock for the process's life — every subsequent `TxFrom`/`TxTo`/`UpdatedAt` would stamp
  near the poisoned value, silently excluding them from every sane `TxAt`/`AsOf`/`TagAsOf` pin taken
  afterward. Fix: `advanceClockFloor` now rejects any `to` landing more than `maxClockAdvanceSkewMillis`
  (~10 years, deliberately generous — real cross-machine HLC peer skew is NTP-scale, at most hours; the
  bound exists to catch orders-of-magnitude unit/scale errors, not legitimate skew) ahead of
  `c.clock().UnixMilli()`, returning the new `ErrInvalidClockAdvance` sentinel (re-exported as
  `graph.ErrInvalidClockAdvance`, documented in `docs/errors.md`) instead of silently poisoning the
  floor. `advanceClockFloor`'s signature changed to `(types.Instant, error)` (its one caller,
  `TempOps.AdvanceClock`, updated to propagate the error). Two pre-existing tests
  (`TestPeekTx_DoesNotAdvanceClock`'s `1<<50`-ms jump — ~35.7 million years ahead — and its
  epoch-relative-not-wall-relative `big` constant, which was itself a latent bug making the jump land
  in 1975, *before* current wall-clock) were fixed to jump a bounded-but-still-deterministic 5 years
  ahead of the observed wall-clock baseline rather than an absolute epoch offset. Added
  `TestAdvanceClock_RejectsImplausibleFarFutureTarget` (confirmed RED by temporarily disabling the new
  guard: the poisoned target was silently accepted and a subsequent write's `TxFrom` reflected it;
  GREEN after the fix, with an explicit floor-unchanged assertion proving the rejected call doesn't
  poison state) and `TestAdvanceClock_AcceptsGenerousSkewTolerance` (a 1-year-ahead peer timestamp — the
  documented legitimate use case — is still accepted). Full `go test ./...` and
  `go test -race ./pkg/graph/...` pass.
- **10e. [FIXED — doc-only, `internal/core/temporal_cascade.go`; also closes 10i via
  `internal/core/cascade_prevhash_test.go`] Cascade's `PrevHash` didn't implement its own documented
  "supersedes on the VT axis" contract (MEDIUM, query correctness unaffected — `VerifyNodeChain`
  doesn't fail).** `temporal_cascade.go:30-33` (doc) vs `:202-205,403-406` (impl derives `PrevHash`
  from "most-recent-non-eclipsed template," not true VT lineage). Zero test coverage.

  **Investigation.** Confirmed the implementation: `prevHash := template.Integrity().Hash`, where
  `template` is selected purely as "the most-recent-non-eclipsed version" (chosen for label/property
  carry-over, a DIFFERENT concern from VT-axis lineage) — not derived from any positional or temporal
  "what does this row supersede" computation. Traced WHY this doesn't break verification:
  `verifyChainLinkage` (`integrity.go:126-166`) only requires a non-genesis row's `PrevHash` to match
  SOME hash present anywhere in the SAME entity's full chain (`hashSet`) — never the
  immediately-preceding-by-version or VT-axis-adjacent row specifically. `template` is always a member
  of that chain by construction, so linkage verification can never fail on this choice, confirming the
  "query correctness unaffected" framing precisely.

  **Why doc-fix, not behavior-fix.** Deriving a TRUE VT-axis predecessor at write time would need new
  logic in the exact file where BACKLOG 10b already proved three independent fix attempts (targeting
  the READ-time positional-bounds derivation, a related but different concern) all broke in non-obvious
  multi-cascade ways caught only by the bitemporal oracle fuzz harness — not by either hand-written
  repro case. Given the change carries that proven risk class and delivers no query-correctness
  benefit (verification already tolerates the current choice), the appropriate fix is documentation
  accuracy: the file's header comment was rewritten to state precisely what `PrevHash` links to
  (the template row, not VT-axis lineage), explain why that's safe (citing `verifyChainLinkage`'s
  actual contract), and explicitly warn future changes against "fixing" this without re-running the
  full oracle harness at full iteration count.

  **Test** (`internal/core/cascade_prevhash_test.go`, new file — also closes 10i's "zero test
  coverage" gap): `TestCascade_MidHistoryInsertion_PrevHashLinksToTemplate` extends the existing
  `TestCascade_MidHistoryInsertion` scenario (two-tile timeline, mid-history insertion) to capture the
  current row's hash immediately before the cascade (the row the cascade will pick as `template`,
  since the inserted interval doesn't touch it), then asserts the newly-inserted row's `PrevHash`
  equals that captured hash, AND that `VerifyNodeChain` still returns `(true, nil)` afterward.

  **Verification.** No RED→GREEN cycle applies (pure documentation fix, zero behavior change — the
  test passes identically with or without the comment edit, since it pins EXISTING, unchanged
  behavior). As extra diligence given the file's proven sensitivity, ran the full bitemporal oracle
  fuzz harness at full iteration count (`go test ./pkg/graph/internal/core/... -run
  'TestBitemporalOracle|TestCascade'`) — all seeds pass, confirming the comment-only edit disturbed
  nothing. Full `go build ./...` and `go vet ./...` clean. `go test ./pkg/graph/internal/core/...` full
  package pass. Full-repo `go test ./...` clean — no regressions.
- **10f. [FIXED for `migration_bitemporal.go`; `temporal.go`'s mirror DELIBERATELY left unchanged —
  see rationale] Inherited-`ValidFrom` detection compares array-adjacent chain entries, not
  version-adjacent ones — can misfire after history truncation on legacy pre-migration entities
  (MEDIUM, low-likelihood trigger).** `internal/core/migration_bitemporal.go:100-131,159-188`,
  `temporal.go:408-414,536-541`.

  **Bug (migration_bitemporal.go).** `migrateNodeHistoryClearInheritedValidFrom`/
  `migrateRelHistoryClearInheritedValidFrom` build `chain` as `hist` (from `GetNodeHistory`/
  `GetRelHistory`, ascending by VERSION) + `current` appended last, then compare `chain[i]` against
  `chain[i-1]` — array-adjacent. After `TruncateNodeHistory`/`CompactHistoryNodes` removes middle
  versions, the returned history has GAPS, so an array-adjacent pair is not necessarily the entity's
  TRUE version-adjacent pair. A coincidental `ValidFrom` match between two INDEPENDENTLY,
  GENUINELY-asserted versions that became array-neighbors only because the true intervening version was
  trimmed away would be misread as "inherited" and WRONGLY CLEARED — a real, caller-supplied
  `ValidFrom` silently lost.

  **Fix.** Added a version-adjacency guard (`if v.Version() != prev.Version()+1 { continue }`) to both
  loops, immediately before the `ValidFrom` comparison — the doc comment's own algorithm description
  ("clear ValidFrom IF it equals the IMMEDIATELY-PRECEDING VERSION's ValidFrom") already specified this
  intent; the implementation just never enforced it. The guard is monotonically conservative: it can
  only leave a value un-cleared when uncertain, never wrongly clear one.

  **Why `temporal.go`'s `nodeInheritedValidFrom`/`relInheritedValidFrom` were deliberately NOT given the
  same fix.** These are the LIVE READ-PATH mirror (gated behind `!c.bitemporalMigrated`, so only active
  for legacy pre-migration data), but their `chain` is sorted by EFFECTIVE-VALID-FROM (via
  `sortNodeChainForResolve`/`sortRelChainForResolve`), not by version — array-adjacency there is
  ALREADY semantically "the version immediately preceding this one IN VALID-TIME order," which is
  arguably the CORRECT neighbor for this heuristic's actual purpose (was this value blindly copied
  forward from whatever version the resolver would have considered adjacent at the time). Applying the
  same "`Version() == prev.Version()+1`" guard there would ALSO suppress the heuristic for ordinary,
  NON-truncated cascade-affected chains (a cascade's mid-history insertion routinely creates versions
  whose valid-from order differs from version-number order — see `TestCascade_MidHistoryInsertion`),
  which is a materially different and untested tradeoff: fewer false positives (good) traded for more
  false negatives on legitimate legacy-artifact detection (a regression in the heuristic's actual job).
  Given this function sits in the exact subsystem where BACKLOG 10b's three independent fix attempts
  each broke in non-obvious multi-cascade ways caught only by the bitemporal oracle fuzz harness, and
  given this half is a low-risk/low-likelihood-trigger LEGACY-ONLY compatibility shim (not the
  correctness-critical primary path — once migrated, it's dead code), the read-path mirror is left
  unchanged pending a dedicated investigation into whether valid-from-order or version-order adjacency
  is actually correct for it — a judgment call, not a mechanical port of the migration fix.

  **Test** (`internal/core/migration_bitemporal_test.go`):
  `TestBitemporalMigration_TruncationGapDoesNotMisfire` — v0 (Version=0, ValidFrom=5000) with v1
  deliberately absent (simulating truncation), current v2 (Version=2, ValidFrom=5000 — coincidentally
  equal, NOT copied from v0) with `UpdatedAt` set; asserts the migration preserves v2's `ValidFrom`
  rather than wrongly clearing it, alongside the pre-existing
  `TestBitemporalMigration_ClearsInheritedValidFrom` (genuine version-adjacent inheritance, still
  correctly cleared) and `TestBitemporalMigration_PreservesExplicitValidFrom` (differing values, never
  touched) as non-regression coverage.

  **Verification.** RED confirmed via `git stash push -- internal/core/migration_bitemporal.go`: the
  new test failed with `post-migration ValidFrom = 0, want 5000` — the pre-fix code wrongly cleared the
  coincidentally-matching value. Stash popped, GREEN confirmed, plus all 4 pre-existing migration tests
  still pass. As extra diligence given this file's proximity to BACKLOG 10b's proven fragility, ran the
  full bitemporal oracle fuzz harness at full iteration count
  (`go test ./pkg/graph/internal/core/... -run 'TestBitemporalOracle|TestBitemporalMigration|TestCascade'`)
  — all seeds pass. Full `go build ./...` and `go vet ./...` clean. `go test
  ./pkg/graph/internal/core/...` full package pass. Full-repo `go test ./...` clean — no regressions.
- **10g. [FIXED — `pkg/graph/temporal_two_doors_rel_test.go`] Rule-17 cross-door equivalence test
  (`TestTemporalTwoDoorsAgreeOnLabelQueries`) covered nodes only, not relationship types — and the two
  implementations are structurally asymmetric (MEDIUM).** `RelsByTypeAt` thinly delegates to the
  generic door; `nodesByLabelAtLocked` is a *separate* hand-written implementation duplicating
  candidate-gathering + B4-prune + resolve — exactly the lessons 42/58 drift shape, guarded only on the
  node side.

  **Fix.** Added `TestTemporalTwoDoorsAgreeOnTypeQueries`, the direct relationship-type mirror of
  `TestTemporalTwoDoorsAgreeOnLabelQueries`: same adversarial dataset shape (explicit closed interval,
  explicit open interval, no world-time assertion / snowflake fallback, a deleted-but-history-queryable
  entity), same three doors compared (`RelsByTypeAt` the named door, `Rels().ByType` the generic
  `QueryOpts` door, and a per-ID `RelAt` resolver walk), same point-in-time + overlap-interval +
  Allen-meets-touching-interval assertions, run against both memory and badger backends. One structural
  adaptation: the node test's scenario D ("label held on the genesis version, removed on the current
  version") has no relationship analog — grep confirmed no `AddType`/`RemoveType` door exists for
  relationships (type is single and immutable after creation, unlike a node's multi-label set) — so D
  is omitted with an explanatory comment; A/B/C/E carry over directly.

  **Verification.** The new test passes on first run against both backends (and repeated runs,
  `-count=3`), confirming `RelsByTypeAt` and `Rels().ByType` currently agree exactly on this
  adversarial dataset despite their structural implementation asymmetry — this closes the TEST-GAP
  (guarding the pair against FUTURE lesson-42/58-style drift) rather than fixing an active divergence;
  the finding's own framing ("guarded only on the node side") was about missing coverage, not a proven
  bug. `go test ./pkg/graph/...` full package pass. Full-repo `go test ./...` clean — no regressions.
- **10h. B4 envelope prune wiring inconsistent across ByLabel/ByType-with-property doors; `ByType`
  structurally cannot get it at all (LOW, perf + Node/Rel asymmetry — see BACKLOG 21).**
  `temporal_queries.go:145-149,894-898` (pruned) vs `:784-822,973-1011,1333-1373` (not pruned). Root
  cause: `store.TemporalCandidateCapability.PruneTemporalCandidates` is typed to `types.NodeID` only.
- **10i. [FIXED — closed together with 10e, see `internal/core/cascade_prevhash_test.go`] No test for
  cascade `PrevHash` semantics (TEST-GAP, direct consequence of 10e).**
- **10j. No test races concurrent `TagAsOf` writes to the *same* tag name (TEST-GAP, code inspection
  shows it's safe in practice — global `asofMu` serializes).**
- **10k. [FIXED — closed together with 10f, see `TestBitemporalMigration_TruncationGapDoesNotMisfire`]
  Migration test suite lacked a truncation + inherited-ValidFrom combination case (TEST-GAP, tied to
  10f).**
- **10l. `RangeCardinality`'s ordered/prefix-scan sibling doors don't independently test the
  `TxPin`-conflict validation path (TEST-GAP, low risk — code path is identical to tested doors).**
- **10m. `resolveNodeVersionAt`/`resolveRelVersionAt` fast path is O(n) linear reverse-scan despite
  the chain being provably sorted/non-overlapping at that point — could be O(log n) via
  `sort.Search` (MEDIUM, perf).** `temporal.go:167-207,419-451`.
- **10n. "Single resolution seam" is a call-graph guarantee, not a file-boundary — worth stating
  explicitly so future lesson-17/58 audits know to check `temporal.go`/`temporal_cascade.go` too, not
  just `chain_resolver.go` (LOW, organizational).**
- **10o. `Diff`/`DiffCallback` acquires `c.mu.RLock` per entity, not one atomic snapshot — honestly
  disclosed accepted tradeoff, but the exposure window under concurrent writes is untested
  (LOW/informational).**

### BACKLOG 11 — Batch / ingest / tx concurrency hardening

- **11a. [FIXED — `internal/core/tx.go`, `tx_getnode_closed_test.go`] `GraphTx.GetNode`/`GetRelationship`
  bypass the tx-mirror locking contract every other tx accessor uses — a Path-B (v4.1.0) refactor miss
  (MEDIUM-HIGH).** `internal/core/tx.go` (formerly cited as `:985-1018` before other fixes in this pass
  shifted line numbers).

  **Bug**: both methods used bare `tx.lockActive()` (only `tx.mu`, the tx-local mutex) instead of
  `lockActiveCore()` (`tx.mu` + `c.mu.RLock()` + a `closed` check) — the pattern every OTHER tx read
  mirror (`Export`, `Snapshot`, `VerifyShard`, `Labels`, ...) uses — then called the lock-free
  `getCurrentNode`/`getCurrentRelationship` directly. Since a v4.1.0 Path-B tx does NOT hold `c.mu` for
  its whole lifetime (only per-call — `BeginTx`'s own doc comment: "acquires the tx-serialization mutex
  (`c.txMu`), NOT `c.mu.Lock`"), nothing prevented `Close()` from running fully (including closing the
  underlying store) while an open tx's `GetNode`/`GetRelationship` call was in flight or afterward — the
  stale doc comment ("Safe because the tx holds the write lock") was describing the PRE-v4.1.0 model,
  no longer true. The two methods' doc comments were the only place in the file still claiming it.

  **Fix**: both now use `lockActiveCore()`/`unlockActiveCore()`, exactly matching every sibling tx read
  method. Doc comments corrected to describe the actual v4.1.0 Path-B contract.

  **Tests** (`tx_getnode_closed_test.go`): `TestGraphTx_GetNode_AfterCloseReturnsErrGraphClosed` /
  `TestGraphTx_GetRelationship_AfterCloseReturnsErrGraphClosed` reproduce the logical half of the race
  deterministically (an internal `package core` test, so no synthetic goroutine timing is needed): begin
  a tx, `Close()` the graph out from under it WITHOUT committing/rolling back (`Close()` does not wait
  for open transactions — it only briefly takes `c.mu.Lock()` to tear down index providers), then call
  `GetNode`/`GetRelationship` and assert the SAME `ErrGraphClosed` contract every sibling tx read method
  already honors. `TestGraphTx_GetNode_StillWorksInsideOpenTx` is the non-regression counterpart, proving
  the fix doesn't break the ordinary still-open-tx read path.

  RED confirmed via `git stash push -- tx.go`: both tests failed with `graph: store already closed` (a
  raw, backend-specific error leaking through instead of the documented `ErrGraphClosed` contract) —
  proving the pre-fix code let the closed-store read through uncontrolled rather than gating it via
  `c.mu`/`closed`. Popped the stash, confirmed GREEN on all 3. Full `go build ./...` + `go vet ./...`
  clean; `go test ./pkg/graph/internal/core/...` clean; `go test -race ./pkg/graph/internal/core/...`
  clean (133s); full repo `go test ./...` clean.
- **11b. [FIXED — `internal/core/tx.go`, `internal/core/registry_rollback.go`,
  `internal/core/tx_rollback_test.go`] Tx-rollback registry de-allocation can silently corrupt an
  entity a concurrent writer already persisted (HIGH).** `tx.go:946-976`, `registry_rollback.go:86-153`,
  `ingest_concurrent.go:91-107`. A tx's newly-allocated label token is persisted *immediately* (before
  Commit); a concurrent writer can `Lookup` and persist an entity referencing it before the tx
  `Rollback()`s and unconditionally de-allocates the token — the already-persisted entity's label token
  now dangles, and the next distinct label name reuses the number, silently reassigning the entity's
  label.
  Fix: `GraphTx.restoreRegistries` no longer blindly `ImportNames`s the pre-tx snapshot. It now computes
  the names the tx itself newly registered (`newlyAllocatedNames`, the append-only registry tail beyond
  the pre-tx snapshot) and reclaims each only when NO current node/rel references its token — checked
  via the O(1) `NodeCountByLabel`/`RelCountByType` counters (`StatsCapability`, not a scan), under the
  SAME exclusive `c.mu.Lock` `Rollback` already holds. By the time this check runs, `Rollback`'s earlier
  steps have already deleted every entity the tx itself created (steps 1-7 precede `restoreRegistries`),
  so a nonzero count can only mean a genuinely concurrent writer adopted the token — never the tx's own
  now-deleted entity. A referenced token is left registered (the rare leaked registry slot is a
  strictly better failure mode than a silently mis-labeled entity); `RollbackNames` (an existing
  primitive, previously unused by this path) mutates the registry object in place under its own lock
  instead of the old swap-a-new-registry-and-replace-the-pointer dance, and also safely refuses (no
  mutation) if the registry structurally drifted beyond the tx's own allocation. New shared helpers
  `rollbackLabelsIfUnreferenced` / `rollbackRelTypesIfUnreferenced` / `anyTokenReferenced` live in
  `registry_rollback.go`.
  Scope decision: the fix is wired into `tx.go`'s `restoreRegistries` ONLY, not retrofitted into
  `registry_rollback.go`'s other direct `RollbackNames` call sites (`getOrCreateLabelPersisted`,
  `getOrCreateLabelsWithSnapshot`, `getOrCreateBatchNodeLabelsWithSnapshot`, `getOrCreateRelTypePersisted`,
  `restoreNewLabelsOnError`, `restoreNewRelTypeOnError`) — first wired identically everywhere, this
  broke `TestAddNodeLabel_CorruptFutureTokenRollsBackRegistry`, which seeds a store row carrying a raw
  label bit for a token number *before* that token is ever named (a corrupted/untrusted-store scenario),
  then triggers an `AddLabel` whose OWN entity mutation fails. The "referenced" check cannot distinguish
  a genuine concurrent adopter from a pre-existing corrupt row sharing the same freshly-minted token
  number without a "reference count immediately before this call's own allocation" baseline, which those
  other call sites don't capture. `GraphTx.Rollback` is different: its own-entity cleanup happens FIRST
  (see above), so the ambiguity provably cannot arise there. Reverted the other call sites back to their
  original unconditional-`RollbackNames` behavior and documented the caveat + scope decision inline on
  `rollbackLabelsIfUnreferenced`. `ingest_concurrent.go:91-107` (`concurrentTokensResolvable`) was
  audited and found NOT to be a de-allocation site at all — concurrent-mode ingest declare-on-prepare
  registers tokens unconditionally with no probe-restamp/rollback step (confirmed against CLAUDE.md's
  ADR-0007 description), so it needs no fix. Also audited the third `RollbackNames` call site,
  `apply_record.go`'s `refetchRegistriesLocked` (replica registry refetch) — safe by construction: it
  runs only on a read-only replica (no standalone writes possible) under exclusive `c.mu.Lock` for its
  whole duration, so no concurrent writer can be racing it; left unchanged.
  Added `TestGraphTx_RollbackDoesNotDeallocateLabelTokenAdoptedByConcurrentWriter`: a tx allocates a new
  label, a standalone `Add` (simulating the concurrent writer) adopts the same token before the tx rolls
  back, then asserts the token stays registered to the SAME number, the concurrent entity's label still
  resolves correctly, the tx's own entity is still cleaned up, and a subsequently-allocated distinct
  label gets a genuinely different token (no reuse). Confirmed RED against the pre-fix unconditional
  `RollbackNames` logic (temporarily reverted `restoreRegistries` to call `RollbackNames` directly): the
  token was de-allocated despite the concurrent reference, exactly as the bug predicts. GREEN after the
  fix. Full `go test ./...` and `go test -race ./pkg/graph/...` pass, including the pre-existing
  `TestReplicaConvergence_RolledBackNewTokenTx` (verified the change-log-enabled single-writer rollback
  path is unaffected — the tx's own now-deleted entity means the reference check still allows reclaim in
  that scenario, same as before) and the full `TestR9_*` registry-persistence suite.
- **11c. [FIXED — `internal/core/batch_execute.go`, `internal/core/batch_commit_log_scope_error_test.go`]
  `Batch.Execute` silently dropped a `CommitLogScope` failure when the batch already had a per-op
  failure (MEDIUM).** `batch_execute.go:586-594` — `if err != nil && result.Failed == 0` swallowed the
  log-commit error otherwise, defeating the change-log's replica-divergence-detection purpose exactly
  when it matters most.

  **Bug.** `CommitLogScope()`'s error was only appended to `result.Errors`/counted in `result.Failed`
  when `result.Failed == 0` at that point — i.e., only when NO per-op failure had already occurred. A
  batch with an earlier per-op failure (e.g. `UpdateNode` on a nonexistent ID) whose `CommitLogScope`
  ALSO then failed lost the log-commit error entirely: the caller saw only `ErrBatchFailed` with the
  original per-op error, with no signal that the change-log commit itself failed too — exactly the
  "committed-but-unlogged data, replica-divergence risk" scenario the surrounding comment already
  documents as the reason to surface it, silently dropped in precisely the case (a batch already
  degraded) where a caller would most need to know.

  **Fix.** Removed the `&& result.Failed == 0` guard — the `CommitLogScope` error is now always
  appended to `result.Errors` (as a `BatchError{Op: "commit-change-log"}`) and always increments
  `result.Failed`, regardless of any pre-existing per-op failures.

  **Tests** (`internal/core/batch_commit_log_scope_error_test.go`, new file): a
  `commitLogScopeFailingStore` wraps a change-log-enabled `badger.Store` (embedding it and overriding
  only `CommitLogScope`) so a test can force it to fail on demand — the plain type-assertion
  `store.(storepkg.TxChangeLogScope)` `internal/core` uses to resolve `c.txLogScope` has no
  backend-type restriction, so the wrapper is picked up correctly.
  `TestBatchExecute_CommitLogScopeErrorSurfacedAlongsidePerOpFailure` — a batch with one per-op failure
  (`UpdateNode` on a nonexistent ID) AND a forced `CommitLogScope` failure asserts BOTH errors appear
  in `result.Errors` and `result.Failed >= 2`.
  `TestBatchExecute_CommitLogScopeErrorSurfacedAlone` — the pre-existing-correct case (no per-op
  failure, only the log-scope failure) still works, pinned as a non-regression counterpart.

  **Verification.** RED confirmed via `git stash push -- internal/core/batch_execute.go`: the
  "alongside" test failed with `result.Errors = [batch UpdateNode...], missing the commit-change-log
  failure` — the "alone" test already passed pre-fix, confirming the bug was specifically the
  `&& result.Failed == 0` interaction, not the error-surfacing mechanism itself. Stash popped, GREEN
  confirmed for both. Full `go build ./...` and `go vet ./...` clean. `go test
  ./pkg/graph/internal/core/...` full package pass. `go test -race ./pkg/graph/internal/core/... -run
  'TestBatchExecute|TestBatch'` clean. Full-repo `go test ./...` clean — no regressions.
- **11d. [FIXED — `internal/core/batch_execute.go`, `internal/core/batch_rel_txfrom_shared_test.go`]
  `TxFrom` not stamped once per commit group for relationships, contradicting CLAUDE.md's documented
  invariant (LOW-MEDIUM).** `batch_execute.go:196` (nodes: shared `txNow`) vs `:397` (rels: computed
  per-rel inside the locked closure). No test asserted node/rel `TxFrom` equality within one batch.

  **Bug.** The node-creation phase computes `txNow := b.g.now()` ONCE, before the node loop, and shares
  it across every node. The relationship-creation phase instead called `txNow = b.g.now()` FRESH inside
  the per-rel locked closure — i.e. once per relationship, not once per batch. `c.now()` is a
  monotonic-floor counter (`context.go`: `next = max(observed, last+1)` — every call returns a value
  STRICTLY GREATER than the previous call's), so distinct relationships created in the SAME batch
  always got DIFFERENT (increasing) `TxFrom` stamps — directly contradicting CLAUDE.md's Ingest
  Pipeline invariant: "A whole commit group shares one `TxFrom`... `TxFrom` is strictly increasing
  ACROSS groups" (i.e. shared WITHIN a group, increasing only BETWEEN groups). Investigated whether
  the per-rel timestamp was intentional (tied to the per-rel endpoint-lock window, as the backlog's
  "may be intentional" caveat raised) — found no such rationale in the code or comments; the surrounding
  comment only ever discusses endpoint-hash freshness, never a TxFrom-per-rel design intent.

  **Fix.** Hoisted `relsTxNow := b.g.now()` to compute once, immediately before the `for _, pr := range
  b.rels` loop (mirroring the node phase exactly), and changed the per-rel closure to assign `txNow =
  relsTxNow` instead of calling `b.g.now()` again. Each rel's own `backfillTxFrom` override (§4.1
  privileged backfill) is still respected exactly as before — only the DEFAULT (non-backfilled) stamp
  source changed from "fresh per-rel" to "shared per-batch."

  **Test** (`internal/core/batch_rel_txfrom_shared_test.go`, new file):
  `TestBatchExecute_RelationshipsShareOneTxFromPerCommitGroup` — a batch creating 3 nodes and 5
  relationships (fanning out to 2 distinct end nodes, so multiple distinct rel rows are genuinely
  created), executed, then every relationship's persisted `TxFrom` is fetched and asserted equal to the
  first relationship's `TxFrom`.

  **Verification.** RED confirmed via `git stash push -- internal/core/batch_execute.go`: the test
  failed with `rel[1] TxFrom = 1784495922774, want 1784495922773` — an exact 1ms-apart pair, matching
  the monotonic-floor counter's per-call increment precisely. Stash popped, GREEN confirmed. Full
  `go build ./...` and `go vet ./...` clean. `go test ./pkg/graph/internal/core/...` full package pass.
  `go test -race ./pkg/graph/internal/core/... -run 'TestBatchExecute|TestBatch'` clean. Full-repo
  `go test ./...` clean — no regressions.
- **11e. `ingestFailureCap`/`failureDrops` eviction path (8192-entry oldest-first, O(n) per insert at
  capacity) is untested under load or concurrency (TEST-GAP).** `ingest.go:344-366`.
- **11f. Change-log-enabled tx mutations take the FULL 32-stripe writer lock per call, not just per
  commit — a throughput cliff (LOW-MEDIUM, documented/intentional, undocumented in the ingest-pipeline
  docs).** `tx.go:387-416`. Fully defeats ADR-0007's `RLockShard` striping win for the lifetime of any
  change-log-enabled tx actively mutating, concurrent with Lanes:N ingest.
- **11g. [FIXED — `internal/core/tx.go`, `subapi.go`] Multiple Path-B-era comments still described the
  pre-4.1.0 "tx holds write lock" model, directly feeding 11a's bug pattern (LOW, code smell).**
  `tx.go:71-72`, `subapi.go:26-27`.

  **Fix.** Rewrote `GraphTx`'s type doc comment (`tx.go`) and `TxAPI.Begin`'s doc comment (`subapi.go`)
  to describe the actual v4.1.0 Path-B contract: `c.txMu` is held for the tx's ENTIRE lifetime
  (serializing tx-vs-tx and tx-vs-batch), while `c.mu` (the graph write lock) is taken only BRIEFLY
  per-call around each method's own body — so a leaked/un-rolled-back `GraphTx` deadlocks subsequent
  transactions and batches, but concurrent standalone mutations and reads from other goroutines are
  unaffected. Pure documentation fix, zero behavior change — this is exactly the stale-comment class
  that fed 11a's actual bug (bare `lockActive()` instead of `lockActiveCore()`), so closing it removes
  a landmine for future refactors reading these comments as ground truth. `go build ./...`, `go vet
  ./...`, and `go test ./pkg/graph/...` clean.
- **11h. `TestOutgoingIncomingForNodesAtTx_RandomizedDivergenceProbe/badger` is intermittently flaky
  under full-suite load (MEDIUM, discovered during BACKLOG 12c's verification run, not yet reproduced
  in isolation).** `internal/core/adjacency_at_tx_test.go:402-554`. One failure observed in a full
  `go test ./pkg/graph/internal/core/...` run (`got 6 node entries ... want 5 entries` — an EXTRA
  relationship visible in the actual result that the reference model says should not be, at one of the
  test's bitemporal pins); 5 immediate reruns of the isolated test all passed. Despite the FIXED
  `rand.NewSource(42)` seed (the data sequence itself is deterministic), the test's own comment already
  documents a known two-clock hazard: mutation stamps are minted via the monotonic-floor `c.now())`,
  while `OutgoingForNodesAtTx`'s "TX-only door" valid-time coverage check probes at WALL now
  (`resolveOpenEndInstant`) — the EXISTING `waitWallPast(pinD)` mitigation waits once, before setup
  ends, until the wall clock passes the LAST logical-clock stamp minted, but does not account for
  `resolveOpenEndInstant` re-probing wall-now AGAIN at EACH query in the loop below (6 pins × 2
  directions × 3 type filters = 36 queries) — under heavy CI/full-suite load (goroutine scheduling
  delays between the wait and a LATER query in the loop), a query's own "wall now" probe could still
  race against something time-dependent in a way the one-time wait doesn't fully close. Needs a
  dedicated minimal repro under artificial scheduling delay (e.g. inserting a controlled sleep between
  `waitWallPast` and the query loop, or between individual queries) before a fix is designed — not
  fixed here to avoid a blind patch to unfamiliar bitemporal clock-skew logic.

### BACKLOG 12 — Replication / import / export hardening

- **12a. [FIXED — internal/core/admin.go, apply_record.go, apply_change_clear_test.go] `ChangeClear` apply omits every Core-level reap the primary's `Reset()` performs — replica
  divergence (CRITICAL).** `internal/core/apply_record.go:97-98` vs `admin.go:240-271`. Only
  `store.Clear()` is applied on the replica; `Reset()` also clears `asOfColumns`, unique constraints/
  owners, compaction/retention watermarks, and op counters — all Core-level in-memory state
  `store.Clear()` cannot touch. A read-only replica can return columnar as-of answers computed against
  data `Clear()` just wiped, or under/over-enforce reset unique constraints. Existing test only checks
  counts+LSN. Fix: factor the reap sequence into a shared helper called by both `Reset()` and the
  `ChangeClear` apply path.
- **12b. [FIXED — `internal/core/import_merge.go`, `pkg/graph/delta_merge_specialtags_test.go`] Delta
  merge (`captureMergeRecord`) cannot replay `ChangeForeignIncoming`/`ChangeForeignIncomingDelete`/
  `ChangeRangePurge`, but `ExportSince` emits them (HIGH).** `internal/core/import_merge.go:261-371` vs
  `export_delta.go:150-172`. Fell to `default: ErrCorruptExport` (misdiagnosed, not silent) — a hard
  availability bug for any tenant combining retention purge or foreign-incoming stores with delta
  backups.
  Fix: added `captureMergeRecord` cases for all three tags, applying `applyChangeRecordLocked`'s own
  reuse of these records (each tag's apply handler is documented idempotent) rather than a generic
  snapshot-and-restore:
  - `ChangeRangePurge` carries no per-entity rows — it names a PREDICATE (label + before-instant) the
    target re-executes, deriving whatever entity set currently matches (ADR-0008 R3). "Capture the
    touched entities for rollback" is not just hard here, it is actively WRONG: retention purge exists
    to durably remove data, so snapshotting the about-to-be-purged rows into `mergeRollback`'s in-memory
    buffers — even temporarily, even if never actually restored — would undermine the exact
    retention/compliance guarantee the primary's purge made. The fix validates the payload decodes (the
    untrusted-stream check every case performs) and deliberately captures NOTHING;
    `applyRangePurgeLocked` is documented idempotent, so a merge retried after an unrelated later-record
    failure simply re-executes the same predicate.
  - `ChangeForeignIncoming` / `ChangeForeignIncomingDelete` (ADR-0010 Model A) carry a rel-ID that
    deliberately belongs to a FOREIGN slot — the whole point of the stub is that it is written
    co-located on the END node's shard, reachable only via that shard's adjacency fold, never a
    slot-routed point read (`ForeignIncomingRelCapability`'s own doc). The generic single-entity
    machinery (`mrb.captureRel`, and the final pass's `verifyRelChainLocked`) both do a plain
    slot-routed `getCurrentRelationship`, which fails closed with `ErrSlotNotLocal` — not
    `ErrRelNotFound` — for exactly this rel-ID; discovered when the first fix attempt (reusing
    `mrb.captureRel`) turned the misdiagnosed-`ErrCorruptExport` bug into a *different* failure
    (`ErrSlotNotLocal`) instead of actually fixing it. Building a bespoke foreign-slot-aware
    read/restore path (reading via `c.Rels.Incoming(endID, ...)`, teaching `mergeRollback`'s rel
    restoration to route foreign-slot IDs through the capability instead of `PutRelationship`/
    `DeleteRelationship`) was assessed as a substantially bigger, higher-risk lift than this pass
    should take on blind — mirroring the BACKLOG 10b lesson (back off from a fix that isn't cleanly
    correct rather than force it through). Both `RecordForeignIncoming` and `DeleteForeignIncoming` are
    documented idempotent ("an already-present/-absent stub is a no-op"/"not an error"), so — exactly
    like the range-purge case — the fix validates the payload decodes and captures nothing; a merge
    retried after an unrelated later-record failure simply re-applies the put/delete.
  Net effect: all three tags now merge successfully (closing the availability bug) with a full,
  correctly-scoped merge-rollback story for every OTHER tag; the residual gap is narrow and documented
  inline — if a merge batch contains one of these three tags AND a later record in the same batch fails,
  rollback does not reverse this tag's own effect (it stays applied) while everything else in the batch
  rolls back to pre-merge state. Given each tag's apply-side idempotency, a caller can always retry the
  merge from the same base cursor to reach a fully consistent state; this is a documented limitation, not
  silent data loss or corruption.
  Added `TestDeltaMerge_ForeignIncomingPutAndDeleteMerges` (sharded end-machine store, records a foreign
  edge, delta-merges the put onto a fresh sharded target, verifies the stub is visible via
  `Incoming(end, "KNOWS")`, then deletes the end node and delta-merges the resulting
  `ChangeForeignIncomingDelete`, verifying the node is gone on the target) and
  `TestDeltaMerge_RangePurgeMerges` (mirrors `TestRetentionPurge_ReplicaConvergence` but through
  `ExportSince`/`ImportMerge` instead of `ApplyChanges`, verifying the purge predicate re-executes on
  the merge target and its retention watermark advances). Confirmed RED against the pre-fix `default:`
  branch (both tests failed with `ErrCorruptExport: unknown change tag N`, exactly as the bug predicts);
  GREEN after the fix. Full `go test ./...` and `go test -race ./pkg/graph/ ./pkg/graph/internal/core/...
  ./pkg/graph/store/sharded/...` pass.
- **12c. [FIXED — `internal/core/import.go`, `import_watermark_monotonic_test.go`] Bootstrap watermark
  write has no monotonicity/idempotency guard (MEDIUM-HIGH).** `internal/core/import.go:589-596`.

  **Bug**: the bootstrap handoff (`Import` recording a snapshot's `SnapshotLSN` as the replica's initial
  applied-LSN watermark) was a raw, unconditional overwrite — unlike `ApplyChange`/`ApplyChanges`, which
  both ENFORCE "a record at or below the current watermark is a no-op" specifically so a buggy, duplicate,
  or out-of-order delivery can never regress the replica. Re-importing an older (or the same) snapshot
  onto an already-tailing replica (whose watermark had already advanced past `SnapshotLSN` via prior
  `ApplyChange` calls) would regress the watermark backward. The replica would then re-tail forward from
  the regressed point, re-applying already-seen records; any entity DELETED after the older snapshot's
  LSN would be reintroduced by the re-import's own row data (which reflects state as of the older,
  pre-delete LSN) and stay live until the re-tail caught back up to the delete's LSN — a genuine, if
  transient, "resurrected" entity.

  **Fix**: mirrors `ApplyChange`'s pattern exactly — read the current watermark via `appliedLSNLocked()`
  and only flush + advance it when `header.SnapshotLSN > current`. A same-or-older snapshot's LSN now
  leaves the existing (higher) watermark untouched, exactly like a stale/duplicate change-log record is
  already a no-op.

  **Tests** (`import_watermark_monotonic_test.go`): `TestImport_BootstrapWatermarkDoesNotRegress` seeds a
  destination graph's watermark to 100 (simulating prior `ApplyChange` tailing), then imports a minimal,
  valid snapshot stream (mirroring the existing `TestImport_HeaderVersionRange`'s construction) whose
  header carries `SnapshotLSN: 50`, and asserts `AppliedLSN()` is STILL 100 afterward.
  `TestImport_BootstrapWatermarkAdvancesWhenNewer` is the non-regression counterpart — a genuinely newer
  `SnapshotLSN: 100` (watermark seeded to 50) must still advance the watermark, proving the fix isn't a
  blanket no-op.

  RED confirmed via `git stash push -- import.go`: the regression test failed with `AppliedLSN after
  import = 50, want 100` — the watermark regressed exactly as the finding describes — while the
  "advances when newer" test correctly stayed green (that behavior was never broken). Popped the stash,
  confirmed GREEN on both. Full `go build ./...` + `go vet ./...` clean;
  `go test ./pkg/graph/internal/core/...` clean; `go test -race ./pkg/graph/internal/core/...` clean
  (132s); full repo `go test ./...` clean. (A pre-existing, unrelated flaky test —
  `TestOutgoingIncomingForNodesAtTx_RandomizedDivergenceProbe/badger` — surfaced once during this
  verification pass; logged separately as BACKLOG 11h, not caused by this fix.)
- **12d. Hash verification skipped whenever the wire carries an entirely-empty integrity block
  (LOW-MEDIUM, documented tradeoff, worth hardening at the untrusted boundary specifically).**
  `import.go:269-301`. A forged/corrupted record with all integrity fields blanked sails through
  unverified.
- **12e. `applyForeignIncomingLocked` missing `noteAppliedTxFrom`, unlike every sibling put/version
  handler (LOW, harmless today, unexplained family asymmetry).** `apply_record.go:404-425`.
- **12f. `noteAppliedTxFrom` runs before hash/property verification in all 4 handlers — over-
  invalidation only, semantically backwards (LOW).** `apply_record.go` (4 sites).
- **12g. [FIXED — `internal/core/apply_change_fuzz_test.go`] No fuzz target for the replica-apply/
  delta-merge decode path (`applyChangeRecordLocked`'s decoders), unlike `Import`'s `FuzzImport`
  (MEDIUM, same untrusted-stream boundary lesson 47 found real bugs on).**

  **Fix.** Added `FuzzApplyChange`, modeled directly on `FuzzImport`'s structure: fuzzes
  `(tag byte, payload []byte)` pairs through `ReplOps.ApplyChange` (which wraps a
  `storepkg.ChangeRecord{LSN, Tag, Payload}` and drives the SAME `applyChangeRecordLocked` decode path
  a primary's change-log feed would over the wire) against a fresh change-log-enabled badger-backed
  graph each iteration. Same contract as `FuzzImport`/lesson 47: apply of ANY `(tag, payload)` must
  NEVER panic or crash the process — it either returns an error (rejected) or applies cleanly, and a
  successful apply must leave the graph self-consistent enough to `Export` without error.

  **Seed corpus** (`fuzzApplyChangeSeedRecords`): captures REAL committed change-log records from a
  change-log-enabled graph exercising a varied mutation sequence (node/rel create, two updates for
  history, label add/remove, rel delete, node delete) via `g.Repl.ChangeFeed(0, 0)` — giving the fuzzer
  structurally realistic starting points for the easily-reachable tags (`ChangeNodePut`, `ChangeRelPut`,
  `ChangeNodeDelete`, `ChangeRelDelete`, `ChangeNodeHistoryVersion`), mirroring `fuzzImportSeedStreams`'
  captured-real-export-stream approach. Supplemented with explicit raw adversarial seeds for every one
  of the 13 valid `ChangeTag` values (empty payload, a single msgpack-nil byte, 4 bytes of `0xff`) plus
  a few invalid tag bytes (`0`, `99`, `255`) — mirroring `FuzzImport`'s truncated/garbage-framing seed
  style for tags the mutation sequence doesn't naturally reach (`ChangeMeta`, `ChangeClear`,
  `ChangeForeignIncoming(Delete)`, `ChangeRangePurge`, history-truncate).

  **Verification.** All 42 seed cases pass under plain `go test`. Ran active fuzzing
  (`go test ./pkg/graph/internal/core/... -run '^$' -fuzz '^FuzzApplyChange$' -fuzztime=60s`) — ~140,000
  executions across 16 workers, 140 interesting corpus entries discovered, zero crashes/panics found,
  confirming the decode path already correctly routes through `storeutil.SafeUnmarshal` end-to-end for
  every reachable tag. No stray corpus artifacts were written to the repo tree (checked `git status`).
  This is a pure test-addition (no production code changed, since nothing crashed), so there is no
  RED→GREEN cycle — the fuzz target itself is the deliverable, closing the "same untrusted-stream
  boundary, no fuzz coverage" gap. Full `go build ./...` and `go vet ./...` clean. `go test
  ./pkg/graph/internal/core/...` full package pass (seed corpus included). Full-repo `go test ./...`
  clean — no regressions.
- **12h. [FIXED — already closed by 12a's own test, `internal/core/apply_change_clear_test.go`]
  `TestReplicaApply_ChangeClearWipesAndReanchors` doesn't assert the Core-level state 12a describes
  (TEST-GAP, direct cause of 12a shipping).** Verified: 12a's fix added a DEDICATED new test,
  `TestApplyChangeRecord_ChangeClearReapsCoreStateLikeReset`, which exercises and asserts every
  Core-level reap target the finding lists (op counters, unique-constraint definitions, UniqueForever
  ownership claims, named as-of tags, the as-of DocValues cache, compaction/retention watermarks) —
  rather than patching the older, narrower `TestReplicaApply_ChangeClearWipesAndReanchors` (which still
  only checks counts+LSN, but the underlying gap this finding describes — "no test asserts the
  Core-level state" — no longer exists in the suite). No further work needed.
- **12i. `strictCheckMergeRecord` (Strict mode) only covers 4 of 8+ mergeable tag kinds — missing
  history-version/truncate tags (LOW, not a correctness bug, incomplete contract).**
  `import_merge.go:409-449`.
- **12j. `applyNodeLabelChangeLocked`'s fail-closed guard rests on an undocumented wire-format
  assumption — single-token label diffs only (informational, correct today).** `apply_record.go:335-362`.
- **12k. `readExportRecord` allocates directly from a length header — the lesson-48 anti-pattern, but
  not currently reachable from untrusted input (informational, landmine for a future refactor).**
  `export.go:464-478`.
- **12l. `ChangeClear` doc ("replica applying it must clear its own state") vs implementation mismatch
  — ties directly to 12a (informational).**

### BACKLOG 13 — Retention / compaction / admin hardening

- **13a. [FIXED — `internal/core/retention_purge.go`, `pkg/graph/retention_purge_midrange_abort_test.go`]
  `purgeRangeAllChunks` drops `UniqueForever` reap on any mid-range abort (ctx cancel or chunk error) —
  permanent ghost owners, NOT fixed by retry (HIGH).** `internal/core/retention_purge.go:177-209`. `reap`
  only ran after the loop exited normally; both early-return paths skipped it even though prior chunks
  had already committed deletions. Purged IDs are gone from the store and never reappear in a later chunk
  result, so retry could never recapture them. Affected both `PurgeExpiredNodes` and the replica apply
  path.
  Fix: `purgeRangeAllChunks` now uses named returns (`report PurgeReport, err error`) with a `defer` that
  ALWAYS calls `reapForeverOwnersForPurged(reap)` regardless of exit path — normal completion, a ctx
  cancellation, or a chunk error. If the reap itself also fails, its error is appended to (not silently
  dropped in favor of) the original error via `%w (additionally failed to reap...)`; if the original path
  succeeded but the reap fails, the reap's error becomes the returned error. `reapForeverOwnersForPurged`
  was already a documented no-op for a nil/empty `purged` map, so the defer is zero-cost on the
  no-forever-constraints / clean-exit path.
  Added `TestPurgeRangeAllChunks_ReapsForeverOwnersOnMidRangeAbort`: a `UniqueForever` constraint on
  `User.email`, 300 owner nodes (> one 256-row chunk), and a wrapper store
  (`failSecondPurgeChunkStore`) that lets the FIRST `PurgeNodesByLabelBefore` chunk commit for real (so
  ~256 nodes are actually deleted) then fails the SECOND call — reproducing "prior chunks already
  committed deletions, then abort" exactly. Asserts `PurgeExpiredNodes` itself returns the injected
  error (not success) AND that the first purged owner's email is reusable afterward (proving the reap
  ran for the already-purged owners despite the overall call failing). Confirmed RED against the pre-fix
  code: the re-add failed with `ErrUniqueViolation ... permanently owned by node N (UniqueForever)` —
  the exact ghost-owner symptom the finding describes. GREEN after the fix. Full `go test ./...` passes;
  not concurrency-sensitive (single-goroutine defer/error-flow change), so `-race` was not required.
  **Follow-up (discovered during BACKLOG 18b's full-suite verification, same PR batch): the test itself
  was flaky** — it asserted `emails[0]` (the FIRST-created user) was reusable, on the comment claim that
  "creation-order == the first chunk's purge order — both iterate the label index in ascending node-ID
  order." That claim is false: `memorystore_retention_purge.go`'s `purgeNodesByLabel` explicitly selects
  victims by ranging `ms.labelIdx[labelToken]`, a Go map, and documents "Map order is random — fine: the
  purge is order-independent" — so which ~256-node subset lands in the FIRST chunk is nondeterministic
  across runs, and `emails[0]` was only sometimes among them. `go test -run
  TestPurgeRangeAllChunks_ReapsForeverOwnersOnMidRangeAbort -count=10` reproduced ~50% failures on the
  unfixed test. Fixed by having `failSecondPurgeChunkStore` record the first (successful) chunk's
  `RetentionPurgeResult.PurgedNodeIDs` and asserting reusability for EXACTLY the nodes that call reports
  purged (looked up via an `emailByID map[types.NodeID]string` built at creation time), not an assumed
  index. `go test -count=30` now passes 30/30 (previously flaky ~50%).
- **13b. [FIXED — `internal/core/retention.go`, `pkg/graph/retention_purge_reset_test.go`] `Admin.Reset`
  leaves per-label retention watermarks un-reaped while preserving the label-token registry — cross-label
  false-positive `ErrRetentionExpired` (HIGH).** `internal/core/retention.go:139-149` vs
  `admin.go:237-239`. The reap comment wrongly borrowed the compaction-stub "never-reused token"
  rationale, but retention watermarks are keyed by label token, which `Reset` explicitly preserves. A
  stale per-label watermark + a new graph-max from an unrelated purged label produced a genuine
  false-positive rejection for entities of a label that was *never* purged post-reset.
  Fix: `reapRetentionForReset` now also clears every CURRENTLY REGISTERED label token's
  `retention_watermark/<token>` MetaKV key (iterating token numbers `1..c.labels.Len()`) via
  `MetaSet(key, nil)` — the same "empty value = unset" convention `retentionWatermarkForLabel` already
  treats identically to an absent key, so this is a safe unset on every in-tree backend, not a hard
  delete requiring a new capability. No new persisted tracking structure needed ("track an in-memory set
  of ever-watermarked labels", the backlog's alternative suggestion): `MetaKVCapability` has no
  key-enumeration primitive, but the label registry is ALREADY the authoritative, capacity-bounded
  (<=65535) enumeration of every token that could possibly hold a watermark key, so iterating it directly
  is simpler and needs no new durable state. Also rewrote the misapplied doc comment (13k) to explain the
  actual compaction-vs-retention distinction: a compaction stub is keyed by ENTITY ID (Reset destroys
  every entity, so a stale stub is naturally orphaned and never looked up again — snowflake IDs are never
  reused), but a retention watermark is keyed by LABEL TOKEN, and Reset deliberately preserves the label
  registry (tokens ARE reused across a reset).
  Added `TestRetentionReset_DoesNotLeakStalePerLabelWatermarkAcrossReset`: purges label "Foo" pre-reset
  (stamping a huge per-label watermark), `Reset()`s, creates a brand-new post-reset "Foo" node, purges an
  UNRELATED label "Bar" with a small boundary (reactivating the graph-max gate without purging anything
  Foo-related), then does a point-in-tx-time `NodeAsOf` read on the new Foo node pinned below both
  boundaries. Confirmed RED against the pre-fix code: the read returned `ErrRetentionExpired` — the
  exact cross-label false positive the finding describes, with Foo's stale pre-reset watermark
  (`farFuture`) leaking across the reset. GREEN after the fix (the natural `ErrNoVersionAsOf` — the node
  legitimately did not exist at that pin — is returned instead). Full `go test ./...` passes; not
  concurrency-sensitive (Reset already runs under `c.mu.Lock` exclusively), so `-race` was not required.
  Resolves 13k (misapplied doc rationale) as a byproduct — same fix, same file.
- **13c. `CompactHistoryNodes`/`Rels` hold the exclusive graph lock across a full O(n) whole-graph
  scan+write, contradicting retention purge's own documented chunked-lock discipline (MEDIUM).**
  `internal/core/compaction.go:518-599,601-681`. No chunking, no periodic lock release, no `Label`
  scoping — always the whole population.
- **13d. [FIXED — `internal/core/core.go`, `internal/core/admin.go`, `pkg/graph/errors.go`,
  `internal/core/admin_direct_test.go`, `pkg/graph/errors_doc_test.go`, `docs/errors.md`, plus
  ~10 existing-test call-site updates] `Admin.Reset` (full-graph destructive wipe) had no
  config-gated safety valve, unlike `AllowRetentionPurge` (MEDIUM).** `internal/core/admin.go:240-271`.

  **Fix.** Added `Config.AllowReset bool` (default `false`, mirroring `AllowRetentionPurge`'s exact
  pattern) and the unexported `Core.allowReset` field wired from it in `New`. `AdminOps.Reset()` now
  checks `!c.allowReset` immediately after `checkWritable()` (so a closed or read-only-replica graph
  still gets `ErrGraphClosed`/`ErrReadOnlyReplica` first, unchanged) and fails closed with the new
  `ErrResetDisabled` sentinel before taking `c.mu.Lock()` — refused, not a partial wipe. The
  replica's `ChangeClear` apply path (`apply_record.go`) calls `c.reapCoreStateForClear()` directly,
  never through `AdminOps.Reset()`, so replica convergence is unaffected by this gate (verified by
  reading the call graph, not just assumed). `ErrResetDisabled` re-exported from `pkg/graph/errors.go`
  and added to `docs/errors.md` + `errors_doc_test.go`'s sentinel inventory (a pre-existing self-check
  test, `TestGraphErrorsFileInventoryComplete`, caught the omission on the first test run).

  **Test-suite ripple.** This is a default-behavior change (Reset silently worked before; now it
  fails closed unless opted in), so ~14 existing test files across `pkg/graph` and
  `pkg/graph/internal/core` that called `.Reset()`/`g.Admin.Reset()` needed `AllowReset: true` added
  to their `Config`. Updated at the narrowest correct scope: shared test-graph constructor helpers
  (`newTxTestGraph`, `newTxTimeGraph`, `newTestGraph`, `bcMemory`, `openTieredUniqueGraph`,
  `uniqueBackends`) got the flag added once each (safe for every OTHER test using them too, since it
  only changes `Reset()` behavior); one-off `New(Config{...})` calls specific to a single
  Reset-calling test got it added inline. `TestAdminOpsClosedGraphReturnsErrGraphClosed` needed NO
  change — traced that `checkWritable()` already checks `closed.Load()` before the new gate is
  reached, so a closed graph still surfaces `ErrGraphClosed`, not `ErrResetDisabled`.
  `admin/example_test.go`'s `ExampleAPI_Reset` godoc example was updated to demonstrate the new
  config knob directly, since it's user-facing documentation.

  **New tests.** `TestAdminOpsReset_DisabledByDefault` (a plain `Config{Store: memory.New()}` graph
  with a node already added: `Reset()` returns `ErrResetDisabled` AND the node count is unchanged —
  refused, not a partial wipe) and `TestAdminOpsReset_SucceedsWhenAllowed` (the `AllowReset: true`
  counterpart, confirms the door still works end-to-end when opted in).

  **Verification.** RED confirmed via `git stash push` on the 3 production files alone (test files
  kept): `go vet` failed to compile across 3 packages (`unknown field AllowReset in struct literal`,
  `undefined: ErrResetDisabled`) — proving every test touched is genuinely load-bearing on the fix,
  not just cosmetically updated. Popped the stash, confirmed GREEN. `go build ./...` + `go vet ./...`
  clean. Full repo `go test ./...` clean (including tutorials, `errors_doc_test.go`'s
  documentation-completeness self-check, and admin/example_test.go's godoc example). `go test -race
  ./pkg/graph/internal/core/...` clean (129s) — Reset takes `c.mu.Lock()`, the same exclusion class
  as tx/batch/Archive/ForceRotate, so this is the concurrency-sensitive package for this change.
- **13e. [INVESTIGATED — no code change; regression test added] `PurgeExpiredNodes` can emit a
  duplicate `ChangeRangePurge` record on operator retry after a crash between watermark-advance and
  log-emit (LOW-MEDIUM, harmless — idempotent apply — but log-noise).** `retention_purge.go:132-147`.

  **Confirmed mechanism.** `advanceRetentionWatermark` is max-monotonic and silently no-ops when the
  target watermark is already `<=` the current one — it returns `nil` either way, so the caller
  cannot distinguish "just advanced it" from "already there." `PurgeExpiredNodes` doesn't try to: it
  unconditionally proceeds to `LogRangePurge` regardless of whether step (1) actually changed
  anything. An operator retrying an identical policy call — the natural response to "did my purge
  actually happen?" after a crash, with no other way to know — durably emits a second
  `ChangeRangePurge` record for the same predicate.

  **Considered and rejected a fix**: skip the log emission when the watermark didn't advance
  (`advanceRetentionWatermark` returning a `bool`). This is UNSAFE — it cannot distinguish "watermark
  already advanced AND already logged by an earlier fully-successful call" from "watermark durably
  advanced but the crash landed BEFORE the log call" (exactly the scenario this finding describes).
  The fix would silently DROP the one log record a replica needs in the second case, trading harmless
  log-noise for an actual replica-divergence bug — strictly worse. A safe fix would need a THIRD
  durably-tracked state ("log emitted for this exact range") co-committed atomically with the
  watermark, which shifts the crash window rather than closing it (a crash between log-emit and
  marking-log-done reopens the same class of gap) for a LOW-MEDIUM cosmetic issue the finding's own
  text already calls harmless.

  **Verification, not a fix.** Added `TestRetentionPurge_RetryAfterWatermarkAdvanceDuplicatesLogButConverges`:
  calls `PurgeExpiredNodes` with an identical policy TWICE (first purges 3 nodes, second purges 0 —
  proving the retry is a genuine no-op on the primary), confirms the feed carries exactly 2
  `ChangeRangePurge` records (proving the duplicate mechanism is real, not just theorized), then
  applies BOTH records to a replica bootstrapped from a pre-purge snapshot and confirms it converges
  to the correct post-purge state with no error — proving the "idempotent apply" half of the finding's
  own claim, which was asserted but not previously pinned by any test. `go build ./...` + `go vet
  ./...` clean; `go test ./pkg/graph/...` clean; full repo `go test ./...` clean.
- **13f. `verifyChainLinkage`'s no-stub "legacy leniency" leaves a real tamper-evidence gap — any
  `PrevHash` accepted when the lowest retained version has no compaction stub (MEDIUM, intentional
  backward-compat tradeoff).** `internal/core/integrity.go:126-167`. Any out-of-band row removal *not*
  via `CompactHistoryNodes`/`Rels` is undetectable by `Verify*Chain`.
- **13g/h/i. Three test-coverage gaps that are exactly what would have caught 13a-13b (TEST-GAP).**
  13g [FIXED — `TestPurgeRangeAllChunks_ReapsForeverOwnersOnMidRangeAbort`, see 13b's fix]: no
  crash/abort-mid-purge simulation test. 13h [OPEN]: no multi-`UniqueForever`-key partial-claim test
  (distinct from 13g — a single node holding MULTIPLE forever keys getting purged, not a multi-chunk
  abort). 13i [FIXED — `TestRetentionReset_DoesNotLeakStalePerLabelWatermarkAcrossReset`, see 13b's fix]:
  no Reset+label-token-reuse+cross-label retention test.
- **13j. Transient in-memory activation on the persist-failure path in `createUnique` — narrow
  self-healing race window (LOW).** `unique_constraints.go:339-353`.
- **13k. [FIXED — see 13b, same commit] Misapplied "never-reused tokens" doc rationale in
  `reapRetentionForReset`'s comment — root cause of 13b (LOW).** `retention.go:134-138`.
- **13l. `Admin.Reset`'s correctness depends on a hand-maintained, unenforced checklist of `reap*`
  calls — no test asserting every MetaKV prefix is reaped-or-documented-safe (LOW, latent cross-backend
  divergence risk for future MetaKV features).** `admin.go:250-268`.
- **13m. `CollectShardDropResidue` (badger-layer primitive backing tiered's fast-drop) has zero direct
  badger-package tests (TEST-GAP, Rule 1 — only indirectly exercised via tiered).**
  `pkg/graph/store/badger/badgerstore_shard_drop.go`.

### BACKLOG 14 — Index / docvalues / stats / vector / events hardening (graph layer)

- **14a. [FIXED — `internal/core/tx.go`, `internal/core/docvalues_asof_cache_test.go`]
  `GraphTx.Rollback()` rewrites history without invalidating the AS-OF DocValues cache — stale/wrong
  cached results after a rolled-back transaction (HIGH).** `internal/core/tx.go:577-677,918-943`. Every
  other history-rewrite site calls `c.asOfColumns.bump()`; `tx.go` had zero such calls. A concurrent
  `DocValuesSnapshotAsOf`/`ForEachDocValuesAsOf` could cache a column reflecting a tx's not-yet-committed
  state (permitted under this graph's documented relaxed per-entity isolation — a tx does not hold `c.mu`
  for its whole lifetime); on rollback the cache was never invalidated and was served indefinitely at
  that `(label, txAt)` pin.
  Fix: added `tx.g.asOfColumns.bump()` right after `Rollback()` acquires `tx.g.mu.Lock()` (before the
  reverse-mutation steps run), matching every other history-rewrite choke point (`Admin.Reset`,
  compaction, retention purge, import/import-merge, replica apply). Added
  `TestAsOfColumnCache_TxRollbackInvalidates`: a pre-existing committed node plus a second node created
  inside an open tx (both visible to a mid-tx `buildAsOfColumns` read, proving the relaxed-isolation
  premise — 2 members cached), then `Rollback()`, then a rebuild at the same pin asserting BOTH a
  different cache pointer AND the correct post-rollback member count (1, not 2) — a pointer-identity-only
  assertion was tried first and turned out to pass even on the unfixed code (the empty/near-empty result
  shape in that narrower scenario bypassed the cache-hit path entirely, making it a false-negative test);
  the two-member design catches the STALE-CONTENT case the bug actually produces. Also added a
  `"tx-rollback"` subtest to the existing `TestAsOfColumnCache_RealChokesBumpEpoch` wiring-coverage table
  (parallel to `"retention-watermark"`/`"compaction-watermark"`/`"backfill"`), asserting
  `asOfColumns.currentEpoch()` actually changes. Confirmed RED against the pre-fix code (temporarily
  disabling the new bump call): both the dedicated test and the new subtest failed exactly as predicted
  (stale pointer / unchanged epoch). GREEN after restoring the fix. Full `go test ./...` and
  `go test -race ./pkg/graph/internal/core/...` pass.
- **14b. [FIXED — `internal/core/core.go`, `internal/core/validation.go`, `CLAUDE.md`, `docs/api.md`,
  `internal/core/validation_direct_test.go`, `internal/core/graph_node_validation_test.go`] Missing
  aggregate length/size cap on slice- and map-typed properties — resource exhaustion (HIGH).**
  `internal/core/validation.go:195-300`. `string`/`[]string` correctly checked length;
  `[]int`/`[]int64`/`[]float32`/`[]float64`/`[]byte`/`[]bool`/`[]any`/maps never bounded container size,
  only per-leaf string length (the `[]int` etc. cases had a `depth+1 > maxPropertyValueLimitDepth` check
  that was DEAD CODE — these are leaf types that never actually recurse to `depth+1`, so it could only
  ever fire at the single exact boundary `depth == maxPropertyValueLimitDepth`, an inconsistent artifact,
  not a real size bound). `MaxPropertiesPerEntity` only bounds property *count*. A 500MB `[]byte` or
  100M-element `[]float64` value passed unrejected.
  Fix: added a NEW, SEPARATE `ValidationLimits.MaxPropertyContainerLength` field (default 100000) rather
  than reusing `MaxPropertyValueSize` for both roles. This was a genuine design decision, not just a
  mechanical fix: reusing `MaxPropertyValueSize` (first attempt) broke the existing, deliberately-named
  `TestAddNodePropertyValueNonStringContainersIgnored` test, which pins the INTENTIONAL design that
  non-string containers are NOT governed by the string-length knob — because a legitimate large numeric
  container (a vector-index `[]float32` embedding, per CLAUDE.md's Vector Indexes section, can
  legitimately have thousands of dimensions) would otherwise be bound by a limit scaled for string
  length, not container cardinality. The two concerns have genuinely different natural scales and needed
  independent knobs. `[]int`/`[]int64`/`[]float32`/`[]float64`/`[]bool`/`[]any`/`map[string]string`/
  `map[string]any` are now bounded by element/entry count against `MaxPropertyContainerLength`; `[]byte`
  by byte length against the same field (also exempt from `MaxPropertyValueSize`, matching the existing
  test's intent); `[]string` is UNCHANGED (per-element string length via `MaxPropertyValueSize` only, no
  new aggregate-count cap) — the backlog's own scoping already called `[]string` "correct". The reflect-
  based fallback (`validatePropertyValueLimit`, for arbitrary slice/map kinds not in the typed fast-path
  switch, e.g. `[]int32` or custom struct fields) got the identical `MaxPropertyContainerLength` cap for
  consistency. Renamed the pre-existing test to
  `TestAddNodePropertyValueNonStringContainersUnaffectedByMaxPropertyValueSize` (same assertions,
  corrected name and doc comment — no longer "ignored", now governed by the separate field) rather than
  breaking or deleting it.
  Added container-cap cases to `TestValidatePropertyValueLimitTypedDirectBranches` and
  `TestValidatePropertyValueLimitReflectBranches` (within-cap accepted, over-cap rejected, `[]string`
  explicitly asserted UNAFFECTED) and a new end-to-end
  `TestGraphMutationsRejectOversizedContainerProperty` (node add/update, relationship add, and a
  within-cap-succeeds case, through the real `Nodes.Add`/`Update`/`Rels.Add` doors, not just the
  validator function directly). Confirmed RED against the pre-fix code (unconditional container-size
  accept): all four oversized-container cases returned `nil` instead of `ErrValueTooLarge`, exactly the
  bug described. GREEN after the fix. Updated `CLAUDE.md`'s `ValidationLimits` config-field-contract
  entry and `docs/api.md`'s validation-limits section (both STABLE-fact updates per this repo's own
  CLAUDE.md maintenance rule). Full `go test ./...` passes; not concurrency-sensitive, so `-race` was not
  required.
- **14c. DDL index-creation capability accessors lack the wrapper-promotion guard that sibling
  query-acceleration resolvers apply to the identical interfaces (MEDIUM, same class as the
  `changeFeedCapability` guard elsewhere).** `internal/core/store_capabilities.go:23-69` vs
  `core.go:927-983`. A wrapper store that merely inherits a method via Go embedding gets DDL success
  (builds+forever-maintains a real index) while the correctly-guarded query resolver never uses it —
  wasted maintenance cost for a permanently inert index.
- **14d. `validateNodesByIDRows`/`validateRelationshipsByIDRows` reject a store behavior CLAUDE.md's
  own documented contract explicitly permits — duplicate-ID aliasing (MEDIUM, genuine ambiguity about
  which side is "wrong").** `internal/core/store_validation.go:414-478`. `GetByIDs([5,5])` against a
  spec-compliant untrusted `Store` that aliases the duplicate row is incorrectly rejected as corruption.
- **14e. Two sibling stats doors disagree on capability-check-vs-label-lookup ordering — inconsistent
  `DisablePlannerStats` fail-closed behavior for an unregistered label (LOW-MEDIUM, lessons 17/58
  drift pattern).** `internal/core/stats.go:111-132,226-247` vs `:144-178,186-217`.
- **14f. FIFO (not LRU) eviction on the as-of DocValues column cache (cap=64) — undercuts the cache's
  own stated goal once a workload exceeds 64 distinct hot pins (LOW, deliberate simplicity tradeoff,
  worth revisiting).** `internal/core/docvalues_asof_cache.go:31-51,121-139`.
- **14g. `graph_epoch.go`'s corrupt-lineage and zero-avoidance branches have no direct test (TEST-GAP).**
  `graph_epoch.go:36-38,43-46`.
- **14h. Composite-index introspection (`ListComposites`/`HasComposite`) has no counterpart for
  property/temporal/vector/rel-property indexes — see BACKLOG 21 for the feature-level entry.**

### BACKLOG 15 — Internal primitives hardening (storeutil / locks / registry / wire codec)

- **15a. [FIXED — `internal/storeutil/wire_decode.go`,
  `internal/storeutil/wire_decode_amplification_test.go`] `decodeIntSlice`/`decodePropertyArray` size
  directly from an untrusted msgpack array-length header with no cap (HIGH, lesson-48 class, hit on
  every `SafeUnmarshal`-protected disk read and replication/import record).**
  `internal/storeutil/wire_decode.go:279,298`. Up to a 50-60x amplification for `PropertyWire` (empty
  fixmap = 1 wire byte vs. ~50+ byte struct), unlike `import.go`'s `reserve()` which lesson 48 explicitly
  capped.
  Fix: both now `make([]T, 0, min(n, wireArrayPreallocCap))` (new constant, 4096 — mirrors
  `importPreallocLimit`) and `append` inside the decode loop instead of `make([]T, n)` +
  indexed-assignment — allocation grows proportional to elements ACTUALLY decoded, not the claimed
  count, while every realistic entity (`MaxLabelsPerNode`/`MaxPropertiesPerEntity` both far under 4096
  by default) still hits the pre-sized fast path with the SAME single-allocation cost as before (no
  performance regression for the common case — `append` up to a capacity already sized to `n` causes
  zero reallocation).
  Investigation note on exploitability: confirmed via a first (WRONG) test attempt that
  `SafeUnmarshal`'s existing `guardMsgpackDepth` pre-scan (a separate, already-hardened trust-boundary
  check — see the "Decode untrusted msgpack only through storeutil.SafeUnmarshal" design rule) ALREADY
  rejects the naive "tiny header claiming a huge count, no body follows" attack shape as truncated,
  BEFORE the real decoder (and thus `decodeIntSlice`/`decodePropertyArray`) ever runs — so a 9-byte
  hostile blob claiming 100M elements with nothing following is not actually exploitable and produced a
  false-positive-GREEN test that passed even against the unpatched code. The REAL exploitable shape
  (confirmed by then correctly showing RED against the unpatched code) requires the attacker to supply
  bytes proportional to the CLAIMED element count at minimal per-element wire cost (guardMsgpackDepth's
  scan needs real bytes for every claimed pending slot) — e.g. ~2MB of single-byte fixint filler
  claiming 2M elements, with element 0 alone being semantically invalid (a type guardMsgpackDepth's
  structure-only walk doesn't catch, but the real decoder does) — which still forces a
  make(n)-sized (tens of MB) allocation *before* that first-element failure is ever detected, wasting
  memory vastly disproportionate to what the attacker had to send. Still a genuine ~8x ([]int) to ~70x
  ([]PropertyWire) amplification DoS, just requiring megabytes rather than a handful of bytes of attacker
  input — documented here so a future reviewer doesn't re-derive the same false-positive test shape.
  Added `TestDecodeIntSlice_HostileArrayLenDoesNotAmplifyAllocation` and
  `TestDecodePropertyArray_HostileArrayLenDoesNotAmplifyAllocation` (the correctly-shaped hostile
  payload above, via `runtime.MemStats.TotalAlloc` delta, mirroring `import_amplification_test.go`'s
  established pattern) plus `TestDecodeIntSlice_RealisticSizeStillDecodesCorrectly` (no correctness
  regression for a small legitimate array). Confirmed RED against the pre-fix code (delta ~16 MB for
  `[]int`, ~144 MB for `[]PropertyWire`, both far over their ceilings); GREEN after the fix. Full
  `go test ./...` passes, including the badger/memory wire-format round-trip and fuzz-corpus-derived
  suites that exercise this exact decode path.
- **15b. [FIXED — `internal/registry/property_key_registry_coverage_test.go`] `PropertyKeyRegistry` had
  only 2 direct tests for 8 public methods, vs. ~25 each for the sibling label/reltype registries — Rule
  1 violation on the hot property-encode path (HIGH).** `internal/registry/property_key_registry_test.go`.
  Untested: capacity overflow returning `(0,nil)` instead of an error (lesson 37),
  concurrent-`GetOrCreate`-same-name race, round-trips.
  Fix (test-gap, no production code change — this is a coverage gap, not a known defect): added 29 new
  direct tests mirroring `label_registry_test.go`'s coverage categories (GetOrCreate/duplicate-returns-
  same-token, Resolve round-trip + out-of-range, Lookup miss/empty, token-0-reserved, zero-value struct
  behaves like a fresh registry (both for direct use and `ImportNames`), concurrent `GetOrCreate` (50
  goroutines, same key, same token), concurrent empty-name rejection, `Len`, whitespace-only name
  rejection, recovery after a rejected empty name, full Export→Import round-trip incl. continuing
  token allocation post-import, `ImportNames` edge cases (non-empty target, invalid `names[0]`, empty
  slice, exact-capacity accepted, one-over-capacity rejected, token order preserved, duplicate entry
  rejected), and a full `AppendNames` suite (success, prefix mismatch, empty/invalid prefix, empty
  suffix no-op, blank suffix entry, duplicate suffix entry, suffix clashing with prefix, capacity
  overflow) — the sibling registries' `append_names_test.go` coverage had no `PropertyKeyRegistry`
  counterpart at all. Two PropertyKeyRegistry-specific deltas from the label/reltype pattern, called out
  explicitly in a new `TestPropertyKeyRegistryCapacityIsSoft` (the direct lesson-37 pin: `GetOrCreate` at
  capacity returns `(0, nil)`, not an error — the wire encoder falls back to the raw key string rather
  than failing the entity write) and `TestPropertyKeyRegistryToken65535IsAssignable`'s tail assertion
  (post-full-capacity `GetOrCreate` is *also* `(0, nil)`, unlike the label registry's hard error): this
  registry has no `ResolveAll`/`RollbackNames` (label/reltype-only doors), so those categories were
  omitted rather than faked. `go tool cover -func` confirms every one of the 8 public methods now sits
  at 91.7-100% (package total 95.5%, up from the pre-fix baseline), well above the 80% Rule 7 floor. Full
  `go test ./...` and `go test -race ./pkg/graph/internal/registry/...` pass.
- **15c. [FIXED — `internal/locks/value_locks_test.go`] `LockStripesExcept` had zero direct or indirect
  test coverage anywhere — the concurrency-critical skip-already-held-stripe path reached from
  `GetOrCreateByKey` (HIGH, correct-by-construction today, no regression net).**
  `internal/locks/value_locks.go:84`, sole call site `unique_constraints.go:593`.
  Fix (test-gap, no production code change — confirmed correct-by-construction, exactly as the finding
  characterized it): added 5 direct tests. `TestLockStripesExceptEmptyHeldDelegatesToLockStripes` pins
  the documented "empty held == LockStripes" contract. `TestLockStripesExceptSkipsHeldStripes` and
  `TestLockStripesExceptAllRequestedAreHeld` hold stripes for REAL (not simulated) and assert the
  skip-set is honored in the returned list. `TestLockStripesExceptDoesNotDeadlockOnHeldStripe` is the
  direct regression guard for the exact bug class this function exists to prevent: since a
  `sync.Mutex` is not reentrant, a broken skip check would make the function try to re-lock a stripe
  the SAME goroutine already holds — an unconditional self-deadlock requiring no `-race` or multiple
  goroutines to observe, guarded by a 2-second timeout so a regression fails fast instead of hanging the
  suite. `TestLockStripesExceptGetOrCreateByKeyShape` mirrors the real call shape (a caller-held keyed
  stripe appearing in both `stripes` and `held`) and additionally proves the OTHER stripes are genuinely
  locked (a concurrent contender blocks until `UnlockStripes` releases them) — closing the "returns the
  right list but doesn't actually lock anything" gap a pure-list-comparison test would miss.
  Verified regression-sensitivity directly: temporarily emptied the `skip` map inside
  `LockStripesExcept` (reproducing exactly the bug class these tests exist to catch) and re-ran the
  suite — `TestLockStripesExceptDoesNotDeadlockOnHeldStripe` failed cleanly via its own timeout exactly
  as designed, while the other three held-stripe tests (no internal timeout guard, since they call
  `LockStripesExcept` synchronously in the same goroutine as the held lock) hung and were caught by the
  outer `go test -timeout` at the suite level — both are valid failure signals, confirming the whole set
  is genuinely sensitive to this bug class, not just superficially plausible. Restored the original code
  (bit-for-bit — `git diff` empty) and confirmed GREEN. Full `go test ./...` and
  `go test -race ./pkg/graph/internal/locks/...` pass.
- **15d. History-delta decode functions have zero fuzz coverage unlike sibling
  `WireToNodeChecked`/`WireToRelChecked` (MEDIUM, same untrusted-bytes trust class lesson 47's fuzzing
  found the original bug on).** `badgerstore_history_delta.go` (4 call sites) +
  `storeutil/wire_history_delta.go`. Fix: add `FuzzDecodeNodeHistoryDelta`/`FuzzDecodeRelHistoryDelta`.
- **15e. `DecodeRangePurge`/`DecodeForeignIncomingDelete` decoders not exercised by the existing
  round-trip/fail-closed test suites despite being live shipped record types (MEDIUM).**
  `storeutil/changelog.go`.
- **15f. `propertyToWire`'s `ptCustom` branch does a full marshal+reflect-unmarshal+2×hash+compare
  round-trip on *every write*, not just type-registration time (MEDIUM, perf, may be intentional
  defense-in-depth).** `storeutil/wire_value.go:429-464`.
- **15g. `ValueStripe` calls `fnv.New64a()` (heap-allocating interface) on every invocation — once per
  constrained property per node write (MEDIUM, perf).** `internal/locks/value_locks.go:44-53`. Fix:
  inline FNV-1a over a local `uint64` accumulator.
- **15h. `TestKeyPrefixesNonOverlapping` omits the 3 newest production key-prefix tags
  (`KeyChangeLog`/`KeyPropertyIndex`/`KeyTemporalIndex`); dead test scaffold aliases real production
  byte values (MEDIUM, test-gap + code smell, zero prod risk).** `storeutil/keys_test.go:297-309`,
  `keys_helpers_test.go:11-14`.
- **15i. `integrity_test.go`/CLAUDE.md attribute the per-type-tag property-value switch to
  `internal/integrity`, but the real switch lives in `pkg/types` — Rule 3 gets enforced against the
  wrong location (MEDIUM, doc/attribution).**
- **15j. `EnvelopeOverlaps` (backs the B4 candidate-prune optimization on every history-aware temporal
  scan) has no direct unit test anywhere — only indirect (LOW-MEDIUM, Rule 1).**
  `storeutil/temporal_filter.go:37`.
- **15k. `WireToNode`/`WireToRel` (unchecked, panic-on-invalid variants) have zero production callers
  but dangerously similar naming to the trust-boundary-safe `*Checked` versions (LOW, foot-gun risk).**
  `storeutil/wire.go:204,346`. Fix: delete or rename unmistakably as test-fixture-only.
- **15l. A `PropertyWire` with both `KeyToken==0` and `Key==""` (neither valid v1 nor v2 form) passes
  unchallenged (LOW-MEDIUM, contingent on unverified downstream behavior).** `wire.go:120-128,458-498`,
  `wire_property_keys.go:41-59`.
- **15m. `decodeMapKeyLen`'s over-long-key path allocates a fresh up-to-65535-byte slice per key
  instead of pooled scratch, on an otherwise zero-alloc path (LOW, perf).**
- **15n. `generatedcreate.FreshGraphID` exported as a mutable package-level `var`, not a
  const/accessor — accidental reassignment zeros a process-wide global (LOW, module-internal blast
  radius).** `internal/generatedcreate/capability.go:16`.
- **15o. `PropertyKeyRegistry` has no `RollbackNames` unlike `Label`/`RelTypeRegistry` — intentional
  (lesson 37) but undocumented at the type level (LOW).**
- **15p. No `PreEncodeRelPutPayloadV2` counterpart to `PreEncodeNodePutPayloadV2` for §4.5 pre-encode —
  see BACKLOG 21 (LOW, likely intentional node-first scope).**
- **15q. `PaginateNodesInOrder` tests the cursor via `after != 0` directly instead of
  `after.SnowflakeID() > 0` like every sibling `Paginate*` function — inconsistent idiom (LOW).**
  `storeutil/pagination.go:95-122`.
- **15r. `wireEncBufPool` (`sync.Pool`, shared on the hot ingest write path across goroutines) has no
  concurrent/parallel-goroutine test pinning its safety guarantee (LOW, `sync.Pool` is safe by design;
  test would pin it rather than rely on the general contract).**

### BACKLOG 16 — In-memory index-engine hardening (HNSW / property & temporal index / HyperLogLog)

- **16a. [FIXED — `internal/index/property_stats_accumulator.go`,
  `internal/index/property_stats_accumulator_test.go`] `property_stats_accumulator.go`'s NaN handling
  permanently corrupts Min/Max — violates the library's own `PropertyTypeClass` NaN-split rule (HIGH).**
  `internal/index/property_stats_accumulator.go:223-292`. `scalarOrderFamily` classified
  `float32`/`float64` as numeric with no NaN check; the first NaN observed set `min=max=NaN` permanently
  (all subsequent `scalarLess` comparisons against a stored NaN are `false` under IEEE 754, so it could
  never be replaced by a real value again). Reachable via real indexable values.
  Fix: `scalarOrderFamily` now returns `""` (unorderable, matching `bool`/`TemporalValue`) for a NaN
  `float32`/`float64` specifically — `math.IsNaN` — while still returning `"n"` for `±Inf` (per the
  library's own `PropertyTypeClass` rule: `±Inf` IS Numeric, only NaN is unorderable). Since
  `Observe`/`Forget`/`Rescan` all funnel through this one function, the fix closes the corruption at the
  single seam. Also found and fixed a SECOND, related gap surfaced while writing the regression test:
  `CombineExtrema` (the cross-shard/cross-accumulator fold helper) has a `runningMin == nil` fast path
  that adopted `incomingMin` UNCONDITIONALLY, without ever consulting `scalarOrderFamily` — so a raw NaN
  handed to `CombineExtrema` directly (bypassing an accumulator's own `Observe`/`Rescan`, which are
  already NaN-safe post-fix) would still wedge the running min/max at NaN. In normal operation this path
  is unreachable (every real caller sources `incomingMin` from an accumulator's `Snapshot()`/`Rescan()`,
  already filtered), but `CombineExtrema` is an exported function with no way to enforce that discipline
  on every caller, so it now also checks `scalarOrderFamily(incomingMin) == ""` up front and treats an
  unorderable incoming value the same as a nil one (no-op, running pair unchanged) — defense in depth
  consistent with the same NaN-safety principle, not scope creep beyond "property_stats_accumulator.go's
  NaN handling".
  Added `TestScalarOrderFamily_NaNExcludedButInfIncluded` (direct classifier pin, both float32/float64,
  plus the ±Inf-stays-Numeric counter-case), `TestPropertyStatsAccumulatorObserveExcludesNaN` (NaN
  observed FIRST must not seed min/max, and real values observed afterward must still establish them
  normally — proving the accumulator is never permanently wedged),
  `TestPropertyStatsAccumulatorObserveNaNAfterRealValuesLeavesMinMaxIntact` (NaN observed mid-stream must
  not disturb an already-established min/max), and `TestCombineExtrema_NaNIncomingDoesNotCorruptRunning`
  (the cross-accumulator fold path, both the nil-running and real-running cases). Confirmed RED against
  the pre-fix code: 3 of the 4 new tests failed exactly as predicted (`min=NaN max=NaN` instead of `nil`,
  `scalarOrderFamily(NaN) = "n"` instead of `""`, `CombineExtrema` adopting NaN unconditionally). GREEN
  after the fix, including every pre-existing test in the file. Full `go test ./...` passes; not
  concurrency-sensitive, so `-race` was not required.
- **16b. [FIXED — `internal/index/rel_property_index.go`, `internal/index/rel_property_index_test.go`]
  `PurgeRelFromAllPropertyIndexes` (corruption-path purge) missed the string-ordered view, unlike the
  node mirror — violates "Index cleanup on corruption must clean ALL indexes" (HIGH).**
  `internal/index/rel_property_index.go:106-125` vs `property_index.go:181-198`. Stale
  `strBuckets`/`strKeys` entries → phantom results for rel string properties.
  Fix: added the missing `idx.purgeOrderedStr(id)` call alongside the existing `idx.purgeOrdered(id)`,
  matching `PurgeNodeFromAllPropertyIndexes` exactly (Node/Rel structural-mirror parity, Testing Rule 2).
  Added `TestRelPropertyIndexPurgeCleansStringOrderedView`: indexes a rel with a STRING-valued property
  (`"city": "berlin"`), asserts the setup actually populated `strBuckets` (so the test can't pass
  vacuously), purges the rel, then asserts both `Entries` AND every `strBuckets` bucket / `strKeys.n` are
  clean. Confirmed RED against the pre-fix code (`strBuckets["berlin"]` still held the purged rel after
  the purge call, exactly the phantom-result symptom the finding describes); GREEN after the fix,
  alongside every pre-existing `TestRelPropertyIndex*` test. Full `go test ./...` passes; not
  concurrency-sensitive, so `-race` was not required.
- **16c. [FIXED — `internal/index/docvalues.go`, `internal/index/docvalues_numeric_types_test.go`]
  `docvalues.go`'s `buildColumn` narrow numeric-type classification silently disabled the columnar
  accelerator for otherwise-valid data (MEDIUM-HIGH).**

  **Bug.** `buildColumn`'s classification switch (`internal/index/docvalues.go:278-307`, pre-fix)
  recognized only `int64/int/int32/float64/float32` as numeric; every other member of the canonical
  scalar-numeric allowlist (`pkg/types/propertyslice.go`'s `isScalarPropertyValue`: `int8, int16, uint,
  uint8, uint16, uint32, uint64` — 7 of the 12 allowlisted numeric types) fell into the
  `default: sawOther = true` arm. Because the classification is column-wide (`sawOther ||
  (sawNumeric && sawString)` ⇒ `colUnbuildable` for the WHOLE column), a single node whose property
  happened to be stored as e.g. `uint8` silently poisoned the columnar accelerator for *every other
  node* on that label/property pair too — no error, no log, just a quiet fallback to the per-node
  path. `buildNumericColumn`'s conversion switch had the matching narrower set, so even after
  widening the classifier the boxing step needed the same treatment.

  **Fix.** Widened the classification switch in `buildColumn` to the full 12-type allowlist
  (`int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64`). Widened
  `buildNumericColumn`'s conversion switch to match, boxing every integer type as `int64` (the
  consumer's documented fast-path type) except `uint`/`uint64`, which get a range check via a new
  `normalizeUint64` helper: values `<= math.MaxInt64` box as exact `int64`; larger magnitudes box as
  `float64` instead of silently wrapping negative via a raw `int64(x)` cast. This mirrors the
  established precedent in `property_stats_accumulator.go`'s `numericValue`/`scalarOrderFamily` (which
  already classify all 12 types uniformly and document the identical "beyond int64/2^53, projection to
  float64 loses precision at the margin, but the alternative — silent wraparound — is worse"
  trade-off) — confirming the accumulator that ultimately reads `docColumn.boxed` values already
  expects and correctly handles this exact int64-vs-float64 boxing scheme, so the fix introduces no new
  contract on the consumer side.

  **Tests** (`internal/index/docvalues_numeric_types_test.go`, new file):
  `TestDocValues_WidenedNumericTypesBuildable` — table-driven over all 7 previously-unsupported types
  (`int8, int16, uint, uint8, uint16, uint32, uint64`), each mixed with an `int64` value on a sibling
  node, asserts `Has("x")` is now true (not silently unbuildable) and the value round-trips as the
  correct `int64`. `TestDocValues_Uint64OverflowFallsBackToFloat64` — a `uint64` value one thousand past
  `math.MaxInt64` boxes as a *positive* `float64`, not a wrapped-negative `int64` (the one genuine edge
  case the widening introduces). `TestDocValues_MixedWidenedTypesInOneColumn` — five nodes with five
  different integer/float widths (`int8, uint16, uint32, int64, float64`) in the SAME column all build
  as one numeric column and sum correctly, proving the realistic case of heterogeneous-width data
  written over time (not just each type in isolation).

  **Verification.** RED confirmed via `git stash push -- internal/index/docvalues.go` against the new
  test file: all 7 `TestDocValues_WidenedNumericTypesBuildable` subtests failed with "column reported
  unbuildable, want buildable (numeric)". Stash popped, GREEN confirmed for all three new tests. Full
  `go build ./...` and `go vet ./...` clean. `go test ./pkg/graph/internal/index/...` — full package
  pass (including the pre-existing `TestDocValues_Int64PrecisionPreserved` int64-precision-above-2^53
  regression, `TestDocValues_HeterogeneousUnbuildable`'s still-correctly-poisoned bool/list/mixed cases,
  and the HNSW/range-cardinality suites in the same package). `go test -race
  ./pkg/graph/internal/index/...` clean. Full-repo `go test ./...` clean across every package
  (`pkg/graph`, `internal/core`, `store/badger`, `store/tiered`, `store/sharded`, tutorials, etc.) — no
  regressions.
- **16d. [FIXED — `internal/index/hyperloglog.go`, `internal/index/hyperloglog_sequential_test.go`]
  `hyperloglog.go`/`property_stats_accumulator.go`: the FNV-1a short-sequential-integer undercount
  (lesson 65) was directly reachable via real property values (years, counters, ages) — production
  `NodePropertyStats`/`NodePropertyStatsSketch` accuracy was affected, not just a test-authoring
  footgun (MEDIUM-HIGH).**

  **Bug.** Lesson 65 documented an ~11x HyperLogLog NDV undercount when `AddString` is fed short
  sequential-decimal inputs (FNV-1a avalanches poorly on inputs differing only in a low-order ASCII
  byte, so the sketch's register-index bits — the TOP `precision` bits of the hash — correlate
  heavily across consecutive inputs and collapse thousands of logically distinct values into a
  handful of registers) and explicitly scoped the finding as test-authoring-only: "the hash choice...
  [is] out of scope." That framing missed that the ACTUAL production hash input,
  `types.IndexablePropertyValueKey(value)`, produces exactly this adversarial shape for ordinary
  numeric properties — `"i64:1990"`, `"i64:1991"`, `"i64:1992"`, ... for a `birthYear` property, or
  `"i64:0".."i64:120"` for an `age` property. Years, ages, small counters, and sequential external IDs
  are common real-world property values, so `NodePropertyStats`/`NodePropertyStatsSketch`'s NDV
  estimate was silently ~11x too low for exactly the kind of low-cardinality-looking-but-actually-fine
  numeric column a query planner would most want an accurate estimate for.

  **Fix.** Added `mix64` (the Murmur3 `fmix64` finalizer — a well-known 3-round xor-shift/multiply
  avalanche mixer) applied to every hash in `HyperLogLog.addHash` before it's split into register
  index and rho bits. This whitens FNV-1a's output without changing the hash function itself, so the
  sketch's collision/uniformity properties become independent of input structure. `HyperLogLog` has no
  `MarshalBinary`/persistence path (confirmed via grep — no sketch is ever serialized to disk; every
  `NodePropertyStats`/sketch capability rebuilds live from a node scan, matching the
  `DisablePlannerStats` doc comment's "maintained on every write... rebuild at open" contract), so this
  is a pure in-memory accuracy fix with no wire-format or migration concern.

  **Tests** (`internal/index/hyperloglog_sequential_test.go`, new file, all feeding the ACTUAL
  production key encoding via `types.IndexablePropertyValueKey`, not raw `fmt.Sprintf`):
  `TestHyperLogLogSequentialIntegerKeysAccuracy` (2000 sequential `int64` keys, error <10%),
  `TestHyperLogLogSequentialIntegerKeysAccuracy10k` (10k sequential keys, error <5% — the same bar the
  pre-existing random-string `TestHyperLogLogAccuracy10k` already holds), `TestHyperLogLogSequential
  IntegerKeysManyDistinctRegisters` (an implementation-independent check that 2000 well-mixed keys
  actually touch ≥800 of the sketch's 16384 registers, catching the exact register-collapse mechanism
  independent of the `Estimate()` formula), and `TestPropertyStatsAccumulator_SequentialYearsAccuracy`
  (exercises the bug at the real production call site — `PropertyStatsAccumulator.Observe` fed 71
  years 1950..2020, error <15%).

  **Verification.** RED confirmed via `git stash push -- internal/index/hyperloglog.go`: all four new
  tests failed reproducing lesson 65's numbers almost exactly (2000 keys estimated at 178, actual
  2000, relative error 0.91; 10k keys estimated at 891; 71 years estimated at NDV 8; only 177/16384
  registers touched). Stash popped, GREEN confirmed for all four. Full `go build ./...` and `go vet
  ./...` clean. `go test ./pkg/graph/internal/index/...` — full package pass, including the
  pre-existing seeded-random-string `TestHyperLogLogAccuracy10k`/`TestHyperLogLogAccuracy100k`
  (<5%/<3% bars, still hold post-fix — the finalizer doesn't regress the already-well-distributed
  case), `TestHyperLogLogMergeEquivalence`, `TestHyperLogLogSparseToDenseConversion`, and every other
  pre-existing sketch/accumulator test. `go test -race ./pkg/graph/internal/index/...` clean.
  Full-repo `go test ./...` clean across every package — no regressions.
- **16e. `hnsw.go`'s `reassignEntryPoint` doesn't pick the max-level survivor — silently collapses
  `maxLevel`, degrading search convergence/quality after entry-point deletion (MEDIUM, no crash, no
  test targets this).** `internal/index/hnsw.go:425-435`.
- **16f. `hnsw.go`'s `connect()` uses reflection-based `sort.Slice` on the hottest construction path,
  contradicting the project's own established `slices.SortFunc` idiom (MEDIUM, perf).**
  `internal/index/hnsw.go:299` — `lru.go:314-316`'s comment explicitly warns against this pattern.
- **16g. `hnsw.go`'s `searchLayer` allocates a fresh `[]bool` visited slice per layer, not reused
  across a query — O(maxLevel) O(n)-byte allocations per query (MEDIUM, perf).**
  `internal/index/hnsw.go:337-345`. Fix: per-graph visited-generation array + epoch counter.
- **16h. `temporal_index.go`'s `Extend`/`Remove` is O(n) linear scan on every node mutation to a
  temporally-indexed label — O(n²) bulk-update under the write lock (MEDIUM).**
  `internal/index/temporal_index.go:130-158,251-270`. Fix: secondary id→index map for O(1) amortized.
- **16i. `hnsw.go`/`hnsw_test.go` has no direct BFS-reachability graph-connectivity regression test —
  only an indirect, `-short`-skipped recall@10 proxy (MEDIUM, TEST-GAP).** CLAUDE.md documents that
  exact BFS test was used during development to catch the naive-closest fragmentation bug; it doesn't
  exist today as a fast always-run test.
- **16j. `property_index_rangecount.go`'s `numImprecise` is a one-way sticky flag, never
  re-evaluated — one transient outlier value permanently disables `RangeCardinality` for the index's
  whole lifetime (LOW-MEDIUM, deliberate but undocumented).**
  `internal/index/property_index_rangecount.go:61-74`.
- **16k. `RangeNodeIDs`'s `inclMin`/`inclMax` parameters are declared but never read — contract drift
  (LOW).** `internal/index/property_index_range.go:230`.
- **16l. `sorted_chunks.go`'s `remove()` has no merge-on-shrink for adjacent undersized chunks (LOW-
  MEDIUM, missing-feature/perf note for long-lived high-churn indexes).**
- **16m. `lru.go`'s `MarkDeleted` on an already-cached key leaves the stale payload and its accounted
  byte size in place until flush — un-evictable, holds full memory against the byte budget (LOW-
  MEDIUM, untested intermediate state).** `internal/index/lru.go:247-257`.

### BACKLOG 17 — Store interface & MemoryStore hardening

- **17a. [FIXED — `store/memory/memorystore_history.go`, `store/memory/memorystore_node.go`,
  `store/memory/relepoch_bump_test.go`] Missing `bumpRelEpoch()` on every adjacency-mutating
  delete-with-history path — stale X5 aggregate reads served as valid (HIGH).**
  `store/memory/memorystore_history.go:170-206,211-323`, `memorystore_node.go:357-417`.
  `DeleteRelWithHistory`/`DeleteNodeCascade`/`DeleteNodeWithHistory` never bumped `relEpoch` despite it
  being documented as bumped on every rel write including delete. Concurrent `DocValuesSnapshot`
  readers' staleness gate passed despite adjacency having changed mid-scan. Zero test coverage.
  Fix: added `defer ms.bumpRelEpoch()` to the top of all three functions, mirroring the SAME
  single-bump-per-call idiom every other adjacency-mutating door in this file already uses (e.g.
  `DeleteRelationship`, `DeleteNodeWithHistory`'s existing `bumpNodeEpoch`) — not "move the bump into
  `deleteRelLocked`" as the finding's own alternative suggested, since that shared low-level helper is
  called from inside loops (the cascade paths), which would multiply the bump count per call; harmless
  per the documented "spurious bump is safe" coarse-counter design, but the top-of-function pattern is
  simpler to verify and matches every existing call site exactly. `DeleteNodeWithHistory` bumps
  unconditionally (even with zero connected relationships) for the same reason `bumpNodeEpoch` already
  does — a spurious bump on a no-op is documented-safe.
  Added 4 tests in a new file: `TestDeleteRelWithHistory_BumpsRelMutationEpoch`,
  `TestDeleteNodeCascade_BumpsRelMutationEpoch`, `TestDeleteNodeWithHistory_BumpsRelMutationEpoch` (no
  connected rels — the spurious-bump-is-safe case), and
  `TestDeleteNodeWithHistory_BumpsRelMutationEpochWithConnectedRel` (the case that matters most — a
  tombstoned rel actually being removed). All read `RelMutationEpoch()` before/after and assert it
  changed. Confirmed RED against the pre-fix code: all 4 failed with "RelMutationEpoch unchanged" exactly
  as the finding predicts. GREEN after the fix. Full `go test ./...` and
  `go test -race ./pkg/graph/store/memory/...` pass.
- **17b. [FIXED — `store/memory/memorystore_rel_index.go`, `store/memory/memorystore_rel_index_test.go`]
  `ForEachRelByTypePropertyRange` invoked its callback WHILE holding the store's RLock — deadlock hazard
  (HIGH).** `store/memory/memorystore_rel_index.go:195-255`. Every sibling streaming method snapshots
  IDs, releases the lock, then calls `fn` outside it, matching the documented `IterationCapability`
  contract and CLAUDE.md's `sync.RWMutex` non-reentrancy warning. This one deadlocked if `fn` re-entered
  any Store read method while a writer was waiting.
  Fix: restructured to the same two-phase pattern `ForEachRelByTypePropertyRangeOrdered` already used —
  the candidate ID set is computed under one brief `RLock`/`RUnlock` window, then each row is looked up
  under its OWN brief `RLock`/`RUnlock`, and `fn` runs with NO lock held at all.
  Added `TestForEachRelByTypePropertyRange_CallbackCanReenterStoreWithoutDeadlock`: a scan goroutine's
  callback signals it has started, waits for a concurrent WRITER goroutine to actually be queued on
  `ms.mu.Lock()` (a `time.Sleep` after starting the writer, giving the scheduler time to block it there
  — this ordering is what makes the deadlock deterministic, since Go's `sync.RWMutex` documents that once
  a `Lock()` call is waiting, no new `RLock()` is granted until it proceeds), THEN calls back into the
  store via `GetRelationship` from inside the callback — guarded by a 2-second timeout so a regression
  fails cleanly instead of hanging the suite. Confirmed RED against the pre-fix code: the test failed
  with "ForEachRelByTypePropertyRange deadlocked when its callback re-entered the store" after the full
  2-second timeout, exactly reproducing the hazard. GREEN after the fix, alongside the pre-existing
  `TestMemStoreForEachRelByTypePropertyRange`. Full `go test ./...` and
  `go test -race ./pkg/graph/store/memory/...` pass.
- **17c. [FIXED — `store/memory/memorystore_retention_purge.go`,
  `store/memory/memorystore_retention_purge_validto_test.go`] Nil-pointer panic in
  `PurgeNodesByLabelValidToBefore` when a node has no `TemporalMetadata` (HIGH, crash/DoS).**
  `store/memory/memorystore_retention_purge.go:69-77`. `n.Temporal()` can legitimately be nil
  (documented, permitted by validators); the predicate did `n.Temporal().ValidTo` with no nil check.
  Reachable via any node stored without `SetTemporal` (raw Store interface, batch import, replication
  apply).
  Fix: applied exactly the finding's own suggested fix — `tm := n.Temporal(); return tm != nil &&
  tm.ValidTo != 0 && tm.ValidTo < before`. A nil-Temporal node makes no `ValidTo` world-time assertion,
  so it is correctly treated the same as an explicit open interval (`ValidTo == 0`): never purged by
  this predicate.
  Added `TestMemoryStorePurgeNodesByLabelValidToBefore_NilTemporalDoesNotPanic` (a node stored without
  `SetTemporal`, guarded by a `recover()` so a regression fails as a normal test failure instead of
  crashing the whole test binary; asserts zero nodes purged and the node survives) and
  `TestMemoryStorePurgeNodesByLabelValidToBefore_PurgesOnlyExpiredValidTo` (four nodes — expired
  ValidTo, open-interval ValidTo=0, future ValidTo, and nil-Temporal — asserting exactly the expired one
  is purged, closing 17d's coverage gap in full: this door had zero direct tests before this fix).
  Confirmed RED against the pre-fix code: both tests failed, the second with an actual unrecovered
  panic and full goroutine dump (`invalid memory address or nil pointer dereference`) exactly
  reproducing the crash. GREEN after the fix. Full `go test ./...` passes; not concurrency-sensitive
  (single predicate function, no locking change), so `-race` was not required.
- **17d. [FIXED — see 17c, same commit] Zero test coverage for `PurgeNodesByLabelValidToBefore` on the
  memory backend — Rule 1 violation, direct cause of 17c shipping (TEST-GAP).**
- **17e. `MemoryStore` never honors `QueryOpts.NoSort` — silent perf-parity gap with badger, which
  does honor it (MEDIUM, not user-visible incorrectness, but breaks memory's role as an oracle for
  NoSort's performance characteristic).** `store/memory/memorystore_query.go` and scan files.
- **17f. Retention purge leaves permanent dangling entries in `labelTxMembers`/`relTypeTxMembers` —
  unbounded memory leak combined with pinned scans (MEDIUM, soundness-preserving but defeats
  retention purge's whole memory-bounding purpose for high-volume event workloads).**
  `store/memory/memorystore_retention_purge.go:117-152`.
- **17g. `snapshotChangesLocked` is O(total log size) per call instead of O(returned records) — O(n²)
  to fully drain via small-limit polling (LOW-MEDIUM, severity capped since the memory changelog is
  explicitly a non-durable test/parity facility).** `store/memory/memorystore_changelog.go:262-278`.
- **17h. Index-creation doors hold the exclusive write lock for the entire scan-and-build, unlike the
  documented 3-phase pattern (LOW-MEDIUM, possibly acceptable for pure-RAM but deviates from a stated
  rule without a carved-out exception).** `store/memory/memorystore_index.go`, `memorystore_rel_index.go`.
- **17i. `defer bumpNodeEpoch()`/`bumpRelEpoch()` fire even on validation-failure/no-op error paths
  (LOW, already documented as a deliberate/safe tradeoff — flagged only for future profiling).**
- **17j. No relationship-side mirror of `PropertyStats` — see BACKLOG 21 (LOW, likely intentional per
  capability doc scoping).**

### BACKLOG 18 — Badger backend hardening

- **18a. [FIXED — `badgerstore_changelog.go`, `badgerstore_node.go`, `badgerstore_rel.go`,
  `badgerstore_history_node.go`, `badgerstore_history_rel.go`,
  `badgerstore_changelog_buildpayload_test.go`] Single-item Put/Replace/label doors mutate state and
  enqueue write ops BEFORE marshaling the change-log payload — a reported failure can leave a
  partially-committed write (HIGH, whole-door-family issue, lesson 58).** Originally cited 6 sites
  (`badgerstore_node.go:79,484,583,678`, `badgerstore_rel.go:131,255`); a grep audit during the fix
  (CLAUDE.md's "every fix needs a grep audit" rule) found 4 MORE call sites with the identical bug
  shape that the original finding's line numbers missed: the four `WithHistory` doors
  (`RemoveNodeLabelTokenWithHistory`, `AddNodeLabelTokenWithHistory`, `ReplaceNodeWithHistory` in
  `badgerstore_history_node.go`; `ReplaceRelWithHistory` in `badgerstore_history_rel.go`). All 10 were
  fixed together — same root cause, same fix pattern, and leaving 4 sibling doors with the identical
  bug after "fixing" the other 6 would have been exactly the kind of partial fix CLAUDE.md's Code
  Review Meta-Lessons warn against ("Every fix needs a grep audit").

  **Bug**: each door called `bs.appendOps(ops...)` (mutating in-memory state and queuing the entity's
  write ops), and only THEN called `bs.logNodePut(n, withHistory)` / `bs.logRelPut(r, withHistory)` /
  `bs.logRelPutTagged(r, foreignIncoming)` — which internally marshals the change-log body
  (`storepkg.NodePutPayload`/`RelPutPayload`) and only calls `logChangeRaw` on success. If that marshal
  step failed, the door returned the error, but the mutation and the queued write ops had ALREADY
  happened — a change-log-enabled store would then flush a data write with no matching record, a
  silent replica/CDC divergence. The established-safe sibling pattern (already used by `DeleteNode`,
  documented inline: "Build the change-log delete record up front... so a marshal error aborts before
  any op is enqueued") builds the payload BEFORE `idxMu.Lock()` and before any mutation.

  **Fix**: split the marshal-and-emit `logNodePut`/`logRelPut`/`logRelPutTagged` into a pure "build"
  phase and an "emit" phase in `badgerstore_changelog.go`. `buildNodePutPayload(n, withHistory)` /
  `buildRelPutPayload(r, withHistory)` marshal the body and return `([]byte, error)` — safe to call
  before the lock, since they take no lock and mutate no state (mirrors the pre-existing
  `buildChangePayload` used by delete-style doors). `relPutTag(foreignIncoming)` is a new pure helper
  choosing `ChangeForeignIncoming` vs `ChangeRelPut` (previously inlined inside `logRelPutTagged`) —
  also safe to call before the lock since it only depends on the `foreignIncoming` parameter, not any
  post-mutation state. All 10 doors now call the build function immediately after their own entity-wire
  marshal (`bs.marshalNodeBytes`/`bs.marshalRelBytes`/`bs.historyNodeValue`/`bs.historyRelValue`), well
  before `bs.idxMu.Lock()`; if it errors, the door returns immediately with nothing mutated and no op
  enqueued. The lock-held tail of each door now ends with a plain `bs.logChangeRaw(tag, payload)` call
  (using the pre-built payload) instead of the old `logErr := bs.log*(...)` + post-unlock error check —
  `logChangeRaw` itself no-ops when the log is disabled, so behavior for the disabled-log case is
  byte-for-byte unchanged (confirmed by the full pre-existing change-log test suite passing unchanged,
  see below).

  **Doors fixed** (payload built before the lock, emitted via `logChangeRaw` under the lock):
  `PutNode`, `ReplaceNode`, `RemoveNodeLabelToken`, `AddNodeLabelToken` (`badgerstore_node.go`);
  `putRelationship` (shared body for `PutRelationship`/`PutRelationshipCoLocated`/
  `PutRelationshipForeignIncoming`), `ReplaceRelationship` (`badgerstore_rel.go`);
  `RemoveNodeLabelTokenWithHistory`, `AddNodeLabelTokenWithHistory`, `ReplaceNodeWithHistory`
  (`badgerstore_history_node.go`); `ReplaceRelWithHistory` (`badgerstore_history_rel.go`).

  **Tests**: a genuine change-log marshal failure is NOT reachable through the public `Store` API on a
  validated node/relationship — `NodeToWireChecked`/`RelToWireChecked` (the same functions the
  change-log payload builders call) already ran successfully moments earlier during the door's own
  entity-wire marshal on the SAME unchanged object, so by construction the change-log build cannot then
  fail differently (this was already the pre-existing doc comment's own admission: "the wire conversion
  of an already-validated node cannot fail in practice"). Given this, the added regression tests in
  `badgerstore_changelog_buildpayload_test.go` exercise the build phase directly — the exact code path
  every fixed door now runs first — rather than attempting an unreachable through-the-door failure
  injection: `TestBuildNodePutPayload_DisabledLogIsNoOp` / `TestBuildRelPutPayload_DisabledLogIsNoOp`
  (parity: `(nil, nil)` when the log is off, matching every prior no-op behavior exactly),
  `TestBuildNodePutPayload_NilNodeReturnsErrorBeforeAnyLockOrMutation` /
  `TestBuildRelPutPayload_NilRelReturnsErrorBeforeAnyLockOrMutation` (a real, reachable error — nil
  input — proves the build phase surfaces it and that this pure function takes no lock and emits no
  change-log record, i.e. exactly the "abort with nothing touched" property every door needs when its
  build call errors), `TestRelPutTag` (exhaustive 2-case coverage of the tag-selection helper, guarding
  against a future edit silently swapping the `ChangeForeignIncoming`/`ChangeRelPut` branches, which
  would misroute a replica's foreign-incoming-stub apply per ADR-0010).

  RED confirmed via `git stash push -- badgerstore_changelog.go badgerstore_history_node.go
  badgerstore_history_rel.go badgerstore_node.go badgerstore_rel.go` (reverting only the production
  files, keeping the new test file): `go test ./pkg/graph/store/badger/... -run TestBuildNodePutPayload`
  failed to COMPILE (`bs.buildNodePutPayload undefined`, `bs.buildRelPutPayload undefined`,
  `undefined: relPutTag`) — the new build-phase API genuinely does not exist without the fix. Popped the
  stash, confirmed GREEN: all 5 new tests pass. Full `go build ./...` + `go vet ./...` clean;
  `go test ./pkg/graph/store/badger/...` (all 27+ pre-existing tests, including the extensive
  change-log record-parity suite — `TestChangeLog_NodePutRelPutParity`, `TestChangeLog_ReplaceDoors`,
  `TestChangeLog_LabelTokenWithHistoryDoors`, etc. — all pass UNCHANGED, confirming byte-for-byte
  behavior parity across the refactor) pass; `go test -race ./pkg/graph/store/badger/...` clean
  (197s); full repo `go test ./...` clean, including `pkg/graph/store/tiered` and
  `pkg/graph/store/sharded` (both embed the badger backend per shard, so they exercise these doors
  transitively).
- **18b. [FIXED — `badgerstore.go`, `badgerstore_rel.go`, `badgerstore_rel_batch.go`,
  `badgerstore_partial.go`, `badgerstore_history_rel.go`, `v3056_fixes_test.go`,
  `badgerstore_rel_rev_test.go`] Relationships lack a write-generation counter (`relRevs`) for safe
  peek-then-lock revalidation — `ReplaceRelationship` can consume stale state, a Node/Rel parity gap
  (HIGH).** `badgerstore_node.go:258-268,300-314` (node has `nodeRevs`) vs the old
  `badgerstore_rel.go:394-421` (`relDeleteInfoStillIndexedLocked` checked only immutable identity
  fields — relIDs/typeIdx/adjacency membership).

  **Bug**: `ReplaceRelationship` prefetches the current row UNLOCKED (`prefetchRel`), then re-validates
  it's "still current" AFTER acquiring `idxMu.Lock()` via `currentRelForPrefetchLocked`, to avoid a
  second Badger read on the common case. The node side guards this reuse with `nodeRevs` — a counter
  bumped on EVERY node data write (property change, label add/remove, anything), so ANY concurrent
  mutation in the prefetch→lock window is detected and the door falls back to a fresh locked read via
  `getNodeLocked`. The rel side had no such counter: its only staleness check
  (`relDeleteInfoStillIndexedLocked`) verifies STRUCTURAL identity only — `relIDs`/`typeIdx`/adjacency
  membership. Since a relationship's type and endpoints are immutable after creation, a concurrent
  PROPERTY-only `ReplaceRelationship` racing the same window leaves every one of those membership sets
  completely unchanged, so the structural check reported "still current" even when the prefetched row's
  property values were stale. `ReplaceRelationship` then used that stale `old` for
  `bs.maintainRelPropertyIndexesRemove(old, id)` — "removing" a value that, by the time the lock was
  held, was no longer even the indexed one (the concurrent writer's own remove/add cycle had already run
  under the lock moments earlier). Net effect: the concurrent writer's real value could be left as a
  PHANTOM orphaned entry in the rel property index forever — never removed by anyone, since neither
  writer's diff pass ever targeted it.

  **Fix**: added `relRevs map[types.RelID]uint64` + `nextRelRev uint64` fields to `Store`
  (`badgerstore.go`, initialized in the constructor and reset in `Clear()`, alongside `nodeRevs`/
  `nextNodeRev`), plus `bumpRelRevLocked`/`deleteRelRevLocked` helpers in `badgerstore_rel.go` mirroring
  `bumpNodeRevLocked`/`deleteNodeRevLocked` exactly. `bumpRelRevLocked` is called by every door that
  writes a live relationship's data: `putRelationship` (the shared create body for
  `PutRelationship`/`PutRelationshipCoLocated`/`PutRelationshipForeignIncoming`), `ReplaceRelationship`,
  `ReplaceRelWithHistory` (`badgerstore_history_rel.go` — always re-reads fresh via `getRelLocked`
  itself so it isn't vulnerable to the stale-reuse bug, but must still bump so a CONCURRENT
  `ReplaceRelationship`'s prefetch detects ITS write), `PutRelationshipsBatch`
  (`badgerstore_rel_batch.go`), and the TieredStore cross-shard split-write leg `PutRelEntityAndOut`
  (`badgerstore_partial.go`). `deleteRelRevLocked` is called once, inside the single shared low-level
  delete mutator `deleteRelByInfo` (covers `DeleteRelationship`, cascade delete, batch delete, and
  `DeleteRelWithHistory` in one place — the same one-seam property already established for
  `deleteRelByInfo`'s other cleanup) — plus the cross-shard split-delete leg `DeleteRelEntityAndOut`,
  which manages `relIDs` deletion inline rather than through `deleteRelByInfo`. `loadIndexes` (open-time
  rebuild) also seeds a rev for every loaded rel row, mirroring the existing `nodeRevs` seeding, so a
  prefetch on a freshly-opened store doesn't needlessly miss the fast path before its first post-open
  write.

  A new `relPrefetchSnapshot{rel, rev}` type + `prefetchRelWithRev` (rev snapshot taken under the SAME
  brief RLock later compared against — mirrors `prefetchNodeDeleteInfo`) replace the plain
  `*types.Relationship` `ReplaceRelationship` used to pass to `currentRelForPrefetchLocked`, which now
  requires BOTH the rev to still match under the lock AND the existing structural check before reusing
  the prefetched row — otherwise falling back to `getRelLocked`. `RelDeleteInfo`/
  `relDeleteInfoStillIndexedLocked`/`prefetchRelDeleteInfo` (the cascade-delete revalidation machinery)
  were deliberately left UNCHANGED — deletion only needs structural identity, never property-value
  staleness, so that mechanism was already correct for its purpose.

  **Tests** (`badgerstore_rel_rev_test.go`): `TestCurrentRelForPrefetchLocked_StaleRevForcesFreshRead` —
  a white-box unit test of the exact new mechanism: prefetch a rel's rev+row, apply a real (single-
  threaded, deterministic) concurrent `ReplaceRelationship` that changes a property, then prove
  `currentRelForPrefetchLocked` returns the FRESH value, not the stale prefetch.
  `TestCurrentRelForPrefetchLocked_UnchangedRevReusesPrefetch` — the fast-path-preserved counterpart (no
  concurrent write ⇒ the prefetched pointer IS reused, proving the fix didn't regress the whole point of
  prefetch-then-lock). `TestReplaceRelationship_ConcurrentPropertyUpdateInPrefetchWindowIsNotStale` — a
  true end-to-end regression through `ReplaceRelationship` itself, using a new
  `replaceRelPrefetchTestHook` (mirrors the existing `historyScanTestHook` pattern — nil in production,
  zero overhead) fired right after the real prefetch and before the real `idxMu.Lock()`, to
  deterministically land a concurrent property-changing `ReplaceRelationship` call inside the actual
  racy window; asserts the rel property index reflects ONLY the true final value afterward, with no
  phantom stale entries for either intermediate value. Also updated the pre-existing
  `TestBadgerStore_RelPrefetchFallsBackAfterIndexedStateChange` (`v3056_fixes_test.go`) to use
  `prefetchRelWithRev`/the new `relPrefetchSnapshot`-typed `currentRelForPrefetchLocked` signature — it
  still passes unchanged in behavior (delete+recreate is detected by BOTH the pre-existing structural
  check and the new rev check).

  RED confirmed via `git stash push -- badgerstore.go badgerstore_partial.go badgerstore_rel.go
  badgerstore_rel_batch.go` (the 4 files containing the new field/type/function definitions;
  `badgerstore_history_rel.go`'s one-line `bumpRelRevLocked` call was left in place since it doesn't
  affect the RED signal — the helper it calls was still removed by the stash): compile failure
  (`bs.prefetchRelWithRev undefined`, `bs.replaceRelPrefetchTestHook undefined`, plus a knock-on failure
  in `badgerstore_history_rel.go`/`badgerstore_rel.go` from the also-reverted 18a functions living in
  the same stashed files). Popped the stash, confirmed GREEN: all 3 new tests + the updated pre-existing
  test pass. Full `go build ./...` + `go vet ./...` clean; `go test ./pkg/graph/store/badger/...` (full
  package) passes; `go test -race ./pkg/graph/store/badger/...` clean (194s — this is exactly the class
  of concurrency-sensitive fix rule 7-style verification exists for); full repo `go test ./...` clean.
  While running the full-suite check, discovered and fixed an UNRELATED pre-existing flaky test
  (`TestPurgeRangeAllChunks_ReapsForeverOwnersOnMidRangeAbort`, BACKLOG 13a) — see that entry's
  follow-up note.
- **18c. [FIXED — `badgerstore_partial.go`, `badgerstore_partial_changelog_test.go`,
  `tiered/tieredstore_changelog_crossshard_rel_test.go`] Cross-shard partial relationship write/delete
  doors (TieredStore split-write) never emit change-log records — silent replica/CDC divergence when
  `ChangeLog` is enabled (HIGH).** `badgerstore_partial.go:39-249`, used by
  `tiered/tieredstore_write_rel.go`.

  **Bug**: `PutRelEntityAndOut`/`PutRelIncoming`/`DeleteRelEntityAndOut`/`DeleteRelIncoming` — the four
  split-write helpers a cross-shard relationship (start/end nodes routing to different shards) uses
  instead of the single-shard `PutRelationship`/`DeleteRelationship` — all mutated state and called
  `bs.appendOps(...)` but never called `bs.logChangeRaw`. A cross-shard rel create/delete was therefore
  COMPLETELY invisible to a change-log-enabled store's feed: not degraded, not delayed — never recorded
  at all. Confirmed by grep: `ChangeRelPut`/`ChangeRelDelete` are emitted by the single-shard doors in
  `badgerstore_rel.go` and nowhere in `badgerstore_partial.go` or `tiered/tieredstore_write_rel.go`. A
  doc comment in `tiered/tieredstore_write_history.go` (`DeleteNodeCascade`'s cross-shard branch)
  claims the per-relationship split-delete path "emits one ChangeRelDelete per edge" — that claim was
  simply WRONG for the actual code (it delegates to `ts.DeleteRelationship`, whose cross-shard branch
  calls exactly these record-free partial doors) and is now made true by this fix.

  **Fix**: `PutRelEntityAndOut` and `DeleteRelEntityAndOut` — the ENTITY-bearing legs (the shard holding
  the full relationship row: start/end/type/properties) — now build and co-commit a `ChangeRelPut` /
  `ChangeRelDelete` record exactly like the single-shard `PutRelationship`/`DeleteRelationship` doors,
  using the SAME `buildRelPutPayload`/`buildChangePayload` build-before-lock helpers BACKLOG 18a
  introduced. `PutRelIncoming`/`DeleteRelIncoming` — the companion in/-index-ONLY legs on the endpoint's
  shard, which carry no entity data (just start/end/type as raw index bytes) — deliberately stay
  record-free, with a doc comment explaining why: the entity leg's record already carries everything a
  replica needs to reconstruct the WHOLE relationship (a replica applying `ChangeRelPut` re-derives both
  split legs via its own routing — the same split any primary/replica would run — exactly like a
  single-shard write's one record already covers all 4 keys). This mirrors the design of the existing
  `ChangeForeignIncoming` tag (a DIFFERENT cross-machine mechanism, ADR-0010, used by the SEPARATE
  `sharded.Store` backend — confirmed by grep that `sharded/rel.go` explicitly avoids
  `PutRelEntityAndOut`/`PutRelIncoming` for exactly this record-emission reason, since ADR-0007's
  `sharded.Store` co-locates a rel's entity + both adjacency legs on ONE shard and never needs this
  split at all — these partial doors are TieredStore-specific).

  **Known consequence (in scope, matches existing precedent, not a new problem)**: these same doors are
  also used by `tiered/tieredstore_write_archive.go`'s `migrateRelationshipPlacement` (moving a rel's
  physical shard placement during node archive/restore) — so an archival move will now ALSO emit a
  `ChangeRelPut`+`ChangeRelDelete` pair for every migrated relationship, just as node archival ALREADY
  does today via `archive.PutNode(node)` + `ref.DeleteNodeCascade(...)` (unconditionally, unrelated to
  this fix). This is bringing rels to PARITY with the pre-existing node-archival behavior, not
  introducing a new class of feed noise; whether archival's redundant-but-idempotent put/delete pairs
  are the RIGHT way to represent a pure storage-tier move to a replica is a pre-existing, orthogonal
  design question (affecting nodes today, independent of this fix) — out of scope for this finding,
  which is specifically "never emit records at all."

  **Tests**: `badgerstore_partial_changelog_test.go` — badger-level unit tests exercising all 4 doors
  directly with `ChangeLog` enabled, mirroring `badgerstore_changelog_test.go`'s established
  single-final-`drainFeed`-plus-exact-tag-sequence convention (`ChangeFeed(0,0)` always reads from the
  beginning, so incremental "drain then check new count" is wrong — confirmed by an early draft of
  these tests failing with inflated counts until rewritten to this pattern):
  `TestPutRelEntityAndOut_EmitsChangeRelPut` (asserts the tag sequence AND decodes the payload —
  correct ID, `WithHistory=false`), `TestPutRelIncoming_EmitsNoChangeRecord`,
  `TestDeleteRelEntityAndOut_EmitsChangeRelDelete` (decodes payload — correct ID, no tombstone),
  `TestDeleteRelIncoming_EmitsNoChangeRecord`. Plus a TieredStore-level end-to-end regression,
  `tiered/tieredstore_changelog_crossshard_rel_test.go`'s
  `TestTieredPutRelationship_CrossShardEmitsChangeRecords`: builds a genuine cross-shard relationship
  (an event-shard node → a reference-shard node, the natural "E→R" split-write branch — verified via
  `shardForNodeID` that the two nodes really do land on different `*BadgerStore` instances, not
  assumed), drives it through the PUBLIC `ts.PutRelationship`/`ts.DeleteRelationship` API (not the
  badger-level helpers directly), and asserts the TieredStore's own merged `ChangeFeed` shows the
  `ChangeRelPut`/`ChangeRelDelete` records.

  RED confirmed for both layers via `git stash push -- badgerstore_partial.go` (reverting only the
  production fix, keeping all new tests): the 2 "emits a record" tests failed with the correct tag
  sequence missing the `RelPut`/`RelDelete` entry, while the 2 "emits no record" tests correctly PASSED
  unchanged (that behavior was never broken) — confirming the tests discriminate precisely; the tiered
  integration test failed with "2 records, want 3" (only the 2 `NodePut`s, no `RelPut`). Popped the
  stash, confirmed GREEN on all 5 new tests. Full `go build ./...` + `go vet ./...` clean;
  `go test ./pkg/graph/store/badger/... ./pkg/graph/store/tiered/...` clean;
  `go test -race ./pkg/graph/store/badger/... ./pkg/graph/store/tiered/...` clean (209s / 55s); full
  repo `go test ./...` clean.
- **18d. [FIXED — `badgerstore_labeltxmembers.go`, `badgerstore_labeltxmembers_delta_test.go`] K1
  label/rel-type membership sidecar silently drops delta-encoded history rows — unsound superset under
  `HistoryDeltaEncoding` (HIGH).** `badgerstore_labeltxmembers.go:142-165,212-235`.

  **Bug**: `ensureLabelTxMembersBuilt`/`ensureRelTypeTxMembersBuilt` (the lazy build for the K1 pinned-
  scan candidate-pruning sidecar) decoded EVERY row in the node/rel current AND history keyspaces via a
  bare `storepkg.SafeUnmarshal(val, &w)` into a plain `NodeWire`/`RelWire`, in BOTH the committed-Badger
  scan and the pending-write-buffer-overlay scan (4 call sites total). Under `HistoryDeltaEncoding`
  (ADR-0009/B6), a non-anchor history row (up to `HistoryAnchorInterval-1` out of every
  `HistoryAnchorInterval` versions — 15/16 by default) is stored as a 1-byte `'D'`-tagged DELTA, not a
  raw struct marshal — its first byte (`0x44`) is never a valid msgpack map header, so
  `SafeUnmarshal` fails on it every time. The failure was silently swallowed ("skip an unreadable row;
  the fold fallback stays correct") — but that comment describes the WRONG invariant: once
  `labelTxMembersBuilt`/`relTypeMembersBuilt` flips true, the sidecar becomes the ONLY candidate source
  a pinned `ByLabel`/`ByType` temporal scan consults (pruning the full-history fold is the entire reason
  the sidecar exists), so a node/rel whose ONLY label/type evidence was a delta-encoded history row was
  silently never recorded as a member. The sidecar's documented contract is a SOUND SUPERSET
  (over-inclusion is safe — the chain resolver is the correctness authority downstream; under-inclusion
  is NOT safe — it lets a real match silently disappear from a pinned scan), so this broke the contract
  in the direction that actually matters.

  **Fix**: two new delta-aware decode helpers, `decodeNodeWireForMembership`/
  `decodeRelWireForMembership`, replace the bare `SafeUnmarshal` at all 4 call sites (committed scan +
  pending overlay, node + rel). They branch on `storepkg.HistoryValueKindOf(val)`: `HistoryFull` →
  `SafeUnmarshal` exactly as before (current rows 0x01/0x02 are always full, never delta-tagged, so this
  path is unchanged for them); `HistoryDelta` → `storepkg.DecodeNodeHistoryDelta`/`DecodeRelHistoryDelta`
  and use `d.Meta` — NO anchor read needed, because `DiffNodeHistory`/`DiffRelHistory` build a delta's
  `Meta` as `target` with `Properties` cleared (`meta := target; meta.Properties = nil`), so every
  non-property field — including `PrimaryLabel`/`ExtraLabels`/`RelType`/`TxFrom` — is carried in `Meta`
  VERBATIM regardless of whether it differs from the anchor. This mirrors the pattern the pre-existing
  `historyNodeTemporal`/`historyRelTemporal` helpers already use for as-of classification (per their own
  doc comment: "a delta carries the full temporal block verbatim in its Meta, so no anchor read is
  needed"), specialized here to a leaner shape (raw wire, not a fully decoded/registry-resolved entity —
  membership only needs label tokens + TxFrom, and calling the heavier
  `decodeNodeHistoryWireForKey`/registry-resolution machinery would be unnecessary overhead for a scan
  that runs over the WHOLE keyspace once).

  **Tests** (`badgerstore_labeltxmembers_delta_test.go`): a shared harness
  (`deltaMembershipTestNode`/`deltaMembershipTestRel`, mirroring `badgerstore_history_delta_test.go`'s
  established `deltaVersionNode` pattern — a large unchanging blob property so a delta reliably wins on
  size, confirmed by construction not assumption) builds a node/rel with NO current row and exactly two
  history-only versions written via `PutNodeVersion`/`PutRelVersion`: v0 (anchor, WITHOUT the target
  label/type — 0 % `HistoryAnchorInterval` == 0 is always an anchor by construction) and v1 (delta,
  WITH the target label/type). This isolates the exact defect: the label/type's ONLY evidence anywhere
  is the v1 DELTA row — v0 deliberately does NOT carry it, so a test that (bug-for-bug) only checked "is
  membership recorded at all" could pass by accident via v0's fine anchor read; requiring the evidence
  to come SPECIFICALLY from the delta closes that gap. Each test self-verifies via
  `storepkg.HistoryValueKindOf` that v1 really was stored as a delta before asserting anything about
  membership (a size regression elsewhere that silently reverted to full-snapshot fallback would
  otherwise make the test pass for the wrong reason). Four tests (Testing Rule 2 — Node/Rel parity —
  plus the two independent code paths within each):
  `TestEnsureLabelTxMembersBuilt_DeltaEncodedCommittedRowIsNotDropped` /
  `TestEnsureLabelTxMembersBuilt_DeltaEncodedPendingRowIsNotDropped` (flushed vs. still-pending —
  the SAME bug existed independently in both the committed-Badger-scan branch and the
  pending-write-buffer-overlay branch, so both needed independent coverage) and their
  `TestEnsureRelTypeTxMembersBuilt_*` mirrors.

  RED confirmed via `git stash push -- badgerstore_labeltxmembers.go`: all 4 tests failed with "…was not
  recorded as a member — BACKLOG 18d regression" (a genuine behavioral failure, not a compile error,
  since the test file only calls the pre-existing public `ForEachLabelTxMember`/`ForEachRelTypeTxMember`
  doors). Popped the stash, confirmed GREEN on all 4. Full `go build ./...` + `go vet ./...` clean;
  `go test ./pkg/graph/store/badger/...` clean; `go test -race ./pkg/graph/store/badger/...` clean
  (201s); full repo `go test ./...` clean.
- **18e. [FIXED — `store/badger/badgerstore_node.go`, `store/badger/badgerstore_history_node.go`,
  `store/badger/badgerstore_node_batch.go`] Vector-index apply happened AFTER other RAM mutations
  were already committed — a failure would leave cache/indexes reflecting an unpersisted state,
  violating lesson 4's "preflight then apply" (MEDIUM).** `badgerstore_node.go:73-76,478-481,572-575,
  668-671`, `badgerstore_node_batch.go:582-585` (plus the 3 mirrored history-writing doors in
  `badgerstore_history_node.go` the finding's line numbers didn't enumerate). In `ReplaceNode` the old
  vector-index entry was already removed before a possible failure — the node would lose its
  vector-index entry entirely while other indexes showed the unpersisted new row.

  **Confirmed mechanism.** All 8 call sites (`PutNode`, `ReplaceNode`, `RemoveNodeLabelToken`,
  `AddNodeLabelToken` in `badgerstore_node.go`; `RemoveNodeLabelTokenWithHistory`,
  `AddNodeLabelTokenWithHistory`, `ReplaceNodeWithHistory` in `badgerstore_history_node.go`; the
  per-node loop in `PutNodesBatch`'s Phase 2 in `badgerstore_node_batch.go`) shared the identical
  shape: `indexpkg.PrepareNodeVectorIndexUpdates` (the validating preflight) ran early and correctly,
  but `indexpkg.AddPreparedNodeToVectorIndexes` (the apply) ran LAST — after cache puts, label-index
  mutation, `nodeHashes`/rev-bump, and property/temporal/high-frequency-index adds had already
  committed to RAM. Since none of `bs.appendOps`/`bs.logChangeRaw` (the durable-write enqueue) had run
  yet at that point, a failed apply would return an error to the caller while leaving every other
  in-memory index/cache reflecting a row that was never queued for durable persistence — a genuine
  RAM/durable-store divergence class, not merely cosmetic.

  **Reachability under current invariants (investigated in depth).** Traced `AddPreparedNodeToVectorIndexes`
  → `VectorIndex.AddOwned` → `addLocked` (`internal/index/vector_index.go`): it re-validates
  `vi.Dims`/`vi.Metric` (`ValidateVectorIndexConfig`), vector length, and vector values — the exact
  same checks `PrepareNodeVectorIndexUpdates` already ran — then unconditionally returns `nil` from
  every remaining code path (`ensurePositionsLocked`/`ensureHNSWLocked`/HNSW insert have no error
  return). At every one of the 8 call sites, `bs.idxMu` is held continuously from before Prepare
  through after Apply (no unlock in between), `bs.vectorIndexes`'s `*VectorIndex` pointers cannot be
  swapped mid-call (DDL also requires `bs.idxMu`), `VectorIndex.Dims`/`Metric` are set once at
  construction and never mutated afterward (grepped every assignment site), and the `vec` slice
  captured during Prepare is the exact slice passed to Apply. Given all of that, a successful Prepare
  provably implies a successful Apply under the codebase as it stands today — the error return on
  `AddPreparedNodeToVectorIndexes` is currently dead on every real call path, which is also why no
  regression test can honestly force the RAM-divergence outcome through the public Store API (or even
  a same-package white-box test) without fabricating a scenario that contradicts the locking
  discipline actually in force.

  **Fix (defensive, zero behavior change).** Reordered all 8 sites so `AddPreparedNodeToVectorIndexes`
  runs immediately after its own `PrepareNodeVectorIndexUpdates` (or immediately after the paired
  `RemoveNodeFromVectorIndexes` purge at the 6 update/history sites, preserving the required
  remove-before-add ordering for the SAME vector-index key), before any other RAM mutation begins. The
  batch `PutNodesBatch` Phase-2 loop got the same per-node reordering. This closes the structural gap
  for any FUTURE failure mode that might be added inside `addLocked` (e.g. a capacity/memory guard),
  without weakening today's error handling or touching `RemoveNodeFromVectorIndexes`'s blanket-purge
  semantics (relied on unconditionally by `DeleteNode`/cascade-delete paths, so making it selective
  was explicitly out of scope for this fix).

  **Verification.** `go build ./...` and `go vet ./...` clean. `go test ./pkg/graph/store/badger/...`
  green (all pre-existing vector-index + node-write tests pass unchanged, confirming the reorder is
  behavior-preserving on every currently-reachable path). `go test ./pkg/graph/...` green (all 27
  packages). `go test -race ./pkg/graph/store/badger/...` green (196s). No new regression test was
  added: per the reachability analysis above, constructing one would require injecting a fault that
  cannot occur through any real call path today, which would make the test assert against fabricated
  behavior rather than a genuine defect. If `addLocked` ever grows a real post-validation failure
  mode, this ordering is what makes that future failure safe — that is the entire value of the fix.
- **18f. Meta/registry persistence (`MetaSet`, `Save*Registry`) lacks the immediate pre-call
  `dbClosed` guard that `flush()` uses (MEDIUM, the same forever-block class CLAUDE.md calls
  "hard-won").** `badgerstore_meta.go:169,190,239,317,335`.
- **18g. [FIXED — `store/badger/badgerstore_rel.go`, `store/badger/badgerstore_rel_batch.go`,
  `store/badger/badgerstore_node_batch.go`, `store/badger/badgerstore_history_node.go`,
  `store/badger/badgerstore_rel_delete_reuse_disk_test.go`] `DeleteRelationshipsBatch`'s staleness
  check skipped adjacency verification entirely in `AdjacencyIndexOnDisk` mode — a rel-ID-reuse-with-
  different-endpoints race (lesson 22's classic case) could orphan the new rel's real adjacency
  entries (MEDIUM).** `badgerstore_rel_batch.go:216-238`, `badgerstore_rel.go:394-421`.

  **Bug.** `relDeleteInfoStillIndexedLocked`'s doc comment claimed "Disk mode keeps no adjacency RAM
  mirror to verify against; relIDs + typeIdx membership above remain the TOCTOU currency check" — but
  that claim is false for a delete-then-recreate-with-the-same-rel-ID-but-different-endpoints race
  (rel IDs can be caller-supplied via `AddByID`/`AddByIDIfAbsent`/`AddByIDForeignEnd`, so reuse is not
  hypothetical). `relIDs[rid]` existing and `typeIdx[relType]` containing `rid` are BOTH still true
  after the reused rel is recreated with the same type — the exact scenario the check exists to catch
  is invisible to it once `!bs.adjOnDisk`'s extra `outIdx`/`inIdx` membership check is compiled out.
  Three TOCTOU call sites relied on this alone: `DeleteRelationshipsBatch` (`badgerstore_rel_batch.go`),
  the cascade-delete preflight (`cascadeDeleteInner` in `badgerstore_node_batch.go`), and the
  with-history tombstone-completeness check (`validateDeleteNodeRelTombstonesLocked` in
  `badgerstore_history_node.go`). A hit on the stale fast path would delete the REUSED relationship's
  row while cleaning up adjacency for the STALE (original) endpoints — orphaning the reused rel's real
  on-disk adjacency entries permanently (nothing else would ever clean them up) while writing a
  phantom delete op for endpoints that no longer own that rel ID. The fourth TOCTOU call site,
  `currentRelForPrefetchLocked` (used by `ReplaceRelationship`), was ALREADY safe — its
  `relPrefetchSnapshot.rev` gate (BACKLOG 18b) wraps every call to `relDeleteInfoStillIndexedLocked`,
  so it never reached this blind spot.

  **Fix.** Added `RelDeleteInfo.Rev` (populated only by the new `prefetchRelDeleteInfo`, which now
  also snapshots `bs.relRevs[rid]` under a brief `RLock` — mirroring `prefetchRelWithRev`/BACKLOG 18b)
  and a new `relDeleteInfoRevCurrentLocked(info)` helper that reports whether the live rev for that
  rel ID still matches the prefetch-time rev. `relRevs` is bumped on every create (including a
  recreate after delete — `bumpRelRevLocked` draws from a single monotonic `nextRelRev` counter that
  never repeats a value for the store's lifetime) and the map entry is deleted entirely on every
  delete (`deleteRelRevLocked`), so ANY write to that specific rel ID between prefetch and the locked
  re-check — property update, delete, or delete+recreate with a reused ID — changes what
  `bs.relRevs[rid]` reads back as. All three vulnerable call sites now gate their fast path on
  `bs.relDeleteInfoRevCurrentLocked(info) && bs.relDeleteInfoStillIndexedLocked(info)`, the same
  two-check composition `currentRelForPrefetchLocked` already used successfully (just with its own
  independent rev already in scope there). `relDeleteInfoStillIndexedLocked` itself was left
  unchanged — the fix closes the gap at the call sites rather than inside the shared structural check,
  so `!adjOnDisk` behavior (which was already correct) is untouched.

  **Tests.** `TestRelDeleteInfoStillIndexedLocked_DiskMode_BlindToEndpointReuse` proves the
  vulnerability directly: in `AdjacencyIndexOnDisk` mode, `relDeleteInfoStillIndexedLocked` ALONE
  still reports a stale prefetch (endpoints 1→2) as "still indexed" after a delete+recreate with the
  same rel ID and different endpoints (3→4) — then asserts the fix's `relDeleteInfoRevCurrentLocked`
  correctly reports it stale. `TestRelDeleteInfoRevCurrentLocked_UnchangedRevReusesPrefetch` is the
  non-regression counterpart (no concurrent write → both checks still pass, fast path preserved).
  `TestDeleteRelationshipsBatch_DiskMode_ReusedIDDoesNotOrphanNewAdjacency` is the door-level
  end-to-end regression through the real `DeleteRelationshipsBatch` door, asserting the reused
  relationship's real adjacency (nodes 3→4) is fully cleaned and the row is gone.

  RED confirmed via `git stash push` on all 4 production files (test-only diff remaining): `go vet`
  failed to compile (`prefetched.Rev undefined`) — the test is load-bearing on the fix's new
  field/helper, not just a behavioral assertion. Popped the stash, confirmed GREEN on all 3 new tests
  plus the full existing `TestCurrentRelForPrefetchLocked_*` suite (BACKLOG 18b, unaffected). Full
  `go build ./...` + `go vet ./...` clean; `go test ./pkg/graph/store/badger/...` clean (25s);
  `go test -race ./pkg/graph/store/badger/...` clean (194s); full repo `go test ./...` clean
  (including tutorials).
- **18h. [FIXED — `store/badger/badgerstore_node_batch.go`,
  `store/badger/badgerstore_cascade_corruption_epoch_test.go`] `cascadeDeleteInner`'s
  corruption-fallback branch skipped per-label DocValues epoch invalidation (MEDIUM, corruption-only
  reachability, same bug class already fixed once for `purgeNodesByLabel` but not carried here).**
  `badgerstore_node_batch.go:149-203`.

  **Bug.** When a node is present in `bs.nodeIDs` but its row can't be loaded (data corruption, or a
  cache/DB inconsistency), `cascadeDeleteInner`'s fallback branch brute-force purges label/property/
  temporal/vector indexes and removes the node from every in-memory liveness map — but never bumped
  `bs.nodeEpochSalt`, unlike `purgeNodesByLabel`'s identical bulk-delete case (which already does
  `defer bs.nodeEpochSalt.Add(1)` as belt-and-braces invalidation, BACKLOG 4b). Without the bump, a
  columnar DocValues query could keep answering from a per-label column snapshot still containing this
  node's now-deleted row. (Investigated the "type-class counters permanently stale" half of the
  original finding text: the surrounding code already has an explicit, deliberate comment — "leave
  property-key counters conservatively positive: without the row we cannot know which keys to
  decrement, and positive overcounts only cause extra scans while undercounts could make planners
  prune matches" — confirming that half is an existing, documented, ACCEPTED tradeoff, not a bug; the
  finding's own suggested fix (`add bs.nodeEpochSalt.Add(1)`) targets only the epoch-bump gap, which is
  the part this fix addresses.)

  **Fix.** Added `bs.nodeEpochSalt.Add(1)` immediately before the fallback branch's return, mirroring
  `purgeNodesByLabel`'s precedent exactly — the label-index scrub in this branch touches an UNKNOWN set
  of labels (a scan over every label bucket, not a single known label), so the coarse salt-wide
  invalidation is the correct granularity, not a single per-label epoch bump.

  **Test** (`store/badger/badgerstore_cascade_corruption_epoch_test.go`, new file):
  `TestCascadeDeleteInner_CorruptionFallback_BumpsNodeEpochSalt` — reproduces the corruption directly
  (package-internal test): seeds `bs.nodeIDs` with an entry that has NO underlying badger row at all
  (never written), forcing `getNodeLocked` to fail exactly like a corrupted/missing row would, then
  drives `cascadeDeleteLocked` (the `idxMu`-locked wrapper) into the fallback branch and asserts
  `nodeEpochSalt` increased by exactly 1, plus confirms the node is genuinely gone from `bs.nodeIDs`.

  **Verification.** RED confirmed via a scripted temporary revert (saved as `.bak`, restored after —
  avoiding git stash's file-level granularity risk of reverting unrelated pre-existing changes
  co-located in the same file, as encountered earlier in this session for 18i): the test failed with
  `nodeEpochSalt = 0, want 1`. Restored from `.bak`, GREEN confirmed. Full `go build ./...` and `go vet
  ./...` clean. `go test ./pkg/graph/store/badger/...` full package pass. `go test -race
  ./pkg/graph/store/badger/... -run 'TestCascadeDelete|TestDeleteNode'` clean. Full-repo `go test ./...`
  clean — no regressions.
- **18i. [FIXED — `store/badger/badgerstore.go`, `store/badger/badgerstore_changelog.go`,
  `store/badger/badgerstore_flush.go`, `store/badger/badgerstore_node_batch.go`,
  `store/badger/badgerstore_rel_batch.go`, `store/badger/badgerstore_retention_purge.go`,
  `store/badger/badgerstore_changelog_race_test.go`] `bs.logEnabled` was read without synchronization
  while written under `wbMu` — a real data race per Go's memory model (MEDIUM).**
  `badgerstore.go:643`, many sites in `badgerstore_changelog.go`.

  **Bug.** `logEnabled` was a plain `bool` field. `DisableChangeLog`/`EnableChangeLog`/
  `EnableChangeLogWithSource` all wrote it under `bs.wbMu.Lock()`. But the PUBLIC accessor
  `ChangeLogEnabled()` (`store.ChangeLogStatusCapability`, called externally — e.g. by
  `internal/core`'s `changeLogStatusEnabled`, or a tiered store's cross-shard status checks) read it
  with NO lock at all. A concurrent `ChangeLogEnabled()` call racing any of the three mutators (e.g.
  during a tiered store's `RecoverChangeLog` path on one shard while another goroutine checks status)
  is a genuine data race under Go's memory model — undefined behavior, not just a benign stale read.

  **Fix.** Changed `logEnabled` from `bool` to `atomic.Bool` and converted every read/write site (~24
  occurrences across 6 files: the internal write-path gates that were already under `wbMu`, plus the
  externally-called `ChangeLogEnabled()`/`DisableChangeLog()`/`EnableChangeLog()`/
  `EnableChangeLogWithSource()`) to `.Load()`/`.Store()`. `logConfigured` (the sibling field) was left
  as a plain `bool` — confirmed via grep it's only ever read/written under `wbMu` or at construction
  time (before any concurrent access is possible), so it has no external unsynchronized reader. The
  struct-literal constructor couldn't set `logEnabled` directly (an `atomic.Bool`'s internal field is
  unexported even within the same package), so the initial value is now set via `bs.logEnabled.Store(...)`
  immediately after construction, before `bs` becomes reachable by any other goroutine.

  **Test** (`store/badger/badgerstore_changelog_race_test.go`, new file):
  `TestChangeLogEnabled_ConcurrentWithToggle_NoRace` — one goroutine loops `DisableChangeLog()`/
  `EnableChangeLog()`, a second concurrently loops `ChangeLogEnabled()`, both gated by a shared
  `context.WithTimeout` (a closed channel broadcasts to both goroutines, unlike a single-shot
  `time.Timer` channel which only one `select` can ever receive from).

  **Verification.** RED confirmed WITHOUT git stash (stashing the touched files risked reverting
  unrelated pre-existing uncommitted changes co-located in the same files from earlier in this
  session — confirmed by an aborted attempt that broke unrelated `buildNodePutPayload`/
  `buildRelPutPayload` helpers) — instead, a scripted temporary revert (saved via `.bak`, restored
  after) flipped just the `logEnabled`-related lines back to a plain `bool` across all 6 files.
  `go test -race` against that reverted state failed IMMEDIATELY with `WARNING: DATA RACE` naming the
  exact write (`EnableChangeLog`) and read (`ChangeLogEnabled`) sites the fix targets. Restored from
  `.bak`, GREEN confirmed. Full `go build ./...` and `go vet ./...` clean. `go test -race
  ./pkg/graph/store/badger/...` — full package pass under the race detector (189s, no other races
  found). Full-repo `go test ./...` clean — no regressions.
- **18j. `Config.ZSTDCompressionLevel` documented as bounded [1,15] but never validated, unlike every
  other tuning knob (LOW-MEDIUM).** `badgerstore.go:246-249` vs `badgerstore_options.go:73-95`.
- **18k. `NodesAsOf`/`RelsAsOf` open one Badger read transaction PER ENTITY instead of sharing one
  across the scan — O(N) independent transactions for a graph-wide as-of query (MEDIUM, perf).**
  `badgerstore_txtime.go:285-333,336-384`.
- **18l. `PutRelEntityAndOut`/`DeleteRelEntityAndOut` skip rel property-index and type-class-count
  maintenance — currently harmless (TieredStore declines both) but these are exported `Store` methods
  any direct caller could invoke (LOW-MEDIUM).** `badgerstore_partial.go`.
- **18m. Oversized-WAL migration guard doesn't cover the "explicit `MemTableSize` reverted to 0
  (stock)" transition — could reproduce the lesson-45 crash via a different input path than the one
  that's tested (LOW-MEDIUM).** `badgerstore.go:720-762`.
- **18n. `ForEachNodeByLabel`'s callback runs inside an open Badger read transaction, contradicting
  its own doc comment ("fn runs WITHOUT any store lock held") — not a deadlock risk but pins Badger's
  min read timestamp for the whole scan, inhibiting value-log GC (LOW-MEDIUM, undocumented
  operational tradeoff).** `badgerstore_node_scan.go:56-77`.
- **18o. Property/temporal-index-on-disk backfill commits lack the `dbClosed` guard used everywhere
  else — currently unreachable but a landmine for a future refactor (LOW).**
  `badgerstore_property_disk.go:680-693`, `badgerstore_temporal_disk.go:189-203`.
- **18p. `CollectShardDropResidue` requires `checkWritable()` despite being documented as read-only
  ("mutates nothing") — possibly deliberate but undocumented (LOW).** `badgerstore_shard_drop.go:23-26`.
- **18q. Code smells: duplicated ad-hoc "closed" checks instead of `checkOpen()` (`badgerstore_meta.go`,
  `badgerstore.go`); orphaned doc comment for a function living in a different file
  (`badgerstore_rel_batch.go:266-267`); ~20 sites compare `err == badgerv4.ErrKeyNotFound` directly
  instead of `errors.Is`, vs. ~4 using `errors.Is` (lesson 12 convention, may be accepted house style)
  (all LOW).**
- **18r. `EncryptionKeyRotation` negative values silently ignored rather than rejected, inconsistent
  with the fail-closed pattern applied to every other numeric knob (LOW).**
  `badgerstore_options.go:97-115,169-171`.
- **18s. Lesson-68 regression coverage relies on nondeterministic Go map iteration rather than a
  deterministic adversarial construction — has only some probability per run of catching a regression
  (MEDIUM, TEST-GAP).** `internal/core/history_delta_test.go:41-61`.
- **18t. No direct self-loop round-trip test at the raw Store layer; no test pins change-log-marshal-
  failure-mid-write behavior (relevant to 18a); no adversarial test proves the frozen-row guard
  actually rejects mutation on an owned/ingest-transferred cache entry (TEST-GAP, 3 gaps).**
- **18u. Missing-feature (likely intentional): no zero-copy ownership-transfer cache path
  (`freezeRelForCache`) for bulk relationship writes, unlike nodes' `freezeNodeForCache` — see
  BACKLOG 21.**
- **18v. Vector-index apply-order (18e) and every other item in this section were cross-verified by
  two independent review passes on the same code (part A + part B); items not listed here (e.g.
  `dbClosed` guard on the primary `flush()` path, wire-format-version enforcement, encryption
  validation, `NodeAsOf`/`RelAsOf` pending-overlay-before-view ordering) were explicitly verified
  correct.**

### BACKLOG 19 — TieredStore hardening

- **19a. [FIXED — store/tiered/retention_purge_drop.go, retention_purge_drop_test.go] `Close()` races the cold-shard-drop drain protocol on `ts.eventShards` — concurrent map
  read/write, a genuine FATAL unrecoverable crash (CRITICAL).** `tieredstore.go:679-683,688-698`
  ranges under `lifecycleMu` only; `retention_purge_drop.go:91-97,110-112` (`dropOneShard`) mutates
  the same map under `ts.mu.Lock()` only, never `lifecycleMu`. A `PurgeNodesByLabelBefore` drain
  racing an operator `Close()` crashes the process. Fix: route the drop path through
  `beginSequentialStoreWideOperation()` like every sibling mutator.
- **19b. [FIXED — store/tiered/retention_purge_drop.go, retention_purge_drop_test.go] `dropOneShard` destroys the shard directory before the catalog removal is durably persisted —
  no rollback, unlike every sibling catalog mutator (CRITICAL).** `retention_purge_drop.go:136-153`
  orders `close → RemoveAll → catalog.RemoveShard → catalog.Save`. A crash between `RemoveAll` and
  `catalog.Save()` leaves a catalog entry pointing at a deleted directory — bricks `New()` entirely for
  a warm shard, or leaves a permanent zombie catalog entry for a cold one. `shard_catalog.go`'s own
  doc comment says to pair `RemoveShard` with `snapshotShards()`/`restoreShards()`, which this doesn't
  do. Fix: persist the catalog removal (or a durable tombstone) before/atomically with the physical
  delete; add a startup repair path reconciling a catalog entry with a missing directory.
- **19c. [FIXED — store/tiered/tieredstore_changelog.go, tieredstore_changelog_reorder_test.go] Barrier + W-bound change-feed watermark may not be safe under a genuinely concurrent writer —
  "Finding-1" (the cross-shard flush-reordering silent-loss class CLAUDE.md documents as already
  closed) may not actually be closed (CRITICAL/HIGH, traced via code, not reproduced).**
  `tieredstore_changelog.go:300-352`. Sequential flush pass then a separate sequential max-LSN pass,
  no lock excluding new mints during either pass, each shard's background `flushLoop` uncoordinated
  with the barrier — a numerically-lower LSN on one shard can still be un-flushed when a
  numerically-higher LSN elsewhere is already durable, so `w = max(...)` pages past the gap and a
  consumer can never see the lower LSN again.
- **19d. [FIXED — TEST-GAP, `retention_purge_drop_concurrent_writer_test.go`] No genuine concurrent-
  writer test exists for either the drain protocol or the change-feed barrier — the exact adversarial
  tests both designs call for are missing (HIGH, TEST-GAP, root cause of 19a-19c shipping undetected).**
  Test scaffolding already exists (`test_export.go`'s
  `CheckoutStoreForTest`/`ActiveReqsForTest`/`LockShardMuForTest`).

  **Change-feed barrier side**: already closed as part of BACKLOG 19c's fix —
  `tieredstore_changelog_reorder_test.go`'s `TestTieredChangeFeedConcurrentWritersNoGaps` runs 8 real
  concurrent writer goroutines against a concurrent `ForEachChange` reader goroutine and asserts gapless,
  non-reordered LSN delivery both live and in a final full drain. That test pre-dates this specific 19d
  entry's closure but satisfies exactly what it asks for.

  **Drain protocol side (this fix)**: added `TestTieredColdShardFastDrop_ConcurrentWriters` — runs the
  fast-drop purge concurrently with real writer goroutines (4 cross-shard `PutRelationship` writers + 1
  `AddNodeLabelToken` writer) hammering the SAME candidate shard the drop targets, through the public API
  with real timing (not a deterministic simulation, unlike BACKLOG 19e's targeted regression test), under
  `-race`. The writers deliberately touch a SURVIVOR node set (an "Other"-labeled group co-located on the
  candidate shard via the same rotation window, but never eligible for this purge) rather than the
  purge-target nodes themselves — racing writes against nodes the purge may concurrently delete would
  conflate this test with the separate, already-documented MEDIUM-severity gap BACKLOG 19g describes
  ("`putRelationshipLocked`'s write ordering has no residue reconciliation on crash... undetectable except
  via manual `VerifyShard`/`RunRepair`") — a node legitimately vanishing mid-write is a KNOWN, ACCEPTED
  race with its own entry; this test isolates the DISTINCT property 19d asks for. Rather than asserting
  one specific interleaving (which would be flaky by construction against real goroutine scheduling), it
  asserts what must hold under EVERY interleaving: no crash/race (caught by `-race`), every survivor node
  still present, and `RunRepair` reports zero orphaned/missing in/ entries — full consistency regardless
  of how the drain and the writers interleaved.

  **Process note — a real, separate gap surfaced during this investigation**: an EARLIER draft of this
  test (before isolating writers to a survivor set) had writers create relationships directly to
  purge-target nodes, which under `-race -count=10` intermittently made `RunRepair` itself return a HARD
  ERROR (`resolve start shard for rel N: graph: node not found`) instead of treating a relationship whose
  endpoint legitimately vanished (the exact 19g race) as an orphan to report/clean rather than a fatal
  abort of the whole repair pass. This looks like a genuine, previously-unknown gap in `RunRepair`'s error
  handling (Phase 2's `findNodeInAnyShardStore` returning `nil` should arguably fold into
  `OrphanedInEntries`-style accounting rather than aborting), but reproducing and fixing it is a distinct
  finding from 19d itself — NOT fixed here to keep this item's scope to what it actually asks for; logged
  as **19s. `RunRepair` Phase 2 aborts the whole repair pass with a hard error when a relationship's
  endpoint has vanished (a legitimate 19g race), instead of treating it as an orphan like Phase 1 does
  (MEDIUM, discovered via BACKLOG 19d's adversarial test, not yet reproduced in isolation).**
  `tieredstore_repair.go:132-145`.

  Full `go build ./...` + `go vet ./...` clean; `go test ./pkg/graph/store/tiered/...` clean;
  `go test -race ./pkg/graph/store/tiered/... -run TestTieredColdShardFastDrop_ConcurrentWriters
  -count=15` clean (15/15); full repo `go test ./...` clean.
- **19e. [FIXED — `store/tiered/retention_purge_drop.go`, `retention_purge_drop_toctou_test.go`] TOCTOU
  on cold-shard-drop's single-label eligibility re-check — the second, authoritative check result is
  discarded (HIGH).** `retention_purge_drop.go:75-86,118-126`.

  **Bug**: `dropOneShard`'s drain protocol checks single-label eligibility TWICE — once before the
  unlink (a cheap early-out: `onlyLabel, _, _, err := store.CollectShardDropResidue(labelToken)`, `if
  !onlyLabel { return ... }`) and once AFTER the drain completes, when the shard is provably quiescent
  (`_, nodeIDs, rels, cerr := store.CollectShardDropResidue(labelToken)` — the leading return discarded
  with `_`). A request that STARTED before the unlink but only PUBLISHED its effect (e.g. a concurrent
  `AddNodeLabelToken` adding a FOREIGN label) entirely between the first check and the unlink is
  invisible to the drain (the drain only waits for requests still in-flight AT unlink time — one that
  started and finished in that earlier window was never counted as in-flight), so the SECOND check is
  the only place that could catch it — and its result was thrown away. The shard was then dropped
  unconditionally, PHYSICALLY DESTROYING the foreign-labeled node's directory-resident data, even though
  the purge was only authorized to remove nodes carrying the ORIGINAL target label.

  **Fix**: capture the second `CollectShardDropResidue` call's leading return as `stillOnlyLabel` and
  gate the rest of the drop on it — `if !stillOnlyLabel { re-link and return ok=false }` — mirroring the
  drain-timeout branch's existing re-link discipline exactly (same `ts.mu.Lock(); ts.eventShards[es.name]
  = es; ts.mu.Unlock()` pattern), so a late-discovered foreign label falls back to the safe row-scan path
  instead of destroying data.

  **Test** (`retention_purge_drop_toctou_test.go`,
  `TestTieredColdShardFastDrop_ForeignLabelDuringDrainBlocksTheDrop`): reproduces the exact race
  deterministically (no timing flakiness) using the test scaffolding BACKLOG 19d's own text flagged as
  "already exists unused" (`CheckoutStoreForTest`/`CheckinStoreForTest`/`ActiveReqsForTest`,
  `EventShardsForTest`, `MuForTest`) — holds the candidate shard's `activeReqs` artificially non-zero via
  a held `CheckoutStoreForTest` checkout (so `dropOneShard`'s drain loop is FORCED to spin), starts the
  purge in a goroutine, polls until the shard is provably unlinked from `ts.eventShards` (proof the drop
  has passed its first check and is now blocked in the drain), then — while it is provably spinning —
  writes a foreign-labeled node DIRECTLY to the still-open checked-out store, simulating the in-flight
  request's effect landing during the drain window. Releasing the checkout lets the drain finish and the
  second (authoritative) check run. Asserts: the foreign node's data survives (read directly from the
  candidate shard's own `*BadgerStore`, NOT via `ts.GetNode` — the foreign node's snowflake ID was minted
  AFTER rotation, so ts-level routing would look in the current hot shard regardless of whether the
  ROTATED shard was dropped; the actual discriminator this test needs is whether the rotated shard's
  directory/store still physically exists, which only a direct read proves), the event-shard count is
  UNCHANGED (the shard was re-linked, not dropped), and the ORIGINAL target-labeled nodes were still
  correctly purged via the safe row-scan fallback (`purgeNodesFanOut`, which still runs over every open
  shard including the re-linked one) — proving the fix doesn't regress the purge's overall correctness,
  only the unsafe shortcut.

  RED confirmed via `git stash push -- retention_purge_drop.go` (this also reverted BACKLOG 19a/19b's
  fixes, harmlessly — neither affects this specific scenario): the test failed with `graph: store already
  closed` reading the foreign node — the exact "shard was destroyed with live foreign-labeled data still
  on it" symptom the finding describes. Popped the stash, confirmed GREEN. Full `go build ./...` +
  `go vet ./...` clean; `go test ./pkg/graph/store/tiered/...` clean; `go test -race
  ./pkg/graph/store/tiered/...` clean (52s); full repo `go test ./...` clean.
- **19f. [VERIFIED — no code change; `retention_19f_delete_ordering_test.go`] Inconsistent cross-shard
  delete ordering between `DeleteRelationship` and `DeleteRelWithHistory` — a lesson-58 "fixed in one
  door, left behind in its sibling" pattern (HIGH).** `tieredstore_write_rel.go:325-336` vs
  `tieredstore_write_history.go:654-679` (deliberately reversed with an explicit crash-safety comment).

  **Investigation**: `DeleteRelationship`'s cross-shard branch deletes the ENTITY+OUT leg FIRST, then
  the IN leg (`entityShard.DeleteRelEntityAndOut` → `inShard.DeleteRelIncoming`); `DeleteRelWithHistory`
  deletes the IN leg FIRST, then the entity leg (`endShard.DeleteRelIncoming` →
  `entityShard.DeleteRelWithHistory`) — genuinely opposite orderings between two sibling doors, exactly
  the shape lesson 58's meta-pattern warns about. Traced the reasoning rather than assuming either a bug
  or a false alarm:

  The orderings are DELIBERATE and each is individually correct, driven by an asymmetry in ROLLBACK
  difficulty on an ERROR return (not a crash) from the second leg: `DeleteRelationship`'s entity leg
  (`PutRelEntityAndOut`/`DeleteRelEntityAndOut`) is plain, symmetric CRUD with no version chain — safe to
  undo, so it can go FIRST (if the in/-leg then fails, undo the easy entity leg). `DeleteRelWithHistory`'s
  entity leg ALSO advances the version chain and writes a tombstone-history row — the Store contract has
  no "undo a WithHistory write" primitive (the sibling door's own comment says exactly this), so it MUST
  go LAST — the in/-leg (cheap, symmetric via `PutRelIncoming`) goes first instead, so a failure of the
  irreversible leg still permits a clean rollback of the reversible one. Reordering `DeleteRelWithHistory`
  to match `DeleteRelationship` would not fix anything — it would just move the irreversibility problem to
  the OTHER leg, since the entity-with-history leg is the one that fundamentally can't be undone
  regardless of which position it's called from.

  The two orderings' worst-case CRASH residues (as opposed to error-return residues) are different SHAPES,
  but BOTH are already covered by the existing `RunRepair` machinery, confirmed by tracing
  `tieredstore_repair.go`: `DeleteRelationship`'s crash-mid-window (entity gone, in/ entry orphaned) is
  Phase 1's "orphaned in/ entries" case (already tested —
  `TestTieredStore_Repair_OrphanedIncoming`). `DeleteRelWithHistory`'s crash-mid-window (in/ entry gone,
  entity STILL LIVE — its delete never committed, so the rel is correctly still alive from the caller's
  perspective) is Phase 2's "missing in/ entries" case, which RE-CREATES the in/ entry — the objectively
  CORRECT repair for this shape, since a crash before the entity-shard write means the delete never
  logically happened. The generic mechanism was already tested
  (`TestTieredStore_Repair_MissingIncoming`), but not the SPECIFIC `DeleteRelWithHistory` crash-window
  shape end-to-end.

  **Conclusion: no code change.** The ordering difference is correct-by-construction, not an accidental
  omission — reordering either door would trade one irreversibility problem for another without closing
  any actual gap, and the crash residue from either ordering is already detected and repaired.

  **Test** (`retention_19f_delete_ordering_test.go`,
  `TestTieredStore_Repair_MissingIncoming_AfterDeleteRelWithHistoryCrashWindow`): closes the specific
  verification gap ("traced via code, not reproduced") by reproducing `DeleteRelWithHistory`'s exact
  crash-mid-window end-to-end — creates a genuine cross-shard relationship (event start, reference end),
  then simulates the crash by calling the in/-leg delete DIRECTLY (bypassing the full door) instead of
  going through `DeleteRelWithHistory` — reproducing exactly the observable on-disk state a crash between
  its two steps would leave. Confirms the relationship is still LIVE (`GetRelationship` succeeds — the
  delete never logically happened) with its incoming index missing (the residue), then runs `RunRepair`
  and confirms `MissingInEntries == 1`, `OrphanedInEntries == 0`, and both the low-level incoming-index
  view AND the public `IncomingRelationships` query are fully restored.

  Full `go build ./...` + `go vet ./...` clean; `go test ./pkg/graph/store/tiered/...` clean (this is a
  verification-only addition with no production code change, so no RED/GREEN cycle or `-race` run was
  needed — the new test passed on first run against the existing, unmodified code, which is itself the
  point: proving the safety net already works).
- **19g. [FIXED — `store/tiered/tieredstore_write_rel.go`,
  `store/tiered/tieredstore_write_rel_crash_residue_test.go`] `putRelationshipLocked`'s documented
  E→R write ordering appeared to have no residue reconciliation on crash — a durable phantom
  incoming-index entry with no backing entity, undetectable except via manual `VerifyShard`/
  `RunRepair` (MEDIUM).** `tieredstore_write_rel.go:147-162`.

  **Investigated in depth — the reconciliation and read-safety mechanisms already existed; this was a
  documentation + test-gap finding, not a missing-functionality bug.** The existing rollback (`if err
  := entityShard.PutRelEntityAndOut(r); err != nil { ...delete the in/ write... }`) only covers a
  SYNCHRONOUS failure on the second write; it genuinely cannot run across a process crash between the
  two writes, so the finding's core observation is correct. But: (1) `RunRepair`'s Phase 1 already
  scans every shard's `IncomingIndexEntries()` and purges exactly this residue shape — proven by the
  pre-existing `TestTieredStore_Repair_OrphanedIncoming`, which manually constructs the identical
  scenario (a `PutRelIncoming` call with no matching entity row) and confirms `RunRepair` cleans it
  up. (2) The read path is ALREADY safe in the crash→repair window: `IncomingRelationships` /
  `IncomingRelationshipsForNodes` resolve relIDs through `getUniqueRelationshipsByIDs`, which tracks
  unresolved IDs in a `pending` set and simply omits them from the returned map rather than erroring —
  confirmed by tracing every return path in the function, none of which fails on a nonempty leftover
  `pending`. So a phantom in/ entry never produces a wrong or crashing query result, only a (correct)
  absent one, since the relationship never fully committed. Automatic repair-at-open was considered
  and rejected: `RunRepair` opens every shard (including cold ones) for the whole run — running it
  unconditionally on every `New()` would contradict the codebase's own lazy/sequential shard-access
  discipline (the exact complaint BACKLOG 19i raises about `RunRepair` itself) and would tank startup
  time for large stores; it is an operator-triggered admin action by design, like `VerifyShard`.

  **Fix.** Added an inline crash-consistency contract comment on both split-write branches in
  `putRelationshipLocked` (E→R and R→E/E→E), explicitly stating: what residue a crash between the two
  writes leaves, why it's safe to read around (citing the `getUniqueRelationshipsByIDs` omission
  behavior), and that `RunRepair` (not this door) is the reconciliation path — so a future reader
  doesn't have to independently rediscover this by cross-referencing `tieredstore_repair.go`. Closed
  the one genuine test gap: no test previously proved a QUERY issued in the crash→repair window
  behaves safely (only that repair eventually cleans up, and that a pre-repair inIdx entry is
  visible via the low-level `IncomingRelIDs` accessor — not that the door-level `IncomingRelationships`
  query tolerates it). New `TestIncomingRelationships_SkipsPhantomInEntryFromCrashedCrossShardWrite`
  constructs the crash residue exactly like `TestTieredStore_Repair_OrphanedIncoming`, then calls
  `IncomingRelationships` BEFORE running repair and asserts it returns `(nil, nil)` rather than an
  error or a phantom entry, then runs `RunRepair` afterward to confirm the residue is still
  independently cleaned up (the query itself doesn't consume/repair it).

  **Verification.** New test passes with NO logic change (only the doc comment was added) — expected,
  since the underlying mechanism was already correct; the test closes a coverage gap, not a bug.
  `go build ./...` + `go vet ./...` clean; `go test ./pkg/graph/store/tiered/...` clean (33s); full
  repo `go test ./...` clean (including tutorials).
- **19h. `TxChangeLogScope`'s per-tx shard snapshot has a documented, untested gap under rotation
  (MEDIUM).** `tieredstore_changelog.go:584-699`.
- **19i. `RunRepair` pins EVERY shard (including all cold shards) open simultaneously for the whole
  repair run, contrary to lesson 8 — `RebuildCatalog` in the same file does it correctly (MEDIUM).**
  `tieredstore_repair.go:34-42`.
- **19j. Cross-shard result accumulation in `AllNodes`/`AllNodeIDs` fanout is unbounded — materializes
  every shard's full result into RAM concurrently, undocumented as a caveat despite the streaming
  alternative (`ForEachByLabel`/`IterByLabel`) existing precisely to avoid this (MEDIUM).**
  `tieredstore_read_bulk.go:62-70,323-333`.
- **19k. [FIXED — `store/tiered/tieredstore_migrate.go`, `store/tiered/tieredstore.go`,
  `store/tiered/tieredstore_migrate_concurrency_test.go`] `MigrateFromBadger` had no enforced
  single-writer discipline (MEDIUM).** `tieredstore_migrate.go:25-122`.

  **Bug.** `MigrateFromBadger` operates directly on raw `*BadgerStore`/`*Store` pointers, below the
  `Core`/entity-lock layer, and mutates SHARED state on `dst` with no locking at all:
  `dst.ontology.SetLabelRegistry` swaps the ontology's routing table, and `insertedNodes`/
  `insertedRels` drive the rollback path on failure. Two concurrent `MigrateFromBadger` calls against
  the SAME `dst` could interleave these — each call's `SetLabelRegistry` could clobber the other's, and
  a rollback triggered by one call's failure could act on the WRONG call's bookkeeping.

  **Fix.** Added `migrateMu sync.Mutex` to `tiered.Store` and take it for `MigrateFromBadger`'s entire
  duration (from right after the nil-input checks through every return path). This serializes
  CONCURRENT `MigrateFromBadger` calls against the same destination — the enforceable form of
  "single-writer discipline" at this layer, since `PutNode`/`PutRelationship` don't take `migrateMu`
  themselves (avoiding any reentrant-lock deadlock risk from the migration loop's own `dst.PutNode`/
  `dst.PutRelationship` calls). Extended the doc comment to state explicitly what this does and
  doesn't cover: it does NOT serialize against a `Core`/`Graph` already open on `dst` performing
  ordinary concurrent writes (a different, harder problem) — the documented safe usage remains running
  migration BEFORE any `Core`/`Graph` opens `dst`, as an offline step.

  **Test** (`store/tiered/tieredstore_migrate_concurrency_test.go`, new file, using the same
  direct-mutex-acquisition simulation technique as this session's other TOCTOU regression tests):
  `TestMigrateFromBadger_SerializesConcurrentCalls` — manually holds `dst.migrateMu`, starts a
  `MigrateFromBadger` call in a goroutine, confirms it blocks for 150ms, releases the lock, and confirms
  the call then proceeds and succeeds.

  **Verification.** RED confirmed via `git stash push -- store/tiered/tieredstore_migrate.go
  store/tiered/tieredstore.go`: the test failed to COMPILE (`dst.migrateMu undefined`) — the strongest
  possible confirmation the guard was genuinely absent. Stash popped, GREEN confirmed. Full `go build
  ./...` and `go vet ./...` clean. `go test ./pkg/graph/store/tiered/...` full package pass. `go test
  -race ./pkg/graph/store/tiered/... -run TestMigrateFromBadger` clean. Full-repo `go test ./...`
  clean — no regressions.
- **19l. [FIXED — TEST-GAP, `shard_catalog_removeshard_test.go`] `RemoveShard` has ZERO direct test
  coverage, including the exact failure mode from 19b (HIGH, TEST-GAP).** `shard_catalog.go:240`. Prior
  coverage was entirely indirect (via `retention_purge_drop.go`'s `dropOneShard`) — Testing Rule 1:
  indirect coverage via delegation does not count. No production code change — `RemoveShard` was already
  correct; this closes a pure coverage gap.

  **Tests** (`shard_catalog_removeshard_test.go`, 6 new): `TestShardCatalog_RemoveShard_RemovesExistingEntry`
  / `TestShardCatalog_RemoveShard_MissingNameReturnsFalse` (the two direct return-value branches),
  `TestShardCatalog_RemoveShard_OnlyRemovesTheNamedEntry` (3 entries, removes the middle one, asserts the
  other two survive untouched — guards the slice-splice logic against an off-by-one that would silently
  delete the WRONG shard's catalog entry, a data-loss-adjacent class of bug),
  `TestShardCatalog_RemoveShard_DoubleRemoveIsIdempotentFalse`. Plus the specific 19b/19l-referenced
  failure-recovery discipline: `TestShardCatalog_RemoveShard_SnapshotRestoreRollsBack` directly proves the
  `snapshotShards()` → `RemoveShard()` → `restoreShards(snapshot)` rollback sequence `dropOneShard` relies
  on (BACKLOG 19b's fix, verified in isolation rather than only through the larger integration path) —
  removes an entry, then restores from the pre-removal snapshot, and asserts the entry comes back with
  every field intact (including slice fields `Labels`/`RelTypes`) and the OTHER (never-removed) entry is
  untouched by the round-trip. `TestShardCatalog_SnapshotShards_IsADeepCopy` guards the other half of that
  contract — a snapshot must be independent of later catalog mutations, or a rollback target itself gets
  silently corrupted by unrelated catalog activity between the snapshot and the restore.

  All 6 tests pass immediately against the unmodified `shard_catalog.go` (no RED/GREEN cycle — this is a
  pure coverage addition confirming already-correct behavior, mirroring how BACKLOG 15b/15c's coverage
  gaps were closed earlier in this pass). Full `go build ./...` + `go vet ./...` clean;
  `go test ./pkg/graph/store/tiered/...` clean.
- **19m. Duplicated cross-shard residue-sweep logic between `sweepDroppedShardResidue` and
  `purgeNodesFanOut`'s phase 2 — a future fix to one is likely to miss the other (LOW).**
  `retention_purge_drop.go:167-200` vs `retention_purge.go:98-125`.
- **19n. Unbounded spin-wait in `Close()`'s shard drains, inconsistent with the bounded/timed-out
  drain used by the purge protocol (LOW, requires a pre-existing checkin leak to trigger).**
  `tieredstore.go:679-683,706-708,717-719`.
- **19o. `preflightArchiveNodeDestination` performs an untracked destructive mutation despite its
  "preflight" name — harmless in effect but breaks the check/apply separation the rest of the file
  follows (MEDIUM).** `tieredstore_write_archive.go:341-373`.
- **19p. Dead defensive-only bound check would silently swallow the exact invariant violation 19c
  would produce, instead of surfacing it (LOW).** `tieredstore_changelog.go:415-419`.
- **19q. Global `nodeCreateMu`/`relCreateMu` serialize ALL creates store-wide — a hard throughput
  ceiling for the stated TB/day workload, likely unavoidable given the correctness requirement (LOW,
  scaling constraint, not obviously cheap to fix).** `tieredstore.go:257-258`.
- **19r. Under-commented TOCTOU defense in fanout cold-shard reclassification — real but narrow-
  window, reads as removable dead code to a future maintainer (LOW).**
  `tieredstore_read_fanout.go:26-59`.
- **19s. [FIXED — `store/tiered/tieredstore_repair.go`,
  `store/tiered/tieredstore_repair_orphaned_endpoint_test.go`] `RunRepair` Phase 2 aborted the whole
  repair pass with a hard error when a relationship's endpoint had vanished (a legitimate 19g race),
  instead of treating it as an orphan like Phase 1 does (MEDIUM, discovered via BACKLOG 19d's
  adversarial concurrent-writer test, not yet reproduced in isolation).** `tieredstore_repair.go:132-145`.

  **Minimal repro constructed.** The single-shard `Store.DeleteNode` guards against deleting a node
  with a live relationship, so "create both endpoints + the rel, then delete one endpoint" cannot be
  driven through the public API. Mirroring `TestTieredStore_Repair_MissingIncoming`'s existing
  technique, the repro instead writes the relationship's entity+out/ row directly via the lower-level
  `PutRelEntityAndOut` door (no endpoint-liveness check — that validation lives one layer up, in
  `putRelationshipLocked`) referencing an end-node ID that was never created on any shard. This is the
  same observable state a node purge racing a concurrent relationship write would leave behind, and
  reproduces the reported abort deterministically (`TestTieredStore_Repair_OrphanedRelEndpointDoesNotAbortPass`,
  RED-confirmed via `git stash` — the new `RepairResult.OrphanedRelEndpoints` field is compile-load-bearing).

  **Fix.** In Phase 2's endpoint-resolution block, `findNodeInAnyShardStore` returning a genuine I/O
  error is still treated as fatal (unchanged — an operational failure must not be silently swallowed,
  per the existing Phase-2 doc comment). But `startShard == nil || endShard == nil` (endpoint not
  found anywhere, not an I/O error) now increments a new `RepairResult.OrphanedRelEndpoints` counter
  and `continue`s to the next relationship, instead of returning
  `fmt.Errorf("resolve start/end shard for rel %d: %w", ..., ErrNodeNotFound)` and aborting the whole
  pass — mirroring exactly how Phase 1 already treats `entityStore == nil` (in/ entry with no backing
  entity) as `OrphanedInEntries++`, not an error. Deliberately does NOT auto-delete the orphaned
  relationship: whether this is a genuine permanent orphan or a relationship still mid-write when
  observed is ambiguous from a repair pass's point-in-time snapshot, and speculatively deleting risks
  destroying a legitimate relationship on observation-window noise (same caution CLAUDE.md's "Repair
  tools don't replace correctness" lesson calls for) — an operator gets a count to investigate, not a
  silent mutation.

  **Verification.** `go build ./...` + `go vet ./...` clean. RED confirmed via `git stash push` on the
  production file alone: `go vet` failed to compile (`result.OrphanedRelEndpoints undefined`). Popped
  the stash, confirmed GREEN on the new test plus the full existing `TestTieredStore_Repair_*` suite
  (5 tests, all still pass — Phase 1 and the genuine-I/O-error path are untouched).
  `go test ./pkg/graph/store/tiered/...` clean (33s); full repo `go test ./...` clean (including
  tutorials).

### BACKLOG 20 — Sharded backend hardening (WIP status)

- **20a. [FIXED — `store/sharded/verify.go`, `verify_foreign_incoming_test.go`] `VerifyConsistency`
  false-positives on every Model-A foreign-incoming stub — every legitimate cross-machine edge stub is
  misdiagnosed as corruption (HIGH).** `store/sharded/verify.go:88-94`. The `ShardMismatch` check had no
  carve-out for foreign-incoming stubs, unlike `endpointOrphan` which does.

  **Bug**: a Model-A foreign-incoming half-edge stub (ADR-0010 §3.3) is DELIBERATELY co-located on its
  END node's local shard, with its rel-ID's OWN slot belonging to a FOREIGN machine — that mismatch is
  the entire point of the stub (`IncomingRelationships(END)` must find it locally without a slot-routed
  `GetRelationship`). `VerifyConsistency`'s `ShardMismatch` check for rels compared every rel's own slot
  against the shard storing it with no exception, so EVERY legitimate stub failed this check — a 100%
  false-positive rate on a documented, intentional storage pattern, not a rare edge case.
  `endpointOrphan` (the sibling check just below it in the same file) already carves out exactly this
  shape ("The endpoint's slot is not local — a cross-partition edge whose node lives on another
  machine... Not an orphan here"), making the `ShardMismatch` check's omission a clear "fixed in one
  door, left behind in its sibling" gap within the SAME function.

  **Fix**: reordered the loop to fetch the relationship (`shard.GetRelationship(rid)`) BEFORE the
  `ShardMismatch` check (previously after), so the check can inspect `r.EndNodeID()`. New helper
  `isForeignIncomingStubShape(foundOn *badger.Store, r *types.Relationship) bool` reports whether `r`'s
  END node is local to `foundOn` — the stub's defining co-location rule, and the SAME "is this endpoint
  local" test `endpointOrphan` already applies, reused for structural consistency. There is no
  persistent per-row marker distinguishing a genuine stub from a corrupted row (documented explicitly in
  the new code comments), so this is a STRUCTURAL match, not a definitive identification — a rel whose
  end node is NOT local to the shard storing it has no such explanation and is still correctly flagged
  (proven by a dedicated adversarial test, see below).

  **Tests** (`verify_foreign_incoming_test.go`, internal `package sharded` to access `newMemStore`/
  `mkNodeID`/`mkRelID`/`s.shards[i]` test helpers already used by the existing sibling test
  `TestVerifyConsistencyDetectsShardMismatch`): `TestVerifyConsistencyForeignIncomingStubNotFlagged`
  builds a legitimate stub via the real production door (`RecordForeignIncoming`, mirroring
  `TestRecordForeignIncoming_StoreWrite`'s exact setup) and asserts `ShardMismatches` is empty (the
  Report is fully `OK()`). `TestVerifyConsistencyGenuinelyMisroutedRelStillFlagged` is the adversarial
  counterpart proving the fix is NOT a blanket exemption: both endpoints live on shard[1], but a rel
  carrying a THIRD, unrelated slot is written directly onto shard[0] (mirroring
  `TestVerifyConsistencyDetectsShardMismatch`'s node-side technique for the rel side) — neither the
  rel's own slot NOR its end node's home shard explain being on shard 0, so it has no foreign-incoming-
  stub shape and must still be reported; asserts exactly 1 `ShardMismatch` with the correct
  shard/slot/expected-shard/kind/ID fields.

  RED confirmed via `git stash push -- verify.go`: `TestVerifyConsistencyForeignIncomingStubNotFlagged`
  failed with exactly the false-positive the finding describes (`ShardMismatches = [{Shard:0 ID:...
  Slot:9 ExpectedShard:-1 Kind:rel}]`), while `TestVerifyConsistencyGenuinelyMisroutedRelStillFlagged`
  correctly PASSED unchanged (that detection was never broken — confirms the two tests discriminate
  precisely). Popped the stash, confirmed GREEN on both, plus the pre-existing
  `TestVerifyConsistencyDetectsShardMismatch` unaffected. Full `go build ./...` + `go vet ./...` clean;
  `go test ./pkg/graph/store/sharded/...` clean; `go test -race ./pkg/graph/store/sharded/...` clean;
  full repo `go test ./...` clean.
- **20b. [FIXED — `store/sharded/property_index.go`, `composite_index.go`, `high_frequency_index.go`,
  `rel_property_index.go`, `temporal_index.go`, `vector_index.go`, `fanout_rollback_test.go`] DDL
  fan-out (`fanOutUniform`) has no rollback on partial shard failure (HIGH).**
  `store/sharded/property_index.go:91-103` + composite/HF/rel-property/temporal/vector callers.

  **Bug**: `fanOutUniform` runs a DDL closure (create/drop) on every shard in PARALLEL, then coalesces
  the per-shard results via `coalesceUniform`: nil if every shard succeeded, the single common sentinel
  if EVERY shard failed identically, or `errors.Join(errs...)` for genuine divergence (a MIX of success
  and failure, or different failures on different shards). The mixed case — the exact shape a mid-build
  I/O failure on one shard produces while the others succeed — was returned as an error with NO
  rollback: the index was left built on N-1 shards and absent on one, with no reconciliation path. For
  vector indexes specifically this was WORSE: `CreateVectorIndexWithOptions` only updates the
  store-level `vectorDefs` bookkeeping map AFTER the whole fan-out returns nil, so on a partial failure
  the physically-built per-shard HNSW/brute-force index data on the succeeded shards became
  PERMANENTLY ORPHANED — untracked by `vectorDefs`, unreachable via `SearchNearestNodes` (which
  consults `vectorDefs` first), and un-droppable via the normal `DropVectorIndex` door (which also
  routes through the now store-level-untracked def), consuming disk space with zero recovery path
  short of manual per-shard surgery.

  **Fix**: new `fanOutUniformCreate(do, rollback func(shard *badgerShard) error) error` in
  `property_index.go` (beside `fanOutUniform`/`coalesceUniform`, which it reuses for the initial
  coalesce). On any non-nil overall result — the mixed case AND the "every shard failed, but not
  identically" case — it rolls back every shard whose `do()` returned nil (skipping ones that never
  succeeded, which is always safe/idempotent since `rollback` is each index type's own matching Drop
  door) via a second parallel fan-out. A rollback failure is joined into the returned error via `%w`
  (never silently swallowed), so a stuck rollback stays visible to the caller rather than masquerading
  as a clean failure. All 6 `Create*Index`/`CreateVectorIndexWithOptions` callers now route through
  `fanOutUniformCreate(create, drop)` instead of plain `fanOutUniform(create)`; all 6 `Drop*Index`
  callers are UNCHANGED (still plain `fanOutUniform`) — a partial DROP failure degrades some shards to
  a full-scan fallback rather than corrupting results (every property/composite/rel-property/temporal
  index door already falls back to a scan when its local index is absent, per each package's own
  architecture notes), a materially milder failure mode than a partial CREATE, and "rolling back a drop"
  would mean re-running a potentially expensive index build just to reach a state a plain retry of the
  drop already reaches. For vector indexes, `fanOutUniformCreate` closes the orphaned-disk-state problem
  directly: a failed create is now FULLY reverted on every shard before `CreateVectorIndexWithOptions`
  ever returns, so no per-shard state can survive a failed call for `vectorDefs` to fail to track — the
  fix eliminates the orphan class rather than trying to make the bookkeeping map catch up with it.

  **Tests** (`fanout_rollback_test.go`): `fanOutUniformCreate` is exercised directly with synthetic
  `do`/`rollback` closures (not tied to any one index type), covering the mechanism ONCE for all 6 real
  callers: `TestFanOutUniformCreate_NoRollbackOnFullSuccess` (sanity — no rollback fires when nothing
  failed), `TestFanOutUniformCreate_UniformFailureNoRollbackNeeded` (every shard fails identically —
  nothing succeeded, so rollback is correctly a no-op), `TestFanOutUniformCreate_RollsBackOnPartialFailure`
  (the core regression — one shard fails, the rest succeed, and exactly the succeeded shards — never the
  failed one — get rolled back), `TestFanOutUniformCreate_RollbackFailureIsJoinedNotSwallowed` (a
  rollback that itself fails must remain visible via `errors.Is` on the returned error, not disappear).
  Plus one end-to-end proof for a real caller,
  `TestCreatePropertyIndex_PartialFailureRollsBackSucceededShards`: pre-seeds ONE shard with the index
  directly (simulating either residual state from an earlier failed attempt or simply the exact mixed-
  result shape a genuine partial I/O failure produces), drives a fresh `CreatePropertyIndex` call through
  the PUBLIC API, and asserts every OTHER shard has been rolled back (a fresh `CreatePropertyIndex` on
  each succeeds cleanly rather than returning `ErrIndexExists`, which would indicate a leftover
  un-rolled-back index).

  RED confirmed two ways: `git stash push` on the 6 production files made the 4 direct-mechanism tests
  fail to COMPILE (`fanOutUniformCreate` doesn't exist without the fix); a targeted manual revert of
  `CreatePropertyIndex` alone (back to plain `fanOutUniform`, `fanOutUniformCreate` still defined but
  unused by this one caller) made the end-to-end test fail with `shard[0] still has the index after the
  failed CreatePropertyIndex call` — the exact "partial success with no rollback" symptom the finding
  describes. Restored the fix, confirmed GREEN on all 5. Full `go build ./...` + `go vet ./...` clean;
  `go test ./pkg/graph/store/sharded/...` clean; `go test -race ./pkg/graph/store/sharded/...` clean;
  full repo `go test ./...` clean.
- **20c. `DeleteNodeWithHistory` cross-shard rel tombstones not applied in deterministic (sorted)
  order, unlike the sibling `DeleteNodeCascade` — breaks repair reproducibility (MEDIUM).**
  `store/sharded/history.go:166-203`.
- **20d. [FIXED — TEST-GAP, `delete_node_with_history_test.go`] Zero test coverage for
  `DeleteNodeWithHistory` in the sharded package — the most cross-shard-hazardous door in the file,
  unlike every sibling mutation door which has a dedicated `*CrossShard` test (HIGH, TEST-GAP).**
  `store/sharded/history.go:153-207`. No production code change — the door was already correct; this
  closes a pure coverage gap (confirmed by grep: zero prior usages of `DeleteNodeWithHistory` anywhere
  in the package's test files).

  `DeleteNodeWithHistory` independently classifies every connected relationship tombstone into three
  categories — LOCAL (rel's OWN ID slot matches the node's shard — deleted atomically alongside the
  node tombstone via the node-shard's own `DeleteNodeWithHistory`), REMOTE (rel's own slot is a
  DIFFERENT local shard — deleted individually first via `DeleteRelWithHistory`), and FOREIGN-INCOMING
  STUB (rel's own slot is foreign — a Model-A half-edge with no version chain, removed via
  `DeleteRelationshipForeignIncoming` BEFORE the node's own delete so the node-shard's tombstone
  validation and cascade sweep see stub-free adjacency) — making it the file's most cross-shard-hazardous
  door by the finding's own description, yet the one with zero direct coverage.

  **Tests** (`delete_node_with_history_test.go`, mirroring the existing sibling pattern
  `TestDeleteNodeCascadeCrossShard`/`TestDeleteNodesBatchCrossShard` in `sharded_s2_test.go`):
  `TestDeleteNodeWithHistoryCrossShard` exercises the LOCAL + REMOTE categories together in one call (a
  hub node on shard 0, a LOCAL rel whose own ID slot is also 0, a REMOTE rel whose own ID slot is a
  different local shard) and asserts: the node is gone as a live row but its history is preserved (B32);
  both rels are gone as live rows with THEIR history also preserved; both neighbor nodes survive with
  clean adjacency; `VerifyConsistency` reports a fully clean `Report`. `TestDeleteNodeWithHistoryForeignIncomingStub`
  covers the third category in isolation: builds a genuine Model-A stub via `RecordForeignIncoming`
  (mirroring the ADR-0010 test pattern used elsewhere in the package), passes it as a `RelTombstone` with
  a FOREIGN rel-ID slot, and confirms the door correctly routes it through the
  `shardIndexForRel`-returns-`ErrSlotNotLocal` → `DeleteRelationshipForeignIncoming` path rather than
  mis-treating it as local or remote — the node deletes cleanly, the stub's raw adjacency index entry
  (checked directly via the shard's own `IncomingRelIDs`, since the node itself is gone by the time of
  the check) is gone, and the final `VerifyConsistency` is clean.

  Both tests pass immediately against the unmodified `history.go` (no RED/GREEN cycle — pure coverage
  confirming already-correct behavior, mirroring how BACKLOG 15b/15c/19l's coverage gaps were closed
  earlier in this pass). Full `go build ./...` + `go vet ./...` clean;
  `go test ./pkg/graph/store/sharded/...` clean; `go test -race ./pkg/graph/store/sharded/...` clean;
  full repo `go test ./...` clean.
- **20e. §4.5 pre-encoded-put fast path never routed for sharded despite the capability being
  satisfied — the ADR-0006 throughput benefit doesn't materialize for this backend today (MEDIUM,
  documented gap in the code's own comment).** `store/sharded/batch.go:328-336`.
- **20f. [FIXED — `store/sharded/stats_iter.go`, `store/sharded/deleted_iteration_test.go`]
  `DeletedIterationCapability` was entirely absent from the sharded backend, an oversight (not a
  documented decline like the other 4 intentional declines) — silent O(total history) fallback for
  adjacency-at-t queries on a sharded deployment (MEDIUM-HIGH).**

  **Bug.** `sharded.Store` had zero occurrences of `ForEachDeletedNodeID`/`ForEachDeletedRelID`
  anywhere (confirmed via grep before the fix) — unlike the package's own doc comment
  (`sharded.go:13-15`), which explicitly lists ONLY 4 intentional declines it shares with tiered
  (`TransactionTimeQuery`, `HistoryRollbackTrim`, label/rel-type-tx membership, depth iteration) —
  `DeletedIterationCapability` (the non-depth-aware variant) is not among them. Because
  `internal/core` type-asserts for this capability at runtime and silently falls back to full
  history iteration when absent (`forEachDeletedNodeIDByDepth`/`forEachDeletedRelIDByDepth`,
  `internal/core/temporal.go:609-634`), every temporal adjacency query
  (`OutgoingRelsAt`/`IncomingRelsAt`/`NeighborsAt`) on a sharded deployment paid O(total history)
  instead of O(deleted count) — correct, but silently degraded, with no error or log surfacing the
  missing acceleration.

  **Fix.** Added `ForEachDeletedNodeID`/`ForEachDeletedRelID` to `store/sharded/stats_iter.go`,
  mechanically mirroring the existing `ForEachNodeID`/`ForEachRelID` fan-out pattern: both reuse the
  package's existing generic `forEachID`/`forEachRelID` sequential-fan-out helpers, delegating to each
  shard's OWN `DeletedIterationCapability` — `badgerShard` is a direct type alias for `*badger.Store`
  (`sharded.go:85`), which already implements the capability natively, so each shard's fan-out call is
  a one-line delegation. Unlike `tiered.Store` (time-sharded — the same entity's history can span
  multiple shards, requiring cross-shard ID dedup), `sharded.Store` routes every entity to exactly ONE
  shard by ID, so no dedup is needed — each shard's deleted-ID set is disjoint from every other
  shard's by construction, the same reasoning the pre-existing `ForEachNodeID`/`ForEachNodeHistoryID`
  fan-outs already rely on. `DepthDeletedIterationCapability` (the depth-aware variant) remains
  correctly UNIMPLEMENTED — sharded has no depth/tier concept, so that capability's absence is one of
  the package doc comment's genuine intentional declines, unaffected by this fix.

  **Tests** (`store/sharded/deleted_iteration_test.go`, new file, mirroring
  `sharded_contract_test.go`'s existing `newMemStore`/`mkNodeID`/`mkRelID`/`putNode`/`putRel` fixture
  helpers): `TestForEachDeletedNodeID_FoldsAcrossShards` and `TestForEachDeletedRelID_FoldsAcrossShards`
  — 4 shards, half the entities deleted via `DeleteNodeWithHistory`/`DeleteRelWithHistory` (genuine
  history-row-but-no-current-row deletions, not just absent IDs), half left live; asserts the fold
  visits EXACTLY the deleted set from every shard (both a completeness check — nothing missed on any
  shard — and a negative-assertion check — no still-live entity is visited).
  `TestForEachDeletedNodeID_EarlyStopAndNilCallback` pins the same contract every other `ForEach*` door
  on this store already honors: a nil callback is rejected, and returning `false` stops iteration after
  exactly one visit (not a full-shard scan).

  **Verification.** RED confirmed two ways: `git stash push -- store/sharded/stats_iter.go` against the
  new test file produced a COMPILE failure (`st.ForEachDeletedNodeID undefined`, `st.ForEachDeletedRelID
  undefined`) — the strongest possible confirmation the capability was genuinely absent, not just
  buggy. Stash popped, GREEN confirmed for all three new tests. Full `go build ./...` and `go vet ./...`
  clean. `go test ./pkg/graph/store/sharded/...` — full package pass (all pre-existing contract/fan-out/
  changelog/index-parity tests). `go test -race ./pkg/graph/store/sharded/...` clean. Full-repo
  `go test ./...` clean across every package — no regressions.
- **20g. Adjacency reads always fan out to every claimed shard regardless of endpoint locality — an
  under-documented architectural cost that scales with `SlotCount` regardless of traversal locality
  (MEDIUM, perf).** `store/sharded/rel.go:241-288`, `node.go:183-248`.
- **20h. No upfront cross-validation between `Config.IngestLanes` and the sharded store's claimed
  slot range at construction time — misconfiguration only caught reactively at first write
  (LOW-MEDIUM).**
- **20i. `PutRelationshipsBatch` per-rel apply loop is not atomic per shard group and non-deterministic
  order — same class as 20c (LOW).** `store/sharded/batch.go:220-229`.
- **20j. `PruneTemporalCandidates` has no cross-backend equivalence test vs. a single badger store
  (LOW, TEST-GAP).**
- **20k. `forEachShardErr`/`fanOutUniform`/`parallelShards` spawn one goroutine per shard
  unconditionally, violating lesson 8's bounded-worker-pool rule — low practical impact given the
  32-shard hard cap (LOW).**
- **20l. `GetNodesByIDs`/`GetRelationshipsByIDs` shard-bucket application is sequential, not
  parallel, unlike every other multi-shard read in the file (LOW, perf).** `store/sharded/bulk.go:94-158`.
- **20m. Catalog is fixed identity-only — no re-sharding/rebalancing path exists at all; see BACKLOG
  21 for the feature-level entry (MEDIUM, missing feature, likely intentional "not yet" for a WIP
  backend).**
- **20n. `Clear()` does not restore `vectorDefs`/`propKeyReg` in-memory state — likely fails safe via
  `coalesceUniform` but unverified; RAM/disk desync (LOW-MEDIUM).** `store/sharded/sharded.go:423-445`.

### BACKLOG 21 — Missing library-level features (cross-cutting)

Collected here from "missing feature" notes scattered across the subsystem audits — all rho-tkg-owned,
none sigma's:

- **21a. No `RelPropertyStats` (NDV/min/max HyperLogLog estimator) mirror on the relationship side.**
  `RangeCardinality`↔`RelRangeCardinality` and `PropertyTypeClassCounts`↔`RelPropertyTypeClassCounts`
  are deliberately mirrored pairs (the latter shipped as BACKLOG 5B specifically as a rel ordering-
  soundness primitive); `PropertyStats` has no rel-side equivalent, so a planner costing a
  relationship-property predicate has count/type-class tools but no selectivity estimate. Noted
  independently by 3 separate subsystem audits (façade, memory backend, stats layer).
- **21b. No index-introspection doors for property/temporal/vector/rel-property indexes, unlike
  `HasComposite`/`ListComposites` for composite indexes.** A query planner has no way to ask "does
  label X have a property/temporal/vector index on key Y, with what config?" without issuing the
  query and inferring from latency. `HasComposite`'s own doc comment frames the need generally
  ("so a planner can prove the accelerated path exists before routing"). Fix: add
  `HasProperty(label,key)`/`HasTemporal(label)`/`VectorIndexInfo(label,key)`/`HasRelProperty(type,key)`.
- **21c. No `RelTypeTemporalCandidateCapability` mirror of the node-side B4 prune capability** — the
  store contract's `PruneTemporalCandidates` is typed to `types.NodeID` only, so `relsByTypeLocked`
  can never get B4 acceleration unlike `nodesByLabelLocked` (see BACKLOG 10h).
- **21d. Sharded backend: `DeletedIterationCapability` entirely absent (see BACKLOG 20f)** — worth
  tracking here too since it's a genuine capability gap, not just a bug.
- **21e. Sharded backend: no re-sharding/rebalancing path** — growing/shrinking `SlotCount` on an
  existing deployment is unsupported; a mismatch is a fail-closed `ErrCatalogConflict` with no
  migration tool (see BACKLOG 20m). Likely intentional for the current WIP stage but worth an explicit
  decision/roadmap note before this backend leaves WIP status.
- **21f. No `PreEncodeRelPutPayloadV2` counterpart to the node-side §4.5 pre-encode fast path** — a
  relationship-heavy concurrent-ingest workload always pays the second msgpack pass that a node-heavy
  workload avoids (see BACKLOG 15p).
- **21g. Badger backend: no zero-copy ownership-transfer cache path (`freezeRelForCache`) for bulk
  relationship writes, unlike nodes' `freezeNodeForCache`/`PutNodesBatchOwnedPreEncoded`** — the
  public `PreEncodedPutCapability` contract is explicitly node-scoped by design (ADR-0006 §4.5), so
  this is very likely deliberate, but flagged as a future-extension opportunity if bulk relationship
  ingest throughput ever becomes a target (see BACKLOG 18u).
