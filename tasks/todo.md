# tkg/v4 — architecture-review remediation plan (2026-06-10)

Source: full architecture review of v4.4.2 (7 subsystem deep-dives, findings
verified against code before listing). Previous todo.md content (v3.1.11 era)
was fully merged/obsolete and is superseded by this plan.

Working rules for this pass:

- All changes land under `[Unreleased]` in CHANGELOG.md (no version bump, so
  `TestDocsMetadataMatchesSourceOfTruth` stays green).
- **Test after each phase.** Tests must be adversarial, not happy-path:
  multi-entity diverging lifecycles, exact-set assertions, negative
  assertions, corrupted/hostile input, crash-shaped scenarios, concurrency
  with `-race`. Every test must fail if the implementation silently did the
  old/wrong thing (Testing Rules 15–16).
- Run targeted `go test -race` for each touched package per phase; full
  `make check` at the end.

## Phase 1 — On-disk format versioning  [HIGH #1] ✅ DONE

Landed: `FormatVersion` (`fv`) on NodeWire/RelWire + custom encoders; sentinel
`store.ErrWireFormatVersionUnsupported` (+ graph re-export); badger
`wire_format_version` meta marker verified/stamped at open
(badgerstore_format.go); loadIndexes fails closed on future-format rows
instead of silently dropping them (the adversarial test caught exactly this
gap in the pre-existing `persisted == rawEntityRows` heal-down branch).
Tests: wire_format_version_test.go (storeutil), badgerstore_format_test.go.
Full suite green.

Problem: `NodeWire`/`RelWire` (`storeutil/wire.go`) carry no format version;
no store-level format marker checked at open. Schema evolution decodes old
rows silently with zero values; mixed-version data after partial upgrade is
undetectable.

- [ ] Add `FormatVersion` (`msgpack:"fv"`) to `NodeWire` and `RelWire`.
      Decode: `0` (absent on legacy rows) is treated as version 1;
      `> CurrentWireVersion` → fail closed with new sentinel
      `ErrWireVersionUnsupported`. Encode: always write current version.
      Audit lesson 39: update any custom `EncodeMsgpack` so the field is
      actually emitted.
- [ ] Store-level marker: meta-KV key `wire_format_version` (badger), written
      at open when absent, validated at open — stored > supported → refuse to
      open (fail closed), stored < supported → allowed (rows self-describe).
      Tiered: every shard already shares badger meta handling; verify the
      marker per shard.
- [ ] Adversarial tests: (a) hand-craft a row with fv=99 → Get fails with
      `errors.Is(ErrWireVersionUnsupported)`, store does NOT silently drop the
      row into "not found"; (b) legacy row without fv decodes and is
      readable (backward compat); (c) meta marker ahead of binary → New()
      fails closed; (d) marker absent (pre-upgrade dir) → opens and stamps;
      (e) reopen after stamp → still opens; (f) mixed legacy+current rows in
      one store both readable.

## Phase 2 — Single source of truth for temporal predicates  [HIGH #2] ✅ DONE

Landed: storeutil exports `MatchesPointInTime`/`MatchesInterval` as the
canonical predicates (boundary-exact direct tests in
temporal_predicates_direct_test.go); core `nodeValidFrom`/`relValidFrom`/
`isNodeValidAt` delegate to storeutil (generator/Layout equivalence verified:
same epoch, µs, 5/10 bits). Cross-door equivalence test
`TestTemporalTwoDoorsAgreeOnLabelQueries` (pkg/graph, memory+badger).

**The equivalence test found a real bug**: AddLabel/RemoveLabel and the
node/rel property-CAS paths inherited ValidFrom/ValidTo from the previous
version (lesson 33's bug class behind 4 more doors) — historical label
queries resolved to the post-mutation state. Fixed in node_label.go +
property_cas.go (all four sites clear VT); regression tests per door in
inherited_validtime_regression_test.go; lesson 42 added. Full suite green.

Problem: effective-valid-from and point/interval predicates exist twice —
core (`internal/core/temporal.go`: `nodeValidFrom`, `isNodeValidAt`, during
scans) and storeutil (`temporal_filter.go`: `EntityValidFrom`,
`MatchesTemporalFilter` used by store push-down). Drift changes query results
depending on which door serves the query.

- [ ] Refactor core helpers to delegate to the storeutil predicates (core
      already imports storeutil; no new dependency edge). Zero behavior
      change intended.
- [ ] Cross-door equivalence test (adversarial): one dataset containing —
      explicit ValidFrom/ValidTo, unset VT (snowflake fallback), entity whose
      label held mid-interval but not on most-recent version, deleted entity
      with history, zero-width eclipsed row. Assert EXACT same ID sets from:
      named door (`NodesByLabelAt`), generic door (`ByLabel` with temporal
      opts → store push-down), and per-ID resolver (`NodeAt`). Run against
      memory AND badger. Must include "must NOT contain" assertions.

## Phase 3 — Shared mutation kernel: batch vs standalone rel-create  [HIGH #3] ✅ DONE

Landed: `relationship_create_kernel.go` — `prepareRelCreate` (canonical input
validation order), `relEndpointHashLadder` (constraint/hash capability
ladder), `buildRelFromSpec`, and `createRelWithTypeRollback` (token
allocation + persist + rollback, exactly-once registry finish, partial-live
contract). All three standalone doors (Add / AddByID / AddByIDIfAbsent) and
the batch Execute door now share the kernel (~250 lines of quadruplication
removed). Batch queue-time validation preamble intentionally kept (its error
messages are part of observed behavior); door-equivalence tests pin it.
Tests: batch_door_equivalence_test.go (self-loop parity both directions,
missing-endpoint + type-token rollback, valid-time parity incl. resolver
boundaries, integrity/endpoint-hash parity, reserved-prefix parity).
Core race suite + full suite green.

Problem (verified): `batch_execute.go:282` re-implements relationship
creation inline (LockTwo + endpoint checks) instead of sharing code with
`addRelationshipInternal`. Every invariant added to the standalone path can
silently miss the batch door (the MR protocol compensates by process).

- [ ] Read full API of `relationship_add.go`, `batch_execute.go`,
      `batch_queue.go`, `tx_mutations.go` first.
- [ ] Extract a lock-free creation kernel (validate + stamp + integrity +
      write) used by both standalone (entity locks + RLock) and batch
      (global Lock) paths. Keep lock-acquisition strategy at the callers.
- [ ] Audit node-create/update/delete batch paths for the same duplication;
      extract where it exists.
- [ ] Adversarial tests proving the doors are equivalent: for each invariant
      (self-loop rejection per `AllowSelfLoops`, missing endpoint, duplicate
      ID, `tkg_valid_from` before previous, reserved-prefix property, hash
      chain / FromNodeHash-ToNodeHash freshness, event emission, registry
      rollback on mid-batch failure) — assert the BATCH path rejects/behaves
      exactly like standalone. A test that passes when batch silently skips a
      check is not acceptable.

## Phase 4 — Bound the badger dirty-cache  [MED #5] ✅ DONE

Landed: `Config.MaxPendingWrites` (default `DefaultMaxPendingWrites` =
100_000; negative disables; moot under SyncWrites). The post-mutation hook
`flushIfNeeded` (unified from `flushIfSyncWrites` + 20 inline
`if bs.syncWrites` sites — single seam now) makes the WRITER flush
synchronously when the pending buffer crosses the bound: backpressure
instead of unbounded pending-map + dirty-cache growth. Flush errors surface
to the writer (fail closed) with ops requeued. Visibility:
`Store.PendingWriteCount()`. Scope cut (documented): StoreStats interface
left untouched (type-asserted optional interface — widening it breaks
out-of-tree implementers).
Tests: badgerstore_pressure_test.go — burst bound + zero-loss exact set,
8-writer concurrent bound under -race, negative opt-out, flush-failure
surfacing via the OnPropertyKeyGrow write-ahead seam + requeue + recovery.
Full suite green.

Problem: dirty LRU entries are never evicted and there is no dirty-count
ceiling or write backpressure; a sustained write burst faster than the flush
interval grows memory without bound.

- [ ] Add `MaxDirty` (name TBD after reading config) to `BadgerStoreConfig`
      with a sane default; when the dirty count crosses the threshold, the
      writing goroutine triggers a synchronous flush (outside idxMu; respect
      flushMu and dbClosed) instead of growing the queue.
- [ ] Expose dirty/clean counts via the existing StoreStats opt-in.
- [ ] Adversarial tests: tiny threshold + write burst → assert dirty count
      stays bounded AND no writes lost (read-back exact set); concurrent
      writers during forced flush (`-race`); threshold crossing while Close
      is in flight → fail closed, no deadlock, no lost flush; flush error
      path (closed DB) surfaces, doesn't wedge writers.

## Phase 5 — Tiered store: background-error recovery + observability  [MED #6, #7] ✅ DONE

Landed: `tiered.Store.RecoverBackgroundError()` — operator-driven recovery
from the sticky background error: re-probes persistence via an atomic
catalog save; success clears the gate without close/reopen, failure retains
the original cause (joined with the probe failure). Fail-closed default
unchanged; doc comments updated (the "no clear/reset path" claim was stale
the moment this landed). RunRepair now warns (slog) with exact counts when
it fixed cross-shard inconsistencies. Retry-exhaustion errors in
deleteNodeInternal and lockRelationshipCurrentEndpoints now report what kept
changing on the final attempt (lock-set vs observed sizes / peeked vs locked
endpoints). Exposure via g.Tier() deferred (needs core.AdminOps + tier.Ops +
wrapper additions; noted for a follow-up release).
Test: tieredstore_recover_test.go — poison → fail-closed reads+writes →
recovery refused while catalog dir unwritable (gate retained, cause kept) →
heal → recovery clears → full read/write WITHOUT reopen → idempotent.
Full suite green.

- [ ] `recordBackgroundError` (tieredstore.go:611): bounded retry for
      catalog.Save at the rotation/save call sites (transient I/O), and an
      explicit recovery operation that re-attempts catalog persistence and
      clears the sticky error on success (decide surface after reading:
      tiered.Store method, optionally exposed via g.Tier()).
- [ ] Repair observability: when RunRepair fixes/detects >0 issues, surface
      counts in RepairResult (verify present) + warn-level log.
- [ ] Delete/endpoint-lock TOCTOU retry exhaustion: include retry count and
      what changed in the returned error (wrapped sentinel preserved).
- [ ] Adversarial tests: inject failing catalog dir (chmod/rename) →
      operations fail with the background error; restore dir + recovery op →
      store usable again WITHOUT reopen; recovery while dir still broken →
      sticky error remains; assert `errors.Is` still matches at every layer.

## Phase 6 — Error-sentinel consolidation + anti-drift test  [MED #8] ✅ DONE

Audit result: NO duplicate declarations existed — store/core/io/index each
own their sentinels, every other surface (pkg/graph/errors.go, badger
aliases, tiered/aliases.go, core's io aliases) is a pure alias. (The review
agent's "io declares duplicates" claim was a false positive: pkg/graph/io is
the canonical home; core aliases FROM it.) What was missing was protection:
added errors_identity_test.go (identity assertions across all exported
surfaces + behavioral errors.Is checks through every qualifier on real
engine errors) and the canonical-surface doc header on pkg/graph/errors.go.

- [ ] Audit every sentinel: exactly one canonical declaration; all other
      surfaces (pkg/graph/errors.go, index, io, temporal, tiered aliases) are
      pure aliases (`= pkg.Err…`), never duplicate `errors.New`.
- [ ] Add an identity test that asserts `errors.Is` equivalence across all
      exported homes for each sentinel (fails if someone re-declares).
- [ ] Document in pkg/graph/errors.go that it is the canonical consumer
      import surface.

## Phase 7 — Small hardening fixes  [LOW] ✅ DONE

- types.ErrNilNode/ErrNilRelationship message prefix graph:→types: (no test
  asserted the literals; identities unchanged).
- Doc contracts hardened: Temporal()/Integrity() MUST-NOT-mutate (both
  mirror types), AppendPropertyHashBytes panic contract (both),
  CreateVector rebuild-on-restart latency note, Store-interface sharded
  class-immutability invariant note (B33).
- Tiered registry-injection guard: VERIFIED ALREADY CORRECT — v4.3.1 seeds
  ts.propKeyReg from the ref shard immediately at open (tieredstore.go:279),
  before any other shard opens; rotation-created shards inject the seeded
  instance. No change needed (review finding was stale).

- [ ] `pkg/types/errors.go`: `ErrNilNode`/`ErrNilRelationship` message prefix
      `graph:` → `types:` (check no test asserts the literal string).
- [ ] Doc-comment hardening: `Node.Temporal()`/`Integrity()` (+ Relationship
      mirrors) — explicit MUST-NOT-mutate-outside-graph-layer warning;
      `AppendPropertyHashBytes` panic-recovery contract;
      vector-index rebuild-on-restart latency note at the public surface;
      Store-interface doc note: sharded backends must enforce primary-label
      class immutability (B33).
- [ ] Tiered registry-injection defensive guard: seed `ts.propKeyReg` from
      the ref shard immediately at open so later shard opens never inject
      nil (verify current ordering first; add assertion/test).

## Phase 8 — Documentation debt + CHANGELOG  ✅ DONE

architecture.md: badger format-versioning + write-pressure sections; canonical
temporal predicates; valid-time-inheritance rule; cascade crash-tolerance
invariants (the three reasons non-atomicity is safe); bitemporal shim sunset
conditions; RunRepair/RecoverBackgroundError in Admin & Repair; new
"Deferred Architectural Decisions" section. CLAUDE.md: MaxPendingWrites in
config table; wire-versioning/canonical-predicates/create-kernel design
rules; tiered recovery bullet. CHANGELOG [Unreleased]: one entry per phase.
lessons.md: lesson 42 (deep-copy mutation doors).

- [ ] Cascade interval-edit crash-tolerance assumptions → docs/architecture.md
      (each row write atomic; eclipsed rows resolver-invisible; no
      in-progress marker by design).
- [ ] Bitemporal back-compat sunset plan (inherited-ValidFrom heuristic,
      `bitemporalMigrated` flag, UpdatedAt-as-vEnd fallback): state the
      retirement condition explicitly.
- [ ] Record deferred decisions (below) in docs/architecture.md.
- [ ] CHANGELOG `[Unreleased]`: one entry per phase. Update CLAUDE.md /
      AGENTS.md prose where behavior changed (no Status bump).
- [ ] tasks/lessons.md: add lessons learned during this pass (if any new
      pattern emerged).

## Final gate ✅ DONE (2026-06-10)

- `make check` (vet + build + full test suite): green, all 33 packages.
- `-race` on core, badger, memory, tiered, types, graph, storeutil: green.
- `make cover`: total 84.5% (floor 80%). New code: prepareRelCreate 94.7%,
  relEndpointHashLadder 92.9%, buildRelFromSpec 86.7%,
  createRelWithTypeRollback 97.1%, MatchesPointInTime/MatchesInterval 100%,
  flushIfNeeded/pendingLen/PendingWriteCount 100%,
  verifyAndStampWireFormatVersion 92.9%, RecoverBackgroundError 92.3%.
- `TestDocsMetadataMatchesSourceOfTruth`: green (all work under
  [Unreleased]; no version bump).

## Deferred (explicit decisions, not silent omissions)

- **Core sub-packaging** (split internal/core into core/tx, core/temporal,
  core/mutate): multi-week restructure, no behavior change; needs its own
  planned effort. Phase 3's shared kernel reduces the most acute coupling.
- **`pkg/graph/hash` / `pkg/graph/io` renames**: breaking; v5 item.
- **Iteration-capability matrix redesign** (parameterized iteration
  interface): breaking store-contract change; v5 item.
- **Eclipsed-row explicit wire flag**: becomes cheap once Phase 1's format
  versioning exists; schedule with the next wire bump rather than forcing a
  second decode path now.
- **tier package returning `tiered.*` types**: changing is breaking; noted in
  docs as a v5 cleanup.
