# Architecture Review - 2026-06-24

Scope: review of the uncommitted horizontal-scaling working tree (Phase 0 op-log
+ Phase 1 read replicas, on top of v4.9.4) and the broader Phase-1 surface.
Started from a flagged concern that "fix C" (the badger `flushing` commit-window
overlay) was not well analysed. Extended to the full Phase-1 correctness surface,
then to test analysis, coverage, and a documentation reconciliation pass. Code
changes WERE made this pass (one bug fix, one hardening, tests, docs); the tree is
green (34/34 packages, `-race` on badger/graph/tiered/memory) and uncommitted.

## Result

Of 7 reviewed surfaces: **1 real bug fixed**, **1 defense-in-depth gap hardened**,
**5 verified correct as written**. Every finding is resolved (fixed-and-tested),
none deferred. The Phase-1 implementation is high quality — trust boundaries,
durability ordering, the gapless-bootstrap invariant, and ID-collision safety all
hold.

## Reviewed Surfaces

### 1. Badger `flushing` commit-window overlay (fix C) — BUG FIXED

Status: real bug, fixed + 12 tests (lessons 52–53).

`flush()` releases `idxMu` before the WriteBatch commit, so for the whole commit
window a just-written row is gone from `pending` and not yet in a Badger `View`.
The `flushing` map parks the swapped-out snapshot so overlay readers still see
those rows — but the fix was incomplete AND self-harming:

- Only 1 of ~13 overlay readers (`pendingHistoryIDOverlay`) consulted `flushing`;
  the single-key version lookups, history scans, `Max*HistoryID`, truncate/trim
  retention scans, the AsOf version-chain overlay, and the on-disk label/adjacency
  overlays all still dropped in-flight rows. Routed every reader through
  `rangePending` / `lookupPending` (the latter had been dead code).
- The success path never cleared `flushing`; combined with `Clear()` not resetting
  it, a wiped history keyspace RESURRECTED phantom history IDs. Now cleared on
  commit success, on `Clear()`, and on the failed-flush requeue.
- The cascade-delete and incoming-repair MUTATORS that compute delete sets also
  consult `flushing` now (else they orphan a persisted index key during the
  window).

Evidence: `badgerstore_flush.go:250` (success clear), `:160` (requeue clear),
`badgerstore.go:1354` (Clear reset); `badgerstore_history.go` rangePending/
lookupPending wired across readers; `badgerstore_flushing_test.go` (12 tests, all
failing-first verified by reverting each fix). Memory + tiered backends have no
async commit window — unaffected.

### 2. ApplyChanges ascending-LSN guard (finding A) — CORRECT

Status: no issue. New `store.ErrChangesNotAscending`: a record whose LSN is not
strictly greater than the previous stops the batch (strictly-ascending prefix
still flushed + watermarked) instead of being silently swallowed by the
already-applied watermark skip, which would leave a permanent coverage gap. The
normal at-least-once redelivery pattern (an ascending batch starting below
`AppliedLSN`) still passes. Evidence: `replication.go:251-268`.

### 3. Clear-reanchor LSN durability (finding B) — CORRECT

Status: no issue. `clearAndReanchorChangeLog` wipes via `DropPrefix` while keeping
`LastLSNKey` continuously durable, so a mid-Clear crash can't reseed the LSN
allocator to 0 and strand a tailing consumer. The counters-before-data ordering is
load-bearing: `reconcilePersistedCounter` (`badgerstore.go:1207`) sends
`persisted>0, liveRows=0` to a fatal `ErrInvalidStoreMutation`, so reversing the
order would brick reopen after a crash. `Clear()` holds both `flushMu` + `idxMu`,
so no writer/flush races the scan→delete→drop→marker sequence. Lesson 52.

### 4. Replica apply engine — REVIEWED, 1 finding HARDENED

Status: idempotency sound; one defense-in-depth gap hardened + tests.

`PutNodeVersion`/`PutRelVersion` are blind upserts, so history-version replay is
idempotent (the doc's claim holds). Every handler tolerates redelivery (identical
row no-op; delete tolerates missing entity). Tag emission/handler cross-check is
clean: all 9 active tags emitted by both backends + handled; `ChangeMeta` emitted
by neither (deferred, consistent).

Finding (hardened): `labelTokenDiff` collapsed a multi-token label diff to one
arbitrary token, and `applyNodeLabelChangeLocked` only failed closed on
add+remove — so a malformed/gapped feed could silently apply one token and diverge
the label index from the row. `labelTokenDiff` now reports added/removed counts and
the apply path rejects any diff that is not exactly one token. Evidence:
`apply_record.go` `labelTokenDiff` / `applyNodeLabelChangeLocked`;
`apply_label_diff_test.go` (6 count cases + fail-closed, failing-first via the
`single-token mutation` message). The legitimate single-token path is still
covered by the replica convergence tests.

### 5. SnapshotLSN export/import gapless bootstrap — CORRECT

Status: no issue. Export captures `SnapshotLSN` = `LastCommittedLSN` (the committed
watermark — a LOWER bound on snapshot coverage) under `c.mu.Lock`, before streaming
entities (`export.go:104,138`). A background flush advancing the watermark between
capture and stream only causes a re-applied OVERLAP (idempotent no-op), never a
gap — the safe direction. Import auto-applies `header.SnapshotLSN` to the watermark
with flush-before-watermark ordering after hash-chain verification
(`import.go:522-528`). Covered by `snapshot_lsn_test.go` + `phase1_snapshot_lsn_test.go`.

### 6. IDSlotLease failover hint — CORRECT

Status: no issue. Read via `SafeUnmarshal` (untrusted-bytes boundary) with slot
range-validated 0-15 on BOTH read and write, so a corrupt/foreign record fails
closed at read rather than being carried into a `New()` that rejects it. Documented
last-writer-wins (not CAS) — the orchestrator serializes writes and assigns slots;
split-brain ID-collision safety depends on a promoted node and its predecessor
holding DIFFERENT slots, a coordination concern the library rightly does not try to
enforce. Evidence: `replication.go:111-167`.

### 7. Tiered change-log declining — CORRECT

Status: no issue. `tiered.Store` does not implement `ChangeFeedCapability`, so the
type assertion fails and `changeFeedCapability` (`core.go:712`) returns nil. The
`embedsNativeCapability` reflection guard forces nil for a future wrapper that
merely EMBEDS a native store (preventing it from promoting one shard's per-shard
log as the cluster feed). All feed reads route through `changeFeedOrErr`
(`replication.go:29`), which fails closed with `ErrCapabilityNotSupported`.

## Follow-up work (test analysis → coverage → documentation run)

- **Test analysis**: 3763 tests / 355 files, mature (1966 `errors.Is`, exact-set
  helpers, two-phase temporal, 14 adversarial/fuzz files). 83.0% cross-package
  coverage (`-coverpkg=./...`, > `COVER_MIN=80`). 55 genuinely-uncovered functions
  mapped; standout was the 0% replica apply engine (a per-package-attribution
  artifact, but several handlers were genuinely untested).
- **Coverage**: added `TestReplicaConvergence_RelDeleteAndVersionInterval` (+
  `mustRelHistory`) closing 3 of 4 apply-engine gaps (rel-delete, node/rel
  history-version) and exercising `SetNode/RelVersionInterval`. Remaining documented
  gaps: `applyHistoryTruncateLocked` (tx/import-only, no façade door), docvalues
  multi-label, tx temporal mirrors.
- **Documentation run** (full-docs reconcile): all 11 living docs reconciled against
  code — CLAUDE/architecture/SPEC/api/design/perf×2 (factual drift: sub-API 14→15,
  store sentinels 12→30, registries 2→3, snowflake 41→48-bit µs, removed
  non-existent `ImportWithOptions`/`RegisterLegacyProvider`, etc.); AGENTS.md fully
  reconciled (stale pre-v4.0 `pkg/graph` block → thin-façade; `g.mu`-for-tx-lifetime
  → v4.1.0 `c.txMu`/`Core.mu` Path B); README release notes extended v4.9.1–4 +
  [Unreleased]. CHANGELOG/lessons/todo updated. Self-review of the change set found
  no inconsistencies (lessons use no brittle line refs; all doc symbols verified to
  exist; CHANGELOG claims match code).

Gate: build clean, `go vet`/`gofmt` clean, `TestDocsMetadataMatchesSourceOfTruth`
green, full suite 34/34, `-race` green on badger/graph/tiered/memory, no dead links.
Nothing committed.
