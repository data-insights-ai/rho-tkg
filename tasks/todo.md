# tkg/v4 — open work

Completed review passes and the change-log/replica hardening (badger `flushing`
overlay fix C, Clear-reanchor LSN durability, `ApplyChanges` ascending guard,
multi-token-label-diff apply hardening, and the rolled-back-token append-only
STOPGAP) are recorded in `CHANGELOG.md` `[Unreleased]` and `tasks/lessons.md`
49–54. This file tracks **only open work**; finished changes move to `CHANGELOG.md`.

---

# Per-transaction change-log buffer (the PROPER fix, replacing the append-only stopgap)

**Goal.** A ROLLED-BACK transaction (and a partially-failed batch) must emit
NOTHING to the change-log feed — mirroring the in-memory `txEventBuffer` for
events. This is the proper architectural fix that SUPERSEDES the append-only-
registry stopgap (lesson 54) and ALSO removes its two documented residuals: the
transient replica phantom and the CDC-asymmetry (a rolled-back tx today durably
ships its forward + reverse entity records).

**Why the stopgap is insufficient.** The change-log is a PHYSICAL redo log emitted
in-backend per store mutation; a `GraphTx` applies mutations IMMEDIATELY during the
body and reverses them on rollback (also emitting records). The stopgap only stops
the rolled-back tokens from being de-allocated; it does not stop the entity churn
or close the transient-phantom window. The user asked for the per-tx buffer.

## Architecture facts established (do not re-derive)

- Records are emitted IN-BACKEND: `logChangeRaw` / `appendOpsLogged` (badger
  `badgerstore_changelog.go`), `logChangeLocked` (memory `memorystore_changelog.go`),
  each minting an LSN at door time and appending to `pendingLog` / `changeLog`.
- Records CO-COMMIT with data in ONE WriteBatch in badger `flush()`
  (`badgerstore_flush.go:120-259`): `pending` (data) + counters + `pendingLog` +
  `LastLSNKey`, crash-atomic (lesson 49). Memory has no flush — records append
  directly under `ms.mu`.
- CONCURRENCY (Path B, v4.1.0): a tx mutation holds `c.mu.RLock` via
  `lockActiveCore` (tx.go) — it is **shared**, so concurrent STANDALONE mutations
  (also `c.mu.RLock`) run in parallel and emit records concurrently. (The
  node_add.go comments claiming "tx holds c.mu.Lock" are STALE.) So a store-global
  "scope active" flag would MISROUTE a concurrent standalone record into the tx
  buffer unless the flag is only ever true while NO standalone can run.
- Events dodge this because they dispatch in the WRAPPER (`NodeOps.Add` →
  `runUnderRLock` returns `ep`, then `dispatchEvent` AFTER the lock). Records can't
  use that template — they are backend-emitted.

## Design (decided)

A store-global record buffer (`scopeLog`) + a `scopeActive` divert flag, made safe
by a **per-mutation divert toggle the core flips only under an EXCLUSIVE write
lock**, with **LSNs minted at COMMIT** (a rolled-back tx burns none, leaves no feed
gap). New optional `store.TxChangeLogScope` capability:
`BeginLogScope / SetLogDivert(on) / CommitLogScope / DiscardLogScope`. Gated on
`changeLogEnabled` so non-ChangeLog graphs keep full Path B concurrency.

The concurrency lever: a ChangeLog-enabled tx takes `c.mu.Lock` (EXCLUSIVE)
**per-mutation** (NOT per-lifetime — that would re-introduce the v3.4 in-tx-read
deadlock, lesson 31) and brackets the mutation with `SetLogDivert(true/false)`.
Because the exclusive Lock excludes a concurrent standalone's RLock, `scopeActive`
is true only when no standalone can emit. Reads stay on RLock (concurrent).

## DONE

- [x] **Store-side (badger).** `scopeLog [][]byte` + `scopeActive bool` fields
      (badgerstore.go, under wbMu); divert in `logChangeRaw` + `appendOpsLogged`
      (no LSN minted while diverted); `BeginLogScope` / `SetLogDivert` /
      `CommitLogScope` (mint contiguous LSNs at commit → pendingLog → flush) /
      `DiscardLogScope` (drop, burn no LSN) in badgerstore_changelog.go.
- [x] **Capability interface** `store.TxChangeLogScope` in `pkg/graph/store/changefeed.go`.
- [x] **Store-level tests** `badgerstore_logscope_test.go` (discard emits nothing +
      burns no LSN; commit mints contiguous LSNs + co-commits data; non-diverted
      write while scope-open-but-divert-off emits eagerly). All pass; no regression
      in existing change-log/replica/flushing tests.

## DONE — TX path (the user's explicit ask) — COMPLETE + race-clean

- [x] **Memory store scope** (`memorystore.go` + `memorystore_changelog.go`):
      divert + Begin/SetLogDivert/Commit/Discard; `memorystore_logscope_test.go`.
- [x] **Core capability resolve** (`core.go` `txChangeLogScope`, gated on enabled).
- [x] **Core locking change** — `lockActiveCoreWrite` / `lockActiveCoreWriteContext`
      / `unlockActiveCoreWrite` (tx.go): exclusive `c.mu.Lock` + `SetLogDivert` when
      a scope is active, RLock otherwise. All 14 `tx_mutations.go` mutation methods
      switched; READS keep `lockActiveCore` (RLock). No reentrancy (per-mutation,
      not per-lifetime).
- [x] **Wire tx lifecycle** (tx.go): Begin→BeginLogScope; Commit→CommitLogScope
      after persistRegistries (error ⇒ not done); Rollback→divert ON across reverse
      mutations then DiscardLogScope.
- [x] **Revert the TX `restoreRegistries` stopgap** → exact pre-tx-registry rollback
      (safe: the buffer makes the tx emit nothing). KEPT the
      `restoreNewLabelsOnError`/`restoreNewRelTypeOnError` skip (non-scoped
      batch/standalone emit eagerly) and propkeys append-only.
- [x] **Tests** (fail-first verified where noted): store-level
      (`badgerstore_logscope_test.go`, `memorystore_logscope_test.go`);
      `TestReplicaConvergence_RolledBackTxEmitsNothing` (was ...ChurnConverges,
      rewritten — rolled-back tx ships 0 records, no LSN burned);
      `TestReplicaConvergence_RolledBackNewTokenTx` (replica converges, token
      exactly de-allocated); `TestReplicaConvergence_CommittedTxViaBuffer`
      (contiguous commit-time LSNs, byte-exact converge);
      `TestTxScope_ConcurrentStandaloneNotMisrouted` (**-race**, FAIL-FIRST verified
      — fails/under-counts when the exclusive Lock is reverted to RLock).
- [x] **Accepted residual documented** (CHANGELOG + lesson 54): change-log-tx DATA
      still flushes during the body; a crash between a mid-tx data flush and commit
      leaves committed-but-unlogged data — invisible to the feed (watermark not
      advanced), no worse than pre-existing SyncWrites tx non-atomicity. Closing it
      fully needs a staged-write-set tx (rejected — far larger).
- [x] **Docs**: CHANGELOG `[Unreleased]` per-tx-buffer entry; lesson 54 updated
      (IMPLEMENTED for tx + the two implementation lessons: backend-emitted records
      ⇒ exclusive-per-mutation-lock not handle-threading; co-commit only at commit).

## DONE — Batch + Import

- [x] **`Batch.Execute` scope.** Batch holds `c.mu.Lock` + `c.txMu` for its whole
      duration (exclusive → divert stays ON the whole batch, no per-mutation toggle,
      no misroute). Because the batch KEEPS successful ops (`result.Failed++;
      continue`) it ALWAYS commits the scope (never discards — discarding would lose
      records for committed data). `PutNodesBatch` is two-phase (validate then
      apply-cannot-fail), and the `deletePartialBatchNodes` cleanup emits matching
      delete records, so every emitted record has corresponding committed-or-cleaned
      data — no orphan records; a failed-op's create+delete churn converges. The
      append-only token stopgap is KEPT for the batch (it commits its records).
      Early-return/panic paths discard the scope via the defer (scopeOpen/
      scopeCommitted flags) so no scope leaks. Pinned by
      `TestReplicaConvergence_CommittedBatchViaBuffer` (records emitted at batch-end
      with contiguous LSNs; replica converges); `-race` green.
- [x] **`IO().Import` rollback poison — GUARDED by the append-only stopgap.**
      `importRollback.restoreRegistries` now gated on `changeLogEnabled` (keep
      tokens). A read-only replica's bootstrap import has the log OFF so emits
      nothing. FOLLOW-UP (optional, lower priority): wrap import in a scope so a
      failed import emits NOTHING — needs import to also take `c.txMu`.

## Remaining follow-ups (optional, lower priority)

- [ ] **Import-under-a-scope** (proper fix beyond the import stopgap): wrap
      `IO().Import` in a `TxChangeLogScope` so a FAILED import emits NOTHING (not
      just "no poison"). Needs import to also take `c.txMu` (it currently takes only
      `c.mu.Lock`, so a between-mutations open tx scope would collide with import's
      BeginLogScope). Locking change — deferred. Current stopgap is correct.
- [ ] **tiered**: confirmed it does NOT implement `TxChangeLogScope` (no change-log)
      → core resolves nil → tx/batch emit eagerly (moot). If a sharded backend ever
      gains a change-log, replicate the scope (B33-class).
- [ ] **Coverage polish**: `lockActiveCoreWriteContext` happy path (tx
      ImportNodeWithID under change-log) is lightly covered — add a direct test.

## Gate (DONE for the implemented work)
- [x] `go test -race` green: badger, memory, graph, internal/core (full suite 34/34).
- [x] `gofmt`/`go vet` clean; `TestDocsMetadataMatchesSourceOfTruth` green.
- [ ] `make check` / `make cover` final pass (security tools not installed locally;
      gosec/govulncheck findings are pre-existing, see the 2026-06-25 review snapshot).
