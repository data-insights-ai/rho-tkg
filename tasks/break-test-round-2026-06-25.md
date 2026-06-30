# Break-Test Round — 2026-06-25

Scope: adversarial break-test audit of the uncommitted horizontal-scaling change
set (Phase 0/1 change-log + read replicas, plus the in-session fix-C / apply
hardening), answering "everything green? new features fully tested? fully
documented?". A 38-agent audit enumerated unpinned break-cases across 6 areas,
each adversarially verified for real+reachable; confirmed gaps were implemented
test-first. Code changes WERE made (one real bug fixed, 22 new tests, docs).

## Gate (authoritative, this session)

- gofmt / go vet / go build: clean.
- `go test -race ./...`: **34/34 packages, 0 failures** (with all changes integrated).
- cover (`-coverpkg=./...`): **83.1%** (> COVER_MIN 80).
- gosec: 9× G115, ALL in unchanged files (0 from this change set); govulncheck:
  1× `crypto/x509` stdlib advisory (Go-toolchain level). Both pre-existing.
- golangci-lint: NOT installable in this environment (style linter; not a
  correctness/security gate). NOTE: the earlier `make ci` "exit 0" task
  notification was wrong — it actually exited 2 at the missing-linter step.

## Real bug found + fixed (test-first)

**gap-20 — change-log-disabled `Clear()` reused LSNs.** The log-OFF `Clear()` arm
was a bare `db.DropAll()` that wiped `LastLSNKey`. Sequence `ChangeLog`-on →
reopen off + `Clear` → reopen on reseeded the LSN allocator to 0 (failing test:
pre-Clear watermark 2, post-reopen write LSN 1) → silent replica divergence, the
exact lesson-52 failure through the log-disabled door. Fix: both `Clear()` arms
now share `clearDataPreservingLastLSN`, preserving `LastLSNKey` whenever present;
only a never-logged store still `DropAll`s. Pinned by
`TestChangeLog_LogOffClearPreservesWatermark` (fail-first verified). Lesson 52
extended; CHANGELOG updated.

## SECOND real bug — rolled-back-token feed poisoning (the serious one)

Found by deeper analysis after the operator pushed back on treating the
tx-rollback/change-log interaction as a mere documented property ("this is
production code, analyse"). A transaction that allocates a NEW label/rel-type
token emits a durable `ChangeRelPut`/`ChangeNodePut` referencing it, then
`Rollback()` (`restoreRegistries`) DE-allocated the token by rebuilding the
registry from the pre-tx snapshot. The feed then permanently referenced a token
the primary no longer held: a replica — EVEN WITH a `ReplicationSource` — could
never resolve it (the refetch finds it absent, the primary rolled it back too) and
**stalled forever** at that LSN; the number was then REUSED for a different name →
silent divergence. Confirmed by a failing test ("rel type token 1 not in registry
(size 0)").

Scope was the whole token-de-allocation FAMILY: tx rollback (`restoreRegistries`),
standalone label/rel-type add error, batch partial-failure
(`restoreNewLabelsCreateOnError` deletes the nodes it already created — whose puts
referenced the token — then de-allocates), and index creation — all funnel through
`restoreNewLabelsOnError` / `restoreNewRelTypeOnError`.

Fix: registries are APPEND-ONLY across rollback when the change-log is ENABLED. The
de-allocation chokepoints keep tx-allocated tokens (already persisted; an unused
token is harmless), so every emitted record stays resolvable and replicas converge;
log-off behavior unchanged. Gate signal is the new `store.ChangeLogStatus`
`ChangeLogEnabled()` probe captured into `c.changeLogEnabled` — NOT `changeFeed !=
nil` (badger/memory always implement the feed methods; that flag is true even when
the log is off — the trap hit on the first attempt). Completeness proven: of the 6
`RollbackNames` sites, the 4 `getOrCreate*` allocation/persist-failure rollbacks are
PRE-record (no emitted record to orphan; correctly NOT gated, must de-allocate to
keep RAM==disk); the 2 entity-failure rollbacks are POST-record (gated). No
poisoning door remains.

Tests (fail-first verified): `TestReplicaConvergence_RolledBackNewTokenTx`
(integration — stalled before the fix), `TestRestoreNew{Label,RelType}OnError_
KeepsTokenWhenChangeLogEnabled` (white-box leaf gate). Lesson 54 rewritten (the
earlier "no behavior change" framing was wrong); CHANGELOG corrected.

Better solution recorded (not implemented): a per-tx change-log buffer mirroring
`txEventBuffer` (discard on Rollback, emit on Commit) would eliminate the leak AND
the transient-phantom + CDC-asymmetry residuals, but conflicts with the
co-commit-with-data crash-safety invariant (lesson 49) and needs a tx-aware log
scope or a staged-write-set tx — a deliberate tx-architecture change. The
append-only fix is the correct minimal stopgap; the token "leak" is bounded by
distinct schema names (not traffic), symmetric across primary/replica, and
fail-closed.

## Residual contract (after the bug above is fixed) — lesson 54

The change-log is a PHYSICAL redo log: a `GraphTx` applies its writes immediately
(rollback reverses them), the log is emitted in-backend per store mutation, so a
ROLLED-BACK tx still ships its create + hard-cascade rollback-delete ENTITY records
(verified: 2 records past the watermark for a create-then-rollback). With the
token-poisoning fix in place a replica CONVERGES (tails create-then-delete to the
correct final state), but it transiently materializes the uncommitted entity, and
the feed is asymmetric with events (discarded on rollback) — so it is not a logical
committed-tx CDC source. This is also the only public primary door emitting a
hard-cascade `ChangeNodeDelete` (WithHistory=false), so the apply `!WithHistory`
branch is live precisely because of rollback churn. These residuals are CONTRACT,
not a bug (the token de-allocation WAS the bug — see the section above) and are
eliminated by the same per-tx-log-buffer upgrade. Pinned by
`TestReplicaConvergence_RolledBackTxChurnConverges`; lesson 54.

## New break-tests added (22), all pass under -race; key ones fail-first verified

badger (14): NodeAsOf/RelAsOf parked-superseded-version, Truncate/Trim count
parked versions, pending-DELETE-over-flushing-SET precedence, adjacency-disk
incoming/typed/metas, cascade-scrub + incoming-repair END-TO-END key removal, a
REAL concurrent flush-vs-readers `-race` test (fires DATA RACE if `wbMu` is
removed from `rangePending`), log-off-Clear watermark preservation (gap-20),
never-logged-Clear, double-Clear monotonic marker.

pkg/graph (6): ChangeClear apply wipes+reanchors, corrupt/hash-mismatch/wrong-tag
payload fails closed WITHOUT advancing the watermark, equal-LSN duplicate
rejected, guard-tracks-across-stale-skips, per-tag idempotent delete-of-missing,
rolled-back-tx churn converges.

core (2): labelTokenDiff 2-removed count, applyNodeLabelChangeLocked fails closed
on a 2-removed diff (symmetric half of the bug the change fixed).

Fail-first verified by reverting the production behavior and observing failure:
gap-20 (LSN reuse), NodeAsOf overlay, 2-removed guard, and the race test's
data-race detection (all confirmed to fail/fire on revert).

## Honest gaps NOT closed (named, not buried)

- The concurrency `-race` test catches the `wbMu` DATA RACE but NOT a silent
  flushing-loop removal (that functional property is covered by the deterministic
  white-box `parkPendingIntoFlushing` tests). Complementary, not redundant.
- gap-23 (plain no-history label-door apply end-to-end), gap-16 (cascade
  flush-trigger pressure test), gap-15 / gap-6 (history-ID set-algebra / delete-
  scrub error arms) were judged lower value and left as named gaps.
- not-reachable / already-covered / not-worth-it audit candidates were correctly
  not implemented.

## Release readiness

NOT release-ready as a tag: this round found+fixed a real silent-divergence bug,
the work is still UNCOMMITTED, and it is mid-phase (Phase 1, partial — sigma's
network/orchestration half deferred). Recommendation: land via commit/PR (matching
#2/#3/#4), not a rushed tag. Nothing committed.
