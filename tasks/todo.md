# todo — reanalysis: v5 plan (Downloads/PLAN.md, RESEARCH-REVIEW.md, DISCUSSION.md) vs the code (2026-10-09, later)

## User requests

1. "reanalyse the full code and the new architecture change" — check: `tasks/review-v5-plan-vs-code-20261009.md`
   exists, every claim carries file:line, agent findings cross-checked; summary in chat. [x] (2026-10-09)
2. (ai-soc-main-e7 cross-session, on René's behalf) seven consumer requests — check: each answered take /
   decline / already have in the reply and recorded in the review §5. [x] (eight items, sent 2026-10-09)

Plan: five read-only survey agents (temporal model, storage, tx/consistency, access/indexes — Opus; consumer
inventory — Sonnet); I read AGENTS.md, backlog, lessons 43/46/55/60/62/71/73, handovers, CHANGELOG head.
No code changes in this task.

---

# todo — DeleteWithTx / UpdateWithTx behind AllowTxBackfill (2026-10-09)

Spec: `tasks/handover-tx-backfill-delete-update-20261009.md` (§6 overrides §2-§4; §5 is the test list).

## User requests

1. "analyse if all these changes are good and how to do it properly" — check: handover §6 written. [x] (§6, 2026-10-09)
2. "tests always first and no happy path tests, always break the code tests" — check: every new test in the
   branch names a faulty implementation; red output kept in `tasks/evidence/tx-backfill-delete-update/red-*.txt`
   before the code commit. [ ]
3. "analyse if these changes break any existing code.. rho-tkg is in a lot of other libraries" — check: §6.10
   [x]; after implementation R0 green and `go build ./... && go vet ./...` green in sigma-tkgd, ai-soc
   engine, agent-bookkeeping against the merged tree (`replace` in a scratch copy, never committed there). [ ]
4. "then lets go.. spawn parallel opus agents in worktrees" — check: branches merged into main, worktrees
   removed, `make test-race` + `make cover` (new code >= 80 %) green on main. [ ]
5. "spawn a parallel sonnet agent which analyses what the other agents are doing and report if some agent
   diverges" — check: watchdog running per phase; every DIVERGENCE message acted on and noted in Review. [ ]

## Plan (Opus agents, git worktrees, merged by me)

Phase 1 (parallel)
- [x] W1 rels-core (merged; 216 PASS under -race, red: evidence/red-w1.txt): `ErrTxOrder` (wraps `ErrInvalidTxFrom`), `checkTxOrder` under the entity lock, `at` seam on
      rel delete, caller instant on rel update (all update doors via the temporal path), close-collision refusal
      only for a caller instant, no-op update keeps t (refuse), `rels.Ops` + fakes, `Rels().DeleteWithTx/UpdateWithTx`.
      Red tests R0(rels) R1-R8 R10 R11(rels), all 4 backends.
      W1 ledger (worktree agent-abb0effa193edc7bc):
      - in code? no: rel delete stamps `deleteInstantForRelationship` (relationship_delete.go:99), update stamps
        `relVersionUpdateInstant` (relationship_update.go:151); no caller instant on either.
      - R0 `TestTxBackfillRel_PlainDoorsUnchanged` (core, 4 backends) run BEFORE the refactor, kept green:
        breaks if a refactor changes plain stamps (F+1 floor, clamp, close-collision move) on Delete/Update/GraphTx.
      - R1-R11 `TestTxBackfillRel_*` (core, 4 backends) red against doors stubbed to the plain path; facade
        `TestRelsWithTx_*` (pkg/graph) + rels spy. Proof: tasks/evidence/tx-backfill-delete-update/red-w1.txt,
        green-w1.txt.
      - R8 decision: a caller-instant delete refuses a recorded close at or after t (ErrTxOrder), so pin t-1 keeps
        the believed ValidTo (one tombstone row cannot both clamp and keep it; backlog known limitation).
- [x] W2 asof-cache (merged c688821; 55 subtests green on main, red: evidence/red-w2*.txt): bump AFTER the store write (move out of `resolveBackfillTxFrom` into a post-write call at
      every backfill door); replica `applyNodeDeleteLocked`/`applyRelDeleteLocked` report min(TxTo, DeletedAt).
      Red tests R14 (AddWithTx today), R13 (crafted delete records via ApplyChange).

Phase 2 (parallel, after W1+W2 merged)
- [x] W3 nodes (merged 18f8924; r0-before-w3 72 green, red-w3 284 red, green-w3 658 under -race): node DeleteWithTx/UpdateWithTx standalone + GraphTx twins, cascade one instant + order check on
      every cascaded rel (R9), `nodes.Ops` + fakes, node R0-R10, R13/R14 for the node doors.
- [x] W4 rel GraphTx twins + batch + ingest (merged e44e95d; red-w4.txt 152 red, green-w4.txt; all 8 doors seamed, whole-unit pre-flight refusal): R12 (rollback, door equivalence), R16 (batch/ingest: seam or explicit refusal).

Phase 3
- [ ] W5 finish: R15 cross-backend oracle, R11 over every door, docs/api.md, stale comments (§6.8), lesson 59
      amendment, CHANGELOG `[Unreleased]` 4.44.0, `make test-race`, `make cover`.
- [ ] Review agent per AGENTS.md MR protocol on the merged diff; fixes applied.
- [ ] Dependents build/vet against the merged tree (request 3).

Commits: no agent attribution lines (user rule 2026-10-02). No push, no tag.

## Review

(after phase 3)

---

# todo — item F: tiered composite + relationship temporal indexes (backlog 10, 2026-10-09)

## User requests

1. backlog 10 (ai-soc request 3, decided yes): tiered builds composite and rel temporal indexes — check:
   red tests in `tasks/evidence/tiered-indexes/red-*.txt`, green in `green-*.txt`; `go test ./pkg/... -count=1`. [ ]
2. "measure before building": resident bytes per indexed row + week projection — check:
   `tasks/evidence/tiered-indexes/measure-*.txt`, numbers in CHANGELOG. [x] 156 B/row rel temporal, 310 B/row
   composite (structure), rebuild 0.02 M rels/s (graph); a week (258-412 M rows) = 40-64 GB: does not fit.
3. docs: CHANGELOG Unreleased subsection, architecture.md tiered sections, AGENTS.md TieredStore — check: diff. [ ]

## Ledger (per step: in code? / red test / break cases / proof)

- S1 rel temporal on tiered. In code? no: `store/tiered/temporal_index_listing.go:24-31` lists none, no
  `CreateRelTemporalIndex`. Red: `TestTieredRelTemporalIndex_*` (tiered), `TestTieredRelTemporalPrune_ParityWithBadger*`
  (core). Break cases: token 0, duplicate, drop unknown/twice, closed store, prune with no filter / no index.
- S2 cross-shard rel rows not in the per-shard envelope. In code? no: `store/badger/badgerstore_partial.go:115`
  (PutRelEntityAndOut maintains only the property index), `:250` (DeleteRelEntityAndOut no purge). Red: the E->R
  rel in the parity scenario is kept by tiered, dropped by badger.
- S3 composite on tiered. In code? no: `store/tiered/index_introspection.go:8-17`. Red: `TestTieredComposite_*`.
  Break cases: token 0, 1 / 5 keys, duplicate key, tkg_ key, duplicate create, drop twice, closed store.
- Decision: per-shard fan-out, ref shard is the anchor (definitions persist per badger shard as today); prune
  consults the shards in the query's depth, skips ref/archive envelopes once an archive exists (archive moves
  split a rel's rows between ref and archive). Bound (hot+warm only) proposed, not built.
