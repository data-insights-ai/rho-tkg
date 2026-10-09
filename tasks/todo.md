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
- [ ] W1 rels-core: `ErrTxOrder` (wraps `ErrInvalidTxFrom`), `checkTxOrder` under the entity lock, `at` seam on
      rel delete, caller instant on rel update (all update doors via the temporal path), close-collision refusal
      only for a caller instant, no-op update keeps t (refuse), `rels.Ops` + fakes, `Rels().DeleteWithTx/UpdateWithTx`.
      Red tests R0(rels) R1-R8 R10 R11(rels), all 4 backends.
- [x] W2 asof-cache (merged c688821; 55 subtests green on main, red: evidence/red-w2*.txt): bump AFTER the store write (move out of `resolveBackfillTxFrom` into a post-write call at
      every backfill door); replica `applyNodeDeleteLocked`/`applyRelDeleteLocked` report min(TxTo, DeletedAt).
      Red tests R14 (AddWithTx today), R13 (crafted delete records via ApplyChange).

Phase 2 (parallel, after W1+W2 merged)
- [ ] W3 nodes: node DeleteWithTx/UpdateWithTx standalone + GraphTx twins, cascade one instant + order check on
      every cascaded rel (R9), `nodes.Ops` + fakes, node R0-R10, R13/R14 for the node doors.
- [ ] W4 rel GraphTx twins + batch + ingest: R12 (rollback, door equivalence), R16 (batch/ingest: seam or explicit refusal).

Phase 3
- [ ] W5 finish: R15 cross-backend oracle, R11 over every door, docs/api.md, stale comments (§6.8), lesson 59
      amendment, CHANGELOG `[Unreleased]` 4.44.0, `make test-race`, `make cover`.
- [ ] Review agent per AGENTS.md MR protocol on the merged diff; fixes applied.
- [ ] Dependents build/vet against the merged tree (request 3).

Commits: no agent attribution lines (user rule 2026-10-02). No push, no tag.

## Review

(after phase 3)
