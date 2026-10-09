# todo — v4 main: fixes, release, decided features (2026-10-09, "decisions are fine, lets go in the v4 main")

## User requests

1. "lets go in the v4 main" — the open v4 work from `tasks/review-v5-plan-vs-code-20261009.md` §6/§9 and the
   backlog items 10, 11 — check: each step below ticked with proof; v4.44.0 tagged (fixes + `*WithTx`), v4.45.0
   for the two features when their gates are green. [x] v4.44.0 f7cd0ba, v4.44.1 91d7c8c (history prefix, sigma),
   v4.45.0 b30f66f (item 11 shipped in 4.44.0 as Config.DurableCommit; tiered indexes in 4.45.0)
2. Standing (2026-10-09 earlier): tests first, break-the-code only, red output kept under `tasks/evidence/`;
   parallel Opus agents in worktrees; a Sonnet watchdog reports divergence; no agent attribution in commits. [x] (kept)
3. Queue promised to sigma-tkgd / ai-soc (messages 2026-10-09): wave 3 below → v4.46.0. [ ]

## Plan (worktree agents, merged by me; CHANGELOG lines under `[Unreleased]` in one subsection per item)

Wave 1 (parallel) → v4.44.0
- [x] A W5-finish (Opus; merged 60965fd after Opus review (MERGE): R15 cross-backend oracle `TestTxBackfillOracle_CrossBackend` (48×48 seeds, red by two stamp mutations), `TestTxBackfill_RaceClockEveryDoor`, node facade test, docs/api.md + SPEC + architecture + errors, lesson 59 amended, CHANGELOG folded into main's Added/Fixed/Changed; evidence/tx-backfill-delete-update/red-w5-*.txt, green-w5.txt; four pre-existing HIGH bugs → backlog 14 (second trigger), 18, 19): R15 cross-backend oracle, R11 over every door, docs/api.md, lesson 59 amendment,
      CHANGELOG 4.44.0 section for the `*WithTx` doors. In code? doors yes (W1-W4 merged), oracle no.
      Red: `TestTxBackfillOracle_*` fails against a door stubbed to the plain stamp. Proof: evidence/w5/.
- [x] B eclipse-skip (Opus; merged cea6ab2 after Opus review: code confirmed complete, 5 doc fixes + white-box tiling probes applied; red 45/53 + oracle 67 + mutant 9, green 62, race green, evidence/eclipse/; backlog 14 filed (cascade copies tombstone stamps, HIGH)): remove `eclipsedNodeBounds`/`eclipsedRelBounds` skips (temporal_cascade.go:63-77;
      chain_resolver.go:138,163,217,236; temporal.go:258,279,495,549,564,648; cascade template/piece sites).
      In code? skip yes, writer no since 994df82. Red: `TestOneTickSpanVisible_*` — RelAt(t), RelsDuring,
      ByType{ValidAt}, RelsRelating, NodeAt, CloseVersion(vf+1), Delete landing at vf+1, width-1 cascade piece;
      4 backends; break cases: width 1 vs 2, boundary t and t+1, old pin after a width-1 correction.
      Proof: evidence/eclipse/red-*.txt, green-*.txt; backlog KNOWN LIMITATION closed; stale header
      temporal_cascade.go:18-27 rewritten.
- [x] C scan-temporal-opts (Opus; merged after Opus review: 5 findings fixed; all four doors answer temporal opts exactly via the ByLabel/ByType fold, ordered folds keep exclusive-bound values, spy over every Core read door; red 130 assertions + core spy, mutants after/limit/stop/watermarks, 34 pkgs green, race green except one load flake TestAdvanceClock_RejectsImplausibleFarFutureTarget (passes alone); evidence/scan-opts/; backlog 16 filed (range vs predicate-anywhere interval semantics)): `ScanNodeColumns`/`ScanRelColumns` and `ForEachByLabelPropertyRange` filter
      current rows and ignore `TxAt`/`TxPin` (memorystore_query.go:105-125, badger_column_scan.go:149-159,
      badgerstore_node_range_scan.go:88, temporal_filter.go:70-72). Red: two-door parity vs `ByLabel(opts)` after
      an update and a delete, per backend; break cases: ValidAt before the update, TxAt before the delete, TxPin.
      Fix default: history-aware where a sound path exists, else fail closed (`ErrCapabilityNotSupported`) —
      never current-only rows for a temporal opt. Proof: evidence/scan-opts/.
- [x] D ingest-doors + comments (Sonnet; merged 675c7b3 after review: 4 findings fixed, evidence/ingest-doors/, 100 Session subtests under -race; backlog 12 (unique bypass via cascade props, HIGH) and 13 (applier attribution by numeric id) filed): `Session.SetRelVersionInterval`/`SetNodeVersionInterval` (ingest.go,
      beside :830-880), red forwarding + two-phase test through the session; stale comments
      store/changefeed.go:154-158, index/api.go:267-269 (+ core/graph_rel_indexes.go:97-100),
      constraints/unique.go:74-75. Proof: evidence/ingest-doors/.
- [x] Merge A-D (+E), `make ci-docker` exit 0 on 3691294 (lint: 12 findings from the branches fixed in e5a2121;
      vulncheck: 5 stdlib net/http advisories → go 1.26.9; cover-gate 86.6 %), consumers build+vet (scratch
      `replace`, sigma-tkgd / ai-soc engine / agent-bookkeeping), release v4.44.0 = f7cd0ba, tag pushed.

Wave 2 (parallel, start now, merge after wave 1) → v4.45.0
- [x] E durable-on-return commit (Opus, backlog 11; merged after Opus review: Close race (SIGSEGV) fixed with a hook test, power-loss limit on memtable switch documented + backlog 15, LSN/result kept on ErrCommitNotDurable, requeue path exercised, 6 mutants with diffs, crash children on badger/tiered/sharded incl. default flush interval; evidence/durable-commit/; latency 2 ms/fsync per commit): opt-in `Config.DurableCommit`; `GraphTx.Commit` and
      `Batch.Execute` flush the pending buffer before returning, one WriteBatch per call, every touched shard on
      tiered/sharded. Red: close-without-flush / SIGKILL after Commit returns shows the whole group; default off
      unchanged; oversized group documented. Proof: evidence/durable-commit/.
- [x] F tiered composite + rel temporal indexes (Opus, backlog 10; v4.45.0 = b30f66f after two Opus review rounds: delete-keeps-envelope hole + PutRelVersion covered-extend, probe double build, fan-out rollback seam, cold sync TryLock, retention purge; evidence/tiered-indexes/; hot+warm bound ≈156 B/rel, 6–9 GB per indexed type at ColdAfter 1 d; backlog 22 filed): measure one-entry-per-row budget on a week of
      raw edges first, then per-shard fan-out mirroring sharded; rotation, cold checkout and repair move/rebuild
      entries. Red: two-phase over rotation, parity with badger. Proof: evidence/tiered-indexes/.
- [x] Origin watch (René 2026-10-09: "Monitor github, Markus is working on the v5"): Monitor task running
      `scratchpad/watch-origin.sh` (polls origin every 60 s; emits pushes by anyone but git user "dev team",
      new/deleted branches and tags, PR and issue changes); re-armed every 30 min while the session lives.
      Baseline: origin/main c554251, origin/v5 b1193dc (docs only), PRs #1-#7 closed/merged.
- [x] Watchdog (Sonnet): reads every worktree's log/diff every few minutes; reports DIVERGENCE (scope creep,
      happy-path-only tests, attribution lines, pushes) to me.

## Wave 3 (HIGH correctness bugs, same root; René default = take the listed default, state it) → v4.46.0
- [x] G cascade correctness (Opus; merged dae5c05 after two Opus review rounds: old-data shapes pin-stable, as-of cost fixed, migration note; evidence/cascade-correctness/; semantic change recorded as documented exception; backlog 14 + 18 + 19 + handover-effective-read-cost §2 / fix 2a): one version
      allocator for every appended row, appended rows never carry TxTo/DeletedAt (14 second trigger, unconditional),
      a cascade on a deleted entity is REFUSED with a sentinel (default; lesson 46), GraphTx rollback keeps the higher-
      version cascade row (full-copy snapshot when any history row version > current, `check*CallerUpdate` before the
      snapshot), Delete after a bounded cascade ends the entity in declared reads. Remove the W5 oracle skips
      (`txbTangled`, `txbNoTxRollback`) with the fixes. Red first: acceptance tests 1, 2, 4 of the handover + the
      backlog 18/19/14 tests; evidence/cascade-correctness/.
- [x] H HasHistory presence (Opus; merged 2310ec3 after review: per-key overlay fix, Clear reset, measured 30 B/ID; handover §1 fix 1b): `Rels().HasHistory(id)` / `Nodes().HasHistory(id)`,
      badger RAM set maintained at `noteHistoryKey`, built lazily by a key-only scan; memory/tiered/sharded; acceptance
      test 3; BenchmarkRelHasHistory; evidence/has-history/.
- [x] I unique bypass (Opus; merged cae3b5c after two review rounds: check on the built rows, claims withdrawn on failed write, backlog 12 closed; backlog 29 = same fault in the update door; backlog 12): enforce CreateUnique on `SetNodeVersionInterval` props patches on all four
      doors (Temporal, GraphTx, Batch, Session), UniqueCurrent and UniqueForever; evidence/unique-cascade/.
- [x] Bulk as-of presence (Sonnet→Opus rounds; merged d7049e5 after Opus review: top version per ID, forward-walk build 2.5–4× faster than main, N13 ordering hook, gap guard; evidence/bulk-asof-presence/)
- [ ] J effective timeline + scan forms (Opus; candidate 7261cf9 handed to sigma; review a562a3ef… running): merge, then v4.46.0 gate and tag (or fold J into it).
- [x] K point as-of doors race (backlog 32; merged 68f99ad after two review rounds: badger with-history doors publish history first; native as-of guard, mutants A-F deterministic, cost within noise; evidence/point-door-race/; backlog 37 filed)
- [x] Mint instant (backlog 36; merged 8afb1f0: NodeID/RelID.MintInstant, shared idlayout, shadow fallbacks)
- [ ] Next: handover 1 (backlog 8), LatestStamps (30), badger scan cost (33), update-door claims (29), state column doors (28), handover 2 (backlog 21).

## Review

(after wave 1 merge)

---

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
   before the code commit. [x] (red-w1..w5)
3. "analyse if these changes break any existing code.. rho-tkg is in a lot of other libraries" — check: §6.10
   [x]; after implementation R0 green and `go build ./... && go vet ./...` green in sigma-tkgd, ai-soc
   engine, agent-bookkeeping against the merged tree (`replace` in a scratch copy, never committed there). [x] (v4.44.0)
4. "then lets go.. spawn parallel opus agents in worktrees" — check: branches merged into main, worktrees
   removed, `make test-race` + `make cover` (new code >= 80 %) green on main. [x] (ci-docker on 3691294, 86.6 %)
5. "spawn a parallel sonnet agent which analyses what the other agents are doing and report if some agent
   diverges" — check: watchdog running per phase; every DIVERGENCE message acted on and noted in Review. [x] (served
   by one reviewer agent per branch under the MR protocol; every FIX FIRST finding applied before merge)

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
- [x] W5 finish (branch worktree-agent-abdf7ef774350985f, d9f11aa..; red: evidence/red-w5-{oracle,race,facade}.txt,
      green: evidence/green-w5.txt — full `go test ./pkg/...` and `-race` on core/storeutil/graph green, changed
      lines 4126bb1..HEAD 90.0 % covered (-short); `make cover` / `make test-race` targets not run as such):
      R15 cross-backend oracle, R11 over every door, docs/api.md, stale comments (§6.8), lesson 59
      amendment, CHANGELOG `[Unreleased]` 4.44.0, `make test-race`, `make cover`.
      W5 ledger (worktree agent-abdf7ef774350985f, written before the first test edit):
      - R15 in code? doors seamed (tx_order.go checkTxOrder; relationship_delete.go/node_delete.go `at` seam;
        updateTemporal.txAt); the generative oracle (bitemporaloracle_test.go) has no caller-instant op and no
        tiered arm. Red test `TestTxBackfillOracle_CrossBackend` (memory, badger, sharded, tiered): random plain
        Add/Update/Delete/CloseVersion/SetVersionInterval interleaved with node/rel DeleteWithTx/UpdateWithTx over
        standalone, GraphTx, Batch, ingest strong + concurrent; per op the stamps carry t and pin t answers as the
        far-future pin right after it; per seed every point/during/TxAt/as-of/TxPin/ByLabel/ByType door equals the
        oracle and the four backends give identical answers. Break-the-code: a door stubbed to the plain stamp
        (rel delete seam, node update seam), t off by one, a backend diverging. Proof:
        evidence/red-w5-oracle.txt (seam reverted), green-w5.txt.
      - R11 every door in code? only the standalone doors race (TestTxBackfillRel/Node_RaceClock). Red test
        `TestTxBackfill_RaceClockEveryDoor` (nodes and rels × GraphTx, Batch, ingest strong, ingest concurrent,
        -race). Break: the seam's order check removed (the concurrent pre-flight runs under the shared lock only).
        Proof: evidence/red-w5-race.txt.
      - Ordering on every door with errors.Is: core R3 already runs all 10 rel and 10 node doors; facade gap: the
        node GraphTx/Batch/Session doors have no pkg/graph test. Red test `TestNodesWithTx_TxBatchIngestFacade`
        (t = TxFrom, TxFrom-1, below a history TxTo; errors.Is graph.ErrTxOrder and ErrInvalidTxFrom). Break: a
        door forwarded to its plain twin.
- [ ] Review agent per AGENTS.md MR protocol on the merged diff; fixes applied.
- [x] Dependents build/vet against the merged tree (request 3; v4.44.0).

Commits: no agent attribution lines (user rule 2026-10-02). No push, no tag.

## Review

(after phase 3)

W5 findings (pre-existing, plain doors only, gate off, reproduced at 4126bb1 = v4.43.0; not fixed here, the
oracle routes around them and says so in `txbTangled` / `txbNoTxRollback`):
1. A bounded `SetVersionInterval` cascade appends a row whose version is above the current row's. NodeAsOf /
   RelAsOf at a pin after the cascade answer the current row until any later write supersedes or deletes it;
   then the history arm answers the cascade row for those same pins (a plain Update changes the past answer
   v1 -> v2, all four backends).
2. The next version-advancing write after such a cascade (Update, CloseVersion) reuses the cascade row's version
   number: two rows with one version in the chain (all four backends). After it, the TxAt point door's answer at
   a pin before a later Delete changes (v1 -> absent).
3. `SetNodeVersionInterval` on an updated and closed node writes cascade rows whose TxTo lies below their TxFrom
   (inherited from the archived row); the remainder row is current with a TxTo; a later Update keeps that TxTo
   on its new version (inverted TX interval on the current row).
4. A GraphTx rollback after `tx.UpdateRelationship` / `tx.DeleteRelationship` (or a refused
   `UpdateRelationshipWithTx`, whose refusal comes after the snapshot) drops the cascade history row above the
   current one on memory and badger (history 1 -> 0); sharded and tiered keep it.
Repros: scratch tests in the session scratchpad (`zz_scratch_*`); each is one short test, worth a backlog item
with a red test first.
