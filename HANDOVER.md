# HANDOVER — rho-tkg v4 line, written 2026-10-10 (session end, all tasks stopped)

Repo `/home/renework2023/Work/2026/datainsights/rho-tkg` (module `github.com/data-insights-ai/rho-tkg/v4`, Go 1.26.9),
branch `main` = `origin/main` = `f5d112f` + this file. Latest tag `v4.49.0` (`aedc56d`). Working tree clean, **no agents
running, no monitors running, no open worktrees of ours**. Read this file, then `tasks/lessons.md` (esp. 77, 78),
`tasks/todo.md`, `tasks/backlog.md`, and `CHANGELOG.md` top before the first action.

## 1. User's last requests (verbatim order, newest first)

0. "for the next step: rho-tkg lacks is a way to say 'this was never true'.. also add it to the handover.md.. request from
   ai-soc" — **NEXT STEP (René, 2026-10-10, after the handover was written; this OVERRIDES the earlier relayed decision
   'no Retract door needed' — newest user message wins).** Spec = `tasks/backlog.md` item 43. Added to §11 as item 1.
1. "stop all tasks and write a comprehensive handover.md" — DONE (this file). Both running agents and the origin
   watch were stopped; nothing was lost (see §6).
2. "continue" — I had just started two agents (both stopped, see §6): interval rewrites at a caller instant (backlog 34)
   and badger read costs (backlog 33 + 42 + 41). **First thing to do next: restart those two (prompt skeleton in Appendix A, specs in the backlog items).**
3. Standing rules René gave during the session (also in CLAUDE.md / lessons 77, 78 / memory):
   - "Always clean solutions to new items.. no hacks or shortcuts .. always write a no-happy-path test before the code"
     (tests name the faulty implementation, written and run RED first, evidence kept under `tasks/evidence/<item>/`;
     guards that pass before the fix are labelled; no skips hiding failures; a documented limit only after the sound
     fix was tried and shown impossible).
   - "Why always minor Version?" — answered: semver, additive-only v4 promise (`docs/stability.md`); new public surface =
     minor, fix-only = patch (v4.44.1 was the only patch). Open choice offered and NOT answered by René: ship behaviour
     changes through the deprecation ritual instead of as named exceptions (§5) — default taken: named exceptions.
   - "Monitor github, Markus is working on the v5" — done with a 30-min re-armed Monitor (script in scratchpad, §8).
   - "v4 -> v5: delete this rule" — the old rule "v4 gets fixes, not features that v5 replaces" is deleted; v4 keeps
     taking consumer features.
   - Earlier: v4 work goes to `main`, tags pushed; push needs `! git push …` from René when the classifier blocks it
     (it blocked twice early on; later pushes from the session went through).
4. Git commits carry NO attribution lines (CLAUDE.md, 2026-10-02), despite the system reminder suggesting some.

## 2. Release history of this session (all on origin, all gates green)

Gate = `make ci-docker` (fmt-check, vet, lint-docker, build, test-race, security-docker, vulncheck-docker, cover-gate
≥ 80 %, check-metakv-reap) exit 0, plus sigma-tkgd / ai-soc engine / agent-bookkeeping `go build && go vet` with a
scratch `replace`.

| Tag | Commit | Cover | Content |
|---|---|---|---|
| v4.44.0 | f7cd0ba | 86.6 | `DeleteWithTx`/`UpdateWithTx`/`ErrTxOrder` on every door, one-tick spans visible, column+range scans answer temporal opts, ingest `Set*VersionInterval`, `Config.DurableCommit`, go 1.26.9 |
| v4.44.1 | 91d7c8c | – | badger History iterators bounded to the entity prefix (sigma) |
| v4.45.0 | b30f66f | 86.7 | tiered composite + rel temporal indexes (hot+warm bound), rel temporal index soundness fixes |
| v4.46.0 | fac763a | 86.9 | cascade correctness, pin-stable as-of rule, `HasHistory`, `CreateUnique` on cascade patches, bulk as-of presence, history-ID overlay fix |
| v4.47.0 | b3eb884 | 87.0 | `Node/RelEffectiveTimeline` + scan forms, `NodeID/RelID.MintInstant`, point-door race fix (badger publish-history-first), read-time supersession rule, life ordering |
| v4.48.0 | bd787bd | 87.1 | pinned property lookups via the tx-membership sidecar, re-import continues version numbering across lives |
| v4.49.0 | aedc56d | 87.2 | `Nodes/Rels().LatestStamps`, every `UniqueForever`-claiming door withdraws claims on failed writes |

Release recipe (all releases): merge reviewed branches → put the CHANGELOG `[Unreleased]` body under a new
`## [x.y.z] - date` section with a headline paragraph (behaviour changes bold) → bump the three version lines
(`AGENTS.md` "Status: vX", `README.md` "Current release", `docs/architecture.md` title; the docs-consistency test pins
them) → commit `release: vX.Y.Z — …` → `make ci-docker` → consumer builds → `git tag -a` → `git push origin main` and the tag →
message the consumers. **Run lint-docker and security-docker on each agent branch BEFORE merging** (three gate
re-runs this session were caused by findings that came in through merged branches).

## 3. Process that worked (reuse it)

- One Opus agent per item in `isolation: worktree`, prompt carries: spec files, lessons 77/78, TDD rules, mutants, docs,
  "run lint-docker + security-docker on your branch before reporting", "no attribution lines", "no pkill -f", "do not
  touch files owned by the parallel agent". Then ONE Opus reviewer per branch (read-only, scratch exports in a fresh
  scratchpad directory, re-runs mutants, writes its own brute-force oracle where the semantics are subtle); the review
  finds real defects almost every time (examples: clock-floor hazard in the re-import, unique claim leak, incomplete
  close rule). Fix round via SendMessage to the SAME agent, then a second reviewer pass on the delta, then merge.
- Merge conflicts are nearly always `CHANGELOG.md` and `tasks/backlog.md` (keep both sides; entries go under
  `## [Unreleased]` with their own `### Added/Fixed/Changed`, never into a released section) and `bench.yml BENCH_FILTER`.
- Worktrees: `git worktree unlock` then `git worktree remove --force` before `git branch -d`.
- Evidence per item under `tasks/evidence/<item>/` (red runs, green runs, mutants, index file). Ledger in `tasks/todo.md`.

## 4. Consumers and who to talk to

- **sigma-tkgd** session `sigma-tkgd-c3` (socket `uds:/run/user/1000/cc-socks/3891112.sock`) — **that session is STOPPED; the next sigma session will announce a NEW name to us (the old name goes away); until then messages to the old address will not be answered**. Pins the latest tag. Uses:
  timeline doors, scan forms, `HasHistory`, `LatestStamps`, `MintInstant`. Moved its pinned reads to the STATE doors.
  Open from sigma: confirm the signatures of the broader effective scans (backlog 35), interval rewrites at a caller
  instant (34), state column fast path (28, sigma measured 1M-node pinned count: badger 13–14 s vs 1.4 s unpinned),
  retention for the overview (21, not blocking until `@overview` starts). Its `/admin/import` can see `ErrTxOrder` on a
  backfilled re-import of a deleted ID (notified).
- **ai-soc** session `ai-soc-main-e7` (socket `uds:/run/user/1000/cc-socks/3892289.sock`). Embeds rho + sigma in one process,
  badger/tiered, estates in one graph, bursts grown by `SetRelVersionInterval`. Pins latest; engine at v4.41 → moving up.
  Contract it pinned: timeline segments never merge distinct rows with equal content. Decisions from René via ai-soc:
  rel unique key WITHDRAWN (snowflake IDs are the global IDs), tiered indexes YES (done), durable commit YES (done),
  provider-abort NO, group instant NO.
- **agent-bookkeeping** (v4.40): builds clean, no special needs.
- **Markus Nissl** builds **v5** on branch `origin/v5` (own Go module `…/rho-tkg/v5` in `v5/`; Raft log, two-group
  transactions, graph store, snapshots, FDB/TigerGraph comparator tools; ~86 commits since 2026-10-09; history was
  force-rewritten once, our plan commit `b1193dc` is still an ancestor). `tasks/v4-changes-for-v5-importer-20261010.md`
  (on main) tells him what the v4 changes mean for the importer. Do not push to `v5`.

## 5. Behaviour changes shipped (and their justification)

- v4.46.0: record doors (`NodeAsOf`/`RelAsOf`/`NodesAsOf`/`RelsAsOf`, `TxPin` scans) answer "the newest row recorded by
  the pin" (pin-stable), no longer "the current row"; the state doors are `NodeAtTx`/`RelAtTx`,
  `ByLabel`/`ByType` with `ValidAt+TxAt`, and the timeline. Documented exception in `docs/stability.md`, justified by
  correctness bug (answers at earlier pins changed after later writes) and consumer notification.
- v4.48.0: backfilled re-import at/below the chain's stamps → `ErrTxOrder` (second documented exception). Re-import
  now continues versions across lives. Chains written by v4.43–v4.47 that overwrote history stay lossy (no migration).
- v4.47.0: read-time supersession rule + life ordering change answers for a close followed by a bounded cascade and for
  re-imported IDs on tiered/sharded (migration blocks in the CHANGELOG `### Fixed`).
- René has not answered the "deprecation ritual instead" alternative; default stands.

## 6. State of in-flight work at stop time

| Task | State | Action |
|---|---|---|
| P: `Set*VersionIntervalWithTx` (backlog 34) | agent stopped before reporting; no worktree left, nothing committed | restart (prompt skeleton in Appendix A) |
| Q: badger read costs (backlog 33 + 42 + 41) | agent stopped after capturing one baseline; worktree removed; baseline saved as `tasks/evidence/badger-read-cost/01-baseline-latest-stamps-rotating.txt` (LatestStamps rotating, rels=5000: memory ≈ 41 ns, badger ≈ 74 ns, 0 allocs) | restart (prompt skeleton in Appendix A); that file is the only artefact |
| origin watch | stopped | re-arm if still wanted (§8) |
| other sessions' processes (`go test … TestBOWholeEndToEnd`, `go test -race ./aisoc/...`) | belong to ai-soc/sigma, not ours | leave alone |

Not committed anywhere else: nothing. `git status` clean (except this file and the evidence file when you read this).

## 7. Next steps, in order (each: red tests first, reviewer per branch, lint+security on the branch, then merge)

1. **Backlog 34** `SetNodeVersionIntervalWithTx`/`SetRelVersionIntervalWithTx` on Temporal, GraphTx, BatchBuilder, ingest
   Session: mirror `DeleteWithTx/UpdateWithTx` (`AllowTxBackfill` gate, `ErrInvalidTxFrom`, `ErrTxOrder` against the
   whole chain incl. re-imported lives, ONE caller instant `t` for every appended row, commit clock not advanced,
   `notePastDatedWrite`, whole-unit pre-flight in batch/ingest, replica reproduces stamps, plain doors unchanged, W5
   oracle `tx_backfill_oracle_test.go` gets the op). Prompt basis: Appendix A with backlog 34 + handover
   `tasks/handover-tx-backfill-delete-update-20261009.md` as the spec.
2. **Backlog 33 + 42, then 41** (badger read cost): current-row stamps capability reading only the temporal tail on an
   entity-cache miss (LatestStamps cache miss today 6.5–7.1 µs, 25 allocs; sharded/tiered 8–12 allocs); effective-timeline
   scan gather reading each rel once (13–17 µs/rel on badger vs 1.3 µs memory); then merge the stamps sidecar into the
   presence map (saves ~30 B/ID, 66→~36); step 3 only if the risk pays. Register benchmarks as allocs-gated rows.
3. **Backlog 35** broader effective scans — wait for sigma's confirmation of: `ForEachNodeEffective(pin, fn)`,
   `ForEachRelEffective(pin, fn)`, `ForEachRelEffectiveAtNodes(nodeIDs, dir, relTypes, pin, fn)`,
   `NodesEffectiveByIDs`/`RelsEffectiveByIDs`, optional `ForEachNodeEffectiveByLabels`.
4. **Backlog 28** state column fast path (design first; numbers in the item), **21** retention (handover
   `tasks/handover-overview-retention-20261009.md`), then the residual list below.
5. Keep consumers informed after each tag (messages with the migration points); keep v4.50.0 as the next minor.

### Open backlog items (verify each line's status before acting; headers drift)

HIGH/MEDIUM: 13 (ingest applier attributes group errors by numeric id), 15 (durable commit power-loss gap on memtable
switch → `SyncWrites` advice), 16 (range folds under an interval: predicate-anywhere vs resolved version), 22 (badger rel
temporal index create races concurrent writes, not reproduced), 25 (future close then delete changes an earlier pin's
answer), 33/41/42/34/35/28/21 above. LOW/cosmetic: 23 (`changeFeedPage` prefetch), 26 (old collided chains), 31 (as-of
doors vs version gaps), 37 (tiered rollback restore window), 39 (stubs survive `Admin().Reset`), 40 (resumption bound of an
earlier life). Older: 0 (column segments S4/S6/S7), 1–5, 7. Also: RAM budget for property sidecars; `NodesByLabelAt`
still folds all history instead of using the K1 sidecar.

**Known doc drift to fix**: `tasks/backlog.md` header line "Remaining open work … HIGH: items 12, 14, 18, 19" is stale
(all closed); item 10's text still says "BUILT on branch …" (merged in v4.45.0); item 11 and 6 are done (v4.44.0); item 20
is partly done (1a v4.44.1, 1b/1c, 2a done) — check what remains.

## 8. Environment facts and traps

- Gates: `make ci-docker` ≈ 12–15 min on the shared 32-core host (load 10–25 from other agents); the bench gate
  comparator is noisy on identical code on a loaded host — the PinnedRel family is **allocs-gated** (time canary is
  opt-in via `TIME_CANARY`), LatestStamps benchmarks too.
- Never `pkill -f` / `pgrep -f` (other sessions' tests share the machine; one agent did it once). Kill by PID you started.
- `git archive` / `git worktree add` commands containing "github.com" were rejected by a sandbox hook for some agents;
  the Write tool works. The permission classifier blocked `git push` from the session twice early on; René ran
  `! git push …` himself.
- Scratch/consumer replace modfiles: `/tmp/claude-1000/-home-renework2023-Work-2026-datainsights-rho-tkg/d8867122-d92d-44f6-8bc1-21ec6c585e85/scratchpad/consumers/{sigma-tkgd,engine,agent-bookkeeping}/go.mod`
  (build with `cd <consumer> && GOFLAGS=-mod=mod go build -modfile=<that go.mod> ./...`). The origin watch script
  `…/scratchpad/watch-origin.sh` (polls `git ls-remote` + `gh pr/issue list` every 60 s, reports pushes by anyone but
  git user "dev team", new/deleted refs/tags, PR/issue changes). These live in /tmp and may be gone; both are trivial to
  recreate. The ai-soc/sigma/agent-bookkeeping module paths for the replace are in §4's repos
  (`~/Work/2026/datainsights/{sigma-tkgd,ai-soc/ai-soc-main/engine}`, `~/Work/2026/bds421/sigma/agent-bookkeeping`).
- The session's memory notes (project memory dir `~/.claude/projects/-home-renework2023-Work-2026-datainsights-rho-tkg/memory/`):
  `v5-work-markus-monitor-origin.md`, `feedback-clean-solutions-red-test-first.md`.
- Tag `v5-plan-20261009` marks the plan commit on `v5` (docs only, not a Go version).
- Test names to know: `TestTxBackfillOracle_CrossBackend` (`TXB_ORACLE_SEEDS`), `TestEffectiveTimeline_PointwiseOracle`
  (`ET_ORACLE_SEEDS`), `TestReImportLifeOracle`, `TestHasHistoryDifferential`, `TestLatestStampsDifferential`,
  `TestPointDoorRace_UnderMovingWriters`, `TestUniqueClaims_*`, `TestUniqueCascade_*`; bitemporal oracles in
  `pkg/graph/internal/core/bitemporaloracle*_test.go`.

## 9. Lessons added this session

`tasks/lessons.md` 78 (clean root-cause solution + red test first; detector greps). Amended: 59 (privileged override at
the shared seam; whole-unit pre-flight), 64 (publish history before the current row), 55 (superseded by the scoped log
for GraphTx), 35 (eclipse skip removed). Candidate lessons NOT yet written (add if they recur): "run lint+security on
agent branches before merge", "a read-time rule that changes answers on stored data needs an old-data fixture test
written with the old code", "an oracle that restates the rule is not independent evidence — write the brute-force
belief definition separately".

---

## 10. All worktrees and branches (state at 2026-10-10 ~10:40, after "also all worktrees … into handover.md")

`git worktree list`:

| Path | HEAD | Ours? | State / action |
|---|---|---|---|
| `/home/renework2023/Work/2026/datainsights/rho-tkg` | `main` (this commit) | yes | the only worktree of ours; clean |
| `/tmp/claude-1000/-home-renework2023-Work-2026-datainsights-sigma-tkgd/a2d498b9-aa4c-4865-a7c8-f9c94a767c33/scratchpad/v446/rho` | `6bef04a` detached | **no** (sigma-tkgd session's scratch checkout of the first timeline candidate) | leave alone; stale candidate, never merged |

Agent worktrees created this session were all merged (and removed) or removed unchanged; the last two were
`agent-a4dbe9cf09a163ad4` (backlog 34: removed by the harness when stopped, nothing committed) and `agent-a3f21bd141e76b243`
(backlog 33/42/41: only one untracked baseline file, saved into `tasks/evidence/badger-read-cost/` on main, worktree and
branch deleted).

Local branches:

| Branch | Commit | Status |
|---|---|---|
| `main` | = `origin/main` | release line; all tags v4.44.0–v4.49.0 reachable |
| `v5` (local) | `b1193dc` (behind `origin/v5` by 87) | our docs-only v5 plan commit; `origin/v5` is Markus's work now — **do not push or reset it**; `git fetch` then read only |
| `wip/badger-scan-flush-evict` | `42de1f3` | OLD (pre-session) stopped-mid-task branch from v4.36: "test + candidate fix, unverified"; topic was fixed on main since (CHANGELOG [Unreleased]/Fixed 2026-09-24 "badger reads dropped or replaced rows when a flush + eviction landed mid-read"); candidate for deletion after René confirms |

Remote branches: `origin/main`, `origin/v5` (Markus). Tags on origin: `v4.44.0 … v4.49.0`, `v5-plan-20261009`.
Merge-and-remove rule for future agent worktrees (CLAUDE.md): merge the branch back, `git worktree unlock` + `git worktree remove --force`,
`git branch -d` BEFORE calling a task done (git refuses `-d` while a merge is uncommitted — commit the merge first).

## 11. TODO (the live ledger; `tasks/todo.md` mirrors it but its "Next wave" line is stale)

Legend: [ ] open, (S) = waits for sigma, (R) = waits for René's decision.

0. [ ] **NEXT STEP — backlog 43 "Retraction: a way to say this was never true"** (`Retract`/`RetractWithTx` + GraphTx, Batch,
   Session twins). Requested by ai-soc (recovery ends the edges of unfinished commit groups, retracts wrong records),
   confirmed by René 2026-10-10. History of the thread, so nobody is confused: sigma first relayed René's decision
   'Delete = validity end, retraction stays an explicit ENDED fact in ai-soc, no door' (kept: Delete is unchanged); then René
   asked for the door as the next step. Semantics: tx-time "belief ends at T; at pins >= T absent for every valid time; at pins
   < T unchanged"; full door list, open design points with defaults (marker as a reserved stored property on the tombstone,
   life cap at −∞ in `lifeEnds`, additive/minor) and the red-test list are in backlog 43. Workflow: spec check against the
   code first (the `tkg_` reserved-key rules, `lifeEnds`, supersession rule, timeline sweep), then Opus agent with Appendix A,
   reviewer, fix round, lint+security, merge, gate, tag v4.50.0 with the other pending items, tell ai-soc (and the new
   sigma session) the exact signatures.
1. [ ] **Restart backlog 34** `Set{Node,Rel}VersionIntervalWithTx` (all doors) — sigma replay need.
2. [ ] **Restart backlog 33 + 42 (+ 41 last)** badger read cost — baseline saved in `tasks/evidence/badger-read-cost/`.
3. [ ] Backlog 35 broader effective scans — signatures CONFIRMED by sigma 2026-10-10 (all five, as recorded in the backlog item).
4. (moved up: see item 0 below — the retraction door is the NEXT STEP)
5. [ ] Backlog 28 state column fast path (design first; sigma numbers inside the item) (S).
6. [ ] Backlog 21 retention (PurgeExpiredRels, per-type gate) — handover `tasks/handover-overview-retention-20261009.md` (S, not blocking).
7. [ ] Residual backlog: 13, 15, 16, 22, 23, 25, 26, 31, 37, 39, 40 (see §7), RAM budget for property sidecars,
   `NodesByLabelAt` via K1, older items 0–5, 7.
8. [ ] Fix the doc drift listed in §7 (backlog header, item 10/11/6/20 text, stale `tasks/todo.md` "Next wave").
9. [ ] Delete the stale `wip/badger-scan-flush-evict` branch after René confirms (R).
10. [ ] (R) Answer: keep behaviour changes as named stability exceptions (default) or ship through the deprecation ritual.
11. [ ] Optional: write the v4→v5 importer reference tests Markus might want (note on main: `tasks/v4-changes-for-v5-importer-20261010.md`);
    re-arm the origin watch (§8) if René still wants to follow `v5`.
12. [ ] Next release: v4.50.0 (minor) when items 1–2 are merged; headline the behaviour changes, gate, tag, notify consumers.

Done and verified this session (do not redo): see §2 table plus CHANGELOG; the ledger rows in `tasks/todo.md` carry the evidence paths.

## 12. Decision log (the reasoning behind the non-obvious choices; "cot" read as: why, not a transcript)

- **Order of work**: HIGH correctness bugs found by reviews first (cascade/version collisions/rollback, unique bypass,
  re-import data loss), consumer-blocking features next (timeline doors, HasHistory, LatestStamps), perf items after.
  Reason: silent wrong answers on a bitemporal store are worse than slow reads, and sigma/ai-soc pin the latest tag.
- **As-of rule**: pin-stable "newest row recorded by the pin" over "current row while current": the old rule changed
  answers at EARLIER pins after later writes (lesson 62), and restoring the old meaning needs a persisted slot marker
  (wire change = v5). Cost: a documented exception and consumer migration (sigma moved to state doors).
- **Re-import**: refuse the ambiguous backfilled input (ErrTxOrder) instead of ordering by version: the stored rows cannot
  distinguish a demoted import from a first-life cascade row without a life marker; refusing keeps write order = TxFrom
  order = version order for every chain written from now.
- **Clock handling**: a re-import never advances the shared commit clock (found by review: a delete stamped via valid time
  50 years ahead would have pushed every writer's clock; lesson 71 territory).
- **Tiered**: rel temporal index only on hot+warm shards (156 B/rel measured; 40–64 GB/week if every shard were indexed at
  ai-soc rates); cold rows are never pruned, answers unchanged.
- **Pinned property sidecar**: K1-style superset (resolver stays the authority) instead of an exact version index (a
  second as-of rule would drift); lazy build; the PinnedRel bench family gated on allocs/op only because timing on shared
  hosts swung +42…+178 % on identical code while allocs were exactly stable.
- **LatestStamps**: sidecar over history rows only, current row read directly (a running max cannot be lowered by a plain
  ReplaceNode); tiered matches `History`'s dedup of a version present on two shards exactly (contract stays literal).
- **Segments never merge distinct rows with equal content**: a segment names the stored record that answers it.
- **Process**: one reviewer per branch always found something real; fix rounds go to the same agent; lint+security on the
  branch before merging (lesson, not yet written); never accept an oracle that restates the rule as evidence for the rule.
- **Retraction (reversal)**: first answered by analysis (no existing way; proposal Retract/RetractWithTx), then relayed as 'no door
  needed' (René via sigma/ai-soc), then René asked for it as the next step. The model fits: a transaction-time tombstone
  marked as a retraction, life capped at −∞; Delete stays validity-end. Not built; backlog 43 holds the spec.
- **Things I got wrong (so you do not repeat them)**: dropped sigma's backlog item 10 by overwriting a block (restored
  as 21); told ai-soc "adjacent identical rows merge" (wrong; corrected); said sigma could use a mint-instant helper
  before checking package layout; merged branches without running lint/security first (3 gate re-runs); deleted an old
  agent worktree that held three uncommitted files without checking first (the replacement agent re-created the oracle).


---

## Appendix A — agent prompt skeleton that produced the good results (fill the <>)

```
You work in your own git worktree of the Go repo rho-tkg (module github.com/data-insights-ai/rho-tkg/v4), branched from
main (<version>). Item <X> = tasks/backlog.md item <N> (<who asked, why>). Read FIRST: AGENTS.md (Session Protocol, Testing
Rules 1-17, <Concurrency/Persistence>), tasks/lessons.md <relevant numbers> and 78, the backlog item, the handover
<tasks/handover-….md>, and the code you build on: <files with the mechanism, named precisely>.
THE BUG/FEATURE: <measured symptom, file:line cause, repro>.
TASK: <clean root-cause solution at the shared seam; decisions to take with the default stated; compatibility: no wire/
format change unless allowed; old stored data keeps reading as before>.
TDD, break-the-code only, red first, evidence under tasks/evidence/<item>/: <the faulty implementations to name and the
inputs that break them>; two-phase tests (rule 15); both doors (rule 17); node/rel parity (rule 2); errors.Is (rule 4);
every new public method a direct test (rule 1); mutants (each red); guards labelled; no skips.
Docs: CHANGELOG `## [Unreleased]` -> `### Added/Fixed/Changed` with measured numbers (+ migration block for read-side
changes), docs/api.md, AGENTS.md only if a stable line changes; close the backlog item (Closed table + line; edit ONLY it).
Rules: go build/vet, gofmt -l pkg bench empty, go test ./pkg/... -count=1, go test -race on <packages>, then
make lint-docker AND make security-docker on your branch BEFORE reporting; coverage of new code >= 80 %. Small conventional
commits, NO attribution lines of any kind. Do not push, tag or merge; do not edit tasks/todo.md. Never use pkill/pgrep -f.
Another agent works in parallel on <area>: do not touch <files>.
Final report: branch, commits, red/green evidence with counts, decisions, files touched, measured numbers, open items.
```

Reviewer skeleton: "Read-only MR review … (do not edit/commit/merge/push; no pkill -f; scratch exports only under the
scratchpad, a FRESH directory per export). Follow AGENTS.md MR Review Protocol exactly. Verify the author's claims with
fresh experiments (old-data fixtures written with the OLD code, your own brute-force model, mutants re-run, perf paired
A/B), `git merge-tree $(git merge-base main HEAD) main HEAD | grep -c '^<<<<<<<'`, attribution lines
`git log main..HEAD --format=%B | grep -ci 'co-authored\|claude'`, lint/security evidence real. Report MERGE / FIX FIRST with
file:line findings."  Resume the same agent with SendMessage for fix rounds.

## Appendix B — consumer build check (recreate if /tmp is gone)

For each consumer `C` in `~/Work/2026/datainsights/sigma-tkgd`, `~/Work/2026/datainsights/ai-soc/ai-soc-main/engine`,
`~/Work/2026/bds421/sigma/agent-bookkeeping`: `mkdir -p $S/<name>; cp C/go.mod C/go.sum $S/<name>/;
(cd $S/<name> && go mod edit -replace github.com/data-insights-ai/rho-tkg/v4=/home/renework2023/Work/2026/datainsights/rho-tkg go.mod)`
then `cd C && GOFLAGS=-mod=mod go build -modfile=$S/<name>/go.mod ./... && GOFLAGS=-mod=mod go vet -modfile=$S/<name>/go.mod ./...`
(all three built and vetted clean against v4.49.0).

## Appendix C — the origin watch (Monitor tool, timeout 1800000 ms, re-arm on expiry; one at a time, two overlap and double-report)

```bash
#!/usr/bin/env bash
# Watches origin of rho-tkg for pushes by anyone but "dev team" (this machine's git user),
# plus every new/deleted branch or tag, plus PR and issue changes. One line per event.
cd /home/renework2023/Work/2026/datainsights/rho-tkg || exit 1
ME="dev team"

declare -A P
load() { # name of assoc array to fill from ls-remote
  local -n arr=$1; arr=()
  local sha ref
  while read -r sha ref; do [ -n "$ref" ] && arr["$ref"]=$sha; done < <(git ls-remote --heads --tags origin 2>/dev/null)
}
load P
[ ${#P[@]} -eq 0 ] && { echo "watch: initial ls-remote failed"; exit 1; }

prs() { gh pr list --state all --limit 30 --json number,title,state,headRefName,updatedAt,author \
  --jq '.[] | "PR #\(.number) [\(.state)] \(.headRefName) by \(.author.login): \(.title) @\(.updatedAt)"' 2>/dev/null | sort; }
issues() { gh issue list --state all --limit 30 --json number,title,state,updatedAt,author \
  --jq '.[] | "ISSUE #\(.number) [\(.state)] by \(.author.login): \(.title) @\(.updatedAt)"' 2>/dev/null | sort; }
prevprs=$(prs); previss=$(issues)

echo "watch armed: ${#P[@]} refs, $(echo "$prevprs" | grep -c .) PRs"
while true; do
  sleep 60
  declare -A C; load C
  if [ ${#C[@]} -gt 0 ]; then
    git fetch -q origin --prune --tags 2>/dev/null
    for ref in "${!C[@]}"; do
      new=${C[$ref]}; old=${P[$ref]:-}
      [ "$new" = "$old" ] && continue
      short=${ref#refs/heads/}; short=${short#refs/tags/}
      case "$ref" in *'^{}') continue ;; esac
      if [ -z "$old" ]; then
        case "$ref" in
          refs/tags/*) echo "NEW TAG $short -> ${new:0:7}" ;;
          *) echo "NEW BRANCH $short @ ${new:0:7}: $(git log -1 --format='%an: %s' "$new" 2>/dev/null | cut -c1-120)" ;;
        esac
      else
        range="$old..$new"
        authors=$(git log --format='%an' "$range" 2>/dev/null | sort -u)
        if [ -z "$authors" ]; then
          echo "REF MOVED (force-push or rewind) $short ${old:0:7} -> ${new:0:7}"
        elif echo "$authors" | grep -qvx "$ME"; then
          n=$(git rev-list --count "$range" 2>/dev/null)
          echo "PUSH $short +$n commit(s) by $(echo "$authors" | grep -vx "$ME" | paste -sd, -):"
          git log --format='  %h %an: %s' "$range" 2>/dev/null | grep -v " $ME:" | head -5 | cut -c1-140
        fi
      fi
    done
    for ref in "${!P[@]}"; do
      [ -z "${C[$ref]:-}" ] && case "$ref" in *'^{}') ;; *) echo "DELETED ${ref#refs/*/}" ;; esac
    done
    P=(); for k in "${!C[@]}"; do P[$k]=${C[$k]}; done
  fi
  unset C
  curprs=$(prs)
  if [ -n "$curprs" ] && [ "$curprs" != "$prevprs" ]; then
    comm -13 <(echo "$prevprs") <(echo "$curprs"); prevprs=$curprs
  fi
  curiss=$(issues)
  if [ -n "$curiss" ] && [ "$curiss" != "$previss" ]; then
    comm -13 <(echo "$previss") <(echo "$curiss"); previss=$curiss
  fi
done
```

## Appendix D — what a fresh session can and cannot rely on

- CAN: everything in the repo at `main` (`HANDOVER.md`, `CHANGELOG.md`, `tasks/lessons.md`, `tasks/backlog.md`,
  `tasks/todo.md`, `tasks/evidence/**`, `docs/**`, git tags) and the memory notes in
  `~/.claude/projects/-home-renework2023-Work-2026-datainsights-rho-tkg/memory/`.
- CANNOT: the cross-session socket addresses in §4 (they belong to sessions that may have ended; use `ListAgents` to find
  live ones), any agent context or ids from this session, the `/tmp/claude-1000/…/scratchpad` files (scratch exports,
  reviewer probes, benchmark outputs) — the evidence worth keeping was copied into `tasks/evidence/`.
- Global instructions (`~/CLAUDE.md`) and the repo's `CLAUDE.md`/`AGENTS.md` load automatically; the standing rules in §1
  repeat the ones that mattered.
