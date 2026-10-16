# v5 handover: review of branch `v5` and the work open before a merge into main

Date 2026-10-12. Reviewed: origin/v5 `3ede096d` (173 commits ahead of main, author Markus Nissl), origin/main `dbd74cf4` (v4.51.1, 23 commits not in v5), merge-base `44303a17` (v4.51.0). Work dir: worktree `rho-tkg-v5-handover`. Paths below are relative to the repository root; `v5/` is the nested module.

## 1. Answer

Mechanically, yes: a trial merge of origin/v5 into origin/main has one conflict (`CHANGELOG.md`), 1,701 files, and touches v4 code in two files only (`pkg/graph/internal/core/ingest.go:497-503`, a no-op refactor, and one memory-store test). As a product, no: `v5/pkg` still exports only `temporal` and `types` (`v5/pkg/types/instant.go:11`, `default_axis.go:12-34`); every graph store, planner, transaction and replication package is `v5/internal/`, so no consumer can open a graph, write, read or import. PLAN has no merge criterion; its phases stand at V0/V1 accepted, V2 active, V3–V7 open (`tasks/backlog.md:79-86`). Critical path to a consumer-usable v5: (1) close V2 package 4 (`tasks/backlog.md:81`); (2) V3 compact engine; (3) V4 public graph API including `types.NodeID/RelID` (`docs/v5/PLAN.md:878`, absent from `v5/pkg/types`); (4) V6 importer (today only a read-side preflight, `tools/v5-migrate/README.md:3-6`) plus consumer migrations in sigma-tkgd and ai-soc; (5) V7 versioned contracts and the tag `v5/v5.0.0`. Recommendation: merge the branch as an unreleased module only after the five small items in §3 (1–5) are done; no v5 tag.

## 2. State and quality

| Area | Done | Verdict | Evidence |
|---|---|---|---|
| Build, vet, fmt | `cd v5 && go build ./...` 10 s, `go vet ./...` clean, `gofmt -l` empty | OK | this review, Go 1.26.9 (`v5/go.mod:3`) |
| Tests | `go test -count=1 ./...` (full, no `-short`): 12/12 packages ok in 9 min 36 s; graphapply 570 s, txnproto 30 s, replica 25 s, rest < 10 s | OK | `v5/Makefile:17-18` (`test` uses `-short`); 1,390 `Test*`, 18 `Fuzz*`, 3,247 `errors.Is` in tests |
| Test quality | Refusal-first tests with hostile inputs, truncation and bit-flip loops, error-precedence tables, fuzz seeds; process-kill seams via child re-exec | Good; one informational test asserts nothing | `v5/pkg/temporal/codec_test.go:19-345`, `v5/internal/dist/graphapply/registered_journal_test.go` (`TestRegisteredJournalInvalidWire`: every prefix and 23 byte flips), `v5/pkg/types/instant_test.go:83-191`; no assertion: `v5/internal/graphstore/full_accounting_test.go` (`TestFullRepresentationSizes`, `t.Log` only) |
| Skips | 21 `t.Skip` sites: parent/child process seams, Unix-only SIGKILL, disk measurements behind `RHO_*` env opt-ins; 2 `testing.Short()` guards | OK, deliberate | `v5/internal/raftlog/id_consumption_bench_test.go`, `v5/internal/dist/graphapply/replicated_process_test.go` |
| Public API | `pkg/temporal`: Axis, Position, Bound, Scope, PointKnowledge, Integer/Rational, Allen, codecs, hashes, Limits. `pkg/types`: `Instant`, `GraphID`, `DefaultAxisBinding` | Values only; no graph, node, relationship, transaction, snapshot or change type | `go doc ./pkg/...`; `v5/pkg/types/instant.go:11`, `default_axis.go:12,34` |
| Engine (internal) | graphapply 68 src/113 test files, graphstore 50/56, raftlog 33/53, replica 8/20, txnproto 7/8, graphstate 14/26, state, assertion, idalloc; 69 k LOC code, 116 k LOC tests | Substantial, well tested, not callable by consumers; largest files 1.0–1.7 k LOC | `v5/internal/graphstore/partition_transaction_stage.go` (1,734), `current_presence_tree.go` (1,312), `v5/internal/raftlog/application.go` (1,256) |
| Dependencies | Pebble v2.1.7, etcd raft v3.7.0, protobuf v1.36.11 | Confined to `internal/`; `pkg/` imports neither | `v5/go.mod:6-9`; grep of `v5/pkg` for pebble/etcd: 0 |
| PLAN reference surface | `OpenSnapshot/Lookup/Scan/Expand/Timeline/Changes/WatchChanges/Capabilities/Statistics/ExplainAccess` | Sketch only; none exists as an exported graph door (`Scan` exists on the internal `ApplicationView` and `timeblock.Block`) | `docs/v5/PLAN.md:370-377`; `v5/internal/raftlog/application.go:902`, `v5/internal/timeblock/block.go:572` |
| Evidence: hosted CI | Backlog cites hosted runs for E15 `30dd0881`, E16 `40e12ef6`, E17 `13612eb3`, E18 `47d98010`, `c61d1124`, `b9da041b`, `7e9caf6f`, `5edb2792`, `fee9b82d`, `f9e0eb63` | All ten verified `success` via `gh run list --branch v5`; HEAD `3ede096d` was `in_progress` at review time | `tasks/backlog.md:153-370` (V2 completion sequence) |
| Evidence: acceptance records | `tasks/evidence/v2-*/acceptance.json` exist for partition-routing, cross-partition, certified-reads, remote-e15, range-e17, index-e18, allocation-p08 | Present; coverage percentages and SHA manifests inside not recomputed (unverified) | `tasks/evidence/v2-range-e17/acceptance.json`, `tasks/evidence/v2-index-e18/acceptance.json` |
| Evidence: independent reference suite | `docs/v5/reference/independent/check.py` | FAILS: "validation.json is stale". Only the recorded `PLAN.md` hash differs (295 suite files match); PLAN was revised after validation.json was written. Backlog says it passes | `check.py:98-111`; claims `tasks/backlog.md:99`, `:418` |
| Docs vs code | PLAN keeps `types.NodeID/RelID`; `pkg/types` has neither. PLAN: status only in the backlog; CHANGELOG `[Unreleased]` carries 28 v5 status bullets of 54. PLAN §8a delta stops at v4.51.0: the v4.51.1 doors (`CurrentStampsCapability`, `ColumnData.Other`, `ForEachRelEffectiveAtNodes`, `MemberIDsCapability`, `HistoryStampsCapability`) have 0 hits in PLAN and both consumer manifests | Three inconsistencies, all small | `docs/v5/PLAN.md:878`, `:12`, `:895-911`; `CHANGELOG.md:11-185`; `docs/v5/reference/consumer-*.json` |
| Framing | PLAN and DISCUSSION call rho "the graph database" | Owner framing is "embeddable knowledge graph library, not a database" (René 2026-10-11); wording decision for René, not a code issue | `docs/v5/PLAN.md:8`, `docs/v5/DISCUSSION.md:3` |
| v5 on main's analysis | v5 rewrote `docs/v5-analysis.md` (René's 2026-10-11 version) and root `HANDOVER.md` §15 | Merges without conflict and replaces main's text; 45-row door mapping retained, percentages removed | `git diff origin/main origin/v5 -- docs/v5-analysis.md HANDOVER.md` |
| `tools/v5-migrate` | Builds; `go test` 11 parents/51 runs pass in 0.06 s. Read-side preflight (`PreflightNode/Rel`) over an open v4 graph; no destination writer | OK as a component; pinned to v4 `v4.50.1` via `replace => ../..` while main is v4.51.1; README demands requalification for a newer reader | `tools/v5-migrate/go.mod:5-6,31-33`, `README.md:3-16` |
| `tools/v5-substrate/fdb` | cgo, needs `foundationdb/fdb_c.h` | Not built here (header absent); separate module, root/v5 builds unaffected | `tools/v5-substrate/fdb/go.mod:5` |
| `tools/vendor-compare` | Builds; protocol only, no results | Comparative capacity (V7) not started | `docs/v5/PLAN.md:934-1020`, `tools/vendor-compare/README.md` |
| CI and Makefile | ci.yml matrix root + v5 + v5-migrate; root race in four batches; root `make` dispatches to v5, graph-compare and migrate by default | Works on v5 (10 green runs); doubles root CI time; main's ci.yml has no v5 job | `.github/workflows/ci.yml:28-29,63-67,169-174`; `Makefile:20,29,41`; `scripts/test-race.sh` |
| Repository weight | `tasks/evidence` 143 MB, 43 tarballs (55 MB) added by v5 | Large for a source repo | `du -sh tasks/evidence` |
| Module docs | No `v5/README.md`, no `doc.go`, no tagging note; SECURITY.md supports v4.x only | Missing for a published module | `ls v5`; `SECURITY.md:11`; `docs/stability.md:15` |

## 3. Open before and after the merge (prerequisites first)

| # | Item | Size | Closing proof |
|---|---|---|---|
| 1 | Decide what the merge means: bring `v5/` to main as an unreleased module (default) or wait for a v5 release. Record in `tasks/backlog.md` | S | Backlog line with the decision |
| 2 | Resolve the `CHANGELOG.md` conflict: keep main's `[4.51.1]` section; put v5 bullets under one `[Unreleased]` subsection "v5 module (unreleased)" | S | Trial merge clean; root `make check` and `cd v5 && make check` pass |
| 3 | Refresh the independent validation after the PLAN change (`python3 -B check.py --write-validation` after reviewing the PLAN diff) | S | `python3 -B docs/v5/reference/independent/check.py` exits 0 |
| 4 | Extend PLAN §8a and `consumer-contracts.json` / `consumer-migration.json` to v4.51.1 (`CurrentStampsCapability`, `HistoryStampsCapability`, `MemberIDsCapability`, `ColumnData.Other`, `ForEachRelEffectiveAtNodes`, `RelsEffectiveByIDs`) | S | Table rows present; grep hits |
| 5 | `tools/v5-migrate`: requalify against the v4.51.1 reader (bump `go.mod` require, rerun gates, new `qualification.json` hashes) | S | `qualification.json` names `dbd74cf4`/v4.51.1; tests pass |
| 6 | Evidence weight: keep the 55 MB of tarballs out of main (release assets or LFS) or accept the size | M | `du -sh tasks/evidence` after merge; decision recorded |
| 7 | Module hygiene: `v5/README.md` + `doc.go` (status, public packages, "not a release"), tagging scheme `v5/vX.Y.Z` for the nested module, SECURITY.md and stability.md lines for v5 | M | Files exist; `go list -m github.com/data-insights-ai/rho-tkg/v5@v5/<tag>` resolves after the first tag |
| 8 | Keep main's `docs/v5-analysis.md` §5 anchors intact or accept v5's rewrite (it replaces René's version without conflict) | S | Diff reviewed by René |
| 9 | V2 package 4: broader ownership recovery, identity reuse/fencing, application GC, physical-host durability, transactional-KV comparison and engine decision | L | E15–E18 serial-history/cut oracle under process and message faults; engine decision in backlog (`PLAN.md:820,845`) |
| 10 | V3 compact engine: seal/merge/recovery, paged catalog/indexes, budgets | L | Byte ledger at 790 K/3.15 M/12.6 M rows; crash/reopen oracle; v4 fast-path comparison (`PLAN.md:821,846`) |
| 11 | V4 public graph API: a public package with open/close, writes (create, correct, close, retract), `OpenSnapshot`, `Lookup/Scan/Expand`, `Timeline`, `Changes/WatchChanges`, `Statistics`; `types.NodeID/RelID` as promised (`PLAN.md:878`); each "None yet" row of the 45-door mapping gets a public successor or an explicit decline | L | A module outside the repo compiles against it; sigma's pinned `access` contract mutation-then-historical tests pass (`PLAN.md:822,882`) |
| 12 | V6 importer and operations: destination writer over the preflight, retraction `fv=3`/`rx`, one-tick ambiguity report, backup/restore, retention/erase; consumer migrations committed in sigma-tkgd and ai-soc | L | Canonical parity + ambiguity report on a v4 store; consumer repo commits (`PLAN.md:824,849`) |
| 13 | V7: versioned API/wire/descriptor contracts, full CI/race/security, comparative capacity vs Neo4j/TigerGraph/Memgraph and v4, tag `v5/v5.0.0` | L | Five-run byte/latency ledger with dispersion; tag exists (`PLAN.md:825,850`) |

Consumer readiness (checked 2026-10-12 on local checkouts): sigma-tkgd main `a51cc194b` pins `rho-tkg/v4 v4.50.1`, ai-soc main pins `v4.49.2`; neither imports `rho-tkg/v5`. Both wait on item 11 (public API), ai-soc also on sigma moving first.

Main's 23 commits not in v5 are v4-only (v4.50.2 folded into v4.51.1: backlog 33, 35, 42, 48; 46 files in `pkg/graph`). v5 needs them only as PLAN §8a rows and manifest entries (item 4); no v5 code depends on them.

## 4. Not verified

- Tests ran once, no `-race`; the six-process SIGKILL seams ran as part of the suite, the disk-measurement and genesis-cohort lanes stayed skipped (env opt-ins).
- Not run: `-race`, coverage gate, golangci-lint, gosec, govulncheck (tools not installed here); hosted CI for `3ede096d` was still running.
- Coverage percentages, statement counts and SHA-256 manifests inside `tasks/evidence/*/acceptance.json` and `tools/v5-migrate/evidence/qualification.json` were not recomputed.
- `tools/v5-substrate/fdb` (needs FoundationDB headers) and `tools/vendor-compare` runs (need Docker) were not executed.
- Consumer repositories were grepped for imports and pins only; no consumer build against v5 was attempted (none is possible without item 11).
