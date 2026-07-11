# tkg/v4 — open work

**Shipped since this file was opened** (finished changes move to `CHANGELOG.md`):
the per-transaction change-log buffer — a rolled-back tx or partially-failed batch
now emits NOTHING to the op-log feed via the optional `store.TxChangeLogScope`
capability (records buffered, LSNs minted at commit, discarded on rollback) — plus
the badger `flushing` commit-window overlay fix, `Clear()` LSN-reanchor durability,
the `ApplyChanges` ascending-LSN guard, the multi-token-label-diff apply hardening,
and the rolled-back-token append-only stopgap. All released in `CHANGELOG.md`
`[4.10.1]`–`[4.10.3]` and captured in `tasks/lessons.md` 49–58. The §4.1
transaction-time backfill + §4.2 named as-of tags shipped in `[4.11.0]` (lesson 59). Tx-aware
adjacency push-down — `g.Rels().OutgoingForNodesAtTx(nodeIDs, typeName, txAt)` /
`IncomingForNodesAtTx(...)` resolve pinned adjacency through the adjacency index + the
deleted-relationship fold instead of a full history-aware `ByType` scan, agreeing with a pinned
`ByType` scan filtered by endpoint by construction (`txAt == 0` delegates to the existing
`OutgoingForNodes`/`IncomingForNodes` verbatim). See `CHANGELOG.md` `[Unreleased]`.

This file tracks **only open work**.

---

## HP (hallucination-prevention) forward work — see `tasks/hp-workplan-2026-07-04.md`

rho-tkg is **sufficient** for the current HP milestone (v4.11.1: sound as-of pinning incl.
the delete-tombstone fix + tamper-evident anchors). The critical path is *sigma-tkgd*
HP1.2/HP1.3, which needs no new rho-tkg API — so these are **demand-ordered**, pulled when
runtime scale / a later phase makes them bite. Full rationale + break-tests in the workplan.

- [x] **RT-1 Tx-aware adjacency push-down** — CLOSED by `g.Rels().OutgoingForNodesAtTx`/
      `IncomingForNodesAtTx`: pinned adjacency resolves through the live adjacency index + the
      deleted-rel fold, re-resolving each candidate through the same chain seam as the generic
      `TxAt` door; divergence-probed against the pinned `ByType` scan on all three backends.
- [x] **RT-2 TxAt-door valid-time guardrail** — CLOSED by `QueryOpts.TxPin`: a belief-state pin doing
      pure knowledge-time resolution with no valid-time filter (identical to `NodesAsOf`) through the
      generic `ByLabel`/`ByType`/`All` door; the `TxAt`-alone door keeps its behaviour but its godoc now
      warns loudly, and mixing `TxPin` with any other temporal filter returns `ErrConflictingTemporalOpts`.

- [ ] **RT-3** (hold for HP Phase 2.5) dry-run constraint validation (validate-without-write).
- [ ] **RT-4** (hold for HP Phase 3.9) wire the unused vector index (`vector_search.go:54`) + fuzzy resolver.
- [ ] **RT-5** (hold for audit) whole-graph merkle/snapshot root.

## Remaining follow-ups (optional, lower priority)

- [ ] **Import-under-a-scope** (proper fix beyond the import stopgap): wrap
      `IO().Import` in a `TxChangeLogScope` so a FAILED import emits NOTHING (not
      just "no poison"). Needs import to also take `c.txMu` (it currently takes only
      `c.mu.Lock`, so a between-mutations open tx scope would collide with import's
      `BeginLogScope`). Locking change — deferred. The current append-only stopgap
      (import `restoreRegistries` gated on `changeLogEnabled`) is correct.
- [ ] **tiered**: confirmed it does NOT implement `TxChangeLogScope` (no change-log)
      → core resolves nil → tx/batch emit eagerly (moot). If a sharded backend ever
      gains a change-log, replicate the scope (B33-class).
- [ ] **Coverage polish**: `lockActiveCoreWriteContext` happy path (tx
      `ImportNodeWithID` under change-log) is lightly covered — add a direct test.

## CI gate

- `make ci-docker` runs the full gate — `fmt-check` + `vet` + `lint` + `build` +
  `test-race` + `security` + `vulncheck` + `cover-gate` — with `golangci-lint`,
  `gosec`, and `govulncheck` inside the go.mod-matching `golang:<version>` image
  (the three tools are not installed on the host; Docker is). See CLAUDE.md
  "Running lint/security/vulncheck via Docker".
- Pre-existing baseline findings (a stdlib-only `govulncheck` advisory fixed in a
  later Go patch, ~9 `gosec` G115s, ~39 `golangci-lint` findings) all live in files
  outside recent changes — filter gate output to `git diff --name-only`.
