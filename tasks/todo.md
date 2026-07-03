# tkg/v4 — open work

**Shipped since this file was opened** (finished changes move to `CHANGELOG.md`):
the per-transaction change-log buffer — a rolled-back tx or partially-failed batch
now emits NOTHING to the op-log feed via the optional `store.TxChangeLogScope`
capability (records buffered, LSNs minted at commit, discarded on rollback) — plus
the badger `flushing` commit-window overlay fix, `Clear()` LSN-reanchor durability,
the `ApplyChanges` ascending-LSN guard, the multi-token-label-diff apply hardening,
and the rolled-back-token append-only stopgap. All released in `CHANGELOG.md`
`[4.10.1]`–`[4.10.3]` and captured in `tasks/lessons.md` 49–58. The §4.1
transaction-time backfill + §4.2 named as-of tags shipped in `[4.11.0]` (lesson 59).

This file tracks **only open work**.

---

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
