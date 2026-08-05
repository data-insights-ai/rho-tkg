# rho-tkg backlog

**Single todo/roadmap file.** Done work is **dropped** from here — `CHANGELOG.md`
is the source of truth for what shipped, why, and measured numbers. Keep only
genuinely open items (and short pointers to closed epics / reopen criteria).

This file tracks only rho-tkg work. External orchestration and product-layer
RPCs that already have local primitives here are out of scope.

**Severity legend:** CRITICAL = crash / data loss / replica divergence / silent
corruption. HIGH = silent wrong answer or reachable correctness bug. MEDIUM =
concurrency edge / perf cliff / contract inconsistency. LOW = smell / doc drift.
TEST-GAP = real behavior unverified (may hide a bug). FEATURE = plausible
capability not yet built. DO-NOT-BUILD = decided against; reopen criteria only.

**Remaining open work:** no CRITICAL or HIGH items. Open:

1. **Import-under-a-scope** (improvement-not-bug, deferred locking)
2. **BACKLOG 22** — six TEST-GAP research items (from the retired `.harden/` ledger)

---

## Open — import-under-a-scope

Carried over from the retired `tasks/todo.md` (`4f71fc3`).

- **Import-under-a-scope** (the proper fix beyond the import stopgap): wrap
  `IO().Import` in a `TxChangeLogScope` so a FAILED import emits NOTHING, not just
  "no poison". Import emits change-log records IN-BACKEND eagerly, outside any
  scope, so `importRollback.restoreRegistries` is still gated on
  `changeLogEnabled` — the append-only stopgap — rather than de-allocating tokens
  exactly. Needs import to also take `c.txMu` (it takes only `c.mu.Lock` today, so
  a between-mutations open tx scope would collide with import's `BeginLogScope`);
  a locking change, deferred. The current stopgap is correct — see
  `tasks/lessons.md` 55.

  **Measured 2026-08-05** (`BenchmarkImport_ChangeLogCost`, 5,000 nodes + 5,000
  rels, variance <1%):

  | | time | allocs |
  |---|---|---|
  | ChangeLog off | 216ms | 1.4M |
  | ChangeLog on | 621ms | 7.0M |

  The STOPGAP itself costs nothing: `restoreRegistries` is reached only from
  `rollback()`, i.e. only on a FAILED import, and its change-log branch is an early
  `return nil` — cheaper, not dearer. Nothing on the success path touches it.

  The eager emission the stopgap works around costs 2.9x, so the prize is real —
  but a SECOND BLOCKER, not previously recorded here, is that the fix may not
  collect it. A scope BUFFERS records in memory (badger: `bs.scopeLog`, an
  unbounded `[][]byte` released only at commit). A transaction is small; an import
  is the whole graph, and bootstrap-importing into a change-log-enabled store is
  exactly the case that would hold every record at once. So the item trades a 2.9x
  time cost for a memory cost proportional to the entire import, and wants a
  spill/chunk story before the locking change is even worth planning.

---

## Open — BACKLOG 22: adversarial / soak research park

Rescued 2026-08-05 when the June 2026 `.harden/coverage.md` ledger was deleted.
**TEST-GAP / research**, not known defects. The wire-decode and import-amplification
bugs that ledger found shipped as v4.9.2 / v4.9.3. These six were the "attack next"
remainder and were never numbered into BACKLOG 6–21. Partial later coverage is
noted per item.

Prioritize only when a consumer needs the confidence or when touching the named
subsystem.

### 22a. [TEST-GAP] Tiered crash-fault injection between cross-shard writes

Process-kill between cross-shard split-writes (E→R / R→E), mid-flush, mid-cascade.
Happy-path and a few residue paths exist (`tieredstore_write_rel_crash_residue_test.go`,
`tiered_registry_crash_test.go`); no systematic fault-injection matrix that kills at
each ordering step and asserts reopen + `RunRepair` / rollback leave a consistent
neighborhood.

### 22b. [TEST-GAP] Clock-skew vs hot→warm rotation and cold demotion

Rotation / cold demotion / `ShardWindow` edges under non-monotonic or skewed wall
clock (tests must not use sub-millisecond windows). Atomic catalog rotation tests
exist; deliberate clock jumps across window boundaries do not.

### 22c. [TEST-GAP] Fuzz tiered metadata decoders independently

Tiered catalog / registry / temporal-index / vector-index definition files
(`registry_file.go`, `temporal_index_file.go`, `vector_index_file.go`, catalog
JSON) are not fuzzed. Badger meta goes through `SafeUnmarshal` but is not
independently fuzzed. Goal: native Go fuzz + committed `testdata/fuzz/` crashers,
same discipline as `FuzzWireTo*Checked` / `FuzzImport`.

### 22d. [TEST-GAP] Property / vector index queries under adversarial values

NaN / ±Inf / huge-dim / mixed-type values on **query** paths (not only write
validation). Partial coverage exists (dim-mismatch sentinels, some type-class
handling, vector tests); not an exhaustive battery or fuzz on the query door.

### 22e. [TEST-GAP] Long-running concurrency soak under the race detector

Mixed standalone + tx + batch + concurrent ingest for minutes under `-race`,
beyond targeted race tests. Opt-in / manual (or a long CI job) — not `make test`
short mode.

### 22f. [TEST-GAP] Resource-exhaustion cliffs on huge-but-valid inputs

Max labels / properties / containers / blobs at `ValidationLimits` ceilings —
latency/alloc cliffs. Valid inputs must not OOM or hang; fail closed with clear
sentinels where a limit exists.

---

## Not tracked here (cross-team)

Consumer builds these; rho-tkg already exposes the local primitives:

- START→END foreign-stub-delete fan-out (BACKLOG 2 Inc 4c)
- Consumer-gated constraint dry-run (HP2.5)

When consumer pins a shape that needs a **new** rho-tkg primitive, it re-enters
**Open** as a concrete item.

---

## Closed (pointers only — detail in CHANGELOG)

| Epic | Where it landed |
|------|-----------------|
| BACKLOG 1 — Retention purge (ex-ADR-0008 R2–R5) | CHANGELOG (4.18–4.24 era) |
| BACKLOG 2 — Cross-machine Model A (ADR-0010 §3.3) | CHANGELOG |
| BACKLOG 3 — Columnar / streaming whole-node fetch | CHANGELOG |
| BACKLOG 4 — Review adaptations (4b–4e; **4a DO NOT BUILD**) | CHANGELOG |
| BACKLOG 5 — Rel ordering-soundness (`RelRangeCardinality`, type-class) | CHANGELOG |
| BACKLOG 6–21 — full-library hardening (~196 findings) | closed 2026-07-18…22, `[4.24.0]` |
| Last HIGH (10b cascade/resumption) + 10c perf follow-up | `[4.24.0]` / next-day fix |
| Consumer-gated ask batch (5 items, 2026-07-29) | `[4.25.0]` same day |
| CI bench-gate (blocking) | 2026-07-29, `bench.yml` |

Recover closed investigation prose via `git log --all -- tasks/backlog.md` if needed.

### DO NOT BUILD / reopen criteria (keep here so they are not re-filed)

- **Per-version temporal-envelope prune (ex-BACKLOG 4a):** owner-decided net-negative
  for the confirmed workload. Detail in CHANGELOG.
- **Wire `fv` bump** (widen temporal tail to full envelope + eclipsed-row flag) **and
  inverted-suffix history keyspace:** DECIDED NOT BUILT 2026-07-29 after no-format-change
  paths shipped (tail-peek TX; selection-skeleton + zero-alloc token scanner VT).
  **Reopen only if** consumer re-runs depth oracles against current HEAD **and** residual
  depth-linearity still breaks a concrete consumer latency budget — then the prepared
  v3 design is the next step.
- **Zone maps before I/O (columnar phase-2 ZM):** withdrawn 2026-08-04; envelope prune +
  in-RAM `BlockCanMatch` cover the need. Reopen only if a consumer needs a block-level
  pre-I/O prune that those cannot express without breaking full-membership snapshots
  (see CHANGELOG `[4.27.0]`).
