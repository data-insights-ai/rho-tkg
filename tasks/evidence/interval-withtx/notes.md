# Item P (backlog 34) — `SetNodeVersionIntervalWithTx` / `SetRelVersionIntervalWithTx`

Requested by sigma-tkgd: a replayed burst growth `[vs,ve) -> [vs,ve')` must be stamped at the record's own
transaction instant, not at the commit clock. Mirrors `DeleteWithTx` / `UpdateWithTx`
(handover tasks/handover-tx-backfill-delete-update-20261009.md §6, removed from main 2026-10-12, in git history).

## Ledger (written before the first test or code edit, base 7b3e11b = v4.49.0 + docs)

| Step | In code before? (file:line at base) | Red test | Break-the-code cases | Proof |
|---|---|---|---|---|
| Kernel seam: one caller instant for every appended row | no: `core/temporal_cascade.go:142` and `:691` stamp `now := c.now()` for every appended row | `TestIntervalWithTx_StampsEveryAppendedRow` | bounded `[a,b)` (2 pieces + resumption taking the slot), open `[a,0)` (last piece takes the slot); mutants: ignores t, t+1, resumption stamped with the clock, slot row stamped with the clock | red-1.txt, green-1.txt, mutants.txt |
| Order rule against the whole chain | no (the cascade has none) | `TestIntervalWithTx_Order`, `TestCheckCascadeTxOrder` | t = newest TxFrom, newest-1, binding stamp on a history row above the current row (lesson 62 shape), binding row not overlapping the target interval (decision: still ErrTxOrder), re-imported life; unit: each of TxFrom/TxTo/UpdatedAt/DeletedAt binding, nil rows; counterpart max+1 stamped exactly; every refusal changes nothing | red-1.txt, green-1.txt |
| Gate + value (`resolveCallerTxInstant`) | no door exists | `TestIntervalWithTx_GateAndValue` | gate off (valid t, order-violating t), t = 0, -1, clock+1, MaxInt64 with gate on and off | red-1.txt, green-1.txt |
| Commit clock not advanced by the door | no door | `TestIntervalWithTx_CommitClockNotAdvanced` | frozen clock: instants burned by the WithTx door == by the plain door (the gate's read replaces the kernel's); mutant: the kernel also reads `c.now()` | red-1.txt, green-1.txt, mutants.txt |
| As-of column cache after the write | no door | `TestIntervalWithTx_PrimaryCacheStale` (W2 pattern: a build interleaved at the gate), `TestIntervalWithTx_RelDoorsReportPastDatedWrite` | mutants: no report; report before the write; batch/ingest fold missing the cascade instants | red-1.txt, green-1.txt, mutants.txt |
| Deleted entity still refused | yes for the plain doors (`temporal_cascade.go:128-132`) | `TestIntervalWithTx_DeletedEntity` | deleted node/rel on every WithTx door → ErrEntityDeleted + not-found; never-existed → not-found only; nothing written | red-1.txt, green-1.txt |
| Two-phase reads (rules 15, 17) | no | `TestIntervalWithTx_TwoPhase` | pins t-1 unchanged (AtTx, AsOf, ByLabel/ByType{ValidAt,TxAt}, {TxPin}, effective timeline), pin t sees the correction and equals the far-future pin, timeline == AtTx at t; after a later plain write both pins still answer the same | red-1.txt, green-1.txt |
| Burst-growth replay `[vs,ve)` → `[vs,ve')` → `[vs,ve'')` at t1 < t2 | no | `TestIntervalWithTx_BurstGrowthReplay` | every door; RelAtTx/NodeAtTx at pins t0/t1/t2 just inside/outside each end; effective timeline ends; `Verify*Chain` | red-1.txt, green-1.txt |
| GraphTx twins + rollback | no | `TestIntervalWithTx_TxRollbackEquiv` | rollback: export bytes + pins identical; commit equals the standalone door (export equivalence) | red-1.txt, green-1.txt |
| Batch / ingest whole-unit pre-flight, one op per entity per unit | no: `batch_callertx_preflight.go:45` `hasCallerTx` ignores cascades | `TestIntervalWithTx_BatchIngestWholeUnit`, `TestIntervalWithTx_IngestStrongGroupAlone` | refused cascade + plain op on another entity → nothing written; cascade + second op on the same entity; cascaded rel of a node delete in the unit; deleted entity in the unit; strong mode: a group with a caller cascade is applied alone | red-1.txt, green-1.txt, mutants.txt |
| Replica reproduces the stamps | change feed carries rows verbatim (no code expected) | `TestIntervalWithTx_ReplicaReproducesStamps` | replica chain stamps == primary's, pins t-1/t agree, warm replica cache updated | green-1.txt |
| Plain doors unchanged | yes | `TestIntervalWithTx_PlainDoorsUnchanged` (GUARD: green before the change) | the plain cascade on every door stamps the clock reading on every appended row | r0-before.txt |
| Facade (`temporal.API`, Ops, spy, nil receiver) + errors.Is per public door | no | `TestAPIIntervalWithTxForwardsInstantVerbatim`, nil-receiver table, `TestIntervalWithTx_PublicDoors` (pkg/graph) | instant dropped/replaced, plain door called, sentinel swallowed by a wrapper | red-1.txt, green-1.txt |
| W5 oracle generator | no | `TestTxBackfillOracle_CrossBackend` with the new op kinds, `TXB_ORACLE_SEEDS=100` | per op: every appended row at t, pins t-1/t; refused t <= newest stamp; cross-backend chains and answers | red-oracle.txt, green-oracle.txt |
| Race: order check under the entity lock | n/a | `TestIntervalWithTx_RaceClock` (-race) | plain updates and plain cascades race the caller-instant cascades; every accepted t above every lower-version stamp | green (-race) |

Decisions (defaults, documented in docs/api.md):
- The order rule is about the entity's chain, not the target interval: t must exceed every TxFrom and TxTo of every
  row of the chain (all lives of a re-imported ID), also of rows whose valid interval does not overlap
  `[validFrom, validTo)`. It is `checkTxOrder(t, 0, chain...)` from `tx_order.go`, the rule of the other caller-instant
  doors (review 2026-10-11: a cascade-only rule that also counted UpdatedAt/DeletedAt cannot change a result through the
  doors, since stampAppendedRow sets UpdatedAt = TxFrom and a tombstone carries DeletedAt = TxTo; it and
  `TestCheckCascadeTxOrder`, with mutants M5d-M5f, were removed).
- A `validFrom` after t is allowed (a correction recorded at t may concern the future: valid time and transaction
  time are independent axes; the appended rows carry an explicit `ValidFrom`, so `UpdatedAt = t` never derives a
  valid start).

## Review round 2026-10-11

Applied: shared `checkTxOrder` (above); the batch door doc says the whole-unit pre-flight covers the caller-instant
refusals only (unique constraint / property limit can still fail the op mid-unit, as for UpdateWithTx); tests trimmed
(TwoPhase folded into BurstGrowthReplay, ValidFromAfterT is the "future" shape of StampsEveryAppendedRow, RaceClock one
small case, PublicDoors one errors.Is row per sentinel per door, RelDoorsReportPastDatedWrite dropped as a subset of
PrimaryCacheStale); mutants re-run (mutants.txt). Red files above are from the first, larger suite (kept as recorded).
