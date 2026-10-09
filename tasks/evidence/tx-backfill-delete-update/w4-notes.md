# W4 notes — relationship caller-instant doors in GraphTx, Batch and ingest

Ledger (written before the first test or code edit):

| Step | In code before W4? | Red test | Break-the-code cases | Proof |
|---|---|---|---|---|
| GraphTx `DeleteRelationshipWithTx` / `UpdateRelationshipWithTx` | no: `tx_mutations.go` DeleteRelationship passes `at = 0` (~:448), UpdateRelationship has no instant (~:224) | `TestTxBackfillRel_GateOff`, `_InvalidInstant`, `_OrderEqualReversed`, `_OrderValidStart`, `_DoorsReportPastDatedWrite` (door table extended), `TestTxBackfillRelW4_CloseCollision`, `_ScheduledCloseAfterT`, `_TxRollbackEquiv`, `_DoorEquivalence` | gate off, t 0/-1/clock+1/MaxInt64, t = TxFrom / TxFrom-1 / below a history TxTo, t <= ValidFrom / UpdatedAt, close at t, close after t, rollback, commit vs standalone bytes | red-w4.txt, green-w4.txt |
| Batch `DeleteRelationshipWithTx` / `UpdateRelationshipWithTx` | no: `batch_execute.go` steps 4/5 carry no instant, `relDeletes` is `[]RelID` | same door table + `TestTxBackfillRelW4_BatchIngestDoors` | as above + mixed batch with one order failure (whole batch refused, skeleton TxFrom 0), two ops on one rel, no-op update | red-w4.txt, green-w4.txt |
| Ingest `Session.{Delete,Update}RelationshipWithTx` (strong + concurrent) | no: `ingest.go` Session has plain rel doors only; `ingest_concurrent.go` delete passes `at = 0` | same door table + `_BatchIngestDoors` (strong, concurrent) + `_IngestCoalescedGroups` | as above + a refused group coalesced with good groups (the good groups must commit) | red-w4.txt, green-w4.txt |

## Doors and decisions

All eight doors got the seam; none is refused outright.

- GraphTx: `tx.DeleteRelationshipWithTx` shares the plain twin's body
  (`deleteRelationshipAtLocked(id, at)`: snapshot + history snapshot, seam,
  deletedRels record); `tx.UpdateRelationshipWithTx` snapshots then calls
  `updateRelationshipPreparedInternal` with `tmp.txAt`. Gate before the seam
  (`resolveCallerTxInstant`), `notePastDatedWrite(at)` deferred after the write.
  No-op (empty map after the existence check, equal values in the seam) refuses
  with `ErrTxOrder`. Commit/registry/changelog behaviour unchanged; Rollback
  already bumps the as-of cache (`tx.go` Rollback).
- Batch / ingest queue doors gate the instant at queue time (value, then
  privilege) and write nothing; an empty update map refuses at queue time.
  Deletes queue into a new `relTxDeletes []pendingRelTxDelete` (Execute step
  5b, concurrent apply after plain deletes); updates ride `relUpdates` with
  `update.temporal.txAt`, so the existing step 4 / concurrent loop reaches the
  seam unchanged.
- Whole-unit refusal: `precheckRelCallerTxOps` runs before any write of the
  unit (Execute, under its exclusive lock; concurrent apply, under the shared
  lock + per-rel entity lock) and refuses the unit when one caller-instant op
  would be refused. It reuses the seam's own checks (`checkRelCallerDelete`,
  `checkRelCallerUpdate`, extracted from the seams so the two cannot diverge).
  Batch returns `nil, ErrBatchFailed ⊃ BatchError ⊃ ErrTxOrder`; concurrent
  returns "graph: concurrent ingest group refused before any write: …".
  Without it a batch keeps its successful ops, leaving a partial past that no
  later write at an earlier t can repair.
- A caller-instant op must be the ONLY op on its relationship in a unit
  (creates, plain/caller updates and deletes, cascades count) — `ErrTxOrder`.
  Each op alone passes against the stored chain, but Execute applies creates,
  then updates, then deletes, and plain ops stamp the clock above any t, so the
  later op could fail after the earlier was stamped. Successive writes on one
  relationship go in successive units. Relaxing this (simulating the chain
  across a unit's ops) is additive.
- Strong-mode ingest: `applyCommitGroup` applies a group that carries a
  caller-instant op in a unit of its own (`applyCommitSegment`), never
  coalesced, so its refusal fails that group only and the seq order holds.
- Concurrent ingest residual (documented in code): the pre-flight is not
  atomic against a racing standalone writer (shared lock); such a race makes
  the seam refuse that one op afterwards — per-entity atomicity, as every
  concurrent-mode op.
- Not covered by the pre-flight (keep the batch's per-op semantics, and the
  failing op writes nothing): property-count overflow after the merge, store
  I/O errors, ctx cancellation.

## Test notes

- `txbDoors()` (W1 file) now returns the standalone pair plus `txbW4Doors()`,
  so R1-R4 and the as-of report run over all ten doors x four backends.
  `txbDoor.clockReads` (added after the red run, fixture only): the GraphTx
  wrapper's BeginTx takes one commit-clock instant before the door gates t, so
  R2's "clock+1" is `x + 1 + clockReads` for it (the off-by-one stays exact).
- The tx test door COMMITS even when the call fails, so a door that writes
  before it refuses keeps its write and the unchanged assertions catch it.
- Batch/ingest test doors always apply the unit after a queue error, so a
  queue door that queues and then errors is caught.
- `TestRelsWithTx_TxBatchIngestFacade` (pkg/graph) was written after the core
  implementation; mutation-verified (Session update forwarded to the plain
  door → red).
- Mutation checks run (each red, then restored): Execute pre-flight removed;
  concurrent pre-flight removed; same-rel guard removed (reversed updates
  partially applied); coalescing segmentation removed (sibling groups failed);
  GraphTx update snapshot removed (export differs after Rollback).

## Open for phase 3

- R11 race over the W4 doors (plain Updates racing tx/batch/ingest WithTx).
- R15 oracle including batch/ingest units.
- docs/api.md: the six new methods; the batch whole-unit refusal and the
  one-op-per-relationship rule; strong-mode non-coalescing of caller-instant
  groups.
- Node twins (W3) need the same pre-flight in Batch/ingest; `precheckRelCallerTxOps`
  is relationship-only — a shared unit pre-flight should check node and rel
  ops together (a node delete cascading a rel that a caller-instant op touches
  in the same unit).
