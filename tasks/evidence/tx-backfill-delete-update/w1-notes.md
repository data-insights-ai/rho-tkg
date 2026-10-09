# W1 notes — rel DeleteWithTx / UpdateWithTx (worktree agent-abb0effa193edc7bc)

Evidence: `r0-before-w1.txt` (R0 green on b8999a9 before the seam), `red-w1.txt` (doors stubbed to the plain
path; cache guard red after merging W2), `green-w1.txt` (-race).

Helpers the node/GraphTx/batch/ingest phases reuse (`internal/core/tx_order.go`):
- `resolveCallerTxInstant(t)` — t <= 0 → ErrInvalidTxFrom, then `resolveBackfillTxFrom` (value, then privilege).
  Follow it with `defer c.notePastDatedWrite(at)` (W2).
- `checkTxOrder(t, start, chain...)` — entity-agnostic, under the entity lock; t > start and > every TxFrom/TxTo.
- `checkCallerDeleteCloses(t, tms...)` — refuses a recorded close at or after t ("recorded close at or after t
  (ValidTo v, t t)"); pass every row a delete tombstones (node + cascaded rels).
- `relChainTemporals`, `relTxDeleteStart`, `checkRelCallerTx` — rel-specific; nodes need `node*` twins
  (`nodeTxDeleteStart = max(nodeValidFrom, nodeCurrentVersionStart)`).
- Seams: `deleteRelationshipInternal(ctx, id, at)` (0 = plain); `updateTemporal.txAt` consumed by
  `updateRelationshipPreparedInternal` (0 = plain); `updateRelationshipAtInternal` for the map path.

Decisions: R8 refuse (coordinator-confirmed). No-op/empty update at t → ErrTxOrder after the existence check.
Update start = `relCurrentVersionStart` (as the plain floor); delete start = max(valid-from, version start).
Note for replay callers: an update without tkg_valid_from resets ValidFrom, so the next version's start derives
from the id's mint time — a later DeleteWithTx at a t before that mint time then refuses (the tombstone would
invert the valid interval); replays pass tkg_valid_from on UpdateWithTx too.
