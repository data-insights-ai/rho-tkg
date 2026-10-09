# W3 notes — node DeleteWithTx / UpdateWithTx (every door)

## Ledger (written before the first build)

| Step | In code today? | Red test | Break-the-code cases | Proof |
|---|---|---|---|---|
| R0 plain node doors | yes: `deleteInstantForNodeCascade` (temporal.go:69), `nodeVersionUpdateInstant` (temporal.go:163), `deleteNodeLocked` (node_delete.go:313) | `TestTxBackfillNode_PlainDoorsUnchanged` | F+1/F+2 floor, clock window, cascade rel with a later valid-from pushes the one instant, close collision on node and on a cascaded rel moves the instant, scheduled close clamped; standalone, GraphTx, batch | `r0-before-w3.txt` (green before the seam) |
| doors | no: `nodes.Ops` has no WithTx; GraphTx/batch/ingest node doors take no instant | R1 `GateOff`, R2 `InvalidInstant` | gate off, t = 0, -1, clock+1, MaxInt64, reserved keys, every door (standalone, GraphTx, batch, ingest sync/concurrent) | `red-w3.txt` |
| order | no node twin of `checkRelCallerTx` | R3 `OrderEqualReversed`, R4 `OrderValidStart` | t = TxFrom, TxFrom-1, below a history TxTo above current (lesson 62), explicit/derived ValidFrom, UpdatedAt above TxFrom | `red-w3.txt` |
| stamps | no: `deleteNodeLocked` stamps `deleteInstantForNodeCascade`, update stamps `nodeVersionUpdateInstant` | R5 `DeleteIgnoresT`, R6 `UpdateStamps` | pins t-1/t/NowTx, NodesAsOf, ByLabel TxPin, exact sets | `red-w3.txt` |
| closes | no | R7 `CloseCollision`, R8 `ScheduledCloseAfterT` | ValidTo == t, ValidTo > t on the node | `red-w3.txt` |
| cascade | no | R9 `CascadeOrder` | cascaded rel TxFrom > t, cascaded rel ValidTo >= t, cascaded rel history TxTo >= t; whole delete refused, nothing changed; counterpart all tombstones carry t, exact node+rel sets at t-1 and t; tiered cross-shard; foreign stub (sharded) not read for history | `red-w3.txt` |
| dupes/race | no | R10 `Duplicates`, R11 `RaceClock` | second delete, same t twice; plain Updates racing the doors (-race) | `red-w3.txt` |
| GraphTx | no | R12 `TxRollbackEquiv` | rollback restores export bytes and pins; commit equals the standalone door's bytes | `red-w3.txt` |
| cache | partly (W2: replica delete apply, notePastDatedWrite) | R13 `ReplicaDropsNode`, R14 `PrimaryCacheStale` | replica warm P >= t drops the node after the apply; primary warm pin + build between gate and write | `red-w3.txt` |

Proof: `r0-before-w3.txt` (72 R0 subtests green on 64b04c2), `red-w3.txt` (284 subtests red against the
stubs, every failure behavioural), `green-w3.txt` (356 subtests green under -race), `make test-race` green.

## What landed

- Doors: `Nodes().DeleteWithTx/UpdateWithTx` (`nodes.Ops` + spy), `GraphTx.DeleteNodeWithTx/UpdateNodeWithTx`
  (`tx_mutations_node_withtx.go`; bodies shared with the plain twins via `deleteNodeAt`/`updateNodeAt`),
  `BatchBuilder.DeleteNodeWithTx/UpdateNodeWithTx`, `Session.DeleteNodeWithTx/UpdateNodeWithTx` (ingest sync and
  concurrent). Batch/ingest gate at queue time; the order/close/no-op rules run at apply under the entity locks
  (after the W4 merge: in the shared whole-unit pre-flight, see below — a refusal refuses the unit). The GraphTx twins gate before the tx lock (value, then privilege), as the standalone
  doors gate before c.mu.
- Seams: `deleteNodeInternal(ctx, id, at)` -> `deleteNodeLocked(..., at)`; `updateNodeAtInternal(ctx, id, m, at)`
  and `updateTemporal.txAt` consumed by `updateNodePreparedInternal`. at == 0 = today (R0 green before and after).
- Cascade: `checkNodeCascadeCallerTx` (tx_order.go) — order rule on the node (`nodeTxDeleteStart`) and every
  Phase-B rel (`relTxDeleteStart`, own chain), then `checkCallerDeleteCloses` over all rows, all before the
  first write; one instant for every tombstone. Foreign incoming stubs (ADR-0010, slot not local) are skipped
  (no local chain, no tombstone). No store change: memory/badger/sharded/tiered all write the core's tombstones
  (tiered cross-shard Ref<->Ev and shard-local Ref->Ref both covered in R9's counterpart).
- Cache: standalone/GraphTx doors `defer notePastDatedWrite(at)` after the gate; batch Execute and the concurrent
  ingest apply report `pendingNodeCallerTx(nodeUpdates, nodeDeletes)` (a separate defer line beside W2's
  `pendingPastDated`, to keep the W4 merge apart).
- `nodeDeletes` became `[]pendingNodeDelete{id, at}` in BatchBuilder and ingestGroup (W4 may change the adjacent
  `relDeletes` line: a trivial textual conflict).

## Mutation checks (each reverted after the run)

| Mutation | Caught by |
|---|---|
| cascade check skips the rels (`range rels[:0]`) | R9 CascadeOrder, R7 CloseCollision/cascaded_rel, R8 ScheduledCloseAfterT/cascaded_rel |
| foreign stub not skipped | CascadeForeignStub (`entity slot not local ... slot 11`) |
| batch/ingest-sync apply does not report the node instants | R14 PrimaryCacheStale batch/* and ingest_sync/* on all 4 backends |

## Test-fixture corrections after the first implementation run (not code bugs)

- R2 `clock+1` for the GraphTx twin: BeginTx reserves one instant (StartInstant), so the twin's clock read is
  x+1; the first future instant is x+2 for that door.
- R13 replica fixture: the cascaded rel needed an explicit valid-from — with a derived one (mint time = now) the
  cascade rightly refused t (R9 behaviour).
- Self-loops are rejected by default validation; R9's counterpart uses a second inbound rel and a Ref->Ref rel.

## After merging main (W4)

- One shared whole-unit pre-flight: W4's `precheckRelCallerTxOps`/`hasRelCallerTx` became
  `precheckCallerTxOps(callerTxUnit)` / `callerTxUnit.hasCallerTx()` (`batch_callertx_preflight.go`), called
  from Batch.Execute and the concurrent ingest apply, and used by the strong-mode isolation in
  `applyCommitGroup`. It checks node and rel caller-instant ops together, before any write: node update
  (`checkNodeCallerUpdate`, extracted from the seam like W4's `checkRelCallerUpdate`), node delete
  (`precheckNodeCascade` -> `checkNodeCascadeCallerTx` under LockMany on node + cascaded rels), rel ops (W4's).
- One-op-per-entity, extended: node ops count node creates/updates/deletes/cascades; rel ops now also count the
  rels every node delete of the unit (plain or caller-instant) cascades; a caller-instant node delete also
  refuses a rel create ending at the node and any other op on one of its cascaded rels.
- Batch/ingest `UpdateNodeWithTx` refuses an empty map at queue time (as W4's rel twin).
- Red: `red-w3-preflight.txt` (the node unit tests against the rel-only pre-flight behaviour). Mutations, each
  red then restored: node-delete cascades not counted; rel-create endpoint not counted; other ops on a cascaded
  rel ignored; node one-op rule off.
- Green: `green-w3.txt` (658 subtests incl. W1/W2/W4 tests, -race); `make test-race` on the merged tree.

## Open for phase 3

- docs/api.md (node doors, batch/ingest node doors), CHANGELOG, lesson 59 amendment (W5).
- R15 cross-backend oracle with backdated node ends; R11 over the GraphTx/batch/ingest node doors (only the
  standalone node doors race here).
- Replay note (as W1): an UpdateWithTx without `tkg_valid_from` resets ValidFrom; a later DeleteWithTx at a t
  before the derived start refuses. A cascade also refuses when ANY cascaded rel's derived valid-from (its mint
  time) lies after t — replays must pass `tkg_valid_from` on rel creates too.
