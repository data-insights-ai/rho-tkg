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
