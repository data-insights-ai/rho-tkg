# W4 notes — relationship caller-instant doors in GraphTx, Batch and ingest

Ledger (written before the first test or code edit):

| Step | In code before W4? | Red test | Break-the-code cases | Proof |
|---|---|---|---|---|
| GraphTx `DeleteRelationshipWithTx` / `UpdateRelationshipWithTx` | no: `tx_mutations.go` DeleteRelationship passes `at = 0` (~:448), UpdateRelationship has no instant (~:224) | `TestTxBackfillRel_GateOff`, `_InvalidInstant`, `_OrderEqualReversed`, `_OrderValidStart`, `_DoorsReportPastDatedWrite` (door table extended), `TestTxBackfillRelW4_CloseCollision`, `_ScheduledCloseAfterT`, `_TxRollbackEquiv` | gate off, t 0/-1/clock+1/MaxInt64, t = TxFrom / TxFrom-1 / below a history TxTo, t <= ValidFrom / UpdatedAt, close at t, close after t, rollback, commit vs standalone bytes | red-w4.txt, green-w4.txt |
| Batch `DeleteRelationshipWithTx` / `UpdateRelationshipWithTx` | no: `batch_execute.go` steps 4/5 carry no instant, `relDeletes` is `[]RelID` | same door table + `TestTxBackfillRelW4_BatchIngestDoors` | as above + mixed batch with one order failure (whole batch refused, skeleton TxFrom 0), two ops on one rel, no-op update | red-w4.txt, green-w4.txt |
| Ingest `Session.{Delete,Update}RelationshipWithTx` (strong + concurrent) | no: `ingest.go` Session has plain rel doors only; `ingest_concurrent.go` delete passes `at = 0` | same door table + `_BatchIngestDoors` (strong, concurrent, coalesced strong groups) | as above + a refused group coalesced with a good group (the good group must commit) | red-w4.txt, green-w4.txt |
