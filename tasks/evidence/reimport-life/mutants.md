# Backlog 38 — mutants (one real edit each, run against the new tests, then reverted)

Command per mutant: `go test ./pkg/graph/internal/core/ -run 'TestReImport|TestAsOfBackfilledReImportOfDeletedID|TestAtTxReImportOverlapsEarlierLife' -count=1 -short`
(M4 also `go test ./pkg/graph/ -run TestReImportContinuesTheChainFacade`).

| # | Edit | Red tests |
|---|---|---|
| M1 | allocator restarts at 0 (`lifeStartOf`: `version = 0`) | KeepsEveryLife, AboveCascadeRows, BackfillMustFollowTheChain, AfterDeleteAheadOfClock, TxRollbackRestoresEarlierLife, ReplicaApply, ExportImportRoundTrip, CompactionAcrossLives, LifeOracle (9 of 12) |
| M2 | allocator takes one above the last current row (the tombstone) only, not the chain's top | AboveCascadeRows |
| M3 | write order ignores the tombstone (`chainWriteOrder` = version order) | OldChainReadsAsBefore (overlap shape) |
| M3b | life ends ignore the tombstone (`chainLifeEnds` = nil) | KeepsEveryLife |
| M4 | refusal off by one (`t < maxStamp`) | BackfillMustFollowTheChain, BackfillRefusalLeavesNoToken, LifeOracle; facade ReImportContinuesTheChainFacade |
| M5 | plain door does not raise the clock floor | AfterDeleteAheadOfClock |
| M6 | re-import links no predecessor (`PrevHash = ""`) | KeepsEveryLife, AboveCascadeRows, BackfillMustFollowTheChain, AfterDeleteAheadOfClock, ReplicaApply, ExportImportRoundTrip, CompactionAcrossLives, LifeOracle |
| M7 | GraphTx rollback truncates the created ID's whole history (`from = 0`) | TxRollbackRestoresEarlierLife (8 of 8: trim branch on memory/badger, copy branch on sharded/tiered) |

Raw per-mutant lines: `mutants-raw.txt`.

Note: M3 survived the first cut of the suite. With continued versions a new chain's
(life, version) order IS version order, so `TestAtTxReImportOverlapsEarlierLife` no longer
exercises the interleaving it was written for; the old-chain guard
(`TestReImportOldChainReadsAsBefore`) got the overlap shape as a v4.43–v4.47 stored chain
(re-import at version 0 written straight to the store) and now kills M3.
