# todo — pointer

The one plan for v4 and v5 (user requests, rules, work packages WP-01..WP-26, waves, deferrals, release gates, decisions, review log) is `tasks/todo.md` on `main` (René 2026-10-16: "i want one consistent todo.md plan"; lesson 84). This branch carries no second plan; PLAN phases and the V2 A-F queue stay in `tasks/backlog.md` as the acceptance reference.

## WP-35 ledger (branch wp-35-compat-tx-stats, v5compat)
1. Drop side ledger (clock.go ledger), `~tf/~dt`, per-instant commits -> tests `TestOneCommitPerTx`, `TestBackfillPinReads` (stamps 1000/2000, pins 999/1999/2000, TxFrom==1000, no `~tf/~dt` in raw v5 props, no ledger file, future stamp refused). In code today: clock.go:36-140 ledger, write.go:72 instant, write.go:637 run. Break-the-code: pin below first stamp, pin == stamp, stamp above wall clock, two stamps in one Tx -> one revision.
2. GraphTx.Begin/Commit/Rollback/CommittedLSN -> `TestTxBeginCommit`, `TestTxBeginRollback` (double Commit = ErrTxDone, rollback leaves CommittedTx). tx.go:44-60 declined today.
3. Stats counts == lookup -> `TestCountsEqualLookup` (delete, retract, rels, empty graph). declined today: stats/stats.go.
4. Constraints.CreateUnique over Graph.DeclareUnique (WP-34 present at 6afdca90) -> `TestCreateUnique` (violation, duplicates refused, twice = exists, reopen keeps).
5. Acceptance: v5compat `go test ./...` + tools/v5-conformance `go test ./...` (compat profile) 0 fail.
Proof (all ticked): red run kept in scratchpad red35.txt (5 tests red); after: v5compat `go test -race ./...` ok, tools/v5-conformance `go test -count=1 ./...` ok (compat profile 0 fail), go.mod untouched.
Review: (a) one Tx = one commit, so an entity written at two caller stamps in one Tx is known as of the later stamp (engine granularity is per entity per revision); suite case GraphTxTwinsApplyAtCallerInstants relaxed for graphs implementing SingleCommitTx() (compat adapter only). (b) the writer mutex g.mu stays: it only orders the commit-clock prediction (engine Update already serialises); the GetOrCreate-specific lock is gone. (c) CreateUnique is per key over all labels, kinds int/string/bool. (d) tf of listed rows costs one Timeline read per returned entity.
