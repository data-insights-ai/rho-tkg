# Ledger: history iterators without opts.Prefix (backlog 20, handover 1a)

Written before the first code change. tasks/todo.md and tasks/backlog.md belong to main; this is the worktree-local ledger.

Request: set `opts.Prefix = prefix` at the 4 PrefetchValues=true history sites; audit the iterator family; benchmark before/after.

| step | in code? | red test | break-the-code cases | proof |
|---|---|---|---|---|
| prefix in getRelHistoryByPrefix | history_rel.go:357 no Prefix | TestHistoryScanAllocGate (rel) | mutation: drop the Prefix line again | red-*.txt / green-*.txt |
| prefix in relHistoryVersionsFromPrefix | history_rel.go:464 (serves RelHistoryVersionsFrom, the paged door) | same gate, paged | mutation | same |
| prefix in getNodeHistoryByPrefix | history_node.go:765 | same, node | mutation | same |
| prefix in nodeHistoryVersionsFromPrefix | history_node.go:877 (NodeHistoryVersionsFrom) | same, paged | mutation | same |
| behaviour (may pass today) | n/a | TestHistoryPrefix_* in badgerstore_history_prefix_test.go | neighbours lower/higher/last-byte/high bytes; none; out-of-order puts; pending overlay; truncate (overlay delete); tombstone; reopen | green before and after |
| bench | n/a | BenchmarkRelHistory / BenchmarkNodeHistory density 0, 0.1%, 1% | n/a | bench-before.txt, bench-after.txt |
