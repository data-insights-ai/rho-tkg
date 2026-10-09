# Bulk as-of presence check - ledger line (written before the first build)

Request: NodesAsOf/RelsAsOf read key current+1 for every entity (snapshotHistoryKeyExists) - measured +0.6 us/entity
without history (53 -> 65 ms / 20 K), +1.2 us with one history row. Point doors already gate on HasNodeHistory.

In code? No. badgerstore_txtime.go:669/683 (nodeAsOfInTxn/relAsOfInTxn) use snapshotHistoryKeyExists only.

Decision (a): a sound lock order exists. A presence build takes buildMu -> wbMu only (lock order flushMu -> idxMu ->
buildMu -> wbMu is not inverted), but building under the scan's idxMu.RLock would hold writers off for a whole
history-keyspace scan, so the bulk door builds BEFORE taking idxMu (lazy-build entry), then re-checks `built` under the
RLock (Clear resets under idxMu.Lock, so `built` cannot flip during the hold). It never builds under the lock; if the set
is not built it falls back to the key probe.

Soundness of a live-set negative against a start-of-scan snapshot: the scan's view (overlay + txn) is older than the live
set. A negative is trusted only if (1) the set was built when the scan began, (2) the ID is in neither `has` nor
`unknown`, (3) no history DELETE was noted since just before the overlay capture (new counter `deletes`, read before the
overlay and re-read after the lookup). Without a delete the set only grows after the snapshot, so a live negative implies
a snapshot negative. Unknown IDs and any delete since scan start fall back to the snapshot key probe (as today).
Probe-only mode (Config.HistoryPresenceProbeOnly): no set, probe as today.

Tests (red first, then code, then green):
 T1 benchmarks BenchmarkNodesAsOfBulk/RelsAsOfBulk (20 K, none / one row): numbers before, after.
 T2 randomized equivalence with pending / flushed / deleted / truncated (unknown) / cleared / reopened states;
    bulk == per-entity point door == SelectAsOf oracle.
    break-the-code mutants: M1 unknown treated as no-history, M2 delete-since-scan-start ignored, M3 presence consulted
    without the built guard (set unbuilt -> everything "no history"), M4 node/rel kind swapped.
 T3 delete mid-scan: bulkAsOfScanTestHook deletes an unvisited entity's history; result must equal the scan-start
    snapshot (M2 red).
 T4 -race: writers add history while NodesAsOf / RelsAsOf run.
Proof: evidence files in this directory.
