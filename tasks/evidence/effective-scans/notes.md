# Backlog 35 — broader effective scans (ledger)

Started 2026-10-12 on branch worktree-agent-ae135f99f073391a4 (base 91684e54).

## In code before the change?
- Per-entity doors and the two scan forms only: `pkg/graph/internal/core/effective_timeline.go:317,344`
  (`NodeEffectiveTimeline` / `RelEffectiveTimeline`), `:631` `ForEachNodeEffectiveByLabel`, `:687`
  `ForEachRelEffectiveByType`. No label/type-free scan, no batched by-IDs, no seeded adjacency form.
- Adjacency candidates of the pinned doors (`OutgoingForNodesAtPin`, `queries.go:1566`) use
  `forEachRelAdjacencyCandidateID` (current adjacency + DELETED rel IDs, "endpoints are immutable").
  A rel ID deleted and re-imported with other endpoints (`relationship_update.go:137`) keeps its
  earlier life's endpoints in history: the deleted-only fold misses that life. The new door must not.

## Plan (signatures confirmed by sigma 2026-10-10)
1. `ForEachNodeEffective(pin, fn)` / `ForEachRelEffective(pin, fn)`
2. `ForEachRelEffectiveAtNodes(nodeIDs, dir, relTypes, pin, fn)` (`temporal.Direction`: Outgoing / Incoming)
3. `NodesEffectiveByIDs(ids, pin, fn)` / `RelsEffectiveByIDs(ids, pin, fn)`
4. `ForEachNodeEffectiveByLabels(labels, pin, fn)`
Shared seam: one streaming body (`streamNodeTimelines` / `streamRelTimelines`) used by the two
existing scan forms and the new doors; candidate gathers reuse `forEachNodeCandidateIDByDepth` /
`forEachRelCandidateIDByDepth` (current ∪ history) and `nodeLabelCandidatesAt`.

## Red tests (written before the code; doors stubbed to return nothing)
- `TestEffectiveScans_MatchPerEntityTimeline` (oracle chains, memory/badger/sharded/tiered, before
  and after rollback + re-import + compaction): for every door and pin, the streamed set equals the
  per-entity timeline filtered by the door's predicate (exact set, each entity once, contiguous,
  by-IDs in input order); coverage counters: deleted-before-pin, deleted-after-pin,
  created-after-pin, closed, live, rel with both endpoints seeded, rel listed only through an
  earlier life's endpoint; fn false stops after one segment.
  Break-the-code: drops deleted-before-pin, lists created-after-pin, no dedupe (history ∪ current,
  duplicate IDs / seeds, several labels), unknown ID errors, deleted-only adjacency fold, whole
  timeline instead of the seeded segments, direction ignored, type filter ignored, fn stop ignored.
- `TestEffectiveScans_Errors` (memory): nil fn, pin 0, future pin, closed graph, invalid ID in a
  batch (no fn call), invalid direction, invalid label / type, scan doors below the compaction
  watermark, scan doors below a retention watermark of a label no node carries. Break-the-code: a
  validation skipped, the scan pin gate replaced by the point gate.
- `TestEffectiveScanWrappers` (temporal package): the six new wrappers forward verbatim, nil API refused.

## Proof files
red-*.txt, green-*.txt, mutants.txt, bench.txt in this directory.

## Result (2026-10-12)
- Red: red-1-core-doors-stubbed.txt (both core tests fail on stubs), red-2-wrappers-stubbed.txt.
- Green: green-1-core.txt (new + existing effective-timeline tests), green-2-wrappers.txt,
  green-3-full-short-suite.txt (`go test -short ./...`), green-4-race.txt (-race -short core, temporal, graph).
- Mutants: mutants.txt, 17 of 17 killed; each test kills at least one mutant no other test kills
  (Errors: M12-M14, wrappers: M15, oracle: the rest).
- Bench: bench.txt (host shared with other agents; -count=3). Per relationship: memory 0.5-1.4 µs, badger
  3.9-10.7 µs; one RelEffectiveTimeline call 0.5-1.5 µs / 5.2-5.6 µs (plain). The scans stream through the
  per-entity body: no per-call validation, one gather per call.
- Findings filed: backlog 49 (tiered re-import moving shards loses the earlier life's history,
  finding-tiered-reimport-*), backlog 50 (pinned adjacency doors miss a re-imported rel's earlier life,
  finding-pinned-adjacency-*).

## Review round (2026-10-12)
1. Cost model: `BenchmarkEffectiveScansOneSeed` (bench-one-seed-full-history.txt): one seed, 50 rels, 100 K rels
   all with history: at-nodes 0.48 s memory / 3.4 s badger vs by-ids 0.13 ms / 0.6 ms. CHANGELOG/api.md state the
   O(H)-per-call model; the cheap sound gather is written into backlog 50 (follow-up, not built).
2. Empty seeds return before the gather (no behaviour change: the answer was already empty).
3. ByLabels folds node history once for several labels on stores without the K1 sidecar; one label skips the
   dedupe map. No behaviour change; mutants-review.txt: M18 (no history fold) and M19 (no dedupe) killed.
4. TestEffectiveTimeline_ConcurrentWriters runs every scan door (guard: passes before and after; -race green-6).
5. stability.md: sub-API `Ops` interfaces are wiring seams. 6. Backlog 49 reworded (unreachable, not lost).
Gates: green-5 (go test -short ./...), green-6 (-race core/temporal/graph), lint-docker 0 issues, security-docker clean.

## After the rebase onto main (33/42, 48 merged)
- ForEachNodeEffectiveByLabel now delegates to the several-labels body, which still read rows in its gather:
  TestEffectiveScanReadsEachRowOnce went red for `nodes` (red-3-reads-each-row-once-after-rebase.txt), as did the new
  `nodes by labels` case. Fix: the label gathers use 33's ID-only `nodeLabelCandidateIDsAt` / the MemberIDs index
  (`nodeLabelCurrentIDs`); `all rels`, `nodes by ids`, `rels by ids` were green before (guards). green-7.
- ForEachRelEffectiveByType keeps 33's `relTypeCandidateIDsAt` with the streamRelTimelines tail.
- ForEachRelEffectiveAtNodes reads the seeds' adjacency rows (no ID-only adjacency read exists); tied to backlog 50.
- bench-after-rebase.txt (shared host, noisy).
