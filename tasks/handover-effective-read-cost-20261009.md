# Handover: History reads cost a value prefetch, and a delete after a bounded cascade leaves the entity readable

Date 2026-10-09. From sigma-tkgd stream "effective" (pins v4.43.0). Verified at main `dc6aa5b` with throwaway tests in a scratch copy
(memory, badger on disk and reopened, tiered; not committed). Nothing here is implemented. Tests first, run red (AGENTS.md rules 15-17).

| # | sigma claim | verdict | action |
|---|---|---|---|
| 1 | History on badger ~14 us per entity once history exists | confirmed; cause is a missing iterator bound | 1a now, 1b/1c next |
| 2 | Delete after a cascade leaves the cascade row readable by RelAsOf | confirmed for BOUNDED cascades; valid-time reads resurrect the rel too | bug, fix 2a |
| 3 | one-tick rows `[v, v+1)` read as eclipsed | true at v4.43.0; fixed on main by `30cfea0` (unreleased) | release, add pins |

## 1. History cost on badger
Path: `Rels().History` -> `getRelHistory` (core/store_fetch.go:157) -> `GetRelHistory` (store/badger/badgerstore_history_rel.go:319) ->
`getRelHistoryByPrefix` (:346). `RelAtTx` gets there only when `relCurrentAnswersAt` declines (core/temporal_queries.go:475, 653) and the skeleton
path (`badgerstore_temporalmeta.go:40`, PrefetchValues=false) declines. Cause: the iterator uses `DefaultIteratorOptions` + `PrefetchValues = true` and
no `opts.Prefix` (:357-358; mirrors `_history_rel.go:464-465`, `_history_node.go:765-766`, `:877-878`). Badger prefetches up to 100 items after the Seek
whether or not they share the prefix, so a lookup for an entity WITHOUT history reads values of other entities' history. Evidence: with only
`opts.Prefix` added the cost is 1.7-1.8 us at every density; with prefetch off and no prefix it is flat 2.1-2.2 us. Also per call:
`pendingHistoryVersionOverlay` walks the write buffer (badgerstore_history.go:345). Row read alone: 0.15 us hot, 5.4-6.1 us cold.

| badger, reopened (store-level 200 K rels; core 50 K rels) | today | target |
|---|---|---|
| `GetRelHistory`, plain rel, no history in the graph / history on 0.1 % of rels | 5.9 / 57 us | <= 2 us, flat |
| `Rels().History`, plain rel, 1 % of rels cascaded | 195 us | <= 2.5 us |
| `RelAtTx` plain rel, hot / cold row (1 % cascaded) | 2.0 / 9.5 us (3.5 / 18.9) | < 1 us / cold row + 1 us |
| `RelAtTx` cascaded rel (skeleton path) | 18.5 us | <= 6 us |
| `HasHistory(id)` (new) | - | <= 0.1 us, 0 allocs |

- **1a, now, own commit:** `opts.Prefix = prefix` at the four sites (node and rel mirrors). 1 M entities x 1.8 us = 1.8 s per scan: 8x better, not enough.
- **1b (done, Unreleased: `Nodes()/Rels().HasHistory`, `store.HistoryPresenceCapability`; tests and mutants in `tasks/evidence/has-history/`):** per-entity presence without a row-format change. Badger keeps a RAM set of IDs with history rows, updated where every history key already
  passes (`noteHistoryKey`, badgerstore_history_count.go:51, under wbMu), built lazily once by the key-only `ForEachRelHistoryID` (history_rel.go:603;
  200 IDs took 0.2-0.5 ms). RAM is O(IDs with history); the belief-watermark sidecar is O(N) plus a value scan on first use (79 ms at 50 K rels).
  Surface `Rels().HasHistory(id)` / `Nodes()`; memory is `len(relHistory[id]) > 0`; tiered/sharded route by shard.
- **1c (done, Unreleased: `Temporal().Node/RelEffectiveTimeline` and the scan forms of backlog 27; tests, mutants and benchmarks in `tasks/evidence/effective-timeline/`):** `Temporal().RelEffectiveTimeline(id, pin)` returns `[from, to, row]` segments in valid-time order: sigma's per-segment loop in one call. Plain
  entity: the current row after 1b. Else skeletons + the cut-and-resolve of `correctionCuts` / `relCorrectionSegments` (temporal_cascade.go:384, 446),
  decode winners only. `RelAtTx` already is the point form of "effective at a pin"; no new point door.

## 2. Delete after a bounded cascade
Repro (memory, badger, tiered; node and rel): Add vf=1000; `SetRelVersionInterval(id, 2000, 3000, patch)`; `Rels().Delete(id)`. Chain: v0 `[1000,0)`
genesis stays open, v1 resumption `[3000,0)` holds the current slot, v2 piece `[2000,3000)` is history. Delete stamps only the current row
(core/relationship_delete.go:151, :171 `DeleteRelWithHistory(id, current.Version(), ...)`; nodes node_delete.go:421, :444). After it:
`RelAsOf(far)` = v2 and `RelsAsOf(far)` lists it (want `ErrNoVersionAsOf`); `RelAt(far)`, `RelAtTx(far, pin after delete)`, `RelsAt(far)` = v0, so the
deleted rel is valid forever. The open-ended cascade `[2000,0)` is correct and is the only shape `asof_cascade_delete_test.go` covers. Before the
delete `RelAsOf(now)` = v1 (`txTimeMatchesCurrent`, badgerstore_txtime.go:38), after it v2: the arms disagree on "the record at a pin".
Cause: `SelectAsOf` takes the highest VERSION (internal/storeutil/asof_select.go:42, :66) and badger's reverse scan does the same
(badgerstore_txtime.go:127, :405), but a bounded cascade puts a history row above the current slot. The point resolver caps only the stamped row, and
the append-only cascade never closes v0 (temporal_cascade.go header). **Bug** against `docs/architecture.md:988` ("any pin at or after the delete
excludes it"), SelectAsOf's own doc and lesson 62, which assumed the delete hits the final version. Past valid time stays readable (as for a plain delete).
- **2a, recommended (read seam, repairs existing stores, no wire change):** a tombstone (`DeletedAt != 0`) ends its life: an as-of pin >= DeletedAt is
  absent; a valid-time probe >= the tombstone's clamped ValidTo finds no version. Lands in `SelectAsOf`, `resolveRelChain`/`resolveNodeChain`
  (chain_resolver.go:187), badger `RelAsOf`/`NodeAsOf` (a deleted entity reads its skeletons once, ~2-3 us; live ones skip) and the store predicates
  (rule 17: `MatchesPointInTime`, ByType with QueryOpts, column/segment scans). Hazard: re-import of a deleted ID is allowed (relationship_import.go:111;
  backlog "Re-import of a deleted ID"), so the rule is per life: rows with TxFrom > DeletedAt are the next life. Same root as backlog 18 (cascade rows
  take `maxVersion+1` above the current slot, temporal_cascade.go:119-128): design 2a together with 18's single version allocator.
- **2b, not before v5:** delete appends closing rows; the change-log record and replica apply (apply_record.go:671) must carry them; old data stays wrong.

**3. One-tick rows.** Fixed on main by `30cfea0` (CHANGELOG [Unreleased]). At v4.43.0 `SetRelVersionInterval` over `[v, v+1)` fails ("cascade requires at least one
non-eclipsed version") and RelAt/RelsAt miss the row on all three backends; on main all pass. Ship in the next tag and tell sigma.

## Acceptance tests (first, red output kept; memory, badger, tiered, sharded; node and rel)
1. `TestAsOfBoundedCascadeThenDelete`: create vf=1000 (t1), cascade `[2000,3000)` (t2), then Delete. At t1 v0, at t2 the current slot, after: absent from
   `*AsOf`/`*sAsOf` with a bystander entity (exact set). Break cases: cascade at the genesis vf, two nested bounded cascades, a gap piece before the
   first vf, delete via tx, batch, ingest and `DeleteWithTx`, re-import after delete (absent between, present after, old life hidden).
2. `TestValidTimeAfterDeleteBoundedCascade`: `RelAt`, `RelAtTx`, `RelsAt`, `Snapshot`, `OutgoingRelsAt`, ByType(ValidAt) agree: absent at valid >= delete
   instant; v0 at 1500, v2 at 2500 with a pin after it; pins before the delete unchanged. Add Delete to the oracle's bounded-cascade generator.
3. `HasHistory` vs `len(History(id)) > 0` over randomized create/update/cascade/delete/truncate/compaction/retention-purge/Clear/reopen/rollback;
   `GetRelHistory(x)` returns exactly x's rows while neighbours hold history. 4. One-tick: cascade over a born-instant row (`tkg_valid_to = vf+1`) and
   re-cascade over a one-tick piece (`one_tick_span_test.go` lacks both).
Benchmarks (ReportAllocs, into bench-gate): `BenchmarkRelHistory/density={0,0.1%,1%}`, `BenchmarkRelAtTx/{plain,cascaded}/{hot,cold}`,
`BenchmarkRelHasHistory`, `BenchmarkRelEffectiveTimeline`; targets above (profile `RelAtTx` first: 0.15 of its 2.0 us is the row read).

Note 2026-10-10: sigma's cut check and late-belief detection no longer need a History read per entity: backlog 30 added
`Nodes()/Rels().LatestStamps(id)` (31-83 ns, 0 allocs at 0-10,000 versions; CHANGELOG [Unreleased] Added).
