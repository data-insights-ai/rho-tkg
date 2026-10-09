# Handover: a pinned property lookup costs the size of the history, not the number of matches

Date 2026-10-09. From the sigma-tkgd realtime ingest work (stream B, branch `impl/pushdown`).
Code is cited at v4.43.0 (`46f63fa`); the files cited are unchanged at HEAD `c688821` except `core.go`. Nothing here is
implemented. Tests first, run red, then code (AGENTS.md rules 15-17).

## 1. Finding

sigma, 1 M relationships (200 k revised, 50 k deleted after insert), one seat-scoped
`Rels().ByTypeAndProperty(..., QueryOpts{TxPin})`: 1.2-1.5 s against 11 ms with no churn, 13-22 % faster than a
full type scan, equal answers (sigma's numbers, not re-measured here). Reproduced in shape with a throwaway
test (`go test -overlay`, not committed): 100 k rels of one indexed type, 500 values, 200 matches.

| backend | pinned, no churn | pinned, 20 % revised + 5 % deleted | other type churned, this type untouched |
|---|---|---|---|
| memory | 0.10 ms | 20.7 ms | 0.08 -> 13.6 ms (25 k of 50 k revised in the other type) |
| badger (in-memory) | 5.7 ms | 199 ms | 1.9 -> 253 ms |

## 2. Cause (verified)

1. With a temporal filter the door takes the current index matches (`internal/core/graph_rel_property_query.go:80`)
   and calls `forEachRelCandidateIDByDepth` (`:98`; door `:33`).
2. That fold seeds a set with them and adds EVERY relationship ID with a history row
   (`internal/core/temporal.go:722-742`, `forEachRelHistoryIDByDepth` `:958`, `:978-983` -> `store.ForEachRelHistoryID`).
   No type token is passed, so the set spans all types: a lookup in an untouched type pays for churn in another.
3. Each candidate then loads its chain: `findRelVersionForOpts` -> `relAsOfLocked` (`temporal.go:1182-1185`).
4. No store can narrow it. Memory ranges over `relHistory` (`store/memory/memorystore.go:83`, `memorystore_query.go:449`).
   Badger scans the `0x08` keys, 1 B + rel ID + version, no type (`internal/storeutil/keys.go:33,45,136`;
   `badgerstore_history_rel.go:603`, `AllRelHistoryIDsFrom :702`). Sharded and tiered fold their shards
   (`sharded/stats_iter.go:213`, `tiered/tieredstore_read_history_rel.go:303-307`).
5. Same fold in the doors of rule 17: `graph_property_query.go:96` and `:200` (`ByLabelAndProperty`, `ByLabelAndProperties`),
   `temporal_queries.go:1030`, `:1108` (nodes At/During), `:1229`, `:1619` (`RelsByTypePropertyAt`/`During`).
6. No test pins this door semantically: `rels/api_test.go:766` is a forwarding spy.

## 3. What exists

| structure | content | helps? |
|---|---|---|
| property index (memory `memorystore.go:90`; badger RAM `badgerstore_rel_index.go:322-335`; sharded fans out; tiered declines, `tieredstore_write.go:180`) | value key -> rel IDs of the CURRENT version only (replace drops the old value, `memorystore_rel.go:269-277`; delete `:341`; `memorystore_history.go:969-978`) | seeds the fold only |
| history (`relHistory`, `0x08` keys) | rel ID -> versions; no type, no value | it is the folded set |
| `typeIdx` (`memorystore.go:75`, current only); K1 `relTypeTxMembers` (`:237`; badger `badgerstore_labeltxmembers.go:392`; `store/capabilities.go:361-367`) | K1: type -> every rel that ever had it + first TxFrom | `ByType{TxPin}` uses K1 (`queries.go:593-597`); this door does not; K1 alone = the type scan |
| column segments (ADR-0011); valid-time envelope (`memorystore_reltype_temporal_candidate.go`) | sealed CURRENT rows, history stays in the row store; valid-time envelope per rel | no; no (not for `TxPin`) |

## 4. Options

| option | pinned lookup cost | build cost, risk | verdict |
|---|---|---|---|
| A. type-scoped history-ID set, or filter the fold by type | matches + history of the type | S; badger keys carry no type | no: a dominant type keeps the cost |
| B. K1 candidates, as `ByType` does | type size | XS | no: the type scan; fallback for an unindexed (type,key) |
| C. exact version index, postings (id, TxFrom, TxTo), range at the pin | matches | L; must reproduce `SelectAsOf` (lesson 62) incl. non-monotone TxFrom after cascades; truncate, compaction, purge, replica apply, `AddWithTx`, planned `UpdateWithTx` edit intervals | no: a second as-of rule |
| D. K1-style sound superset: (type,key,valueKey) -> {rel ID: first TxFrom} over every version that carried the value; append-only; resolver stays the authority | rels that ever carried the value x their versions | M; template `memorystore_labeltxmembers.go` (205 lines), badger (419), tests (541) | recommended |

## 5. Recommendation: D

* Optional capability `RelPropertyTxMembershipCapability.ForEachRelPropertyTxMember(typeTok, key, valueKey, fn(id, firstTx) bool)`
  beside `capabilities.go:361-367`, wired like `core.go:1391`. Absent -> today's fold, same answers (tiered, wrappers).
* Only for (type,key) with a declared rel property index; lazy on the first temporal lookup; maintained at every `AddRelToPropertyIndexes`
  call site and history-row insert (K1's `recordRelTypeMemberLocked` sites plus the replace doors).
* Core: members U currentIDs, drop firstTx > `effectiveTxPin` (`temporal.go:1020-1025`), then the same `findRelVersionForOpts` + `pred`;
  valid-time filters use the same candidates. Node twin keyed (key,valueKey); composite = intersection.
* Open: RAM is one posting per (rel, distinct value ever) per indexed key, about (1 + revision rate) times today's index; measure bytes per
  posting at 1 M and 10 M first. The first lookup after reopen is an O(history) build (K1 precedent): eager or lazy for sigma?

## 6. Tests first (two-phase, exact sets): memory, memory with sealed `RelSegments`, badger, sharded, tiered, a wrapper; each with and without the index

* T1 `TestPinnedByTypeAndPropertyEqualsPinnedScan`: A value 1 -> 2 -> 1; B created 2, deleted; C 1 -> 3, then deleted; D 1, untouched; E created after
  the first pin; G carried 1 only on v0; H backfilled by `AddWithTx` between stamps; X of another type, same values. Pins: before the first write, every
  TxFrom/TxTo/DeletedAt and +-1, between, after all, far future. Assert the literal ID+version set per pin AND equality with `RelsAsOf(p)` and
  `ByType{TxPin:p}` filtered. Break the code: resolve at current, drop deleted rels, record only the current value, prune off by one (`firstTx == pin` stays).
* T2 doors agree: `RelsByTypePropertyAt`/`During` vs `ByTypeAndProperty{ValidAt|ValidStart,ValidEnd|TxAt}`; node twins, `ByLabelAndProperties`.
* T3 randomized probe in `bitemporaloracle_test.go` (add, update, delete, close, cascade, `AddWithTx`): indexed vs unindexed vs oracle.
* T4 lifecycle: reopen, `Clear`, `TruncateRelHistory`, `CompactHistoryRels` (pin below the watermark still `ErrHistoryCompacted`), retention purge,
  replica `ApplyChange`, read before the write buffer flushes (lessons 64/74), index created after history exists, dropped and re-created.
* T5 structural: chain loads for one value equal its ever-members and stay put when churn on other types and values grows 10x.

Benchmark `BenchmarkPinnedRelPropertyLookup` in `bench/pinned_scan_test.go` (K1's fixture style): 1 and 5 types, 100 k and 1 M, sigma profile, 200 matches
and broad, memory/badger/sharded, in `bench-gate`. Target: churned pinned lookup <= 2x the no-churn one at the same N (sigma 1 M: <= 22 ms vs 11 ms; today
200x memory, 35x badger at 100 k), ns/op and allocs/op flat (+-20 %) as unrelated churn grows 10x; current lookup and ingest writes unchanged (+-5 %).
Order: T1, T5 red -> capability + memory -> badger -> sharded -> core switch -> node twin -> T2-T4 -> bench -> CHANGELOG, `docs/query-planners.md`.
