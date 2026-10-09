# Handover: purge relationships by age, gate retention reads per type (sigma-tkgd temporal overview)

Date 2026-10-09. From sigma-tkgd `tasks/design-temporal-overview.md` §6 (decided 2026-10-09). Cited at main `0622da6` (code identical to `32568c4`,
the DeleteWithTx/UpdateWithTx merge). Nothing implemented. Tests first, run red (AGENTS.md rules 15-17). Probes below were throwaway tests, not committed.
sigma keeps a long-lived overview (nodes `Overview`+`ov_*`, rels `OV_PAIR`), written back once per cut, and lets raw event RELATIONSHIPS age out.

## 1. Claims checked at main
| sigma claim | cited | at main |
|---|---|---|
| only nodes purge | `admin/api.go:152` | holds (door `core/retention_purge.go:90`); no rel purge exists. Doc `admin/api.go:145-151` is stale: tiered and sharded implement it (`tiered/retention_purge.go:9-12`, `sharded/retention_purge.go:12-15`), a change-log gets `ChangeRangePurge` (`core/retention_purge.go:87-89,143-147`) |
| edges go with an endpoint | `retention.go:252-259` | moved: that is `checkRelPointRetention` (`retention.go:251-258`), a comment. Removal: `badger/badgerstore_retention_purge.go:93-224` (cascade `:174`, history `:190-207`), `memory/memorystore_retention_purge.go:85-190`, `tiered/retention_purge.go:69-107`, `sharded/retention_purge.go:50-98` |
| scan gate is graph-wide | `retention.go:191-216` | holds, `:191-218` (compare `:210-216`); reached from `compaction.go:380-390` (call `:389`) by 30 scan-door call sites and `txtime.go:278,369`. Rel point gate is graph-wide too (`retention.go:256-258`; `txtime.go:118`, `temporal_queries.go:660`, `count_at.go:280,290`); node point gate is per label (`:227-249`) |
| effect | | probe: after `PurgeExpiredNodes("Event")`, `Nodes().ByLabel("Overview", TxPin<wm)` = ErrRetentionExpired, `Temporal().NodeAsOf(overview)` passes |
Found on the way (probe, memory and badger): (a) `PurgeExpiredNodes` does not reach ENDED entities: live node purged, the deleted node's `History()` kept 1 row. An ended rel is history-only
(`badger/badgerstore_history_rel.go:142-190`: entity, type and adjacency keys go, one history row stays); `DeleteWithTx` (`rels/api.go:283`) makes them on purpose. (b) `Export`->`Import` carries no watermark:
the copy answered the same low pin with nil (keys appear only in `retention.go`, `metakv_reap_policy.go`). (c) `Temporal().OutgoingRelsAt/IncomingRelsAt/NeighborsAt` have no retention gate (nil below the watermark).

## 2. Item 1: `PurgeExpiredRels` (new admin door, mirror of `PurgeExpiredNodes`)
```go
type RelPurgePolicy struct{ Type string; Mode PurgeMode; Before types.Instant } // Before exclusive; alias in admin/api.go:32-62
func (a *API) PurgeExpiredRels(ctx context.Context, p RelPurgePolicy) (PurgeReport, error) // RelsPurged; Watermark = this type's
```
* Predicate `PurgeByAge`: rel snowflake mint time < Before, live AND ended. `PurgeByValidTo` returns `ErrInvalidPurgePolicy` in this cut (ended rels have no current `ValidTo`; decision 1).
  Like nodes, ByAge ignores liveness: a rel minted < Before and still believed at a later pin is gone from reads at that pin.
* Removes the row, type/adjacency (both legs)/property/temporal index entries, counters, the whole history incl. tombstone; no tombstone left; nodes untouched.
* Same gates and order as `core/retention_purge.go:90-153`: open, ctx, writable, `AllowRetentionPurge`, policy, capability; unknown type = no-op (no token created); watermark FIRST, then the log record, then 256-chunks. Idempotent, resumable.
* Store (new optional interfaces beside `store/capabilities.go:638-669`, never new methods on old ones): `RelRetentionPurgeCapability{PurgeRelsByTypeBefore(typeTok, before, chunk) (RetentionPurgeResult, error)}`,
  `RelRangePurgeLogCapability{LogRelRangePurge(typeTok, before, mode)}`. Candidates = type index UNION deleted IDs (`ForEachDeletedRelID`, `capabilities.go:746`; history keys are ID-ordered, so ByAge stops at the first ID >= Before).
| backend | how |
|---|---|
| memory | one lock; reap `relTypeTxMembers` as `memorystore_retention_purge.go:147-150`; a `RelSegments` type: dead sealed copy via the overlay (`memorystore_segments.go:34-40`) |
| badger | Phase A/B of `purgeNodesByLabel` with `deleteRelByInfo` + `historyTruncateDeleteKeys`; coarse `bumpRelEpoch` (poisons every type, `badgerstore_node.go:376-381`), as `:103-106` |
| sharded | entity and both legs sit on the rel-ID shard (`sharded/rel.go:12-16`): per-shard fan-out through `forEachShardErr`, no sweep; a foreign-end stub is the other machine's purge |
| tiered | entity + out-leg on the start node's shard, in-leg on the end's: per-shard scan, then `sweepRelResidue` (`tiered/retention_purge_drop.go:266`). No O(1) shard drop (`:54`): those shards hold other data |

## 3. Item 2: per-type gate (nodes and rels get separate watermark families)
* New MetaKV `retention_rel_watermark/<relTypeToken>` (max-monotonic) and `retention_rel_max_watermark` + atomic. `retention_max_watermark` keeps meaning "max over node-label purges", so node behaviour is unchanged.
* Gate (pin p; MetaGet only when p is below the fast max, as `retention.go:227-249`): scan of rel type T fails iff p < nodeMax or p < wm(T); rel point read of type T the same (type from the current row, else newest history row);
  all-rels and untyped adjacency scans: p < max(nodeMax, relMax); node scans and node point reads: p < nodeMax, so a rel purge NEVER fails an `Overview` read, and `OV_PAIR` is unaffected by a purge of the event type.
  `validateTemporalQueryOptsScan(opts)` (`compaction.go:380`) gains a scope argument (node label / rel type / all); the 4 `checkRelPointRetention` callers pass the rel.
* Register the keys: `metakv_reap_policy.go:82,100` (`check-metakv-reap`) and `reapRetentionForReset` (`retention.go:164`), looping `1..relTypes.Len()` (tokens are reused after Reset, `retention.go:139-163`).
* Phase 2, only if raw events ever become NODES: per-label gating of node scans is sound only if a purge also raises the watermark of every other label of a purged node and every rel type it cascaded (the store result must report them). Not now (decision 2).

## 4. Interactions
* As-of cache holds NODE columns only (`docvalues_asof_cache.go:37-40`): a rel purge needs no bump and must not make one (sigma's replay reads cached `Overview` columns). Pin it by test.
  The node purge bumps BEFORE it removes anything (`retention.go:76`, removal `retention_purge.go:151`), against the rule `docvalues_asof_cache.go:29-35` (R14); not reproduced, a hook between watermark and chunks proves it.
  Fix with a `defer` bump after `purgeRangeAllChunks` and in `applyRangePurgeLocked` (`:235`); `compaction.go:222` has the same shape.
* Change feed: new tag `ChangeRelRangePurge = 14`, body `{TypeToken, Before, Mode}`. Not a kind field on tag 13: an old replica would read a type token as a label token and purge nodes. An old replica stops at
  `apply_record.go:212` (unknown tag): upgrade replicas first. Touch: `changefeed.go:87,117,126`, `storeutil/changelog.go:95`, `changelog_wire.go:368`, `replication/change_identity.go:107,189`, `apply_record.go:192`,
  `import_merge.go:413` (capture nothing), `instant_floor.go:213`, `docs/persistence.md:233` ("13 in total"). sigma's `MirrorWatcher` ignores tags it does not list (`mirror_watcher.go:194`), as for 13.
* Replicas: apply re-runs the predicate on the replica's own state, advances the replica's own rel watermark, emits nothing, idempotent (as `applyRangePurgeLocked`). Bootstrap hole (b): carry both watermark families in the
  export header (importers accept older headers) and apply them on import; default: fix both nodes and rels (decision 3).
* DeleteWithTx/UpdateWithTx: no write guard below a watermark exists for nodes (`context.go:257-271` checks only `0 < txFrom <= now`); keep it: a backdated write succeeds, reads below the watermark still fail closed,
  `notePastDatedWrite` still bumps. A node cascade at one instant makes many ended rels: they are in scope of §2. Downgrade: an older binary ignores the rel watermarks, so do not downgrade after the first rel purge.

## 5. Item 3: where a new version of a long-lived node lands on tiered (from the code)
On its OWNER shard, never the hot shard: ref shard if it holds the ID, else the archive, else the event shard whose time window contains the snowflake MINT instant (`tieredstore_routing.go:248-286`,
`timestampToEventShardEntry :59-72`, hot only as the no-window fallback). `ReplaceNode` (`tieredstore_write_node.go:165`) and `ReplaceNodeWithHistory` (`tieredstore_write_history.go:26`) route there and write the current row
and the history row in that shard. A cold owner is lazy-opened for the write (`tieredstore_lifecycle.go:112-153`) and closed on checkin unless `MaxOpenColdShards` retains it. A `RefLabels` label lives on the ref shard
(always open, never rotated or dropped); its class cannot change on update (`tieredstore_write.go:31`). The docs say only "existing event entities continue to update/delete on their owner shard"
(`docs/persistence.md:218`): add this paragraph. For sigma: nodes rewritten every cut (coverage, span) should be reference-class, else each cut reopens a months-old shard; immutable bucket nodes are minted hot and never rewritten.
sharded: fixed by the ID's slot; memory, badger: one store.

## 6. Acceptance tests, red first (each on memory, badger, tiered with `newTestTieredStore` + `demoteToCold`, sharded; rule 15 two-phase, rule 16 exact sets, `errors.Is`)
1. Two-phase: EVENT rels at t0, an `OV_PAIR` rel, an `Overview` node; mutate after; purge EVENT. Pin >= Before: survivor set exact. Pin < Before: ByType, property lookup, `CountByTypeAt`, point `RelAsOf` = ErrRetentionExpired;
   `Overview` ByLabel and `OV_PAIR` ByType at the same pin succeed (red today); node purge still fails every scan below its watermark.
2. Ended: rel ended by `DeleteWithTx`, and by a node cascade `DeleteWithTx`; after the purge `History` is empty, `RelCountByType`, `Stats().HistoryCounts`, property/temporal indexes hold no ghost, `VerifyConsistency` clean.
3. Break the code: `Before <= 0`, empty or unknown `Type` (no token created), mint == Before kept, rerun is a no-op, > 256 rels, ctx cancelled mid-range then rerun completes, reopen with the watermark written and rows not yet removed,
   gates (`AllowRetentionPurge` off, `ReadOnlyReplica`, capability absent -> `ErrCapabilityNotSupported`), `ByValidTo` rejected.
4. Gate matrix: all-rels and untyped adjacency fail below relMax; rel point per type; Reset clears rel watermarks and a reused type token starts clean; reload at open.
5. Cross-shard: tiered rel with start in a cold shard and end in the ref shard (in-leg gone); sharded rel whose endpoints live on other slots; `RelSegments` type on memory: `ScanRelSegments`/`ScanRelColumns` exact set.
6. Feed: three-way parity (`changefeed_parity_test.go`) byte-identical; replica converges and refuses the low pin; a replica bootstrapped AFTER a purge refuses it too; tag 14 in `ChangeOpOf`/`DecodeChangeIdentity`/`ImportMerge`.
7. As-of cache: node column cached at P survives a rel purge (epoch unchanged); a node purge advances it only after the last chunk (hook). 8. `-race`: purge against AddRel/DeleteWithTx/ByType; a `GraphTx` that touched a purged rel then rolls back does not resurrect it (probe first; node purge may share the hole).
9. Benchmarks with `ReportAllocs`: `BenchmarkPurgeRels` per backend, `BenchmarkScanGate` (no-retention path must stay at one atomic load).

## 7. Version and docs
MINOR: new public API (`Admin().PurgeExpiredRels`, `RelPurgePolicy`, tag 14, two store capabilities, new MetaKV keys). `[Unreleased]` in CHANGELOG is empty at main although W3/W4 merged, so this ships with
DeleteWithTx as 4.44.0 if that is not cut, else 4.45.0. Update `docs/api.md:262-270`, `docs/errors.md:245`, `docs/persistence.md:218,233`, the stale comment `admin/api.go:145-151`, and CHANGELOG.

## 8. Open decisions (defaults taken)
1. `PurgeByValidTo` for rels: declined for now; reopen if sigma closes its event rels. 2. Per-label node gate: not built (Phase 2). 3. Export carries watermarks: yes, nodes and rels.
sigma side: pass `Before <= RetirableBefore` and clamp to now (ByAge is ingest time; a future-dated event minted earlier would be purged unfolded).
