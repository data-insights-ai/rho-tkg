# v4 changes since 32568c4 that the v4 → v5 importer and the v5 migration contract must account for

Historical retained source note at v4.51.1: main consolidated this file on 2026-10-12,
but the v5 mapping additions are retained here as evidence. Current reader contracts and
qualification status are in `docs/v5/PLAN.md` §8a and `docs/v5/reference/consumer-*.json`;
this note does not qualify the v4.51.3 reader or create another roadmap.

Updated 2026-10-12 (original handover 2026-10-10). For Markus (branch `v5`, `docs/v5/PLAN.md` §9 "Migration"). Source of truth is `CHANGELOG.md` on main
(v4.44.0 … v4.51.1 at cutoff `dbd74cf45c65c1086798012e92453315cab75143`); this note lists only what changes how a v4 store must be READ or MAPPED. This note changes no importer implementation.

## 1. The as-of rule changed (v4.46.0)

`NodeAsOf` / `RelAsOf` / `NodesAsOf` / `RelsAsOf` and the `TxPin` scans answer "the newest row RECORDED BY the pin"
(pin-stable, newest by version), not "the current row while it is current" (v4.44.0/4.45.0). After a bounded cascade
(`SetNodeVersionInterval`) the as-of answer is the corrected slice, not `Get`. The STATE doors are `NodeAtTx` /
`RelAtTx` (and `ByLabel` / `ByType` with `ValidAt + TxAt`): "the row valid at t as recorded at the pin".
v4.47.0 adds `Node/RelEffectiveTimeline(id, pin)` (contiguous `[from,to,row]` segments, pointwise equal to the state door;
segments never merge distinct rows with equal content) and the scan forms including entities deleted before the pin.
**Importer consequence:** map v4 history by the STATE definition (valid interval of each row over the transaction-time
belief at the pin), not by "current row" and not by raw `Get`/`History` order; the effective timeline is the reference.

## 2. Version numbering and stamps on appended rows (v4.46.0)

- One allocator for every appended row: versions are unique and increase in write order per entity. Chains written by
  v4.45 and earlier (since the append-only cascade 994df82, v4.9.0) can hold two rows with ONE version (a cascade row numbered `maxVersion+1` collided with the next Update's
  `current.Version()+1`, backlog 18) and rows with `TxTo < TxFrom` (cascade pieces copied the template's stamps, backlog 14).
  The read side tolerates both; the importer must not assume version uniqueness or `TxFrom ≤ TxTo` on OLD chains.
- Appended rows carry no `TxTo` / `DeletedAt`. A cascade on a deleted entity is refused (`ErrEntityDeleted`).
- TxFrom is NOT co-monotonic with version (`validInstantAfter` bumps stamps above the clock; `AllowTxBackfill` writes past
  instants), and one `GraphTx`/Batch spans many instants: transaction boundaries cannot be recovered from `TxFrom`
  order (already in PLAN §9; the v4.44 `*WithTx` doors add caller-chosen instants).

## 3. One-tick intervals are ordinary spans (v4.44.0)

A row `[t, t+1)` is read as an ordinary one-millisecond span by current v4 doors; `QueryOpts.IncludeEclipsed` is a
reserved no-op. Commit 994df82 (2026-06-12) removed the known eclipse writer, but dates alone do not establish which
binary, import or copied history wrote a store. PLAN §9 governs: import one-tick rows as genuine spans only with
verified writer/format provenance excluding sentinel encoding for those rows. Otherwise preserve raw evidence and
report ambiguity; never infer an event, and do not treat a post-2026-06-12 history start as sufficient provenance.

## 4. Closes and deletes read differently (v4.47.0, v4.46.0)

- A row replaced by an Update / `CloseVersion` / label change ends where its replacer starts (read-time supersession
  rule, `core/chain_supersession.go`); a closed entity no longer reads valid again after a later bounded cascade.
  Chains are sorted by life then version (`chainWriteOrder`) so re-imported IDs agree on every backend.
- Delete after a bounded cascade ends the entity in every read door; valid-time reads cap every row of a deleted entity at
  the delete instant (past valid time stays readable). v4 still stamps the delete tombstone IN PLACE and clamps a later
  dated close to the delete instant (backlog 25: pins before the delete lose that close); v5's lifecycle closes replace this.

## 5. Re-import (v4.48.0)

- Up to v4.47.x a re-import of a deleted ID restarted at version 0 and its first move to history OVERWROTE the earlier
  life's history v0: that history is LOST in already-written stores and `Verify*Chain` stays false for them (no migration).
  **The importer must flag these chains as lossy**, not synthesise the missing life.
- From v4.48.0 a re-import continues the numbering above everything stored (`Version() = earlier top + 1`, `PrevHash`
  links across lives) and a backfilled re-import (`AllowTxBackfill`, raw `tkg_tx_from`) at or below any stamp of the chain is
  refused with `ErrTxOrder`. A "life" on new chains is a tombstone in version order; on old chains it is "number of
  deletes recorded before the row's TxFrom".
- `Nodes/Rels().HasHistory(id)` and `LatestStamps(id)` (v4.46/v4.49) give O(1) presence and newest stamps (max TxFrom,
  max of TxTo/DeletedAt, deleted) — usable as importer guards instead of reading History per entity.

## 6. Identity and time bits (v4.47.0)

`types.NodeID.MintInstant()` / `RelID.MintInstant()` are the single derivation of an ID's mint instant (Unix ms,
epoch 2026-01-01; layout in `pkg/internal/idlayout`); zero/negative IDs return 0. A row without a recorded valid-from
starts at the mint instant ("`tkg_valid_from = 0` = unset" on every door; explicit 0 stays accepted). Backlog 31/26:
chains with version gaps or collided versions stored by old releases answer in the old order.

## 7. Other behaviour the importer will meet

- `Config.DurableCommit` (v4.44.0): opt-in flush+fsync at commit; a memtable switch can leave the retired WAL unsynced.
- Unique constraints: `UniqueForever` claims are withdrawn on failed writes; `SetNodeVersionInterval` patches are checked
  (v4.46/v4.49). A claim persisted before a crash between claim and row write stays (needs an atomic commit: PLAN §5.2).
- Tiered store: composite and relationship temporal indexes (v4.45.0); the rel temporal index lives on hot and warm
  shards only. The property tx-membership sidecar serves pinned property lookups (v4.48.0).
- Tests worth reusing as import oracles: `core/bitemporaloracle*_test.go`, `effective_timeline_oracle_test.go`
  (pointwise equality, 3 M checks), `tx_backfill_oracle_test.go`, `reimport_life_oracle_test.go`, and the evidence
  folders under `tasks/evidence/` (cascade-correctness, effective-timeline, reimport-life, latest-stamps).

## 8. Belief retraction and row-format protection (v4.49.1 / v4.49.2)

`Retract` / `RetractWithTx` on nodes and relationships, GraphTx, Batch and ingest are distinct from Delete: at pins
at/after T the retracted life answers at no valid time; pins before T are unchanged. Node retraction cascades at one
instant; re-import may start a new visible life. Preserve `TemporalMetadata.Retracted`, tombstones, raw audit/source
fields and hashes; map belief retraction separately from a validity-end close. The marker is not in the content hash.
Only retraction tombstones now emit `fv=3` with `rx`; ordinary rows and the store marker stay 2. Current readers accept
fv<=3, including v4.49.1's fv=2+rx rows. Older readers fail closed on fv3 but can misread already-written fv2+rx as a
plain Delete: upgrade every reader before retraction writers; a date/version of an export alone cannot repair that.

## 9. Additional consumer doors at the v4.50.0 cutoff

`Index().InventoryEpoch()` is a nonpersistent per-Graph DDL invalidation counter, not a data epoch or cut.
`CreatePropertyWithOptions` / `CreateRelPropertyWithOptions` and `PropertyOptions` / `RelPropertyOptions` expose opt-in
`RangeCounts`; unsupported stores refuse rather than drop options, and old readers may load optional persisted rc as
a plain index. Range counts remain non-temporal, answer NaN bounds as 0 where exact, and sharded relationship counts
sum each owned row once. Endpoint-ordinal adjacency retains ID order and callback isolation; the memory allocation
improvement changes cost, not semantics. Carry these doors/capabilities into the migration inventory; historical
consumer pins and measured v4.43 runs are not relabeled as v4.50.0 acceptance. See CHANGELOG `[4.50.0]` and PLAN §8a.

## 10. Interval corrections at a caller transaction instant (v4.50.1)

`Temporal().SetNodeVersionIntervalWithTx` / `SetRelVersionIntervalWithTx`, GraphTx, BatchBuilder and ingest
Session twins record the interval patch at `txFrom`: every appended correction, resumption and current-slot row
has `TxFrom = UpdatedAt = txFrom`; stored rows are untouched and earlier pins retain the uncorrected belief.
Valid time is independent (`validFrom > txFrom` is allowed). The caller instant must be positive/nonfuture,
pass `AllowTxBackfill` and follow every TxFrom/TxTo in the chain across re-imported lives, including rows outside
the corrected interval; hard-deleted entities refuse. A failed call writes nothing. Deferred units preflight
all caller-instant operations and correction conflicts on the same entity, including cascaded relationship
deletes, before writing. Plain interval doors still stamp the clock.

Importer consequence: retain each raw caller stamp and source knowledge-time interpretation alongside the
exact corrected valid-time state. New v5 database revisions commit at new certified cuts; a source wall-clock
stamp does not construct a cut, and equal stamps do not establish global atomic transaction groups. Preserve
historical consumer pins and older evidence. Source/oracle evidence: CHANGELOG `[4.50.1]`,
`tasks/evidence/interval-withtx/` (red runs, mutants, W5 oracle at 100 seeds).


## 11. Access and statistics contracts at v4.51.0

Source cutoff `44303a17a391d663cd48567c55bbb9ee85cdf892` extends the earlier mapping contracts without changing
v4 row formats or dependencies. `Stats().PropertyNDV` / `RelPropertyNDV` return estimated HLL NDV and current
indexable-scalar Count without an exact Min/Max rescan; unsupported/disabled capabilities refuse.
`RelHistoryCountByType` counts identities with history, including deleted ones: memory/badger answer,
tiered/sharded decline; ID reuse across types makes it an upper bound, so only a supported zero proves none.

Known-label empty DocValues intersections now return `ok=true` with no rows. Historical property seeks on
unindexed keys use label/type ever-members as candidate supersets, preserving lost labels, deleted rows and
transaction-pin acquisition pruning; unsupported/depth-scoped paths keep full history resolution.
History-free valid-time adjacency may use the live row, while pins/history/deletes retain full resolution.
Memory adjacency and unfiltered relationship columns preserve ascending IDs and endpoint ordinals.
Badger's immutable column patch preserves held snapshots, exact types/dictionaries/validity and rebuild
fallbacks. Per-type degree-cache epochs may over-invalidate via stripe collisions/coarse writes; global
relationship epochs still invalidate column/expand reads. These stamps are not certified cuts.

The data-only current-source delta in `docs/v5/reference/consumer-contracts.json` and `consumer-migration.json`
records source hashes and required checks separately from unchanged historical consumer pins. No new v5 public
API or completed importer is implied. That initial integration required fresh reader qualification;
new NDV/history/empty-intersection/historical-seek/adjacency/column-cache checks and aggregate gates remain pending.


## 12. Current stamps, columns and broader state scans at v4.51.1

Source cutoff `dbd74cf45c65c1086798012e92453315cab75143` carries `CurrentStampsCapability` / `RowStamps`, the existing
`HistoryStampsCapability` with tiered routing-probe reuse, and `MemberIDsCapability`. Current stamps select the
current owner row; history stamps retain the history fold/dedup rule. Partial temporal decoding is not full-body
validation. ID-only membership is current, ascending and pending-write aware, then widened to historical
candidates and hydrated; unreadable index members cannot become empty answers. Inherited native/sharded wrapper
capabilities must preserve row overrides. These accelerators neither certify cuts nor establish complete history.

`ColumnData.Other` marks held values outside the column, also Null with zero typed slots, for same-selection row
fallback. `RelColumnOtherSource` is optional; absence of its marker is not proof of absence. Typed arrays remain
parallel even for leading/all absences; Other resets per batch. Every integer width follows snapshot normalization:
uint/uint64 above MaxInt64 become float64, fitting/nonfitting numeric mixtures refuse. Preserve raw concrete
integer evidence separately from that projection.

Six broader `Temporal()` doors stream the same per-entity state timeline: `ForEachNodeEffective`,
`ForEachRelEffective`, `NodesEffectiveByIDs`, `RelsEffectiveByIDs`, `ForEachRelEffectiveAtNodes` and
`ForEachNodeEffectiveByLabels`. Relationship view is declared; label selection is a union; explicit ID order is
first occurrence, other entity order unspecified. `Direction` is Outgoing/Incoming; invalid direction is
`ErrInvalidDirection`. Input, pin and retention/compaction refusals remain explicit, with callbacks outside locks.
Seeded gather includes all history to retain earlier endpoints across reimported lives, at O(H) timeline cost;
per-entity isolation and one call do not promise one certified graph cut. Upstream backlog 49 still records
tiered cross-shard reimport making earlier history unreachable, and 50 records older pinned adjacency missing
earlier endpoints. Broader scans cannot recover inaccessible per-entity history; preserve/report incomplete source
evidence rather than infer a complete history from current state or clean hashes.

The manifests retain the exact v4.51.0 source delta as history and separately pin this reader and pending
obligations. Earlier v4.51.0 hosted migration qualification stays historical. V4.51.1 needs the full migration
preflight narrow gates and independent source/input/scanner qualification, followed by aggregate/hosted checks;
no new v5 public successor or V2–V7 acceptance follows from this source merge.
