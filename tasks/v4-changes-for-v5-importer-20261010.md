# v4 changes since 32568c4 that the v4 → v5 importer and the v5 migration contract must account for

Date 2026-10-10. For Markus (branch `v5`, `docs/v5/PLAN.md` §9 "Migration"). Source of truth is `CHANGELOG.md` on main
(v4.44.0 … v4.50.0 at cutoff `95a1de39e424d6b8dd25f372645280667135c8ae`); this note lists only what changes how a v4 store must be READ or MAPPED. Nothing here edits the v5 branch.

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
