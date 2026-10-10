# Handover: end and supersede belief at a chosen transaction instant (`DeleteWithTx`, `UpdateWithTx`)

**STATUS 2026-10-10: DONE in v4.44.0** (`DeleteWithTx` / `UpdateWithTx` and their GraphTx, Batch and ingest twins,
`ErrTxOrder`; CHANGELOG `[4.44.0]` Added; evidence `tasks/evidence/tx-backfill-delete-update/`). Still open from this
spec: lifting the R8 refusal (caller-instant delete over a recorded close at or after t) = `tasks/backlog.md` item 7;
the same caller instant for interval rewrites = backlog item 34. File:line citations are at v4.43.0 (`4126bb1`) and
have moved since; the text below is the original spec.

Date 2026-10-09. From the sigma-tkgd realtime ingest design
(`sigma-tkgd/tasks/design-realtime-ingest-session.md` §4, branch `design/realtime-ingest-session`).
Code is cited at v4.43.0 (`4126bb1`). Nothing here is implemented. Tests first, run red, then code.

## 1. Why

A consumer replays or streams records whose transaction instant is the record's own, not the write clock.
`AddWithTx` already stamps a caller `TxFrom` for creates (`pkg/graph/rels/api.go:114`,
`internal/core/relationship_add.go:91-96`, gate `Config.AllowTxBackfill`, `internal/core/core.go:745-756`).
Nothing ends belief at a caller instant: `Delete(ctx, id)` takes none (`rels/api.go:248`) and stamps
`TxTo = DeletedAt = now` (`internal/core/relationship_delete.go:99-105`, `internal/core/temporal.go:88-90`,
`:132-141`); `Update` stamps the superseded version's `TxTo` and the new version's `TxFrom` with the same
`now` (`internal/core/relationship_update.go:151`, `:166-176`). `docs/api.md:79` states it: "Backfill is
CREATE-only — updates/deletes keep the monotonic system TxFrom". The AI-SOC therefore models an ending as an
extra `ENDED` relationship read by its rules (ai-soc `release/v5.0.0` `strata/base/base.go:353-371`), which
belongs in the store, not in a consumer's program (citation corrected in §6.9).

## 2. API

```go
// pkg/graph/rels/api.go, beside AddWithTx (:114) and Delete (:248); pkg/graph/nodes mirror.
func (a *API) DeleteWithTx(ctx context.Context, id types.RelID, txTo types.Instant) error
func (a *API) UpdateWithTx(ctx context.Context, id types.RelID, updates map[string]any, txFrom types.Instant) (*types.Relationship, error)
// pkg/graph/internal/core/tx_mutations.go, beside DeleteRelationship (:438) and UpdateRelationship (:224).
func (tx *GraphTx) DeleteRelationshipWithTx(id types.RelID, txTo types.Instant) error
func (tx *GraphTx) UpdateRelationshipWithTx(id types.RelID, updates map[string]any, txFrom types.Instant) (*types.Relationship, error)
```

Both doors: `ErrTxBackfillDisabled` unless `Config.AllowTxBackfill`; `ErrInvalidTxFrom` for an instant that
is not positive or is in the future (the rule of `resolveBackfillTxFrom`, `internal/core/context.go:252-267`),
reusing that function so the gate order (value, then privilege) and the as-of column cache bump stay one
place. The reserved property `tkg_tx_to` (`pkg/types/shadow.go:19`) stays rejected on every door; the
instant travels only as the argument, as `tx_from` does for creates (sigma `wire/ingest.go:39-47`).

## 3. Semantics

* `DeleteWithTx(id, t)`: the current version's tombstone carries `TxTo = DeletedAt = t`; `ValidTo` is
  clamped to `t` only when open or later than `t`, as today (`temporal.go:126-136`). A read pinned before
  `t` sees the row with its then belief (`internal/core/chain_resolver.go:40-43`), a read pinned at or
  after `t` does not (`:52-58`, `storeutil.SelectAsOf`). The current row is removed as today; only the
  tombstone stamps differ.
* `UpdateWithTx(id, props, t)`: the superseded version gets `TxTo = t`, the new version `TxFrom = t`
  (`relationship_update.go:166-176` with `t` in place of `now`), `UpdatedAt = t`.
* Ordering: `t` must be strictly greater than the current version's `TxFrom` and not below the latest
  `TxTo` in the chain; otherwise `ErrInvalidTxFrom` (a new sentinel `ErrTxOrder` is acceptable if the
  message must differ). A `t` equal to `TxFrom` would make a zero-width belief interval (`core.go:367`).
* The commit clock is not advanced by `t` (as `AddWithTx` does not advance it), so `NowTx` stays the pin
  above every write stamped with the clock; a backfilled `t` is below `NowTx` by construction.
* `deleteInstantClearOfCloses` (`temporal.go:107-118`) must not move a caller instant: a close recorded
  exactly at `t` is the one collision it guards; with a caller instant, refuse (`ErrInvalidTxFrom`) instead
  of silently moving `t`.
* `validInstantAfter` (`temporal.go:62-67`) is not applied to a caller instant: a belief may end before the
  valid interval starts. The backlog item "Future transaction time from `validInstantAfter`"
  (`tasks/backlog.md:40-43`) is unaffected but adjacent; do not fix it here.
* Idempotency: a second `DeleteWithTx` on a deleted id is `ErrRelNotFound` as today; `UpdateWithTx` with
  the same `t` twice is refused by the ordering rule above.
* Every backend: memory, badger, sharded, tiered all implement `DeleteRelWithHistory`
  (`store/memory/memorystore_history.go:200`, `store/badger/badgerstore_history_rel.go:126`,
  `store/sharded/history.go:218`, `store/tiered/tieredstore_write_history.go:612`); the tombstone's
  stamps are already in the row they write, so no store change is expected.
* Change feed: `ChangeRelDelete` carries the tombstone (`store/changefeed.go:22-28`), so a replica
  reproduces `TxTo = t`; the supersession path restores the previous version's `TxTo` from the new
  version's `TxFrom` (`internal/core/apply_record.go:412-445`), which holds for `UpdateWithTx` too.
* Hashes: `TxFrom`/`TxTo` are not hashed (`relationship_update.go:169`), so chains verify unchanged.

## 4. File-level changes

| File | Change |
|---|---|
| `pkg/graph/rels/api.go`, `pkg/graph/nodes/api.go` | the two doors, forwarding to `RelOps`/`NodeOps` |
| `internal/core/relationship_delete.go:46-130` | `Delete` calls a shared `deleteRelationshipAt(ctx, id, at)`; `at == 0` means `deleteInstantForRelationship` (`:99`), else the gated caller instant |
| `internal/core/node_delete.go:324` | the cascade twin: one instant for the node and every cascaded relationship |
| `internal/core/temporal.go:88-118` | `deleteInstantClearOfCloses` refuses instead of moving a caller instant |
| `internal/core/relationship_update.go:151`, `:166-176`; `node_update.go:174` | the version instant from the caller when given |
| `internal/core/tx_mutations.go:438-473`, `:224` | the `GraphTx` twins (snapshot and rollback unchanged) |
| `internal/core/context.go:252-267` | reuse `resolveBackfillTxFrom` for `TxTo`; rename or document |
| `docs/api.md:79`, `:82`, `:169` | the doors, and "Backfill is CREATE-only" becomes "creates, deletes and updates" |
| `CHANGELOG.md` `[Unreleased]` | minor version 4.44.0 (additive surface, `CHANGELOG.md:1702-1704`; `rels.Ops`/`nodes.Ops` gain methods); migration: upgrade replicas before any writer uses the doors |

## 5. Red tests, written and run before any code (break-the-code only, AGENTS.md rules 15/16)

No happy-path tests. Each test names the faulty implementation it catches; an accepted case appears only as
the counterpart assertion inside a refusal test. Every refusal asserts nothing changed (row, `History` length,
TxFrom/TxTo identical). Every test is table-driven over memory, badger, sharded, tiered (cross-shard rel).

| # | Test | Breaks with | Catches |
|---|---|---|---|
| R0 | `PlainDoorsUnchanged` | plain `Delete`/`Update`/`GraphTx` delete before vs after the refactor, incl. `ValidTo == now` close collision | refactor changes today's stamps or stops moving the instant |
| R1 | `Gate_Off` | gate off, valid t; rels, nodes, standalone and `GraphTx` | gate checked after the write; gate missing on a twin |
| R2 | `InvalidInstant` | t = 0, -1, now+1, MaxInt64; gate on and off | 0 read as "use the clock"; no upper bound; gate before value |
| R3 | `OrderEqualReversed` | t = TxFrom, TxFrom-1, below an older TxTo, below a history TxFrom (lesson 62 fixture); counterpart TxFrom+1 stored exactly | rule checks only the current row; `>=` instead of `>` |
| R4 | `OrderValidStart` | explicit and derived ValidFrom >= t; `UpdateWithTx` at t <= current `UpdatedAt`; expects `ErrTxOrder`, not `ErrInvalidStoreMutation` | inverted valid interval; store error leaking |
| R5 | `IgnoresT` | TxTo = DeletedAt = t in History; `RelAsOf` at t-1 / t / NowTx; `RelsAsOf(t)` must NOT contain it; exact set at t-1; ByType TxPin t-1 | stamps now; off-by-one at the pin |
| R6 | `UpdateStamps` | old TxTo = t, new TxFrom = UpdatedAt = t (field compare); next plain Update > t | prev.TxTo = now; UpdatedAt left at now |
| R7 | `CloseCollision` | ValidTo == t on the rel and on a cascaded rel | t moved silently |
| R8 | `ScheduledCloseAfterT` | recorded ValidTo >= t refuses with `ErrTxOrder` ("recorded close at or after t", both instants), nothing changed; counterpart ValidTo < t untouched; plain Delete still clamps (R0) | close lost by the clamp (pin t-1 would show ValidTo = t, a falsified past belief) |
| R9 | `CascadeOrder` | a cascaded rel with TxFrom > t refuses the whole delete; counterpart: all tombstones carry t, exact node+rel set at t-1 | cascade stamps rels with now; only the node checked |
| R10 | `Duplicates` | `DeleteWithTx` twice: `ErrRelNotFound`, history +1 only; `UpdateWithTx` same t twice: `ErrTxOrder` | double tombstone / version |
| R11 | `RaceClock` (-race) | N plain Updates racing the WithTx doors; TxFrom strictly rising, prev.TxTo == next.TxFrom | order check outside the entity lock |
| R12 | `TxRollbackEquiv` | rollback: export bytes and pins identical; commit: bytes equal the standalone door | twin skips snapshot or diverges |
| R13 | `ReplicaDiverge` | export bytes and pins t-1/t/now; warm replica `CountByLabelAt(P >= t)` and `DocValuesSnapshotAsOf` drop the node after apply | replica delete apply never bumps the as-of cache |
| R14 | `PrimaryCacheStale` | warm cached pin, node `DeleteWithTx`, count drops; hook interleaves a build between bump and write | bump before the write |
| R15 | `CrossBackendOracle` | bitemporal oracle with backdated ends; exact set diff per pin | backend divergence |
| R16 | `NoopIgnoresT` / `BatchIngestDoors` | unchanged or empty props; batch and concurrent ingest doors | t silently dropped; door gap |

Run `make test-race` and `make cover`; new code at or above 80 %.

## 6. Review corrections (2026-10-09, two Opus reviews: internal + dependents). These override §2-§4.

1. Ordering rule, under the entity lock: t > max(TxFrom, TxTo) over the whole chain AND t > the version's
   effective start (`rel/nodeCurrentVersionStart`, `UpdatedAt`); same check on every cascaded rel. Otherwise
   valid time inverts (derived ValidFrom, `relationship_add.go:314-323`) or the store rejects
   (`store/invariants.go:297-302`). Replay creates must therefore pass `tkg_valid_from` with `AddWithTx`.
2. Error: new `ErrTxOrder` that wraps `ErrInvalidTxFrom` (`errors.Is` matches both, so sigma's map,
   `wire/errors.go:121-124`, stays a validation error); its message names the conflicting stamp.
3. `t == 0` guarded in the facade (`resolveBackfillTxFrom` returns `(0, nil)`, `context.go:253`).
4. The close-collision refusal is keyed on "caller instant given"; plain `Delete` keeps moving the instant
   (`temporal.go:107-118`).
5. As-of cache: bump after the store write, not before (`context.go:265`); `applyNodeDeleteLocked` /
   `applyRelDeleteLocked` (`apply_record.go:579-649`) report the tombstone's min(TxTo, DeletedAt) as a
   past-dated write.
6. No-op update path (`relationship_update.go:96-99`, empty map) must not drop t: refuse or stamp.
7. Doors: batch (`batch_execute.go:532,579`) and concurrent ingest (`ingest_concurrent.go:399,424`) get the
   same seam or an explicit refusal plus a doc line. Seam choice: `at` on `deleteRelationshipInternal` /
   `deleteNodeLocked`; for update, a gated caller instant in the temporal path so all 4 update doors inherit it.
8. File table adds: `rels.Ops`/`nodes.Ops` (`rels/api.go:25`, `nodes/api.go:25`) and their fakes in
   `*/api_test.go`; comments at `core.go:758`, `context.go:250`, `compaction.go:420-422`,
   `docvalues_asof_cache.go:19-23`, `changelog.go:239,271`; amend lesson 59.
9. Citations fixed: `core.go:745-756` → 750-761; `relationship_update.go:166-176` → 166-179;
   `node_update.go:174` → :155 (instant) and :171-184 (stamps); `core.go:367` is the commit-clock comment, not
   zero-width; `docs/api.md:169` is CloseVersion; the ai-soc `ENDED` workaround is on branch
   `arch-consolidate` (efe22d7f, superseded WIP), not `release/v5.0.0`.
10. Dependents (none implement `Ops`/`GraphTx`; no wire change): sigma-tkgd v4.43.0, ai-soc engine v4.41.0,
    agent-bookkeeping v4.40.0, all build/vet green; ai-soc engine/cutexec `replace`s the live rho-tkg tree
    (picks up uncommitted edits; baseline already red on go 1.27.1). sigma's pinned-read promise
    (`sharedrun/run.go:70-76`) and Tyla EDB cache (`policy/tyla_limits.go:78`) need invalidation once sigma
    calls the doors.
11. Decision 2026-10-09 (R8): a caller-instant delete REFUSES a recorded close at or after t (§3's "clamped as
    today" is superseded for the caller door only). One tombstone row cannot both end belief at t and keep the
    close V that pins before t believed (the normalizer reopens ValidTo == DeletedAt, `txtime.go:468-479`).
    Fail closed now; relaxing later is additive. Backlog item 7.
12. Open, unverified: a backfilled t below the retention purge watermark is unguarded (reads fail closed);
    a rel version at t may reference endpoint versions recorded after t.

Implementation order: R0, R2-R6 red → seam + `checkTxOrder` + `ErrTxOrder` → R7/R8 → node twins + cascade
(R9) → cache after write + replica applies (R13/R14) → GraphTx, batch, ingest (R12/R16) → R10, R11, R15
under `make test-race` → docs, comments, lesson 59, CHANGELOG 4.44.0, `make cover`.
