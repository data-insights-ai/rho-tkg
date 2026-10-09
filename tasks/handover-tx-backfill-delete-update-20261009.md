# Handover: end and supersede belief at a chosen transaction instant (`DeleteWithTx`, `UpdateWithTx`)

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
belongs in the store, not in a consumer's program.

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
| `CHANGELOG.md` `[Unreleased]` | patch version; migration: none, opt-in by the gate |

## 5. Acceptance tests to write first (red), two-phase per AGENTS.md rule 15

1. Gate: without `AllowTxBackfill` both doors return `ErrTxBackfillDisabled` and change nothing (row
   still current, history length unchanged); with it, `t <= 0` and `t > now` return `ErrInvalidTxFrom`.
2. Ordering: `t == TxFrom`, `t < TxFrom`, `t` below an earlier version's `TxTo` are refused; `t` one
   millisecond after `TxFrom` is accepted.
3. Pins around `t`: after `DeleteWithTx(id, t)`, `RelAsOf(id, t-1)` returns the row with its original
   `ValidTo`; `RelAsOf(id, t)` and `RelAsOf(id, NowTx())` return `ErrNoVersionAsOf`; `RelsAsOf(t-1)`
   contains it and `RelsAsOf(t)` does not; `Rels().ByType(type, QueryOpts{TxPin: t-1})` includes it.
4. Update: after `UpdateWithTx(id, props, t)`, `RelAsOf(id, t-1)` is the old version, `RelAsOf(id, t)` the
   new one; `History(id)` shows `TxTo = t` on the old row; a later plain `Update` stamps the clock, above `t`.
5. Close collision: a row whose `ValidTo == t` is refused by `DeleteWithTx(id, t)`; `t+1` succeeds.
6. Cascade: `Nodes().DeleteWithTx(node, t)` stamps every cascaded relationship with the same `t`, and a pin
   at `t-1` returns the node and its relationships.
7. Every backend table-driven (memory, badger in memory, sharded, tiered with a cross-shard relationship),
   and under `-race`; a replica fed through `ApplyChanges` reproduces the same `RelAsOf` answers at `t-1`,
   `t` and `now`; `VerifyRelChain` passes after both doors.
8. Standalone and `GraphTx` doors give byte-identical rows (door equivalence, as
   `batch_door_equivalence_test.go` does for creates); rollback after `DeleteRelationshipWithTx` restores
   the row and its history.

Run `make test-race` and `make cover`; new code at or above 80 %.
