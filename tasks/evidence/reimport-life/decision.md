# Backlog 38 + 2026-09-24 "(HIGH?) Re-import of a deleted ID" — decision

## The two bugs (both red on main, `red-main-production-code-*.txt`)

1. **Data loss.** `Nodes().Import` / `Rels().Import` (and `Nodes().AddByIDIfAbsent`, the `GraphTx`
   twins) of a deleted ID wrote the new current row at version 0. History is keyed by version, so the
   first write that moved that row to history (Update, CloseVersion, a cascade) stored it under the
   earlier life's version-0 key: the earlier row was gone, pins taken before the write answered
   differently after it, `Verify*Chain` failed (the earlier v1's PrevHash names the lost row), and
   `IO().Import` of an export of such a graph failed (`imported hash chain does not verify`). A
   `GraphTx` rollback of a re-import deleted the earlier life's whole history
   (`TruncateNodeHistory(id, 0)`).
2. **Life split.** A backfilled re-import (`tkg_tx_from` at or before the delete D) joined the earlier
   life (`chainWriteOrder` / `chainLifeEnds`: a row's life = deletes recorded before its TxFrom): valid-
   time doors read it absent from D on, as-of doors read it present. The plain door hit the same split
   when the delete was stamped ahead of the clock (a row whose valid start lies in the future is deleted
   at `ValidFrom + 1`): the import's clock stamp then lay below D.

## Brute-force belief definition for the shape (written before the fix)

Belief at pin P = the rows with `TxFrom <= P` (tombstone stamps after P un-applied, lesson 60). A
chain's rows are ordered by a total *write order*; a row's life is the number of tombstones before it in
that order; every row of life k ends (valid time) at the delete of life k; within a life the resolver's
rules apply (positional tiling, own bounds, supersession). For the as-of door the newest row is the
last one in write order recorded by P.

Two candidate write orders exist for a backfilled re-import R (TxFrom t_b <= D, written after D):

* **TxFrom order** (R before the delete): R belongs to life 0 and ends at D. Then the live current row
  (R, `Get` answers it) contradicts the belief at every P >= D (deleted), and the as-of door (newest by
  version) answers R. No door assignment is consistent: the store holds a live row the belief says is
  dead.
* **Version order** (R after the delete): R starts life 1. This reading is consistent at every pin —
  but it is not *determinable* from the stored rows. After a bounded cascade the earlier life holds rows
  above the tombstone's version, recorded before D with `TxTo = 0`; once a later cascade demotes R, R is
  such a row too (`TxFrom < D`, `TxTo` 0, version above the tombstone). No stamp tells "first row of
  life 1" from "cascade row of life 0" (the backlog entry's own observation). Placing R by version would
  need a life marker on the row: a wire change.

Hence the write must guarantee that **write order = TxFrom order = version order** for every chain
written from now on: the re-import's TxFrom must exceed every stamp of the chain, and its version must
exceed every version of the chain. The read side then needs no change: the life rule (deletes recorded
before a row's TxFrom) gives, on such chains, exactly the version order, and it stays the rule that reads
chains stored by v4.43–v4.47 (re-import at version 0) as before.

## Decision

* **Versions continue across lives** (`lifeStart` in `core/version_alloc.go`): the re-import takes one
  above the highest version on the chain (history; tombstone included; cascade rows above the tombstone
  included), under the entity lock, before any token is allocated; its `PrevHash` is that row's hash,
  so `Verify*Chain` verifies across lives and compaction/export/import/replica keep working unchanged.
* **A caller instant at or below a stamp of the chain is refused with `ErrTxOrder`** (wraps
  `ErrInvalidTxFrom`), the ordering rule `UpdateWithTx` / `DeleteWithTx` already apply. Nothing is
  written, no label or type token is allocated. This is a behaviour change for an input accepted by
  v4.43–v4.47 (`tkg_tx_from` <= the delete on an ID with history); it is chosen over "order by version"
  because the latter is not determinable without a format change (above) and would leave the valid-time
  and as-of doors disagreeing after a cascade. A create of a fresh ID (no history) keeps accepting any
  past instant.
* **The plain door raises the clock floor past the chain's stamps** (`advanceInstantFloor(maxStamp)`)
  before it stamps `c.now()`: the import and every later write on the entity follow the delete, also
  when the delete was stamped ahead of the clock.
* **GraphTx rollback of a created ID removes rows from the created version on**
  (`TrimNodeHistoryFrom`; without the capability, the rows below are rewritten from a copy) instead of
  the whole history.
* **No migration, no format change.** Old chains keep their rows (the overwritten ones are lost);
  `TestReImportOldChainReadsAsBefore` and `TestAsOfBackfilledReImportOfDeletedID` pin that they read as
  before (guards, green on main).
