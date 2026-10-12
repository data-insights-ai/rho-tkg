# Backlog 50 — pinned adjacency doors and the re-imported earlier life (ledger)

Started 2026-10-12 on branch worktree-agent-ab0ee0097a18b0a1f (base 3f27f957).

## In code before the change?
- Candidate gather of every adjacency door: `pkg/graph/internal/core/temporal.go:789-810`
  (`forEachRelAdjacencyCandidateIDByDepth`: current adjacency ∪ DELETED rel IDs, "endpoints are immutable") and its
  sorted twin `adjacency_version.go:223` (`relAdjacencyCandidateIDsSorted`). A rel ID deleted and re-imported with
  other endpoints (`relationship_import.go:133`, allowed) is live again, so its earlier life is never a candidate.
- `effective_scans.go:400` (`relEffectiveAdjacencyCandidates`) folds EVERY rel with history (O(H) per call).
- No persisted record of a re-import over a deleted life exists (backlog 38 continues versions only).

## Plan
1. Store capability `ReimportedRelCapability` (store/capabilities.go): `NoteReimportedRel(id)` /
   `ForEachReimportedRelID(fn)`; memory (RAM map, Clear resets), badger (one MetaKey per ID, RAM set loaded lazily,
   survives reopen, Clear wipes), sharded (owning shard), tiered (reference shard).
2. Core notes an ID only when a row of an earlier life names another endpoint (memory-lean: same-endpoint re-imports
   are not recorded): `Rels.Import` / tx import (history read by `relLifeStart` already), replica apply of a rel put
   over a deleted life, `IO.Import` (current rel over history, history row under another current row).
3. Gather = current ∪ deleted ∪ re-imported (shared seam, temporal.go + adjacency_version.go); stores without the
   capability fall back to the all-history fold. `ForEachRelEffectiveAtNodes` uses the same gather.
4. AGENTS.md "deleted-only iteration" rule corrected; doc comments naming "endpoints are immutable" corrected.

## Red tests (written before the code)
- `TestPinnedAdjacencyFindsReimportedEarlierLife` (core): every adjacency door (`Outgoing/IncomingForNodesAtPin`,
  `*ForNodesAtTx`, `OutgoingRelsAt` / `IncomingRelsAt`, `NeighborsAt`, `ForEachAdjacentRelAt` /
  `ForEachAdjacentEndpointAt` under TxPin / ValidAt / both, `ForEachRelEffectiveAtNodes` both directions) against a
  brute-force per-ID oracle at every pin and valid instant; memory / badger / sharded / tiered, badger reopen, replica
  apply, export → import. Break cases: re-import with other src, other dst, same endpoints, re-import twice, deleted
  never re-imported, re-imported then deleted again.
- `TestEffectiveAtNodesVisitsSeedAdjacencyOnly` (graph, badger, one-entry cache): rows read by one AtNodes call stay
  within the seed's adjacency + deleted + re-imported, not H.
- `BenchmarkEffectiveAtNodesOneSeed` (bench/, renamed from EffectiveScansOneSeed): before/after numbers.

## Proof files
50-red-*.txt, 50-green-*.txt, 50-bench-*.txt in this directory.

## Result (2026-10-12)
- Red: 50-red-1-adjacency-doors.txt (every backend, replica, export-import, reopen, legacy store fail; controls
  "same endpoints" / "deleted, never re-imported" / "re-imported, deleted again" pass before the fix),
  50-red-2-at-nodes-visits.txt (412 rows read, bound 16).
- Green: 50-green-1 (adjacency test + MetaKV reap policy), 50-green-2 (12 rows read), 50-green-3 (go test -short
  ./...), 50-green-4 (-race core, store/..., graph).
- Mutants: 50-mutants.txt, 13 of 13 killed. Added after: `TestReimportedSetFallbacks` (no set / no MetaKV / marker
  write fails → all-history fold, doors equal the oracle; an invalid ID from an untrusted set → ErrInvalidStoreMutation).
  A gather-level ID validation was removed: the mutant without it survived (every door's per-candidate resolution
  already rejects the ID), so it was dead weight. 50-green-5: final -race run of core.
- Bench: 50-bench-before.txt, 50-bench-after*.txt, 50-bench-before-after.txt (back to back): AtNodes one seed on
  100 K with history badger 5.5-6.9 s → 13-19 ms, memory 1.0-1.1 s → 8-28 ms; by-IDs 1.0-2.7 / 0.2-1.1 ms.
  Remaining O(H): the stores' ForEachDeletedRelID in-RAM ID scan (CPU profile 50-bench-after-badger-cpu.txt:
  ~14 of ~16 ms per call). The "constant factor of by-IDs" gate is not met; follow-up noted in backlog 50.

## Fix round (2026-10-12, Opus-high review)
- 1 HIGH replica of an IO.Import-filled primary: red 50-fix-red-1 (4 backends). 2 MEDIUM open-time backfill: read-only
  open scanned and threw away (red 50-fix-red-1 "2 history reads at open"); now marker probe first, IDs streamed.
  3 LOW older writers: badger store marker 3 (red 50-fix-red-3). 4 gate: seed-following gather over a store-kept
  ex-adjacency (red 50-fix-red-2: 112 rows read, bound 16; bench 50-fix-bench-before/after).
- Design: the re-imported set and the core-side noting were replaced by the store's ex-adjacency (written in the
  same batch as each delete and each history row whose endpoints are not the live row's), so every write door —
  Rels.Import, GraphTx, batches, replica put / delete / history-version apply, IO.Import, ImportMerge, rollback,
  49's PutRelationshipsBatchGeneratedID (a create: nothing to note) — is covered without per-door code.
- Mutants 50-fix-mutants.txt: 16 of 16 killed. A conservative early return on a failed final marker write was
  left: its mutant survives because the index is complete in that session either way.
