# Backlog 51 — effective scans bounded by valid end (ledger)

## In code before the change?
- No. `effective_timeline.go:639` `ForEachRelEffectiveByType`, `effective_scans.go:414`
  `ForEachNodeEffectiveByLabels` enumerate the type's / label's whole history (K1 ever-members or the history fold).

## Doors (new names; existing signatures unchanged)
- `Temporal().ForEachRelEffectiveByTypeFrom(type, pin, validFrom, fn)`
- `Temporal().ForEachNodeEffectiveByLabelsFrom(labels, pin, validFrom, fn)`
- Rule: an entity streams its whole type/label-limited timeline iff one of those segments has `ValidTo >= validFrom`
  or is open (half-open segments; one ending exactly at validFrom counts: "ended before validFrom" and "streams"
  partition the entities, sigma's gone-set rule `end < threshold`).

## Design
- Index shape: per store and kind an `index.ValidEndIndex`: map ID -> {bound, stamp, gen, deathVer, tok, flags}
  (32 B) + per type/label two ordered sets (bound, ID) and (stamp, ID) in sorted 256-entry chunks.
  Candidates(tok, pin, from) = bound >= from  ∪  stamp > pin  ∪  unknown. Core reads each candidate's timeline and
  decides (the resolver stays the authority); a read at pin >= stamp hands the exact end back (Refine, gen-checked).
- Fold (resolver-independent upper bound): max over rows of own ValidTo (0 = open); a tombstone d recorded when
  every earlier stamp <= d caps the bound at d (lifeEnds: a row's life ends at the first delete recorded at or after
  it). Removals, overwrites of a tombstone version, writes during a build, writes after a refine -> unknown (re-read
  by ID on the next call).
- Memory: notes at the row doors under ms.mu; built under ms.mu on first call.
- Badger: notes at the write-buffer funnel (appendOps / appendOpsLoggedRouted, beside noteHistoryKey); build =
  history-stamps protocol (BeginBuild under wbMu, overlay -> unknown, then view scan). RAM, rebuilt on first call
  after reopen (not persisted: derived from durable rows; persisting needs a read-modify-write per row write).
- Tiered / sharded / wrappers: no capability -> the unbounded door's candidates, same answers, not accelerated.

## Proof files
- 01-red.txt: doors on the unbounded candidates: cost test reads 1,005 / 10,005 (want <= 5); semantics pass.
- 02-green.txt: memory and badger read 5 with 1,000 or 10,000 deleted (closed-only: once, then 5).
- 03-test-short-all.txt, 04-race-touched.txt, 05-ram-per-entity.txt, 06-mutants.txt, 07-lint-docker.txt,
  08-security-docker.txt.
