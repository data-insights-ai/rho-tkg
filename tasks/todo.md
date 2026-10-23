# todo

What rho does next, in order. Last release: v4.74.0 (2026-10-23). Background: `HANDOVER.md`. Done work: `docs/CHANGELOG.md`.
Full text of the items before this format (file cites, plans): `git show 791c1aa3:tasks/todo.md`.

Each item: one sentence, then when it is done and what it waits for.

## Now

- **CHAIN** — René 23 Oct: "we have to rewrite the full data chain, we wasted 3 up to 4x data storage and memory"; first
  measure which layer wastes how much, on the BA copy on Flux, with sigma's store analysis tool and rho's stored-size
  read. Done when: the split per layer (ai-soc, sigma, rho) is known. Waits for: René's "run" (sigma runs it).

## Next

- **CHAIN-2** — Decide what each layer changes, against a target such as stored bytes per fact at most 1.5 times its
  values; rho's options: the believed row stays the current row, excise pieces store no row copy, a delta's anchor is
  decoded once per read, decoded rows closer to their stored size. Done when: René has decided. Waits for: CHAIN.

## Waiting

- **R37** (row 121) — Listing facts that were changed still costs more than ai-soc's limit of 4 times a fact's values
  (on v4.73.0: 9.1x cold, 32.6x for rewritten facts). Done when: ai-soc's cold /bars test passes on Flux. Waits for:
  CHAIN-2, which decides the remaining fixes (skeleton cache, relationship decode copy).
- **REOPEN** (row 92) — ai-soc saw a slow history read after a reopen; rho acts only on a profile naming the slow call.
  Done when: the profile is in and acted on. Waits for: ai-soc.
- **REPORT** — ai-soc and sigma report on the doors of v4.70.0 and later; rho builds only what blocks them. Done when:
  both reported. Waits for: ai-soc, sigma.
- **ORIGIN** — A release by Markus on origin is merged and checked against rho's rules at the start of a session.
  Done when: no release of his is unmerged. Waits for: Markus.
- **PENDING** (row 102) — Reading one entity walks the whole write buffer on badger; half-built on branch
  `row102-pending-ranged`. Done when: the cost no longer grows with the buffer, released as a patch. Waits for: ai-soc
  raising its priority, or nothing else open.
- **CACHE** — Every open badger shard has its own cache, so ai-soc's memory grows with open shards. Done when: one
  store-wide cache budget, or a documented size per shard. Waits for: ai-soc's memory numbers.
- **RELSIDECAR** (row 105) — Relationship lookups by several properties may be too slow on BA. Done when: measured
  fast enough, or a sidecar index built. Waits for: a BA measurement.
- **TOMBSTONE** — A delete that keeps the believed end of validity (fixes backlog 4, 7, 25); branch
  `tombstone-writer-v4.63`, about half a day, a minor. Done when: released with a heads-up to sigma and ai-soc. Waits
  for: René, or sigma or ai-soc needing it.
- **NITS** — Backlog 80-82 and review findings not taken. Done when: a consumer hits one and it is fixed. Waits for:
  a consumer.
- **REVIEWS** — Review findings left out under the scope rule. Done when: René decides (my default: leave them until a
  consumer hits one). Waits for: René.
- **PURGE** (row 104) — A purged entity answers "not found" at a time it answered before. Done when: René decides (my
  default: no rho change, sigma treats not-found for a known id as purged). Waits for: René.
- **AXIS** — The announced change of the default valid-time start for new stores has been held for five
  minors. Done when: René decides (my default: keep holding; the next minor makes it or rewords the notice). Waits for: René.
- **FOLDS** (backlog 16) — Range lookups check the resolved version, not any version in the range. Done when: René
  decides (my default: keep today's documented behaviour). Waits for: René.

## Later

- **COLDSTART** (row 108) — A fresh agent tries to work from the 13 docs alone, with new questions. Done when: every gap
  it finds is fixed in the file that owns it.
- **GUARDS** (row 118) — The rules the docs no longer state need tests that fail when broken (René: tests, not prose):
  attribution lines, lesson cites, raw msgpack use, wire round trips, banned sort and compare calls, design-rule gaps.
  Done when: each guard fails on a planted violation.
- **FLAKE** (row 119) — One property test misses a node only when run in parallel; it may be a clock question, not a
  test problem. Done when: explained and fixed.
- **SUITE** (row 119) — About 5,000 lines of memory and badger tests are near copies; they move into the shared store
  suite. Done when: moved and green.
- **STALECITE** — Code comments still cite todo sections that are gone. Done when: repointed, at the next touch of that
  code.

## Parked

- **NOCOMPRESS** (row 119) — Compression cannot be turned off (None is the default value). Waits for: a consumer asking.
- **V5** (row 61) — The v5 line, paused on local branch `v5-takeover`, first tag at its S2 exit. Waits for: René.
- **SHARD** (row 117) — Separate disks per tier (hot, warm, cold), about two weeks, one minor; plan, defaults and the
  blocker (the reference shard grows ~60 GB per BA day): `git show 791c1aa3:tasks/todo.md`. Waits for: René.

### Backlog

No requester yet; each waits for a consumer to hit it or for René. Numbers are stable (code cites "backlog N"); closed
ones are in `docs/CHANGELOG.md`; details: `git show 6e20157c:tasks/backlog.md`.

- 0 — column segments on NVMe (ADR-0011): steps S4 merge, S6, S7 open
- 1 — a failed import should leave no change-log record
- 2 — test gaps: tiered crash matrix, clock skew vs rotation, tiered metadata fuzz, extreme inputs, race soak, validation limits
- 3 — two adjacency-at reads skip badger's fast path and fold deleted rels per call
- 4 — five temporal read edge cases (future-only during-reads, delete of a future row, as-of after a cascade, unset valid start, the 2034 ID horizon)
- 5 — 388 B per HOP row left to compact (key tokens, history versions, cache of frozen rows, string interning)
- 7 — `DeleteWithTx(id, t)` refuses a recorded valid end at or after t; goes with TOMBSTONE and 25
- 10 — tiered lacks the node-side temporal candidate index
- 13 — async ingest can blame the wrong group when a node and a rel share a number
- 16 — range lookups check the resolved version (FOLDS)
- 20 — a missing benchmark for `RelAtTx`
- 23 — the change feed prefetches unused values and may read past its keyspace
- 25 — close, pin, delete: an as-of rewind reopens the old range; goes with TOMBSTONE and 7
- 26 — chains with two rows of one version keep the old order
- 31 — `NodeAsOf` misses a row above a version gap
- 37 — tiered error rollback: a reader can see empty history for a moment
- 39 — compaction stubs survive a reset; an import of an old ID can restart at version 0
- 40 — cascade resumption can write one extra split row
- 45 — three errors lack an exported alias; one is outside the errors inventory test
- 46 — stale comments (old ADR and lesson numbers)
- 47 — an unverified claim about `NodesByLabelAt`; no RAM budget for property sidecars
- 71 — the valid-time hull of current rows takes a full pass per call
- 76 — a number lookup makes one store call per number width
- 78 — `EndValidity` lacks its with-tx, batch and ingest twins
- 80 — whole-axis entities patched while closed keep a stale current row
- 81 — sharded: a rel row with a local start and a remote end is not repaired at open
- 82 — tiered: an adjacency read of a reference node at full depth opens every cold shard
