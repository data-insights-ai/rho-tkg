# todo

What rho does next, in order. Last release: v4.74.0 (2026-10-23). Background, René's open decisions with my defaults,
and the numbered backlog: `HANDOVER.md`. Done work: `docs/CHANGELOG.md`, under the same tag.

Each item: one sentence, then when it is done and what it waits for.

## Now

- **SHARD** — René wants three storage tiers (hot, warm NVMe, cold HDD) built while ai-soc cleans up its data waste; a
  Fable agent designs the architecture and reviews, the main agent implements step by step. Done when: cold shards live
  on their own disk, released, with ai-soc's reference shard fitting the NVMe. Waits for: the Fable design.

## Next

- **CHAIN-2** — Each layer's share of the waste decides what that layer changes, against a target for stored bytes per
  fact. Done when: René has decided per layer. Waits for: CHAIN.

## Waiting

- **CHAIN** — René wants the whole data chain rewritten because it wastes 3 to 4 times the storage and memory, so first
  measure on the BA copy on Flux which layer (ai-soc, sigma, rho) wastes how much. Done when: the split per layer is
  known. Waits for: René's "run" (sigma runs it).
- **R37** — Listing facts that were changed still costs more than ai-soc's limit of 4 times a fact's values. Done when:
  ai-soc's cold /bars test passes on Flux. Waits for: CHAIN-2.
- **REPORT** — ai-soc and sigma have not yet reported on rho's newer doors, and rho builds only what blocks
  them. Done when: both have reported. Waits for: ai-soc, sigma.
- **ORIGIN** — Every release Markus makes on origin has to be merged and checked against rho's rules. Done when: no
  release of his is unmerged. Waits for: Markus.
- **CACHE** — Every open badger shard keeps its own cache, so ai-soc's memory grows with the shards it opens. Done when:
  one cache budget for the whole store, or a documented size per shard. Waits for: ai-soc's memory numbers.
- **TOMBSTONE** — A delete cannot keep the believed end of validity (backlog 4, 7, 25). Done when: released as a minor.
  Waits for: René, or sigma or ai-soc needing it.
- **REVIEWS** — Review findings were left out under the scope rule. Done when: René has decided. Waits for: René.
- **PURGE** — A purged entity answers "not found" at a time it answered before. Done when: René has decided. Waits
  for: René.
- **AXIS** — The announced change of the default valid-time start for new stores is still held. Done when: René has
  decided. Waits for: René.
- **FOLDS** — Range lookups check the resolved version, not every version in the range (backlog 16). Done when: René
  has decided. Waits for: René.

## Later

- **COLDSTART** — A fresh agent has to be able to work from the 13 docs alone. Done when: every gap it finds is fixed
  in the file that owns it.
- **GUARDS** — The rules the docs no longer state need tests that fail when the rule is broken. Done when: each guard
  fails on a planted violation.
- **FLAKE** — One property test misses a node only when it runs beside other tests. Done when: explained and fixed.
- **SUITE** — About 5,000 lines of memory and badger tests are near copies of each other. Done when: they run as the
  shared store suite.

## Parked

- **REOPEN** — ai-soc saw a slow history read after a reopen. Waits for: ai-soc's profile naming the slow call.
- **PENDING** — Reading one entity walks the whole write buffer on badger (half built on branch
  `row102-pending-ranged`). Waits for: ai-soc raising its priority, or nothing else open.
- **RELSIDECAR** — Relationship lookups by several properties may be too slow on BA. Waits for: a BA measurement.
- **NITS** — Small findings no consumer has hit (backlog 80-82, review nits). Waits for: a consumer hitting one.
- **NOCOMPRESS** — Compression cannot be turned off, because "none" is the default value. Waits for: a consumer asking.
- **STALECITE** — Some code comments cite todo sections that no longer exist. Waits for: the next change to that code.
- **V5** — The v5 line is paused on the local branch `v5-takeover`. Waits for: René.
- **BACKLOG** — 26 numbered items without a requester, listed in `HANDOVER.md` because code cites their numbers. Waits
  for: a consumer hitting one, or René.
