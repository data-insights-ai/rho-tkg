# rho-tkg — Hallucination-Prevention (HP) forward workplan

**Date:** 2026-07-04 · **Status:** plan only (no code changed by this doc).
**Context:** this is the *demand-ordered* rho-tkg (ground-layer) work behind the
hallucination-prevention engine plan. It is written from an audit of the harness
(`tkgd-harness`) + `sigma-tkgd` v0.6.39 consuming rho-tkg **v4.11.1**.

## Standing verdict — rho-tkg is *sufficient* for the current HP milestone

As of **v4.11.1**, rho-tkg meets everything HP1.1 (Tyla tx-time pinning) and the
harness's verified-workflow gate need from the ground:

- **As-of read API** — `NodeAtTx`/`RelAtTx`/`NodesAtTx`/`RelsAtTx`, `NodeAsOf`/`NodesAsOf`,
  named marks `TagAsOf`/`ResolveAsOf`, and `AddWithTx` (tx-backfill for reproducibility).
- **As-of *soundness under deletes*** — **v4.11.1 (lesson 60)**: hard Delete is now a
  transaction-time tombstone in the generic `QueryOpts.TxAt` door, so the scan door and
  the named as-of door agree by construction. This closed the one real *correctness*
  divergence HP1.1 probing found (2026-07-03).
- **Tamper-evident provenance anchors** — per-version `tkg_hash` + rel→endpoint
  `tkg_from_hash`/`tkg_to_hash` + `VerifyNodeChain`/`VerifyRelChain`, and stable Snowflake
  IDs across versions. These are the ground primitives sigma-tkgd's HP1.2 (answer→source
  anchoring) and HP1.3 (chase provenance) build on.

**So nothing below is a blocker.** The next HP work on the critical path is *sigma-tkgd
HP1.2/HP1.3*, which needs no new rho-tkg API. The items here are pulled **when the
runtime's scale or a later HP phase makes them bite** — not built speculatively.

## Two invariants that constrain every item here

1. **rho-tkg stays LLM-free.** Its value is being an embeddable, deterministic library;
   every LLM touch lives in the harness/runtime. (Dependency contract: `harness → tkgd →
   tkg/v4`, `harness → rho-llm`; never the reverse.)
2. **Release discipline — batch.** Every rho-tkg tag forces a `sigma-tkgd` bump and then a
   `tkgd-harness` bump (we just rode v4.11.0 → v4.11.1 → sigma v0.6.39 → harness). Group
   RT-* changes into few releases to minimize the downstream churn.

---

## RT-1 — Tx-aware adjacency push-down  (closes HP1.1-resid **a**) · size **M**

**The one genuine *capability* gap in the as-of story** (correctness is already sound).
`OutgoingForNodes(nodeIDs, typeName)` / `IncomingForNodes(...)` take **no `QueryOpts`** —
`pkg/graph/internal/core/queries.go:801,861` and `pkg/graph/rels/api.go:339,348` — so a
tx-pinned edge read cannot use the indexed adjacency and falls back to a full,
history-aware `ByType` scan. sigma-tkgd's `pkg/tyla/edb.go` (`materializeSourceEdges`,
`txAt != 0` arm) already does exactly that scan as a **correct workaround**; RT-1 removes
it. Matters at **runtime (large-graph) scale**, negligible at harness (per-run) scale —
which is why it is forward work, not a current need.

**Change (non-breaking):**
- Add `OutgoingForNodesAtTx(nodeIDs, typeName, opts QueryOpts)` /
  `IncomingForNodesAtTx(...)` threading `QueryOpts.TxAt` through the adjacency index; leave
  the existing methods as `TxAt=0` delegates (no caller churn).
- Resolve edges **and** endpoint versions history-aware at the pin (mirror `NodesAtTx` /
  `NodeAsOf`), so a pinned adjacency read returns belief-at-pin — identical to the scan door.

**Break-tests (Pattern-34 — the two doors must agree *by construction*):**
- Divergence probe: `OutgoingForNodesAtTx` **==** `ByType`-scan-under-`TxAt` on one fixture.
- Adversarial as-of: an edge deleted *after* the pin is visible at pins *before* it;
  endpoints resolve to their as-of versions; edges created *after* the pin are invisible.
- memory + badger mirrors; a large-graph bench showing the push-down beats the full scan
  (the justification for doing it at all).

**Downstream payoff:** sigma-tkgd's `materializeSourceEdges` drops its pinned-edge `ByType`
fallback and calls the push-down.

## RT-2 — TxAt-door valid-time guardrail  (removes a live footgun) · size **S**

sigma-tkgd's **Hardening pass 7** (v0.6.39) found that a naive `QueryOpts{TxAt: t}` routes
tkg through its TX-only door, which resolves each entity's valid-time at **wall-now** — so
pinning knowledge-time silently acted as a *valid-time* filter and **emptied the pinned EDB
of every past-valid fact** (exactly what DatalogMTL programs reason over). The safe path
exists (their `pinnedOpts` — full-range valid window, entity-level valid filtering
disabled, version selection stays belief-at-pin), so this is **not** a correctness gap in
rho-tkg — it is a **DX/safety footgun**: the naive call is silently wrong.

**Change:** promote the safe pattern into rho-tkg — either a first-class "pin tx-time with
an explicit valid-window" read option, or make the TxAt door **fail loud** (or clearly
document + default-safe) rather than silently apply a wall-now valid filter. Any future
consumer (the runtime especially) should be unable to fall into the empty-EDB trap.

**Break-test:** a bitemporal fixture of *past-valid* facts, pinned at a tx-time after their
write, returns them (today's naive door returns none); the guarded/explicit API is the only
tx-pinned path that a consumer can reach.

---

## Hold-for-phase (real, but not until their consumer is active)

- **RT-3 — Dry-run constraint validation** *(hold for HP Phase 2.5, weak-enforcement)*. The
  constraints API is Set/Add/Get only; add a **validate-without-write** path (or document
  the `Tx`+rollback emulation the consumer can use meanwhile) so the engine can check a
  proposed fact set against program + constraints without asserting. Also the sibling of
  sigma-tkgd's EGDs: room for richer constraint kinds (uniqueness/existence/denial).
- **RT-4 — Fuzzy/full-text resolver** *(hold for HP Phase 3.9, entity resolution)*. The
  vector index exists but is **never called and has no embedding source** —
  `pkg/graph/internal/core/vector_search.go:54` (`SearchNearest`), also on the tx path
  `tx_consistent_reads.go:705`. Wire it + a full-text/trigram resolver only when alias
  resolution outgrows exact + declared-key matching.
- **RT-5 — Whole-graph merkle/snapshot root** *(hold for the audit story / harness P4
  durable-evidence)*. Per-entity hash chains already give tamper-evidence; add a single
  signed digest only if the audit trail needs one root.

---

## Priority note (for whoever schedules engine work)

With one parallel engine session, spend it on **sigma-tkgd HP1.2 (answer→source
anchoring)** — the actual critical path — not on rho-tkg. Pull **RT-1** (then **RT-2**)
when runtime-scale as-of reads make the scan fallback / the footgun bite; keep RT-3/4/5
parked until their phase. Batch whatever RT-* you do into few releases (see invariant 2).
