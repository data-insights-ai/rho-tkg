# ADR-0009 — v3 storage-efficiency + temporal-query round: anchor+delta history (B6) and Allen-predicate query doors (OPT10)

Status: DRAFT — owner review requested
Date: 2026-07-16
Depends on: ADR-0001 (history compaction — the trim/stub/watermark this must stay
consistent with), ADR-0005 (change-log — the replication feed whose byte-exactness
must survive a storage-internal history representation), ADR-0006 (ingest pre-encode
— the pre-encoded-wire byte-identity discipline this reuses)

## 1. Problem

Three threads were scoped together as a single "v3 redesign" and grounded against
the actual code before building. Grounding changed the picture; this ADR records
what shipped, what was killed by measurement, and what was deferred.

- **B3 — adaptive timestamp codec.** Every persisted `NodeWire`/`RelWire` stores
  five mid-map timestamps (`vf/vt/ca/ua/da`) as forced 9-byte int64s. Delta-encoding
  them against the entity's snowflake creation instant (recoverable from the stored
  `id` at zero extra bytes) reclaims ~15–20 B/row at the WIRE level.
- **B6 — anchor+delta history.** Version-history rows (badger `0x07`/`0x08`) are
  FULL entity snapshots. A wide entity (5–20 properties, some carrying a large
  unchanging blob: customer/product/contact/SIEM-event) re-serializes that blob on
  every version bump. The unchanged bytes dominate history storage.
- **OPT10 — temporal query surface.** The only interval doors were overlap-based
  (`NodesDuring`) or pairwise-entity (`RelateNodes`). There was no door to ask
  "which entities' valid-interval `Meets` / is `Before` / `Contains` … the window?"
  — the general Allen-relation query. A secondary "zone-map" pruning idea rode along.

## 2. Decisions

### 2.1 B3 — DROPPED by on-disk measurement

Badger stores block-Snappy'd SSTable blocks, not raw rows. The honest metric is
post-block-Snappy size delta, not wire-level byte count. MEASURED: **1.13%**
post-Snappy on representative data — Snappy already captures most of the repeated
fixed-width int64 framing. That is far below the ~10% go/no-go gate set for
introducing the FIRST custom wire decoder and an `fv` bump. B3 was not built.
(The Phase-0.5 hand-written decoder — a strict read-path win, ~1.4–1.6× faster,
independent of B3 — shipped separately and is unaffected.) See lesson 67.

### 2.2 B6 — anchor+delta version history — SHIPPED (opt-in)

Store version history as a full ANCHOR every `HistoryAnchorInterval` (16) versions
and the rest as DELTAS carrying only the properties that CHANGED versus the interval
anchor, plus both integrity hashes and the full temporal block verbatim. Opt-in via
`badger.Config.HistoryDeltaEncoding` (default OFF while it soaks); threaded through
`tiered.Config` / `sharded.Config` / graph `Config`. MEASURED **~39% less history
storage post-block-Snappy** on history-heavy wide entities.

Key properties (why this is safe without touching the integrity, verify, compaction,
or replication layers):

1. **Self-describing framing, ZERO migration.** An anchor is the raw full msgpack
   marshal (a map header `0x8x`/`0xde`/`0xdf` leads); a delta carries a 1-byte
   `'D'` (0x44) prefix that can NEVER be a map header. A legacy pre-B6 row is
   transparently an anchor; reads accept BOTH forms regardless of the flag, so the
   flag is safe to toggle on an existing store. No `fv` bump — the delta is a
   history-VALUE-level representation, not an entity-row format change. A
   delta-unaware (older) binary that reads a `'D'` row fails CLOSED (the fixint tag
   is not a struct/map header → decode error), never a silent misread.
2. **Integrity untouched.** Both hashes (`h`, `ph`) are carried verbatim in every
   delta. `ph` cannot be dropped: bitemporal corrections (`PutNodeVersion`) form a
   DAG where `PrevHash` points to the superseded valid-time version, not `Hash(V−1)`.
   `Verify*Chain` recomputes over reconstructed rows unchanged.
3. **Bounded reads.** Random-access `GetNodeVersion` reconstructs in ≤2 point reads
   (interval anchor + target delta). Whole-chain scans reconstruct from an in-scan
   anchor cache (no extra fetches). `NodeAsOf`/`RelAsOf` classify on the delta's Meta
   (temporal carried verbatim — no anchor read) and reconstruct only the winner.
4. **Replication byte-exactness preserved WITHOUT any change-log change.** The delta
   is storage-internal and NEVER enters the change-log feed: a put-with-history logs
   the current (full) row + a `WithHistory` bit, and the replica reproduces history
   depth via its OWN write path. `DiffNodeHistory` is deterministic, so two backends
   converge. A delta-primary → full-replica convergence test proves it.
5. **Compaction/truncate anchor-safety.** A keep-newest-N truncation re-materializes
   to full ONLY those kept deltas whose interval anchor is being removed (exact
   retention count preserved); trim-from is inherently safe (kept deltas' anchors are
   lower and survive). The current row (`0x01`/`0x02`) is always full. The memory
   backend stays full-snapshot and serves as the differential oracle.

Reconstructed property order is canonical: a delta re-materialize resolves KeyToken
→ Key and re-sorts by key STRING (the decoder validates strict ascending key order),
because the Update path tokenizes in property-validation order, not alphabetical.
See lesson 68.

### 2.3 OPT10 — Allen-predicate doors — SHIPPED; zone-map DEFERRED

Add `g.Temporal().NodesRelating(from, to, rels)` and the rel mirror `RelsRelating`:
entities having a version whose valid-interval `[vStart,vEnd)` has an Allen relation
(in the `types.AllenRelationSet` `rels`) to the query interval `[from,to)`, history-
aware and **predicate-anywhere** (a match on ANY version of the chain counts). These
are the only temporal doors that surface NON-overlapping relations (`Before`/`After`/
`Meets`/`MetBy`).

- **`types.RelateOpen`** is the version-chain classifier: an END of 0 is OPEN (+∞ via
  `math.MaxInt64`), so the routinely-open current-version interval `[start,∞)` and an
  open query end classify exactly. Only ENDS may be open (an open start is rejected).
  The existing `Relate` (rejects any zero endpoint) is untouched — two doors, same
  shape (rule 17).
- **`probeRelate`** is a new chain-resolver kind; `resolve{Node,Rel}ChainRelating`
  classify each version most-recent-first (same seam/rationale as `…During`). The
  query end is passed RAW (0 = open) — NEVER pre-resolved to a concrete "now+" bound,
  which would corrupt the Before/After boundary.
- **Step-1 envelope prune** is wired into the labeled interval door
  `NodesByLabelPropertyDuring` (closing a rule-17 gap versus the already-pruned
  point door). It is OVERLAP-sound, so it is applied ONLY to the overlap-based During
  door — NEVER to the Relating doors, whose Allen set may include non-overlapping
  relations that an overlap prune would wrongly drop.
- **Store-level zone-map: DEFERRED.** Index-level min-from pruning already exists
  (B4 / `TemporalIndex.QueryOverlap`). A new store-global valid-time-ordered all-node
  substrate is high build surface (new capability, maintenance in every write path,
  per-backend impls) for value only rare full-graph temporal scans would see. No
  existing substrate to build on; not built.

## 3. Consequences

- History-heavy wide-entity workloads store ~39% less version history (opt-in, badger
  family). No API change; no `fv` bump; toggle-safe on an existing store.
- The temporal query surface gains the general Allen-relation door — the missing
  non-overlap-relation query — with predicate-anywhere history semantics.
- The change-log / replication / integrity / compaction invariants are all preserved
  by construction (deltas never cross the feed; hashes carried verbatim).
- Follow-up (not in this ADR): the B6 default-ON flip + a `wire_format_version` bump
  are an operational decision after a soak, deliberately deferred.
