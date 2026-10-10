# Query Planner Statistics

This page is the single reference for query-layer authors (e.g. a
Cypher planner) building cost-based decisions on top of rho-tkg — cardinality
estimates, index-usability checks, and staleness detection. Every primitive
below is reachable from `g.Stats()`, `g.Nodes()`, or `g.Rels()`. Nothing here
is a new capability; this page documents the EXISTING contract precisely
(copied from the source doc comments) plus the additive items across the WPs
that extended it: `g.Stats().RangeCardinality` (an alias of
`g.Nodes().RangeCardinality`) and `g.Stats().PropertyStats` (NDV + exact
min/max + count for a `(label, property key)` pair — see "NDV + min/max
statistics" below).

All complexities below are for the in-tree backends (`memory.Store`,
`badger.Store`, `tiered.Store`, `sharded.Store`). An out-of-tree `Store`
implementation that satisfies only `MandatoryStore` may serve some of these at
different cost, or decline the optional ones entirely — see "The capability
story" at the bottom. "Backend coverage" lists, per primitive, which in-tree
backend answers and which declines; the summary table's tiered column predates
the sharded store, so read the two together.

Version scope: this page describes v4.49.0. Doors added after the original
statistics work (4.41.0 to 4.49.0: `ReadCosts`, `ScanKeepsOrder`,
`CountByLabelAt`, `HasHistory`, `LatestStamps`, the effective timelines, the
pinned property lookup) are in "Planner doors added since 4.41.0".

## Summary table

| Primitive | Accessor(s) | Complexity (memory / badger) | Complexity (tiered) | Declines when |
|---|---|---|---|---|
| Total node count | `g.Stats().NodeCount()` | O(1) | O(open shards) | never (mandatory capability) |
| Total relationship count | `g.Stats().RelCount()` | O(1) | O(open shards) | never (mandatory capability) |
| Node count by label | `g.Stats().NodeCountByLabel(label)` | O(1) | O(open shards) | unregistered label → 0, not an error |
| Relationship count by type | `g.Stats().RelCountByType(typeName)` | O(1) | O(open shards) | unregistered type → 0, not an error |
| All label counts | `g.Stats().AllLabelCounts()` | O(distinct registered labels) | O(labels × open shards) | zero-count labels omitted from the map |
| All rel-type counts | `g.Stats().AllRelTypeCounts()` | O(distinct registered types) | O(types × open shards) | zero-count types omitted from the map |
| Node count by label + property-key presence | `g.Stats().NodeCountByLabelAndPropertyKey(label, key)` | O(1) | O(open shards) | `ErrCapabilityNotSupported` on a store without the optional capability |
| NDV + exact min/max + count | `g.Stats().PropertyStats(label, key)` | O(1) amortized; O(nodes carrying label) on a rescan after the current min/max holder is deleted | O(open shards) — per-shard HyperLogLog register-max merge, plus each shard's own independent Min/Max rescan on extremum deletion (see "Tiered NDV fold") | `ErrCapabilityNotSupported` on a store without the optional capability (sharded has none; also a graph-opened store when `Config.DisablePlannerStats` is set) |
| Numeric range cardinality | `g.Nodes().RangeCardinality(...)` / `g.Stats().RangeCardinality(...)` (alias) | O(distinct values in range), no node scan | tiered always declines (does not implement the capability); sharded sums its slots and is exact only when every slot is | `exact=false` (not an error) — see "RangeCardinality decline conditions" |
| **Relationship-side mirrors** | `g.Stats().RelPropertyStats(typeName, key)`, `g.Stats().RelPropertyTypeClassCounts(typeName, key)`, `g.Stats().RelRangeCardinality(...)` / `g.Rels().RangeCardinality(...)` (same operation), `g.Rels().ForEachByTypePropertyRangeOrdered(...)` | same shapes as their node siblings, over the REL property index | tiered declines all of them (rel property indexes are RAM-only per shard); sharded declines them except `RelPropertyTypeClassCounts`, which it sums over its slots (each relationship lives on one slot) | `ErrCapabilityNotSupported` for the two stats doors (an unpopulated `(relType, key)` pair is a zero value, not an error); `exact=false` for `RelRangeCardinality`; `ErrIndexNotFound` for the non-temporal ordered scan |
| Ordered / top-k range scan | `g.Nodes().ForEachByLabelPropertyRangeOrdered(...)` | O(k + log n) index work for a LIMIT-k top-k (RAM); disk mode is O(range) cheap-ID collection + O(k) node fetch. TEMPORAL opts: O(N log N) sound full fold (no index) | non-temporal: `ErrIndexNotFound` (tiered and sharded are not exact native stores — no ordered view); temporal opts: the same O(N log N) fold on every backend | non-temporal: `ErrIndexNotFound` when no property index / capability. Temporal opts are SERVED via the fold (no longer `ErrOrderedScanTemporal`) |
| String prefix scan (`STARTS WITH`) | `g.Nodes().ForEachByLabelPropertyPrefix(...)` / `g.Rels().ForEachByTypePropertyPrefix(...)` | O(k + log n) top-k over the ordered STRING view (RAM); node disk mode is `0x0A` `"s:"+prefix` iteration; EXACT (no over-selection). TEMPORAL opts: O(N log N) sound full fold | `ErrIndexNotFound` (no ordered view) | lex value order asc/desc, ties by id ascending; empty prefix = all strings; temporal opts SERVED via the fold |
| Outgoing / incoming degree | `g.Rels().OutgoingDegree(id, type)` / `IncomingDegree(id, type)` | O(1) via `DegreeCapability`, else O(degree) | O(1) — single-shard lookup on the node's owning shard; sharded has no `DegreeCapability`, so it takes the O(degree) fallback | never — always answers (fast path or fallback) |
| Node / relationship mutation epoch | `g.Nodes().NodeMutationEpoch()` / `g.Rels().RelMutationEpoch()` | O(1) | O(1) where supported | returns 0 (not an error) when the backend lacks the DocValues capability; the per-label `NodeLabelMutationEpoch` is badger and sharded only (0 on memory and tiered) |
| Composite (multi-key) equality lookup | `g.Nodes().ByLabelAndProperties(label, values, opts)` | O(matches) with a matching `g.Index().CreateComposite` definition; else O(label size) scan+filter | O(matches) per shard with a matching definition (every shard builds its own), folded across the shards in the query's depth; else scan+filter per shard | never errors; falls back to scan+filter when no exact-key-set definition exists (see "Composite property indexes" below) |
| Temporal property lookup | `g.Rels().ByTypeAndProperty` / `g.Nodes().ByLabelAndProperty(ies)` with a temporal filter, the named `*PropertyAt` / `*PropertyDuring` doors | O(the value's ever-members) with a declared index (property membership sidecar, built lazily once); else O(all history) | O(all history) (tiered declines the sidecar); memory, badger and sharded use it | never — without the sidecar the full-history fold answers the same |
| Pinned adjacency — bitemporal (TxAt) | `g.Rels().OutgoingForNodesAtTx(nodeIDs, type, txAt)` / `IncomingForNodesAtTx(...)` | adjacency index + O(deleted rels) fold, not a full `ByType` history scan | same adjacency-index push-down per shard | `txAt == 0` delegates to `OutgoingForNodes`/`IncomingForNodes` (no TX filter); **valid-at-now filter — drops past-valid edges**, see below |
| Pinned adjacency — belief-state (TxPin) | `g.Rels().OutgoingForNodesAtPin(nodeIDs, type, pin)` / `IncomingForNodesAtPin(...)` | adjacency index + O(deleted rels) fold; agrees with `ByType{TxPin}` filtered by endpoint by construction | same adjacency-index push-down per shard | `pin == 0` delegates to `OutgoingForNodes`/`IncomingForNodes`; a seed absent from the belief state at the pin is skipped silently (no `ErrNodeNotFound`) |

## Cardinality counters — `NodeCount` / `RelCount` / `AllLabelCounts` / `NodeCountByLabel` / `RelCountByType`

`g.Stats().NodeCount()` and `g.Stats().RelCount()` return the total current
entity count. Both in-tree single-shard backends (`memory.Store`,
`badger.Store`) maintain a live counter (an in-RAM map length for memory, an
`atomic.Int64` for badger) — O(1), no scan. `tiered.Store` folds the
reference shard + (if archived) the archive shard + every open event shard's
own O(1) count, so its cost is O(number of currently open shards), not
O(nodes).

`g.Stats().NodeCountByLabel(label)` / `RelCountByType(typeName)` are the
per-label / per-type siblings — same O(1) per-shard cost (a maintained
counter keyed by label/rel-type token), same O(open shards) tiered fold. An
unregistered label or type name is not an error: the count is 0.

`g.Stats().AllLabelCounts()` / `AllRelTypeCounts()` return a `map[string]int`
covering every REGISTERED label/type (walking the label/rel-type registry's
exported name list, token 1..N) with a per-label/per-type
`NodeCountByLabel`/`RelCountByType` call each — so the cost is
O(distinct registered labels or types), never O(total nodes/rels). Labels or
types with a current count of zero are omitted from the returned map (not
present with value 0). The returned map is always an independent copy —
mutating it never affects the graph's internal state.

## Property-key presence — `NodeCountByLabelAndPropertyKey`

`g.Stats().NodeCountByLabelAndPropertyKey(label, propertyKey)` answers "how
many current nodes carrying `label` have an indexable scalar value for
`propertyKey`" — **key-presence only, NOT value-selectivity**. It does not
tell you how many nodes have `propertyKey = X` for a specific `X`; it tells
you whether the label can satisfy a scalar-equality lookup on that key AT
ALL, and how many nodes would even be candidates. A planner uses this to
CHEAPLY prune labels that cannot satisfy a scalar equality/range predicate —
e.g. before spending an index build or a table scan on `(p:Person {ssn: $x})`,
confirm some `Person` rows actually carry an indexable `ssn` value.

"Indexable scalar value" means the same value shape the property index
accepts (numeric or a value the property-key stats counter treats as
indexable) — a node whose value for the key is a slice, map, or other
non-scalar container is NOT counted, even though the property key IS present
on that node. See the `Signal/tags` case in
`pkg/graph/store/tiered/tieredstore_property_key_counts_test.go`.

Backed by the OPTIONAL `store.NodePropertyKeyStatsCapability` — every in-tree
backend implements it (memory / badger maintain a counter per
`(label token, property key)` pair, updated on every node-mutation door and
rebuilt at index load so the count survives a restart; tiered folds
reference + archive + open event shards, same O(open shards) shape as the
other counters). A store that omits the capability declines with
`store.ErrCapabilityNotSupported` (re-exported as `graph.ErrCapabilityNotSupported`)
— check with `errors.Is`, never a string compare. This is the SAME sentinel
every optional-capability decline in this library uses — see "The capability
story" below.

Complexity: O(1) per shard on memory/badger (maintained counter, no scan);
O(open shards) on tiered. See the cross-shard fold test
`TestTieredStoreNodeCountByLabelAndPropertyKey` in
`pkg/graph/store/tiered/tieredstore_property_key_counts_test.go` — it already
covers the reference shard + archive shard + warm/hot event shards folding
into one total, so this WP does not duplicate it.

## NDV + min/max statistics — `PropertyStats`

`g.Stats().PropertyStats(label, propertyKey)` returns `store.PropertyStats`:

```go
type PropertyStats struct {
    NDV   int64 // ESTIMATED distinct-value count (HyperLogLog)
    Min   any   // EXACT minimum, scalar-ordered value families only
    Max   any   // EXACT maximum, scalar-ordered value families only
    Count int64 // same presence count NodeCountByLabelAndPropertyKey returns
}
```

It is the richer sibling of `NodeCountByLabelAndPropertyKey` above: `Count`
confirms the label can satisfy a scalar predicate on the key AT ALL; `NDV`
and `Min`/`Max` are what a cost-based planner needs next — an equality
predicate's expected selectivity is roughly `Count / NDV`, and a range
predicate outside `[Min, Max]` can be pruned to zero rows without touching
the property index at all.

### Complexity

O(1) amortized on memory/badger — the NDV sketch and min/max are maintained
incrementally on the SAME node-mutation doors as the `NodeCountByLabelAndPropertyKey`
presence counter (`PutNode`/`ReplaceNode`/`DeleteNode`/`DeleteNodeCascade`/
`Add`+`RemoveNodeLabelToken`/batch variants), and rebuilt from the persisted
node rows at badger index load — so the count/NDV/min/max all survive a
restart exactly like the presence counter does. The ONE non-O(1) case: after
the node holding the current `Min` or `Max` is deleted (or replaced with a
different value), the NEXT `PropertyStats` call for that pair pays an
O(nodes currently carrying the label) rescan to recompute an exact
replacement extremum — see "Deletion semantics" below.

### NDV — HyperLogLog sketch

Backed by an in-tree, dependency-free HyperLogLog sketch
(`pkg/graph/internal/index/hyperloglog.go`; Flajolet et al. 2007) at
precision 14 (16384 registers; standard error ≈ 1.04/√16384 ≈ 0.81%). It
starts SPARSE (a map of only the non-zero registers — cheap for the common
case of a handful of distinct values per key) and converts to a dense
`[]uint8` register array once the sparse map would cost more than the dense
array; it never converts back. Accuracy is pinned by a seeded regression
(`pkg/graph/internal/index/hyperloglog_test.go`): **relative error < 5% at
10,000 distinct values, < 3% at 100,000 distinct values.** Adding the SAME
value any number of times never moves the estimate (idempotent — the
defining property that distinguishes a cardinality sketch from a plain
counter). Sketches merge by per-register max (`HyperLogLog.Merge`) — exact
with respect to the union of both inputs' streams — which is exactly what the
tiered backend's cross-shard NDV fold uses (see "Tiered NDV fold" below).

### Min/Max value families

Only two value families participate in the EXACT min/max: **numeric** (any
of the property allowlist's `int`/`uint`/`float` types — compared via a
float64 projection, so int64/uint64 magnitudes beyond 2^53 lose exact
comparison precision at the margin, though the ORIGINAL unconverted value is
always what gets stored/returned) and **string** (ordinary Go string
comparison). Every other indexable scalar type — `bool`, `types.TemporalValue`
— still contributes to `Count` and `NDV` but leaves `Min`/`Max` at `nil`:
there is no total order defined for it in this package. A `(label,
propertyKey)` pair whose current live nodes hold ONLY unordered-family values
reports `Count > 0`, `NDV > 0`, `Min == nil`, `Max == nil`.

Mixed families for the same key (a property that holds `int64` on some nodes
and `string` on others — unusual in a well-typed graph) resolve by
first-family-wins: whichever family is observed FIRST governs Min/Max for
that key from then on; a later value from a different family is excluded
from Min/Max (still counted via `Count`/`NDV`). A rescan (see below) re-runs
this same first-observed-in-the-scan-order rule from scratch.

### Deletion semantics

**NDV never decreases.** HyperLogLog has no removal operation — a deleted
value's contribution to the sketch persists until the sketch is rebuilt from
scratch (badger index load / process restart; the memory backend has no
"rebuild" moment since it never restarts with existing data). A planner
reading `NDV` after heavy churn should treat it as "distinct values EVER
observed for currently-tracked nodes," not "distinct values held by the
CURRENT population" — the presence counter's `Count` is the exact-population
number; `NDV` is the estimator's number.

**Min/Max use "mark dirty, rescan lazily" (not eager per-delete recomputation).**
Deleting (or replacing) the node holding the current Min or Max marks the
accumulator dirty rather than immediately re-scanning every other node
carrying the label (which would make every delete pay an O(label size) cost,
even when the deleted value was never the extremum). The dirty flag is
resolved lazily: the NEXT `PropertyStats` read for that pair pays a single
O(nodes carrying the label) scan of the CURRENT live nodes to recompute an
exact new Min/Max, then caches the result until the next such deletion. This
means:

- `Count` and `NDV` are ALWAYS current as of the call (no lazy component).
- `Min`/`Max` are exact as of the call too, for a SINGLE-THREADED caller (the
  rescan happens synchronously inside the same `PropertyStats` call that
  observes `dirty == true`, never returned stale to the caller). The
  "laziness" is purely about WHEN the O(n) rescan cost is paid (deferred to
  the next read instead of every delete), not about correctness.
- **Locking differs by backend, but both return an exact Min/Max even under
  a concurrent mutation.** The memory backend holds one lock for the ENTIRE
  `NodePropertyStats` call, including the rescan (its node lookups are direct
  in-process map reads with no re-entrant locking risk) — fully atomic, no
  race window. Badger's rescan node-fetch can hit the LRU cache cold and
  needs its own brief `idxMu.RLock()` per node
  (`prefetchNodeScan`/`prefetchNodeNoFill`), so holding one lock across the
  whole call would self-deadlock (`sync.RWMutex` is not reentrant — see
  AGENTS.md "Concurrency"); badger's rescan therefore collects the current
  node values with `idxMu` released. That unlocked-collect window is guarded
  by an optimistic **write generation** (`PropertyStatsAccumulator.WriteGen`,
  bumped under `idxMu.Lock()` on every `Observe`/`Forget`): the rescan reads
  the generation under the lock BEFORE the collect and re-reads it under the
  lock BEFORE committing `Rescan`. If it moved, a concurrent mutation landed
  in the window (e.g. a `PutNode` adding a NEW live extremum), so the freshly
  collected values are stale — the rescan DISCARDS them and redoes the
  collect, bounded by `propertyStatsRescanMaxAttempts`. Without this guard the
  stale collect would overwrite the concurrent extremum and clear `dirty`,
  persisting a WRONG exact Min/Max indefinitely (a lost-update ordering bug,
  not a data race — the race detector never sees it). On retry exhaustion
  (a sustained write storm on one pair) it returns the live snapshot WITHOUT
  committing a possibly-stale rescan and leaves the pair `dirty`, so a later
  quiescent read reconciles; because `Observe` keeps Min/Max monotonically
  correct for additions, the fallback never under-reports a live extremum.
  `Count`/`NDV` are always exact/current on both backends. See lesson 63.

This choice (deferred rescan over eager per-delete recomputation) was made
because deletes are typically far more frequent than planner-stat reads in a
write-heavy workload, and the presence counter's own O(1) maintenance
already pays for tracking "does this pair still have data" — paying an
extra O(label size) walk on every delete regardless of whether it touched
the extremum would be strictly worse for that workload shape. See
`TestMemoryStoreNodePropertyStatsDeleteExtremumTriggersRescan` /
`TestBadgerStoreNodePropertyStatsDeleteExtremumTriggersRescan` for the
two-phase regression (create three nodes, confirm Max, delete the Max
holder, confirm the NEW Max reflects the survivors — repeated down to the
empty case).

### Tiered NDV fold

`tiered.Store` implements `store.NodePropertyStatsCapability` by folding
across every shard — `refShard`, `refArchive` (if open/present), and every
event shard (hot + warm + cold) — mirroring the checkout/checkin discipline
of the presence-only sibling `NodeCountByLabelAndPropertyKey`
(`tieredstore_read_bulk.go`).

`Count` folds by SUM and `Min`/`Max` fold by min-of-mins/max-of-maxes
(`index.CombineExtrema`, the same first-family-wins mixed-family tie-break
rule `Observe`/`Rescan` use — see "Min/Max value families" above). `NDV`
CANNOT fold by summing per-shard estimates — that over-counts any value
present on more than one shard — so each concrete shard exposes its RAW
HyperLogLog sketch via a store-internal (not public-contract) accessor,
`NodePropertyStatsSketch(labelToken, propertyKey) (sketch *index.HyperLogLog,
min, max any, count int64, err error)` (badger and memory), and the tiered
fold register-max `Merge`s every shard's sketch into one combined sketch,
calling `Estimate()` exactly ONCE on the result
(`pkg/graph/store/tiered/tieredstore_property_stats.go`). `Merge` returns
`ErrHLLPrecisionMismatch` if two sketches were built at different
precisions; every shard uses the same `index.DefaultHLLPrecision`, so this
cannot fire in practice, but the tiered fold PROPAGATES the error rather than
discarding it — silently ignoring a precision mismatch would silently
under-count NDV, exactly the failure the merge exists to prevent (ADR-0005
§3.1, tiered parity; that ADR file has been removed from `docs/adr/` and is
recoverable from history with `git log --all -- docs/adr/`).

Min/Max on tiered still uses the "mark dirty, rescan lazily" per-shard
behavior described above — each shard's own `NodePropertyStatsSketch` call
runs that shard's dirty-triggered rescan before returning its exact min/max,
so a delete-the-extremum on any one shard is reconciled by the NEXT
cross-shard `PropertyStats` read exactly as it would be on a single-shard
backend.

## Numeric range cardinality — `RangeCardinality`

`g.Nodes().RangeCardinality(label, propKey, min, max, inclMin, inclMax, opts)`
returns the count of `label`'s current nodes whose numeric `propKey` value
lies in `[min, max]` (inclusivity per the two bool flags), summed DIRECTLY
from the property index's sorted per-value bucket sizes — **O(distinct
values in the range), with NO node scan**. This is the fast path behind
`count(n) WHERE n.k > x` style predicates once the whole predicate can be
captured by a single range: the caller must ensure `[min, max]` captures the
WHOLE predicate before relying on the returned count as the query answer.

`g.Stats().RangeCardinality(...)` (added in this WP) is an **additive alias**
with the identical signature and identical semantics, forwarding to the SAME
core operation `g.Nodes().RangeCardinality` uses
(`core.NodeOps.RangeCardinality`) — so a planner that only imports `g.Stats()`
for its cost model does not also need the `nodes` sub-API for this one
statistic. `g.Nodes().RangeCardinality` itself is unchanged.

### RangeCardinality decline conditions

The second return value, `exact bool`, is the caller's fast-path/scan signal:
`exact == false` means the caller must fall back to a scan-and-count (e.g.
`ForEachByLabel` with a manual predicate check) because the bucket-sum answer
is either unavailable or untrustworthy. Enumerated exactly as coded in
`core.NodeOps.RangeCardinality` / `pkg/graph/internal/index/property_index_rangecount.go`:

1. **The graph is closed.** `RangeCardinality` on a closed graph returns
   `(0, false, graph.ErrGraphClosed)` — this is the one decline condition that
   is ALSO a sentinel error (every other decline below returns `err == nil`).
2. **Any temporal coordinate is set** — `opts.ValidAt`, `opts.ValidStart` /
   `opts.ValidEnd`, `opts.TxAt`, or `opts.TxPin` (all four checked; a
   knowledge-time pin declines exactly like a valid-time filter). The property
   index's bucket sizes are valid-time AGNOSTIC (built over current-state
   membership only), so a temporal predicate always declines — `(0, false, nil)`.
3. **The store lacks the native capability** — no type assertion to the
   internal `NodeRangeCardinality(...)` scanner interface succeeds. Every
   in-tree backend implements it; an out-of-tree store need not.
4. **The label is unregistered.** `(0, false, nil)` — the caller's scan would
   find zero rows anyway, so this is a cheap equivalent decline, not an error.
5. **No property index exists yet for `(label, propKey)`**, or the index
   exists but is **poisoned** — it CURRENTLY holds an integer magnitude past
   `2^53`, where float64 sort keys can collide with a neighboring value and
   the bucket sum would silently miscount. While poisoned, the index declines
   for every range query, not just queries touching the large value. The
   poison is a count of such entries (`numImpreciseCount`, BACKLOG 16j), so it
   lifts once every such value has been removed or updated away; the one
   exception is the corruption-path purge sweep, which never decrements it
   (staying imprecise is the safe direction). On a sharded store one poisoned
   slot makes the whole sum inexact.

Fractional values and fractional bounds are counted EXACTLY when `exact ==
true` — there is no separate "approximate but exact enough" state; it is
either an exact bucket-sum count or a full decline.

## Ordered / top-k range scan — `ForEachByLabelPropertyRangeOrdered`

`g.Nodes().ForEachByLabelPropertyRangeOrdered(label, propKey, min, max,
inclMin, inclMax, desc, opts, fn)` is the CONTRACTUAL ordered access path — the
one door that serves `ORDER BY n.propKey [ASC|DESC] [LIMIT k]` from the index
instead of a materialize-and-sort. It streams the label's nodes whose numeric
`propKey` value lies within `[min, max]` to `fn` in **value order**, and `fn`
returning `false` stops the scan.

### Ordering contract

- **Value order**: ascending by the numeric value, or descending when `desc`
  is true. All numeric widths (int/uint/float, any size) share one ordered
  domain — an `int64(5)`, a `uint64(5)` and a `float64(5.0)` sort to the same
  position (magnitude `5.0`).
- **Ties by node ID ASCENDING**, in BOTH directions. Two nodes with equal
  values are emitted in ascending snowflake-ID order whether the scan is
  ascending or descending — the tie-break never flips with `desc`.
- **Over-selecting candidate filter** (same contract as
  `ForEachByLabelPropertyRange`): the door widens the bounds by one ulp and
  NEVER skips a boundary bucket, because an `int64` magnitude past `2^53`
  collapses onto a neighbouring `float64` sort key (the exact-value /
  float-precision caveats). So `fn` receives CANDIDATES and MUST re-check the
  predicate with exact comparison semantics — including the `inclMin`/`inclMax`
  inclusivity, which the door itself does not apply.

### Compiling `ORDER BY ... LIMIT k`

A query layer compiles `MATCH (n:Label) WHERE n.p >= lo AND n.p <= hi RETURN n
ORDER BY n.p ASC LIMIT k` to:

```go
kept := make([]*types.Node, 0, k)
err := g.Nodes().ForEachByLabelPropertyRangeOrdered(
    "Label", "p", lo, hi, true, true, /*desc=*/false, storepkg.QueryOpts{},
    func(n *types.Node) bool {
        v := /* exact numeric value of n.p */
        if v < lo || v > hi { return true } // over-selected candidate: skip
        kept = append(kept, n)
        return len(kept) < k // stop once we have k rows -> LIMIT pushdown
    })
```

Because `fn` returns `false` the moment it has `k` rows, the LIMIT is pushed
into the index: the scan seeks (O(log n)) then walks only the first `k`
in-range candidates, so the top-k costs **O(k + log n)** index work and
materializes only `k` nodes — never the whole range. `ORDER BY ... DESC LIMIT
k` is the same call with `desc = true`.

### Complexity

- **Memory / badger (RAM ordered view)**: fully lazy and paged. A top-k walks
  seek + O(k) plus a small constant page slack (the ordered view is snapshotted
  a page at a time under the index lock so `fn` can run lock-free and even call
  back into the store). No full-range collection.
- **Badger (`PropertyIndexOnDisk`, the `0x0A` keyspace)**: the ordered
  candidate IDs are collected up front in value order (cheap 8-byte IDs,
  pending-write overlay merged), then node materialization is streamed with the
  SAME `fn`-driven early stop — so the expensive per-node decode still stays
  bounded by what `fn` consumes, even though the ID collection is O(range).
- **Benchmark** (`bench/ordered_topk_test.go`, `BenchmarkOrderedTopK`, top-10
  by value over 100k distinct values; figures recorded when K3a landed in
  v4.13.0, 2026-07-11, not re-measured since): the ordered arm vs the pre-K3a
  collect-then-limit shape (a full `ByLabel` scan sorted by value, truncated
  to k):

  | Backend | ordered top-10 | collect-then-limit | speedup |
  |---|---|---|---|
  | memory | ~17 µs | ~354 ms | ~20,000× |
  | badger | ~45 µs | ~575 ms | ~13,000× |

### Two paths: index fast path (current-state) + temporal fold

With NO temporal `QueryOpts`, the ordered door reads the LIVE current row set
from the valid-time-agnostic ordered property view — the O(k + log n) top-k fast
path above.

With a TEMPORAL `QueryOpts` (`ValidAt` / `ValidStart`+`ValidEnd` / `TxAt` /
`TxPin`) the door instead serves a SOUND FULL FOLD: every label/type member is
resolved to its version AT THE PIN (via the same chain resolver + B4 valid-time
prune as the temporal `ByLabel`/`ByType` door), the value predicate is applied to
the value-AT-t, then the survivors are sorted by that value. This is the only
sound answer — the current-state index would both MISS a node in range then but
not now AND over-report the reverse. It is O(N log N) in the label/type's temporal
membership (value-at-t is not indexed, so no early-stop by value), and it needs NO
property index (it reads resolved values directly). Prefix scans (node + rel)
share the same fold. Previously this was declined with `graph.ErrOrderedScanTemporal`
(now a legacy, no-longer-returned sentinel).

### Declines

- `graph.ErrIndexNotFound` — NON-temporal scan with no property index for
  `(label, propKey)`, the store lacks the ordered-scan capability, or the store is
  not an exact native store (tiered and store wrappers decline; callers fall back
  to a label scan + sort). An unregistered label is a cheap `nil` (no rows), not an
  error. The TEMPORAL fold path does not require an index and never returns
  `ErrIndexNotFound`.

## Degree — `OutgoingDegree` / `IncomingDegree`

`g.Rels().OutgoingDegree(nodeID, typeName)` / `IncomingDegree(nodeID, typeName)`
return the number of outgoing/incoming relationships for a node, optionally
type-filtered (`typeName == ""` means all types). Backed by the OPTIONAL
`store.DegreeCapability` — when the store implements it, the answer comes
from the adjacency index's entry count with NO relationship materialization
(O(1)); without it, the graph layer falls back to
`len(Outgoing(...))`/`len(Incoming(...))`, which is O(degree) (it must
materialize and validate every adjacent relationship row). Every in-tree
backend implements `DegreeCapability`. An unregistered `typeName` is not an
error for a nonexistent node/type combination — the same "declines to zero"
shape as the label/type counters, except that a genuinely missing `nodeID`
still surfaces `store.ErrNodeNotFound` (existence is always checked; only the
type-filter's absence is silent).

## Staleness — `NodeMutationEpoch` / `RelMutationEpoch`

`g.Nodes().NodeMutationEpoch()` and `g.Rels().RelMutationEpoch()` return a
monotonically increasing `uint64` counter that advances on every node (resp.
relationship) mutation the backend's DocValues capability tracks — property
edits included, not only label/index changes (a lesson from the columnar
DocValues work: a property-only `Update` never touches the label index, so an
epoch hooked only to label-index writes would silently serve a stale
snapshot). A caller that took a lock-free snapshot (e.g. via
`ForEachDocValues`/`DocValuesSnapshot`) re-reads the current epoch after
consuming the rows and discards the result if it no longer matches the
snapshot's epoch — the standard optimistic-concurrency "Gate 2" check used
throughout the columnar aggregation path. `NodeMutationEpoch`/
`RelMutationEpoch` return `0` (not an error) when the backend lacks the
DocValues capability — a planner reading `0` on every call cannot distinguish
"never mutated" from "unsupported"; check the corresponding DocValues call's
`ok` return if that distinction matters.

What moves `RelMutationEpoch` (since v4.41, on memory, badger, tiered and
sharded): every write of a relationship's row — create, a property set or
removed (`SetProperty`, `DeleteProperty`, `CompareAndSetProperty`, `Update`,
`UpdateInPlace`, their tx and batch forms), `CloseVersion`,
`SetRelVersionInterval`, a version-history write — and every removal — `Delete`,
a node delete that cascades, a transaction rollback, retention purge, exact
erasure, `Clear`. Before v4.41 a property write moved it on no backend and a
delete moved it on memory only. On the tiered store both epochs are store-wide
counters every shard advances, so a write to a cold shard that is closed again
before the next read still moves them.

`g.Nodes().NodeLabelMutationEpoch(label)` is the PER-LABEL sibling: it advances
only when a node carrying THAT label is written, and it is the value a
single-label DocValues result returns as its `gen`. A Gate-2 re-check on a
single-label aggregate should use it rather than the global
`NodeMutationEpoch`, which an unrelated-label write also advances — otherwise a
still-valid result is discarded. Same `0`-means-`0`-or-unsupported caveat (an
unknown label also returns `0`). Only badger and sharded (which sums its slots'
epochs) implement it: on memory and tiered it returns `0` always, so a planner
on those backends must use the global `NodeMutationEpoch`.

## Composite property indexes — `CreateComposite` / `ByLabelAndProperties`

`g.Index().CreateComposite(label, keys)` builds an index over an ORDERED
tuple of 2–4 declared property keys under one label; `g.Nodes().ByLabelAndProperties(label,
values, opts)` is its query door — `values` is a `map[string]any` supplying a
value for every key an index of the same declared key SET was built over
(order-independent from the caller's side — a Go map has no order).

### When a composite index beats a single-key index + post-filter

A single-key property index (`g.Index().CreateProperty`) narrows a scan to
`(label, oneKey) = value` — a second predicate on a different key is still a
POST-FILTER over that candidate set. This is fine when the first key is
already fairly selective. It stops being fine when the **first key is
UNSELECTIVE** (e.g. `status = "active"` matches 90% of rows) but the FULL
predicate (`status = "active" AND region = "eu-west-1"`) is selective — a
single-key index on `status` still has to fetch and post-filter 90% of the
label's rows. A composite index on `(status, region)` answers the same query
in O(matches) because the SECOND key is folded into the index's key space
instead of a post-filter. See `bench/composite_index_test.go`'s
`BenchmarkCompositeLookupVsSingleIndexPlusFilter` for exactly this shape (a
100k-node fixture where the first key is deliberately unselective — 5
distinct values, 20 % of the label each — and the composite pair with a
50-value second key is selective).

Conversely, do NOT reach for a composite index when the first key ALONE is
already selective enough — the single-key index + post-filter is simpler,
cheaper to maintain (composite index entries also cost RAM), and answers the
same query with the same big-O once the candidate set from the first key is
already small.

### v1 scope (equality-only, RAM-only, node-only)

- **Equality-only.** `values` must supply an exact value for EVERY key the
  matching definition declares — there is no partial-prefix lookup (querying
  a subset of a definition's keys) and no range semantics on any component.
  A query whose key SET does not exactly match any registered definition
  still answers correctly (see "Mandatory fallback" below) — it just isn't
  accelerated.
- **RAM-only.** Composite index ENTRIES always live in memory on every
  in-tree store (`memory`, `badger`, and the badger shards of `tiered` and
  `sharded`) — there is no on-disk mode analogous to
  `badger.Config.PropertyIndexOnDisk`. DEFINITIONS (label + declared key
  list) are persisted on `badger.Store` so a reopen rebuilds the same
  definitions by re-scanning current node state (same shape as the
  single-key property index's own RAM-mode rebuild). A store with many
  large composite indexes should budget RAM accordingly; on-disk composite
  entries are a documented follow-up.
- **Node-only.** There is no composite index over relationship properties.
  (Single-key relationship property indexes do exist —
  `RelPropertyIndexCapability`, `g.Index().CreateRelProperty` — and back the
  rel-side doors above; only the multi-key composite is node-only.)
- **Tiered builds it per shard (backlog 10).** `tiered.Store` fans the
  definition out to every shard (reference, archive, every event shard, and
  the hot shard a later rotation opens) and folds the per-shard matches;
  event labels are allowed. Each shard keeps its entries in RAM and rebuilds
  them when it opens (~310 B per indexed node with distinct tuples).

### Proving the accelerated path exists — `HasComposite` / `ListComposites`

`g.Index().HasComposite(label, keys) (bool, error)` is an ORDER-INSENSITIVE
key-SET match, exactly the rule `ByLabelAndProperties` uses to choose index over
scan, so a planner can check BEFORE routing a multi-property equality through
the door (routing blindly regresses the single-key-index case to a scan plus
post-filter). `ListComposites(label) ([][]string, error)` returns every declared
tuple in its declared order (distinct orderings of one key set are distinct
definitions and are both listed). O(definitions on the label); there is no
DDL-epoch signal, so call per plan rather than caching across DDL you do not
control. Backed by `store.CompositeIndexIntrospectionCapability` (memory,
badger, tiered since 4.45.0, sharded); a store without it declines with
`ErrCapabilityNotSupported`.

### Key-set identity, not key-order identity

A composite index's identity is `(labelToken, DECLARED KEY ORDER)` — creating
`["first", "last"]` and `["last", "first"]` under the same label are TWO
distinct definitions (both usable; a query's key SET, not order, decides
which definition accelerates it, since `values` is an unordered map). This
is deliberate: the on-disk/in-RAM entry key is built by concatenating each
component's canonical value key IN THE DECLARED ORDER, so two orderings of
the same key set produce different entry-key spaces even for the same
underlying node.

### Collision-free key concatenation

Composite entry keys are built with a LENGTH-PREFIXED concatenation
(`indexpkg.EncodeCompositeKeyTuple`): each component's canonical value-key
byte length is written before its bytes. A naive plain-concatenation or
single-separator join can alias two DIFFERENT ordered key lists onto the
same encoded string — e.g. `["ab", "c"]` and `["a", "bc"]` both
plain-concatenate to `"abc"` — which would silently merge two distinct
composite tuples (or two distinct index DEFINITIONS, since the same scheme
also encodes a definition's declared key names) onto one map slot.
Length-prefixing is a standard bijective encoding: parsing back (read a
4-byte length, then that many bytes, repeat) is always unambiguous, so no
two distinct ordered lists can ever collide. See
`TestEncodeCompositeKeyTupleCollisionBattery` in
`pkg/graph/internal/index/composite_property_index_test.go` for adversarial
inputs chosen so a naive scheme WOULD collide.

### Float equality semantics

Composite equality SUPPORTS floats, using the exact same lesson-25
bit-pattern semantics `types.IndexablePropertyValueKey` already applies to
the single-key property index (`+0`/`-0` collapse to one key, `NaN` is
pinned to one key, `float32` and `float64` holding the same magnitude stay
distinct types). This is a deliberate departure from the unique-constraint
precedent (`ErrUniqueUnsupportedType` for float keys/values) — a composite
index is an EQUALITY accelerator, not a business-identity constraint, so
exact bit-pattern equality is a sound, unsurprising definition; unique
constraints have the additional (here irrelevant) concern that two writers
might mean "the same value" despite differing bit patterns.

### Mandatory fallback — correctness without the capability

`CompositePropertyIndexCapability` is NOT part of the `Store` composed
interface (unlike the single-key `PropertyIndexCapability`, which IS
embedded) — a backend can satisfy `MandatoryStore`/`Store` fully while
omitting it entirely. `g.Nodes().ByLabelAndProperties` therefore has a
graph-layer fallback (mirroring `ByLabelAndProperty`'s own fallback) that
scans `NodesByLabel` and applies `indexpkg.NodeMatchesAllProperties` — the
SAME AND-conjunction predicate the backend's own internal scan-and-filter
and the accelerated index path all agree on — as a pure post-filter. Every
in-tree backend (`memory`/`badger`) ALSO applies this same fallback
internally whenever no composite index exists for the requested key set, so
the query is correct on every backend at every point, with or without an
index.

## Pinned adjacency — `OutgoingForNodesAtTx` / `OutgoingForNodesAtPin` (and incoming mirrors)

Both families expand a batch of seed nodes to their relationships *as the graph
was recorded at a transaction-time pin*, resolving each candidate through the
adjacency index (live per-node adjacency UNIONED with the deleted-relationship
fold, since rel endpoints are immutable) rather than a full history-aware
`ByType` scan. They differ ONLY in how they treat VALID time — and picking the
wrong one is the exact footgun that motivated the belief-state door:

- **`OutgoingForNodesAtTx(nodeIDs, type, txAt)` / `IncomingForNodesAtTx(...)` —
  bitemporal.** Agrees with the `QueryOpts{TxAt: txAt}` scan door filtered by
  endpoint. When no valid-time opts are set, the `TxAt` arm applies a POINT
  valid-time probe at **now** (the later of the wall clock and the graph's
  transaction clock — never earlier than a version the graph recorded), so an
  edge whose valid interval lies wholly
  in the past is SILENTLY DROPPED even though it was believed at `txAt`:
  a `CloseVersion`-ed edge, or a width-1 `[t, t+1)` point-event edge (the
  standard point-event encoding). Use this only when you genuinely want
  "believed at `txAt` AND still valid now". `txAt == 0` delegates to the
  plain current-state door.

- **`OutgoingForNodesAtPin(nodeIDs, type, pin)` / `IncomingForNodesAtPin(...)` —
  belief-state (AS-OF-SYSTEM-TIME).** Pure knowledge-time resolution with NO
  valid-time filtering; agrees with `ByType(QueryOpts{TxPin: pin})` filtered by
  endpoint BY CONSTRUCTION (both funnel through the same as-of resolver —
  `findRelVersionForOpts`'s `TxPin` arm → `relAsOfLocked` → the chain resolver +
  `storeutil.SelectAsOf`). It returns EVERY edge believed at the pin regardless
  of valid time: past-valid facts, point events, and unset-`valid_from`
  (snowflake-fallback) edges alike. An edge hard-deleted after the pin is still
  visible (delete is a transaction-time tombstone); one created after the pin is
  invisible; a backfilled edge (`AddWithTx`) is visible from its backfilled
  `TxFrom` onward. `pin == 0` delegates to the plain current-state door.

**Which to use:** for reconstructing a historical knowledge state
(AS-OF-SYSTEM-TIME `$pin`), always use the `*AtPin` doors — the `*AtTx` doors'
valid-at-now filter will silently drop point events and closed intervals. The
`*AtTx` doors remain for callers who explicitly want the bitemporal "recorded by
`txAt` and valid now" intersection.

**Seed tolerance (AtPin only):** unlike the current-state and `*AtTx` doors —
which hard-error `ErrNodeNotFound` on a seed that is absent from CURRENT state —
the `*AtPin` doors tolerate a seed that was part of the belief state at the pin
but was HARD-DELETED afterwards. Such a seed's live adjacency entries were purged
by the delete cascade, so it is excluded from the store's live-adjacency probe
(which would otherwise error), but its pre-delete edges — themselves
cascade-deleted, hence present in the deleted-relationship fold and still naming
the seed as their (immutable) endpoint — are recovered and returned. A seed that
never existed at the pin (or was created only after it) contributes nothing and
is skipped silently, matching `ByType{TxPin}` filtered by endpoint, which simply
has no entry for such a node. Seed IDs are still format-validated, so a
zero/invalid ID is rejected with `ErrInvalidStoreMutation`.

Both families push the live-adjacency probe down per shard on `tiered.Store`
(cross-shard endpoints supported) and fold in deleted relationships via the same
`DeletedIterationCapability` the single-node `OutgoingRelsAt`/`IncomingRelsAt`
doors use.

## Temporal property lookups — the property membership sidecar

A property lookup with a temporal filter — `g.Rels().ByTypeAndProperty` /
`g.Nodes().ByLabelAndProperty` / `ByLabelAndProperties` with `TxPin`, `TxAt`,
`ValidAt` or `ValidStart`+`ValidEnd`, and the named
`g.Temporal().RelsByTypePropertyAt` / `RelsByTypePropertyDuring` /
`NodesByLabelPropertyAt` / `NodesByLabelPropertyDuring` — cannot answer from
the property index alone: the index holds CURRENT values, and a rel that
carried the value in an older version (or was deleted) matches at a pin
before the change. Each candidate is resolved through the same chain resolver
as every other temporal door and the predicate is re-checked on the resolved
version; the question is only which candidates to resolve.

| Store | Candidates | Cost |
|---|---|---|
| memory, badger, sharded, with a DECLARED index on `(type, key)` / `(label, key)` | current index matches ∪ the value's ever-members from the store's property membership sidecar, minus members whose earliest row carrying the value was recorded after the effective pin (`TxPin`, else `TxAt`; a pure valid-time read prunes nothing) | O(rels that ever carried the value × their versions) |
| no declared index, tiered, a wrapper store | current matches ∪ every ID with a history row (the full fold) | O(all history of the graph) |

Both rows return the same answer (the sidecar is a sound superset; the
resolver rejects the rest). `ByLabelAndProperties` intersects the sidecar
sets of the keys that have a declared single-key index; keys without one
constrain nothing.

The sidecar (`store.RelPropertyTxMembershipCapability` /
`NodePropertyTxMembershipCapability`) maps `(scope, key, value)` to every
entity whose rows ever carried the value — under the type, or for nodes under
the label IN THE SAME ROW — with the lowest TxFrom of those rows. It is
append-only (a later value change, delete, truncation or compaction never
removes a posting), RAM-only and LAZY: built on the first temporal lookup for
the `(scope, key)` after open (O(history) once: one scan of the current and
history rows), then recorded at every row write. Clear / `Admin().Reset()`,
an index drop, retention purge and exact erasure drop the sidecars; the next
lookup rebuilds them. On badger the build does not hold the write lock: it
turns recording on, captures the write buffer, scans the committed keyspaces
and installs the scan only if no drop happened meanwhile.
`PropertyTxMembershipStats()` (store capability) reports built sidecars,
postings, builds and summed build time.

A depth-scoped read (`QueryOpts.Depth` other than `DepthAll`) on a store with
depth-aware history (tiered) keeps the depth fold. A wrapper store that merely
embeds a native store is forced to the full-history fold, as for K1 (below).

Measured (backlog 8, `BenchmarkPinnedRelPropertyLookup`, 32-core x86, 2026-10;
100 k relationships, 200 matching, 20 % revised and 5 % deleted): pinned
lookup 227 ms to 0.13 ms on badger, 13.8 ms to 0.095 ms on memory, 221 ms to
0.18 ms on sharded; the lazy build of one sidecar at 1 M relationships is
0.62 s (memory) to 3.7 s (badger). Source: CHANGELOG `[4.48.0]`, "Temporal
property lookups cost the value's ever-members, not the history of the
graph".

## Backend coverage (v4.49.0)

Which in-tree backend answers a primitive natively and which declines. Read from
the method sets of `memory.Store`, `badger.Store`, `tiered.Store` and
`sharded.Store` at v4.49.0; "no" is a decline or a slower fallback as stated in
the primitive's own section.

| Primitive | memory | badger | tiered | sharded |
|---|---|---|---|---|
| Counters, `NodeCountByLabelAndPropertyKey`, node `PropertyTypeClassCounts` | yes | yes | yes | yes |
| Node `PropertyStats` (NDV, min/max) | yes | yes | yes | no |
| `RelPropertyStats` | yes | yes | no | no |
| `RelPropertyTypeClassCounts` | yes | yes | no | yes |
| `RangeCardinality` (node) | yes | yes | no | yes |
| `RelRangeCardinality` | yes | yes | no | no |
| Ordered / prefix scans, non-temporal (node and rel) | yes | yes | no | no |
| O(1) degree (`DegreeCapability`) | yes | yes | yes | no (O(degree)) |
| Composite index, introspection | yes | yes | yes | yes |
| Valid-time envelope prune: node temporal index / rel temporal index | yes / yes | yes / yes | no / yes (hot + warm shards) | yes / yes |
| K1 label and rel-type transaction-time membership | yes | yes | no | no |
| Property transaction-time membership (pinned property lookup) | yes | yes | no | yes |
| `Stats().HistoryCounts()` | yes | yes | no (`ok=false`) | yes |
| `HasHistory`, `LatestStamps` store capabilities | yes | yes | yes | yes |
| `Stats().ReadCosts()` | yes | yes | yes | yes |
| Column scans (`ScanNodeColumns`, `ScanRelColumns`) | yes | yes | `ok=false` | `ok=false` |
| `ScanKeepsOrder` true | yes | yes (RAM label index) | no | no |
| DocValues snapshots | yes | yes | yes | yes |
| `NodeLabelMutationEpoch` | no (0) | yes | no (0) | yes |
| `RelTypeDegreeStats` from the adjacency index | no (streams once) | yes | no (streams once) | no (streams once) |

A wrapper store that embeds an in-tree store inherits its optional
capabilities, except that the graph layer uses the transaction-time membership
sidecars only from exact native stores (K1) and from exact native stores or the
sharded store, which implements the property sidecar directly: a wrapper that
merely embeds a native store takes the full-history fold. The graph layer, not
`store.CapabilitiesOf`, makes that decision (the latter is a structural probe
for diagnostics).

## How temporal options are answered

Every door below takes `QueryOpts`. The coordinates are `ValidAt`,
`ValidStart`+`ValidEnd` (an interval filter only when both are positive; a lone
bound is no filter), `TxAt` and `TxPin`. `TxPin` is a pure belief-state pin: it
answers "the newest row recorded by the pin" (since 4.46.0, which after a
bounded cascade is the corrected slice, not the current row), filters no valid
time, and combined with any valid-time coordinate or `TxAt` is refused with
`ErrConflictingTemporalOpts`. `TxAt` alone is bitemporal and applies a point
valid-time probe at now (the later of the wall clock and the transaction
clock). For "the state at valid time t as recorded at the pin" use `ValidAt` +
`TxAt`, `NodeAtTx` / `RelAtTx`, or the effective timelines below.

| Door | How a temporal coordinate is answered | Exact? |
|---|---|---|
| `ByLabel` / `ByType`, `CountByLabelAt` / `CountByTypeAt`, `ForEachByLabel` / `ForEachByType` | candidate set = current members united with the label's (type's) ever-members from the K1 sidecar where the backend has it (memory, badger; members whose earliest acquisition is after the effective pin, `TxPin` else `TxAt`, are pruned), else the full-history fold; a declared temporal index (`CreateTemporal` / `CreateRelTemporal`) prunes candidates by valid-time envelope; the chain resolver decides each candidate | yes; cost O(candidates x chain) |
| `ScanNodeColumns`, `ScanRelColumns` (a type or `""`), `ForEachByLabelPropertyRange`, `ForEachByTypePropertyRange` | since 4.44.0 the same fold and resolver as `ByLabel` / `ByType`: each version's values, `ValidFrom` / `ValidTo`, history included (deleted entities, a label held only on an earlier version); before 4.44.0 they filtered the current rows by valid time and ignored `TxAt` / `TxPin`. The range doors serve a temporal opt without a property index, in ID order, with the bounds applied inclusively for `fn` to re-check; under `ValidStart`+`ValidEnd` they test the value of the version `ByLabel` resolves (the most recent overlapping one), whereas `ByLabelAndProperty` matches a value held anywhere in the interval | yes; cost = `ByLabel` with the same opts |
| `ForEachByLabelPropertyRangeOrdered`, `ForEachByLabelPropertyPrefix` (and rel mirrors) | sound full fold, O(N log N), no index needed, any backend; `fn` re-checks inclusivity | yes |
| `RangeCardinality`, `RelRangeCardinality` | any coordinate declines: `(0, false, nil)` | n/a |
| `ByTypeAndProperty`, `ByLabelAndProperty(ies)`, named `*PropertyAt` / `*PropertyDuring` | property membership sidecar (see "Temporal property lookups") | yes |
| `OutgoingForNodesAtTx` / `AtPin` and incoming mirrors | adjacency index plus deleted-relationship fold (see "Pinned adjacency") | yes |
| `NodeEffectiveTimeline(id, pin)` / `RelEffectiveTimeline(id, pin)`, `ForEachNodeEffectiveByLabel(label, pin, fn)` / `ForEachRelEffectiveByType(type, pin, fn)` | see "Planner doors added since 4.41.0" | yes |

K1 (`store.LabelTxMembershipCapability`, `RelTypeTxMembershipCapability`) is an
append-only sound superset kept in RAM, built lazily on the first temporal read
after open (O(history) once) and maintained at every row write; removing a label,
deleting the entity or compacting never drops a member. A member with an unset
(0) first `TxFrom` is never pruned (4.48.0). Tiered, sharded and wrapper stores
take the full fold, O(everything that ever had history).

Tiered and temporal indexes: since 4.45.0 tiered builds composite and relationship
temporal indexes per shard (each shard keeps its own badger index over its own
rows; the reference shard anchors the definitions). The relationship temporal
index is bounded to hot and warm shards by default (a cold shard keeps none and a
cold shard's relationships are never pruned, so answers are unchanged); resident
cost measured at 4.45.0: about 156 B per indexed relationship, 310 B per node of
a 3-key composite, both on every shard carrying the index.

## Planner doors added since 4.41.0

| Door | What it states | Cost and backends |
|---|---|---|
| `g.Stats().ReadCosts() (store.ReadCosts, ok bool, err)` | the backend kind and nominal nanoseconds per row for a held (decoded in RAM) and a decoded (read from the store) row: `LendHeld/Decoded`, `ScanRowHeld/Decoded`, `AdjacencyRelHeld/Decoded`, plus `HeldRows` (-1 every row, 0 a byte budget governs, n rows per kind, per shard on tiered and sharded). Figures are medians of `BenchmarkReadCosts` (one CPU, Apple M4 Max, 4,000 and 20,000 nodes), rounded to two digits: magnitudes for pricing access paths, not a promise for any machine | O(1); all four in-tree stores; `ok=false` for a store without `store.ReadCostCapability`; a wrapper inherits |
| `g.Nodes().ScanKeepsOrder(label)` / `g.Rels().ScanKeepsOrder(type)` | whether a streaming `ForEachByLabel` / `ForEachByType` walks a kept ascending-ID list (no per-scan collect and sort). Charge a scan's sort only where it is false. False for a temporal scan, badger with `LabelIndexOnDisk`, tiered, sharded, external stores, and (rels, memory) a declared segment type | O(1); true on memory and badger only |
| `g.Nodes().CountByLabelAt(label, opts)` / `g.Rels().CountByTypeAt(type, opts)` | `len(ByLabel(label, opts))` without building it; same options, validation and errors as `ByLabel` (`After` and `Limit` honoured); an unknown label counts 0 | O(1) with no temporal filter and no paging (the label counter); `TxPin` alone can hit a cached as-of column; otherwise the candidate fold, no materialisation |
| `g.Stats().HistoryCounts() (store.HistoryCounts, ok bool, err)` | how many nodes and relationships have history rows (what a temporal property lookup without a sidecar resolves, beside the current matches) | exact, no pass per call; memory, badger, sharded; tiered `ok=false` |
| `g.Nodes().HasHistory(id)` / `g.Rels().HasHistory(id)` | whether an entity has a history row, equal to `len(History(id)) > 0` at every moment, without reading rows; an effective-state read uses it to skip `History` when the current row answers | memory map; badger RAM set built once by a key-only scan; tiered and sharded route; a store without the capability falls back to `History` |
| `g.Nodes().LatestStamps(id)` / `g.Rels().LatestStamps(id)` `(txFrom, txTo, deleted, err)` | the entity's newest transaction stamps over ALL rows (current, every history row, the tombstone): the largest `TxFrom`, the largest `TxTo` or `DeletedAt` (0 when no row was ended), and whether the entity has rows but no current row. Equals the fold of `Get` + `History` at every moment. Unknown ID: `ErrNodeNotFound` / `ErrRelNotFound` | measured at 4.49.0 (32-core x86, `tasks/evidence/latest-stamps/`): 31-83 ns, 0 allocs on memory and badger against 6.4 ms (memory) and 36 ms (badger) for the `History` scan at 10,000 versions; sharded and tiered allocate per call (0.45-1.6 us); a store without `store.HistoryStampsCapability` is folded from `History`. Badger builds a RAM sidecar lazily (66-85 B per ID with history) |
| `g.Temporal().NodeEffectiveTimeline(id, pin)` / `RelEffectiveTimeline(id, pin)` | the entity's state over valid time as recorded at the pin: ascending, half-open, non-overlapping segments, gaps omitted; for every instant t the segment containing t holds the row `NodeAtTx(id, t, pin)` returns. `ValidFrom` is the effective start (never 0); a deleted entity's last segment ends at the delete instant; created after the pin returns `nil, nil`. DECLARED view (not masked by endpoint validity). Errors include `ErrInvalidTimeRange` (pin <= 0), `ErrTxPinTooNew`, `ErrHistoryCompacted`, `ErrRetentionExpired` | one resolution per entity instead of `Get` + `History` + `RelAtTx` per row bound |
| `g.Temporal().ForEachNodeEffectiveByLabel(label, pin, fn)` / `ForEachRelEffectiveByType(type, pin, fn)` | the same segments for every entity that carried the label (type) in a row recorded by the pin, deleted-before-the-pin entities included (which `ByLabel{TxPin}` drops); at every instant t the segments containing t equal `ByLabel(label, {ValidAt: t, TxAt: pin})`. Entity order unspecified, one entity's segments contiguous and ascending | one scan; `fn` runs without graph locks |
| `g.Temporal().CommittedTx()` | a pin at which every write is committed (the open transaction's `StartInstant`, else a fresh `NowTx`), so a pinned read sees no uncommitted transaction write and repeats. Not covered: a privileged backfill and a standalone mutation in flight | O(1) |
| `g.Stats().RelTypeDegreeStats(type)` | largest out- and in-degree of a type's relationships (`MaxOut/MaxIn` with the smallest node ID holding each), the relationship, start and end counts; `""` is every type. Hub-aware input for an adjacency cost (mean out-degree = Rels / Starts). `Exact` is false when a writer moved the epoch across the bounded retries or the store has no epoch | derived, not maintained: the first call after a write to the type is O(rels of the type) (badger answers from the adjacency index), then O(1) until the relationship mutation epoch moves |
| `g.Stats().PropertyTypeClassCounts(label, key)` / `RelPropertyTypeClassCounts(type, key)` | the EXACT partition {Numeric, NaN, String, Bool, Other, Missing} of the label's current nodes by the type class of the key's value; `Present()` = Numeric+NaN+String+Bool+Other; Missing is `NodeCountByLabel` minus present. "Every present value is orderable-numeric" is `Numeric == Present()`, an O(1) gate that replaces an O(distinct values) `RangeCardinality(-inf, +inf)` probe | O(1); exactness is a correctness guarantee (same choke point as the presence counter); see "Backend coverage" and `Config.DisablePlannerStats` |
| `g.Index().ListTemporal()`, `ListRelTemporal()`, `HasRelTemporal(type)` | the declared temporal indexes, so a planner can tell whether valid-time envelope pruning applies (badger persists relationship-type temporal indexes since 4.42.0) | O(definitions) |
| `g.Nodes().DocValuesColumn(label, key) (store.DocValuesColumn, ok bool, err)` | what `ForEachDocValues` / `DocValuesSnapshot` would build for the key (numeric, string, or none: mixed values, a bool/list/map/struct, an empty or over-cap label, no column path), derived from the exact class counters without building it; valid until the next node write | `ok=false` when the store keeps no counters |

## The capability story for external stores

Every OPTIONAL statistics primitive above (`NodeCountByLabelAndPropertyKey`,
`PropertyStats`, the RangeCardinality fast path, `DegreeCapability`) is
backed by a `Store` capability interface declared in `pkg/graph/store`
(`capabilities.go` for `NodePropertyKeyStatsCapability` and
`DegreeCapability`, `property_stats.go` for `NodePropertyStatsCapability`;
the RangeCardinality scanner is an unexported interface asserted in
`pkg/graph/internal/core/queries.go`) and type-asserted by the graph's core
layer at the call site that needs it. A `Store` implementation only has to
satisfy `store.MandatoryStore` — the capability interfaces above are all
additive.

There is exactly ONE decline shape for a genuinely MISSING capability:
`store.ErrCapabilityNotSupported` (re-exported as `graph.ErrCapabilityNotSupported`),
checked with `errors.Is(err, graph.ErrCapabilityNotSupported)` — never a
string comparison on the error message, which is diagnostic-only and may be
wrapped with the missing capability's name. `NodeCountByLabelAndPropertyKey`
and `PropertyStats` are the two primitives on this page that surface this
sentinel directly (`core.nodeCountByLabelAndPropertyKey` /
`core.nodePropertyStats` return it verbatim on a failed type assertion) —
unlike `RangeCardinality`/degree below, `PropertyStats` has no graceful
fallback, so an external `Store` implementation that omits
`NodePropertyStatsCapability` sees this sentinel outright rather than a
degraded-but-correct answer (memory, badger and tiered implement the capability, see "Tiered NDV fold"
above; sharded does not, and a store the graph opens itself declines when
`Config.DisablePlannerStats` is set).
`RangeCardinality` and the degree methods instead have a GRACEFUL
fallback baked into the graph layer (scan-and-count for range cardinality's
`exact=false`; `len(Outgoing/Incoming(...))` for degree), so a store missing
those two capabilities never surfaces `ErrCapabilityNotSupported` — it just
costs more (the sharded store takes the degree fallback).

`Config.DisablePlannerStats` (default false) is a RUNTIME decline of the same
sentinel on a store that implements the capabilities: it stops the per-write
maintenance of the presence counter, the NDV/min/max accumulator and the exact
type-class partition (the node `PropertyStats`,
`NodeCountByLabelAndPropertyKey` and node `PropertyTypeClassCounts` doors then
return `ErrCapabilityNotSupported`), and skips the rebuild at open. No
correctness path reads those counters, and `RangeCardinality` is NOT declined
(it reads the property index). A planner must therefore treat the sentinel as
"statistics unavailable", not as "backend too old". Which shape a given primitive uses is called out explicitly in
its section above; do not assume one from the other.
