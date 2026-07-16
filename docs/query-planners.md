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
`badger.Store`, `tiered.Store`). An out-of-tree `Store` implementation that
satisfies only `MandatoryStore` may serve some of these at different cost, or
decline the optional ones entirely — see "The capability story" at the
bottom.

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
| NDV + exact min/max + count | `g.Stats().PropertyStats(label, key)` | O(1) amortized; O(nodes carrying label) on a rescan after the current min/max holder is deleted | O(open shards) — per-shard HyperLogLog register-max merge, plus each shard's own independent Min/Max rescan on extremum deletion (see "Tiered NDV fold") | `ErrCapabilityNotSupported` on a store without the optional capability |
| Numeric range cardinality | `g.Nodes().RangeCardinality(...)` / `g.Stats().RangeCardinality(...)` (alias) | O(distinct values in range), no node scan | always declines (tiered does not implement the capability) | `exact=false` (not an error) — see "RangeCardinality decline conditions" |
| Ordered / top-k range scan | `g.Nodes().ForEachByLabelPropertyRangeOrdered(...)` | O(k + log n) index work for a LIMIT-k top-k (RAM); disk mode is O(range) cheap-ID collection + O(k) node fetch. TEMPORAL opts: O(N log N) sound full fold (no index) | `ErrIndexNotFound` (tiered is not an exact native store — no ordered view) | non-temporal: `ErrIndexNotFound` when no property index / capability. Temporal opts are SERVED via the fold (no longer `ErrOrderedScanTemporal`) |
| String prefix scan (`STARTS WITH`) | `g.Nodes().ForEachByLabelPropertyPrefix(...)` / `g.Rels().ForEachByTypePropertyPrefix(...)` | O(k + log n) top-k over the ordered STRING view (RAM); node disk mode is `0x0A` `"s:"+prefix` iteration; EXACT (no over-selection). TEMPORAL opts: O(N log N) sound full fold | `ErrIndexNotFound` (no ordered view) | lex value order asc/desc, ties by id ascending; empty prefix = all strings; temporal opts SERVED via the fold |
| Outgoing / incoming degree | `g.Rels().OutgoingDegree(id, type)` / `IncomingDegree(id, type)` | O(1) via `DegreeCapability`, else O(degree) | O(1) — single-shard lookup on the node's owning shard | never — always answers (fast path or fallback) |
| Node / relationship mutation epoch | `g.Nodes().NodeMutationEpoch()` / `g.Rels().RelMutationEpoch()` | O(1) | O(1) where supported | returns 0 (not an error) when the backend lacks the DocValues capability |
| Pinned adjacency (transaction-time) | `g.Rels().OutgoingForNodesAtTx(nodeIDs, type, txAt)` / `IncomingForNodesAtTx(...)` | adjacency index + O(deleted rels) fold, not a full `ByType` history scan | same adjacency-index push-down per shard | `txAt == 0` delegates to `OutgoingForNodes`/`IncomingForNodes` (no TX filter) |
| Composite (multi-key) equality lookup | `g.Nodes().ByLabelAndProperties(label, values, opts)` | O(matches) with a matching `g.Index().CreateComposite` definition; else O(label size) scan+filter | O(label size) scan+filter — v1 has no accelerated composite index on tiered | never errors; falls back to scan+filter when no exact-key-set definition exists (see "Composite property indexes" below) |

| Pinned adjacency — bitemporal (TxAt) | `g.Rels().OutgoingForNodesAtTx(nodeIDs, type, txAt)` / `IncomingForNodesAtTx(...)` | adjacency index + O(deleted rels) fold, not a full `ByType` history scan | same adjacency-index push-down per shard | `txAt == 0` delegates to `OutgoingForNodes`/`IncomingForNodes` (no TX filter); **wall-now valid filter — drops past-valid edges**, see below |
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
  CLAUDE.md "Concurrency"); badger's rescan therefore collects the current
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
  `Count`/`NDV` are always exact/current on both backends. See lesson 62.

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
under-count NDV, exactly the failure the merge exists to prevent. See
docs/adr/0005-tiered-parity.md §3.1.

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
2. **A temporal filter is set** (`opts.ValidAt != 0` or
   `opts.ValidStart/ValidEnd != 0`). The property index's bucket sizes are
   valid-time AGNOSTIC (built over current-state membership only), so a
   temporal predicate always declines — `(0, false, nil)`.
3. **The store lacks the native capability** — no type assertion to the
   internal `NodeRangeCardinality(...)` scanner interface succeeds. Every
   in-tree backend implements it; an out-of-tree store need not.
4. **The label is unregistered.** `(0, false, nil)` — the caller's scan would
   find zero rows anyway, so this is a cheap equivalent decline, not an error.
5. **No property index exists yet for `(label, propKey)`**, or the index
   exists but is **poisoned** — it has ever indexed an integer magnitude past
   `2^53`, where float64 sort keys can collide with a neighboring value and
   the bucket sum would silently miscount. Once poisoned, the index declines
   for every future range query, not just queries touching the large value.

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
- **Benchmark** (`bench/ordered_topk_test.go`, top-10 by value over 100k
  distinct values): the ordered arm vs the pre-K3a collect-then-limit shape (a
  full `ByLabel` scan sorted by value, truncated to k):

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
100k-node fixture where the first key is deliberately unselective — ~10
distinct values — and the composite key pair is selective).

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
- **RAM-only.** Composite index ENTRIES always live in memory on both
  `memory.Store` and `badger.Store` — there is no on-disk mode analogous to
  `badger.Config.PropertyIndexOnDisk`. DEFINITIONS (label + declared key
  list) are persisted on `badger.Store` so a reopen rebuilds the same
  definitions by re-scanning current node state (same shape as the
  single-key property index's own RAM-mode rebuild). A store with many
  large composite indexes should budget RAM accordingly; on-disk composite
  entries are a documented follow-up.
- **Node-only.** Mirrors the existing single-key `PropertyIndexCapability`,
  which has no relationship equivalent in this library today.
- **Tiered declines the acceleration in v1.** `tiered.Store` does not
  implement `CompositePropertyIndexCapability` — `g.Index().CreateComposite`
  on a tiered-backed graph returns `ErrCapabilityNotSupported`, and
  `g.Nodes().ByLabelAndProperties` answers via the graph-layer mandatory
  fallback (a label scan + post-filter using the mandatory `NodesByLabel`
  surface) — correct, just unaccelerated. Reference-label-scoped tiered
  acceleration (mirroring the single-key index's `ErrEventPropertyIndex`
  gate) is a documented follow-up.

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
  valid-time probe at **wall-now**, so an edge whose valid interval lies wholly
  in the past is SILENTLY DROPPED even though it was believed at `txAt`:
  a `CloseVersion`-ed edge, or a width-1 `[t, t+1)` point-event edge (the
  standard point-event encoding). Use this only when you genuinely want
  "believed at `txAt` AND still valid at wall-now". `txAt == 0` delegates to the
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
wall-now valid filter will silently drop point events and closed intervals. The
`*AtTx` doors remain for callers who explicitly want the bitemporal "recorded by
`txAt` and valid at wall-now" intersection.

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

## The capability story for external stores

Every OPTIONAL statistics primitive above (`NodeCountByLabelAndPropertyKey`,
`PropertyStats`, the RangeCardinality fast path, `DegreeCapability`) is
backed by a `Store` capability interface declared in
`pkg/graph/store/capabilities.go` and type-asserted by the graph's core
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
degraded-but-correct answer (all three in-tree backends — memory, badger,
tiered — implement the capability; see "Tiered NDV fold" above).
`RangeCardinality` and the degree methods instead have a GRACEFUL
fallback baked into the graph layer (scan-and-count for range cardinality's
`exact=false`; `len(Outgoing/Incoming(...))` for degree), so a store missing
those two capabilities never surfaces `ErrCapabilityNotSupported` — it just
costs more. Which shape a given primitive uses is called out explicitly in
its section above; do not assume one from the other.
