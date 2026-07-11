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
| NDV + exact min/max + count | `g.Stats().PropertyStats(label, key)` | O(1) amortized; O(nodes carrying label) on a rescan after the current min/max holder is deleted | always declines — tiered does not implement `NodePropertyStatsCapability` (v1) | `ErrCapabilityNotSupported` on a store without the optional capability |
| Numeric range cardinality | `g.Nodes().RangeCardinality(...)` / `g.Stats().RangeCardinality(...)` (alias) | O(distinct values in range), no node scan | always declines (tiered does not implement the capability) | `exact=false` (not an error) — see "RangeCardinality decline conditions" |
| Outgoing / incoming degree | `g.Rels().OutgoingDegree(id, type)` / `IncomingDegree(id, type)` | O(1) via `DegreeCapability`, else O(degree) | O(1) — single-shard lookup on the node's owning shard | never — always answers (fast path or fallback) |
| Node / relationship mutation epoch | `g.Nodes().NodeMutationEpoch()` / `g.Rels().RelMutationEpoch()` | O(1) | O(1) where supported | returns 0 (not an error) when the backend lacks the DocValues capability |
| Pinned adjacency (transaction-time) | `g.Rels().OutgoingForNodesAtTx(nodeIDs, type, txAt)` / `IncomingForNodesAtTx(...)` | adjacency index + O(deleted rels) fold, not a full `ByType` history scan | same adjacency-index push-down per shard | `txAt == 0` delegates to `OutgoingForNodes`/`IncomingForNodes` (no TX filter) |

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
`core.nodePropertyStats` return it verbatim on a failed type assertion —
`PropertyStats` is the one currently reachable ONLY via this decline shape on
tiered, since it has no graceful fallback the way RangeCardinality/degree
do). `RangeCardinality` and the degree methods instead have a GRACEFUL
fallback baked into the graph layer (scan-and-count for range cardinality's
`exact=false`; `len(Outgoing/Incoming(...))` for degree), so a store missing
those two capabilities never surfaces `ErrCapabilityNotSupported` — it just
costs more. Which shape a given primitive uses is called out explicitly in
its section above; do not assume one from the other.
