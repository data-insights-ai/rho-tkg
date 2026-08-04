# Plan — typed columns, a typed column door, and temporal zone maps

Status: proposed 2026-08-04. Scope is ADDITIVE. No redesign, no breaking change
to a published contract.

## What already exists (read this before proposing anything)

The library is **not** missing a columnar structure. `index.LabelDocValues`
(`pkg/graph/internal/index/docvalues.go`) is one:

- ordinal-aligned columns sharing one `nodeIDs` vector,
- a `present` bitset per column (absent/null without a sentinel value),
- **strings already dictionary-encoded** — `dict []any` + `codes []uint32`,
- immutable, built at one store mutation epoch, and validated **lock-free** by
  re-reading that epoch after the scan (`NodeMutationEpoch`, per-label since
  BACKLOG 4b). An interleaving write advances the epoch and the consumer
  discards the rows.

That epoch-validated snapshot substrate is the hard part and it is done. What
follows is three narrow gaps inside it, not a new backend.

## The gap, stated exactly

**G1 — the numeric column is boxed.** `docColumn.boxed []any` holds one `any`
per row. Its doc comment justifies this honestly: boxing once at build beats
boxing per read, and it matches the memory of the per-node path it replaced.
Both claims are true. But it means the column path *inherited* the row path's
representation instead of improving on it:

| representation | bytes/row | heap objects/row |
|---|---|---|
| `boxed []any` (today) | 16 (iface header) + 8 (boxed int64) = **24** | 1 |
| `ints []int64` | **8** | 0 |

The dynamic type of each boxed value is load-bearing — it is what preserves
int64-vs-float64 so a consumer's accumulator stays bit-exact above 2^53. Any
replacement MUST preserve that distinction. Collapsing to `[]float64` is
therefore forbidden.

**G2 — the door re-boxes.** `types.NodeColumnReader` is a PUBLISHED interface:

```go
type NodeColumnReader interface {
    Row(id NodeID, vals []any, present []bool) bool
    Epoch() uint64
}
```

`[]any` is in the contract, as is `nodes.API.ForEachDocValues`. So a perfectly
typed column would still hand out boxed values at the door. Fixing G1 without
fixing G2 moves the allocation rather than removing it — and fixing G2 by
changing these signatures is a breaking change. It needs a NEW door, not an
edited one.

**G3 — no temporal zone maps.** `ForEachDocValues` documents its membership as
"the full in-RAM label index — the same unfiltered set the zero-QueryOpts scan
returns (no valid-time filter)". A bitemporal predicate is therefore evaluated
per row, after the scan. The library's defining feature is a post-filter at its
fastest read path.

## R1 — typed numeric columns, with boxing made lazy

Replace `docColumn.boxed []any` for `ColNumeric` with:

```go
ints    []int64   // populated for every non-float row
flts    []float64 // nil unless the column actually contains a float
isFloat bitset    // nil when the column is uniformly int64 (the common case)
```

A uniformly-int64 column — overwhelmingly the common shape — stores `ints`
only: 8 bytes per row, no bitset, no heap object. Mixed columns keep working
(no consumer regresses into `colUnbuildable`); `isFloat` selects the array.

**The trap, and why the obvious fix is wrong.** If typed storage replaces
`boxed` outright, then `ForEachRow` must do `any(ints[ord])` per value — which
*allocates*, because Go heap-escapes an int64 on interface conversion outside
the small-value cache. That is a REGRESSION for every existing consumer of the
published door, in exchange for a win only new consumers see. Do not do this.

**Instead: keep `boxed` as a lazily-materialised compat cache**, built on first
use of a boxed-API call and guarded by `sync.Once` (safe and race-free because
the snapshot is immutable by construction).

- typed consumers: never allocate `boxed` at all — pure win,
- boxed consumers: pay exactly what they pay today, once per snapshot,
  amortised over every read — no regression,
- memory doubles only for a label a boxed consumer actually touches.

Strictly better than today at both doors. This design is a consequence of
checking the callers; it is not the first thing that comes to mind.

Do NOT touch `ColString`. Dictionary encoding already boxes each distinct term
once, so a string column with many duplicates is already better than typed
storage would be.

## R2 — a typed column door

`store.NodeColumnScanCapability` (`ColumnBatch` with `Ints []int64`,
`Flts []float64`, `Strs []string`, `Bools []bool`, `Null`, `Kinds`) is already
the right typed shape and is already implemented for `memory`. Two moves:

1. **Implement it for badger, backed by `LabelDocValues`.** With R1 the batch
   fields are the column arrays — sliced, not transcoded. Today's memory
   implementation converts from row structs at read time; this one does not
   convert at all.
2. **Expose a typed accessor on `LabelDocValues`** so the capability can reach
   the arrays without the boxed path.

`ForEachRow`, `ForEachDocValues`, `DocValuesSnapshot`, `NodeColumnReader` all
keep working, unchanged, forever. New door beside the old one.

**Existing refusals stay refusals** — `LabelIndexOnDisk` (membership not in
RAM), empty/over-cap label, unbuildable column. A refusal is a visible
`ok=false` fallback; silently returning something subtly different is the
failure mode to avoid (a mixed-numeric column silently widened to float64
changes which values a consumer's equality test matches).

## R3 — temporal zone maps (specified, sized, NOT yet built)

Carry `validFrom`/`validTo` as two `[]int64` columns in the snapshot, plus a
per-block (suggested 4096 ordinals, matching the existing batch size) min/max
of each. A scan carrying a valid-time predicate then skips whole blocks whose
`[min,max]` cannot intersect the query interval.

This is the item that turns bitemporality from a cost into an asset: today
every row is tested; with zone maps a query over a narrow window touches only
the blocks that can possibly match, and cold historical blocks are skipped on
metadata alone.

**Honest sizing: this is the largest of the three and it is not a
drop-in.** The build path does not currently read temporal metadata at all,
and DocValues membership is deliberately defined as the *unfiltered* label set
— so the snapshot's contract, not just its contents, changes. It also
interacts with the existing `DocValuesSnapshotAsOf` (label, txAt) cache, which
is a transaction-time sibling of the same idea. Sequenced after R1+R2 rather
than bundled with them, and specified here so it is not re-derived later.

## Not an item: content-based shard placement

For the record, so it stops being re-proposed: a shard is `catalog[decompose(id).Node]`,
baked into the snowflake at MINT time. `sharded.go` argues correctly that an
entity's row cannot move without changing its ID, which would break every
relationship pointing at it, its hash chain, and any external reference —
so non-identity rebalance is OUT OF SCOPE, and grow/empty-shrink via
export/import plus a fail-closed `ErrSlotNotLocal` is the supported path.

That is the right design for write locality and ID stability, and nothing here
proposes changing it. The consequence worth writing down is what it means for
readers: **placement is identity-derived and fixed, so it can never be made to
match a query's partition key.** A consumer that wants data-parallel reads must
own its own placement map above the store; the store can serve such a consumer
with fast typed scans (R1–R3), but cannot push the partitioning down. This is a
stated boundary, not a defect, and it needs no code.

## CORRECTION (2026-08-04) — the row path does NOT box per read

This RETRACTS a claim in `b8b80bd`'s commit message (and in G1 above, where it
also shaped the framing): that the column build still pays ~2 allocations per
row inside `getProp`, "the same defect one layer down".

That number was the BENCHMARK'S OWN getter, which constructed `int64(id)` fresh
on every call. The real getter returns the already-boxed `Property.Value`:

| getter                                    | allocs / 100k rows |
|---|---|
| synthetic (constructs and boxes per call) | 199,497 |
| realistic (returns the stored box)        | **7** |

**The box already exists in storage.** `Property{Key string; Value any}` holds a
boxed value, so reading it is an interface-header copy — measured 0.26 ns and
zero allocations. Every "the accessor returns `any`, therefore it allocates"
inference is wrong for reads.

### What this means for adding typed accessors (asked directly, answered here)

A typed accessor over `any` storage buys nothing in the common case. It is a 26x
win in exactly ONE case — when the STORED type differs from the WANTED one,
because a new value is then constructed:

| read                                        | ns/op | allocs |
|---|---|---|
| already-boxed -> type assert                | 0.26  | 0 |
| convert + re-box (`int32` -> `any(int64)`)  | 6.84  | 1 |
| typed accessor (convert -> `int64`)         | 0.29  | 0 |

So: add typed accessors ONLY where a conversion actually happens
(Instant/int/int32 -> int64). Do NOT add a blanket typed surface — for a value
already stored as the wanted type it is pure API surface for 0.26 ns.

Eliminating the boxing generally is a different and much larger project: typed
STORAGE, replacing `Property.Value any`. That would also cut the per-property
footprint (16B string header + 16B interface + an 8B heap box). Not planned
here; noted so the distinction between "typed accessor" (narrow, cheap, mostly
pointless) and "typed storage" (broad, expensive, real) stays explicit.

## Where the columnar layer SITS (asked directly, answered here)

It is an accelerator INSIDE the existing backends. Not a replacement backend,
not a new one, not a storage format:

```
        consumer (query engine, exporter, analytics)
                 |
       +---------+----------+
  typed door           boxed door        <- both served from the SAME columns
  ScanNodeColumns      ForEachDocValues
  (optional            NodeColumnReader
   capability)          (published)
       +---------+----------+
                 |
       docColumns map[uint16]*LabelDocValues   <- DERIVED CACHE, a field on the
                 |  built on demand, epoch-validated, dropped when stale
                 |  NOT persisted, NOT a storage format         backend struct
                 v
       the row store (badger / memory)         <- system of record, unchanged
```

`memorystore.go:167` and badger's equivalent each hold this map. It is built
lazily from the row store, stamped with the mutation epoch, and discarded when a
write advances it. Nothing about it is durable, so there is no migration and no
on-disk compat marker — which is why R1 could change the representation freely:
it changed the representation of a CACHE, not of a storage format.

Optional at every level: a backend that does not build columns still works, the
capability is type-asserted, and every documented refusal falls back to the
per-node path.

R2a is where this stops being purely mechanical. Putting ValidFrom/ValidTo into
the snapshot keeps it a derived cache, but it starts encoding bitemporal
semantics, so the "membership is the UNFILTERED label set" contract must be
restated deliberately rather than quietly extended.

## CORRECTION (2026-08-04, found while starting R2) — R2 and R3 are one item

The ordering above (R1, then R2, then R3 "sequenced after") is WRONG, and the
reason is worth keeping so it is not re-derived:

**`ColumnBatch` mandates `ValidFrom []int64` / `ValidTo []int64`.** Its own doc
comment explains why — a bitemporal consumer reading columns still has to know
WHEN each row holds, and fetching that separately means holding the entity
after all, which is the materialisation a column scan exists to avoid.

**`LabelDocValues` carries no temporal metadata whatsoever.** Not at any of its
three build sites (`core/docvalues.go`, `memory/memorystore_docvalues.go`,
`badger/badgerstore_docvalues.go`), because its membership is DELIBERATELY the
unfiltered label set.

So a DocValues-backed `ScanNodeColumns` cannot fill the batch it is required to
fill. The temporal columns are **R2's prerequisite, not R3's payload.**

Compounding it: `ScanNodeColumns` takes `QueryOpts`. Serving it from an
unfiltered snapshot means applying the valid-time predicate PER ROW after the
scan — exactly the cost zone maps exist to remove. A DocValues-backed R2
without zone maps would be correct and no faster on the temporal axis.

**Revised sequence:**

- **R2a — temporal columns in the snapshot.** `LabelDocValues` gains
  `validFrom`/`validTo` `[]int64`, populated at build from the same node the
  property getter already touches (so no extra I/O — badger's build already
  iterates nodes via `bulkNodePropGetter`). Build signature gains a temporal
  accessor; three call sites supply it. This is the unlock for everything below.
- **R2b — badger `ScanNodeColumns` over the snapshot.** Now able to fill the
  batch. Slices the typed arrays; no transcode.
- **R2c/R3 — block min/max over the temporal columns.** Small once R2a exists:
  a per-4096-ordinal min/max pair and a skip test in the scan loop.

Do NOT attempt R2b before R2a. The obvious workaround — implementing badger's
capability over its bulk ROW iterator instead, the way the memory backend does
— is a legitimate independent option, but it converts at read time and does not
use the column store, so it should be chosen deliberately as a stopgap and
labelled one, not slipped in as if it were the columnar path.

## Would a full typed/columnar STRUCTURE win big? (asked directly)

Short answer: **not on read CPU, and the honest case for it rests on one number
nobody has measured yet — rebuild amortisation.** Setting out what is known, so
the decision is not made on vibes.

**Reads are already cheap, so "boxing is slow" is not the argument.** A stored
value read through `any` costs 0.26 ns and zero allocations, because the box
already exists in storage. Replacing the representation cannot beat 0.26 ns.
Anyone proposing typed storage on read-CPU grounds is proposing it for a cost
that is not there.

**The real wins are memory and locality, and they are bounded and known:**

| layer | per row | note |
|---|---|---|
| `Property{Key string; Value any}` | 32 B + 8 B box | 16 B key header + 16 B iface |
| typed column (shipped, R1) | 8 B | already achieved, in the CACHE |

So the columnar layer ALREADY captures the layout win for analytical reads.
A native columnar backend does not add a new factor there — it makes the
existing factor durable.

**What a native structure would actually add, that the cache cannot:**

1. **No rebuild.** This is the one that matters. The snapshot is invalidated by
   epoch advance, so every write to a label throws away that label's columns and
   the next read rebuilds them. Measured at 100k rows, one column:
   build 1,038 us against a typed scan of 270 us — **one rebuild costs ~3.8
   scans.** If a label is written more often than roughly once per four reads of
   it, the cache is net-negative and a native columnar store wins by exactly the
   margin the rebuild wastes. Below that ratio the cache already captures most of
   the benefit and a rewrite buys little.
2. **Persistence** — columns survive restart instead of being rebuilt cold.
3. **Storage-level zone maps** — skip blocks before I/O, not after. The zone map
   shipped in R2a skips work only once the snapshot is already in RAM.

**Therefore the decision hinges on a read/write ratio per label, which is
workload-specific and currently unmeasured.** Per-label epochs (BACKLOG 4b)
already stop unrelated writes from invalidating a label, which pushes many
workloads into the favourable region.

**The measurement that would settle it**, before any rewrite is justified:
instrument snapshot build count against snapshot read count per label under a
representative workload. If builds/reads > ~0.26 (the 1/3.8 break-even), the
rebuild tax is real and a native structure pays for itself. If it is far below,
the cache is the right architecture and the remaining wins are persistence and
cold-start only — worth doing eventually, not worth a redesign now.

**Recommendation: finish R2b/R2c first.** They are additive, they are cheap, and
they make the columnar path reachable by real consumers — which is also what
produces the build/read telemetry the rewrite decision needs. Deciding on a
rewrite before that telemetry exists would be deciding without the one number
that distinguishes the two answers.

## Order and acceptance

1. **R1** — typed numeric + lazy boxed cache. Accept: existing docvalues tests
   pass UNCHANGED (they assert through the boxed door, which is the point); a
   new test asserts int64 above 2^53 survives bit-exact and a mixed column
   still builds; an allocation benchmark shows the typed path at 0 allocs/row.
2. **R2** — badger capability over DocValues + typed accessor. Accept: a
   value-level test that the typed door and the boxed door return the SAME
   values for the same label (a shape match is not a payload match), and that
   every documented refusal still refuses.
3. **R3** — zone maps. Separate, sized above.

Gate for each: `GOWORK=off go build ./... && GOWORK=off go test ./...`
(31 packages), plus `go test -bench` on the docvalues benches for R1.
