# Backlog 48 — `Other` marker on `ColumnData`

Requested by sigma-tkgd 2026-10-10. Branch off main at 91684e5.

## In code before the change

- `store.ColumnData` (`pkg/graph/store/capabilities.go:1304`) carried `Null` only; a list, a map, a kind
  mismatch and a missing value all came out `Null == true`.
- One fill seam for every backend that has a column scan: the three row drivers in
  `pkg/graph/store/column_batch_build.go` (`ScanColumnsFromNodes`, `ScanColumnsFromRels`,
  `ScanColumnsFromRelSource`), plus badger's columnar path (`appendFromView`).
  - memory: row drivers only (`memorystore_column_scan.go`, `memorystore_rel_column_scan.go`); declared
    segment rels: `scanRelColumnsFromSegments` -> `segColumnSource` -> `ScanColumnsFromRelSource`.
  - badger: columnar path, else the row drivers. The columnar path only serves columns `buildColumn`
    (`internal/index/docvalues.go:581`) built: a list/map/struct (`sawOther`) or numbers mixed with strings make
    the column unbuildable, int and float mixed is `Mixed()`; both decline to the row path. So the columnar path
    never meets a list, map or kind-mismatched cell and needs no change.
  - tiered, sharded: no `NodeColumnScanCapability` / `RelColumnScanCapability`; the facade returns `ok=false`
    (core `column_scan.go`: `!has` returns before the temporal path too). Nothing to fill; the test asserts it.
  - pinned scans (ValidAt, TxAt): core `scan_temporal_opts.go` -> `ScanColumnsFromNodes/Rels`, same drivers.
- Found while writing the red test (pre-existing): a column whose first rows are absent (or Other) has a
  SHORTER typed slice than `Null`: `appendAbsent(c, known=false)` pads nothing and the kind resolution does not
  backfill. Probe: rows `{}, {x: 5}` -> `Null=[true false] Ints=[5]`; a reader indexing `Ints[c][k]` reads the
  wrong row or panics (sigma's `columnValue` does exactly that). A column that never resolves a kind has no typed
  slice at all. "Other implies a zero typed slot" needs the slot, so the fix aligns the typed slice of `Kinds[c]`
  to `len(IDs)` (backfill at kind resolution, pad at emit).

## Decisions

1. **Other cell => `Null == true`, typed slot zero** (deviates from the spec's suggested `Null == false`).
   Evidence: (a) today such a cell is `Null == true` in every driver, and the spec says the `Null` meaning must
   not change; (b) sigma-tkgd's reader (`internal/tyla/source/graph/columns.go:82`, `columnValue`) checks `Null`
   and otherwise reads the typed slot; with `Null == false` a reader not yet aware of `Other` would turn a list
   into a fact `0` / `""` (silent wrong answer), with `Null == true` it keeps today's "missing"; (c) the meaning
   mirrored, `SegmentColumnValues`: `Other[k]` implies `!Present[k]`, and `Null` is `!Present`. So `Other`
   refines `Null`: `Null && !Other` = missing, `Null && Other` = held outside the column, read the row.
2. **What counts as Other**: the row holds the key but the column cannot carry the value — not a column scalar
   (`classifyScalar` !ok: list, map, struct, int8/int16/uint widths, ...) or a scalar of another kind than
   `Kinds[c]` (string in an int column). Mixed int/float still refuses (`ErrMixedNumericColumn`), unchanged.
   Not the same partition as `PropertyTypeClassCounts.Other` (slices, maps, structs): an int8 is Numeric there
   and Other here; a string in an int column is String there. Documented on the field.
3. **Rel batch filled identically.** `ColumnData` exists so nodes and relationships answer alike (its own
   comment); a rel `Other` that is always nil would mean "no Other cell" on nodes and "not reported" on rels for
   the same struct. Row drivers: same code. `RelColumnSource` gains `Other(i, c) bool`, asked only where
   `Value` reports `ok=false` (rare cells), so the per-value hot loop is unchanged; `segColumnSource` keeps a
   per-cell class instead of `ok` (absent / scalar / other) and its int8/int16/uint arm becomes Other.
4. **Nil when the batch has no Other cell**: allocated on the first mark of a batch, padded to `len(IDs)` for
   every column before the callback, dropped by the batch reset. Zero extra allocation on the common path:
   `TestColumnData_OtherPerBatch/no_other_cell` asserts nil; the row-path benchmarks' allocs/op are compared
   before/after in `bench-*.txt`.
5. badger columnar: unchanged (never meets an Other cell, see above).

## Tests (red first)

| Test | Kills |
|---|---|
| `TestColumnScanOther_MatchesRowRead` (pkg/graph; memory, badger, tiered, sharded, memory-segments; current, ValidAt 1500, TxAt pin; nodes and rels) | Other never set; Other set for a missing cell; Other on the wrong column index; Other cell with `Null == false` or a non-zero slot; stale Other after the fixing/breaking update (pinned vs current); misaligned typed slot after a leading Other cell; short Other column (no pad); segment source never asking `Other` / int8 arm |
| `TestColumnData_OtherPerBatch` (pkg/graph/store) | Other allocated without an Other cell; Other carried into the next batch (reset) |
| `TestColumnDrivers_NodeAndRelAgree` (extended: renders Other) | node / rel / source driver drifting on Other |

Mutants: `mutants.txt`. Red output: `red-*.txt`. Green: `green-*.txt`.

## Results

- Red: `red-facade.txt` (before any code: the int8-only column `s` has no typed slice), `red-facade-aligned.txt`
  (after the alignment, before Other: every Other cell reads `other:false`, memory/badger/memory-segments, current,
  ValidAt 1500 and TxAt pin), `red-store.txt` (`other_in_first_batch_only`: no Other).
- Green: `green-facade.txt`, `green-store.txt`. Mutants: `mutants.txt`, 13 of 13 killed.
- Allocs: `bench-before.txt` / `bench-after.txt` (interleaved, 6 runs each, base 91684e5 vs this branch):
  allocs/op identical on every row (nodes 52 / 66, rels 56 / 70 at n=1000 / 10000); B/op +24..48 (the batch
  struct carries one more slice header); time within the run-to-run noise.
- Not gated in `bench/` (that suite holds no column-scan row); the nil-on-the-common-path guarantee is the
  `no_other_cell` assertion.

## Found, not changed (pre-existing, out of scope)

- Narrow integer widths diverge between badger's columnar path and the row path. Probe (three nodes, `s` int8
  only, `ScanNodeColumns(["s"])`): memory `Null=[true true true]` (now `Other` all true); badger columnar
  `Null=[false false false] Ints=[1 2 3]`. `buildColumn` accepts int8/int16/uint* as numeric, `classifyScalar`
  does not. Both answers are correct for a reader that honours `Other`; they are not byte-identical. Documented on
  the field and in api.md. Aligning them means widening `classifyScalar` (and the segment arm) — a behaviour change
  for the row path, left to a decision.
