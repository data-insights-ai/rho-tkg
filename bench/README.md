# bench/ — cross-backend performance suite

Committed benchmark scenarios for the graph engine, run against **both**
in-memory backends (`memory.Store` and Badger's `BadgerInMemory` mode) via a
shared harness (`harness_test.go`). Every scenario is a `func BenchmarkX(b
*testing.B)` with one `b.Run("memory", ...)` and one `b.Run("badger", ...)`
sub-benchmark, EXCEPT `ANNSearch10k` (memory-only — see its note below),
`PinnedScanScaling`, and `ChangeLogTxSerialization` (Badger-only — see their
notes below); `PinnedRelPropertyLookup` adds a third, `sharded`, arm only when
`RHO_TKG_PINNED_REL_SIZES` is set. Read-only scenarios (`PointReadHit`,
`LabelScan10k`, `TwoHop`, `TemporalPoint`, `AsOfPin`, `PinnedScanScaling`, and
the history, effective-timeline, ordered, lookup and stamp scenarios in the
table below) build their fixture once per (scenario, backend) pair using the
`for b.Loop() { ... }` protocol (Go 1.24+) so the setup cost is paid exactly
once, never repeated across the testing framework's timing-calibration passes.

The write scenarios (`Ingest1kSingle`, `Ingest10kBatch`,
`BulkAddNodes10k` in `ingest_test.go`, the ingest-pipeline family in
`ingest_pipeline_test.go`, and `ChangeLogTxSerialization` in
`changelog_tx_test.go`) are the opposite shape — each iteration *writes*
(thousands of new nodes, or a concurrent-goroutine write workload) — so they
deliberately use the classic `for i := 0; i < b.N; i++ { ... }` loop instead,
with a fresh graph built and torn down every iteration via manual
`b.StopTimer()`/`b.StartTimer()` around the untimed construction/close. A
single graph reused across
`b.Loop()` iterations previously accumulated nodes from every prior iteration,
so ns/op silently grew with the run's iteration count instead of measuring a
constant-size ingest; see `ingest_test.go`'s package comment for why
`b.StopTimer()` inside a `for b.Loop() { ... }` loop doesn't work as a fix
(it poisons the loop's internal state and hard-fails the benchmark on the
next iteration unless the timer is resumed before the loop condition
re-fires) and why the classic `b.N` loop is the correct shape here instead.

## Scenarios

| Benchmark | What it measures |
|---|---|
| `PointReadHit` | Warm cache-hit `g.Nodes().Get` over a 2k-node working set |
| `LabelScan10k` | Full `g.Nodes().ByLabel` scan over 10k same-labeled nodes; `sorted` (default) and `nosort` (`QueryOpts.NoSort`) sub-variants |
| `TwoHop` | Decode-free two-hop traversal via `g.Rels().ForEachAdjacentEndpoint` over a 10k-node / 30k-relationship fixture |
| `TemporalPoint` | `g.Temporal().NodesAt` (valid-time point query) over a graph where every node has an explicit 5-version `tkg_valid_from` chain, pinned mid-chain |
| `AsOfPin` | `g.Temporal().NodesAsOf` (transaction-time query) pinned to the middle of a 5-round update history via `NowTx()` |
| `RelHistoryPlain` | `g.Rels().History` for a relationship without history among 10k relationships of which 1 % hold history (the per-entity cost an effective-state read paid before `HasHistory`, handover effective-read-cost §1) |
| `RelHasHistory` | `g.Rels().HasHistory` on the same fixture, `miss` (plain) and `hit` (updated) sub-variants; badger's RAM set is built by a warm-up call outside the timed loop |
| `RelEffectiveTimeline` | `g.Temporal().RelEffectiveTimeline` at a pin over 200 K relationships (`BENCH_EFFECTIVE_RELS` overrides) of which 1 % carry a bounded correction; `plain` (one segment, no history), `plain-hot` (64 plain relationships cycled, so their rows stay in the store's caches) and `cascaded` (three segments) sub-variants; the fixture is built once per backend per process |
| `NodeAtTxLongChain` / `NodeEffectiveTimelineLongChain` | one node with n = 300 / 1000 / 3000 Updates and one bounded correction (a non-monotonic chain: the resolver's own-bounds arm and its supersession rule run on every read): `NodeAtTx` at the correction, and the whole timeline (n + 3 segments) |
| `RelEffectiveLoop` | the consumer loop the timeline replaces on the same fixture and sub-variants: `Get` + `History` + `RelAtTx` at every row bound |
| `ForEachRelEffectiveByType` | one full `g.Temporal().ForEachRelEffectiveByType` scan of the fixture at the pin; `ns/rel` (a custom metric, not read by the time gate) is the per-relationship cost |
| `OrderedTopK` | `g.Nodes().ForEachByLabelPropertyRangeOrdered` top-10 (`ordered`, the LIMIT pushed into the index) against the pre-K3a `collect-then-limit` shape (a full label scan, sorted, truncated) over 100 k distinct values (`docs/query-planners.md` "Ordered / top-k range scan") |
| `CompositeLookupVsSingleIndexPlusFilter` | `g.Nodes().ByLabelAndProperties` over a composite index (`composite_lookup`) against a single-key index plus a caller-side post-filter (`single_index_plus_filter`) on 100 k nodes whose first key (5 values) is unselective and whose composite pair (x 50 regions) is selective |
| `RelPropertyLookup10k` | `g.Rels().ByTypeAndProperty` with a relationship property index (`indexed`) against the type scan plus filter (`scan`) over 10 k relationships where a selective weight matches one |
| `Ingest1kSingle` | 1,000 nodes ingested one at a time via `g.Nodes().Add` (the no-batching baseline) |
| `Ingest10kBatch` | 10,000 nodes ingested via `BatchBuilder.AddNode` + one `Execute` |
| `BulkAddNodes10k` | 10,000 nodes ingested via the write-only `BatchBuilder.AddNodes` bulk path |
| `ANNSearch10k` | `g.Index().SearchNearest` (k=10) over a 10k x 128-dim vector index, `hnsw` (default approximate engine) vs `bruteforce` (`VectorIndexOptions.UseBruteForce`) sub-variants — memory backend only (the vector index is store-level in-memory regardless of which Store backend hosts the node rows, so the badger sub-benchmark would be redundant) |
| `PinnedScanScaling` | Historical M1 measurement (original write-up retired; scenario remains in-tree): `ByLabel` plain vs `TxPin`/`TxAt`-pinned vs `NodesAsOf`-filtered, across {10k,100k} entities x {1,5,5+20%-deleted}-version churn x {broad,selective} label selectivity — BadgerInMemory only. Quantifies whether a pinned/as-of scan costs `O(current matches)` like plain `ByLabel` or `O(everything that ever had history)`. |
| `PinnedRelPropertyLookup` | Backlog 8: `g.Rels().ByTypeAndProperty` with `TxPin` (200 matches) vs the same lookup without a pin, 1 or 5 types, profiles nochurn / sigma (20 % revised, 5 % deleted) / unrelated churn x1 and x10; reports the lazy sidecar build as `build-ms` (never gated). Default: 20 000 rels, memory and badger, 24 rows (the bench-gate canary). `RHO_TKG_PINNED_REL_SIZES=100000,1000000` runs the measurement matrix: sharded too, and a broad 5 % value. Gate: allocs-gated; time rows reported, not gated, on shared hosts; opt-in time canary (see below) |
| `LatestStamps` | Backlog 30: `g.Rels().LatestStamps` on one relationship with 0 (`plain`), 100 and 1,000 history versions (updates plus corrections whose pieces are appended above the current row), memory and badger; the badger sidecar is built by a warm-up call. Gate: allocs-gated with the `PinnedRelPropertyLookup` family (the door is 0 allocs/op; a 0 allocs/op row fails as soon as it allocates — the History fold it replaces allocates per row) |
| `IngestPipeline`, `IngestConcurrent`, `IngestPipelineChangeLog`, `IngestConcurrentChangeLog`, `IngestSingleDurable`, `IngestPipelineDurable` | the ingest session family (`ingest_pipeline_test.go`): 10 k node-creates through the prepare-parallel / apply-sequential pipeline at 1 and 8 producers, the concurrent self-applying mode, the change-log on versus off, and the durable pair on a real badger directory with `SyncWrites`, 1,000 creates (single `Add` against the pipeline's group commit). All of them match the gate's `Ingest` prefix |
| `ChangeLogTxSerialization` | Historical M2 measurement (original write-up retired; scenario remains in-tree): aggregate ops/sec for standalone `Add` vs tx-per-batch (`Begin`->10x`AddNode`->`Commit`) at {1,4,16} concurrent goroutines, `Config.ChangeLog` on/off — a manual goroutine-fan-out harness reporting a custom `ops/sec` metric (not `ns/op`), since throughput scaling with goroutine count — not single-call latency — is what's under test. BadgerInMemory only (`ChangeLog` is a Badger/memory capability). |

`ANNSearch10k` note: measured locally when the scenario was added (Apple M4 Max, `-benchtime=200x`; figures as recorded, not re-run since) at
~193µs/op (`hnsw`) vs ~816µs/op (`bruteforce`) — roughly a 4x speedup at this
10k-point, single-query-vector-at-a-time scale. Brute-force is already a fast
flat scan at 10k points (a 128-dim linear scan is cheap in absolute terms), so
the *relative* HNSW advantage at this modest corpus size is more modest than
the 10-100x gaps typically reported in the ANN literature at 100k-1M+ point
scale, where the O(n) vs O(log n) gap widens substantially. Run
`go test -bench=BenchmarkANNSearch10k -benchtime=200x ./bench` to reproduce.

## ADR-0011 S0 baseline (a measurement test, not a benchmark)

`segment_baseline_test.go` records the row store's cost for the column-segment
gates: a synthday-shaped workload (`internal/synthhop`: the ai-soc mix of HOP,
ORIGIN, VATTR and CATTR at the three synthday sizes, or HOP alone) written
through `g.Rels().AddWithTx` into memory, badger (ai-soc's `engine.OpenAt`
config) and badger lean; it reports resident B/rel, objects/rel, on-disk B/rel,
HOP `ByType`/`ForEachByType` rows/s and write time. It is skipped unless
`RHO_TKG_SEGMENT_BASELINE` is set (a tiny smoke variant always runs):

```bash
RHO_TKG_SEGMENT_BASELINE=790k,3.15M,12.6M RHO_TKG_SEGMENT_BASELINE_REPEAT=2 \
  go test -run 'TestSegmentBaseline$' -v -count=1 -timeout 0 ./bench
```

Options: `RHO_TKG_SEGMENT_BASELINE_MODES` (memory,badger,lean),
`RHO_TKG_SEGMENT_BASELINE_WORKLOADS` (mix,hop), `RHO_TKG_SEGMENT_BASELINE_DIR`.
The 12.6 M badger configurations take ~1 min each and ~4 GB of heap. Measured
numbers live in CHANGELOG and ADR-0011 §6.

## Running

```bash
# Every benchmark, once each (a smoke check that all of them still run; no CI
# job runs this - the PR gate is bench.yml below):
go test -bench=. -benchtime=1x -run '^$' ./bench

# The informal/local suite (fixed 0.3s per sub-benchmark, one count):
make bench

# Capture a per-machine baseline for later comparison:
make bench-baseline

# Compare the current working tree against that baseline (installs
# benchstat if missing) and fail if any time-gated scenario regressed by >15%
# (REGRESSION_THRESHOLD_PCT) or an allocs-gated row (PinnedRelPropertyLookup,
# LatestStamps) allocates more than ALLOCS_THRESHOLD_PCT (10) above baseline:
make bench-check
```

`bench/local-baseline.txt` (written by `make bench-baseline`) and the scratch
files `make bench-check` produces (`bench/local-current.txt`,
`bench/local-benchstat.csv`; `BENCH_CURRENT` and `BENCH_REPORT_CSV` override the
paths) are all gitignored (`bench/local-*`) — baselines
are **per-machine**, never commit one or compare a baseline captured on one
machine/instant against a run on another; hardware and background load swamp
real regressions at this scale.

## Noise caveats

- These scenarios run with `-count=1` for speed. `benchstat`'s own
  significance test needs several samples per side to say anything other
  than "~" (not significant) — at `n=1` it *always* prints "~" in its
  human-readable delta column, even for a real 100x regression. `bench-check`
  therefore does not read that column: `bench/bench-compare.sh` (which
  `bench/bench-check.sh` and the CI gate delegate to) uses
  `benchstat -format csv` (which prints the raw per-file numbers
  unconditionally) and computes the percentage itself, so real regressions
  are still caught even without statistical significance.
- The flip side: at `n=1`, ordinary system noise (a busy laptop, a shared CI
  runner, thermal throttling) can *also* clear the 15% threshold on the
  fastest scenarios (sub-microsecond ones like `TwoHop`/`PointReadHit` are
  the most sensitive, since a few hundred nanoseconds of scheduler jitter is
  already a large relative swing). Treat a `bench-check` failure as a
  *signal to look closer*, not as proof of a regression — re-run it, and
  prefer `make bench-graph-baseline` / `make bench-graph-production-small`
  (which support `BENCH_COUNT`/`PROD_BENCH_COUNT` > 1 for a real statistical
  comparison) before concluding a change made something slower.
- **Cross-revision micro-benchmarks** (optional, not a CI gate):
  `make bench-compare` runs `bench/bench-compare-revisions.sh`, which checks
  out each ref in a detached worktree and prints a TSV of
  AddNode/AddRelationship/label mutator timings. Defaults are `HEAD`,
  `v4.27.0`, `v4.26.0`. Pass explicit refs to the script for anything else.
  This is diagnostic only — the blocking regression gate remains
  `bench-gate` / `make bench-check`.
- **Fixed: write-scenario accumulation drift.** `Ingest1kSingle`,
  `Ingest10kBatch`, and `BulkAddNodes10k` used to reuse one graph across every
  `b.Loop()` iteration, so ns/op grew with the run's total iteration count
  instead of measuring a constant-size ingest (an iteration late in a
  higher-`-benchtime` run wrote into an already much larger graph than an
  iteration early in a 1x run) — this alone produced a false-positive >15%
  regression flag on an otherwise-untouched scenario. Each of the three now
  builds and tears down a fresh, empty graph every iteration with
  construction/teardown excluded from timing (see `ingest_test.go`), so ns/op
  is flat regardless of `-benchtime` iteration count; any remaining spread
  between runs is ordinary single-sample process noise (comparable to the
  spread between independent `-benchtime=1x` invocations), not systematic
  drift.

## CI: `.github/workflows/bench.yml` (blocking PR gate + manual dispatch)

The flip-to-blocking plan is WIRED (owner-approved 2026-07-29). Two jobs, one
comparator (`bench/bench-compare.sh` — the same threshold logic
`bench-check.sh` uses locally, factored so it exists exactly once):

- **`bench-gate` (pull_request, BLOCKING)**: on a PR touching `pkg/**`,
  `bench/**`, `go.mod`, or the workflow itself, benchmarks the merge-base
  and HEAD on the SAME runner with `-count=3` (benchstat compares MEDIANS,
  absorbing single-sample scheduler spikes) and FAILS the check when a core
  scenario's median time regresses beyond `REGRESSION_THRESHOLD_PCT=30`
  (deliberately looser than the 15% local gate — GitHub-hosted runners are
  noisier than a dedicated machine). Two `go test` runs feed it: the
  `BENCH_FILTER` set (`PointReadHit`, `LabelScan10k`, `TwoHop`,
  `TemporalPoint`, `AsOfPin`, the `Ingest` prefix, `BulkAddNodes10k`,
  `ANNSearch10k`, `OrderedTopK`, the `CompositeLookup` prefix,
  `RelPropertyLookup10k`, `RelHistoryPlain`, `RelHasHistory`,
  `RelEffectiveTimeline`, `RelEffectiveLoop`, `ForEachRelEffectiveByType`,
  `NodeAtTxLongChain`, `NodeEffectiveTimelineLongChain`; `-count=3`) and the
  `PINNED_REL_FILTER` set (`PinnedRelPropertyLookup`, `LatestStamps`;
  `-count=5`), both at `-benchtime=0.3s`. The two multi-minute measurement
  STUDIES (`PinnedScanScaling`, `ChangeLogTxSerialization`) are excluded
  from the gate via the `-bench` filter — they are one-off measurement
  campaigns, not regression canaries. The gate rules live in
  `bench/bench-gate.awk` (tests: `bench_gate_test.go` over real benchstat CSV
  fixtures in `testdata/gate/`): only the `sec/op` block is read as time
  (a custom `b.ReportMetric` unit such as `build-ms` is ignored), and one
  benchmark family — `PinnedRelPropertyLookup` and `LatestStamps`
  (`ALLOCS_GATE_FAMILY`) — is **allocs-gated** (`ALLOCS_THRESHOLD_PCT=10`,
  5 samples; a row with a 0 allocs/op baseline fails as soon as it allocates);
  its time rows are reported, not gated, on shared hosts (the aggregate
  `geomean` row is recomputed over the time-gated rows only). Reason: identical code swung
  +42..+178 % in time on that family while allocs/op did not move at all, and
  the regression it exists to catch — the lookup falling back to the history
  fold — shows as 1,423 -> 106,555 allocs/op. Opt-in time canary for quiet
  runners: `TIME_CANARY=canary` time-gates its 8 rows (memory | badger x
  {1type/sigma, 5types/unrelated-x10} x matches200) again; any other value is
  used as the regex. `ALLOCS_GATE_FAMILY=none` time-gates every row. A failure is a *signal to re-run
  once, then look closer*: a repeated failure on the same PR is a real
  regression signal.
- **`bench-compare` (workflow_dispatch, informational)**: the original
  manual full-suite comparison (single `-count=1` sample, studies included)
  for sanity-checking a perf claim from a fork or ad-hoc branch.

Local `make bench-baseline` + `make bench-check` on stable hardware remains
the authoritative high-resolution tooling; the CI gate is the coarse
backstop that catches large regressions before merge.

## Updating baselines intentionally

After a deliberate performance change (optimization or an accepted
trade-off), just re-run `make bench-baseline` locally before continuing work
that depends on comparing against the new normal — there is nothing to
commit; the baseline file is per-machine and gitignored by design.
