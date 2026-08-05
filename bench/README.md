# bench/ — cross-backend performance suite

Committed benchmark scenarios for the graph engine, run against **both**
in-memory backends (`memory.Store` and Badger's `BadgerInMemory` mode) via a
shared harness (`harness_test.go`). Every scenario is a `func BenchmarkX(b
*testing.B)` with one `b.Run("memory", ...)` and one `b.Run("badger", ...)`
sub-benchmark, EXCEPT `ANNSearch10k` (memory-only — see its note below),
`PinnedScanScaling`, and `ChangeLogTxSerialization` (Badger-only — see their
notes below). Read-only scenarios (`PointReadHit`, `LabelScan10k`, `TwoHop`,
`TemporalPoint`, `AsOfPin`, `PinnedScanScaling`) build their fixture once per
(scenario, backend) pair using the `for b.Loop() { ... }` protocol (Go 1.24+)
so the setup cost is paid exactly once, never repeated across the testing
framework's timing-calibration passes.

The four write scenarios (`Ingest1kSingle`, `Ingest10kBatch`,
`BulkAddNodes10k` in `ingest_test.go`, and `ChangeLogTxSerialization` in
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
| `Ingest1kSingle` | 1,000 nodes ingested one at a time via `g.Nodes().Add` (the no-batching baseline) |
| `Ingest10kBatch` | 10,000 nodes ingested via `BatchBuilder.AddNode` + one `Execute` |
| `BulkAddNodes10k` | 10,000 nodes ingested via the write-only `BatchBuilder.AddNodes` bulk path |
| `ANNSearch10k` | `g.Index().SearchNearest` (k=10) over a 10k x 128-dim vector index, `hnsw` (default approximate engine) vs `bruteforce` (`VectorIndexOptions.UseBruteForce`) sub-variants — memory backend only (the vector index is store-level in-memory regardless of which Store backend hosts the node rows, so the badger sub-benchmark would be redundant) |
| `PinnedScanScaling` | Historical M1 measurement (original write-up retired; scenario remains in-tree): `ByLabel` plain vs `TxPin`/`TxAt`-pinned vs `NodesAsOf`-filtered, across {10k,100k} entities x {1,5,5+20%-deleted}-version churn x {broad,selective} label selectivity — BadgerInMemory only. Quantifies whether a pinned/as-of scan costs `O(current matches)` like plain `ByLabel` or `O(everything that ever had history)`. |
| `ChangeLogTxSerialization` | Historical M2 measurement (original write-up retired; scenario remains in-tree): aggregate ops/sec for standalone `Add` vs tx-per-batch (`Begin`->10x`AddNode`->`Commit`) at {1,4,16} concurrent goroutines, `Config.ChangeLog` on/off — a manual goroutine-fan-out harness reporting a custom `ops/sec` metric (not `ns/op`), since throughput scaling with goroutine count — not single-call latency — is what's under test. BadgerInMemory only (`ChangeLog` is a Badger/memory capability). |

`ANNSearch10k` note: measured locally (Apple M4 Max, `-benchtime=200x`) at
~193µs/op (`hnsw`) vs ~816µs/op (`bruteforce`) — roughly a 4x speedup at this
10k-point, single-query-vector-at-a-time scale. Brute-force is already a fast
flat scan at 10k points (a 128-dim linear scan is cheap in absolute terms), so
the *relative* HNSW advantage at this modest corpus size is more modest than
the 10-100x gaps typically reported in the ANN literature at 100k-1M+ point
scale, where the O(n) vs O(log n) gap widens substantially. Run
`go test -bench=BenchmarkANNSearch10k -benchtime=200x ./bench` to reproduce.

## Running

```bash
# Every benchmark, once each (the mandatory CI-equivalent smoke gate):
go test -bench=. -benchtime=1x -run '^$' ./bench

# The informal/local suite (fixed 0.3s per sub-benchmark, one count):
make bench

# Capture a per-machine baseline for later comparison:
make bench-baseline

# Compare the current working tree against that baseline (installs
# benchstat if missing) and fail if any scenario regressed time by >15%:
make bench-check
```

`bench/local-baseline.txt` (written by `make bench-baseline`) and the scratch
files `make bench-check` produces (`bench/local-current.txt`,
`bench/local-benchstat.csv`) are all gitignored (`bench/local-*`) — baselines
are **per-machine**, never commit one or compare a baseline captured on one
machine/instant against a run on another; hardware and background load swamp
real regressions at this scale.

## Noise caveats

- These scenarios run with `-count=1` for speed. `benchstat`'s own
  significance test needs several samples per side to say anything other
  than "~" (not significant) — at `n=1` it *always* prints "~" in its
  human-readable delta column, even for a real 100x regression. `bench-check`
  therefore does not read that column: `bench/bench-check.sh` uses
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
  noisier than a dedicated machine). The two multi-minute measurement
  STUDIES (`PinnedScanScaling`, `ChangeLogTxSerialization`) are excluded
  from the gate via the `-bench` filter — they are one-off measurement
  campaigns, not regression canaries. A failure is a *signal to re-run
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
