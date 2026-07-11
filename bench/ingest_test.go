package bench

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
)

// Fixed per-iteration ingest sizes (see WP scenario names): the single-Add
// path is benchmarked at 1k nodes/iteration, the batch paths at 10k.
const (
	ingestSingleCount = 1000
	ingestBatchCount  = 10000
	bulkAddNodesCount = 10000
)

// The three ingest benchmarks below deliberately do NOT use the package's
// usual "for b.Loop() { ... }" protocol (see harness_test.go's package doc).
// That protocol is correct when a fixture is built ONCE and every iteration
// reads it (LabelScan10k, PointReadHit, ...): b.Loop() runs the whole
// calibration inside a single call to the benchmark function, so pre-loop
// setup is genuinely paid once. These three benchmarks are the opposite
// shape — each iteration WRITES thousands of new nodes into the graph — so a
// fixture built once outside the loop keeps growing across iterations, and
// ns/op silently drifts upward with -benchtime iteration count instead of
// measuring a constant-size ingest (verified: BenchmarkIngest10kBatch/badger
// measured ~15.5ms/op at -benchtime=1x vs ~29.7ms/op at -benchtime=20x on
// the old for-b.Loop()-with-one-shared-graph shape).
//
// The fix needs a FRESH graph every iteration with construction/teardown
// excluded from timing. That rules out b.Loop() too: B.StopTimer() sets an
// internal "poison" bit that forces the next b.Loop() re-check onto its slow
// path, which then hard-fails the benchmark ("B.Loop called with timer
// stopped") unless a StartTimer() resume is squeezed in before the loop
// condition is re-evaluated — a fragile, non-obvious dance for a benefit
// (skipping repeat outer-function invocations during time-based calibration)
// that doesn't apply here anyway, since nothing persists across those repeat
// invocations once the graph is built INSIDE the loop. So these three use
// the classic "for i := 0; i < b.N; i++ { ... }" loop with manual
// StopTimer()/StartTimer() around the untimed per-iteration setup/teardown —
// the documented, unconditionally-safe pattern for per-iteration fixtures.

// newIngestGraph constructs a fresh graph for bc for exactly one timed
// iteration. Unlike harness_test.go's newBenchGraph, it does NOT register a
// b.Cleanup close: these benchmarks open and close a graph on every
// iteration (holding potentially many 10k+-node graphs open until
// sub-benchmark teardown would defeat the purpose of this file — see the
// package-level comment above), so the caller closes it explicitly inside
// the timed loop body, immediately after that iteration's writes complete.
func newIngestGraph(tb testing.TB, bc backendCase) *graph.Graph {
	tb.Helper()
	cfg := graph.Config{SnowflakeNodeID: bc.snowflakeNode}
	if bc.badger {
		cfg.BadgerInMemory = true
	}
	g, err := graph.New(cfg)
	if err != nil {
		tb.Fatalf("%s: graph.New: %v", bc.name, err)
	}
	return g
}

// BenchmarkIngest1kSingle measures ingesting 1,000 nodes one at a time via
// the standalone g.Nodes().Add door — the naive/no-batching baseline that
// Ingest10kBatch and BulkAddNodes10k are compared against. Each iteration
// gets a fresh, empty graph (see package comment above).
func BenchmarkIngest1kSingle(b *testing.B) {
	for _, bc := range backendCases {
		b.Run(bc.name, func(b *testing.B) {
			ctx := benchCtx()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				func() {
					b.StopTimer()
					g := newIngestGraph(b, bc)
					// Deferred so a b.Fatalf mid-iteration (which unwinds via
					// runtime.Goexit and would skip a bare trailing Close) still
					// stops the timer and releases the store's flush goroutine.
					defer func() {
						b.StopTimer()
						if err := g.Close(); err != nil {
							b.Logf("%s: graph.Close: %v", bc.name, err)
						}
					}()
					b.StartTimer()

					for n := 0; n < ingestSingleCount; n++ {
						if _, err := g.Nodes().Add(ctx, []string{"Ingest"}, map[string]any{"seq": n}); err != nil {
							b.Fatalf("add node %d: %v", n, err)
						}
					}

				}()
			}
		})
	}
}

// BenchmarkIngest10kBatch measures ingesting 10,000 nodes queued through
// BatchBuilder.AddNode and committed in a single Execute — the caller-visible-
// skeleton batch path (nodes usable as relationship endpoints within the batch).
// Each iteration gets a fresh, empty graph (see package comment above).
func BenchmarkIngest10kBatch(b *testing.B) {
	for _, bc := range backendCases {
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				func() {
					b.StopTimer()
					g := newIngestGraph(b, bc)
					// Deferred so a b.Fatalf mid-iteration (which unwinds via
					// runtime.Goexit and would skip a bare trailing Close) still
					// stops the timer and releases the store's flush goroutine.
					defer func() {
						b.StopTimer()
						if err := g.Close(); err != nil {
							b.Logf("%s: graph.Close: %v", bc.name, err)
						}
					}()
					b.StartTimer()

					_, err := g.Batch().Run(func(bb *graph.BatchBuilder) error {
						for n := 0; n < ingestBatchCount; n++ {
							if _, err := bb.AddNode([]string{"Ingest"}, map[string]any{"seq": n}); err != nil {
								return err
							}
						}
						return nil
					})
					if err != nil {
						b.Fatalf("batch ingest: %v", err)
					}

				}()
			}
		})
	}
}

// BenchmarkBulkAddNodes10k measures ingesting 10,000 nodes via
// BatchBuilder.AddNodes — the write-only bulk path (no caller-visible node
// skeletons) enabled by hash-suffix precomputation, compared against the
// per-node AddNode batch path (Ingest10kBatch) at the same scale. Each
// iteration gets a fresh, empty graph (see package comment above).
func BenchmarkBulkAddNodes10k(b *testing.B) {
	for _, bc := range backendCases {
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				func() {
					b.StopTimer()
					g := newIngestGraph(b, bc)
					// Deferred so a b.Fatalf mid-iteration (which unwinds via
					// runtime.Goexit and would skip a bare trailing Close) still
					// stops the timer and releases the store's flush goroutine.
					defer func() {
						b.StopTimer()
						if err := g.Close(); err != nil {
							b.Logf("%s: graph.Close: %v", bc.name, err)
						}
					}()
					b.StartTimer()

					_, err := g.Batch().Run(func(bb *graph.BatchBuilder) error {
						return bb.AddNodes([]string{"Ingest"}, map[string]any{"seq": 1}, bulkAddNodesCount)
					})
					if err != nil {
						b.Fatalf("bulk add nodes: %v", err)
					}

				}()
			}
		})
	}
}
