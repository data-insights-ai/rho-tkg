package bench

import (
	"context"
	"sync"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
)

// pipelineIngestCount is the number of node-creates each timed iteration drives
// through the ADR-0006 ingest pipeline (node-create-dominated per the workload
// thesis). Matched to Ingest10kBatch so the pipeline is comparable to the batch
// and single-Add doors at the same scale.
const pipelineIngestCount = 10000

// pipelineSubmitEvery bounds how many prepared node-creates a producer
// accumulates before submitting a group (so the applier group-commits rather
// than flushing per node).
const pipelineSubmitEvery = 512

// newPipelineGraph builds a fresh graph for one timed iteration. Like
// newIngestGraph it does NOT register a Cleanup close — the caller closes it
// inside the timed loop body (these benchmarks open/close a graph per
// iteration; see ingest_test.go's package comment).
func newPipelineGraph(tb testing.TB, bc backendCase, changeLog bool) *graph.Graph {
	tb.Helper()
	cfg := graph.Config{SnowflakeNodeID: bc.snowflakeNode, ChangeLog: changeLog}
	if bc.badger {
		cfg.BadgerInMemory = true
	}
	g, err := graph.New(cfg)
	if err != nil {
		tb.Fatalf("%s: graph.New: %v", bc.name, err)
	}
	return g
}

// runPipelineOnce drives pipelineIngestCount node-creates through the pipeline,
// spread across `producers` goroutines each preparing on its own thread, and
// waits until every write is applied and visible. Returns nothing; a failure
// fails the benchmark.
func runPipelineOnce(b *testing.B, g *graph.Graph, producers int) {
	runPipelineOnceN(b, g, producers, pipelineIngestCount)
}

func runPipelineOnceN(b *testing.B, g *graph.Graph, producers, count int) {
	perProducer := count / producers
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			sess, err := g.Ingest().NewSession(ingest.IngestOptions{
				Sync:          false,
				DeclareLabels: []string{"Ingest"},
				QueueBound:    64,
			})
			if err != nil {
				b.Errorf("NewSession: %v", err)
				return
			}
			var last ingest.SubmitToken
			pending := 0
			for n := 0; n < perProducer; n++ {
				if _, err := sess.AddNode([]string{"Ingest"}, map[string]any{"seq": int64(n), "p": int64(p)}); err != nil {
					b.Errorf("AddNode: %v", err)
					return
				}
				pending++
				if pending >= pipelineSubmitEvery {
					tok, err := sess.Submit()
					if err != nil {
						b.Errorf("Submit: %v", err)
						return
					}
					last = tok
					pending = 0
				}
			}
			tok, err := sess.Submit()
			if err != nil {
				b.Errorf("Submit tail: %v", err)
				return
			}
			if tok.Seq != 0 {
				last = tok
			}
			if err := g.Ingest().WaitApplied(last); err != nil {
				b.Errorf("WaitApplied: %v", err)
			}
		}(p)
	}
	wg.Wait()
}

// BenchmarkIngestPipeline measures the prepare-parallel / apply-sequential
// pipeline at 1 and 8 producers, on memory and badger-in-memory, comparable to
// the single-Add baseline (BenchmarkIngest1kSingle scaled) and the batch door
// (BenchmarkIngest10kBatch). The 8-producer arm is the ADR-0006 §7 acceptance
// bar (must meaningfully exceed the single-threaded standalone rate).
func BenchmarkIngestPipeline(b *testing.B) {
	for _, bc := range backendCases {
		for _, producers := range []int{1, 8} {
			name := bc.name
			if producers == 1 {
				name += "/p1"
			} else {
				name += "/p8"
			}
			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					func() {
						b.StopTimer()
						g := newPipelineGraph(b, bc, false)
						defer func() {
							b.StopTimer()
							if err := g.Close(); err != nil {
								b.Logf("%s: graph.Close: %v", bc.name, err)
							}
						}()
						b.StartTimer()
						runPipelineOnce(b, g, producers)
					}()
				}
			})
		}
	}
}

// runConcurrentOnce drives pipelineIngestCount node-creates through CONCURRENT
// ingest sessions (§14 concurrent mode): each of the `producers` goroutines
// self-applies its own groups under the shared read lock — no single applier,
// no handoff.
func runConcurrentOnce(b *testing.B, g *graph.Graph, producers int) {
	perProducer := pipelineIngestCount / producers
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			sess, err := g.Ingest().NewSession(ingest.IngestOptions{
				Concurrent:    true,
				DeclareLabels: []string{"Ingest"},
			})
			if err != nil {
				b.Errorf("NewSession: %v", err)
				return
			}
			pending := 0
			for n := 0; n < perProducer; n++ {
				if _, err := sess.AddNode([]string{"Ingest"}, map[string]any{"seq": int64(n), "p": int64(p)}); err != nil {
					b.Errorf("AddNode: %v", err)
					return
				}
				pending++
				if pending >= pipelineSubmitEvery {
					if _, err := sess.Submit(); err != nil {
						b.Errorf("Submit: %v", err)
						return
					}
					pending = 0
				}
			}
			if _, err := sess.Submit(); err != nil {
				b.Errorf("Submit tail: %v", err)
			}
		}(p)
	}
	wg.Wait()
}

// BenchmarkIngestConcurrent measures the §14 concurrent mode (self-applying
// sessions, Lanes:N) at 1 and 8 producers on both backends — directly
// comparable to BenchmarkIngestPipeline (the strong-mode single applier over
// the same workload and group size).
func BenchmarkIngestConcurrent(b *testing.B) {
	for _, bc := range backendCases {
		for _, producers := range []int{1, 8} {
			name := bc.name
			if producers == 1 {
				name += "/p1"
			} else {
				name += "/p8"
			}
			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					func() {
						b.StopTimer()
						g := newPipelineGraph(b, bc, false)
						defer func() {
							b.StopTimer()
							if err := g.Close(); err != nil {
								b.Logf("%s: graph.Close: %v", bc.name, err)
							}
						}()
						b.StartTimer()
						runConcurrentOnce(b, g, producers)
					}()
				}
			})
		}
	}
}

// BenchmarkIngestPipelineChangeLog measures the group-commit pipeline WITH the
// change-log enabled versus without (M2 context — the group-commit design
// should shrink the per-op log overhead relative to the per-mutation tx-log
// serialization). Report the with/without ratio. 8 producers, both backends.
func BenchmarkIngestPipelineChangeLog(b *testing.B) {
	for _, bc := range backendCases {
		for _, cl := range []bool{false, true} {
			name := bc.name
			if cl {
				name += "/log-on"
			} else {
				name += "/log-off"
			}
			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					func() {
						b.StopTimer()
						g := newPipelineGraph(b, bc, cl)
						defer func() {
							b.StopTimer()
							if err := g.Close(); err != nil {
								b.Logf("%s: graph.Close: %v", bc.name, err)
							}
						}()
						b.StartTimer()
						runPipelineOnce(b, g, 8)
					}()
				}
			})
		}
	}
}

// BenchmarkIngestConcurrentChangeLog measures the CONCURRENT (§14) mode with
// the change-log on vs off at 8 sessions — after the producer-side payload
// pre-encode, the log's per-insert cost should be a fraction of the strong
// pipeline's (whose applier still pays the record buffering serially).
func BenchmarkIngestConcurrentChangeLog(b *testing.B) {
	for _, bc := range backendCases {
		for _, cl := range []bool{false, true} {
			name := bc.name
			if cl {
				name += "/log-on"
			} else {
				name += "/log-off"
			}
			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					func() {
						b.StopTimer()
						g := newPipelineGraph(b, bc, cl)
						defer func() {
							b.StopTimer()
							if err := g.Close(); err != nil {
								b.Logf("%s: graph.Close: %v", bc.name, err)
							}
						}()
						b.StartTimer()
						runConcurrentOnce(b, g, 8)
					}()
				}
			})
		}
	}
}

// durableCount is the per-iteration node count for the DURABLE (real badger dir
// + SyncWrites) comparison. Smaller than the in-memory count because a
// per-mutation fsync (the standalone baseline) is ms-scale.
const durableCount = 1000

// BenchmarkIngestSingleDurable is the single-threaded standalone-Add baseline on
// a REAL badger directory with SyncWrites — every Add pays its own fsync. This
// is the durable reference frame the pipeline's group commit is measured
// against (§4.6: the applier flushes once per group, not per mutation).
func BenchmarkIngestSingleDurable(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		func() {
			b.StopTimer()
			g, err := graph.New(graph.Config{SnowflakeNodeID: 2, BadgerDir: b.TempDir(), SyncWrites: true})
			if err != nil {
				b.Fatalf("graph.New: %v", err)
			}
			defer func() {
				b.StopTimer()
				_ = g.Close()
			}()
			b.StartTimer()
			for n := 0; n < durableCount; n++ {
				if _, err := g.Nodes().Add(ctx, []string{"Ingest"}, map[string]any{"seq": int64(n)}); err != nil {
					b.Fatalf("add: %v", err)
				}
			}
		}()
	}
}

// BenchmarkIngestPipelineDurable is the pipeline at 8 producers on a REAL badger
// directory with SyncWrites. The applier group-commits (one fsync per group),
// so it does not pay the per-mutation fsync BenchmarkIngestSingleDurable does —
// the honest Stage-1 win (fsync amortization) on a durable backend.
func BenchmarkIngestPipelineDurable(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		func() {
			b.StopTimer()
			g, err := graph.New(graph.Config{SnowflakeNodeID: 3, BadgerDir: b.TempDir(), SyncWrites: true})
			if err != nil {
				b.Fatalf("graph.New: %v", err)
			}
			defer func() {
				b.StopTimer()
				_ = g.Close()
			}()
			b.StartTimer()
			runPipelineOnceN(b, g, 8, durableCount)
		}()
	}
}
