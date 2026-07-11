package bench

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
)

// M2 — CHANGE-LOG TX SERIALIZATION (tasks/measurements-2026-07-11.md).
//
// Hypothesis under test: with Config.ChangeLog enabled, a transaction's
// mutation door (GraphTx.AddNode et al.) takes c.mu.Lock EXCLUSIVE per
// mutation — see (*GraphTx).lockActiveCoreWrite, tx.go: when a per-tx
// change-log scope is active it upgrades from RLock to Lock and toggles
// SetLogDivert so no concurrent standalone mutation can have its own
// change-log record misrouted into the transaction's buffer (lesson 53-55).
// That per-mutation EXCLUSIVE lock is what this benchmark measures the cost
// of: concurrent tx-per-batch write throughput should stay flat as
// goroutines increase (every goroutine's every mutation serializes on the
// same c.mu), while concurrent STANDALONE write throughput (which never
// takes this per-mutation exclusive path — see NodeOps.Add's runUnderRLock)
// should scale roughly with goroutine count, changelog on or off, since
// standalone writes only ever contend on the much finer-grained 256-shard
// entity-lock manager.
//
// Manual goroutine worker harness (not b.RunParallel): each cell needs
// EXPLICIT control over which door (standalone vs tx-per-batch) a goroutine
// runs and a per-goroutine FIXED OP BUDGET, timed by wall-clock
// (time.Now/time.Since) around a sync.WaitGroup fan-out/fan-in — reported as
// an aggregate ops/sec custom metric via b.ReportMetric, not the framework's
// built-in ns/op (which would report per-CALL cost of the benchmark
// function, not per-mutation throughput across goroutines).
//
// Fixtures use the SAME classic "for i := 0; i < b.N; i++" + manual
// StopTimer/StartTimer shape as ingest_test.go (a fresh, empty BadgerInMemory
// graph built and closed OUTSIDE the timed region on every iteration) — this
// benchmark WRITES on every call, so the shared-once-fixture b.Loop() shape
// documented in harness_test.go does not apply; see ingest_test.go's package
// comment for why the classic b.N loop (not b.Loop()) is the correct,
// StopTimer-safe shape for a per-iteration fresh-graph write benchmark.
const (
	// changeLogOpsPerGoroutine is the FIXED node-create op budget every
	// goroutine executes, regardless of concurrency level — so total work
	// scales with goroutine count (1x = 2,000 ops, 4x = 8,000, 16x = 32,000),
	// the natural shape for a throughput-SCALING measurement: flat
	// aggregate ops/sec as concurrency rises is the serialization signature
	// under test, not an artifact of a shrinking per-goroutine share of a
	// fixed total.
	changeLogOpsPerGoroutine = 2000
	// changeLogTxBatchSize is the WP's "10 CreateNode" per transaction.
	changeLogTxBatchSize = 10
)

var changeLogConcurrencies = []int{1, 4, 16}

// changeLogDoors are the two write paths under comparison. door.name is the
// benchmark leaf name; work performs exactly changeLogOpsPerGoroutine node
// creates against g using goroutine index gi to keep property values
// distinct (not load-bearing — just avoids every goroutine racing to write
// literally identical properties, which is harmless but confusing to debug).
type changeLogDoor struct {
	name string
	work func(ctx context.Context, g *graph.Graph, gi int) error
}

var changeLogDoors = []changeLogDoor{
	{
		name: "standalone",
		work: func(ctx context.Context, g *graph.Graph, gi int) error {
			for k := 0; k < changeLogOpsPerGoroutine; k++ {
				if _, err := g.Nodes().Add(ctx, []string{"CLTx"}, map[string]any{"g": gi, "seq": k}); err != nil {
					return fmt.Errorf("standalone add (g=%d,seq=%d): %w", gi, k, err)
				}
			}
			return nil
		},
	},
	{
		name: "tx-batch",
		work: func(_ context.Context, g *graph.Graph, gi int) error {
			batches := changeLogOpsPerGoroutine / changeLogTxBatchSize
			for bi := 0; bi < batches; bi++ {
				err := g.Tx().Run(func(tx *graph.GraphTx) error {
					for k := 0; k < changeLogTxBatchSize; k++ {
						if _, err := tx.AddNode([]string{"CLTx"}, map[string]any{"g": gi, "batch": bi, "seq": k}); err != nil {
							return err
						}
					}
					return nil
				})
				if err != nil {
					return fmt.Errorf("tx batch (g=%d,batch=%d): %w", gi, bi, err)
				}
			}
			return nil
		},
	},
}

// runConcurrentHarness fans work out across concurrency goroutines (each
// running exactly once, doing its own changeLogOpsPerGoroutine-sized
// budget), waits for all to finish, and returns the WALL-CLOCK elapsed time
// for the whole fan-out/fan-in and the first error observed (if any).
func runConcurrentHarness(concurrency int, work func(gi int) error) (time.Duration, error) {
	var wg sync.WaitGroup
	errs := make([]error, concurrency)
	start := time.Now()
	for gi := 0; gi < concurrency; gi++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = work(idx)
		}(gi)
	}
	wg.Wait()
	elapsed := time.Since(start)
	for _, err := range errs {
		if err != nil {
			return elapsed, err
		}
	}
	return elapsed, nil
}

// BenchmarkChangeLogTxSerialization is M2: the full changeLog(on/off) x
// door(standalone/tx-batch) x concurrency(1/4/16) = 12-cell matrix. Every
// cell gets its own graph with a distinct SnowflakeNodeID (0..11, all within
// the valid 0-15 range) — never shared across cells, per the WP.
func BenchmarkChangeLogTxSerialization(b *testing.B) {
	type cell struct {
		changeLog   bool
		door        changeLogDoor
		concurrency int
	}
	var cells []cell
	for _, cl := range []bool{false, true} {
		for _, door := range changeLogDoors {
			for _, conc := range changeLogConcurrencies {
				cells = append(cells, cell{changeLog: cl, door: door, concurrency: conc})
			}
		}
	}

	for i, c := range cells {
		snowflakeID := int64(i)
		name := fmt.Sprintf("changelog=%v/%s/conc=%d", c.changeLog, c.door.name, c.concurrency)
		b.Run(name, func(b *testing.B) {
			ctx := benchCtx()
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				func() {
					b.StopTimer()
					g, err := graph.New(graph.Config{
						SnowflakeNodeID: snowflakeID,
						BadgerInMemory:  true,
						ChangeLog:       c.changeLog,
					})
					if err != nil {
						b.Fatalf("graph.New: %v", err)
					}
					defer func() {
						b.StopTimer()
						if err := g.Close(); err != nil {
							b.Logf("%s: graph.Close: %v", name, err)
						}
					}()
					b.StartTimer()

					elapsed, harnessErr := runConcurrentHarness(c.concurrency, func(gi int) error {
						return c.door.work(ctx, g, gi)
					})

					b.StopTimer()
					if harnessErr != nil {
						b.Fatalf("%s: %v", name, harnessErr)
					}
					totalOps := c.concurrency * changeLogOpsPerGoroutine
					opsPerSec := float64(totalOps) / elapsed.Seconds()
					b.ReportMetric(opsPerSec, "ops/sec")
					b.StartTimer()
				}()
			}
		})
	}
}
