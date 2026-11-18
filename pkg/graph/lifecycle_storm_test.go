package graph_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Lifecycle storm: transactions, standalone writes, scans, and temporal
// queries hammer the graph while Close fires mid-flight. Every worker must
// terminate within the watchdog window (no deadlock against Close's
// lock-drain), nothing may panic, and every error must belong to the
// fail-closed family — anything else means a path leaked an internal error
// or, worse, half-applied a mutation on a closing store.
func TestLifecycleStormCloseMidFlight(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	for name, open := range frozenTestBackends(t) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			g := open(t)

			seed := make([]*types.Node, 0, 10)
			for i := 0; i < 10; i++ {
				n, err := g.Nodes().Add(ctx, []string{"Storm"}, map[string]any{"i": i})
				if err != nil {
					t.Fatalf("seed: %v", err)
				}
				seed = append(seed, n)
			}

			allowed := func(err error) bool {
				switch {
				case err == nil,
					errors.Is(err, graphpkg.ErrGraphClosed),
					errors.Is(err, graphpkg.ErrAlreadyClosed),
					errors.Is(err, storepkg.ErrStoreClosed),
					errors.Is(err, graphpkg.ErrTxDone),
					errors.Is(err, graphpkg.ErrNodeNotFound),
					errors.Is(err, graphpkg.ErrRelNotFound),
					errors.Is(err, graphpkg.ErrLabelNotFound),
					errors.Is(err, storepkg.ErrNoVersionValidAt):
					return true
				}
				return false
			}

			var wg sync.WaitGroup
			bad := make(chan error, 64)
			report := func(kind string, err error) {
				if !allowed(err) {
					select {
					case bad <- fmt.Errorf("%s: unexpected error class: %w", kind, err):
					default:
					}
				}
			}
			stop := make(chan struct{})
			stopWorkers := sync.OnceFunc(func() { close(stop) })
			t.Cleanup(stopWorkers) // a watchdog failure below still stops them

			// iters counts each worker's finished iterations, so the test
			// waits on progress rather than on the wall clock.
			var iters []*atomic.Int64
			worker := func(body func(i int)) {
				done := new(atomic.Int64)
				iters = append(iters, done)
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer func() {
						if r := recover(); r != nil {
							select {
							case bad <- fmt.Errorf("PANIC: %v", r):
							default:
							}
						}
					}()
					for i := 0; ; i++ {
						select {
						case <-stop:
							return
						default:
						}
						body(i)
						done.Add(1)
					}
				}()
			}
			// waitPast blocks until every worker has finished more than
			// floor[k] iterations, or fails the test at the watchdog.
			waitPast := func(what string, floor []int64) {
				deadline := time.Now().Add(30 * time.Second)
				for k := 0; k < len(iters); {
					if iters[k].Load() > floor[k] {
						k++
						continue
					}
					if time.Now().After(deadline) {
						t.Fatalf("worker %d stuck at %d iterations %s — deadlock against lifecycle gating", k, iters[k].Load(), what)
					}
					time.Sleep(100 * time.Microsecond)
				}
			}
			snapshot := func() []int64 {
				out := make([]int64, len(iters))
				for k, c := range iters {
					out[k] = c.Load()
				}
				return out
			}

			// Standalone writers.
			for w := 0; w < 2; w++ {
				worker(func(i int) {
					n := seed[i%len(seed)]
					report("SetProperty", g.Nodes().SetProperty(ctx, n.ID(), "k", i))
				})
			}
			// Tx runners.
			worker(func(i int) {
				err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
					if _, err := tx.AddNode([]string{"Storm"}, map[string]any{"tx": i}); err != nil {
						return err
					}
					_, err := tx.UpdateNode(seed[i%len(seed)].ID(), map[string]any{"u": i})
					return err
				})
				report("TxRun", err)
			})
			// Scanners + temporal readers.
			worker(func(i int) {
				_, err := g.Nodes().ByLabel("Storm", storepkg.QueryOpts{})
				report("ByLabel", err)
				_, err = g.Temporal().NodeAt(seed[i%len(seed)].ID(), types.Instant(time.Now().UnixMilli()))
				report("NodeAt", err)
			})

			// Close fires mid-flight: every worker has run a few
			// iterations against the open graph.
			warm := make([]int64, len(iters))
			for k := range warm {
				warm[k] = 5
			}
			waitPast("before Close", warm)
			if err := g.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
			// Workers observe closed errors and keep running until told to
			// stop: each finishes two more iterations, so at least one call
			// that began after Close returned — none wedges on the closed graph.
			atClose := snapshot()
			for k := range atClose {
				atClose[k] += 2
			}
			waitPast("after Close", atClose)
			stopWorkers()

			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(30 * time.Second):
				t.Fatalf("workers did not terminate after Close — deadlock against lifecycle gating")
			}
			close(bad)
			for err := range bad {
				t.Error(err)
			}

			// Close is idempotent.
			if err := g.Close(); err != nil && !errors.Is(err, graphpkg.ErrGraphClosed) {
				t.Errorf("second Close: %v", err)
			}
		})
	}
}
