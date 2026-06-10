package graph_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
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

			worker := func(body func(i int)) {
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
					}
				}()
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

			time.Sleep(30 * time.Millisecond)
			if err := g.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
			// Workers should observe closed errors and keep running until
			// told to stop — prove no call wedges against the closed graph.
			time.Sleep(20 * time.Millisecond)
			close(stop)

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
