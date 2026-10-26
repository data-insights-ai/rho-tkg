package core

import (
	"testing"
	"time"

	eventspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// EVENTCLOCK (SCALE, ai-soc's Flux run k10262212: time.Now 15 % of CPU in the
// first minute, Core.now 35 % of it). Transaction and batch mutations took
// their event timestamp with c.now() before the publish checked for a
// publisher, so a graph without one read the clock, ran a CAS and reserved an
// instant per mutation, pushing the commit-clock floor ahead of the wall. With
// no publisher a mutation takes no event timestamp: under a frozen clock each
// workload reads the clock exactly as often as with a publisher, less the
// instants its events were stamped with (one per stamping c.now(), so the
// distinct event timestamps).
func TestEventClockNoPublisherReservesNoInstant(t *testing.T) {
	const n = 20
	start := time.UnixMilli(1_700_000_000_000)
	// seed creates n nodes, each with a rel to the next, in one transaction.
	seed := func(t *testing.T, g *Core) []*types.Node {
		tx, err := g.BeginTx()
		if err != nil {
			t.Fatal(err)
		}
		nodes := make([]*types.Node, n)
		for i := range nodes {
			if nodes[i], err = tx.AddNode([]string{"Page"}, map[string]any{"k": int64(i)}); err != nil {
				t.Fatal(err)
			}
		}
		for i := 1; i < n; i++ {
			if _, err := tx.AddRelationship("NEXT", nodes[i-1], nodes[i], nil); err != nil {
				t.Fatal(err)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		return nodes
	}
	inTx := func(t *testing.T, g *Core, fn func(tx *GraphTx) error) {
		tx, err := g.BeginTx()
		if err != nil {
			t.Fatal(err)
		}
		if err := fn(tx); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	inBatch := func(t *testing.T, g *Core, fn func(b *BatchBuilder) error) {
		b, err := NewBatchBuilder(g)
		if err != nil {
			t.Fatal(err)
		}
		if err := fn(b); err != nil {
			t.Fatal(err)
		}
		if _, err := b.Execute(); err != nil {
			t.Fatal(err)
		}
	}
	workloads := []struct {
		name string
		run  func(t *testing.T, g *Core, nodes []*types.Node)
	}{
		{"transaction creates", func(t *testing.T, g *Core, _ []*types.Node) {
			inTx(t, g, func(tx *GraphTx) error {
				for i := range n {
					if _, err := tx.AddNode([]string{"Page"}, map[string]any{"k": int64(i)}); err != nil {
						return err
					}
				}
				return nil
			})
		}},
		{"transaction updates", func(t *testing.T, g *Core, nodes []*types.Node) {
			inTx(t, g, func(tx *GraphTx) error {
				for _, nd := range nodes {
					if _, err := tx.UpdateNode(nd.ID(), map[string]any{"k": int64(-1)}); err != nil {
						return err
					}
				}
				return nil
			})
		}},
		{"transaction cascade deletes", func(t *testing.T, g *Core, nodes []*types.Node) {
			inTx(t, g, func(tx *GraphTx) error {
				for _, nd := range nodes {
					if err := tx.DeleteNode(nd.ID()); err != nil {
						return err
					}
				}
				return nil
			})
		}},
		{"batch updates", func(t *testing.T, g *Core, nodes []*types.Node) {
			inBatch(t, g, func(b *BatchBuilder) error {
				for _, nd := range nodes {
					if err := b.UpdateNode(nd.ID(), map[string]any{"k": int64(-1)}); err != nil {
						return err
					}
				}
				return nil
			})
		}},
		{"batch cascade deletes", func(t *testing.T, g *Core, nodes []*types.Node) {
			inBatch(t, g, func(b *BatchBuilder) error {
				for _, nd := range nodes {
					if err := b.DeleteNode(nd.ID()); err != nil {
						return err
					}
				}
				return nil
			})
		}},
	}
	// measure runs w on a fresh seeded graph under a frozen clock and returns
	// its clock reads and the distinct timestamps of the events it published.
	measure := func(t *testing.T, run func(t *testing.T, g *Core, nodes []*types.Node), publisher bool) (reads, stamps int) {
		g, err := New(Config{})
		if err != nil {
			t.Fatal(err)
		}
		defer g.Close()
		g.SetClockForTest(t, func() time.Time { reads++; return start })
		nodes := seed(t, g)
		seen := map[types.Instant]bool{}
		if publisher {
			bus := eventspkg.NewEventBus()
			bus.Subscribe(func(e eventspkg.Event) { seen[e.Timestamp] = true })
			if err := g.Events.SetSync(bus); err != nil {
				t.Fatal(err)
			}
		}
		reads = 0
		run(t, g, nodes)
		return reads, len(seen)
	}
	for _, w := range workloads {
		t.Run(w.name, func(t *testing.T) {
			bare, _ := measure(t, w.run, false)
			withPub, stamps := measure(t, w.run, true)
			if stamps == 0 {
				t.Fatal("the workload published no event: it checks nothing")
			}
			if bare != withPub-stamps {
				t.Fatalf("without a publisher %d clock reads, with one %d reads and %d event stamps: want %d",
					bare, withPub, stamps, withPub-stamps)
			}
		})
	}
}
