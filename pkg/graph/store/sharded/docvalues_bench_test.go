package sharded_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkShardedDocValues: a sum over one numeric column of a label spread
// over 2 or 4 lane slots of a sharded store (sigma-tkgd C3v question 7, C7
// open question 5). rows reads the property from every node (the path the
// store offered before it built columns); docvalues streams the slots'
// columns; rowat reads the snapshot by position with 1 and 4 workers.
// lookup/* reads every member's value by ID: Lend + GetProperty against the
// snapshot's Row.
func BenchmarkShardedDocValues(b *testing.B) {
	for _, n := range []int{20000, 200000} {
		for _, lanes := range []uint8{2, 4} {
			b.Run(fmt.Sprintf("n=%d/slots=%d", n, lanes), func(b *testing.B) {
				benchShardedDocValues(b, n, lanes)
			})
		}
	}
}

func benchShardedDocValues(b *testing.B, n int, lanes uint8) {
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2 + lanes})
	if err != nil {
		b.Fatal(err)
	}
	g, err := graph.New(graph.Config{Store: st, SnowflakeNodeID: 0, IngestLanes: lanes})
	if err != nil {
		b.Fatal(err)
	}
	defer g.Close()
	ids := make([]types.NodeID, 0, n)
	per := n / int(lanes)
	for l := range int(lanes) {
		sess, err := g.Ingest().NewSession(ingest.IngestOptions{Concurrent: true})
		if err != nil {
			b.Fatal(err)
		}
		for i := range per {
			nd, err := sess.AddNode([]string{"P"}, map[string]any{"age": int64((l*per + i) % 90)})
			if err != nil {
				b.Fatal(err)
			}
			ids = append(ids, nd.ID())
		}
		if _, err := sess.Submit(); err != nil {
			b.Fatal(err)
		}
		_ = sess.Close()
	}
	var want int64
	for i := range per * int(lanes) {
		want += int64(i % 90)
	}
	check := func(b *testing.B, sum int64) {
		if sum != want {
			b.Fatalf("sum %d, want %d", sum, want)
		}
	}
	b.Run("rows", func(b *testing.B) {
		for b.Loop() {
			var sum int64
			if err := g.Nodes().ForEachByLabel("P", storepkg.QueryOpts{NoSort: true}, func(nd *types.Node) bool {
				if v, ok := nd.GetProperty("age"); ok {
					sum += v.(int64)
				}
				return true
			}); err != nil {
				b.Fatal(err)
			}
			check(b, sum)
		}
	})
	b.Run("docvalues", func(b *testing.B) {
		for b.Loop() {
			var sum int64
			if _, ok, err := g.Nodes().ForEachDocValues("P", []string{"age"}, func(_ types.NodeID, vals []any, present []bool) bool {
				if present[0] {
					sum += vals[0].(int64)
				}
				return true
			}); err != nil || !ok {
				b.Fatal(ok, err)
			}
			check(b, sum)
		}
	})
	for _, workers := range []int{1, 4} {
		b.Run(fmt.Sprintf("rowat/workers=%d", workers), func(b *testing.B) {
			for b.Loop() {
				reader, _, ok, err := g.Nodes().DocValuesSnapshot("P", []string{"age"})
				if err != nil || !ok {
					b.Fatal(ok, err)
				}
				rr := reader.(types.NodeColumnRowReader)
				sums := make([]int64, workers)
				per := (rr.Len() + workers - 1) / workers
				var wg sync.WaitGroup
				for w := range workers {
					lo, hi := w*per, min((w+1)*per, rr.Len())
					wg.Add(1)
					go func() {
						defer wg.Done()
						vals, present := make([]any, 1), make([]bool, 1)
						for i := lo; i < hi; i++ {
							rr.RowAt(i, vals, present)
							if present[0] {
								sums[w] += vals[0].(int64)
							}
						}
					}()
				}
				wg.Wait()
				var sum int64
				for _, s := range sums {
					sum += s
				}
				check(b, sum)
			}
		})
	}
	ctx := context.Background()
	b.Run("lookup/lend", func(b *testing.B) {
		for b.Loop() {
			var sum int64
			for _, id := range ids {
				nd, err := g.Nodes().Lend(ctx, id)
				if err != nil {
					b.Fatal(err)
				}
				v, _ := nd.GetProperty("age")
				sum += v.(int64)
			}
			check(b, sum)
		}
	})
	b.Run("lookup/row", func(b *testing.B) {
		reader, _, ok, err := g.Nodes().DocValuesSnapshot("P", []string{"age"})
		if err != nil || !ok {
			b.Fatal(ok, err)
		}
		vals, present := make([]any, 1), make([]bool, 1)
		for b.Loop() {
			var sum int64
			for _, id := range ids {
				if !reader.Row(id, vals, present) {
					b.Fatal("not a row")
				}
				sum += vals[0].(int64)
			}
			check(b, sum)
		}
	})
}
