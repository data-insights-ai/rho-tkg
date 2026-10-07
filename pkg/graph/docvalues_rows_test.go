package graph_test

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

type columnRow struct {
	id      types.NodeID
	age     any
	present bool
}

// readColumnRows reads every position of a row reader with workers goroutines,
// each taking one contiguous range.
func readColumnRows(rr types.NodeColumnRowReader, workers int) []columnRow {
	out := make([]columnRow, rr.Len())
	var wg sync.WaitGroup
	per := (rr.Len() + workers - 1) / workers
	for w := range workers {
		lo, hi := w*per, min((w+1)*per, rr.Len())
		wg.Add(1)
		go func() {
			defer wg.Done()
			vals, present := make([]any, 1), make([]bool, 1)
			for i := lo; i < hi; i++ {
				id := rr.RowAt(i, vals, present)
				out[i] = columnRow{id, vals[0], present[0]}
			}
		}()
	}
	wg.Wait()
	return out
}

// A label's column snapshot reads by position on memory, badger and tiered:
// Len is the member count, the positions hold every member once with the
// values ForEachDocValues streams, ascending on memory and badger, and four
// workers on disjoint ranges read the same rows as one. Two-phase: a snapshot
// taken before an update keeps the old value at its position, one taken after
// has the new. A position outside the range panics.
func TestDocValuesSnapshotRowsByPosition(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		if b.name == "sharded" {
			t.Skip("sharded has no column snapshot")
		}
		ctx := context.Background()
		label := "P"
		if b.tiered {
			label = "Ref"
		}
		var first types.NodeID
		for i := range 300 {
			props := map[string]any{"age": int64(i)}
			if i%7 == 0 {
				props = map[string]any{"other": "x"} // a member without the property
			}
			n, err := g.Nodes().Add(ctx, []string{label}, props)
			if err != nil {
				t.Fatal(err)
			}
			if i == 1 {
				first = n.ID()
			}
		}
		if _, err := g.Nodes().Add(ctx, []string{"Elsewhere"}, map[string]any{"age": int64(999)}); err != nil {
			t.Fatal(err)
		}
		reader, _, ok, err := g.Nodes().DocValuesSnapshot(label, []string{"age"})
		if err != nil || !ok {
			t.Fatalf("snapshot: %v %v", ok, err)
		}
		rr, isRows := reader.(types.NodeColumnRowReader)
		if !isRows {
			t.Fatal("the snapshot does not read by position")
		}
		if rr.Len() != 300 {
			t.Fatalf("Len = %d, want 300", rr.Len())
		}
		var streamed []columnRow
		if _, ok, err := g.Nodes().ForEachDocValues(label, []string{"age"}, func(id types.NodeID, vals []any, present []bool) bool {
			streamed = append(streamed, columnRow{id, vals[0], present[0]})
			return true
		}); err != nil || !ok {
			t.Fatalf("ForEachDocValues: %v %v", ok, err)
		}
		one := readColumnRows(rr, 1)
		four := readColumnRows(rr, 4)
		if !slices.Equal(one, four) {
			t.Fatal("four workers read other rows than one")
		}
		byID := func(rows []columnRow) []columnRow {
			s := slices.Clone(rows)
			slices.SortFunc(s, func(a, b columnRow) int { return int(a.id.SnowflakeID() - b.id.SnowflakeID()) })
			return s
		}
		if !slices.Equal(byID(one), byID(streamed)) {
			t.Fatal("the positions do not hold the streamed rows")
		}
		if !b.tiered && !slices.IsSortedFunc(one, func(a, b columnRow) int { return int(a.id.SnowflakeID() - b.id.SnowflakeID()) }) {
			t.Fatal("positions are not in ascending ID order")
		}
		absent := 0
		for _, r := range one {
			if !r.present {
				absent++
			}
		}
		if absent != 43 {
			t.Fatalf("%d members without the property, want 43", absent)
		}

		if _, err := g.Nodes().Update(ctx, first, map[string]any{"age": int64(-1)}); err != nil {
			t.Fatal(err)
		}
		after, _, ok, err := g.Nodes().DocValuesSnapshot(label, []string{"age"})
		if err != nil || !ok {
			t.Fatal(err)
		}
		find := func(rows []columnRow) any {
			for _, r := range rows {
				if r.id == first {
					return r.age
				}
			}
			return nil
		}
		if got := find(readColumnRows(rr, 2)); got != int64(1) {
			t.Fatalf("the earlier snapshot reads %v, want 1", got)
		}
		if got := find(readColumnRows(after.(types.NodeColumnRowReader), 2)); got != int64(-1) {
			t.Fatalf("the later snapshot reads %v, want -1", got)
		}
		func() {
			defer func() {
				if recover() == nil {
					t.Fatal("RowAt(Len) did not panic")
				}
			}()
			rr.RowAt(rr.Len(), make([]any, 1), make([]bool, 1))
		}()
	})
}

// BenchmarkDocValuesMorsels: summing one column of 200,000 members (memory)
// with ForEachDocValues from the start, against reading the snapshot's
// positions in ranges with 1, 4 and 8 workers (sigma-tkgd C4c store request
// 4: a column scan split across workers).
func BenchmarkDocValuesMorsels(b *testing.B) {
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 0})
	if err != nil {
		b.Fatal(err)
	}
	defer g.Close()
	const n = 200000
	batch, err := g.Batch().New()
	if err != nil {
		b.Fatal(err)
	}
	for i := range n {
		if _, err := batch.AddNode([]string{"P"}, map[string]any{"age": int64(i % 90)}); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := batch.Execute(); err != nil {
		b.Fatal(err)
	}
	const want = int64(n/90*(89*90/2) + (n%90)*(n%90-1)/2)
	b.Run("ForEachDocValues", func(b *testing.B) {
		for b.Loop() {
			var sum int64
			if _, ok, err := g.Nodes().ForEachDocValues("P", []string{"age"}, func(_ types.NodeID, vals []any, present []bool) bool {
				if present[0] {
					sum += vals[0].(int64)
				}
				return true
			}); err != nil || !ok || sum != want {
				b.Fatalf("sum %d, %v %v", sum, ok, err)
			}
		}
	})
	for _, workers := range []int{1, 4, 8} {
		b.Run(fmt.Sprintf("RowAt/workers=%d", workers), func(b *testing.B) {
			for b.Loop() {
				reader, _, ok, err := g.Nodes().DocValuesSnapshot("P", []string{"age"})
				if err != nil || !ok {
					b.Fatal(err)
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
						var s int64
						for i := lo; i < hi; i++ {
							rr.RowAt(i, vals, present)
							if present[0] {
								s += vals[0].(int64)
							}
						}
						sums[w] = s
					}()
				}
				wg.Wait()
				var sum int64
				for _, s := range sums {
					sum += s
				}
				if sum != want {
					b.Fatalf("sum %d, want %d", sum, want)
				}
			}
		})
	}
}
