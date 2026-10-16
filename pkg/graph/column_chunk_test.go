package graph_test

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// chunkCell renders column c of chunk row i as RowAt hands it out (the
// boxed value, nil where absent).
func chunkCell(t *testing.T, chunk *types.ColumnChunk, c, i int) any {
	t.Helper()
	v := chunk.Cols[c]
	if !v.Present[i] {
		return nil
	}
	switch v.Kind {
	case types.ColumnVectorInt:
		return v.Ints[i]
	case types.ColumnVectorFloat:
		return v.Flts[i]
	case types.ColumnVectorNumeric:
		if v.IsFloat[i] {
			return v.Flts[i]
		}
		return v.Ints[i]
	case types.ColumnVectorString:
		return v.Dict[v.Codes[i]]
	}
	t.Fatalf("column %d: kind %d", c, v.Kind)
	return nil
}

// assertChunksMatchRows reads every chunk bound of a list (empty, single
// rows, ranges, the whole snapshot) through ReadChunk into one reused chunk
// and compares each row with RowAt: IDs, presence, values and Go types.
func assertChunksMatchRows(t *testing.T, name string, reader types.NodeColumnReader, width int, rng *rand.Rand) {
	t.Helper()
	cr, ok := reader.(types.NodeColumnChunkReader)
	if !ok {
		t.Fatalf("%s: snapshot %T reads no chunks", name, reader)
	}
	n := cr.Len()
	bounds := [][2]int{{0, 0}, {0, n}, {n, n}}
	if n > 0 {
		bounds = append(bounds, [2]int{0, 1}, [2]int{n - 1, n})
	}
	for range 20 {
		a, b := rng.IntN(n+1), rng.IntN(n+1)
		bounds = append(bounds, [2]int{min(a, b), max(a, b)})
	}
	var chunk types.ColumnChunk
	vals, present := make([]any, width), make([]bool, width)
	for _, bd := range bounds {
		cr.ReadChunk(bd[0], bd[1], &chunk)
		if len(chunk.IDs) != bd[1]-bd[0] || len(chunk.Cols) != width {
			t.Fatalf("%s %v: chunk of %d rows, %d columns", name, bd, len(chunk.IDs), len(chunk.Cols))
		}
		for i := range chunk.IDs {
			id := cr.RowAt(bd[0]+i, vals, present)
			if chunk.IDs[i] != id {
				t.Fatalf("%s %v row %d: id %d, RowAt %d", name, bd, i, chunk.IDs[i], id)
			}
			for c := range width {
				got, want := chunkCell(t, &chunk, c, i), vals[c]
				if !present[c] {
					want = nil
				}
				if fmt.Sprintf("%T %v", got, got) != fmt.Sprintf("%T %v", want, want) {
					t.Fatalf("%s %v row %d col %d: chunk %T %v, RowAt %T %v", name, bd, i, c, got, got, want, want)
				}
			}
		}
	}
}

// TestColumnChunksMatchRowAt (sigma-tkgd round 9, W10's typed chunk scan):
// on every backend the label's column snapshot reads by typed chunks
// exactly what RowAt hands out per position — integers, floats, a mixed
// numeric column, strings, absent values, an all-absent column, a uint64
// above MaxInt64 and a float32 — current and as of a pin, the pinned
// snapshot unchanged by later updates, deletes and a retraction (two-phase).
func TestColumnChunksMatchRowAt(t *testing.T) {
	keys := []string{"i", "f", "m", "s", "none"}
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		rng := rand.New(rand.NewPCG(9, 4))
		var ids []types.NodeID
		for k := range 300 {
			props := map[string]any{}
			if k%7 != 0 {
				props["i"] = int64(rng.IntN(1000) - 500)
			}
			if k%5 != 0 {
				props["f"] = rng.Float64() * 100
			}
			switch k % 4 {
			case 0:
				props["m"] = int64(k)
			case 1:
				props["m"] = float32(k) + 0.5
			case 2:
				props["m"] = uint64(math.MaxInt64) + uint64(k)
			}
			if k%3 != 0 {
				props["s"] = fmt.Sprintf("v%d", rng.IntN(20))
			}
			n, err := g.Nodes().Add(ctx, []string{"C"}, props)
			if err != nil {
				t.Fatal(err)
			}
			ids = append(ids, n.ID())
		}
		snap, _, ok, err := g.Nodes().DocValuesSnapshot("C", keys)
		if err != nil || !ok {
			t.Fatalf("snapshot: ok=%v err=%v", ok, err)
		}
		assertChunksMatchRows(t, b.name+" current", snap, len(keys), rng)
		kinds := snap.(types.NodeColumnChunkReader)
		var chunk types.ColumnChunk
		kinds.ReadChunk(0, kinds.Len(), &chunk)
		wantKinds := []types.ColumnVectorKind{types.ColumnVectorInt, types.ColumnVectorFloat, types.ColumnVectorNumeric,
			types.ColumnVectorString, types.ColumnVectorInt}
		for c, k := range wantKinds {
			if chunk.Cols[c].Kind != k {
				t.Fatalf("%s column %s: kind %d, want %d", b.name, keys[c], chunk.Cols[c].Kind, k)
			}
		}

		pin, err := g.Temporal().NowTx()
		if err != nil {
			t.Fatal(err)
		}
		before := map[types.NodeID][]any{}
		vals, present := make([]any, len(keys)), make([]bool, len(keys))
		rows := snap.(types.NodeColumnRowReader)
		for i := range rows.Len() {
			id := rows.RowAt(i, vals, present)
			before[id] = append([]any(nil), vals...)
		}
		// Phase 2: writes after the pin.
		for k, id := range ids[:60] {
			switch k % 3 {
			case 0:
				if _, err := g.Nodes().Update(ctx, id, map[string]any{"i": int64(9999), "s": "changed"}); err != nil {
					t.Fatal(err)
				}
			case 1:
				if err := g.Nodes().Delete(ctx, id); err != nil {
					t.Fatal(err)
				}
			}
		}
		retracted, err := g.Nodes().Add(ctx, []string{"C"}, map[string]any{"i": int64(1), "s": "gone"})
		if err != nil {
			t.Fatal(err)
		}
		if err := g.Nodes().Retract(ctx, retracted.ID()); err != nil {
			t.Fatal(err)
		}
		asOf, _, ok, err := g.Nodes().DocValuesSnapshotAsOf("C", keys, pin)
		if err != nil || !ok {
			t.Fatalf("as-of snapshot: ok=%v err=%v", ok, err)
		}
		assertChunksMatchRows(t, b.name+" as of", asOf, len(keys), rng)
		ac := asOf.(types.NodeColumnChunkReader)
		ac.ReadChunk(0, ac.Len(), &chunk)
		if len(chunk.IDs) != len(before) {
			t.Fatalf("%s as of: %d rows, want the %d at the pin", b.name, len(chunk.IDs), len(before))
		}
		for i, id := range chunk.IDs {
			want, ok := before[id]
			if !ok {
				t.Fatalf("%s as of: node %d was no member at the pin", b.name, id)
			}
			for c := range keys {
				if got := chunkCell(t, &chunk, c, i); fmt.Sprint(got) != fmt.Sprint(want[c]) {
					t.Fatalf("%s as of node %d col %s: %v, at the pin %v", b.name, id, keys[c], got, want[c])
				}
			}
		}
		now, _, ok, err := g.Nodes().DocValuesSnapshot("C", keys)
		if err != nil || !ok {
			t.Fatalf("current snapshot after writes: ok=%v err=%v", ok, err)
		}
		assertChunksMatchRows(t, b.name+" after writes", now, len(keys), rng)
	})
}

// TestColumnChunkReuseAndBounds: a chunk read again into the same buffers
// allocates nothing; bounds outside the snapshot panic; chunks read from
// several goroutines at once agree with RowAt (run under -race).
func TestColumnChunkReuseAndBounds(t *testing.T) {
	g, err := graphpkg.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	ctx := context.Background()
	for k := range 4096 {
		if _, err := g.Nodes().Add(ctx, []string{"C"}, map[string]any{"i": int64(k), "s": fmt.Sprintf("v%d", k%50)}); err != nil {
			t.Fatal(err)
		}
	}
	snap, _, ok, err := g.Nodes().DocValuesSnapshot("C", []string{"i", "s"})
	if err != nil || !ok {
		t.Fatalf("snapshot: ok=%v err=%v", ok, err)
	}
	cr := snap.(types.NodeColumnChunkReader)
	var chunk types.ColumnChunk
	cr.ReadChunk(0, 1024, &chunk)
	if allocs := testing.AllocsPerRun(20, func() { cr.ReadChunk(1024, 2048, &chunk) }); allocs != 0 {
		t.Fatalf("ReadChunk into a sized chunk: %v allocations", allocs)
	}
	for _, bd := range [][2]int{{-1, 2}, {3, 2}, {0, cr.Len() + 1}} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("ReadChunk(%d, %d): no panic", bd[0], bd[1])
				}
			}()
			cr.ReadChunk(bd[0], bd[1], &chunk)
		}()
	}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var mine types.ColumnChunk
			vals, present := make([]any, 2), make([]bool, 2)
			for from := w * 512; from < cr.Len(); from += 8 * 512 {
				to := min(from+512, cr.Len())
				cr.ReadChunk(from, to, &mine)
				for i := range mine.IDs {
					id := cr.RowAt(from+i, vals, present)
					if mine.IDs[i] != id || mine.Cols[0].Ints[i] != vals[0].(int64) || mine.Cols[1].Dict[mine.Cols[1].Codes[i]] != vals[1].(string) {
						errs <- fmt.Errorf("worker %d row %d differs from RowAt", w, from+i)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}
