package graph_test

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Typed chunks of the doors that boxed (sigma-tkgd round 10 request 5):
// the label intersection, the ordinal doors and a column of strings on one
// part of a composite store and numbers on another.

// r10ChunkGraph writes nodes under labels A and B (and both) with an int,
// a float, a mixed numeric (incl. a uint64 above MaxInt64 and a float32)
// and a string column, some values absent.
func r10ChunkGraph(t *testing.T, g *graphpkg.Graph, seed uint64) {
	t.Helper()
	ctx := context.Background()
	rng := rand.New(rand.NewPCG(seed, 5))
	for k := range 240 {
		labels := [][]string{{"A"}, {"B"}, {"A", "B"}, {"A", "B", "C"}}[k%4]
		props := map[string]any{}
		if k%7 != 0 {
			props["i"] = int64(rng.IntN(1000) - 500)
		}
		if k%5 != 0 {
			props["f"] = rng.Float64() * 100
		}
		switch k % 3 {
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
		if _, err := g.Nodes().Add(ctx, labels, props); err != nil {
			t.Fatal(err)
		}
	}
}

// r10Rows is a door's rows: IDs, ordinals (when given) and boxed values.
type r10Rows struct {
	ids  []types.NodeID
	ords []uint32
	vals [][]any
}

func (r *r10Rows) add(id types.NodeID, ord uint32, vals []any, present []bool, withOrd bool) bool {
	r.ids = append(r.ids, id)
	if withOrd {
		r.ords = append(r.ords, ord)
	}
	row := make([]any, len(vals))
	for i := range vals {
		if present[i] {
			row[i] = vals[i]
		}
	}
	r.vals = append(r.vals, row)
	return true
}

// chunkRows reads every row of cr through one chunk (as the doors' streams
// hand them out).
func chunkRows(t *testing.T, cr types.NodeColumnChunkReader, width int) (rows r10Rows) {
	t.Helper()
	var chunk types.ColumnChunk
	cr.ReadChunk(0, cr.Len(), &chunk)
	rows.ids = chunk.IDs
	rows.ords = chunk.Ordinals
	for i := range chunk.IDs {
		row := make([]any, width)
		for c := range width {
			row[c] = chunkCell(t, &chunk, c, i)
		}
		rows.vals = append(rows.vals, row)
	}
	return rows
}

func sameRows(a, b r10Rows) bool {
	return slices.Equal(a.ids, b.ids) && slices.Equal(a.ords, b.ords) && fmt.Sprintf("%T%v", a.vals, a.vals) == fmt.Sprintf("%T%v", b.vals, b.vals)
}

// TestRound10SnapshotMultiAndOrdinals (5a, 5b): on every backend the
// intersection snapshot holds exactly ForEachDocValuesMulti's rows in its
// order with the same values and Go types, read by chunks exactly as RowAt;
// the ordinal snapshots carry ForEachDocValuesOrdinal's /
// ForEachDocValuesMultiOrdinal's ordinals beside the IDs (tiered numbers no
// node: they decline); a plain snapshot leaves Ordinals empty even in a
// chunk that held some; an empty intersection of known labels is an empty
// snapshot, an unknown label a decline; a snapshot taken before writes
// stays as it was (two-phase).
func TestRound10SnapshotMultiAndOrdinals(t *testing.T) {
	keys := []string{"i", "f", "m", "s", "none"}
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		r10ChunkGraph(t, g, 3)
		rng := rand.New(rand.NewPCG(4, 4))
		var stream r10Rows
		if _, ok, err := g.Nodes().ForEachDocValuesMulti([]string{"A", "B"}, keys, func(id types.NodeID, vals []any, present []bool) bool {
			return stream.add(id, 0, vals, present, false)
		}); err != nil || !ok {
			t.Fatalf("stream: ok=%v err=%v", ok, err)
		}
		snap, gen, ok, err := g.Nodes().DocValuesSnapshotMulti([]string{"A", "B"}, keys)
		if err != nil || !ok || snap == nil {
			t.Fatalf("multi snapshot: ok=%v err=%v", ok, err)
		}
		if gen == 0 {
			t.Fatalf("multi snapshot gen 0")
		}
		assertChunksMatchRows(t, b.name+" multi", snap, len(keys), rng)
		got := chunkRows(t, snap.(types.NodeColumnChunkReader), len(keys))
		if len(got.ids) != 120 || !sameRows(got, stream) {
			t.Fatalf("multi snapshot rows %d differ from ForEachDocValuesMulti's %d", len(got.ids), len(stream.ids))
		}
		if len(got.ords) != 0 {
			t.Fatalf("plain snapshot carried %d ordinals", len(got.ords))
		}

		// Ordinals.
		for _, multi := range []bool{false, true} {
			var want r10Rows
			var cr types.NodeColumnChunkReader
			var okStream, okSnap bool
			if multi {
				_, okStream, err = g.Nodes().ForEachDocValuesMultiOrdinal([]string{"A", "B"}, keys, func(id types.NodeID, ord uint32, vals []any, present []bool) bool {
					return want.add(id, ord, vals, present, true)
				})
				if err == nil {
					cr, _, okSnap, err = g.Nodes().DocValuesSnapshotMultiOrdinal([]string{"A", "B"}, keys)
				}
			} else {
				_, okStream, err = g.Nodes().ForEachDocValuesOrdinal("A", keys, func(id types.NodeID, ord uint32, vals []any, present []bool) bool {
					return want.add(id, ord, vals, present, true)
				})
				if err == nil {
					cr, _, okSnap, err = g.Nodes().DocValuesSnapshotOrdinal("A", keys)
				}
			}
			if err != nil || okStream != okSnap {
				t.Fatalf("multi=%v: stream ok %v, snapshot ok %v, err %v", multi, okStream, okSnap, err)
			}
			if b.tiered {
				if okSnap {
					t.Fatalf("multi=%v: tiered numbers no node, yet the ordinal snapshot served", multi)
				}
				continue
			}
			if !okSnap {
				t.Fatalf("multi=%v: ordinal snapshot declined on %s", multi, b.name)
			}
			assertChunksMatchRows(t, b.name+" ordinal", cr, len(keys), rng)
			gotOrd := chunkRows(t, cr, len(keys))
			if !sameRows(gotOrd, want) || slices.Contains(gotOrd.ords, 0) {
				t.Fatalf("multi=%v: ordinal snapshot rows/ordinals differ from the stream (%d rows, ordinals %v)", multi, len(gotOrd.ids), gotOrd.ords[:min(5, len(gotOrd.ords))])
			}
			for k, id := range gotOrd.ids {
				n, err := g.Nodes().Get(context.Background(), id)
				if err != nil || n.Ordinal() != gotOrd.ords[k] {
					t.Fatalf("multi=%v: node %d ordinal %d, chunk %d (%v)", multi, id, n.Ordinal(), gotOrd.ords[k], err)
				}
			}
			// A chunk that held ordinals, read again from a plain snapshot.
			var chunk types.ColumnChunk
			cr.ReadChunk(0, cr.Len(), &chunk)
			snap.(types.NodeColumnChunkReader).ReadChunk(0, 3, &chunk)
			if len(chunk.Ordinals) != 0 {
				t.Fatalf("plain snapshot left %d stale ordinals in a reused chunk", len(chunk.Ordinals))
			}
		}

		// Empty intersection of known labels; unknown labels.
		if _, err := g.Nodes().Add(context.Background(), []string{"Z"}, map[string]any{"i": int64(1)}); err != nil {
			t.Fatal(err)
		}
		empty, _, ok, err := g.Nodes().DocValuesSnapshotMulti([]string{"C", "Z"}, keys)
		if err != nil || !ok || empty.(types.NodeColumnChunkReader).Len() != 0 {
			t.Fatalf("empty intersection: ok=%v err=%v", ok, err)
		}
		for _, labels := range [][]string{{"A", "NoSuch"}, {}} {
			if _, _, ok, err := g.Nodes().DocValuesSnapshotMulti(labels, keys); err != nil || ok {
				t.Fatalf("labels %v: ok=%v err=%v, want a decline", labels, ok, err)
			}
			if _, _, ok, err := g.Nodes().DocValuesSnapshotMultiOrdinal(labels, keys); err != nil || ok {
				t.Fatalf("ordinal labels %v: ok=%v err=%v, want a decline", labels, ok, err)
			}
		}
		if _, _, ok, err := g.Nodes().DocValuesSnapshotOrdinal("NoSuch", keys); err != nil || ok {
			t.Fatalf("unknown label: ok=%v err=%v", ok, err)
		}

		// Two-phase: later writes leave a taken snapshot as it was.
		ctx := context.Background()
		for _, id := range got.ids[:20] {
			if _, err := g.Nodes().Update(ctx, id, map[string]any{"i": int64(424242), "s": "later"}); err != nil {
				t.Fatal(err)
			}
		}
		if again := chunkRows(t, snap.(types.NodeColumnChunkReader), len(keys)); !sameRows(again, got) {
			t.Fatal("a snapshot changed after later writes")
		}
		now, _, ok, err := g.Nodes().DocValuesSnapshotMulti([]string{"A", "B"}, keys)
		if err != nil || !ok {
			t.Fatalf("after writes: ok=%v err=%v", ok, err)
		}
		if fresh := chunkRows(t, now.(types.NodeColumnChunkReader), len(keys)); sameRows(fresh, got) {
			t.Fatal("a snapshot taken after the writes shows none of them")
		}
	})
}

// TestRound10OrdinalSnapshotDeclinesWithoutOrdinals: badger with
// DisableOrdinals numbers no node, so the ordinal snapshots decline (no
// zeros handed out), as ForEachDocValuesOrdinal does.
func TestRound10OrdinalSnapshotDeclinesWithoutOrdinals(t *testing.T) {
	st, err := badger.New(badger.Config{InMemory: true, DisableOrdinals: true})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graphpkg.New(graphpkg.Config{Store: st})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if _, err := g.Nodes().Add(context.Background(), []string{"A", "B"}, map[string]any{"v": int64(1)}); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, err := g.Nodes().DocValuesSnapshotOrdinal("A", []string{"v"}); err != nil || ok {
		t.Fatalf("single: ok=%v err=%v", ok, err)
	}
	if _, _, ok, err := g.Nodes().DocValuesSnapshotMultiOrdinal([]string{"A", "B"}, []string{"v"}); err != nil || ok {
		t.Fatalf("multi: ok=%v err=%v", ok, err)
	}
	if _, _, ok, err := g.Nodes().DocValuesSnapshotMulti([]string{"A", "B"}, []string{"v"}); err != nil || !ok {
		t.Fatalf("plain intersection snapshot: ok=%v err=%v", ok, err)
	}
}

// TestRound10MixedColumnAcrossParts (5c): a column that holds strings on
// one part of a composite store and numbers on another (tiered: the
// reference shard and an event shard; sharded: two slots, in the sharded
// package's TestShardedMixedColumnChunks) reads by chunks
// as ColumnVectorMixed, row by row exactly what RowAt hands out (tags,
// values, Go types, absent rows), single label and intersection; the
// stores that keep one part (memory, badger) still decline the column (a
// part's column holds one kind).
func TestRound10MixedColumnAcrossParts(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		if b.name == "sharded" {
			// One graph handle mints every node on one slot; the sharded
			// package's TestShardedMixedColumnChunks spreads them over lanes.
			return
		}
		ctx := context.Background()
		rng := rand.New(rand.NewPCG(8, 8))
		var strs, nums int
		for k := range 200 {
			labels := []string{"A", "B"}
			if b.tiered && k%2 == 0 {
				labels = []string{"Ref", "A", "B"}
			}
			n, err := g.Nodes().Add(ctx, labels, nil)
			if err != nil {
				t.Fatal(err)
			}
			part := k % 2
			props := map[string]any{"k": int64(k)}
			switch {
			case k%9 == 0: // absent
			case part == 0:
				props["x"] = fmt.Sprintf("s%d", k%13)
				strs++
			case k%3 == 0:
				props["x"] = float64(k) + 0.25
				nums++
			default:
				props["x"] = int64(k)
				nums++
			}
			if _, err := g.Nodes().Update(ctx, n.ID(), props); err != nil {
				t.Fatal(err)
			}
		}
		if strs == 0 || nums == 0 {
			t.Fatalf("generator: %d strings, %d numbers", strs, nums)
		}
		keys := []string{"x", "k"}
		composite := b.tiered
		for _, multi := range []bool{false, true} {
			var snap types.NodeColumnReader
			var ok bool
			var err error
			if multi {
				snap, _, ok, err = g.Nodes().DocValuesSnapshotMulti([]string{"A", "B"}, keys)
			} else {
				snap, _, ok, err = g.Nodes().DocValuesSnapshot("A", keys)
			}
			if err != nil {
				t.Fatal(err)
			}
			if !composite {
				if ok {
					t.Fatalf("multi=%v: %s served a column of strings and numbers", multi, b.name)
				}
				continue
			}
			if !ok {
				t.Fatalf("multi=%v: snapshot declined", multi)
			}
			assertChunksMatchRows(t, fmt.Sprintf("%s mixed multi=%v", b.name, multi), snap, len(keys), rng)
			cr := snap.(types.NodeColumnChunkReader)
			var chunk types.ColumnChunk
			cr.ReadChunk(0, cr.Len(), &chunk)
			x := chunk.Cols[0]
			if x.Kind != types.ColumnVectorMixed || len(x.Tags) != cr.Len() || len(x.IsFloat) != 0 {
				t.Fatalf("multi=%v: kind %d, %d tags for %d rows", multi, x.Kind, len(x.Tags), cr.Len())
			}
			seen := map[types.ColumnVectorKind]int{}
			for i, p := range x.Present {
				if !p && x.Tags[i] != 0 {
					t.Fatalf("absent row %d tagged %d", i, x.Tags[i])
				}
				if p {
					seen[x.Tags[i]]++
				}
			}
			if seen[types.ColumnVectorString] != strs || seen[types.ColumnVectorInt]+seen[types.ColumnVectorFloat] != nums || seen[types.ColumnVectorFloat] == 0 {
				t.Fatalf("multi=%v: tags %v, want %d strings and %d numbers", multi, seen, strs, nums)
			}
		}
	})
}
