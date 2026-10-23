package sharded_test

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestShardedMixedColumnChunks (sigma-tkgd round 10 request 5c): a label
// spread over several slots whose column holds strings on the even slots
// and numbers on the odd ones reads by typed chunks as ColumnVectorMixed:
// every position equals RowAt (tag, value, Go type, absent rows), single
// label and intersection; the ordinal snapshots carry ForEachDocValuesOrdinal's
// ordinals across the slots.
func TestShardedMixedColumnChunks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g := newLanedShardedGraph(t, 4)
	nodes, slots := addSpread(t, g, []string{"P", "Q"}, 60, func(i int) map[string]any { return map[string]any{"k": int64(i)} })
	if len(slots) < 2 {
		t.Fatalf("nodes on %d slot(s), want several", len(slots))
	}
	strs, nums := 0, 0
	for i, n := range nodes {
		slot := g.Admin().DecomposeNodeID(n.ID()).NodeID
		var v any
		switch {
		case i%9 == 0:
			continue
		case slot%2 == 0:
			v, strs = fmt.Sprintf("s%d", i%7), strs+1
		case i%2 == 0:
			v, nums = float64(i)+0.5, nums+1
		default:
			v, nums = int64(i), nums+1
		}
		if _, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"x": v}); err != nil {
			t.Fatal(err)
		}
	}
	if strs == 0 || nums == 0 {
		t.Fatalf("%d strings, %d numbers: the slots did not split the kinds", strs, nums)
	}
	keys := []string{"x", "k"}
	for _, multi := range []bool{false, true} {
		var snap types.NodeColumnReader
		var ok bool
		var err error
		if multi {
			snap, _, ok, err = g.Nodes().DocValuesSnapshotMulti([]string{"P", "Q"}, keys)
		} else {
			snap, _, ok, err = g.Nodes().DocValuesSnapshot("P", keys)
		}
		if err != nil || !ok {
			t.Fatalf("multi=%v: ok=%v err=%v", multi, ok, err)
		}
		cr, isChunks := snap.(types.NodeColumnChunkReader)
		if !isChunks {
			t.Fatalf("multi=%v: snapshot %T reads no chunks", multi, snap)
		}
		var chunk types.ColumnChunk
		cr.ReadChunk(0, cr.Len(), &chunk)
		x := chunk.Cols[0]
		if x.Kind != types.ColumnVectorMixed {
			t.Fatalf("multi=%v: kind %d, want mixed", multi, x.Kind)
		}
		vals, present := make([]any, 2), make([]bool, 2)
		tags := map[types.ColumnVectorKind]int{}
		for i := range cr.Len() {
			id := cr.RowAt(i, vals, present)
			if chunk.IDs[i] != id || x.Present[i] != present[0] {
				t.Fatalf("multi=%v row %d: id %d present %v, RowAt %d %v", multi, i, chunk.IDs[i], x.Present[i], id, present[0])
			}
			if !present[0] {
				if x.Tags[i] != 0 {
					t.Fatalf("absent row %d tagged %d", i, x.Tags[i])
				}
				continue
			}
			tags[x.Tags[i]]++
			var got any
			switch x.Tags[i] {
			case types.ColumnVectorInt:
				got = x.Ints[i]
			case types.ColumnVectorFloat:
				got = x.Flts[i]
			case types.ColumnVectorString:
				got = x.Dict[x.Codes[i]]
			}
			if fmt.Sprintf("%T %v", got, got) != fmt.Sprintf("%T %v", vals[0], vals[0]) {
				t.Fatalf("multi=%v row %d: chunk %T %v, RowAt %T %v", multi, i, got, got, vals[0], vals[0])
			}
		}
		if tags[types.ColumnVectorString] != strs || tags[types.ColumnVectorInt]+tags[types.ColumnVectorFloat] != nums {
			t.Fatalf("multi=%v: tags %v, want %d strings %d numbers", multi, tags, strs, nums)
		}
	}
	// Ordinals across the slots.
	var ids []types.NodeID
	var ords []uint32
	if _, ok, err := g.Nodes().ForEachDocValuesOrdinal("P", []string{"k"}, func(id types.NodeID, ord uint32, _ []any, _ []bool) bool {
		ids, ords = append(ids, id), append(ords, ord)
		return true
	}); err != nil || !ok {
		t.Fatalf("ordinal stream: ok=%v err=%v", ok, err)
	}
	cr, _, ok, err := g.Nodes().DocValuesSnapshotOrdinal("P", []string{"k"})
	if err != nil || !ok {
		t.Fatalf("ordinal snapshot: ok=%v err=%v", ok, err)
	}
	var chunk types.ColumnChunk
	cr.ReadChunk(0, cr.Len(), &chunk)
	if !slices.Equal(chunk.IDs, ids) || !slices.Equal(chunk.Ordinals, ords) {
		t.Fatalf("ordinal snapshot: %d ids / %d ordinals differ from the stream's %d", len(chunk.IDs), len(chunk.Ordinals), len(ids))
	}
}
