package sharded_test

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

type shardedColumnRow struct {
	id      types.NodeID
	val     any
	present bool
}

func sortRowsByID(rows []shardedColumnRow) []shardedColumnRow {
	s := slices.Clone(rows)
	slices.SortFunc(s, func(a, b shardedColumnRow) int {
		switch {
		case a.id.SnowflakeID() < b.id.SnowflakeID():
			return -1
		case a.id.SnowflakeID() > b.id.SnowflakeID():
			return 1
		}
		return 0
	})
	return s
}

// rowsFromRecords is the ground truth: every current member of label with
// its key read from the node's own row.
func rowsFromRecords(t *testing.T, g *graph.Graph, label, key string) []shardedColumnRow {
	t.Helper()
	var out []shardedColumnRow
	if err := g.Nodes().ForEachByLabel(label, storepkg.QueryOpts{}, func(n *types.Node) bool {
		v, ok := n.GetProperty(key)
		out = append(out, shardedColumnRow{n.ID(), v, ok})
		return true
	}); err != nil {
		t.Fatal(err)
	}
	return sortRowsByID(out)
}

func streamedRows(t *testing.T, g *graph.Graph, labels []string, key string) ([]shardedColumnRow, uint64, bool) {
	t.Helper()
	var out []shardedColumnRow
	fn := func(id types.NodeID, vals []any, present []bool) bool {
		out = append(out, shardedColumnRow{id, vals[0], present[0]})
		return true
	}
	var (
		gen uint64
		ok  bool
		err error
	)
	if len(labels) == 1 {
		gen, ok, err = g.Nodes().ForEachDocValues(labels[0], []string{key}, fn)
	} else {
		gen, ok, err = g.Nodes().ForEachDocValuesMulti(labels, []string{key}, fn)
	}
	if err != nil {
		t.Fatal(err)
	}
	return out, gen, ok
}

func positionRows(rr types.NodeColumnRowReader, workers int) []shardedColumnRow {
	out := make([]shardedColumnRow, rr.Len())
	per := (rr.Len() + workers - 1) / workers
	var wg sync.WaitGroup
	for w := range workers {
		lo, hi := w*per, min((w+1)*per, rr.Len())
		wg.Add(1)
		go func() {
			defer wg.Done()
			vals, present := make([]any, 1), make([]bool, 1)
			for i := lo; i < hi; i++ {
				id := rr.RowAt(i, vals, present)
				out[i] = shardedColumnRow{id, vals[0], present[0]}
			}
		}()
	}
	wg.Wait()
	return out
}

// addSpread creates count nodes with labels and props(i), one ingest session
// per node so the nodes land on several slots; returns them and the slots used.
func addSpread(t *testing.T, g *graph.Graph, labels []string, count int, props func(i int) map[string]any) ([]*types.Node, map[int64]struct{}) {
	t.Helper()
	slots := map[int64]struct{}{}
	nodes := make([]*types.Node, 0, count)
	for i := range count {
		sess, err := g.Ingest().NewSession(ingest.IngestOptions{Concurrent: true})
		if err != nil {
			t.Fatal(err)
		}
		n, err := sess.AddNode(labels, props(i))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := sess.Submit(); err != nil {
			t.Fatal(err)
		}
		_ = sess.Close()
		slots[g.Admin().DecomposeNodeID(n.ID()).NodeID] = struct{}{}
		nodes = append(nodes, n)
	}
	return nodes, slots
}

// The sharded store serves DocValues columns across its slots: on a label
// spread over four lane slots, ForEachDocValues, ForEachDocValuesMulti and the
// snapshot (Row, Len/RowAt with one and four workers) hand out every member
// exactly once with the value its own row holds, members without the property
// absent; a non-member and an unclaimed-slot ID are not rows. Two-phase: a
// snapshot taken before an update keeps the old value, one taken after has the
// new, and the label's epoch (the stream's gen) moves with the write. A mixed
// column and an empty label decline; DocValuesColumn states numeric for the
// pure column and none for the mixed one.
func TestShardedDocValuesAcrossSlots(t *testing.T) {
	ctx := context.Background()
	g := newLanedShardedGraph(t, 4)
	nodes, slots := addSpread(t, g, []string{"P"}, 60, func(i int) map[string]any {
		p := map[string]any{"age": int64(i), "mixed": int64(i)}
		if i%7 == 0 {
			p = map[string]any{"other": "x"}
		}
		if i == 5 {
			p["mixed"] = "five"
		}
		return p
	})
	if len(slots) < 2 {
		t.Fatalf("nodes on %d slot(s), want several", len(slots))
	}
	both, _ := addSpread(t, g, []string{"P", "Q"}, 12, func(i int) map[string]any { return map[string]any{"age": int64(1000 + i)} })
	nodes = append(nodes, both...)
	if _, _, err := addSpreadOther(g); err != nil {
		t.Fatal(err)
	}

	truth := rowsFromRecords(t, g, "P", "age")
	if len(truth) != 72 {
		t.Fatalf("%d members, want 72", len(truth))
	}
	streamed, gen, ok := streamedRows(t, g, []string{"P"}, "age")
	if !ok {
		t.Fatal("ForEachDocValues declined on sharded")
	}
	if !slices.Equal(sortRowsByID(streamed), truth) {
		t.Fatalf("streamed rows differ from the rows' values:\n%v\n%v", sortRowsByID(streamed), truth)
	}
	if gen != g.Nodes().NodeLabelMutationEpoch("P") {
		t.Fatalf("gen %d, label epoch %d", gen, g.Nodes().NodeLabelMutationEpoch("P"))
	}

	multi, _, ok := streamedRows(t, g, []string{"P", "Q"}, "age")
	if !ok {
		t.Fatal("ForEachDocValuesMulti declined on sharded")
	}
	if want := rowsFromRecords(t, g, "Q", "age"); !slices.Equal(sortRowsByID(multi), want) {
		t.Fatalf("intersection rows %v, want %v", sortRowsByID(multi), want)
	}

	reader, snapGen, ok, err := g.Nodes().DocValuesSnapshot("P", []string{"age"})
	if err != nil || !ok {
		t.Fatalf("DocValuesSnapshot on sharded: %v %v", ok, err)
	}
	if snapGen != gen || reader.Epoch() != gen {
		t.Fatalf("snapshot gen %d / epoch %d, want %d", snapGen, reader.Epoch(), gen)
	}
	rr, isRows := reader.(types.NodeColumnRowReader)
	if !isRows || rr.Len() != 72 {
		t.Fatalf("by position: %v, Len %d", isRows, rr.Len())
	}
	one := positionRows(rr, 1)
	if four := positionRows(rr, 4); !slices.Equal(one, four) {
		t.Fatal("four workers read other rows than one")
	}
	if !slices.Equal(sortRowsByID(one), truth) {
		t.Fatal("the positions do not hold every member once")
	}
	if !slices.Equal(one, streamed) {
		t.Fatal("positions and the stream are not in the same order")
	}
	vals, present := make([]any, 1), make([]bool, 1)
	for _, r := range truth {
		if !reader.Row(r.id, vals, present) || vals[0] != r.val || present[0] != r.present {
			t.Fatalf("Row(%d) = %v %v, want %v %v", r.id, vals[0], present[0], r.val, r.present)
		}
	}
	outsider, err := g.Nodes().Add(ctx, []string{"Elsewhere"}, map[string]any{"age": int64(5)})
	if err != nil {
		t.Fatal(err)
	}
	if reader.Row(outsider.ID(), vals, present) {
		t.Fatal("a non-member is a row")
	}
	if reader.Row(types.NodeID(31<<10|1<<20), vals, present) {
		t.Fatal("an ID of an unclaimed slot is a row")
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("RowAt(Len) did not panic")
			}
		}()
		rr.RowAt(rr.Len(), vals, present)
	}()

	// Two-phase: update a member on a lane slot.
	target := nodes[3]
	if _, err := g.Nodes().Update(ctx, target.ID(), map[string]any{"age": int64(-1)}); err != nil {
		t.Fatal(err)
	}
	if g.Nodes().NodeLabelMutationEpoch("P") == gen {
		t.Fatal("the label epoch did not move with a write on a lane slot")
	}
	if !reader.Row(target.ID(), vals, present) || vals[0] != int64(3) {
		t.Fatalf("the earlier snapshot reads %v, want 3", vals[0])
	}
	after, _, ok, err := g.Nodes().DocValuesSnapshot("P", []string{"age"})
	if err != nil || !ok {
		t.Fatal(err)
	}
	if !after.Row(target.ID(), vals, present) || vals[0] != int64(-1) {
		t.Fatalf("the later snapshot reads %v, want -1", vals[0])
	}

	// Declines.
	if _, _, ok := streamedRows(t, g, []string{"P"}, "mixed"); ok {
		t.Fatal("a column mixing numbers and strings did not decline")
	}
	if _, _, ok, err := g.Nodes().DocValuesSnapshot("Missing", []string{"age"}); ok || err != nil {
		t.Fatalf("an unknown label: %v %v", ok, err)
	}
	if kind, ok, err := g.Nodes().DocValuesColumn("P", "age"); kind != storepkg.DocValuesNumeric || !ok || err != nil {
		t.Fatalf("DocValuesColumn age: %v %v %v", kind, ok, err)
	}
	if kind, ok, err := g.Nodes().DocValuesColumn("P", "mixed"); kind != storepkg.DocValuesNone || !ok || err != nil {
		t.Fatalf("DocValuesColumn mixed: %v %v %v", kind, ok, err)
	}
}

// addSpreadOther adds noise: nodes of another label on the interactive slot.
func addSpreadOther(g *graph.Graph) (int, bool, error) {
	for i := range 5 {
		if _, err := g.Nodes().Add(context.Background(), []string{"Noise"}, map[string]any{"age": fmt.Sprint(i)}); err != nil {
			return i, false, err
		}
	}
	return 5, true, nil
}

// Concurrent readers of one sharded snapshot (run under -race).
func TestShardedDocValuesSnapshotConcurrentReads(t *testing.T) {
	g := newLanedShardedGraph(t, 4)
	addSpread(t, g, []string{"P"}, 40, func(i int) map[string]any { return map[string]any{"age": int64(i)} })
	reader, _, ok, err := g.Nodes().DocValuesSnapshot("P", []string{"age"})
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	rr := reader.(types.NodeColumnRowReader)
	want := positionRows(rr, 1)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got := positionRows(rr, 3)
			vals, present := make([]any, 1), make([]bool, 1)
			for _, r := range want {
				reader.Row(r.id, vals, present)
			}
			if !slices.Equal(got, want) {
				t.Error("concurrent reads differ")
			}
		}()
	}
	wg.Wait()
}
