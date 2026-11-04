package graph_test

import (
	"bytes"
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BORROWREAD: a 64 KiB []byte property read with BorrowProperty from a lent
// row is the row's own array, on memory and on badger (the row read back from
// disk after a reopen): two borrows hand out the same bytes, while
// GetProperty keeps its copy (the allocation count, 0, is logged). A later
// Update or UpdateInPlace of the node, and closing the store, leave the
// borrowed bytes as they were. Not parallel: AllocsPerRun (the log) refuses a
// parallel test.
func TestBorrowPropertyReadsABlobWithoutACopy(t *testing.T) {
	for _, b := range []struct {
		name string
		dir  bool
	}{{"memory", false}, {"badger", true}} {
		t.Run(b.name, func(t *testing.T) {
			dir := ""
			if b.dir {
				dir = t.TempDir()
			}
			g, err := graphpkg.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			blob := bytes.Repeat([]byte{7}, 64<<10)
			n, err := g.Nodes().Add(ctx, []string{"Block"}, map[string]any{"b": blob, "k": []int64{1, 2, 3}})
			if err != nil {
				t.Fatal(err)
			}
			if b.dir {
				if err := g.Close(); err != nil {
					t.Fatal(err)
				}
				if g, err = graphpkg.Open(dir); err != nil {
					t.Fatal(err)
				}
			}
			defer func() { _ = g.Close() }()
			row, err := g.Nodes().Lend(ctx, n.ID())
			if err != nil {
				t.Fatal(err)
			}
			v, _ := row.BorrowProperty("b")
			got := v.([]byte)
			if !bytes.Equal(got, blob) {
				t.Fatal("borrowed bytes differ from the written blob")
			}
			if again, _ := row.BorrowProperty("b"); &again.([]byte)[0] != &got[0] {
				t.Fatal("BorrowProperty of a 64 KiB []byte: a second borrow got other bytes, want the row's own")
			}
			k1, _ := row.BorrowProperty("k")
			k2, _ := row.BorrowProperty("k")
			if l1, l2 := k1.([]int64), k2.([]int64); len(l1) != 3 || &l1[0] != &l2[0] {
				t.Fatalf("BorrowProperty of a []int64: %v and %v, want the row's own [1 2 3] twice", l1, l2)
			}
			t.Logf("BorrowProperty: %.0f allocs for the []byte, %.0f for the []int64",
				testing.AllocsPerRun(20, func() { _, _ = row.BorrowProperty("b") }),
				testing.AllocsPerRun(20, func() { _, _ = row.BorrowProperty("k") }))
			if _, ok := row.BorrowProperty("c"); ok {
				t.Fatal("BorrowProperty found a missing key")
			}
			v, _ = row.GetProperty("b")
			if c := v.([]byte); &c[0] == &got[0] {
				t.Fatal("GetProperty returned the stored bytes, want a copy")
			}
			if _, err := g.Nodes().UpdateInPlace(ctx, n.ID(), map[string]any{"b": bytes.Repeat([]byte{8}, 64<<10)}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"b": bytes.Repeat([]byte{9}, 64<<10)}); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, blob) {
				t.Fatal("a write to the node changed the borrowed bytes")
			}
			now, err := g.Nodes().Lend(ctx, n.ID())
			if err != nil {
				t.Fatal(err)
			}
			if v, _ := now.BorrowProperty("b"); v.([]byte)[0] != 9 {
				t.Fatalf("current row borrows %d, want the updated 9", v.([]byte)[0])
			}
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, blob) {
				t.Fatal("closing the store changed the borrowed bytes")
			}
		})
	}
	var nilNode *types.Node
	if _, ok := nilNode.BorrowProperty("b"); ok {
		t.Fatal("nil node BorrowProperty found a key")
	}
}
