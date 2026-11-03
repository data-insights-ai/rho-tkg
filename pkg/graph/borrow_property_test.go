package graph_test

import (
	"bytes"
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BORROWREAD: a 64 KiB []byte property read with BorrowProperty from a lent
// row allocates nothing, on memory and on badger (the row read back from disk
// after a reopen), while GetProperty keeps its copy. A later Update or
// UpdateInPlace of the node, and closing the store, leave the borrowed bytes
// as they were. Not parallel: AllocsPerRun refuses a parallel test.
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
			var got []byte
			if allocs := testing.AllocsPerRun(20, func() {
				v, _ := row.BorrowProperty("b")
				got = v.([]byte)
			}); allocs != 0 {
				t.Fatalf("BorrowProperty of a 64 KiB []byte: %.0f allocs, want 0", allocs)
			}
			if !bytes.Equal(got, blob) {
				t.Fatal("borrowed bytes differ from the written blob")
			}
			if allocs := testing.AllocsPerRun(20, func() { _, _ = row.BorrowProperty("k") }); allocs != 0 {
				t.Fatalf("BorrowProperty of a []int64: %.0f allocs, want 0", allocs)
			}
			if _, ok := row.BorrowProperty("c"); ok {
				t.Fatal("BorrowProperty found a missing key")
			}
			v, _ := row.GetProperty("b")
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
