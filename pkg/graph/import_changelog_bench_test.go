package graph_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkImport_ChangeLogCost sizes the import-under-a-scope backlog item.
//
// Import emits change-log records EAGERLY, one at a time, outside any scope. The
// gap between ChangeLog on and off is what that eager emission costs, and therefore
// the ceiling on what buffering it into a TxChangeLogScope could recover:
//
//	5,000 nodes + 5,000 rels   time      allocs
//	ChangeLog off              216ms     1.4M
//	ChangeLog on               621ms     7.0M     (2.9x time, 5x allocs, variance <1%)
//
// Read it as the SIZE OF THE PRIZE, not as a promise the fix collects it. A scope
// buffers records in memory (badger: bs.scopeLog, an unbounded [][]byte released
// only at commit) and an import is the whole graph rather than one small
// transaction — so wrapping import in a scope trades this time cost for a memory
// cost proportional to the entire import. That is a second blocker the backlog item
// does not record, alongside the locking change it does.
func benchImportPayload(b *testing.B, n int) []byte {
	b.Helper()
	src, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 1, BadgerInMemory: true})
	if err != nil {
		b.Fatalf("source graph: %v", err)
	}
	defer src.Close()
	ctx := context.Background()
	var prev *types.Node
	for i := range n {
		nd, err := src.Nodes().Add(ctx, []string{"P"},
			map[string]any{"i": int64(i), "city": fmt.Sprintf("c%d", i%13)})
		if err != nil {
			b.Fatalf("Add: %v", err)
		}
		if prev != nil {
			if _, err := src.Rels().Add(ctx, "NEXT", prev, nd, nil); err != nil {
				b.Fatalf("Rels().Add: %v", err)
			}
		}
		prev = nd
	}
	var buf bytes.Buffer
	if err := src.IO().Export(&buf); err != nil {
		b.Fatalf("Export: %v", err)
	}
	return buf.Bytes()
}

func BenchmarkImport_ChangeLogCost(b *testing.B) {
	const n = 5000
	payload := benchImportPayload(b, n)
	for _, cl := range []bool{false, true} {
		b.Run(fmt.Sprintf("changelog=%v", cl), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				g, err := graphpkg.New(graphpkg.Config{
					SnowflakeNodeID: 2, BadgerInMemory: true, ChangeLog: cl,
				})
				if err != nil {
					b.Fatalf("New: %v", err)
				}
				b.StartTimer()
				if err := g.IO().Import(bytes.NewReader(payload), tkgio.ImportOptions{}); err != nil {
					b.Fatalf("Import: %v", err)
				}
				b.StopTimer()
				g.Close()
				b.StartTimer()
			}
		})
	}
}
